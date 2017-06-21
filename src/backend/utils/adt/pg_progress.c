/*
 * progress.c
 *	  Monitor progression of request: PROGRESS
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/monitor.c
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#include "nodes/nodes.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "catalog/pg_type.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/pg_progress.h"
#include "access/xact.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "storage/backendid.h"
#include "storage/dsm.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/execParallel.h"
#include "commands/defrem.h"
#include "access/relscan.h"
#include "access/parallel.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/tuplesort.h"
#include "utils/tuplestore.h"
#include "storage/buffile.h"
#include "utils/ruleutils.h"
#include "postmaster/bgworker_internals.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"
#include "pgstat.h"


/*
 * Buffer in which the full report is collected before being transformed into
 * tuples to be sent by the monitoring backend.
 */
char* progress_buf;
unsigned int progress_buf_len;

/* 
 * Monitoring progress waits 5 seconds for monitored backend response, by default (Guc parameter).
 *
 * If this timeout is too short, it may not leave enough time for monotored backend to dump its
 * progression about the SQL query it is running.
 *
 * If this timeout is too long, a cancelled SQL query in a backend could block the monitoring
 * backend too for a longi time.
 */
int progress_timeout = 5;

char* msg_timeout		= "<backend timeout>";
char* msg_idle			= "<idle backend>";
char* msg_out_of_xact		= "<out of transaction>";
char* msg_null_plan		= "<NULL planned statement>";
char* msg_utility_stmt		= "<utility statement>";
char* msg_qry_not_started	= "<query not yet started>";
char* msg_qry_cancel		= "<query cancel pending>";
char* msg_reco_pending 		= "<recovery conflict pending>";
char* msg_proc_die		= "<proc die pending>";
char* msg_qry_running		= "<query running>";

/*
 * Backend type (single worker, parallel main worker, parallel child worker
 */
#define SINGLE_WORKER		0
#define MAIN_WORKER		1
#define CHILD_WORKER		2

/*
 * Number of colums for pg_progress SQL function and field size with trailing '\0'
 */
#define PG_PROGRESS_COLS	9

#define PG_PROGRESS_PID		9
#define PG_PROGRESS_BID		9
#define PG_PROGRESS_LINEID 	9
#define PG_PROGRESS_INDENT	9
#define PG_PROGRESS_TYPE	65
#define PG_PROGRESS_NAME	257
#define PG_PROGRESS_VALUE	257
#define PG_PROGRESS_UNIT	9

/*
 * Progress node type
 */
#define PROP			"property"
#define NODE			"node"
#define RELATIONSHIP		"relationship"

/*
 * Units for reports
 */
#define NO_UNIT			""
#define BLK_UNIT		"block"
#define ROW_UNIT		"row"
#define PERCENT_UNIT		"percent"
#define SECOND_UNIT		"second"
#define BYTE_UNIT		"byte"
#define KBYTE_UNIT		"KByte"
#define MBYTE_UNIT		"MByte"
#define GBYTE_UNIT		"Gbyte"
#define TBYTE_UNIT		"TByte"

/*
 * Verbosity report level
 */
#define VERBOSE_TIME_REPORT		1
#define VERBOSE_DISK_USE		1

#define VERBOSE_ROW_SCAN		2
#define VERBOSE_INDEX_SCAN		2
#define	VERBOSE_BUFFILE			2
#define VERBOSE_HASH_JOIN		2
#define VERBOSE_TAPES			2
#define VERBOSE_BACKEND_SELF		2
#define VERBOSE_BACKEND_OTHER		2
#define VERBOSE_BACKEND_MONITOR		2

#define VERBOSE_TARGETS			3
#define VERBOSE_STACK			3
#define VERBOSE_HASH_JOIN_DETAILED	3
#define VERBOSE_TAPES_DETAILED		3
#define VERBOSE_STMT			3

/*
 * Report only SQL querries which have been running longer than this value
 */
int progress_time_threshold = 3;

/*
 * One ProgressCtl is allocated for each backend process.
 *
 * One backend can be monitored by one other backend at a time.
 * The LWLock ensure that one backend can be only monitored by one other backend at a time.
 * But monitoring backends can interleave their requests of progress report
 */
#define MAX_WORKERS			MAX_PARALLEL_WORKER_LIMIT

typedef struct ProgressCtl {
	bool monitoring;		/* True if monitoring backend. Avoid cross monitoring */
        bool verbose;			/* Verbosity */

	bool parallel;			/* true if parallel query */
	bool child;			/* true if child worker, false if main worker */
	unsigned int child_indent;	/* Indentation base value for child worker */
	int child_pid[MAX_WORKERS];

	unsigned long disk_size;	/* Disk size in bytes used by the backend for sorts, stores, hashes */

	unsigned short command;		/* What we ask from the backend */
	unsigned short status;		/* Status of progress collection */
	
	char* shm_page;			/* Share memory page to dump progress report */
	unsigned short shm_len;		/* Content length in shm page */
	unsigned int report_size;	/* total length of report */

	
	struct Latch latch;		/* Used by requestor to wait for backend to complete its report */
	LWLock lock;			/* Used to serialize request for progress report against a backend */

	unsigned short proc_cnt;	/* nomber of proc requesting this backend: debugging stuff */
	int pid;
} ProgressCtl;

#define SIZE_OF_PID_ARRAY 		(MAX_WORKERS * sizeof(int))

/* Command above */
#define PRG_CTL_CMD_UNDEFINED		0	
#define PRG_CTL_CMD_COLLECT		1	/* Start progress collection */
#define PRG_CTL_CMD_CONT		2	/* Continue to send progress data */

/* Status above */
#define PRG_CTL_STATUS_UNDEFINED	0
#define PRG_CTL_STATUS_OK		1	/* Progress report completed */
#define PRG_CTL_STATUS_MORE		2	/* More data in progress report */
#define PRG_CTL_STATUS_FAILED		3	/* Failed to collect data */

static char progress_command[3][10] = { "UNDEFINED", "COLLECT", "CONT" };
static char progress_status[3][10] = { "UNDEFINED", "OK", "MORE" };

struct ProgressCtl* progress_ctl_array;	/* Array of MaxBackends ProgressCtl */
char* progress_shm_buf;			/* Array of MaxBackends BLCKSZ share memory to dump progress in */

static void ProgressLockRequest(ProgressCtl* req);
static void ProgressUnlockRequest(ProgressCtl* req);


typedef struct ProgressState {
	int pid;			/* pid of backend of child worker if parallel */
	int ppid;			/* pid of parent worker */
	int bid;

	/*
	 * State for output formating
	 */
	int indent;			/* current indentation level */
	int lineid;			/* needed for indentation */
	bool verbose;			/* be verbose */
	StringInfo str;			/* output buffer */

	/*
	 * Parallelism data
	 */
	bool parallel;			/* true if parallel query */
	bool child;			/* true if parallel and child backend */

	/*
	 * Track report local memory context
	 */ 
	MemoryContext memcontext;

	/*
	 * State related to current plan/execution tree
	 */
	PlannedStmt* pstmt;
	struct Plan* plan;
	struct PlanState* planstate;
	List* rtable;
	List* rtable_names;
	List* deparse_cxt;		/* context list for deparsing expressions */
	EState* es;			/* Top level data */
	Bitmapset* printed_subplans;    /* ids of SubPlans we've printed */

	unsigned long disk_size;        /* track on disk use for sorts, stores, and hashes */
} ProgressState;

/*
 * No progress request unless requested.
 */
volatile bool progress_requested = false;
ProgressState* progress_state;
MemoryContext progress_context;

unsigned int progress_report_size;
char* progress_cursor;


/*
 * local functions
 */
static void ProgressUtility(PlannedStmt* pstmt, ProgressState* ps);
static void ProgressIndexStmt(IndexStmt* stmt, ProgressState* ps);
static void ProgressPlan(QueryDesc* query, ProgressState* ps);
static void ProgressNode(PlanState* planstate, List* ancestors,
	const char* relationship, const char* plan_name, ProgressState* ps);

static ProgressState* CreateProgressState(void);
static void ProgressIndent(ProgressState* ps);
static void ProgressUnindent(ProgressState* ps);

static void ProgressMasterAndSlaves(int pid, int verbose,
	Tuplestorestate* tupstore, TupleDesc tupdesc);
static void ProgressPid(int pid, int ppid, int verbose, Tuplestorestate* tupstore,
	TupleDesc tupdesc, int* child_pid_array, int* child_indent);
static void ProgressSpecialPid(int pid, int bid, Tuplestorestate* tupstore, TupleDesc tupdesc, char* buf);
static char* ProgressFetchReport(int pid, int bid, int verbose, int* child_pid_array, int* child_indent);

static bool ReportHasChildren(Plan* plan, PlanState* planstate);

/*
 * Individual nodes of interest are:
 * - scan data: for heap or index
 * - sort data: for any relation or tuplestore
 * Other nodes only wait on above nodes
 */
static void ProgressGather(GatherState* gs, ProgressState* ps);
static void ProgressGatherMerge(GatherMergeState* gs, ProgressState* ps);
static void ProgressParallelExecInfo(ParallelContext* pc, ProgressState* ps);

static void ProgressScanBlks(ScanState* ss, ProgressState* ps);
static void __ProgressScanBlks(HeapScanDesc hsd, ProgressState* ps);
static void ProgressScanRows(Scan* plan, PlanState* plantstate, ProgressState* ps);
static void ProgressTidScan(TidScanState* ts, ProgressState* ps);
static void ProgressCustomScan(CustomScanState* cs, ProgressState* ps);
static void ProgressIndexScan(IndexScanState* is, ProgressState* ps); 

static void ProgressLimit(LimitState* ls, ProgressState* ps);
static void ProgressModifyTable(ModifyTableState * planstate, ProgressState* ps);
static void ProgressHashJoin(HashJoinState* planstate, ProgressState* ps);
static void ProgressHash(HashState* planstate, ProgressState* ps);
static void ProgressHashJoinTable(HashJoinTable hashtable, ProgressState* ps);
static void ProgressBufFileRW(BufFile* bf, ProgressState* ps, unsigned long *reads,
	unsigned long * writes, unsigned long *disk_size);
static void ProgressBufFile(BufFile* bf, ProgressState* ps);
static void ProgressMaterial(MaterialState* planstate, ProgressState* ps);
static void ProgressTupleStore(Tuplestorestate* tss, ProgressState* ps);
static void ProgressAgg(AggState* planstate, ProgressState* ps);
static void ProgressSort(SortState* ss, ProgressState* ps);
static void ProgressTupleSort(Tuplesortstate* tss, ProgressState* ps); 
static void dumpTapes(struct ts_report* tsr, ProgressState* ps);

static void ReportTime(QueryDesc* query, ProgressState* ps);
static void ReportStack(ProgressState* ps);
static void ReportDisk(ProgressState* ps);

static void ProgressDumpRequest(int pid);
static void ProgressResetRequest(ProgressCtl* req);

static void ProgressPropLong(ProgressState* ps, const char* type,
	const char* name, unsigned long value, const char* unit);
static void ProgressPropText(ProgressState* ps, const char* type,
	const char* name, const char* value);
static void ProgressPropTextStr(StringInfo str, int pid, int bid,
	int lineid, int indent, const char* type, const char* name, const char* value);
static void ProgressPropList(ProgressState* ps, const char* type,
	const char *name, List *data);

/*
 * Function to expose quals, sorts, sub plans, members
 */
static char* setopCmd(SetOp* setop);
static char* joinType(Join* join);

static void show_plan_tlist(PlanState *planstate, List *ancestors, ProgressState *es);

typedef void (*functionNode)(PlanState* planstate, List* ancestors,
	const char* relationship, const char* plan_name, ProgressState* ps);

static void ReportMemberNodes(List *plans, PlanState **planstates, List *ancestors, ProgressState *es, functionNode fn);
static void ReportSubPlans(List *plans, List *ancestors, const char *relationship, ProgressState *es, functionNode fn);
static void ReportCustomChildren(CustomScanState *css, List *ancestors, ProgressState *es, functionNode fn);


Size ProgressShmemSize(void)
{
	Size size;

	/* Must match ProgressShmemInit */
	size = mul_size(MaxBackends, sizeof(ProgressCtl));
	size = add_size(size, mul_size(MaxBackends, sizeof(struct Latch)));
	size = add_size(size, mul_size(MaxBackends, BLCKSZ));

	return size;
}

/*
 * Initialize our shared memory area
 */
void ProgressShmemInit(void)
{
	bool found;
	size_t size = 0;
	char* shm_page;

	/* 
	 * Allocate share memory buffer, one page per backend
	 */
	size = mul_size(MaxBackends, BLCKSZ);
	progress_shm_buf = ShmemInitStruct("Progress Shm Buffer", size, &found);
	if (!found) {
		memset(progress_shm_buf, 0, (MaxBackends * BLCKSZ));
	}

	/*
	 * Allocate progress request meta data, one for each backend
	 */
	size = mul_size(MaxBackends, sizeof(ProgressCtl));
	progress_ctl_array = ShmemInitStruct("ProgressCtl array", size, &found);
	LWLockRegisterTranche(LWTRANCHE_PROGRESS, "progress_request");
	if (!found) {
		int i;
		ProgressCtl* req;

		req = progress_ctl_array;
		shm_page = progress_shm_buf;

		for (i = 0; i < MaxBackends; i++) {
			/* Already zeroed above */
			memset(req, 0, sizeof(ProgressCtl));
	
			InitSharedLatch(&(req->latch));

			req->parallel = false;
			req->child = false;
			req->child_indent = 0;

			req->disk_size = 0;
			req->verbose = 0;

			req->shm_page = shm_page;
			req->shm_len = 0;
			shm_page += BLCKSZ;

			req->report_size = 0;

			req->command = PRG_CTL_CMD_UNDEFINED;
			req->status = PRG_CTL_STATUS_UNDEFINED;

			LWLockInitialize(&req->lock, LWTRANCHE_PROGRESS);
			req->proc_cnt = 0;	

			memset(req->child_pid, 0, sizeof(int) * MAX_WORKERS);

			req++;
		}
	}

	return;
}

#define DEBUG_REQUEST	0

/*
 * Dump request management
 */
static
void ProgressDumpRequest(int pid)
{
	int bid;
	ProgressCtl* req;

	bid = ProcPidGetBackendId(pid);
	req = progress_ctl_array + bid;
	if (DEBUG_REQUEST) {
		elog(LOG, "backend pid=%d bid=%d"
			" cmd=%s status=%s"
			" verbose=%d"
			" parallel=%d child= %d indent=%d"
			" report_size=%d shm_len=%d",
			pid, bid,
			progress_command[req->command],
			progress_status[req->status],
			req->verbose,
			req->parallel, req->child, req->child_indent,
			req->report_size, req->shm_len);
	}
}

static
void ProgressResetRequest(ProgressCtl* req)
{
	if (DEBUG_REQUEST) {
		elog(LOG, "reset progress request at addr %p", req);
	}

	req->command = PRG_CTL_CMD_UNDEFINED;
	req->status = PRG_CTL_STATUS_UNDEFINED;

	req->parallel = false;
	req->child = false;
	req->child_indent = 0;
	req->disk_size = 0;
	req->verbose = 0;

	InitSharedLatch(&(req->latch));

	memset(req->shm_page, 0, BLCKSZ);
	req->shm_len = 0;

	memset(req->child_pid, 0, sizeof(int) * MAX_WORKERS);
}



#define DEBUG_ROW_FORMAT	0
#define debug_row_format(format, ...)	\
	if (DEBUG_ROW_FORMAT)		\
		elog(LOG, "RowFormat => " format, ##__VA_ARGS__) 

/*
 * Report of rows in pg_progress tables
 */
static
void ProgressPropLong(ProgressState* ps,
	const char* type, const char* name, unsigned long value, const char* unit)
{
	/*
	 * Fields are: pid, lineid, indent, name, value, unit
	 */
	char pid_str[PG_PROGRESS_PID];
	char bid_str[PG_PROGRESS_BID];
	char lineid_str[PG_PROGRESS_LINEID];
	char indent_str[PG_PROGRESS_INDENT];
	char value_str[PG_PROGRESS_VALUE];

	sprintf(pid_str, "%d", ps->pid);
	sprintf(bid_str, "%d", ps->bid);
	sprintf(lineid_str, "%d", ps->lineid);
	sprintf(indent_str, "%d", ps->indent);
	sprintf(value_str, "%lu", value);

	debug_row_format("ProgressPropLong PID_STR = %s", pid_str);	

	appendStringInfo(ps->str, "%s|%s|%s|%s|%s|%s|%s|%s|",
		pid_str, bid_str, lineid_str, indent_str, type, name, value_str, unit);

	ps->lineid++;
}

static
void ProgressPropText(ProgressState* ps,
	const char* type, const char* name, const char* value)
{
	/*
	 * Fields are: pid, lineid, indent, name, value, unit
	 */
	char pid_str[PG_PROGRESS_PID];
	char bid_str[PG_PROGRESS_BID];
	char lineid_str[PG_PROGRESS_LINEID];
	char indent_str[PG_PROGRESS_INDENT];

	sprintf(pid_str, "%d", ps->pid);
	sprintf(bid_str, "%d", ps->bid);
	sprintf(lineid_str, "%d", ps->lineid);
	sprintf(indent_str, "%d", ps->indent);

	debug_row_format("ProgressPropText PID_STR = %s", pid_str);	

	appendStringInfo(ps->str, "%s|%s|%s|%s|%s|%s|%s||",
		pid_str, bid_str, lineid_str, indent_str, type, name, value);

	ps->lineid++;
}

static
void ProgressPropTextStr(StringInfo str, int pid, int bid, int lineid,
	int indent, const char* type, const char* name, const char* value)
{
	debug_row_format("ProgressPropTextStr PID_STR = %d", pid);

	appendStringInfo(str, "%d|%d|%d|%d|%s|%s|%s||",
		pid, bid, lineid, indent, type, name, value);
}

static
void ProgressPropList(ProgressState* ps, const char* type, const char *name, List *data)
{
	ListCell* lc;

	foreach(lc, data) {
		ProgressPropText(ps, type, name, (const char *) lfirst(lc));
	}
}

static
void ProgressResetReport(ProgressState* ps)
{
	resetStringInfo(ps->str);
}

static
void ProgressSetMonitoringSelf(void)
{
	ProgressCtl* req;
	
	req = progress_ctl_array + ProcPidGetBackendId(getpid());
	req->monitoring = true;
}

static
void ProgressUnsetMonitoringSelf(void)
{
	ProgressCtl* req;
	
	req = progress_ctl_array + ProcPidGetBackendId(getpid());
	req->monitoring = false;
}

static
bool ProgressTestMonitoringSelf(int pid) 
{
	ProgressCtl* req;

	req = progress_ctl_array + ProcPidGetBackendId(pid);
	return req->monitoring;
}


#define DEBUG_PROGRESS	0
#define debug_progress(format, ...)	\
	if (DEBUG_PROGRESS)		\
		ereport(LOG, (errmsg("Progress => " format, ##__VA_ARGS__), errhidestmt(true)));


/*
 * Colums are: pid, lineid, indent, property, value, unit
 */
Datum pg_progress(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	Tuplestorestate* tupstore;
	ReturnSetInfo* rsinfo;

	int pid;
	unsigned short verbose;

	int num_backends;
	int curr_backend;

	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	debug_progress("start");

	/*
	 * Check we are allowed to monitor other backends
	 */
	if (!superuser()) {
		ereport(ERROR, (
			errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			errmsg("permission denied to monitor backends"),
			errhint("Must be superuser to monitor backends.")));
	}

	/*
	 * Set ourself as monitoring backend to avoid being request a progress dump
	 * from other monitoring backends
	 */
	ProgressSetMonitoringSelf();
	
	/*
	 * pid = 0 means collect progress report for all backends
	 */
	pid = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT32(0);
	verbose = PG_ARGISNULL(0) ? false : PG_GETARG_UINT16(1);
	debug_progress("pid = %d, verbose = %d", pid, verbose);
	
	/*
	 * Build a tuple descriptor for our result type
	 */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		debug_progress("return type must be a row type");
	}

	/*
	 * Switch to query memory context
	 */
	rsinfo = (ReturnSetInfo*) fcinfo->resultinfo;
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	if (pid > 0) {
		/* Target specific pid given as SQL function argupment */
		ProgressMasterAndSlaves(pid, verbose, tupstore, tupdesc);
	} else {
		/* Loop over all backends */
		num_backends = pgstat_fetch_stat_numbackends();
		debug_progress("num backends = %d", num_backends);

		for (curr_backend = 1; curr_backend <= num_backends; curr_backend++) {
			LocalPgBackendStatus* local_beentry;
			PgBackendStatus* beentry;

			BackendType backend_type;

			long secs;
			int usecs;

			debug_progress("loop on backend %d", curr_backend);
			local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
			if (!local_beentry) {
				char lbuf[] = "<backend information not available>";

				ProgressSpecialPid(0, curr_backend, tupstore, tupdesc, lbuf);
				continue;
			}

			beentry = &local_beentry->backendStatus;
			pid = beentry->st_procpid;
			backend_type = beentry->st_backendType;

			/*
			 * Do not monitor oneself
			 */
			if (pid == getpid()) {
				debug_progress("backend is self");
				if (verbose >= VERBOSE_BACKEND_SELF)
					ProgressSpecialPid(pid, curr_backend, tupstore,
						tupdesc, "<self backend>");

				continue;
			}

			/*
			 * Is this a regular backend?
			 */
			if (backend_type != B_BACKEND) {
				debug_progress("backend is not standard");
				if (verbose >= VERBOSE_BACKEND_OTHER)
					ProgressSpecialPid(pid, curr_backend, tupstore,
						tupdesc, "<not a standard backend>");

				continue;
			}

			/*
			 * Is this a monitoring backend?
			 */
			if (ProgressTestMonitoringSelf(pid)) {
				debug_progress("backend is monitor");
				if (verbose >= VERBOSE_BACKEND_MONITOR)
					ProgressSpecialPid(pid, curr_backend, tupstore,
						tupdesc, "<monitoring backend>");

				continue;
			}

			/*
			 * Has the query run for at least progress_time_threshold seconds?
			 */
			TimestampDifference(beentry->st_activity_start_timestamp, GetCurrentTimestamp(), &secs, &usecs);
			debug_progress("delta secs %ld", secs);

			if (secs < progress_time_threshold) {
				char lbuf[PG_PROGRESS_VALUE];

				debug_progress("too short life time");

				snprintf(lbuf, PG_PROGRESS_VALUE, "<backend has run for less than %d seconds>", progress_time_threshold);
				ProgressSpecialPid(pid, curr_backend, tupstore, tupdesc, lbuf);
				continue;
			}

			/*
			 * Do the report collect for a master and its slaves if any
			 */
			ProgressMasterAndSlaves(pid, verbose, tupstore, tupdesc);
		}
	}

	tuplestore_donestoring(tupstore);

	MemoryContextSwitchTo(oldcontext);
	ProgressUnsetMonitoringSelf();

	return (Datum) 0;
}

static
void ProgressMasterAndSlaves(int pid, int verbose,
	Tuplestorestate* tupstore, TupleDesc tupdesc)
{
	int pid_index;	
	int child_pid;	
	int child_indent;
	int child_pid_array[MAX_WORKERS];

	/* 
	 * Collect progress report from master backend
	 */
	child_indent = 0;
	ProgressPid(pid, 0, verbose, tupstore, tupdesc, child_pid_array, &child_indent);

	/*
	 * Check for child pid of current pid.
	 */
	pid_index = 0;
	while (child_pid_array[pid_index] != 0) {
		debug_progress("collect child data");

		child_pid = child_pid_array[pid_index];
		ProgressPid(child_pid, pid, verbose, tupstore, tupdesc, NULL, &child_indent);
		pid_index++;
	}

	child_indent = 0;
	memset(child_pid_array, 0, SIZE_OF_PID_ARRAY);
}

#define DEBUG_PARSER	0
#define debug_parser(format, ...)	\
	if (DEBUG_PARSER)		\
		ereport(LOG, (errmsg("Parser => " format, ##__VA_ARGS__), errhidestmt(true)));

static
void ProgressPid(int pid, int ppid, int verbose,
	Tuplestorestate* tupstore, TupleDesc tupdesc,
	int* child_pid_array, int* child_indent)
{
	int bid;

	Datum values[PG_PROGRESS_COLS];
	bool nulls[PG_PROGRESS_COLS];

	char pid_str[PG_PROGRESS_PID];
	int pid_val;

	char bid_str[PG_PROGRESS_BID];
	int bid_val;

	char lineid_str[PG_PROGRESS_LINEID];
	int lineid_val;

	char indent_str[PG_PROGRESS_INDENT];
	int indent_val;

	char type_str[PG_PROGRESS_TYPE];
	char name_str[PG_PROGRESS_NAME];
	char value_str[PG_PROGRESS_VALUE];
	char unit_str[PG_PROGRESS_UNIT];

	char* buf;
	unsigned int tuple_index = 0;

	char* token_start;
	char* token_next;
	unsigned short token_length;
	unsigned short total_length;
	int i;
	
	debug_parser("start");

	/* Convert pid to backend_id */
	bid = ProcPidGetBackendId(pid);
	if (bid == InvalidBackendId) {
		ereport(ERROR, (
       		errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
		errmsg("Invalid backend process pid")));
	}

	if (pid == getpid()) {
		ereport(ERROR, (
		errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
		errmsg("Cannot request status from self")));
	}

	debug_parser("pid = %d, bid = %d", pid, bid);

	/*
	 * buf is allocated in ProgressFetchReport() and freed below
	 * All is done in the same context
	 */
	buf = ProgressFetchReport(pid, bid, verbose, child_pid_array, child_indent);

	total_length = strlen(buf);
	token_start = buf;
	token_next = strchr(token_start, '|');
	if (token_next == NULL) {
		elog(LOG, "No output");
		return;
	}

	debug_parser("total_length = %d, token_start = %p, token_next = %p",
		total_length, token_start, token_next);
	
	i = 0;

	while (token_start != NULL) {
		token_length = (unsigned short)(token_next - token_start);
		debug_parser("token_length=%d, tuple_index=%d, field=%d",
			token_length, tuple_index, i);

		switch(i) {
                case 0:
                        snprintf(pid_str, token_length + 1, "%s", token_start);
                        pid_str[token_length + 1] = '\0';
                        break;
                case 1:
                        snprintf(bid_str, token_length + 1, "%s", token_start);
                        bid_str[token_length + 1] = '\0';
                        break;
                case 2:
                        snprintf(lineid_str, token_length + 1, "%s", token_start);
                        lineid_str[token_length + 1] = '\0';
                        break;
                case 3:
                        snprintf(indent_str, token_length + 1, "%s", token_start);
                        indent_str[token_length + 1] = '\0';
                        break;
                case 4:
                        snprintf(type_str, token_length + 1, "%s", token_start);
                        type_str[token_length + 1] = '\0';
                        break;
                case 5:
                        snprintf(name_str, token_length + 1, "%s", token_start);
                        name_str[token_length + 1] = '\0';
                        break;
                case 6:
                        snprintf(value_str, token_length + 1, "%s", token_start);
                        value_str[token_length + 1] = '\0';
                        break;
                case 7:
                        snprintf(unit_str, token_length + 1, "%s", token_start);
                        unit_str[token_length + 1] = '\0';
                        break;
                };

                i++;

                if (i == 8) {
			/* PK */
			pid_val = atoi(pid_str);	
			values[0] = Int32GetDatum(pid_val);
			nulls[0] = false;

			values[1] = Int32GetDatum(ppid);
			nulls[1] = false;

			/* PK */
			bid_val = atoi(bid_str);	
			values[2] = Int32GetDatum(bid_val);
			nulls[2] = false;

			/* PK */
			lineid_val = atoi(lineid_str);
			values[3] = Int32GetDatum(lineid_val);
			nulls[3] = false;

			/* PK */
			indent_val = atoi(indent_str);
			values[4] = Int32GetDatum(indent_val);
			nulls[4] = false;

			/* PK */
			values[5] = CStringGetTextDatum(type_str);
			nulls[5] = false;

			/* PK */
			values[6] = CStringGetTextDatum(name_str);
			nulls[6] = false;

			if (strlen(value_str) == 0) {
				nulls[7] = true;
			} else {
				values[7] = CStringGetTextDatum(value_str);
				nulls[7] = false;
			}

			if (strlen(unit_str) == 0) {
				nulls[8] = true;
			} else {
				values[8] = CStringGetTextDatum(unit_str);
				nulls[8] = false;
			}

			debug_parser("Parser => storing tuple");
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			tuple_index++;

                        i = 0;
                }

		token_start = token_next + 1;
		if (token_start >= buf + total_length)
			break;

                token_next = strchr(token_start, '|');
		debug_parser("token_start = %p token_next = %p tuple_index = %u token_content = %s",
			token_start, token_next, tuple_index, token_start);
	}

	pfree(progress_buf);
	progress_buf = NULL;
	progress_buf_len = 0;

	debug_parser("Parser end");
}

static
void ProgressSpecialPid(int pid, int bid, Tuplestorestate* tupstore, TupleDesc tupdesc, char* buf)
{
	Datum values[PG_PROGRESS_COLS];
	bool nulls[PG_PROGRESS_COLS];

	values[0] = Int32GetDatum(pid);
	nulls[0] = false;
				
	values[1] = Int32GetDatum(0);
	nulls[1] = false;

	values[2] = Int32GetDatum(bid);
	nulls[2] = false;

	values[3] = Int32GetDatum(0);
	nulls[3] = false;

	values[4] = Int32GetDatum(0);
	nulls[4] = false;

	values[5] = CStringGetTextDatum(PROP);
	nulls[5] = false;

	values[6] = CStringGetTextDatum(buf);
	nulls[6] = false;

	nulls[7] = true;
	nulls[8] = true;

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

#define WAIT_FLAGS	(WL_LATCH_SET | WL_TIMEOUT)
#define WAIT_TIMEOUT	(progress_timeout * 1000L)

#define PREPARE_LATCH(req)		\
	OwnLatch(&((req)->latch));		\
        ResetLatch(&((req)->latch))

#define WAIT_LATCH(req)			\
	WaitLatch(&((req)->latch), WAIT_FLAGS, WAIT_TIMEOUT, WAIT_EVENT_PROGRESS); \
	DisownLatch(&((req)->latch))

#define DEBUG_FETCHER		0
#define debug_fetcher(format, ...)	\
	if (DEBUG_FETCHER)		\
		ereport(LOG, (errmsg("Fetcher => " format, ##__VA_ARGS__), errhidestmt(true)));


static void ProgressLockRequest(ProgressCtl* req)
{
	LWLockAcquire(&(req->lock), LW_EXCLUSIVE);
	req->pid = getpid();
	req->proc_cnt++;
	Assert(req->proc_cnt == 1);
}

static void ProgressUnlockRequest(ProgressCtl* req)
{
	Assert(req->proc_cnt == 1);
	req->proc_cnt--;
	LWLockRelease(&(req->lock));
	req->pid = 0;
}


/*
 * ProgressFetchReport:
 * 	Log a request to a backend in order to fetch its progress log
 *
 * 	If child_indent != 0, this is a child process
 */
static char* ProgressFetchReport(int pid, int bid, int verbose,
	int* child_pid_array, int* child_indent)
{
	ProgressCtl* req;
	char* buf = NULL;
	unsigned int str_len = 0;
	StringInfo str;

	debug_fetcher("start of ProgressFetchReport with pid = %d, bid = %d", pid, bid);

	/*
	 * Serialize signals/request to get the progress state of the query
	 */
	req = progress_ctl_array + bid;
	ProgressLockRequest(req);	

	ProgressResetRequest(req);
	req->command = PRG_CTL_CMD_COLLECT;
	req->status = PRG_CTL_STATUS_UNDEFINED;
	req->verbose = verbose;
	if (*child_indent != 0) {
		req->parallel = true;
		req->child = true;
		req->child_indent = *child_indent;
	}

	Assert(progress_buf == NULL);

	do {
		ProgressDumpRequest(pid);
		
		PREPARE_LATCH(req);
		SendProcSignal(pid, PROCSIG_PROGRESS, bid);
		debug_fetcher("waiting on latch");

		WAIT_LATCH(req);
		debug_fetcher("finish latch wait req->status=%d req->shm_len=%d req->report_size=%d",
			req->status, req->shm_len, req->report_size);

		/*
		 * Allocate the buffer needed to collect the complete report which 
		 * may be sent in BLCKSZ size chunks. Buffer is at least BLCKSZ
		 */
		if (progress_buf == NULL) {
			progress_buf_len = req->report_size;
			if (progress_buf_len < BLCKSZ)
				progress_buf_len = BLCKSZ;

			progress_buf = palloc0(progress_buf_len);
			buf = progress_buf;
		}

		switch(req->status) {
		case PRG_CTL_STATUS_UNDEFINED: 
                	/* We have timed out on progress_timeout */
			debug_fetcher("status=undefined");
                	str = makeStringInfo();
                	ProgressPropTextStr(str, pid, bid, 0, 0, PROP, "status", msg_timeout);
                	str_len = strlen(str->data) + 1;
			Assert(str_len < BLCKSZ);
                	memcpy(buf, str->data, str_len);
			break;
	
		case PRG_CTL_STATUS_OK:
			/* We have a result computed by the monitored backend */
			debug_fetcher("status=ok");
			memcpy(buf, req->shm_page, req->shm_len);
			break;

		case PRG_CTL_STATUS_MORE:
			/* We have more data */
			debug_fetcher("status=more");
			memcpy(buf, req->shm_page, req->shm_len);
			req->command = PRG_CTL_CMD_CONT;
			buf += req->shm_len;
			break;

		case PRG_CTL_STATUS_FAILED:
			debug_fetcher("status=failed");
			str = makeStringInfo();
			ProgressPropTextStr(str, pid, bid, 0, 0, PROP, "status", "failed");
			str_len = strlen(str->data) + 1;
			Assert(str_len < BLCKSZ);
			memcpy(buf, str->data, str_len);
			break;
		};
	} while (req->status == PRG_CTL_STATUS_MORE);

	debug_fetcher("buf: %s", progress_buf);

	/*
	 * Copy list of child worker backend pid to allox for progress request of these
	 * backends
	 *
	 * *child_indent == 0 => Master
	 * *child_indent != 0 => Child
	 */
	if (*child_indent == 0) {
		memcpy(child_pid_array, req->child_pid, SIZE_OF_PID_ARRAY);
		if (req->child_indent) 
			*child_indent = req->child_indent;
	}

	/*
	 * End serialization
	 */
	ProgressUnlockRequest(req);	

	return progress_buf;
}

static
ProgressState* CreateProgressState(void)
{
	StringInfo str;
	ProgressState* prg;

	str = makeStringInfo();
 
	prg = (ProgressState*) palloc0(sizeof(ProgressState));
	prg->parallel = false;
	prg->child = false;
	prg->str = str;
	prg->indent = 0;
	prg->rtable = NULL;
	prg->plan = NULL;
	prg->pid = 0;
	prg->lineid = 0;

	return prg;
}

static
void ProgressIndent(ProgressState* ps)
{
	ps->indent++;
}

static
void ProgressUnindent(ProgressState* ps)
{
	ps->indent--;
}

/*
 * Request handling
 */
void HandleProgressSignal(void)
{
	progress_requested = true;
	InterruptPending = true;
}

#define DEBUG_HANDLER		0
#define debug_handler(format, ...)	\
	if (DEBUG_HANDLER)		\
		ereport(LOG, (errmsg("Handler => " format, ##__VA_ARGS__), errhidestmt(true)));


void HandleProgressRequest(void)
{
	ProgressCtl* req;
	ProgressState* ps;		/* local pointer to ease codying style */
	MemoryContext oldcontext;

	unsigned int dump_rest;

	unsigned short running = 0;
	bool child = false;

	req = progress_ctl_array + MyBackendId;
	debug_handler("request received with command: %s", progress_command[req->command]);

	/*
	 * We hold interrupt here because the current SQL query could be cancelled at any time. In which 
	 * case, the current backend would not call SetLatch(). Monitoring backend would wait endlessly.
	 *
	 * To avoid such situation, a further safety measure has been added: the monitoring backend waits
	 * the response for a maximum of progress_timeout time. After this timeout has expired, the monitoring
	 * backend sends back the respponse which is then empty.
	 */
	HOLD_INTERRUPTS();
	progress_requested = false;

	/*
	 * If the signal was sent to request clearing of DSM resource, do so and exit 
	 * on SetLatch()
	 */
	if (DEBUG_HANDLER)
		ProgressDumpRequest(getpid());

	/*
	 * Do not recollect progress if we have already done it and not yet completed
	 * the dump report in shm
	 */
	if (req->command == PRG_CTL_CMD_CONT)
		goto cont;

	Assert(req->command == PRG_CTL_CMD_COLLECT);

	/*
	 * Create a memory context for the report
	 */
	debug_handler("starting collect report");
	if (progress_context == NULL) {
		debug_handler("allocating MemoryContext");
		progress_context =  AllocSetContextCreate(TopMemoryContext,
			"ProgressState", ALLOCSET_DEFAULT_SIZES);
	}

	oldcontext = MemoryContextSwitchTo(progress_context);

	if (progress_state == NULL) {
		debug_handler("allocating progress_state");
		progress_state = CreateProgressState();
		progress_state->memcontext = progress_context;
	} else {
		ProgressResetReport(progress_state);
	}

	/*
	 * Prepare report
	 */
	ps = progress_state;

	Assert(ps != NULL);
	Assert(ps->str != NULL);
	Assert(ps->str->data != NULL);

	memset(req->child_pid, 0, sizeof(int) * MAX_WORKERS);

	ps->verbose = req->verbose;
	ps->parallel = req->parallel;
	ps->child = req->child;
	ps->indent = req->child_indent;
	ps->disk_size = 0;
	ps->pid = getpid();
	ps->bid = MyBackendId;

	/* Local params */
	child = req->child;

	/*
	 * Only clear previous content of ps->str
	 */
	if (MyQueryDesc == NULL) {
		if (MyUtilityPlannedStmt != NULL) {
			ProgressUtility(MyUtilityPlannedStmt, ps);
		} else {
			ProgressPropText(ps, PROP, "status", msg_idle);
		}
	} else if (!IsTransactionState()) {
		ProgressPropText(ps, PROP, "status", msg_out_of_xact);
	} else if (MyQueryDesc->plannedstmt == NULL) {
		ProgressPropText(ps, PROP, "status", msg_null_plan);
	} else if (MyQueryDesc->plannedstmt->commandType == CMD_UTILITY) {
		ProgressPropText(ps, PROP, "status", msg_utility_stmt);
	} else if (MyQueryDesc->already_executed == false) {
		ProgressPropText(ps, PROP, "status", msg_qry_not_started);
	} else if (QueryCancelPending) {
		ProgressPropText(ps, PROP, "status", msg_qry_cancel);
	} else if (RecoveryConflictPending) {
		ProgressPropText(ps, PROP, "status", msg_reco_pending);
	} else if (ProcDiePending) {
		ProgressPropText(ps, PROP, "status", msg_proc_die);
	} else {
		running = 1;
		if (!child)
			ProgressPropText(ps, PROP, "status", "<query running>");
	}

	/*
	 * Log SQL statement if requested by verbose level
	 */
	if (ps->verbose >= VERBOSE_STMT
		&& !child
		&& running) {
		if (MyQueryDesc != NULL && MyQueryDesc->sourceText != NULL)
			ProgressPropText(ps, PROP, "query", MyQueryDesc->sourceText);
	}

	if (running) {
		if (!child)
			ReportTime(MyQueryDesc, ps);

		if (ps->verbose >= VERBOSE_STACK) {
			ReportStack(ps);
		}

		ProgressPlan(MyQueryDesc, ps);

		/*
		 * Disk use reporting must come after ProgressPlan() 
		 * which collects actual disk use.
		 */
		if (ps->verbose >=  VERBOSE_DISK_USE
			&& !child) {
			ReportDisk(ps);
		}
	}

	/* 
	 * Dump disk size used for stores, sorts, and hashes
	 */
	req->disk_size = ps->disk_size;

	/* 
	 * Start of dump in shm page, one page at a time
	 */
	progress_report_size = strlen(ps->str->data) + 1;
	req->report_size = progress_report_size;
	progress_cursor = ps->str->data;
	
	MemoryContextSwitchTo(oldcontext);
	debug_handler("end of report: %s", ps->str->data);

cont:
	debug_handler("push report in shm");

	Assert(progress_context != NULL);
	dump_rest = progress_report_size - (unsigned int)(progress_cursor - progress_state->str->data);
	debug_handler("dump_rest = %d", dump_rest);

	if (dump_rest > BLCKSZ) {
		debug_handler("Report is larger than 1 shm page");
		memcpy(req->shm_page, progress_cursor, BLCKSZ);
		progress_cursor += BLCKSZ;

		req->shm_len = BLCKSZ;
		req->status = PRG_CTL_STATUS_MORE;
	} else {
		debug_handler("Report is smaller or equal than 1 shm page");
		memcpy(req->shm_page, progress_cursor, dump_rest);
		progress_cursor += dump_rest;

		req->shm_len = dump_rest;
		req->status = PRG_CTL_STATUS_OK;
	}

	if (req->status == PRG_CTL_STATUS_OK) {
		debug_handler("cleaning memory context");
		MemoryContextDelete(progress_state->memcontext);
		progress_context = NULL;
		progress_state = NULL;
	}
	
	debug_handler("setting latch");

	/*
	 * Notify of progress state delivery
	 */
	SetLatch(&(req->latch));
	RESUME_INTERRUPTS();
}

static
void ProgressUtility(
	PlannedStmt* pstmt,
	ProgressState* ps)
{
	Node* parsetree;

	if (pstmt == NULL) {
               return;
        }

	parsetree = pstmt->utilityStmt;
	if (IsA(parsetree, IndexStmt)) {
		IndexStmt* stmt;

		stmt = (IndexStmt*) parsetree;
		ProgressIndexStmt(stmt, ps);
	}
}

static
void ProgressIndexStmt(IndexStmt* stmt, ProgressState* ps)
{
	HeapScanDesc scan;

	scan = CreateIndexHeapScan;
	if (scan == NULL) {
		ProgressPropText(ps, PROP, "status", msg_qry_not_started);
	} else {
		Assert(MyUtilityQueryString != NULL);

		ProgressPropText(ps, PROP, "query", MyUtilityQueryString);	
		ProgressIndent(ps);
		__ProgressScanBlks(scan, ps);
		ProgressUnindent(ps);
	}
}

static
void ProgressPlan(
	QueryDesc* query,
	ProgressState* ps)
{
	Bitmapset* rels_used = NULL;
	PlanState* planstate;

	/*
	 * Set up ProgressState fields associated with this plan tree
	 */
	Assert(query->plannedstmt != NULL);

	/* Top level tree data */
	if (query->plannedstmt != NULL)
		ps->pstmt = query->plannedstmt;

	if (query->plannedstmt->planTree != NULL)
		ps->plan = query->plannedstmt->planTree;

	if (query->planstate != NULL)
		ps->planstate = query->planstate;

	if (query->estate != NULL)
		ps->es = query->estate;

	if (query->plannedstmt->rtable != NULL)
		ps->rtable = query->plannedstmt->rtable;

	ExplainPreScanNode(query->planstate, &rels_used);

	ps->rtable_names = select_rtable_names_for_explain(ps->rtable, rels_used);
	ps->deparse_cxt = deparse_context_for_plan_rtable(ps->rtable, ps->rtable_names);
	ps->printed_subplans = NULL;

	planstate = query->planstate;
	if (IsA(planstate, GatherState) && ((Gather*) planstate->plan)->invisible) {
		planstate = outerPlanState(planstate);
	}

	if (ps->parallel && ps->child)
		ProgressNode(planstate, NIL, "child worker", NULL, ps);
	else
		ProgressNode(planstate, NIL, NULL, NULL, ps);
}

	
#define DEBUG_COLLECTOR		0
#define debug_collector(format, ...)	\
	if (DEBUG_COLLECTOR)		\
		ereport(LOG, (errmsg("Collector => " format, ##__VA_ARGS__), errhidestmt(true)));


/*
 * This is the main workhorse for collecting query execution progress.
 *
 * planstate is the current execution state in the global execution tree
 * relationship: describes the relationship of this plan state to its parent
 * 	"outer", "inner". It is null at tol level.
 */
static
void ProgressNode(
	PlanState* planstate,
	List* ancestors,
	const char* relationship,
	const char* plan_name,
	ProgressState* ps)
{
	Plan* plan = planstate->plan;
	PlanInfo info;
	bool haschildren;
	int ret;

	debug_collector("node %s", nodeToString(plan));

	/*
	 * 1st step: display the node type
	 */
	ret = planNodeInfo(plan, &info);
	if (ret != 0) {
		elog(LOG, "unknown node type for plan");
	}

	ProgressPropText(ps, RELATIONSHIP, "relationship",
		relationship != NULL ? relationship : "progression");
	ProgressIndent(ps);

	/*
	 * Report node top properties
	 */
	if (info.pname)
		ProgressPropText(ps, NODE, "node name", info.pname);

	if (plan_name)
		ProgressPropText(ps, NODE, "plan name", plan_name);

	if (plan->parallel_aware)
		ProgressPropText(ps, PROP, "node mode", "parallel");
          
	/*
	 * Second step
	 */
	switch(nodeTag(plan)) {
	case T_SeqScan:		// ScanState
	case T_SampleScan:	// ScanState
	case T_BitmapHeapScan:	// ScanState
	case T_SubqueryScan:	// ScanState
	case T_FunctionScan:	// ScanState
	case T_ValuesScan:	// ScanState
	case T_CteScan:		// ScanState
	case T_WorkTableScan:	// ScanState
                ProgressScanRows((Scan*) plan, planstate, ps);
		ProgressScanBlks((ScanState*) planstate, ps);
                break;

	case T_TidScan:		// ScanState
		ProgressTidScan((TidScanState*) planstate, ps);
		ProgressScanBlks((ScanState*) planstate, ps);
		break;

	case T_Limit:		// PlanState
		ProgressLimit((LimitState*) planstate, ps);
		break;

	case T_ForeignScan:	// ScanState
	case T_CustomScan:	// ScanState
		ProgressCustomScan((CustomScanState*) planstate, ps);
                ProgressScanRows((Scan*) plan, planstate, ps);
		break;

	case T_IndexScan:	// ScanState
	case T_IndexOnlyScan:	// ScanState
	case T_BitmapIndexScan:	// ScanState
		ProgressScanBlks((ScanState*) planstate, ps);
		ProgressIndexScan((IndexScanState*) planstate, ps); 
		break;

	case T_ModifyTable:	// PlanState
		/*
		 * Dealt below with mt_plans array of PlanState nodes
		 */
		ProgressModifyTable((ModifyTableState *) planstate, ps);	
		break;

	case T_NestLoop:	// JoinState (includes a Planstate)
	case T_MergeJoin:	// JoinState (includes a Planstate)
		/*
		 * Does not perform long ops. Only Join
		 */
		break;

	case T_HashJoin: {	// JoinState (includes a Planstate)
		/* 
		 * uses a HashJoin with BufFile
		 */
		const char* jointype;

		jointype = joinType((Join*) plan);
		ProgressPropText(ps, PROP, "join type", jointype);

		}

		ProgressHashJoin((HashJoinState*) planstate, ps);
		break;

	case T_SetOp: {		// PlanState
		/*
		 *  Only uses a in memory hash table
		 */
		const char* setopcmd;

		setopcmd = setopCmd((SetOp*) plan);
		ProgressPropText(ps, PROP, "command", setopcmd);

		}
		break;

	case T_Sort:		// ScanState
		ProgressSort((SortState*) planstate, ps);
		break;

	case T_Material:	// ScanState
		/*
		 * Uses: ScanState and Tuplestorestate
		 */
		ProgressMaterial((MaterialState*) planstate, ps);
		ProgressScanBlks((ScanState*) planstate, ps);
		break;

	case T_Group:		// ScanState
		ProgressScanBlks((ScanState*) planstate, ps);
		break;

	case T_Agg:		// ScanState
		/* 
		 * Use tuplesortstate 2 times. Not reflected in child nodes
		 */
		ProgressAgg((AggState*) planstate, ps);
		break;

	case T_WindowAgg:	// ScanState
		/*
		 * Has a Tuplestorestate (field buffer)
		 */
		ProgressTupleStore(((WindowAggState*) plan)->buffer, ps);
		break;

	case T_Unique:		// PlanState
		/* 
		 * Does not store any tuple.
		 * Just fetch tuple and compare with previous one.
		 */
		break;

	case T_Gather:		// PlanState
		/* 
		 * Does not store any tuple.
		 * Used for parallel query
		 */
		ProgressGather((GatherState*) planstate, ps);
 		break;

	case T_GatherMerge:	// PlanState
		ProgressGatherMerge((GatherMergeState*) planstate, ps);
		break;

	case T_Hash:		// PlanState
		/* 
		 * Has a potential on file hash data
		 */
		ProgressHash((HashState*) planstate, ps);
		break;

	case T_LockRows:	// PlanState
		/*
		 * Only store tuples in memory array
		 */
		break;
	
	default:
		break;
	}

	/*
	 * Target list
	 */
        if (ps->verbose >= VERBOSE_TARGETS) {
        	show_plan_tlist(planstate, ancestors, ps);
	}

	/*
	 * Get ready to display the child plans.
	 * Pass current PlanState as head of ancestors list for children
	 */
	haschildren = ReportHasChildren(plan, planstate);
	if (haschildren) {
		ancestors = lcons(planstate, ancestors);
	}

	/*
	 * initPlan-s
	 */
	if (planstate->initPlan) {
		ReportSubPlans(planstate->initPlan, ancestors, "InitPlan", ps, ProgressNode);
	}

	/*
	 * lefttree
	 */
	if (outerPlanState(planstate)) {
		ProgressNode(outerPlanState(planstate), ancestors, "Outer", NULL, ps);
	}

	/*
	 * righttree
	 */
	if (innerPlanState(planstate)) {
		ProgressNode(innerPlanState(planstate), ancestors, "Inner", NULL, ps);
	}

	/*
	 * special child plans
	 */
	switch (nodeTag(plan)) {
	case T_ModifyTable:
		ReportMemberNodes(((ModifyTable*) plan)->plans,
			((ModifyTableState*) planstate)->mt_plans, ancestors, ps, ProgressNode);
		break;

	case T_Append:
		ReportMemberNodes(((Append*) plan)->appendplans,
			((AppendState*) planstate)->appendplans, ancestors, ps, ProgressNode);
		break;

	case T_MergeAppend:
		ReportMemberNodes(((MergeAppend*) plan)->mergeplans,
			((MergeAppendState*) planstate)->mergeplans, ancestors, ps, ProgressNode);
		break;

	case T_BitmapAnd:
		ReportMemberNodes(((BitmapAnd*) plan)->bitmapplans,
			((BitmapAndState*) planstate)->bitmapplans, ancestors, ps, ProgressNode);
		break;

	case T_BitmapOr:
		ReportMemberNodes(((BitmapOr*) plan)->bitmapplans,
			((BitmapOrState*) planstate)->bitmapplans, ancestors, ps, ProgressNode);
		break;

	case T_SubqueryScan:
		ProgressNode(((SubqueryScanState*) planstate)->subplan, ancestors,
			"Subquery", NULL, ps);
		break;

	case T_CustomScan:
		ReportCustomChildren((CustomScanState*) planstate, ancestors, ps, ProgressNode);
		break;

	default:
		break;
	}

	/*
	 * subPlan-s
	 */
	if (planstate->subPlan)
		ReportSubPlans(planstate->subPlan, ancestors, "SubPlan", ps, ProgressNode);

	/*
	 * end of child plans
	 */
	if (haschildren)
		ancestors = list_delete_first(ancestors);

	ProgressUnindent(ps);
}


/******************************************************************************
 * Report details about sorts, quals, targets, scan of execution plan.
 * This code is similar and mostly identical with the one of the EXPLAIN command.
 ******************************************************************************/

/*
 * Show the targetlist of a plan node
 */
static void
show_plan_tlist(PlanState *planstate, List *ancestors, ProgressState *es)
{
        Plan* plan = planstate->plan;
        List* context; 
        List* result = NIL;
        bool useprefix;
        ListCell* lc;

        /* No work if empty tlist (this occurs eg in bitmap indexscans) */
        if (plan->targetlist == NIL)
                return;

        /* The tlist of an Append isn't real helpful, so suppress it */
        if (IsA(plan, Append))
                return;

        /* Likewise for MergeAppend and RecursiveUnion */
        if (IsA(plan, MergeAppend))
                return;

        if (IsA(plan, RecursiveUnion))
                return;

        /*
         * Likewise for ForeignScan that executes a direct INSERT/UPDATE/DELETE
         *
         * Note: the tlist for a ForeignScan that executes a direct INSERT/UPDATE
         * might contain subplan output expressions that are confusing in this
         * context.  The tlist for a ForeignScan that executes a direct UPDATE/
         * DELETE always contains "junk" target columns to identify the exact row
         * to update or delete, which would be confusing in this context.  So, we
         * suppress it in all the cases.
         */
        if (IsA(plan, ForeignScan) && ((ForeignScan *) plan)->operation != CMD_SELECT)
                return;

        /* Set up deparsing context */
        context = set_deparse_context_planstate(es->deparse_cxt, (Node *) planstate, ancestors);
        useprefix = list_length(es->rtable) > 1;

        /* Deparse each result column (we now include resjunk ones) */
        foreach(lc, plan->targetlist) {
                TargetEntry *tle;

		tle = (TargetEntry *) lfirst(lc);
                result = lappend(result, deparse_expression((Node *) tle->expr, context, useprefix, false));
        }

        /* Print results */
        ProgressPropList(es, "target", "target name", result);
}

/*
 * Return pointer to static string describing the join type
 */
static
char* setopCmd(SetOp* setop)
{
	char* setopcmd;

	switch (setop->cmd) {
	case SETOPCMD_INTERSECT:
		setopcmd = "Intersect";
		break;

	case SETOPCMD_INTERSECT_ALL:
		setopcmd = "Intersect All";
		break;

	case SETOPCMD_EXCEPT:
		setopcmd = "Except";
		break;

	case SETOPCMD_EXCEPT_ALL:
		setopcmd = "Except All";
		break;

	default:
		setopcmd = "???";
		break;
	}

	return setopcmd;
}

/*
 * Return pointer to static string describing the join type
 */
static
char* joinType(Join* join)
{
	char* jointype;

	switch (((Join*) join)->jointype) {
	case JOIN_INNER:
		jointype = "Inner";
		break;

	case JOIN_LEFT:
		jointype = "Left";
		break;

	case JOIN_FULL:
		jointype = "Full";
		break;

	case JOIN_RIGHT:
		jointype = "Right";
		break;

	case JOIN_SEMI:
		jointype = "Semi";
		break;

	case JOIN_ANTI:
		jointype = "Anti";
		break;

	default:
		jointype = "???";
		break;
	}

	return jointype;
}

static bool
ReportHasChildren(Plan* plan, PlanState* planstate)
{
	bool haschildren;

	haschildren = planstate->initPlan || outerPlanState(planstate)
		|| innerPlanState(planstate)
		|| IsA(plan, ModifyTable)
		|| IsA(plan, Append)
		|| IsA(plan, MergeAppend)
		|| IsA(plan, BitmapAnd)
		|| IsA(plan, BitmapOr)
		|| IsA(plan, SubqueryScan)
		|| (IsA(planstate, CustomScanState) && ((CustomScanState*) planstate)->custom_ps != NIL)
		|| planstate->subPlan;

	return haschildren;
}

/*
 * Explain the constituent plans of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 *
 * Note: we don't actually need to examine the Plan list members, but
 * we need the list in order to determine the length of the PlanState array.
 */
static void
ReportMemberNodes(List *plans, PlanState **planstates,
	List *ancestors, ProgressState *es,
	functionNode fn)
{
        int nplans = list_length(plans);
        int j;

        for (j = 0; j < nplans; j++)
                (*fn)(planstates[j], ancestors, "Member", NULL, es);
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 */
static void
ReportSubPlans(List *plans, List *ancestors, const char *relationship,
	ProgressState *es, functionNode fn)
{
        ListCell   *lst;

        foreach(lst, plans) {
                SubPlanState *sps = (SubPlanState *) lfirst(lst);
                SubPlan    *sp = (SubPlan *) sps->subplan;

                /*
                 * There can be multiple SubPlan nodes referencing the same physical
                 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
                 * We should print a subplan only once, so track which ones we already
                 * printed.  This state must be global across the plan tree, since the
                 * duplicate nodes could be in different plan nodes, eg both a bitmap
                 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
                 * do not worry too much about which plan node we show the subplan as
                 * attached to in such cases.)
                 */
                if (bms_is_member(sp->plan_id, es->printed_subplans))
                        continue;

                es->printed_subplans = bms_add_member(es->printed_subplans, sp->plan_id);
                (*fn)(sps->planstate, ancestors, relationship, sp->plan_name, es);
        }
}

/*
 * Explain a list of children of a CustomScan.
 */
void
ReportCustomChildren(CustomScanState *css, List *ancestors, ProgressState *es, functionNode fn)
{
       ListCell   *cell;
       const char *label =
       (list_length(css->custom_ps) != 1 ? "children" : "child");

       foreach(cell, css->custom_ps)
               (*fn)((PlanState *) lfirst(cell), ancestors, label, NULL, es);
}



/**********************************************************************************
 * Indivual Progress report functions for the different execution nodes starts here.
 * These functions are leaf function of the Progress tree of functions to be called.
 *
 * For each of theses function, we need to be paranoiac because the execution tree
 * and plan tree can be in any state. Which means, that any pointer may be NULL.
 *
 * Only ProgressState data is reliable. Pointers about ProgressState data can be reference
 * without checking pointers values. All other data must be checked against NULL 
 * pointers.
 **********************************************************************************/

/*
 * Deal with worker backends
 */
static
void ProgressGather(GatherState* gs, ProgressState* ps)
{
	ParallelContext* pc;

	if (gs == NULL)
		return;

	if (gs->pei == NULL)
		return;

	if (gs->pei->pcxt == NULL)
		return;

	pc = gs->pei->pcxt;
	ProgressParallelExecInfo(pc, ps);
}

static
void ProgressGatherMerge(GatherMergeState* gms, ProgressState* ps)
{
	ParallelContext* pc;

	if (gms == NULL)
		return;

	if (gms->pei == NULL)
                return;

        if (gms->pei->pcxt == NULL)
                return;

	pc = gms->pei->pcxt;
	ProgressParallelExecInfo(pc, ps);
}

static
void ProgressParallelExecInfo(ParallelContext* pc, ProgressState* ps)
{
	ProgressCtl* req;		// Current req struct for current backend
	int pid;
	int pid_index;
	int i;

	debug_collector("ProgressParallelExecInfo node");
	if (pc == NULL) {
		debug_collector("ParallelContext is NULL");
		return;
	}

	ps->parallel = true;
	ps->child = false;

	req = progress_ctl_array + MyBackendId;
	req->parallel = true;
	req->child = false;
	req->child_indent = ps->indent;

	debug_collector("collect master parallel worker data");
	ProgressDumpRequest(getpid());

	/* 
	 * write pid of child worker in main req pid array
	 * note the child indentation for table indent field
	 */
	pid_index = 0;
	for (i = 0; i < pc->nworkers_launched; ++i) {
		pid = pc->worker[i].pid;
		req->child_pid[pid_index] = pid;
		pid_index++;
	}
}

static
void ProgressScanBlks(ScanState* ss, ProgressState* ps)
{
	HeapScanDesc hsd;

	if (ss == NULL) {
		debug_collector("SCAN ss is null");
		return;
	}


	hsd = ss->ss_currentScanDesc;
	if (hsd == NULL) {
		debug_collector("SCAN hsd is null");
		return;
	}

	__ProgressScanBlks(hsd, ps);
}

static 
void __ProgressScanBlks(HeapScanDesc hsd, ProgressState* ps) 
{
	ParallelHeapScanDesc phsd;
	unsigned int nr_blks;

	if (hsd == NULL)
		return;	

	phsd = hsd->rs_parallel;
	if (phsd != NULL) {
		/* Parallel query */
		ProgressPropText(ps, PROP, "scan mode", "parallel");
		if (phsd->phs_nblocks != 0 && phsd->phs_cblock != InvalidBlockNumber) {
			if (phsd->phs_cblock > phsd->phs_startblock)
				nr_blks = phsd->phs_cblock - phsd->phs_startblock;
			else
				nr_blks = phsd->phs_cblock + phsd->phs_nblocks - phsd->phs_startblock;

			ProgressPropLong(ps, PROP, "fetched", nr_blks, BLK_UNIT);
			ProgressPropLong(ps, PROP, "total", phsd->phs_nblocks, BLK_UNIT);
			ProgressPropLong(ps, PROP, "completion", 100 * nr_blks/(phsd->phs_nblocks), PERCENT_UNIT);
		} else {
			if (phsd->phs_nblocks != 0)
				ProgressPropLong(ps, PROP, "total", phsd->phs_nblocks, BLK_UNIT);

			ProgressPropLong(ps, PROP, "completion", 100, PERCENT_UNIT);
		}
	} else {
		/* Not a parallel query */
		if (hsd->rs_nblocks != 0 && hsd->rs_cblock != InvalidBlockNumber) {
			if (hsd->rs_cblock > hsd->rs_startblock)
				nr_blks = hsd->rs_cblock - hsd->rs_startblock;
			else
				nr_blks = hsd->rs_cblock + hsd->rs_nblocks - hsd->rs_startblock;

	
			ProgressPropLong(ps, PROP, "fetched", nr_blks, BLK_UNIT);
			ProgressPropLong(ps, PROP, "total", hsd->rs_nblocks, BLK_UNIT);
			ProgressPropLong(ps, PROP, "completion", 100 * nr_blks/(hsd->rs_nblocks), PERCENT_UNIT);
		} else {
			if (hsd->rs_nblocks != 0)
				ProgressPropLong(ps, PROP, "total", hsd->rs_nblocks, BLK_UNIT);

			ProgressPropLong(ps, PROP, "completion", 100, PERCENT_UNIT);
		}
	}
}

static 
void ProgressScanRows(Scan* plan, PlanState* planstate, ProgressState* ps)
{
	Index rti;
	RangeTblEntry* rte;
	char* objectname;

	if (plan == NULL)
		return;

	if (planstate == NULL)
		return;

	rti = plan->scanrelid; 
	rte = rt_fetch(rti, ps->rtable);
	objectname = get_rel_name(rte->relid);

	if (objectname != NULL) {
		ProgressPropText(ps, PROP, "scan on", quote_identifier(objectname));
	}

	if (ps->verbose >= VERBOSE_ROW_SCAN) {	
		ProgressPropLong(ps, PROP, "fetched",
			(unsigned long) planstate->plan_rows, ROW_UNIT);
		ProgressPropLong(ps, PROP, "total",
			(unsigned long) plan->plan.plan_rows, ROW_UNIT);
		ProgressPropLong(ps, PROP, "completion",
			(unsigned short) planstate->percent_done, PERCENT_UNIT);
	}
}

static
void ProgressTidScan(TidScanState* ts, ProgressState* ps)
{
	unsigned int percent;

	if (ts == NULL) {
		return;
	}

	if (ts->tss_NumTids == 0)
		percent = 0;
	else 
		percent = (unsigned short)(100 * (ts->tss_TidPtr) / (ts->tss_NumTids));

	ProgressPropLong(ps, PROP, "fetched", (long int) ts->tss_TidPtr, ROW_UNIT);
	ProgressPropLong(ps, PROP, "total", (long int) ts->tss_NumTids, ROW_UNIT);
	ProgressPropLong(ps, PROP, "completion", percent, PERCENT_UNIT);
}

static
void ProgressLimit(LimitState* ls, ProgressState* ps)
{
	if (ls == NULL)
		return;

	if (ls->position == 0) {
		ProgressPropLong(ps, PROP, "offset", 0, PERCENT_UNIT);
		ProgressPropLong(ps, PROP, "count", 0, PERCENT_UNIT);
	}

	if (ls->position > 0 && ls->position <= ls->offset) {
		ProgressPropLong(ps, PROP, "offset",
			(unsigned short)(100 * (ls->position)/(ls->offset)), PERCENT_UNIT);
		ProgressPropLong(ps, PROP, "count", 0, PERCENT_UNIT);
	}

	if (ls->position > ls->offset) {
		ProgressPropLong(ps, PROP, "offset", 100, PERCENT_UNIT);
		ProgressPropLong(ps, PROP, "count",
			(unsigned short)(100 * (ls->position - ls->offset)/(ls->count)), PERCENT_UNIT);
	}
}

/*
 * Report of CustomScan is not implemented.
 */
static
void ProgressCustomScan(CustomScanState* cs, ProgressState* ps)
{
	return;
}

static
void ProgressIndexScan(IndexScanState* is, ProgressState* ps) 
{
	PlanState planstate;
	Plan* p;

	if (is == NULL) {
		return;
	}

	planstate = is->ss.ps;
	p = planstate.plan;
	if (p == NULL) {
		return;
	}

	if (ps->verbose >= VERBOSE_ROW_SCAN) {
		ProgressPropLong(ps, PROP, "fetched", (long int) planstate.plan_rows, ROW_UNIT);
		ProgressPropLong(ps, PROP, "total", (long int) p->plan_rows, ROW_UNIT);
	}

	ProgressPropLong(ps, PROP, "completion", (unsigned short) planstate.percent_done, PERCENT_UNIT);
}

static
void ProgressModifyTable(ModifyTableState *mts, ProgressState* ps)
{
	EState* es;

	if (mts == NULL)
		return;

	es = mts->ps.state;
	if (es == NULL)
		return;

	ProgressPropLong(ps, PROP, "modified", (long int) es->es_processed, ROW_UNIT);
}

static
void ProgressHash(HashState* hs, ProgressState* ps)
{
	if (hs == NULL)
		return;
	
	ProgressHashJoinTable((HashJoinTable) hs->hashtable, ps);
}

static
void ProgressHashJoin(HashJoinState* hjs, ProgressState* ps)
{
	if (hjs == NULL)
		return;

	ProgressHashJoinTable((HashJoinTable) hjs->hj_HashTable, ps);
}

/*
 * HashJoinTable is not a node type
 */
static
void ProgressHashJoinTable(HashJoinTable hashtable, ProgressState* ps)
{
	int i;
	unsigned long reads;
	unsigned long writes;
	unsigned long disk_size;
	unsigned long lreads;
	unsigned long lwrites;
	unsigned long ldisk_size;

	/*
	 * Could be used but not yet allocated
	 */
	if (hashtable == NULL)
		return;
		
	if (hashtable->nbatch <= 1)
		return;

	if (ps->verbose >= VERBOSE_HASH_JOIN)
		ProgressPropLong(ps, PROP, "hashtable nbatch", hashtable->nbatch, "");

	/*
	 * Display global reads and writes
	 */
	reads = 0;
	writes = 0;
	disk_size = 0;

	for (i = 0; i < hashtable->nbatch; i++) {
		if (hashtable->innerBatchFile[i]) {
			ProgressBufFileRW(hashtable->innerBatchFile[i],
				ps, &lreads, &lwrites, &ldisk_size);
			reads += lreads;
			writes += lwrites;
			disk_size += ldisk_size;
		}

		if (hashtable->outerBatchFile[i]) {
			ProgressBufFileRW(hashtable->outerBatchFile[i],
				ps, &lreads, &lwrites, &ldisk_size);
			reads += lreads;
			writes += lwrites;
			disk_size += ldisk_size;
		}
	}

	/* 
	 * Update SQL query wide disk use
  	 */
	ps->disk_size += disk_size;

	if (ps->verbose >= VERBOSE_HASH_JOIN) {
		ProgressPropLong(ps, PROP, "read", reads/1024, KBYTE_UNIT);
		ProgressPropLong(ps, PROP, "write", writes/1024, KBYTE_UNIT);
	}

	if (writes > 0)
		ProgressPropLong(ps, PROP, "completion", reads/writes, PERCENT_UNIT);

	if (ps->verbose >= VERBOSE_DISK_USE)
		ProgressPropLong(ps, PROP, "disk used", disk_size/1024, KBYTE_UNIT);

	/*
	 * Only display details if requested
	 */ 
	if (ps->verbose < VERBOSE_HASH_JOIN_DETAILED)
		return;

	if (hashtable->nbatch == 0)
		return;

	ps->indent++;
	for (i = 0; i < hashtable->nbatch; i++) {
		ProgressPropLong(ps, PROP, "batch", (long int) i, "");

		if (hashtable->innerBatchFile[i]) {
			ps->indent++;
			ProgressPropText(ps, PROP, "group", "inner");
			ProgressBufFile(hashtable->innerBatchFile[i], ps);
			ps->indent--;
		}

		if (hashtable->outerBatchFile[i]) {
			ps->indent++;
			ProgressPropText(ps, PROP, "group", "outer");
			ProgressBufFile(hashtable->outerBatchFile[i], ps);
			ps->indent--;
		}
	}

	ps->indent--;
}

static
void ProgressBufFileRW(BufFile* bf, ProgressState* ps,
	unsigned long* reads, unsigned long* writes, unsigned long* disk_size)
{
	MemoryContext oldcontext;
	struct buffile_state* bfs;
	int i;

	if (bf == NULL)
		return;

	*reads = 0;
	*writes = 0;
	*disk_size = 0;

	oldcontext = MemoryContextSwitchTo(ps->memcontext);
	bfs = BufFileState(bf);
	MemoryContextSwitchTo(oldcontext);

	*disk_size = bfs->disk_size;	

	for (i = 0; i < bfs->numFiles; i++) {
		*reads += bfs->bytes_read[i];
		*writes += bfs->bytes_write[i];
	}
}
	
static
void ProgressBufFile(BufFile* bf, ProgressState* ps)
{
	int i;
	struct buffile_state* bfs;
	MemoryContext oldcontext;
	
	if (bf == NULL)
		return;

        oldcontext = MemoryContextSwitchTo(ps->memcontext);
	bfs = BufFileState(bf);
	MemoryContextSwitchTo(oldcontext);

	if (ps->verbose < VERBOSE_BUFFILE)
		return;

	ps->indent++;
	ProgressPropLong(ps, PROP, "buffile nr files", bfs->numFiles, "");

	if (bfs->numFiles == 0)
		return;

	if (ps->verbose >= VERBOSE_DISK_USE)
		ProgressPropLong(ps, PROP, "disk used", bfs->disk_size/1024,  KBYTE_UNIT);

	for (i = 0; i < bfs->numFiles; i++) {
		ps->indent++;
		ProgressPropLong(ps, NODE, "file", i, "");
		ProgressPropLong(ps, PROP, "read", bfs->bytes_read[i]/1024, KBYTE_UNIT);
		ProgressPropLong(ps, PROP, "write", bfs->bytes_write[i]/1024, KBYTE_UNIT);
		ps->indent--;
	}	

	ps->indent--;
}

static
void ProgressMaterial(MaterialState* planstate, ProgressState* ps)
{
	Tuplestorestate* tss;

	if (planstate == NULL)
		return;

	tss = planstate->tuplestorestate;
	ProgressTupleStore(tss, ps);

}
/*
 * Tuplestorestate is not a node type
 */
static
void ProgressTupleStore(Tuplestorestate* tss, ProgressState* ps)
{
	struct tss_report tssr;

	if (tss == NULL)
		return;

	tuplestore_get_state(tss, &tssr);

	switch (tssr.status) {
	case TSS_INMEM:
		/* Add separator */
		ProgressPropLong(ps, PROP, "memory write", (long int) tssr.memtupcount, ROW_UNIT);
		if (tssr.memtupskipped > 0)
			ProgressPropLong(ps, PROP, "memory skipped", (long int) tssr.memtupskipped, ROW_UNIT);

		ProgressPropLong(ps, PROP, "memory read", (long int) tssr.memtupread, ROW_UNIT);
		if (tssr.memtupdeleted)
			ProgressPropLong(ps, PROP, "memory deleted", (long int) tssr.memtupread, ROW_UNIT);
		break;
	
	case TSS_WRITEFILE:
	case TSS_READFILE:
		if (tssr.status == TSS_WRITEFILE)
			ProgressPropText(ps, PROP, "file store", "write");
		else 
			ProgressPropText(ps, PROP, "file store", "read");

		ProgressPropLong(ps, PROP, "readptrcount", tssr.readptrcount, "");
		ProgressPropLong(ps, PROP, "write", (long int ) tssr.tuples_count, ROW_UNIT);
		if (tssr.tuples_skipped)
			ProgressPropLong(ps, PROP, "skipped", (long int) tssr.tuples_skipped, ROW_UNIT);

		ProgressPropLong(ps, PROP, "read", (long int) tssr.tuples_read, ROW_UNIT);
		if (tssr.tuples_deleted)
			ProgressPropLong(ps, PROP, "deleted", (long int) tssr.tuples_deleted, ROW_UNIT);

		ps->disk_size += tssr.disk_size;
		if (ps->verbose >= VERBOSE_DISK_USE)
			ProgressPropLong(ps, PROP, "disk used", tssr.disk_size/2014, KBYTE_UNIT);
		break;

	default:
		break;
	}
}

static
void ProgressAgg(AggState* planstate, ProgressState* ps)
{
	if (planstate == NULL)
		return;

	ProgressTupleSort(planstate->sort_in, ps);
	ProgressTupleSort(planstate->sort_out, ps);
}

static
void ProgressSort(SortState* ss, ProgressState* ps)
{
	Assert(nodeTag(ss) == T_SortState);

	if (ss == NULL)
		return;

	if (ss->tuplesortstate == NULL)
		return;

	ProgressTupleSort(ss->tuplesortstate, ps);
}

static
void ProgressTupleSort(Tuplesortstate* tss, ProgressState* ps)
{
	struct ts_report* tsr;
	MemoryContext oldcontext;
	char status[] = "sort status";
	
	if (tss == NULL)
		return;
	
	oldcontext = MemoryContextSwitchTo(ps->memcontext);
	tsr = tuplesort_get_state(tss);
	MemoryContextSwitchTo(oldcontext);

	switch (tsr->status) {
	case TSS_INITIAL:		/* Loading tuples in mem still within memory limit */
	case TSS_BOUNDED:		/* Loading tuples in mem into bounded-size heap */
		ProgressPropText(ps, PROP, status, "loading tuples in memory");
		ProgressPropLong(ps, PROP, "tuples in memory", tsr->memtupcount, ROW_UNIT);	
		break;

	case TSS_SORTEDINMEM:		/* Sort completed entirely in memory */
		ProgressPropText(ps, PROP, status, "sort completed in memory");
		ProgressPropLong(ps, PROP, "tuples in memory", tsr->memtupcount, ROW_UNIT);	
		break;

	case TSS_BUILDRUNS:		/* Dumping tuples to tape */
		switch (tsr->sub_status) {
		case TSSS_INIT_TAPES:
			ProgressPropText(ps, PROP, status, "on tapes initializing");
			break;

		case TSSS_DUMPING_TUPLES:
			ProgressPropText(ps, PROP, status, "on tapes writing");
			break;

		case TSSS_SORTING_ON_TAPES:
			ProgressPropText(ps, PROP, status, "on tapes sorting");
			break;

		case TSSS_MERGING_TAPES:
			ProgressPropText(ps, PROP, status, "on tapes merging");
			break;
		default:
			;
		};

		dumpTapes(tsr, ps);
		break;
	
	case TSS_FINALMERGE: 		/* Performing final merge on-the-fly */
		ProgressPropText(ps, PROP, status, "on tapes final merge");
		dumpTapes(tsr, ps);	
		break;

	case TSS_SORTEDONTAPE:		/* Sort completed, final run is on tape */
		switch (tsr->sub_status) {
		case TSSS_FETCHING_FROM_TAPES:
			ProgressPropText(ps, PROP, status,
				"fetching from sorted tapes");
			break;

		case TSSS_FETCHING_FROM_TAPES_WITH_MERGE:
			ProgressPropText(ps, PROP, status,
				"fetching from sorted tapes with merge");
			break;

		default:
			;
		};

		dumpTapes(tsr, ps);	
		break;

	default:
		ProgressPropText(ps, PROP, status, "unexpected sort state");
	};
}

static
void dumpTapes(struct ts_report* tsr, ProgressState* ps)
{
	int i;
	int percent_effective;

	if (tsr == NULL)
		return;

	if (tsr->tp_write_effective > 0) {
		percent_effective = 100 * (tsr->tp_read_effective)/(tsr->tp_write_effective);
	} else {
		percent_effective = 0;
	}

	if (ps->verbose >= VERBOSE_TAPES) {
		ProgressPropLong(ps, PROP, "merge reads", tsr->tp_read_merge, ROW_UNIT);
		ProgressPropLong(ps, PROP, "merge writes", tsr->tp_write_merge, ROW_UNIT);
		ProgressPropLong(ps, PROP, "effective reads", tsr->tp_read_effective, ROW_UNIT);
		ProgressPropLong(ps, PROP, "effective writes", tsr->tp_write_effective, ROW_UNIT);
	}

	ProgressPropLong(ps, PROP, "completion", percent_effective, PERCENT_UNIT);
	if (ps->verbose >= VERBOSE_DISK_USE)
		ProgressPropLong(ps, PROP, "tape size", tsr->blocks_alloc, BLK_UNIT);

	/*
	 * Update total disk size used 
	 */
	ps->disk_size += tsr->blocks_alloc * BLCKSZ;

	if (ps->verbose < VERBOSE_TAPES_DETAILED)
		return;

	/*
	 * Verbose report
	 */
	ProgressPropLong(ps, PROP, "tapes total", tsr->maxTapes, NO_UNIT);
	ProgressPropLong(ps, PROP, "tapes actives", tsr->activeTapes, NO_UNIT);

	if (tsr->result_tape != -1)
		ProgressPropLong(ps, PROP, "tape result", tsr->result_tape, NO_UNIT);

	if (tsr->maxTapes != 0) {
		for (i = 0; i< tsr->maxTapes; i++) {
			ps->indent++;
			ProgressPropLong(ps, NODE, "tape idx", i, NO_UNIT);

			ps->indent++;
			if (tsr->tp_fib != NULL)
				ProgressPropLong(ps, PROP, "fib", tsr->tp_fib[i], NO_UNIT);

			if (tsr->tp_runs != NULL)
				ProgressPropLong(ps, PROP, "runs", tsr->tp_runs[i], NO_UNIT);

			if (tsr->tp_dummy != NULL)
				ProgressPropLong(ps, PROP, "dummy", tsr->tp_dummy[i], NO_UNIT);

			if (tsr->tp_read != NULL)
				ProgressPropLong(ps, PROP, "read", tsr->tp_read[i], ROW_UNIT);

			if (tsr->tp_write)	
				ProgressPropLong(ps, PROP, "write", tsr->tp_write[i], ROW_UNIT);
				
			ps->indent--;
			ps->indent--;
		}
	}
}

static
void ReportTime(QueryDesc* query, ProgressState* ps)
{
	instr_time currenttime;

	if (query == NULL)
		return;

	if (query->totaltime == NULL)
		return;

	INSTR_TIME_SET_CURRENT(currenttime);
	INSTR_TIME_SUBTRACT(currenttime, query->totaltime->starttime);

	if (ps->verbose >= VERBOSE_TIME_REPORT) {
		ProgressPropLong(ps, PROP, "time used",
			INSTR_TIME_GET_MILLISEC(currenttime)/1000, SECOND_UNIT);
	}
}

static  
void ReportStack(ProgressState* ps)
{
	unsigned long depth;
	unsigned long max_depth;

	depth =	get_stack_depth();
	max_depth = get_max_stack_depth();

	if (ps->verbose >= VERBOSE_STACK) {
		ProgressPropLong(ps, PROP, "stack depth", depth, BYTE_UNIT);
		ProgressPropLong(ps, PROP, "max stack depth", max_depth, BYTE_UNIT);
	}
}

static
void ReportDisk(ProgressState*  ps)
{
	unsigned long size;
	char* unit;

	size = ps->disk_size;
	
	if (size < 1024) {
		unit = BYTE_UNIT;
	} else if (size >= 1024 && size < 1024 * 1024) {
		unit = KBYTE_UNIT;
		size = size / 1024;
	} else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
		unit = MBYTE_UNIT;
		size = size / (1024 * 1024);
	} else {
		unit = GBYTE_UNIT;
		size = size / (1024 * 1024 * 1024);
	}
	
	if (ps->verbose >= VERBOSE_DISK_USE) 	
		ProgressPropLong(ps, PROP, "disk used", size, unit);
}
