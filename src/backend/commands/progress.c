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
#include "executor/progress.h"
#include "access/xact.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "storage/backendid.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/execParallel.h"
#include "commands/defrem.h"
#include "commands/report.h"
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
#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"

static int log_stmt = 1;		/* log query monitored */
static int debug = 1;

/* 
 * Monitoring progress waits 5secs for monitored backend response.
 *
 * If this timeout is too short, it may not leave enough time for monotored backend to dump its
 * progression about the SQL query it is running.
 *
 * If this timeout is too long, a cancelled SQL query in a backend could block the monitoring
 * backend too for a longi time.
 */
unsigned short PROGRESS_TIMEOUT = 10;
unsigned short PROGRESS_TIMEOUT_CHILD = 5;
unsigned short progress_did_timeout = 0;
char* progress_backend_timeout = "<backend timeout>";

/*
 * Backend type (single worker, parallel main worker, parallel child worker
 */
#define SINGLE_WORKER	0
#define MAIN_WORKER	1
#define CHILD_WORKER	2

/*
 * Number of colums for pg_progress SQL function
 */
#define PG_PROGRESS_COLS	6

/*
 * One ProgressCtl is allocated for each backend process which is to be potentially monitored
 * The array of progress_ctl structures is protected by ProgressLock global lock.
 *
 * Only one backend can be monitored at a time. This may be improved with a finer granulary
 * using a LWLock tranche of MAX_NR_BACKENDS locks. In which case, one backend can be monitored
 * independantly of the otther backends.
 *
 * The LWLock ensure that one backend can be only monitored by one other backend at a time.
 * Other backends trying to monitor an already monitered backend will be put in
 * queue of the LWWlock.
 */
typedef struct ProgressCtl {
        bool verbose;			/* be verbose */

	bool parallel;			/* true if parallel query */
	bool child;			/* true if child worker */
	unsigned int child_indent;	/* Indentation for child worker */

	unsigned long disk_size;	/* Disk size in bytes used by the backend for sorts, stores, hashes */

	char* buf;			/* progress status report in shm */
	struct Latch* latch;		/* Used by requestor to wait for backend to complete its report */
} ProgressCtl;

struct ProgressCtl* progress_ctl_array;	/* Array of MaxBackends ProgressCtl */
char* dump_buf_array;			/* SHMEM buffers one for each backend */
struct Latch* resp_latch_array;		/* Array of MaxBackends latches to synchronize response
					 * from monitored backend to monitoring backend */

typedef struct ProgressState {
	bool verbose;           /* be verbose */
 
	bool child;             /* true if parallel and child backend */
	bool parallel;          /* true if parallel query */
	bool parallel_reported; /* true if parallel backends already reported once */

	/*
	 * State for output formating
	 */
	StringInfo str;         /* output buffer */
	int indent;             /* current indentation level */
	int pid;
	int lineid;

	List* grouping_stack;   /* format-specific grouping state */
 
	MemoryContext memcontext;

	/*
	 * State related to current plan/execution tree
	 */
	PlannedStmt* pstmt;
	struct Plan* plan;
	struct PlanState* planstate;
	List* rtable;
	List* rtable_names;
	List* deparse_cxt;      /* context list for deparsing expressions */
	EState* es;             /* Top level data */
	Bitmapset* printed_subplans;    /* ids of SubPlans we've printed */

	unsigned long disk_size;        /* PROGRESS command track on disk use for sorts, stores, and hashes */
} ProgressState;

/*
 * No progress request unless requested.
 */
volatile bool progress_requested = false;

/*
 * local functions
 */
static void ProgressPlan(QueryDesc* query, ProgressState* ps);
static void ProgressNode(PlanState* planstate, List* ancestors,
	const char* relationship, const char* plan_name, ProgressState* ps);

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

extern void ReportText(const char* label, const char* value, ReportState* rpt);
extern void ReportTextNoNewLine(const char* label, const char* value, ReportState* rpt);

static void ReportTime(QueryDesc* query, ProgressState* ps);
static void ReportStack(ProgressState* ps);
static void ReportDisk(ProgressState* ps);

static void ProgressDumpRequest(ProgressCtl* req);
static void ProgressResetRequest(ProgressCtl* req);

static void ProgressPropLong(ProgressState* ps, const char* prop, unsigned long value, const char* unit);
static void ProgressPropText(ProgressState* ps, const char* prop, const char* value);

static ProgressState* CreateProgressState(void);



Size ProgressShmemSize(void)
{
	Size size;

	/* Must match ProgressShmemInit */
	size = mul_size(MaxBackends, sizeof(ProgressCtl));
	size = add_size(size, mul_size(MaxBackends, PROGRESS_AREA_SIZE));
	size = add_size(size, mul_size(MaxBackends, sizeof(struct Latch)));

	return size;
}

/*
 * Initialize our shared memory area
 */
void ProgressShmemInit(void)
{
	bool found;
	size_t size = 0;

	/*
 	 * Allocated shared latches for response to progress request
 	 */
	size = mul_size(MaxBackends, sizeof(struct Latch));
	resp_latch_array = ShmemInitStruct("Progress latches", size, &found);
	if (!found) {
		int i;
		struct Latch* l;

		l = resp_latch_array;
		for (i = 0; i < MaxBackends; i++) {
			InitSharedLatch(l);
			l++;
		}	
	}

	/*
	 * Allocate SHMEM buffers for backend communication
	 */
	size = MaxBackends * PROGRESS_AREA_SIZE;
	dump_buf_array = (char*) ShmemInitStruct("Backend Dump Pages", size, &found);
	if (!found) {
        	memset(dump_buf_array, 0, size);
	}

	/*
	 * Allocate progress request meta data, one for each backend
	 */
	size = mul_size(MaxBackends, sizeof(ProgressCtl));
	progress_ctl_array = ShmemInitStruct("ProgressCtl array", size, &found);
	if (!found) {
		int i;
		ProgressCtl* req;
		struct Latch* latch;

		req = progress_ctl_array;
		latch = resp_latch_array;
		for (i = 0; i < MaxBackends; i++) {
			/* Already zeroed above */
			memset(req, 0, sizeof(ProgressCtl));
	
			/* set default value */
			req->latch = latch;
			req->buf = dump_buf_array + i * PROGRESS_AREA_SIZE;
			req->parallel = false;
			req->child = false;
			req->child_indent = 0;
			req->disk_size = 0;
			req++;
			latch++;
		}
	}

	return;
}

static
void ProgressPropLong(ProgressState* ps, const char* prop, unsigned long value, const char* unit)
{
	/*
	 * Fields are: pid, lineid, indent, name, value, unit
	 */
	char pid_str[9];
	char lineid_str[9];
	char indent_str[9];
	char value_str[17];

	sprintf(pid_str, "%d", ps->pid);
	sprintf(lineid_str, "%d", ps->lineid);
	sprintf(indent_str, "%d", ps->indent);
	sprintf(value_str, "%lu", value);
	
	appendStringInfo(ps->str, "%s|%s|%s|%s|%s|%s|", pid_str, lineid_str, indent_str, prop, value_str, unit);
	ps->lineid++;
}

static
void ProgressPropText(ProgressState* ps, const char* prop, const char* value)
{
	/*
	 * Fields are: pid, lineid, indent, name, value, unit
	 */
	char pid_str[9];
	char lineid_str[9];
	char indent_str[9];

	sprintf(pid_str, "%d", ps->pid);
	sprintf(lineid_str, "%d", ps->lineid);
	sprintf(indent_str, "%d", ps->indent);
	
	appendStringInfo(ps->str, "%s|%s|%s|%s|%s||", pid_str, lineid_str, indent_str, prop, value);
	ps->lineid++;
}

static
void ProgressResetReport(ProgressState* ps)
{
	resetStringInfo(ps->str);
}

/*
 * Colums are: pid, lineid, indent, property, value, unit
 */
Datum pg_progress(PG_FUNCTION_ARGS)
{
	int pid;
	char* buf;

	unsigned short verbose;

	Datum values[PG_PROGRESS_COLS];
	bool nulls[PG_PROGRESS_COLS];
	TupleDesc tupdesc;
	Tuplestorestate* tupstore;
	ReturnSetInfo* rsinfo;

	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	char pid_str[9];
	char lineid_str[9];
	char indent_str[9];
	char prop_str[256];
	char value_str[17];
	char unit_str[9];

	if (debug)
		elog(LOG, "Start of pg_progress");

	/*
	 * pid = 0 means collect progress report for all backends
	 */
	pid = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT32(0);
	verbose = PG_ARGISNULL(0) ? false : PG_GETARG_UINT16(1);

	rsinfo = (ReturnSetInfo*) fcinfo->resultinfo;

	/*
	 * Build a tuple descriptor for our result type
	 */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		elog(ERROR, "return type must be a row type");
	}

	/*
	 * Switch to query memory context
	 */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Allocate buf for local work */
	buf = palloc0(PROGRESS_AREA_SIZE);
	ProgressSendRequest(pid, verbose, buf);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	while (sscanf(buf, "%9[^|],%9[^|],%9[^|],%256[^|],%17[^|],%9[^|]",
		pid_str, lineid_str, indent_str, prop_str, value_str, unit_str) != EOF) {
		values[0] = CStringGetDatum(pid_str);
		nulls[0] = false;

		values[1] = CStringGetDatum(lineid_str);
		nulls[1] = false;

		values[2] = CStringGetDatum(indent_str);
		nulls[2] = false;

		values[3] = CStringGetDatum(prop_str);
		nulls[3] = false;

		values[4] = CStringGetDatum(value_str);
		nulls[4] = false;

		values[5] = CStringGetDatum(unit_str);
		nulls[5] = false;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	
	tuplestore_donestoring(tupstore);

	pfree(buf);
	MemoryContextSwitchTo(oldcontext);

	return (Datum) 0;
}

/*
 * ProgressSendRequest:
 * 	Log a request to a backend in order to fetch its progress log
 *	This is initaited by the SQL command: PROGRESS pid.
 */
void ProgressSendRequest(int pid, int verbose, char* buf)
{
	BackendId bid;
	ProgressCtl* req;			// Used for the request
	unsigned int buf_len = 0;

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

	/*
	 * Serialize signals/request to get the progress state of the query
	 */
	LWLockAcquire(ProgressLock, LW_EXCLUSIVE);

	req = progress_ctl_array + bid;
	ProgressResetRequest(req);
	req->verbose = verbose;
	ProgressDumpRequest(req);

	OwnLatch(req->latch);
	ResetLatch(req->latch);

	SendProcSignal(pid, PROCSIG_PROGRESS, bid);
	WaitLatch(req->latch, WL_LATCH_SET | WL_TIMEOUT , PROGRESS_TIMEOUT * 1000L, WAIT_EVENT_PROGRESS);
	DisownLatch(req->latch);

	/* Fetch result and clear SHM buffer */
	if (strlen(req->buf) == 0) {
		/* We have timed out on PROGRESS_TIMEOUT */
		progress_did_timeout = 1;	
		memcpy(buf, progress_backend_timeout, strlen(progress_backend_timeout));
	} else {
		/* We have a result computed by the monitored backend */
		buf_len = strlen(req->buf);
		memcpy(buf, req->buf, buf_len);
	}

	/*
	 * Clear shm buffer
	 */
	memset(req->buf, 0, PROGRESS_AREA_SIZE);

	/*
	 * End serialization
	 */
	LWLockRelease(ProgressLock);

	if (progress_did_timeout) {
		ereport(ERROR, (
		errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
		errmsg("timeout to get response")));
	}
}

static
ProgressState* CreateProgressState(void)
{
	StringInfo str;
	ProgressState* prg;

	str = makeStringInfo();
 
	prg = (ProgressState*) palloc0(sizeof(ProgressState));
	prg->parallel = false;
	prg->parallel_reported = false;
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
void ProgressDumpRequest(ProgressCtl* req)
{
	elog(LOG, "backend bid=%d pid=%d, verbose=%d, indent=%d, parallel=%d child=%d",
		MyBackendId, getpid(), req->verbose, req->child_indent, req->parallel, req->child);
}

static
void ProgressResetRequest(ProgressCtl* req)
{
	elog(LOG, "reset progress request at addr %p", req);

	req->parallel = false;
	req->child = false;
	req->child_indent = 0;
	req->disk_size = 0;

	InitSharedLatch(req->latch);
	memset(req->buf, 0, PROGRESS_AREA_SIZE);
}

void HandleProgressSignal(void)
{
	progress_requested = true;
	InterruptPending = true;
}

void HandleProgressRequest(void)
{
	ProgressCtl* req;
	ProgressState* ps;

	MemoryContext oldcontext;
	MemoryContext progress_context;

	unsigned short running = 0;
	char* shmBufferTooShort = "shm buffer is too small";
	bool child = false;

	/*
	 * We hold interrupt here because the current SQL query could be cancelled at any time. In which 
	 * case, the current backend would not call SetLatch(). Monitoring backend would wait endlessly.
	 *
	 * To avoid such situation, a further safety measure has been added: the monitoring backend waits
	 * the response for a maximum of PROGRESS_TIMEOUT time. After this timeout has expired, the monitoring
	 * backend sends back the respponse which is empty.
	 *
	 * The current backend could indeed be interrupted before the HOLD_INTERRUPTS() is reached.
	 */
	HOLD_INTERRUPTS();

	progress_context =  AllocSetContextCreate(CurrentMemoryContext, "ProgressState", ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(progress_context);

	ps = CreateProgressState();
	ps->memcontext = progress_context;

	Assert(ps != NULL);
	Assert(ps->str != NULL);
	Assert(ps->str->data != NULL);

	req = progress_ctl_array + MyBackendId;

	ps->parallel = req->parallel;
	ps->verbose = req->verbose;
	ps->child = req->child;
	ps->indent = req->child_indent;
	ps->disk_size = 0;

	/* Local params */
	child = req->child;

	if (debug)
		ProgressDumpRequest(req);

	/*
	 * Clear previous content of ps->str
	 */
	ProgressResetReport(ps);

	if (MyQueryDesc == NULL) {
		ProgressPropText(ps, "status", "<idle backend>");
	} else if (!IsTransactionState()) {
		ProgressPropText(ps, "status", "<out of transaction>");
	} else if (MyQueryDesc->plannedstmt == NULL) {
		ProgressPropText(ps, "status", "<NULL planned statement>");
	} else if (MyQueryDesc->plannedstmt->commandType == CMD_UTILITY) {
		ProgressPropText(ps, "status", "<utility statement>");
	} else if (MyQueryDesc->already_executed == false) {
		ProgressPropText(ps, "status", "<query not yet started>");
	} else if (QueryCancelPending) {
		ProgressPropText(ps, "status", "<query cancel pending>");
	} else if (RecoveryConflictPending) {
		ProgressPropText(ps, "status", "<recovery conflict pending>");
	} else if (ProcDiePending) {
		ProgressPropText(ps, "status", "<proc die pending>");
	} else {
		if (!child)
			ProgressPropText(ps, "status", "<query running>");

		running = 1;
	}

	if (log_stmt && !child) {
		if (MyQueryDesc != NULL && MyQueryDesc->sourceText != NULL)
			ProgressPropText(ps, "query", MyQueryDesc->sourceText);
	}

	if (running) {
		if (!child)
			ReportTime(MyQueryDesc, ps);

		if (ps->verbose) {
			ReportStack(ps);
		}

		ProgressPlan(MyQueryDesc, ps);
		if (!child)
			ReportDisk(ps); 	/* must come after ProgressPlan() */
	}

	/* 
	 * Dump in SHM the string buffer content
	 */
	if (strlen(ps->str->data) < PROGRESS_AREA_SIZE) {
		/* Mind the '\0' char at the end of the string */
		memcpy(req->buf, ps->str->data, strlen(ps->str->data) + 1); 
	} else {
		memcpy(req->buf, shmBufferTooShort, strlen(shmBufferTooShort));
		elog(LOG, "Needed size for buffer %d", (int) strlen(ps->str->data));
	}

	/* Dump disk size used for stores, sorts, and hashes */
	req->disk_size = ps->disk_size;

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(ps->memcontext);
	
	SetLatch(req->latch);		// Notify of progress state delivery
	RESUME_INTERRUPTS();
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

	ReportPreScanNode(query->planstate, &rels_used);

	ps->rtable_names = select_rtable_names_for_explain(ps->rtable, rels_used);
	ps->deparse_cxt = deparse_context_for_plan_rtable(ps->rtable, ps->rtable_names);
	ps->printed_subplans = NULL;

	planstate = query->planstate;
	if (IsA(planstate, GatherState) && ((Gather*) planstate->plan)->invisible) {
		planstate = outerPlanState(planstate);
	}

	if (ps->child)
		ProgressNode(planstate, NIL, "child worker", NULL, ps);
	else if (ps->parallel)
		ProgressNode(planstate, NIL, "main worker", NULL, ps);
	else
		ProgressNode(planstate, NIL, "single worker", NULL, ps);
}
	
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
	int save_indent = ps->indent;
	bool haschildren;
	int ret;

	if (debug)
		elog(LOG, "=> %s", nodeToString(plan));

	
	/*
	 * 1st step: display the node type
	 */
	ret = planNodeInfo(plan, &info);
	if (ret != 0) {
		elog(LOG, "unknown node type for plan");
	}

	ProgressPropText(ps, "node", relationship != NULL ? relationship : "progress");

	if (ps->parallel && !ps->parallel_reported) {
		if (ps->child)
			ProgressPropLong(ps, "worker child", getpid(), "");
		else
			ProgressPropLong(ps, "worker parent", getpid(), "");
	
		ps->parallel_reported = true;
	}

	/*
	 * Report node top properties
	 */
	ProgressPropText(ps, "node name", info.pname);
	if (plan_name)
		ProgressPropText(ps, "plan name", plan_name);

	if (plan->parallel_aware)
		ProgressPropText(ps, "node mode", "parallel");
                 
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

		switch (((Join*) plan)->jointype) {
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

		ProgressPropText(ps, "join type", jointype);

		}

		ProgressHashJoin((HashJoinState*) planstate, ps);
		break;

	case T_SetOp: {		// PlanState
		/*
		 *  Only uses a in memory hash table
		 */
		const char* setopcmd;

		switch (((SetOp*) plan)->cmd) {
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

		ProgressPropText(ps, "command", setopcmd);

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
        //if (ps->verbose)
        //       show_plan_tlist(planstate, ancestors, ps);

	/*
	 * Controls (sort, qual, ...) 
	 */
	//show_control_qual(planstate, ancestors, ps);

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
	//if (planstate->initPlan) {
	//		ReportSubPlans(planstate->initPlan, ancestors, "InitPlan", ps, ProgressNode);
	//	}

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
/*
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
*/

	/*
	 * subPlan-s
	 */
//	if (planstate->subPlan) {
//		ReportSubPlans(planstate->subPlan, ancestors, "SubPlan", ps, ProgressNode);
//	}

	/*
	 * end of child plans
	 */
	if (haschildren) {
		ancestors = list_delete_first(ancestors);
	}

	/*
	 * in text format, undo whatever indentation we added
	 */
	ps->indent = save_indent;
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

	pc = gs->pei->pcxt;
	ProgressParallelExecInfo(pc, ps);
}

static
void ProgressGatherMerge(GatherMergeState* gms, ProgressState* ps)
{
	ParallelContext* pc;

	pc = gms->pei->pcxt;
	ProgressParallelExecInfo(pc, ps);
}

static
void ProgressParallelExecInfo(ParallelContext* pc, ProgressState* ps)
{
	int i;
	int pid;
	BackendId bid;
	ProgressCtl* req;			// Used for the request
	unsigned int shmbuf_len;
	char* lbuf;

	if (debug)
		elog(LOG, "ProgressParallelExecInfo node");

	if (pc == NULL) {
		elog(LOG, "ParallelContext is NULL");
		return;
	}

	if (debug)
		elog(LOG, "ParallelContext number of workers launched %d", pc->nworkers_launched); 

	ps->parallel = true;

	for (i = 0; i < pc->nworkers_launched; ++i) {
		pid = pc->worker[i].pid;
		bid = ProcPidGetBackendId(pid);
		if (bid == InvalidBackendId)
			continue;

		req = progress_ctl_array + bid;
		req->child = true;
		req->child_indent = ps->indent;
		req->parallel = true;

		ps->parallel_reported = false;

		OwnLatch(req->latch);
		ResetLatch(req->latch);

		SendProcSignal(pid, PROCSIG_PROGRESS, bid);
		WaitLatch(req->latch, WL_LATCH_SET | WL_TIMEOUT , PROGRESS_TIMEOUT_CHILD * 1000L, WAIT_EVENT_PROGRESS);
		DisownLatch(req->latch);

		/* Fetch result */
		shmbuf_len = strlen(req->buf);
		lbuf  = palloc0(PROGRESS_AREA_SIZE);
		if (shmbuf_len == 0) {
			/* We have timed out on PROGRESS_TIMEOUT */
			if (debug)
				elog(LOG, "Did a timeout");

			ProgressPropText(ps, "status", progress_backend_timeout);
		} else {
			/* We have a result computed by the monitored backend */
			memcpy(lbuf, req->buf, shmbuf_len);
			memset(req->buf, 0, PROGRESS_AREA_SIZE);

			appendStringInfoString(ps->str, lbuf);
			ps->disk_size += req->disk_size;
		}

		pfree(lbuf);
	}
}

static
void ProgressScanBlks(ScanState* ss, ProgressState* ps)
{
	HeapScanDesc hsd;
	ParallelHeapScanDesc phsd;
	unsigned int nr_blks;

	if (ss == NULL)
		return;

	hsd = ss->ss_currentScanDesc;
	if (hsd == NULL) {
		return;
	}

	phsd = hsd->rs_parallel;
	if (hsd->rs_parallel != NULL) {
		/* Parallel query */
		if (phsd->phs_nblocks != 0 && phsd->phs_cblock != InvalidBlockNumber) {
			if (phsd->phs_cblock > phsd->phs_startblock)
				nr_blks = phsd->phs_cblock - phsd->phs_startblock;
			else
				nr_blks = phsd->phs_cblock + phsd->phs_nblocks - phsd->phs_startblock;

			ProgressPropLong(ps, "fetched", nr_blks, "blocks");
			ProgressPropLong(ps, "total", phsd->phs_nblocks, "blocks");
			ProgressPropLong(ps, "block", 100 * nr_blks/(phsd->phs_nblocks), "%");
		}
	} else {
		/* Not a parallel query */
		if (hsd->rs_nblocks != 0 && hsd->rs_cblock != InvalidBlockNumber) {
			if (hsd->rs_cblock > hsd->rs_startblock)
				nr_blks = hsd->rs_cblock - hsd->rs_startblock;
			else
				nr_blks = hsd->rs_cblock + hsd->rs_nblocks - hsd->rs_startblock;

	
			ProgressPropLong(ps, "fetched", nr_blks, "blocks");
			ProgressPropLong(ps, "total", hsd->rs_nblocks, "blocks");
			ProgressPropLong(ps, "block", 100 * nr_blks/(hsd->rs_nblocks), "%");
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
		ProgressPropText(ps, "rows scan on", quote_identifier(objectname));
	}
	
	ProgressPropLong(ps, "fetched", (unsigned long) planstate->plan_rows, "rows");
	ProgressPropLong(ps, "total", (unsigned long) plan->plan.plan_rows, "rows");
	ProgressPropLong(ps, "rows", (unsigned short) planstate->percent_done, "rows");
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
		
	ProgressPropLong(ps, "fetched", (long int) ts->tss_TidPtr, "rows");
	ProgressPropLong(ps, "total", (long int) ts->tss_NumTids, "rows");
	ProgressPropLong(ps, "rows", percent, "%");
}

static
void ProgressLimit(LimitState* ls, ProgressState* ps)
{
	if (ls == NULL)
		return;

	if (ls->position == 0) {
		ProgressPropLong(ps, "offset", 0, "%");
		ProgressPropLong(ps, "count", 0, "%");
	}

	if (ls->position > 0 && ls->position <= ls->offset) {
		ProgressPropLong(ps, "offset", (unsigned short)(100 * (ls->position)/(ls->offset)), "%");
		ProgressPropLong(ps, "count", 0, "%");
	}

	if (ls->position > ls->offset) {
		ProgressPropLong(ps, "offset", 100, "%");
		ProgressPropLong(ps, "count", (unsigned short)(100 * (ls->position - ls->offset)/(ls->count)), "%");
	}
}

static
void ProgressCustomScan(CustomScanState* cs, ProgressState* ps)
{
	if (cs == NULL)
		return;

//	if (cs->methods->ProgressCustomScan) {
//		cs->methods->ProgressCustomScan(cs, NULL, ps);
//	}
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

	ProgressPropLong(ps, "fetched", (long int) planstate.plan_rows, "rows");
	ProgressPropLong(ps, "total", (long int) p->plan_rows, "rows");
	ProgressPropLong(ps, "rows", (unsigned short) planstate.percent_done, "%");
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

	ProgressPropLong(ps, "modified", (long int) es->es_processed, "rows");
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

	ProgressPropLong(ps, "hashtable nbatch", hashtable->nbatch, "");

	/*
	 * Display global reads and writes
	 */
	reads = 0;
	writes = 0;
	disk_size = 0;

	for (i = 0; i < hashtable->nbatch; i++) {
		if (hashtable->innerBatchFile[i]) {
			ProgressBufFileRW(hashtable->innerBatchFile[i], ps, &lreads, &lwrites, &ldisk_size);
			reads += lreads;
			writes += lwrites;
			disk_size += ldisk_size;
		}

		if (hashtable->outerBatchFile[i]) {
			ProgressBufFileRW(hashtable->outerBatchFile[i], ps, &lreads, &lwrites, &ldisk_size);
			reads += lreads;
			writes += lwrites;
			disk_size += ldisk_size;
		}
	}

	/* 
	 * Update SQL query wide disk use
  	 */
	ps->disk_size += disk_size;

	ProgressPropLong(ps, "read", reads/1024, "KB");
	ProgressPropLong(ps, "write", writes/1024, "KB");
	ProgressPropLong(ps, "disk used", disk_size/1024, "KB");

	/*
	 * Only display details if requested
	 */ 
	if (ps->verbose == false)
		return;

	if (hashtable->nbatch == 0)
		return;

	ps->indent++;
	for (i = 0; i < hashtable->nbatch; i++) {
		ProgressPropLong(ps, "batch", (long int) i, "");

		if (hashtable->innerBatchFile[i]) {
			ps->indent++;
			ProgressPropText(ps, "group", "inner");
			ProgressBufFile(hashtable->innerBatchFile[i], ps);
			ps->indent--;
		}

		if (hashtable->outerBatchFile[i]) {
			ps->indent++;
			ProgressPropText(ps, "group", "outer");
			ProgressBufFile(hashtable->outerBatchFile[i], ps);
			ps->indent--;
		}
	}

	ps->indent--;
}

static
void ProgressBufFileRW(BufFile* bf, ProgressState* ps,
	unsigned long *reads, unsigned long * writes, unsigned long *disk_size)
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

	ps->indent++;
	ProgressPropLong(ps, "buffile nr files", bfs->numFiles, "");

	if (bfs->numFiles == 0)
		return;

	ProgressPropLong(ps, "disk used", bfs->disk_size/1024,  "KB");

	for (i = 0; i < bfs->numFiles; i++) {
		ps->indent++;
		ProgressPropLong(ps, "file", i, "");
		ProgressPropLong(ps, "read", bfs->bytes_read[i]/1024, "KB");
		ProgressPropLong(ps, "write", bfs->bytes_write[i]/1024, "KB");
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
		ProgressPropLong(ps, "memory write", (long int) tssr.memtupcount, "rows");
		if (tssr.memtupskipped > 0)
			ProgressPropLong(ps, "memory skipped", (long int) tssr.memtupskipped, "rows");

		ProgressPropLong(ps, "memory read", (long int) tssr.memtupread, "rows");
		if (tssr.memtupdeleted)
			ProgressPropLong(ps, "memory deleted", (long int) tssr.memtupread, "rows");
		break;
	
	case TSS_WRITEFILE:
	case TSS_READFILE:
		if (tssr.status == TSS_WRITEFILE)
			ProgressPropText(ps, "file store", "write");
		else 
			ProgressPropText(ps, "file store", "read");

		ProgressPropLong(ps, "readptrcount", tssr.readptrcount, "");
		ProgressPropLong(ps, "write", (long int ) tssr.tuples_count, "rows");
		if (tssr.tuples_skipped)
			ProgressPropLong(ps, "skipped", (long int) tssr.tuples_skipped, "rows");

		ProgressPropLong(ps, "read", (long int) tssr.tuples_read, "rows");
		if (tssr.tuples_deleted)
			ProgressPropLong(ps, "deleted", (long int) tssr.tuples_deleted, "rows");

		ps->disk_size += tssr.disk_size;
		ProgressPropLong(ps, "disk used", tssr.disk_size/2014, "KB");
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
	
	if (tss == NULL)
		return;
	
	oldcontext = MemoryContextSwitchTo(ps->memcontext);
	tsr = tuplesort_get_state(tss);
	MemoryContextSwitchTo(oldcontext);

	switch (tsr->status) {
	case TSS_INITIAL:		/* Loading tuples in mem still within memory limit */
	case TSS_BOUNDED:		/* Loading tuples in mem into bounded-size heap */
		ProgressPropText(ps, "status", "loading tuples in memory");
		ProgressPropLong(ps, "tuples in memory", tsr->memtupcount, "rows");	
		break;

	case TSS_SORTEDINMEM:		/* Sort completed entirely in memory */
		ProgressPropText(ps, "status", "sort completed in memory");
		ProgressPropLong(ps, "tuples in memory", tsr->memtupcount, "rows");	
		break;

	case TSS_BUILDRUNS:		/* Dumping tuples to tape */
		switch (tsr->sub_status) {
		case TSSS_INIT_TAPES:
			ProgressPropText(ps, "status", "on tapes initializing");
			break;

		case TSSS_DUMPING_TUPLES:
			ProgressPropText(ps, "status", "on tapes writing");
			break;

		case TSSS_SORTING_ON_TAPES:
			ProgressPropText(ps, "status", "on tapes sorting");
			break;

		case TSSS_MERGING_TAPES:
			ProgressPropText(ps, "status", "on tapes merging");
			break;
		default:
			;
		};

		dumpTapes(tsr, ps);
		break;
	
	case TSS_FINALMERGE: 		/* Performing final merge on-the-fly */
		ProgressPropText(ps, "status", "on tapes final merge");
		dumpTapes(tsr, ps);	
		break;

	case TSS_SORTEDONTAPE:		/* Sort completed, final run is on tape */
		switch (tsr->sub_status) {
		case TSSS_FETCHING_FROM_TAPES:
			ProgressPropText(ps, "status", "fetching from sorted tapes");
			break;

		case TSSS_FETCHING_FROM_TAPES_WITH_MERGE:
			ProgressPropText(ps, "status", "fetching from sorted tapes with merge");
			break;
		default:
			;
		};

		dumpTapes(tsr, ps);	
		break;

	default:
		ProgressPropText(ps, "status", "unexpected sort state");
	};
}

static
void dumpTapes(struct ts_report* tsr, ProgressState* ps)
{
	int i;
	int percent_effective;

	if (tsr == NULL)
		return;

	if (tsr->tp_write_effective > 0)
		percent_effective = (tsr->tp_read_effective * 100)/tsr->tp_write_effective;
	else 
		percent_effective = 0;

	ProgressPropLong(ps, "merge reads", tsr->tp_read_merge, "rows");
	ProgressPropLong(ps, "merge writes", tsr->tp_write_merge, "rows");
	ProgressPropLong(ps, "effective reads", tsr->tp_read_effective, "rows");
	ProgressPropLong(ps, "effective writes", tsr->tp_write_effective, "rows");
	ProgressPropLong(ps, "effective", percent_effective, "%");
	ProgressPropLong(ps, "tape size", tsr->blocks_alloc, "BLKS");

	/*
	 * Update total disk size used 
	 */
	ps->disk_size += tsr->blocks_alloc * BLCKSZ;

	if (!ps->verbose)
		return;

	/*
	 * Verbose report
	 */
	ProgressPropLong(ps, "tapes total", tsr->maxTapes, "");
	ProgressPropLong(ps, "tapes actives", tsr->activeTapes, "");

	if (tsr->result_tape != -1)
		ProgressPropLong(ps, "tape result", tsr->result_tape, "");

	if (tsr->maxTapes != 0) {
		for (i = 0; i< tsr->maxTapes; i++) {
			ProgressPropLong(ps, "tape idx", i, "");

			ps->indent++;
			if (tsr->tp_fib != NULL)
				ProgressPropLong(ps, "fib", tsr->tp_fib[i], "");

			if (tsr->tp_runs != NULL)
				ProgressPropLong(ps, "runs", tsr->tp_runs[i], "");

			if (tsr->tp_dummy != NULL)
				ProgressPropLong(ps, "dummy", tsr->tp_dummy[i], "");

			if (tsr->tp_read != NULL)
				ProgressPropLong(ps, "read", tsr->tp_read[i], "");

			if (tsr->tp_write)	
				ProgressPropLong(ps, "write", tsr->tp_write[i], "");
				
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

	ProgressPropLong(ps, "time used", INSTR_TIME_GET_MILLISEC(currenttime)/1000, "seconds");
}

static  
void ReportStack(ProgressState* ps)
{
	unsigned long depth;
	unsigned long max_depth;

	depth =	get_stack_depth();
	max_depth = get_max_stack_depth();

	ProgressPropLong(ps, "stack depth", depth, "Bytes");
	ProgressPropLong(ps, "max stack depth", max_depth, "Bytes");
}

static
void ReportDisk(ProgressState*  ps)
{
	unsigned long size;
	char* unit;

	size = ps->disk_size;
	
	if (size < 1024) {
		unit = "B";
	} else if (size >= 1024 && size < 1024 * 1024) {
		unit = "KB";
		size = size / 1024;
	} else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
		unit = "MB";
		size = size / (1024 * 1024);
	} else {
		unit = "GB";
		size = size / (1024 * 1024 * 1024);
	}
		
	ProgressPropLong(ps, "disk used", size, unit);
}
