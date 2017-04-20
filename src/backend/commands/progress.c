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
#include "commands/defrem.h"
#include "commands/report.h"
#include "access/relscan.h"
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

static int log_stmt = 0;		/* log query monitored */
static int debug = 0;

/* 
 * Monitoring progress waits 5secs for monitored backend response.
 *
 * If this timeout is too short, it may not leave enough time for monotored backend to dump its
 * progression about the SQL query it is running.
 *
 * If this timeout is too long, a cancelled SQL query in a backend could block the monitoring
 * backend too for a longi time.
 */
unsigned short PROGRESS_TIMEOUT = 5;


/*
 * One ProgressCtl is allocated for each backend process which is to be potentially monitored
 * The array of progress_ctl structures is protected by ProgressLock global lock.
 *
 * Only one backend can be monitored at a time. This may be improved with a finer granulary
 * using a LWLock tranche of MAX_NR_BACKENDS locks. In which case, one backend can be monitored
 * independantly of the otther backends.
 *
 * The LWLock ensure that one backend can be only monitored by one other backend at a time.
 * Other backends trying to monitor an already monitered backend will be put in queue of the LWWlock.
 */
typedef struct ProgressCtl {
	ReportFormat format; 		/* format of the progress response to be delivered */

	/*
	 * options
	 */
        bool verbose;			/* be verbose */
        bool buffers;			/* print buffer usage */
        bool timing;			/* print detailed node timing */


	char* buf;			/* progress status report in shm */
	struct Latch* latch;		/* Used by requestor to wait for backend to complete its report */
} ProgressCtl;

struct ProgressCtl* progress_ctl_array;	/* Array of MaxBackends ProgressCtl */
char* dump_buf_array;			/* SHMEM buffers one for each backend */
struct Latch* resp_latch_array;		/* Array of MaxBackends latches to synchronize response from 
					 * monitored backend to monitoring backend */

/*
 * No progress request unless requested.
 */
volatile bool progress_requested = false;

/* 
 * get options and tupledesc for result
 */
static void ProgressGetOptions(ProgressCtl* prg, ProgressStmt* stmt, ParseState* pstate);
static TupleDesc ProgressResultDesc(ProgressCtl* prg);

/*
 * local functions
 */
static void ProgressPlan(QueryDesc* query, ReportState* ps);
static void ProgressNode(PlanState* planstate, List* ancestors,
	const char* relationship, const char* plan_name, ReportState* ps);

/*
 * Individual nodes of interest are:
 * - scan data: for heap or index
 * - sort data: for any relation or tuplestore
 * Other nodes only wait on above nodes
 */
static void ProgressScanBlks(ScanState* ss, ReportState* ps);
static void ProgressScanRows(Scan* plan, PlanState* plantstate, ReportState* ps);
static void ProgressTidScan(TidScanState* ts, ReportState* ps);
static void ProgressLimit(LimitState* ls, ReportState* ps);
static void ProgressCustomScan(CustomScanState* cs, ReportState* ps);
static void ProgressIndexScan(IndexScanState* is, ReportState* ps); 
static void ProgressModifyTable(ModifyTableState * planstate, ReportState* ps);
static void ProgressHashJoin(HashJoinState* planstate, ReportState* ps);
static void ProgressHash(HashState* planstate, ReportState* ps);
static void ProgressHashJoinTable(HashJoinTable hashtable, ReportState* ps);
static void ProgressBufFileRW(BufFile* bf, ReportState* ps, unsigned long *reads, unsigned long * writes);
static void ProgressBufFile(BufFile* bf, ReportState* ps);
static void ProgressMaterial(MaterialState* planstate, ReportState* ps);
static void ProgressTupleStore(Tuplestorestate* tss, ReportState* ps);
static void ProgressAgg(AggState* planstate, ReportState* ps);
static void ProgressSort(SortState* ss, ReportState* ps);
static void ProgressTupleSort(Tuplesortstate* tss, ReportState* ps); 
static void dumpTapes(struct ts_report* tsr, ReportState* ps);

static void ReportTime(QueryDesc* query, ReportState* ps);
static void ReportStack(QueryDesc* query, ReportState* ps);
static void ReportRSS(QueryDesc* query, ReportState* ps);
static void ReportDisk(QueryDesc* query, ReportState* ps);



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

	/* Allocated shared latches for response to progress request */
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

	/* Allocate SHMEM buffers for backend communication */
	size = MaxBackends * PROGRESS_AREA_SIZE;
	dump_buf_array = (char*) ShmemInitStruct("Backend Dump Pages", size, &found);
	if (!found) {
        	memset(dump_buf_array, 0, size);
	}

	/* Allocate progress request meta data, one for each backend */
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
			req->format = REPORT_FORMAT_TEXT;	
			req->latch = latch;
			req->buf = dump_buf_array + i * PROGRESS_AREA_SIZE;
			req++;
			latch++;
		}
	}

	return;
}

/*
 * Each backend needs to have its own progress_state
 */
void ProgressBackendInit(void)
{
	//progress_state = CreateReportState(0);
}

void ProgressBackendExit(int code, Datum arg)
{
	//FreeReportState(progress_state);
}

/*
 * ProgressSendRequest:
 * 	Log a request to a backend in order to fetch its progress log
 *	This is initaited by the SQL command: PROGRESS pid.
 */
void ProgressSendRequest(
	ParseState* pstate,
	ProgressStmt *stmt,
	DestReceiver* dest)
{
	BackendId bid;
	ProgressCtl* req;			// Used for the request
	TupOutputState* tstate;
	char* buf;
	MemoryContext local_context;
	MemoryContext old_context;
	char* backend_timeout = "<backend timeout>";

	/* Convert pid to backend_id */
	bid = ProcPidGetBackendId(stmt->pid);
	if (bid == InvalidBackendId) {
		ereport(ERROR, (
       		errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
		errmsg("Invalid backend process pid")));
	}

	if (stmt->pid == getpid()) {
		ereport(ERROR, (
		errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
		errmsg("Cannot request status from self")));
	}

	/* Collect progress state from monitored backend str data */
	local_context =  AllocSetContextCreate(CurrentMemoryContext, "ProgressState", ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(local_context);

	/* Allocate buf for local work */
	buf = palloc0(PROGRESS_AREA_SIZE);
	MemoryContextSwitchTo(old_context);

	/* Serialize signals/request to get the progress state of the query */
	LWLockAcquire(ProgressLock, LW_EXCLUSIVE);

	req = progress_ctl_array + bid;
	ProgressGetOptions(req, stmt, pstate);

	OwnLatch(req->latch);
	ResetLatch(req->latch);

	SendProcSignal(stmt->pid, PROCSIG_PROGRESS, bid);
	WaitLatch(req->latch, WL_LATCH_SET | WL_TIMEOUT , PROGRESS_TIMEOUT * 1000L, WAIT_EVENT_PROGRESS);
	DisownLatch(req->latch);

	/* Fetch result and clear SHM buffer */
	if (strlen(req->buf) == 0) {
		/* We have timed out on PROGRESS_TIMEOUT */
		memcpy(buf, backend_timeout, strlen(backend_timeout));
	} else {
		/* We have a result computed by the monitored backend */
		memcpy(buf, req->buf, strlen(req->buf));
	}
	memset(req->buf, 0, PROGRESS_AREA_SIZE);

	/* End serialization */
	LWLockRelease(ProgressLock);

	/* Send response to client */
	tstate = begin_tup_output_tupdesc(dest, ProgressResultDesc(req));
	if (req->format == REPORT_FORMAT_TEXT)
		do_text_output_multiline(tstate, buf);
	else
		do_text_output_oneline(tstate, buf);

	end_tup_output(tstate);

	MemoryContextDelete(local_context);	// pfree(buf);
}

static void ProgressGetOptions(ProgressCtl* req, ProgressStmt* stmt, ParseState* pstate)
{
	unsigned short result_type;
	ListCell* lc;

	/* default options */
	req->format = REPORT_FORMAT_TEXT;
	req->verbose = 0;
	req->buffers = 0;
	req->timing = 0;

	/*
	 * Check for format option
	 */
	foreach (lc, stmt->options) {
		DefElem* opt = (DefElem*) lfirst(lc);

		if (strcmp(opt->defname, "format") == 0) {
			char* p = defGetString(opt);

			if (strcmp(p, "xml") == 0) {
				result_type = REPORT_FORMAT_XML;
			} else if (strcmp(p, "json") == 0) {
				result_type = REPORT_FORMAT_JSON;
			} else if (strcmp(p, "yaml") == 0) {
				result_type = REPORT_FORMAT_YAML;
			} else if (strcmp(p, "text") == 0) {
				result_type = REPORT_FORMAT_TEXT;
			} else {
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unrecognized value for EXPLAIN option \"%s\": \"%s\"",
					opt->defname, p), parser_errposition(pstate, opt->location)));
			}
			req->format = result_type;
		} else if (strcmp(opt->defname, "verbose") == 0) {
			req->verbose = defGetBoolean(opt);
		} else {
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("unrecognized EXPLAIN option \"%s\"", opt->defname),
					parser_errposition(pstate, opt->location)));
		}
	}
}

static TupleDesc ProgressResultDesc(
	ProgressCtl* prg)
{
	TupleDesc tupdesc;
	Oid result_type = TEXTOID;

	switch(prg->format) {
	case REPORT_FORMAT_XML:
		result_type = XMLOID;
		break;
	case REPORT_FORMAT_JSON:
		result_type = JSONOID;
		break;
	default:
		result_type = TEXTOID;
		/* No YAMLOID */
	}

	/*
	 * Need a tuple descriptor representing a single TEXT or XML column
	 */
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "PLAN PROGRESS", (Oid) result_type, -1, 0);

	return tupdesc;
}

void HandleProgressSignal(void)
{
	progress_requested = true;
	InterruptPending = true;
}

void HandleProgressRequest(void)
{
	ProgressCtl* req;
	ReportState* ps;
	MemoryContext oldcontext;
	MemoryContext progress_context;
	char* shmBufferTooShort = "shm buffer is too small";

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

	progress_context =  AllocSetContextCreate(CurrentMemoryContext, "ReportState", ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(progress_context);

	ps = CreateReportState(0);
	ps->memcontext = progress_context;

	Assert(ps != NULL);
	Assert(ps->str != NULL);
	Assert(ps->str->data != NULL);

	req = progress_ctl_array + MyBackendId;

	/* Clear previous content of ps->str */
	ps->format = req->format;
	ps->verbose = req->verbose;
	ps->buffers = req->buffers;
	ps->timing = req->timing;
	resetStringInfo(ps->str);

	if (MyQueryDesc == NULL) {
		appendStringInfo(ps->str, "<idle backend>\n");
		goto out;
	}

	if (!IsTransactionState()) {
		appendStringInfo(ps->str, "<out of transaction>\n");
		goto out;
	}

	if (MyQueryDesc->plannedstmt == NULL) {
		appendStringInfo(ps->str, "<NULL planned statement>");
		goto out;
	}

	if (MyQueryDesc->plannedstmt->commandType == CMD_UTILITY) {
		appendStringInfo(ps->str, "<utility statement>\n");
		goto out;
	}

	if (log_stmt) {
		appendStringInfo(ps->str, "QUERY: %s", MyQueryDesc->sourceText);
		appendStringInfoChar(ps->str, '\n');
	}

	ReportBeginOutput(ps);
	ProgressPlan(MyQueryDesc, ps);
	ReportTime(MyQueryDesc, ps);
	ReportStack(MyQueryDesc, ps);
	ReportRSS(MyQueryDesc, ps);
	ReportDisk(MyQueryDesc, ps);
	ReportEndOutput(ps);

out:
	/* Dump in SHM the string buffer content */
	if (strlen(ps->str->data) < PROGRESS_AREA_SIZE) {
		/* Mind the '\0' char at the end of the string */
		memcpy(req->buf, ps->str->data, strlen(ps->str->data) + 1); 
	} else {
		memcpy(req->buf, shmBufferTooShort, strlen(shmBufferTooShort));
		elog(LOG, "Needed size for buffer %d", (int) strlen(ps->str->data));
		elog(LOG, "Buffer %s", ps->str->data);
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(ps->memcontext);
	
	SetLatch(req->latch);		// Notify of progress state delivery
	RESUME_INTERRUPTS();
}

static void ProgressPlan(
	QueryDesc* query,
	ReportState* ps)
{
	Bitmapset* rels_used = NULL;
	PlanState* planstate;

	/*
	 * Set up ReportState fields associated with this plan tree
	 */
	Assert(query->plannedstmt != NULL);

	/* Top level tree data */
	ps->plan = query->plannedstmt->planTree;
	ps->planstate = query->planstate;
	ps->es = query->estate;

	ps->pstmt = query->plannedstmt;
	ps->rtable = query->plannedstmt->rtable;

	ReportPreScanNode(query->planstate, &rels_used);

	ps->rtable_names = select_rtable_names_for_explain(ps->rtable, rels_used);
	ps->deparse_cxt = deparse_context_for_plan_rtable(ps->rtable, ps->rtable_names);
	ps->printed_subplans = NULL;

	planstate = query->planstate;
	if (IsA(planstate, GatherState) && ((Gather*) planstate->plan)->invisible) {
		planstate = outerPlanState(planstate);
	}

	ProgressNode(planstate, NIL, NULL, NULL, ps);
}
	
/*
 * This is the main workhorse for collecting query execution progress.
 *
 * planstate is the current execution state in the global execution tree
 * relationship: describes the relationship of this plan state to its parent
 * 	"outer", "inner". It is null at tol level.
 */
static void ProgressNode(
	PlanState* planstate,
	List* ancestors,
	const char* relationship,
	const char* plan_name,
	ReportState* ps)
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

	ReportOpenGroup("Progress", relationship ? NULL : "Progress", true, ps);
	ReportProperties(plan, &info, plan_name, relationship, ps);

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

		if (ps->format == REPORT_FORMAT_TEXT) {
			/*
			 * For historical reasons, the join type is interpolated
			 * into the node type name...
			 */
			if (((Join*) plan)->jointype != JOIN_INNER) {
				appendStringInfo(ps->str, " %s Join", jointype);
			} else if (!IsA(plan, NestLoop)) {
				appendStringInfoString(ps->str, " Join");
			}
		} else {
			ReportPropertyText("Join Type", jointype, ps);
		}

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

		if (ps->format == REPORT_FORMAT_TEXT) {
			appendStringInfo(ps->str, " %s", setopcmd);
		} else {
			ReportPropertyText("Command", setopcmd, ps);
		}

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
		 * Use tuplesortstate 2 times.
		 * Not reflected in child nodes
		 */
		ProgressAgg((AggState*) planstate, ps);
		break;

	case T_WindowAgg:	// ScanState
		// Has a Tuplestorestate (field buffer)
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
		 */
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
	 * In text format, first line ends here
	 */
	ReportNewLine(ps);
	
	/*
	 * Target list
	 */
        if (ps->verbose)
                show_plan_tlist(planstate, ancestors, ps);

	/*
	 * Controls (sort, qual, ...) 
	 */
	show_control_qual(planstate, ancestors, ps);

	/*
	 * Get ready to display the child plans.
	 * Pass current PlanState as head of ancestors list for children
	 */
	haschildren = ReportHasChildren(plan, planstate);
	if (haschildren) {
		ReportOpenGroup("Progress", "Progress", false, ps);
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
	if (planstate->subPlan) {
		ReportSubPlans(planstate->subPlan, ancestors, "SubPlan", ps, ProgressNode);
	}

	/*
	 * end of child plans
	 */
	if (haschildren) {
		ancestors = list_delete_first(ancestors);
		ReportCloseGroup("Progress", "Progress", false, ps);
	}

	/*
	 * in text format, undo whatever indentation we added
	 */
	if (ps->format == REPORT_FORMAT_TEXT) {
		ps->indent = save_indent;
	}
	
	ReportCloseGroup("Progress", relationship ? NULL : "Plan", true, ps);
}


/**********************************************************************************
 * Indivual Progress report functions for the different execution nodes starts here.
 * These functions are leaf function of the Progress tree of functions to be called.
 **********************************************************************************/


/*
 * Monitor progress of commands by page access based on HeapScanDesc
 */
static
void ProgressScanBlks(ScanState* ss, ReportState* ps)
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
		if (phsd->phs_nblocks != 0 && phsd->phs_cblock != InvalidBlockNumber) {
			if (phsd->phs_cblock > phsd->phs_startblock)
				nr_blks = phsd->phs_cblock - phsd->phs_startblock;
			else
				nr_blks = phsd->phs_cblock + phsd->phs_nblocks - phsd->phs_startblock;

			appendStringInfo(ps->str, " blks %u/%u %u%%", 
				nr_blks, phsd->phs_nblocks, 
				100 * nr_blks/(phsd->phs_nblocks));
		}
	} else {
		/* Not a parallel query */
		if (hsd->rs_nblocks != 0 && hsd->rs_cblock != InvalidBlockNumber) {
			if (hsd->rs_cblock > hsd->rs_startblock)
				nr_blks = hsd->rs_cblock - hsd->rs_startblock;
			else
				nr_blks = hsd->rs_cblock + hsd->rs_nblocks - hsd->rs_startblock;

			appendStringInfo(ps->str, " blks %u/%u %u%%", 
				nr_blks, hsd->rs_nblocks,
				100 * nr_blks/(hsd->rs_nblocks));
		}
	}
}

static
void ProgressScanRows(Scan* plan, PlanState* planstate, ReportState* ps)
{
	Index rti;
	RangeTblEntry* rte;
	char* objectname;

	if (plan == NULL)
		return;

	rti = plan->scanrelid; 
	rte = rt_fetch(rti, ps->rtable);

	if (ps->format == REPORT_FORMAT_TEXT)
		appendStringInfo(ps->str, " on");

	objectname = get_rel_name(rte->relid);
	if (objectname != NULL) {
		appendStringInfo(ps->str, " %s", quote_identifier(objectname));
	}

	if (ps->format == REPORT_FORMAT_TEXT) {
		appendStringInfo(ps->str, " => rows %ld/%ld %d%%",
			(long int) planstate->plan_rows,
			(long int) plan->plan.plan_rows,
			(unsigned short) planstate->percent_done);
	}
}

static
void ProgressTidScan(TidScanState* ts, ReportState* ps)
{
	unsigned int percent;

	if (ts == NULL) {
		return;
	}

	if (ts->tss_NumTids == 0)
		percent = 0;
	else 
		percent = (unsigned short)(100 * (ts->tss_TidPtr) / (ts->tss_NumTids));
		
	if (ps->format == REPORT_FORMAT_TEXT) {
		appendStringInfo(ps->str, " => rows %ld/%ld %d%%",
			(long int) ts->tss_TidPtr, (long int) ts->tss_NumTids, percent);
	}
}

static
void ProgressLimit(LimitState* ls, ReportState* ps)
{
	if (ls == NULL)
		return;

	if (ps->format == REPORT_FORMAT_TEXT) {
		if (ls->position == 0) {
			appendStringInfoSpaces(ps->str, ps->indent * 2);
			appendStringInfo(ps->str, " => offset 0%% limit 0%%");
			return;
		}

		if (ls->position > 0 && ls->position <= ls->offset) {
			appendStringInfoSpaces(ps->str, ps->indent * 2);
			appendStringInfo(ps->str, " => offset %d%% limit 0%%",
				(unsigned short)(100 * (ls->position)/(ls->offset)));
			return;
		}

		if (ls->position > ls->offset) {
			appendStringInfoSpaces(ps->str, ps->indent * 2);
			appendStringInfo(ps->str, " => offset 100%% limit %d%%",
				(unsigned short)(100 * (ls->position - ls->offset)/(ls->count)));
			return;
		}
	}
}

static
void ProgressCustomScan(CustomScanState* cs, ReportState* ps)
{
	if (cs == NULL)
		return;

	if (cs->methods->ProgressCustomScan) {
		cs->methods->ProgressCustomScan(cs, NULL, ps);
	}
}

static
void ProgressIndexScan(IndexScanState* is, ReportState* ps) 
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

	if (ps->format == REPORT_FORMAT_TEXT) {
		appendStringInfo(ps->str, " => rows %ld/%ld %d%%",
			(long int) planstate.plan_rows,
			(long int) p->plan_rows,
			(unsigned short) planstate.percent_done);
	}
}

static
void ProgressModifyTable(ModifyTableState *mts, ReportState* ps)
{
	EState* es;

	if (mts == NULL)
		return;

	es = mts->ps.state;
	if (es == NULL)
		return;

	if (ps->format == REPORT_FORMAT_TEXT) {
		appendStringInfo(ps->str, " => rows %ld", (long int) es->es_processed);
	}
}

static
void ProgressHash(HashState* hs, ReportState* ps)
{
	if (hs == NULL)
		return;
	
	ProgressHashJoinTable((HashJoinTable) hs->hashtable, ps);
}

static
void ProgressHashJoin(HashJoinState* hjs, ReportState* ps)
{
	if (hjs == NULL)
		return;

	ProgressHashJoinTable((HashJoinTable) hjs->hj_HashTable, ps);
}

/*
 * HashJoinTable is not a node type
 */
static
void ProgressHashJoinTable(HashJoinTable hashtable, ReportState* ps)
{
	int i;
	unsigned long reads;
	unsigned long writes;
	unsigned long lreads;
	unsigned long lwrites;

	/*
	 * Could be used but not yet allocated
	 */
	if (hashtable == NULL)
		return;
		
	if (hashtable->nbatch <= 1)
		return;

	appendStringInfo(ps->str, " hashtable nbatch %d", hashtable->nbatch);

	/*
	 * Display global reads and writes
	 */
	reads = 0;
	writes = 0;
	for (i = 0; i < hashtable->nbatch; i++) {
		if (hashtable->innerBatchFile[i]) {
			ProgressBufFileRW(hashtable->innerBatchFile[i], ps, &lreads, &lwrites);
			reads += lreads;
			writes += lwrites;
		}

		if (hashtable->outerBatchFile[i]) {
			ProgressBufFileRW(hashtable->outerBatchFile[i], ps, &lreads, &lwrites);
			reads += lreads;
			writes += lwrites;
		}
	}

	appendStringInfo(ps->str, " kbytes read/write %ld/%ld", reads/1024, writes/1024);

	/*
	 * Only display details if requested
	 */ 
	if (ps->verbose == false)
		return;

	ps->indent++;
	for (i = 0; i < hashtable->nbatch; i++) {
		appendStringInfoSpaces(ps->str, ps->indent * 2);	
		appendStringInfo(ps->str, "batch %d\n", i);
		if (hashtable->innerBatchFile[i]) {
			ps->indent++;
			appendStringInfoSpaces(ps->str, ps->indent * 2);	
			appendStringInfo(ps->str, "inner ");
			ProgressBufFile(hashtable->innerBatchFile[i], ps);
			ps->indent--;
		}

		if (hashtable->outerBatchFile[i]) {
			ps->indent++;
			appendStringInfoSpaces(ps->str, ps->indent * 2);	
			appendStringInfo(ps->str, "outer ");
			ProgressBufFile(hashtable->outerBatchFile[i], ps);
			ps->indent--;
		}
	}
	ps->indent--;
}

static
void ProgressBufFileRW(BufFile* bf, ReportState* ps,
	unsigned long *reads, unsigned long * writes)
{
	MemoryContext oldcontext;
	struct buffile_state* bfs;
	int i;

	if (bf == NULL)
		return;

	*reads = 0;
	*writes = 0;

	oldcontext = MemoryContextSwitchTo(ps->memcontext);
	bfs = BufFileState(bf);
	MemoryContextSwitchTo(oldcontext);
	
	for (i = 0; i < bfs->numFiles; i++) {
		*reads += bfs->bytes_read[i];
		*writes += bfs->bytes_write[i];
	}
}
	
static
void ProgressBufFile(BufFile* bf, ReportState* ps)
{
	int i;
	struct buffile_state* bfs;
	MemoryContext oldcontext;
	
	if (bf == NULL)
		return;

        oldcontext = MemoryContextSwitchTo(ps->memcontext);
	bfs = BufFileState(bf);
	MemoryContextSwitchTo(oldcontext);
	
	appendStringInfo(ps->str, "buffile with %d files\n", bfs->numFiles);
	ps->indent++;
	for (i = 0; i < bfs->numFiles; i++) {
		appendStringInfoSpaces(ps->str, ps->indent * 2);	
		appendStringInfo(ps->str, "file %d r/w (kbytes) %d/%d\n", 
			i, bfs->bytes_read[i]/1024, bfs->bytes_write[i]/1024);
	}	
	ps->indent--;
}

static
void ProgressMaterial(MaterialState* planstate, ReportState* ps)
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
void ProgressTupleStore(Tuplestorestate* tss, ReportState* ps)
{
	struct tss_report tssr;

	if (tss == NULL)
		return;

	tuplestore_get_state(tss, &tssr);

	switch (tssr.status) {
	case TSS_INMEM:
		appendStringInfo(ps->str, " => memory tuples write=%ld",
			(long int) tssr.memtupcount);
		if (tssr.memtupskipped > 0) {
			appendStringInfo(ps->str, " skipped=%ld", (long int) tssr.memtupskipped);
		}

		appendStringInfo(ps->str, " read=%ld", (long int) tssr.memtupread);
		if (tssr.memtupdeleted) {
			appendStringInfo(ps->str, " deleted=%ld", (long int) tssr.memtupread);
		}

		appendStringInfo(ps->str, ")");
		break;
	
	case TSS_WRITEFILE:
	case TSS_READFILE:
		appendStringInfo(ps->str, " => file");
		if (tssr.status == TSS_WRITEFILE)
			appendStringInfo(ps->str, " write");
		else 
			appendStringInfo(ps->str, " read");

		appendStringInfo(ps->str, " readptrcount=%d", tssr.readptrcount);
		appendStringInfo(ps->str, " rows (write=%ld", (long int ) tssr.tuples_count);
		if (tssr.tuples_skipped) {
			appendStringInfo(ps->str, " skipped=%ld", (long int) tssr.tuples_skipped);
		}

		appendStringInfo(ps->str, " read=%ld", (long int) tssr.tuples_read);
		if (tssr.tuples_deleted) {
			appendStringInfo(ps->str, " deleted=%ld", (long int ) tssr.tuples_deleted);
		}

		appendStringInfo(ps->str, ")");
		break;

	default:
		break;
	}
}

static
void ProgressAgg(AggState* planstate, ReportState* ps)
{
	if (planstate == NULL)
		return;

	ProgressTupleSort(planstate->sort_in, ps);
	ProgressTupleSort(planstate->sort_out, ps);
}

static
void ProgressSort(SortState* ss, ReportState* ps)
{
	Assert(nodeTag(ss) == T_SortState);

	if (ss == NULL)
		return;

	if (ss->tuplesortstate == NULL)
		return;

	ProgressTupleSort(ss->tuplesortstate, ps);
}

static
void ProgressTupleSort(Tuplesortstate* tss, ReportState* ps)
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
		appendStringInfo(ps->str, "=> loading tuples in memory %d",
			tsr->memtupcount);
		break;

	case TSS_SORTEDINMEM:		/* Sort completed entirely in memory */
		appendStringInfo(ps->str, "=> sort completed in memory %d",
			tsr->memtupcount);
		break;

	case TSS_BUILDRUNS:		/* Dumping tuples to tape */
		appendStringInfo(ps->str, "=> dumping tuples to tapes");
		switch (tsr->sub_status) {
		case TSSS_INIT_TAPES:
			appendStringInfo(ps->str, " / init tapes");
			break;

		case TSSS_DUMPING_TUPLES:
			appendStringInfo(ps->str, " / dumping tuples");
			break;

		case TSSS_SORTING_ON_TAPES:
			appendStringInfo(ps->str, " / sorting on tapes");
			break;

		case TSSS_MERGING_TAPES:
			appendStringInfo(ps->str, " / merging tapes");
			break;
		default:
			;
		};

		appendStringInfo(ps->str, "\n");
		dumpTapes(tsr, ps);
		break;
	
	case TSS_FINALMERGE: 		/* Performing final merge on-the-fly */
		appendStringInfo(ps->str, "=> final merge sort on tapes\n");
		dumpTapes(tsr, ps);	
		break;

	case TSS_SORTEDONTAPE:		/* Sort completed, final run is on tape */
		appendStringInfo(ps->str, "=> sort completed on tape");
		switch (tsr->sub_status) {
		case TSSS_FETCHING_FROM_TAPES:
			appendStringInfo(ps->str, " / fetching from tapes");
			break;

		case TSSS_FETCHING_FROM_TAPES_WITH_MERGE:
			appendStringInfo(ps->str, " / fetching from tapes with merge");
			break;
		default:
			;
		};

		appendStringInfo(ps->str, "\n");
		dumpTapes(tsr, ps);	
		break;

	default:
		appendStringInfo(ps->str, "=> unexpected sort state\n");
	};
}

static
void dumpTapes(struct ts_report* tsr, ReportState* ps)
{
	int i;
	int percent_effective;

	if (tsr == NULL)
		return;

	if (ps->verbose) {
		appendStringInfoSpaces(ps->str, ps->indent * 2);
		appendStringInfo(ps->str, ": total=%d actives=%d",
			tsr->maxTapes, tsr->activeTapes); 

		if (tsr->result_tape != -1)
			appendStringInfo(ps->str, " result=%d", tsr->result_tape);

		appendStringInfo(ps->str, "\n");

		for (i = 0; i< tsr->maxTapes; i++) {
			appendStringInfoSpaces(ps->str, ps->indent * 2);
			appendStringInfo(ps->str, "  -> tape %d: %d %d  %d %d %d\n",	
				i, tsr->tp_fib[i], tsr->tp_runs[i], tsr->tp_dummy[i],
				tsr->tp_read[i], tsr->tp_write[i]);
		}
	}

	appendStringInfoSpaces(ps->str, ps->indent * 2);

	if (tsr->tp_write_effective > 0)
		percent_effective = (tsr->tp_read_effective * 100)/tsr->tp_write_effective;
	else 
		percent_effective = 0;

	appendStringInfo(ps->str, "rows r/w merge %d/%d rows r/w effective %d/%d %d%%",
		tsr->tp_read_merge, tsr->tp_write_merge,
		tsr->tp_read_effective, tsr->tp_write_effective,
		percent_effective);
}

static
void ReportTime(QueryDesc* query, ReportState* ps)
{
	instr_time currenttime;

	if (query == NULL)
		return;

	if (query->totaltime == NULL)
		return;

	INSTR_TIME_SET_CURRENT(currenttime);
	INSTR_TIME_SUBTRACT(currenttime, query->totaltime->starttime);

	appendStringInfo(ps->str, "time used (s): %.0g\n",  INSTR_TIME_GET_MILLISEC(currenttime)/1000);
}

static  
void ReportStack(QueryDesc* query, ReportState* ps)
{

}

static 
void ReportRSS(QueryDesc* query , ReportState*  ps)
{

}

static
void ReportDisk(QueryDesc* query, ReportState*  ps)
{

}

