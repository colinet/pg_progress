/*-------------------------------------------------------------------------
 *
 * explain.c
 *	  Explain query execution plans
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/prepare.h"
#include "commands/report.h"
#include "executor/hashjoin.h"
#include "foreign/fdwapi.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"
#include "utils/xml.h"


/* Hook for plugins to get control in ExplainOneQuery() */
ExplainOneQuery_hook_type ExplainOneQuery_hook = NULL;

/* Hook for plugins to get control in explain_get_index_name() */
explain_get_index_name_hook_type explain_get_index_name_hook = NULL;

static void ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
       ReportState *es, const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv);
static void report_triggers(ResultRelInfo *rInfo, bool show_relname, ReportState *es);

static double elapsed_time(instr_time *starttime);
static void ExplainNode(PlanState *planstate, List *ancestors, const char *relationship, const char *plan_name, ReportState *es);
static void ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir, ReportState *es);
static void ExplainScanTarget(Scan *plan, ReportState *es);
static void ExplainModifyTarget(ModifyTable *plan, ReportState *es);
static void ExplainTargetRel(Plan *plan, Index rti, ReportState *es);



/*
 * ExplainQuery -
 *	  execute an EXPLAIN command
 */
void
ExplainQuery(ParseState *pstate, ExplainStmt *stmt, const char *queryString,
			 ParamListInfo params, QueryEnvironment *queryEnv,
			 DestReceiver *dest)
{
	ReportState *es = CreateReportState(0);
	TupOutputState *tstate;
	List	   *rewritten;
	ListCell   *lc;
	bool		timing_set = false;
	bool		summary_set = false;

	SetReportStateCosts(es, true);

	/* Parse options list. */
	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "analyze") == 0)
			es->analyze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "verbose") == 0)
			es->verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "costs") == 0)
			es->costs = defGetBoolean(opt);
		else if (strcmp(opt->defname, "buffers") == 0)
			es->buffers = defGetBoolean(opt);
		else if (strcmp(opt->defname, "timing") == 0)
		{
			timing_set = true;
			es->timing = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "summary") == 0)
		{
			summary_set = true;
			es->summary = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "format") == 0)
		{
			char	   *p = defGetString(opt);

			if (strcmp(p, "text") == 0)
				es->format = REPORT_FORMAT_TEXT;
			else if (strcmp(p, "xml") == 0)
				es->format = REPORT_FORMAT_XML;
			else if (strcmp(p, "json") == 0)
				es->format = REPORT_FORMAT_JSON;
			else if (strcmp(p, "yaml") == 0)
				es->format = REPORT_FORMAT_YAML;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("unrecognized value for EXPLAIN option \"%s\": \"%s\"",
					   opt->defname, p),
						 parser_errposition(pstate, opt->location)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized EXPLAIN option \"%s\"",
							opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	if (es->buffers && !es->analyze)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("EXPLAIN option BUFFERS requires ANALYZE")));

	/* if the timing was not set explicitly, set default value */
	es->timing = (timing_set) ? es->timing : es->analyze;

	/* check that timing is used with EXPLAIN ANALYZE */
	if (es->timing && !es->analyze)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("EXPLAIN option TIMING requires ANALYZE")));

	/* if the summary was not set explicitly, set default value */
	es->summary = (summary_set) ? es->summary : es->analyze;

	/*
	 * Parse analysis was done already, but we still have to run the rule
	 * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
	 * came straight from the parser, or suitable locks were acquired by
	 * plancache.c.
	 *
	 * Because the rewriter and planner tend to scribble on the input, we make
	 * a preliminary copy of the source querytree.  This prevents problems in
	 * the case that the EXPLAIN is in a portal or plpgsql function and is
	 * executed repeatedly.  (See also the same hack in DECLARE CURSOR and
	 * PREPARE.)  XXX FIXME someday.
	 */
	rewritten = QueryRewrite(castNode(Query, copyObject(stmt->query)));

	/* emit opening boilerplate */
	ReportBeginOutput(es);

	if (rewritten == NIL)
	{
		/*
		 * In the case of an INSTEAD NOTHING, tell at least that.  But in
		 * non-text format, the output is delimited, so this isn't necessary.
		 */
		if (es->format == REPORT_FORMAT_TEXT)
			appendStringInfoString(es->str, "Query rewrites to nothing\n");
	}
	else
	{
		ListCell   *l;

		/* Explain every plan */
		foreach(l, rewritten)
		{
			ExplainOneQuery(lfirst_node(Query, l),
							CURSOR_OPT_PARALLEL_OK, NULL, es,
							queryString, params, queryEnv);

			/* Separate plans with an appropriate separator */
			if (lnext(l) != NULL)
				ReportSeparatePlans(es);
		}
	}

	/* emit closing boilerplate */
	ReportEndOutput(es);
	Assert(es->indent == 0);

	/* output tuples */
	tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc(stmt));
	if (es->format == REPORT_FORMAT_TEXT)
		do_text_output_multiline(tstate, es->str->data);
	else
		do_text_output_oneline(tstate, es->str->data);
	end_tup_output(tstate);

	pfree(es->str->data);
}

/*
 * ExplainResultDesc -
 *	  construct the result tupledesc for an EXPLAIN
 */
TupleDesc
ExplainResultDesc(ExplainStmt *stmt)
{
	TupleDesc	tupdesc;
	ListCell   *lc;
	Oid		result_type = TEXTOID;

	/* Check for XML format option */
	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "format") == 0) {
			char* p = defGetString(opt);

			if (strcmp(p, "xml") == 0)
				result_type = XMLOID;
			else if (strcmp(p, "json") == 0)
				result_type = JSONOID;
			else
				result_type = TEXTOID;
			/* don't "break", as ExplainQuery will use the last value */
		}
	}

	/* Need a tuple descriptor representing a single TEXT or XML column */
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "QUERY PLAN", result_type, -1, 0);

	return tupdesc;
}

/*
 * ExplainOneQuery -
 *	  print out the execution plan for one Query
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 */
static void
ExplainOneQuery(Query *query, int cursorOptions,
				IntoClause *into, ReportState *es,
				const char *queryString, ParamListInfo params,
				QueryEnvironment *queryEnv)
{
	/* planner will not cope with utility statements */
	if (query->commandType == CMD_UTILITY)
	{
		ExplainOneUtility(query->utilityStmt, into, es, queryString, params,
						  queryEnv);
		return;
	}

	/* if an advisor plugin is present, let it manage things */
	if (ExplainOneQuery_hook)
		(*ExplainOneQuery_hook) (query, cursorOptions, into, es,
								 queryString, params);
	else
	{
		PlannedStmt *plan;
		instr_time	planstart,
					planduration;

		INSTR_TIME_SET_CURRENT(planstart);

		/* plan the query */
		plan = pg_plan_query(query, cursorOptions, params);

		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planstart);

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
					   &planduration);
	}
}

/*
 * ExplainOneUtility -
 *	  print out the execution plan for one utility statement
 *	  (In general, utility statements don't have plans, but there are some
 *	  we treat as special cases)
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case.
 */
void
ExplainOneUtility(Node *utilityStmt, IntoClause *into, ReportState *es,
				  const char *queryString, ParamListInfo params,
				  QueryEnvironment *queryEnv)
{
	if (utilityStmt == NULL)
		return;

	if (IsA(utilityStmt, CreateTableAsStmt))
	{
		/*
		 * We have to rewrite the contained SELECT and then pass it back to
		 * ExplainOneQuery.  It's probably not really necessary to copy the
		 * contained parsetree another time, but let's be safe.
		 *
		 * Like ExecCreateTableAs, disallow parallelism in the plan.
		 */
		CreateTableAsStmt *ctas = (CreateTableAsStmt *) utilityStmt;
		List	   *rewritten;

		rewritten = QueryRewrite(castNode(Query, copyObject(ctas->query)));
		Assert(list_length(rewritten) == 1);
		ExplainOneQuery(linitial_node(Query, rewritten),
						0, ctas->into, es,
						queryString, params, queryEnv);
	}
	else if (IsA(utilityStmt, DeclareCursorStmt))
	{
		/*
		 * Likewise for DECLARE CURSOR.
		 *
		 * Notice that if you say EXPLAIN ANALYZE DECLARE CURSOR then we'll
		 * actually run the query.  This is different from pre-8.3 behavior
		 * but seems more useful than not running the query.  No cursor will
		 * be created, however.
		 */
		DeclareCursorStmt *dcs = (DeclareCursorStmt *) utilityStmt;
		List	   *rewritten;

		rewritten = QueryRewrite(castNode(Query, copyObject(dcs->query)));
		Assert(list_length(rewritten) == 1);
		ExplainOneQuery(linitial_node(Query, rewritten),
						dcs->options, NULL, es,
						queryString, params, queryEnv);
	}
	else if (IsA(utilityStmt, ExecuteStmt))
		ExplainExecuteQuery((ExecuteStmt *) utilityStmt, into, es,
							queryString, params, queryEnv);
	else if (IsA(utilityStmt, NotifyStmt))
	{
		if (es->format == REPORT_FORMAT_TEXT)
			appendStringInfoString(es->str, "NOTIFY\n");
		else
			ReportDummyGroup("Notify", NULL, es);
	}
	else
	{
		if (es->format == REPORT_FORMAT_TEXT)
			appendStringInfoString(es->str,
							  "Utility statements have no plan structure\n");
		else
			ReportDummyGroup("Utility Statement", NULL, es);
	}
}

/*
 * ExplainOnePlan -
 *		given a planned query, execute it if needed, and then print
 *		EXPLAIN output
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt,
 * in which case executing the query should result in creating that table.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case, and because an index advisor plugin would need
 * to call it.
 */
void
ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ReportState *es,
			   const char *queryString, ParamListInfo params,
			   QueryEnvironment *queryEnv, const instr_time *planduration)
{
	DestReceiver *dest;
	QueryDesc  *queryDesc;
	instr_time	starttime;
	double		totaltime = 0;
	int			eflags;
	int			instrument_option = 0;

	Assert(plannedstmt->commandType != CMD_UTILITY);

	if (es->analyze && es->timing)
		instrument_option |= INSTRUMENT_TIMER;
	else if (es->analyze)
		instrument_option |= INSTRUMENT_ROWS;

	if (es->buffers)
		instrument_option |= INSTRUMENT_BUFFERS;

	/*
	 * We always collect timing for the entire statement, even when node-level
	 * timing is off, so we don't look at es->timing here.  (We could skip
	 * this if !es->summary, but it's hardly worth the complication.)
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/*
	 * Normally we discard the query's output, but if explaining CREATE TABLE
	 * AS, we'd better use the appropriate tuple receiver.
	 */
	if (into)
		dest = CreateIntoRelDestReceiver(into);
	else
		dest = None_Receiver;

	/* Create a QueryDesc for the query */
	queryDesc = CreateQueryDesc(plannedstmt, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, instrument_option);

	/* Select execution options */
	if (es->analyze)
		eflags = 0;				/* default run-to-completion flags */
	else
		eflags = EXEC_FLAG_EXPLAIN_ONLY;
	if (into)
		eflags |= GetIntoRelEFlags(into);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, eflags);

	/* Execute the plan for statistics if asked for */
	if (es->analyze)
	{
		ScanDirection dir;

		/* EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird */
		if (into && into->skipData)
			dir = NoMovementScanDirection;
		else
			dir = ForwardScanDirection;

		/* run the plan */
		ExecutorRun(queryDesc, dir, 0L, true);

		/* run cleanup too */
		ExecutorFinish(queryDesc);

		/* We can't run ExecutorEnd 'till we're done printing the stats... */
		totaltime += elapsed_time(&starttime);
	}

	ReportOpenGroup("Query", NULL, true, es);

	/* Create textual dump of plan tree */
	ExplainPrintPlan(es, queryDesc);

	if (es->summary && planduration)
	{
		double		plantime = INSTR_TIME_GET_DOUBLE(*planduration);

		if (es->format == REPORT_FORMAT_TEXT)
			appendStringInfo(es->str, "Planning time: %.3f ms\n",
							 1000.0 * plantime);
		else
			ReportPropertyFloat("Planning Time", 1000.0 * plantime, 3, es);
	}

	/* Print info about runtime of triggers */
	if (es->analyze)
		ExplainPrintTriggers(es, queryDesc);

	/*
	 * Close down the query and free resources.  Include time for this in the
	 * total execution time (although it should be pretty minimal).
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	/* We need a CCI just in case query expanded to multiple plans */
	if (es->analyze)
		CommandCounterIncrement();

	totaltime += elapsed_time(&starttime);

	/*
	 * We only report execution time if we actually ran the query (that is,
	 * the user specified ANALYZE), and if summary reporting is enabled (the
	 * user can set SUMMARY OFF to not have the timing information included in
	 * the output).  By default, ANALYZE sets SUMMARY to true.
	 */
	if (es->summary && es->analyze)
	{
		if (es->format == REPORT_FORMAT_TEXT)
			appendStringInfo(es->str, "Execution time: %.3f ms\n",
							 1000.0 * totaltime);
		else
			ReportPropertyFloat("Execution Time", 1000.0 * totaltime,
								 3, es);
	}

	ReportCloseGroup("Query", NULL, true, es);
}

/*
 * ExplainPrintPlan -
 *	  convert a QueryDesc's plan tree to text and append it to es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Also, output formatting state
 * such as the indent level is assumed valid.  Plan-tree-specific fields
 * in *es are initialized here.
 *
 * NB: will not work on utility statements
 */
void
ExplainPrintPlan(ReportState *es, QueryDesc *queryDesc)
{
	Bitmapset  *rels_used = NULL;
	PlanState  *ps;

	/* Set up ReportState fields associated with this plan tree */
	Assert(queryDesc->plannedstmt != NULL);
	es->pstmt = queryDesc->plannedstmt;
	es->rtable = queryDesc->plannedstmt->rtable;

	ReportPreScanNode(queryDesc->planstate, &rels_used);

	es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
	es->deparse_cxt = deparse_context_for_plan_rtable(es->rtable, es->rtable_names);
	es->printed_subplans = NULL;

	/*
	 * Sometimes we mark a Gather node as "invisible", which means that it's
	 * not displayed in EXPLAIN output.  The purpose of this is to allow
	 * running regression tests with force_parallel_mode=regress to get the
	 * same results as running the same tests with force_parallel_mode=off.
	 */
	ps = queryDesc->planstate;
	if (IsA(ps, GatherState) &&((Gather *) ps->plan)->invisible)
		ps = outerPlanState(ps);
	ExplainNode(ps, NIL, NULL, NULL, es);
}

/*
 * ExplainPrintTriggers -
 *	  convert a QueryDesc's trigger statistics to text and append it to
 *	  es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Other fields in *es are
 * initialized here.
 */
void
ExplainPrintTriggers(ReportState *es, QueryDesc *queryDesc)
{
	ResultRelInfo *rInfo;
	bool		show_relname;
	int			numrels = queryDesc->estate->es_num_result_relations;
	List	   *targrels = queryDesc->estate->es_trig_target_relations;
	int			nr;
	ListCell   *l;

	ReportOpenGroup("Triggers", "Triggers", false, es);

	show_relname = (numrels > 1 || targrels != NIL);
	rInfo = queryDesc->estate->es_result_relations;
	for (nr = 0; nr < numrels; rInfo++, nr++)
		report_triggers(rInfo, show_relname, es);

	foreach(l, targrels)
	{
		rInfo = (ResultRelInfo *) lfirst(l);
		report_triggers(rInfo, show_relname, es);
	}

	ReportCloseGroup("Triggers", "Triggers", false, es);
}

/*
 * report_triggers -
 *		report execution stats for a single relation's triggers
 */
static void
report_triggers(ResultRelInfo *rInfo, bool show_relname, ReportState *es)
{
	int			nt;

	if (!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument)
		return;
	for (nt = 0; nt < rInfo->ri_TrigDesc->numtriggers; nt++)
	{
		Trigger    *trig = rInfo->ri_TrigDesc->triggers + nt;
		Instrumentation *instr = rInfo->ri_TrigInstrument + nt;
		char	   *relname;
		char	   *conname = NULL;

		/* Must clean up instrumentation state */
		InstrEndLoop(instr);

		/*
		 * We ignore triggers that were never invoked; they likely aren't
		 * relevant to the current query type.
		 */
		if (instr->ntuples == 0)
			continue;

		ReportOpenGroup("Trigger", NULL, true, es);

		relname = RelationGetRelationName(rInfo->ri_RelationDesc);
		if (OidIsValid(trig->tgconstraint))
			conname = get_constraint_name(trig->tgconstraint);

		/*
		 * In text format, we avoid printing both the trigger name and the
		 * constraint name unless VERBOSE is specified.  In non-text formats
		 * we just print everything.
		 */
		if (es->format == REPORT_FORMAT_TEXT)
		{
			if (es->verbose || conname == NULL)
				appendStringInfo(es->str, "Trigger %s", trig->tgname);
			else
				appendStringInfoString(es->str, "Trigger");
			if (conname)
				appendStringInfo(es->str, " for constraint %s", conname);
			if (show_relname)
				appendStringInfo(es->str, " on %s", relname);
			if (es->timing)
				appendStringInfo(es->str, ": time=%.3f calls=%.0f\n",
								 1000.0 * instr->total, instr->ntuples);
			else
				appendStringInfo(es->str, ": calls=%.0f\n", instr->ntuples);
		}
		else
		{
			ReportPropertyText("Trigger Name", trig->tgname, es);
			if (conname)
				ReportPropertyText("Constraint Name", conname, es);
			ReportPropertyText("Relation", relname, es);
			if (es->timing)
				ReportPropertyFloat("Time", 1000.0 * instr->total, 3, es);
			ReportPropertyFloat("Calls", instr->ntuples, 0, es);
		}

		if (conname)
			pfree(conname);

		ReportCloseGroup("Trigger", NULL, true, es);
	}
}

/* Compute elapsed time in seconds since given timestamp */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_DOUBLE(endtime);
}

/*
 * ExplainNode -
 *	  Appends a description of a plan tree to es->str
 *
 * planstate points to the executor state node for the current plan node.
 * We need to work from a PlanState node, not just a Plan node, in order to
 * get at the instrumentation data (if any) as well as the list of subplans.
 *
 * ancestors is a list of parent PlanState nodes, most-closely-nested first.
 * These are needed in order to interpret PARAM_EXEC Params.
 *
 * relationship describes the relationship of this plan node to its parent
 * (eg, "Outer", "Inner"); it can be null at top level.  plan_name is an
 * optional name to be attached to the node.
 *
 * In text format, es->indent is controlled in this function since we only
 * want it to change at plan-node boundaries.  In non-text formats, es->indent
 * corresponds to the nesting depth of logical output groups, and therefore
 * is controlled by ReportOpenGroup/ReportCloseGroup.
 */
static void
ExplainNode(PlanState *planstate, List *ancestors,
			const char *relationship, const char *plan_name,
			ReportState *es)
{
	Plan	   *plan = planstate->plan;

	PlanInfo info;
	int     save_indent = es->indent;
	bool    haschildren;
	int ret;

	ret = planNodeInfo(plan, &info);
	if (ret != 0) {
		elog(LOG, "unknwon node type for plan");
	}

	ReportOpenGroup("Plan", relationship ? NULL : "Plan", true, es);
	ReportProperties(plan, &info, plan_name, relationship, es);

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
			ExplainScanTarget((Scan *) plan, es);
			break;
		case T_ForeignScan:
		case T_CustomScan:
			if (((Scan *) plan)->scanrelid > 0)
				ExplainScanTarget((Scan *) plan, es);
			break;
		case T_IndexScan:
			{
				IndexScan  *indexscan = (IndexScan *) plan;

				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexscan, es);
			}
			break;
		case T_IndexOnlyScan:
			{
				IndexOnlyScan *indexonlyscan = (IndexOnlyScan *) plan;

				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexonlyscan, es);
			}
			break;
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmapindexscan = (BitmapIndexScan *) plan;
				const char *indexname =
				explain_get_index_name(bitmapindexscan->indexid);

				if (es->format == REPORT_FORMAT_TEXT)
					appendStringInfo(es->str, " on %s", indexname);
				else
					ReportPropertyText("Index Name", indexname, es);
			}
			break;
		case T_ModifyTable:
			ExplainModifyTarget((ModifyTable *) plan, es);
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				const char *jointype;

				switch (((Join *) plan)->jointype)
				{
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
				if (es->format == REPORT_FORMAT_TEXT)
				{
					/*
					 * For historical reasons, the join type is interpolated
					 * into the node type name...
					 */
					if (((Join *) plan)->jointype != JOIN_INNER)
						appendStringInfo(es->str, " %s Join", jointype);
					else if (!IsA(plan, NestLoop))
						appendStringInfoString(es->str, " Join");
				}
				else
					ReportPropertyText("Join Type", jointype, es);
			}
			break;
		case T_SetOp:
			{
				const char *setopcmd;

				switch (((SetOp *) plan)->cmd)
				{
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
				if (es->format == REPORT_FORMAT_TEXT)
					appendStringInfo(es->str, " %s", setopcmd);
				else
					ReportPropertyText("Command", setopcmd, es);
			}
			break;
		default:
			break;
	}

	if (es->costs)
	{
		if (es->format == REPORT_FORMAT_TEXT)
		{
			appendStringInfo(es->str, "  (cost=%.2f..%.2f rows=%.0f width=%d)",
							 plan->startup_cost, plan->total_cost,
							 plan->plan_rows, plan->plan_width);
		}
		else
		{
			ReportPropertyFloat("Startup Cost", plan->startup_cost, 2, es);
			ReportPropertyFloat("Total Cost", plan->total_cost, 2, es);
			ReportPropertyFloat("Plan Rows", plan->plan_rows, 0, es);
			ReportPropertyInteger("Plan Width", plan->plan_width, es);
		}
	}

	/*
	 * We have to forcibly clean up the instrumentation state because we
	 * haven't done ExecutorEnd yet.  This is pretty grotty ...
	 *
	 * Note: contrib/auto_explain could cause instrumentation to be set up
	 * even though we didn't ask for it here.  Be careful not to print any
	 * instrumentation results the user didn't ask for.  But we do the
	 * InstrEndLoop call anyway, if possible, to reduce the number of cases
	 * auto_explain has to contend with.
	 */
	if (planstate->instrument)
		InstrEndLoop(planstate->instrument);

	if (es->analyze &&
		planstate->instrument && planstate->instrument->nloops > 0)
	{
		double		nloops = planstate->instrument->nloops;
		double		startup_sec = 1000.0 * planstate->instrument->startup / nloops;
		double		total_sec = 1000.0 * planstate->instrument->total / nloops;
		double		rows = planstate->instrument->ntuples / nloops;

		if (es->format == REPORT_FORMAT_TEXT)
		{
			if (es->timing)
				appendStringInfo(es->str,
							" (actual time=%.3f..%.3f rows=%.0f loops=%.0f)",
								 startup_sec, total_sec, rows, nloops);
			else
				appendStringInfo(es->str,
								 " (actual rows=%.0f loops=%.0f)",
								 rows, nloops);
		}
		else
		{
			if (es->timing)
			{
				ReportPropertyFloat("Actual Startup Time", startup_sec, 3, es);
				ReportPropertyFloat("Actual Total Time", total_sec, 3, es);
			}
			ReportPropertyFloat("Actual Rows", rows, 0, es);
			ReportPropertyFloat("Actual Loops", nloops, 0, es);
		}
	}
	else if (es->analyze)
	{
		if (es->format == REPORT_FORMAT_TEXT)
			appendStringInfoString(es->str, " (never executed)");
		else
		{
			if (es->timing)
			{
				ReportPropertyFloat("Actual Startup Time", 0.0, 3, es);
				ReportPropertyFloat("Actual Total Time", 0.0, 3, es);
			}
			ReportPropertyFloat("Actual Rows", 0.0, 0, es);
			ReportPropertyFloat("Actual Loops", 0.0, 0, es);
		}
	}

	/* in text format, first line ends here */
	ReportNewLine(es);

	/* target list */
	if (es->verbose)
		show_plan_tlist(planstate, ancestors, es);

	/* unique join */
	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* try not to be too chatty about this in text mode */
			if (es->format != REPORT_FORMAT_TEXT ||
				(es->verbose && ((Join *) plan)->inner_unique))
				ReportPropertyBool("Inner Unique",
									((Join *) plan)->inner_unique,
									es);
			break;
		default:
			break;
	}

	/* quals, sort keys, etc */
	show_control_qual(planstate, ancestors, es);

	/* Show buffer usage */
	if (es->buffers && planstate->instrument)
		show_buffer_usage(es, &planstate->instrument->bufusage);

	/* Show worker detail */
	if (es->analyze && es->verbose && planstate->worker_instrument)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;
		bool		opened_group = false;
		int			n;

		for (n = 0; n < w->num_workers; ++n)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;
			double		startup_sec;
			double		total_sec;
			double		rows;

			if (nloops <= 0)
				continue;

			startup_sec = 1000.0 * instrument->startup / nloops;
			total_sec = 1000.0 * instrument->total / nloops;
			rows = instrument->ntuples / nloops;

			if (es->format == REPORT_FORMAT_TEXT)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfo(es->str, "Worker %d: ", n);
				if (es->timing)
					appendStringInfo(es->str,
							 "actual time=%.3f..%.3f rows=%.0f loops=%.0f\n",
									 startup_sec, total_sec, rows, nloops);
				else
					appendStringInfo(es->str,
									 "actual rows=%.0f loops=%.0f\n",
									 rows, nloops);
				es->indent++;
				if (es->buffers)
					show_buffer_usage(es, &instrument->bufusage);
				es->indent--;
			}
			else
			{
				if (!opened_group)
				{
					ReportOpenGroup("Workers", "Workers", false, es);
					opened_group = true;
				}
				ReportOpenGroup("Worker", NULL, true, es);
				ReportPropertyInteger("Worker Number", n, es);

				if (es->timing)
				{
					ReportPropertyFloat("Actual Startup Time", startup_sec, 3, es);
					ReportPropertyFloat("Actual Total Time", total_sec, 3, es);
				}
				ReportPropertyFloat("Actual Rows", rows, 0, es);
				ReportPropertyFloat("Actual Loops", nloops, 0, es);

				if (es->buffers)
					show_buffer_usage(es, &instrument->bufusage);

				ReportCloseGroup("Worker", NULL, true, es);
			}
		}

		if (opened_group)
			ReportCloseGroup("Workers", "Workers", false, es);
	}

	/* Get ready to display the child plans */
	haschildren = ReportHasChildren(plan, planstate);
	if (haschildren)
	{
		ReportOpenGroup("Plans", "Plans", false, es);
		/* Pass current PlanState as head of ancestors list for children */
		ancestors = lcons(planstate, ancestors);
	}

	/* initPlan-s */
	if (planstate->initPlan)
		ReportSubPlans(planstate->initPlan, ancestors, "InitPlan", es, ExplainNode);

	/* lefttree */
	if (outerPlanState(planstate))
		ExplainNode(outerPlanState(planstate), ancestors, "Outer", NULL, es);

	/* righttree */
	if (innerPlanState(planstate))
		ExplainNode(innerPlanState(planstate), ancestors, "Inner", NULL, es);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			ReportMemberNodes(((ModifyTable*) plan)->plans,
				((ModifyTableState*) planstate)->mt_plans, ancestors, es, ExplainNode);
			break;
		case T_Append:
			ReportMemberNodes(((Append *) plan)->appendplans,
				((AppendState *) planstate)->appendplans, ancestors, es, ExplainNode);
			break;
		case T_MergeAppend:
			ReportMemberNodes(((MergeAppend *) plan)->mergeplans,
				((MergeAppendState *) planstate)->mergeplans, ancestors, es, ExplainNode);
			break;
		case T_BitmapAnd:
			ReportMemberNodes(((BitmapAnd *) plan)->bitmapplans,
				((BitmapAndState *) planstate)->bitmapplans, ancestors, es, ExplainNode);
			break;
		case T_BitmapOr:
			ReportMemberNodes(((BitmapOr *) plan)->bitmapplans,
				((BitmapOrState *) planstate)->bitmapplans, ancestors, es, ExplainNode);
			break;
		case T_SubqueryScan:
			ExplainNode(((SubqueryScanState *) planstate)->subplan, ancestors, "Subquery", NULL, es);
			break;
		case T_CustomScan:
			ReportCustomChildren((CustomScanState *) planstate, ancestors, es, ExplainNode);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		ReportSubPlans(planstate->subPlan, ancestors, "SubPlan", es, ExplainNode);

	/* end of child plans */
	if (haschildren)
	{
		ancestors = list_delete_first(ancestors);
		ReportCloseGroup("Plans", "Plans", false, es);
	}

	/* in text format, undo whatever indentation we added */
	if (es->format == REPORT_FORMAT_TEXT)
		es->indent = save_indent;

	ReportCloseGroup("Plan",
					  relationship ? NULL : "Plan",
					  true, es);
}

/*
 * Show buffer usage details.
 */
void
show_buffer_usage(ReportState *es, const BufferUsage *usage)
{
	if (es->format == REPORT_FORMAT_TEXT)
	{
		bool has_shared = (usage->shared_blks_hit > 0 ||
					usage->shared_blks_read > 0 ||
					usage->shared_blks_dirtied > 0 ||
					usage->shared_blks_written > 0);
		bool has_local = (usage->local_blks_hit > 0 ||
					usage->local_blks_read > 0 ||
					usage->local_blks_dirtied > 0 ||
					usage->local_blks_written > 0);
		bool has_temp = (usage->temp_blks_read > 0 ||
					usage->temp_blks_written > 0);
		bool has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) ||
                                        !INSTR_TIME_IS_ZERO(usage->blk_write_time));

		/* Show only positive counter values. */
		if (has_shared || has_local || has_temp)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str, "Buffers:");

			if (has_shared)
			{
				appendStringInfoString(es->str, " shared");
				if (usage->shared_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
						usage->shared_blks_hit);

				if (usage->shared_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
						usage->shared_blks_read);

				if (usage->shared_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
						usage->shared_blks_dirtied);

				if (usage->shared_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
						usage->shared_blks_written);

				if (has_local || has_temp)
					appendStringInfoChar(es->str, ',');
			}

			if (has_local)
			{
				appendStringInfoString(es->str, " local");
				if (usage->local_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
						usage->local_blks_hit);

				if (usage->local_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
						usage->local_blks_read);

				if (usage->local_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
						usage->local_blks_dirtied);

				if (usage->local_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
						usage->local_blks_written);

				if (has_temp)
					appendStringInfoChar(es->str, ',');
			}

			if (has_temp)
			{
				appendStringInfoString(es->str, " temp");
				if (usage->temp_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
						usage->temp_blks_read);

				if (usage->temp_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
						usage->temp_blks_written);
			}

			appendStringInfoChar(es->str, '\n');
		}

		/* As above, show only positive counter values. */
		if (has_timing)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str, "I/O Timings:");

			if (!INSTR_TIME_IS_ZERO(usage->blk_read_time))
				appendStringInfo(es->str, " read=%0.3f",
					INSTR_TIME_GET_MILLISEC(usage->blk_read_time));

			if (!INSTR_TIME_IS_ZERO(usage->blk_write_time))
				appendStringInfo(es->str, " write=%0.3f",
					INSTR_TIME_GET_MILLISEC(usage->blk_write_time));

			appendStringInfoChar(es->str, '\n');
		}
	} 
	else
	{
		ReportPropertyLong("Shared Hit Blocks", usage->shared_blks_hit, es);
		ReportPropertyLong("Shared Read Blocks", usage->shared_blks_read, es);
		ReportPropertyLong("Shared Dirtied Blocks", usage->shared_blks_dirtied, es);
		ReportPropertyLong("Shared Written Blocks", usage->shared_blks_written, es);
		ReportPropertyLong("Local Hit Blocks", usage->local_blks_hit, es);
		ReportPropertyLong("Local Read Blocks", usage->local_blks_read, es);
		ReportPropertyLong("Local Dirtied Blocks", usage->local_blks_dirtied, es);
		ReportPropertyLong("Local Written Blocks", usage->local_blks_written, es);
		ReportPropertyLong("Temp Read Blocks", usage->temp_blks_read, es);
		ReportPropertyLong("Temp Written Blocks", usage->temp_blks_written, es);
		if (track_io_timing)
		{
			ReportPropertyFloat("I/O Read Time", INSTR_TIME_GET_MILLISEC(usage->blk_read_time), 3, es);
			ReportPropertyFloat("I/O Write Time", INSTR_TIME_GET_MILLISEC(usage->blk_write_time), 3, es);
		}
	}
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
static void
ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
					ReportState *es)
{
	const char *indexname = explain_get_index_name(indexid);

	if (es->format == REPORT_FORMAT_TEXT)
	{
		if (ScanDirectionIsBackward(indexorderdir))
			appendStringInfoString(es->str, " Backward");

		appendStringInfo(es->str, " using %s", indexname);
	}
	else
	{
		const char *scandir;	

		switch (indexorderdir)
		{
			case BackwardScanDirection:
				scandir = "Backward";
				break;
			case NoMovementScanDirection:
				scandir = "NoMovement";
				break;
			case ForwardScanDirection:
				scandir = "Forward";
				break;
			default:
				scandir = "???";
				break;
		}

		ReportPropertyText("Scan Direction", scandir, es);
		ReportPropertyText("Index Name", indexname, es);
	}
}

/*
 * Show the target of a Scan node
 */
static void
ExplainScanTarget(Scan *plan, ReportState *es)
{
	ExplainTargetRel((Plan *) plan, plan->scanrelid, es);
}

/*
 * Show the target of a ModifyTable node
 *
 * Here we show the nominal target (ie, the relation that was named in the
 * original query).  If the actual target(s) is/are different, we'll show them
 * in show_modifytable_info().
 */
static void
ExplainModifyTarget(ModifyTable *plan, ReportState *es)
{
	ExplainTargetRel((Plan *) plan, plan->nominalRelation, es);
}

/*
 * Show the target relation of a scan or modify node
 */
static void
ExplainTargetRel(Plan *plan, Index rti, ReportState *es)
{
	char       *objectname = NULL;
	char       *namespace = NULL;
	const char *objecttag = NULL;
	RangeTblEntry *rte;
	char       *refname;

	rte = rt_fetch(rti, es->rtable);
	refname = (char *) list_nth(es->rtable_names, rti - 1);
	if (refname == NULL)
		refname = rte->eref->aliasname;

	switch (nodeTag(plan))

	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_ModifyTable:
			/* Assert it's on a real relation */
			Assert(rte->rtekind == RTE_RELATION);
			objectname = get_rel_name(rte->relid);
			if (es->verbose)
				namespace = get_namespace_name(get_rel_namespace(rte->relid));

			objecttag = "Relation Name";

			break;

		case T_FunctionScan:
		{
			FunctionScan *fscan = (FunctionScan *) plan;

			/* Assert it's on a RangeFunction */
			Assert(rte->rtekind == RTE_FUNCTION);

			/*
			 * If the expression is still a function call of a single
			 * function, we can get the real name of the function.
			 * Otherwise, punt.  (Even if it was a single function call
			 * originally, the optimizer could have simplified it away.)
			 */
			if (list_length(fscan->functions) == 1)
			{
				RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(fscan->functions);

				if (IsA(rtfunc->funcexpr, FuncExpr))
				{
					FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
					Oid                     funcid = funcexpr->funcid;

					objectname = get_func_name(funcid);
					if (es->verbose)
						namespace =
							get_namespace_name(get_func_namespace(funcid));
				}
			}
			objecttag = "Function Name";

			}
			break;

		case T_ValuesScan:
			Assert(rte->rtekind == RTE_VALUES);
			break;
               case T_CteScan:
                       /* Assert it's on a non-self-reference CTE */
                       Assert(rte->rtekind == RTE_CTE);
                       Assert(!rte->self_reference);
                      objectname = rte->ctename;
                       objecttag = "CTE Name";
                       break;
               case T_WorkTableScan:
                       /* Assert it's on a self-reference CTE */
                      Assert(rte->rtekind == RTE_CTE);
                       Assert(rte->self_reference);
                       objectname = rte->ctename;
                       objecttag = "CTE Name";
                      break;
               default:
			break;
	}

	if (es->format == REPORT_FORMAT_TEXT)
	{
		appendStringInfoString(es->str, " on");
		if (namespace != NULL)
			appendStringInfo(es->str, " %s.%s", quote_identifier(namespace),
							quote_identifier(objectname));
		else if (objectname != NULL)
			appendStringInfo(es->str, " %s", quote_identifier(objectname));

		if (objectname == NULL || strcmp(refname, objectname) != 0)
			appendStringInfo(es->str, " %s", quote_identifier(refname));
	}
	else
	{
		if (objecttag != NULL && objectname != NULL)
			ReportPropertyText(objecttag, objectname, es);
		if (namespace != NULL)
			ReportPropertyText("Schema", namespace, es);
		ReportPropertyText("Alias", refname, es);
	}
}
