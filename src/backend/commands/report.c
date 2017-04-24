/*-------------------------------------------------------------------------
 *
 * report.c
 *	  Display plans properties
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/report.c
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


static void ReportXMLTag(const char *tagname, int flags, ReportState *rpt);
static void ReportJSONLineEnding(ReportState *rpt);
static void ReportYAMLLineStarting(ReportState *rpt);
static void escape_yaml(StringInfo buf, const char *str);


ReportState* CreateReportState(int needed)
{
	StringInfo str;
	ReportState* prg;

	str = makeStringInfo();

	/* default allocation size is 1024 */
	if (needed > 1024)
		enlargeStringInfo(str, needed);

	prg = (ReportState*) palloc0(sizeof(ReportState));
	prg->format = REPORT_FORMAT_TEXT;   /* default */
	prg->str = str;
	prg->indent = 0;

	prg->rtable = NULL;
	prg->plan = NULL;

	return prg;
}

int SetReportStateCosts(ReportState* prg, bool costs)
{
	if (prg != NULL) {
		prg->costs = costs;
		return 0;
	} else {
		return 1;
	}
}

void FreeReportState(ReportState* prg)
{
	if (prg == NULL)
		return;

	if (prg->str != NULL) {
		if (prg->str->data != NULL)
			pfree(prg->str->data);

		pfree(prg->str);
	}

	pfree(prg);
}

/*
 * ReportQueryText -
 *	  add a "Query Text" node that contains the actual text of the query
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.
 *
 */
void
ReportQueryText(ReportState *es, QueryDesc *queryDesc)
{
	if (queryDesc->sourceText)
		ReportPropertyText("Query Text", queryDesc->sourceText, es);
}

/*
 * ReportPreScanNode -
 *	  Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 * This ensures that we don't confusingly assign un-suffixed aliases to RTEs
 * that never appear in the EXPLAIN output (such as inheritance parents).
 */
bool
ReportPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_ForeignScan:
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
			break;
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
									   ((CustomScan *) plan)->custom_relids);
			break;
		case T_ModifyTable:
			*rels_used = bms_add_member(*rels_used,
									((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
										 ((ModifyTable *) plan)->exclRelRTI);
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, ReportPreScanNode, rels_used);
}

/*
 * Show the targetlist of a plan node
 */
void
show_plan_tlist(PlanState *planstate, List *ancestors, ReportState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	bool		useprefix;
	ListCell   *lc;

	elog(LOG, "TGT start");

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

	elog(LOG, "TGT step 1");

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

	if (IsA(plan, ForeignScan) &&
		((ForeignScan *) plan)->operation != CMD_SELECT)
		return;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Deparse each result column (we now include resjunk ones) */
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		result = lappend(result,
						 deparse_expression((Node *) tle->expr, context,
											useprefix, false));
	}

	/* Print results */
	ReportPropertyList("Output", result, es);
}

void show_control_qual(PlanState *planstate, List *ancestors, ReportState *es)
{
	Plan       *plan = planstate->plan;

	/* quals, sort keys, etc */
	switch (nodeTag(plan))
	{
		case T_IndexScan:
			show_scan_qual(((IndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexScan *) plan)->indexqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexScan *) plan)->indexorderbyorig,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_IndexOnlyScan:
			show_scan_qual(((IndexOnlyScan *) plan)->indexqual,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexOnlyScan *) plan)->indexqual)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexOnlyScan *) plan)->indexorderby,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				ReportPropertyLong("Heap Fetches",
				   ((IndexOnlyScanState *) planstate)->ioss_HeapFetches, es);
			break;
		case T_BitmapIndexScan:
			show_scan_qual(((BitmapIndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			break;
		case T_BitmapHeapScan:
			show_scan_qual(((BitmapHeapScan *) plan)->bitmapqualorig,
						   "Recheck Cond", planstate, ancestors, es);
			if (((BitmapHeapScan *) plan)->bitmapqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				show_tidbitmap_info((BitmapHeapScanState *) planstate, es);
			break;
		case T_SampleScan:
			show_tablesample(((SampleScan *) plan)->tablesample,
							 planstate, ancestors, es);
			/* FALL THRU to print additional fields the same as SeqScan */
		case T_SeqScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_SubqueryScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Gather:
			{
				Gather	   *gather = (Gather *) plan;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				ReportPropertyInteger("Workers Planned",
									   gather->num_workers, es);
				if (es->analyze)
				{
					int			nworkers;

					nworkers = ((GatherState *) planstate)->nworkers_launched;
					ReportPropertyInteger("Workers Launched",
										   nworkers, es);
				}
				if (gather->single_copy || es->format != REPORT_FORMAT_TEXT)
					ReportPropertyBool("Single Copy", gather->single_copy, es);
			}
			break;
		case T_FunctionScan:
			if (es->verbose)
			{
				List	   *fexprs = NIL;
				ListCell   *lc;

				foreach(lc, ((FunctionScan *) plan)->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

					fexprs = lappend(fexprs, rtfunc->funcexpr);
				}
				/* We rely on show_expression to insert commas as needed */
				show_expression((Node *) fexprs,
								"Function Call", planstate, ancestors,
								es->verbose, es);
			}
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_TidScan:
			{
				/*
				 * The tidquals list has OR semantics, so be sure to show it
				 * as an OR condition.
				 */
				List	   *tidquals = ((TidScan *) plan)->tidquals;

				if (list_length(tidquals) > 1)
					tidquals = list_make1(make_orclause(tidquals));
				show_scan_qual(tidquals, "TID Cond", planstate, ancestors, es);
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
			}
			break;
		case T_ForeignScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			show_foreignscan_info((ForeignScanState *) planstate, es);
			break;
		case T_CustomScan:
			{
				CustomScanState *css = (CustomScanState *) planstate;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				if (css->methods->ExplainCustomScan)
					css->methods->ExplainCustomScan(css, ancestors, es);
			}
			break;
		case T_NestLoop:
			show_upper_qual(((NestLoop *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((NestLoop *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_MergeJoin:
			show_upper_qual(((MergeJoin *) plan)->mergeclauses,
							"Merge Cond", planstate, ancestors, es);
			show_upper_qual(((MergeJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((MergeJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_HashJoin:
			show_upper_qual(((HashJoin *) plan)->hashclauses,
							"Hash Cond", planstate, ancestors, es);
			show_upper_qual(((HashJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((HashJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_Agg:
			show_agg_keys(castNode(AggState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Group:
			show_group_keys(castNode(GroupState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Sort:
			show_sort_keys(castNode(SortState, planstate), ancestors, es);
			show_sort_info(castNode(SortState, planstate), es);
			break;
		case T_MergeAppend:
			show_merge_append_keys(castNode(MergeAppendState, planstate),
								   ancestors, es);
			break;
		case T_Result:
			show_upper_qual((List *) ((Result *) plan)->resconstantqual,
							"One-Time Filter", planstate, ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_ModifyTable:
			show_modifytable_info(castNode(ModifyTableState, planstate), ancestors,
								  es);
			break;
		case T_Hash:
			show_hash_info(castNode(HashState, planstate), es);
			break;
		default:
			break;
	}
}

/*
 * Show a generic expression
 */
void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ReportState *es)
{
	List	   *context;
	char	   *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
				(Node *) planstate,
				ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ReportPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
void
show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ReportState *es)
{
	Node	   *node;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* And show it */
	show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
void
show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ReportState *es)
{
	bool		useprefix;

	useprefix = (IsA(planstate->plan, SubqueryScan) ||es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
void
show_upper_qual(List *qual, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ReportState *es)
{
	bool		useprefix;

	useprefix = (list_length(es->rtable) > 1 || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show the sort keys for a Sort node.
 */
void
show_sort_keys(SortState *sortstate, List *ancestors, ReportState *es)
{
	Sort	   *plan = (Sort *) sortstate->ss.ps.plan;

	show_sort_group_keys((PlanState *) sortstate, "Sort Key",
						 plan->numCols, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

/*
 * Likewise, for a MergeAppend node.
 */
void
show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
					   ReportState *es)
{
	MergeAppend *plan = (MergeAppend *) mstate->ps.plan;

	show_sort_group_keys((PlanState *) mstate, "Sort Key",
						 plan->numCols, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

/*
 * Show the grouping keys for an Agg node.
 */
void
show_agg_keys(AggState *astate, List *ancestors,
			  ReportState *es)
{
	Agg		   *plan = (Agg *) astate->ss.ps.plan;

	if (plan->numCols > 0 || plan->groupingSets)
	{

		/* The key columns refer to the tlist of the child plan */

		ancestors = lcons(astate, ancestors);

		if (plan->groupingSets)
			show_grouping_sets(outerPlanState(astate), plan, ancestors, es);
		else
			show_sort_group_keys(outerPlanState(astate), "Group Key",
								 plan->numCols, plan->grpColIdx,
								 NULL, NULL, NULL,
								 ancestors, es);

		ancestors = list_delete_first(ancestors);
	}
}

void
show_grouping_sets(PlanState *planstate, Agg *agg,
				   List *ancestors, ReportState *es)
{
	List	   *context;
	bool		useprefix;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	ReportOpenGroup("Grouping Sets", "Grouping Sets", false, es);

	show_grouping_set_keys(planstate, agg, NULL,
						   context, useprefix, ancestors, es);

	foreach(lc, agg->chain)
	{
		Agg		   *aggnode = lfirst(lc);
		Sort	   *sortnode = (Sort *) aggnode->plan.lefttree;

		show_grouping_set_keys(planstate, aggnode, sortnode,
							   context, useprefix, ancestors, es);
	}

	ReportCloseGroup("Grouping Sets", "Grouping Sets", false, es);
}

void
show_grouping_set_keys(PlanState *planstate,
					   Agg *aggnode, Sort *sortnode,
					   List *context, bool useprefix,
					   List *ancestors, ReportState *es)
{
	Plan	   *plan = planstate->plan;
	char	   *exprstr;
	ListCell   *lc;
	List	   *gsets = aggnode->groupingSets;
	AttrNumber *keycols = aggnode->grpColIdx;

	ReportOpenGroup("Grouping Set", NULL, true, es);

	if (sortnode)
	{
		show_sort_group_keys(planstate, "Sort Key",
							 sortnode->numCols, sortnode->sortColIdx,
							 sortnode->sortOperators, sortnode->collations,
							 sortnode->nullsFirst,
							 ancestors, es);
		if (es->format == REPORT_FORMAT_TEXT)
			es->indent++;
	}

	ReportOpenGroup("Group Keys", "Group Keys", false, es);

	foreach(lc, gsets)
	{
		List	   *result = NIL;
		ListCell   *lc2;

		foreach(lc2, (List *) lfirst(lc))
		{
			Index		i = lfirst_int(lc2);
			AttrNumber	keyresno = keycols[i];
			TargetEntry *target = get_tle_by_resno(plan->targetlist,
												   keyresno);

			if (!target)
				elog(ERROR, "no tlist entry for key %d", keyresno);

			/* Deparse the expression, showing any top-level cast */
			exprstr = deparse_expression((Node *) target->expr, context,
										 useprefix, true);

			result = lappend(result, exprstr);
		}

		if (!result && es->format == REPORT_FORMAT_TEXT)
			ReportPropertyText("Group Key", "()", es);
		else
			ReportPropertyListNested("Group Key", result, es);
	}

	ReportCloseGroup("Group Keys", "Group Keys", false, es);

	if (sortnode && es->format == REPORT_FORMAT_TEXT)
		es->indent--;

	ReportCloseGroup("Grouping Set", NULL, true, es);
}

/*
 * Show the grouping keys for a Group node.
 */
void
show_group_keys(GroupState *gstate, List *ancestors,
				ReportState *es)
{
	Group	   *plan = (Group *) gstate->ss.ps.plan;

	/* The key columns refer to the tlist of the child plan */
	ancestors = lcons(gstate, ancestors);
	show_sort_group_keys(outerPlanState(gstate), "Group Key",
						 plan->numCols, plan->grpColIdx,
						 NULL, NULL, NULL,
						 ancestors, es);
	ancestors = list_delete_first(ancestors);
}

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
void
show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ReportState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	StringInfoData sortkeybuf;
	bool		useprefix;
	int			keyno;

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);


	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	for (keyno = 0; keyno < nkeys; keyno++)
	{

		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
											   keyresno);
		char	   *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);

		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression((Node *) target->expr, context,
									 useprefix, true);
		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);

		/* Append sort order information, if relevant */
		if (sortOperators != NULL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   sortOperators[keyno],
								   collations[keyno],
								   nullsFirst[keyno]);

		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
	}

	ReportPropertyList(qlabel, result, es);
}


/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
void
show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst)
{
	Oid			sortcoltype = exprType(sortexpr);
	bool		reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default.  There are some cases where this is
	 * redundant, eg if expression is a column whose declared collation is
	 * that collation, but it's hard to distinguish that here.
	 */
	if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID)
	{
		char	   *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
	}


	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char	   *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);

	/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}

/*
 * Show TABLESAMPLE properties
 */
void
show_tablesample(TableSampleClause *tsc, PlanState *planstate,
				 List *ancestors, ReportState *es)
{
	List	   *context;
	bool		useprefix;
	char	   *method_name;
	List	   *params = NIL;
	char	   *repeatable;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Get the tablesample method name */
	method_name = get_func_name(tsc->tsmhandler);

	/* Deparse parameter expressions */
	foreach(lc, tsc->args)
	{
		Node	   *arg = (Node *) lfirst(lc);

		params = lappend(params,
						 deparse_expression(arg, context,
											useprefix, false));
	}
	if (tsc->repeatable)
		repeatable = deparse_expression((Node *) tsc->repeatable, context,
										useprefix, false);
	else
		repeatable = NULL;

	/* Print results */
	if (es->format == REPORT_FORMAT_TEXT)
	{
		bool		first = true;

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "Sampling: %s (", method_name);
		foreach(lc, params)
		{
			if (!first)
				appendStringInfoString(es->str, ", ");
			appendStringInfoString(es->str, (const char *) lfirst(lc));
			first = false;
		}
		appendStringInfoChar(es->str, ')');
		if (repeatable)
			appendStringInfo(es->str, " REPEATABLE (%s)", repeatable);
		appendStringInfoChar(es->str, '\n');
	}
	else
	{
		ReportPropertyText("Sampling Method", method_name, es);
		ReportPropertyList("Sampling Parameters", params, es);
		if (repeatable)
			ReportPropertyText("Repeatable Seed", repeatable, es);
	}
}


/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for a sort node
 */
void
show_sort_info(SortState *sortstate, ReportState *es)
{
	if (es->analyze && sortstate->sort_Done &&
		sortstate->tuplesortstate != NULL)
	{
		Tuplesortstate *state = (Tuplesortstate *) sortstate->tuplesortstate;
		const char *sortMethod;
		const char *spaceType;
		long		spaceUsed;

		tuplesort_get_stats(state, &sortMethod, &spaceType, &spaceUsed);

		if (es->format == REPORT_FORMAT_TEXT)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "Sort Method: %s  %s: %ldkB\n",
							 sortMethod, spaceType, spaceUsed);
		}
		else
		{
			ReportPropertyText("Sort Method", sortMethod, es);
			ReportPropertyLong("Sort Space Used", spaceUsed, es);
			ReportPropertyText("Sort Space Type", spaceType, es);
		}
	}
}

/*
 * Show information on hash buckets/batches.
 */
void
show_hash_info(HashState *hashstate, ReportState *es)
{
	HashJoinTable hashtable;

	hashtable = hashstate->hashtable;

	if (hashtable)
	{
		long		spacePeakKb = (hashtable->spacePeak + 1023) / 1024;

		if (es->format != REPORT_FORMAT_TEXT)
		{
			ReportPropertyLong("Hash Buckets", hashtable->nbuckets, es);
			ReportPropertyLong("Original Hash Buckets",
								hashtable->nbuckets_original, es);
			ReportPropertyLong("Hash Batches", hashtable->nbatch, es);
			ReportPropertyLong("Original Hash Batches",
								hashtable->nbatch_original, es);
			ReportPropertyLong("Peak Memory Usage", spacePeakKb, es);
		}
		else if (hashtable->nbatch_original != hashtable->nbatch ||
				 hashtable->nbuckets_original != hashtable->nbuckets)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str,
							 "Buckets: %d (originally %d)  Batches: %d (originally %d)  Memory Usage: %ldkB\n",
							 hashtable->nbuckets,
							 hashtable->nbuckets_original,
							 hashtable->nbatch,
							 hashtable->nbatch_original,
							 spacePeakKb);
		}
		else
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str,
						   "Buckets: %d  Batches: %d  Memory Usage: %ldkB\n",
							 hashtable->nbuckets, hashtable->nbatch,
							 spacePeakKb);
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show exact/lossy pages for a BitmapHeapScan node
 */
void
show_tidbitmap_info(BitmapHeapScanState *planstate, ReportState *es)
{
	if (es->format != REPORT_FORMAT_TEXT)
	{
		ReportPropertyLong("Exact Heap Blocks", planstate->exact_pages, es);
		ReportPropertyLong("Lossy Heap Blocks", planstate->lossy_pages, es);
	}
	else
	{
		if (planstate->exact_pages > 0 || planstate->lossy_pages > 0)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str, "Heap Blocks:");
			if (planstate->exact_pages > 0)
				appendStringInfo(es->str, " exact=%ld", planstate->exact_pages);
			if (planstate->lossy_pages > 0)
				appendStringInfo(es->str, " lossy=%ld", planstate->lossy_pages);
			appendStringInfoChar(es->str, '\n');
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
void
show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ReportState *es)
{
	double		nfiltered;
	double		nloops;

	if (!es->analyze || !planstate->instrument)
		return;

	if (which == 2)
		nfiltered = planstate->instrument->nfiltered2;
	else
		nfiltered = planstate->instrument->nfiltered1;
	nloops = planstate->instrument->nloops;

	/* In text mode, suppress zero counts; they're not interesting enough */
	if (nfiltered > 0 || es->format != REPORT_FORMAT_TEXT)
	{
		if (nloops > 0)
			ReportPropertyFloat(qlabel, nfiltered / nloops, 0, es);
		else
			ReportPropertyFloat(qlabel, 0.0, 0, es);
	}
}

/*
 * Show extra information for a ForeignScan node.
 */
void
show_foreignscan_info(ForeignScanState *fsstate, ReportState *es)
{
	FdwRoutine *fdwroutine = fsstate->fdwroutine;

	/* Let the FDW emit whatever fields it wants */
	if (((ForeignScan *) fsstate->ss.ps.plan)->operation != CMD_SELECT)
	{
		if (fdwroutine->ExplainDirectModify != NULL)
			fdwroutine->ExplainDirectModify(fsstate, es);
	}
	else
	{
		if (fdwroutine->ExplainForeignScan != NULL)
			fdwroutine->ExplainForeignScan(fsstate, es);
	}
}

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 */
const char *
explain_get_index_name(Oid indexId)
{
	const char *result;

	if (explain_get_index_name_hook)
		result = (*explain_get_index_name_hook) (indexId);
	else
		result = NULL;
	if (result == NULL)
	{

		/* default behavior: look in the catalogs and quote it */
		result = get_rel_name(indexId);
		if (result == NULL)
			elog(ERROR, "cache lookup failed for index %u", indexId);
		result = quote_identifier(result);
	}
	return result;
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
void
ReportIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
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
void
ReportScanTarget(Scan *plan, ReportState *es)
{
	ReportTargetRel((Plan *) plan, plan->scanrelid, es);
}

/*
 * Show the target of a ModifyTable node
 *
 * Here we show the nominal target (ie, the relation that was named in the
 * original query).  If the actual target(s) is/are different, we'll show them
 * in show_modifytable_info().
 */
void
ReportModifyTarget(ModifyTable *plan, ReportState *es)
{
	ReportTargetRel((Plan *) plan, plan->nominalRelation, es);
}

/*
 * Show the target relation of a scan or modify node
 */
void
ReportTargetRel(Plan *plan, Index rti, ReportState *es)
{
	char	   *objectname = NULL;
	char	   *namespace = NULL;
	const char *objecttag = NULL;
	RangeTblEntry *rte;
	char	   *refname;

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
						Oid			funcid = funcexpr->funcid;

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

/*
 * Show extra information for a ModifyTable node
 *
 * We have three objectives here.  First, if there's more than one target
 * table or it's different from the nominal target, identify the actual
 * target(s).  Second, give FDWs a chance to display extra info about foreign
 * targets.  Third, show information about ON CONFLICT.
 */
void
show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
					  ReportState *es)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	const char *operation;
	const char *foperation;
	bool		labeltargets;
	int			j;
	List	   *idxNames = NIL;
	ListCell   *lst;

	switch (node->operation)
	{
		case CMD_INSERT:
			operation = "Insert";
			foperation = "Foreign Insert";
			break;
		case CMD_UPDATE:
			operation = "Update";
			foperation = "Foreign Update";
			break;
		case CMD_DELETE:
			operation = "Delete";
			foperation = "Foreign Delete";
			break;
		default:
			operation = "???";
			foperation = "Foreign ???";
			break;
	}

	/* Should we explicitly label target relations? */
	labeltargets = (mtstate->mt_nplans > 1 ||
					(mtstate->mt_nplans == 1 &&
	   mtstate->resultRelInfo->ri_RangeTableIndex != node->nominalRelation));

	if (labeltargets)
		ReportOpenGroup("Target Tables", "Target Tables", false, es);

	for (j = 0; j < mtstate->mt_nplans; j++)
	{
		ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + j;
		FdwRoutine *fdwroutine = resultRelInfo->ri_FdwRoutine;

		if (labeltargets)
		{

			/* Open a group for this target */
			ReportOpenGroup("Target Table", NULL, true, es);

			/*
			 * In text mode, decorate each target with operation type, so that
			 * ReportTargetRel's output of " on foo" will read nicely.
			 */
			if (es->format == REPORT_FORMAT_TEXT)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfoString(es->str,
									   fdwroutine ? foperation : operation);
			}

			/* Identify target */
			ReportTargetRel((Plan *) node,
							 resultRelInfo->ri_RangeTableIndex,
							 es);

			if (es->format == REPORT_FORMAT_TEXT)
			{
				appendStringInfoChar(es->str, '\n');
				es->indent++;
			}
		}

		/* Give FDW a chance if needed */
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			fdwroutine != NULL &&
			fdwroutine->ExplainForeignModify != NULL)
		{
			List	   *fdw_private = (List *) list_nth(node->fdwPrivLists, j);

			fdwroutine->ExplainForeignModify(mtstate,
											 resultRelInfo,
											 fdw_private,
											 j,
											 es);
		}

		if (labeltargets)
		{

			/* Undo the indentation we added in text format */
			if (es->format == REPORT_FORMAT_TEXT)
				es->indent--;

			/* Close the group */
			ReportCloseGroup("Target Table", NULL, true, es);
		}
	}

	/* Gather names of ON CONFLICT arbiter indexes */
	foreach(lst, node->arbiterIndexes)
	{
		char	   *indexname = get_rel_name(lfirst_oid(lst));

		idxNames = lappend(idxNames, indexname);
	}

	if (node->onConflictAction != ONCONFLICT_NONE)
	{
		ReportProperty("Conflict Resolution",
						node->onConflictAction == ONCONFLICT_NOTHING ?
						"NOTHING" : "UPDATE",
						false, es, true);

		/*
		 * Don't display arbiter indexes at all when DO NOTHING variant
		 * implicitly ignores all conflicts
		 */
		if (idxNames)
			ReportPropertyList("Conflict Arbiter Indexes", idxNames, es);

		/* ON CONFLICT DO UPDATE WHERE qual is specially displayed */
		if (node->onConflictWhere)
		{
			show_upper_qual((List *) node->onConflictWhere, "Conflict Filter",
							&mtstate->ps, ancestors, es);
			show_instrumentation_count("Rows Removed by Conflict Filter", 1, &mtstate->ps, es);
		}

		/* EXPLAIN ANALYZE display of actual outcome for each tuple proposed */
		if (es->analyze && mtstate->ps.instrument)
		{
			double		total;
			double		insert_path;
			double		other_path;

			InstrEndLoop(mtstate->mt_plans[0]->instrument);

			/* count the number of source rows */
			total = mtstate->mt_plans[0]->instrument->ntuples;
			other_path = mtstate->ps.instrument->nfiltered2;
			insert_path = total - other_path;

			ReportPropertyFloat("Tuples Inserted", insert_path, 0, es);
			ReportPropertyFloat("Conflicting Tuples", other_path, 0, es);
		}
	}

	if (labeltargets)
		ReportCloseGroup("Target Tables", "Target Tables", false, es);
}

bool ReportHasChildren(Plan* plan, PlanState* planstate)
{
	bool haschildren;

        haschildren = planstate->initPlan
                || outerPlanState(planstate)
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
void
ReportMemberNodes(List *plans, PlanState **planstates,
				   List *ancestors, ReportState *es,
		functionNode fn)
{
	int			nplans = list_length(plans);
	int			j;

	for (j = 0; j < nplans; j++)
		(*fn)(planstates[j], ancestors, "Member", NULL, es);
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 */
void
ReportSubPlans(List *plans, List *ancestors, const char *relationship,
		ReportState *es, functionNode fn)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
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
ReportCustomChildren(CustomScanState *css, List *ancestors, ReportState *es, functionNode fn)
{
	ListCell   *cell;
	const char *label =
	(list_length(css->custom_ps) != 1 ? "children" : "child");

	foreach(cell, css->custom_ps)
		(*fn)((PlanState *) lfirst(cell), ancestors, label, NULL, es);
}

/*
 * Explain a property, such as sort keys or targets, that takes the form of
 * a list of unlabeled items.  "data" is a list of C strings.
 */
void
ReportPropertyList(const char *qlabel, List *data, ReportState *rpt)
{
	ListCell   *lc;
	bool		first = true;

	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:
			appendStringInfoSpaces(rpt->str, rpt->indent * 2);
			appendStringInfo(rpt->str, "%s: ", qlabel);
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(rpt->str, ", ");
				appendStringInfoString(rpt->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(rpt->str, '\n');
			break;

		case REPORT_FORMAT_XML:
			ReportXMLTag(qlabel, X_OPENING, rpt);
			foreach(lc, data)
			{
				char	   *str;

				appendStringInfoSpaces(rpt->str, rpt->indent * 2 + 2);
				appendStringInfoString(rpt->str, "<Item>");
				str = escape_xml((const char *) lfirst(lc));
				appendStringInfoString(rpt->str, str);
				pfree(str);
				appendStringInfoString(rpt->str, "</Item>\n");
			}
			ReportXMLTag(qlabel, X_CLOSING, rpt);
			break;

		case REPORT_FORMAT_JSON:
			ReportJSONLineEnding(rpt);
			appendStringInfoSpaces(rpt->str, rpt->indent * 2);
			escape_json(rpt->str, qlabel);
			appendStringInfoString(rpt->str, ": [");
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(rpt->str, ", ");
				escape_json(rpt->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(rpt->str, ']');
			break;

		case REPORT_FORMAT_YAML:
			ReportYAMLLineStarting(rpt);
			appendStringInfo(rpt->str, "%s: ", qlabel);
			foreach(lc, data)
			{
				appendStringInfoChar(rpt->str, '\n');
				appendStringInfoSpaces(rpt->str, rpt->indent * 2 + 2);
				appendStringInfoString(rpt->str, "- ");
				escape_yaml(rpt->str, (const char *) lfirst(lc));
			}
			break;
	}
}

/*
 * Explain a property that takes the form of a list of unlabeled items within
 * another list.  "data" is a list of C strings.
 */
void
ReportPropertyListNested(const char *qlabel, List *data, ReportState *rpt)
{
	ListCell   *lc;
	bool		first = true;

	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:
		case REPORT_FORMAT_XML:
			ReportPropertyList(qlabel, data, rpt);
			return;

		case REPORT_FORMAT_JSON:
			ReportJSONLineEnding(rpt);
			appendStringInfoSpaces(rpt->str, rpt->indent * 2);
			appendStringInfoChar(rpt->str, '[');
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(rpt->str, ", ");
				escape_json(rpt->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(rpt->str, ']');
			break;

		case REPORT_FORMAT_YAML:
			ReportYAMLLineStarting(rpt);
			appendStringInfoString(rpt->str, "- [");
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(rpt->str, ", ");
				escape_yaml(rpt->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(rpt->str, ']');
			break;
	}
}

/*
 * Explain a simple property.
 *
 * If "numeric" is true, the value is a number (or other value that
 * doesn't need quoting in JSON).
 *
 * This usually should not be invoked directly, but via one of the datatype
 * specific routines ReportPropertyText, ReportPropertyInteger, etc.
 */
void
ReportProperty(const char *qlabel, const char *value, bool numeric,
				ReportState *rpt, bool newline)
{
	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:
			appendStringInfoSpaces(rpt->str, rpt->indent * 2);
			if (newline)
				appendStringInfo(rpt->str, "%s: %s\n", qlabel, value);
			else 
				appendStringInfo(rpt->str, "%s: %s", qlabel, value);
			break;

		case REPORT_FORMAT_XML:
			{
				char	   *str;

				appendStringInfoSpaces(rpt->str, rpt->indent * 2);
				ReportXMLTag(qlabel, X_OPENING | X_NOWHITESPACE, rpt);
				str = escape_xml(value);
				appendStringInfoString(rpt->str, str);
				pfree(str);
				ReportXMLTag(qlabel, X_CLOSING | X_NOWHITESPACE, rpt);
				appendStringInfoChar(rpt->str, '\n');
			}
			break;

		case REPORT_FORMAT_JSON:
			ReportJSONLineEnding(rpt);
			appendStringInfoSpaces(rpt->str, rpt->indent * 2);
			escape_json(rpt->str, qlabel);
			appendStringInfoString(rpt->str, ": ");
			if (numeric)
				appendStringInfoString(rpt->str, value);
			else
				escape_json(rpt->str, value);
			break;

		case REPORT_FORMAT_YAML:
			ReportYAMLLineStarting(rpt);
			appendStringInfo(rpt->str, "%s: ", qlabel);
			if (numeric)
				appendStringInfoString(rpt->str, value);
			else
				escape_yaml(rpt->str, value);
			break;
	}
}

void ReportProperties(Plan* plan, PlanInfo* info, const char* plan_name,
	const char* relationship, ReportState* rpt)
{
	if (rpt->format == REPORT_FORMAT_TEXT) {
		if (plan_name) {
			appendStringInfoSpaces(rpt->str, rpt->indent * 2);
			appendStringInfo(rpt->str, "%s\n", plan_name);
			rpt->indent++;
		}

		if (rpt->indent) {
			appendStringInfoSpaces(rpt->str, rpt->indent * 2);
			appendStringInfoString(rpt->str, "->  ");
			rpt->indent += 2;
		}

		if (plan->parallel_aware) {
			appendStringInfoString(rpt->str, "Parallel ");
		}

		appendStringInfoString(rpt->str, info->pname);
		rpt->indent++;

	} else {
		ReportPropertyText("Node Type", info->sname, rpt);

		if (info->strategy) {
			ReportPropertyText("Strategy", info->strategy, rpt);
		}

		if (info->partialmode) {
			ReportPropertyText("Partial Mode", info->partialmode, rpt);
		}

		if (info->operation) {
			ReportPropertyText("Operation", info->operation, rpt);
		}

		if (relationship) {
			ReportPropertyText("Parent Relationship", relationship, rpt);
		}

		if (plan_name) {
			ReportPropertyText("Subplan Name", plan_name, rpt);
		}

		if (info->custom_name) {
			ReportPropertyText("Custom Plan Provider", info->custom_name, rpt);
		}

		ReportPropertyBool("Parallel Aware", plan->parallel_aware, rpt);
	}
}

/*
 * Explain a string-valued property.
 */
void
ReportPropertyText(const char *qlabel, const char *value, ReportState* rpt)
{
	ReportProperty(qlabel, value, false, rpt, true);
}

/*
 * Explain an integer-valued property.
 */
void
ReportPropertyInteger(const char *qlabel, int value, ReportState *rpt)
{
	char	buf[32];

	snprintf(buf, sizeof(buf), "%d", value);
	ReportProperty(qlabel, buf, true, rpt, true);
}

/*
 * Explain an integer-valued property without newline.
 */
void
ReportPropertyIntegerNoNewLine(const char *qlabel, int value, ReportState *rpt)
{
	char	buf[32];

	snprintf(buf, sizeof(buf), "%d", value);
	ReportProperty(qlabel, buf, true, rpt, false);
}

/*
 * Explain a long-integer-valued property.
 */
void
ReportPropertyLong(const char *qlabel, long value, ReportState *rpt)
{
	char	buf[32];

	snprintf(buf, sizeof(buf), "%ld", value);
	ReportProperty(qlabel, buf, true, rpt, true);
}

/*
 * Explain a long-integer-valued property without newline.
 */
void
ReportPropertyLongNoNewLine(const char *qlabel, long value, ReportState *rpt)
{
	char	buf[32];

	snprintf(buf, sizeof(buf), "%ld", value);
	ReportProperty(qlabel, buf, true, rpt, false);
}

/*
 * Explain a float-valued property, using the specified number of
 * fractional digits.
 */
void
ReportPropertyFloat(const char *qlabel, double value, int ndigits,
					 ReportState *rpt)
{
	char		buf[256];

	snprintf(buf, sizeof(buf), "%.*f", ndigits, value);
	ReportProperty(qlabel, buf, true, rpt, true);
}

/*
 * Explain a bool-valued property.
 */
void
ReportPropertyBool(const char *qlabel, bool value, ReportState *rpt)
{
	ReportProperty(qlabel, value ? "true" : "false", true, rpt, true);
}

/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
void
ReportOpenGroup(const char *objtype, const char *labelname,
				 bool labeled, ReportState *rpt)
{
	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:

			/* nothing to do */
			break;

		case REPORT_FORMAT_XML:
			ReportXMLTag(objtype, X_OPENING, rpt);
			rpt->indent++;
			break;

		case REPORT_FORMAT_JSON:
			ReportJSONLineEnding(rpt);
			appendStringInfoSpaces(rpt->str, 2 * rpt->indent);
			if (labelname)
			{
				escape_json(rpt->str, labelname);
				appendStringInfoString(rpt->str, ": ");
			}
			appendStringInfoChar(rpt->str, labeled ? '{' : '[');

			/*
			 * In JSON format, the grouping_stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level, 1 means we've
			 * emitted something (and so the next item needs a comma). See
			 * ReportJSONLineEnding().
			 */
			rpt->grouping_stack = lcons_int(0, rpt->grouping_stack);
			rpt->indent++;
			break;

		case REPORT_FORMAT_YAML:

			/*
			 * In YAML format, the grouping stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level AND this grouping
			 * level is unlabelled and must be marked with "- ".  See
			 * PlanYAMLLineStarting().
			 */
			ReportYAMLLineStarting(rpt);
			if (labelname)
			{
				appendStringInfo(rpt->str, "%s: ", labelname);
				rpt->grouping_stack = lcons_int(1, rpt->grouping_stack);
			}
			else
			{
				appendStringInfoString(rpt->str, "- ");
				rpt->grouping_stack = lcons_int(0, rpt->grouping_stack);
			}
			rpt->indent++;
			break;
	}
}

/*
 * Close a group of related objects.
 * Parameters must match the corresponding ReportOpenGroup call.
 */
void
ReportCloseGroup(const char *objtype, const char *labelname,
				  bool labeled, ReportState *rpt)
{
	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:
			/* nothing to do */
			break;

		case REPORT_FORMAT_XML:
			rpt->indent--;
			ReportXMLTag(objtype, X_CLOSING, rpt);
			break;

		case REPORT_FORMAT_JSON:
			rpt->indent--;
			appendStringInfoChar(rpt->str, '\n');
			appendStringInfoSpaces(rpt->str, 2 * rpt->indent);
			appendStringInfoChar(rpt->str, labeled ? '}' : ']');
			rpt->grouping_stack = list_delete_first(rpt->grouping_stack);
			break;

		case REPORT_FORMAT_YAML:
			rpt->indent--;
			rpt->grouping_stack = list_delete_first(rpt->grouping_stack);
			break;
	}
}


/*
 * Emit a "dummy" group that never has any members.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 */
void
ReportDummyGroup(const char *objtype, const char *labelname, ReportState *rpt)
{
	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:

			/* nothing to do */

			break;

		case REPORT_FORMAT_XML:
			ReportXMLTag(objtype, X_CLOSE_IMMEDIATE, rpt);
			break;

		case REPORT_FORMAT_JSON:
			ReportJSONLineEnding(rpt);
			appendStringInfoSpaces(rpt->str, 2 * rpt->indent);
			if (labelname)
			{
				escape_json(rpt->str, labelname);
				appendStringInfoString(rpt->str, ": ");
			}
			escape_json(rpt->str, objtype);
			break;

		case REPORT_FORMAT_YAML:
			ReportYAMLLineStarting(rpt);
			if (labelname)
			{
				escape_yaml(rpt->str, labelname);
				appendStringInfoString(rpt->str, ": ");
			}
			else
			{
				appendStringInfoString(rpt->str, "- ");
			}
			escape_yaml(rpt->str, objtype);
			break;
	}
}

/*
 * Emit the start-of-output boilerplate.
 *
 * This is just enough different from processing a subgroup that we need
 * a separate pair of subroutines.
 */
void
ReportBeginOutput(ReportState *rpt)
{
	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:
			/* nothing to do */
			break;

		case REPORT_FORMAT_XML:
			appendStringInfoString(rpt->str,
			 "<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n");
			rpt->indent++;
			break;

		case REPORT_FORMAT_JSON:
			/* top-level structure is an array of plans */
			appendStringInfoChar(rpt->str, '[');
			rpt->grouping_stack = lcons_int(0, rpt->grouping_stack);
			rpt->indent++;
			break;

		case REPORT_FORMAT_YAML:
			rpt->grouping_stack = lcons_int(0, rpt->grouping_stack);
			break;
	}
}

/*
 * Emit the end-of-output boilerplate.
 */
void
ReportEndOutput(ReportState* rpt)
{
	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:
			/* nothing to do */
			break;

		case REPORT_FORMAT_XML:
			rpt->indent--;
			appendStringInfoString(rpt->str, "</explain>");
			break;

		case REPORT_FORMAT_JSON:
			rpt->indent--;
			appendStringInfoString(rpt->str, "\n]");
			rpt->grouping_stack = list_delete_first(rpt->grouping_stack);
			break;

		case REPORT_FORMAT_YAML:
			rpt->grouping_stack = list_delete_first(rpt->grouping_stack);
			break;
	}
}

/*
 * Put an appropriate separator between multiple plans
 */
void
ReportSeparatePlans(ReportState* rpt)
{
	switch (rpt->format)
	{
		case REPORT_FORMAT_TEXT:
			/* add a blank line */
			appendStringInfoChar(rpt->str, '\n');
			break;

		case REPORT_FORMAT_XML:
		case REPORT_FORMAT_JSON:
		case REPORT_FORMAT_YAML:
			/* nothing to do */
			break;
	}
}

/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML restricts tag names more than our other output formats, eg they can't
 * contain white space or slashes.  Replace invalid characters with dashes,
 * so that for example "I/O Read Time" becomes "I-O-Read-Time".
 */
static void
ReportXMLTag(const char *tagname, int flags, ReportState *rpt)
{
	const char *s;
	const char *valid = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";

	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoSpaces(rpt->str, 2 * rpt->indent);
	appendStringInfoCharMacro(rpt->str, '<');
	if ((flags & X_CLOSING) != 0)
		appendStringInfoCharMacro(rpt->str, '/');
	for (s = tagname; *s; s++)
		appendStringInfoChar(rpt->str, strchr(valid, *s) ? *s : '-');
	if ((flags & X_CLOSE_IMMEDIATE) != 0)
		appendStringInfoString(rpt->str, " /");
	appendStringInfoCharMacro(rpt->str, '>');
	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoCharMacro(rpt->str, '\n');
}

/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.  To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
static void
ReportJSONLineEnding(ReportState *rpt)
{
	Assert(rpt->format == REPORT_FORMAT_JSON);
	if (linitial_int(rpt->grouping_stack) != 0)
		appendStringInfoChar(rpt->str, ',');
	else
		linitial_int(rpt->grouping_stack) = 1;
	appendStringInfoChar(rpt->str, '\n');
}

/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabelled group, for which
 * it begins immediately after the "- " that introduces the group.  The first
 * property of the group appears on the same line as the opening "- ".
 */
static void
ReportYAMLLineStarting(ReportState *rpt)
{
	Assert(rpt->format == REPORT_FORMAT_YAML);
	if (linitial_int(rpt->grouping_stack) == 0)
	{
		linitial_int(rpt->grouping_stack) = 1;
	}
	else
	{
		appendStringInfoChar(rpt->str, '\n');
		appendStringInfoSpaces(rpt->str, rpt->indent * 2);
	}
}

/*
 * YAML is a superset of JSON; unfortunately, the YAML quoting rules are
 * ridiculously complicated -- as documented in sections 5.3 and 7.3.3 of
 * http://yaml.org/spec/1.2/spec.html -- so we chose to just quote everything.
 * Empty strings, strings with leading or trailing whitespace, and strings
 * containing a variety of special characters must certainly be quoted or the
 * output is invalid; and other seemingly harmless strings like "0xa" or
 * "true" must be quoted, lest they be interpreted as a hexadecimal or Boolean
 * constant rather than a string.
 */
static void
escape_yaml(StringInfo buf, const char *str)
{
	escape_json(buf, str);
}

/*
 * In text format, first line ends here
 */
void 
ReportNewLine(ReportState* rpt)
{
	if (rpt->format == REPORT_FORMAT_TEXT) {
                appendStringInfoChar(rpt->str, '\n');
        }
}

/*
 * Add a space to TEXT format output
 */
void 
ReportSpace(ReportState* rpt)
{
	if (rpt->format == REPORT_FORMAT_TEXT) {
		appendStringInfoSpaces(rpt->str, 1);
	}
}

/*
 * Add a space to TEXT format output
 */
void
ReportSpaces(unsigned short nr_spaces, ReportState* rpt)
{
        if (rpt->format == REPORT_FORMAT_TEXT) {
                appendStringInfoSpaces(rpt->str, nr_spaces);
        }
}

/*
 * Add text separator to TEXT format output
 */
void 
ReportSeparator(const char* text, ReportState* rpt)
{
	if (rpt->format == REPORT_FORMAT_TEXT) {
		appendStringInfo(rpt->str, "%s", text);
	}
}
