/*-------------------------------------------------------------------------
 *
 * report.h
 *
 *        prototypes for report.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/report.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPORT_H
#define REPORT_H

#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "parser/parse_node.h"

typedef enum ReportFormat {
	REPORT_FORMAT_TEXT,
	REPORT_FORMAT_XML,
	REPORT_FORMAT_JSON,
	REPORT_FORMAT_YAML
} ReportFormat;

/*
 * The top level report state data
 */
typedef struct ReportState {
        /*
         * options
         */
        bool verbose;			/* be verbose */
        bool buffers;			/* print buffer usage */
        bool timing;			/* print detailed node timing */
	bool analyze;
	bool costs;
	bool summary;

	bool child;			/* true if parallel and child backend */
	bool inline_output;		/* allow dense output. Several fields are
					   concatened on one line with abbrviations */
	bool parallel;			/* true if parallel query */
	bool parallel_reported;		/* true if parallel backends already reported once */

	bool nonewline;			/* true if next node should avoid new line on report */
        /*
         * State for output formating   
         */
        StringInfo str;                 /* output buffer */
        StringInfo str2;                 /* output buffer */
        ReportFormat format;		/* format used to output progress */
        int indent;                     /* current indentation level */
        List* grouping_stack;           /* format-specific grouping state */

	MemoryContext memcontext;
        /*
         * State related to current plan/execution tree
         */
	PlannedStmt* pstmt;
        struct Plan* plan;
        struct PlanState* planstate;
        List* rtable;
        List* rtable_names;
        List* deparse_cxt;              /* context list for deparsing expressions */
        EState* es;                     /* Top level data */
        Bitmapset* printed_subplans;    /* ids of SubPlans we've printed */

	unsigned long disk_size;	/* PROGRESS command track on disk use for sorts,
				           stores, and hashes */
} ReportState;

/* OR-able flags for ReportXMLTag() */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4

extern ReportState* CreateReportState(int needed);
extern void FreeReportState(ReportState* prg);
extern int SetReportStateCosts(ReportState* prg, bool costs);

extern void ReportOpenGroup(const char *objtype, const char *labelname, bool labeled, ReportState *rpt);
extern void ReportCloseGroup(const char *objtype, const char *labelname, bool labeled, ReportState *rpt);

extern void ReportBeginOutput(ReportState *rpt);
extern void ReportEndOutput(ReportState* rpt);

extern void ReportBeginChildOutput(ReportState *rpt);
extern void ReportEndChildOutput(ReportState* rpt);

extern void ReportSeparatePlans(ReportState* rpt);

extern void ReportProperty(const char *qlabel, const char *value, bool numeric, ReportState *rpt, bool newline);
extern void ReportProperties(Plan* plan, PlanInfo* info, const char* plan_name, const char* relationship, ReportState* rpt);
extern void ReportPropertyList(const char *qlabel, List *data, ReportState *rpt);
extern void ReportPropertyListNested(const char *qlabel, List *data, ReportState *rpt);
extern void ReportPropertyText(const char *qlabel, const char *value, ReportState* rpt);
extern void ReportPropertyInteger(const char *qlabel, int value, ReportState *rpt);
extern void ReportPropertyIntegerNoNewLine(const char *qlabel, int value, ReportState *rpt);
extern void ReportPropertyLong(const char *qlabel, long value, ReportState *rpt);
extern void ReportPropertyLongNoNewLine(const char *qlabel, long value, ReportState *rpt);
extern void ReportPropertyFloat(const char *qlabel, double value, int ndigits, ReportState *rpt);
extern void ReportPropertyBool(const char *qlabel, bool value, ReportState *rpt);

extern void ReportText(const char* label, const char* value, ReportState* rpt);
extern void ReportTextNoNewLine(const char* label, const char* value, ReportState* rpt);

extern void ReportNewLine(ReportState* rpt);
extern void ReportSpace(ReportState* rpt);
extern void ReportSpaces(unsigned short nr_spaces, ReportState* rpt);
extern void ReportSeparator(const char* separator, ReportState* rpt);

extern void ReportDummyGroup(const char *objtype, const char *labelname, ReportState *rpt);

extern void ReportScanTarget(Scan *plan, ReportState *es);
extern void ReportTargetRel(Plan *plan, Index rti, ReportState *es);
extern void ReportIndexScanDetails(Oid indexid, ScanDirection indexorderdir, ReportState *es);
extern void ReportModifyTarget(ModifyTable *plan, ReportState *es);

extern bool ReportHasChildren(Plan* plan, PlanState* planstate);

extern void ReportQueryText(ReportState *es, QueryDesc *queryDesc);
extern bool ReportPreScanNode(PlanState *planstate, Bitmapset **rels_used);

typedef void (*functionNode)(PlanState* planstate, List* ancestors, const char* relationship,
        const char* plan_name, ReportState* ps);

extern void ReportMemberNodes(List *plans, PlanState **planstates, List *ancestors, ReportState *es, functionNode fn);
extern void ReportSubPlans(List *plans, List *ancestors, const char *relationship, ReportState *es, functionNode fn);
extern void ReportCustomChildren(CustomScanState *css, List *ancestors, ReportState *es, functionNode fn);

extern void show_expression(Node *node, const char *qlabel, PlanState *planstate, List *ancestors, bool useprefix, ReportState *es);
extern void show_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors, bool useprefix, ReportState *es);
extern void show_scan_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors, ReportState *es);
extern void show_upper_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors, ReportState *es);
extern void show_sort_keys(SortState *sortstate, List *ancestors, ReportState *es);
extern void show_merge_append_keys(MergeAppendState *mstate, List *ancestors, ReportState *es);
extern void show_agg_keys(AggState *astate, List *ancestors, ReportState *es);
extern void show_grouping_sets(PlanState *planstate, Agg *agg, List *ancestors, ReportState *es);
extern void show_grouping_set_keys(PlanState *planstate, Agg *aggnode, Sort *sortnode,
		List *context, bool useprefix, List *ancestors, ReportState *es);
extern void show_group_keys(GroupState *gstate, List *ancestors, ReportState *es);
extern void show_sort_group_keys(PlanState *planstate, const char *qlabel, int nkeys, AttrNumber *keycols,
		Oid *sortOperators, Oid *collations, bool *nullsFirst, List *ancestors, ReportState *es);
extern void show_sortorder_options(StringInfo buf, Node *sortexpr, Oid sortOperator, Oid collation, bool nullsFirst);
extern void show_tablesample(TableSampleClause *tsc, PlanState *planstate, List *ancestors, ReportState *es);
extern void show_sort_info(SortState *sortstate, ReportState *es);
extern void show_hash_info(HashState *hashstate, ReportState *es);
extern void show_tidbitmap_info(BitmapHeapScanState *planstate, ReportState *es);
extern void show_instrumentation_count(const char *qlabel, int which, PlanState *planstate, ReportState *es);
extern void show_foreignscan_info(ForeignScanState *fsstate, ReportState *es);
extern const char *explain_get_index_name(Oid indexId);
extern void show_modifytable_info(ModifyTableState *mtstate, List *ancestors, ReportState *es);
extern void show_plan_tlist(PlanState *planstate, List *ancestors, ReportState *es);
extern void show_control_qual(PlanState *planstate, List *ancestors, ReportState *es);

#endif
