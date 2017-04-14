/*-------------------------------------------------------------------------
 *
 * explain.h
 *	  prototypes for explain.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/explain.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAIN_H
#define EXPLAIN_H

#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "parser/parse_node.h"
#include "commands/report.h"

/* Hook for plugins to get control in ExplainOneQuery() */
typedef void (*ExplainOneQuery_hook_type) (Query *query,
													   int cursorOptions,
													   IntoClause *into,
													   ReportState *es,
													 const char *queryString,
													   ParamListInfo params);
extern PGDLLIMPORT ExplainOneQuery_hook_type ExplainOneQuery_hook;

/* Hook for plugins to get control in explain_get_index_name() */
typedef const char *(*explain_get_index_name_hook_type) (Oid indexId);
extern PGDLLIMPORT explain_get_index_name_hook_type explain_get_index_name_hook;


extern void ExplainQuery(ParseState *pstate, ExplainStmt *stmt, const char *queryString,
			 ParamListInfo params, QueryEnvironment *queryEnv, DestReceiver *dest);

extern TupleDesc ExplainResultDesc(ExplainStmt *stmt);

extern void ExplainOneUtility(Node *utilityStmt, IntoClause *into,
				  ReportState *es, const char *queryString,
				  ParamListInfo params, QueryEnvironment *queryEnv);

extern void ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into,
			   ReportState *es, const char *queryString,
			   ParamListInfo params, QueryEnvironment *queryEnv,
			   const instr_time *planduration);

extern void ExplainPrintPlan(ReportState *es, QueryDesc *queryDesc);
extern void ExplainPrintTriggers(ReportState *es, QueryDesc *queryDesc);

extern void ExplainBeginOutput(ReportState *es);
extern void ExplainEndOutput(ReportState *es);
extern void ExplainSeparatePlans(ReportState *es);
extern void show_buffer_usage(ReportState* es, const BufferUsage* usage);

#endif   /* EXPLAIN_H */
