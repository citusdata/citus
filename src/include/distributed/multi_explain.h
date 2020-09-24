/*-------------------------------------------------------------------------
 *
 * multi_explain.h
 *	  Explain support for Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_EXPLAIN_H
#define MULTI_EXPLAIN_H

#include "executor/executor.h"
#include "tuple_destination.h"

typedef enum
{
	EXPLAIN_ANALYZE_SORT_BY_TIME = 0,
	EXPLAIN_ANALYZE_SORT_BY_TASK_ID = 1
} ExplainAnalyzeSortMethods;

/* Config variables managed via guc.c to explain distributed query plans */
extern bool ExplainDistributedQueries;
extern bool ExplainAllTasks;
extern int ExplainAnalyzeSortMethod;

extern void FreeSavedExplainPlan(void);
extern void CitusExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
								 ExplainState *es, const char *queryString, ParamListInfo
								 params,
								 QueryEnvironment *queryEnv);
extern List * ExplainAnalyzeTaskList(List *originalTaskList,
									 TupleDestination *defaultTupleDest, TupleDesc
									 tupleDesc, ParamListInfo params);
extern bool RequestedForExplainAnalyze(CitusScanState *node);
extern void ResetExplainAnalyzeData(List *taskList);

#endif /* MULTI_EXPLAIN_H */
