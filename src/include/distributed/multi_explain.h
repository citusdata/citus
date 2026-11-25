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

#include "tuple_destination.h"

#include "executor/executor.h"

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
extern List * ExplainAnalyzeTaskList(CitusScanState *scanState,
									 TupleDestination *defaultTupleDest, TupleDesc
									 tupleDesc, ParamListInfo params);
extern void ExecuteSubPlanForExplainAnalyze(CitusScanState *scanState,
											DistributedSubPlan *subPlan,
											DestReceiver *dest, int workerCount,
											bool writeLocalFile);
extern bool BeginSubPlanInstrumentation(QueryDesc *queryDesc, int eflags);
extern void AttachSubPlanReports(QueryDesc *queryDesc);
extern void SaveSubPlanRowCounts(QueryDesc *queryDesc);
extern bool RequestedForExplainAnalyze(CitusScanState *node);
extern void ResetExplainAnalyzeData(List *taskList);

#endif /* MULTI_EXPLAIN_H */
