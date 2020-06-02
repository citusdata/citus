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

/* Config variables managed via guc.c to explain distributed query plans */
extern bool ExplainDistributedQueries;
extern bool ExplainAllTasks;
extern bool ExplainWorkerQuery;
extern bool ExplainNetworkStats;


extern void InstallExplainAnalyzeHooks(List *taskList);
extern bool RequestedForExplainAnalyze(CustomScanState *node);
extern void CitusExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
								 ExplainState *es, const char *queryString, ParamListInfo
								 params,
								 QueryEnvironment *queryEnv);
extern bool ShouldSaveWorkerQueryExplainAnalyze(QueryDesc *queryDesc);
extern void SaveQueryExplainAnalyze(QueryDesc *queryDesc);


#endif /* MULTI_EXPLAIN_H */
