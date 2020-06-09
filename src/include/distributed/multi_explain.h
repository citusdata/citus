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

/* Config variables managed via guc.c to explain distributed query plans */
extern bool ExplainDistributedQueries;
extern bool ExplainAllTasks;

extern void FreeSavedExplainPlan(void);
extern void CitusExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
								 ExplainState *es, const char *queryString, ParamListInfo
								 params,
								 QueryEnvironment *queryEnv);
extern List * ExplainAnalyzeTaskList(List *originalTaskList,
									 TupleDestination *defaultTupleDest, TupleDesc
									 tupleDesc, ParamListInfo params);
extern bool RequestedForExplainAnalyze(CitusScanState *node);

#endif /* MULTI_EXPLAIN_H */
