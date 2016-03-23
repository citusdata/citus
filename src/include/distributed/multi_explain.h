/*-------------------------------------------------------------------------
 *
 * multi_explain.h
 *	  Explain support for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_EXPLAIN_H
#define MULTI_EXPLAIN_H

#include "executor/executor.h"

/* Config variables managed via guc.c to explain distributed query plans */
extern bool ExplainMultiLogicalPlan;
extern bool ExplainMultiPhysicalPlan;

extern void MultiExplainOneQuery(Query *query, IntoClause *into, ExplainState *es,
								 const char *queryString, ParamListInfo params);

#endif /* MULTI_EXPLAIN_H */
