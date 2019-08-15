/*-------------------------------------------------------------------------
 * query_pushdown_planning.h
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *    Function declarations used in query pushdown logic.
 *
 *-------------------------------------------------------------------------
 */

#ifndef QUERY_PUSHDOWN_PLANNING
#define QUERY_PUSHDOWN_PLANNING

#include "postgres.h"

#include "distributed/distributed_planner.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/errormessage.h"


/* Config variables managed via guc.c */
extern bool SubqueryPushdown;


extern bool ShouldUseSubqueryPushDown(Query *originalQuery, Query *rewrittenQuery);
extern bool JoinTreeContainsSubquery(Query *query);
extern bool JoinTreeContainsWithoutFrom(Query *query);
extern bool WhereClauseContainsSubquery(Query *query);
extern bool SafeToPushdownWindowFunction(Query *query, StringInfo *errorDetail);
extern MultiNode * SubqueryMultiNodeTree(Query *originalQuery,
										 Query *queryTree,
										 PlannerRestrictionContext *
										 plannerRestrictionContext);
extern DeferredErrorMessage * DeferErrorIfUnsupportedSubqueryPushdown(Query *
																	  originalQuery,
																	  PlannerRestrictionContext
																	  *
																	  plannerRestrictionContext);
extern DeferredErrorMessage * DeferErrorIfCannotPushdownSubquery(Query *subqueryTree,
																 bool
																 outerMostQueryHasLimit);
extern DeferredErrorMessage * DeferErrorIfUnsupportedUnionQuery(Query *queryTree);


#endif /* QUERY_PUSHDOWN_PLANNING_H */
