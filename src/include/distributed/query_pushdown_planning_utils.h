/*-------------------------------------------------------------------------
 * query_pushdown_planning_utils.h
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *    Function declarations used in query pushdown logic.
 *
 *-------------------------------------------------------------------------
 */

#ifndef QUERY_PUSHDOWN_PLANNING_UTILS
#define QUERY_PUSHDOWN_PLANNING_UTILS

#include "postgres.h"

#include "distributed/distributed_planner.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/errormessage.h"


extern DeferredErrorMessage * DeferErrorIfUnsupportedSubqueryPushdown(Query *
																	  originalQuery,
																	  PlannerRestrictionContext
																	  *
																	  plannerRestrictionContext);
extern DeferredErrorMessage * DeferErrorIfCannotPushdownSubquery(Query *subqueryTree,
																 bool
																 outerMostQueryHasLimit);
extern bool ShouldUseSubqueryPushDown(Query *originalQuery, Query *rewrittenQuery);
extern bool JoinTreeContainsSubquery(Query *query);
extern bool WhereClauseContainsSubquery(Query *query);
extern bool SafeToPushdownWindowFunction(Query *query, StringInfo *errorDetail);
extern List * QueryPushdownSqlTaskList(Query *query, uint64 jobId,
									   RelationRestrictionContext *
									   relationRestrictionContext,
									   List *prunedRelationShardList, TaskType taskType);
extern MultiNode * SubqueryMultiNodeTree(Query *originalQuery,
										 Query *queryTree,
										 PlannerRestrictionContext *
										 plannerRestrictionContext);


#endif /* QUERY_PUSHDOWN_PLANNING_UTILS_H */
