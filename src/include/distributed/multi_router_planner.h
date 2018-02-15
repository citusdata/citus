/*-------------------------------------------------------------------------
 *
 * multi_router_planner.h
 *
 * Declarations for public functions and types related to router planning.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_ROUTER_PLANNER_H
#define MULTI_ROUTER_PLANNER_H

#include "c.h"

#include "distributed/errormessage.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/distributed_planner.h"
#include "nodes/parsenodes.h"


/* reserved alias name for UPSERTs */
#define CITUS_TABLE_ALIAS "citus_table_alias"

extern bool EnableRouterExecution;

extern DistributedPlan * CreateRouterPlan(Query *originalQuery, Query *query,
										  RelationRestrictionContext *restrictionContext);
extern DistributedPlan * CreateModifyPlan(Query *originalQuery, Query *query,
										  PlannerRestrictionContext *
										  plannerRestrictionContext);
extern DeferredErrorMessage * PlanRouterQuery(Query *originalQuery,
											  RelationRestrictionContext *
											  restrictionContext,
											  List **placementList, uint64 *anchorShardId,
											  List **relationShardList, bool
											  replacePrunedQueryWithDummy,
											  bool *multiShardModifyQuery);
extern List * RouterInsertTaskList(Query *query, DeferredErrorMessage **planningError);
extern List * TargetShardIntervalsForQuery(Query *query,
										   RelationRestrictionContext *restrictionContext,
										   bool *multiShardQuery);
extern List * WorkersContainingAllShards(List *prunedShardIntervalsList);
extern List * IntersectPlacementList(List *lhsPlacementList, List *rhsPlacementList);
extern DeferredErrorMessage * ModifyQuerySupported(Query *queryTree, Query *originalQuery,
												   bool multiShardQuery);
extern List * ShardIntervalOpExpressions(ShardInterval *shardInterval, Index rteIndex);
extern RelationRestrictionContext * CopyRelationRestrictionContext(
	RelationRestrictionContext *oldContext);

extern Oid ExtractFirstDistributedTableId(Query *query);
extern RangeTblEntry * ExtractSelectRangeTableEntry(Query *query);
extern RangeTblEntry * ExtractInsertRangeTableEntry(Query *query);
extern RangeTblEntry * ExtractDistributedInsertValuesRTE(Query *query);
extern bool IsMultiRowInsert(Query *query);
extern void AddShardIntervalRestrictionToSelect(Query *subqery,
												ShardInterval *shardInterval);
extern bool UpdateOrDeleteQuery(Query *query);


#endif /* MULTI_ROUTER_PLANNER_H */
