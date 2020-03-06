/*-------------------------------------------------------------------------
 *
 * multi_router_planner.h
 *
 * Declarations for public functions and types related to router planning.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_ROUTER_PLANNER_H
#define MULTI_ROUTER_PLANNER_H

#include "c.h"

#include "distributed/errormessage.h"
#include "distributed/log_utils.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/distributed_planner.h"
#include "nodes/parsenodes.h"


/* reserved alias name for UPSERTs */
#define CITUS_TABLE_ALIAS "citus_table_alias"

extern bool EnableRouterExecution;
extern bool EnableFastPathRouterPlanner;

extern DistributedPlan * CreateRouterPlan(Query *originalQuery, Query *query,
										  PlannerRestrictionContext *
										  plannerRestrictionContext);
extern DistributedPlan * CreateModifyPlan(Query *originalQuery, Query *query,
										  PlannerRestrictionContext *
										  plannerRestrictionContext);
extern DeferredErrorMessage * PlanRouterQuery(Query *originalQuery,
											  PlannerRestrictionContext *
											  plannerRestrictionContext,
											  List **placementList, uint64 *anchorShardId,
											  List **relationShardList,
											  List **prunedShardIntervalListList,
											  bool replacePrunedQueryWithDummy,
											  bool *multiShardModifyQuery,
											  Const **partitionValueConst);
extern List * RelationShardListForShardIntervalList(List *shardIntervalList,
													bool *shardsPresent);
extern List * FindRouterWorkerList(List *shardIntervalList, bool shardsPresent,
								   bool replacePrunedQueryWithDummy);
extern List * RouterInsertTaskList(Query *query, bool parametersInQueryResolved,
								   DeferredErrorMessage **planningError);
extern Const * ExtractInsertPartitionKeyValue(Query *query);
extern List * TargetShardIntervalsForRestrictInfo(RelationRestrictionContext *
												  restrictionContext,
												  bool *multiShardQuery,
												  Const **partitionValueConst);
extern List * WorkersContainingAllShards(List *prunedShardIntervalsList);
extern List * IntersectPlacementList(List *lhsPlacementList, List *rhsPlacementList);
extern DeferredErrorMessage * ModifyQuerySupported(Query *queryTree, Query *originalQuery,
												   bool multiShardQuery,
												   PlannerRestrictionContext *
												   plannerRestrictionContext);
extern DeferredErrorMessage * ErrorIfOnConflictNotSupported(Query *queryTree);
extern List * ShardIntervalOpExpressions(ShardInterval *shardInterval, Index rteIndex);
extern RelationRestrictionContext * CopyRelationRestrictionContext(
	RelationRestrictionContext *oldContext);

extern Oid ExtractFirstCitusTableId(Query *query);
extern RangeTblEntry * ExtractSelectRangeTableEntry(Query *query);
extern Oid ModifyQueryResultRelationId(Query *query);
extern RangeTblEntry * ExtractResultRelationRTE(Query *query);
extern RangeTblEntry * ExtractDistributedInsertValuesRTE(Query *query);
extern bool IsMultiRowInsert(Query *query);
extern void AddShardIntervalRestrictionToSelect(Query *subqery,
												ShardInterval *shardInterval);
extern bool UpdateOrDeleteQuery(Query *query);
extern List * WorkersContainingAllShards(List *prunedShardIntervalsList);

extern uint64 GetAnchorShardId(List *relationShardList);
extern List * TargetShardIntervalForFastPathQuery(Query *query,
												  bool *isMultiShardQuery,
												  Const *inputDistributionKeyValue,
												  Const **outGoingPartitionValueConst);
extern void GenerateSingleShardRouterTaskList(Job *job,
											  List *relationShardList,
											  List *placementList, uint64 shardId);

/*
 * FastPathPlanner is a subset of router planner, that's why we prefer to
 * keep the external function here.
 */extern PlannedStmt * GeneratePlaceHolderPlannedStmt(Query *parse);

extern PlannedStmt * FastPathPlanner(Query *originalQuery, Query *parse, ParamListInfo
									 boundParams);
extern bool FastPathRouterQuery(Query *query, Node **distributionKeyValue);


#endif /* MULTI_ROUTER_PLANNER_H */
