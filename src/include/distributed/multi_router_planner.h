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

#include "nodes/parsenodes.h"

#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"


/* reserved alias name for UPSERTs */
#define CITUS_TABLE_ALIAS "citus_table_alias"

extern bool EnableRouterExecution;
extern bool EnableFastPathRouterPlanner;

extern bool EnableNonColocatedRouterQueryPushdown;

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
											  Const **partitionValueConst,
											  bool *containOnlyLocalTable);
extern List * RelationShardListForShardIntervalList(List *shardIntervalList,
													bool *shardsPresent);
extern List * CreateTaskPlacementListForShardIntervals(List *shardIntervalList,
													   bool shardsPresent,
													   bool generateDummyPlacement,
													   bool hasLocalRelation);
extern List * RouterInsertTaskList(Query *query, bool parametersInQueryResolved,
								   DeferredErrorMessage **planningError);
extern Const * ExtractInsertPartitionKeyValue(Query *query);
extern List * TargetShardIntervalsForRestrictInfo(RelationRestrictionContext *
												  restrictionContext,
												  bool *multiShardQuery,
												  Const **partitionValueConst);
extern List * PlacementsForWorkersContainingAllShards(List *shardIntervalListList);
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
extern RangeTblEntry * ExtractResultRelationRTEOrError(Query *query);
extern RangeTblEntry * ExtractDistributedInsertValuesRTE(Query *query);
extern bool IsMultiRowInsert(Query *query);
extern void AddPartitionKeyNotNullFilterToSelect(Query *subqery);
extern bool UpdateOrDeleteOrMergeQuery(Query *query);
extern bool IsMergeQuery(Query *query);

extern uint64 GetAnchorShardId(List *relationShardList);
extern List * TargetShardIntervalForFastPathQuery(Query *query,
												  bool *isMultiShardQuery,
												  Const *inputDistributionKeyValue,
												  Const **outGoingPartitionValueConst);
extern void GenerateSingleShardRouterTaskList(Job *job,
											  List *relationShardList,
											  List *placementList,
											  uint64 shardId,
											  bool isLocalTableModification);

/*
 * FastPathPlanner is a subset of router planner, that's why we prefer to
 * keep the external function here.
 */extern PlannedStmt * GeneratePlaceHolderPlannedStmt(Query *parse);

extern PlannedStmt * FastPathPlanner(Query *originalQuery, Query *parse, ParamListInfo
									 boundParams);
extern bool FastPathRouterQuery(Query *query, Node **distributionKeyValue);
extern bool JoinConditionIsOnFalse(List *relOptInfo);
extern Oid ResultRelationOidForQuery(Query *query);
extern DeferredErrorMessage * TargetlistAndFunctionsSupported(Oid resultRelationId,
															  FromExpr *joinTree,
															  Node *quals,
															  List *targetList,
															  CmdType commandType,
															  List *returningList);
extern bool NodeIsFieldStore(Node *node);
extern bool TargetEntryChangesValue(TargetEntry *targetEntry, Var *column,
									FromExpr *joinTree);
extern bool MasterIrreducibleExpression(Node *expression, bool *varArgument,
										bool *badCoalesce);
extern bool HasDangerousJoinUsing(List *rtableList, Node *jtnode);
extern Job * RouterJob(Query *originalQuery,
					   PlannerRestrictionContext *plannerRestrictionContext,
					   DeferredErrorMessage **planningError);
extern bool ContainsOnlyLocalTables(RTEListProperties *rteProperties);
extern RangeTblEntry * ExtractSourceResultRangeTableEntry(Query *query);

#endif /* MULTI_ROUTER_PLANNER_H */
