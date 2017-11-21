/*-------------------------------------------------------------------------
 *
 * distributed_planner.h
 *	  General Citus planner code.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_PLANNER_H
#define DISTRIBUTED_PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

#include "distributed/citus_nodes.h"


/* values used by jobs and tasks which do not require identifiers */
#define INVALID_JOB_ID 0
#define INVALID_TASK_ID 0
#define MULTI_TASK_QUERY_INFO_OFF 0  /* do not log multi-task queries */

typedef struct RelationRestrictionContext
{
	bool hasDistributedRelation;
	bool hasLocalRelation;
	bool allReferenceTables;
	List *relationRestrictionList;
} RelationRestrictionContext;

typedef struct RelationRestriction
{
	Index index;
	Oid relationId;
	bool distributedRelation;
	RangeTblEntry *rte;
	RelOptInfo *relOptInfo;
	PlannerInfo *plannerInfo;
	PlannerInfo *parentPlannerInfo;
	List *parentPlannerParamList;
	List *prunedShardIntervalList;
} RelationRestriction;

typedef struct JoinRestrictionContext
{
	List *joinRestrictionList;
} JoinRestrictionContext;

typedef struct JoinRestriction
{
	JoinType joinType;
	List *joinRestrictInfoList;
	PlannerInfo *plannerInfo;
	RelOptInfo *innerrel;
	RelOptInfo *outerrel;
} JoinRestriction;

typedef struct PlannerRestrictionContext
{
	RelationRestrictionContext *relationRestrictionContext;
	JoinRestrictionContext *joinRestrictionContext;
	MemoryContext memoryContext;
} PlannerRestrictionContext;

typedef struct RelationShard
{
	CitusNode type;
	Oid relationId;
	uint64 shardId;
} RelationShard;


extern PlannedStmt * distributed_planner(Query *parse, int cursorOptions,
										 ParamListInfo boundParams);
extern struct DistributedPlan * GetDistributedPlan(CustomScan *node);
extern void multi_relation_restriction_hook(PlannerInfo *root, RelOptInfo *relOptInfo,
											Index index, RangeTblEntry *rte);
extern void multi_join_restriction_hook(PlannerInfo *root,
										RelOptInfo *joinrel,
										RelOptInfo *outerrel,
										RelOptInfo *innerrel,
										JoinType jointype,
										JoinPathExtraData *extra);
extern bool NeedsDistributedPlanning(Query *queryTree);
extern bool IsModifyCommand(Query *query);
extern bool IsUpdateOrDelete(struct DistributedPlan *distributedPlan);
extern bool IsModifyDistributedPlan(struct DistributedPlan *distributedPlan);
extern bool IsMultiTaskPlan(struct DistributedPlan *distributedPlan);
extern bool IsMultiShardModifyPlan(struct DistributedPlan *distributedPlan);
extern bool ContainsResultFunction(Node *node);
extern RangeTblEntry * RemoteScanRangeTableEntry(List *columnNameList);


extern int GetRTEIdentity(RangeTblEntry *rte);

#endif /* DISTRIBUTED_PLANNER_H */
