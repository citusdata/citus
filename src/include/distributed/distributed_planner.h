/*-------------------------------------------------------------------------
 *
 * distributed_planner.h
 *	  General Citus planner code.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_PLANNER_H
#define DISTRIBUTED_PLANNER_H

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "nodes/plannodes.h"

#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif

#include "distributed/citus_nodes.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"


/* values used by jobs and tasks which do not require identifiers */
#define INVALID_JOB_ID 0
#define INVALID_TASK_ID 0

#define CURSOR_OPT_FORCE_DISTRIBUTED 0x080000


/* level of planner calls */
extern int PlannerLevel;


typedef struct RelationRestrictionContext
{
	bool hasDistributedRelation;
	bool hasLocalRelation;
	bool allReferenceTables;
	List *relationRestrictionList;
} RelationRestrictionContext;


typedef struct RootPlanParams
{
	PlannerInfo *root;

	/*
	 * Copy of root->plan_params. root->plan_params is not preserved in
	 * relation_restriction_equivalence, so we need to create a copy.
	 */
	List *plan_params;
} RootPlanParams;

typedef struct RelationRestriction
{
	Index index;
	Oid relationId;
	bool distributedRelation;
	RangeTblEntry *rte;
	RelOptInfo *relOptInfo;
	PlannerInfo *plannerInfo;
	List *prunedShardIntervalList;

	/* list of RootPlanParams for all outer nodes */
	List *outerPlanParamsList;
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

typedef struct FastPathRestrictionContext
{
	bool fastPathRouterQuery;

	/*
	 * While calculating fastPathRouterQuery, we could sometimes be
	 * able to extract the distribution key value as well (such as when
	 * there are no prepared statements). Could be NULL when the distribution
	 * key contains parameter, so check for it before using.
	 */
	Const *distributionKeyValue;

	/*
	 * Set to true when distKey = Param; in the queryTree
	 */
	bool distributionKeyHasParam;
}FastPathRestrictionContext;

typedef struct PlannerRestrictionContext
{
	RelationRestrictionContext *relationRestrictionContext;
	JoinRestrictionContext *joinRestrictionContext;

	/*
	 * When the query is qualified for fast path, we don't have
	 * the RelationRestrictionContext and JoinRestrictionContext
	 * since those are dependent to calling standard_planner.
	 * Instead, we keep this struct to pass some extra information.
	 */
	FastPathRestrictionContext *fastPathRestrictionContext;
	bool hasSemiJoin;
	MemoryContext memoryContext;
} PlannerRestrictionContext;

typedef struct RelationShard
{
	CitusNode type;
	Oid relationId;
	uint64 shardId;
} RelationShard;

typedef struct RelationRowLock
{
	CitusNode type;
	Oid relationId;
	LockClauseStrength rowLockStrength;
} RelationRowLock;


typedef struct DistributedPlanningContext
{
	/* The parsed query that is given to the planner. It is a slightly modified
	 * to work with the standard_planner */
	Query *query;

	/* A copy of the original parsed query that is given to the planner. This
	 * doesn't contain most of the changes that are made to parse. There's one
	 * that change that is made for non fast path router queries though, which
	 * is the assigning of RTE identities using AssignRTEIdentities. This is
	 * NULL for non distributed plans, since those don't need it. */
	Query *originalQuery;

	/* the cursor options given to the planner */
	int cursorOptions;

	/* the ParamListInfo that is given to the planner */
	ParamListInfo boundParams;

	/* Plan created either by standard_planner or by FastPathPlanner */
	PlannedStmt *plan;

	/* Our custom restriction context */
	PlannerRestrictionContext *plannerRestrictionContext;
} DistributedPlanningContext;


/*
 * CitusCustomScanPath is injected into the planner during the master query planning phase
 * of the logical planner.
 * We call out to the standard planner to plan the master query part for the output of the
 * logical planner. This makes it easier to implement new sql features into the logical
 * planner by not having to manually implement the plan creation for the query on the
 * master.
 */
typedef struct CitusCustomScanPath
{
	CustomPath custom_path;

	/*
	 * Custom scan node computed by the citus planner that will produce the tuples for the
	 * path we are injecting during the planning of the master query
	 */
	CustomScan *remoteScan;
} CitusCustomScanPath;


extern PlannedStmt * distributed_planner(Query *parse, int cursorOptions,
										 ParamListInfo boundParams);
extern List * ExtractRangeTableEntryList(Query *query);
extern List * ExtractReferenceTableRTEList(List *rteList);
extern bool NeedsDistributedPlanning(Query *query);
extern struct DistributedPlan * GetDistributedPlan(CustomScan *node);
extern void multi_relation_restriction_hook(PlannerInfo *root, RelOptInfo *relOptInfo,
											Index restrictionIndex, RangeTblEntry *rte);
extern void multi_join_restriction_hook(PlannerInfo *root,
										RelOptInfo *joinrel,
										RelOptInfo *outerrel,
										RelOptInfo *innerrel,
										JoinType jointype,
										JoinPathExtraData *extra);
extern bool HasUnresolvedExternParamsWalker(Node *expression, ParamListInfo boundParams);
extern bool IsModifyCommand(Query *query);
extern void EnsurePartitionTableNotReplicated(Oid relationId);
extern Node * ResolveExternalParams(Node *inputNode, ParamListInfo boundParams);
extern bool IsMultiTaskPlan(struct DistributedPlan *distributedPlan);
extern RangeTblEntry * RemoteScanRangeTableEntry(List *columnNameList);
extern int GetRTEIdentity(RangeTblEntry *rte);
extern int32 BlessRecordExpression(Expr *expr);
extern void DissuadePlannerFromUsingPlan(PlannedStmt *plan);
extern PlannedStmt * FinalizePlan(PlannedStmt *localPlan,
								  struct DistributedPlan *distributedPlan);

#endif /* DISTRIBUTED_PLANNER_H */
