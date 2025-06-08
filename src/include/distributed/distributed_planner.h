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

#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"

#include "pg_version_constants.h"

#include "distributed/citus_nodes.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"


/* values used by jobs and tasks which do not require identifiers */
#define INVALID_JOB_ID 0
#define INVALID_TASK_ID 0

#define CURSOR_OPT_FORCE_DISTRIBUTED 0x080000

#define MAX_ANALYZE_OUTPUT 32

/* level of planner calls */
extern int PlannerLevel;


typedef struct RelationRestrictionContext
{
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
	bool citusTable;
	RangeTblEntry *rte;
	RelOptInfo *relOptInfo;
	PlannerInfo *plannerInfo;

	/* list of RootPlanParams for all outer nodes */
	List *outerPlanParamsList;

	/* list of translated vars, this is copied from postgres since it gets deleted on postgres*/
	List *translatedVars;
} RelationRestriction;

typedef struct JoinRestrictionContext
{
	List *joinRestrictionList;
	bool hasSemiJoin;
	bool hasOuterJoin;
} JoinRestrictionContext;

typedef struct JoinRestriction
{
	JoinType joinType;
	List *joinRestrictInfoList;
	PlannerInfo *plannerInfo;
	Relids innerrelRelids;
	Relids outerrelRelids;
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
} FastPathRestrictionContext;

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


/*
 * Parameters to be set according to range table entries of a query.
 */
typedef struct RTEListProperties
{
	bool hasPostgresLocalTable;

	bool hasReferenceTable;
	bool hasCitusLocalTable;

	/* includes hash, single-shard, append and range partitioned tables */
	bool hasDistributedTable;

	/*
	 * Effectively, hasDistributedTable is equal to
	 *  "hasDistTableWithShardKey || hasSingleShardDistTable".
	 *
	 * We provide below two for the callers that want to know what kind of
	 * distributed tables that given query has references to.
	 */
	bool hasDistTableWithShardKey;
	bool hasSingleShardDistTable;

	/* union of hasReferenceTable, hasCitusLocalTable and hasDistributedTable */
	bool hasCitusTable;

	bool hasMaterializedView;
} RTEListProperties;


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
 * CitusCustomScanPath is injected into the planner during the combine query planning
 * phase of the logical planner.
 *
 * We call out to the standard planner to plan the combine query part for the output of
 * the logical planner. This makes it easier to implement new sql features into the
 * logical planner by not having to manually implement the plan creation for the combine
 * query on the coordinator..
 */
typedef struct CitusCustomScanPath
{
	CustomPath custom_path;

	/*
	 * Custom scan node computed by the citus planner that will produce the tuples for the
	 * path we are injecting during the planning of the combine query
	 */
	CustomScan *remoteScan;
} CitusCustomScanPath;


extern PlannedStmt * distributed_planner(Query *parse,
										 const char *query_string,
										 int cursorOptions,
										 ParamListInfo boundParams);


/*
 * Common hint message to workaround using postgres local and citus local tables
 * in distributed queries
 */
#define LOCAL_TABLE_SUBQUERY_CTE_HINT \
	"Use CTE's or subqueries to select from local tables and use them in joins"

extern List * ExtractRangeTableEntryList(Query *query);
extern bool NeedsDistributedPlanning(Query *query);
extern List * TranslatedVarsForRteIdentity(int rteIdentity);
extern struct DistributedPlan * GetDistributedPlan(CustomScan *node);
extern void multi_relation_restriction_hook(PlannerInfo *root, RelOptInfo *relOptInfo,
											Index restrictionIndex, RangeTblEntry *rte);
extern void multi_get_relation_info_hook(PlannerInfo *root, Oid relationObjectId, bool
										 inhparent, RelOptInfo *rel);
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
extern bool GetOriginalInh(RangeTblEntry *rte);
extern LOCKMODE GetQueryLockMode(Query *query);
extern int32 BlessRecordExpression(Expr *expr);
extern void DissuadePlannerFromUsingPlan(PlannedStmt *plan);
extern PlannedStmt * FinalizePlan(PlannedStmt *localPlan,
								  struct DistributedPlan *distributedPlan);
extern bool ContainsSingleShardTable(Query *query);
extern RTEListProperties * GetRTEListPropertiesForQuery(Query *query);


extern struct DistributedPlan * CreateDistributedPlan(uint64 planId,
													  bool allowRecursivePlanning,
													  Query *originalQuery,
													  Query *query,
													  ParamListInfo boundParams,
													  bool hasUnresolvedParams,
													  PlannerRestrictionContext *
													  plannerRestrictionContext);

#endif /* DISTRIBUTED_PLANNER_H */
