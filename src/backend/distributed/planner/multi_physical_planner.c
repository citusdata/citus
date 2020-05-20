/*-------------------------------------------------------------------------
 *
 * multi_physical_planner.c
 *	  Routines for creating physical plans from given multi-relational algebra
 *	  trees.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include <math.h>
#include <stdint.h>

#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/skey.h"
#include "access/xlog.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "distributed/backend_data.h"
#include "distributed/listutils.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/log_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_pruning.h"
#include "distributed/task_tracker.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#else
#include "nodes/relation.h"
#include "optimizer/var.h"
#endif
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/typcache.h"


/* Policy to use when assigning tasks to worker nodes */
int TaskAssignmentPolicy = TASK_ASSIGNMENT_GREEDY;
bool EnableUniqueJobIds = true;


/*
 * OperatorCache is used for caching operator identifiers for given typeId,
 * accessMethodId and strategyNumber. It is initialized to empty list as
 * there are no items in the cache.
 */
static List *OperatorCache = NIL;


/* context passed down in AddAnyValueAggregates mutator */
typedef struct AddAnyValueAggregatesContext
{
	/* SortGroupClauses corresponding to the GROUP BY clause */
	List *groupClauseList;

	/* TargetEntry's to which the GROUP BY clauses refer */
	List *groupByTargetEntryList;

	/*
	 * haveNonVarGrouping is true if there are expressions in the
	 * GROUP BY target entries. We use this as an optimisation to
	 * skip expensive checks when possible.
	 */
	bool haveNonVarGrouping;
} AddAnyValueAggregatesContext;


/* Local functions forward declarations for job creation */
static Job * BuildJobTree(MultiTreeRoot *multiTree);
static MultiNode * LeftMostNode(MultiTreeRoot *multiTree);
static Oid RangePartitionJoinBaseRelationId(MultiJoin *joinNode);
static MultiTable * FindTableNode(MultiNode *multiNode, int rangeTableId);
static Query * BuildJobQuery(MultiNode *multiNode, List *dependentJobList);
static Query * BuildReduceQuery(MultiExtendedOp *extendedOpNode, List *dependentJobList);
static List * BaseRangeTableList(MultiNode *multiNode);
static List * QueryTargetList(MultiNode *multiNode);
static List * TargetEntryList(List *expressionList);
static Node * AddAnyValueAggregates(Node *node, AddAnyValueAggregatesContext *context);
static List * QueryGroupClauseList(MultiNode *multiNode);
static List * QuerySelectClauseList(MultiNode *multiNode);
static List * QueryJoinClauseList(MultiNode *multiNode);
static List * QueryFromList(List *rangeTableList);
static Node * QueryJoinTree(MultiNode *multiNode, List *dependentJobList,
							List **rangeTableList);
static RangeTblEntry * JoinRangeTableEntry(JoinExpr *joinExpr, List *dependentJobList,
										   List *rangeTableList);
static int ExtractRangeTableId(Node *node);
static void ExtractColumns(RangeTblEntry *rangeTableEntry, int rangeTableId,
						   List *dependentJobList, List **columnNames, List **columnVars);
static RangeTblEntry * DerivedRangeTableEntry(MultiNode *multiNode, List *columnNames,
											  List *tableIdList, List *funcColumnNames,
											  List *funcColumnTypes,
											  List *funcColumnTypeMods,
											  List *funcCollations);

static List * DerivedColumnNameList(uint32 columnCount, uint64 generatingJobId);
static Query * BuildSubqueryJobQuery(MultiNode *multiNode);
static void UpdateAllColumnAttributes(Node *columnContainer, List *rangeTableList,
									  List *dependentJobList);
static void UpdateColumnAttributes(Var *column, List *rangeTableList,
								   List *dependentJobList);
static Index NewTableId(Index originalTableId, List *rangeTableList);
static AttrNumber NewColumnId(Index originalTableId, AttrNumber originalColumnId,
							  RangeTblEntry *newRangeTableEntry, List *dependentJobList);
static Job * JobForRangeTable(List *jobList, RangeTblEntry *rangeTableEntry);
static Job * JobForTableIdList(List *jobList, List *searchedTableIdList);
static List * ChildNodeList(MultiNode *multiNode);
static Job * BuildJob(Query *jobQuery, List *dependentJobList);
static MapMergeJob * BuildMapMergeJob(Query *jobQuery, List *dependentJobList,
									  Var *partitionKey, PartitionType partitionType,
									  Oid baseRelationId,
									  BoundaryNodeJobType boundaryNodeJobType);
static uint32 HashPartitionCount(void);
static ArrayType * SplitPointObject(ShardInterval **shardIntervalArray,
									uint32 shardIntervalCount);

/* Local functions forward declarations for task list creation and helper functions */
static bool DistributedPlanRouterExecutable(DistributedPlan *distributedPlan);
static Job * BuildJobTreeTaskList(Job *jobTree,
								  PlannerRestrictionContext *plannerRestrictionContext);
static bool IsInnerTableOfOuterJoin(RelationRestriction *relationRestriction);
static void ErrorIfUnsupportedShardDistribution(Query *query);
static Task * QueryPushdownTaskCreate(Query *originalQuery, int shardIndex,
									  RelationRestrictionContext *restrictionContext,
									  uint32 taskId,
									  TaskType taskType,
									  bool modifyRequiresMasterEvaluation);
static bool ShardIntervalsEqual(FmgrInfo *comparisonFunction,
								Oid collation,
								ShardInterval *firstInterval,
								ShardInterval *secondInterval);
static List * SqlTaskList(Job *job);
static bool DependsOnHashPartitionJob(Job *job);
static uint32 AnchorRangeTableId(List *rangeTableList);
static List * BaseRangeTableIdList(List *rangeTableList);
static List * AnchorRangeTableIdList(List *rangeTableList, List *baseRangeTableIdList);
static void AdjustColumnOldAttributes(List *expressionList);
static List * RangeTableFragmentsList(List *rangeTableList, List *whereClauseList,
									  List *dependentJobList);
static OperatorCacheEntry * LookupOperatorByType(Oid typeId, Oid accessMethodId,
												 int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static List * FragmentCombinationList(List *rangeTableFragmentsList, Query *jobQuery,
									  List *dependentJobList);
static JoinSequenceNode * JoinSequenceArray(List *rangeTableFragmentsList,
											Query *jobQuery, List *dependentJobList);
static bool PartitionedOnColumn(Var *column, List *rangeTableList,
								List *dependentJobList);
static void CheckJoinBetweenColumns(OpExpr *joinClause);
static List * FindRangeTableFragmentsList(List *rangeTableFragmentsList, int taskId);
static bool JoinPrunable(RangeTableFragment *leftFragment,
						 RangeTableFragment *rightFragment);
static ShardInterval * FragmentInterval(RangeTableFragment *fragment);
static StringInfo FragmentIntervalString(ShardInterval *fragmentInterval);
static List * DataFetchTaskList(uint64 jobId, uint32 taskIdIndex, List *fragmentList);
static StringInfo DatumArrayString(Datum *datumArray, uint32 datumCount, Oid datumTypeId);
static List * BuildRelationShardList(List *rangeTableList, List *fragmentList);
static void UpdateRangeTableAlias(List *rangeTableList, List *fragmentList);
static Alias * FragmentAlias(RangeTblEntry *rangeTableEntry,
							 RangeTableFragment *fragment);
static uint64 AnchorShardId(List *fragmentList, uint32 anchorRangeTableId);
static List * PruneSqlTaskDependencies(List *sqlTaskList);
static List * AssignTaskList(List *sqlTaskList);
static bool HasMergeTaskDependencies(List *sqlTaskList);
static List * GreedyAssignTaskList(List *taskList);
static Task * GreedyAssignTask(WorkerNode *workerNode, List *taskList,
							   List *activeShardPlacementLists);
static List * ReorderAndAssignTaskList(List *taskList,
									   List * (*reorderFunction)(Task *, List *));
static int CompareTasksByShardId(const void *leftElement, const void *rightElement);
static List * ActiveShardPlacementLists(List *taskList);
static List * ActivePlacementList(List *placementList);
static List * LeftRotateList(List *list, uint32 rotateCount);
static List * FindDependentMergeTaskList(Task *sqlTask);
static List * AssignDualHashTaskList(List *taskList);
static void AssignDataFetchDependencies(List *taskList);
static uint32 TaskListHighestTaskId(List *taskList);
static List * MapTaskList(MapMergeJob *mapMergeJob, List *filterTaskList);
static StringInfo CreateMapQueryString(MapMergeJob *mapMergeJob, Task *filterTask,
									   char *partitionColumnName);
static char * ColumnName(Var *column, List *rangeTableList);
static List * MergeTaskList(MapMergeJob *mapMergeJob, List *mapTaskList,
							uint32 taskIdIndex);
static StringInfo ColumnNameArrayString(uint32 columnCount, uint64 generatingJobId);
static StringInfo ColumnTypeArrayString(List *targetEntryList);
static StringInfo MergeTableQueryString(uint32 taskIdIndex, List *targetEntryList);
static StringInfo IntermediateTableQueryString(uint64 jobId, uint32 taskIdIndex,
											   Query *reduceQuery);
static uint32 FinalTargetEntryCount(List *targetEntryList);
static bool CoPlacedShardIntervals(ShardInterval *firstInterval,
								   ShardInterval *secondInterval);


/*
 * CreatePhysicalDistributedPlan is the entry point for physical plan generation. The
 * function builds the physical plan; this plan includes the list of tasks to be
 * executed on worker nodes, and the final query to run on the master node.
 */
DistributedPlan *
CreatePhysicalDistributedPlan(MultiTreeRoot *multiTree,
							  PlannerRestrictionContext *plannerRestrictionContext)
{
	/* build the worker job tree and check that we only have one job in the tree */
	Job *workerJob = BuildJobTree(multiTree);

	/* create the tree of executable tasks for the worker job */
	workerJob = BuildJobTreeTaskList(workerJob, plannerRestrictionContext);

	/* build the final merge query to execute on the master */
	List *masterDependentJobList = list_make1(workerJob);
	Query *masterQuery = BuildJobQuery((MultiNode *) multiTree, masterDependentJobList);

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->workerJob = workerJob;
	distributedPlan->masterQuery = masterQuery;
	distributedPlan->routerExecutable = DistributedPlanRouterExecutable(distributedPlan);
	distributedPlan->modLevel = ROW_MODIFY_READONLY;
	distributedPlan->expectResults = true;

	return distributedPlan;
}


/*
 * DistributedPlanRouterExecutable returns true if the input distributedPlan is
 * router executable.
 *
 * Note that all the multi plans that are created by router planner are
 * already router executable. Thus, this function should only be called
 * for multi plans that are not generated by router planner.
 */
static bool
DistributedPlanRouterExecutable(DistributedPlan *distributedPlan)
{
	Query *masterQuery = distributedPlan->masterQuery;
	Job *job = distributedPlan->workerJob;
	List *workerTaskList = job->taskList;
	int taskCount = list_length(workerTaskList);
	int dependentJobCount = list_length(job->dependentJobList);

	if (!EnableRouterExecution)
	{
		return false;
	}

	/* router executor cannot execute SELECT queries that hit more than one shard */
	if (taskCount != 1)
	{
		return false;
	}

	/* router executor cannot execute repartition jobs */
	if (dependentJobCount > 0)
	{
		return false;
	}

	/*
	 * Router executor does not run master query. This means that aggregation and
	 * sorting on the master query wouldn't be executed. Thus, such plans shouldn't be
	 * qualified as router executable.
	 */
	if (masterQuery != NULL && list_length(masterQuery->sortClause) > 0)
	{
		return false;
	}

	/*
	 * Note that worker query having an aggregate means that the master query should
	 * have either an aggregate or a function expression which has to be executed for
	 * the correct results.
	 */
	bool masterQueryHasAggregates = job->jobQuery->hasAggs;
	if (masterQueryHasAggregates)
	{
		return false;
	}

	return true;
}


/*
 * BuildJobTree builds the physical job tree from the given logical plan tree.
 * The function walks over the logical plan from the bottom up, finds boundaries
 * for jobs, and creates the query structure for each job. The function also
 * sets dependencies between jobs, and then returns the top level worker job.
 */
static Job *
BuildJobTree(MultiTreeRoot *multiTree)
{
	/* start building the tree from the deepest left node */
	MultiNode *leftMostNode = LeftMostNode(multiTree);
	MultiNode *currentNode = leftMostNode;
	MultiNode *parentNode = ParentNode(currentNode);
	List *loopDependentJobList = NIL;
	Job *topLevelJob = NULL;

	while (parentNode != NULL)
	{
		CitusNodeTag currentNodeType = CitusNodeTag(currentNode);
		CitusNodeTag parentNodeType = CitusNodeTag(parentNode);
		BoundaryNodeJobType boundaryNodeJobType = JOB_INVALID_FIRST;

		/* we first check if this node forms the boundary for a remote job */
		if (currentNodeType == T_MultiJoin)
		{
			MultiJoin *joinNode = (MultiJoin *) currentNode;
			if (joinNode->joinRuleType == SINGLE_HASH_PARTITION_JOIN ||
				joinNode->joinRuleType == SINGLE_RANGE_PARTITION_JOIN ||
				joinNode->joinRuleType == DUAL_PARTITION_JOIN)
			{
				boundaryNodeJobType = JOIN_MAP_MERGE_JOB;
			}
		}
		else if (currentNodeType == T_MultiPartition &&
				 parentNodeType == T_MultiExtendedOp)
		{
			boundaryNodeJobType = SUBQUERY_MAP_MERGE_JOB;
		}
		else if (currentNodeType == T_MultiCollect &&
				 parentNodeType != T_MultiPartition)
		{
			boundaryNodeJobType = TOP_LEVEL_WORKER_JOB;
		}

		/*
		 * If this node is at the boundary for a repartition or top level worker
		 * job, we build the corresponding job(s) and set their dependencies.
		 */
		if (boundaryNodeJobType == JOIN_MAP_MERGE_JOB)
		{
			MultiJoin *joinNode = (MultiJoin *) currentNode;
			MultiNode *leftChildNode = joinNode->binaryNode.leftChildNode;
			MultiNode *rightChildNode = joinNode->binaryNode.rightChildNode;

			PartitionType partitionType = PARTITION_INVALID_FIRST;
			Oid baseRelationId = InvalidOid;

			if (joinNode->joinRuleType == SINGLE_RANGE_PARTITION_JOIN)
			{
				partitionType = RANGE_PARTITION_TYPE;
				baseRelationId = RangePartitionJoinBaseRelationId(joinNode);
			}
			else if (joinNode->joinRuleType == SINGLE_HASH_PARTITION_JOIN)
			{
				partitionType = SINGLE_HASH_PARTITION_TYPE;
				baseRelationId = RangePartitionJoinBaseRelationId(joinNode);
			}
			else if (joinNode->joinRuleType == DUAL_PARTITION_JOIN)
			{
				partitionType = DUAL_HASH_PARTITION_TYPE;
			}

			if (CitusIsA(leftChildNode, MultiPartition))
			{
				MultiPartition *partitionNode = (MultiPartition *) leftChildNode;
				MultiNode *queryNode = GrandChildNode((MultiUnaryNode *) partitionNode);
				Var *partitionKey = partitionNode->partitionColumn;

				/* build query and partition job */
				List *dependentJobList = list_copy(loopDependentJobList);
				Query *jobQuery = BuildJobQuery(queryNode, dependentJobList);

				MapMergeJob *mapMergeJob = BuildMapMergeJob(jobQuery, dependentJobList,
															partitionKey, partitionType,
															baseRelationId,
															JOIN_MAP_MERGE_JOB);

				/* reset dependent job list */
				loopDependentJobList = NIL;
				loopDependentJobList = list_make1(mapMergeJob);
			}

			if (CitusIsA(rightChildNode, MultiPartition))
			{
				MultiPartition *partitionNode = (MultiPartition *) rightChildNode;
				MultiNode *queryNode = GrandChildNode((MultiUnaryNode *) partitionNode);
				Var *partitionKey = partitionNode->partitionColumn;

				/*
				 * The right query and right partition job do not depend on any
				 * jobs since our logical plan tree is left deep.
				 */
				Query *jobQuery = BuildJobQuery(queryNode, NIL);
				MapMergeJob *mapMergeJob = BuildMapMergeJob(jobQuery, NIL,
															partitionKey, partitionType,
															baseRelationId,
															JOIN_MAP_MERGE_JOB);

				/* append to the dependent job list for on-going dependencies */
				loopDependentJobList = lappend(loopDependentJobList, mapMergeJob);
			}
		}
		else if (boundaryNodeJobType == SUBQUERY_MAP_MERGE_JOB)
		{
			MultiPartition *partitionNode = (MultiPartition *) currentNode;
			MultiNode *queryNode = GrandChildNode((MultiUnaryNode *) partitionNode);
			Var *partitionKey = partitionNode->partitionColumn;

			/* build query and partition job */
			List *dependentJobList = list_copy(loopDependentJobList);
			Query *jobQuery = BuildJobQuery(queryNode, dependentJobList);

			MapMergeJob *mapMergeJob = BuildMapMergeJob(jobQuery, dependentJobList,
														partitionKey,
														DUAL_HASH_PARTITION_TYPE,
														InvalidOid,
														SUBQUERY_MAP_MERGE_JOB);

			Query *reduceQuery = BuildReduceQuery((MultiExtendedOp *) parentNode,
												  list_make1(mapMergeJob));
			mapMergeJob->reduceQuery = reduceQuery;

			/* reset dependent job list */
			loopDependentJobList = NIL;
			loopDependentJobList = list_make1(mapMergeJob);
		}
		else if (boundaryNodeJobType == TOP_LEVEL_WORKER_JOB)
		{
			MultiNode *childNode = ChildNode((MultiUnaryNode *) currentNode);
			List *dependentJobList = list_copy(loopDependentJobList);
			bool subqueryPushdown = false;

			List *subqueryMultiTableList = SubqueryMultiTableList(childNode);
			int subqueryCount = list_length(subqueryMultiTableList);

			if (subqueryCount > 0)
			{
				subqueryPushdown = true;
			}

			/*
			 * Build top level query. If subquery pushdown is set, we use
			 * sligthly different version of BuildJobQuery(). They are similar
			 * but we don't need some parts of BuildJobQuery() for subquery
			 * pushdown such as updating column attributes etc.
			 */
			if (subqueryPushdown)
			{
				Query *topLevelQuery = BuildSubqueryJobQuery(childNode);

				topLevelJob = BuildJob(topLevelQuery, dependentJobList);
				topLevelJob->subqueryPushdown = true;
			}
			else
			{
				Query *topLevelQuery = BuildJobQuery(childNode, dependentJobList);

				topLevelJob = BuildJob(topLevelQuery, dependentJobList);
			}
		}

		/* walk up the tree */
		currentNode = parentNode;
		parentNode = ParentNode(currentNode);
	}

	return topLevelJob;
}


/*
 * LeftMostNode finds the deepest left node in the left-deep logical plan tree.
 * We build the physical plan by traversing the logical plan from the bottom up;
 * and this function helps us find the bottom of the logical tree.
 */
static MultiNode *
LeftMostNode(MultiTreeRoot *multiTree)
{
	MultiNode *currentNode = (MultiNode *) multiTree;
	MultiNode *leftChildNode = ChildNode((MultiUnaryNode *) multiTree);

	while (leftChildNode != NULL)
	{
		currentNode = leftChildNode;

		if (UnaryOperator(currentNode))
		{
			leftChildNode = ChildNode((MultiUnaryNode *) currentNode);
		}
		else if (BinaryOperator(currentNode))
		{
			MultiBinaryNode *binaryNode = (MultiBinaryNode *) currentNode;
			leftChildNode = binaryNode->leftChildNode;
		}
	}

	return currentNode;
}


/*
 * RangePartitionJoinBaseRelationId finds partition node from join node, and
 * returns base relation id of this node. Note that this function assumes that
 * given join node is range partition join type.
 */
static Oid
RangePartitionJoinBaseRelationId(MultiJoin *joinNode)
{
	MultiPartition *partitionNode = NULL;

	MultiNode *leftChildNode = joinNode->binaryNode.leftChildNode;
	MultiNode *rightChildNode = joinNode->binaryNode.rightChildNode;

	if (CitusIsA(leftChildNode, MultiPartition))
	{
		partitionNode = (MultiPartition *) leftChildNode;
	}
	else if (CitusIsA(rightChildNode, MultiPartition))
	{
		partitionNode = (MultiPartition *) rightChildNode;
	}

	Index baseTableId = partitionNode->splitPointTableId;
	MultiTable *baseTable = FindTableNode((MultiNode *) joinNode, baseTableId);
	Oid baseRelationId = baseTable->relationId;

	return baseRelationId;
}


/*
 * FindTableNode walks over the given logical plan tree, and returns the table
 * node that corresponds to the given range tableId.
 */
static MultiTable *
FindTableNode(MultiNode *multiNode, int rangeTableId)
{
	MultiTable *foundTableNode = NULL;
	List *tableNodeList = FindNodesOfType(multiNode, T_MultiTable);
	ListCell *tableNodeCell = NULL;

	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = (MultiTable *) lfirst(tableNodeCell);
		if (tableNode->rangeTableId == rangeTableId)
		{
			foundTableNode = tableNode;
			break;
		}
	}

	Assert(foundTableNode != NULL);
	return foundTableNode;
}


/*
 * BuildJobQuery traverses the given logical plan tree, determines the job that
 * corresponds to this part of the tree, and builds the query structure for that
 * particular job. The function assumes that jobs this particular job depends on
 * have already been built, as their output is needed to build the query.
 */
static Query *
BuildJobQuery(MultiNode *multiNode, List *dependentJobList)
{
	bool updateColumnAttributes = false;
	List *targetList = NIL;
	List *sortClauseList = NIL;
	Node *limitCount = NULL;
	Node *limitOffset = NULL;
	Node *havingQual = NULL;
	bool hasDistinctOn = false;
	List *distinctClause = NIL;
	bool isRepartitionJoin = false;
	bool hasWindowFuncs = false;
	List *windowClause = NIL;

	/* we start building jobs from below the collect node */
	Assert(!CitusIsA(multiNode, MultiCollect));

	/*
	 * First check if we are building a master/worker query. If we are building
	 * a worker query, we update the column attributes for target entries, select
	 * and join columns. Because if underlying query includes repartition joins,
	 * then we create multiple queries from a join. In this case, range table lists
	 * and column lists are subject to change.
	 *
	 * Note that we don't do this for master queries, as column attributes for
	 * master target entries are already set during the master/worker split.
	 */
	MultiNode *parentNode = ParentNode(multiNode);
	if (parentNode != NULL)
	{
		updateColumnAttributes = true;
	}

	/*
	 * If we are building this query on a repartitioned subquery job then we
	 * don't need to update column attributes.
	 */
	if (dependentJobList != NIL)
	{
		Job *job = (Job *) linitial(dependentJobList);
		if (CitusIsA(job, MapMergeJob))
		{
			MapMergeJob *mapMergeJob = (MapMergeJob *) job;
			isRepartitionJoin = true;
			if (mapMergeJob->reduceQuery)
			{
				updateColumnAttributes = false;
			}
		}
	}

	/*
	 * If we have an extended operator, then we copy the operator's target list.
	 * Otherwise, we use the target list based on the MultiProject node at this
	 * level in the query tree.
	 */
	List *extendedOpNodeList = FindNodesOfType(multiNode, T_MultiExtendedOp);
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);
		targetList = copyObject(extendedOp->targetList);
		distinctClause = extendedOp->distinctClause;
		hasDistinctOn = extendedOp->hasDistinctOn;
		hasWindowFuncs = extendedOp->hasWindowFuncs;
		windowClause = extendedOp->windowClause;
	}
	else
	{
		targetList = QueryTargetList(multiNode);
	}

	/* build the join tree and the range table list */
	List *rangeTableList = BaseRangeTableList(multiNode);
	Node *joinRoot = QueryJoinTree(multiNode, dependentJobList, &rangeTableList);

	/* update the column attributes for target entries */
	if (updateColumnAttributes)
	{
		UpdateAllColumnAttributes((Node *) targetList, rangeTableList, dependentJobList);
	}

	/* extract limit count/offset and sort clauses */
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);

		limitCount = extendedOp->limitCount;
		limitOffset = extendedOp->limitOffset;
		sortClauseList = extendedOp->sortClauseList;
		havingQual = extendedOp->havingQual;
	}

	/* build group clauses */
	List *groupClauseList = QueryGroupClauseList(multiNode);


	/* build the where clause list using select predicates */
	List *selectClauseList = QuerySelectClauseList(multiNode);

	/* set correct column attributes for select and having clauses */
	if (updateColumnAttributes)
	{
		UpdateAllColumnAttributes((Node *) selectClauseList, rangeTableList,
								  dependentJobList);
		UpdateAllColumnAttributes(havingQual, rangeTableList, dependentJobList);
	}

	/*
	 * Group by on primary key allows all columns to appear in the target
	 * list, but after re-partitioning we will be querying an intermediate
	 * table that does not have the primary key. We therefore wrap all the
	 * columns that do not appear in the GROUP BY in an any_value aggregate.
	 */
	if (groupClauseList != NIL && isRepartitionJoin)
	{
		targetList = (List *) WrapUngroupedVarsInAnyValueAggregate(
			(Node *) targetList, groupClauseList, targetList, true);

		havingQual = WrapUngroupedVarsInAnyValueAggregate(
			(Node *) havingQual, groupClauseList, targetList, false);
	}

	/*
	 * Build the From/Where construct. We keep the where-clause list implicitly
	 * AND'd, since both partition and join pruning depends on the clauses being
	 * expressed as a list.
	 */
	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->quals = (Node *) list_copy(selectClauseList);
	joinTree->fromlist = list_make1(joinRoot);

	/* build the query structure for this job */
	Query *jobQuery = makeNode(Query);
	jobQuery->commandType = CMD_SELECT;
	jobQuery->querySource = QSRC_ORIGINAL;
	jobQuery->canSetTag = true;
	jobQuery->rtable = rangeTableList;
	jobQuery->targetList = targetList;
	jobQuery->jointree = joinTree;
	jobQuery->sortClause = sortClauseList;
	jobQuery->groupClause = groupClauseList;
	jobQuery->limitOffset = limitOffset;
	jobQuery->limitCount = limitCount;
	jobQuery->havingQual = havingQual;
	jobQuery->hasAggs = contain_aggs_of_level((Node *) targetList, 0) ||
						contain_aggs_of_level((Node *) havingQual, 0);
	jobQuery->distinctClause = distinctClause;
	jobQuery->hasDistinctOn = hasDistinctOn;
	jobQuery->windowClause = windowClause;
	jobQuery->hasWindowFuncs = hasWindowFuncs;
	jobQuery->hasSubLinks = checkExprHasSubLink((Node *) jobQuery);

	Assert(jobQuery->hasWindowFuncs == contain_window_function((Node *) jobQuery));

	return jobQuery;
}


/*
 * BuildReduceQuery traverses the given logical plan tree, determines the job that
 * corresponds to this part of the tree, and builds the query structure for that
 * particular job. The function assumes that jobs this particular job depends on
 * have already been built, as their output is needed to build the query.
 */
static Query *
BuildReduceQuery(MultiExtendedOp *extendedOpNode, List *dependentJobList)
{
	MultiNode *multiNode = (MultiNode *) extendedOpNode;
	List *derivedRangeTableList = NIL;
	List *targetList = NIL;
	ListCell *columnCell = NULL;
	List *columnNameList = NIL;

	Job *dependentJob = linitial(dependentJobList);
	List *dependentTargetList = dependentJob->jobQuery->targetList;
	uint32 columnCount = (uint32) list_length(dependentTargetList);

	for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		StringInfo columnNameString = makeStringInfo();

		appendStringInfo(columnNameString, MERGE_COLUMN_FORMAT, columnIndex);

		Value *columnValue = makeString(columnNameString->data);
		columnNameList = lappend(columnNameList, columnValue);
	}

	/* create a derived range table for the subtree below the collect */
	RangeTblEntry *rangeTableEntry = DerivedRangeTableEntry(multiNode, columnNameList,
															OutputTableIdList(multiNode),
															NIL, NIL, NIL, NIL);
	rangeTableEntry->eref->colnames = columnNameList;
	ModifyRangeTblExtraData(rangeTableEntry, CITUS_RTE_SHARD, NULL, NULL, NULL);
	derivedRangeTableList = lappend(derivedRangeTableList, rangeTableEntry);

	targetList = copyObject(extendedOpNode->targetList);
	List *columnList = pull_var_clause_default((Node *) targetList);

	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		Index originalTableId = column->varnoold;

		/* find the new table identifier */
		Index newTableId = NewTableId(originalTableId, derivedRangeTableList);
		column->varno = newTableId;
	}

	/* build the where clause list using select and join predicates */
	List *selectClauseList = QuerySelectClauseList((MultiNode *) extendedOpNode);
	List *joinClauseList = QueryJoinClauseList((MultiNode *) extendedOpNode);
	List *whereClauseList = list_concat(selectClauseList, joinClauseList);

	/*
	 * Build the From/Where construct. We keep the where-clause list implicitly
	 * AND'd, since both partition and join pruning depends on the clauses being
	 * expressed as a list.
	 */
	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->quals = (Node *) whereClauseList;
	joinTree->fromlist = QueryFromList(derivedRangeTableList);

	/* build the query structure for this job */
	Query *reduceQuery = makeNode(Query);
	reduceQuery->commandType = CMD_SELECT;
	reduceQuery->querySource = QSRC_ORIGINAL;
	reduceQuery->canSetTag = true;
	reduceQuery->rtable = derivedRangeTableList;
	reduceQuery->targetList = targetList;
	reduceQuery->jointree = joinTree;
	reduceQuery->sortClause = extendedOpNode->sortClauseList;
	reduceQuery->groupClause = extendedOpNode->groupClauseList;
	reduceQuery->limitOffset = extendedOpNode->limitOffset;
	reduceQuery->limitCount = extendedOpNode->limitCount;
	reduceQuery->havingQual = extendedOpNode->havingQual;
	reduceQuery->hasAggs = contain_aggs_of_level((Node *) targetList, 0);

	return reduceQuery;
}


/*
 * BaseRangeTableList returns the list of range table entries for base tables in
 * the query. These base tables stand in contrast to derived tables generated by
 * repartition jobs. Note that this function only considers base tables relevant
 * to the current query, and does not visit nodes under the collect node.
 */
static List *
BaseRangeTableList(MultiNode *multiNode)
{
	List *baseRangeTableList = NIL;
	List *pendingNodeList = list_make1(multiNode);

	while (pendingNodeList != NIL)
	{
		MultiNode *currMultiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(currMultiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		if (nodeType == T_MultiTable)
		{
			/*
			 * We represent subqueries as MultiTables, and so for base table
			 * entries we skip the subquery ones.
			 */
			MultiTable *multiTable = (MultiTable *) currMultiNode;
			if (multiTable->relationId != SUBQUERY_RELATION_ID &&
				multiTable->relationId != SUBQUERY_PUSHDOWN_RELATION_ID)
			{
				RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
				rangeTableEntry->inFromCl = true;
				rangeTableEntry->eref = multiTable->referenceNames;
				rangeTableEntry->alias = multiTable->alias;
				rangeTableEntry->relid = multiTable->relationId;
				SetRangeTblExtraData(rangeTableEntry, CITUS_RTE_RELATION, NULL, NULL,
									 list_make1_int(multiTable->rangeTableId),
									 NIL, NIL, NIL, NIL);

				baseRangeTableList = lappend(baseRangeTableList, rangeTableEntry);
			}
		}

		/* do not visit nodes that belong to remote queries */
		if (nodeType != T_MultiCollect)
		{
			List *childNodeList = ChildNodeList(currMultiNode);
			pendingNodeList = list_concat(pendingNodeList, childNodeList);
		}
	}

	return baseRangeTableList;
}


/*
 * DerivedRangeTableEntry builds a range table entry for the derived table. This
 * derived table either represents the output of a repartition job; or the data
 * on worker nodes in case of the master node query.
 */
static RangeTblEntry *
DerivedRangeTableEntry(MultiNode *multiNode, List *columnList, List *tableIdList,
					   List *funcColumnNames, List *funcColumnTypes,
					   List *funcColumnTypeMods, List *funcCollations)
{
	RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->eref = makeNode(Alias);
	rangeTableEntry->eref->colnames = columnList;

	SetRangeTblExtraData(rangeTableEntry, CITUS_RTE_REMOTE_QUERY, NULL, NULL, tableIdList,
						 funcColumnNames, funcColumnTypes, funcColumnTypeMods,
						 funcCollations);

	return rangeTableEntry;
}


/*
 * DerivedColumnNameList builds a column name list for derived (intermediate)
 * tables. These column names are then used when building the create stament
 * query string for derived tables.
 */
static List *
DerivedColumnNameList(uint32 columnCount, uint64 generatingJobId)
{
	List *columnNameList = NIL;

	for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		StringInfo columnName = makeStringInfo();

		appendStringInfo(columnName, "intermediate_column_");
		appendStringInfo(columnName, UINT64_FORMAT "_", generatingJobId);
		appendStringInfo(columnName, "%u", columnIndex);

		Value *columnValue = makeString(columnName->data);
		columnNameList = lappend(columnNameList, columnValue);
	}

	return columnNameList;
}


/*
 * QueryTargetList returns the target entry list for the projected columns
 * needed to evaluate the operators above the given multiNode. To do this,
 * the function retrieves a list of all MultiProject nodes below the given
 * node and picks the columns from the top-most MultiProject node, as this
 * will be the minimal list of columns needed. Note that this function relies
 * on a pre-order traversal of the operator tree by the function FindNodesOfType.
 */
static List *
QueryTargetList(MultiNode *multiNode)
{
	List *projectNodeList = FindNodesOfType(multiNode, T_MultiProject);
	Assert(list_length(projectNodeList) > 0);

	MultiProject *topProjectNode = (MultiProject *) linitial(projectNodeList);
	List *columnList = topProjectNode->columnList;
	List *queryTargetList = TargetEntryList(columnList);

	Assert(queryTargetList != NIL);
	return queryTargetList;
}


/*
 * TargetEntryList creates a target entry for each expression in the given list,
 * and returns the newly created target entries in a list.
 */
static List *
TargetEntryList(List *expressionList)
{
	List *targetEntryList = NIL;
	ListCell *expressionCell = NULL;

	foreach(expressionCell, expressionList)
	{
		Expr *expression = (Expr *) lfirst(expressionCell);

		TargetEntry *targetEntry = makeTargetEntry(expression,
												   list_length(targetEntryList) + 1,
												   NULL, false);
		targetEntryList = lappend(targetEntryList, targetEntry);
	}

	return targetEntryList;
}


/*
 * WrapUngroupedVarsInAnyValueAggregate finds Var nodes in the expression
 * that do not refer to any GROUP BY column and wraps them in an any_value
 * aggregate. These columns are allowed when the GROUP BY is on a primary
 * key of a relation, but not if we wrap the relation in a subquery.
 * However, since we still know the value is unique, any_value gives the
 * right result.
 */
Node *
WrapUngroupedVarsInAnyValueAggregate(Node *expression, List *groupClauseList,
									 List *targetList, bool checkExpressionEquality)
{
	if (expression == NULL)
	{
		return NULL;
	}

	AddAnyValueAggregatesContext context;
	context.groupClauseList = groupClauseList;
	context.groupByTargetEntryList = GroupTargetEntryList(groupClauseList, targetList);
	context.haveNonVarGrouping = false;

	if (checkExpressionEquality)
	{
		/*
		 * If the GROUP BY contains non-Var expressions, we need to do an expensive
		 * subexpression equality check.
		 */
		TargetEntry *targetEntry = NULL;
		foreach_ptr(targetEntry, context.groupByTargetEntryList)
		{
			if (!IsA(targetEntry->expr, Var))
			{
				context.haveNonVarGrouping = true;
				break;
			}
		}
	}

	/* put the result in the same memory context */
	MemoryContext nodeContext = GetMemoryChunkContext(expression);
	MemoryContext oldContext = MemoryContextSwitchTo(nodeContext);

	Node *result = expression_tree_mutator(expression, AddAnyValueAggregates,
										   &context);

	MemoryContextSwitchTo(oldContext);

	return result;
}


/*
 * AddAnyValueAggregates wraps all vars that do not appear in the GROUP BY
 * clause or are inside an aggregate function in an any_value aggregate
 * function. This is needed because postgres allows columns that are not
 * in the GROUP BY to appear on the target list as long as the primary key
 * of the table is in the GROUP BY, but we sometimes wrap the join tree
 * in a subquery in which case the primary key information is lost.
 *
 * This function copies parts of the node tree, but may contain references
 * to the original node tree.
 *
 * The implementation is derived from / inspired by
 * check_ungrouped_columns_walker.
 */
static Node *
AddAnyValueAggregates(Node *node, AddAnyValueAggregatesContext *context)
{
	if (node == NULL)
	{
		return node;
	}

	if (IsA(node, Aggref) || IsA(node, GroupingFunc))
	{
		/* any column is allowed to appear in an aggregate or grouping */
		return node;
	}
	else if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		/*
		 * Check whether this Var appears in the GROUP BY.
		 */
		TargetEntry *groupByTargetEntry = NULL;
		foreach_ptr(groupByTargetEntry, context->groupByTargetEntryList)
		{
			if (!IsA(groupByTargetEntry->expr, Var))
			{
				continue;
			}

			Var *groupByVar = (Var *) groupByTargetEntry->expr;

			/* we should only be doing this at the top level of the query */
			Assert(groupByVar->varlevelsup == 0);

			if (var->varno == groupByVar->varno &&
				var->varattno == groupByVar->varattno)
			{
				/* this Var is in the GROUP BY, do not wrap it */
				return node;
			}
		}

		/*
		 * We have found a Var that does not appear in the GROUP BY.
		 * Wrap it in an any_value aggregate.
		 */
		Aggref *agg = makeNode(Aggref);
		agg->aggfnoid = CitusAnyValueFunctionId();
		agg->aggtype = var->vartype;
		agg->args = list_make1(makeTargetEntry((Expr *) var, 1, NULL, false));
		agg->aggkind = AGGKIND_NORMAL;
		agg->aggtranstype = InvalidOid;
		agg->aggargtypes = list_make1_oid(var->vartype);
		agg->aggsplit = AGGSPLIT_SIMPLE;
		agg->aggcollid = exprCollation((Node *) var);
		return (Node *) agg;
	}
	else if (context->haveNonVarGrouping)
	{
		/*
		 * The GROUP BY contains at least one expression. Check whether the
		 * current expression is equal to one of the GROUP BY expressions.
		 * Otherwise, continue to descend into subexpressions.
		 */
		TargetEntry *groupByTargetEntry = NULL;
		foreach_ptr(groupByTargetEntry, context->groupByTargetEntryList)
		{
			if (equal(node, groupByTargetEntry->expr))
			{
				/* do not descend into mutator, all Vars are safe */
				return node;
			}
		}
	}

	return expression_tree_mutator(node, AddAnyValueAggregates, context);
}


/*
 * QueryGroupClauseList extracts the group clause list from the logical plan. If
 * no grouping clauses exist, the function returns an empty list.
 */
static List *
QueryGroupClauseList(MultiNode *multiNode)
{
	List *groupClauseList = NIL;
	List *pendingNodeList = list_make1(multiNode);

	while (pendingNodeList != NIL)
	{
		MultiNode *currMultiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(currMultiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		/* extract the group clause list from the extended operator */
		if (nodeType == T_MultiExtendedOp)
		{
			MultiExtendedOp *extendedOpNode = (MultiExtendedOp *) currMultiNode;
			groupClauseList = extendedOpNode->groupClauseList;
		}

		/* add children only if this node isn't a multi collect and multi table */
		if (nodeType != T_MultiCollect && nodeType != T_MultiTable)
		{
			List *childNodeList = ChildNodeList(currMultiNode);
			pendingNodeList = list_concat(pendingNodeList, childNodeList);
		}
	}

	return groupClauseList;
}


/*
 * QuerySelectClauseList traverses the given logical plan tree, and extracts all
 * select clauses from the select nodes. Note that this function does not walk
 * below a collect node; the clauses below the collect node apply to a remote
 * query, and they would have been captured by the remote job we depend upon.
 */
static List *
QuerySelectClauseList(MultiNode *multiNode)
{
	List *selectClauseList = NIL;
	List *pendingNodeList = list_make1(multiNode);

	while (pendingNodeList != NIL)
	{
		MultiNode *currMultiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(currMultiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		/* extract select clauses from the multi select node */
		if (nodeType == T_MultiSelect)
		{
			MultiSelect *selectNode = (MultiSelect *) currMultiNode;
			List *clauseList = copyObject(selectNode->selectClauseList);
			selectClauseList = list_concat(selectClauseList, clauseList);
		}

		/* add children only if this node isn't a multi collect */
		if (nodeType != T_MultiCollect)
		{
			List *childNodeList = ChildNodeList(currMultiNode);
			pendingNodeList = list_concat(pendingNodeList, childNodeList);
		}
	}

	return selectClauseList;
}


/*
 * QueryJoinClauseList traverses the given logical plan tree, and extracts all
 * join clauses from the join nodes. Note that this function does not walk below
 * a collect node; the clauses below the collect node apply to another query,
 * and they would have been captured by the remote job we depend upon.
 */
static List *
QueryJoinClauseList(MultiNode *multiNode)
{
	List *joinClauseList = NIL;
	List *pendingNodeList = list_make1(multiNode);

	while (pendingNodeList != NIL)
	{
		MultiNode *currMultiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(currMultiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		/* extract join clauses from the multi join node */
		if (nodeType == T_MultiJoin)
		{
			MultiJoin *joinNode = (MultiJoin *) currMultiNode;
			List *clauseList = copyObject(joinNode->joinClauseList);
			joinClauseList = list_concat(joinClauseList, clauseList);
		}

		/* add this node's children only if the node isn't a multi collect */
		if (nodeType != T_MultiCollect)
		{
			List *childNodeList = ChildNodeList(currMultiNode);
			pendingNodeList = list_concat(pendingNodeList, childNodeList);
		}
	}

	return joinClauseList;
}


/*
 * Create a tree of JoinExpr and RangeTblRef nodes for the job query from
 * a given multiNode. If the tree contains MultiCollect or MultiJoin nodes,
 * add corresponding entries to the range table list. We need to construct
 * the entries at the same time as the tree to know the appropriate rtindex.
 */
static Node *
QueryJoinTree(MultiNode *multiNode, List *dependentJobList, List **rangeTableList)
{
	CitusNodeTag nodeType = CitusNodeTag(multiNode);

	switch (nodeType)
	{
		case T_MultiJoin:
		{
			MultiJoin *joinNode = (MultiJoin *) multiNode;
			MultiBinaryNode *binaryNode = (MultiBinaryNode *) multiNode;
			ListCell *columnCell = NULL;
			JoinExpr *joinExpr = makeNode(JoinExpr);
			joinExpr->jointype = joinNode->joinType;
			joinExpr->isNatural = false;
			joinExpr->larg = QueryJoinTree(binaryNode->leftChildNode, dependentJobList,
										   rangeTableList);
			joinExpr->rarg = QueryJoinTree(binaryNode->rightChildNode, dependentJobList,
										   rangeTableList);
			joinExpr->usingClause = NIL;
			joinExpr->alias = NULL;
			joinExpr->rtindex = list_length(*rangeTableList) + 1;

			/*
			 * PostgreSQL's optimizer may mark left joins as anti-joins, when there
			 * is a right-hand-join-key-is-null restriction, but there is no logic
			 * in ruleutils to deparse anti-joins, so we cannot construct a task
			 * query containing anti-joins. We therefore translate anti-joins back
			 * into left-joins. At some point, we may also want to use different
			 * join pruning logic for anti-joins.
			 *
			 * This approach would not work for anti-joins introduced via NOT EXISTS
			 * sublinks, but currently such queries are prevented by error checks in
			 * the logical planner.
			 */
			if (joinExpr->jointype == JOIN_ANTI)
			{
				joinExpr->jointype = JOIN_LEFT;
			}

			RangeTblEntry *rangeTableEntry = JoinRangeTableEntry(joinExpr,
																 dependentJobList,
																 *rangeTableList);
			*rangeTableList = lappend(*rangeTableList, rangeTableEntry);

			/* fix the column attributes in ON (...) clauses */
			List *columnList = pull_var_clause_default((Node *) joinNode->joinClauseList);
			foreach(columnCell, columnList)
			{
				Var *column = (Var *) lfirst(columnCell);
				UpdateColumnAttributes(column, *rangeTableList, dependentJobList);

				/* adjust our column old attributes for partition pruning to work */
				column->varnoold = column->varno;
				column->varoattno = column->varattno;
			}

			/* make AND clauses explicit after fixing them */
			joinExpr->quals = (Node *) make_ands_explicit(joinNode->joinClauseList);

			return (Node *) joinExpr;
		}

		case T_MultiTable:
		{
			MultiTable *rangeTableNode = (MultiTable *) multiNode;
			MultiUnaryNode *unaryNode = (MultiUnaryNode *) multiNode;

			if (unaryNode->childNode != NULL)
			{
				/* MultiTable is actually a subquery, return the query tree below */
				Node *childNode = QueryJoinTree(unaryNode->childNode, dependentJobList,
												rangeTableList);

				return childNode;
			}
			else
			{
				RangeTblRef *rangeTableRef = makeNode(RangeTblRef);
				uint32 rangeTableId = rangeTableNode->rangeTableId;
				rangeTableRef->rtindex = NewTableId(rangeTableId, *rangeTableList);

				return (Node *) rangeTableRef;
			}
		}

		case T_MultiCollect:
		{
			List *tableIdList = OutputTableIdList(multiNode);
			Job *dependentJob = JobForTableIdList(dependentJobList, tableIdList);
			List *dependentTargetList = dependentJob->jobQuery->targetList;

			/* compute column names for the derived table */
			uint32 columnCount = (uint32) list_length(dependentTargetList);
			List *columnNameList = DerivedColumnNameList(columnCount,
														 dependentJob->jobId);

			List *funcColumnNames = NIL;
			List *funcColumnTypes = NIL;
			List *funcColumnTypeMods = NIL;
			List *funcCollations = NIL;

			TargetEntry *targetEntry = NULL;
			foreach_ptr(targetEntry, dependentTargetList)
			{
				Node *expr = (Node *) targetEntry->expr;

				char *name = targetEntry->resname;
				if (name == NULL)
				{
					name = pstrdup("unnamed");
				}

				funcColumnNames = lappend(funcColumnNames, makeString(name));

				funcColumnTypes = lappend_oid(funcColumnTypes, exprType(expr));
				funcColumnTypeMods = lappend_int(funcColumnTypeMods, exprTypmod(expr));
				funcCollations = lappend_oid(funcCollations, exprCollation(expr));
			}

			RangeTblEntry *rangeTableEntry = DerivedRangeTableEntry(multiNode,
																	columnNameList,
																	tableIdList,
																	funcColumnNames,
																	funcColumnTypes,
																	funcColumnTypeMods,
																	funcCollations);

			RangeTblRef *rangeTableRef = makeNode(RangeTblRef);

			rangeTableRef->rtindex = list_length(*rangeTableList) + 1;
			*rangeTableList = lappend(*rangeTableList, rangeTableEntry);

			return (Node *) rangeTableRef;
		}

		case T_MultiCartesianProduct:
		{
			MultiBinaryNode *binaryNode = (MultiBinaryNode *) multiNode;

			JoinExpr *joinExpr = makeNode(JoinExpr);
			joinExpr->jointype = JOIN_INNER;
			joinExpr->isNatural = false;
			joinExpr->larg = QueryJoinTree(binaryNode->leftChildNode, dependentJobList,
										   rangeTableList);
			joinExpr->rarg = QueryJoinTree(binaryNode->rightChildNode, dependentJobList,
										   rangeTableList);
			joinExpr->usingClause = NIL;
			joinExpr->alias = NULL;
			joinExpr->quals = NULL;
			joinExpr->rtindex = list_length(*rangeTableList) + 1;

			RangeTblEntry *rangeTableEntry = JoinRangeTableEntry(joinExpr,
																 dependentJobList,
																 *rangeTableList);
			*rangeTableList = lappend(*rangeTableList, rangeTableEntry);

			return (Node *) joinExpr;
		}

		case T_MultiTreeRoot:
		case T_MultiSelect:
		case T_MultiProject:
		case T_MultiExtendedOp:
		case T_MultiPartition:
		{
			MultiUnaryNode *unaryNode = (MultiUnaryNode *) multiNode;

			Assert(UnaryOperator(multiNode));

			Node *childNode = QueryJoinTree(unaryNode->childNode, dependentJobList,
											rangeTableList);

			return childNode;
		}

		default:
		{
			ereport(ERROR, (errmsg("unrecognized multi-node type: %d", nodeType)));
		}
	}
}


/*
 * JoinRangeTableEntry builds a range table entry for a fully initialized JoinExpr node.
 * The column names and vars are determined using expandRTE, analogous to
 * transformFromClauseItem.
 */
static RangeTblEntry *
JoinRangeTableEntry(JoinExpr *joinExpr, List *dependentJobList, List *rangeTableList)
{
	RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
	List *joinedColumnNames = NIL;
	List *joinedColumnVars = NIL;
	List *leftColumnNames = NIL;
	List *leftColumnVars = NIL;
	int leftRangeTableId = ExtractRangeTableId(joinExpr->larg);
	RangeTblEntry *leftRTE = rt_fetch(leftRangeTableId, rangeTableList);
	List *rightColumnNames = NIL;
	List *rightColumnVars = NIL;
	int rightRangeTableId = ExtractRangeTableId(joinExpr->rarg);
	RangeTblEntry *rightRTE = rt_fetch(rightRangeTableId, rangeTableList);

	rangeTableEntry->rtekind = RTE_JOIN;
	rangeTableEntry->relid = InvalidOid;
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->alias = joinExpr->alias;
	rangeTableEntry->jointype = joinExpr->jointype;
	rangeTableEntry->subquery = NULL;
	rangeTableEntry->eref = makeAlias("unnamed_join", NIL);

	ExtractColumns(leftRTE, leftRangeTableId, dependentJobList,
				   &leftColumnNames, &leftColumnVars);
	ExtractColumns(rightRTE, rightRangeTableId, dependentJobList,
				   &rightColumnNames, &rightColumnVars);

	joinedColumnNames = list_concat(joinedColumnNames, leftColumnNames);
	joinedColumnVars = list_concat(joinedColumnVars, leftColumnVars);
	joinedColumnNames = list_concat(joinedColumnNames, rightColumnNames);
	joinedColumnVars = list_concat(joinedColumnVars, rightColumnVars);

	rangeTableEntry->eref->colnames = joinedColumnNames;
	rangeTableEntry->joinaliasvars = joinedColumnVars;

	return rangeTableEntry;
}


/*
 * ExtractRangeTableId gets the range table id from a node that could
 * either be a JoinExpr or RangeTblRef.
 */
static int
ExtractRangeTableId(Node *node)
{
	int rangeTableId = 0;

	if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) node;
		rangeTableId = joinExpr->rtindex;
	}
	else if (IsA(node, RangeTblRef))
	{
		RangeTblRef *rangeTableRef = (RangeTblRef *) node;
		rangeTableId = rangeTableRef->rtindex;
	}

	Assert(rangeTableId > 0);

	return rangeTableId;
}


/*
 * ExtractColumns gets a list of column names and vars for a given range
 * table entry using expandRTE. Since the range table entries in a job
 * query are mocked RTE_FUNCTION entries, it first translates the RTE's
 * to a form that expandRTE can handle.
 */
static void
ExtractColumns(RangeTblEntry *rangeTableEntry, int rangeTableId, List *dependentJobList,
			   List **columnNames, List **columnVars)
{
	RangeTblEntry *callingRTE = NULL;

	CitusRTEKind rangeTableKind = GetRangeTblKind(rangeTableEntry);
	if (rangeTableKind == CITUS_RTE_JOIN)
	{
		/*
		 * For joins, we can call expandRTE directly.
		 */
		callingRTE = rangeTableEntry;
	}
	else if (rangeTableKind == CITUS_RTE_RELATION)
	{
		/*
		 * For distributed tables, we construct a regular table RTE to call
		 * expandRTE, which will extract columns from the distributed table
		 * schema.
		 */
		callingRTE = makeNode(RangeTblEntry);
		callingRTE->rtekind = RTE_RELATION;
		callingRTE->eref = rangeTableEntry->eref;
		callingRTE->relid = rangeTableEntry->relid;
	}
	else if (rangeTableKind == CITUS_RTE_REMOTE_QUERY)
	{
		Job *dependentJob = JobForRangeTable(dependentJobList, rangeTableEntry);
		Query *jobQuery = dependentJob->jobQuery;

		/*
		 * For re-partition jobs, we construct a subquery RTE to call expandRTE,
		 * which will extract the columns from the target list of the job query.
		 */
		callingRTE = makeNode(RangeTblEntry);
		callingRTE->rtekind = RTE_SUBQUERY;
		callingRTE->eref = rangeTableEntry->eref;
		callingRTE->subquery = jobQuery;
	}
	else
	{
		ereport(ERROR, (errmsg("unsupported Citus RTE kind: %d", rangeTableKind)));
	}

	expandRTE(callingRTE, rangeTableId, 0, -1, false, columnNames, columnVars);
}


/*
 * QueryFromList creates the from list construct that is used for building the
 * query's join tree. The function creates the from list by making a range table
 * reference for each entry in the given range table list.
 */
static List *
QueryFromList(List *rangeTableList)
{
	List *fromList = NIL;
	int rangeTableCount = list_length(rangeTableList);

	for (Index rangeTableIndex = 1; rangeTableIndex <= rangeTableCount; rangeTableIndex++)
	{
		RangeTblRef *rangeTableReference = makeNode(RangeTblRef);
		rangeTableReference->rtindex = rangeTableIndex;

		fromList = lappend(fromList, rangeTableReference);
	}

	return fromList;
}


/*
 * BuildSubqueryJobQuery traverses the given logical plan tree, finds MultiTable
 * which represents the subquery. It builds the query structure by adding this
 * subquery as it is to range table list of the query.
 *
 * Such as if user runs a query like this;
 *
 * SELECT avg(id) FROM (
 *     SELECT ... FROM ()
 * )
 *
 * then this function will build this worker query as keeping subquery as it is;
 *
 * SELECT sum(id), count(id) FROM (
 *     SELECT ... FROM ()
 * )
 */
static Query *
BuildSubqueryJobQuery(MultiNode *multiNode)
{
	List *targetList = NIL;
	List *sortClauseList = NIL;
	Node *havingQual = NULL;
	Node *limitCount = NULL;
	Node *limitOffset = NULL;
	bool hasAggregates = false;
	List *distinctClause = NIL;
	bool hasDistinctOn = false;
	bool hasWindowFuncs = false;
	List *windowClause = NIL;

	/* we start building jobs from below the collect node */
	Assert(!CitusIsA(multiNode, MultiCollect));

	List *subqueryMultiTableList = SubqueryMultiTableList(multiNode);
	Assert(list_length(subqueryMultiTableList) == 1);

	MultiTable *multiTable = (MultiTable *) linitial(subqueryMultiTableList);
	Query *subquery = multiTable->subquery;

	/*  build subquery range table list */
	RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
	rangeTableEntry->rtekind = RTE_SUBQUERY;
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->eref = multiTable->referenceNames;
	rangeTableEntry->alias = multiTable->alias;
	rangeTableEntry->subquery = subquery;

	List *rangeTableList = list_make1(rangeTableEntry);

	/*
	 * If we have an extended operator, then we copy the operator's target list.
	 * Otherwise, we use the target list based on the MultiProject node at this
	 * level in the query tree.
	 */
	List *extendedOpNodeList = FindNodesOfType(multiNode, T_MultiExtendedOp);
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);
		targetList = copyObject(extendedOp->targetList);
	}
	else
	{
		targetList = QueryTargetList(multiNode);
	}

	/* extract limit count/offset, sort and having clauses */
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);

		limitCount = extendedOp->limitCount;
		limitOffset = extendedOp->limitOffset;
		sortClauseList = extendedOp->sortClauseList;
		havingQual = extendedOp->havingQual;
		distinctClause = extendedOp->distinctClause;
		hasDistinctOn = extendedOp->hasDistinctOn;
		hasWindowFuncs = extendedOp->hasWindowFuncs;
		windowClause = extendedOp->windowClause;
	}

	/* build group clauses */
	List *groupClauseList = QueryGroupClauseList(multiNode);

	/* build the where clause list using select predicates */
	List *whereClauseList = QuerySelectClauseList(multiNode);

	if (contain_aggs_of_level((Node *) targetList, 0) ||
		contain_aggs_of_level((Node *) havingQual, 0))
	{
		hasAggregates = true;
	}

	/* distinct is not sent to worker query if there are top level aggregates */
	if (hasAggregates)
	{
		hasDistinctOn = false;
		distinctClause = NIL;
	}


	/*
	 * Build the From/Where construct. We keep the where-clause list implicitly
	 * AND'd, since both partition and join pruning depends on the clauses being
	 * expressed as a list.
	 */
	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->quals = (Node *) whereClauseList;
	joinTree->fromlist = QueryFromList(rangeTableList);

	/* build the query structure for this job */
	Query *jobQuery = makeNode(Query);
	jobQuery->commandType = CMD_SELECT;
	jobQuery->querySource = QSRC_ORIGINAL;
	jobQuery->canSetTag = true;
	jobQuery->rtable = rangeTableList;
	jobQuery->targetList = targetList;
	jobQuery->jointree = joinTree;
	jobQuery->sortClause = sortClauseList;
	jobQuery->groupClause = groupClauseList;
	jobQuery->limitOffset = limitOffset;
	jobQuery->limitCount = limitCount;
	jobQuery->havingQual = havingQual;
	jobQuery->hasAggs = hasAggregates;
	jobQuery->hasDistinctOn = hasDistinctOn;
	jobQuery->distinctClause = distinctClause;
	jobQuery->hasWindowFuncs = hasWindowFuncs;
	jobQuery->windowClause = windowClause;
	jobQuery->hasSubLinks = checkExprHasSubLink((Node *) jobQuery);

	Assert(jobQuery->hasWindowFuncs == contain_window_function((Node *) jobQuery));

	return jobQuery;
}


/*
 * UpdateAllColumnAttributes extracts column references from provided columnContainer
 * and calls UpdateColumnAttributes to updates the column's range table reference (varno) and
 * column attribute number for the range table (varattno).
 */
static void
UpdateAllColumnAttributes(Node *columnContainer, List *rangeTableList,
						  List *dependentJobList)
{
	ListCell *columnCell = NULL;
	List *columnList = pull_var_clause_default(columnContainer);
	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		UpdateColumnAttributes(column, rangeTableList, dependentJobList);
	}
}


/*
 * UpdateColumnAttributes updates the column's range table reference (varno) and
 * column attribute number for the range table (varattno). The function uses the
 * newly built range table list to update the given column's attributes.
 */
static void
UpdateColumnAttributes(Var *column, List *rangeTableList, List *dependentJobList)
{
	Index originalTableId = column->varnoold;
	AttrNumber originalColumnId = column->varoattno;

	/* find the new table identifier */
	Index newTableId = NewTableId(originalTableId, rangeTableList);
	AttrNumber newColumnId = originalColumnId;

	/* if this is a derived table, find the new column identifier */
	RangeTblEntry *newRangeTableEntry = rt_fetch(newTableId, rangeTableList);
	if (GetRangeTblKind(newRangeTableEntry) == CITUS_RTE_REMOTE_QUERY)
	{
		newColumnId = NewColumnId(originalTableId, originalColumnId,
								  newRangeTableEntry, dependentJobList);
	}

	column->varno = newTableId;
	column->varattno = newColumnId;
}


/*
 * NewTableId determines the new tableId for the query that is currently being
 * built. In this query, the original tableId represents the order of the table
 * in the initial parse tree. When queries involve repartitioning, we re-order
 * tables; and the new tableId corresponds to this new table order.
 */
static Index
NewTableId(Index originalTableId, List *rangeTableList)
{
	Index rangeTableIndex = 1;
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		List *originalTableIdList = NIL;

		ExtractRangeTblExtraData(rangeTableEntry, NULL, NULL, NULL, &originalTableIdList);

		bool listMember = list_member_int(originalTableIdList, originalTableId);
		if (listMember)
		{
			return rangeTableIndex;
		}

		rangeTableIndex++;
	}

	ereport(ERROR, (errmsg("Unrecognized range table id %d", (int) originalTableId)));

	return 0;
}


/*
 * NewColumnId determines the new columnId for the query that is currently being
 * built. In this query, the original columnId corresponds to the column in base
 * tables. When the current query is a partition job and generates intermediate
 * tables, the columns have a different order and the new columnId corresponds
 * to this order. Please note that this function assumes columnIds for dependent
 * jobs have already been updated.
 */
static AttrNumber
NewColumnId(Index originalTableId, AttrNumber originalColumnId,
			RangeTblEntry *newRangeTableEntry, List *dependentJobList)
{
	AttrNumber newColumnId = 1;
	AttrNumber columnIndex = 1;

	Job *dependentJob = JobForRangeTable(dependentJobList, newRangeTableEntry);
	List *targetEntryList = dependentJob->jobQuery->targetList;

	ListCell *targetEntryCell = NULL;
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Expr *expression = targetEntry->expr;

		Var *column = (Var *) expression;
		Assert(IsA(expression, Var));

		/*
		 * Check against the *old* values for this column, as the new values
		 * would have been updated already.
		 */
		if (column->varnoold == originalTableId &&
			column->varoattno == originalColumnId)
		{
			newColumnId = columnIndex;
			break;
		}

		columnIndex++;
	}

	return newColumnId;
}


/*
 * JobForRangeTable returns the job that corresponds to the given range table
 * entry. The function walks over jobs in the given job list, and compares each
 * job's table list against the given range table entry's table list. When two
 * table lists match, the function returns the matching job. Note that we call
 * this function in practice when we need to determine which one of the jobs we
 * depend upon corresponds to given range table entry.
 */
static Job *
JobForRangeTable(List *jobList, RangeTblEntry *rangeTableEntry)
{
	List *searchedTableIdList = NIL;
	CitusRTEKind rangeTableKind;

	ExtractRangeTblExtraData(rangeTableEntry, &rangeTableKind, NULL, NULL,
							 &searchedTableIdList);

	Assert(rangeTableKind == CITUS_RTE_REMOTE_QUERY);

	Job *searchedJob = JobForTableIdList(jobList, searchedTableIdList);

	return searchedJob;
}


/*
 * JobForTableIdList returns the job that corresponds to the given
 * tableIdList. The function walks over jobs in the given job list, and
 * compares each job's table list against the given table list. When the
 * two table lists match, the function returns the matching job.
 */
static Job *
JobForTableIdList(List *jobList, List *searchedTableIdList)
{
	Job *searchedJob = NULL;
	ListCell *jobCell = NULL;

	foreach(jobCell, jobList)
	{
		Job *job = (Job *) lfirst(jobCell);
		List *jobRangeTableList = job->jobQuery->rtable;
		List *jobTableIdList = NIL;
		ListCell *jobRangeTableCell = NULL;

		foreach(jobRangeTableCell, jobRangeTableList)
		{
			RangeTblEntry *jobRangeTable = (RangeTblEntry *) lfirst(jobRangeTableCell);
			List *tableIdList = NIL;

			ExtractRangeTblExtraData(jobRangeTable, NULL, NULL, NULL, &tableIdList);

			/* copy the list since list_concat is destructive */
			tableIdList = list_copy(tableIdList);
			jobTableIdList = list_concat(jobTableIdList, tableIdList);
		}

		/*
		 * Check if the searched range table's tableIds and the current job's
		 * tableIds are the same.
		 */
		List *lhsDiff = list_difference_int(jobTableIdList, searchedTableIdList);
		List *rhsDiff = list_difference_int(searchedTableIdList, jobTableIdList);
		if (lhsDiff == NIL && rhsDiff == NIL)
		{
			searchedJob = job;
			break;
		}
	}

	Assert(searchedJob != NULL);
	return searchedJob;
}


/* Returns the list of children for the given multi node. */
static List *
ChildNodeList(MultiNode *multiNode)
{
	List *childNodeList = NIL;
	bool isUnaryNode = UnaryOperator(multiNode);
	bool isBinaryNode = BinaryOperator(multiNode);

	/* relation table nodes don't have any children */
	if (CitusIsA(multiNode, MultiTable))
	{
		MultiTable *multiTable = (MultiTable *) multiNode;
		if (multiTable->relationId != SUBQUERY_RELATION_ID)
		{
			return NIL;
		}
	}

	if (isUnaryNode)
	{
		MultiUnaryNode *unaryNode = (MultiUnaryNode *) multiNode;
		childNodeList = list_make1(unaryNode->childNode);
	}
	else if (isBinaryNode)
	{
		MultiBinaryNode *binaryNode = (MultiBinaryNode *) multiNode;
		childNodeList = list_make2(binaryNode->leftChildNode,
								   binaryNode->rightChildNode);
	}

	return childNodeList;
}


/*
 * UniqueJobId allocates and returns a unique jobId for the job to be executed.
 *
 * The resulting job ID is built up as:
 * <16-bit group ID><24-bit process ID><1-bit secondary flag><23-bit local counter>
 *
 * When citus.enable_unique_job_ids is off then only the local counter is
 * included to get repeatable results.
 */
uint64
UniqueJobId(void)
{
	static uint32 jobIdCounter = 0;

	uint64 jobId = 0;
	uint64 processId = 0;
	uint64 localGroupId = 0;

	jobIdCounter++;

	if (EnableUniqueJobIds)
	{
		/*
		 * Add the local group id information to the jobId to
		 * prevent concurrent jobs on different groups to conflict.
		 */
		localGroupId = GetLocalGroupId() & 0xFF;
		jobId = jobId | (localGroupId << 48);

		/*
		 * Add the current process ID to distinguish jobs by this
		 * backends from jobs started by other backends. Process
		 * IDs can have at most 24-bits on platforms supported by
		 * Citus.
		 */
		processId = MyProcPid & 0xFFFFFF;
		jobId = jobId | (processId << 24);

		/*
		 * Add an extra bit for secondaries to distinguish their
		 * jobs from primaries.
		 */
		if (RecoveryInProgress())
		{
			jobId = jobId | (1 << 23);
		}
	}

	/*
	 * Use the remaining 23 bits to distinguish jobs by the
	 * same backend.
	 */
	uint64 jobIdNumber = jobIdCounter & 0x1FFFFFF;
	jobId = jobId | jobIdNumber;

	return jobId;
}


/* Builds a job from the given job query and dependent job list. */
static Job *
BuildJob(Query *jobQuery, List *dependentJobList)
{
	Job *job = CitusMakeNode(Job);
	job->jobId = UniqueJobId();
	job->jobQuery = jobQuery;
	job->dependentJobList = dependentJobList;
	job->requiresMasterEvaluation = false;

	return job;
}


/*
 * BuildMapMergeJob builds a MapMerge job from the given query and dependent job
 * list. The function then copies and updates the logical plan's partition
 * column, and uses the join rule type to determine the physical repartitioning
 * method to apply.
 */
static MapMergeJob *
BuildMapMergeJob(Query *jobQuery, List *dependentJobList, Var *partitionKey,
				 PartitionType partitionType, Oid baseRelationId,
				 BoundaryNodeJobType boundaryNodeJobType)
{
	List *rangeTableList = jobQuery->rtable;
	Var *partitionColumn = copyObject(partitionKey);

	/* update the logical partition key's table and column identifiers */
	if (boundaryNodeJobType != SUBQUERY_MAP_MERGE_JOB)
	{
		UpdateColumnAttributes(partitionColumn, rangeTableList, dependentJobList);
	}

	MapMergeJob *mapMergeJob = CitusMakeNode(MapMergeJob);
	mapMergeJob->job.jobId = UniqueJobId();
	mapMergeJob->job.jobQuery = jobQuery;
	mapMergeJob->job.dependentJobList = dependentJobList;
	mapMergeJob->partitionColumn = partitionColumn;
	mapMergeJob->sortedShardIntervalArrayLength = 0;

	/*
	 * We assume dual partition join defaults to hash partitioning, and single
	 * partition join defaults to range partitioning. In practice, the join type
	 * should have no impact on the physical repartitioning (hash/range) method.
	 * If join type is not set, this means this job represents a subquery, and
	 * uses hash partitioning.
	 */
	if (partitionType == DUAL_HASH_PARTITION_TYPE)
	{
		uint32 partitionCount = HashPartitionCount();

		mapMergeJob->partitionType = DUAL_HASH_PARTITION_TYPE;
		mapMergeJob->partitionCount = partitionCount;
	}
	else if (partitionType == SINGLE_HASH_PARTITION_TYPE || partitionType ==
			 RANGE_PARTITION_TYPE)
	{
		CitusTableCacheEntry *cache = GetCitusTableCacheEntry(baseRelationId);
		int shardCount = cache->shardIntervalArrayLength;
		ShardInterval **cachedSortedShardIntervalArray =
			cache->sortedShardIntervalArray;
		bool hasUninitializedShardInterval =
			cache->hasUninitializedShardInterval;

		ShardInterval **sortedShardIntervalArray =
			palloc0(sizeof(ShardInterval) * shardCount);

		for (int shardIndex = 0; shardIndex < shardCount; shardIndex++)
		{
			sortedShardIntervalArray[shardIndex] =
				CopyShardInterval(cachedSortedShardIntervalArray[shardIndex]);
		}

		if (hasUninitializedShardInterval)
		{
			ereport(ERROR, (errmsg("cannot range repartition shard with "
								   "missing min/max values")));
		}

		mapMergeJob->partitionType = partitionType;
		mapMergeJob->partitionCount = (uint32) shardCount;
		mapMergeJob->sortedShardIntervalArray = sortedShardIntervalArray;
		mapMergeJob->sortedShardIntervalArrayLength = shardCount;
	}

	return mapMergeJob;
}


/*
 * HashPartitionCount returns the number of partition files we create for a hash
 * partition task. The function follows Hadoop's method for picking the number
 * of reduce tasks: 0.95 or 1.75 * node count * max reduces per node. We choose
 * the lower constant 0.95 so that all tasks can start immediately, but round it
 * to 1.0 so that we have a smooth number of partition tasks.
 */
static uint32
HashPartitionCount(void)
{
	uint32 groupCount = ActiveReadableWorkerNodeCount();
	double maxReduceTasksPerNode = MaxRunningTasksPerNode / 2.0;

	uint32 partitionCount = (uint32) rint(groupCount * maxReduceTasksPerNode);
	return partitionCount;
}


/*
 * SplitPointObject walks over shard intervals in the given array, extracts each
 * shard interval's minimum value, sorts and inserts these minimum values into a
 * new array. This sorted array is then used by the MapMerge job.
 */
static ArrayType *
SplitPointObject(ShardInterval **shardIntervalArray, uint32 shardIntervalCount)
{
	Oid typeId = InvalidOid;
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;

	/* allocate an array for shard min values */
	uint32 minDatumCount = shardIntervalCount;
	Datum *minDatumArray = palloc0(minDatumCount * sizeof(Datum));

	for (uint32 intervalIndex = 0; intervalIndex < shardIntervalCount; intervalIndex++)
	{
		ShardInterval *shardInterval = shardIntervalArray[intervalIndex];
		minDatumArray[intervalIndex] = shardInterval->minValue;
		Assert(shardInterval->minValueExists);

		/* resolve the datum type on the first pass */
		if (intervalIndex == 0)
		{
			typeId = shardInterval->valueTypeId;
		}
	}

	/* construct the split point object from the sorted array */
	get_typlenbyvalalign(typeId, &typeLength, &typeByValue, &typeAlignment);
	ArrayType *splitPointObject = construct_array(minDatumArray, minDatumCount, typeId,
												  typeLength, typeByValue, typeAlignment);

	return splitPointObject;
}


/* ------------------------------------------------------------
 * Functions that relate to building and assigning tasks follow
 * ------------------------------------------------------------
 */

/*
 * BuildJobTreeTaskList takes in the given job tree and walks over jobs in this
 * tree bottom up. The function then creates tasks for each job in the tree,
 * sets dependencies between tasks and their downstream dependencies and assigns
 * tasks to worker nodes.
 */
static Job *
BuildJobTreeTaskList(Job *jobTree, PlannerRestrictionContext *plannerRestrictionContext)
{
	List *flattenedJobList = NIL;

	/*
	 * We traverse the job tree in preorder, and append each visited job to our
	 * flattened list. This way, each job in our list appears before the jobs it
	 * depends on.
	 */
	List *jobStack = list_make1(jobTree);
	while (jobStack != NIL)
	{
		Job *job = (Job *) llast(jobStack);
		flattenedJobList = lappend(flattenedJobList, job);

		/* pop top element and push its children to the stack */
		jobStack = list_delete_ptr(jobStack, job);
		jobStack = list_union_ptr(jobStack, job->dependentJobList);
	}

	/*
	 * We walk the job list in reverse order to visit jobs bottom up. This way,
	 * we can create dependencies between tasks bottom up, and assign them to
	 * worker nodes accordingly.
	 */
	uint32 flattenedJobCount = (int32) list_length(flattenedJobList);
	for (int32 jobIndex = (flattenedJobCount - 1); jobIndex >= 0; jobIndex--)
	{
		Job *job = (Job *) list_nth(flattenedJobList, jobIndex);
		List *sqlTaskList = NIL;
		ListCell *assignedSqlTaskCell = NULL;

		/* create sql tasks for the job, and prune redundant data fetch tasks */
		if (job->subqueryPushdown)
		{
			bool isMultiShardQuery = false;
			List *prunedRelationShardList =
				TargetShardIntervalsForRestrictInfo(plannerRestrictionContext->
													relationRestrictionContext,
													&isMultiShardQuery, NULL);

			sqlTaskList = QueryPushdownSqlTaskList(job->jobQuery, job->jobId,
												   plannerRestrictionContext->
												   relationRestrictionContext,
												   prunedRelationShardList, READ_TASK,
												   false);
		}
		else
		{
			sqlTaskList = SqlTaskList(job);
		}

		sqlTaskList = PruneSqlTaskDependencies(sqlTaskList);

		/*
		 * We first assign sql and merge tasks to worker nodes. Next, we assign
		 * sql tasks' data fetch dependencies.
		 */
		List *assignedSqlTaskList = AssignTaskList(sqlTaskList);
		AssignDataFetchDependencies(assignedSqlTaskList);

		/* if the parameters has not been resolved, record it */
		job->parametersInJobQueryResolved =
			!HasUnresolvedExternParamsWalker((Node *) job->jobQuery, NULL);

		/*
		 * Make final adjustments for the assigned tasks.
		 *
		 * First, update SELECT tasks' parameters resolved field.
		 *
		 * Second, assign merge task's data fetch dependencies.
		 */
		foreach(assignedSqlTaskCell, assignedSqlTaskList)
		{
			Task *assignedSqlTask = (Task *) lfirst(assignedSqlTaskCell);

			/* we don't support parameters in the physical planner */
			if (assignedSqlTask->taskType == READ_TASK)
			{
				assignedSqlTask->parametersInQueryStringResolved =
					job->parametersInJobQueryResolved;
			}

			List *assignedMergeTaskList = FindDependentMergeTaskList(assignedSqlTask);
			AssignDataFetchDependencies(assignedMergeTaskList);
		}

		/*
		 * If we have a MapMerge job, the map tasks in this job wrap around the
		 * SQL tasks and their assignments.
		 */
		if (CitusIsA(job, MapMergeJob))
		{
			MapMergeJob *mapMergeJob = (MapMergeJob *) job;
			uint32 taskIdIndex = TaskListHighestTaskId(assignedSqlTaskList) + 1;

			List *mapTaskList = MapTaskList(mapMergeJob, assignedSqlTaskList);
			List *mergeTaskList = MergeTaskList(mapMergeJob, mapTaskList, taskIdIndex);

			mapMergeJob->mapTaskList = mapTaskList;
			mapMergeJob->mergeTaskList = mergeTaskList;
		}
		else
		{
			job->taskList = assignedSqlTaskList;
		}
	}

	return jobTree;
}


/*
 * QueryPushdownSqlTaskList creates a list of SQL tasks to execute the given subquery
 * pushdown job. For this, it is being checked whether the query is router
 * plannable per target shard interval. For those router plannable worker
 * queries, we create a SQL task and append the task to the task list that is going
 * to be executed.
 */
List *
QueryPushdownSqlTaskList(Query *query, uint64 jobId,
						 RelationRestrictionContext *relationRestrictionContext,
						 List *prunedRelationShardList, TaskType taskType, bool
						 modifyRequiresMasterEvaluation)
{
	List *sqlTaskList = NIL;
	ListCell *restrictionCell = NULL;
	uint32 taskIdIndex = 1; /* 0 is reserved for invalid taskId */
	int shardCount = 0;
	bool *taskRequiredForShardIndex = NULL;
	ListCell *prunedRelationShardCell = NULL;

	/* error if shards are not co-partitioned */
	ErrorIfUnsupportedShardDistribution(query);

	if (list_length(relationRestrictionContext->relationRestrictionList) == 0)
	{
		ereport(ERROR, (errmsg("cannot handle complex subqueries when the "
							   "router executor is disabled")));
	}

	/* defaults to be used if this is a reference table-only query */
	int minShardOffset = 0;
	int maxShardOffset = 0;

	forboth(prunedRelationShardCell, prunedRelationShardList,
			restrictionCell, relationRestrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;
		List *prunedShardList = (List *) lfirst(prunedRelationShardCell);
		ListCell *shardIntervalCell = NULL;

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE)
		{
			continue;
		}

		/* we expect distributed tables to have the same shard count */
		if (shardCount > 0 && shardCount != cacheEntry->shardIntervalArrayLength)
		{
			ereport(ERROR, (errmsg("shard counts of co-located tables do not "
								   "match")));
		}

		if (taskRequiredForShardIndex == NULL)
		{
			shardCount = cacheEntry->shardIntervalArrayLength;
			taskRequiredForShardIndex = (bool *) palloc0(shardCount);

			/* there is a distributed table, find the shard range */
			minShardOffset = shardCount;
			maxShardOffset = -1;
		}

		/*
		 * For left joins we don't care about the shards pruned for the right hand side.
		 * If the right hand side would prune to a smaller set we should still send it to
		 * all tables of the left hand side. However if the right hand side is bigger than
		 * the left hand side we don't have to send the query to any shard that is not
		 * matching anything on the left hand side.
		 *
		 * Instead we will simply skip any RelationRestriction if it is an OUTER join and
		 * the table is part of the non-outer side of the join.
		 */
		if (IsInnerTableOfOuterJoin(relationRestriction))
		{
			continue;
		}

		foreach(shardIntervalCell, prunedShardList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
			int shardIndex = shardInterval->shardIndex;

			taskRequiredForShardIndex[shardIndex] = true;

			minShardOffset = Min(minShardOffset, shardIndex);
			maxShardOffset = Max(maxShardOffset, shardIndex);
		}
	}

	/*
	 * To avoid iterating through all shards indexes we keep the minimum and maximum
	 * offsets of shards that were not pruned away. This optimisation is primarily
	 * relevant for queries on range-distributed tables that, due to range filters,
	 * prune to a small number of adjacent shards.
	 *
	 * In other cases, such as an OR condition on a hash-distributed table, we may
	 * still visit most or all shards even if some of them were pruned away. However,
	 * given that hash-distributed tables typically only have a few shards the
	 * iteration is still very fast.
	 */
	for (int shardOffset = minShardOffset; shardOffset <= maxShardOffset; shardOffset++)
	{
		if (taskRequiredForShardIndex != NULL && !taskRequiredForShardIndex[shardOffset])
		{
			/* this shard index is pruned away for all relations */
			continue;
		}

		Task *subqueryTask = QueryPushdownTaskCreate(query, shardOffset,
													 relationRestrictionContext,
													 taskIdIndex,
													 taskType,
													 modifyRequiresMasterEvaluation);
		subqueryTask->jobId = jobId;
		sqlTaskList = lappend(sqlTaskList, subqueryTask);

		++taskIdIndex;
	}

	/* If it is a modify task with multiple tables */
	if (taskType == MODIFY_TASK && list_length(
			relationRestrictionContext->relationRestrictionList) > 1)
	{
		ListCell *taskCell = NULL;
		foreach(taskCell, sqlTaskList)
		{
			Task *task = (Task *) lfirst(taskCell);
			task->modifyWithSubquery = true;
		}
	}

	return sqlTaskList;
}


/*
 * IsInnerTableOfOuterJoin tests based on the join information envoded in a
 * RelationRestriction if the table accessed for this relation is
 *   a) in an outer join
 *   b) on the inner part of said join
 *
 * The function returns true only if both conditions above hold true
 */
static bool
IsInnerTableOfOuterJoin(RelationRestriction *relationRestriction)
{
	RestrictInfo *joinInfo = NULL;
	foreach_ptr(joinInfo, relationRestriction->relOptInfo->joininfo)
	{
		if (joinInfo->outer_relids == NULL)
		{
			/* not an outer join */
			continue;
		}

		/*
		 * This join restriction info describes an outer join, we need to figure out if
		 * our table is in the non outer part of this join. If that is the case this is a
		 * non outer table of an outer join.
		 */
		bool isInOuter = bms_is_member(relationRestriction->relOptInfo->relid,
									   joinInfo->outer_relids);
		if (!isInOuter)
		{
			/* this table is joined in the inner part of an outer join */
			return true;
		}
	}

	/* we have not found any join clause that satisfies both requirements */
	return false;
}


/*
 * ErrorIfUnsupportedShardDistribution gets list of relations in the given query
 * and checks if two conditions below hold for them, otherwise it errors out.
 * a. Every relation is distributed by range or hash. This means shards are
 * disjoint based on the partition column.
 * b. All relations have 1-to-1 shard partitioning between them. This means
 * shard count for every relation is same and for every shard in a relation
 * there is exactly one shard in other relations with same min/max values.
 */
static void
ErrorIfUnsupportedShardDistribution(Query *query)
{
	Oid firstTableRelationId = InvalidOid;
	List *relationIdList = DistributedRelationIdList(query);
	List *nonReferenceRelations = NIL;
	ListCell *relationIdCell = NULL;
	uint32 relationIndex = 0;
	uint32 rangeDistributedRelationCount = 0;
	uint32 hashDistributedRelationCount = 0;
	uint32 appendDistributedRelationCount = 0;

	foreach(relationIdCell, relationIdList)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		char partitionMethod = PartitionMethod(relationId);
		if (partitionMethod == DISTRIBUTE_BY_RANGE)
		{
			rangeDistributedRelationCount++;
			nonReferenceRelations = lappend_oid(nonReferenceRelations,
												relationId);
		}
		else if (partitionMethod == DISTRIBUTE_BY_HASH)
		{
			hashDistributedRelationCount++;
			nonReferenceRelations = lappend_oid(nonReferenceRelations,
												relationId);
		}
		else if (partitionMethod == DISTRIBUTE_BY_NONE)
		{
			/* do not need to handle reference tables */
			continue;
		}
		else
		{
			CitusTableCacheEntry *distTableEntry = GetCitusTableCacheEntry(relationId);
			if (distTableEntry->hasOverlappingShardInterval)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot push down this subquery"),
								errdetail("Currently append partitioned relations "
										  "with overlapping shard intervals are "
										  "not supported")));
			}

			appendDistributedRelationCount++;
		}
	}

	if ((rangeDistributedRelationCount > 0) && (hashDistributedRelationCount > 0))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("A query including both range and hash "
								  "partitioned relations are unsupported")));
	}
	else if ((rangeDistributedRelationCount > 0) && (appendDistributedRelationCount > 0))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("A query including both range and append "
								  "partitioned relations are unsupported")));
	}
	else if ((appendDistributedRelationCount > 0) && (hashDistributedRelationCount > 0))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("A query including both append and hash "
								  "partitioned relations are unsupported")));
	}

	foreach(relationIdCell, nonReferenceRelations)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		Oid currentRelationId = relationId;

		/* get shard list of first relation and continue for the next relation */
		if (relationIndex == 0)
		{
			firstTableRelationId = relationId;
			relationIndex++;

			continue;
		}

		/* check if this table has 1-1 shard partitioning with first table */
		bool coPartitionedTables = CoPartitionedTables(firstTableRelationId,
													   currentRelationId);
		if (!coPartitionedTables)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot push down this subquery"),
							errdetail("Shards of relations in subquery need to "
									  "have 1-to-1 shard partitioning")));
		}
	}
}


/*
 * SubqueryTaskCreate creates a sql task by replacing the target
 * shardInterval's boundary value.
 */
static Task *
QueryPushdownTaskCreate(Query *originalQuery, int shardIndex,
						RelationRestrictionContext *restrictionContext, uint32 taskId,
						TaskType taskType, bool modifyRequiresMasterEvaluation)
{
	Query *taskQuery = copyObject(originalQuery);

	StringInfo queryString = makeStringInfo();
	ListCell *restrictionCell = NULL;
	List *taskShardList = NIL;
	List *relationShardList = NIL;
	uint64 jobId = INVALID_JOB_ID;
	uint64 anchorShardId = INVALID_SHARD_ID;
	bool modifyWithSubselect = false;
	RangeTblEntry *resultRangeTable = NULL;
	Oid resultRelationOid = InvalidOid;

	/*
	 * If it is a modify query with sub-select, we need to set result relation shard's id
	 * as anchor shard id.
	 */
	if (UpdateOrDeleteQuery(originalQuery))
	{
		resultRangeTable = rt_fetch(originalQuery->resultRelation, originalQuery->rtable);
		resultRelationOid = resultRangeTable->relid;
		modifyWithSubselect = true;
	}

	/*
	 * Find the relevant shard out of each relation for this task.
	 */
	foreach(restrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;
		ShardInterval *shardInterval = NULL;

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE)
		{
			/* reference table only has one shard */
			shardInterval = cacheEntry->sortedShardIntervalArray[0];

			/* only use reference table as anchor shard if none exists yet */
			if (anchorShardId == INVALID_SHARD_ID)
			{
				anchorShardId = shardInterval->shardId;
			}
		}
		else if (UpdateOrDeleteQuery(originalQuery))
		{
			shardInterval = cacheEntry->sortedShardIntervalArray[shardIndex];
			if (!modifyWithSubselect || relationId == resultRelationOid)
			{
				/* for UPDATE/DELETE the shard in the result relation becomes the anchor shard */
				anchorShardId = shardInterval->shardId;
			}
		}
		else
		{
			/* for SELECT we pick an arbitrary shard as the anchor shard */
			shardInterval = cacheEntry->sortedShardIntervalArray[shardIndex];
			anchorShardId = shardInterval->shardId;
		}

		ShardInterval *copiedShardInterval = CopyShardInterval(shardInterval);

		taskShardList = lappend(taskShardList, list_make1(copiedShardInterval));

		RelationShard *relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = copiedShardInterval->relationId;
		relationShard->shardId = copiedShardInterval->shardId;

		relationShardList = lappend(relationShardList, relationShard);
	}

	Assert(anchorShardId != INVALID_SHARD_ID);

	List *selectPlacementList = WorkersContainingAllShards(taskShardList);
	if (list_length(selectPlacementList) == 0)
	{
		ereport(ERROR, (errmsg("cannot find a worker that has active placements for all "
							   "shards in the query")));
	}

	/*
	 * Augment the relations in the query with the shard IDs.
	 */
	UpdateRelationToShardNames((Node *) taskQuery, relationShardList);

	/*
	 * Ands are made implicit during shard pruning, as predicate comparison and
	 * refutation depend on it being so. We need to make them explicit again so
	 * that the query string is generated as (...) AND (...) as opposed to
	 * (...), (...).
	 */
	if (taskQuery->jointree->quals != NULL && IsA(taskQuery->jointree->quals, List))
	{
		taskQuery->jointree->quals = (Node *) make_ands_explicit(
			(List *) taskQuery->jointree->quals);
	}

	Task *subqueryTask = CreateBasicTask(jobId, taskId, taskType, NULL);

	if ((taskType == MODIFY_TASK && !modifyRequiresMasterEvaluation) ||
		taskType == READ_TASK)
	{
		pg_get_query_def(taskQuery, queryString);
		ereport(DEBUG4, (errmsg("distributed statement: %s",
								ApplyLogRedaction(queryString->data))));
		SetTaskQueryString(subqueryTask, queryString->data);
	}

	subqueryTask->dependentTaskList = NULL;
	subqueryTask->anchorShardId = anchorShardId;
	subqueryTask->taskPlacementList = selectPlacementList;
	subqueryTask->relationShardList = relationShardList;

	return subqueryTask;
}


/*
 * CoPartitionedTables checks if given two distributed tables have 1-to-1 shard
 * placement matching. It first checks for the shard count, if tables don't have
 * same amount shard then it returns false. Note that, if any table does not
 * have any shard, it returns true. If two tables have same amount of shards,
 * we check colocationIds for hash distributed tables and shardInterval's min
 * max values for append and range distributed tables.
 */
bool
CoPartitionedTables(Oid firstRelationId, Oid secondRelationId)
{
	if (firstRelationId == secondRelationId)
	{
		return true;
	}

	CitusTableCacheEntry *firstTableCache = GetCitusTableCacheEntry(firstRelationId);
	CitusTableCacheEntry *secondTableCache = GetCitusTableCacheEntry(secondRelationId);

	ShardInterval **sortedFirstIntervalArray = firstTableCache->sortedShardIntervalArray;
	ShardInterval **sortedSecondIntervalArray =
		secondTableCache->sortedShardIntervalArray;
	uint32 firstListShardCount = firstTableCache->shardIntervalArrayLength;
	uint32 secondListShardCount = secondTableCache->shardIntervalArrayLength;
	FmgrInfo *comparisonFunction = firstTableCache->shardIntervalCompareFunction;

	/* reference tables are always & only copartitioned with reference tables */
	if (firstTableCache->partitionMethod == DISTRIBUTE_BY_NONE &&
		secondTableCache->partitionMethod == DISTRIBUTE_BY_NONE)
	{
		return true;
	}
	else if (firstTableCache->partitionMethod == DISTRIBUTE_BY_NONE ||
			 secondTableCache->partitionMethod == DISTRIBUTE_BY_NONE)
	{
		return false;
	}

	if (firstListShardCount != secondListShardCount)
	{
		return false;
	}

	/* if there are not any shards just return true */
	if (firstListShardCount == 0)
	{
		return true;
	}

	Assert(comparisonFunction != NULL);

	/*
	 * Check if the tables have the same colocation ID - if so, we know
	 * they're colocated.
	 */
	if (firstTableCache->colocationId != INVALID_COLOCATION_ID &&
		firstTableCache->colocationId == secondTableCache->colocationId)
	{
		return true;
	}

	/*
	 * For hash distributed tables two tables are accepted as colocated only if
	 * they have the same colocationId. Otherwise they may have same minimum and
	 * maximum values for each shard interval, yet hash function may result with
	 * different values for the same value. int vs bigint can be given as an
	 * example.
	 */
	if (firstTableCache->partitionMethod == DISTRIBUTE_BY_HASH ||
		secondTableCache->partitionMethod == DISTRIBUTE_BY_HASH)
	{
		return false;
	}


	/*
	 * Don't compare unequal types
	 */
	Oid collation = firstTableCache->partitionColumn->varcollid;
	if (firstTableCache->partitionColumn->vartype !=
		secondTableCache->partitionColumn->vartype ||
		collation != secondTableCache->partitionColumn->varcollid)
	{
		return false;
	}


	/*
	 * If not known to be colocated check if the remaining shards are
	 * anyway. Do so by comparing the shard interval arrays that are sorted on
	 * interval minimum values. Then it compares every shard interval in order
	 * and if any pair of shard intervals are not equal or they are not located
	 * in the same node it returns false.
	 */
	for (uint32 intervalIndex = 0; intervalIndex < firstListShardCount; intervalIndex++)
	{
		ShardInterval *firstInterval = sortedFirstIntervalArray[intervalIndex];
		ShardInterval *secondInterval = sortedSecondIntervalArray[intervalIndex];

		bool shardIntervalsEqual = ShardIntervalsEqual(comparisonFunction,
													   collation,
													   firstInterval,
													   secondInterval);
		if (!shardIntervalsEqual || !CoPlacedShardIntervals(firstInterval,
															secondInterval))
		{
			return false;
		}
	}

	return true;
}


/*
 * CoPlacedShardIntervals checks whether the given intervals located in the same nodes.
 */
static bool
CoPlacedShardIntervals(ShardInterval *firstInterval, ShardInterval *secondInterval)
{
	List *firstShardPlacementList = ShardPlacementList(firstInterval->shardId);
	List *secondShardPlacementList = ShardPlacementList(secondInterval->shardId);
	ListCell *firstShardPlacementCell = NULL;
	ListCell *secondShardPlacementCell = NULL;

	/* Shards must have same number of placements */
	if (list_length(firstShardPlacementList) != list_length(secondShardPlacementList))
	{
		return false;
	}

	firstShardPlacementList = SortList(firstShardPlacementList, CompareShardPlacements);
	secondShardPlacementList = SortList(secondShardPlacementList, CompareShardPlacements);

	forboth(firstShardPlacementCell, firstShardPlacementList, secondShardPlacementCell,
			secondShardPlacementList)
	{
		ShardPlacement *firstShardPlacement = (ShardPlacement *) lfirst(
			firstShardPlacementCell);
		ShardPlacement *secondShardPlacement = (ShardPlacement *) lfirst(
			secondShardPlacementCell);

		if (firstShardPlacement->nodeId != secondShardPlacement->nodeId)
		{
			return false;
		}
	}

	return true;
}


/*
 * ShardIntervalsEqual checks if given shard intervals have equal min/max values.
 */
static bool
ShardIntervalsEqual(FmgrInfo *comparisonFunction, Oid collation,
					ShardInterval *firstInterval, ShardInterval *secondInterval)
{
	bool shardIntervalsEqual = false;

	Datum firstMin = firstInterval->minValue;
	Datum firstMax = firstInterval->maxValue;
	Datum secondMin = secondInterval->minValue;
	Datum secondMax = secondInterval->maxValue;

	if (firstInterval->minValueExists && firstInterval->maxValueExists &&
		secondInterval->minValueExists && secondInterval->maxValueExists)
	{
		Datum minDatum = FunctionCall2Coll(comparisonFunction, collation, firstMin,
										   secondMin);
		Datum maxDatum = FunctionCall2Coll(comparisonFunction, collation, firstMax,
										   secondMax);
		int firstComparison = DatumGetInt32(minDatum);
		int secondComparison = DatumGetInt32(maxDatum);

		if (firstComparison == 0 && secondComparison == 0)
		{
			shardIntervalsEqual = true;
		}
	}

	return shardIntervalsEqual;
}


/*
 * SqlTaskList creates a list of SQL tasks to execute the given job. For this,
 * the function walks over each range table in the job's range table list, gets
 * each range table's table fragments, and prunes unneeded table fragments. The
 * function then joins table fragments from different range tables, and creates
 * all fragment combinations. For each created combination, the function builds
 * a SQL task, and appends this task to a task list.
 */
static List *
SqlTaskList(Job *job)
{
	List *sqlTaskList = NIL;
	uint32 taskIdIndex = 1; /* 0 is reserved for invalid taskId */
	uint64 jobId = job->jobId;
	bool anchorRangeTableBasedAssignment = false;
	uint32 anchorRangeTableId = 0;

	Query *jobQuery = job->jobQuery;
	List *rangeTableList = jobQuery->rtable;
	List *whereClauseList = (List *) jobQuery->jointree->quals;
	List *dependentJobList = job->dependentJobList;

	/*
	 * If we don't depend on a hash partition, then we determine the largest
	 * table around which we build our queries. This reduces data fetching.
	 */
	bool dependsOnHashPartitionJob = DependsOnHashPartitionJob(job);
	if (!dependsOnHashPartitionJob)
	{
		anchorRangeTableBasedAssignment = true;
		anchorRangeTableId = AnchorRangeTableId(rangeTableList);

		Assert(anchorRangeTableId != 0);
		Assert(anchorRangeTableId <= list_length(rangeTableList));
	}

	/* adjust our column old attributes for partition pruning to work */
	AdjustColumnOldAttributes(whereClauseList);
	AdjustColumnOldAttributes(jobQuery->targetList);

	/*
	 * Ands are made implicit during shard pruning, as predicate comparison and
	 * refutation depend on it being so. We need to make them explicit again so
	 * that the query string is generated as (...) AND (...) as opposed to
	 * (...), (...).
	 */
	Node *whereClauseTree = (Node *) make_ands_explicit(
		(List *) jobQuery->jointree->quals);
	jobQuery->jointree->quals = whereClauseTree;

	/*
	 * For each range table, we first get a list of their shards or merge tasks.
	 * We also apply partition pruning based on the selection criteria. If all
	 * range table fragments are pruned away, we return an empty task list.
	 */
	List *rangeTableFragmentsList = RangeTableFragmentsList(rangeTableList,
															whereClauseList,
															dependentJobList);
	if (rangeTableFragmentsList == NIL)
	{
		return NIL;
	}

	/*
	 * We then generate fragment combinations according to how range tables join
	 * with each other (and apply join pruning). Each fragment combination then
	 * represents one SQL task's dependencies.
	 */
	List *fragmentCombinationList = FragmentCombinationList(rangeTableFragmentsList,
															jobQuery, dependentJobList);

	ListCell *fragmentCombinationCell = NULL;
	foreach(fragmentCombinationCell, fragmentCombinationList)
	{
		List *fragmentCombination = (List *) lfirst(fragmentCombinationCell);

		/* create tasks to fetch fragments required for the sql task */
		List *dataFetchTaskList = DataFetchTaskList(jobId, taskIdIndex,
													fragmentCombination);
		int32 dataFetchTaskCount = list_length(dataFetchTaskList);
		taskIdIndex += dataFetchTaskCount;

		/* update range table entries with fragment aliases (in place) */
		Query *taskQuery = copyObject(jobQuery);
		List *fragmentRangeTableList = taskQuery->rtable;
		UpdateRangeTableAlias(fragmentRangeTableList, fragmentCombination);

		/* transform the updated task query to a SQL query string */
		StringInfo sqlQueryString = makeStringInfo();
		pg_get_query_def(taskQuery, sqlQueryString);

		Task *sqlTask = CreateBasicTask(jobId, taskIdIndex, READ_TASK,
										sqlQueryString->data);
		sqlTask->dependentTaskList = dataFetchTaskList;
		sqlTask->relationShardList = BuildRelationShardList(fragmentRangeTableList,
															fragmentCombination);

		/* log the query string we generated */
		ereport(DEBUG4, (errmsg("generated sql query for task %d", sqlTask->taskId),
						 errdetail("query string: \"%s\"",
								   ApplyLogRedaction(sqlQueryString->data))));

		sqlTask->anchorShardId = INVALID_SHARD_ID;
		if (anchorRangeTableBasedAssignment)
		{
			sqlTask->anchorShardId = AnchorShardId(fragmentCombination,
												   anchorRangeTableId);
		}

		taskIdIndex++;
		sqlTaskList = lappend(sqlTaskList, sqlTask);
	}

	return sqlTaskList;
}


/*
 * DependsOnHashPartitionJob checks if the given job depends on a hash
 * partitioning job.
 */
static bool
DependsOnHashPartitionJob(Job *job)
{
	bool dependsOnHashPartitionJob = false;
	List *dependentJobList = job->dependentJobList;

	uint32 dependentJobCount = (uint32) list_length(dependentJobList);
	if (dependentJobCount > 0)
	{
		Job *dependentJob = (Job *) linitial(dependentJobList);
		if (CitusIsA(dependentJob, MapMergeJob))
		{
			MapMergeJob *mapMergeJob = (MapMergeJob *) dependentJob;
			if (mapMergeJob->partitionType == DUAL_HASH_PARTITION_TYPE)
			{
				dependsOnHashPartitionJob = true;
			}
		}
	}

	return dependsOnHashPartitionJob;
}


/*
 * AnchorRangeTableId determines the table around which we build our queries,
 * and returns this table's range table id. We refer to this table as the anchor
 * table, and make sure that the anchor table's shards are moved or cached only
 * when absolutely necessary.
 */
static uint32
AnchorRangeTableId(List *rangeTableList)
{
	uint32 anchorRangeTableId = 0;
	uint64 maxTableSize = 0;

	/*
	 * We first filter anything but ordinary tables. Then, we pick the table(s)
	 * with the most number of shards as our anchor table. If multiple tables
	 * have the most number of shards, we have a draw.
	 */
	List *baseTableIdList = BaseRangeTableIdList(rangeTableList);
	List *anchorTableIdList = AnchorRangeTableIdList(rangeTableList, baseTableIdList);
	ListCell *anchorTableIdCell = NULL;

	int anchorTableIdCount = list_length(anchorTableIdList);
	Assert(anchorTableIdCount > 0);

	if (anchorTableIdCount == 1)
	{
		anchorRangeTableId = (uint32) linitial_int(anchorTableIdList);
		return anchorRangeTableId;
	}

	/*
	 * If more than one table has the most number of shards, we break the draw
	 * by comparing table sizes and picking the table with the largest size.
	 */
	foreach(anchorTableIdCell, anchorTableIdList)
	{
		uint32 anchorTableId = (uint32) lfirst_int(anchorTableIdCell);
		RangeTblEntry *tableEntry = rt_fetch(anchorTableId, rangeTableList);
		uint64 tableSize = 0;

		List *shardList = LoadShardList(tableEntry->relid);
		ListCell *shardCell = NULL;

		foreach(shardCell, shardList)
		{
			uint64 *shardIdPointer = (uint64 *) lfirst(shardCell);
			uint64 shardId = (*shardIdPointer);
			uint64 shardSize = ShardLength(shardId);

			tableSize += shardSize;
		}

		if (tableSize > maxTableSize)
		{
			maxTableSize = tableSize;
			anchorRangeTableId = anchorTableId;
		}
	}

	if (anchorRangeTableId == 0)
	{
		/* all tables have the same shard count and size 0, pick the first */
		anchorRangeTableId = (uint32) linitial_int(anchorTableIdList);
	}

	return anchorRangeTableId;
}


/*
 * BaseRangeTableIdList walks over range tables in the given range table list,
 * finds range tables that correspond to base (non-repartitioned) tables, and
 * returns these range tables' identifiers in a new list.
 */
static List *
BaseRangeTableIdList(List *rangeTableList)
{
	List *baseRangeTableIdList = NIL;
	uint32 rangeTableId = 1;

	ListCell *rangeTableCell = NULL;
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (GetRangeTblKind(rangeTableEntry) == CITUS_RTE_RELATION)
		{
			baseRangeTableIdList = lappend_int(baseRangeTableIdList, rangeTableId);
		}

		rangeTableId++;
	}

	return baseRangeTableIdList;
}


/*
 * AnchorRangeTableIdList finds ordinary table(s) with the most number of shards
 * and returns the corresponding range table id(s) in a list.
 */
static List *
AnchorRangeTableIdList(List *rangeTableList, List *baseRangeTableIdList)
{
	List *anchorTableIdList = NIL;
	uint32 maxShardCount = 0;
	ListCell *baseRangeTableIdCell = NULL;

	uint32 baseRangeTableCount = list_length(baseRangeTableIdList);
	if (baseRangeTableCount == 1)
	{
		return baseRangeTableIdList;
	}

	foreach(baseRangeTableIdCell, baseRangeTableIdList)
	{
		uint32 baseRangeTableId = (uint32) lfirst_int(baseRangeTableIdCell);
		RangeTblEntry *tableEntry = rt_fetch(baseRangeTableId, rangeTableList);
		List *shardList = LoadShardList(tableEntry->relid);

		uint32 shardCount = (uint32) list_length(shardList);
		if (shardCount > maxShardCount)
		{
			anchorTableIdList = list_make1_int(baseRangeTableId);
			maxShardCount = shardCount;
		}
		else if (shardCount == maxShardCount)
		{
			anchorTableIdList = lappend_int(anchorTableIdList, baseRangeTableId);
		}
	}

	return anchorTableIdList;
}


/*
 * AdjustColumnOldAttributes adjust the old tableId (varnoold) and old columnId
 * (varoattno), and sets them equal to the new values. We need this adjustment
 * for partition pruning where we compare these columns with partition columns
 * loaded from system catalogs. Since columns loaded from system catalogs always
 * have the same old and new values, we also need to adjust column values here.
 */
static void
AdjustColumnOldAttributes(List *expressionList)
{
	List *columnList = pull_var_clause_default((Node *) expressionList);
	ListCell *columnCell = NULL;

	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		column->varnoold = column->varno;
		column->varoattno = column->varattno;
	}
}


/*
 * RangeTableFragmentsList walks over range tables in the given range table list
 * and for each table, the function creates a list of its fragments. A fragment
 * in this list represents either a regular shard or a merge task. Once a list
 * for each range table is constructed, the function applies partition pruning
 * using the given where clause list. Then, the function appends the fragment
 * list for each range table to a list of lists, and returns this list of lists.
 */
static List *
RangeTableFragmentsList(List *rangeTableList, List *whereClauseList,
						List *dependentJobList)
{
	List *rangeTableFragmentsList = NIL;
	uint32 rangeTableIndex = 0;
	const uint32 fragmentSize = sizeof(RangeTableFragment);

	ListCell *rangeTableCell = NULL;
	foreach(rangeTableCell, rangeTableList)
	{
		uint32 tableId = rangeTableIndex + 1; /* tableId starts from 1 */

		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		CitusRTEKind rangeTableKind = GetRangeTblKind(rangeTableEntry);

		if (rangeTableKind == CITUS_RTE_RELATION)
		{
			Oid relationId = rangeTableEntry->relid;
			ListCell *shardIntervalCell = NULL;
			List *shardFragmentList = NIL;
			List *prunedShardIntervalList = PruneShards(relationId, tableId,
														whereClauseList, NULL);

			/*
			 * If we prune all shards for one table, query results will be empty.
			 * We can therefore return NIL for the task list here.
			 */
			if (prunedShardIntervalList == NIL)
			{
				return NIL;
			}

			foreach(shardIntervalCell, prunedShardIntervalList)
			{
				ShardInterval *shardInterval =
					(ShardInterval *) lfirst(shardIntervalCell);

				RangeTableFragment *shardFragment = palloc0(fragmentSize);
				shardFragment->fragmentReference = shardInterval;
				shardFragment->fragmentType = CITUS_RTE_RELATION;
				shardFragment->rangeTableId = tableId;

				shardFragmentList = lappend(shardFragmentList, shardFragment);
			}

			rangeTableFragmentsList = lappend(rangeTableFragmentsList,
											  shardFragmentList);
		}
		else if (rangeTableKind == CITUS_RTE_REMOTE_QUERY)
		{
			List *mergeTaskFragmentList = NIL;
			ListCell *mergeTaskCell = NULL;

			Job *dependentJob = JobForRangeTable(dependentJobList, rangeTableEntry);
			Assert(CitusIsA(dependentJob, MapMergeJob));

			MapMergeJob *dependentMapMergeJob = (MapMergeJob *) dependentJob;
			List *mergeTaskList = dependentMapMergeJob->mergeTaskList;

			/* if there are no tasks for the dependent job, just return NIL */
			if (mergeTaskList == NIL)
			{
				return NIL;
			}

			foreach(mergeTaskCell, mergeTaskList)
			{
				Task *mergeTask = (Task *) lfirst(mergeTaskCell);

				RangeTableFragment *mergeTaskFragment = palloc0(fragmentSize);
				mergeTaskFragment->fragmentReference = mergeTask;
				mergeTaskFragment->fragmentType = CITUS_RTE_REMOTE_QUERY;
				mergeTaskFragment->rangeTableId = tableId;

				mergeTaskFragmentList = lappend(mergeTaskFragmentList, mergeTaskFragment);
			}

			rangeTableFragmentsList = lappend(rangeTableFragmentsList,
											  mergeTaskFragmentList);
		}

		rangeTableIndex++;
	}

	return rangeTableFragmentsList;
}


/*
 * BuildBaseConstraint builds and returns a base constraint. This constraint
 * implements an expression in the form of (column <= max && column >= min),
 * where column is the partition key, and min and max values represent a shard's
 * min and max values. These shard values are filled in after the constraint is
 * built.
 */
Node *
BuildBaseConstraint(Var *column)
{
	/* Build these expressions with only one argument for now */
	OpExpr *lessThanExpr = MakeOpExpression(column, BTLessEqualStrategyNumber);
	OpExpr *greaterThanExpr = MakeOpExpression(column, BTGreaterEqualStrategyNumber);

	/* Build base constaint as an and of two qual conditions */
	Node *baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

	return baseConstraint;
}


/*
 * MakeOpExpression builds an operator expression node. This operator expression
 * implements the operator clause as defined by the variable and the strategy
 * number.
 */
OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid typeId = variable->vartype;
	Oid typeModId = variable->vartypmod;
	Oid collationId = variable->varcollid;

	Oid accessMethodId = BTREE_AM_OID;

	OperatorCacheEntry *operatorCacheEntry = LookupOperatorByType(typeId, accessMethodId,
																  strategyNumber);

	Oid operatorId = operatorCacheEntry->operatorId;
	Oid operatorClassInputType = operatorCacheEntry->operatorClassInputType;
	char typeType = operatorCacheEntry->typeType;

	/*
	 * Relabel variable if input type of default operator class is not equal to
	 * the variable type. Note that we don't relabel the variable if the default
	 * operator class variable type is a pseudo-type.
	 */
	if (operatorClassInputType != typeId && typeType != TYPTYPE_PSEUDO)
	{
		variable = (Var *) makeRelabelType((Expr *) variable, operatorClassInputType,
										   -1, collationId, COERCE_IMPLICIT_CAST);
	}

	Const *constantValue = makeNullConst(operatorClassInputType, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	OpExpr *expression = (OpExpr *) make_opclause(operatorId,
												  InvalidOid, /* no result type yet */
												  false, /* no return set */
												  (Expr *) variable,
												  (Expr *) constantValue,
												  InvalidOid, collationId);

	/* Set implementing function id and result type */
	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}


/*
 * LookupOperatorByType is a wrapper around GetOperatorByType(),
 * operatorClassInputType() and get_typtype() functions that uses a cache to avoid
 * multiple lookups of operators and its related fields within a single session by
 * their types, access methods and strategy numbers.
 * LookupOperatorByType function errors out if it cannot find corresponding
 * default operator class with the given parameters on the system catalogs.
 */
static OperatorCacheEntry *
LookupOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	OperatorCacheEntry *matchingCacheEntry = NULL;
	ListCell *cacheEntryCell = NULL;

	/* search the cache */
	foreach(cacheEntryCell, OperatorCache)
	{
		OperatorCacheEntry *cacheEntry = lfirst(cacheEntryCell);

		if ((cacheEntry->typeId == typeId) &&
			(cacheEntry->accessMethodId == accessMethodId) &&
			(cacheEntry->strategyNumber == strategyNumber))
		{
			matchingCacheEntry = cacheEntry;
			break;
		}
	}

	/* if not found in the cache, call GetOperatorByType and put the result in cache */
	if (matchingCacheEntry == NULL)
	{
		Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

		if (operatorClassId == InvalidOid)
		{
			/* if operatorId is invalid, error out */
			ereport(ERROR, (errmsg("cannot find default operator class for type:%d,"
								   " access method: %d", typeId, accessMethodId)));
		}

		/* fill the other fields to the cache */
		Oid operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);
		Oid operatorClassInputType = get_opclass_input_type(operatorClassId);
		char typeType = get_typtype(operatorClassInputType);

		/* make sure we've initialized CacheMemoryContext */
		if (CacheMemoryContext == NULL)
		{
			CreateCacheMemoryContext();
		}

		MemoryContext oldContext = MemoryContextSwitchTo(CacheMemoryContext);

		matchingCacheEntry = palloc0(sizeof(OperatorCacheEntry));
		matchingCacheEntry->typeId = typeId;
		matchingCacheEntry->accessMethodId = accessMethodId;
		matchingCacheEntry->strategyNumber = strategyNumber;
		matchingCacheEntry->operatorId = operatorId;
		matchingCacheEntry->operatorClassInputType = operatorClassInputType;
		matchingCacheEntry->typeType = typeType;

		OperatorCache = lappend(OperatorCache, matchingCacheEntry);

		MemoryContextSwitchTo(oldContext);
	}

	return matchingCacheEntry;
}


/*
 * GetOperatorByType returns the operator oid for the given type, access method,
 * and strategy number.
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamily = get_opclass_family(operatorClassId);
	Oid operatorClassInputType = get_opclass_input_type(operatorClassId);

	/* Lookup for the operator with the desired input type in the family */
	Oid operatorId = get_opfamily_member(operatorFamily, operatorClassInputType,
										 operatorClassInputType, strategyNumber);
	return operatorId;
}


/*
 * BinaryOpExpression checks that a given expression is a binary operator. If
 * this is the case it returns true and sets leftOperand and rightOperand to
 * the left and right hand side of the operator. left/rightOperand will be
 * stripped of implicit coercions by strip_implicit_coercions.
 */
bool
BinaryOpExpression(Expr *clause, Node **leftOperand, Node **rightOperand)
{
	if (!is_opclause(clause) || list_length(((OpExpr *) clause)->args) != 2)
	{
		if (leftOperand != NULL)
		{
			*leftOperand = NULL;
		}
		if (rightOperand != NULL)
		{
			*leftOperand = NULL;
		}
		return false;
	}
	if (leftOperand != NULL)
	{
		*leftOperand = get_leftop(clause);
		Assert(*leftOperand != NULL);
		*leftOperand = strip_implicit_coercions(*leftOperand);
	}
	if (rightOperand != NULL)
	{
		*rightOperand = get_rightop(clause);
		Assert(*rightOperand != NULL);
		*rightOperand = strip_implicit_coercions(*rightOperand);
	}
	return true;
}


/*
 * SimpleOpExpression checks that given expression is a simple operator
 * expression. A simple operator expression is a binary operator expression with
 * operands of a var and a non-null constant.
 */
bool
SimpleOpExpression(Expr *clause)
{
	Const *constantClause = NULL;

	Node *leftOperand;
	Node *rightOperand;
	if (!BinaryOpExpression(clause, &leftOperand, &rightOperand))
	{
		return false;
	}

	if (IsA(rightOperand, Const) && IsA(leftOperand, Var))
	{
		constantClause = (Const *) rightOperand;
	}
	else if (IsA(leftOperand, Const) && IsA(rightOperand, Var))
	{
		constantClause = (Const *) leftOperand;
	}
	else
	{
		return false;
	}

	if (constantClause->constisnull)
	{
		return false;
	}

	return true;
}


/*
 * OpExpressionContainsColumn checks if the operator expression contains the
 * given partition column. We assume that given operator expression is a simple
 * operator expression which means it is a binary operator expression with
 * operands of a var and a non-null constant.
 */
bool
OpExpressionContainsColumn(OpExpr *operatorExpression, Var *partitionColumn)
{
	Node *leftOperand;
	Node *rightOperand;
	if (!BinaryOpExpression((Expr *) operatorExpression, &leftOperand, &rightOperand))
	{
		return false;
	}
	Var *column = NULL;

	if (IsA(leftOperand, Var))
	{
		column = (Var *) leftOperand;
	}
	else
	{
		column = (Var *) rightOperand;
	}

	return equal(column, partitionColumn);
}


/*
 * MakeInt4Column creates a column of int4 type with invalid table id and max
 * attribute number.
 */
Var *
MakeInt4Column()
{
	Index tableId = 0;
	AttrNumber columnAttributeNumber = RESERVED_HASHED_COLUMN_ID;
	Oid columnType = INT4OID;
	int32 columnTypeMod = -1;
	Oid columnCollationOid = InvalidOid;
	Index columnLevelSup = 0;

	Var *int4Column = makeVar(tableId, columnAttributeNumber, columnType,
							  columnTypeMod, columnCollationOid, columnLevelSup);
	return int4Column;
}


/*
 * MakeInt4Constant creates a new constant of int4 type and assigns the given
 * value as a constant value.
 */
Const *
MakeInt4Constant(Datum constantValue)
{
	Oid constantType = INT4OID;
	int32 constantTypeMode = -1;
	Oid constantCollationId = InvalidOid;
	int constantLength = sizeof(int32);
	bool constantIsNull = false;
	bool constantByValue = true;

	Const *int4Constant = makeConst(constantType, constantTypeMode, constantCollationId,
									constantLength, constantValue, constantIsNull,
									constantByValue);
	return int4Constant;
}


/* Updates the base constraint with the given min/max values. */
void
UpdateConstraint(Node *baseConstraint, ShardInterval *shardInterval)
{
	BoolExpr *andExpr = (BoolExpr *) baseConstraint;
	Node *lessThanExpr = (Node *) linitial(andExpr->args);
	Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

	Node *minNode = get_rightop((Expr *) greaterThanExpr); /* right op */
	Node *maxNode = get_rightop((Expr *) lessThanExpr);    /* right op */

	Assert(shardInterval != NULL);
	Assert(shardInterval->minValueExists);
	Assert(shardInterval->maxValueExists);
	Assert(minNode != NULL);
	Assert(maxNode != NULL);
	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	Const *minConstant = (Const *) minNode;
	Const *maxConstant = (Const *) maxNode;

	minConstant->constvalue = datumCopy(shardInterval->minValue,
										shardInterval->valueByVal,
										shardInterval->valueTypeLen);
	maxConstant->constvalue = datumCopy(shardInterval->maxValue,
										shardInterval->valueByVal,
										shardInterval->valueTypeLen);

	minConstant->constisnull = false;
	maxConstant->constisnull = false;
}


/*
 * FragmentCombinationList first builds an ordered sequence of range tables that
 * join together. The function then iteratively adds fragments from each joined
 * range table, and forms fragment combinations (lists) that cover all tables.
 * While doing so, the function also performs join pruning to remove unnecessary
 * fragment pairs. Last, the function adds each fragment combination (list) to a
 * list, and returns this list.
 */
static List *
FragmentCombinationList(List *rangeTableFragmentsList, Query *jobQuery,
						List *dependentJobList)
{
	List *fragmentCombinationList = NIL;
	List *fragmentCombinationQueue = NIL;
	List *emptyList = NIL;

	/* find a sequence that joins the range tables in the list */
	JoinSequenceNode *joinSequenceArray = JoinSequenceArray(rangeTableFragmentsList,
															jobQuery,
															dependentJobList);

	/*
	 * We use breadth-first search with pruning to create fragment combinations.
	 * For this, we first queue the root node (an empty combination), and then
	 * start traversing our search space.
	 */
	fragmentCombinationQueue = lappend(fragmentCombinationQueue, emptyList);
	while (fragmentCombinationQueue != NIL)
	{
		ListCell *tableFragmentCell = NULL;
		int32 joiningTableSequenceIndex = -1;

		/* pop first element from the fragment queue */
		List *fragmentCombination = linitial(fragmentCombinationQueue);
		fragmentCombinationQueue = list_delete_first(fragmentCombinationQueue);

		/*
		 * If this combination covered all range tables in a join sequence, add
		 * this combination to our result set.
		 */
		int32 joinSequenceIndex = list_length(fragmentCombination);
		int32 rangeTableCount = list_length(rangeTableFragmentsList);
		if (joinSequenceIndex == rangeTableCount)
		{
			fragmentCombinationList = lappend(fragmentCombinationList,
											  fragmentCombination);
			continue;
		}

		/* find the next range table to add to our search space */
		uint32 tableId = joinSequenceArray[joinSequenceIndex].rangeTableId;
		List *tableFragments = FindRangeTableFragmentsList(rangeTableFragmentsList,
														   tableId);

		/* resolve sequence index for the previous range table we join against */
		int32 joiningTableId = joinSequenceArray[joinSequenceIndex].joiningRangeTableId;
		if (joiningTableId != NON_PRUNABLE_JOIN)
		{
			for (int32 sequenceIndex = 0; sequenceIndex < rangeTableCount;
				 sequenceIndex++)
			{
				JoinSequenceNode *joinSequenceNode = &joinSequenceArray[sequenceIndex];
				if (joinSequenceNode->rangeTableId == joiningTableId)
				{
					joiningTableSequenceIndex = sequenceIndex;
					break;
				}
			}

			Assert(joiningTableSequenceIndex != -1);
		}

		/*
		 * We walk over each range table fragment, and check if we can prune out
		 * this fragment joining with the existing fragment combination. If we
		 * can't prune away, we create a new fragment combination and add it to
		 * our search space.
		 */
		foreach(tableFragmentCell, tableFragments)
		{
			RangeTableFragment *tableFragment = lfirst(tableFragmentCell);
			bool joinPrunable = false;

			if (joiningTableId != NON_PRUNABLE_JOIN)
			{
				RangeTableFragment *joiningTableFragment =
					list_nth(fragmentCombination, joiningTableSequenceIndex);

				joinPrunable = JoinPrunable(joiningTableFragment, tableFragment);
			}

			/* if join can't be pruned, extend fragment combination and search */
			if (!joinPrunable)
			{
				List *newFragmentCombination = list_copy(fragmentCombination);
				newFragmentCombination = lappend(newFragmentCombination, tableFragment);

				fragmentCombinationQueue = lappend(fragmentCombinationQueue,
												   newFragmentCombination);
			}
		}
	}

	return fragmentCombinationList;
}


/*
 * NodeIsRangeTblRefReferenceTable checks if the node is a RangeTblRef that
 * points to a reference table in the rangeTableList.
 */
static bool
NodeIsRangeTblRefReferenceTable(Node *node, List *rangeTableList)
{
	if (!IsA(node, RangeTblRef))
	{
		return false;
	}
	RangeTblRef *tableRef = castNode(RangeTblRef, node);
	RangeTblEntry *rangeTableEntry = rt_fetch(tableRef->rtindex, rangeTableList);
	CitusRTEKind rangeTableType = GetRangeTblKind(rangeTableEntry);
	if (rangeTableType != CITUS_RTE_RELATION)
	{
		return false;
	}
	return PartitionMethod(rangeTableEntry->relid) == DISTRIBUTE_BY_NONE;
}


/*
 * JoinSequenceArray walks over the join nodes in the job query and constructs a join
 * sequence containing an entry for each joined table. The function then returns an
 * array of join sequence nodes, in which each node contains the id of a table in the
 * range table list and the id of a preceding table with which it is joined, if any.
 */
static JoinSequenceNode *
JoinSequenceArray(List *rangeTableFragmentsList, Query *jobQuery, List *dependentJobList)
{
	List *rangeTableList = jobQuery->rtable;
	uint32 rangeTableCount = (uint32) list_length(rangeTableList);
	uint32 sequenceNodeSize = sizeof(JoinSequenceNode);
	uint32 joinedTableCount = 0;
	ListCell *joinExprCell = NULL;
	uint32 firstRangeTableId = 1;
	JoinSequenceNode *joinSequenceArray = palloc0(rangeTableCount * sequenceNodeSize);

	List *joinExprList = JoinExprList(jobQuery->jointree);

	/* pick first range table as starting table for the join sequence */
	if (list_length(joinExprList) > 0)
	{
		JoinExpr *firstExpr = (JoinExpr *) linitial(joinExprList);
		RangeTblRef *leftTableRef = (RangeTblRef *) firstExpr->larg;
		firstRangeTableId = leftTableRef->rtindex;
	}
	else
	{
		/* when there are no joins, the join sequence contains a node for the table */
		firstRangeTableId = 1;
	}

	joinSequenceArray[joinedTableCount].rangeTableId = firstRangeTableId;
	joinSequenceArray[joinedTableCount].joiningRangeTableId = NON_PRUNABLE_JOIN;
	joinedTableCount++;

	foreach(joinExprCell, joinExprList)
	{
		JoinExpr *joinExpr = (JoinExpr *) lfirst(joinExprCell);
		RangeTblRef *rightTableRef = castNode(RangeTblRef, joinExpr->rarg);
		uint32 nextRangeTableId = rightTableRef->rtindex;
		Index existingRangeTableId = 0;
		bool applyJoinPruning = false;

		List *nextJoinClauseList = make_ands_implicit((Expr *) joinExpr->quals);
		bool leftIsReferenceTable = NodeIsRangeTblRefReferenceTable(joinExpr->larg,
																	rangeTableList);
		bool rightIsReferenceTable = NodeIsRangeTblRefReferenceTable(joinExpr->rarg,
																	 rangeTableList);
		bool isReferenceJoin = IsSupportedReferenceJoin(joinExpr->jointype,
														leftIsReferenceTable,
														rightIsReferenceTable);

		/*
		 * If next join clause list is empty, the user tried a cartesian product
		 * between tables. We don't support this functionality for non
		 * reference joins, and error out.
		 */
		if (nextJoinClauseList == NIL && !isReferenceJoin)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning on this query"),
							errdetail("Cartesian products are currently unsupported")));
		}

		/*
		 * We now determine if we can apply join pruning between existing range
		 * tables and this new one.
		 */
		Node *nextJoinClause = NULL;
		foreach_ptr(nextJoinClause, nextJoinClauseList)
		{
			if (!NodeIsEqualsOpExpr(nextJoinClause))
			{
				continue;
			}

			OpExpr *nextJoinClauseOpExpr = castNode(OpExpr, nextJoinClause);

			if (!IsJoinClause((Node *) nextJoinClauseOpExpr))
			{
				continue;
			}

			Var *leftColumn = LeftColumnOrNULL(nextJoinClauseOpExpr);
			Var *rightColumn = RightColumnOrNULL(nextJoinClauseOpExpr);
			if (leftColumn == NULL || rightColumn == NULL)
			{
				continue;
			}

			Index leftRangeTableId = leftColumn->varno;
			Index rightRangeTableId = rightColumn->varno;

			/*
			 * We have a table from the existing join list joining with the next
			 * table. First resolve the existing table's range table id.
			 */
			if (leftRangeTableId == nextRangeTableId)
			{
				existingRangeTableId = rightRangeTableId;
			}
			else
			{
				existingRangeTableId = leftRangeTableId;
			}

			/*
			 * Then, we check if we can apply join pruning between the existing
			 * range table and this new one. For this, columns need to have the
			 * same type and be the partition column for their respective tables.
			 */
			if (leftColumn->vartype != rightColumn->vartype)
			{
				continue;
			}

			bool leftPartitioned = PartitionedOnColumn(leftColumn, rangeTableList,
													   dependentJobList);
			bool rightPartitioned = PartitionedOnColumn(rightColumn, rangeTableList,
														dependentJobList);
			if (leftPartitioned && rightPartitioned)
			{
				/* make sure this join clause references only simple columns */
				CheckJoinBetweenColumns(nextJoinClauseOpExpr);

				applyJoinPruning = true;
				break;
			}
		}

		/* set next joining range table's info in the join sequence */
		JoinSequenceNode *nextJoinSequenceNode = &joinSequenceArray[joinedTableCount];
		if (applyJoinPruning)
		{
			nextJoinSequenceNode->rangeTableId = nextRangeTableId;
			nextJoinSequenceNode->joiningRangeTableId = (int32) existingRangeTableId;
		}
		else
		{
			nextJoinSequenceNode->rangeTableId = nextRangeTableId;
			nextJoinSequenceNode->joiningRangeTableId = NON_PRUNABLE_JOIN;
		}

		joinedTableCount++;
	}

	return joinSequenceArray;
}


/*
 * PartitionedOnColumn finds the given column's range table entry, and checks if
 * that range table is partitioned on the given column. Note that since reference
 * tables do not have partition columns, the function returns false when the distributed
 * relation is a reference table.
 */
static bool
PartitionedOnColumn(Var *column, List *rangeTableList, List *dependentJobList)
{
	bool partitionedOnColumn = false;
	Index rangeTableId = column->varno;
	RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableId, rangeTableList);

	CitusRTEKind rangeTableType = GetRangeTblKind(rangeTableEntry);
	if (rangeTableType == CITUS_RTE_RELATION)
	{
		Oid relationId = rangeTableEntry->relid;
		char partitionMethod = PartitionMethod(relationId);
		Var *partitionColumn = PartitionColumn(relationId, rangeTableId);

		/* reference tables do not have partition columns */
		if (partitionMethod == DISTRIBUTE_BY_NONE)
		{
			partitionedOnColumn = false;

			return partitionedOnColumn;
		}

		if (partitionColumn->varattno == column->varattno)
		{
			partitionedOnColumn = true;
		}
	}
	else if (rangeTableType == CITUS_RTE_REMOTE_QUERY)
	{
		Job *job = JobForRangeTable(dependentJobList, rangeTableEntry);
		MapMergeJob *mapMergeJob = (MapMergeJob *) job;

		/*
		 * The column's current attribute number is it's location in the target
		 * list for the table represented by the remote query. We retrieve this
		 * value from the target list to compare against the partition column
		 * as stored in the job.
		 */
		List *targetEntryList = job->jobQuery->targetList;
		int32 columnIndex = column->varattno - 1;
		Assert(columnIndex >= 0);
		Assert(columnIndex < list_length(targetEntryList));

		TargetEntry *targetEntry = (TargetEntry *) list_nth(targetEntryList, columnIndex);
		Var *remoteRelationColumn = (Var *) targetEntry->expr;
		Assert(IsA(remoteRelationColumn, Var));

		/* retrieve the partition column for the job */
		Var *partitionColumn = mapMergeJob->partitionColumn;
		if (partitionColumn->varattno == remoteRelationColumn->varattno)
		{
			partitionedOnColumn = true;
		}
	}

	return partitionedOnColumn;
}


/* Checks that the join clause references only simple columns. */
static void
CheckJoinBetweenColumns(OpExpr *joinClause)
{
	List *argumentList = joinClause->args;
	Node *leftArgument = (Node *) linitial(argumentList);
	Node *rightArgument = (Node *) lsecond(argumentList);
	Node *strippedLeftArgument = strip_implicit_coercions(leftArgument);
	Node *strippedRightArgument = strip_implicit_coercions(rightArgument);

	NodeTag leftArgumentType = nodeTag(strippedLeftArgument);
	NodeTag rightArgumentType = nodeTag(strippedRightArgument);

	if (leftArgumentType != T_Var || rightArgumentType != T_Var)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform local joins that involve expressions"),
						errdetail("local joins can be performed between columns only")));
	}
}


/*
 * FindRangeTableFragmentsList walks over the given list of range table fragments
 * and, returns the one with the given table id.
 */
static List *
FindRangeTableFragmentsList(List *rangeTableFragmentsList, int tableId)
{
	List *foundTableFragments = NIL;
	ListCell *rangeTableFragmentsCell = NULL;

	foreach(rangeTableFragmentsCell, rangeTableFragmentsList)
	{
		List *tableFragments = (List *) lfirst(rangeTableFragmentsCell);
		if (tableFragments != NIL)
		{
			RangeTableFragment *tableFragment =
				(RangeTableFragment *) linitial(tableFragments);
			if (tableFragment->rangeTableId == tableId)
			{
				foundTableFragments = tableFragments;
				break;
			}
		}
	}

	return foundTableFragments;
}


/*
 * JoinPrunable checks if a join between the given left and right fragments can
 * be pruned away, without performing the actual join. To do this, the function
 * checks if we have a hash repartition join. If we do, the function determines
 * pruning based on partitionIds. Else if we have a merge repartition join, the
 * function checks if the two fragments have disjoint intervals.
 */
static bool
JoinPrunable(RangeTableFragment *leftFragment, RangeTableFragment *rightFragment)
{
	/*
	 * If both range tables are remote queries, we then have a hash repartition
	 * join. In that case, we can just prune away this join if left and right
	 * hand side fragments have the same partitionId.
	 */
	if (leftFragment->fragmentType == CITUS_RTE_REMOTE_QUERY &&
		rightFragment->fragmentType == CITUS_RTE_REMOTE_QUERY)
	{
		Task *leftMergeTask = (Task *) leftFragment->fragmentReference;
		Task *rightMergeTask = (Task *) rightFragment->fragmentReference;


		if (leftMergeTask->partitionId != rightMergeTask->partitionId)
		{
			ereport(DEBUG2, (errmsg("join prunable for task partitionId %u and %u",
									leftMergeTask->partitionId,
									rightMergeTask->partitionId)));
			return true;
		}
		else
		{
			return false;
		}
	}


	/*
	 * We have a single (re)partition join. We now get shard intervals for both
	 * fragments, and then check if these intervals overlap.
	 */
	ShardInterval *leftFragmentInterval = FragmentInterval(leftFragment);
	ShardInterval *rightFragmentInterval = FragmentInterval(rightFragment);

	bool overlap = ShardIntervalsOverlap(leftFragmentInterval, rightFragmentInterval);
	if (!overlap)
	{
		if (IsLoggableLevel(DEBUG2))
		{
			StringInfo leftString = FragmentIntervalString(leftFragmentInterval);
			StringInfo rightString = FragmentIntervalString(rightFragmentInterval);

			ereport(DEBUG2, (errmsg("join prunable for intervals %s and %s",
									leftString->data, rightString->data)));
		}

		return true;
	}

	return false;
}


/*
 * FragmentInterval takes the given fragment, and determines the range of data
 * covered by this fragment. The function then returns this range (interval).
 */
static ShardInterval *
FragmentInterval(RangeTableFragment *fragment)
{
	ShardInterval *fragmentInterval = NULL;
	if (fragment->fragmentType == CITUS_RTE_RELATION)
	{
		Assert(CitusIsA(fragment->fragmentReference, ShardInterval));
		fragmentInterval = (ShardInterval *) fragment->fragmentReference;
	}
	else if (fragment->fragmentType == CITUS_RTE_REMOTE_QUERY)
	{
		Assert(CitusIsA(fragment->fragmentReference, Task));

		Task *mergeTask = (Task *) fragment->fragmentReference;
		fragmentInterval = mergeTask->shardInterval;
	}

	return fragmentInterval;
}


/* Checks if the given shard intervals have overlapping ranges. */
bool
ShardIntervalsOverlap(ShardInterval *firstInterval, ShardInterval *secondInterval)
{
	CitusTableCacheEntry *intervalRelation =
		GetCitusTableCacheEntry(firstInterval->relationId);

	Assert(intervalRelation->partitionMethod != DISTRIBUTE_BY_NONE);

	FmgrInfo *comparisonFunction = intervalRelation->shardIntervalCompareFunction;
	Oid collation = intervalRelation->partitionColumn->varcollid;


	Datum firstMin = firstInterval->minValue;
	Datum firstMax = firstInterval->maxValue;
	Datum secondMin = secondInterval->minValue;
	Datum secondMax = secondInterval->maxValue;

	/*
	 * We need to have min/max values for both intervals first. Then, we assume
	 * two intervals i1 = [min1, max1] and i2 = [min2, max2] do not overlap if
	 * (max1 < min2) or (max2 < min1). For details, please see the explanation
	 * on overlapping intervals at http://www.rgrjr.com/emacs/overlap.html.
	 */
	if (firstInterval->minValueExists && firstInterval->maxValueExists &&
		secondInterval->minValueExists && secondInterval->maxValueExists)
	{
		Datum firstDatum = FunctionCall2Coll(comparisonFunction, collation, firstMax,
											 secondMin);
		Datum secondDatum = FunctionCall2Coll(comparisonFunction, collation, secondMax,
											  firstMin);
		int firstComparison = DatumGetInt32(firstDatum);
		int secondComparison = DatumGetInt32(secondDatum);

		if (firstComparison < 0 || secondComparison < 0)
		{
			return false;
		}
	}

	return true;
}


/*
 * FragmentIntervalString takes the given fragment interval, and converts this
 * interval into its string representation for use in debug messages.
 */
static StringInfo
FragmentIntervalString(ShardInterval *fragmentInterval)
{
	Oid typeId = fragmentInterval->valueTypeId;
	Oid outputFunctionId = InvalidOid;
	bool typeVariableLength = false;

	Assert(fragmentInterval->minValueExists);
	Assert(fragmentInterval->maxValueExists);

	FmgrInfo *outputFunction = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	getTypeOutputInfo(typeId, &outputFunctionId, &typeVariableLength);
	fmgr_info(outputFunctionId, outputFunction);

	char *minValueString = OutputFunctionCall(outputFunction, fragmentInterval->minValue);
	char *maxValueString = OutputFunctionCall(outputFunction, fragmentInterval->maxValue);

	StringInfo fragmentIntervalString = makeStringInfo();
	appendStringInfo(fragmentIntervalString, "[%s,%s]", minValueString, maxValueString);

	return fragmentIntervalString;
}


/*
 * DataFetchTaskList builds a merge fetch task for every remote query result
 * in the given fragment list, appends these merge fetch tasks into a list,
 * and returns this list.
 */
static List *
DataFetchTaskList(uint64 jobId, uint32 taskIdIndex, List *fragmentList)
{
	List *dataFetchTaskList = NIL;
	ListCell *fragmentCell = NULL;

	foreach(fragmentCell, fragmentList)
	{
		RangeTableFragment *fragment = (RangeTableFragment *) lfirst(fragmentCell);
		if (fragment->fragmentType == CITUS_RTE_REMOTE_QUERY)
		{
			Task *mergeTask = (Task *) fragment->fragmentReference;
			char *undefinedQueryString = NULL;

			/* create merge fetch task and have it depend on the merge task */
			Task *mergeFetchTask = CreateBasicTask(jobId, taskIdIndex, MERGE_FETCH_TASK,
												   undefinedQueryString);
			mergeFetchTask->dependentTaskList = list_make1(mergeTask);

			dataFetchTaskList = lappend(dataFetchTaskList, mergeFetchTask);
			taskIdIndex++;
		}
	}

	return dataFetchTaskList;
}


/* Helper function to return a datum array's external string representation. */
static StringInfo
DatumArrayString(Datum *datumArray, uint32 datumCount, Oid datumTypeId)
{
	int16 typeLength = 0;
	bool typeByValue = false;
	char typeAlignment = 0;

	/* construct the array object from the given array */
	get_typlenbyvalalign(datumTypeId, &typeLength, &typeByValue, &typeAlignment);
	ArrayType *arrayObject = construct_array(datumArray, datumCount, datumTypeId,
											 typeLength, typeByValue, typeAlignment);
	Datum arrayObjectDatum = PointerGetDatum(arrayObject);

	/* convert the array object to its string representation */
	FmgrInfo *arrayOutFunction = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	fmgr_info(F_ARRAY_OUT, arrayOutFunction);

	Datum arrayStringDatum = FunctionCall1(arrayOutFunction, arrayObjectDatum);
	char *arrayString = DatumGetCString(arrayStringDatum);

	StringInfo arrayStringInfo = makeStringInfo();
	appendStringInfo(arrayStringInfo, "%s", arrayString);

	return arrayStringInfo;
}


/*
 * CreateBasicTask creates a task, initializes fields that are common to each task,
 * and returns the created task.
 */
Task *
CreateBasicTask(uint64 jobId, uint32 taskId, TaskType taskType, char *queryString)
{
	Task *task = CitusMakeNode(Task);
	task->jobId = jobId;
	task->taskId = taskId;
	task->taskType = taskType;
	task->replicationModel = REPLICATION_MODEL_INVALID;
	SetTaskQueryString(task, queryString);

	return task;
}


/*
 * BuildRelationShardList builds a list of RelationShard pairs for a task.
 * This represents the mapping of range table entries to shard IDs for a
 * task for the purposes of locking, deparsing, and connection management.
 */
static List *
BuildRelationShardList(List *rangeTableList, List *fragmentList)
{
	List *relationShardList = NIL;
	ListCell *fragmentCell = NULL;

	foreach(fragmentCell, fragmentList)
	{
		RangeTableFragment *fragment = (RangeTableFragment *) lfirst(fragmentCell);
		Index rangeTableId = fragment->rangeTableId;
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableId, rangeTableList);

		CitusRTEKind fragmentType = fragment->fragmentType;
		if (fragmentType == CITUS_RTE_RELATION)
		{
			ShardInterval *shardInterval = (ShardInterval *) fragment->fragmentReference;
			RelationShard *relationShard = CitusMakeNode(RelationShard);

			relationShard->relationId = rangeTableEntry->relid;
			relationShard->shardId = shardInterval->shardId;

			relationShardList = lappend(relationShardList, relationShard);
		}
	}

	return relationShardList;
}


/*
 * UpdateRangeTableAlias walks over each fragment in the given fragment list,
 * and creates an alias that represents the fragment name to be used in the
 * query. The function then updates the corresponding range table entry with
 * this alias.
 */
static void
UpdateRangeTableAlias(List *rangeTableList, List *fragmentList)
{
	ListCell *fragmentCell = NULL;
	foreach(fragmentCell, fragmentList)
	{
		RangeTableFragment *fragment = (RangeTableFragment *) lfirst(fragmentCell);
		Index rangeTableId = fragment->rangeTableId;
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableId, rangeTableList);

		Alias *fragmentAlias = FragmentAlias(rangeTableEntry, fragment);
		rangeTableEntry->alias = fragmentAlias;
	}
}


/*
 * FragmentAlias creates an alias structure that captures the table fragment's
 * name on the worker node. Each fragment represents either a regular shard, or
 * a merge task.
 */
static Alias *
FragmentAlias(RangeTblEntry *rangeTableEntry, RangeTableFragment *fragment)
{
	char *aliasName = NULL;
	char *schemaName = NULL;
	char *fragmentName = NULL;

	CitusRTEKind fragmentType = fragment->fragmentType;
	if (fragmentType == CITUS_RTE_RELATION)
	{
		ShardInterval *shardInterval = (ShardInterval *) fragment->fragmentReference;
		uint64 shardId = shardInterval->shardId;

		Oid relationId = rangeTableEntry->relid;
		char *relationName = get_rel_name(relationId);

		/*
		 * If the table is not in the default namespace (public), we include it in
		 * the fragment alias.
		 */
		Oid schemaId = get_rel_namespace(relationId);
		schemaName = get_namespace_name(schemaId);
		if (strncmp(schemaName, "public", NAMEDATALEN) == 0)
		{
			schemaName = NULL;
		}

		aliasName = relationName;

		/*
		 * Set shard name in alias to <relation_name>_<shard_id>.
		 */
		fragmentName = pstrdup(relationName);
		AppendShardIdToName(&fragmentName, shardId);
	}
	else if (fragmentType == CITUS_RTE_REMOTE_QUERY)
	{
		Task *mergeTask = (Task *) fragment->fragmentReference;
		uint64 jobId = mergeTask->jobId;
		uint32 taskId = mergeTask->taskId;

		StringInfo jobSchemaName = JobSchemaName(jobId);
		StringInfo taskTableName = TaskTableName(taskId);

		StringInfo aliasNameString = makeStringInfo();
		appendStringInfo(aliasNameString, "%s.%s",
						 jobSchemaName->data, taskTableName->data);

		aliasName = aliasNameString->data;
		fragmentName = taskTableName->data;
		schemaName = jobSchemaName->data;
	}

	/*
	 * We need to set the aliasname to relation name, as pg_get_query_def() uses
	 * the relation name to disambiguate column names from different tables.
	 */
	Alias *alias = rangeTableEntry->alias;
	if (alias == NULL)
	{
		alias = makeNode(Alias);
		alias->aliasname = aliasName;
	}

	ModifyRangeTblExtraData(rangeTableEntry, CITUS_RTE_SHARD,
							schemaName, fragmentName, NIL);

	return alias;
}


/*
 * AnchorShardId walks over each fragment in the given fragment list, finds the
 * fragment that corresponds to the given anchor range tableId, and returns this
 * fragment's shard identifier. Note that the given tableId must correspond to a
 * base relation.
 */
static uint64
AnchorShardId(List *fragmentList, uint32 anchorRangeTableId)
{
	uint64 anchorShardId = INVALID_SHARD_ID;
	ListCell *fragmentCell = NULL;

	foreach(fragmentCell, fragmentList)
	{
		RangeTableFragment *fragment = (RangeTableFragment *) lfirst(fragmentCell);
		if (fragment->rangeTableId == anchorRangeTableId)
		{
			Assert(fragment->fragmentType == CITUS_RTE_RELATION);
			Assert(CitusIsA(fragment->fragmentReference, ShardInterval));

			ShardInterval *shardInterval = (ShardInterval *) fragment->fragmentReference;
			anchorShardId = shardInterval->shardId;
			break;
		}
	}

	Assert(anchorShardId != INVALID_SHARD_ID);
	return anchorShardId;
}


/*
 * PruneSqlTaskDependencies iterates over each sql task from the given sql task
 * list, and prunes away merge-fetch tasks, as the task assignment algorithm
 * ensures co-location of these tasks.
 */
static List *
PruneSqlTaskDependencies(List *sqlTaskList)
{
	ListCell *sqlTaskCell = NULL;
	foreach(sqlTaskCell, sqlTaskList)
	{
		Task *sqlTask = (Task *) lfirst(sqlTaskCell);
		List *dependentTaskList = sqlTask->dependentTaskList;
		List *prunedDependendTaskList = NIL;

		ListCell *dependentTaskCell = NULL;
		foreach(dependentTaskCell, dependentTaskList)
		{
			Task *dataFetchTask = (Task *) lfirst(dependentTaskCell);

			/*
			 * If we have a merge fetch task, our task assignment algorithm makes
			 * sure that the sql task is colocated with the anchor shard / merge
			 * task. We can therefore prune out this data fetch task.
			 */
			if (dataFetchTask->taskType == MERGE_FETCH_TASK)
			{
				List *mergeFetchDependencyList = dataFetchTask->dependentTaskList;
				Assert(list_length(mergeFetchDependencyList) == 1);

				Task *mergeTaskReference = (Task *) linitial(mergeFetchDependencyList);
				prunedDependendTaskList = lappend(prunedDependendTaskList,
												  mergeTaskReference);

				ereport(DEBUG2, (errmsg("pruning merge fetch taskId %d",
										dataFetchTask->taskId),
								 errdetail("Creating dependency on merge taskId %d",
										   mergeTaskReference->taskId)));
			}
		}

		sqlTask->dependentTaskList = prunedDependendTaskList;
	}

	return sqlTaskList;
}


/*
 * MapTaskList creates a list of map tasks for the given MapMerge job. For this,
 * the function walks over each filter task (sql task) in the given filter task
 * list, and wraps this task with a map function call. The map function call
 * repartitions the filter task's output according to MapMerge job's parameters.
 */
static List *
MapTaskList(MapMergeJob *mapMergeJob, List *filterTaskList)
{
	List *mapTaskList = NIL;
	Query *filterQuery = mapMergeJob->job.jobQuery;
	List *rangeTableList = filterQuery->rtable;
	ListCell *filterTaskCell = NULL;
	Var *partitionColumn = mapMergeJob->partitionColumn;
	char *partitionColumnName = NULL;

	List *groupClauseList = filterQuery->groupClause;
	if (groupClauseList != NIL)
	{
		List *targetEntryList = filterQuery->targetList;
		List *groupTargetEntryList = GroupTargetEntryList(groupClauseList,
														  targetEntryList);
		TargetEntry *groupByTargetEntry = (TargetEntry *) linitial(groupTargetEntryList);

		partitionColumnName = groupByTargetEntry->resname;
	}
	else
	{
		TargetEntry *targetEntry = tlist_member((Expr *) partitionColumn,
												filterQuery->targetList);
		if (targetEntry != NULL)
		{
			/* targetEntry->resname may be NULL */
			partitionColumnName = targetEntry->resname;
		}

		if (partitionColumnName == NULL)
		{
			partitionColumnName = ColumnName(partitionColumn, rangeTableList);
		}
	}

	foreach(filterTaskCell, filterTaskList)
	{
		Task *filterTask = (Task *) lfirst(filterTaskCell);
		StringInfo mapQueryString = CreateMapQueryString(mapMergeJob, filterTask,
														 partitionColumnName);

		/* convert filter query task into map task */
		Task *mapTask = filterTask;
		SetTaskQueryString(mapTask, mapQueryString->data);
		mapTask->taskType = MAP_TASK;

		mapTaskList = lappend(mapTaskList, mapTask);
	}

	return mapTaskList;
}


/*
 * CreateMapQueryString creates and returns the map query string for the given filterTask.
 */
static StringInfo
CreateMapQueryString(MapMergeJob *mapMergeJob, Task *filterTask,
					 char *partitionColumnName)
{
	uint64 jobId = filterTask->jobId;
	uint32 taskId = filterTask->taskId;

	/* wrap repartition query string around filter query string */
	StringInfo mapQueryString = makeStringInfo();
	char *filterQueryString = TaskQueryStringForAllPlacements(filterTask);
	char *filterQueryEscapedText = quote_literal_cstr(filterQueryString);
	PartitionType partitionType = mapMergeJob->partitionType;

	Var *partitionColumn = mapMergeJob->partitionColumn;
	Oid partitionColumnType = partitionColumn->vartype;
	char *partitionColumnTypeFullName = format_type_be_qualified(partitionColumnType);
	int32 partitionColumnTypeMod = partitionColumn->vartypmod;

	ShardInterval **intervalArray = mapMergeJob->sortedShardIntervalArray;
	uint32 intervalCount = mapMergeJob->partitionCount;

	if (partitionType == DUAL_HASH_PARTITION_TYPE)
	{
		partitionColumnType = INT4OID;
		partitionColumnTypeMod = get_typmodin(INT4OID);
		intervalArray = GenerateSyntheticShardIntervalArray(intervalCount);
	}
	else if (partitionType == SINGLE_HASH_PARTITION_TYPE)
	{
		partitionColumnType = INT4OID;
		partitionColumnTypeMod = get_typmodin(INT4OID);
	}

	ArrayType *splitPointObject = SplitPointObject(intervalArray, intervalCount);
	StringInfo splitPointString = ArrayObjectToString(splitPointObject,
													  partitionColumnType,
													  partitionColumnTypeMod);

	char *partitionCommand = NULL;
	if (partitionType == RANGE_PARTITION_TYPE)
	{
		partitionCommand = RANGE_PARTITION_COMMAND;
	}
	else
	{
		partitionCommand = HASH_PARTITION_COMMAND;
	}

	appendStringInfo(mapQueryString, partitionCommand, jobId, taskId,
					 filterQueryEscapedText, partitionColumnName,
					 partitionColumnTypeFullName, splitPointString->data);
	return mapQueryString;
}


/*
 * GenerateSyntheticShardIntervalArray returns a shard interval pointer array
 * which has a uniform hash distribution for the given input partitionCount.
 *
 * The function only fills the min/max values of shard the intervals. Thus, should
 * not be used for general purpose operations.
 */
ShardInterval **
GenerateSyntheticShardIntervalArray(int partitionCount)
{
	ShardInterval **shardIntervalArray = palloc0(partitionCount *
												 sizeof(ShardInterval *));
	uint64 hashTokenIncrement = HASH_TOKEN_COUNT / partitionCount;

	for (int shardIndex = 0; shardIndex < partitionCount; ++shardIndex)
	{
		ShardInterval *shardInterval = CitusMakeNode(ShardInterval);

		/* calculate the split of the hash space */
		int32 shardMinHashToken = INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);

		shardInterval->relationId = InvalidOid;
		shardInterval->minValueExists = true;
		shardInterval->minValue = Int32GetDatum(shardMinHashToken);

		shardInterval->maxValueExists = true;
		shardInterval->maxValue = Int32GetDatum(shardMaxHashToken);

		shardInterval->shardId = INVALID_SHARD_ID;
		shardInterval->valueTypeId = INT4OID;

		shardIntervalArray[shardIndex] = shardInterval;
	}

	return shardIntervalArray;
}


/*
 * Determine RowModifyLevel required for given query
 */
RowModifyLevel
RowModifyLevelForQuery(Query *query)
{
	CmdType commandType = query->commandType;

	if (commandType == CMD_SELECT)
	{
		return ROW_MODIFY_READONLY;
	}

	if (commandType == CMD_INSERT)
	{
		if (query->onConflict == NULL)
		{
			return ROW_MODIFY_COMMUTATIVE;
		}
		else
		{
			return ROW_MODIFY_NONCOMMUTATIVE;
		}
	}

	if (commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return ROW_MODIFY_NONCOMMUTATIVE;
	}

	return ROW_MODIFY_NONE;
}


/*
 * ColumnName resolves the given column's name. The given column could belong to
 * a regular table or to an intermediate table formed to execute a distributed
 * query.
 */
static char *
ColumnName(Var *column, List *rangeTableList)
{
	char *columnName = NULL;
	Index tableId = column->varno;
	AttrNumber columnNumber = column->varattno;
	RangeTblEntry *rangeTableEntry = rt_fetch(tableId, rangeTableList);

	CitusRTEKind rangeTableKind = GetRangeTblKind(rangeTableEntry);
	if (rangeTableKind == CITUS_RTE_REMOTE_QUERY)
	{
		Alias *referenceNames = rangeTableEntry->eref;
		List *columnNameList = referenceNames->colnames;
		int columnIndex = columnNumber - 1;

		Value *columnValue = (Value *) list_nth(columnNameList, columnIndex);
		columnName = strVal(columnValue);
	}
	else if (rangeTableKind == CITUS_RTE_RELATION)
	{
		Oid relationId = rangeTableEntry->relid;
		columnName = get_attname(relationId, columnNumber, false);
	}

	Assert(columnName != NULL);
	return columnName;
}


/*
 * ArrayObjectToString converts an SQL object to its string representation.
 */
StringInfo
ArrayObjectToString(ArrayType *arrayObject, Oid columnType, int32 columnTypeMod)
{
	Datum arrayDatum = PointerGetDatum(arrayObject);
	Oid outputFunctionId = InvalidOid;
	bool typeVariableLength = false;

	Oid arrayOutType = get_array_type(columnType);
	if (arrayOutType == InvalidOid)
	{
		char *columnTypeName = format_type_be(columnType);
		ereport(ERROR, (errmsg("cannot range repartition table on column type %s",
							   columnTypeName)));
	}

	FmgrInfo *arrayOutFunction = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	getTypeOutputInfo(arrayOutType, &outputFunctionId, &typeVariableLength);
	fmgr_info(outputFunctionId, arrayOutFunction);

	char *arrayOutputText = OutputFunctionCall(arrayOutFunction, arrayDatum);
	char *arrayOutputEscapedText = quote_literal_cstr(arrayOutputText);

	/* add an explicit cast to array's string representation */
	char *arrayOutTypeName = format_type_with_typemod(arrayOutType, columnTypeMod);

	StringInfo arrayString = makeStringInfo();
	appendStringInfo(arrayString, "%s::%s",
					 arrayOutputEscapedText, arrayOutTypeName);

	return arrayString;
}


/*
 * MergeTaskList creates a list of merge tasks for the given MapMerge job. While
 * doing this, the function also establishes dependencies between each merge
 * task and its downstream map task dependencies by creating "map fetch" tasks.
 */
static List *
MergeTaskList(MapMergeJob *mapMergeJob, List *mapTaskList, uint32 taskIdIndex)
{
	List *mergeTaskList = NIL;
	uint64 jobId = mapMergeJob->job.jobId;
	uint32 partitionCount = mapMergeJob->partitionCount;

	/* build column name and column type arrays (table schema) */
	Query *filterQuery = mapMergeJob->job.jobQuery;
	List *targetEntryList = filterQuery->targetList;

	/* if all map tasks were pruned away, return NIL for merge tasks */
	if (mapTaskList == NIL)
	{
		return NIL;
	}

	/*
	 * XXX: We currently ignore the 0th partition bucket that range partitioning
	 * generates. This bucket holds all values less than the minimum value or
	 * NULLs, both of which we can currently ignore. However, when we support
	 * range re-partitioned OUTER joins, we will need these rows for the
	 * relation whose rows are retained in the OUTER join.
	 */
	uint32 initialPartitionId = 0;
	if (mapMergeJob->partitionType == RANGE_PARTITION_TYPE)
	{
		initialPartitionId = 1;
		partitionCount = partitionCount + 1;
	}
	else if (mapMergeJob->partitionType == SINGLE_HASH_PARTITION_TYPE)
	{
		initialPartitionId = 0;
	}

	/* build merge tasks and their associated "map output fetch" tasks */
	for (uint32 partitionId = initialPartitionId; partitionId < partitionCount;
		 partitionId++)
	{
		Task *mergeTask = NULL;
		List *mapOutputFetchTaskList = NIL;
		ListCell *mapTaskCell = NULL;
		uint32 mergeTaskId = taskIdIndex;

		Query *reduceQuery = mapMergeJob->reduceQuery;
		if (reduceQuery == NULL)
		{
			uint32 columnCount = (uint32) list_length(targetEntryList);
			StringInfo columnNames = ColumnNameArrayString(columnCount, jobId);
			StringInfo columnTypes = ColumnTypeArrayString(targetEntryList);

			StringInfo mergeQueryString = makeStringInfo();
			appendStringInfo(mergeQueryString, MERGE_FILES_INTO_TABLE_COMMAND,
							 jobId, taskIdIndex, columnNames->data, columnTypes->data);

			/* create merge task */
			mergeTask = CreateBasicTask(jobId, mergeTaskId, MERGE_TASK,
										mergeQueryString->data);
		}
		else
		{
			StringInfo mergeTableQueryString =
				MergeTableQueryString(taskIdIndex, targetEntryList);
			char *escapedMergeTableQueryString =
				quote_literal_cstr(mergeTableQueryString->data);
			StringInfo intermediateTableQueryString =
				IntermediateTableQueryString(jobId, taskIdIndex, reduceQuery);
			char *escapedIntermediateTableQueryString =
				quote_literal_cstr(intermediateTableQueryString->data);
			StringInfo mergeAndRunQueryString = makeStringInfo();
			appendStringInfo(mergeAndRunQueryString, MERGE_FILES_AND_RUN_QUERY_COMMAND,
							 jobId, taskIdIndex, escapedMergeTableQueryString,
							 escapedIntermediateTableQueryString);

			mergeTask = CreateBasicTask(jobId, mergeTaskId, MERGE_TASK,
										mergeAndRunQueryString->data);
		}

		mergeTask->partitionId = partitionId;
		taskIdIndex++;

		/* create tasks to fetch map outputs to this merge task */
		foreach(mapTaskCell, mapTaskList)
		{
			Task *mapTask = (Task *) lfirst(mapTaskCell);

			/* find the node name/port for map task's execution */
			List *mapTaskPlacementList = mapTask->taskPlacementList;

			ShardPlacement *mapTaskPlacement = linitial(mapTaskPlacementList);
			char *mapTaskNodeName = mapTaskPlacement->nodeName;
			uint32 mapTaskNodePort = mapTaskPlacement->nodePort;

			/*
			 * If replication factor is 1, then we know that we will use the first and
			 * the only placement. If task tracker is used, then it will regenerate the
			 * query string because if there are multiple placements then it does not
			 * know in which placement the parent map task was successful.
			 */
			StringInfo mapFetchQueryString = makeStringInfo();
			appendStringInfo(mapFetchQueryString, MAP_OUTPUT_FETCH_COMMAND,
							 mapTask->jobId, mapTask->taskId, partitionId,
							 mergeTaskId, /* fetch results to merge task */
							 mapTaskNodeName, mapTaskNodePort);

			Task *mapOutputFetchTask = CreateBasicTask(jobId, taskIdIndex,
													   MAP_OUTPUT_FETCH_TASK,
													   mapFetchQueryString->data);
			mapOutputFetchTask->partitionId = partitionId;
			mapOutputFetchTask->upstreamTaskId = mergeTaskId;
			mapOutputFetchTask->dependentTaskList = list_make1(mapTask);
			taskIdIndex++;

			mapOutputFetchTaskList = lappend(mapOutputFetchTaskList, mapOutputFetchTask);
		}

		/* merge task depends on completion of fetch tasks */
		mergeTask->dependentTaskList = mapOutputFetchTaskList;

		/* if single repartitioned, each merge task represents an interval */
		if (mapMergeJob->partitionType == RANGE_PARTITION_TYPE)
		{
			int32 mergeTaskIntervalId = partitionId - 1;
			ShardInterval **mergeTaskIntervals = mapMergeJob->sortedShardIntervalArray;
			Assert(mergeTaskIntervalId >= 0);

			mergeTask->shardInterval = mergeTaskIntervals[mergeTaskIntervalId];
		}
		else if (mapMergeJob->partitionType == SINGLE_HASH_PARTITION_TYPE)
		{
			int32 mergeTaskIntervalId = partitionId;
			ShardInterval **mergeTaskIntervals = mapMergeJob->sortedShardIntervalArray;
			Assert(mergeTaskIntervalId >= 0);

			mergeTask->shardInterval = mergeTaskIntervals[mergeTaskIntervalId];
		}

		mergeTaskList = lappend(mergeTaskList, mergeTask);
	}

	return mergeTaskList;
}


/*
 * ColumnNameArrayString creates a list of column names for a merged table, and
 * outputs this list of column names in their (array) string representation.
 */
static StringInfo
ColumnNameArrayString(uint32 columnCount, uint64 generatingJobId)
{
	Datum *columnNameArray = palloc0(columnCount * sizeof(Datum));
	uint32 columnNameIndex = 0;

	/* build list of intermediate column names, generated by given jobId */
	List *columnNameList = DerivedColumnNameList(columnCount, generatingJobId);

	ListCell *columnNameCell = NULL;
	foreach(columnNameCell, columnNameList)
	{
		Value *columnNameValue = (Value *) lfirst(columnNameCell);
		char *columnNameString = strVal(columnNameValue);
		Datum columnName = CStringGetDatum(columnNameString);

		columnNameArray[columnNameIndex] = columnName;
		columnNameIndex++;
	}

	StringInfo columnNameArrayString = DatumArrayString(columnNameArray, columnCount,
														CSTRINGOID);

	return columnNameArrayString;
}


/*
 * ColumnTypeArrayString resolves a list of column types for a merged table, and
 * outputs this list of column types in their (array) string representation.
 */
static StringInfo
ColumnTypeArrayString(List *targetEntryList)
{
	ListCell *targetEntryCell = NULL;

	uint32 columnCount = (uint32) list_length(targetEntryList);
	Datum *columnTypeArray = palloc0(columnCount * sizeof(Datum));
	uint32 columnTypeIndex = 0;

	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *columnExpression = (Node *) targetEntry->expr;
		Oid columnTypeId = exprType(columnExpression);
		int32 columnTypeMod = exprTypmod(columnExpression);

		char *columnTypeName = format_type_with_typemod(columnTypeId, columnTypeMod);
		Datum columnType = CStringGetDatum(columnTypeName);

		columnTypeArray[columnTypeIndex] = columnType;
		columnTypeIndex++;
	}

	StringInfo columnTypeArrayString = DatumArrayString(columnTypeArray, columnCount,
														CSTRINGOID);

	return columnTypeArrayString;
}


/*
 * AssignTaskList assigns locations to given tasks based on dependencies between
 * tasks and configured task assignment policies. The function also handles the
 * case where multiple SQL tasks depend on the same merge task, and makes sure
 * that this group of multiple SQL tasks and the merge task are assigned to the
 * same location.
 */
static List *
AssignTaskList(List *sqlTaskList)
{
	List *assignedSqlTaskList = NIL;
	bool hasAnchorShardId = false;
	ListCell *sqlTaskCell = NULL;
	List *primarySqlTaskList = NIL;
	ListCell *primarySqlTaskCell = NULL;
	ListCell *constrainedSqlTaskCell = NULL;

	/* no tasks to assign */
	if (sqlTaskList == NIL)
	{
		return NIL;
	}

	Task *firstSqlTask = (Task *) linitial(sqlTaskList);
	if (firstSqlTask->anchorShardId != INVALID_SHARD_ID)
	{
		hasAnchorShardId = true;
	}

	/*
	 * If these SQL tasks don't depend on any merge tasks, we can assign each
	 * one independently of the other. We therefore go ahead and assign these
	 * SQL tasks using the "anchor shard based" assignment algorithms.
	 */
	bool hasMergeTaskDependencies = HasMergeTaskDependencies(sqlTaskList);
	if (!hasMergeTaskDependencies)
	{
		Assert(hasAnchorShardId);

		assignedSqlTaskList = AssignAnchorShardTaskList(sqlTaskList);

		return assignedSqlTaskList;
	}

	/*
	 * SQL tasks can depend on merge tasks in one of two ways: (1) each SQL task
	 * depends on merge task(s) that no other SQL task depends upon, (2) several
	 * SQL tasks depend on the same merge task(s) and all need to be assigned to
	 * the same worker node. To handle the second case, we first pick a primary
	 * SQL task among those that depend on the same merge task, and assign it.
	 */
	foreach(sqlTaskCell, sqlTaskList)
	{
		Task *sqlTask = (Task *) lfirst(sqlTaskCell);
		List *mergeTaskList = FindDependentMergeTaskList(sqlTask);

		Task *firstMergeTask = (Task *) linitial(mergeTaskList);
		if (!firstMergeTask->assignmentConstrained)
		{
			firstMergeTask->assignmentConstrained = true;

			primarySqlTaskList = lappend(primarySqlTaskList, sqlTask);
		}
	}

	if (hasAnchorShardId)
	{
		primarySqlTaskList = AssignAnchorShardTaskList(primarySqlTaskList);
	}
	else
	{
		primarySqlTaskList = AssignDualHashTaskList(primarySqlTaskList);
	}

	/* propagate SQL task assignments to the merge tasks we depend upon */
	foreach(primarySqlTaskCell, primarySqlTaskList)
	{
		Task *sqlTask = (Task *) lfirst(primarySqlTaskCell);
		List *mergeTaskList = FindDependentMergeTaskList(sqlTask);

		ListCell *mergeTaskCell = NULL;
		foreach(mergeTaskCell, mergeTaskList)
		{
			Task *mergeTask = (Task *) lfirst(mergeTaskCell);
			Assert(mergeTask->taskPlacementList == NIL);

			mergeTask->taskPlacementList = list_copy(sqlTask->taskPlacementList);
		}

		assignedSqlTaskList = lappend(assignedSqlTaskList, sqlTask);
	}

	/*
	 * If we had a set of SQL tasks depending on the same merge task, we only
	 * assigned one SQL task from that set. We call the assigned SQL task the
	 * primary, and note that the remaining SQL tasks are constrained by the
	 * primary's task assignment. We propagate the primary's task assignment in
	 * each set to the remaining (constrained) tasks.
	 */
	List *constrainedSqlTaskList = TaskListDifference(sqlTaskList, primarySqlTaskList);

	foreach(constrainedSqlTaskCell, constrainedSqlTaskList)
	{
		Task *sqlTask = (Task *) lfirst(constrainedSqlTaskCell);
		List *mergeTaskList = FindDependentMergeTaskList(sqlTask);
		List *mergeTaskPlacementList = NIL;

		ListCell *mergeTaskCell = NULL;
		foreach(mergeTaskCell, mergeTaskList)
		{
			Task *mergeTask = (Task *) lfirst(mergeTaskCell);

			/*
			 * If we have more than one merge task, both of them should have the
			 * same task placement list.
			 */
			mergeTaskPlacementList = mergeTask->taskPlacementList;
			Assert(mergeTaskPlacementList != NIL);

			ereport(DEBUG3, (errmsg("propagating assignment from merge task %d "
									"to constrained sql task %d",
									mergeTask->taskId, sqlTask->taskId)));
		}

		sqlTask->taskPlacementList = list_copy(mergeTaskPlacementList);

		assignedSqlTaskList = lappend(assignedSqlTaskList, sqlTask);
	}

	return assignedSqlTaskList;
}


/*
 * HasMergeTaskDependencies checks if sql tasks in the given sql task list have
 * any dependencies on merge tasks. If they do, the function returns true.
 */
static bool
HasMergeTaskDependencies(List *sqlTaskList)
{
	bool hasMergeTaskDependencies = false;
	Task *sqlTask = (Task *) linitial(sqlTaskList);
	List *dependentTaskList = sqlTask->dependentTaskList;

	ListCell *dependentTaskCell = NULL;
	foreach(dependentTaskCell, dependentTaskList)
	{
		Task *dependentTask = (Task *) lfirst(dependentTaskCell);
		if (dependentTask->taskType == MERGE_TASK)
		{
			hasMergeTaskDependencies = true;
			break;
		}
	}

	return hasMergeTaskDependencies;
}


/* Return true if two tasks are equal, false otherwise. */
bool
TasksEqual(const Task *a, const Task *b)
{
	Assert(CitusIsA(a, Task));
	Assert(CitusIsA(b, Task));

	if (a->taskType != b->taskType)
	{
		return false;
	}
	if (a->jobId != b->jobId)
	{
		return false;
	}
	if (a->taskId != b->taskId)
	{
		return false;
	}

	return true;
}


/*
 * TaskListAppendUnique returns a list that contains the elements of the
 * input task list and appends the input task parameter if it doesn't already
 * exists the list.
 */
List *
TaskListAppendUnique(List *list, Task *task)
{
	if (TaskListMember(list, task))
	{
		return list;
	}

	return lappend(list, task);
}


/*
 * TaskListConcatUnique append to list1 each member of list2 that isn't
 * already in list1. Whether an element is already a member of the list
 * is determined via TaskListMember().
 */
List *
TaskListConcatUnique(List *list1, List *list2)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, list2)
	{
		Task *currentTask = (Task *) lfirst(taskCell);

		if (!TaskListMember(list1, currentTask))
		{
			list1 = lappend(list1, currentTask);
		}
	}

	return list1;
}


/* Is the passed in Task a member of the list. */
bool
TaskListMember(const List *taskList, const Task *task)
{
	const ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		if (TasksEqual((Task *) lfirst(taskCell), task))
		{
			return true;
		}
	}

	return false;
}


/*
 * TaskListDifference returns a list that contains all the tasks in taskList1
 * that are not in taskList2. The returned list is freshly allocated via
 * palloc(), but the cells themselves point to the same objects as the cells
 * of the input lists.
 */
List *
TaskListDifference(const List *list1, const List *list2)
{
	const ListCell *taskCell = NULL;
	List *resultList = NIL;

	if (list2 == NIL)
	{
		return list_copy(list1);
	}

	foreach(taskCell, list1)
	{
		if (!TaskListMember(list2, lfirst(taskCell)))
		{
			resultList = lappend(resultList, lfirst(taskCell));
		}
	}

	return resultList;
}


/*
 * AssignAnchorShardTaskList assigns locations to the given tasks based on the
 * configured task assignment policy. The distributed executor later sends these
 * tasks to their assigned locations for remote execution.
 */
List *
AssignAnchorShardTaskList(List *taskList)
{
	List *assignedTaskList = NIL;

	/* choose task assignment policy based on config value */
	if (TaskAssignmentPolicy == TASK_ASSIGNMENT_GREEDY)
	{
		assignedTaskList = GreedyAssignTaskList(taskList);
	}
	else if (TaskAssignmentPolicy == TASK_ASSIGNMENT_FIRST_REPLICA)
	{
		assignedTaskList = FirstReplicaAssignTaskList(taskList);
	}
	else if (TaskAssignmentPolicy == TASK_ASSIGNMENT_ROUND_ROBIN)
	{
		assignedTaskList = RoundRobinAssignTaskList(taskList);
	}

	Assert(assignedTaskList != NIL);
	return assignedTaskList;
}


/*
 * GreedyAssignTaskList uses a greedy algorithm similar to Hadoop's, and assigns
 * locations to the given tasks. The ideal assignment algorithm balances three
 * properties: (a) determinism, (b) even load distribution, and (c) consistency
 * across similar task lists. To maintain these properties, the algorithm sorts
 * all its input lists.
 */
static List *
GreedyAssignTaskList(List *taskList)
{
	List *assignedTaskList = NIL;
	uint32 assignedTaskCount = 0;
	uint32 taskCount = list_length(taskList);

	/* get the worker node list and sort the list */
	List *workerNodeList = ActiveReadableNodeList();
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/*
	 * We first sort tasks by their anchor shard id. We then walk over each task
	 * in the sorted list, get the task's anchor shard id, and look up the shard
	 * placements (locations) for this shard id. Next, we sort the placements by
	 * their insertion time, and append them to a new list.
	 */
	taskList = SortList(taskList, CompareTasksByShardId);
	List *activeShardPlacementLists = ActiveShardPlacementLists(taskList);

	while (assignedTaskCount < taskCount)
	{
		ListCell *workerNodeCell = NULL;
		uint32 loopStartTaskCount = assignedTaskCount;

		/* walk over each node and check if we can assign a task to it */
		foreach(workerNodeCell, workerNodeList)
		{
			WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

			Task *assignedTask = GreedyAssignTask(workerNode, taskList,
												  activeShardPlacementLists);
			if (assignedTask != NULL)
			{
				assignedTaskList = lappend(assignedTaskList, assignedTask);
				assignedTaskCount++;
			}
		}

		/* if we could not assign any new tasks, avoid looping forever */
		if (assignedTaskCount == loopStartTaskCount)
		{
			uint32 remainingTaskCount = taskCount - assignedTaskCount;
			ereport(ERROR, (errmsg("failed to assign %u task(s) to worker nodes",
								   remainingTaskCount)));
		}
	}

	return assignedTaskList;
}


/*
 * GreedyAssignTask tries to assign a task to the given worker node. To do this,
 * the function walks over tasks' anchor shard ids, and finds the first set of
 * nodes the shards were replicated to. If any of these replica nodes and the
 * given worker node match, the corresponding task is assigned to that node. If
 * not, the function goes on to search the second set of replicas and so forth.
 *
 * Note that this function has side-effects; when the function assigns a new
 * task, it overwrites the corresponding task list pointer.
 */
static Task *
GreedyAssignTask(WorkerNode *workerNode, List *taskList, List *activeShardPlacementLists)
{
	Task *assignedTask = NULL;
	List *taskPlacementList = NIL;
	ShardPlacement *primaryPlacement = NULL;
	uint32 rotatePlacementListBy = 0;
	uint32 replicaIndex = 0;
	uint32 replicaCount = ShardReplicationFactor;
	const char *workerName = workerNode->workerName;
	const uint32 workerPort = workerNode->workerPort;

	while ((assignedTask == NULL) && (replicaIndex < replicaCount))
	{
		/* walk over all tasks and try to assign one */
		ListCell *taskCell = NULL;
		ListCell *placementListCell = NULL;

		forboth(taskCell, taskList, placementListCell, activeShardPlacementLists)
		{
			Task *task = (Task *) lfirst(taskCell);
			List *placementList = (List *) lfirst(placementListCell);

			/* check if we already assigned this task */
			if (task == NULL)
			{
				continue;
			}

			/* check if we have enough replicas */
			uint32 placementCount = list_length(placementList);
			if (placementCount <= replicaIndex)
			{
				continue;
			}

			ShardPlacement *placement = (ShardPlacement *) list_nth(placementList,
																	replicaIndex);
			if ((strncmp(placement->nodeName, workerName, WORKER_LENGTH) == 0) &&
				(placement->nodePort == workerPort))
			{
				/* we found a task to assign to the given worker node */
				assignedTask = task;
				taskPlacementList = placementList;
				rotatePlacementListBy = replicaIndex;

				/* overwrite task list to signal that this task is assigned */
				taskCell->data.ptr_value = NULL;
				break;
			}
		}

		/* go over the next set of shard replica placements */
		replicaIndex++;
	}

	/* if we found a task placement list, rotate and assign task placements */
	if (assignedTask != NULL)
	{
		taskPlacementList = LeftRotateList(taskPlacementList, rotatePlacementListBy);
		assignedTask->taskPlacementList = taskPlacementList;

		primaryPlacement = (ShardPlacement *) linitial(assignedTask->taskPlacementList);
		ereport(DEBUG3, (errmsg("assigned task %u to node %s:%u", assignedTask->taskId,
								primaryPlacement->nodeName,
								primaryPlacement->nodePort)));
	}

	return assignedTask;
}


/*
 * FirstReplicaAssignTaskList assigns locations to the given tasks simply by
 * looking at placements for a given shard. A particular task's assignments are
 * then ordered by the insertion order of the relevant placements rows. In other
 * words, a task for a specific shard is simply assigned to the first replica
 * for that shard. This algorithm is extremely simple and intended for use when
 * a customer has placed shards carefully and wants strong guarantees about
 * which shards will be used by what nodes (i.e. for stronger memory residency
 * guarantees).
 */
List *
FirstReplicaAssignTaskList(List *taskList)
{
	/* No additional reordering need take place for this algorithm */
	List *(*reorderFunction)(Task *, List *) = NULL;

	taskList = ReorderAndAssignTaskList(taskList, reorderFunction);

	return taskList;
}


/*
 * RoundRobinAssignTaskList uses a round-robin algorithm to assign locations to
 * the given tasks. An ideal round-robin implementation requires keeping shared
 * state for task assignments; and we instead approximate our implementation by
 * relying on the sequentially increasing jobId. For each task, we mod its jobId
 * by the number of active shard placements, and ensure that we rotate between
 * these placements across subsequent queries.
 */
List *
RoundRobinAssignTaskList(List *taskList)
{
	taskList = ReorderAndAssignTaskList(taskList, RoundRobinReorder);

	return taskList;
}


/*
 * RoundRobinReorder implements the core of the round-robin assignment policy.
 * It takes a task and placement list and rotates a copy of the placement list
 * based on the latest stable transaction id provided by PostgreSQL.
 *
 * We prefer to use transactionId as the seed for the rotation to use the replicas
 * in the same worker node within the same transaction. This becomes more important
 * when we're reading from (the same or multiple) reference tables within a
 * transaction. With this approach, we can prevent reads to expand the worker nodes
 * that participate in a distributed transaction.
 *
 * Note that we prefer PostgreSQL's transactionId over distributed transactionId that
 * Citus generates since the distributed transactionId is generated during the execution
 * where as task-assignment happens duing the planning.
 */
List *
RoundRobinReorder(Task *task, List *placementList)
{
	TransactionId transactionId = GetMyProcLocalTransactionId();
	uint32 activePlacementCount = list_length(placementList);
	uint32 roundRobinIndex = (transactionId % activePlacementCount);

	placementList = LeftRotateList(placementList, roundRobinIndex);

	return placementList;
}


/*
 * ReorderAndAssignTaskList finds the placements for a task based on its anchor
 * shard id and then sorts them by insertion time. If reorderFunction is given,
 * it is used to reorder the placements list in a custom fashion (for instance,
 * by rotation or shuffling). Returns the task list with placements assigned.
 */
static List *
ReorderAndAssignTaskList(List *taskList, List * (*reorderFunction)(Task *, List *))
{
	List *assignedTaskList = NIL;
	ListCell *taskCell = NULL;
	ListCell *placementListCell = NULL;
	uint32 unAssignedTaskCount = 0;

	if (taskList == NIL)
	{
		return NIL;
	}

	/*
	 * We first sort tasks by their anchor shard id. We then sort placements for
	 * each anchor shard by the placement's insertion time. Note that we sort
	 * these lists just to make our policy more deterministic.
	 */
	taskList = SortList(taskList, CompareTasksByShardId);
	List *activeShardPlacementLists = ActiveShardPlacementLists(taskList);

	forboth(taskCell, taskList, placementListCell, activeShardPlacementLists)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *placementList = (List *) lfirst(placementListCell);

		/* inactive placements are already filtered out */
		uint32 activePlacementCount = list_length(placementList);
		if (activePlacementCount > 0)
		{
			if (reorderFunction != NULL)
			{
				placementList = reorderFunction(task, placementList);
			}
			task->taskPlacementList = placementList;

			ShardPlacement *primaryPlacement = (ShardPlacement *) linitial(
				task->taskPlacementList);
			ereport(DEBUG3, (errmsg("assigned task %u to node %s:%u", task->taskId,
									primaryPlacement->nodeName,
									primaryPlacement->nodePort)));

			assignedTaskList = lappend(assignedTaskList, task);
		}
		else
		{
			unAssignedTaskCount++;
		}
	}

	/* if we have unassigned tasks, error out */
	if (unAssignedTaskCount > 0)
	{
		ereport(ERROR, (errmsg("failed to assign %u task(s) to worker nodes",
							   unAssignedTaskCount)));
	}

	return assignedTaskList;
}


/* Helper function to compare two tasks by their anchor shardId. */
static int
CompareTasksByShardId(const void *leftElement, const void *rightElement)
{
	const Task *leftTask = *((const Task **) leftElement);
	const Task *rightTask = *((const Task **) rightElement);

	uint64 leftShardId = leftTask->anchorShardId;
	uint64 rightShardId = rightTask->anchorShardId;

	/* we compare 64-bit integers, instead of casting their difference to int */
	if (leftShardId > rightShardId)
	{
		return 1;
	}
	else if (leftShardId < rightShardId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


/*
 * ActiveShardPlacementLists finds the active shard placement list for each task in
 * the given task list, sorts each shard placement list by shard creation time,
 * and adds the sorted placement list into a new list of lists. The function also
 * ensures a one-to-one mapping between each placement list in the new list of
 * lists and each task in the given task list.
 */
static List *
ActiveShardPlacementLists(List *taskList)
{
	List *shardPlacementLists = NIL;
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		uint64 anchorShardId = task->anchorShardId;

		List *shardPlacementList = ActiveShardPlacementList(anchorShardId);

		/* filter out shard placements that reside in inactive nodes */
		List *activeShardPlacementList = ActivePlacementList(shardPlacementList);
		if (activeShardPlacementList == NIL)
		{
			ereport(ERROR,
					(errmsg("no active placements were found for shard " UINT64_FORMAT,
							anchorShardId)));
		}

		/* sort shard placements by their creation time */
		activeShardPlacementList = SortList(activeShardPlacementList,
											CompareShardPlacements);
		shardPlacementLists = lappend(shardPlacementLists, activeShardPlacementList);
	}

	return shardPlacementLists;
}


/*
 * CompareShardPlacements compares two shard placements by their tuple oid; this
 * oid reflects the tuple's insertion order into pg_dist_placement.
 */
int
CompareShardPlacements(const void *leftElement, const void *rightElement)
{
	const ShardPlacement *leftPlacement = *((const ShardPlacement **) leftElement);
	const ShardPlacement *rightPlacement = *((const ShardPlacement **) rightElement);

	uint64 leftPlacementId = leftPlacement->placementId;
	uint64 rightPlacementId = rightPlacement->placementId;

	if (leftPlacementId < rightPlacementId)
	{
		return -1;
	}
	else if (leftPlacementId > rightPlacementId)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}


/*
 * ActivePlacementList walks over shard placements in the given list, and finds
 * the corresponding worker node for each placement. The function then checks if
 * that worker node is active, and if it is, appends the placement to a new list.
 * The function last returns the new placement list.
 */
static List *
ActivePlacementList(List *placementList)
{
	List *activePlacementList = NIL;
	ListCell *placementCell = NULL;

	foreach(placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);

		/* check if the worker node for this shard placement is active */
		WorkerNode *workerNode = FindWorkerNode(placement->nodeName, placement->nodePort);
		if (workerNode != NULL && workerNode->isActive)
		{
			activePlacementList = lappend(activePlacementList, placement);
		}
	}

	return activePlacementList;
}


/*
 * LeftRotateList returns a copy of the given list that has been cyclically
 * shifted to the left by the given rotation count. For this, the function
 * repeatedly moves the list's first element to the end of the list, and
 * then returns the newly rotated list.
 */
static List *
LeftRotateList(List *list, uint32 rotateCount)
{
	List *rotatedList = list_copy(list);

	for (uint32 rotateIndex = 0; rotateIndex < rotateCount; rotateIndex++)
	{
		void *firstElement = linitial(rotatedList);

		rotatedList = list_delete_first(rotatedList);
		rotatedList = lappend(rotatedList, firstElement);
	}

	return rotatedList;
}


/*
 * FindDependentMergeTaskList walks over the given task's dependent task list,
 * finds the merge tasks in the list, and returns those found tasks in a new
 * list.
 */
static List *
FindDependentMergeTaskList(Task *sqlTask)
{
	List *dependentMergeTaskList = NIL;
	List *dependentTaskList = sqlTask->dependentTaskList;

	ListCell *dependentTaskCell = NULL;
	foreach(dependentTaskCell, dependentTaskList)
	{
		Task *dependentTask = (Task *) lfirst(dependentTaskCell);
		if (dependentTask->taskType == MERGE_TASK)
		{
			dependentMergeTaskList = lappend(dependentMergeTaskList, dependentTask);
		}
	}

	return dependentMergeTaskList;
}


/*
 * AssignDualHashTaskList uses a round-robin algorithm to assign locations to
 * tasks; these tasks don't have any anchor shards and instead operate on (hash
 * repartitioned) merged tables.
 */
static List *
AssignDualHashTaskList(List *taskList)
{
	List *assignedTaskList = NIL;
	ListCell *taskCell = NULL;
	Task *firstTask = (Task *) linitial(taskList);
	uint64 jobId = firstTask->jobId;
	uint32 assignedTaskIndex = 0;

	/*
	 * We start assigning tasks at an index determined by the jobId. This way,
	 * if subsequent jobs have a small number of tasks, we won't allocate the
	 * tasks to the same worker repeatedly.
	 */
	List *workerNodeList = ActiveReadableWorkerNodeList();
	uint32 workerNodeCount = (uint32) list_length(workerNodeList);
	uint32 beginningNodeIndex = jobId % workerNodeCount;

	/* sort worker node list and task list for deterministic results */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);
	taskList = SortList(taskList, CompareTasksByTaskId);

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *taskPlacementList = NIL;

		for (uint32 replicaIndex = 0; replicaIndex < ShardReplicationFactor;
			 replicaIndex++)
		{
			uint32 assignmentOffset = beginningNodeIndex + assignedTaskIndex +
									  replicaIndex;
			uint32 assignmentIndex = assignmentOffset % workerNodeCount;
			WorkerNode *workerNode = list_nth(workerNodeList, assignmentIndex);

			ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
			SetPlacementNodeMetadata(taskPlacement, workerNode);

			taskPlacementList = lappend(taskPlacementList, taskPlacement);
		}

		task->taskPlacementList = taskPlacementList;

		ShardPlacement *primaryPlacement = (ShardPlacement *) linitial(
			task->taskPlacementList);
		ereport(DEBUG3, (errmsg("assigned task %u to node %s:%u", task->taskId,
								primaryPlacement->nodeName,
								primaryPlacement->nodePort)));

		assignedTaskList = lappend(assignedTaskList, task);
		assignedTaskIndex++;
	}

	return assignedTaskList;
}


/*
 * SetPlacementNodeMetadata sets nodename, nodeport, nodeid and groupid for the placement.
 */
void
SetPlacementNodeMetadata(ShardPlacement *placement, WorkerNode *workerNode)
{
	placement->nodeName = pstrdup(workerNode->workerName);
	placement->nodePort = workerNode->workerPort;
	placement->nodeId = workerNode->nodeId;
	placement->groupId = workerNode->groupId;
}


/* Helper function to compare two tasks by their taskId. */
int
CompareTasksByTaskId(const void *leftElement, const void *rightElement)
{
	const Task *leftTask = *((const Task **) leftElement);
	const Task *rightTask = *((const Task **) rightElement);

	uint32 leftTaskId = leftTask->taskId;
	uint32 rightTaskId = rightTask->taskId;

	int taskIdDiff = leftTaskId - rightTaskId;
	return taskIdDiff;
}


/*
 * AssignDataFetchDependencies walks over tasks in the given sql or merge task
 * list. The function then propagates worker node assignments from each sql or
 * merge task to the task's data fetch dependencies.
 */
static void
AssignDataFetchDependencies(List *taskList)
{
	ListCell *taskCell = NULL;
	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *dependentTaskList = task->dependentTaskList;
		ListCell *dependentTaskCell = NULL;

		Assert(task->taskPlacementList != NIL);
		Assert(task->taskType == READ_TASK || task->taskType == MERGE_TASK);

		foreach(dependentTaskCell, dependentTaskList)
		{
			Task *dependentTask = (Task *) lfirst(dependentTaskCell);
			if (dependentTask->taskType == MAP_OUTPUT_FETCH_TASK)
			{
				dependentTask->taskPlacementList = task->taskPlacementList;
			}
		}
	}
}


/*
 * TaskListHighestTaskId walks over tasks in the given task list, finds the task
 * that has the largest taskId, and returns that taskId.
 *
 * Note: This function assumes that the dependent taskId's are set before the
 * taskId's for the given task list.
 */
static uint32
TaskListHighestTaskId(List *taskList)
{
	uint32 highestTaskId = 0;
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		if (task->taskId > highestTaskId)
		{
			highestTaskId = task->taskId;
		}
	}

	return highestTaskId;
}


/*
 * MergeTableQueryString builds a query string which creates a merge task table
 * within the job's schema, which should have already been created by the task
 * tracker protocol.
 */
static StringInfo
MergeTableQueryString(uint32 taskIdIndex, List *targetEntryList)
{
	StringInfo taskTableName = TaskTableName(taskIdIndex);
	StringInfo mergeTableQueryString = makeStringInfo();
	StringInfo mergeTableName = makeStringInfo();
	StringInfo columnsString = makeStringInfo();
	ListCell *targetEntryCell = NULL;
	uint32 columnIndex = 0;

	appendStringInfo(mergeTableName, "%s%s", taskTableName->data, MERGE_TABLE_SUFFIX);

	uint32 columnCount = (uint32) list_length(targetEntryList);

	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *columnExpression = (Node *) targetEntry->expr;
		Oid columnTypeId = exprType(columnExpression);
		int32 columnTypeMod = exprTypmod(columnExpression);

		StringInfo columnNameString = makeStringInfo();
		appendStringInfo(columnNameString, MERGE_COLUMN_FORMAT, columnIndex);

		char *columnName = columnNameString->data;
		char *columnType = format_type_with_typemod(columnTypeId, columnTypeMod);

		appendStringInfo(columnsString, "%s %s", columnName, columnType);

		columnIndex++;
		if (columnIndex != columnCount)
		{
			appendStringInfo(columnsString, ", ");
		}
	}

	appendStringInfo(mergeTableQueryString, CREATE_TABLE_COMMAND, mergeTableName->data,
					 columnsString->data);

	return mergeTableQueryString;
}


/*
 * IntermediateTableQueryString builds a query string which creates a task table
 * by running reduce query on already created merge table.
 */
static StringInfo
IntermediateTableQueryString(uint64 jobId, uint32 taskIdIndex, Query *reduceQuery)
{
	StringInfo taskTableName = TaskTableName(taskIdIndex);
	StringInfo intermediateTableQueryString = makeStringInfo();
	StringInfo mergeTableName = makeStringInfo();
	StringInfo columnsString = makeStringInfo();
	StringInfo taskReduceQueryString = makeStringInfo();
	Query *taskReduceQuery = copyObject(reduceQuery);
	ListCell *columnNameCell = NULL;
	uint32 columnIndex = 0;

	uint32 columnCount = FinalTargetEntryCount(reduceQuery->targetList);
	List *columnNames = DerivedColumnNameList(columnCount, jobId);

	foreach(columnNameCell, columnNames)
	{
		Value *columnNameValue = (Value *) lfirst(columnNameCell);
		char *columnName = strVal(columnNameValue);

		appendStringInfo(columnsString, "%s", columnName);

		columnIndex++;
		if (columnIndex != columnCount)
		{
			appendStringInfo(columnsString, ", ");
		}
	}

	appendStringInfo(mergeTableName, "%s%s", taskTableName->data, MERGE_TABLE_SUFFIX);

	List *rangeTableList = taskReduceQuery->rtable;
	RangeTblEntry *rangeTableEntry = (RangeTblEntry *) linitial(rangeTableList);
	Alias *referenceNames = rangeTableEntry->eref;
	referenceNames->aliasname = mergeTableName->data;

	rangeTableEntry->alias = rangeTableEntry->eref;

	ModifyRangeTblExtraData(rangeTableEntry, GetRangeTblKind(rangeTableEntry),
							NULL, mergeTableName->data, NIL);

	pg_get_query_def(taskReduceQuery, taskReduceQueryString);

	appendStringInfo(intermediateTableQueryString, CREATE_TABLE_AS_COMMAND,
					 taskTableName->data, columnsString->data,
					 taskReduceQueryString->data);

	return intermediateTableQueryString;
}


/*
 * FinalTargetEntryCount returns count of target entries in the final target
 * entry list.
 */
static uint32
FinalTargetEntryCount(List *targetEntryList)
{
	uint32 finalTargetEntryCount = 0;
	ListCell *targetEntryCell = NULL;

	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		if (!targetEntry->resjunk)
		{
			finalTargetEntryCount++;
		}
	}

	return finalTargetEntryCount;
}
