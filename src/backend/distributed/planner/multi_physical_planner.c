/*-------------------------------------------------------------------------
 *
 * multi_physical_planner.c
 *	  Routines for creating physical plans from given multi-relational algebra
 *	  trees.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/skey.h"
#include "catalog/pg_am.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "distributed/listutils.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/task_tracker.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/typcache.h"


/* Policy to use when assigning tasks to worker nodes */
int TaskAssignmentPolicy = TASK_ASSIGNMENT_GREEDY;


/*
 * OperatorCache is used for caching operator identifiers for given typeId,
 * accessMethodId and strategyNumber. It is initialized to empty list as
 * there are no items in the cache.
 */
static List *OperatorCache = NIL;


/* Local functions forward declarations for job creation */
static Job * BuildJobTree(MultiTreeRoot *multiTree);
static MultiNode * LeftMostNode(MultiTreeRoot *multiTree);
static Oid RangePartitionJoinBaseRelationId(MultiJoin *joinNode);
static MultiTable * FindTableNode(MultiNode *multiNode, int rangeTableId);
static Query * BuildJobQuery(MultiNode *multiNode, List *dependedJobList);
static Query * BuildReduceQuery(MultiExtendedOp *extendedOpNode, List *dependedJobList);
static List * BaseRangeTableList(MultiNode *multiNode);
static List * QueryTargetList(MultiNode *multiNode);
static List * TargetEntryList(List *expressionList);
static List * QueryGroupClauseList(MultiNode *multiNode);
static List * QuerySelectClauseList(MultiNode *multiNode);
static List * QueryJoinClauseList(MultiNode *multiNode);
static List * QueryFromList(List *rangeTableList);
static Node * QueryJoinTree(MultiNode *multiNode, List *dependedJobList,
							List **rangeTableList);
static RangeTblEntry * JoinRangeTableEntry(JoinExpr *joinExpr, List *dependedJobList,
										   List *rangeTableList);
static int ExtractRangeTableId(Node *node);
static void ExtractColumns(RangeTblEntry *rangeTableEntry, int rangeTableId,
						   List *dependedJobList, List **columnNames, List **columnVars);
static RangeTblEntry * DerivedRangeTableEntry(MultiNode *multiNode, List *columnNames,
											  List *tableIdList);
static List * DerivedColumnNameList(uint32 columnCount, uint64 generatingJobId);
static Query * BuildSubqueryJobQuery(MultiNode *multiNode);
static void UpdateColumnAttributes(Var *column, List *rangeTableList,
								   List *dependedJobList);
static Index NewTableId(Index originalTableId, List *rangeTableList);
static AttrNumber NewColumnId(Index originalTableId, AttrNumber originalColumnId,
							  RangeTblEntry *newRangeTableEntry, List *dependedJobList);
static Job * JobForRangeTable(List *jobList, RangeTblEntry *rangeTableEntry);
static Job * JobForTableIdList(List *jobList, List *searchedTableIdList);
static List * ChildNodeList(MultiNode *multiNode);
static uint64 UniqueJobId(void);
static Job * BuildJob(Query *jobQuery, List *dependedJobList);
static MapMergeJob * BuildMapMergeJob(Query *jobQuery, List *dependedJobList,
									  Var *partitionKey, PartitionType partitionType,
									  Oid baseRelationId,
									  BoundaryNodeJobType boundaryNodeJobType);
static uint32 HashPartitionCount(void);
static ArrayType * SplitPointObject(ShardInterval **shardIntervalArray,
									uint32 shardIntervalCount);

/* Local functions forward declarations for task list creation */
static Job * BuildJobTreeTaskList(Job *jobTree);
static List * SubquerySqlTaskList(Job *job);
static List * SqlTaskList(Job *job);
static bool DependsOnHashPartitionJob(Job *job);
static uint32 AnchorRangeTableId(List *rangeTableList);
static List * BaseRangeTableIdList(List *rangeTableList);
static List * AnchorRangeTableIdList(List *rangeTableList, List *baseRangeTableIdList);
static void AdjustColumnOldAttributes(List *expressionList);
static List * RangeTableFragmentsList(List *rangeTableList, List *whereClauseList,
									  List *dependedJobList);
static OperatorCacheEntry * LookupOperatorByType(Oid typeId, Oid accessMethodId,
												 int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static Node * HashableClauseMutator(Node *originalNode, Var *partitionColumn);
static Var * MakeInt4Column(void);
static Const * MakeInt4Constant(Datum constantValue);
static OpExpr * MakeHashedOperatorExpression(OpExpr *operatorExpression);
static OpExpr * MakeOpExpressionWithZeroConst(void);
static List * BuildRestrictInfoList(List *qualList);
static List * FragmentCombinationList(List *rangeTableFragmentsList, Query *jobQuery,
									  List *dependedJobList);
static JoinSequenceNode * JoinSequenceArray(List *rangeTableFragmentsList,
											Query *jobQuery, List *dependedJobList);
static bool PartitionedOnColumn(Var *column, List *rangeTableList, List *dependedJobList);
static void CheckJoinBetweenColumns(OpExpr *joinClause);
static List * FindRangeTableFragmentsList(List *rangeTableFragmentsList, int taskId);
static bool JoinPrunable(RangeTableFragment *leftFragment,
						 RangeTableFragment *rightFragment);
static ShardInterval * FragmentInterval(RangeTableFragment *fragment);
static StringInfo FragmentIntervalString(ShardInterval *fragmentInterval);
static List * UniqueFragmentList(List *fragmentList);
static List * DataFetchTaskList(uint64 jobId, uint32 taskIdIndex, List *fragmentList);
static StringInfo NodeNameArrayString(List *workerNodeList);
static StringInfo NodePortArrayString(List *workerNodeList);
static StringInfo DatumArrayString(Datum *datumArray, uint32 datumCount, Oid datumTypeId);
static Task * CreateBasicTask(uint64 jobId, uint32 taskId, TaskType taskType,
							  char *queryString);
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
static List * RoundRobinAssignTaskList(List *taskList);
static List * RoundRobinReorder(Task *task, List *placementList);
static List * ReorderAndAssignTaskList(List *taskList,
									   List * (*reorderFunction)(Task *, List *));
static int CompareTasksByShardId(const void *leftElement, const void *rightElement);
static List * ActiveShardPlacementLists(List *taskList);
static List * ActivePlacementList(List *placementList);
static List * LeftRotateList(List *list, uint32 rotateCount);
static List * FindDependedMergeTaskList(Task *sqlTask);
static List * AssignDualHashTaskList(List *taskList);
static int CompareTasksByTaskId(const void *leftElement, const void *rightElement);
static void AssignDataFetchDependencies(List *taskList);
static uint32 TaskListHighestTaskId(List *taskList);
static List * MapTaskList(MapMergeJob *mapMergeJob, List *filterTaskList);
static char * ColumnName(Var *column, List *rangeTableList);
static StringInfo SplitPointArrayString(ArrayType *splitPointObject,
										Oid columnType, int32 columnTypeMod);
static List * MergeTaskList(MapMergeJob *mapMergeJob, List *mapTaskList,
							uint32 taskIdIndex);
static StringInfo ColumnNameArrayString(uint32 columnCount, uint64 generatingJobId);
static StringInfo ColumnTypeArrayString(List *targetEntryList);
static StringInfo MergeTableQueryString(uint32 taskIdIndex, List *targetEntryList);
static StringInfo IntermediateTableQueryString(uint64 jobId, uint32 taskIdIndex,
											   Query *reduceQuery);
static uint32 FinalTargetEntryCount(List *targetEntryList);


/*
 * MultiPhysicalPlanCreate is the entry point for physical plan generation. The
 * function builds the physical plan; this plan includes the list of tasks to be
 * executed on worker nodes, and the final query to run on the master node.
 */
MultiPlan *
MultiPhysicalPlanCreate(MultiTreeRoot *multiTree)
{
	MultiPlan *multiPlan = NULL;
	StringInfo jobSchemaName = NULL;
	Job *workerJob = NULL;
	uint64 workerJobId = 0;
	Query *masterQuery = NULL;
	List *masterDependedJobList = NIL;

	/* build the worker job tree and check that we only one job in the tree */
	workerJob = BuildJobTree(multiTree);

	/* create the tree of executable tasks for the worker job */
	workerJob = BuildJobTreeTaskList(workerJob);
	workerJobId = workerJob->jobId;

	/* get job schema name */
	jobSchemaName = JobSchemaName(workerJobId);

	/* build the final merge query to execute on the master */
	masterDependedJobList = list_make1(workerJob);
	masterQuery = BuildJobQuery((MultiNode *) multiTree, masterDependedJobList);

	multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->workerJob = workerJob;
	multiPlan->masterQuery = masterQuery;
	multiPlan->masterTableName = jobSchemaName->data;

	return multiPlan;
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
	List *loopDependedJobList = NIL;
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
			if (joinNode->joinRuleType == SINGLE_PARTITION_JOIN ||
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

			if (joinNode->joinRuleType == SINGLE_PARTITION_JOIN)
			{
				partitionType = RANGE_PARTITION_TYPE;
				baseRelationId = RangePartitionJoinBaseRelationId(joinNode);
			}
			else if (joinNode->joinRuleType == DUAL_PARTITION_JOIN)
			{
				partitionType = HASH_PARTITION_TYPE;
			}

			if (CitusIsA(leftChildNode, MultiPartition))
			{
				MultiPartition *partitionNode = (MultiPartition *) leftChildNode;
				MultiNode *queryNode = GrandChildNode((MultiUnaryNode *) partitionNode);
				Var *partitionKey = partitionNode->partitionColumn;

				/* build query and partition job */
				List *dependedJobList = list_copy(loopDependedJobList);
				Query *jobQuery = BuildJobQuery(queryNode, dependedJobList);

				MapMergeJob *mapMergeJob = BuildMapMergeJob(jobQuery, dependedJobList,
															partitionKey, partitionType,
															baseRelationId,
															JOIN_MAP_MERGE_JOB);

				/* reset depended job list */
				loopDependedJobList = NIL;
				loopDependedJobList = list_make1(mapMergeJob);
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

				/* append to the depended job list for on-going dependencies */
				loopDependedJobList = lappend(loopDependedJobList, mapMergeJob);
			}
		}
		else if (boundaryNodeJobType == SUBQUERY_MAP_MERGE_JOB)
		{
			MultiPartition *partitionNode = (MultiPartition *) currentNode;
			MultiNode *queryNode = GrandChildNode((MultiUnaryNode *) partitionNode);
			Var *partitionKey = partitionNode->partitionColumn;

			/* build query and partition job */
			List *dependedJobList = list_copy(loopDependedJobList);
			Query *jobQuery = BuildJobQuery(queryNode, dependedJobList);

			MapMergeJob *mapMergeJob = BuildMapMergeJob(jobQuery, dependedJobList,
														partitionKey, HASH_PARTITION_TYPE,
														InvalidOid,
														SUBQUERY_MAP_MERGE_JOB);

			Query *reduceQuery = BuildReduceQuery((MultiExtendedOp *) parentNode,
												  list_make1(mapMergeJob));
			mapMergeJob->reduceQuery = reduceQuery;

			/* reset depended job list */
			loopDependedJobList = NIL;
			loopDependedJobList = list_make1(mapMergeJob);
		}
		else if (boundaryNodeJobType == TOP_LEVEL_WORKER_JOB)
		{
			MultiNode *childNode = ChildNode((MultiUnaryNode *) currentNode);
			List *dependedJobList = list_copy(loopDependedJobList);
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

				topLevelJob = BuildJob(topLevelQuery, dependedJobList);
				topLevelJob->subqueryPushdown = true;
			}
			else
			{
				Query *topLevelQuery = BuildJobQuery(childNode, dependedJobList);

				topLevelJob = BuildJob(topLevelQuery, dependedJobList);
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
	MultiTable *baseTable = NULL;
	Index baseTableId = 0;
	Oid baseRelationId = InvalidOid;

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

	baseTableId = partitionNode->splitPointTableId;
	baseTable = FindTableNode((MultiNode *) joinNode, baseTableId);
	baseRelationId = baseTable->relationId;

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
 * particular job. The function assumes that jobs, this particular job depends on,
 * have already been built, as their output is needed to build the query.
 */
static Query *
BuildJobQuery(MultiNode *multiNode, List *dependedJobList)
{
	Query *jobQuery = NULL;
	MultiNode *parentNode = NULL;
	bool updateColumnAttributes = false;
	List *rangeTableList = NIL;
	List *targetList = NIL;
	List *extendedOpNodeList = NIL;
	List *sortClauseList = NIL;
	List *groupClauseList = NIL;
	List *selectClauseList = NIL;
	List *columnList = NIL;
	Node *limitCount = NULL;
	Node *limitOffset = NULL;
	ListCell *columnCell = NULL;
	FromExpr *joinTree = NULL;
	Node *joinRoot = NULL;

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
	parentNode = ParentNode(multiNode);
	if (parentNode != NULL)
	{
		updateColumnAttributes = true;
	}

	/*
	 * If we are building this query on a repartitioned subquery job then we
	 * don't need to update column attributes.
	 */
	if (dependedJobList != NIL)
	{
		Job *job = (Job *) linitial(dependedJobList);
		if (CitusIsA(job, MapMergeJob))
		{
			MapMergeJob *mapMergeJob = (MapMergeJob *) job;
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
	extendedOpNodeList = FindNodesOfType(multiNode, T_MultiExtendedOp);
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);
		targetList = copyObject(extendedOp->targetList);
	}
	else
	{
		targetList = QueryTargetList(multiNode);
	}

	/* build the join tree and the range table list */
	rangeTableList = BaseRangeTableList(multiNode);
	joinRoot = QueryJoinTree(multiNode, dependedJobList, &rangeTableList);

	/* update the column attributes for target entries */
	if (updateColumnAttributes)
	{
		ListCell *columnCell = NULL;
		List *columnList = pull_var_clause_default((Node *) targetList);
		foreach(columnCell, columnList)
		{
			Var *column = (Var *) lfirst(columnCell);
			UpdateColumnAttributes(column, rangeTableList, dependedJobList);
		}
	}

	/* extract limit count/offset and sort clauses */
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);

		limitCount = extendedOp->limitCount;
		limitOffset = extendedOp->limitOffset;
		sortClauseList = extendedOp->sortClauseList;
	}

	/* build group clauses */
	groupClauseList = QueryGroupClauseList(multiNode);

	/* build the where clause list using select predicates */
	selectClauseList = QuerySelectClauseList(multiNode);

	/* set correct column attributes for select columns */
	if (updateColumnAttributes)
	{
		columnCell = NULL;
		columnList = pull_var_clause_default((Node *) selectClauseList);
		foreach(columnCell, columnList)
		{
			Var *column = (Var *) lfirst(columnCell);
			UpdateColumnAttributes(column, rangeTableList, dependedJobList);
		}
	}

	/*
	 * Build the From/Where construct. We keep the where-clause list implicitly
	 * AND'd, since both partition and join pruning depends on the clauses being
	 * expressed as a list.
	 */
	joinTree = makeNode(FromExpr);
	joinTree->quals = (Node *) list_copy(selectClauseList);
	joinTree->fromlist = list_make1(joinRoot);

	/* build the query structure for this job */
	jobQuery = makeNode(Query);
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
	jobQuery->hasAggs = contain_agg_clause((Node *) targetList);

	return jobQuery;
}


/*
 * BuildReduceQuery traverses the given logical plan tree, determines the job that
 * corresponds to this part of the tree, and builds the query structure for that
 * particular job. The function assumes that jobs this particular job depends on
 * have already been built, as their output is needed to build the query.
 */
static Query *
BuildReduceQuery(MultiExtendedOp *extendedOpNode, List *dependedJobList)
{
	Query *reduceQuery = NULL;
	MultiNode *multiNode = (MultiNode *) extendedOpNode;
	List *derivedRangeTableList = NIL;
	List *targetList = NIL;
	List *whereClauseList = NIL;
	List *selectClauseList = NIL;
	List *joinClauseList = NIL;
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	FromExpr *joinTree = NULL;
	List *columnNameList = NIL;
	RangeTblEntry *rangeTableEntry = NULL;

	Job *dependedJob = linitial(dependedJobList);
	List *dependedTargetList = dependedJob->jobQuery->targetList;
	uint32 columnCount = (uint32) list_length(dependedTargetList);
	uint32 columnIndex = 0;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Value *columnValue = NULL;
		StringInfo columnNameString = makeStringInfo();

		appendStringInfo(columnNameString, MERGE_COLUMN_FORMAT, columnIndex);

		columnValue = makeString(columnNameString->data);
		columnNameList = lappend(columnNameList, columnValue);
	}

	/* create a derived range table for the subtree below the collect */
	rangeTableEntry = DerivedRangeTableEntry(multiNode, columnNameList,
											 OutputTableIdList(multiNode));
	rangeTableEntry->eref->colnames = columnNameList;
	ModifyRangeTblExtraData(rangeTableEntry, CITUS_RTE_SHARD, NULL, NULL, NULL);
	derivedRangeTableList = lappend(derivedRangeTableList, rangeTableEntry);

	targetList = copyObject(extendedOpNode->targetList);
	columnList = pull_var_clause_default((Node *) targetList);

	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		Index originalTableId = column->varnoold;

		/* find the new table identifier */
		Index newTableId = NewTableId(originalTableId, derivedRangeTableList);
		column->varno = newTableId;
	}

	/* build the where clause list using select and join predicates */
	selectClauseList = QuerySelectClauseList((MultiNode *) extendedOpNode);
	joinClauseList = QueryJoinClauseList((MultiNode *) extendedOpNode);
	whereClauseList = list_concat(selectClauseList, joinClauseList);

	/*
	 * Build the From/Where construct. We keep the where-clause list implicitly
	 * AND'd, since both partition and join pruning depends on the clauses being
	 * expressed as a list.
	 */
	joinTree = makeNode(FromExpr);
	joinTree->quals = (Node *) whereClauseList;
	joinTree->fromlist = QueryFromList(derivedRangeTableList);

	/* build the query structure for this job */
	reduceQuery = makeNode(Query);
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
	reduceQuery->hasAggs = contain_agg_clause((Node *) targetList);

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
		MultiNode *multiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(multiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		if (nodeType == T_MultiTable)
		{
			/*
			 * We represent subqueries as MultiTables, and so for base table
			 * entries we skip the subquery ones.
			 */
			MultiTable *multiTable = (MultiTable *) multiNode;
			if (multiTable->relationId != SUBQUERY_RELATION_ID &&
				multiTable->relationId != HEAP_ANALYTICS_SUBQUERY_RELATION_ID)
			{
				RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
				rangeTableEntry->inFromCl = true;
				rangeTableEntry->eref = multiTable->referenceNames;
				rangeTableEntry->alias = multiTable->alias;
				rangeTableEntry->relid = multiTable->relationId;
				SetRangeTblExtraData(rangeTableEntry, CITUS_RTE_RELATION, NULL, NULL,
									 list_make1_int(multiTable->rangeTableId));

				baseRangeTableList = lappend(baseRangeTableList, rangeTableEntry);
			}
		}

		/* do not visit nodes that belong to remote queries */
		if (nodeType != T_MultiCollect)
		{
			List *childNodeList = ChildNodeList(multiNode);
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
DerivedRangeTableEntry(MultiNode *multiNode, List *columnList, List *tableIdList)
{
	RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->eref = makeNode(Alias);
	rangeTableEntry->eref->colnames = columnList;

	SetRangeTblExtraData(rangeTableEntry, CITUS_RTE_REMOTE_QUERY, NULL, NULL,
						 tableIdList);

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
	uint32 columnIndex = 0;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		StringInfo columnName = makeStringInfo();
		Value *columnValue = NULL;

		appendStringInfo(columnName, "intermediate_column_");
		appendStringInfo(columnName, UINT64_FORMAT "_", generatingJobId);
		appendStringInfo(columnName, "%u", columnIndex);

		columnValue = makeString(columnName->data);
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
	MultiProject *topProjectNode = NULL;
	List *columnList = NIL;
	List *queryTargetList = NIL;

	List *projectNodeList = FindNodesOfType(multiNode, T_MultiProject);
	Assert(list_length(projectNodeList) > 0);

	topProjectNode = (MultiProject *) linitial(projectNodeList);
	columnList = topProjectNode->columnList;
	queryTargetList = TargetEntryList(columnList);

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
		MultiNode *multiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(multiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		/* extract the group clause list from the extended operator */
		if (nodeType == T_MultiExtendedOp)
		{
			MultiExtendedOp *extendedOpNode = (MultiExtendedOp *) multiNode;
			groupClauseList = extendedOpNode->groupClauseList;
		}

		/* add children only if this node isn't a multi collect and multi table */
		if (nodeType != T_MultiCollect && nodeType != T_MultiTable)
		{
			List *childNodeList = ChildNodeList(multiNode);
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
		MultiNode *multiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(multiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		/* extract select clauses from the multi select node */
		if (nodeType == T_MultiSelect)
		{
			MultiSelect *selectNode = (MultiSelect *) multiNode;
			List *clauseList = copyObject(selectNode->selectClauseList);
			selectClauseList = list_concat(selectClauseList, clauseList);
		}

		/* add children only if this node isn't a multi collect */
		if (nodeType != T_MultiCollect)
		{
			List *childNodeList = ChildNodeList(multiNode);
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
		MultiNode *multiNode = (MultiNode *) linitial(pendingNodeList);
		CitusNodeTag nodeType = CitusNodeTag(multiNode);
		pendingNodeList = list_delete_first(pendingNodeList);

		/* extract join clauses from the multi join node */
		if (nodeType == T_MultiJoin)
		{
			MultiJoin *joinNode = (MultiJoin *) multiNode;
			List *clauseList = copyObject(joinNode->joinClauseList);
			joinClauseList = list_concat(joinClauseList, clauseList);
		}

		/* add this node's children only if the node isn't a multi collect */
		if (nodeType != T_MultiCollect)
		{
			List *childNodeList = ChildNodeList(multiNode);
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
QueryJoinTree(MultiNode *multiNode, List *dependedJobList, List **rangeTableList)
{
	CitusNodeTag nodeType = CitusNodeTag(multiNode);

	switch (nodeType)
	{
		case T_MultiJoin:
		{
			MultiJoin *joinNode = (MultiJoin *) multiNode;
			MultiBinaryNode *binaryNode = (MultiBinaryNode *) multiNode;
			List *columnList = NIL;
			ListCell *columnCell = NULL;
			RangeTblEntry *rangeTableEntry = NULL;
			JoinExpr *joinExpr = makeNode(JoinExpr);
			joinExpr->jointype = joinNode->joinType;
			joinExpr->isNatural = false;
			joinExpr->larg = QueryJoinTree(binaryNode->leftChildNode, dependedJobList,
										   rangeTableList);
			joinExpr->rarg = QueryJoinTree(binaryNode->rightChildNode, dependedJobList,
										   rangeTableList);
			joinExpr->usingClause = NIL;
			joinExpr->alias = NULL;
			joinExpr->rtindex = list_length(*rangeTableList) + 1;

			/*
			 * PostgreSQL's optimizer may mark left joins as anti-joins, when there
			 * is an right-hand-join-key-is-null restriction, but there is no logic
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

			rangeTableEntry = JoinRangeTableEntry(joinExpr, dependedJobList,
												  *rangeTableList);
			*rangeTableList = lappend(*rangeTableList, rangeTableEntry);

			/* fix the column attributes in ON (...) clauses */
			columnList = pull_var_clause_default((Node *) joinNode->joinClauseList);
			foreach(columnCell, columnList)
			{
				Var *column = (Var *) lfirst(columnCell);
				UpdateColumnAttributes(column, *rangeTableList, dependedJobList);

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
				Node *childNode = QueryJoinTree(unaryNode->childNode, dependedJobList,
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
			Job *dependedJob = JobForTableIdList(dependedJobList, tableIdList);
			List *dependedTargetList = dependedJob->jobQuery->targetList;

			/* compute column names for the derived table */
			uint32 columnCount = (uint32) list_length(dependedTargetList);
			List *columnNameList = DerivedColumnNameList(columnCount, dependedJob->jobId);

			RangeTblEntry *rangeTableEntry = DerivedRangeTableEntry(multiNode,
																	columnNameList,
																	tableIdList);
			RangeTblRef *rangeTableRef = makeNode(RangeTblRef);

			rangeTableRef->rtindex = list_length(*rangeTableList) + 1;
			*rangeTableList = lappend(*rangeTableList, rangeTableEntry);

			return (Node *) rangeTableRef;
		}

		case T_MultiCartesianProduct:
		{
			MultiBinaryNode *binaryNode = (MultiBinaryNode *) multiNode;
			RangeTblEntry *rangeTableEntry = NULL;

			JoinExpr *joinExpr = makeNode(JoinExpr);
			joinExpr->jointype = JOIN_INNER;
			joinExpr->isNatural = false;
			joinExpr->larg = QueryJoinTree(binaryNode->leftChildNode, dependedJobList,
										   rangeTableList);
			joinExpr->rarg = QueryJoinTree(binaryNode->rightChildNode, dependedJobList,
										   rangeTableList);
			joinExpr->usingClause = NIL;
			joinExpr->alias = NULL;
			joinExpr->quals = NULL;
			joinExpr->rtindex = list_length(*rangeTableList) + 1;

			rangeTableEntry = JoinRangeTableEntry(joinExpr, dependedJobList,
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
			Node *childNode = NULL;

			Assert(UnaryOperator(multiNode));

			childNode = QueryJoinTree(unaryNode->childNode, dependedJobList,
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
JoinRangeTableEntry(JoinExpr *joinExpr, List *dependedJobList, List *rangeTableList)
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

	ExtractColumns(leftRTE, leftRangeTableId, dependedJobList,
				   &leftColumnNames, &leftColumnVars);
	ExtractColumns(rightRTE, rightRangeTableId, dependedJobList,
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
ExtractColumns(RangeTblEntry *rangeTableEntry, int rangeTableId, List *dependedJobList,
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
		Job *dependedJob = JobForRangeTable(dependedJobList, rangeTableEntry);
		Query *jobQuery = dependedJob->jobQuery;

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
	Index rangeTableIndex = 1;
	int rangeTableCount = list_length(rangeTableList);

	for (rangeTableIndex = 1; rangeTableIndex <= rangeTableCount; rangeTableIndex++)
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
	Query *jobQuery = NULL;
	Query *subquery = NULL;
	MultiTable *multiTable = NULL;
	RangeTblEntry *rangeTableEntry = NULL;
	List *subqueryMultiTableList = NIL;
	List *rangeTableList = NIL;
	List *targetList = NIL;
	List *extendedOpNodeList = NIL;
	List *sortClauseList = NIL;
	List *groupClauseList = NIL;
	List *whereClauseList = NIL;
	Node *limitCount = NULL;
	Node *limitOffset = NULL;
	FromExpr *joinTree = NULL;

	/* we start building jobs from below the collect node */
	Assert(!CitusIsA(multiNode, MultiCollect));

	subqueryMultiTableList = SubqueryMultiTableList(multiNode);
	Assert(list_length(subqueryMultiTableList) == 1);

	multiTable = (MultiTable *) linitial(subqueryMultiTableList);
	subquery = multiTable->subquery;

	/*  build subquery range table list */
	rangeTableEntry = makeNode(RangeTblEntry);
	rangeTableEntry->rtekind = RTE_SUBQUERY;
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->eref = multiTable->referenceNames;
	rangeTableEntry->alias = multiTable->alias;
	rangeTableEntry->subquery = subquery;

	rangeTableList = list_make1(rangeTableEntry);

	/*
	 * If we have an extended operator, then we copy the operator's target list.
	 * Otherwise, we use the target list based on the MultiProject node at this
	 * level in the query tree.
	 */
	extendedOpNodeList = FindNodesOfType(multiNode, T_MultiExtendedOp);
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);
		targetList = copyObject(extendedOp->targetList);
	}
	else
	{
		targetList = QueryTargetList(multiNode);
	}

	/* extract limit count/offset and sort clauses */
	if (extendedOpNodeList != NIL)
	{
		MultiExtendedOp *extendedOp = (MultiExtendedOp *) linitial(extendedOpNodeList);

		limitCount = extendedOp->limitCount;
		limitOffset = extendedOp->limitOffset;
		sortClauseList = extendedOp->sortClauseList;
	}

	/* build group clauses */
	groupClauseList = QueryGroupClauseList(multiNode);

	/* build the where clause list using select predicates */
	whereClauseList = QuerySelectClauseList(multiNode);

	/*
	 * Build the From/Where construct. We keep the where-clause list implicitly
	 * AND'd, since both partition and join pruning depends on the clauses being
	 * expressed as a list.
	 */
	joinTree = makeNode(FromExpr);
	joinTree->quals = (Node *) whereClauseList;
	joinTree->fromlist = QueryFromList(rangeTableList);

	/* build the query structure for this job */
	jobQuery = makeNode(Query);
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
	jobQuery->hasAggs = contain_agg_clause((Node *) targetList);

	return jobQuery;
}


/*
 * UpdateColumnAttributes updates the column's range table reference (varno) and
 * column attribute number for the range table (varattno). The function uses the
 * newly built range table list to update the given column's attributes.
 */
static void
UpdateColumnAttributes(Var *column, List *rangeTableList, List *dependedJobList)
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
								  newRangeTableEntry, dependedJobList);
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
	Index newTableId = 0;
	Index rangeTableIndex = 1;
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		List *originalTableIdList = NIL;
		bool listMember = false;

		ExtractRangeTblExtraData(rangeTableEntry, NULL, NULL, NULL, &originalTableIdList);

		listMember = list_member_int(originalTableIdList, originalTableId);
		if (listMember)
		{
			newTableId = rangeTableIndex;
			break;
		}

		rangeTableIndex++;
	}

	return newTableId;
}


/*
 * NewColumnId determines the new columnId for the query that is currently being
 * built. In this query, the original columnId corresponds to the column in base
 * tables. When the current query is a partition job and generates intermediate
 * tables, the columns have a different order and the new columnId corresponds
 * to this order. Please note that this function assumes columnIds for depended
 * jobs have already been updated.
 */
static AttrNumber
NewColumnId(Index originalTableId, AttrNumber originalColumnId,
			RangeTblEntry *newRangeTableEntry, List *dependedJobList)
{
	AttrNumber newColumnId = 1;
	AttrNumber columnIndex = 1;

	Job *dependedJob = JobForRangeTable(dependedJobList, newRangeTableEntry);
	List *targetEntryList = dependedJob->jobQuery->targetList;

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
	Job *searchedJob = NULL;
	List *searchedTableIdList = NIL;
	CitusRTEKind rangeTableKind;

	ExtractRangeTblExtraData(rangeTableEntry, &rangeTableKind, NULL, NULL,
							 &searchedTableIdList);

	Assert(rangeTableKind == CITUS_RTE_REMOTE_QUERY);

	searchedJob = JobForTableIdList(jobList, searchedTableIdList);

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
		List *lhsDiff = NIL;
		List *rhsDiff = NIL;

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
		lhsDiff = list_difference_int(jobTableIdList, searchedTableIdList);
		rhsDiff = list_difference_int(searchedTableIdList, jobTableIdList);
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
	bool unaryNode = UnaryOperator(multiNode);
	bool binaryNode = BinaryOperator(multiNode);

	/* relation table nodes don't have any children */
	if (CitusIsA(multiNode, MultiTable))
	{
		MultiTable *multiTable = (MultiTable *) multiNode;
		if (multiTable->relationId != SUBQUERY_RELATION_ID)
		{
			return NIL;
		}
	}

	if (unaryNode)
	{
		MultiUnaryNode *unaryNode = (MultiUnaryNode *) multiNode;
		childNodeList = list_make1(unaryNode->childNode);
	}
	else if (binaryNode)
	{
		MultiBinaryNode *binaryNode = (MultiBinaryNode *) multiNode;
		childNodeList = list_make2(binaryNode->leftChildNode,
								   binaryNode->rightChildNode);
	}

	return childNodeList;
}


/*
 * UniqueJobId allocates and returns a unique jobId for the job to be executed.
 * This allocation occurs both in shared memory and in write ahead logs; writing
 * to logs avoids the risk of having jobId collisions.
 *
 * Please note that the jobId sequence wraps around after 2^32 integers. This
 * leaves the upper 32-bits to slave nodes and their jobs.
 */
static uint64
UniqueJobId(void)
{
	text *sequenceName = cstring_to_text(JOBID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Datum jobIdDatum = 0;
	int64 jobId = 0;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique jobId from sequence */
	jobIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);
	jobId = DatumGetInt64(jobIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	return jobId;
}


/* Builds a job from the given job query and depended job list. */
static Job *
BuildJob(Query *jobQuery, List *dependedJobList)
{
	Job *job = CitusMakeNode(Job);
	job->jobId = UniqueJobId();
	job->jobQuery = jobQuery;
	job->dependedJobList = dependedJobList;

	return job;
}


/*
 * BuildMapMergeJob builds a MapMerge job from the given query and depended job
 * list. The function then copies and updates the logical plan's partition
 * column, and uses the join rule type to determine the physical repartitioning
 * method to apply.
 */
static MapMergeJob *
BuildMapMergeJob(Query *jobQuery, List *dependedJobList, Var *partitionKey,
				 PartitionType partitionType, Oid baseRelationId,
				 BoundaryNodeJobType boundaryNodeJobType)
{
	MapMergeJob *mapMergeJob = NULL;
	List *rangeTableList = jobQuery->rtable;
	Var *partitionColumn = copyObject(partitionKey);

	/* update the logical partition key's table and column identifiers */
	if (boundaryNodeJobType != SUBQUERY_MAP_MERGE_JOB)
	{
		UpdateColumnAttributes(partitionColumn, rangeTableList, dependedJobList);
	}

	mapMergeJob = CitusMakeNode(MapMergeJob);
	mapMergeJob->job.jobId = UniqueJobId();
	mapMergeJob->job.jobQuery = jobQuery;
	mapMergeJob->job.dependedJobList = dependedJobList;
	mapMergeJob->partitionColumn = partitionColumn;
	mapMergeJob->sortedShardIntervalArrayLength = 0;

	/*
	 * We assume dual partition join defaults to hash partitioning, and single
	 * partition join defaults to range partitioning. In practice, the join type
	 * should have no impact on the physical repartitioning (hash/range) method.
	 * If join type is not set, this means this job represents a subquery, and
	 * uses hash partitioning.
	 */
	if (partitionType == HASH_PARTITION_TYPE)
	{
		uint32 partitionCount = HashPartitionCount();

		mapMergeJob->partitionType = HASH_PARTITION_TYPE;
		mapMergeJob->partitionCount = partitionCount;
	}
	else if (partitionType == RANGE_PARTITION_TYPE)
	{
		/* build the split point object for the table on the right-hand side */
		DistTableCacheEntry *cache = DistributedTableCacheEntry(baseRelationId);
		bool hasUninitializedShardInterval = false;
		uint32 shardCount = cache->shardIntervalArrayLength;
		ShardInterval **sortedShardIntervalArray = cache->sortedShardIntervalArray;
		char basePartitionMethod PG_USED_FOR_ASSERTS_ONLY = 0;

		hasUninitializedShardInterval = cache->hasUninitializedShardInterval;
		if (hasUninitializedShardInterval)
		{
			ereport(ERROR, (errmsg("cannot range repartition shard with "
								   "missing min/max values")));
		}

		basePartitionMethod = PartitionMethod(baseRelationId);

		/* this join-type currently doesn't work for hash partitioned tables */
		Assert(basePartitionMethod != DISTRIBUTE_BY_HASH);

		mapMergeJob->partitionType = RANGE_PARTITION_TYPE;
		mapMergeJob->partitionCount = shardCount;
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
	uint32 nodeCount = WorkerGetLiveNodeCount();
	double maxReduceTasksPerNode = MaxRunningTasksPerNode / 2.0;

	uint32 partitionCount = (uint32) rint(nodeCount * maxReduceTasksPerNode);
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
	ArrayType *splitPointObject = NULL;
	uint32 intervalIndex = 0;
	Oid typeId = InvalidOid;
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;

	/* allocate an array for shard min values */
	uint32 minDatumCount = shardIntervalCount;
	Datum *minDatumArray = palloc0(minDatumCount * sizeof(Datum));

	for (intervalIndex = 0; intervalIndex < shardIntervalCount; intervalIndex++)
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
	splitPointObject = construct_array(minDatumArray, minDatumCount, typeId,
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
BuildJobTreeTaskList(Job *jobTree)
{
	List *flattenedJobList = NIL;
	uint32 flattenedJobCount = 0;
	int32 jobIndex = 0;

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
		jobStack = list_union_ptr(jobStack, job->dependedJobList);
	}

	/*
	 * We walk the job list in reverse order to visit jobs bottom up. This way,
	 * we can create dependencies between tasks bottom up, and assign them to
	 * worker nodes accordingly.
	 */
	flattenedJobCount = (int32) list_length(flattenedJobList);
	for (jobIndex = (flattenedJobCount - 1); jobIndex >= 0; jobIndex--)
	{
		Job *job = (Job *) list_nth(flattenedJobList, jobIndex);
		List *sqlTaskList = NIL;
		List *assignedSqlTaskList = NIL;
		ListCell *assignedSqlTaskCell = NULL;

		/* create sql tasks for the job, and prune redundant data fetch tasks */
		if (job->subqueryPushdown)
		{
			sqlTaskList = SubquerySqlTaskList(job);
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
		assignedSqlTaskList = AssignTaskList(sqlTaskList);
		AssignDataFetchDependencies(assignedSqlTaskList);

		/* now assign merge task's data fetch dependencies */
		foreach(assignedSqlTaskCell, assignedSqlTaskList)
		{
			Task *assignedSqlTask = (Task *) lfirst(assignedSqlTaskCell);
			List *assignedMergeTaskList = FindDependedMergeTaskList(assignedSqlTask);

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
 * SubquerySqlTaskList creates a list of SQL tasks to execute the given subquery
 * pushdown job. For this, it gets all range tables in the subquery tree, then
 * walks over each range table in the list, gets shards for each range table,
 * and prunes unneeded shards. Then for remaining shards, fragments are created
 * and merged to create fragment combinations. For each created combination, the
 * function builds a SQL task, and appends this task to a task list.
 */
static List *
SubquerySqlTaskList(Job *job)
{
	Query *subquery = job->jobQuery;
	uint64 jobId = job->jobId;
	List *sqlTaskList = NIL;
	List *fragmentCombinationList = NIL;
	List *opExpressionList = NIL;
	List *queryList = NIL;
	List *rangeTableList = NIL;
	ListCell *fragmentCombinationCell = NULL;
	ListCell *rangeTableCell = NULL;
	ListCell *queryCell = NULL;
	Node *whereClauseTree = NULL;
	uint32 taskIdIndex = 1; /* 0 is reserved for invalid taskId */
	uint32 anchorRangeTableId = 0;
	uint32 rangeTableIndex = 0;
	const uint32 fragmentSize = sizeof(RangeTableFragment);
	uint64 largestTableSize = 0;

	/* find filters on partition columns */
	ExtractQueryWalker((Node *) subquery, &queryList);
	foreach(queryCell, queryList)
	{
		Query *query = (Query *) lfirst(queryCell);
		bool leafQuery = LeafQuery(query);

		if (!leafQuery)
		{
			continue;
		}

		/* we have some filters on partition column */
		opExpressionList = PartitionColumnOpExpressionList(query);
		if (opExpressionList != NIL)
		{
			break;
		}
	}

	/* get list of all range tables in subquery tree */
	ExtractRangeTableRelationWalker((Node *) subquery, &rangeTableList);

	/*
	 * For each range table entry, first we prune shards for the relation
	 * referenced in the range table. Then we sort remaining shards and create
	 * fragments in this order and add these fragments to fragment combination
	 * list.
	 */
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		Oid relationId = rangeTableEntry->relid;
		List *shardIntervalList = LoadShardIntervalList(relationId);
		List *finalShardIntervalList = NIL;
		ListCell *fragmentCombinationCell = NULL;
		ListCell *shardIntervalCell = NULL;
		uint32 tableId = rangeTableIndex + 1; /* tableId starts from 1 */
		uint32 finalShardCount = 0;
		uint64 tableSize = 0;

		if (opExpressionList != NIL)
		{
			Var *partitionColumn = PartitionColumn(relationId, tableId);
			List *whereClauseList = ReplaceColumnsInOpExpressionList(opExpressionList,
																	 partitionColumn);
			finalShardIntervalList = PruneShardList(relationId, tableId, whereClauseList,
													shardIntervalList);
		}
		else
		{
			finalShardIntervalList = shardIntervalList;
		}

		/* if all shards are pruned away, we return an empty task list */
		finalShardCount = list_length(finalShardIntervalList);
		if (finalShardCount == 0)
		{
			return NIL;
		}

		fragmentCombinationCell = list_head(fragmentCombinationList);

		foreach(shardIntervalCell, finalShardIntervalList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);

			RangeTableFragment *shardFragment = palloc0(fragmentSize);
			shardFragment->fragmentReference = shardInterval;
			shardFragment->fragmentType = CITUS_RTE_RELATION;
			shardFragment->rangeTableId = tableId;

			tableSize += ShardLength(shardInterval->shardId);

			if (tableId == 1)
			{
				List *fragmentCombination = list_make1(shardFragment);
				fragmentCombinationList = lappend(fragmentCombinationList,
												  fragmentCombination);
			}
			else
			{
				List *fragmentCombination = (List *) lfirst(fragmentCombinationCell);
				fragmentCombination = lappend(fragmentCombination, shardFragment);

				/* get next fragment for the first relation list */
				fragmentCombinationCell = lnext(fragmentCombinationCell);
			}
		}

		/*
		 * Determine anchor table using shards which survive pruning instead of calling
		 * AnchorRangeTableId
		 */
		if (anchorRangeTableId == 0 || tableSize > largestTableSize)
		{
			largestTableSize = tableSize;
			anchorRangeTableId = tableId;
		}

		rangeTableIndex++;
	}

	/*
	 * Ands are made implicit during shard pruning, as predicate comparison and
	 * refutation depend on it being so. We need to make them explicit again so
	 * that the query string is generated as (...) AND (...) as opposed to
	 * (...), (...).
	 */
	whereClauseTree = (Node *) make_ands_explicit((List *) subquery->jointree->quals);
	subquery->jointree->quals = whereClauseTree;

	/* create tasks from every fragment combination */
	foreach(fragmentCombinationCell, fragmentCombinationList)
	{
		List *fragmentCombination = (List *) lfirst(fragmentCombinationCell);
		List *taskRangeTableList = NIL;
		Query *taskQuery = copyObject(subquery);
		Task *sqlTask = NULL;
		StringInfo sqlQueryString = NULL;

		/* create tasks to fetch fragments required for the sql task */
		List *uniqueFragmentList = UniqueFragmentList(fragmentCombination);
		List *dataFetchTaskList = DataFetchTaskList(jobId, taskIdIndex,
													uniqueFragmentList);
		int32 dataFetchTaskCount = list_length(dataFetchTaskList);
		taskIdIndex += dataFetchTaskCount;

		ExtractRangeTableRelationWalker((Node *) taskQuery, &taskRangeTableList);
		UpdateRangeTableAlias(taskRangeTableList, fragmentCombination);

		/* transform the updated task query to a SQL query string */
		sqlQueryString = makeStringInfo();
		pg_get_query_def(taskQuery, sqlQueryString);

		sqlTask = CreateBasicTask(jobId, taskIdIndex, SQL_TASK, sqlQueryString->data);
		sqlTask->dependedTaskList = dataFetchTaskList;

		/* log the query string we generated */
		ereport(DEBUG4, (errmsg("generated sql query for job " UINT64_FORMAT
								" and task %d", sqlTask->jobId, sqlTask->taskId),
						 errdetail("query string: \"%s\"", sqlQueryString->data)));

		sqlTask->anchorShardId = AnchorShardId(fragmentCombination, anchorRangeTableId);

		taskIdIndex++;
		sqlTaskList = lappend(sqlTaskList, sqlTask);
	}

	return sqlTaskList;
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
	Node *whereClauseTree = NULL;
	List *rangeTableFragmentsList = NIL;
	List *fragmentCombinationList = NIL;
	ListCell *fragmentCombinationCell = NULL;

	Query *jobQuery = job->jobQuery;
	List *rangeTableList = jobQuery->rtable;
	List *whereClauseList = (List *) jobQuery->jointree->quals;
	List *dependedJobList = job->dependedJobList;

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
	whereClauseTree = (Node *) make_ands_explicit((List *) jobQuery->jointree->quals);
	jobQuery->jointree->quals = whereClauseTree;

	/*
	 * For each range table, we first get a list of their shards or merge tasks.
	 * We also apply partition pruning based on the selection criteria. If all
	 * range table fragments are pruned away, we return an empty task list.
	 */
	rangeTableFragmentsList = RangeTableFragmentsList(rangeTableList, whereClauseList,
													  dependedJobList);
	if (rangeTableFragmentsList == NIL)
	{
		return NIL;
	}

	/*
	 * We then generate fragment combinations according to how range tables join
	 * with each other (and apply join pruning). Each fragment combination then
	 * represents one SQL task's dependencies.
	 */
	fragmentCombinationList = FragmentCombinationList(rangeTableFragmentsList,
													  jobQuery, dependedJobList);

	fragmentCombinationCell = NULL;
	foreach(fragmentCombinationCell, fragmentCombinationList)
	{
		List *fragmentCombination = (List *) lfirst(fragmentCombinationCell);
		List *dataFetchTaskList = NIL;
		int32 dataFetchTaskCount = 0;
		StringInfo sqlQueryString = NULL;
		Task *sqlTask = NULL;
		Query *taskQuery = NULL;
		List *fragmentRangeTableList = NIL;

		/* create tasks to fetch fragments required for the sql task */
		dataFetchTaskList = DataFetchTaskList(jobId, taskIdIndex, fragmentCombination);
		dataFetchTaskCount = list_length(dataFetchTaskList);
		taskIdIndex += dataFetchTaskCount;

		/* update range table entries with fragment aliases (in place) */
		taskQuery = copyObject(jobQuery);
		fragmentRangeTableList = taskQuery->rtable;
		UpdateRangeTableAlias(fragmentRangeTableList, fragmentCombination);

		/* transform the updated task query to a SQL query string */
		sqlQueryString = makeStringInfo();
		pg_get_query_def(taskQuery, sqlQueryString);

		sqlTask = CreateBasicTask(jobId, taskIdIndex, SQL_TASK, sqlQueryString->data);
		sqlTask->dependedTaskList = dataFetchTaskList;

		/* log the query string we generated */
		ereport(DEBUG4, (errmsg("generated sql query for job " UINT64_FORMAT
								" and task %d", sqlTask->jobId, sqlTask->taskId),
						 errdetail("query string: \"%s\"", sqlQueryString->data)));

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
	List *dependedJobList = job->dependedJobList;

	uint32 dependedJobCount = (uint32) list_length(dependedJobList);
	if (dependedJobCount > 0)
	{
		Job *dependedJob = (Job *) linitial(dependedJobList);
		if (CitusIsA(dependedJob, MapMergeJob))
		{
			MapMergeJob *mapMergeJob = (MapMergeJob *) dependedJob;
			if (mapMergeJob->partitionType == HASH_PARTITION_TYPE)
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
						List *dependedJobList)
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

			List *shardIntervalList = LoadShardIntervalList(relationId);
			List *prunedShardIntervalList = PruneShardList(relationId, tableId,
														   whereClauseList,
														   shardIntervalList);

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
			MapMergeJob *dependedMapMergeJob = NULL;
			List *mergeTaskFragmentList = NIL;
			List *mergeTaskList = NIL;
			ListCell *mergeTaskCell = NULL;

			Job *dependedJob = JobForRangeTable(dependedJobList, rangeTableEntry);
			Assert(CitusIsA(dependedJob, MapMergeJob));

			dependedMapMergeJob = (MapMergeJob *) dependedJob;
			mergeTaskList = dependedMapMergeJob->mergeTaskList;

			/* if there are no tasks for the depended job, just return NIL */
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
 * PruneShardList prunes shard intervals from given list based on the selection criteria,
 * and returns remaining shard intervals in another list.
 */
List *
PruneShardList(Oid relationId, Index tableId, List *whereClauseList,
			   List *shardIntervalList)
{
	List *remainingShardList = NIL;
	ListCell *shardIntervalCell = NULL;
	List *restrictInfoList = NIL;
	Node *baseConstraint = NULL;

	Var *partitionColumn = PartitionColumn(relationId, tableId);
	char partitionMethod = PartitionMethod(relationId);

	/* build the filter clause list for the partition method */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		Node *hashedNode = HashableClauseMutator((Node *) whereClauseList,
												 partitionColumn);

		List *hashedClauseList = (List *) hashedNode;
		restrictInfoList = BuildRestrictInfoList(hashedClauseList);
	}
	else
	{
		restrictInfoList = BuildRestrictInfoList(whereClauseList);
	}

	/* override the partition column for hash partitioning */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		partitionColumn = MakeInt4Column();
	}

	/* build the base expression for constraint */
	baseConstraint = BuildBaseConstraint(partitionColumn);

	/* walk over shard list and check if shards can be pruned */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		List *constraintList = NIL;
		bool shardPruned = false;

		if (shardInterval->minValueExists && shardInterval->maxValueExists)
		{
			/* set the min/max values in the base constraint */
			UpdateConstraint(baseConstraint, shardInterval);
			constraintList = list_make1(baseConstraint);

			shardPruned = predicate_refuted_by(constraintList, restrictInfoList);
		}

		if (shardPruned)
		{
			ereport(DEBUG2, (errmsg("predicate pruning for shardId "
									UINT64_FORMAT, shardInterval->shardId)));
		}
		else
		{
			remainingShardList = lappend(remainingShardList, shardInterval);
		}
	}

	return remainingShardList;
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
	Node *baseConstraint = NULL;
	OpExpr *lessThanExpr = NULL;
	OpExpr *greaterThanExpr = NULL;

	/* Build these expressions with only one argument for now */
	lessThanExpr = MakeOpExpression(column, BTLessEqualStrategyNumber);
	greaterThanExpr = MakeOpExpression(column, BTGreaterEqualStrategyNumber);

	/* Build base constaint as an and of two qual conditions */
	baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

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

	OperatorCacheEntry *operatorCacheEntry = NULL;
	Oid accessMethodId = BTREE_AM_OID;
	Oid operatorId = InvalidOid;
	Oid operatorClassInputType = InvalidOid;
	Const *constantValue = NULL;
	OpExpr *expression = NULL;
	char typeType = 0;

	operatorCacheEntry = LookupOperatorByType(typeId, accessMethodId, strategyNumber);

	operatorId = operatorCacheEntry->operatorId;
	operatorClassInputType = operatorCacheEntry->operatorClassInputType;
	typeType = operatorCacheEntry->typeType;

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

	constantValue = makeNullConst(operatorClassInputType, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	expression = (OpExpr *) make_opclause(operatorId,
										  InvalidOid, /* no result type yet */
										  false,      /* no return set */
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
		MemoryContext oldContext = NULL;
		Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);
		Oid operatorId = InvalidOid;
		Oid operatorClassInputType = InvalidOid;
		char typeType = InvalidOid;

		if (operatorClassId == InvalidOid)
		{
			/* if operatorId is invalid, error out */
			ereport(ERROR, (errmsg("cannot find default operator class for type:%d,"
								   " access method: %d", typeId, accessMethodId)));
		}

		/* fill the other fields to the cache */
		operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);
		operatorClassInputType = get_opclass_input_type(operatorClassId);
		typeType = get_typtype(operatorClassInputType);

		/* make sure we've initialized CacheMemoryContext */
		if (CacheMemoryContext == NULL)
		{
			CreateCacheMemoryContext();
		}

		oldContext = MemoryContextSwitchTo(CacheMemoryContext);

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
 * SimpleOpExpression checks that given expression is a simple operator
 * expression. A simple operator expression is a binary operator expression with
 * operands of a var and a non-null constant.
 */
bool
SimpleOpExpression(Expr *clause)
{
	Node *leftOperand = NULL;
	Node *rightOperand = NULL;
	Const *constantClause = NULL;

	if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2)
	{
		leftOperand = get_leftop(clause);
		rightOperand = get_rightop(clause);
	}
	else
	{
		return false; /* not a binary opclause */
	}

	/* strip coercions before doing check */
	leftOperand = strip_implicit_coercions(leftOperand);
	rightOperand = strip_implicit_coercions(rightOperand);

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
 * HashableClauseMutator walks over the original where clause list, replaces
 * hashable nodes with hashed versions and keeps other nodes as they are.
 */
static Node *
HashableClauseMutator(Node *originalNode, Var *partitionColumn)
{
	Node *newNode = NULL;
	if (originalNode == NULL)
	{
		return NULL;
	}

	if (IsA(originalNode, OpExpr))
	{
		OpExpr *operatorExpression = (OpExpr *) originalNode;
		bool hasPartitionColumn = false;

		Oid leftHashFunction = InvalidOid;
		Oid rightHashFunction = InvalidOid;

		/*
		 * If operatorExpression->opno is NOT the registered '=' operator for
		 * any hash opfamilies, then get_op_hash_functions will return false.
		 * This means this function both ensures a hash function exists for the
		 * types in question AND filters out any clauses lacking equality ops.
		 */
		bool hasHashFunction = get_op_hash_functions(operatorExpression->opno,
													 &leftHashFunction,
													 &rightHashFunction);

		bool simpleOpExpression = SimpleOpExpression((Expr *) operatorExpression);
		if (simpleOpExpression)
		{
			hasPartitionColumn = OpExpressionContainsColumn(operatorExpression,
															partitionColumn);
		}

		if (hasHashFunction && hasPartitionColumn)
		{
			OpExpr *hashedOperatorExpression =
				MakeHashedOperatorExpression((OpExpr *) originalNode);
			newNode = (Node *) hashedOperatorExpression;
		}
	}
	else if (IsA(originalNode, NullTest))
	{
		NullTest *nullTest = (NullTest *) originalNode;
		Var *column = NULL;

		Expr *nullTestOperand = nullTest->arg;
		if (IsA(nullTestOperand, Var))
		{
			column = (Var *) nullTestOperand;
		}

		if ((column != NULL) && equal(column, partitionColumn) &&
			(nullTest->nulltesttype == IS_NULL))
		{
			OpExpr *opExpressionWithZeroConst = MakeOpExpressionWithZeroConst();
			newNode = (Node *) opExpressionWithZeroConst;
		}
	}
	else if (IsA(originalNode, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *arrayOperatorExpression = (ScalarArrayOpExpr *) originalNode;
		Node *leftOpExpression = linitial(arrayOperatorExpression->args);
		Node *strippedLeftOpExpression = strip_implicit_coercions(leftOpExpression);
		bool usingEqualityOperator = OperatorImplementsEquality(
			arrayOperatorExpression->opno);

		/*
		 * Citus cannot prune hash-distributed shards with ANY/ALL. We show a NOTICE
		 * if the expression is ANY/ALL performed on the partition column with equality.
		 */
		if (usingEqualityOperator && strippedLeftOpExpression != NULL &&
			equal(strippedLeftOpExpression, partitionColumn))
		{
			ereport(NOTICE, (errmsg("cannot use shard pruning with "
									"ANY/ALL (array expression)"),
							 errhint("Consider rewriting the expression with "
									 "OR/AND clauses.")));
		}
	}

	/*
	 * If this node is not hashable, continue walking down the expression tree
	 * to find and hash clauses which are eligible.
	 */
	if (newNode == NULL)
	{
		newNode = expression_tree_mutator(originalNode, HashableClauseMutator,
										  (void *) partitionColumn);
	}

	return newNode;
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
	Node *leftOperand = get_leftop((Expr *) operatorExpression);
	Node *rightOperand = get_rightop((Expr *) operatorExpression);
	Var *column = NULL;

	/* strip coercions before doing check */
	leftOperand = strip_implicit_coercions(leftOperand);
	rightOperand = strip_implicit_coercions(rightOperand);

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
 * MakeHashedOperatorExpression creates a new operator expression with a column
 * of int4 type and hashed constant value.
 */
static OpExpr *
MakeHashedOperatorExpression(OpExpr *operatorExpression)
{
	const Oid hashResultTypeId = INT4OID;
	TypeCacheEntry *hashResultTypeEntry = NULL;
	Oid operatorId = InvalidOid;
	OpExpr *hashedExpression = NULL;
	Var *hashedColumn = NULL;
	Datum hashedValue = 0;
	Const *hashedConstant = NULL;
	FmgrInfo *hashFunction = NULL;
	TypeCacheEntry *typeEntry = NULL;

	Node *leftOperand = get_leftop((Expr *) operatorExpression);
	Node *rightOperand = get_rightop((Expr *) operatorExpression);
	Const *constant = NULL;

	if (IsA(rightOperand, Const))
	{
		constant = (Const *) rightOperand;
	}
	else
	{
		constant = (Const *) leftOperand;
	}

	/* Load the operator from type cache */
	hashResultTypeEntry = lookup_type_cache(hashResultTypeId, TYPECACHE_EQ_OPR);
	operatorId = hashResultTypeEntry->eq_opr;

	/* Get a column with int4 type */
	hashedColumn = MakeInt4Column();

	/* Load the hash function from type cache */
	typeEntry = lookup_type_cache(constant->consttype, TYPECACHE_HASH_PROC_FINFO);
	hashFunction = &(typeEntry->hash_proc_finfo);
	if (!OidIsValid(hashFunction->fn_oid))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify a hash function for type %s",
							   format_type_be(constant->consttype)),
						errdatatype(constant->consttype)));
	}

	/*
	 * Note that any changes to PostgreSQL's hashing functions will change the
	 * new value created by this function.
	 */
	hashedValue = FunctionCall1(hashFunction, constant->constvalue);
	hashedConstant = MakeInt4Constant(hashedValue);

	/* Now create the expression with modified partition column and hashed constant */
	hashedExpression = (OpExpr *) make_opclause(operatorId,
												InvalidOid, /* no result type yet */
												false,    /* no return set */
												(Expr *) hashedColumn,
												(Expr *) hashedConstant,
												InvalidOid, InvalidOid);

	/* Set implementing function id and result type */
	hashedExpression->opfuncid = get_opcode(operatorId);
	hashedExpression->opresulttype = get_func_rettype(hashedExpression->opfuncid);

	return hashedExpression;
}


/*
 * MakeInt4Column creates a column of int4 type with invalid table id and max
 * attribute number.
 */
static Var *
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
static Const *
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


/*
 * MakeOpExpressionWithZeroConst creates a new operator expression with equality
 * check to zero and returns it.
 */
static OpExpr *
MakeOpExpressionWithZeroConst()
{
	Var *int4Column = MakeInt4Column();
	OpExpr *operatorExpression = MakeOpExpression(int4Column, BTEqualStrategyNumber);
	Const *constant = (Const *) get_rightop((Expr *) operatorExpression);
	constant->constvalue = Int32GetDatum(0);
	constant->constisnull = false;

	return operatorExpression;
}


/*
 * BuildRestrictInfoList builds restrict info list using the selection criteria,
 * and then return this list. Note that this function assumes there is only one
 * relation for now.
 */
static List *
BuildRestrictInfoList(List *qualList)
{
	List *restrictInfoList = NIL;
	ListCell *qualCell = NULL;

	foreach(qualCell, qualList)
	{
		RestrictInfo *restrictInfo = NULL;
		Node *qualNode = (Node *) lfirst(qualCell);

		restrictInfo = make_simple_restrictinfo((Expr *) qualNode);
		restrictInfoList = lappend(restrictInfoList, restrictInfo);
	}

	return restrictInfoList;
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
	Const *minConstant = NULL;
	Const *maxConstant = NULL;

	Assert(shardInterval != NULL);
	Assert(shardInterval->minValueExists);
	Assert(shardInterval->maxValueExists);
	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	minConstant = (Const *) minNode;
	maxConstant = (Const *) maxNode;

	minConstant->constvalue = shardInterval->minValue;
	maxConstant->constvalue = shardInterval->maxValue;

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
						List *dependedJobList)
{
	List *fragmentCombinationList = NIL;
	JoinSequenceNode *joinSequenceArray = NULL;
	List *fragmentCombinationQueue = NIL;
	List *emptyList = NIL;

	/* find a sequence that joins the range tables in the list */
	joinSequenceArray = JoinSequenceArray(rangeTableFragmentsList, jobQuery,
										  dependedJobList);

	/*
	 * We use breadth-first search with pruning to create fragment combinations.
	 * For this, we first queue the root node (an empty combination), and then
	 * start traversing our search space.
	 */
	fragmentCombinationQueue = lappend(fragmentCombinationQueue, emptyList);
	while (fragmentCombinationQueue != NIL)
	{
		List *fragmentCombination = NIL;
		int32 joinSequenceIndex = 0;
		uint32 tableId = 0;
		List *tableFragments = NIL;
		ListCell *tableFragmentCell = NULL;
		int32 joiningTableId = NON_PRUNABLE_JOIN;
		int32 joiningTableSequenceIndex = -1;
		int32 rangeTableCount = 0;

		/* pop first element from the fragment queue */
		fragmentCombination = linitial(fragmentCombinationQueue);
		fragmentCombinationQueue = list_delete_first(fragmentCombinationQueue);

		/*
		 * If this combination covered all range tables in a join sequence, add
		 * this combination to our result set.
		 */
		joinSequenceIndex = list_length(fragmentCombination);
		rangeTableCount = list_length(rangeTableFragmentsList);
		if (joinSequenceIndex == rangeTableCount)
		{
			fragmentCombinationList = lappend(fragmentCombinationList,
											  fragmentCombination);
			continue;
		}

		/* find the next range table to add to our search space */
		tableId = joinSequenceArray[joinSequenceIndex].rangeTableId;
		tableFragments = FindRangeTableFragmentsList(rangeTableFragmentsList, tableId);

		/* resolve sequence index for the previous range table we join against */
		joiningTableId = joinSequenceArray[joinSequenceIndex].joiningRangeTableId;
		if (joiningTableId != NON_PRUNABLE_JOIN)
		{
			int32 sequenceIndex = 0;
			for (sequenceIndex = 0; sequenceIndex < rangeTableCount; sequenceIndex++)
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
 * JoinSequenceArray walks over the join nodes in the job query and constructs a join
 * sequence containing an entry for each joined table. The function then returns an
 * array of join sequence nodes, in which each node contains the id of a table in the
 * range table list and the id of a preceding table with which it is joined, if any.
 */
static JoinSequenceNode *
JoinSequenceArray(List *rangeTableFragmentsList, Query *jobQuery, List *dependedJobList)
{
	List *rangeTableList = jobQuery->rtable;
	uint32 rangeTableCount = (uint32) list_length(rangeTableList);
	uint32 sequenceNodeSize = sizeof(JoinSequenceNode);
	uint32 joinedTableCount = 0;
	List *joinedTableList = NIL;
	List *joinExprList = NIL;
	ListCell *joinExprCell = NULL;
	uint32 firstRangeTableId = 1;
	JoinSequenceNode *joinSequenceArray = palloc0(rangeTableCount * sequenceNodeSize);

	joinExprList = JoinExprList(jobQuery->jointree);

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
		JoinType joinType = joinExpr->jointype;
		RangeTblRef *rightTableRef = (RangeTblRef *) joinExpr->rarg;
		JoinSequenceNode *nextJoinSequenceNode = NULL;
		uint32 nextRangeTableId = rightTableRef->rtindex;
		ListCell *nextJoinClauseCell = NULL;
		Index existingRangeTableId = 0;
		bool applyJoinPruning = false;

		List *nextJoinClauseList = make_ands_implicit((Expr *) joinExpr->quals);

		/*
		 * If next join clause list is empty, the user tried a cartesian product
		 * between tables. We don't support this functionality, and error out.
		 */
		if (nextJoinClauseList == NIL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning on this query"),
							errdetail("Cartesian products are currently unsupported")));
		}

		/*
		 * We now determine if we can apply join pruning between existing range
		 * tables and this new one.
		 */
		foreach(nextJoinClauseCell, nextJoinClauseList)
		{
			OpExpr *nextJoinClause = (OpExpr *) lfirst(nextJoinClauseCell);
			Var *leftColumn = NULL;
			Var *rightColumn = NULL;
			Index leftRangeTableId = 0;
			Index rightRangeTableId = 0;
			bool leftPartitioned = false;
			bool rightPartitioned = false;

			if (!IsJoinClause((Node *) nextJoinClause))
			{
				continue;
			}

			leftColumn = LeftColumn(nextJoinClause);
			rightColumn = RightColumn(nextJoinClause);
			leftRangeTableId = leftColumn->varno;
			rightRangeTableId = rightColumn->varno;

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

			/*
			 * Check if this is a broadcast outer join, meaning the inner table has only
			 * 1 shard.
			 *
			 * Broadcast outer join is a special case. In a left join, we want to join
			 * every fragment on the left with the one fragment on the right to ensure
			 * that all results from the left are included. As an optimization, we could
			 * perform these joins with any empty set instead of an actual fragment, but
			 * in any case they must not be pruned.
			 */
			if (IS_OUTER_JOIN(joinType))
			{
				int innerRangeTableId = 0;
				List *tableFragments = NIL;
				int fragmentCount = 0;

				if (joinType == JOIN_RIGHT)
				{
					innerRangeTableId = existingRangeTableId;
				}
				else
				{
					/*
					 * Note: For a full join the logical planner ensures a 1-1 mapping,
					 * thus it is sufficient to check one side.
					 */
					innerRangeTableId = nextRangeTableId;
				}

				tableFragments = FindRangeTableFragmentsList(rangeTableFragmentsList,
															 innerRangeTableId);
				fragmentCount = list_length(tableFragments);
				if (fragmentCount == 1)
				{
					continue;
				}
			}

			leftPartitioned = PartitionedOnColumn(leftColumn, rangeTableList,
												  dependedJobList);
			rightPartitioned = PartitionedOnColumn(rightColumn, rangeTableList,
												   dependedJobList);
			if (leftPartitioned && rightPartitioned)
			{
				/* make sure this join clause references only simple columns */
				CheckJoinBetweenColumns(nextJoinClause);

				applyJoinPruning = true;
				break;
			}
		}

		/* set next joining range table's info in the join sequence */
		nextJoinSequenceNode = &joinSequenceArray[joinedTableCount];
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

		joinedTableList = lappend_int(joinedTableList, nextRangeTableId);
		joinedTableCount++;
	}

	return joinSequenceArray;
}


/*
 * PartitionedOnColumn finds the given column's range table entry, and checks if
 * that range table is partitioned on the given column.
 */
static bool
PartitionedOnColumn(Var *column, List *rangeTableList, List *dependedJobList)
{
	bool partitionedOnColumn = false;
	Index rangeTableId = column->varno;
	RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableId, rangeTableList);

	CitusRTEKind rangeTableType = GetRangeTblKind(rangeTableEntry);
	if (rangeTableType == CITUS_RTE_RELATION)
	{
		Oid relationId = rangeTableEntry->relid;
		Var *partitionColumn = PartitionColumn(relationId, rangeTableId);

		if (partitionColumn->varattno == column->varattno)
		{
			partitionedOnColumn = true;
		}
	}
	else if (rangeTableType == CITUS_RTE_REMOTE_QUERY)
	{
		Job *job = JobForRangeTable(dependedJobList, rangeTableEntry);
		MapMergeJob *mapMergeJob = (MapMergeJob *) job;
		Var *partitionColumn = NULL;
		Var *remoteRelationColumn = NULL;
		TargetEntry *targetEntry = NULL;

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

		targetEntry = (TargetEntry *) list_nth(targetEntryList, columnIndex);
		remoteRelationColumn = (Var *) targetEntry->expr;
		Assert(IsA(remoteRelationColumn, Var));

		/* retrieve the partition column for the job */
		partitionColumn = mapMergeJob->partitionColumn;
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
	bool joinPrunable = false;
	bool overlap = false;
	ShardInterval *leftFragmentInterval = NULL;
	ShardInterval *rightFragmentInterval = NULL;

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
	 * We have a range (re)partition join. We now get shard intervals for both
	 * fragments, and then check if these intervals overlap.
	 */
	leftFragmentInterval = FragmentInterval(leftFragment);
	rightFragmentInterval = FragmentInterval(rightFragment);

	overlap = ShardIntervalsOverlap(leftFragmentInterval, rightFragmentInterval);
	if (!overlap)
	{
		if (log_min_messages <= DEBUG2 || client_min_messages <= DEBUG2)
		{
			StringInfo leftString = FragmentIntervalString(leftFragmentInterval);
			StringInfo rightString = FragmentIntervalString(rightFragmentInterval);

			ereport(DEBUG2, (errmsg("join prunable for intervals %s and %s",
									leftString->data, rightString->data)));
		}

		joinPrunable = true;
	}

	return joinPrunable;
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
		Task *mergeTask = NULL;

		Assert(CitusIsA(fragment->fragmentReference, Task));

		mergeTask = (Task *) fragment->fragmentReference;
		fragmentInterval = mergeTask->shardInterval;
	}

	return fragmentInterval;
}


/* Checks if the given shard intervals have overlapping ranges. */
bool
ShardIntervalsOverlap(ShardInterval *firstInterval, ShardInterval *secondInterval)
{
	bool nonOverlap = false;
	DistTableCacheEntry *intervalRelation =
		DistributedTableCacheEntry(firstInterval->relationId);
	FmgrInfo *comparisonFunction = intervalRelation->shardIntervalCompareFunction;

	Datum firstMin = 0;
	Datum firstMax = 0;
	Datum secondMin = 0;
	Datum secondMax = 0;


	firstMin = firstInterval->minValue;
	firstMax = firstInterval->maxValue;
	secondMin = secondInterval->minValue;
	secondMax = secondInterval->maxValue;

	/*
	 * We need to have min/max values for both intervals first. Then, we assume
	 * two intervals i1 = [min1, max1] and i2 = [min2, max2] do not overlap if
	 * (max1 < min2) or (max2 < min1). For details, please see the explanation
	 * on overlapping intervals at http://www.rgrjr.com/emacs/overlap.html.
	 */
	if (firstInterval->minValueExists && firstInterval->maxValueExists &&
		secondInterval->minValueExists && secondInterval->maxValueExists)
	{
		Datum firstDatum = CompareCall2(comparisonFunction, firstMax, secondMin);
		Datum secondDatum = CompareCall2(comparisonFunction, secondMax, firstMin);
		int firstComparison = DatumGetInt32(firstDatum);
		int secondComparison = DatumGetInt32(secondDatum);

		if (firstComparison < 0 || secondComparison < 0)
		{
			nonOverlap = true;
		}
	}

	return (!nonOverlap);
}


/*
 * FragmentIntervalString takes the given fragment interval, and converts this
 * interval into its string representation for use in debug messages.
 */
static StringInfo
FragmentIntervalString(ShardInterval *fragmentInterval)
{
	StringInfo fragmentIntervalString = NULL;
	Oid typeId = fragmentInterval->valueTypeId;
	Oid outputFunctionId = InvalidOid;
	bool typeVariableLength = false;
	FmgrInfo *outputFunction = NULL;
	char *minValueString = NULL;
	char *maxValueString = NULL;

	Assert(fragmentInterval->minValueExists);
	Assert(fragmentInterval->maxValueExists);

	outputFunction = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	getTypeOutputInfo(typeId, &outputFunctionId, &typeVariableLength);
	fmgr_info(outputFunctionId, outputFunction);

	minValueString = OutputFunctionCall(outputFunction, fragmentInterval->minValue);
	maxValueString = OutputFunctionCall(outputFunction, fragmentInterval->maxValue);

	fragmentIntervalString = makeStringInfo();
	appendStringInfo(fragmentIntervalString, "[%s,%s]", minValueString, maxValueString);

	return fragmentIntervalString;
}


/*
 * UniqueFragmentList walks over the given relation fragment list, compares
 * shard ids, eliminate duplicates and returns a new fragment list of unique
 * shard ids. Note that this is a helper function for subquery pushdown, and it
 * is used to prevent creating multiple data fetch tasks for same shards.
 */
static List *
UniqueFragmentList(List *fragmentList)
{
	List *uniqueFragmentList = NIL;
	ListCell *fragmentCell = NULL;

	foreach(fragmentCell, fragmentList)
	{
		ShardInterval *shardInterval = NULL;
		bool shardIdAlreadyAdded = false;
		ListCell *uniqueFragmentCell = NULL;

		RangeTableFragment *fragment = (RangeTableFragment *) lfirst(fragmentCell);
		Assert(fragment->fragmentType == CITUS_RTE_RELATION);

		Assert(CitusIsA(fragment->fragmentReference, ShardInterval));
		shardInterval = (ShardInterval *) fragment->fragmentReference;

		foreach(uniqueFragmentCell, uniqueFragmentList)
		{
			RangeTableFragment *uniqueFragment =
				(RangeTableFragment *) lfirst(uniqueFragmentCell);
			ShardInterval *uniqueShardInterval =
				(ShardInterval *) uniqueFragment->fragmentReference;

			if (shardInterval->shardId == uniqueShardInterval->shardId)
			{
				shardIdAlreadyAdded = true;
				break;
			}
		}

		if (!shardIdAlreadyAdded)
		{
			uniqueFragmentList = lappend(uniqueFragmentList, fragment);
		}
	}

	return uniqueFragmentList;
}


/*
 * DataFetchTaskList builds a data fetch task for every shard in the given shard
 * list, appends these data fetch tasks into a list, and returns this list.
 */
static List *
DataFetchTaskList(uint64 jobId, uint32 taskIdIndex, List *fragmentList)
{
	List *dataFetchTaskList = NIL;
	ListCell *fragmentCell = NULL;

	foreach(fragmentCell, fragmentList)
	{
		RangeTableFragment *fragment = (RangeTableFragment *) lfirst(fragmentCell);
		if (fragment->fragmentType == CITUS_RTE_RELATION)
		{
			ShardInterval *shardInterval = fragment->fragmentReference;
			uint64 shardId = shardInterval->shardId;
			StringInfo shardFetchQueryString = ShardFetchQueryString(shardId);

			Task *shardFetchTask = CreateBasicTask(jobId, taskIdIndex, SHARD_FETCH_TASK,
												   shardFetchQueryString->data);
			shardFetchTask->shardId = shardId;

			dataFetchTaskList = lappend(dataFetchTaskList, shardFetchTask);
			taskIdIndex++;
		}
		else if (fragment->fragmentType == CITUS_RTE_REMOTE_QUERY)
		{
			Task *mergeTask = (Task *) fragment->fragmentReference;
			char *undefinedQueryString = NULL;

			/* create merge fetch task and have it depend on the merge task */
			Task *mergeFetchTask = CreateBasicTask(jobId, taskIdIndex, MERGE_FETCH_TASK,
												   undefinedQueryString);
			mergeFetchTask->dependedTaskList = list_make1(mergeTask);

			dataFetchTaskList = lappend(dataFetchTaskList, mergeFetchTask);
			taskIdIndex++;
		}
	}

	return dataFetchTaskList;
}


/*
 * ShardFetchQueryString constructs a query string to fetch the given shard from
 * the shards' placements.
 */
StringInfo
ShardFetchQueryString(uint64 shardId)
{
	StringInfo shardFetchQuery = NULL;
	uint64 shardLength = ShardLength(shardId);

	/* construct two array strings for node names and port numbers */
	List *shardPlacements = FinalizedShardPlacementList(shardId);
	StringInfo nodeNameArrayString = NodeNameArrayString(shardPlacements);
	StringInfo nodePortArrayString = NodePortArrayString(shardPlacements);

	/* check storage type to create the correct query string */
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	char storageType = shardInterval->storageType;
	char *shardTableName = NULL;

	/*
	 * If user specified a shard alias in pg_dist_shard, error out and display a
	 * message explaining the limitation.
	 */
	char *shardAliasName = LoadShardAlias(shardInterval->relationId, shardId);
	if (shardAliasName != NULL)
	{
		ereport(ERROR, (errmsg("cannot fetch shard " UINT64_FORMAT, shardId),
						errdetail("Fetching shards with aliases is currently "
								  "unsupported")));
	}
	else
	{
		/* construct the shard name */
		char *tableName = get_rel_name(shardInterval->relationId);
		shardTableName = pstrdup(tableName);
		AppendShardIdToName(&shardTableName, shardId);
	}

	shardFetchQuery = makeStringInfo();
	if (storageType == SHARD_STORAGE_TABLE || storageType == SHARD_STORAGE_RELAY ||
		storageType == SHARD_STORAGE_COLUMNAR)
	{
		appendStringInfo(shardFetchQuery, TABLE_FETCH_COMMAND,
						 shardTableName, shardLength,
						 nodeNameArrayString->data, nodePortArrayString->data);
	}
	else if (storageType == SHARD_STORAGE_FOREIGN)
	{
		appendStringInfo(shardFetchQuery, FOREIGN_FETCH_COMMAND,
						 shardTableName, shardLength,
						 nodeNameArrayString->data, nodePortArrayString->data);
	}

	return shardFetchQuery;
}


/*
 * NodeNameArrayString extracts the node names from the given node list, stores
 * these node names in an array, and returns the array's string representation.
 */
static StringInfo
NodeNameArrayString(List *shardPlacementList)
{
	StringInfo nodeNameArrayString = NULL;
	ListCell *shardPlacementCell = NULL;

	uint32 nodeNameCount = (uint32) list_length(shardPlacementList);
	Datum *nodeNameArray = palloc0(nodeNameCount * sizeof(Datum));
	uint32 nodeNameIndex = 0;

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		Datum nodeName = CStringGetDatum(shardPlacement->nodeName);

		nodeNameArray[nodeNameIndex] = nodeName;
		nodeNameIndex++;
	}

	nodeNameArrayString = DatumArrayString(nodeNameArray, nodeNameCount, CSTRINGOID);

	return nodeNameArrayString;
}


/*
 * NodePortArrayString extracts the node ports from the given node list, stores
 * these node ports in an array, and returns the array's string representation.
 */
static StringInfo
NodePortArrayString(List *shardPlacementList)
{
	StringInfo nodePortArrayString = NULL;
	ListCell *shardPlacementCell = NULL;

	uint32 nodePortCount = (uint32) list_length(shardPlacementList);
	Datum *nodePortArray = palloc0(nodePortCount * sizeof(Datum));
	uint32 nodePortIndex = 0;

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		Datum nodePort = UInt32GetDatum(shardPlacement->nodePort);

		nodePortArray[nodePortIndex] = nodePort;
		nodePortIndex++;
	}

	nodePortArrayString = DatumArrayString(nodePortArray, nodePortCount, INT4OID);

	return nodePortArrayString;
}


/* Helper function to return a datum array's external string representation. */
static StringInfo
DatumArrayString(Datum *datumArray, uint32 datumCount, Oid datumTypeId)
{
	StringInfo arrayStringInfo = NULL;
	FmgrInfo *arrayOutFunction = NULL;
	ArrayType *arrayObject = NULL;
	Datum arrayObjectDatum = 0;
	Datum arrayStringDatum = 0;
	char *arrayString = NULL;
	int16 typeLength = 0;
	bool typeByValue = false;
	char typeAlignment = 0;

	/* construct the array object from the given array */
	get_typlenbyvalalign(datumTypeId, &typeLength, &typeByValue, &typeAlignment);
	arrayObject = construct_array(datumArray, datumCount, datumTypeId,
								  typeLength, typeByValue, typeAlignment);
	arrayObjectDatum = PointerGetDatum(arrayObject);

	/* convert the array object to its string representation */
	arrayOutFunction = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	fmgr_info(ARRAY_OUT_FUNC_ID, arrayOutFunction);

	arrayStringDatum = FunctionCall1(arrayOutFunction, arrayObjectDatum);
	arrayString = DatumGetCString(arrayStringDatum);

	arrayStringInfo = makeStringInfo();
	appendStringInfo(arrayStringInfo, "%s", arrayString);

	return arrayStringInfo;
}


/*
 * CreateBasicTask creates a task, initializes fields that are common to each task,
 * and returns the created task.
 */
static Task *
CreateBasicTask(uint64 jobId, uint32 taskId, TaskType taskType, char *queryString)
{
	Task *task = CitusMakeNode(Task);
	task->jobId = jobId;
	task->taskId = taskId;
	task->taskType = taskType;
	task->queryString = queryString;

	return task;
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
	Alias *alias = NULL;
	char *aliasName = NULL;
	char *schemaName = NULL;
	char *fragmentName = NULL;

	CitusRTEKind fragmentType = fragment->fragmentType;
	if (fragmentType == CITUS_RTE_RELATION)
	{
		char *shardAliasName = NULL;
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
		 * If user specified a shard name in pg_dist_shard, use that name in alias.
		 * Otherwise, set shard name in alias to <relation_name>_<shard_id>.
		 */
		shardAliasName = LoadShardAlias(relationId, shardId);
		if (shardAliasName != NULL)
		{
			fragmentName = shardAliasName;
		}
		else
		{
			char *shardName = pstrdup(relationName);
			AppendShardIdToName(&shardName, shardId);

			fragmentName = shardName;
		}
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
	alias = rangeTableEntry->alias;
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
			ShardInterval *shardInterval = NULL;

			Assert(fragment->fragmentType == CITUS_RTE_RELATION);
			Assert(CitusIsA(fragment->fragmentReference, ShardInterval));

			shardInterval = (ShardInterval *) fragment->fragmentReference;
			anchorShardId = shardInterval->shardId;
			break;
		}
	}

	Assert(anchorShardId != INVALID_SHARD_ID);
	return anchorShardId;
}


/*
 * PruneSqlTaskDependencies iterates over each sql task from the given sql task
 * list, and prunes away any data fetch tasks which are redundant or not needed
 * for the completion of that task. Specifically the function prunes away data
 * fetch tasks for the anchor shard and any merge-fetch tasks, as the task
 * assignment algorithm ensures co-location of these tasks.
 */
static List *
PruneSqlTaskDependencies(List *sqlTaskList)
{
	ListCell *sqlTaskCell = NULL;
	foreach(sqlTaskCell, sqlTaskList)
	{
		Task *sqlTask = (Task *) lfirst(sqlTaskCell);
		List *dependedTaskList = sqlTask->dependedTaskList;
		List *prunedDependedTaskList = NIL;

		ListCell *dependedTaskCell = NULL;
		foreach(dependedTaskCell, dependedTaskList)
		{
			Task *dataFetchTask = (Task *) lfirst(dependedTaskCell);

			/*
			 * If we have a shard fetch task for the anchor shard, or if we have
			 * a merge fetch task, our task assignment algorithm makes sure that
			 * the sql task is colocated with the anchor shard / merge task. We
			 * can therefore prune out this data fetch task.
			 */
			if (dataFetchTask->taskType == SHARD_FETCH_TASK &&
				dataFetchTask->shardId != sqlTask->anchorShardId)
			{
				prunedDependedTaskList = lappend(prunedDependedTaskList, dataFetchTask);
			}
			else if (dataFetchTask->taskType == MERGE_FETCH_TASK)
			{
				Task *mergeTaskReference = NULL;
				List *mergeFetchDependencyList = dataFetchTask->dependedTaskList;
				Assert(list_length(mergeFetchDependencyList) == 1);

				mergeTaskReference = (Task *) linitial(mergeFetchDependencyList);
				prunedDependedTaskList = lappend(prunedDependedTaskList,
												 mergeTaskReference);

				ereport(DEBUG2, (errmsg("pruning merge fetch taskId %d",
										dataFetchTask->taskId),
								 errdetail("Creating dependency on merge taskId %d",
										   mergeTaskReference->taskId)));
			}
		}

		sqlTask->dependedTaskList = prunedDependedTaskList;
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
	Oid partitionColumnType = partitionColumn->vartype;
	int32 partitionColumnTypeMod = partitionColumn->vartypmod;
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
		partitionColumnName = ColumnName(partitionColumn, rangeTableList);
	}

	foreach(filterTaskCell, filterTaskList)
	{
		Task *filterTask = (Task *) lfirst(filterTaskCell);
		uint64 jobId = filterTask->jobId;
		uint32 taskId = filterTask->taskId;
		Task *mapTask = NULL;

		/* wrap repartition query string around filter query string */
		StringInfo mapQueryString = makeStringInfo();
		char *filterQueryString = filterTask->queryString;
		char *filterQueryEscapedText = quote_literal_cstr(filterQueryString);

		PartitionType partitionType = mapMergeJob->partitionType;
		if (partitionType == RANGE_PARTITION_TYPE)
		{
			ShardInterval **intervalArray = mapMergeJob->sortedShardIntervalArray;
			uint32 intervalCount = mapMergeJob->partitionCount;

			ArrayType *splitPointObject = SplitPointObject(intervalArray, intervalCount);
			StringInfo splitPointString = SplitPointArrayString(splitPointObject,
																partitionColumnType,
																partitionColumnTypeMod);

			appendStringInfo(mapQueryString, RANGE_PARTITION_COMMAND, jobId, taskId,
							 filterQueryEscapedText, partitionColumnName,
							 partitionColumnType, splitPointString->data);
		}
		else
		{
			uint32 partitionCount = mapMergeJob->partitionCount;

			appendStringInfo(mapQueryString, HASH_PARTITION_COMMAND, jobId, taskId,
							 filterQueryEscapedText, partitionColumnName,
							 partitionColumnType, partitionCount);
		}

		/* convert filter query task into map task */
		mapTask = filterTask;
		mapTask->queryString = mapQueryString->data;
		mapTask->taskType = MAP_TASK;

		mapTaskList = lappend(mapTaskList, mapTask);
	}

	return mapTaskList;
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
		columnName = get_attname(relationId, columnNumber);
	}

	Assert(columnName != NULL);
	return columnName;
}


/*
 * SplitPointArrayString takes the array representation of the given split point
 * object, and converts this array (and array's typed elements) to their string
 * representations.
 */
static StringInfo
SplitPointArrayString(ArrayType *splitPointObject, Oid columnType, int32 columnTypeMod)
{
	StringInfo splitPointArrayString = NULL;
	Datum splitPointDatum = PointerGetDatum(splitPointObject);
	Oid outputFunctionId = InvalidOid;
	bool typeVariableLength = false;
	FmgrInfo *arrayOutFunction = NULL;
	char *arrayOutputText = NULL;
	char *arrayOutputEscapedText = NULL;
	char *arrayOutTypeName = NULL;

	Oid arrayOutType = get_array_type(columnType);
	if (arrayOutType == InvalidOid)
	{
		char *columnTypeName = format_type_be(columnType);
		ereport(ERROR, (errmsg("cannot range repartition table on column type %s",
							   columnTypeName)));
	}

	arrayOutFunction = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	getTypeOutputInfo(arrayOutType, &outputFunctionId, &typeVariableLength);
	fmgr_info(outputFunctionId, arrayOutFunction);

	arrayOutputText = OutputFunctionCall(arrayOutFunction, splitPointDatum);
	arrayOutputEscapedText = quote_literal_cstr(arrayOutputText);

	/* add an explicit cast to array's string representation */
	arrayOutTypeName = format_type_with_typemod(arrayOutType, columnTypeMod);

	splitPointArrayString = makeStringInfo();
	appendStringInfo(splitPointArrayString, "%s::%s",
					 arrayOutputEscapedText, arrayOutTypeName);

	return splitPointArrayString;
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
	uint32 partitionId = 0;
	uint32 initialPartitionId = 0;

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
	initialPartitionId = 0;
	if (mapMergeJob->partitionType == RANGE_PARTITION_TYPE)
	{
		initialPartitionId = 1;
		partitionCount = partitionCount + 1;
	}

	/* build merge tasks and their associated "map output fetch" tasks */
	for (partitionId = initialPartitionId; partitionId < partitionCount; partitionId++)
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

			/* we need node names for the query, and we'll resolve them later */
			char *undefinedQueryString = NULL;
			Task *mapOutputFetchTask = CreateBasicTask(jobId, taskIdIndex,
													   MAP_OUTPUT_FETCH_TASK,
													   undefinedQueryString);
			mapOutputFetchTask->partitionId = partitionId;
			mapOutputFetchTask->upstreamTaskId = mergeTaskId;
			mapOutputFetchTask->dependedTaskList = list_make1(mapTask);
			taskIdIndex++;

			mapOutputFetchTaskList = lappend(mapOutputFetchTaskList, mapOutputFetchTask);
		}

		/* merge task depends on completion of fetch tasks */
		mergeTask->dependedTaskList = mapOutputFetchTaskList;

		/* if range repartitioned, each merge task represents an interval */
		if (mapMergeJob->partitionType == RANGE_PARTITION_TYPE)
		{
			int32 mergeTaskIntervalId = partitionId - 1;
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
	StringInfo columnNameArrayString = NULL;
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

	columnNameArrayString = DatumArrayString(columnNameArray, columnCount, CSTRINGOID);

	return columnNameArrayString;
}


/*
 * ColumnTypeArrayString resolves a list of column types for a merged table, and
 * outputs this list of column types in their (array) string representation.
 */
static StringInfo
ColumnTypeArrayString(List *targetEntryList)
{
	StringInfo columnTypeArrayString = NULL;
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

	columnTypeArrayString = DatumArrayString(columnTypeArray, columnCount, CSTRINGOID);

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
	Task *firstSqlTask = NULL;
	bool hasAnchorShardId = false;
	bool hasMergeTaskDependencies = false;
	ListCell *sqlTaskCell = NULL;
	List *primarySqlTaskList = NIL;
	ListCell *primarySqlTaskCell = NULL;
	List *constrainedSqlTaskList = NIL;
	ListCell *constrainedSqlTaskCell = NULL;

	/* no tasks to assign */
	if (sqlTaskList == NIL)
	{
		return NIL;
	}

	firstSqlTask = (Task *) linitial(sqlTaskList);
	if (firstSqlTask->anchorShardId != INVALID_SHARD_ID)
	{
		hasAnchorShardId = true;
	}

	/*
	 * If these SQL tasks don't depend on any merge tasks, we can assign each
	 * one independently of the other. We therefore go ahead and assign these
	 * SQL tasks using the "anchor shard based" assignment algorithms.
	 */
	hasMergeTaskDependencies = HasMergeTaskDependencies(sqlTaskList);
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
		List *mergeTaskList = FindDependedMergeTaskList(sqlTask);

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
		List *mergeTaskList = FindDependedMergeTaskList(sqlTask);

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
	constrainedSqlTaskList = TaskListDifference(sqlTaskList, primarySqlTaskList);

	foreach(constrainedSqlTaskCell, constrainedSqlTaskList)
	{
		Task *sqlTask = (Task *) lfirst(constrainedSqlTaskCell);
		List *mergeTaskList = FindDependedMergeTaskList(sqlTask);
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
	List *dependedTaskList = sqlTask->dependedTaskList;

	ListCell *dependedTaskCell = NULL;
	foreach(dependedTaskCell, dependedTaskList)
	{
		Task *dependedTask = (Task *) lfirst(dependedTaskCell);
		if (dependedTask->taskType == MERGE_TASK)
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
 * TaskListConcatUnique append to list1 each member of list2  that isn't
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
 * TaskListUnion generate the union of two tasks lists. This is calculated by
 * copying list1 via list_copy(), then adding to it all the members of list2
 * that aren't already in list1.
 */
List *
TaskListUnion(const List *list1, const List *list2)
{
	const ListCell *taskCell = NULL;
	List *resultList = NIL;

	resultList = list_copy(list1);

	foreach(taskCell, list2)
	{
		if (!TaskListMember(resultList, lfirst(taskCell)))
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
	List *activeShardPlacementLists = NIL;
	uint32 assignedTaskCount = 0;
	uint32 taskCount = list_length(taskList);

	/* get the worker node list and sort the list */
	List *workerNodeList = WorkerNodeList();
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/*
	 * We first sort tasks by their anchor shard id. We then walk over each task
	 * in the sorted list, get the task's anchor shard id, and look up the shard
	 * placements (locations) for this shard id. Next, we sort the placements by
	 * their insertion time, and append them to a new list.
	 */
	taskList = SortList(taskList, CompareTasksByShardId);
	activeShardPlacementLists = ActiveShardPlacementLists(taskList);

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
			ShardPlacement *placement = NULL;
			uint32 placementCount = 0;

			/* check if we already assigned this task */
			if (task == NULL)
			{
				continue;
			}

			/* check if we have enough replicas */
			placementCount = list_length(placementList);
			if (placementCount <= replicaIndex)
			{
				continue;
			}

			placement = (ShardPlacement *) list_nth(placementList, replicaIndex);
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
static List *
RoundRobinAssignTaskList(List *taskList)
{
	taskList = ReorderAndAssignTaskList(taskList, RoundRobinReorder);

	return taskList;
}


/*
 * RoundRobinReorder implements the core of the round-robin assignment policy.
 * It takes a task and placement list and rotates a copy of the placement list
 * based on the task's jobId. The rotated copy is returned.
 */
static List *
RoundRobinReorder(Task *task, List *placementList)
{
	uint64 jobId = task->jobId;
	uint32 activePlacementCount = list_length(placementList);
	uint32 roundRobinIndex = (jobId % activePlacementCount);

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
	List *activeShardPlacementLists = NIL;
	ListCell *taskCell = NULL;
	ListCell *placementListCell = NULL;
	uint32 unAssignedTaskCount = 0;

	/*
	 * We first sort tasks by their anchor shard id. We then sort placements for
	 * each anchor shard by the placement's insertion time. Note that we sort
	 * these lists just to make our policy more deterministic.
	 */
	taskList = SortList(taskList, CompareTasksByShardId);
	activeShardPlacementLists = ActiveShardPlacementLists(taskList);

	forboth(taskCell, taskList, placementListCell, activeShardPlacementLists)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *placementList = (List *) lfirst(placementListCell);

		/* inactive placements are already filtered out */
		uint32 activePlacementCount = list_length(placementList);
		if (activePlacementCount > 0)
		{
			ShardPlacement *primaryPlacement = NULL;

			if (reorderFunction != NULL)
			{
				placementList = reorderFunction(task, placementList);
			}
			task->taskPlacementList = placementList;

			primaryPlacement = (ShardPlacement *) linitial(task->taskPlacementList);
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
 * the given task list, sorts each shard placement list by tuple insertion time,
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

		List *shardPlacementList = FinalizedShardPlacementList(anchorShardId);

		/* filter out shard placements that reside in inactive nodes */
		List *activeShardPlacementList = ActivePlacementList(shardPlacementList);

		/* sort shard placements by their insertion time */
		activeShardPlacementList = SortList(activeShardPlacementList,
											CompareShardPlacements);
		shardPlacementLists = lappend(shardPlacementLists, activeShardPlacementList);
	}

	return shardPlacementLists;
}


/*
 * CompareShardPlacements compares two shard placements by their tuple oid; this
 * oid reflects the tuple's insertion order into pg_dist_shard_placement.
 */
int
CompareShardPlacements(const void *leftElement, const void *rightElement)
{
	const ShardPlacement *leftPlacement = *((const ShardPlacement **) leftElement);
	const ShardPlacement *rightPlacement = *((const ShardPlacement **) rightElement);

	Oid leftTupleOid = leftPlacement->tupleOid;
	Oid rightTupleOid = rightPlacement->tupleOid;

	/* tuples that are inserted earlier appear first */
	int tupleOidDiff = leftTupleOid - rightTupleOid;
	return tupleOidDiff;
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
		bool workerNodeActive = false;

		/* check if the worker node for this shard placement is active */
		workerNodeActive = WorkerNodeActive(placement->nodeName, placement->nodePort);
		if (workerNodeActive)
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

	uint32 rotateIndex = 0;
	for (rotateIndex = 0; rotateIndex < rotateCount; rotateIndex++)
	{
		void *firstElement = linitial(rotatedList);

		rotatedList = list_delete_first(rotatedList);
		rotatedList = lappend(rotatedList, firstElement);
	}

	return rotatedList;
}


/*
 * FindDependedMergeTaskList walks over the given task's depended task list,
 * finds the merge tasks in the list, and returns those found tasks in a new
 * list.
 */
static List *
FindDependedMergeTaskList(Task *sqlTask)
{
	List *dependedMergeTaskList = NIL;
	List *dependedTaskList = sqlTask->dependedTaskList;

	ListCell *dependedTaskCell = NULL;
	foreach(dependedTaskCell, dependedTaskList)
	{
		Task *dependedTask = (Task *) lfirst(dependedTaskCell);
		if (dependedTask->taskType == MERGE_TASK)
		{
			dependedMergeTaskList = lappend(dependedMergeTaskList, dependedTask);
		}
	}

	return dependedMergeTaskList;
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
	List *workerNodeList = WorkerNodeList();
	uint32 workerNodeCount = (uint32) list_length(workerNodeList);
	uint32 beginningNodeIndex = jobId % workerNodeCount;

	/* sort worker node list and task list for deterministic results */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);
	taskList = SortList(taskList, CompareTasksByTaskId);

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *taskPlacementList = NIL;
		ShardPlacement *primaryPlacement = NULL;

		uint32 replicaIndex = 0;
		for (replicaIndex = 0; replicaIndex < ShardReplicationFactor; replicaIndex++)
		{
			uint32 assignmentOffset = beginningNodeIndex + assignedTaskIndex +
									  replicaIndex;
			uint32 assignmentIndex = assignmentOffset % workerNodeCount;
			WorkerNode *workerNode = list_nth(workerNodeList, assignmentIndex);

			ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
			taskPlacement->nodeName = pstrdup(workerNode->workerName);
			taskPlacement->nodePort = workerNode->workerPort;

			taskPlacementList = lappend(taskPlacementList, taskPlacement);
		}

		task->taskPlacementList = taskPlacementList;

		primaryPlacement = (ShardPlacement *) linitial(task->taskPlacementList);
		ereport(DEBUG3, (errmsg("assigned task %u to node %s:%u", task->taskId,
								primaryPlacement->nodeName,
								primaryPlacement->nodePort)));

		assignedTaskList = lappend(assignedTaskList, task);
		assignedTaskIndex++;
	}

	return assignedTaskList;
}


/* Helper function to compare two tasks by their taskId. */
static int
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
		List *dependedTaskList = task->dependedTaskList;
		ListCell *dependedTaskCell = NULL;

		Assert(task->taskPlacementList != NIL);
		Assert(task->taskType == SQL_TASK || task->taskType == MERGE_TASK);

		foreach(dependedTaskCell, dependedTaskList)
		{
			Task *dependedTask = (Task *) lfirst(dependedTaskCell);
			if (dependedTask->taskType == SHARD_FETCH_TASK ||
				dependedTask->taskType == MAP_OUTPUT_FETCH_TASK)
			{
				dependedTask->taskPlacementList = task->taskPlacementList;
			}
		}
	}
}


/*
 * TaskListHighestTaskId walks over tasks in the given task list, finds the task
 * that has the largest taskId, and returns that taskId.
 *
 * Note: This function assumes that the depended taskId's are set before the
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
	uint32 columnCount = 0;
	uint32 columnIndex = 0;

	appendStringInfo(mergeTableName, "%s%s", taskTableName->data, MERGE_TABLE_SUFFIX);

	columnCount = (uint32) list_length(targetEntryList);

	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *columnExpression = (Node *) targetEntry->expr;
		Oid columnTypeId = exprType(columnExpression);
		int32 columnTypeMod = exprTypmod(columnExpression);
		char *columnName = NULL;
		char *columnType = NULL;

		StringInfo columnNameString = makeStringInfo();
		appendStringInfo(columnNameString, MERGE_COLUMN_FORMAT, columnIndex);

		columnName = columnNameString->data;
		columnType = format_type_with_typemod(columnTypeId, columnTypeMod);

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
	RangeTblEntry *rangeTableEntry = NULL;
	Alias *referenceNames = NULL;
	List *columnNames = NIL;
	List *rangeTableList = NIL;
	ListCell *columnNameCell = NULL;
	uint32 columnCount = 0;
	uint32 columnIndex = 0;

	columnCount = FinalTargetEntryCount(reduceQuery->targetList);
	columnNames = DerivedColumnNameList(columnCount, jobId);

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

	rangeTableList = taskReduceQuery->rtable;
	rangeTableEntry = (RangeTblEntry *) linitial(rangeTableList);
	referenceNames = rangeTableEntry->eref;
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
