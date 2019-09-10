/*-------------------------------------------------------------------------
 *
 * multi_logical_optimizer.c
 *	  Routines for optimizing logical plan trees based on multi-relational
 *	  algebra.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/extended_op_node_utils.h"
#include "distributed/function_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_enabled_custom_aggregates.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/* Config variable managed via guc.c */
int LimitClauseRowFetchCount = -1; /* number of rows to fetch from each task */
double CountDistinctErrorRate = 0.0; /* precision of count(distinct) approximate */


typedef struct MasterAggregateWalkerContext
{
	AttrNumber columnId;
	bool pullDistinctColumns;
} MasterAggregateWalkerContext;

typedef struct WorkerAggregateWalkerContext
{
	List *expressionList;
	bool createGroupByClause;
	bool pullDistinctColumns;
} WorkerAggregateWalkerContext;


/*
 * QueryTargetList encapsulates the necessary fields to form
 * worker query's target list.
 */
typedef struct QueryTargetList
{
	List *targetEntryList; /* the list of target entries */
	AttrNumber targetProjectionNumber; /* the index of the last entry */
} QueryTargetList;


/*
 * QueryGroupClause encapsulates the necessary fields to form
 * worker query's group by clause.
 */
typedef struct QueryGroupClause
{
	List *groupClauseList; /* the list of group clause entries */
	Index *nextSortGroupRefIndex; /* pointer to the index of the largest sort group reference index */
} QueryGroupClause;


/*
 * QueryDistinctClause encapsulates the necessary fields to form
 * worker query's DISTINCT/DISTINCT ON parts.
 */
typedef struct QueryDistinctClause
{
	List *workerDistinctClause; /* the list of distinct clause entries */
	bool workerHasDistinctOn;
} QueryDistinctClause;


/*
 * QueryWindowClause encapsulates the necessary fields to form
 * worker query's window clause.
 */
typedef struct QueryWindowClause
{
	List *workerWindowClauseList; /* the list of window clause entries */
	bool hasWindowFunctions;
	Index *nextSortGroupRefIndex; /* see QueryGroupClause */
} QueryWindowClause;


/*
 * QueryOrderByLimit encapsulates the necessary fields to form
 * worker query's order by and limit clauses. Note that we don't
 * keep track of limit offset clause since it is incorporated
 * into the limit clause during the processing.
 */
typedef struct QueryOrderByLimit
{
	Node *workerLimitCount;
	List *workerSortClauseList;
	Index *nextSortGroupRefIndex; /* see QueryGroupClause */
} QueryOrderByLimit;


/*
 * OrderByLimitReference a structure that is used commonly while
 * processing sort and limit clauses.
 */
typedef struct OrderByLimitReference
{
	bool groupedByDisjointPartitionColumn;
	bool groupClauseIsEmpty;
	bool sortClauseIsEmpty;
	bool hasOrderByAggregate;
	bool canApproximate;
	bool hasDistinctOn;
} OrderByLimitReference;


/* Local functions forward declarations */
static MultiSelect * AndSelectNode(MultiSelect *selectNode);
static MultiSelect * OrSelectNode(MultiSelect *selectNode);
static List * OrSelectClauseList(List *selectClauseList);
static void PushDownNodeLoop(MultiUnaryNode *currentNode);
static void PullUpCollectLoop(MultiCollect *collectNode);
static void AddressProjectSpecialConditions(MultiProject *projectNode);
static List * ListCopyDeep(List *nodeList);
static PushDownStatus CanPushDown(MultiUnaryNode *parentNode);
static PullUpStatus CanPullUp(MultiUnaryNode *childNode);
static PushDownStatus Commutative(MultiUnaryNode *parentNode,
								  MultiUnaryNode *childNode);
static PushDownStatus Distributive(MultiUnaryNode *parentNode,
								   MultiBinaryNode *childNode);
static PullUpStatus Factorizable(MultiBinaryNode *parentNode,
								 MultiUnaryNode *childNode);
static List * SelectClauseTableIdList(List *selectClauseList);
static MultiUnaryNode * GenerateLeftNode(MultiUnaryNode *currentNode,
										 MultiBinaryNode *binaryNode);
static MultiUnaryNode * GenerateRightNode(MultiUnaryNode *currentNode,
										  MultiBinaryNode *binaryNode);
static MultiUnaryNode * GenerateNode(MultiUnaryNode *currentNode, MultiNode *childNode);
static List * TableIdListColumns(List *tableIdList, List *columnList);
static List * TableIdListSelectClauses(List *tableIdList, List *selectClauseList);
static void PushDownBelowUnaryChild(MultiUnaryNode *currentNode,
									MultiUnaryNode *childNode);
static void PlaceUnaryNodeChild(MultiUnaryNode *unaryNode, MultiUnaryNode *childNode);
static void PlaceBinaryNodeLeftChild(MultiBinaryNode *binaryNode,
									 MultiUnaryNode *newLeftChildNode);
static void PlaceBinaryNodeRightChild(MultiBinaryNode *binaryNode,
									  MultiUnaryNode *newRightChildNode);
static void RemoveUnaryNode(MultiUnaryNode *unaryNode);
static void PullUpUnaryNode(MultiUnaryNode *unaryNode);
static void ParentSetNewChild(MultiNode *parentNode, MultiNode *oldChildNode,
							  MultiNode *newChildNode);

/* Local functions forward declarations for aggregate expressions */
static void ApplyExtendedOpNodes(MultiExtendedOp *originalNode,
								 MultiExtendedOp *masterNode,
								 MultiExtendedOp *workerNode);
static void TransformSubqueryNode(MultiTable *subqueryNode);
static MultiExtendedOp * MasterExtendedOpNode(MultiExtendedOp *originalOpNode,
											  ExtendedOpNodeProperties *
											  extendedOpNodeProperties);
static Node * MasterAggregateMutator(Node *originalNode,
									 MasterAggregateWalkerContext *walkerContext);
static Expr * MasterAggregateExpression(Aggref *originalAggregate,
										MasterAggregateWalkerContext *walkerContext);
static Expr * MasterAverageExpression(Oid sumAggregateType, Oid countAggregateType,
									  AttrNumber *columnId);
static Expr * AddTypeConversion(Node *originalAggregate, Node *newExpression);
static MultiExtendedOp * WorkerExtendedOpNode(MultiExtendedOp *originalOpNode,
											  ExtendedOpNodeProperties *
											  extendedOpNodeProperties);
static bool TargetListHasAggragates(List *targetEntryList);
static void ProcessTargetListForWorkerQuery(List *targetEntryList,
											ExtendedOpNodeProperties *
											extendedOpNodeProperties,
											QueryTargetList *queryTargetList,
											QueryGroupClause *queryGroupClause);
static void ProcessHavingClauseForWorkerQuery(Node *havingQual,
											  ExtendedOpNodeProperties *
											  extendedOpNodeProperties,
											  Node **workerHavingQual,
											  QueryTargetList *queryTargetList,
											  QueryGroupClause *queryGroupClause);
static void ProcessDistinctClauseForWorkerQuery(List *distinctClause, bool hasDistinctOn,
												List *groupClauseList,
												bool queryHasAggregates,
												QueryDistinctClause *queryDistinctClause,
												bool *distinctPreventsLimitPushdown);
static void ProcessWindowFunctionsForWorkerQuery(List *windowClauseList,
												 List *originalTargetEntryList,
												 QueryWindowClause *queryWindowClause,
												 QueryTargetList *queryTargetList);
static void ProcessLimitOrderByForWorkerQuery(OrderByLimitReference orderByLimitReference,
											  Node *originalLimitCount, Node *limitOffset,
											  List *sortClauseList, List *groupClauseList,
											  List *originalTargetList,
											  QueryOrderByLimit *queryOrderByLimit,
											  QueryTargetList *queryTargetList);
static OrderByLimitReference BuildOrderByLimitReference(bool hasDistinctOn, bool
														groupedByDisjointPartitionColumn,
														List *groupClause,
														List *sortClauseList,
														List *targetList);
static void ExpandWorkerTargetEntry(List *expressionList,
									TargetEntry *originalTargetEntry,
									bool addToGroupByClause,
									QueryTargetList *queryTargetList,
									QueryGroupClause *queryGroupClause);
static Index GetNextSortGroupRef(List *targetEntryList);
static TargetEntry * GenerateWorkerTargetEntry(TargetEntry *targetEntry,
											   Expr *workerExpression,
											   AttrNumber targetProjectionNumber);
static void AppendTargetEntryToGroupClause(TargetEntry *targetEntry,
										   QueryGroupClause *queryGroupClause);
static bool WorkerAggregateWalker(Node *node,
								  WorkerAggregateWalkerContext *walkerContext);
static List * WorkerAggregateExpressionList(Aggref *originalAggregate,
											WorkerAggregateWalkerContext *walkerContextry);
static AggregateType GetAggregateType(Oid aggFunctionId);
static Oid AggregateArgumentType(Aggref *aggregate);
static bool AggregateEnabledCustom(const char *functionName);
static Oid AggregateFunctionOidWithoutInput(const char *functionName);
static Oid AggregateFunctionOid(const char *functionName, Oid inputType);
static Oid TypeOid(Oid schemaId, const char *typeName);
static SortGroupClause * CreateSortGroupClause(Var *column);

/* Local functions forward declarations for count(distinct) approximations */
static const char * CountDistinctHashFunctionName(Oid argumentType);
static int CountDistinctStorageSize(double approximationErrorRate);
static Const * MakeIntegerConst(int32 integerValue);
static Const * MakeIntegerConstInt64(int64 integerValue);

/* Local functions forward declarations for aggregate expression checks */
static void ErrorIfContainsUnsupportedAggregate(MultiNode *logicalPlanNode);
static void ErrorIfUnsupportedArrayAggregate(Aggref *arrayAggregateExpression);
static void ErrorIfUnsupportedJsonAggregate(AggregateType type,
											Aggref *aggregateExpression);
static void ErrorIfUnsupportedJsonObjectAggregate(AggregateType type,
												  Aggref *aggregateExpression);
static void ErrorIfUnsupportedAggregateDistinct(Aggref *aggregateExpression,
												MultiNode *logicalPlanNode);
static Var * AggregateDistinctColumn(Aggref *aggregateExpression);
static bool TablePartitioningSupportsDistinct(List *tableNodeList,
											  MultiExtendedOp *opNode,
											  Var *distinctColumn,
											  AggregateType aggregateType);

/* Local functions forward declarations for limit clauses */
static Node * WorkerLimitCount(Node *limitCount, Node *limitOffset, OrderByLimitReference
							   orderByLimitReference);
static List * WorkerSortClauseList(Node *limitCount,
								   List *groupClauseList, List *sortClauseList,
								   OrderByLimitReference orderByLimitReference);
static List * GenerateNewTargetEntriesForSortClauses(List *originalTargetList,
													 List *sortClauseList,
													 AttrNumber *targetProjectionNumber,
													 Index *nextSortGroupRefIndex);
static bool CanPushDownLimitApproximate(List *sortClauseList, List *targetList);
static bool HasOrderByAggregate(List *sortClauseList, List *targetList);
static bool HasOrderByAverage(List *sortClauseList, List *targetList);
static bool HasOrderByComplexExpression(List *sortClauseList, List *targetList);
static bool HasOrderByHllType(List *sortClauseList, List *targetList);


/*
 * MultiLogicalPlanOptimize applies multi-relational algebra optimizations on
 * the given logical plan tree. Specifically, the function applies four set of
 * optimizations in a particular order.
 *
 * First, the function splits the search node into two nodes that contain And
 * and Or clauses, and pushes down the node that contains And clauses. Second,
 * the function pushes down the project node; this node either contains columns
 * to return to the user, or aggregate expressions used by the aggregate node.
 * Third, the function pulls up the collect operators in the tree. Fourth, the
 * function finds the extended operator node, and splits this node into master
 * and worker extended operator nodes.
 */
void
MultiLogicalPlanOptimize(MultiTreeRoot *multiLogicalPlan)
{
	bool hasOrderByHllType = false;
	List *selectNodeList = NIL;
	List *projectNodeList = NIL;
	List *collectNodeList = NIL;
	List *extendedOpNodeList = NIL;
	List *tableNodeList = NIL;
	ListCell *collectNodeCell = NULL;
	ListCell *tableNodeCell = NULL;
	MultiProject *projectNode = NULL;
	MultiExtendedOp *extendedOpNode = NULL;
	MultiExtendedOp *masterExtendedOpNode = NULL;
	MultiExtendedOp *workerExtendedOpNode = NULL;
	ExtendedOpNodeProperties extendedOpNodeProperties;
	MultiNode *logicalPlanNode = (MultiNode *) multiLogicalPlan;

	/* check that we can optimize aggregates in the plan */
	ErrorIfContainsUnsupportedAggregate(logicalPlanNode);

	/*
	 * If a select node exists, we use the idempower property to split the node
	 * into two nodes that contain And and Or clauses. If both And and Or nodes
	 * exist, we modify the tree in place to swap the original select node with
	 * And and Or nodes. We then push down the And select node if it exists.
	 */
	selectNodeList = FindNodesOfType(logicalPlanNode, T_MultiSelect);
	if (selectNodeList != NIL)
	{
		MultiSelect *selectNode = (MultiSelect *) linitial(selectNodeList);
		MultiSelect *andSelectNode = AndSelectNode(selectNode);
		MultiSelect *orSelectNode = OrSelectNode(selectNode);

		if (andSelectNode != NULL && orSelectNode != NULL)
		{
			MultiNode *parentNode = ParentNode((MultiNode *) selectNode);
			MultiNode *childNode = ChildNode((MultiUnaryNode *) selectNode);
			Assert(UnaryOperator(parentNode));

			SetChild((MultiUnaryNode *) parentNode, (MultiNode *) orSelectNode);
			SetChild((MultiUnaryNode *) orSelectNode, (MultiNode *) andSelectNode);
			SetChild((MultiUnaryNode *) andSelectNode, (MultiNode *) childNode);
		}
		else if (andSelectNode != NULL && orSelectNode == NULL)
		{
			andSelectNode = selectNode; /* no need to modify the tree */
		}

		if (andSelectNode != NULL)
		{
			PushDownNodeLoop((MultiUnaryNode *) andSelectNode);
		}
	}

	/* push down the multi project node */
	projectNodeList = FindNodesOfType(logicalPlanNode, T_MultiProject);
	projectNode = (MultiProject *) linitial(projectNodeList);
	PushDownNodeLoop((MultiUnaryNode *) projectNode);

	/* pull up collect nodes and merge duplicate collects */
	collectNodeList = FindNodesOfType(logicalPlanNode, T_MultiCollect);
	foreach(collectNodeCell, collectNodeList)
	{
		MultiCollect *collectNode = (MultiCollect *) lfirst(collectNodeCell);
		PullUpCollectLoop(collectNode);
	}

	/*
	 * We split the extended operator node into its equivalent master and worker
	 * operator nodes; and if the extended operator has aggregates, we transform
	 * aggregate functions accordingly for the master and worker operator nodes.
	 * If we can push down the limit clause, we also add limit count and sort
	 * clause list to the worker operator node. We then push the worker operator
	 * node below the collect node.
	 */
	extendedOpNodeList = FindNodesOfType(logicalPlanNode, T_MultiExtendedOp);
	extendedOpNode = (MultiExtendedOp *) linitial(extendedOpNodeList);

	extendedOpNodeProperties = BuildExtendedOpNodeProperties(extendedOpNode);

	masterExtendedOpNode =
		MasterExtendedOpNode(extendedOpNode, &extendedOpNodeProperties);
	workerExtendedOpNode =
		WorkerExtendedOpNode(extendedOpNode, &extendedOpNodeProperties);

	ApplyExtendedOpNodes(extendedOpNode, masterExtendedOpNode, workerExtendedOpNode);

	tableNodeList = FindNodesOfType(logicalPlanNode, T_MultiTable);
	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = (MultiTable *) lfirst(tableNodeCell);
		if (tableNode->relationId == SUBQUERY_RELATION_ID)
		{
			ErrorIfContainsUnsupportedAggregate((MultiNode *) tableNode);
			TransformSubqueryNode(tableNode);
		}
	}

	/*
	 * When enabled, count(distinct) approximation uses hll as the intermediate
	 * data type. We currently have a mismatch between hll target entry and sort
	 * clause's sortop oid, so we can't push an order by on the hll data type to
	 * the worker node. We check that here and error out if necessary.
	 */
	hasOrderByHllType = HasOrderByHllType(workerExtendedOpNode->sortClauseList,
										  workerExtendedOpNode->targetList);
	if (hasOrderByHllType)
	{
		ereport(ERROR, (errmsg("cannot approximate count(distinct) and order by it"),
						errhint("You might need to disable approximations for either "
								"count(distinct) or limit through configuration.")));
	}
}


/*
 * AndSelectNode looks for AND clauses in the given select node. If they exist,
 * the function returns these clauses in a new node. Otherwise, the function
 * returns null.
 */
static MultiSelect *
AndSelectNode(MultiSelect *selectNode)
{
	MultiSelect *andSelectNode = NULL;
	List *selectClauseList = selectNode->selectClauseList;
	List *orSelectClauseList = OrSelectClauseList(selectClauseList);

	/* AND clauses are select clauses that are not OR clauses */
	List *andSelectClauseList = list_difference(selectClauseList, orSelectClauseList);
	if (andSelectClauseList != NIL)
	{
		andSelectNode = CitusMakeNode(MultiSelect);
		andSelectNode->selectClauseList = andSelectClauseList;
	}

	return andSelectNode;
}


/*
 * OrSelectNode looks for OR clauses in the given select node. If they exist,
 * the function returns these clauses in a new node. Otherwise, the function
 * returns null.
 */
static MultiSelect *
OrSelectNode(MultiSelect *selectNode)
{
	MultiSelect *orSelectNode = NULL;
	List *selectClauseList = selectNode->selectClauseList;
	List *orSelectClauseList = OrSelectClauseList(selectClauseList);

	if (orSelectClauseList != NIL)
	{
		orSelectNode = CitusMakeNode(MultiSelect);
		orSelectNode->selectClauseList = orSelectClauseList;
	}

	return orSelectNode;
}


/*
 * OrSelectClauseList walks over the select clause list, and returns all clauses
 * that have OR expressions in them.
 */
static List *
OrSelectClauseList(List *selectClauseList)
{
	List *orSelectClauseList = NIL;
	ListCell *selectClauseCell = NULL;

	foreach(selectClauseCell, selectClauseList)
	{
		Node *selectClause = (Node *) lfirst(selectClauseCell);
		bool orClause = or_clause(selectClause);
		if (orClause)
		{
			orSelectClauseList = lappend(orSelectClauseList, selectClause);
		}
	}

	return orSelectClauseList;
}


/*
 * PushDownNodeLoop pushes down the current node as far down the plan tree as
 * possible. For this, the function first addresses any special conditions that
 * may apply on the current node. Then, the function pushes down the current
 * node if its child node is unary. If the child is binary, the function splits
 * the current node into two nodes by applying generation rules, and recurses
 * into itself to push down these two nodes.
 */
static void
PushDownNodeLoop(MultiUnaryNode *currentNode)
{
	MultiUnaryNode *projectNodeGenerated = NULL;
	MultiUnaryNode *leftNodeGenerated = NULL;
	MultiUnaryNode *rightNodeGenerated = NULL;

	PushDownStatus pushDownStatus = CanPushDown(currentNode);
	while (pushDownStatus == PUSH_DOWN_VALID ||
		   pushDownStatus == PUSH_DOWN_SPECIAL_CONDITIONS)
	{
		MultiNode *childNode = currentNode->childNode;
		bool unaryChild = UnaryOperator(childNode);
		bool binaryChild = BinaryOperator(childNode);

		/*
		 * We first check if we can use the idempower property to split the
		 * project node. We split at a partition node as it captures the
		 * minimal set of columns needed from a partition job. After the split
		 * we break from the loop and recursively call pushdown for the
		 * generated project node.
		 */
		MultiNode *parentNode = ParentNode((MultiNode *) currentNode);
		CitusNodeTag currentNodeType = CitusNodeTag(currentNode);
		CitusNodeTag parentNodeType = CitusNodeTag(parentNode);

		if (currentNodeType == T_MultiProject && parentNodeType == T_MultiPartition)
		{
			projectNodeGenerated = GenerateNode(currentNode, childNode);
			PlaceUnaryNodeChild(currentNode, projectNodeGenerated);

			break;
		}

		/* address any special conditions before we can perform the pushdown */
		if (pushDownStatus == PUSH_DOWN_SPECIAL_CONDITIONS)
		{
			MultiProject *projectNode = (MultiProject *) currentNode;
			Assert(currentNodeType == T_MultiProject);

			AddressProjectSpecialConditions(projectNode);
		}

		if (unaryChild)
		{
			MultiUnaryNode *unaryChildNode = (MultiUnaryNode *) childNode;
			PushDownBelowUnaryChild(currentNode, unaryChildNode);
		}
		else if (binaryChild)
		{
			MultiBinaryNode *binaryChildNode = (MultiBinaryNode *) childNode;
			leftNodeGenerated = GenerateLeftNode(currentNode, binaryChildNode);
			rightNodeGenerated = GenerateRightNode(currentNode, binaryChildNode);

			/* push down the generated nodes below the binary child node */
			PlaceBinaryNodeLeftChild(binaryChildNode, leftNodeGenerated);
			PlaceBinaryNodeRightChild(binaryChildNode, rightNodeGenerated);

			/*
			 * Remove the current node, and break out of the push down loop for
			 * the current node. Then, recurse into the push down function for
			 * the newly generated nodes.
			 */
			RemoveUnaryNode(currentNode);
			break;
		}

		pushDownStatus = CanPushDown(currentNode);
	}

	/* recursively perform pushdown of any nodes generated in the loop */
	if (projectNodeGenerated != NULL)
	{
		PushDownNodeLoop(projectNodeGenerated);
	}
	if (leftNodeGenerated != NULL)
	{
		PushDownNodeLoop(leftNodeGenerated);
	}
	if (rightNodeGenerated != NULL)
	{
		PushDownNodeLoop(rightNodeGenerated);
	}
}


/*
 * PullUpCollectLoop pulls up the collect node as far up as possible in the plan
 * tree. The function also merges two collect nodes that are direct descendants
 * of each other by removing the given collect node from the tree.
 */
static void
PullUpCollectLoop(MultiCollect *collectNode)
{
	MultiNode *childNode = NULL;
	MultiUnaryNode *currentNode = (MultiUnaryNode *) collectNode;

	PullUpStatus pullUpStatus = CanPullUp(currentNode);
	while (pullUpStatus == PULL_UP_VALID)
	{
		PullUpUnaryNode(currentNode);
		pullUpStatus = CanPullUp(currentNode);
	}

	/*
	 * After pulling up the collect node, if we find that our child node is also
	 * a collect, we merge the two collect nodes together by removing this node.
	 */
	childNode = currentNode->childNode;
	if (CitusIsA(childNode, MultiCollect))
	{
		RemoveUnaryNode(currentNode);
	}
}


/*
 * AddressProjectSpecialConditions adds columns to the project node if necessary
 * to make the node commutative and distributive with its child node. For this,
 * the function checks for any special conditions between the project and child
 * node, and determines the child node columns to add for the special conditions
 * to apply. The function then adds these columns to the project node.
 */
static void
AddressProjectSpecialConditions(MultiProject *projectNode)
{
	MultiNode *childNode = ChildNode((MultiUnaryNode *) projectNode);
	CitusNodeTag childNodeTag = CitusNodeTag(childNode);
	List *childColumnList = NIL;

	/*
	 * We check if we need to include any child columns in the project node to
	 * address the following special conditions.
	 *
	 * SNC1: project node must include child node's projected columns, or
	 * SNC2: project node must include child node's partition column,  or
	 * SNC3: project node must include child node's selection columns, or
	 * NSC1: project node must include child node's join columns.
	 */
	if (childNodeTag == T_MultiProject)
	{
		MultiProject *projectChildNode = (MultiProject *) childNode;
		List *projectColumnList = projectChildNode->columnList;

		childColumnList = ListCopyDeep(projectColumnList);
	}
	else if (childNodeTag == T_MultiPartition)
	{
		MultiPartition *partitionNode = (MultiPartition *) childNode;
		Var *partitionColumn = partitionNode->partitionColumn;
		List *partitionColumnList = list_make1(partitionColumn);

		childColumnList = ListCopyDeep(partitionColumnList);
	}
	else if (childNodeTag == T_MultiSelect)
	{
		MultiSelect *selectNode = (MultiSelect *) childNode;
		Node *selectClauseList = (Node *) selectNode->selectClauseList;
		List *selectList = pull_var_clause_default(selectClauseList);

		childColumnList = ListCopyDeep(selectList);
	}
	else if (childNodeTag == T_MultiJoin)
	{
		MultiJoin *joinNode = (MultiJoin *) childNode;
		Node *joinClauseList = (Node *) joinNode->joinClauseList;
		List *joinList = pull_var_clause_default(joinClauseList);

		childColumnList = ListCopyDeep(joinList);
	}

	/*
	 * If we need to include any child columns, then find the columns that are
	 * not already in the project column list, and add them.
	 */
	if (childColumnList != NIL)
	{
		List *projectColumnList = projectNode->columnList;
		List *newColumnList = list_concat_unique(projectColumnList, childColumnList);

		projectNode->columnList = newColumnList;
	}
}


/* Deep copies the given node list, and returns the deep copied list. */
static List *
ListCopyDeep(List *nodeList)
{
	List *nodeCopyList = NIL;
	ListCell *nodeCell = NULL;

	foreach(nodeCell, nodeList)
	{
		Node *node = (Node *) lfirst(nodeCell);
		Node *nodeCopy = copyObject(node);

		nodeCopyList = lappend(nodeCopyList, nodeCopy);
	}

	return nodeCopyList;
}


/*
 * CanPushDown determines if a particular node can be moved below its child. The
 * criteria for pushing down a node is determined by multi-relational algebra's
 * rules for commutativity and distributivity.
 */
static PushDownStatus
CanPushDown(MultiUnaryNode *parentNode)
{
	PushDownStatus pushDownStatus = PUSH_DOWN_INVALID_FIRST;
	MultiNode *childNode = parentNode->childNode;
	bool unaryChild = UnaryOperator(childNode);
	bool binaryChild = BinaryOperator(childNode);

	if (unaryChild)
	{
		pushDownStatus = Commutative(parentNode, (MultiUnaryNode *) childNode);
	}
	else if (binaryChild)
	{
		pushDownStatus = Distributive(parentNode, (MultiBinaryNode *) childNode);
	}

	Assert(pushDownStatus != PUSH_DOWN_INVALID_FIRST);
	return pushDownStatus;
}


/*
 * CanPullUp determines if a particular node can be moved above its parent. The
 * criteria for pulling up a node is determined by multi-relational algebra's
 * rules for commutativity and factorizability.
 */
static PullUpStatus
CanPullUp(MultiUnaryNode *childNode)
{
	PullUpStatus pullUpStatus = PULL_UP_INVALID_FIRST;
	MultiNode *parentNode = ParentNode((MultiNode *) childNode);
	bool unaryParent = UnaryOperator(parentNode);
	bool binaryParent = BinaryOperator(parentNode);

	if (unaryParent)
	{
		/*
		 * Evaluate if parent can be pushed down below the child node, since it
		 * is equivalent to pulling up the child above its parent.
		 */
		PushDownStatus parentPushDownStatus = PUSH_DOWN_INVALID_FIRST;
		parentPushDownStatus = Commutative((MultiUnaryNode *) parentNode, childNode);

		if (parentPushDownStatus == PUSH_DOWN_VALID)
		{
			pullUpStatus = PULL_UP_VALID;
		}
		else
		{
			pullUpStatus = PULL_UP_NOT_VALID;
		}
	}
	else if (binaryParent)
	{
		pullUpStatus = Factorizable((MultiBinaryNode *) parentNode, childNode);
	}

	Assert(pullUpStatus != PULL_UP_INVALID_FIRST);
	return pullUpStatus;
}


/*
 * Commutative returns a status which denotes whether the given parent node can
 * be pushed down below its child node using the commutative property.
 */
static PushDownStatus
Commutative(MultiUnaryNode *parentNode, MultiUnaryNode *childNode)
{
	PushDownStatus pushDownStatus = PUSH_DOWN_NOT_VALID;
	CitusNodeTag parentNodeTag = CitusNodeTag(parentNode);
	CitusNodeTag childNodeTag = CitusNodeTag(childNode);

	/* we cannot be commutative with non-query operators */
	if (childNodeTag == T_MultiTreeRoot || childNodeTag == T_MultiTable)
	{
		return PUSH_DOWN_NOT_VALID;
	}

	/* first check for commutative operators and no special conditions */
	if ((parentNodeTag == T_MultiPartition && childNodeTag == T_MultiProject) ||
		(parentNodeTag == T_MultiPartition && childNodeTag == T_MultiPartition) ||
		(parentNodeTag == T_MultiPartition && childNodeTag == T_MultiSelect))
	{
		pushDownStatus = PUSH_DOWN_VALID;
	}
	if ((parentNodeTag == T_MultiCollect && childNodeTag == T_MultiProject) ||
		(parentNodeTag == T_MultiCollect && childNodeTag == T_MultiCollect) ||
		(parentNodeTag == T_MultiCollect && childNodeTag == T_MultiSelect))
	{
		pushDownStatus = PUSH_DOWN_VALID;
	}
	if (parentNodeTag == T_MultiSelect)
	{
		pushDownStatus = PUSH_DOWN_VALID;
	}
	if (parentNodeTag == T_MultiProject && childNodeTag == T_MultiCollect)
	{
		pushDownStatus = PUSH_DOWN_VALID;
	}

	/*
	 * The project node is commutative with the below operators given that
	 * its special conditions apply.
	 */
	if ((parentNodeTag == T_MultiProject && childNodeTag == T_MultiProject) ||
		(parentNodeTag == T_MultiProject && childNodeTag == T_MultiPartition) ||
		(parentNodeTag == T_MultiProject && childNodeTag == T_MultiSelect) ||
		(parentNodeTag == T_MultiProject && childNodeTag == T_MultiJoin))
	{
		pushDownStatus = PUSH_DOWN_SPECIAL_CONDITIONS;
	}

	return pushDownStatus;
}


/*
 * Distributive returns a status which denotes whether the given parent node can
 * be pushed down below its binary child node using the distributive property.
 */
static PushDownStatus
Distributive(MultiUnaryNode *parentNode, MultiBinaryNode *childNode)
{
	PushDownStatus pushDownStatus = PUSH_DOWN_NOT_VALID;
	CitusNodeTag parentNodeTag = CitusNodeTag(parentNode);
	CitusNodeTag childNodeTag = CitusNodeTag(childNode);

	/* special condition checks for partition operator are not implemented */
	Assert(parentNodeTag != T_MultiPartition);

	/*
	 * The project node is distributive with the join operator given that its
	 * special conditions apply.
	 */
	if (parentNodeTag == T_MultiProject)
	{
		pushDownStatus = PUSH_DOWN_SPECIAL_CONDITIONS;
	}

	/* collect node is distributive without special conditions */
	if ((parentNodeTag == T_MultiCollect && childNodeTag == T_MultiJoin) ||
		(parentNodeTag == T_MultiCollect && childNodeTag == T_MultiCartesianProduct))
	{
		pushDownStatus = PUSH_DOWN_VALID;
	}

	/*
	 * The select node is distributive with a binary operator if all tables in
	 * the select clauses are output by the binary child. The select clauses are
	 * individually AND'd; and therefore this check is sufficient to implement
	 * the NSC3 special condition in multi-relational algebra.
	 */
	if ((parentNodeTag == T_MultiSelect && childNodeTag == T_MultiJoin) ||
		(parentNodeTag == T_MultiSelect && childNodeTag == T_MultiCartesianProduct))
	{
		MultiSelect *selectNode = (MultiSelect *) parentNode;
		List *selectClauseList = selectNode->selectClauseList;

		List *selectTableIdList = SelectClauseTableIdList(selectClauseList);
		List *childTableIdList = OutputTableIdList((MultiNode *) childNode);

		/* find tables that are in select clause list, but not in child list */
		List *diffList = list_difference_int(selectTableIdList, childTableIdList);
		if (diffList == NIL)
		{
			pushDownStatus = PUSH_DOWN_VALID;
		}
	}

	return pushDownStatus;
}


/*
 * Factorizable returns a status which denotes whether the given unary child
 * node can be pulled up above its binary parent node using the factorizability
 * property. The function currently performs this check only for collect node
 * types; other node types have generation rules that are not yet implemented.
 */
static PullUpStatus
Factorizable(MultiBinaryNode *parentNode, MultiUnaryNode *childNode)
{
	PullUpStatus pullUpStatus = PULL_UP_NOT_VALID;
	CitusNodeTag parentNodeTag = CitusNodeTag(parentNode);
	CitusNodeTag childNodeTag = CitusNodeTag(childNode);

	/*
	 * The following nodes are factorizable with their parents, but we don't
	 * have their generation rules implemented. We therefore assert here.
	 */
	Assert(childNodeTag != T_MultiProject);
	Assert(childNodeTag != T_MultiPartition);
	Assert(childNodeTag != T_MultiSelect);

	if ((childNodeTag == T_MultiCollect && parentNodeTag == T_MultiJoin) ||
		(childNodeTag == T_MultiCollect && parentNodeTag == T_MultiCartesianProduct))
	{
		pullUpStatus = PULL_UP_VALID;
	}

	return pullUpStatus;
}


/*
 * SelectClauseTableIdList finds the (range) table identifier for each select
 * clause in the given list, and returns these identifiers in a new list.
 */
static List *
SelectClauseTableIdList(List *selectClauseList)
{
	List *tableIdList = NIL;
	ListCell *selectClauseCell = NULL;

	foreach(selectClauseCell, selectClauseList)
	{
		Node *selectClause = (Node *) lfirst(selectClauseCell);
		List *selectColumnList = pull_var_clause_default(selectClause);
		Var *selectColumn = NULL;
		int selectColumnTableId = 0;

		if (list_length(selectColumnList) == 0)
		{
			/* filter is a constant, e.g. false or 1=0 */
			continue;
		}

		selectColumn = (Var *) linitial(selectColumnList);
		selectColumnTableId = (int) selectColumn->varno;

		tableIdList = lappend_int(tableIdList, selectColumnTableId);
	}

	return tableIdList;
}


/*
 * GenerateLeftNode splits the current node over the binary node by applying the
 * generation rule for distributivity in multi-relational algebra. After the
 * split, the function returns the left node.
 */
static MultiUnaryNode *
GenerateLeftNode(MultiUnaryNode *currentNode, MultiBinaryNode *binaryNode)
{
	MultiNode *leftChildNode = binaryNode->leftChildNode;
	MultiUnaryNode *leftNodeGenerated = GenerateNode(currentNode, leftChildNode);

	return leftNodeGenerated;
}


/*
 * GenerateRightNode splits the current node over the binary node by applying
 * the generation rule for distributivity in multi-relational algebra. After the
 * split, the function returns the right node.
 */
static MultiUnaryNode *
GenerateRightNode(MultiUnaryNode *currentNode, MultiBinaryNode *binaryNode)
{
	MultiNode *rightChildNode = binaryNode->rightChildNode;
	MultiUnaryNode *rightNodeGenerated = GenerateNode(currentNode, rightChildNode);

	return rightNodeGenerated;
}


/*
 * GenerateNode determines the current node's type, and applies the relevant
 * generation node for that node type. If the current node is a project node,
 * the function creates a new project node with attributes that only have the
 * child subtree's tables. Else if the current node is a select node, the
 * function creates a new select node with select clauses that only belong to
 * the tables output by the child node's subtree.
 */
static MultiUnaryNode *
GenerateNode(MultiUnaryNode *currentNode, MultiNode *childNode)
{
	MultiUnaryNode *generatedNode = NULL;
	CitusNodeTag currentNodeType = CitusNodeTag(currentNode);
	List *tableIdList = OutputTableIdList(childNode);

	if (currentNodeType == T_MultiProject)
	{
		MultiProject *projectNode = (MultiProject *) currentNode;
		List *columnList = copyObject(projectNode->columnList);

		List *newColumnList = TableIdListColumns(tableIdList, columnList);
		if (newColumnList != NIL)
		{
			MultiProject *newProjectNode = CitusMakeNode(MultiProject);
			newProjectNode->columnList = newColumnList;

			generatedNode = (MultiUnaryNode *) newProjectNode;
		}
	}
	else if (currentNodeType == T_MultiSelect)
	{
		MultiSelect *selectNode = (MultiSelect *) currentNode;
		List *selectClauseList = copyObject(selectNode->selectClauseList);
		List *newSelectClauseList = NIL;

		newSelectClauseList = TableIdListSelectClauses(tableIdList, selectClauseList);
		if (newSelectClauseList != NIL)
		{
			MultiSelect *newSelectNode = CitusMakeNode(MultiSelect);
			newSelectNode->selectClauseList = newSelectClauseList;

			generatedNode = (MultiUnaryNode *) newSelectNode;
		}
	}

	return generatedNode;
}


/*
 * TableIdListColumns walks over the given column list, finds columns belonging
 * to the given table id list, and returns the found columns in a new list.
 */
static List *
TableIdListColumns(List *tableIdList, List *columnList)
{
	List *tableColumnList = NIL;
	ListCell *columnCell = NULL;

	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		int columnTableId = (int) column->varno;

		bool tableListMember = list_member_int(tableIdList, columnTableId);
		if (tableListMember)
		{
			tableColumnList = lappend(tableColumnList, column);
		}
	}

	return tableColumnList;
}


/*
 * TableIdListSelectClauses walks over the given select clause list, finds the
 * select clauses whose column references belong to the given table list, and
 * returns the found clauses in a new list.
 */
static List *
TableIdListSelectClauses(List *tableIdList, List *selectClauseList)
{
	List *tableSelectClauseList = NIL;
	ListCell *selectClauseCell = NULL;

	foreach(selectClauseCell, selectClauseList)
	{
		Node *selectClause = (Node *) lfirst(selectClauseCell);

		List *selectColumnList = pull_var_clause_default(selectClause);
		if (list_length(selectColumnList) == 0)
		{
			/* filter is a constant, e.g. false or 1=0, always include it */
			tableSelectClauseList = lappend(tableSelectClauseList, selectClause);
		}
		else
		{
			Var *selectColumn = (Var *) linitial(selectColumnList);
			int selectClauseTableId = (int) selectColumn->varno;

			bool tableIdListMember = list_member_int(tableIdList, selectClauseTableId);
			if (tableIdListMember)
			{
				tableSelectClauseList = lappend(tableSelectClauseList, selectClause);
			}
		}
	}

	return tableSelectClauseList;
}


/* Pushes down the current node below its unary child node. */
static void
PushDownBelowUnaryChild(MultiUnaryNode *currentNode, MultiUnaryNode *childNode)
{
	MultiNode *parentNode = ParentNode((MultiNode *) currentNode);
	MultiNode *childChildNode = ChildNode(childNode);

	/* current node's parent now points to the child node */
	ParentSetNewChild(parentNode, (MultiNode *) currentNode, (MultiNode *) childNode);

	/* current node's child becomes its parent */
	SetChild(childNode, (MultiNode *) currentNode);

	/* current node points to the child node's child */
	SetChild(currentNode, childChildNode);
}


/*
 * PlaceUnaryNodeChild inserts the new node as a child node under the given
 * unary node. The function also places the previous child node under the new
 * child node.
 */
static void
PlaceUnaryNodeChild(MultiUnaryNode *unaryNode, MultiUnaryNode *newChildNode)
{
	MultiNode *oldChildNode = ChildNode(unaryNode);

	SetChild(unaryNode, (MultiNode *) newChildNode);
	SetChild(newChildNode, oldChildNode);
}


/*
 * PlaceBinaryNodeLeftChild inserts the new left child as the binary node's left
 * child. The function also places the previous left child below the new child
 * node.
 */
static void
PlaceBinaryNodeLeftChild(MultiBinaryNode *binaryNode, MultiUnaryNode *newLeftChildNode)
{
	if (newLeftChildNode == NULL)
	{
		return;
	}

	SetChild(newLeftChildNode, binaryNode->leftChildNode);
	SetLeftChild(binaryNode, (MultiNode *) newLeftChildNode);
}


/*
 * PlaceBinaryNodeRightChild inserts the new right child as the binary node's
 * right child. The function also places the previous right child below the new
 * child node.
 */
static void
PlaceBinaryNodeRightChild(MultiBinaryNode *binaryNode, MultiUnaryNode *newRightChildNode)
{
	if (newRightChildNode == NULL)
	{
		return;
	}

	SetChild(newRightChildNode, binaryNode->rightChildNode);
	SetRightChild(binaryNode, (MultiNode *) newRightChildNode);
}


/* Removes the given unary node from the logical plan, and frees the node. */
static void
RemoveUnaryNode(MultiUnaryNode *unaryNode)
{
	MultiNode *parentNode = ParentNode((MultiNode *) unaryNode);
	MultiNode *childNode = ChildNode(unaryNode);

	/* set parent to directly point to unary node's child */
	ParentSetNewChild(parentNode, (MultiNode *) unaryNode, childNode);

	pfree(unaryNode);
}


/* Pulls up the given current node above its parent node. */
static void
PullUpUnaryNode(MultiUnaryNode *unaryNode)
{
	MultiNode *parentNode = ParentNode((MultiNode *) unaryNode);
	bool unaryParent = UnaryOperator(parentNode);
	bool binaryParent = BinaryOperator(parentNode);

	if (unaryParent)
	{
		/* pulling up a node is the same as pushing down the node's unary parent */
		MultiUnaryNode *unaryParentNode = (MultiUnaryNode *) parentNode;
		PushDownBelowUnaryChild(unaryParentNode, unaryNode);
	}
	else if (binaryParent)
	{
		MultiBinaryNode *binaryParentNode = (MultiBinaryNode *) parentNode;
		MultiNode *parentParentNode = ParentNode((MultiNode *) binaryParentNode);
		MultiNode *childNode = unaryNode->childNode;

		/* make the parent node point to the unary node's child node */
		if (binaryParentNode->leftChildNode == ((MultiNode *) unaryNode))
		{
			SetLeftChild(binaryParentNode, childNode);
		}
		else
		{
			SetRightChild(binaryParentNode, childNode);
		}

		/* make the parent parent node point to the unary node */
		ParentSetNewChild(parentParentNode, parentNode, (MultiNode *) unaryNode);

		/* make the unary node point to the (old) parent node */
		SetChild(unaryNode, parentNode);
	}
}


/*
 * ParentSetNewChild takes in the given parent node, and replaces the parent's
 * old child node with the new child node. The function needs the old child node
 * in case the parent is a binary node and the function needs to determine which
 * side of the parent node the new child node needs to go to.
 */
static void
ParentSetNewChild(MultiNode *parentNode, MultiNode *oldChildNode,
				  MultiNode *newChildNode)
{
	bool unaryParent = UnaryOperator(parentNode);
	bool binaryParent = BinaryOperator(parentNode);

	if (unaryParent)
	{
		MultiUnaryNode *unaryParentNode = (MultiUnaryNode *) parentNode;
		SetChild(unaryParentNode, newChildNode);
	}
	else if (binaryParent)
	{
		MultiBinaryNode *binaryParentNode = (MultiBinaryNode *) parentNode;

		/* determine which side of the parent the old child is on */
		if (binaryParentNode->leftChildNode == oldChildNode)
		{
			SetLeftChild(binaryParentNode, newChildNode);
		}
		else
		{
			SetRightChild(binaryParentNode, newChildNode);
		}
	}
}


/*
 * ApplyExtendedOpNodes replaces the original extended operator node with the
 * master and worker extended operator nodes. The function then pushes down the
 * worker node below the original node's child node. Note that for the push down
 * to apply, the original node's child must be a collect node.
 */
static void
ApplyExtendedOpNodes(MultiExtendedOp *originalNode, MultiExtendedOp *masterNode,
					 MultiExtendedOp *workerNode)
{
	MultiNode *parentNode = ParentNode((MultiNode *) originalNode);
	MultiNode *collectNode = ChildNode((MultiUnaryNode *) originalNode);
	MultiNode *collectChildNode = ChildNode((MultiUnaryNode *) collectNode);

	/* original node's child must be a collect node */
	Assert(CitusIsA(collectNode, MultiCollect));
	Assert(UnaryOperator(parentNode));

	/* swap the original aggregate node with the master extended node */
	SetChild((MultiUnaryNode *) parentNode, (MultiNode *) masterNode);
	SetChild((MultiUnaryNode *) masterNode, (MultiNode *) collectNode);

	/* add the worker extended node below the collect node */
	SetChild((MultiUnaryNode *) collectNode, (MultiNode *) workerNode);
	SetChild((MultiUnaryNode *) workerNode, (MultiNode *) collectChildNode);

	/* clean up the original extended operator node */
	pfree(originalNode);
}


/*
 * TransformSubqueryNode splits the extended operator node under subquery
 * multi table node into its equivalent master and worker operator nodes, and
 * we transform aggregate functions accordingly for the master and worker
 * operator nodes. We create a partition node based on the first group by
 * column of the extended operator node and set it as the child of the master
 * operator node.
 */
static void
TransformSubqueryNode(MultiTable *subqueryNode)
{
	MultiExtendedOp *extendedOpNode =
		(MultiExtendedOp *) ChildNode((MultiUnaryNode *) subqueryNode);
	MultiNode *collectNode = ChildNode((MultiUnaryNode *) extendedOpNode);
	MultiNode *collectChildNode = ChildNode((MultiUnaryNode *) collectNode);

	ExtendedOpNodeProperties extendedOpNodeProperties =
		BuildExtendedOpNodeProperties(extendedOpNode);
	MultiExtendedOp *masterExtendedOpNode =
		MasterExtendedOpNode(extendedOpNode, &extendedOpNodeProperties);
	MultiExtendedOp *workerExtendedOpNode =
		WorkerExtendedOpNode(extendedOpNode, &extendedOpNodeProperties);

	List *groupClauseList = extendedOpNode->groupClauseList;
	List *targetEntryList = extendedOpNode->targetList;
	List *groupTargetEntryList = GroupTargetEntryList(groupClauseList, targetEntryList);
	TargetEntry *groupByTargetEntry = (TargetEntry *) linitial(groupTargetEntryList);
	Expr *groupByExpression = groupByTargetEntry->expr;

	MultiPartition *partitionNode = CitusMakeNode(MultiPartition);

	/*
	 * If group by is on a function expression, then we create a new column from
	 * function expression result type. Because later while creating partition
	 * tasks, we expect a column type to partition intermediate results.
	 * Note that we will only need partition type. So we set column type to
	 * result type of the function expression, and set other fields of column to
	 * default values.
	 */
	if (IsA(groupByExpression, Var))
	{
		partitionNode->partitionColumn = (Var *) groupByExpression;
	}
	else if (IsA(groupByExpression, FuncExpr))
	{
		FuncExpr *functionExpression = (FuncExpr *) groupByExpression;
		Index tableId = 0;
		AttrNumber columnAttributeNumber = InvalidAttrNumber;
		Oid columnType = functionExpression->funcresulttype;
		int32 columnTypeMod = -1;
		Oid columnCollationOid = InvalidOid;
		Index columnLevelSup = 0;

		Var *partitionColumn = makeVar(tableId, columnAttributeNumber, columnType,
									   columnTypeMod, columnCollationOid, columnLevelSup);
		partitionNode->partitionColumn = partitionColumn;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot run this subquery"),
						errdetail("Currently only columns and function expressions "
								  "are allowed in group by expression of subqueries")));
	}

	SetChild((MultiUnaryNode *) subqueryNode, (MultiNode *) masterExtendedOpNode);
	SetChild((MultiUnaryNode *) masterExtendedOpNode, (MultiNode *) partitionNode);
	SetChild((MultiUnaryNode *) partitionNode, (MultiNode *) collectNode);
	SetChild((MultiUnaryNode *) collectNode, (MultiNode *) workerExtendedOpNode);
	SetChild((MultiUnaryNode *) workerExtendedOpNode, (MultiNode *) collectChildNode);
}


/*
 * MasterExtendedOpNode creates the master extended operator node from the given
 * target entries. The function walks over these target entries; and for entries
 * with aggregates in them, this function calls the aggregate expression mutator
 * function.
 *
 * Note that the function logically depends on the worker extended operator node
 * function. If the target entry does not contain aggregate functions, we assume
 * all work is done on the worker side, and create a column that references the
 * worker nodes' results.
 */
static MultiExtendedOp *
MasterExtendedOpNode(MultiExtendedOp *originalOpNode,
					 ExtendedOpNodeProperties *extendedOpNodeProperties)
{
	MultiExtendedOp *masterExtendedOpNode = NULL;
	List *targetEntryList = originalOpNode->targetList;
	List *newTargetEntryList = NIL;
	ListCell *targetEntryCell = NULL;
	Node *originalHavingQual = originalOpNode->havingQual;
	Node *newHavingQual = NULL;
	MasterAggregateWalkerContext *walkerContext = palloc0(
		sizeof(MasterAggregateWalkerContext));

	walkerContext->columnId = 1;
	walkerContext->pullDistinctColumns = extendedOpNodeProperties->pullDistinctColumns;

	/* iterate over original target entries */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *originalTargetEntry = (TargetEntry *) lfirst(targetEntryCell);
		TargetEntry *newTargetEntry = copyObject(originalTargetEntry);
		Expr *originalExpression = originalTargetEntry->expr;
		Expr *newExpression = NULL;

		bool hasAggregates = contain_agg_clause((Node *) originalExpression);
		bool hasWindowFunction = contain_window_function((Node *) originalExpression);

		/*
		 * if the aggregate belongs to a window function, it is not mutated, but pushed
		 * down to worker as it is. Master query should treat that as a Var.
		 */
		if (hasAggregates && !hasWindowFunction)
		{
			Node *newNode = MasterAggregateMutator((Node *) originalExpression,
												   walkerContext);
			newExpression = (Expr *) newNode;
		}
		else
		{
			/*
			 * The expression does not have any aggregates. We simply make it
			 * reference the output generated by worker nodes.
			 */
			const uint32 masterTableId = 1; /* only one table on master node */

			Var *column = makeVarFromTargetEntry(masterTableId, originalTargetEntry);
			column->varattno = walkerContext->columnId;
			column->varoattno = walkerContext->columnId;
			walkerContext->columnId++;

			newExpression = (Expr *) column;
		}

		newTargetEntry->expr = newExpression;
		newTargetEntryList = lappend(newTargetEntryList, newTargetEntry);
	}

	if (originalHavingQual != NULL)
	{
		newHavingQual = MasterAggregateMutator(originalHavingQual, walkerContext);
	}

	masterExtendedOpNode = CitusMakeNode(MultiExtendedOp);
	masterExtendedOpNode->targetList = newTargetEntryList;
	masterExtendedOpNode->groupClauseList = originalOpNode->groupClauseList;
	masterExtendedOpNode->sortClauseList = originalOpNode->sortClauseList;
	masterExtendedOpNode->distinctClause = originalOpNode->distinctClause;
	masterExtendedOpNode->hasDistinctOn = originalOpNode->hasDistinctOn;
	masterExtendedOpNode->limitCount = originalOpNode->limitCount;
	masterExtendedOpNode->limitOffset = originalOpNode->limitOffset;
	masterExtendedOpNode->havingQual = newHavingQual;

	return masterExtendedOpNode;
}


/*
 * MasterAggregateMutator walks over the original target entry expression, and
 * creates the new expression tree to execute on the master node. The function
 * transforms aggregates, and copies columns; and recurses into the expression
 * mutator function for all other expression types.
 *
 * Please note that the recursive mutator function traverses the expression tree
 * in depth first order. For this function to set attribute numbers correctly,
 * WorkerAggregateWalker() *must* walk over the expression tree in the same
 * depth first order.
 */
static Node *
MasterAggregateMutator(Node *originalNode, MasterAggregateWalkerContext *walkerContext)
{
	Node *newNode = NULL;
	if (originalNode == NULL)
	{
		return NULL;
	}

	if (IsA(originalNode, Aggref))
	{
		Aggref *originalAggregate = (Aggref *) originalNode;
		Expr *newExpression = MasterAggregateExpression(originalAggregate, walkerContext);

		newNode = (Node *) newExpression;
	}
	else if (IsA(originalNode, Var))
	{
		uint32 masterTableId = 1; /* one table on the master node */
		Var *newColumn = copyObject((Var *) originalNode);
		newColumn->varno = masterTableId;
		newColumn->varattno = walkerContext->columnId;
		walkerContext->columnId++;

		newNode = (Node *) newColumn;
	}
	else
	{
		newNode = expression_tree_mutator(originalNode, MasterAggregateMutator,
										  (void *) walkerContext);
	}

	return newNode;
}


/*
 * MasterAggregateExpression creates the master aggregate expression using the
 * original aggregate and aggregate's type information. This function handles
 * the average, count, array_agg, hll and topn aggregates separately due to
 * differences in these aggregate functions' transformations.
 *
 * Note that this function has implicit knowledge of the transformations applied
 * for worker nodes on the original aggregate. The function uses this implicit
 * knowledge to create the appropriate master function with correct data types.
 */
static Expr *
MasterAggregateExpression(Aggref *originalAggregate,
						  MasterAggregateWalkerContext *walkerContext)
{
	AggregateType aggregateType = GetAggregateType(originalAggregate->aggfnoid);
	Expr *newMasterExpression = NULL;
	Expr *typeConvertedExpression = NULL;
	const uint32 masterTableId = 1;  /* one table on the master node */
	const Index columnLevelsUp = 0;  /* normal column */
	const AttrNumber argumentId = 1; /* our aggregates have single arguments */
	AggClauseCosts aggregateCosts;
	HeapTuple aggTuple;
	Form_pg_aggregate aggform;
	Oid combine;

	if (aggregateType == AGGREGATE_COUNT && originalAggregate->aggdistinct &&
		CountDistinctErrorRate == DISABLE_DISTINCT_APPROXIMATION &&
		walkerContext->pullDistinctColumns)
	{
		Aggref *aggregate = (Aggref *) copyObject(originalAggregate);
		List *varList = pull_var_clause_default((Node *) aggregate);
		ListCell *varCell = NULL;
		List *uniqueVarList = NIL;
		int startColumnCount = walkerContext->columnId;

		/* determine unique vars that were placed in target list by worker */
		foreach(varCell, varList)
		{
			Var *column = (Var *) lfirst(varCell);
			uniqueVarList = list_append_unique(uniqueVarList, copyObject(column));
		}

		/*
		 * Go over each var inside aggregate and update their varattno's according to
		 * worker query target entry column index.
		 */
		foreach(varCell, varList)
		{
			Var *columnToUpdate = (Var *) lfirst(varCell);
			ListCell *uniqueVarCell = NULL;
			int columnIndex = 0;

			foreach(uniqueVarCell, uniqueVarList)
			{
				Var *currentVar = (Var *) lfirst(uniqueVarCell);
				if (equal(columnToUpdate, currentVar))
				{
					break;
				}
				columnIndex++;
			}

			columnToUpdate->varno = masterTableId;
			columnToUpdate->varnoold = masterTableId;
			columnToUpdate->varattno = startColumnCount + columnIndex;
			columnToUpdate->varoattno = startColumnCount + columnIndex;
		}

		/* we added that many columns */
		walkerContext->columnId += list_length(uniqueVarList);

		newMasterExpression = (Expr *) aggregate;
	}
	else if (aggregateType == AGGREGATE_COUNT && originalAggregate->aggdistinct &&
			 CountDistinctErrorRate != DISABLE_DISTINCT_APPROXIMATION)
	{
		/*
		 * If enabled, we check for count(distinct) approximations before count
		 * distincts. For this, we first compute hll_add_agg(hll_hash(column)) on
		 * worker nodes, and get hll values. We then gather hlls on the master
		 * node, and compute hll_cardinality(hll_union_agg(hll)).
		 */
		const int argCount = 1;
		const int defaultTypeMod = -1;

		TargetEntry *hllTargetEntry = NULL;
		Aggref *unionAggregate = NULL;
		FuncExpr *cardinalityExpression = NULL;

		/* extract schema name of hll */
		Oid hllId = get_extension_oid(HLL_EXTENSION_NAME, false);
		Oid hllSchemaOid = get_extension_schema(hllId);
		const char *hllSchemaName = get_namespace_name(hllSchemaOid);

		Oid unionFunctionId = FunctionOid(hllSchemaName, HLL_UNION_AGGREGATE_NAME,
										  argCount);
		Oid cardinalityFunctionId = FunctionOid(hllSchemaName, HLL_CARDINALITY_FUNC_NAME,
												argCount);
		Oid cardinalityReturnType = get_func_rettype(cardinalityFunctionId);

		Oid hllType = TypeOid(hllSchemaOid, HLL_TYPE_NAME);
		Oid hllTypeCollationId = get_typcollation(hllType);
		Var *hllColumn = makeVar(masterTableId, walkerContext->columnId, hllType,
								 defaultTypeMod,
								 hllTypeCollationId, columnLevelsUp);
		walkerContext->columnId++;

		hllTargetEntry = makeTargetEntry((Expr *) hllColumn, argumentId, NULL, false);

		unionAggregate = makeNode(Aggref);
		unionAggregate->aggfnoid = unionFunctionId;
		unionAggregate->aggtype = hllType;
		unionAggregate->args = list_make1(hllTargetEntry);
		unionAggregate->aggkind = AGGKIND_NORMAL;
		unionAggregate->aggfilter = NULL;
		unionAggregate->aggtranstype = InvalidOid;
		unionAggregate->aggargtypes = list_make1_oid(unionAggregate->aggtype);
		unionAggregate->aggsplit = AGGSPLIT_SIMPLE;

		cardinalityExpression = makeNode(FuncExpr);
		cardinalityExpression->funcid = cardinalityFunctionId;
		cardinalityExpression->funcresulttype = cardinalityReturnType;
		cardinalityExpression->args = list_make1(unionAggregate);

		newMasterExpression = (Expr *) cardinalityExpression;
	}
	else if (aggregateType == AGGREGATE_AVERAGE)
	{
		/*
		 * If the original aggregate is an average, we first compute sum(colum)
		 * and count(column) on worker nodes. Then, we compute (sum(sum(column))
		 * / sum(count(column))) on the master node.
		 */
		const char *sumAggregateName = AggregateNames[AGGREGATE_SUM];
		const char *countAggregateName = AggregateNames[AGGREGATE_COUNT];

		Oid argumentType = AggregateArgumentType(originalAggregate);

		Oid sumFunctionId = AggregateFunctionOid(sumAggregateName, argumentType);
		Oid countFunctionId = AggregateFunctionOid(countAggregateName, ANYOID);

		/* calculate the aggregate types that worker nodes are going to return */
		Oid workerSumReturnType = get_func_rettype(sumFunctionId);
		Oid workerCountReturnType = get_func_rettype(countFunctionId);

		/* create the expression sum(sum(column) / sum(count(column))) */
		newMasterExpression = MasterAverageExpression(workerSumReturnType,
													  workerCountReturnType,
													  &(walkerContext->columnId));
	}
	else if (aggregateType == AGGREGATE_COUNT)
	{
		/*
		 * Count aggregates are handled in two steps. First, worker nodes report
		 * their count results. Then, the master node sums up these results.
		 */
		Var *column = NULL;
		TargetEntry *columnTargetEntry = NULL;
		CoerceViaIO *coerceExpr = NULL;
		Const *zeroConst = NULL;
		List *coalesceArgs = NULL;
		CoalesceExpr *coalesceExpr = NULL;

		/* worker aggregate and original aggregate have the same return type */
		Oid workerReturnType = exprType((Node *) originalAggregate);
		int32 workerReturnTypeMod = exprTypmod((Node *) originalAggregate);
		Oid workerCollationId = exprCollation((Node *) originalAggregate);

		const char *sumAggregateName = AggregateNames[AGGREGATE_SUM];
		Oid sumFunctionId = AggregateFunctionOid(sumAggregateName, workerReturnType);
		Oid masterReturnType = get_func_rettype(sumFunctionId);

		Aggref *newMasterAggregate = copyObject(originalAggregate);
		newMasterAggregate->aggstar = false;
		newMasterAggregate->aggdistinct = NULL;
		newMasterAggregate->aggfnoid = sumFunctionId;
		newMasterAggregate->aggtype = masterReturnType;
		newMasterAggregate->aggfilter = NULL;
		newMasterAggregate->aggtranstype = InvalidOid;
		newMasterAggregate->aggargtypes = list_make1_oid(newMasterAggregate->aggtype);
		newMasterAggregate->aggsplit = AGGSPLIT_SIMPLE;

		column = makeVar(masterTableId, walkerContext->columnId, workerReturnType,
						 workerReturnTypeMod, workerCollationId, columnLevelsUp);
		walkerContext->columnId++;

		/* aggref expects its arguments to be wrapped in target entries */
		columnTargetEntry = makeTargetEntry((Expr *) column, argumentId, NULL, false);
		newMasterAggregate->args = list_make1(columnTargetEntry);

		/* cast numeric sum result to bigint (count's return type) */
		coerceExpr = makeNode(CoerceViaIO);
		coerceExpr->arg = (Expr *) newMasterAggregate;
		coerceExpr->resulttype = INT8OID;
		coerceExpr->resultcollid = InvalidOid;
		coerceExpr->coerceformat = COERCE_IMPLICIT_CAST;
		coerceExpr->location = -1;

		/* convert NULL to 0 in case of no rows */
		zeroConst = MakeIntegerConstInt64(0);
		coalesceArgs = list_make2(coerceExpr, zeroConst);

		coalesceExpr = makeNode(CoalesceExpr);
		coalesceExpr->coalescetype = INT8OID;
		coalesceExpr->coalescecollid = InvalidOid;
		coalesceExpr->args = coalesceArgs;
		coalesceExpr->location = -1;

		newMasterExpression = (Expr *) coalesceExpr;
	}
	else if (aggregateType == AGGREGATE_ARRAY_AGG ||
			 aggregateType == AGGREGATE_JSONB_AGG ||
			 aggregateType == AGGREGATE_JSONB_OBJECT_AGG ||
			 aggregateType == AGGREGATE_JSON_AGG ||
			 aggregateType == AGGREGATE_JSON_OBJECT_AGG)
	{
		/*
		 * Array and json aggregates are handled in two steps. First, we compute
		 * array_agg() or json aggregate on the worker nodes. Then, we gather
		 * the arrays or jsons on the master and compute the array_cat_agg()
		 * or jsonb_cat_agg() aggregate on them to get the final array or json.
		 */
		Var *column = NULL;
		TargetEntry *catAggArgument = NULL;
		Aggref *newMasterAggregate = NULL;
		Oid aggregateFunctionId = InvalidOid;
		const char *catAggregateName = NULL;
		Oid catInputType = InvalidOid;

		/* worker aggregate and original aggregate have same return type */
		Oid workerReturnType = exprType((Node *) originalAggregate);
		int32 workerReturnTypeMod = exprTypmod((Node *) originalAggregate);
		Oid workerCollationId = exprCollation((Node *) originalAggregate);

		/* assert that we do not support array or json aggregation with
		 * distinct or order by */
		Assert(!originalAggregate->aggorder);
		Assert(!originalAggregate->aggdistinct);

		if (aggregateType == AGGREGATE_ARRAY_AGG)
		{
			/* array_cat_agg() takes anyarray as input */
			catAggregateName = ARRAY_CAT_AGGREGATE_NAME;
			catInputType = ANYARRAYOID;
		}
		else if (aggregateType == AGGREGATE_JSONB_AGG ||
				 aggregateType == AGGREGATE_JSONB_OBJECT_AGG)
		{
			/* jsonb_cat_agg() takes jsonb as input */
			catAggregateName = JSONB_CAT_AGGREGATE_NAME;
			catInputType = JSONBOID;
		}
		else
		{
			/* json_cat_agg() takes json as input */
			catAggregateName = JSON_CAT_AGGREGATE_NAME;
			catInputType = JSONOID;
		}

		Assert(catAggregateName != NULL);
		Assert(catInputType != InvalidOid);

		aggregateFunctionId = AggregateFunctionOid(catAggregateName,
												   catInputType);

		/* create argument for the array_cat_agg() or jsonb_cat_agg() aggregate */
		column = makeVar(masterTableId, walkerContext->columnId, workerReturnType,
						 workerReturnTypeMod, workerCollationId, columnLevelsUp);
		catAggArgument = makeTargetEntry((Expr *) column, argumentId, NULL, false);
		walkerContext->columnId++;

		/* construct the master array_cat_agg() or jsonb_cat_agg() expression */
		newMasterAggregate = copyObject(originalAggregate);
		newMasterAggregate->aggfnoid = aggregateFunctionId;
		newMasterAggregate->args = list_make1(catAggArgument);
		newMasterAggregate->aggfilter = NULL;
		newMasterAggregate->aggtranstype = InvalidOid;
		newMasterAggregate->aggargtypes = list_make1_oid(ANYARRAYOID);
		newMasterAggregate->aggsplit = AGGSPLIT_SIMPLE;

		newMasterExpression = (Expr *) newMasterAggregate;
	}
	else if (aggregateType == AGGREGATE_HLL_ADD ||
			 aggregateType == AGGREGATE_HLL_UNION)
	{
		/*
		 * If hll aggregates are called, we simply create the hll_union_aggregate
		 * to apply in the master after running the original aggregate in
		 * workers.
		 */
		TargetEntry *hllTargetEntry = NULL;
		Aggref *unionAggregate = NULL;

		Oid hllType = exprType((Node *) originalAggregate);
		Oid unionFunctionId = AggregateFunctionOid(HLL_UNION_AGGREGATE_NAME, hllType);
		int32 hllReturnTypeMod = exprTypmod((Node *) originalAggregate);
		Oid hllTypeCollationId = exprCollation((Node *) originalAggregate);

		Var *hllColumn = makeVar(masterTableId, walkerContext->columnId, hllType,
								 hllReturnTypeMod, hllTypeCollationId, columnLevelsUp);
		walkerContext->columnId++;

		hllTargetEntry = makeTargetEntry((Expr *) hllColumn, argumentId, NULL, false);

		unionAggregate = makeNode(Aggref);
		unionAggregate->aggfnoid = unionFunctionId;
		unionAggregate->aggtype = hllType;
		unionAggregate->args = list_make1(hllTargetEntry);
		unionAggregate->aggkind = AGGKIND_NORMAL;
		unionAggregate->aggfilter = NULL;
		unionAggregate->aggtranstype = InvalidOid;
		unionAggregate->aggargtypes = list_make1_oid(hllType);
		unionAggregate->aggsplit = AGGSPLIT_SIMPLE;

		newMasterExpression = (Expr *) unionAggregate;
	}
	else if (aggregateType == AGGREGATE_TOPN_UNION_AGG ||
			 aggregateType == AGGREGATE_TOPN_ADD_AGG)
	{
		/*
		 * Top-N aggregates are handled in two steps. First, we compute
		 * topn_add_agg() or topn_union_agg() aggregates on the worker nodes.
		 * Then, we gather the Top-Ns on the master and take the union of all
		 * to get the final topn.
		 */
		TargetEntry *topNTargetEntry = NULL;
		Aggref *unionAggregate = NULL;

		/* worker aggregate and original aggregate have same return type */
		Oid topnType = exprType((Node *) originalAggregate);
		Oid unionFunctionId = AggregateFunctionOid(TOPN_UNION_AGGREGATE_NAME,
												   topnType);
		int32 topnReturnTypeMod = exprTypmod((Node *) originalAggregate);
		Oid topnTypeCollationId = exprCollation((Node *) originalAggregate);

		/* create argument for the topn_union_agg() aggregate */
		Var *topnColumn = makeVar(masterTableId, walkerContext->columnId, topnType,
								  topnReturnTypeMod, topnTypeCollationId, columnLevelsUp);
		walkerContext->columnId++;

		topNTargetEntry = makeTargetEntry((Expr *) topnColumn, argumentId, NULL, false);

		/* construct the master topn_union_agg() expression */
		unionAggregate = makeNode(Aggref);
		unionAggregate->aggfnoid = unionFunctionId;
		unionAggregate->aggtype = topnType;
		unionAggregate->args = list_make1(topNTargetEntry);
		unionAggregate->aggkind = AGGKIND_NORMAL;
		unionAggregate->aggfilter = NULL;
		unionAggregate->aggtranstype = InvalidOid;
		unionAggregate->aggargtypes = list_make1_oid(topnType);
		unionAggregate->aggsplit = AGGSPLIT_SIMPLE;

		newMasterExpression = (Expr *) unionAggregate;
	}
	else if (aggregateType == AGGREGATE_CUSTOM)
	{
		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(originalAggregate->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
		{
			elog(WARNING, "citus cache lookup failed for aggregate %u",
				 originalAggregate->aggfnoid);
			combine = InvalidOid;
		}
		else
		{
			aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
			combine = aggform->aggcombinefn;
			ReleaseSysCache(aggTuple);
		}

		if (combine != InvalidOid)
		{
			Const *aggparam = NULL;
			Var *column = NULL;
			List *aggArguments = NIL;
			Aggref *newMasterAggregate = NULL;
			Oid coordCombineId = AggregateFunctionOidWithoutInput(
				COORD_COMBINE_AGGREGATE_NAME);

			Oid workerReturnType = BYTEAOID;
			int32 workerReturnTypeMod = -1;
			Oid workerCollationId = InvalidOid;

			aggparam = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid), ObjectIdGetDatum(
									 originalAggregate->aggfnoid), false, true);
			column = makeVar(masterTableId, walkerContext->columnId, workerReturnType,
							 workerReturnTypeMod, workerCollationId, columnLevelsUp);

			aggArguments = list_make1(makeTargetEntry((Expr *) aggparam, 1, NULL, false));
			aggArguments = lappend(aggArguments, makeTargetEntry((Expr *) column, 2, NULL,
																 false));

			/* coord_combine_agg(agg, workercol) */
			newMasterAggregate = makeNode(Aggref);
			newMasterAggregate->aggfnoid = coordCombineId;
			newMasterAggregate->aggtype = originalAggregate->aggtype;
			newMasterAggregate->args = aggArguments;
			newMasterAggregate->aggkind = AGGKIND_NORMAL;
			newMasterAggregate->aggfilter = originalAggregate->aggfilter;
			newMasterAggregate->aggtranstype = INTERNALOID;
			newMasterAggregate->aggargtypes = list_concat(list_make1_oid(OIDOID),
														  list_make1_oid(BYTEAOID));
			newMasterAggregate->aggsplit = AGGSPLIT_SIMPLE;

			newMasterExpression = (Expr *) newMasterAggregate;
		}
	}
	else
	{
		/*
		 * All other aggregates are handled as they are. These include sum, min,
		 * and max.
		 */
		Var *column = NULL;
		TargetEntry *columnTargetEntry = NULL;

		/* worker aggregate and original aggregate have the same return type */
		Oid workerReturnType = exprType((Node *) originalAggregate);
		int32 workerReturnTypeMod = exprTypmod((Node *) originalAggregate);
		Oid workerCollationId = exprCollation((Node *) originalAggregate);

		const char *aggregateName = AggregateNames[aggregateType];
		Oid aggregateFunctionId = AggregateFunctionOid(aggregateName, workerReturnType);
		Oid masterReturnType = get_func_rettype(aggregateFunctionId);

		Aggref *newMasterAggregate = copyObject(originalAggregate);
		newMasterAggregate->aggdistinct = NULL;
		newMasterAggregate->aggfnoid = aggregateFunctionId;
		newMasterAggregate->aggtype = masterReturnType;
		newMasterAggregate->aggfilter = NULL;

		column = makeVar(masterTableId, walkerContext->columnId, workerReturnType,
						 workerReturnTypeMod, workerCollationId, columnLevelsUp);
		walkerContext->columnId++;

		/* aggref expects its arguments to be wrapped in target entries */
		columnTargetEntry = makeTargetEntry((Expr *) column, argumentId, NULL, false);
		newMasterAggregate->args = list_make1(columnTargetEntry);

		newMasterExpression = (Expr *) newMasterAggregate;
	}


	/*
	 * Aggregate functions could have changed the return type. If so, we wrap
	 * the new expression with a conversion function to make it have the same
	 * type as the original aggregate. We need this since functions like sorting
	 * and grouping have already been chosen based on the original type.
	 */
	typeConvertedExpression = AddTypeConversion((Node *) originalAggregate,
												(Node *) newMasterExpression);
	if (typeConvertedExpression != NULL)
	{
		newMasterExpression = typeConvertedExpression;
	}

	/* Run AggRefs through cost machinery to mark required fields sanely */
	memset(&aggregateCosts, 0, sizeof(aggregateCosts));

	get_agg_clause_costs(NULL, (Node *) newMasterExpression, AGGSPLIT_SIMPLE,
						 &aggregateCosts);

	return newMasterExpression;
}


/*
 * MasterAverageExpression creates an expression of the form (sum(column1) /
 * sum(column2)), where column1 is the sum of the original value, and column2 is
 * the count of that value. This expression allows us to evaluate the average
 * function over distributed data.
 */
static Expr *
MasterAverageExpression(Oid sumAggregateType, Oid countAggregateType,
						AttrNumber *columnId)
{
	const char *sumAggregateName = AggregateNames[AGGREGATE_SUM];
	const uint32 masterTableId = 1;
	const int32 defaultTypeMod = -1;
	const Index defaultLevelsUp = 0;
	const AttrNumber argumentId = 1;

	Oid sumTypeCollationId = get_typcollation(sumAggregateType);
	Oid countTypeCollationId = get_typcollation(countAggregateType);
	Var *firstColumn = NULL;
	Var *secondColumn = NULL;
	TargetEntry *firstTargetEntry = NULL;
	TargetEntry *secondTargetEntry = NULL;
	Aggref *firstSum = NULL;
	Aggref *secondSum = NULL;
	List *operatorNameList = NIL;
	Expr *opExpr = NULL;

	/* create the first argument for sum(column1) */
	firstColumn = makeVar(masterTableId, (*columnId), sumAggregateType,
						  defaultTypeMod, sumTypeCollationId, defaultLevelsUp);
	firstTargetEntry = makeTargetEntry((Expr *) firstColumn, argumentId, NULL, false);
	(*columnId)++;

	firstSum = makeNode(Aggref);
	firstSum->aggfnoid = AggregateFunctionOid(sumAggregateName, sumAggregateType);
	firstSum->aggtype = get_func_rettype(firstSum->aggfnoid);
	firstSum->args = list_make1(firstTargetEntry);
	firstSum->aggkind = AGGKIND_NORMAL;
	firstSum->aggtranstype = InvalidOid;
	firstSum->aggargtypes = list_make1_oid(firstSum->aggtype);
	firstSum->aggsplit = AGGSPLIT_SIMPLE;

	/* create the second argument for sum(column2) */
	secondColumn = makeVar(masterTableId, (*columnId), countAggregateType,
						   defaultTypeMod, countTypeCollationId, defaultLevelsUp);
	secondTargetEntry = makeTargetEntry((Expr *) secondColumn, argumentId, NULL, false);
	(*columnId)++;

	secondSum = makeNode(Aggref);
	secondSum->aggfnoid = AggregateFunctionOid(sumAggregateName, countAggregateType);
	secondSum->aggtype = get_func_rettype(secondSum->aggfnoid);
	secondSum->args = list_make1(secondTargetEntry);
	secondSum->aggkind = AGGKIND_NORMAL;
	secondSum->aggtranstype = InvalidOid;
	secondSum->aggargtypes = list_make1_oid(firstSum->aggtype);
	secondSum->aggsplit = AGGSPLIT_SIMPLE;

	/*
	 * Build the division operator between these two aggregates. This function
	 * will convert the types of the aggregates if necessary.
	 */
	operatorNameList = list_make1(makeString(DIVISION_OPER_NAME));
	opExpr = make_op(NULL, operatorNameList, (Node *) firstSum, (Node *) secondSum, NULL,
					 -1);

	return opExpr;
}


/*
 * AddTypeConversion checks if the given expressions generate the same types. If
 * they don't, the function adds a type conversion function on top of the new
 * expression to have it generate the same type as the original aggregate.
 */
static Expr *
AddTypeConversion(Node *originalAggregate, Node *newExpression)
{
	Oid newTypeId = exprType(newExpression);
	Oid originalTypeId = exprType(originalAggregate);
	int32 originalTypeMod = exprTypmod(originalAggregate);
	Node *typeConvertedExpression = NULL;

	/* nothing to do if the two types are the same */
	if (originalTypeId == newTypeId)
	{
		return NULL;
	}

	/* otherwise, add a type conversion function */
	typeConvertedExpression = coerce_to_target_type(NULL, newExpression, newTypeId,
													originalTypeId, originalTypeMod,
													COERCION_EXPLICIT,
													COERCE_EXPLICIT_CAST, -1);
	Assert(typeConvertedExpression != NULL);
	return (Expr *) typeConvertedExpression;
}


/*
 * WorkerExtendedOpNode creates the worker extended operator node from the given
 * originalOpNode and extendedOpNodeProperties.
 *
 * For the details of the processing see the comments of the functions that
 * are called from this function.
 */
static MultiExtendedOp *
WorkerExtendedOpNode(MultiExtendedOp *originalOpNode,
					 ExtendedOpNodeProperties *extendedOpNodeProperties)
{
	MultiExtendedOp *workerExtendedOpNode = NULL;
	Index nextSortGroupRefIndex = 0;
	bool distinctPreventsLimitPushdown = false;
	bool groupByExtended = false;
	bool groupedByDisjointPartitionColumn =
		extendedOpNodeProperties->groupedByDisjointPartitionColumn;

	QueryTargetList queryTargetList;
	QueryGroupClause queryGroupClause;
	QueryDistinctClause queryDistinctClause;
	QueryWindowClause queryWindowClause;
	QueryOrderByLimit queryOrderByLimit;
	Node *queryHavingQual = NULL;

	List *originalTargetEntryList = originalOpNode->targetList;
	List *originalGroupClauseList = originalOpNode->groupClauseList;
	List *originalSortClauseList = originalOpNode->sortClauseList;
	Node *originalHavingQual = originalOpNode->havingQual;
	Node *originalLimitCount = originalOpNode->limitCount;
	Node *originalLimitOffset = originalOpNode->limitOffset;
	List *originalWindowClause = originalOpNode->windowClause;
	List *originalDistinctClause = originalOpNode->distinctClause;
	bool hasDistinctOn = originalOpNode->hasDistinctOn;

	int originalGroupClauseLength = list_length(originalGroupClauseList);
	bool queryHasAggregates = TargetListHasAggragates(originalTargetEntryList);

	/* initialize to default values */
	memset(&queryTargetList, 0, sizeof(queryGroupClause));
	memset(&queryGroupClause, 0, sizeof(queryGroupClause));
	memset(&queryDistinctClause, 0, sizeof(queryGroupClause));
	memset(&queryWindowClause, 0, sizeof(queryGroupClause));
	memset(&queryOrderByLimit, 0, sizeof(queryGroupClause));

	/* calculate the next sort group index based on the original target list */
	nextSortGroupRefIndex = GetNextSortGroupRef(originalTargetEntryList);

	/* targetProjectionNumber starts from 1 */
	queryTargetList.targetProjectionNumber = 1;

	/* worker query always include all the group by entries in the query */
	queryGroupClause.groupClauseList = copyObject(originalGroupClauseList);

	/*
	 * nextSortGroupRefIndex is used by group by, window and order by clauses.
	 * Thus, we pass a reference to a single nextSortGroupRefIndex and expect
	 * it modified separately while processing those parts of the query.
	 */
	queryGroupClause.nextSortGroupRefIndex = &nextSortGroupRefIndex;
	queryWindowClause.nextSortGroupRefIndex = &nextSortGroupRefIndex;
	queryOrderByLimit.nextSortGroupRefIndex = &nextSortGroupRefIndex;

	/* process each part of the query in order to generate the worker query's parts */
	ProcessTargetListForWorkerQuery(originalTargetEntryList, extendedOpNodeProperties,
									&queryTargetList, &queryGroupClause);

	ProcessHavingClauseForWorkerQuery(originalHavingQual, extendedOpNodeProperties,
									  &queryHavingQual, &queryTargetList,
									  &queryGroupClause);

	ProcessDistinctClauseForWorkerQuery(originalDistinctClause, hasDistinctOn,
										queryGroupClause.groupClauseList,
										queryHasAggregates, &queryDistinctClause,
										&distinctPreventsLimitPushdown);

	ProcessWindowFunctionsForWorkerQuery(originalWindowClause, originalTargetEntryList,
										 &queryWindowClause, &queryTargetList);

	/*
	 * Order by and limit clauses are relevant to each other, and processing
	 * them together makes it handy for us.
	 *
	 * The other parts of the query might have already prohibited pushing down
	 * LIMIT and ORDER BY clauses as described below:
	 *      (1) Creating a new group by clause during aggregate mutation, or
	 *      (2) Distinct clause is not pushed down
	 */
	groupByExtended =
		list_length(queryGroupClause.groupClauseList) > originalGroupClauseLength;
	if (!groupByExtended && !distinctPreventsLimitPushdown)
	{
		/* both sort and limit clauses rely on similar information */
		OrderByLimitReference limitOrderByReference =
			BuildOrderByLimitReference(hasDistinctOn,
									   groupedByDisjointPartitionColumn,
									   originalGroupClauseList,
									   originalSortClauseList,
									   originalTargetEntryList);

		ProcessLimitOrderByForWorkerQuery(limitOrderByReference, originalLimitCount,
										  originalLimitOffset, originalSortClauseList,
										  originalGroupClauseList,
										  originalTargetEntryList,
										  &queryOrderByLimit,
										  &queryTargetList);
	}

	/* finally, fill the extended op node with the data we gathered */
	workerExtendedOpNode = CitusMakeNode(MultiExtendedOp);

	workerExtendedOpNode->targetList = queryTargetList.targetEntryList;
	workerExtendedOpNode->groupClauseList = queryGroupClause.groupClauseList;
	workerExtendedOpNode->havingQual = queryHavingQual;
	workerExtendedOpNode->hasDistinctOn = queryDistinctClause.workerHasDistinctOn;
	workerExtendedOpNode->distinctClause = queryDistinctClause.workerDistinctClause;
	workerExtendedOpNode->hasWindowFuncs = queryWindowClause.hasWindowFunctions;
	workerExtendedOpNode->windowClause = queryWindowClause.workerWindowClauseList;
	workerExtendedOpNode->sortClauseList = queryOrderByLimit.workerSortClauseList;
	workerExtendedOpNode->limitCount = queryOrderByLimit.workerLimitCount;

	return workerExtendedOpNode;
}


/*
 * ProcessTargetListForWorkerQuery gets the inputs and modifies the outputs
 * such that the worker query's target list and group by clauses are extended
 * for the given inputs.
 *
 * The function walks over the input targetEntryList. For the entries
 * with aggregates in them, it calls the recursive aggregate walker function to
 * create aggregates for the worker nodes. For example, the avg() is sent to
 * the worker with two expressions count() and sum(). Thus, a single target entry
 * might end up with multiple expressions in the worker query.
 *
 * The function doesn't change the aggragates in the window functions and sends them
 * as-is. The reason is that Citus currently only supports pushing down window
 * functions as-is. As we implement pull-to-master window functions, we should
 * revisit here as well.
 *
 * The function also handles count distinct operator if it is used in repartition
 * subqueries or on non-partition columns (e.g., cannot be pushed down). Each
 * column in count distinct aggregate is added to target list, and group by
 * list of worker extended operator. This approach guarantees the distinctness
 * in the worker queries.
 *
 *     inputs: targetEntryList, extendedOpNodeProperties
 *     outputs: queryTargetList, queryGroupClause
 */
static void
ProcessTargetListForWorkerQuery(List *targetEntryList,
								ExtendedOpNodeProperties *extendedOpNodeProperties,
								QueryTargetList *queryTargetList,
								QueryGroupClause *queryGroupClause)
{
	ListCell *targetEntryCell = NULL;
	WorkerAggregateWalkerContext *workerAggContext =
		palloc0(sizeof(WorkerAggregateWalkerContext));

	workerAggContext->expressionList = NIL;
	workerAggContext->pullDistinctColumns = extendedOpNodeProperties->pullDistinctColumns;

	/* iterate over original target entries */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *originalTargetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Expr *originalExpression = originalTargetEntry->expr;
		List *newExpressionList = NIL;
		bool hasAggregates = contain_agg_clause((Node *) originalExpression);
		bool hasWindowFunction = contain_window_function((Node *) originalExpression);

		/* reset walker context */
		workerAggContext->expressionList = NIL;
		workerAggContext->createGroupByClause = false;

		/*
		 * If the expression uses aggregates inside window function contain agg
		 * clause still returns true. We want to make sure it is not a part of
		 * window function before we proceed.
		 */
		if (hasAggregates && !hasWindowFunction)
		{
			WorkerAggregateWalker((Node *) originalExpression, workerAggContext);

			newExpressionList = workerAggContext->expressionList;
		}
		else
		{
			newExpressionList = list_make1(originalExpression);
		}

		ExpandWorkerTargetEntry(newExpressionList, originalTargetEntry,
								workerAggContext->createGroupByClause,
								queryTargetList, queryGroupClause);
	}
}


/*
 * ProcessHavingClauseForWorkerQuery gets the inputs and modifies the outputs
 * such that the worker query's target list and group by clauses are extended
 * based on the inputs.
 *
 * The rule is that Citus always applies the HAVING clause on the
 * coordinator. Thus, it pulls the necessary data from the workers. Also, when the
 * having clause is safe to pushdown to the workers, workerHavingQual is set to
 * be the original having clause.
 *
 * TODO: Citus currently always pulls the expressions in the having clause to the
 * coordinator and apply it on the coordinator. Do we really need to pull those
 * expressions to the coordinator and apply the having on the coordinator if we're
 * already pushing down the HAVING clause?
 *
 *     inputs: originalHavingQual, extendedOpNodeProperties
 *     outputs: workerHavingQual, queryTargetList, queryGroupClause
 */
static void
ProcessHavingClauseForWorkerQuery(Node *originalHavingQual,
								  ExtendedOpNodeProperties *extendedOpNodeProperties,
								  Node **workerHavingQual,
								  QueryTargetList *queryTargetList,
								  QueryGroupClause *queryGroupClause)
{
	List *newExpressionList = NIL;
	TargetEntry *targetEntry = NULL;
	WorkerAggregateWalkerContext *workerAggContext = NULL;

	if (originalHavingQual == NULL)
	{
		return;
	}

	*workerHavingQual = NULL;

	workerAggContext = palloc0(sizeof(WorkerAggregateWalkerContext));
	workerAggContext->expressionList = NIL;
	workerAggContext->pullDistinctColumns = extendedOpNodeProperties->pullDistinctColumns;
	workerAggContext->createGroupByClause = false;

	WorkerAggregateWalker(originalHavingQual, workerAggContext);
	newExpressionList = workerAggContext->expressionList;

	ExpandWorkerTargetEntry(newExpressionList, targetEntry,
							workerAggContext->createGroupByClause,
							queryTargetList, queryGroupClause);

	/*
	 * If grouped by a partition column whose values are shards have disjoint sets
	 * of partition values, we can push down the having qualifier.
	 *
	 * When a query with subquery is provided, we can't determine if
	 * groupedByDisjointPartitionColumn, therefore we also check if there is a
	 * window function too. If there is a window function we would know that it
	 * is safe to push down (i.e. it is partitioned on distribution column, and
	 * if there is a group by, it contains distribution column).
	 *
	 */
	if (extendedOpNodeProperties->groupedByDisjointPartitionColumn ||
		extendedOpNodeProperties->pushDownWindowFunctions)
	{
		/*
		 * We converted the having expression to a list in subquery pushdown
		 * planner. However, this query cannot be parsed as it is in the worker.
		 * We should convert this back to being explicit for worker query
		 * so that it can be parsed when it hits to the standard planner in
		 * worker.
		 */
		if (IsA(originalHavingQual, List))
		{
			*workerHavingQual =
				(Node *) make_ands_explicit((List *) originalHavingQual);
		}
		else
		{
			*workerHavingQual = originalHavingQual;
		}
	}
}


/*
 * PrcoessDistinctClauseForWorkerQuery gets the inputs and modifies the outputs
 * such that worker query's DISTINCT and DISTINCT ON clauses are set accordingly.
 * Note the the function may or may not decide to pushdown the DISTINCT and DISTINCT
 * on clauses based on the inputs.
 *
 * See the detailed comments in the function for the rules of pushing down DISTINCT
 * and DISTINCT ON clauses to the worker queries.
 *
 * The function also sets distinctPreventsLimitPushdown. As the name reveals,
 * distinct could prevent pushwing down LIMIT clauses later in the planning.
 * For the details, see the comments in the function.
 *
 *     inputs: distinctClause, hasDistinctOn, groupClauseList, queryHasAggregates
 *     outputs: queryDistinctClause, distinctPreventsLimitPushdown
 *
 */
static void
ProcessDistinctClauseForWorkerQuery(List *distinctClause, bool hasDistinctOn,
									List *groupClauseList,
									bool queryHasAggregates,
									QueryDistinctClause *queryDistinctClause,
									bool *distinctPreventsLimitPushdown)
{
	bool distinctClauseSupersetofGroupClause = false;
	bool shouldPushdownDistinct = false;

	if (distinctClause == NIL)
	{
		return;
	}

	*distinctPreventsLimitPushdown = false;

	if (groupClauseList == NIL ||
		IsGroupBySubsetOfDistinct(groupClauseList, distinctClause))
	{
		distinctClauseSupersetofGroupClause = true;
	}
	else
	{
		distinctClauseSupersetofGroupClause = false;

		/*
		 * GROUP BY being a subset of DISTINCT guarantees the
		 * distinctness on the workers. Otherwise, pushing down
		 * LIMIT might cause missing the necessary data from
		 * the worker query
		 */
		*distinctPreventsLimitPushdown = true;
	}

	/*
	 * Distinct is pushed down to worker query only if the query does not
	 * contain an aggregate in which master processing might be required to
	 * complete the final result before distinct operation. We also prevent
	 * distinct pushdown if distinct clause is missing some entries that
	 * group by clause has.
	 */
	shouldPushdownDistinct = !queryHasAggregates &&
							 distinctClauseSupersetofGroupClause;
	if (shouldPushdownDistinct)
	{
		queryDistinctClause->workerDistinctClause = distinctClause;
		queryDistinctClause->workerHasDistinctOn = hasDistinctOn;
	}
}


/*
 * ProcessWindowFunctionsForWorkerQuery gets the inputs and modifies the outputs such
 * that worker query's workerWindowClauseList is set when the window clauses are safe to
 * pushdown.
 *
 * TODO: Citus only supports pushing down window clauses as-is under certain circumstances.
 * And, at this point in the planning, we are guaraanted to process a window function
 * which is safe to pushdown as-is. It should also be possible to pull the relevant data
 * to the coordinator and apply the window clauses for the remaining cases.
 *
 * Note that even though Citus only pushes down the window functions, it may need to
 * modify the target list of the worker query when the window function refers to
 * an avg(). The reason is that any aggragate which is also referred by other
 * target entries would be mutated by Citus. Thus, we add a copy of the same aggragate
 * to the worker target list to make sure that the window function refers to the
 * non-mutated aggragate.
 *
 *     inputs: windowClauseList, originalTargetEntryList
 *     outputs: queryWindowClause, queryTargetList
 *
 */
static void
ProcessWindowFunctionsForWorkerQuery(List *windowClauseList,
									 List *originalTargetEntryList,
									 QueryWindowClause *queryWindowClause,
									 QueryTargetList *queryTargetList)
{
	ListCell *windowClauseCell = NULL;

	if (windowClauseList == NIL)
	{
		queryWindowClause->hasWindowFunctions = false;

		return;
	}

	foreach(windowClauseCell, windowClauseList)
	{
		WindowClause *windowClause = (WindowClause *) lfirst(windowClauseCell);

		List *partitionClauseTargetList =
			GenerateNewTargetEntriesForSortClauses(originalTargetEntryList,
												   windowClause->partitionClause,
												   &(queryTargetList->
													 targetProjectionNumber),
												   queryWindowClause->
												   nextSortGroupRefIndex);
		List *orderClauseTargetList =
			GenerateNewTargetEntriesForSortClauses(originalTargetEntryList,
												   windowClause->orderClause,
												   &(queryTargetList->
													 targetProjectionNumber),
												   queryWindowClause->
												   nextSortGroupRefIndex);

		/*
		 * Note that even Citus does push down the window clauses as-is, we may still need to
		 * add the generated entries to the target list. The reason is that the same aggragates
		 * might be referred from another target entry that is a bare aggragate (e.g., no window
		 * functions), which would have been mutated. For instance, when an average aggragate
		 * is mutated on the target list, the window function would refer to a sum aggragate,
		 * which is obviously wrong.
		 */
		queryTargetList->targetEntryList = list_concat(queryTargetList->targetEntryList,
													   partitionClauseTargetList);
		queryTargetList->targetEntryList = list_concat(queryTargetList->targetEntryList,
													   orderClauseTargetList);
	}

	queryWindowClause->workerWindowClauseList = windowClauseList;
	queryWindowClause->hasWindowFunctions = true;
}


/*
 * ProcessLimitOrderByForWorkerQuery gets the inputs and modifies the outputs
 * such that worker query's LIMIT and ORDER BY clauses are set accordingly.
 * Adding entries to ORDER BY might trigger adding new entries to newTargetEntryList.
 * See GenerateNewTargetEntriesForSortClauses() for the details.
 *
 * For the decisions on whether and how to pushdown LIMIT and ORDER BY are documented
 * in the functions that are called from this function.
 *
 *     inputs: sortLimitReference, originalLimitCount, limitOffset,
 *             sortClauseList, groupClauseList, originalTargetList
 *     outputs: queryOrderByLimit, queryTargetList
 */
static void
ProcessLimitOrderByForWorkerQuery(OrderByLimitReference orderByLimitReference,
								  Node *originalLimitCount, Node *limitOffset,
								  List *sortClauseList, List *groupClauseList,
								  List *originalTargetList,
								  QueryOrderByLimit *queryOrderByLimit,
								  QueryTargetList *queryTargetList)
{
	List *newTargetEntryListForSortClauses = NIL;

	queryOrderByLimit->workerLimitCount =
		WorkerLimitCount(originalLimitCount, limitOffset, orderByLimitReference);

	queryOrderByLimit->workerSortClauseList =
		WorkerSortClauseList(originalLimitCount,
							 groupClauseList,
							 sortClauseList,
							 orderByLimitReference);

	/*
	 * TODO: Do we really need to add the target entries if we're not pushing
	 * down ORDER BY?
	 */
	newTargetEntryListForSortClauses =
		GenerateNewTargetEntriesForSortClauses(originalTargetList,
											   queryOrderByLimit->workerSortClauseList,
											   &(queryTargetList->targetProjectionNumber),
											   queryOrderByLimit->nextSortGroupRefIndex);

	queryTargetList->targetEntryList =
		list_concat(queryTargetList->targetEntryList, newTargetEntryListForSortClauses);
}


/*
 * BuildLimitOrderByReference is a helper function that simply builds
 * the necessary information for processing the limit and order by.
 * The return value should be used in a read-only manner.
 */
static OrderByLimitReference
BuildOrderByLimitReference(bool hasDistinctOn, bool groupedByDisjointPartitionColumn,
						   List *groupClause, List *sortClauseList, List *targetList)
{
	OrderByLimitReference limitOrderByReference;

	limitOrderByReference.groupedByDisjointPartitionColumn =
		groupedByDisjointPartitionColumn;
	limitOrderByReference.hasDistinctOn = hasDistinctOn;
	limitOrderByReference.groupClauseIsEmpty = (groupClause == NIL);
	limitOrderByReference.sortClauseIsEmpty = (sortClauseList == NIL);
	limitOrderByReference.canApproximate =
		CanPushDownLimitApproximate(sortClauseList, targetList);
	limitOrderByReference.hasOrderByAggregate =
		HasOrderByAggregate(sortClauseList, targetList);

	return limitOrderByReference;
}


/*
 * TargetListHasAggragates returns true if any of the elements in the
 * target list contain aggragates that are not inside the window functions.
 */
static bool
TargetListHasAggragates(List *targetEntryList)
{
	ListCell *targetEntryCell = NULL;

	/* iterate over original target entries */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Expr *targetExpr = targetEntry->expr;
		bool hasAggregates = contain_agg_clause((Node *) targetExpr);
		bool hasWindowFunction = contain_window_function((Node *) targetExpr);

		/*
		 * If the expression uses aggregates inside window function contain agg
		 * clause still returns true. We want to make sure it is not a part of
		 * window function before we proceed.
		 */
		if (hasAggregates && !hasWindowFunction)
		{
			return true;
		}
	}

	return false;
}


/*
 * ExpandWorkerTargetEntry is a utility function which processes the
 * expressions that are intended to be added to the worker target list.
 *
 * In summary, the function gets a list of expressions, converts them to target
 * entries and updates all the necessary fields such that the expression is correctly
 * added to the worker query's target list.
 *
 * Inputs:
 *  - expressionList: The list of expressions that should be added to the worker query's
 *                    target list.
 *  - originalTargetEntry: Target entry that the expressionList generated for. NULL
 *                         if the expressionList is not generated from any target entry.
 *  - addToGroupByClause: True if the expressionList should also be added to the
 *                        worker query's GROUP BY clause.
 */
static void
ExpandWorkerTargetEntry(List *expressionList, TargetEntry *originalTargetEntry,
						bool addToGroupByClause, QueryTargetList *queryTargetList,
						QueryGroupClause *queryGroupClause)
{
	ListCell *newExpressionCell = NULL;

	/* now create target entries for each new expression */
	foreach(newExpressionCell, expressionList)
	{
		Expr *newExpression = (Expr *) lfirst(newExpressionCell);
		TargetEntry *newTargetEntry = NULL;

		/* generate and add the new target entry to the target list */
		newTargetEntry =
			GenerateWorkerTargetEntry(originalTargetEntry, newExpression,
									  queryTargetList->targetProjectionNumber);
		(queryTargetList->targetProjectionNumber)++;
		queryTargetList->targetEntryList =
			lappend(queryTargetList->targetEntryList, newTargetEntry);

		/*
		 * Detect new targets of type Var and add it to group clause list.
		 * This case is expected only if the target entry has aggregates and
		 * it is inside a repartitioned subquery. We create group by entry
		 * for each Var in target list. This code does not check if this
		 * Var was already in the target list or in group by clauses.
		 */
		if (IsA(newExpression, Var) && addToGroupByClause)
		{
			AppendTargetEntryToGroupClause(newTargetEntry, queryGroupClause);
		}
	}
}


/*
 * GetNextSortGroupRef gets a target list entry and returns
 * the next ressortgroupref that should be used based on the
 * input target list.
 */
static Index
GetNextSortGroupRef(List *targetEntryList)
{
	ListCell *targetEntryCell = NULL;
	Index nextSortGroupRefIndex = 0;

	/* find max of sort group ref index */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		if (targetEntry->ressortgroupref > nextSortGroupRefIndex)
		{
			nextSortGroupRefIndex = targetEntry->ressortgroupref;
		}
	}

	/* next group ref index starts from max group ref index + 1 */
	nextSortGroupRefIndex++;

	return nextSortGroupRefIndex;
}


/*
 * GenerateWorkerTargetEntry is a simple utility function which gets a
 * target entry, an expression and a targetProjectionNumber.
 *
 * The function returns a newly allocated target entry which can be added
 * to the worker's target list.
 */
static TargetEntry *
GenerateWorkerTargetEntry(TargetEntry *targetEntry, Expr *workerExpression,
						  AttrNumber targetProjectionNumber)
{
	TargetEntry *newTargetEntry = NULL;

	/*
	 * If a target entry is already provided, use a copy of
	 * it because some of the callers rely on resorigtbl and
	 * resorigcol.
	 */
	if (targetEntry)
	{
		newTargetEntry = copyObject(targetEntry);
	}
	else
	{
		newTargetEntry = makeNode(TargetEntry);
	}

	if (newTargetEntry->resname == NULL)
	{
		StringInfo columnNameString = makeStringInfo();

		appendStringInfo(columnNameString, WORKER_COLUMN_FORMAT,
						 targetProjectionNumber);

		newTargetEntry->resname = columnNameString->data;
	}

	/* we can generate a target entry without any expressions */
	Assert(workerExpression != NULL);

	/* force resjunk to false as we may need this on the master */
	newTargetEntry->expr = workerExpression;
	newTargetEntry->resjunk = false;
	newTargetEntry->resno = targetProjectionNumber;

	return newTargetEntry;
}


/*
 * AppendTargetEntryToGroupClause gets a target entry, pointer to group list
 * and the ressortgroupref index.
 *
 * The function modifies all of the three input such that the target entry is
 * appended to the group clause and the index is incremented by one.
 */
static void
AppendTargetEntryToGroupClause(TargetEntry *targetEntry,
							   QueryGroupClause *queryGroupClause)
{
	Expr *targetExpr PG_USED_FOR_ASSERTS_ONLY = targetEntry->expr;
	Var *targetColumn = NULL;
	SortGroupClause *groupByClause = NULL;

	/* we currently only support appending Var target entries */
	AssertArg(IsA(targetExpr, Var));

	targetColumn = (Var *) targetEntry->expr;
	groupByClause = CreateSortGroupClause(targetColumn);

	/* the target entry should have an index */
	targetEntry->ressortgroupref = *queryGroupClause->nextSortGroupRefIndex;

	/* the group by clause entry should point to the correct index in the target list */
	groupByClause->tleSortGroupRef = *queryGroupClause->nextSortGroupRefIndex;

	/* update the group by list and the index's value */
	queryGroupClause->groupClauseList =
		lappend(queryGroupClause->groupClauseList, groupByClause);
	(*queryGroupClause->nextSortGroupRefIndex)++;
}


/*
 * WorkerAggregateWalker walks over the original target entry expression, and
 * creates the list of expression trees (potentially more than one) to execute
 * on the worker nodes. The function creates new expressions for aggregates and
 * columns; and recurses into expression_tree_walker() for all other expression
 * types.
 */
static bool
WorkerAggregateWalker(Node *node, WorkerAggregateWalkerContext *walkerContext)
{
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Aggref))
	{
		Aggref *originalAggregate = (Aggref *) node;
		List *workerAggregateList = WorkerAggregateExpressionList(originalAggregate,
																  walkerContext);

		walkerContext->expressionList = list_concat(walkerContext->expressionList,
													workerAggregateList);
	}
	else if (IsA(node, Var))
	{
		Var *originalColumn = (Var *) node;
		walkerContext->expressionList = lappend(walkerContext->expressionList,
												originalColumn);
	}
	else
	{
		walkerResult = expression_tree_walker(node, WorkerAggregateWalker,
											  (void *) walkerContext);
	}

	return walkerResult;
}


/*
 * WorkerAggregateExpressionList takes in the original aggregate function, and
 * determines the transformed aggregate functions to execute on worker nodes.
 * The function then returns these aggregates in a list. It also creates
 * group by clauses for newly added targets to be placed in the extended operator
 * node.
 */
static List *
WorkerAggregateExpressionList(Aggref *originalAggregate,
							  WorkerAggregateWalkerContext *walkerContext)
{
	AggregateType aggregateType = GetAggregateType(originalAggregate->aggfnoid);
	List *workerAggregateList = NIL;
	AggClauseCosts aggregateCosts;
	HeapTuple aggTuple;
	Form_pg_aggregate aggform;
	Oid combine;

	if (aggregateType == AGGREGATE_COUNT && originalAggregate->aggdistinct &&
		CountDistinctErrorRate == DISABLE_DISTINCT_APPROXIMATION &&
		walkerContext->pullDistinctColumns)
	{
		Aggref *aggregate = (Aggref *) copyObject(originalAggregate);
		List *columnList = pull_var_clause_default((Node *) aggregate);
		ListCell *columnCell = NULL;
		foreach(columnCell, columnList)
		{
			Var *column = (Var *) lfirst(columnCell);
			workerAggregateList = list_append_unique(workerAggregateList, column);
		}

		walkerContext->createGroupByClause = true;
	}
	else if (aggregateType == AGGREGATE_COUNT && originalAggregate->aggdistinct &&
			 CountDistinctErrorRate != DISABLE_DISTINCT_APPROXIMATION)
	{
		/*
		 * If the original aggregate is a count(distinct) approximation, we want
		 * to compute hll_add_agg(hll_hash(var), storageSize) on worker nodes.
		 */
		const AttrNumber firstArgumentId = 1;
		const AttrNumber secondArgumentId = 2;
		const int hashArgumentCount = 2;
		const int addArgumentCount = 2;

		TargetEntry *hashedColumnArgument = NULL;
		TargetEntry *storageSizeArgument = NULL;
		List *addAggregateArgumentList = NIL;
		Aggref *addAggregateFunction = NULL;

		/* init hll_hash() related variables */
		Oid argumentType = AggregateArgumentType(originalAggregate);
		TargetEntry *argument = (TargetEntry *) linitial(originalAggregate->args);
		Expr *argumentExpression = copyObject(argument->expr);

		/* extract schema name of hll */
		Oid hllId = get_extension_oid(HLL_EXTENSION_NAME, false);
		Oid hllSchemaOid = get_extension_schema(hllId);
		const char *hllSchemaName = get_namespace_name(hllSchemaOid);

		const char *hashFunctionName = CountDistinctHashFunctionName(argumentType);
		Oid hashFunctionId = FunctionOid(hllSchemaName, hashFunctionName,
										 hashArgumentCount);
		Oid hashFunctionReturnType = get_func_rettype(hashFunctionId);

		/* init hll_add_agg() related variables */
		Oid addFunctionId = FunctionOid(hllSchemaName, HLL_ADD_AGGREGATE_NAME,
										addArgumentCount);
		Oid hllType = TypeOid(hllSchemaOid, HLL_TYPE_NAME);
		int logOfStorageSize = CountDistinctStorageSize(CountDistinctErrorRate);
		Const *logOfStorageSizeConst = MakeIntegerConst(logOfStorageSize);

		/* construct hll_hash() expression */
		FuncExpr *hashFunction = makeNode(FuncExpr);
		hashFunction->funcid = hashFunctionId;
		hashFunction->funcresulttype = hashFunctionReturnType;
		hashFunction->args = list_make1(argumentExpression);

		/* construct hll_add_agg() expression */
		hashedColumnArgument = makeTargetEntry((Expr *) hashFunction,
											   firstArgumentId, NULL, false);
		storageSizeArgument = makeTargetEntry((Expr *) logOfStorageSizeConst,
											  secondArgumentId, NULL, false);
		addAggregateArgumentList = list_make2(hashedColumnArgument, storageSizeArgument);

		addAggregateFunction = makeNode(Aggref);
		addAggregateFunction->aggfnoid = addFunctionId;
		addAggregateFunction->aggtype = hllType;
		addAggregateFunction->args = addAggregateArgumentList;
		addAggregateFunction->aggkind = AGGKIND_NORMAL;
		addAggregateFunction->aggfilter = (Expr *) copyObject(
			originalAggregate->aggfilter);

		workerAggregateList = lappend(workerAggregateList, addAggregateFunction);
	}
	else if (aggregateType == AGGREGATE_AVERAGE)
	{
		/*
		 * If the original aggregate is an average, we want to compute sum(var)
		 * and count(var) on worker nodes.
		 */
		Aggref *sumAggregate = copyObject(originalAggregate);
		Aggref *countAggregate = copyObject(originalAggregate);

		/* extract function names for sum and count */
		const char *sumAggregateName = AggregateNames[AGGREGATE_SUM];
		const char *countAggregateName = AggregateNames[AGGREGATE_COUNT];

		/*
		 * Find the type of the expression over which we execute the aggregate.
		 * We then need to find the right sum function for that type.
		 */
		Oid argumentType = AggregateArgumentType(originalAggregate);

		/* find function implementing sum over the original type */
		sumAggregate->aggfnoid = AggregateFunctionOid(sumAggregateName, argumentType);
		sumAggregate->aggtype = get_func_rettype(sumAggregate->aggfnoid);

		sumAggregate->aggtranstype = InvalidOid;
		sumAggregate->aggargtypes = list_make1_oid(argumentType);
		sumAggregate->aggsplit = AGGSPLIT_SIMPLE;

		/* count has any input type */
		countAggregate->aggfnoid = AggregateFunctionOid(countAggregateName, ANYOID);
		countAggregate->aggtype = get_func_rettype(countAggregate->aggfnoid);
		countAggregate->aggtranstype = InvalidOid;
		countAggregate->aggargtypes = list_make1_oid(argumentType);
		countAggregate->aggsplit = AGGSPLIT_SIMPLE;

		workerAggregateList = lappend(workerAggregateList, sumAggregate);
		workerAggregateList = lappend(workerAggregateList, countAggregate);
	}
	else if (aggregateType == AGGREGATE_CUSTOM)
	{
		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(originalAggregate->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
		{
			elog(WARNING, "citus cache lookup failed for aggregate %u",
				 originalAggregate->aggfnoid);
			combine = InvalidOid;
		}
		else
		{
			aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
			combine = aggform->aggcombinefn;
			ReleaseSysCache(aggTuple);
		}

		if (combine != InvalidOid)
		{
			Const *aggparam = NULL;
			Aggref *newWorkerAggregate = NULL;
			List *aggArguments = NIL;
			ListCell *originalAggArgCell;
			Oid workerPartialId = AggregateFunctionOidWithoutInput(
				WORKER_PARTIAL_AGGREGATE_NAME);

			aggparam = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid), ObjectIdGetDatum(
									 originalAggregate->aggfnoid), false, true);
			aggArguments = list_make1(makeTargetEntry((Expr *) aggparam, 1, NULL, false));
			foreach(originalAggArgCell, originalAggregate->args)
			{
				TargetEntry *arg = lfirst(originalAggArgCell);
				TargetEntry *newArg = copyObject(arg);
				newArg->resno++;
				aggArguments = lappend(aggArguments, newArg);
			}

			/* worker_partial_agg(agg, ...args) */
			newWorkerAggregate = makeNode(Aggref);
			newWorkerAggregate->aggfnoid = workerPartialId;
			newWorkerAggregate->aggtype = BYTEAOID;
			newWorkerAggregate->args = aggArguments;
			newWorkerAggregate->aggkind = AGGKIND_NORMAL;
			newWorkerAggregate->aggfilter = originalAggregate->aggfilter;
			newWorkerAggregate->aggtranstype = INTERNALOID;
			newWorkerAggregate->aggargtypes = list_concat(list_make1_oid(OIDOID),
														  originalAggregate->aggargtypes);
			newWorkerAggregate->aggsplit = AGGSPLIT_SIMPLE;

			workerAggregateList = list_make1(newWorkerAggregate);
		}
	}
	else
	{
		/*
		 * All other aggregates are sent as they are to the worker nodes.
		 */
		Aggref *workerAggregate = copyObject(originalAggregate);
		workerAggregateList = lappend(workerAggregateList, workerAggregate);
	}


	/* Run AggRefs through cost machinery to mark required fields sanely */
	memset(&aggregateCosts, 0, sizeof(aggregateCosts));

	get_agg_clause_costs(NULL, (Node *) workerAggregateList, AGGSPLIT_SIMPLE,
						 &aggregateCosts);

	return workerAggregateList;
}


/*
 * GetAggregateType scans pg_catalog.pg_proc for the given aggregate oid, and
 * finds the aggregate's name. The function then matches the aggregate's name to
 * previously stored strings, and returns the appropriate aggregate type.
 */
static AggregateType
GetAggregateType(Oid aggFunctionId)
{
	char *aggregateProcName = NULL;
	uint32 aggregateCount = 0;
	uint32 aggregateIndex = 0;
	bool found = false;

	/* look up the function name */
	aggregateProcName = get_func_name(aggFunctionId);
	if (aggregateProcName == NULL)
	{
		ereport(ERROR, (errmsg("citus cache lookup failed for function %u",
							   aggFunctionId)));
	}

	if (AggregateEnabledCustom(aggregateProcName))
	{
		return AGGREGATE_CUSTOM;
	}

	aggregateCount = lengthof(AggregateNames);

	Assert(AGGREGATE_INVALID_FIRST == 0);

	for (aggregateIndex = 1; aggregateIndex < aggregateCount; aggregateIndex++)
	{
		const char *aggregateName = AggregateNames[aggregateIndex];
		if (strncmp(aggregateName, aggregateProcName, NAMEDATALEN) == 0)
		{
			found = true;
			break;
		}
	}

	if (!found)
	{
		ereport(ERROR, (errmsg("unsupported aggregate function %s", aggregateProcName)));
	}

	return aggregateIndex;
}


/* Extracts the type of the argument over which the aggregate is operating. */
static Oid
AggregateArgumentType(Aggref *aggregate)
{
	List *argumentList = aggregate->args;
	TargetEntry *argument = (TargetEntry *) linitial(argumentList);
	Oid returnTypeId = exprType((Node *) argument->expr);

	/* Here we currently support aggregates with only one argument; assert that. */
	Assert(list_length(argumentList) == 1);

	return returnTypeId;
}


static bool
AggregateEnabledCustom(const char *functionName)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	bool enabled = false;
	HeapTuple heapTuple = NULL;
	Relation pgDistEnabledCustomAggregates = NULL;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_enabled_custom_aggregates_name,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(functionName));

	pgDistEnabledCustomAggregates = heap_open(DistEnabledCustomAggregatesId(),
											  AccessShareLock);

	scanDescriptor = systable_beginscan(pgDistEnabledCustomAggregates, InvalidOid, false,
										NULL, 1, scanKey);

	heapTuple = systable_getnext(scanDescriptor);

	enabled = HeapTupleIsValid(heapTuple);

	systable_endscan(scanDescriptor);
	heap_close(pgDistEnabledCustomAggregates, AccessShareLock);

	return enabled;
}


/*
 * AggregateFunctionOid performs a reverse lookup on aggregate function name,
 * and returns the corresponding aggregate function oid for the given function
 * name and input type.
 */
static Oid
AggregateFunctionOid(const char *functionName, Oid inputType)
{
	Oid functionOid = InvalidOid;
	Relation procRelation = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	procRelation = heap_open(ProcedureRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_proc_proname,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(functionName));

	scanDescriptor = systable_beginscan(procRelation,
										ProcedureNameArgsNspIndexId, true,
										NULL, scanKeyCount, scanKey);

	/* loop until we find the right function */
	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(heapTuple);
		int argumentCount = procForm->pronargs;

		if (argumentCount == 1)
		{
			/* check if input type and found value type match */
			if (procForm->proargtypes.values[0] == inputType)
			{
#if PG_VERSION_NUM < 120000
				functionOid = HeapTupleGetOid(heapTuple);
#else
				functionOid = procForm->oid;
#endif
				break;
			}
		}
		Assert(argumentCount <= 1);

		heapTuple = systable_getnext(scanDescriptor);
	}

	if (functionOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("no matching oid for function: %s", functionName)));
	}

	systable_endscan(scanDescriptor);
	heap_close(procRelation, AccessShareLock);

	return functionOid;
}


/*
 * AggregateFunctionOid performs a reverse lookup on aggregate function name,
 * and returns the corresponding aggregate function oid for the given function
 * name and input type.
 */
static Oid
AggregateFunctionOidWithoutInput(const char *functionName)
{
	Oid functionOid = InvalidOid;
	Relation procRelation = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	procRelation = heap_open(ProcedureRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_proc_proname,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(functionName));

	scanDescriptor = systable_beginscan(procRelation,
										ProcedureNameArgsNspIndexId, true,
										NULL, scanKeyCount, scanKey);

	/* loop until we find the right function */
	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
#if PG_VERSION_NUM < 120000
		functionOid = HeapTupleGetOid(heapTuple);
#else
		Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(heapTuple);
		functionOid = procForm->oid;
#endif
	}

	if (functionOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("no matching oid for function: %s", functionName)));
	}

	systable_endscan(scanDescriptor);
	heap_close(procRelation, AccessShareLock);

	return functionOid;
}


/*
 * TypeOid looks for a type that has the given name and schema, and returns the
 * corresponding type's oid.
 */
static Oid
TypeOid(Oid schemaId, const char *typeName)
{
	Oid typeOid;

	typeOid = GetSysCacheOid2Compat(TYPENAMENSP, Anum_pg_type_oid,
									PointerGetDatum(typeName),
									ObjectIdGetDatum(schemaId));

	return typeOid;
}


/*
 * CreateSortGroupClause creates SortGroupClause for a given column Var.
 * The caller should set tleSortGroupRef field and respective
 * TargetEntry->ressortgroupref fields to appropriate SortGroupRefIndex.
 */
static SortGroupClause *
CreateSortGroupClause(Var *column)
{
	Oid lessThanOperator = InvalidOid;
	Oid equalsOperator = InvalidOid;
	bool hashable = false;
	SortGroupClause *groupByClause = makeNode(SortGroupClause);

	get_sort_group_operators(column->vartype, true, true, true,
							 &lessThanOperator, &equalsOperator, NULL,
							 &hashable);
	groupByClause->eqop = equalsOperator;
	groupByClause->hashable = hashable;
	groupByClause->nulls_first = false;
	groupByClause->sortop = lessThanOperator;

	return groupByClause;
}


/*
 * CountDistinctHashFunctionName resolves the hll_hash function name to use for
 * the given input type, and returns this function name.
 */
static const char *
CountDistinctHashFunctionName(Oid argumentType)
{
	/* resolve hash function name based on input argument type */
	switch (argumentType)
	{
		case INT4OID:
		{
			return HLL_HASH_INTEGER_FUNC_NAME;
		}

		case INT8OID:
		{
			return HLL_HASH_BIGINT_FUNC_NAME;
		}

		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		{
			return HLL_HASH_TEXT_FUNC_NAME;
		}

		default:
		{
			return HLL_HASH_ANY_FUNC_NAME;
		}
	}
}


/*
 * CountDistinctStorageSize takes in the desired precision for count distinct
 * approximations, and returns the log-base-2 of storage space needed for the
 * HyperLogLog algorithm.
 */
static int
CountDistinctStorageSize(double approximationErrorRate)
{
	double desiredStorageSize = pow((1.04 / approximationErrorRate), 2);
	double logOfDesiredStorageSize = log(desiredStorageSize) / log(2);

	/* keep log2(storage size) inside allowed range */
	int logOfStorageSize = (int) rint(logOfDesiredStorageSize);
	if (logOfStorageSize < 4)
	{
		logOfStorageSize = 4;
	}
	else if (logOfStorageSize > 17)
	{
		logOfStorageSize = 17;
	}

	return logOfStorageSize;
}


/* Makes an integer constant node from the given value, and returns that node. */
static Const *
MakeIntegerConst(int32 integerValue)
{
	const int typeCollationId = get_typcollation(INT4OID);
	const int16 typeLength = get_typlen(INT4OID);
	const int32 typeModifier = -1;
	const bool typeIsNull = false;
	const bool typePassByValue = true;

	Datum integerDatum = Int32GetDatum(integerValue);
	Const *integerConst = makeConst(INT4OID, typeModifier, typeCollationId, typeLength,
									integerDatum, typeIsNull, typePassByValue);

	return integerConst;
}


/* Makes a 64-bit integer constant node from the given value, and returns that node. */
static Const *
MakeIntegerConstInt64(int64 integerValue)
{
	const int typeCollationId = get_typcollation(INT8OID);
	const int16 typeLength = get_typlen(INT8OID);
	const int32 typeModifier = -1;
	const bool typeIsNull = false;
	const bool typePassByValue = true;

	Datum integer64Datum = Int64GetDatum(integerValue);
	Const *integer64Const = makeConst(INT8OID, typeModifier, typeCollationId, typeLength,
									  integer64Datum, typeIsNull, typePassByValue);

	return integer64Const;
}


/*
 * ErrorIfContainsUnsupportedAggregate extracts aggregate expressions from the
 * logical plan, walks over them and uses helper functions to check if we can
 * transform these aggregate expressions and push them down to worker nodes.
 * These helper functions error out if we cannot transform the aggregates.
 */
static void
ErrorIfContainsUnsupportedAggregate(MultiNode *logicalPlanNode)
{
	List *opNodeList = FindNodesOfType(logicalPlanNode, T_MultiExtendedOp);
	MultiExtendedOp *extendedOpNode = (MultiExtendedOp *) linitial(opNodeList);

	List *targetList = extendedOpNode->targetList;

	/*
	 * PVC_REJECT_PLACEHOLDERS is implicit if PVC_INCLUDE_PLACEHOLDERS isn't
	 * specified.
	 */
	List *expressionList = pull_var_clause((Node *) targetList, PVC_INCLUDE_AGGREGATES |
										   PVC_INCLUDE_WINDOWFUNCS);

	ListCell *expressionCell = NULL;
	foreach(expressionCell, expressionList)
	{
		Node *expression = (Node *) lfirst(expressionCell);
		Aggref *aggregateExpression = NULL;
		AggregateType aggregateType = AGGREGATE_INVALID_FIRST;

		/* only consider aggregate expressions */
		if (!IsA(expression, Aggref))
		{
			continue;
		}

		/* GetAggregateType errors out on unsupported aggregate types */
		aggregateExpression = (Aggref *) expression;
		aggregateType = GetAggregateType(aggregateExpression->aggfnoid);
		Assert(aggregateType != AGGREGATE_INVALID_FIRST);

		/*
		 * Check that we can transform the current aggregate expression. These
		 * functions error out on unsupported array_agg and aggregate (distinct)
		 * clauses.
		 */
		if (aggregateType == AGGREGATE_ARRAY_AGG)
		{
			ErrorIfUnsupportedArrayAggregate(aggregateExpression);
		}
		else if (aggregateType == AGGREGATE_JSONB_AGG ||
				 aggregateType == AGGREGATE_JSON_AGG)
		{
			ErrorIfUnsupportedJsonAggregate(aggregateType, aggregateExpression);
		}
		else if (aggregateType == AGGREGATE_JSONB_OBJECT_AGG ||
				 aggregateType == AGGREGATE_JSON_OBJECT_AGG)
		{
			ErrorIfUnsupportedJsonObjectAggregate(aggregateType, aggregateExpression);
		}
		else if (aggregateExpression->aggdistinct)
		{
			ErrorIfUnsupportedAggregateDistinct(aggregateExpression, logicalPlanNode);
		}
	}
}


/*
 * ErrorIfUnsupportedArrayAggregate checks if we can transform the array aggregate
 * expression and push it down to the worker node. If we cannot transform the
 * aggregate, this function errors.
 */
static void
ErrorIfUnsupportedArrayAggregate(Aggref *arrayAggregateExpression)
{
	/* if array_agg has order by, we error out */
	if (arrayAggregateExpression->aggorder)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("array_agg with order by is unsupported")));
	}

	/* if array_agg has distinct, we error out */
	if (arrayAggregateExpression->aggdistinct)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("array_agg (distinct) is unsupported")));
	}
}


/*
 * ErrorIfUnsupportedJsonAggregate checks if we can transform the json
 * aggregate expression and push it down to the worker node. If we cannot
 * transform the aggregate, this function errors.
 */
static void
ErrorIfUnsupportedJsonAggregate(AggregateType type,
								Aggref *aggregateExpression)
{
	/* if json aggregate has order by, we error out */
	if (aggregateExpression->aggorder)
	{
		const char *name = AggregateNames[type];
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s with order by is unsupported", name)));
	}

	/* if json aggregate has distinct, we error out */
	if (aggregateExpression->aggdistinct)
	{
		const char *name = AggregateNames[type];
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s (distinct) is unsupported", name)));
	}
}


/*
 * ErrorIfUnsupportedJsonObjectAggregate checks if we can transform the
 * json object aggregate expression and push it down to the worker node.
 * If we cannot transform the aggregate, this function errors.
 */
static void
ErrorIfUnsupportedJsonObjectAggregate(AggregateType type,
									  Aggref *aggregateExpression)
{
	/* if json object aggregate has order by, we error out */
	if (aggregateExpression->aggorder)
	{
		const char *name = AggregateNames[type];
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s with order by is unsupported", name)));
	}

	/* if json object aggregate has distinct, we error out */
	if (aggregateExpression->aggdistinct)
	{
		const char *name = AggregateNames[type];
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s (distinct) is unsupported", name)));
	}
}


/*
 * ErrorIfUnsupportedAggregateDistinct checks if we can transform the aggregate
 * (distinct expression) and push it down to the worker node. It handles count
 * (distinct) separately to check if we can use distinct approximations. If we
 * cannot transform the aggregate, this function errors.
 */
static void
ErrorIfUnsupportedAggregateDistinct(Aggref *aggregateExpression,
									MultiNode *logicalPlanNode)
{
	char *errorDetail = NULL;
	bool distinctSupported = true;
	List *repartitionNodeList = NIL;
	Var *distinctColumn = NULL;
	List *tableNodeList = NIL;
	List *extendedOpNodeList = NIL;
	MultiExtendedOp *extendedOpNode = NULL;

	AggregateType aggregateType = GetAggregateType(aggregateExpression->aggfnoid);

	/*
	 * We partially support count(distinct) in subqueries, other distinct aggregates in
	 * subqueries are not supported yet.
	 */
	if (aggregateType == AGGREGATE_COUNT)
	{
		Node *aggregateArgument = (Node *) linitial(aggregateExpression->args);
		List *columnList = pull_var_clause_default(aggregateArgument);
		ListCell *columnCell = NULL;
		foreach(columnCell, columnList)
		{
			Var *column = (Var *) lfirst(columnCell);
			if (column->varattno <= 0)
			{
				ereport(ERROR, (errmsg("cannot compute count (distinct)"),
								errdetail("Non-column references are not supported "
										  "yet")));
			}
		}
	}
	else
	{
		List *multiTableNodeList = FindNodesOfType(logicalPlanNode, T_MultiTable);
		ListCell *multiTableNodeCell = NULL;
		foreach(multiTableNodeCell, multiTableNodeList)
		{
			MultiTable *multiTable = (MultiTable *) lfirst(multiTableNodeCell);
			if (multiTable->relationId == SUBQUERY_RELATION_ID ||
				multiTable->relationId == SUBQUERY_PUSHDOWN_RELATION_ID)
			{
				ereport(ERROR, (errmsg("cannot compute aggregate (distinct)"),
								errdetail("Only count(distinct) aggregate is "
										  "supported in subqueries")));
			}
		}
	}

	/* if we have a count(distinct), and distinct approximation is enabled */
	if (aggregateType == AGGREGATE_COUNT &&
		CountDistinctErrorRate != DISABLE_DISTINCT_APPROXIMATION)
	{
		bool missingOK = true;
		Oid distinctExtensionId = get_extension_oid(HLL_EXTENSION_NAME, missingOK);

		/* if extension for distinct approximation is loaded, we are good */
		if (distinctExtensionId != InvalidOid)
		{
			return;
		}
		else
		{
			ereport(ERROR, (errmsg("cannot compute count (distinct) approximation"),
							errhint("You need to have the hll extension loaded.")));
		}
	}

	if (aggregateType == AGGREGATE_COUNT)
	{
		List *aggregateVarList = pull_var_clause_default((Node *) aggregateExpression);
		if (aggregateVarList == NIL)
		{
			distinctSupported = false;
			errorDetail = "aggregate (distinct) with no columns is unsupported";
		}
	}

	repartitionNodeList = FindNodesOfType(logicalPlanNode, T_MultiPartition);
	if (repartitionNodeList != NIL)
	{
		distinctSupported = false;
		errorDetail = "aggregate (distinct) with table repartitioning is unsupported";
	}

	tableNodeList = FindNodesOfType(logicalPlanNode, T_MultiTable);
	extendedOpNodeList = FindNodesOfType(logicalPlanNode, T_MultiExtendedOp);
	extendedOpNode = (MultiExtendedOp *) linitial(extendedOpNodeList);

	distinctColumn = AggregateDistinctColumn(aggregateExpression);
	if (distinctSupported)
	{
		if (distinctColumn == NULL)
		{
			/*
			 * If the query has a single table, and table is grouped by partition
			 * column, then we support count distincts even distinct column can
			 * not be identified.
			 */
			distinctSupported = TablePartitioningSupportsDistinct(tableNodeList,
																  extendedOpNode,
																  distinctColumn,
																  aggregateType);
			if (!distinctSupported)
			{
				errorDetail = "aggregate (distinct) on complex expressions is"
							  " unsupported";
			}
		}
		else if (aggregateType != AGGREGATE_COUNT)
		{
			bool supports = TablePartitioningSupportsDistinct(tableNodeList,
															  extendedOpNode,
															  distinctColumn,
															  aggregateType);
			if (!supports)
			{
				distinctSupported = false;
				errorDetail = "table partitioning is unsuitable for aggregate (distinct)";
			}
		}
	}

	/* if current aggregate expression isn't supported, error out */
	if (!distinctSupported)
	{
		if (aggregateType == AGGREGATE_COUNT)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot compute aggregate (distinct)"),
							errdetail("%s", errorDetail),
							errhint("You can load the hll extension from contrib "
									"packages and enable distinct approximations.")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot compute aggregate (distinct)"),
							errdetail("%s", errorDetail)));
		}
	}
}


/*
 * AggregateDistinctColumn checks if the given aggregate expression's distinct
 * clause is on a single column. If it is, the function finds and returns that
 * column. Otherwise, the function returns null.
 * The function expects to find a single column here, no FieldSelect or other
 * expressions are accepted as a column.
 */
static Var *
AggregateDistinctColumn(Aggref *aggregateExpression)
{
	Var *aggregateColumn = NULL;
	int aggregateArgumentCount = 0;
	TargetEntry *aggregateTargetEntry = NULL;

	/* only consider aggregates with distincts */
	if (!aggregateExpression->aggdistinct)
	{
		return NULL;
	}

	aggregateArgumentCount = list_length(aggregateExpression->args);
	if (aggregateArgumentCount != 1)
	{
		return NULL;
	}

	aggregateTargetEntry = (TargetEntry *) linitial(aggregateExpression->args);
	if (!IsA(aggregateTargetEntry->expr, Var))
	{
		return NULL;
	}

	aggregateColumn = (Var *) aggregateTargetEntry->expr;
	return aggregateColumn;
}


/*
 * TablePartitioningSupportsDistinct walks over all tables in the given list and
 * checks that each table's partitioning method is suitable for pushing down an
 * aggregate (distinct) expression to worker nodes. For this, the function needs
 * to check that task results do not overlap with one another on the distinct
 * column.
 */
static bool
TablePartitioningSupportsDistinct(List *tableNodeList, MultiExtendedOp *opNode,
								  Var *distinctColumn, AggregateType aggregateType)
{
	bool distinctSupported = true;
	ListCell *tableNodeCell = NULL;

	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = (MultiTable *) lfirst(tableNodeCell);
		Oid relationId = tableNode->relationId;
		bool tableDistinctSupported = false;
		char partitionMethod = 0;
		List *shardList = NIL;

		if (relationId == SUBQUERY_RELATION_ID ||
			relationId == SUBQUERY_PUSHDOWN_RELATION_ID)
		{
			return true;
		}

		/* if table has one shard, task results don't overlap */
		shardList = LoadShardList(relationId);
		if (list_length(shardList) == 1)
		{
			continue;
		}

		/*
		 * We need to check that task results don't overlap. We can only do this
		 * if table is range partitioned.
		 */
		partitionMethod = PartitionMethod(relationId);

		if (partitionMethod == DISTRIBUTE_BY_RANGE ||
			partitionMethod == DISTRIBUTE_BY_HASH)
		{
			Var *tablePartitionColumn = tableNode->partitionColumn;
			bool groupedByPartitionColumn = false;

			if (aggregateType == AGGREGATE_COUNT)
			{
				tableDistinctSupported = true;
			}

			/* if distinct is on table partition column, we can push it down */
			if (distinctColumn != NULL &&
				tablePartitionColumn->varno == distinctColumn->varno &&
				tablePartitionColumn->varattno == distinctColumn->varattno)
			{
				tableDistinctSupported = true;
			}

			/* if results are grouped by partition column, we can push down */
			groupedByPartitionColumn = GroupedByColumn(opNode->groupClauseList,
													   opNode->targetList,
													   tablePartitionColumn);
			if (groupedByPartitionColumn)
			{
				tableDistinctSupported = true;
			}
		}

		if (!tableDistinctSupported)
		{
			distinctSupported = false;
			break;
		}
	}

	return distinctSupported;
}


/*
 * GroupedByColumn walks over group clauses in the given list, and checks if any
 * of the group clauses is on the given column.
 */
bool
GroupedByColumn(List *groupClauseList, List *targetList, Var *column)
{
	bool groupedByColumn = false;
	ListCell *groupClauseCell = NULL;

	foreach(groupClauseCell, groupClauseList)
	{
		SortGroupClause *groupClause = (SortGroupClause *) lfirst(groupClauseCell);
		TargetEntry *groupTargetEntry = get_sortgroupclause_tle(groupClause, targetList);

		Expr *groupExpression = (Expr *) groupTargetEntry->expr;
		if (IsA(groupExpression, Var))
		{
			Var *groupColumn = (Var *) groupExpression;
			if (groupColumn->varno == column->varno &&
				groupColumn->varattno == column->varattno)
			{
				groupedByColumn = true;
				break;
			}
		}
	}

	return groupedByColumn;
}


/*
 * SubqueryMultiTableList extracts multi tables in the given logical plan tree
 * and returns subquery multi tables in a new list.
 */
List *
SubqueryMultiTableList(MultiNode *multiNode)
{
	List *subqueryMultiTableList = NIL;
	List *multiTableNodeList = FindNodesOfType(multiNode, T_MultiTable);

	ListCell *multiTableNodeCell = NULL;
	foreach(multiTableNodeCell, multiTableNodeList)
	{
		MultiTable *multiTable = (MultiTable *) lfirst(multiTableNodeCell);
		Query *subquery = multiTable->subquery;

		if (subquery != NULL)
		{
			subqueryMultiTableList = lappend(subqueryMultiTableList, multiTable);
		}
	}

	return subqueryMultiTableList;
}


/*
 * GroupTargetEntryList walks over group clauses in the given list, finds
 * matching target entries and return them in a new list.
 */
List *
GroupTargetEntryList(List *groupClauseList, List *targetEntryList)
{
	List *groupTargetEntryList = NIL;
	ListCell *groupClauseCell = NULL;

	foreach(groupClauseCell, groupClauseList)
	{
		SortGroupClause *groupClause = (SortGroupClause *) lfirst(groupClauseCell);
		TargetEntry *groupTargetEntry =
			get_sortgroupclause_tle(groupClause, targetEntryList);
		groupTargetEntryList = lappend(groupTargetEntryList, groupTargetEntry);
	}

	return groupTargetEntryList;
}


/*
 * IsPartitionColumn returns true if the given column is a partition column.
 * The function uses FindReferencedTableColumn to find the original relation
 * id and column that the column expression refers to. It then checks whether
 * that column is a partition column of the relation.
 *
 * Also, the function returns always false for reference tables given that
 * reference tables do not have partition column. The function does not
 * support queries with CTEs, it would return false if columnExpression
 * refers to a column returned by a CTE.
 */
bool
IsPartitionColumn(Expr *columnExpression, Query *query)
{
	bool isPartitionColumn = false;
	Oid relationId = InvalidOid;
	Var *column = NULL;

	FindReferencedTableColumn(columnExpression, NIL, query, &relationId, &column);

	if (relationId != InvalidOid && column != NULL)
	{
		Var *partitionColumn = DistPartitionKey(relationId);

		/* not all distributed tables have partition column */
		if (partitionColumn != NULL && column->varattno == partitionColumn->varattno)
		{
			isPartitionColumn = true;
		}
	}

	return isPartitionColumn;
}


/*
 * FindReferencedTableColumn recursively traverses query tree to find actual relation
 * id, and column that columnExpression refers to. If columnExpression is a
 * non-relational or computed/derived expression, the function returns InvolidOid for
 * relationId and NULL for column. The caller should provide parent query list from
 * top of the tree to this particular Query's parent. This argument is used to look
 * into CTEs that may be present in the query.
 */
void
FindReferencedTableColumn(Expr *columnExpression, List *parentQueryList, Query *query,
						  Oid *relationId, Var **column)
{
	Var *candidateColumn = NULL;
	List *rangetableList = query->rtable;
	Index rangeTableEntryIndex = 0;
	RangeTblEntry *rangeTableEntry = NULL;
	Expr *strippedColumnExpression = (Expr *) strip_implicit_coercions(
		(Node *) columnExpression);

	*relationId = InvalidOid;
	*column = NULL;

	if (IsA(strippedColumnExpression, Var))
	{
		candidateColumn = (Var *) strippedColumnExpression;
	}
	else if (IsA(strippedColumnExpression, FieldSelect))
	{
		FieldSelect *compositeField = (FieldSelect *) strippedColumnExpression;
		Expr *fieldExpression = compositeField->arg;

		if (IsA(fieldExpression, Var))
		{
			candidateColumn = (Var *) fieldExpression;
		}
	}

	if (candidateColumn == NULL)
	{
		return;
	}

	/*
	 * We currently don't support finding partition keys in the subqueries
	 * that references from outer subqueries. For example, in corrolated
	 * subqueries in WHERE clause, we don't support use of partition keys
	 * in the subquery that is referred from the outer query.
	 */
	if (candidateColumn->varlevelsup > 0)
	{
		return;
	}

	rangeTableEntryIndex = candidateColumn->varno - 1;
	rangeTableEntry = list_nth(rangetableList, rangeTableEntryIndex);

	if (rangeTableEntry->rtekind == RTE_RELATION)
	{
		*relationId = rangeTableEntry->relid;
		*column = candidateColumn;
	}
	else if (rangeTableEntry->rtekind == RTE_SUBQUERY)
	{
		Query *subquery = rangeTableEntry->subquery;
		List *targetEntryList = subquery->targetList;
		AttrNumber targetEntryIndex = candidateColumn->varattno - 1;
		TargetEntry *subqueryTargetEntry = list_nth(targetEntryList, targetEntryIndex);
		Expr *subColumnExpression = subqueryTargetEntry->expr;

		/* append current query to parent query list */
		parentQueryList = lappend(parentQueryList, query);
		FindReferencedTableColumn(subColumnExpression, parentQueryList,
								  subquery, relationId, column);
	}
	else if (rangeTableEntry->rtekind == RTE_JOIN)
	{
		List *joinColumnList = rangeTableEntry->joinaliasvars;
		AttrNumber joinColumnIndex = candidateColumn->varattno - 1;
		Expr *joinColumn = list_nth(joinColumnList, joinColumnIndex);

		/* parent query list stays the same since still in the same query boundary */
		FindReferencedTableColumn(joinColumn, parentQueryList, query,
								  relationId, column);
	}
	else if (rangeTableEntry->rtekind == RTE_CTE)
	{
		int cteParentListIndex = list_length(parentQueryList) -
								 rangeTableEntry->ctelevelsup - 1;
		Query *cteParentQuery = NULL;
		List *cteList = NIL;
		ListCell *cteListCell = NULL;
		CommonTableExpr *cte = NULL;

		/*
		 * This should have been an error case, not marking it as error at the
		 * moment due to usage from IsPartitionColumn. Callers of that function
		 * do not have access to parent query list.
		 */
		if (cteParentListIndex >= 0)
		{
			cteParentQuery = list_nth(parentQueryList, cteParentListIndex);
			cteList = cteParentQuery->cteList;
		}

		foreach(cteListCell, cteList)
		{
			CommonTableExpr *candidateCte = (CommonTableExpr *) lfirst(cteListCell);
			if (strcmp(candidateCte->ctename, rangeTableEntry->ctename) == 0)
			{
				cte = candidateCte;
				break;
			}
		}

		if (cte != NULL)
		{
			Query *cteQuery = (Query *) cte->ctequery;
			List *targetEntryList = cteQuery->targetList;
			AttrNumber targetEntryIndex = candidateColumn->varattno - 1;
			TargetEntry *targetEntry = list_nth(targetEntryList, targetEntryIndex);

			parentQueryList = lappend(parentQueryList, query);
			FindReferencedTableColumn(targetEntry->expr, parentQueryList,
									  cteQuery, relationId, column);
		}
	}
}


/*
 * ExtractQueryWalker walks over a query, and finds all queries in the query
 * tree and returns these queries. Note that the function also recurses into
 * the subqueries in WHERE clause.
 */
bool
ExtractQueryWalker(Node *node, List **queryList)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		(*queryList) = lappend(*queryList, query);
		return query_tree_walker(query, ExtractQueryWalker, queryList, 0);
	}

	return expression_tree_walker(node, ExtractQueryWalker, queryList);
}


/*
 * WorkerLimitCount checks if the given input contains a valid limit node, and
 * if that node can be pushed down. For this, the function checks if this limit
 * count or a meaningful approximation of it can be pushed down to worker nodes.
 * If they can, the function returns the limit count.
 *
 * The limit push-down decision tree is as follows:
 *                                         group by?
 *                                       1/         \0
 *                       group by partition column?   (exact pd)
 *                              0/         \1
 *                          order by?        (exact pd)
 *                       1/           \0
 *           has order by agg?          (no pd)
 *            1/           \0
 *     can approximate?    (exact pd)
 *      1/       \0
 * (approx pd)   (no pd)
 *
 * When an offset is present, the offset value is added to limit because for a query
 * with LIMIT x OFFSET y, (x+y) records should be pulled from the workers.
 *
 * If no limit is present or can be pushed down, then WorkerLimitCount
 * returns null.
 */
static Node *
WorkerLimitCount(Node *limitCount, Node *limitOffset, OrderByLimitReference
				 orderByLimitReference)
{
	Node *workerLimitNode = NULL;
	bool canPushDownLimit = false;
	bool canApproximate = false;

	/* no limit node to push down */
	if (limitCount == NULL)
	{
		return NULL;
	}

	/*
	 * During subquery pushdown planning original query is used. In that case,
	 * certain expressions such as parameters are not evaluated and converted
	 * into Consts on the op node.
	 */
	Assert(IsA(limitCount, Const));
	Assert(limitOffset == NULL || IsA(limitOffset, Const));

	/*
	 * If we don't have group by clauses, or we have group by partition column,
	 * or if we have order by clauses without aggregates, we can push down the
	 * original limit. Else if we have order by clauses with commutative aggregates,
	 * we can push down approximate limits.
	 */
	if (orderByLimitReference.groupClauseIsEmpty ||
		orderByLimitReference.groupedByDisjointPartitionColumn)
	{
		canPushDownLimit = true;
	}
	else if (orderByLimitReference.sortClauseIsEmpty)
	{
		canPushDownLimit = false;
	}
	else if (!orderByLimitReference.hasOrderByAggregate)
	{
		canPushDownLimit = true;
	}
	else
	{
		canApproximate = orderByLimitReference.canApproximate;
	}

	/* create the workerLimitNode according to the decisions above */
	if (canPushDownLimit)
	{
		workerLimitNode = (Node *) copyObject(limitCount);
	}
	else if (canApproximate)
	{
		Const *workerLimitConst = (Const *) copyObject(limitCount);
		int64 workerLimitCount = (int64) LimitClauseRowFetchCount;
		workerLimitConst->constvalue = Int64GetDatum(workerLimitCount);

		workerLimitNode = (Node *) workerLimitConst;
	}

	/*
	 * If offset clause is present and limit can be pushed down (whether exactly or
	 * approximately), add the offset value to limit on workers
	 */
	if (workerLimitNode != NULL && limitOffset != NULL)
	{
		Const *workerLimitConst = (Const *) workerLimitNode;
		Const *workerOffsetConst = (Const *) limitOffset;
		int64 workerLimitCount = DatumGetInt64(workerLimitConst->constvalue);
		int64 workerOffsetCount = DatumGetInt64(workerOffsetConst->constvalue);

		workerLimitCount = workerLimitCount + workerOffsetCount;
		workerLimitNode = (Node *) MakeIntegerConstInt64(workerLimitCount);
	}

	/* display debug message on limit push down */
	if (workerLimitNode != NULL)
	{
		Const *workerLimitConst = (Const *) workerLimitNode;
		int64 workerLimitCount = DatumGetInt64(workerLimitConst->constvalue);

		ereport(DEBUG1, (errmsg("push down of limit count: " INT64_FORMAT,
								workerLimitCount)));
	}

	return workerLimitNode;
}


/*
 * WorkerSortClauseList first checks if the given input contains a limit
 * or hasDistinctOn that can be pushed down. If it does, the function then
 * checks if we need to add any sorting and grouping clauses to the sort list we
 * push down for the limit. If we do, the function adds these clauses and
 * returns them. Otherwise, the function returns null.
 */
static List *
WorkerSortClauseList(Node *limitCount, List *groupClauseList, List *sortClauseList,
					 OrderByLimitReference orderByLimitReference)
{
	List *workerSortClauseList = NIL;

	/* if no limit node and no hasDistinctOn, no need to push down sort clauses */
	if (limitCount == NULL && !orderByLimitReference.hasDistinctOn)
	{
		return NIL;
	}

	sortClauseList = copyObject(sortClauseList);

	/*
	 * If we are pushing down the limit, push down any order by clauses. Also if
	 * we are pushing down the limit because the order by clauses don't have any
	 * aggregates, add group by clauses to the order by list. We do this because
	 * rows that belong to the same grouping may appear in different "offsets"
	 * in different task results. By ordering on the group by clause, we ensure
	 * that query results are consistent.
	 */
	if (orderByLimitReference.groupClauseIsEmpty ||
		orderByLimitReference.groupedByDisjointPartitionColumn)
	{
		workerSortClauseList = sortClauseList;
	}
	else if (sortClauseList != NIL)
	{
		bool orderByNonAggregates = !orderByLimitReference.hasOrderByAggregate;
		bool canApproximate = orderByLimitReference.canApproximate;

		if (orderByNonAggregates)
		{
			workerSortClauseList = sortClauseList;
			workerSortClauseList = list_concat(workerSortClauseList, groupClauseList);
		}
		else if (canApproximate)
		{
			workerSortClauseList = sortClauseList;
		}
	}

	return workerSortClauseList;
}


/*
 * GenerateNewTargetEntriesForSortClauses goes over provided sort clause lists and
 * creates new target entries if needed to make sure sort clauses has correct
 * references. The function returns list of new target entries, caller is
 * responsible to add those target entries to the end of worker target list.
 *
 * The function is required because we change the target entry if it contains an
 * expression having an aggregate operation, or just the AVG aggregate.
 * Afterwards any order by clause referring to original target entry starts
 * to point to a wrong expression.
 *
 * Note the function modifies SortGroupClause items in sortClauseList,
 * targetProjectionNumber, and nextSortGroupRefIndex.
 */
static List *
GenerateNewTargetEntriesForSortClauses(List *originalTargetList,
									   List *sortClauseList,
									   AttrNumber *targetProjectionNumber,
									   Index *nextSortGroupRefIndex)
{
	List *createdTargetList = NIL;
	ListCell *sortClauseCell = NULL;

	foreach(sortClauseCell, sortClauseList)
	{
		SortGroupClause *sgClause = (SortGroupClause *) lfirst(sortClauseCell);
		TargetEntry *targetEntry = get_sortgroupclause_tle(sgClause, originalTargetList);
		Expr *targetExpr = targetEntry->expr;
		bool containsAggregate = contain_agg_clause((Node *) targetExpr);
		bool createNewTargetEntry = false;

		/* we are only interested in target entries containing aggregates */
		if (!containsAggregate)
		{
			continue;
		}

		/*
		 * If the target expression is not an Aggref, it is either an expression
		 * on a single aggregate, or expression containing multiple aggregates.
		 * Worker query mutates these target entries to have a naked target entry
		 * per aggregate function. We want to use original target entries if this
		 * the case.
		 * If the original target expression is an avg aggref, we also want to use
		 * original target entry.
		 */
		if (!IsA(targetExpr, Aggref))
		{
			createNewTargetEntry = true;
		}
		else
		{
			Aggref *aggNode = (Aggref *) targetExpr;
			AggregateType aggregateType = GetAggregateType(aggNode->aggfnoid);
			if (aggregateType == AGGREGATE_AVERAGE)
			{
				createNewTargetEntry = true;
			}
		}

		if (createNewTargetEntry)
		{
			bool resJunk = true;
			AttrNumber nextResNo = (*targetProjectionNumber);
			Expr *newExpr = copyObject(targetExpr);
			TargetEntry *newTargetEntry = makeTargetEntry(newExpr, nextResNo,
														  targetEntry->resname, resJunk);
			newTargetEntry->ressortgroupref = *nextSortGroupRefIndex;

			createdTargetList = lappend(createdTargetList, newTargetEntry);

			sgClause->tleSortGroupRef = *nextSortGroupRefIndex;

			(*nextSortGroupRefIndex)++;
			(*targetProjectionNumber)++;
		}
	}

	return createdTargetList;
}


/*
 * CanPushDownLimitApproximate checks if we can push down the limit clause to
 * the worker nodes, and get approximate and meaningful results. We can do this
 * only when: (1) the user has enabled the limit approximation and (2) the query
 * has order by clauses that are commutative.
 */
static bool
CanPushDownLimitApproximate(List *sortClauseList, List *targetList)
{
	bool canApproximate = false;

	/* user hasn't enabled the limit approximation */
	if (LimitClauseRowFetchCount == DISABLE_LIMIT_APPROXIMATION)
	{
		return false;
	}

	if (sortClauseList != NIL)
	{
		bool orderByAverage = HasOrderByAverage(sortClauseList, targetList);
		bool orderByComplex = HasOrderByComplexExpression(sortClauseList, targetList);

		/*
		 * If we don't have any order by average or any complex expressions with
		 * aggregates in them, we can meaningfully approximate.
		 */
		if (!orderByAverage && !orderByComplex)
		{
			canApproximate = true;
		}
	}

	return canApproximate;
}


/*
 * HasOrderByAggregate walks over the given order by clauses, and checks if we
 * have an order by an aggregate function. If we do, the function returns true.
 */
static bool
HasOrderByAggregate(List *sortClauseList, List *targetList)
{
	bool hasOrderByAggregate = false;
	ListCell *sortClauseCell = NULL;

	foreach(sortClauseCell, sortClauseList)
	{
		SortGroupClause *sortClause = (SortGroupClause *) lfirst(sortClauseCell);
		Node *sortExpression = get_sortgroupclause_expr(sortClause, targetList);

		bool containsAggregate = contain_agg_clause(sortExpression);
		if (containsAggregate)
		{
			hasOrderByAggregate = true;
			break;
		}
	}

	return hasOrderByAggregate;
}


/*
 * HasOrderByAverage walks over the given order by clauses, and checks if we
 * have an order by an average. If we do, the function returns true.
 */
static bool
HasOrderByAverage(List *sortClauseList, List *targetList)
{
	bool hasOrderByAverage = false;
	ListCell *sortClauseCell = NULL;

	foreach(sortClauseCell, sortClauseList)
	{
		SortGroupClause *sortClause = (SortGroupClause *) lfirst(sortClauseCell);
		Node *sortExpression = get_sortgroupclause_expr(sortClause, targetList);

		/* if sort expression is an aggregate, check its type */
		if (IsA(sortExpression, Aggref))
		{
			Aggref *aggregate = (Aggref *) sortExpression;

			AggregateType aggregateType = GetAggregateType(aggregate->aggfnoid);
			if (aggregateType == AGGREGATE_AVERAGE)
			{
				hasOrderByAverage = true;
				break;
			}
		}
	}

	return hasOrderByAverage;
}


/*
 * HasOrderByComplexExpression walks over the given order by clauses, and checks
 * if we have a nested expression that contains an aggregate function within it.
 * If we do, the function returns true.
 */
static bool
HasOrderByComplexExpression(List *sortClauseList, List *targetList)
{
	bool hasOrderByComplexExpression = false;
	ListCell *sortClauseCell = NULL;

	foreach(sortClauseCell, sortClauseList)
	{
		SortGroupClause *sortClause = (SortGroupClause *) lfirst(sortClauseCell);
		Node *sortExpression = get_sortgroupclause_expr(sortClause, targetList);
		bool nestedAggregate = false;

		/* simple aggregate functions are ok */
		if (IsA(sortExpression, Aggref))
		{
			continue;
		}

		nestedAggregate = contain_agg_clause(sortExpression);
		if (nestedAggregate)
		{
			hasOrderByComplexExpression = true;
			break;
		}
	}

	return hasOrderByComplexExpression;
}


/*
 * HasOrderByHllType walks over the given order by clauses, and checks if any of
 * those clauses operate on hll data type. If they do, the function returns true.
 */
static bool
HasOrderByHllType(List *sortClauseList, List *targetList)
{
	bool hasOrderByHllType = false;
	Oid hllId = InvalidOid;
	Oid hllSchemaOid = InvalidOid;
	Oid hllTypeId = InvalidOid;
	ListCell *sortClauseCell = NULL;

	/* check whether HLL is loaded */
	hllId = get_extension_oid(HLL_EXTENSION_NAME, true);
	if (!OidIsValid(hllId))
	{
		return hasOrderByHllType;
	}

	hllSchemaOid = get_extension_schema(hllId);
	hllTypeId = TypeOid(hllSchemaOid, HLL_TYPE_NAME);

	foreach(sortClauseCell, sortClauseList)
	{
		SortGroupClause *sortClause = (SortGroupClause *) lfirst(sortClauseCell);
		Node *sortExpression = get_sortgroupclause_expr(sortClause, targetList);

		Oid sortColumnTypeId = exprType(sortExpression);
		if (sortColumnTypeId == hllTypeId)
		{
			hasOrderByHllType = true;
			break;
		}
	}

	return hasOrderByHllType;
}


/*
 * IsGroupBySubsetOfDistinct checks whether each clause in group clauses also
 * exists in the distinct clauses. Note that, empty group clause is not a subset
 * of distinct clause.
 */
bool
IsGroupBySubsetOfDistinct(List *groupClauses, List *distinctClauses)
{
	ListCell *distinctCell = NULL;
	ListCell *groupCell = NULL;

	/* There must be a group clause */
	if (list_length(groupClauses) == 0)
	{
		return false;
	}

	foreach(groupCell, groupClauses)
	{
		SortGroupClause *groupClause = (SortGroupClause *) lfirst(groupCell);
		bool isFound = false;

		foreach(distinctCell, distinctClauses)
		{
			SortGroupClause *distinctClause = (SortGroupClause *) lfirst(distinctCell);

			if (groupClause->tleSortGroupRef == distinctClause->tleSortGroupRef)
			{
				isFound = true;
				break;
			}
		}

		/*
		 * If we can't find any member of group clause in the distinct clause,
		 * that means group clause is not a subset of distinct clause.
		 */
		if (!isFound)
		{
			return false;
		}
	}

	return true;
}
