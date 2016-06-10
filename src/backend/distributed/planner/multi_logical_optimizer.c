/*-------------------------------------------------------------------------
 *
 * multi_logical_optimizer.c
 *	  Routines for optimizing logical plan trees based on multi-relational
 *	  algebra.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
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
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "distributed/citus_nodes.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* Config variable managed via guc.c */
int LimitClauseRowFetchCount = -1; /* number of rows to fetch from each task */
double CountDistinctErrorRate = 0.0; /* precision of count(distinct) approximate */


typedef struct MasterAggregateWalkerContext
{
	bool repartitionSubquery;
	AttrNumber columnId;
} MasterAggregateWalkerContext;

typedef struct WorkerAggregateWalkerContext
{
	bool repartitionSubquery;
	List *expressionList;
	bool createGroupByClause;
} WorkerAggregateWalkerContext;


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
static MultiExtendedOp * MasterExtendedOpNode(MultiExtendedOp *originalOpNode);
static Node * MasterAggregateMutator(Node *originalNode,
									 MasterAggregateWalkerContext *walkerContext);
static Expr * MasterAggregateExpression(Aggref *originalAggregate,
										MasterAggregateWalkerContext *walkerContext);
static Expr * MasterAverageExpression(Oid sumAggregateType, Oid countAggregateType,
									  AttrNumber *columnId);
static Expr * AddTypeConversion(Node *originalAggregate, Node *newExpression);
static MultiExtendedOp * WorkerExtendedOpNode(MultiExtendedOp *originalOpNode);
static bool WorkerAggregateWalker(Node *node,
								  WorkerAggregateWalkerContext *walkerContext);
static List * WorkerAggregateExpressionList(Aggref *originalAggregate,
											WorkerAggregateWalkerContext *walkerContextry);
static AggregateType GetAggregateType(Oid aggFunctionId);
static Oid AggregateArgumentType(Aggref *aggregate);
static Oid AggregateFunctionOid(const char *functionName, Oid inputType);

/* Local functions forward declarations for count(distinct) approximations */
static char * CountDistinctHashFunctionName(Oid argumentType);
static int CountDistinctStorageSize(double approximationErrorRate);
static Const * MakeIntegerConst(int32 integerValue);

/* Local functions forward declarations for aggregate expression checks */
static void ErrorIfContainsUnsupportedAggregate(MultiNode *logicalPlanNode);
static void ErrorIfUnsupportedArrayAggregate(Aggref *arrayAggregateExpression);
static void ErrorIfUnsupportedAggregateDistinct(Aggref *aggregateExpression,
												MultiNode *logicalPlanNode);
static Var * AggregateDistinctColumn(Aggref *aggregateExpression);
static bool TablePartitioningSupportsDistinct(List *tableNodeList,
											  MultiExtendedOp *opNode,
											  Var *distinctColumn);
static bool GroupedByColumn(List *groupClauseList, List *targetList, Var *column);

/* Local functions forward declarations for subquery pushdown checks */
static void ErrorIfContainsUnsupportedSubquery(MultiNode *logicalPlanNode);
static void ErrorIfCannotPushdownSubquery(Query *subqueryTree, bool outerQueryHasLimit);
static void ErrorIfUnsupportedTableCombination(Query *queryTree);
static void ErrorIfUnsupportedUnionQuery(Query *unionQuery);
static bool TargetListOnPartitionColumn(Query *query, List *targetEntryList);
static bool IsPartitionColumnRecursive(Expr *columnExpression, Query *query);
static FieldSelect * CompositeFieldRecursive(Expr *expression, Query *query);
static bool FullCompositeFieldList(List *compositeFieldList);
static Query * LateralQuery(Query *query);
static bool SupportedLateralQuery(Query *parentQuery, Query *lateralQuery);
static bool JoinOnPartitionColumn(Query *query);
static void ErrorIfUnsupportedShardDistribution(Query *query);
static List * RelationIdList(Query *query);
static bool CoPartitionedTables(Oid firstRelationId, Oid secondRelationId);
static bool ShardIntervalsEqual(FmgrInfo *comparisonFunction,
								ShardInterval *firstInterval,
								ShardInterval *secondInterval);
static void ErrorIfUnsupportedFilters(Query *subquery);
static bool EqualOpExpressionLists(List *firstOpExpressionList,
								   List *secondOpExpressionList);

/* Local functions forward declarations for limit clauses */
static Node * WorkerLimitCount(MultiExtendedOp *originalOpNode);
static List * WorkerSortClauseList(MultiExtendedOp *originalOpNode);
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
	MultiNode *logicalPlanNode = (MultiNode *) multiLogicalPlan;

	/* check that we can optimize aggregates in the plan */
	ErrorIfContainsUnsupportedAggregate(logicalPlanNode);

	/* check that we can pushdown subquery in the plan */
	ErrorIfContainsUnsupportedSubquery(logicalPlanNode);

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

	masterExtendedOpNode = MasterExtendedOpNode(extendedOpNode);
	workerExtendedOpNode = WorkerExtendedOpNode(extendedOpNode);

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

		Assert(list_length(selectColumnList) > 0);
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

		Var *selectColumn = (Var *) linitial(selectColumnList);
		int selectClauseTableId = (int) selectColumn->varno;

		bool tableIdListMember = list_member_int(tableIdList, selectClauseTableId);
		if (tableIdListMember)
		{
			tableSelectClauseList = lappend(tableSelectClauseList, selectClause);
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
	MultiExtendedOp *masterExtendedOpNode = MasterExtendedOpNode(extendedOpNode);
	MultiExtendedOp *workerExtendedOpNode = WorkerExtendedOpNode(extendedOpNode);
	MultiPartition *partitionNode = CitusMakeNode(MultiPartition);
	List *groupClauseList = extendedOpNode->groupClauseList;
	List *targetEntryList = extendedOpNode->targetList;
	List *groupTargetEntryList = GroupTargetEntryList(groupClauseList, targetEntryList);
	TargetEntry *groupByTargetEntry = (TargetEntry *) linitial(groupTargetEntryList);
	Expr *groupByExpression = groupByTargetEntry->expr;

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
MasterExtendedOpNode(MultiExtendedOp *originalOpNode)
{
	MultiExtendedOp *masterExtendedOpNode = NULL;
	List *targetEntryList = originalOpNode->targetList;
	List *newTargetEntryList = NIL;
	ListCell *targetEntryCell = NULL;
	MultiNode *parentNode = ParentNode((MultiNode *) originalOpNode);
	MultiNode *childNode = ChildNode((MultiUnaryNode *) originalOpNode);
	MasterAggregateWalkerContext *walkerContext = palloc0(
		sizeof(MasterAggregateWalkerContext));

	walkerContext->columnId = 1;
	walkerContext->repartitionSubquery = false;

	if (CitusIsA(parentNode, MultiTable) && CitusIsA(childNode, MultiCollect))
	{
		walkerContext->repartitionSubquery = true;
	}

	/* iterate over original target entries */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *originalTargetEntry = (TargetEntry *) lfirst(targetEntryCell);
		TargetEntry *newTargetEntry = copyObject(originalTargetEntry);
		Expr *originalExpression = originalTargetEntry->expr;
		Expr *newExpression = NULL;

		bool hasAggregates = contain_agg_clause((Node *) originalExpression);
		if (hasAggregates)
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

	masterExtendedOpNode = CitusMakeNode(MultiExtendedOp);
	masterExtendedOpNode->targetList = newTargetEntryList;
	masterExtendedOpNode->groupClauseList = originalOpNode->groupClauseList;
	masterExtendedOpNode->sortClauseList = originalOpNode->sortClauseList;
	masterExtendedOpNode->limitCount = originalOpNode->limitCount;

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
		Var *newColumn = copyObject(originalNode);
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
 * the average, count, and array_agg aggregates separately due to differences
 * in these aggregate functions' transformations.
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

	if (aggregateType == AGGREGATE_COUNT && originalAggregate->aggdistinct &&
		CountDistinctErrorRate == DISABLE_DISTINCT_APPROXIMATION &&
		walkerContext->repartitionSubquery)
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
		 * distincts. For this, we first compute hll_add_agg(hll_hash(column) on
		 * worker nodes, and get hll values. We then gather hlls on the master
		 * node, and compute hll_cardinality(hll_union_agg(hll)).
		 */
		const int argCount = 1;
		const int defaultTypeMod = -1;

		TargetEntry *hllTargetEntry = NULL;
		Aggref *unionAggregate = NULL;
		FuncExpr *cardinalityExpression = NULL;

		Oid unionFunctionId = FunctionOid(HLL_UNION_AGGREGATE_NAME, argCount);
		Oid cardinalityFunctionId = FunctionOid(HLL_CARDINALITY_FUNC_NAME, argCount);
		Oid cardinalityReturnType = get_func_rettype(cardinalityFunctionId);

		Oid hllType = TypenameGetTypid(HLL_TYPE_NAME);
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

		column = makeVar(masterTableId, walkerContext->columnId, workerReturnType,
						 workerReturnTypeMod, workerCollationId, columnLevelsUp);
		walkerContext->columnId++;

		/* aggref expects its arguments to be wrapped in target entries */
		columnTargetEntry = makeTargetEntry((Expr *) column, argumentId, NULL, false);
		newMasterAggregate->args = list_make1(columnTargetEntry);

		newMasterExpression = (Expr *) newMasterAggregate;
	}
	else if (aggregateType == AGGREGATE_ARRAY_AGG)
	{
		/*
		 * Array aggregates are handled in two steps. First, we compute array_agg()
		 * on the worker nodes. Then, we gather the arrays on the master and
		 * compute the array_cat_agg() aggregate on them to get the final array.
		 */
		Var *column = NULL;
		TargetEntry *arrayCatAggArgument = NULL;
		Aggref *newMasterAggregate = NULL;
		Oid aggregateFunctionId = InvalidOid;

		/* worker aggregate and original aggregate have same return type */
		Oid workerReturnType = exprType((Node *) originalAggregate);
		int32 workerReturnTypeMod = exprTypmod((Node *) originalAggregate);
		Oid workerCollationId = exprCollation((Node *) originalAggregate);

		/* assert that we do not support array_agg() with distinct or order by */
		Assert(!originalAggregate->aggorder);
		Assert(!originalAggregate->aggdistinct);

		/* array_cat_agg() takes anyarray as input */
		aggregateFunctionId = AggregateFunctionOid(ARRAY_CAT_AGGREGATE_NAME,
												   ANYARRAYOID);

		/* create argument for the array_cat_agg() aggregate */
		column = makeVar(masterTableId, walkerContext->columnId, workerReturnType,
						 workerReturnTypeMod, workerCollationId, columnLevelsUp);
		arrayCatAggArgument = makeTargetEntry((Expr *) column, argumentId, NULL, false);
		walkerContext->columnId++;

		/* construct the master array_cat_agg() expression */
		newMasterAggregate = copyObject(originalAggregate);
		newMasterAggregate->aggfnoid = aggregateFunctionId;
		newMasterAggregate->args = list_make1(arrayCatAggArgument);

		newMasterExpression = (Expr *) newMasterAggregate;
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

	/*
	 * Build the division operator between these two aggregates. This function
	 * will convert the types of the aggregates if necessary.
	 */
	operatorNameList = list_make1(makeString(DIVISION_OPER_NAME));
	opExpr = make_op(NULL, operatorNameList, (Node *) firstSum, (Node *) secondSum, -1);

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
 * target entries. The function walks over these target entries; and for entries
 * with aggregates in them, this function calls the recursive aggregate walker
 * function to create aggregates for the worker nodes. Also, the function checks
 * if we can push down the limit to worker nodes; and if we can, sets the limit
 * count and sort clause list fields in the new operator node. It provides special
 * treatment for count distinct operator if it is used in repartition subqueries.
 * Each column in count distinct aggregate is added to target list, and group by
 * list of worker extended operator.
 */
static MultiExtendedOp *
WorkerExtendedOpNode(MultiExtendedOp *originalOpNode)
{
	MultiExtendedOp *workerExtendedOpNode = NULL;
	MultiNode *parentNode = ParentNode((MultiNode *) originalOpNode);
	MultiNode *childNode = ChildNode((MultiUnaryNode *) originalOpNode);
	List *targetEntryList = originalOpNode->targetList;
	ListCell *targetEntryCell = NULL;
	List *newTargetEntryList = NIL;
	List *groupClauseList = copyObject(originalOpNode->groupClauseList);
	AttrNumber targetProjectionNumber = 1;
	WorkerAggregateWalkerContext *walkerContext =
		palloc0(sizeof(WorkerAggregateWalkerContext));
	Index nextSortGroupRefIndex = 0;

	walkerContext->repartitionSubquery = false;
	walkerContext->expressionList = NIL;

	if (CitusIsA(parentNode, MultiTable) && CitusIsA(childNode, MultiCollect))
	{
		walkerContext->repartitionSubquery = true;

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
	}

	/* iterate over original target entries */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *originalTargetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Expr *originalExpression = originalTargetEntry->expr;
		List *newExpressionList = NIL;
		ListCell *newExpressionCell = NULL;
		bool hasAggregates = contain_agg_clause((Node *) originalExpression);

		walkerContext->expressionList = NIL;
		walkerContext->createGroupByClause = false;

		if (hasAggregates)
		{
			WorkerAggregateWalker((Node *) originalExpression, walkerContext);
			newExpressionList = walkerContext->expressionList;
		}
		else
		{
			newExpressionList = list_make1(originalExpression);
		}

		/* now create target entries for each new expression */
		foreach(newExpressionCell, newExpressionList)
		{
			Expr *newExpression = (Expr *) lfirst(newExpressionCell);
			TargetEntry *newTargetEntry = copyObject(originalTargetEntry);
			newTargetEntry->expr = newExpression;

			/*
			 * Detect new targets of type Var and add it to group clause list.
			 * This case is expected only if the target entry has aggregates and
			 * it is inside a repartitioned subquery. We create group by entry
			 * for each Var in target list. This code does not check if this
			 * Var was already in the target list or in group by clauses.
			 */
			if (IsA(newExpression, Var) && walkerContext->createGroupByClause)
			{
				Var *column = (Var *) newExpression;
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
				groupByClause->tleSortGroupRef = nextSortGroupRefIndex;

				groupClauseList = lappend(groupClauseList, groupByClause);

				newTargetEntry->ressortgroupref = nextSortGroupRefIndex;

				nextSortGroupRefIndex++;
			}

			if (newTargetEntry->resname == NULL)
			{
				StringInfo columnNameString = makeStringInfo();
				appendStringInfo(columnNameString, WORKER_COLUMN_FORMAT,
								 targetProjectionNumber);

				newTargetEntry->resname = columnNameString->data;
			}

			/* force resjunk to false as we may need this on the master */
			newTargetEntry->resjunk = false;
			newTargetEntry->resno = targetProjectionNumber;
			targetProjectionNumber++;
			newTargetEntryList = lappend(newTargetEntryList, newTargetEntry);
		}
	}

	workerExtendedOpNode = CitusMakeNode(MultiExtendedOp);
	workerExtendedOpNode->targetList = newTargetEntryList;
	workerExtendedOpNode->groupClauseList = groupClauseList;

	/* if we can push down the limit, also set related fields */
	workerExtendedOpNode->limitCount = WorkerLimitCount(originalOpNode);
	workerExtendedOpNode->sortClauseList = WorkerSortClauseList(originalOpNode);

	return workerExtendedOpNode;
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

	if (aggregateType == AGGREGATE_COUNT && originalAggregate->aggdistinct &&
		CountDistinctErrorRate == DISABLE_DISTINCT_APPROXIMATION &&
		walkerContext->repartitionSubquery)
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

		char *hashFunctionName = CountDistinctHashFunctionName(argumentType);
		Oid hashFunctionId = FunctionOid(hashFunctionName, hashArgumentCount);
		Oid hashFunctionReturnType = get_func_rettype(hashFunctionId);

		/* init hll_add_agg() related variables */
		Oid addFunctionId = FunctionOid(HLL_ADD_AGGREGATE_NAME, addArgumentCount);
		Oid hllType = TypenameGetTypid(HLL_TYPE_NAME);
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

		/* count has any input type */
		countAggregate->aggfnoid = AggregateFunctionOid(countAggregateName, ANYOID);
		countAggregate->aggtype = get_func_rettype(countAggregate->aggfnoid);

		workerAggregateList = lappend(workerAggregateList, sumAggregate);
		workerAggregateList = lappend(workerAggregateList, countAggregate);
	}
	else
	{
		/*
		 * All other aggregates are sent as they are to the worker nodes. These
		 * aggregate functions include sum, count, min, max, and array_agg.
		 */
		Aggref *workerAggregate = copyObject(originalAggregate);
		workerAggregateList = lappend(workerAggregateList, workerAggregate);
	}

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
		ereport(ERROR, (errmsg("cache lookup failed for function %u", aggFunctionId)));
	}

	aggregateCount = lengthof(AggregateNames);
	for (aggregateIndex = 0; aggregateIndex < aggregateCount; aggregateIndex++)
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

	/* We currently support aggregates with only one argument; assert that. */
	Assert(list_length(argumentList) == 1);

	return returnTypeId;
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
				functionOid = HeapTupleGetOid(heapTuple);
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
 * FunctionOid looks for a function that has the given name and the given number
 * of arguments, and returns the corresponding function's oid.
 */
Oid
FunctionOid(const char *functionName, int argumentCount)
{
	FuncCandidateList functionList = NULL;
	Oid functionOid = InvalidOid;

	List *qualifiedFunctionName = stringToQualifiedNameList(functionName);
	List *argumentList = NIL;
	const bool findVariadics = false;
	const bool findDefaults = false;
	const bool missingOK = true;

	functionList = FuncnameGetCandidates(qualifiedFunctionName, argumentCount,
										 argumentList, findVariadics,
										 findDefaults, missingOK);

	if (functionList == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("function \"%s\" does not exist", functionName)));
	}
	else if (functionList->next != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
						errmsg("more than one function named \"%s\"", functionName)));
	}

	/* get function oid from function list's head */
	functionOid = functionList->oid;
	return functionOid;
}


/*
 * CountDistinctHashFunctionName resolves the hll_hash function name to use for
 * the given input type, and returns this function name.
 */
static char *
CountDistinctHashFunctionName(Oid argumentType)
{
	char *hashFunctionName = NULL;

	/* resolve hash function name based on input argument type */
	switch (argumentType)
	{
		case INT4OID:
		{
			hashFunctionName = pstrdup(HLL_HASH_INTEGER_FUNC_NAME);
			break;
		}

		case INT8OID:
		{
			hashFunctionName = pstrdup(HLL_HASH_BIGINT_FUNC_NAME);
			break;
		}

		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		{
			hashFunctionName = pstrdup(HLL_HASH_TEXT_FUNC_NAME);
			break;
		}

		default:
		{
			hashFunctionName = pstrdup(HLL_HASH_ANY_FUNC_NAME);
			break;
		}
	}

	return hashFunctionName;
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
	List *expressionList = pull_var_clause((Node *) targetList, PVC_INCLUDE_AGGREGATES,
										   PVC_REJECT_PLACEHOLDERS);

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

	/* check if logical plan includes a subquery */
	List *subqueryMultiTableList = SubqueryMultiTableList(logicalPlanNode);
	if (subqueryMultiTableList != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("distinct in the outermost query is unsupported")));
	}

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
			if (multiTable->relationId == SUBQUERY_RELATION_ID)
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
	if (distinctSupported && distinctColumn == NULL)
	{
		/*
		 * If the query has a single table, and table is grouped by partition column,
		 * then we support count distincts even distinct column can not be identified.
		 */
		distinctSupported = TablePartitioningSupportsDistinct(tableNodeList,
															  extendedOpNode,
															  distinctColumn);
		if (!distinctSupported)
		{
			errorDetail = "aggregate (distinct) on complex expressions is unsupported";
		}
	}
	else if (distinctSupported)
	{
		bool supports = TablePartitioningSupportsDistinct(tableNodeList, extendedOpNode,
														  distinctColumn);
		if (!supports)
		{
			distinctSupported = false;
			errorDetail = "table partitioning is unsuitable for aggregate (distinct)";
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
								  Var *distinctColumn)
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

		if (relationId == SUBQUERY_RELATION_ID)
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
static bool
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
 * ErrorIfContainsUnsupportedSubquery extracts subquery multi table from the
 * logical plan and uses helper functions to check if we can push down subquery
 * to worker nodes. These helper functions error out if we cannot push down the
 * the subquery.
 */
static void
ErrorIfContainsUnsupportedSubquery(MultiNode *logicalPlanNode)
{
	Query *subquery = NULL;
	List *extendedOpNodeList = NIL;
	MultiTable *multiTable = NULL;
	MultiExtendedOp *extendedOpNode = NULL;
	bool outerQueryHasLimit = false;

	/* check if logical plan includes a subquery */
	List *subqueryMultiTableList = SubqueryMultiTableList(logicalPlanNode);
	if (subqueryMultiTableList == NIL)
	{
		return;
	}

	/* currently in the planner we only allow one subquery in from-clause*/
	Assert(list_length(subqueryMultiTableList) == 1);

	multiTable = (MultiTable *) linitial(subqueryMultiTableList);
	subquery = multiTable->subquery;

	extendedOpNodeList = FindNodesOfType(logicalPlanNode, T_MultiExtendedOp);
	extendedOpNode = (MultiExtendedOp *) linitial(extendedOpNodeList);

	if (extendedOpNode->limitCount)
	{
		outerQueryHasLimit = true;
	}

	ErrorIfCannotPushdownSubquery(subquery, outerQueryHasLimit);
	ErrorIfUnsupportedShardDistribution(subquery);
	ErrorIfUnsupportedFilters(subquery);
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
 * ErrorIfCannotPushdownSubquery recursively checks if we can push down the given
 * subquery to worker nodes. If we cannot push down the subquery, this function
 * errors out.
 *
 * We can push down a subquery if it follows rules below. We support nested queries
 * as long as they follow the same rules, and we recurse to validate each subquery
 * for this given query.
 * a. If there is an aggregate, it must be grouped on partition column.
 * b. If there is a join, it must be between two regular tables or two subqueries.
 * We don't support join between a regular table and a subquery. And columns on
 * the join condition must be partition columns.
 * c. If there is a distinct clause, it must be on the partition column.
 *
 * This function is very similar to ErrorIfQueryNotSupported() in logical
 * planner, but we don't reuse it, because differently for subqueries we support
 * a subset of distinct, union and left joins.
 *
 * Note that this list of checks is not exhaustive, there can be some cases
 * which we let subquery to run but returned results would be wrong. Such as if
 * a subquery has a group by on another subquery which includes order by with
 * limit, we let this query to run, but results could be wrong depending on the
 * features of underlying tables.
 */
static void
ErrorIfCannotPushdownSubquery(Query *subqueryTree, bool outerQueryHasLimit)
{
	bool preconditionsSatisfied = true;
	char *errorDetail = NULL;
	Query *lateralQuery = NULL;
	List *subqueryEntryList = NIL;
	ListCell *rangeTableEntryCell = NULL;

	ErrorIfUnsupportedTableCombination(subqueryTree);

	if (subqueryTree->hasSubLinks)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries other than from-clause subqueries are unsupported";
	}

	if (subqueryTree->hasWindowFuncs)
	{
		preconditionsSatisfied = false;
		errorDetail = "Window functions are currently unsupported";
	}

	if (subqueryTree->limitOffset)
	{
		preconditionsSatisfied = false;
		errorDetail = "Limit Offset clause is currently unsupported";
	}

	if (subqueryTree->limitCount && !outerQueryHasLimit)
	{
		preconditionsSatisfied = false;
		errorDetail = "Limit in subquery without limit in the outer query is unsupported";
	}

	if (subqueryTree->setOperations)
	{
		SetOperationStmt *setOperationStatement =
			(SetOperationStmt *) subqueryTree->setOperations;

		if (setOperationStatement->op == SETOP_UNION)
		{
			ErrorIfUnsupportedUnionQuery(subqueryTree);
		}
		else
		{
			preconditionsSatisfied = false;
			errorDetail = "Intersect and Except are currently unsupported";
		}
	}

	if (subqueryTree->hasRecursive)
	{
		preconditionsSatisfied = false;
		errorDetail = "Recursive queries are currently unsupported";
	}

	if (subqueryTree->cteList)
	{
		preconditionsSatisfied = false;
		errorDetail = "Common Table Expressions are currently unsupported";
	}

	if (subqueryTree->hasForUpdate)
	{
		preconditionsSatisfied = false;
		errorDetail = "For Update/Share commands are currently unsupported";
	}

	/* group clause list must include partition column */
	if (subqueryTree->groupClause)
	{
		List *groupClauseList = subqueryTree->groupClause;
		List *targetEntryList = subqueryTree->targetList;
		List *groupTargetEntryList = GroupTargetEntryList(groupClauseList,
														  targetEntryList);
		bool groupOnPartitionColumn = TargetListOnPartitionColumn(subqueryTree,
																  groupTargetEntryList);
		if (!groupOnPartitionColumn)
		{
			preconditionsSatisfied = false;
			errorDetail = "Group by list without partition column is currently "
						  "unsupported";
		}
	}

	/* we don't support aggregates without group by */
	if (subqueryTree->hasAggs && (subqueryTree->groupClause == NULL))
	{
		preconditionsSatisfied = false;
		errorDetail = "Aggregates without group by are currently unsupported";
	}

	/* having clause without group by on partition column is not supported */
	if (subqueryTree->havingQual && (subqueryTree->groupClause == NULL))
	{
		preconditionsSatisfied = false;
		errorDetail = "Having qual without group by on partition column is "
					  "currently unsupported";
	}

	/*
	 * Check if join is supported. We check lateral joins differently, because
	 * lateral join representation in query tree is a bit different than normal
	 * join queries.
	 */
	lateralQuery = LateralQuery(subqueryTree);
	if (lateralQuery != NULL)
	{
		bool supportedLateralQuery = SupportedLateralQuery(subqueryTree, lateralQuery);
		if (!supportedLateralQuery)
		{
			preconditionsSatisfied = false;
			errorDetail = "This type of lateral query in subquery is currently "
						  "unsupported";
		}
	}
	else
	{
		List *joinTreeTableIndexList = NIL;
		uint32 joiningTableCount = 0;

		ExtractRangeTableIndexWalker((Node *) subqueryTree->jointree,
									 &joinTreeTableIndexList);
		joiningTableCount = list_length(joinTreeTableIndexList);

		/* if this is a join query, check if join clause is on partition columns */
		if ((joiningTableCount > 1))
		{
			bool joinOnPartitionColumn = JoinOnPartitionColumn(subqueryTree);
			if (!joinOnPartitionColumn)
			{
				preconditionsSatisfied = false;
				errorDetail = "Relations need to be joining on partition columns";
			}
		}
	}

	/* distinct clause list must include partition column */
	if (subqueryTree->distinctClause)
	{
		List *distinctClauseList = subqueryTree->distinctClause;
		List *targetEntryList = subqueryTree->targetList;
		List *distinctTargetEntryList = GroupTargetEntryList(distinctClauseList,
															 targetEntryList);
		bool distinctOnPartitionColumn =
			TargetListOnPartitionColumn(subqueryTree, distinctTargetEntryList);
		if (!distinctOnPartitionColumn)
		{
			preconditionsSatisfied = false;
			errorDetail = "Distinct on columns without partition column is "
						  "currently unsupported";
		}
	}

	/* finally check and error out if not satisfied */
	if (!preconditionsSatisfied)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("%s", errorDetail)));
	}

	/* recursively do same check for subqueries of this query */
	subqueryEntryList = SubqueryEntryList(subqueryTree);
	foreach(rangeTableEntryCell, subqueryEntryList)
	{
		RangeTblEntry *rangeTableEntry =
			(RangeTblEntry *) lfirst(rangeTableEntryCell);

		Query *innerSubquery = rangeTableEntry->subquery;
		ErrorIfCannotPushdownSubquery(innerSubquery, outerQueryHasLimit);
	}
}


/*
 * ErrorIfUnsupportedTableCombination checks if the given query tree contains any
 * unsupported range table combinations. For this, the function walks over all
 * range tables in the join tree, and checks if they correspond to simple relations
 * or subqueries. It also checks if there is a join between a regular table and
 * a subquery and if join is on more than two range table entries.
 */
static void
ErrorIfUnsupportedTableCombination(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	List *joinTreeTableIndexList = NIL;
	ListCell *joinTreeTableIndexCell = NULL;
	bool unsupporteTableCombination = false;
	char *errorDetail = NULL;
	uint32 relationRangeTableCount = 0;
	uint32 subqueryRangeTableCount = 0;

	/*
	 * Extract all range table indexes from the join tree. Note that sub-queries
	 * that get pulled up by PostgreSQL don't appear in this join tree.
	 */
	ExtractRangeTableIndexWalker((Node *) queryTree->jointree, &joinTreeTableIndexList);
	foreach(joinTreeTableIndexCell, joinTreeTableIndexList)
	{
		/*
		 * Join tree's range table index starts from 1 in the query tree. But,
		 * list indexes start from 0.
		 */
		int joinTreeTableIndex = lfirst_int(joinTreeTableIndexCell);
		int rangeTableListIndex = joinTreeTableIndex - 1;

		RangeTblEntry *rangeTableEntry =
			(RangeTblEntry *) list_nth(rangeTableList, rangeTableListIndex);

		/*
		 * Check if the range table in the join tree is a simple relation or a
		 * subquery.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			relationRangeTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_SUBQUERY)
		{
			subqueryRangeTableCount++;
		}
		else
		{
			unsupporteTableCombination = true;
			errorDetail = "Table expressions other than simple relations and "
						  "subqueries are currently unsupported";
			break;
		}
	}

	if ((subqueryRangeTableCount > 0) && (relationRangeTableCount > 0))
	{
		unsupporteTableCombination = true;
		errorDetail = "Joins between regular tables and subqueries are unsupported";
	}

	if ((relationRangeTableCount > 2) || (subqueryRangeTableCount > 2))
	{
		unsupporteTableCombination = true;
		errorDetail = "Joins between more than two relations and subqueries are "
					  "unsupported";
	}

	/* finally check and error out if not satisfied */
	if (unsupporteTableCombination)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("%s", errorDetail)));
	}
}


/*
 * ErrorIfUnsupportedUnionQuery checks if the given union query is a supported
 * one., otherwise it errors out. For these purpose it checks tree conditions;
 * a. Are count of partition column filters same for union subqueries.
 * b. Are target lists of union subquries include partition column.
 * c. Is it a union clause without All option.
 *
 * Note that we check equality of filters in ErrorIfUnsupportedFilters(). We
 * allow leaf queries not having a filter clause on the partition column. We
 * check if a leaf query has a filter on the partition column, it must be same
 * with other queries or if leaf query must not have any filter on the partition
 * column, both are ok. Because joins and nested queries are transitive, it is
 * enough one leaf query to have a filter on the partition column. But unions
 * are not transitive, so here we check if they have same count of filters on
 * the partition column. If count is more than 0, we already checked that they
 * are same, of if count is 0 then both don't have any filter on the partition
 * column.
 */
static void
ErrorIfUnsupportedUnionQuery(Query *unionQuery)
{
	bool supportedUnionQuery = true;
	bool leftQueryOnPartitionColumn = false;
	bool rightQueryOnPartitionColumn = false;
	List *rangeTableList = unionQuery->rtable;
	SetOperationStmt *unionStatement = (SetOperationStmt *) unionQuery->setOperations;
	Query *leftQuery = NULL;
	Query *rightQuery = NULL;
	List *leftOpExpressionList = NIL;
	List *rightOpExpressionList = NIL;
	uint32 leftOpExpressionCount = 0;
	uint32 rightOpExpressionCount = 0;
	char *errorDetail = NULL;

	RangeTblRef *leftRangeTableReference = (RangeTblRef *) unionStatement->larg;
	RangeTblRef *rightRangeTableReference = (RangeTblRef *) unionStatement->rarg;

	int leftTableIndex = leftRangeTableReference->rtindex - 1;
	int rightTableIndex = rightRangeTableReference->rtindex - 1;

	RangeTblEntry *leftRangeTableEntry = (RangeTblEntry *) list_nth(rangeTableList,
																	leftTableIndex);
	RangeTblEntry *rightRangeTableEntry = (RangeTblEntry *) list_nth(rangeTableList,
																	 rightTableIndex);

	Assert(leftRangeTableEntry->rtekind == RTE_SUBQUERY);
	Assert(rightRangeTableEntry->rtekind == RTE_SUBQUERY);

	leftQuery = leftRangeTableEntry->subquery;
	rightQuery = rightRangeTableEntry->subquery;

	/*
	 * Check if subqueries of union have same count of filters on partition
	 * column.
	 */
	leftOpExpressionList = PartitionColumnOpExpressionList(leftQuery);
	rightOpExpressionList = PartitionColumnOpExpressionList(rightQuery);

	leftOpExpressionCount = list_length(leftOpExpressionList);
	rightOpExpressionCount = list_length(rightOpExpressionList);

	if (leftOpExpressionCount != rightOpExpressionCount)
	{
		supportedUnionQuery = false;
		errorDetail = "Union clauses need to have same count of filters on "
					  "partition column";
	}

	/* check if union subqueries have partition column in their target lists */
	leftQueryOnPartitionColumn = TargetListOnPartitionColumn(leftQuery,
															 leftQuery->targetList);
	rightQueryOnPartitionColumn = TargetListOnPartitionColumn(rightQuery,
															  rightQuery->targetList);

	if (!(leftQueryOnPartitionColumn && rightQueryOnPartitionColumn))
	{
		supportedUnionQuery = false;
		errorDetail = "Union clauses need to select partition columns";
	}

	/* check if it is a union all operation */
	if (unionStatement->all)
	{
		supportedUnionQuery = false;
		errorDetail = "Union All clauses are currently unsupported";
	}

	/* finally check and error out if not satisfied */
	if (!supportedUnionQuery)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("%s", errorDetail)));
	}
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
 * TargetListOnPartitionColumn checks if at least one target list entry is on
 * partition column.
 */
static bool
TargetListOnPartitionColumn(Query *query, List *targetEntryList)
{
	bool targetListOnPartitionColumn = false;
	List *compositeFieldList = NIL;

	ListCell *targetEntryCell = NULL;
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Expr *targetExpression = targetEntry->expr;

		bool isPartitionColumn = IsPartitionColumnRecursive(targetExpression, query);
		if (isPartitionColumn)
		{
			FieldSelect *compositeField = CompositeFieldRecursive(targetExpression,
																  query);
			if (compositeField)
			{
				compositeFieldList = lappend(compositeFieldList, compositeField);
			}
			else
			{
				targetListOnPartitionColumn = true;
				break;
			}
		}
	}

	/* check composite fields */
	if (!targetListOnPartitionColumn)
	{
		bool fullCompositeFieldList = FullCompositeFieldList(compositeFieldList);
		if (fullCompositeFieldList)
		{
			targetListOnPartitionColumn = true;
		}
	}

	return targetListOnPartitionColumn;
}


/*
 * IsPartitionColumnRecursive recursively checks if the given column is a partition
 * column. If a column is referenced from a regular table, we directly check if
 * it is a partition column. If a column is referenced from a subquery, then we
 * recursively check that subquery until we reach the source of that column, and
 * verify this column is a partition column. If a column is referenced from a
 * join range table entry, then we resolve which join column it refers and
 * recursively check this column with the same query.
 *
 * Note that if the given expression is a field of a composite type, then this
 * function checks if this composite column is a partition column.
 */
static bool
IsPartitionColumnRecursive(Expr *columnExpression, Query *query)
{
	bool isPartitionColumn = false;
	Var *candidateColumn = NULL;
	List *rangetableList = query->rtable;
	Index rangeTableEntryIndex = 0;
	RangeTblEntry *rangeTableEntry = NULL;
	Expr *strippedColumnExpression = (Expr *) strip_implicit_coercions(
		(Node *) columnExpression);

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
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot push down this subquery"),
							errdetail("Only references to column fields are supported")));
		}
	}

	if (candidateColumn == NULL)
	{
		return false;
	}

	rangeTableEntryIndex = candidateColumn->varno - 1;
	rangeTableEntry = list_nth(rangetableList, rangeTableEntryIndex);

	if (rangeTableEntry->rtekind == RTE_RELATION)
	{
		Oid relationId = rangeTableEntry->relid;
		Var *partitionColumn = PartitionKey(relationId);

		if (candidateColumn->varattno == partitionColumn->varattno)
		{
			isPartitionColumn = true;
		}
	}
	else if (rangeTableEntry->rtekind == RTE_SUBQUERY)
	{
		Query *subquery = rangeTableEntry->subquery;
		List *targetEntryList = subquery->targetList;
		AttrNumber targetEntryIndex = candidateColumn->varattno - 1;
		TargetEntry *subqueryTargetEntry = list_nth(targetEntryList, targetEntryIndex);

		Expr *subqueryExpression = subqueryTargetEntry->expr;
		isPartitionColumn = IsPartitionColumnRecursive(subqueryExpression, subquery);
	}
	else if (rangeTableEntry->rtekind == RTE_JOIN)
	{
		List *joinColumnList = rangeTableEntry->joinaliasvars;
		AttrNumber joinColumnIndex = candidateColumn->varattno - 1;
		Expr *joinColumn = list_nth(joinColumnList, joinColumnIndex);

		isPartitionColumn = IsPartitionColumnRecursive(joinColumn, query);
	}

	return isPartitionColumn;
}


/*
 * CompositeFieldRecursive recursively finds composite field in the query tree
 * referred by given expression. If expression does not refer to a composite
 * field, then it returns NULL.
 *
 * If expression is a field select we directly return composite field. If it is
 * a column is referenced from a subquery, then we recursively check that subquery
 * until we reach the source of that column, and find composite field. If this
 * column is referenced from join range table entry, then we resolve which join
 * column it refers and recursively use this column with the same query.
 */
static FieldSelect *
CompositeFieldRecursive(Expr *expression, Query *query)
{
	FieldSelect *compositeField = NULL;
	List *rangetableList = query->rtable;
	Index rangeTableEntryIndex = 0;
	RangeTblEntry *rangeTableEntry = NULL;
	Var *candidateColumn = NULL;

	if (IsA(expression, FieldSelect))
	{
		compositeField = (FieldSelect *) expression;
		return compositeField;
	}

	if (IsA(expression, Var))
	{
		candidateColumn = (Var *) expression;
	}
	else
	{
		return NULL;
	}

	rangeTableEntryIndex = candidateColumn->varno - 1;
	rangeTableEntry = list_nth(rangetableList, rangeTableEntryIndex);

	if (rangeTableEntry->rtekind == RTE_SUBQUERY)
	{
		Query *subquery = rangeTableEntry->subquery;
		List *targetEntryList = subquery->targetList;
		AttrNumber targetEntryIndex = candidateColumn->varattno - 1;
		TargetEntry *subqueryTargetEntry = list_nth(targetEntryList, targetEntryIndex);

		Expr *subqueryExpression = subqueryTargetEntry->expr;
		compositeField = CompositeFieldRecursive(subqueryExpression, subquery);
	}
	else if (rangeTableEntry->rtekind == RTE_JOIN)
	{
		List *joinColumnList = rangeTableEntry->joinaliasvars;
		AttrNumber joinColumnIndex = candidateColumn->varattno - 1;
		Expr *joinColumn = list_nth(joinColumnList, joinColumnIndex);

		compositeField = CompositeFieldRecursive(joinColumn, query);
	}

	return compositeField;
}


/*
 * FullCompositeFieldList gets a composite field list, and checks if all fields
 * of composite type are used in the list.
 */
static bool
FullCompositeFieldList(List *compositeFieldList)
{
	bool fullCompositeFieldList = true;
	bool *compositeFieldArray = NULL;
	uint32 compositeFieldCount = 0;
	uint32 fieldIndex = 0;

	ListCell *fieldSelectCell = NULL;
	foreach(fieldSelectCell, compositeFieldList)
	{
		FieldSelect *fieldSelect = (FieldSelect *) lfirst(fieldSelectCell);
		uint32 compositeFieldIndex = 0;

		Expr *fieldExpression = fieldSelect->arg;
		if (!IsA(fieldExpression, Var))
		{
			continue;
		}

		if (compositeFieldArray == NULL)
		{
			uint32 index = 0;
			Var *compositeColumn = (Var *) fieldExpression;
			Oid compositeTypeId = compositeColumn->vartype;
			Oid compositeRelationId = get_typ_typrelid(compositeTypeId);

			/* get composite type attribute count */
			Relation relation = relation_open(compositeRelationId, AccessShareLock);
			compositeFieldCount = relation->rd_att->natts;
			compositeFieldArray = palloc0(compositeFieldCount * sizeof(bool));
			relation_close(relation, AccessShareLock);

			for (index = 0; index < compositeFieldCount; index++)
			{
				compositeFieldArray[index] = false;
			}
		}

		compositeFieldIndex = fieldSelect->fieldnum - 1;
		compositeFieldArray[compositeFieldIndex] = true;
	}

	for (fieldIndex = 0; fieldIndex < compositeFieldCount; fieldIndex++)
	{
		if (!compositeFieldArray[fieldIndex])
		{
			fullCompositeFieldList = false;
		}
	}

	if (compositeFieldCount == 0)
	{
		fullCompositeFieldList = false;
	}

	return fullCompositeFieldList;
}


/*
 * LateralQuery walks over the given range table list and if there is a subquery
 * columns with other sibling subquery.
 */
static Query *
LateralQuery(Query *query)
{
	Query *lateralQuery = NULL;
	List *rangeTableList = query->rtable;

	ListCell *rangeTableCell = NULL;
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_SUBQUERY && rangeTableEntry->lateral)
		{
			lateralQuery = rangeTableEntry->subquery;
			break;
		}
	}

	return lateralQuery;
}


/*
 * SupportedLateralQuery checks if the given lateral query is joined on partition
 * columns with another siblings subquery.
 */
static bool
SupportedLateralQuery(Query *parentQuery, Query *lateralQuery)
{
	bool supportedLateralQuery = false;
	List *outerCompositeFieldList = NIL;
	List *localCompositeFieldList = NIL;
	List *whereClauseList = WhereClauseList(lateralQuery->jointree);

	ListCell *whereClauseCell = NULL;
	foreach(whereClauseCell, whereClauseList)
	{
		OpExpr *operatorExpression = NULL;
		List *argumentList = NIL;
		bool equalsOperator = false;
		Expr *leftArgument = NULL;
		Expr *rightArgument = NULL;
		Expr *outerQueryExpression = NULL;
		Expr *localQueryExpression = NULL;
		Var *leftColumn = NULL;
		Var *rightColumn = NULL;
		bool outerColumnIsPartitionColumn = false;
		bool localColumnIsPartitionColumn = false;

		Node *clause = (Node *) lfirst(whereClauseCell);
		if (!IsA(clause, OpExpr))
		{
			continue;
		}

		operatorExpression = (OpExpr *) clause;
		argumentList = operatorExpression->args;

		/*
		 * Join clauses must have two arguments. Note that logic here use to find
		 * join clauses is very similar to IsJoinClause(). But we are not able to
		 * reuse it, because it calls pull_var_clause_default() which in return
		 * deep down calls pull_var_clause_walker(), and this function errors out
		 * for variable level other than 0 which is the case for lateral joins.
		 */
		if (list_length(argumentList) != 2)
		{
			continue;
		}

		equalsOperator = OperatorImplementsEquality(operatorExpression->opno);
		if (!equalsOperator)
		{
			continue;
		}

		/* get left and right side of the expression */
		leftArgument = (Expr *) linitial(argumentList);
		rightArgument = (Expr *) lsecond(argumentList);

		if (IsA(leftArgument, Var))
		{
			leftColumn = (Var *) leftArgument;
		}
		else if (IsA(leftArgument, FieldSelect))
		{
			FieldSelect *fieldSelect = (FieldSelect *) leftArgument;
			Expr *fieldExpression = fieldSelect->arg;

			if (!IsA(fieldExpression, Var))
			{
				continue;
			}

			leftColumn = (Var *) fieldExpression;
		}
		else
		{
			continue;
		}

		if (IsA(rightArgument, Var))
		{
			rightColumn = (Var *) rightArgument;
		}
		else if (IsA(rightArgument, FieldSelect))
		{
			FieldSelect *fieldSelect = (FieldSelect *) rightArgument;
			Expr *fieldExpression = fieldSelect->arg;

			if (!IsA(fieldExpression, Var))
			{
				continue;
			}

			rightColumn = (Var *) fieldExpression;
		}
		else
		{
			continue;
		}

		if (leftColumn->varlevelsup == 1 && rightColumn->varlevelsup == 0)
		{
			outerQueryExpression = leftArgument;
			localQueryExpression = rightArgument;
		}
		else if (leftColumn->varlevelsup == 0 && rightColumn->varlevelsup == 1)
		{
			outerQueryExpression = rightArgument;
			localQueryExpression = leftArgument;
		}
		else
		{
			continue;
		}

		outerColumnIsPartitionColumn = IsPartitionColumnRecursive(outerQueryExpression,
																  parentQuery);
		localColumnIsPartitionColumn = IsPartitionColumnRecursive(localQueryExpression,
																  lateralQuery);

		if (outerColumnIsPartitionColumn && localColumnIsPartitionColumn)
		{
			FieldSelect *outerCompositeField =
				CompositeFieldRecursive(outerQueryExpression, parentQuery);
			FieldSelect *localCompositeField =
				CompositeFieldRecursive(localQueryExpression, lateralQuery);

			/*
			 * If partition colums are composite fields, add them to list to
			 * check later if all composite fields are used.
			 */
			if (outerCompositeField && localCompositeField)
			{
				outerCompositeFieldList = lappend(outerCompositeFieldList,
												  outerCompositeField);
				localCompositeFieldList = lappend(localCompositeFieldList,
												  localCompositeField);
			}

			/* if both sides are not composite fields, they are normal columns */
			if (!(outerCompositeField || localCompositeField))
			{
				supportedLateralQuery = true;
				break;
			}
		}
	}

	/* check composite fields */
	if (!supportedLateralQuery)
	{
		bool outerFullCompositeFieldList =
			FullCompositeFieldList(outerCompositeFieldList);
		bool localFullCompositeFieldList =
			FullCompositeFieldList(localCompositeFieldList);

		if (outerFullCompositeFieldList && localFullCompositeFieldList)
		{
			supportedLateralQuery = true;
		}
	}

	return supportedLateralQuery;
}


/*
 * JoinOnPartitionColumn checks if both sides of at least one join clause are on
 * partition columns.
 */
static bool
JoinOnPartitionColumn(Query *query)
{
	bool joinOnPartitionColumn = false;
	List *leftCompositeFieldList = NIL;
	List *rightCompositeFieldList = NIL;
	List *whereClauseList = WhereClauseList(query->jointree);
	List *joinClauseList = JoinClauseList(whereClauseList);

	ListCell *joinClauseCell = NULL;
	foreach(joinClauseCell, joinClauseList)
	{
		OpExpr *joinClause = (OpExpr *) lfirst(joinClauseCell);
		List *joinArgumentList = joinClause->args;
		Expr *leftArgument = NULL;
		Expr *rightArgument = NULL;
		bool isLeftColumnPartitionColumn = false;
		bool isRightColumnPartitionColumn = false;

		/* get left and right side of the expression */
		leftArgument = (Expr *) linitial(joinArgumentList);
		rightArgument = (Expr *) lsecond(joinArgumentList);

		isLeftColumnPartitionColumn = IsPartitionColumnRecursive(leftArgument, query);
		isRightColumnPartitionColumn = IsPartitionColumnRecursive(rightArgument, query);

		if (isLeftColumnPartitionColumn && isRightColumnPartitionColumn)
		{
			FieldSelect *leftCompositeField =
				CompositeFieldRecursive(leftArgument, query);
			FieldSelect *rightCompositeField =
				CompositeFieldRecursive(rightArgument, query);

			/*
			 * If partition colums are composite fields, add them to list to
			 * check later if all composite fields are used.
			 */
			if (leftCompositeField && rightCompositeField)
			{
				leftCompositeFieldList = lappend(leftCompositeFieldList,
												 leftCompositeField);
				rightCompositeFieldList = lappend(rightCompositeFieldList,
												  rightCompositeField);
			}

			/* if both sides are not composite fields, they are normal columns */
			if (!(leftCompositeField && rightCompositeField))
			{
				joinOnPartitionColumn = true;
				break;
			}
		}
	}

	/* check composite fields */
	if (!joinOnPartitionColumn)
	{
		bool leftFullCompositeFieldList =
			FullCompositeFieldList(leftCompositeFieldList);
		bool rightFullCompositeFieldList =
			FullCompositeFieldList(rightCompositeFieldList);

		if (leftFullCompositeFieldList && rightFullCompositeFieldList)
		{
			joinOnPartitionColumn = true;
		}
	}

	return joinOnPartitionColumn;
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
	List *relationIdList = RelationIdList(query);
	ListCell *relationIdCell = NULL;
	uint32 relationIndex = 0;
	uint32 rangeDistributedRelationCount = 0;
	uint32 hashDistributedRelationCount = 0;

	foreach(relationIdCell, relationIdList)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		char partitionMethod = PartitionMethod(relationId);
		if (partitionMethod == DISTRIBUTE_BY_RANGE)
		{
			rangeDistributedRelationCount++;
		}
		else if (partitionMethod == DISTRIBUTE_BY_HASH)
		{
			hashDistributedRelationCount++;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot push down this subquery"),
							errdetail("Currently range and hash partitioned "
									  "relations are supported")));
		}
	}

	if ((rangeDistributedRelationCount > 0) && (hashDistributedRelationCount > 0))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("A query including both range and hash "
								  "partitioned relations are unsupported")));
	}

	foreach(relationIdCell, relationIdList)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		bool coPartitionedTables = false;
		Oid currentRelationId = relationId;

		/* get shard list of first relation and continue for the next relation */
		if (relationIndex == 0)
		{
			firstTableRelationId = relationId;
			relationIndex++;

			continue;
		}

		/* check if this table has 1-1 shard partitioning with first table */
		coPartitionedTables = CoPartitionedTables(firstTableRelationId,
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
 * RelationIdList returns list of unique relation ids in query tree.
 */
List *
RelationIdList(Query *query)
{
	List *rangeTableList = NIL;
	List *tableEntryList = NIL;
	List *relationIdList = NIL;
	ListCell *tableEntryCell = NULL;

	ExtractRangeTableRelationWalker((Node *) query, &rangeTableList);
	tableEntryList = TableEntryList(rangeTableList);

	foreach(tableEntryCell, tableEntryList)
	{
		TableEntry *tableEntry = (TableEntry *) lfirst(tableEntryCell);
		Oid relationId = tableEntry->relationId;

		relationIdList = list_append_unique_oid(relationIdList, relationId);
	}

	return relationIdList;
}


/*
 * CoPartitionedTables checks if given two distributed tables have 1-to-1 shard
 * partitioning. It uses shard interval array that are sorted on interval minimum
 * values. Then it compares every shard interval in order and if any pair of
 * shard intervals are not equal it returns false.
 */
static bool
CoPartitionedTables(Oid firstRelationId, Oid secondRelationId)
{
	bool coPartitionedTables = true;
	uint32 intervalIndex = 0;
	DistTableCacheEntry *firstTableCache = DistributedTableCacheEntry(firstRelationId);
	DistTableCacheEntry *secondTableCache = DistributedTableCacheEntry(secondRelationId);
	ShardInterval **sortedFirstIntervalArray = firstTableCache->sortedShardIntervalArray;
	ShardInterval **sortedSecondIntervalArray =
		secondTableCache->sortedShardIntervalArray;
	uint32 firstListShardCount = firstTableCache->shardIntervalArrayLength;
	uint32 secondListShardCount = secondTableCache->shardIntervalArrayLength;
	FmgrInfo *comparisonFunction = firstTableCache->shardIntervalCompareFunction;

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

	for (intervalIndex = 0; intervalIndex < firstListShardCount; intervalIndex++)
	{
		ShardInterval *firstInterval = sortedFirstIntervalArray[intervalIndex];
		ShardInterval *secondInterval = sortedSecondIntervalArray[intervalIndex];

		bool shardIntervalsEqual = ShardIntervalsEqual(comparisonFunction,
													   firstInterval,
													   secondInterval);
		if (!shardIntervalsEqual)
		{
			coPartitionedTables = false;
			break;
		}
	}

	return coPartitionedTables;
}


/*
 * ShardIntervalsEqual checks if given shard intervals have equal min/max values.
 */
static bool
ShardIntervalsEqual(FmgrInfo *comparisonFunction, ShardInterval *firstInterval,
					ShardInterval *secondInterval)
{
	bool shardIntervalsEqual = false;
	Datum firstMin = 0;
	Datum firstMax = 0;
	Datum secondMin = 0;
	Datum secondMax = 0;

	firstMin = firstInterval->minValue;
	firstMax = firstInterval->maxValue;
	secondMin = secondInterval->minValue;
	secondMax = secondInterval->maxValue;

	if (firstInterval->minValueExists && firstInterval->maxValueExists &&
		secondInterval->minValueExists && secondInterval->maxValueExists)
	{
		Datum minDatum = CompareCall2(comparisonFunction, firstMin, secondMin);
		Datum maxDatum = CompareCall2(comparisonFunction, firstMax, secondMax);
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
 * ErrorIfUnsupportedFilters checks if all leaf queries in the given query have
 * same filter on the partition column. Note that if there are queries without
 * any filter on the partition column, they don't break this prerequisite.
 */
static void
ErrorIfUnsupportedFilters(Query *subquery)
{
	List *queryList = NIL;
	ListCell *queryCell = NULL;
	List *subqueryOpExpressionList = NIL;
	List *relationIdList = RelationIdList(subquery);

	/*
	 * Get relation id of any relation in the subquery and create partiton column
	 * for this relation. We will use this column to replace columns on operator
	 * expressions on different tables. Then we compare these operator expressions
	 * to see if they consist of same operator and constant value.
	 */
	Oid relationId = linitial_oid(relationIdList);
	Var *partitionColumn = PartitionColumn(relationId, 0);

	ExtractQueryWalker((Node *) subquery, &queryList);
	foreach(queryCell, queryList)
	{
		Query *query = (Query *) lfirst(queryCell);
		List *opExpressionList = NIL;
		List *newOpExpressionList = NIL;

		bool leafQuery = LeafQuery(query);
		if (!leafQuery)
		{
			continue;
		}

		opExpressionList = PartitionColumnOpExpressionList(query);
		if (opExpressionList == NIL)
		{
			continue;
		}

		newOpExpressionList = ReplaceColumnsInOpExpressionList(opExpressionList,
															   partitionColumn);

		if (subqueryOpExpressionList == NIL)
		{
			subqueryOpExpressionList = newOpExpressionList;
		}
		else
		{
			bool equalOpExpressionLists = EqualOpExpressionLists(subqueryOpExpressionList,
																 newOpExpressionList);
			if (!equalOpExpressionLists)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot push down this subquery"),
								errdetail("Currently all leaf queries need to "
										  "have same filters on partition column")));
			}
		}
	}
}


/*
 * ExtractQueryWalker walks over a query, and finds all queries in the query
 * tree and returns these queries.
 */
bool
ExtractQueryWalker(Node *node, List **queryList)
{
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		(*queryList) = lappend(*queryList, query);
		walkerResult = query_tree_walker(query, ExtractQueryWalker, queryList,
										 QTW_EXAMINE_RTES);
	}

	return walkerResult;
}


/*
 * LeafQuery checks if the given query is a leaf query. Leaf queries have only
 * simple relations in the join tree.
 */
bool
LeafQuery(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	List *joinTreeTableIndexList = NIL;
	ListCell *joinTreeTableIndexCell = NULL;
	bool leafQuery = true;

	/*
	 * Extract all range table indexes from the join tree. Note that sub-queries
	 * that get pulled up by PostgreSQL don't appear in this join tree.
	 */
	ExtractRangeTableIndexWalker((Node *) queryTree->jointree, &joinTreeTableIndexList);
	foreach(joinTreeTableIndexCell, joinTreeTableIndexList)
	{
		/*
		 * Join tree's range table index starts from 1 in the query tree. But,
		 * list indexes start from 0.
		 */
		int joinTreeTableIndex = lfirst_int(joinTreeTableIndexCell);
		int rangeTableListIndex = joinTreeTableIndex - 1;

		RangeTblEntry *rangeTableEntry =
			(RangeTblEntry *) list_nth(rangeTableList, rangeTableListIndex);

		/*
		 * Check if the range table in the join tree is a simple relation.
		 */
		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			leafQuery = false;
		}
	}

	return leafQuery;
}


/*
 * PartitionColumnOpExpressionList returns operator expressions which are on
 * partition column in the query. This function walks over where clause list,
 * finds operator expressions on partition column and returns them in a new list.
 */
List *
PartitionColumnOpExpressionList(Query *query)
{
	List *whereClauseList = WhereClauseList(query->jointree);
	List *partitionColumnOpExpressionList = NIL;

	ListCell *whereClauseCell = NULL;
	foreach(whereClauseCell, whereClauseList)
	{
		Node *whereNode = (Node *) lfirst(whereClauseCell);
		Node *leftArgument = NULL;
		Node *rightArgument = NULL;
		Node *strippedLeftArgument = NULL;
		Node *strippedRightArgument = NULL;
		OpExpr *whereClause = NULL;
		List *argumentList = NIL;
		List *rangetableList = NIL;
		uint32 argumentCount = 0;
		Var *candidatePartitionColumn = NULL;
		Var *partitionColumn = NULL;
		Index rangeTableEntryIndex = 0;
		RangeTblEntry *rangeTableEntry = NULL;
		Oid relationId = InvalidOid;

		if (!IsA(whereNode, OpExpr))
		{
			continue;
		}

		whereClause = (OpExpr *) whereNode;
		argumentList = whereClause->args;

		/*
		 * Select clauses must have two arguments. Note that logic here use to
		 * find select clauses is very similar to IsSelectClause(). But we are
		 * not able to reuse it, because it calls pull_var_clause_default()
		 * which in return deep down calls pull_var_clause_walker(), and this
		 * function errors out for variable level other than 0 which is the case
		 * for lateral joins.
		 */
		argumentCount = list_length(argumentList);
		if (argumentCount != 2)
		{
			continue;
		}

		leftArgument = (Node *) linitial(argumentList);
		rightArgument = (Node *) lsecond(argumentList);
		strippedLeftArgument = strip_implicit_coercions(leftArgument);
		strippedRightArgument = strip_implicit_coercions(rightArgument);

		if (IsA(strippedLeftArgument, Var) && IsA(strippedRightArgument, Const))
		{
			candidatePartitionColumn = (Var *) strippedLeftArgument;
		}
		else if (IsA(strippedLeftArgument, Const) && IsA(strippedRightArgument, Var))
		{
			candidatePartitionColumn = (Var *) strippedRightArgument;
		}
		else
		{
			continue;
		}

		rangetableList = query->rtable;
		rangeTableEntryIndex = candidatePartitionColumn->varno - 1;
		rangeTableEntry = list_nth(rangetableList, rangeTableEntryIndex);

		Assert(rangeTableEntry->rtekind == RTE_RELATION);

		relationId = rangeTableEntry->relid;
		partitionColumn = PartitionKey(relationId);

		if (candidatePartitionColumn->varattno == partitionColumn->varattno)
		{
			partitionColumnOpExpressionList = lappend(partitionColumnOpExpressionList,
													  whereClause);
		}
	}

	return partitionColumnOpExpressionList;
}


/*
 * ReplaceColumnsInOpExpressionList walks over the given operator expression
 * list and copies every one them, replaces columns with the given new column
 * and finally returns new copies in a new list of operator expressions.
 */
List *
ReplaceColumnsInOpExpressionList(List *opExpressionList, Var *newColumn)
{
	List *newOpExpressionList = NIL;

	ListCell *opExpressionCell = NULL;
	foreach(opExpressionCell, opExpressionList)
	{
		OpExpr *opExpression = (OpExpr *) lfirst(opExpressionCell);
		OpExpr *copyOpExpression = (OpExpr *) copyObject(opExpression);
		List *argumentList = copyOpExpression->args;
		List *newArgumentList = NIL;

		Node *leftArgument = (Node *) linitial(argumentList);
		Node *rightArgument = (Node *) lsecond(argumentList);
		Node *strippedLeftArgument = strip_implicit_coercions(leftArgument);
		Node *strippedRightArgument = strip_implicit_coercions(rightArgument);

		if (IsA(strippedLeftArgument, Var))
		{
			newArgumentList = list_make2(newColumn, strippedRightArgument);
		}
		else if (IsA(strippedRightArgument, Var))
		{
			newArgumentList = list_make2(strippedLeftArgument, newColumn);
		}

		copyOpExpression->args = newArgumentList;
		newOpExpressionList = lappend(newOpExpressionList, copyOpExpression);
	}

	return newOpExpressionList;
}


/*
 * EqualOpExpressionLists checks if given two operator expression lists are
 * equal.
 */
static bool
EqualOpExpressionLists(List *firstOpExpressionList, List *secondOpExpressionList)
{
	bool equalOpExpressionLists = false;
	ListCell *firstOpExpressionCell = NULL;
	uint32 equalOpExpressionCount = 0;
	uint32 firstOpExpressionCount = list_length(firstOpExpressionList);
	uint32 secondOpExpressionCount = list_length(secondOpExpressionList);

	if (firstOpExpressionCount != secondOpExpressionCount)
	{
		return false;
	}

	foreach(firstOpExpressionCell, firstOpExpressionList)
	{
		OpExpr *firstOpExpression = (OpExpr *) lfirst(firstOpExpressionCell);
		ListCell *secondOpExpressionCell = NULL;

		foreach(secondOpExpressionCell, secondOpExpressionList)
		{
			OpExpr *secondOpExpression = (OpExpr *) lfirst(secondOpExpressionCell);
			bool equalExpressions = equal(firstOpExpression, secondOpExpression);

			if (equalExpressions)
			{
				equalOpExpressionCount++;
				continue;
			}
		}
	}

	if (equalOpExpressionCount == firstOpExpressionCount)
	{
		equalOpExpressionLists = true;
	}

	return equalOpExpressionLists;
}


/*
 * WorkerLimitCount checks if the given extended node contains a limit node, and
 * if that node can be pushed down. For this, the function checks if this limit
 * count or a meaningful approximation of it can be pushed down to worker nodes.
 * If they can, the function returns the limit count. Otherwise, the function
 * returns null.
 */
static Node *
WorkerLimitCount(MultiExtendedOp *originalOpNode)
{
	Node *workerLimitCount = NULL;
	List *groupClauseList = originalOpNode->groupClauseList;
	List *sortClauseList = originalOpNode->sortClauseList;
	List *targetList = originalOpNode->targetList;

	/* no limit node to push down */
	if (originalOpNode->limitCount == NULL)
	{
		return NULL;
	}

	/*
	 * If we don't have group by clauses, or if we have order by clauses without
	 * aggregates, we can push down the original limit. Else if we have order by
	 * clauses with commutative aggregates, we can push down approximate limits.
	 */
	if (groupClauseList == NIL)
	{
		workerLimitCount = originalOpNode->limitCount;
	}
	else if (sortClauseList != NIL)
	{
		bool orderByNonAggregates = !(HasOrderByAggregate(sortClauseList, targetList));
		bool canApproximate = CanPushDownLimitApproximate(sortClauseList, targetList);

		if (orderByNonAggregates)
		{
			workerLimitCount = originalOpNode->limitCount;
		}
		else if (canApproximate)
		{
			Const *workerLimitConst = (Const *) copyObject(originalOpNode->limitCount);
			int64 workerLimitCountInt64 = (int64) LimitClauseRowFetchCount;
			workerLimitConst->constvalue = Int64GetDatum(workerLimitCountInt64);

			workerLimitCount = (Node *) workerLimitConst;
		}
	}

	/* display debug message on limit push down */
	if (workerLimitCount != NULL)
	{
		Const *workerLimitConst = (Const *) workerLimitCount;
		int64 workerLimitCountInt64 = DatumGetInt64(workerLimitConst->constvalue);

		ereport(DEBUG1, (errmsg("push down of limit count: " INT64_FORMAT,
								workerLimitCountInt64)));
	}

	return workerLimitCount;
}


/*
 * WorkerSortClauseList first checks if the given extended node contains a limit
 * that can be pushed down. If it does, the function then checks if we need to
 * add any sorting and grouping clauses to the sort list we push down for the
 * limit. If we do, the function adds these clauses and returns them. Otherwise,
 * the function returns null.
 */
static List *
WorkerSortClauseList(MultiExtendedOp *originalOpNode)
{
	List *workerSortClauseList = NIL;
	List *groupClauseList = originalOpNode->groupClauseList;
	List *sortClauseList = originalOpNode->sortClauseList;
	List *targetList = originalOpNode->targetList;

	/* if no limit node, no need to push down sort clauses */
	if (originalOpNode->limitCount == NULL)
	{
		return NIL;
	}

	/*
	 * If we are pushing down the limit, push down any order by clauses. Also if
	 * we are pushing down the limit because the order by clauses don't have any
	 * aggregates, add group by clauses to the order by list. We do this because
	 * rows that belong to the same grouping may appear in different "offsets"
	 * in different task results. By ordering on the group by clause, we ensure
	 * that query results are consistent.
	 */
	if (groupClauseList == NIL)
	{
		workerSortClauseList = originalOpNode->sortClauseList;
	}
	else if (sortClauseList != NIL)
	{
		bool orderByNonAggregates = !(HasOrderByAggregate(sortClauseList, targetList));
		bool canApproximate = CanPushDownLimitApproximate(sortClauseList, targetList);

		if (orderByNonAggregates)
		{
			workerSortClauseList = list_copy(sortClauseList);
			workerSortClauseList = list_concat(workerSortClauseList, groupClauseList);
		}
		else if (canApproximate)
		{
			workerSortClauseList = originalOpNode->sortClauseList;
		}
	}

	return workerSortClauseList;
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
	Oid hllTypeId = TypenameGetTypid(HLL_TYPE_NAME);

	ListCell *sortClauseCell = NULL;
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
