/*-------------------------------------------------------------------------
 *
 * multi_join_order.c
 *
 * Routines for constructing the join order list using a rule-based approach.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include <limits.h>

#include "access/nbtree.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_am.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "utils/builtins.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/* Config variables managed via guc.c */
bool LogMultiJoinOrder = false; /* print join order as a debugging aid */
bool EnableSingleHashRepartitioning = false;

/* Function pointer type definition for join rule evaluation functions */
typedef JoinOrderNode *(*RuleEvalFunction) (JoinOrderNode *currentJoinNode,
											TableEntry *candidateTable,
											List *applicableJoinClauses,
											JoinType joinType);

static char *RuleNameArray[JOIN_RULE_LAST] = { 0 }; /* ordered join rule names */
static RuleEvalFunction RuleEvalFunctionArray[JOIN_RULE_LAST] = { 0 }; /* join rules */


/* Local functions forward declarations */
static bool JoinExprListWalker(Node *node, List **joinList);
static bool ExtractLeftMostRangeTableIndex(Node *node, int *rangeTableIndex);
static List * JoinOrderForTable(TableEntry *firstTable, List *tableEntryList,
								List *joinClauseList);
static List * BestJoinOrder(List *candidateJoinOrders);
static List * FewestOfJoinRuleType(List *candidateJoinOrders, JoinRuleType ruleType);
static uint32 JoinRuleTypeCount(List *joinOrder, JoinRuleType ruleTypeToCount);
static List * LatestLargeDataTransfer(List *candidateJoinOrders);
static void PrintJoinOrderList(List *joinOrder);
static uint32 LargeDataTransferLocation(List *joinOrder);
static List * TableEntryListDifference(List *lhsTableList, List *rhsTableList);

/* Local functions forward declarations for join evaluations */
static JoinOrderNode * EvaluateJoinRules(List *joinedTableList,
										 JoinOrderNode *currentJoinNode,
										 TableEntry *candidateTable,
										 List *joinClauseList, JoinType joinType);
static List * RangeTableIdList(List *tableList);
static RuleEvalFunction JoinRuleEvalFunction(JoinRuleType ruleType);
static char * JoinRuleName(JoinRuleType ruleType);
static JoinOrderNode * ReferenceJoin(JoinOrderNode *joinNode, TableEntry *candidateTable,
									 List *applicableJoinClauses, JoinType joinType);
static JoinOrderNode * CartesianProductReferenceJoin(JoinOrderNode *joinNode,
													 TableEntry *candidateTable,
													 List *applicableJoinClauses,
													 JoinType joinType);
static JoinOrderNode * LocalJoin(JoinOrderNode *joinNode, TableEntry *candidateTable,
								 List *applicableJoinClauses, JoinType joinType);
static bool JoinOnColumns(List *currentPartitionColumnList, Var *candidatePartitionColumn,
						  List *joinClauseList);
static JoinOrderNode * SinglePartitionJoin(JoinOrderNode *joinNode,
										   TableEntry *candidateTable,
										   List *applicableJoinClauses,
										   JoinType joinType);
static JoinOrderNode * DualPartitionJoin(JoinOrderNode *joinNode,
										 TableEntry *candidateTable,
										 List *applicableJoinClauses,
										 JoinType joinType);
static JoinOrderNode * CartesianProduct(JoinOrderNode *joinNode,
										TableEntry *candidateTable,
										List *applicableJoinClauses,
										JoinType joinType);
static JoinOrderNode * MakeJoinOrderNode(TableEntry *tableEntry,
										 JoinRuleType joinRuleType,
										 List *partitionColumnList, char partitionMethod,
										 TableEntry *anchorTable);


/*
 * JoinExprList flattens the JoinExpr nodes in the FROM expression and translate implicit
 * joins to inner joins. This function does not consider (right-)nested joins.
 */
List *
JoinExprList(FromExpr *fromExpr)
{
	List *joinList = NIL;
	List *fromList = fromExpr->fromlist;
	ListCell *fromCell = NULL;

	foreach(fromCell, fromList)
	{
		Node *nextNode = (Node *) lfirst(fromCell);

		if (joinList != NIL)
		{
			/* multiple nodes in from clause, add an explicit join between them */
			int nextRangeTableIndex = 0;

			/* find the left most range table in this node */
			ExtractLeftMostRangeTableIndex((Node *) fromExpr, &nextRangeTableIndex);

			RangeTblRef *nextRangeTableRef = makeNode(RangeTblRef);
			nextRangeTableRef->rtindex = nextRangeTableIndex;

			/* join the previous node with nextRangeTableRef */
			JoinExpr *newJoinExpr = makeNode(JoinExpr);
			newJoinExpr->jointype = JOIN_INNER;
			newJoinExpr->rarg = (Node *) nextRangeTableRef;
			newJoinExpr->quals = NULL;

			joinList = lappend(joinList, newJoinExpr);
		}

		JoinExprListWalker(nextNode, &joinList);
	}

	return joinList;
}


/*
 * JoinExprListWalker the JoinExpr nodes in a join tree in the order in which joins are
 * to be executed. If there are no joins then no elements are added to joinList.
 */
static bool
JoinExprListWalker(Node *node, List **joinList)
{
	bool walkerResult = false;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) node;

		walkerResult = JoinExprListWalker(joinExpr->larg, joinList);

		(*joinList) = lappend(*joinList, joinExpr);
	}
	else
	{
		walkerResult = expression_tree_walker(node, JoinExprListWalker,
											  joinList);
	}

	return walkerResult;
}


/*
 * ExtractLeftMostRangeTableIndex extracts the range table index of the left-most
 * leaf in a join tree.
 */
static bool
ExtractLeftMostRangeTableIndex(Node *node, int *rangeTableIndex)
{
	bool walkerResult = false;

	Assert(node != NULL);

	if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) node;

		walkerResult = ExtractLeftMostRangeTableIndex(joinExpr->larg, rangeTableIndex);
	}
	else if (IsA(node, RangeTblRef))
	{
		RangeTblRef *rangeTableRef = (RangeTblRef *) node;

		*rangeTableIndex = rangeTableRef->rtindex;
		walkerResult = true;
	}
	else
	{
		walkerResult = expression_tree_walker(node, ExtractLeftMostRangeTableIndex,
											  rangeTableIndex);
	}

	return walkerResult;
}


/*
 * JoinOnColumns determines whether two columns are joined by a given join clause list.
 */
static bool
JoinOnColumns(List *currentPartitionColumnList, Var *candidateColumn,
			  List *joinClauseList)
{
	if (candidateColumn == NULL || list_length(currentPartitionColumnList) == 0)
	{
		/*
		 * LocalJoin can only be happening if we have both a current column and a target
		 * column, otherwise we are not joining two local tables
		 */
		return false;
	}

	Var *currentColumn = NULL;
	foreach_ptr(currentColumn, currentPartitionColumnList)
	{
		Node *joinClause = NULL;
		foreach_ptr(joinClause, joinClauseList)
		{
			if (!NodeIsEqualsOpExpr(joinClause))
			{
				continue;
			}
			OpExpr *joinClauseOpExpr = castNode(OpExpr, joinClause);
			Var *leftColumn = LeftColumnOrNULL(joinClauseOpExpr);
			Var *rightColumn = RightColumnOrNULL(joinClauseOpExpr);

			/*
			 * Check if both join columns and both partition key columns match, since the
			 * current and candidate column's can't be NULL we know they won't match if either
			 * of the columns resolved to NULL above.
			 */
			if (equal(leftColumn, currentColumn) &&
				equal(rightColumn, candidateColumn))
			{
				return true;
			}
			if (equal(leftColumn, candidateColumn) &&
				equal(rightColumn, currentColumn))
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * NodeIsEqualsOpExpr checks if the node is an OpExpr, where the operator
 * matches OperatorImplementsEquality.
 */
bool
NodeIsEqualsOpExpr(Node *node)
{
	if (!IsA(node, OpExpr))
	{
		return false;
	}
	OpExpr *opExpr = castNode(OpExpr, node);
	return OperatorImplementsEquality(opExpr->opno);
}


/*
 * JoinOrderList calculates the best join order and join rules that apply given
 * the list of tables and join clauses. First, the function generates a set of
 * candidate join orders, each with a different table as its first table. Then,
 * the function chooses among these candidates the join order that transfers the
 * least amount of data across the network, and returns this join order.
 */
List *
JoinOrderList(List *tableEntryList, List *joinClauseList)
{
	List *candidateJoinOrderList = NIL;
	ListCell *tableEntryCell = NULL;

	foreach(tableEntryCell, tableEntryList)
	{
		TableEntry *startingTable = (TableEntry *) lfirst(tableEntryCell);

		/* each candidate join order starts with a different table */
		List *candidateJoinOrder = JoinOrderForTable(startingTable, tableEntryList,
													 joinClauseList);

		if (candidateJoinOrder != NULL)
		{
			candidateJoinOrderList = lappend(candidateJoinOrderList, candidateJoinOrder);
		}
	}

	if (list_length(candidateJoinOrderList) == 0)
	{
		/* there are no plans that we can create, time to error */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("complex joins are only supported when all distributed "
							   "tables are joined on their distribution columns with "
							   "equal operator")));
	}

	List *bestJoinOrder = BestJoinOrder(candidateJoinOrderList);

	/* if logging is enabled, print join order */
	if (LogMultiJoinOrder)
	{
		PrintJoinOrderList(bestJoinOrder);
	}

	return bestJoinOrder;
}


/*
 * JoinOrderForTable creates a join order whose first element is the given first
 * table. To determine each subsequent element in the join order, the function
 * then chooses the table that has the lowest ranking join rule, and with which
 * it can join the table to the previous table in the join order. The function
 * repeats this until it determines all elements in the join order list, and
 * returns this list.
 */
static List *
JoinOrderForTable(TableEntry *firstTable, List *tableEntryList, List *joinClauseList)
{
	JoinRuleType firstJoinRule = JOIN_RULE_INVALID_FIRST;
	int joinedTableCount = 1;
	int totalTableCount = list_length(tableEntryList);

	/* create join node for the first table */
	Oid firstRelationId = firstTable->relationId;
	uint32 firstTableId = firstTable->rangeTableId;
	Var *firstPartitionColumn = PartitionColumn(firstRelationId, firstTableId);
	char firstPartitionMethod = PartitionMethod(firstRelationId);

	JoinOrderNode *firstJoinNode = MakeJoinOrderNode(firstTable, firstJoinRule,
													 list_make1(firstPartitionColumn),
													 firstPartitionMethod,
													 firstTable);

	/* add first node to the join order */
	List *joinOrderList = list_make1(firstJoinNode);
	List *joinedTableList = list_make1(firstTable);
	JoinOrderNode *currentJoinNode = firstJoinNode;

	/* loop until we join all remaining tables */
	while (joinedTableCount < totalTableCount)
	{
		ListCell *pendingTableCell = NULL;
		JoinOrderNode *nextJoinNode = NULL;
		JoinRuleType nextJoinRuleType = JOIN_RULE_LAST;

		List *pendingTableList = TableEntryListDifference(tableEntryList,
														  joinedTableList);

		/*
		 * Iterate over all pending tables, and find the next best table to
		 * join. The best table is the one whose join rule requires the least
		 * amount of data transfer.
		 */
		foreach(pendingTableCell, pendingTableList)
		{
			TableEntry *pendingTable = (TableEntry *) lfirst(pendingTableCell);
			JoinType joinType = JOIN_INNER;

			/* evaluate all join rules for this pending table */
			JoinOrderNode *pendingJoinNode = EvaluateJoinRules(joinedTableList,
															   currentJoinNode,
															   pendingTable,
															   joinClauseList, joinType);

			if (pendingJoinNode == NULL)
			{
				/* no join order could be generated, we try our next pending table */
				continue;
			}

			/* if this rule is better than previous ones, keep it */
			JoinRuleType pendingJoinRuleType = pendingJoinNode->joinRuleType;
			if (pendingJoinRuleType < nextJoinRuleType)
			{
				nextJoinNode = pendingJoinNode;
				nextJoinRuleType = pendingJoinRuleType;
			}
		}

		if (nextJoinNode == NULL)
		{
			/*
			 * There is no next join node found, this will repeat indefinitely hence we
			 * bail and let JoinOrderList try a new initial table
			 */
			return NULL;
		}

		Assert(nextJoinNode != NULL);
		TableEntry *nextJoinedTable = nextJoinNode->tableEntry;

		/* add next node to the join order */
		joinOrderList = lappend(joinOrderList, nextJoinNode);
		joinedTableList = lappend(joinedTableList, nextJoinedTable);
		currentJoinNode = nextJoinNode;

		joinedTableCount++;
	}

	return joinOrderList;
}


/*
 * BestJoinOrder takes in a list of candidate join orders, and determines the
 * best join order among these candidates. The function uses two heuristics for
 * this. First, the function chooses join orders that have the fewest number of
 * join operators that cause large data transfers. Second, the function chooses
 * join orders where large data transfers occur later in the execution.
 */
static List *
BestJoinOrder(List *candidateJoinOrders)
{
	uint32 highestValidIndex = JOIN_RULE_LAST - 1;
	uint32 candidateCount PG_USED_FOR_ASSERTS_ONLY = 0;

	/*
	 * We start with the highest ranking rule type (cartesian product), and walk
	 * over these rules in reverse order. For each rule type, we then keep join
	 * orders that only contain the fewest number of join rules of that type.
	 *
	 * For example, the algorithm chooses join orders like the following:
	 * (a) The algorithm prefers join orders with 2 cartesian products (CP) to
	 * those that have 3 or more, if there isn't a join order with fewer CPs.
	 * (b) Assuming that all join orders have the same number of CPs, the
	 * algorithm prefers join orders with 2 dual partitions (DP) to those that
	 * have 3 or more, if there isn't a join order with fewer DPs; and so
	 * forth.
	 */
	for (uint32 ruleTypeIndex = highestValidIndex; ruleTypeIndex > 0; ruleTypeIndex--)
	{
		JoinRuleType ruleType = (JoinRuleType) ruleTypeIndex;

		candidateJoinOrders = FewestOfJoinRuleType(candidateJoinOrders, ruleType);
	}

	/*
	 * If there is a tie, we pick candidate join orders where large data
	 * transfers happen at later stages of query execution. This results in more
	 * data being filtered via joins, selections, and projections earlier on.
	 */
	candidateJoinOrders = LatestLargeDataTransfer(candidateJoinOrders);

	/* we should have at least one join order left after optimizations */
	candidateCount = list_length(candidateJoinOrders);
	Assert(candidateCount > 0);

	/*
	 * If there still is a tie, we pick the join order whose relation appeared
	 * earliest in the query's range table entry list.
	 */
	List *bestJoinOrder = (List *) linitial(candidateJoinOrders);

	return bestJoinOrder;
}


/*
 * FewestOfJoinRuleType finds join orders that have the fewest number of times
 * the given join rule occurs in the candidate join orders, and filters all
 * other join orders. For example, if four candidate join orders have a join
 * rule appearing 3, 5, 3, and 6 times, only two join orders that have the join
 * rule appearing 3 times will be returned.
 */
static List *
FewestOfJoinRuleType(List *candidateJoinOrders, JoinRuleType ruleType)
{
	List *fewestJoinOrders = NULL;
	uint32 fewestRuleCount = INT_MAX;
	ListCell *joinOrderCell = NULL;

	foreach(joinOrderCell, candidateJoinOrders)
	{
		List *joinOrder = (List *) lfirst(joinOrderCell);
		uint32 ruleTypeCount = JoinRuleTypeCount(joinOrder, ruleType);

		if (ruleTypeCount == fewestRuleCount)
		{
			fewestJoinOrders = lappend(fewestJoinOrders, joinOrder);
		}
		else if (ruleTypeCount < fewestRuleCount)
		{
			fewestJoinOrders = list_make1(joinOrder);
			fewestRuleCount = ruleTypeCount;
		}
	}

	return fewestJoinOrders;
}


/* Counts the number of times the given join rule occurs in the join order. */
static uint32
JoinRuleTypeCount(List *joinOrder, JoinRuleType ruleTypeToCount)
{
	uint32 ruleTypeCount = 0;
	ListCell *joinOrderNodeCell = NULL;

	foreach(joinOrderNodeCell, joinOrder)
	{
		JoinOrderNode *joinOrderNode = (JoinOrderNode *) lfirst(joinOrderNodeCell);

		JoinRuleType ruleType = joinOrderNode->joinRuleType;
		if (ruleType == ruleTypeToCount)
		{
			ruleTypeCount++;
		}
	}

	return ruleTypeCount;
}


/*
 * LatestLargeDataTransfer finds and returns join orders where a large data
 * transfer join rule occurs as late as possible in the join order. Late large
 * data transfers result in more data being filtered before data gets shuffled
 * in the network.
 */
static List *
LatestLargeDataTransfer(List *candidateJoinOrders)
{
	List *latestJoinOrders = NIL;
	uint32 latestJoinLocation = 0;
	ListCell *joinOrderCell = NULL;

	foreach(joinOrderCell, candidateJoinOrders)
	{
		List *joinOrder = (List *) lfirst(joinOrderCell);
		uint32 joinRuleLocation = LargeDataTransferLocation(joinOrder);

		if (joinRuleLocation == latestJoinLocation)
		{
			latestJoinOrders = lappend(latestJoinOrders, joinOrder);
		}
		else if (joinRuleLocation > latestJoinLocation)
		{
			latestJoinOrders = list_make1(joinOrder);
			latestJoinLocation = joinRuleLocation;
		}
	}

	return latestJoinOrders;
}


/*
 * LargeDataTransferLocation finds the first location of a large data transfer
 * join rule, and returns that location. If the join order does not have any
 * large data transfer rules, the function returns one location past the end of
 * the join order list.
 */
static uint32
LargeDataTransferLocation(List *joinOrder)
{
	uint32 joinRuleLocation = 0;
	ListCell *joinOrderNodeCell = NULL;

	foreach(joinOrderNodeCell, joinOrder)
	{
		JoinOrderNode *joinOrderNode = (JoinOrderNode *) lfirst(joinOrderNodeCell);
		JoinRuleType joinRuleType = joinOrderNode->joinRuleType;

		/* we consider the following join rules to cause large data transfers */
		if (joinRuleType == SINGLE_HASH_PARTITION_JOIN ||
			joinRuleType == SINGLE_RANGE_PARTITION_JOIN ||
			joinRuleType == DUAL_PARTITION_JOIN ||
			joinRuleType == CARTESIAN_PRODUCT)
		{
			break;
		}

		joinRuleLocation++;
	}

	return joinRuleLocation;
}


/* Prints the join order list and join rules for debugging purposes. */
static void
PrintJoinOrderList(List *joinOrder)
{
	StringInfo printBuffer = makeStringInfo();
	ListCell *joinOrderNodeCell = NULL;
	bool firstJoinNode = true;

	foreach(joinOrderNodeCell, joinOrder)
	{
		JoinOrderNode *joinOrderNode = (JoinOrderNode *) lfirst(joinOrderNodeCell);
		Oid relationId = joinOrderNode->tableEntry->relationId;
		char *relationName = get_rel_name(relationId);

		if (firstJoinNode)
		{
			appendStringInfo(printBuffer, "[ \"%s\" ]", relationName);
			firstJoinNode = false;
		}
		else
		{
			JoinRuleType ruleType = (JoinRuleType) joinOrderNode->joinRuleType;
			char *ruleName = JoinRuleName(ruleType);

			appendStringInfo(printBuffer, "[ %s ", ruleName);
			appendStringInfo(printBuffer, "\"%s\" ]", relationName);
		}
	}

	ereport(LOG, (errmsg("join order: %s",
						 ApplyLogRedaction(printBuffer->data))));
}


/*
 * TableEntryListDifference returns a list containing table entries that are in
 * the left-hand side table list, but not in the right-hand side table list.
 */
static List *
TableEntryListDifference(List *lhsTableList, List *rhsTableList)
{
	List *tableListDifference = NIL;
	ListCell *lhsTableCell = NULL;

	foreach(lhsTableCell, lhsTableList)
	{
		TableEntry *lhsTableEntry = (TableEntry *) lfirst(lhsTableCell);
		ListCell *rhsTableCell = NULL;
		bool lhsTableEntryExists = false;

		foreach(rhsTableCell, rhsTableList)
		{
			TableEntry *rhsTableEntry = (TableEntry *) lfirst(rhsTableCell);

			if ((lhsTableEntry->relationId == rhsTableEntry->relationId) &&
				(lhsTableEntry->rangeTableId == rhsTableEntry->rangeTableId))
			{
				lhsTableEntryExists = true;
			}
		}

		if (!lhsTableEntryExists)
		{
			tableListDifference = lappend(tableListDifference, lhsTableEntry);
		}
	}

	return tableListDifference;
}


/*
 * EvaluateJoinRules takes in a list of already joined tables and a candidate
 * next table, evaluates different join rules between the two tables, and finds
 * the best join rule that applies. The function returns the applicable join
 * order node which includes the join rule and the partition information.
 */
static JoinOrderNode *
EvaluateJoinRules(List *joinedTableList, JoinOrderNode *currentJoinNode,
				  TableEntry *candidateTable, List *joinClauseList,
				  JoinType joinType)
{
	JoinOrderNode *nextJoinNode = NULL;
	uint32 lowestValidIndex = JOIN_RULE_INVALID_FIRST + 1;
	uint32 highestValidIndex = JOIN_RULE_LAST - 1;

	/*
	 * We first find all applicable join clauses between already joined tables
	 * and the candidate table.
	 */
	List *joinedTableIdList = RangeTableIdList(joinedTableList);
	uint32 candidateTableId = candidateTable->rangeTableId;
	List *applicableJoinClauses = ApplicableJoinClauses(joinedTableIdList,
														candidateTableId,
														joinClauseList);

	/* we then evaluate all join rules in order */
	for (uint32 ruleIndex = lowestValidIndex; ruleIndex <= highestValidIndex; ruleIndex++)
	{
		JoinRuleType ruleType = (JoinRuleType) ruleIndex;
		RuleEvalFunction ruleEvalFunction = JoinRuleEvalFunction(ruleType);

		nextJoinNode = (*ruleEvalFunction)(currentJoinNode,
										   candidateTable,
										   applicableJoinClauses,
										   joinType);

		/* break after finding the first join rule that applies */
		if (nextJoinNode != NULL)
		{
			break;
		}
	}

	if (nextJoinNode == NULL)
	{
		return NULL;
	}

	Assert(nextJoinNode != NULL);
	nextJoinNode->joinType = joinType;
	nextJoinNode->joinClauseList = applicableJoinClauses;
	return nextJoinNode;
}


/* Extracts range table identifiers from the given table list, and returns them. */
static List *
RangeTableIdList(List *tableList)
{
	List *rangeTableIdList = NIL;
	ListCell *tableCell = NULL;

	foreach(tableCell, tableList)
	{
		TableEntry *tableEntry = (TableEntry *) lfirst(tableCell);

		uint32 rangeTableId = tableEntry->rangeTableId;
		rangeTableIdList = lappend_int(rangeTableIdList, rangeTableId);
	}

	return rangeTableIdList;
}


/*
 * JoinRuleEvalFunction returns a function pointer for the rule evaluation
 * function; this rule evaluation function corresponds to the given rule type.
 * The function also initializes the rule evaluation function array in a static
 * code block, if the array has not been initialized.
 */
static RuleEvalFunction
JoinRuleEvalFunction(JoinRuleType ruleType)
{
	static bool ruleEvalFunctionsInitialized = false;

	if (!ruleEvalFunctionsInitialized)
	{
		RuleEvalFunctionArray[REFERENCE_JOIN] = &ReferenceJoin;
		RuleEvalFunctionArray[LOCAL_PARTITION_JOIN] = &LocalJoin;
		RuleEvalFunctionArray[SINGLE_RANGE_PARTITION_JOIN] = &SinglePartitionJoin;
		RuleEvalFunctionArray[SINGLE_HASH_PARTITION_JOIN] = &SinglePartitionJoin;
		RuleEvalFunctionArray[DUAL_PARTITION_JOIN] = &DualPartitionJoin;
		RuleEvalFunctionArray[CARTESIAN_PRODUCT_REFERENCE_JOIN] =
			&CartesianProductReferenceJoin;
		RuleEvalFunctionArray[CARTESIAN_PRODUCT] = &CartesianProduct;

		ruleEvalFunctionsInitialized = true;
	}

	RuleEvalFunction ruleEvalFunction = RuleEvalFunctionArray[ruleType];
	Assert(ruleEvalFunction != NULL);

	return ruleEvalFunction;
}


/* Returns a string name for the given join rule type. */
static char *
JoinRuleName(JoinRuleType ruleType)
{
	static bool ruleNamesInitialized = false;

	if (!ruleNamesInitialized)
	{
		/* use strdup() to be independent of memory contexts */
		RuleNameArray[REFERENCE_JOIN] = strdup("reference join");
		RuleNameArray[LOCAL_PARTITION_JOIN] = strdup("local partition join");
		RuleNameArray[SINGLE_HASH_PARTITION_JOIN] =
			strdup("single hash partition join");
		RuleNameArray[SINGLE_RANGE_PARTITION_JOIN] =
			strdup("single range partition join");
		RuleNameArray[DUAL_PARTITION_JOIN] = strdup("dual partition join");
		RuleNameArray[CARTESIAN_PRODUCT_REFERENCE_JOIN] = strdup(
			"cartesian product reference join");
		RuleNameArray[CARTESIAN_PRODUCT] = strdup("cartesian product");

		ruleNamesInitialized = true;
	}

	char *ruleName = RuleNameArray[ruleType];
	Assert(ruleName != NULL);

	return ruleName;
}


/*
 * ReferenceJoin evaluates if the candidate table is a reference table for inner,
 * left and anti join. For right join, current join node must be represented by
 * a reference table. For full join, both of them must be a reference table.
 */
static JoinOrderNode *
ReferenceJoin(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
			  List *applicableJoinClauses, JoinType joinType)
{
	int applicableJoinCount = list_length(applicableJoinClauses);
	if (applicableJoinCount <= 0)
	{
		return NULL;
	}

	char candidatePartitionMethod = PartitionMethod(candidateTable->relationId);
	char leftPartitionMethod = PartitionMethod(currentJoinNode->tableEntry->relationId);

	if (!IsSupportedReferenceJoin(joinType,
								  leftPartitionMethod == DISTRIBUTE_BY_NONE,
								  candidatePartitionMethod == DISTRIBUTE_BY_NONE))
	{
		return NULL;
	}
	return MakeJoinOrderNode(candidateTable, REFERENCE_JOIN,
							 currentJoinNode->partitionColumnList,
							 currentJoinNode->partitionMethod,
							 currentJoinNode->anchorTable);
}


/*
 * IsSupportedReferenceJoin checks if with this join type we can safely do a simple join
 * on the reference table on all the workers.
 */
bool
IsSupportedReferenceJoin(JoinType joinType, bool leftIsReferenceTable,
						 bool rightIsReferenceTable)
{
	if ((joinType == JOIN_INNER || joinType == JOIN_LEFT || joinType == JOIN_ANTI) &&
		rightIsReferenceTable)
	{
		return true;
	}
	else if ((joinType == JOIN_RIGHT) &&
			 leftIsReferenceTable)
	{
		return true;
	}
	else if (joinType == JOIN_FULL && leftIsReferenceTable && rightIsReferenceTable)
	{
		return true;
	}
	return false;
}


/*
 * ReferenceJoin evaluates if the candidate table is a reference table for inner,
 * left and anti join. For right join, current join node must be represented by
 * a reference table. For full join, both of them must be a reference table.
 */
static JoinOrderNode *
CartesianProductReferenceJoin(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
							  List *applicableJoinClauses, JoinType joinType)
{
	char candidatePartitionMethod = PartitionMethod(candidateTable->relationId);
	char leftPartitionMethod = PartitionMethod(currentJoinNode->tableEntry->relationId);

	if (!IsSupportedReferenceJoin(joinType,
								  leftPartitionMethod == DISTRIBUTE_BY_NONE,
								  candidatePartitionMethod == DISTRIBUTE_BY_NONE))
	{
		return NULL;
	}
	return MakeJoinOrderNode(candidateTable, CARTESIAN_PRODUCT_REFERENCE_JOIN,
							 currentJoinNode->partitionColumnList,
							 currentJoinNode->partitionMethod,
							 currentJoinNode->anchorTable);
}


/*
 * LocalJoin takes the current partition key column and the candidate table's
 * partition key column and the partition method for each table. The function
 * then evaluates if tables in the join order and the candidate table can be
 * joined locally, without any data transfers. If they can, the function returns
 * a join order node for a local join. Otherwise, the function returns null.
 *
 * Anchor table is used to decide whether the JoinOrderNode can be joined
 * locally with the candidate table. That table is updated by each join type
 * applied over JoinOrderNode. Note that, we lost the anchor table after
 * dual partitioning and cartesian product.
 */
static JoinOrderNode *
LocalJoin(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
		  List *applicableJoinClauses, JoinType joinType)
{
	Oid relationId = candidateTable->relationId;
	uint32 tableId = candidateTable->rangeTableId;
	Var *candidatePartitionColumn = PartitionColumn(relationId, tableId);
	List *currentPartitionColumnList = currentJoinNode->partitionColumnList;
	char candidatePartitionMethod = PartitionMethod(relationId);
	char currentPartitionMethod = currentJoinNode->partitionMethod;
	TableEntry *currentAnchorTable = currentJoinNode->anchorTable;

	/*
	 * If we previously dual-hash re-partitioned the tables for a join or made cartesian
	 * product, there is no anchor table anymore. In that case we don't allow local join.
	 */
	if (currentAnchorTable == NULL)
	{
		return NULL;
	}

	/* the partition method should be the same for a local join */
	if (currentPartitionMethod != candidatePartitionMethod)
	{
		return NULL;
	}

	bool joinOnPartitionColumns = JoinOnColumns(currentPartitionColumnList,
												candidatePartitionColumn,
												applicableJoinClauses);
	if (!joinOnPartitionColumns)
	{
		return NULL;
	}

	/* shard interval lists must have 1-1 matching for local joins */
	bool coPartitionedTables = CoPartitionedTables(currentAnchorTable->relationId,
												   relationId);

	if (!coPartitionedTables)
	{
		return NULL;
	}

	/*
	 * Since we are applying a local join to the candidate table we need to keep track of
	 * the partition column of the candidate table on the MultiJoinNode. This will allow
	 * subsequent joins colocated with this candidate table to correctly be recognized as
	 * a local join as well.
	 */
	currentPartitionColumnList = list_append_unique(currentPartitionColumnList,
													candidatePartitionColumn);

	JoinOrderNode *nextJoinNode = MakeJoinOrderNode(candidateTable, LOCAL_PARTITION_JOIN,
													currentPartitionColumnList,
													currentPartitionMethod,
													currentAnchorTable);


	return nextJoinNode;
}


/*
 * SinglePartitionJoin takes the current and the candidate table's partition keys
 * and methods. The function then evaluates if either "tables in the join order"
 * or the candidate table is already partitioned on a join column. If they are,
 * the function returns a join order node with the already partitioned column as
 * the next partition key. Otherwise, the function returns null.
 */
static JoinOrderNode *
SinglePartitionJoin(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
					List *applicableJoinClauses, JoinType joinType)
{
	List *currentPartitionColumnList = currentJoinNode->partitionColumnList;
	char currentPartitionMethod = currentJoinNode->partitionMethod;
	TableEntry *currentAnchorTable = currentJoinNode->anchorTable;
	JoinRuleType currentJoinRuleType = currentJoinNode->joinRuleType;


	Oid relationId = candidateTable->relationId;
	uint32 tableId = candidateTable->rangeTableId;
	Var *candidatePartitionColumn = PartitionColumn(relationId, tableId);
	char candidatePartitionMethod = PartitionMethod(relationId);

	/* outer joins are not supported yet */
	if (IS_OUTER_JOIN(joinType))
	{
		return NULL;
	}

	/*
	 * If we previously dual-hash re-partitioned the tables for a join or made
	 * cartesian product, we currently don't allow a single-repartition join.
	 */
	if (currentJoinRuleType == DUAL_PARTITION_JOIN ||
		currentJoinRuleType == CARTESIAN_PRODUCT)
	{
		return NULL;
	}

	OpExpr *joinClause =
		SinglePartitionJoinClause(currentPartitionColumnList, applicableJoinClauses);
	if (joinClause != NULL)
	{
		if (currentPartitionMethod == DISTRIBUTE_BY_HASH)
		{
			/*
			 * Single hash repartitioning may perform worse than dual hash
			 * repartitioning. Thus, we control it via a guc.
			 */
			if (!EnableSingleHashRepartitioning)
			{
				return NULL;
			}

			return MakeJoinOrderNode(candidateTable, SINGLE_HASH_PARTITION_JOIN,
									 currentPartitionColumnList,
									 currentPartitionMethod,
									 currentAnchorTable);
		}
		else
		{
			return MakeJoinOrderNode(candidateTable, SINGLE_RANGE_PARTITION_JOIN,
									 currentPartitionColumnList,
									 currentPartitionMethod,
									 currentAnchorTable);
		}
	}

	/* evaluate re-partitioning the current table only if the rule didn't apply above */
	if (candidatePartitionMethod != DISTRIBUTE_BY_NONE)
	{
		/*
		 * Create a new unique list (set) with the partition column of the candidate table
		 * to check if a single repartition join will work for this table. When it works
		 * the set is retained on the MultiJoinNode for later local join verification.
		 */
		List *candidatePartitionColumnList = list_make1(candidatePartitionColumn);
		joinClause = SinglePartitionJoinClause(candidatePartitionColumnList,
											   applicableJoinClauses);
		if (joinClause != NULL)
		{
			if (candidatePartitionMethod == DISTRIBUTE_BY_HASH)
			{
				/*
				 * Single hash repartitioning may perform worse than dual hash
				 * repartitioning. Thus, we control it via a guc.
				 */
				if (!EnableSingleHashRepartitioning)
				{
					return NULL;
				}

				return MakeJoinOrderNode(candidateTable,
										 SINGLE_HASH_PARTITION_JOIN,
										 candidatePartitionColumnList,
										 candidatePartitionMethod,
										 candidateTable);
			}
			else
			{
				return MakeJoinOrderNode(candidateTable,
										 SINGLE_RANGE_PARTITION_JOIN,
										 candidatePartitionColumnList,
										 candidatePartitionMethod,
										 candidateTable);
			}
		}
	}

	return NULL;
}


/*
 * SinglePartitionJoinClause walks over the applicable join clause list, and
 * finds an applicable join clause for the given partition column. If no such
 * clause exists, the function returns NULL.
 */
OpExpr *
SinglePartitionJoinClause(List *partitionColumnList, List *applicableJoinClauses)
{
	if (list_length(partitionColumnList) == 0)
	{
		return NULL;
	}

	Var *partitionColumn = NULL;
	foreach_ptr(partitionColumn, partitionColumnList)
	{
		Node *applicableJoinClause = NULL;
		foreach_ptr(applicableJoinClause, applicableJoinClauses)
		{
			if (!NodeIsEqualsOpExpr(applicableJoinClause))
			{
				continue;
			}
			OpExpr *applicableJoinOpExpr = castNode(OpExpr, applicableJoinClause);
			Var *leftColumn = LeftColumnOrNULL(applicableJoinOpExpr);
			Var *rightColumn = RightColumnOrNULL(applicableJoinOpExpr);
			if (leftColumn == NULL || rightColumn == NULL)
			{
				/* not a simple partition column join */
				continue;
			}


			/*
			 * We first check if partition column matches either of the join columns
			 * and if it does, we then check if the join column types match. If the
			 * types are different, we will use different hash functions for the two
			 * column types, and will incorrectly repartition the data.
			 */
			if (equal(leftColumn, partitionColumn) || equal(rightColumn, partitionColumn))
			{
				if (leftColumn->vartype == rightColumn->vartype)
				{
					return applicableJoinOpExpr;
				}
				else
				{
					ereport(DEBUG1, (errmsg("single partition column types do not "
											"match")));
				}
			}
		}
	}

	return NULL;
}


/*
 * DualPartitionJoin evaluates if a join clause exists between "tables in the
 * join order" and the candidate table. If such a clause exists, both tables can
 * be repartitioned on the join column; and the function returns a join order
 * node with the join column as the next partition key. Otherwise, the function
 * returns null.
 */
static JoinOrderNode *
DualPartitionJoin(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
				  List *applicableJoinClauses, JoinType joinType)
{
	OpExpr *joinClause = DualPartitionJoinClause(applicableJoinClauses);
	if (joinClause)
	{
		/* because of the dual partition, anchor table and partition column get lost */
		return MakeJoinOrderNode(candidateTable,
								 DUAL_PARTITION_JOIN,
								 NIL,
								 REDISTRIBUTE_BY_HASH,
								 NULL);
	}

	return NULL;
}


/*
 * DualPartitionJoinClause walks over the applicable join clause list, and finds
 * an applicable join clause for dual re-partitioning. If no such clause exists,
 * the function returns NULL.
 */
OpExpr *
DualPartitionJoinClause(List *applicableJoinClauses)
{
	Node *applicableJoinClause = NULL;
	foreach_ptr(applicableJoinClause, applicableJoinClauses)
	{
		if (!NodeIsEqualsOpExpr(applicableJoinClause))
		{
			continue;
		}
		OpExpr *applicableJoinOpExpr = castNode(OpExpr, applicableJoinClause);
		Var *leftColumn = LeftColumnOrNULL(applicableJoinOpExpr);
		Var *rightColumn = RightColumnOrNULL(applicableJoinOpExpr);

		if (leftColumn == NULL || rightColumn == NULL)
		{
			continue;
		}

		/* we only need to check that the join column types match */
		if (leftColumn->vartype == rightColumn->vartype)
		{
			return applicableJoinOpExpr;
		}
		else
		{
			ereport(DEBUG1, (errmsg("dual partition column types do not match")));
		}
	}

	return NULL;
}


/*
 * CartesianProduct always evaluates to true since all tables can be combined
 * using a cartesian product operator. This function acts as a catch-all rule,
 * in case none of the join rules apply.
 */
static JoinOrderNode *
CartesianProduct(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
				 List *applicableJoinClauses, JoinType joinType)
{
	if (list_length(applicableJoinClauses) == 0)
	{
		/* Because of the cartesian product, anchor table information got lost */
		return MakeJoinOrderNode(candidateTable, CARTESIAN_PRODUCT,
								 currentJoinNode->partitionColumnList,
								 currentJoinNode->partitionMethod,
								 NULL);
	}

	return NULL;
}


/* Constructs and returns a join-order node with the given arguments */
JoinOrderNode *
MakeJoinOrderNode(TableEntry *tableEntry, JoinRuleType joinRuleType,
				  List *partitionColumnList, char partitionMethod,
				  TableEntry *anchorTable)
{
	JoinOrderNode *joinOrderNode = palloc0(sizeof(JoinOrderNode));
	joinOrderNode->tableEntry = tableEntry;
	joinOrderNode->joinRuleType = joinRuleType;
	joinOrderNode->joinType = JOIN_INNER;
	joinOrderNode->partitionColumnList = partitionColumnList;
	joinOrderNode->partitionMethod = partitionMethod;
	joinOrderNode->joinClauseList = NIL;
	joinOrderNode->anchorTable = anchorTable;

	return joinOrderNode;
}


/*
 * IsApplicableJoinClause tests if the current joinClause is applicable to the join at
 * hand.
 *
 * Given a list of left hand tables and a candidate right hand table the join clause is
 * valid if atleast 1 column is from the right hand table AND all columns can be found
 * in either the list of tables on the left *or* in the right hand table.
 */
bool
IsApplicableJoinClause(List *leftTableIdList, uint32 rightTableId, Node *joinClause)
{
	List *varList = pull_var_clause_default(joinClause);
	Var *var = NULL;
	bool joinContainsRightTable = false;
	foreach_ptr(var, varList)
	{
		uint32 columnTableId = var->varno;
		if (rightTableId == columnTableId)
		{
			joinContainsRightTable = true;
		}
		else if (!list_member_int(leftTableIdList, columnTableId))
		{
			/*
			 * We couldn't find this column either on the right hand side (first if
			 * statement), nor in the list on the left. This join clause involves a table
			 * not yet available during the candidate join.
			 */
			return false;
		}
	}

	/*
	 * All columns referenced in this clause are available during this join, now the join
	 * is applicable if we found our candidate table as well
	 */
	return joinContainsRightTable;
}


/*
 * ApplicableJoinClauses finds all join clauses that apply between the given
 * left table list and the right table, and returns these found join clauses.
 */
List *
ApplicableJoinClauses(List *leftTableIdList, uint32 rightTableId, List *joinClauseList)
{
	List *applicableJoinClauses = NIL;

	/* make sure joinClauseList contains only join clauses */
	joinClauseList = JoinClauseList(joinClauseList);

	Node *joinClause = NULL;
	foreach_ptr(joinClause, joinClauseList)
	{
		if (IsApplicableJoinClause(leftTableIdList, rightTableId, joinClause))
		{
			applicableJoinClauses = lappend(applicableJoinClauses, joinClause);
		}
	}

	return applicableJoinClauses;
}


/*
 * Returns the left column only when directly referenced in the given join clause,
 * otherwise NULL is returned.
 */
Var *
LeftColumnOrNULL(OpExpr *joinClause)
{
	List *argumentList = joinClause->args;
	Node *leftArgument = (Node *) linitial(argumentList);

	leftArgument = strip_implicit_coercions(leftArgument);
	if (!IsA(leftArgument, Var))
	{
		return NULL;
	}
	return castNode(Var, leftArgument);
}


/*
 * Returns the right column only when directly referenced in the given join clause,
 * otherwise NULL is returned.
 * */
Var *
RightColumnOrNULL(OpExpr *joinClause)
{
	List *argumentList = joinClause->args;
	Node *rightArgument = (Node *) lsecond(argumentList);

	rightArgument = strip_implicit_coercions(rightArgument);
	if (!IsA(rightArgument, Var))
	{
		return NULL;
	}
	return castNode(Var, rightArgument);
}


/*
 * PartitionColumn builds the partition column for the given relation, and sets
 * the partition column's range table references to the given table identifier.
 *
 * Note that this function returns NULL when called for tables having no
 * distribution keys (i.e reference tables & citus local tables).
 */
Var *
PartitionColumn(Oid relationId, uint32 rangeTableId)
{
	Var *partitionKey = DistPartitionKey(relationId);

	if (partitionKey == NULL)
	{
		return NULL;
	}

	Var *partitionColumn = partitionKey;
	partitionColumn->varno = rangeTableId;
	partitionColumn->varnoold = rangeTableId;

	return partitionColumn;
}


/*
 * DistPartitionKey returns the partition key column for the given relation.
 * Note that in the context of distributed join and query planning, the callers
 * of this function *must* set the partition key column's range table reference
 * (varno) to match the table's location in the query range table list.
 *
 * This function returns NULL when called for tables without distribution keys.
 */
Var *
DistPartitionKey(Oid relationId)
{
	CitusTableCacheEntry *partitionEntry = GetCitusTableCacheEntry(relationId);

	if (CitusTableWithoutDistributionKey(partitionEntry->partitionMethod))
	{
		return NULL;
	}

	return copyObject(partitionEntry->partitionColumn);
}


/*
 * ForceDistPartitionKey is the same as DistPartitionKey but errors out instead
 * of returning NULL if this is called with a relationId of a reference table.
 */
Var *
ForceDistPartitionKey(Oid relationId)
{
	Var *partitionKey = DistPartitionKey(relationId);

	if (partitionKey == NULL)
	{
		ereport(ERROR, (errmsg("no distribution column found for relation %d, "
							   "because it is either a reference table or a "
							   "citus local table", relationId)));
	}

	return partitionKey;
}


/* Returns the partition method for the given relation. */
char
PartitionMethod(Oid relationId)
{
	/* errors out if not a distributed table */
	CitusTableCacheEntry *partitionEntry = GetCitusTableCacheEntry(relationId);

	char partitionMethod = partitionEntry->partitionMethod;

	return partitionMethod;
}


/* Returns the replication model for the given relation. */
char
TableReplicationModel(Oid relationId)
{
	/* errors out if not a distributed table */
	CitusTableCacheEntry *partitionEntry = GetCitusTableCacheEntry(relationId);

	char replicationModel = partitionEntry->replicationModel;

	return replicationModel;
}
