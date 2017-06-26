/*-------------------------------------------------------------------------
 *
 * multi_join_order.c
 *
 * Routines for constructing the join order list using a rule-based approach.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <limits.h>

#include "access/nbtree.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_am.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#include "optimizer/var.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/* Config variables managed via guc.c */
int LargeTableShardCount = 4;   /* shard counts for a large table */
bool LogMultiJoinOrder = false; /* print join order as a debugging aid */

/* Function pointer type definition for join rule evaluation functions */
typedef JoinOrderNode *(*RuleEvalFunction) (JoinOrderNode *currentJoinNode,
											TableEntry *candidateTable,
											List *candidateShardList,
											List *applicableJoinClauses,
											JoinType joinType);

static char *RuleNameArray[JOIN_RULE_LAST] = { 0 }; /* ordered join rule names */
static RuleEvalFunction RuleEvalFunctionArray[JOIN_RULE_LAST] = { 0 }; /* join rules */


/* Local functions forward declarations */
static JoinOrderNode * CreateFirstJoinOrderNode(FromExpr *fromExpr,
												List *tableEntryList);
static bool JoinExprListWalker(Node *node, List **joinList);
static bool ExtractLeftMostRangeTableIndex(Node *node, int *rangeTableIndex);
static List * MergeShardIntervals(List *leftShardIntervalList,
								  List *rightShardIntervalList, JoinType joinType);
static bool ShardIntervalsMatch(List *leftShardIntervalList,
								List *rightShardIntervalList);
static List * JoinOrderForTable(TableEntry *firstTable, List *tableEntryList,
								List *joinClauseList);
static List * BestJoinOrder(List *candidateJoinOrders);
static List * FewestOfJoinRuleType(List *candidateJoinOrders, JoinRuleType ruleType);
static uint32 JoinRuleTypeCount(List *joinOrder, JoinRuleType ruleTypeToCount);
static List * LatestLargeDataTransfer(List *candidateJoinOrders);
static void PrintJoinOrderList(List *joinOrder);
static uint32 LargeDataTransferLocation(List *joinOrder);
static List * TableEntryListDifference(List *lhsTableList, List *rhsTableList);
static TableEntry * FindTableEntry(List *tableEntryList, uint32 tableId);

/* Local functions forward declarations for join evaluations */
static JoinOrderNode * EvaluateJoinRules(List *joinedTableList,
										 JoinOrderNode *currentJoinNode,
										 TableEntry *candidateTable,
										 List *candidateShardList,
										 List *joinClauseList, JoinType joinType);
static List * RangeTableIdList(List *tableList);
static RuleEvalFunction JoinRuleEvalFunction(JoinRuleType ruleType);
static char * JoinRuleName(JoinRuleType ruleType);
static JoinOrderNode * BroadcastJoin(JoinOrderNode *joinNode, TableEntry *candidateTable,
									 List *candidateShardList,
									 List *applicableJoinClauses,
									 JoinType joinType);
static JoinOrderNode * LocalJoin(JoinOrderNode *joinNode, TableEntry *candidateTable,
								 List *candidateShardList, List *applicableJoinClauses,
								 JoinType joinType);
static bool JoinOnColumns(Var *currentPartitioncolumn, Var *candidatePartitionColumn,
						  List *joinClauseList);
static JoinOrderNode * SinglePartitionJoin(JoinOrderNode *joinNode,
										   TableEntry *candidateTable,
										   List *candidateShardList,
										   List *applicableJoinClauses,
										   JoinType joinType);
static JoinOrderNode * DualPartitionJoin(JoinOrderNode *joinNode,
										 TableEntry *candidateTable,
										 List *candidateShardList,
										 List *applicableJoinClauses,
										 JoinType joinType);
static JoinOrderNode * CartesianProduct(JoinOrderNode *joinNode,
										TableEntry *candidateTable,
										List *candidateShardList,
										List *applicableJoinClauses,
										JoinType joinType);
static JoinOrderNode * MakeJoinOrderNode(TableEntry *tableEntry, JoinRuleType
										 joinRuleType, Var *partitionColumn,
										 char partitionMethod);


/*
 * FixedJoinOrderList returns a list of join order nodes for the query in the order
 * specified by the user. This is used to handle join trees that contain OUTER joins.
 * The regular JoinOrderList currently assumes that all joins are inner-joins and can
 * thus be arbitrarily reordered, which is not the case for OUTER joins. At some point
 * we should merge these two functions.
 */
List *
FixedJoinOrderList(FromExpr *fromExpr, List *tableEntryList)
{
	List *joinList = NIL;
	ListCell *joinCell = NULL;
	List *joinWhereClauseList = NIL;
	List *joinOrderList = NIL;
	List *joinedTableList = NIL;
	JoinOrderNode *firstJoinNode = NULL;
	JoinOrderNode *currentJoinNode = NULL;
	ListCell *tableEntryCell = NULL;

	foreach(tableEntryCell, tableEntryList)
	{
		TableEntry *rangeTableEntry = (TableEntry *) lfirst(tableEntryCell);
		Oid relationId = rangeTableEntry->relationId;
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);

		if (cacheEntry->partitionMethod != DISTRIBUTE_BY_NONE &&
			cacheEntry->hasUninitializedShardInterval)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning on this query"),
							errdetail("Shards of relations in outer join queries must "
									  "have shard min/max values.")));
		}
	}

	/* get the FROM section as a flattened list of JoinExpr nodes */
	joinList = JoinExprList(fromExpr);

	/* get the join clauses in the WHERE section for implicit joins */
	joinWhereClauseList = JoinClauseList((List *) fromExpr->quals);

	/* create join node for the first table */
	firstJoinNode = CreateFirstJoinOrderNode(fromExpr, tableEntryList);

	/* add first node to the join order */
	joinOrderList = list_make1(firstJoinNode);
	joinedTableList = list_make1(firstJoinNode->tableEntry);
	currentJoinNode = firstJoinNode;

	foreach(joinCell, joinList)
	{
		JoinExpr *joinExpr = (JoinExpr *) lfirst(joinCell);
		List *onClauseList = list_copy((List *) joinExpr->quals);
		List *joinClauseList = list_copy((List *) joinExpr->quals);
		JoinType joinType = joinExpr->jointype;
		RangeTblRef *nextRangeTableRef = NULL;
		TableEntry *nextTable = NULL;
		JoinOrderNode *nextJoinNode = NULL;
		List *candidateShardList = NIL;
		Node *rightArg = joinExpr->rarg;

		/* get the table on the right hand side of the join */
		if (IsA(rightArg, RangeTblRef))
		{
			nextRangeTableRef = (RangeTblRef *) rightArg;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning on this query"),
							errdetail("Subqueries in outer joins are not supported")));
		}

		nextTable = FindTableEntry(tableEntryList, nextRangeTableRef->rtindex);

		if (joinType == JOIN_INNER)
		{
			/* also consider WHERE clauses for INNER joins */
			joinClauseList = list_concat(joinClauseList, joinWhereClauseList);
		}

		/* get the sorted list of shards to check broadcast/local join possibility */
		candidateShardList = LoadShardIntervalList(nextTable->relationId);

		/* find the best join rule type */
		nextJoinNode = EvaluateJoinRules(joinedTableList, currentJoinNode,
										 nextTable, candidateShardList,
										 joinClauseList, joinType);

		if (nextJoinNode->joinRuleType == BROADCAST_JOIN)
		{
			if (joinType == JOIN_RIGHT || joinType == JOIN_FULL)
			{
				/* the overall interval list is now the same as the right side */
				nextJoinNode->shardIntervalList = candidateShardList;
			}
			else if (list_length(candidateShardList) == 1)
			{
				/* the overall interval list is now the same as the left side */
				nextJoinNode->shardIntervalList = currentJoinNode->shardIntervalList;
			}
			else
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot perform distributed planning on this "
									   "query"),
								errdetail("Cannot perform outer joins with broadcast "
										  "joins of more than 1 shard"),
								errhint("Set citus.large_table_shard_count to 1")));
			}
		}
		else if (nextJoinNode->joinRuleType == LOCAL_PARTITION_JOIN)
		{
			/* shard interval lists must have 1-1 matching for local joins */
			bool shardIntervalsMatch =
				ShardIntervalsMatch(currentJoinNode->shardIntervalList,
									candidateShardList);

			if (shardIntervalsMatch)
			{
				nextJoinNode->shardIntervalList =
					MergeShardIntervals(currentJoinNode->shardIntervalList,
										candidateShardList,
										joinType);
			}
			else
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot perform distributed planning on this "
									   "query"),
								errdetail("Shards of relations in outer join queries "
										  "must have 1-to-1 shard partitioning")));
			}
		}
		else
		{
			/* re-partitioning for OUTER joins is not implemented */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot run outer join query if join is not on the "
								   "partition column"),
							errdetail("Outer joins requiring repartitioning are not "
									  "supported.")));
		}

		if (joinType != JOIN_INNER)
		{
			/* preserve non-join clauses for OUTER joins */
			nextJoinNode->joinClauseList = onClauseList;
		}

		/* add next node to the join order */
		joinOrderList = lappend(joinOrderList, nextJoinNode);
		joinedTableList = lappend(joinedTableList, nextTable);
		currentJoinNode = nextJoinNode;
	}

	if (LogMultiJoinOrder)
	{
		PrintJoinOrderList(joinOrderList);
	}

	return joinOrderList;
}


/*
 * CreateFirstJoinOrderNode creates the join order node for the left-most table in the
 * join tree.
 */
static JoinOrderNode *
CreateFirstJoinOrderNode(FromExpr *fromExpr, List *tableEntryList)
{
	JoinOrderNode *firstJoinNode = NULL;
	TableEntry *firstTable = NULL;
	JoinRuleType firstJoinRule = JOIN_RULE_INVALID_FIRST;
	Var *firstPartitionColumn = NULL;
	char firstPartitionMethod = '\0';
	int rangeTableIndex = 0;

	ExtractLeftMostRangeTableIndex((Node *) fromExpr, &rangeTableIndex);

	firstTable = FindTableEntry(tableEntryList, rangeTableIndex);

	firstPartitionColumn = PartitionColumn(firstTable->relationId,
										   firstTable->rangeTableId);
	firstPartitionMethod = PartitionMethod(firstTable->relationId);

	firstJoinNode = MakeJoinOrderNode(firstTable, firstJoinRule,
									  firstPartitionColumn,
									  firstPartitionMethod);

	firstJoinNode->shardIntervalList = LoadShardIntervalList(firstTable->relationId);

	return firstJoinNode;
}


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
			JoinExpr *newJoinExpr = NULL;
			RangeTblRef *nextRangeTableRef = NULL;
			int nextRangeTableIndex = 0;

			/* find the left most range table in this node */
			ExtractLeftMostRangeTableIndex((Node *) fromExpr, &nextRangeTableIndex);

			nextRangeTableRef = makeNode(RangeTblRef);
			nextRangeTableRef->rtindex = nextRangeTableIndex;

			/* join the previous node with nextRangeTableRef */
			newJoinExpr = makeNode(JoinExpr);
			newJoinExpr->jointype = JOIN_INNER;
			newJoinExpr->rarg = (Node *) nextRangeTableRef;
			newJoinExpr->quals = NULL;
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
 * MergeShardIntervals merges given shard interval lists. It assumes that both lists
 * have the same number of shard intervals, and each shard interval overlaps only with
 * a corresponding shard interval from the other shard interval list. It uses union or
 * intersection logic when merging two shard intervals depending on joinType.
 */
static List *
MergeShardIntervals(List *leftShardIntervalList, List *rightShardIntervalList,
					JoinType joinType)
{
	FmgrInfo *comparisonFunction = NULL;
	ShardInterval *firstShardInterval = NULL;
	Oid typeId = InvalidOid;
	bool typeByValue = false;
	int typeLen = 0;
	ListCell *leftShardIntervalCell = NULL;
	ListCell *rightShardIntervalCell = NULL;
	List *mergedShardIntervalList = NIL;
	bool shardUnion = IS_OUTER_JOIN(joinType);

	Assert(list_length(leftShardIntervalList) > 0);
	Assert(list_length(leftShardIntervalList) == list_length(rightShardIntervalList));

	firstShardInterval = (ShardInterval *) linitial(leftShardIntervalList);
	typeId = firstShardInterval->valueTypeId;
	typeByValue = firstShardInterval->valueByVal;
	typeLen = firstShardInterval->valueTypeLen;

	comparisonFunction = GetFunctionInfo(typeId, BTREE_AM_OID, BTORDER_PROC);

	forboth(leftShardIntervalCell, leftShardIntervalList,
			rightShardIntervalCell, rightShardIntervalList)
	{
		ShardInterval *currentInterval = (ShardInterval *) lfirst(leftShardIntervalCell);
		ShardInterval *nextInterval = (ShardInterval *) lfirst(rightShardIntervalCell);
		ShardInterval *newShardInterval = NULL;
		Datum currentMin = currentInterval->minValue;
		Datum currentMax = currentInterval->maxValue;

		newShardInterval = (ShardInterval *) palloc0(sizeof(ShardInterval));
		CopyShardInterval(currentInterval, newShardInterval);

		if (nextInterval->minValueExists)
		{
			Datum nextMin = nextInterval->minValue;
			Datum comparisonDatum = CompareCall2(comparisonFunction, currentMin, nextMin);
			int comparisonResult = DatumGetInt32(comparisonDatum);
			bool nextMinSmaller = comparisonResult > 0;
			bool nextMinLarger = comparisonResult < 0;

			if ((shardUnion && nextMinSmaller) ||
				(!shardUnion && nextMinLarger))
			{
				newShardInterval->minValue = datumCopy(nextMin, typeByValue, typeLen);
			}
		}

		if (nextInterval->maxValueExists)
		{
			Datum nextMax = nextInterval->maxValue;
			Datum comparisonDatum = CompareCall2(comparisonFunction, currentMax, nextMax);
			int comparisonResult = DatumGetInt32(comparisonDatum);
			bool nextMaxLarger = comparisonResult < 0;
			bool nextMaxSmaller = comparisonResult > 0;

			if ((shardUnion && nextMaxLarger) ||
				(!shardUnion && nextMaxSmaller))
			{
				newShardInterval->maxValue = datumCopy(nextMax, typeByValue, typeLen);
			}
		}

		mergedShardIntervalList = lappend(mergedShardIntervalList, newShardInterval);
	}

	return mergedShardIntervalList;
}


/*
 * JoinOnColumns determines whether two columns are joined by a given join clause
 * list.
 */
static bool
JoinOnColumns(Var *currentColumn, Var *candidateColumn, List *joinClauseList)
{
	ListCell *joinClauseCell = NULL;
	bool joinOnColumns = false;

	foreach(joinClauseCell, joinClauseList)
	{
		OpExpr *joinClause = (OpExpr *) lfirst(joinClauseCell);
		Var *leftColumn = LeftColumn(joinClause);
		Var *rightColumn = RightColumn(joinClause);

		/* check if both join columns and both partition key columns match */
		if (equal(leftColumn, currentColumn) &&
			equal(rightColumn, candidateColumn))
		{
			joinOnColumns = true;
			break;
		}
		if (equal(leftColumn, candidateColumn) &&
			equal(rightColumn, currentColumn))
		{
			joinOnColumns = true;
			break;
		}
	}

	return joinOnColumns;
}


/*
 * ShardIntervalsMatch returns true if provided shard interval has one-to-one
 * matching. Shards intervals must be not empty, and their intervals musht be in
 * ascending order of range min values. Shard interval ranges said to be matched
 * only if (1) they have same number of shards, (2) a shard interval on the left
 * side overlaps with corresponding shard on the right side, (3) a shard interval
 * on the right side does not overlap with any other shard. The function does not
 * compare a left shard with every right shard. It compares the left shard with the
 * previous and next shards of the corresponding shard to check they to not overlap
 * for optimization purposes.
 */
static bool
ShardIntervalsMatch(List *leftShardIntervalList, List *rightShardIntervalList)
{
	int leftShardIntervalCount = list_length(leftShardIntervalList);
	int rightShardIntervalCount = list_length(rightShardIntervalList);
	ListCell *leftShardIntervalCell = NULL;
	ListCell *rightShardIntervalCell = NULL;
	ShardInterval *previousRightInterval = NULL;

	/* we do not support outer join queries on tables with no shards */
	if (leftShardIntervalCount == 0 || rightShardIntervalCount == 0)
	{
		return false;
	}

	if (leftShardIntervalCount != rightShardIntervalCount)
	{
		return false;
	}

	forboth(leftShardIntervalCell, leftShardIntervalList,
			rightShardIntervalCell, rightShardIntervalList)
	{
		ShardInterval *leftInterval = (ShardInterval *) lfirst(leftShardIntervalCell);
		ShardInterval *rightInterval = (ShardInterval *) lfirst(rightShardIntervalCell);
		ListCell *nextRightIntervalCell = NULL;

		bool shardIntervalsIntersect = ShardIntervalsOverlap(leftInterval, rightInterval);
		if (!shardIntervalsIntersect)
		{
			return false;
		}

		/*
		 * Compare left interval with a previous right interval, they should not
		 * intersect.
		 */
		if (previousRightInterval != NULL)
		{
			shardIntervalsIntersect = ShardIntervalsOverlap(leftInterval,
															previousRightInterval);
			if (shardIntervalsIntersect)
			{
				return false;
			}
		}

		/*
		 * Compare left interval with a next right interval, they should not
		 * intersect.
		 */
		nextRightIntervalCell = lnext(rightShardIntervalCell);
		if (nextRightIntervalCell != NULL)
		{
			ShardInterval *nextRightInterval =
				(ShardInterval *) lfirst(nextRightIntervalCell);
			shardIntervalsIntersect = ShardIntervalsOverlap(leftInterval,
															nextRightInterval);
			if (shardIntervalsIntersect)
			{
				return false;
			}
		}

		previousRightInterval = rightInterval;
	}

	return true;
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
	List *bestJoinOrder = NIL;
	List *candidateJoinOrderList = NIL;
	ListCell *tableEntryCell = NULL;

	foreach(tableEntryCell, tableEntryList)
	{
		TableEntry *startingTable = (TableEntry *) lfirst(tableEntryCell);
		List *candidateJoinOrder = NIL;

		/* each candidate join order starts with a different table */
		candidateJoinOrder = JoinOrderForTable(startingTable, tableEntryList,
											   joinClauseList);

		candidateJoinOrderList = lappend(candidateJoinOrderList, candidateJoinOrder);
	}

	bestJoinOrder = BestJoinOrder(candidateJoinOrderList);

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
	JoinOrderNode *currentJoinNode = NULL;
	JoinRuleType firstJoinRule = JOIN_RULE_INVALID_FIRST;
	List *joinOrderList = NIL;
	List *joinedTableList = NIL;
	int joinedTableCount = 1;
	int totalTableCount = list_length(tableEntryList);

	/* create join node for the first table */
	Oid firstRelationId = firstTable->relationId;
	uint32 firstTableId = firstTable->rangeTableId;
	Var *firstPartitionColumn = PartitionColumn(firstRelationId, firstTableId);
	char firstPartitionMethod = PartitionMethod(firstRelationId);

	JoinOrderNode *firstJoinNode = MakeJoinOrderNode(firstTable, firstJoinRule,
													 firstPartitionColumn,
													 firstPartitionMethod);

	/* add first node to the join order */
	joinOrderList = list_make1(firstJoinNode);
	joinedTableList = list_make1(firstTable);
	currentJoinNode = firstJoinNode;

	/* loop until we join all remaining tables */
	while (joinedTableCount < totalTableCount)
	{
		List *pendingTableList = NIL;
		ListCell *pendingTableCell = NULL;
		JoinOrderNode *nextJoinNode = NULL;
		TableEntry *nextJoinedTable = NULL;
		JoinRuleType nextJoinRuleType = JOIN_RULE_LAST;

		pendingTableList = TableEntryListDifference(tableEntryList, joinedTableList);

		/*
		 * Iterate over all pending tables, and find the next best table to
		 * join. The best table is the one whose join rule requires the least
		 * amount of data transfer.
		 */
		foreach(pendingTableCell, pendingTableList)
		{
			TableEntry *pendingTable = (TableEntry *) lfirst(pendingTableCell);
			JoinOrderNode *pendingJoinNode = NULL;
			JoinRuleType pendingJoinRuleType = JOIN_RULE_LAST;
			JoinType joinType = JOIN_INNER;
			List *candidateShardList = LoadShardIntervalList(pendingTable->relationId);

			/* evaluate all join rules for this pending table */
			pendingJoinNode = EvaluateJoinRules(joinedTableList, currentJoinNode,
												pendingTable, candidateShardList,
												joinClauseList, joinType);

			/* if this rule is better than previous ones, keep it */
			pendingJoinRuleType = pendingJoinNode->joinRuleType;
			if (pendingJoinRuleType < nextJoinRuleType)
			{
				nextJoinNode = pendingJoinNode;
				nextJoinRuleType = pendingJoinRuleType;
			}
		}

		Assert(nextJoinNode != NULL);
		nextJoinedTable = nextJoinNode->tableEntry;

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
	List *bestJoinOrder = NULL;
	uint32 ruleTypeIndex = 0;
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
	for (ruleTypeIndex = highestValidIndex; ruleTypeIndex > 0; ruleTypeIndex--)
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
	bestJoinOrder = (List *) linitial(candidateJoinOrders);

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
		if (joinRuleType == SINGLE_PARTITION_JOIN ||
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

	ereport(LOG, (errmsg("join order: %s", printBuffer->data)));
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
 * Finds the table entry in tableEntryList with the given range table id.
 */
static TableEntry *
FindTableEntry(List *tableEntryList, uint32 tableId)
{
	ListCell *tableEntryCell = NULL;

	foreach(tableEntryCell, tableEntryList)
	{
		TableEntry *tableEntry = (TableEntry *) lfirst(tableEntryCell);
		if (tableEntry->rangeTableId == tableId)
		{
			return tableEntry;
		}
	}

	return NULL;
}


/*
 * EvaluateJoinRules takes in a list of already joined tables and a candidate
 * next table, evaluates different join rules between the two tables, and finds
 * the best join rule that applies. The function returns the applicable join
 * order node which includes the join rule and the partition information.
 */
static JoinOrderNode *
EvaluateJoinRules(List *joinedTableList, JoinOrderNode *currentJoinNode,
				  TableEntry *candidateTable, List *candidateShardList,
				  List *joinClauseList, JoinType joinType)
{
	JoinOrderNode *nextJoinNode = NULL;
	uint32 candidateTableId = 0;
	List *joinedTableIdList = NIL;
	List *applicableJoinClauses = NIL;
	uint32 lowestValidIndex = JOIN_RULE_INVALID_FIRST + 1;
	uint32 highestValidIndex = JOIN_RULE_LAST - 1;
	uint32 ruleIndex = 0;

	/*
	 * We first find all applicable join clauses between already joined tables
	 * and the candidate table.
	 */
	joinedTableIdList = RangeTableIdList(joinedTableList);
	candidateTableId = candidateTable->rangeTableId;
	applicableJoinClauses = ApplicableJoinClauses(joinedTableIdList, candidateTableId,
												  joinClauseList);

	/* we then evaluate all join rules in order */
	for (ruleIndex = lowestValidIndex; ruleIndex <= highestValidIndex; ruleIndex++)
	{
		JoinRuleType ruleType = (JoinRuleType) ruleIndex;
		RuleEvalFunction ruleEvalFunction = JoinRuleEvalFunction(ruleType);

		nextJoinNode = (*ruleEvalFunction)(currentJoinNode,
										   candidateTable,
										   candidateShardList,
										   applicableJoinClauses,
										   joinType);

		/* break after finding the first join rule that applies */
		if (nextJoinNode != NULL)
		{
			break;
		}
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
	RuleEvalFunction ruleEvalFunction = NULL;

	if (!ruleEvalFunctionsInitialized)
	{
		RuleEvalFunctionArray[BROADCAST_JOIN] = &BroadcastJoin;
		RuleEvalFunctionArray[LOCAL_PARTITION_JOIN] = &LocalJoin;
		RuleEvalFunctionArray[SINGLE_PARTITION_JOIN] = &SinglePartitionJoin;
		RuleEvalFunctionArray[DUAL_PARTITION_JOIN] = &DualPartitionJoin;
		RuleEvalFunctionArray[CARTESIAN_PRODUCT] = &CartesianProduct;

		ruleEvalFunctionsInitialized = true;
	}

	ruleEvalFunction = RuleEvalFunctionArray[ruleType];
	Assert(ruleEvalFunction != NULL);

	return ruleEvalFunction;
}


/* Returns a string name for the given join rule type. */
static char *
JoinRuleName(JoinRuleType ruleType)
{
	static bool ruleNamesInitialized = false;
	char *ruleName = NULL;

	if (!ruleNamesInitialized)
	{
		/* use strdup() to be independent of memory contexts */
		RuleNameArray[BROADCAST_JOIN] = strdup("broadcast join");
		RuleNameArray[LOCAL_PARTITION_JOIN] = strdup("local partition join");
		RuleNameArray[SINGLE_PARTITION_JOIN] = strdup("single partition join");
		RuleNameArray[DUAL_PARTITION_JOIN] = strdup("dual partition join");
		RuleNameArray[CARTESIAN_PRODUCT] = strdup("cartesian product");

		ruleNamesInitialized = true;
	}

	ruleName = RuleNameArray[ruleType];
	Assert(ruleName != NULL);

	return ruleName;
}


/*
 * BroadcastJoin evaluates if the candidate table is small enough to be
 * broadcasted to all nodes in the system. If the table can be broadcasted,
 * the function simply returns a join order node that includes the current
 * partition key and method. Otherwise, the function returns null.
 */
static JoinOrderNode *
BroadcastJoin(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
			  List *candidateShardList, List *applicableJoinClauses,
			  JoinType joinType)
{
	JoinOrderNode *nextJoinNode = NULL;
	int candidateShardCount = list_length(candidateShardList);
	int leftShardCount = list_length(currentJoinNode->shardIntervalList);
	int applicableJoinCount = list_length(applicableJoinClauses);
	bool performBroadcastJoin = false;

	if (applicableJoinCount <= 0)
	{
		return NULL;
	}

	/*
	 * If the table's shard count doesn't exceed the value specified in the
	 * configuration or the table is a reference table, then we assume table
	 * broadcasting is feasible. This assumption is valid only for inner joins.
	 *
	 * Left join requires candidate table to have single shard, right join requires
	 * existing (left) table to have single shard, full outer join requires both tables
	 * to have single shard.
	 */
	if (joinType == JOIN_INNER)
	{
		ShardInterval *initialCandidateShardInterval = NULL;
		char candidatePartitionMethod = '\0';

		if (candidateShardCount > 0)
		{
			initialCandidateShardInterval =
				(ShardInterval *) linitial(candidateShardList);
			candidatePartitionMethod =
				PartitionMethod(initialCandidateShardInterval->relationId);
		}

		if (candidatePartitionMethod == DISTRIBUTE_BY_NONE ||
			candidateShardCount < LargeTableShardCount)
		{
			performBroadcastJoin = true;
		}
	}
	else if ((joinType == JOIN_LEFT || joinType == JOIN_ANTI) && candidateShardCount == 1)
	{
		performBroadcastJoin = true;
	}
	else if (joinType == JOIN_RIGHT && leftShardCount == 1)
	{
		performBroadcastJoin = true;
	}
	else if (joinType == JOIN_FULL && leftShardCount == 1 && candidateShardCount == 1)
	{
		performBroadcastJoin = true;
	}

	if (performBroadcastJoin)
	{
		nextJoinNode = MakeJoinOrderNode(candidateTable, BROADCAST_JOIN,
										 currentJoinNode->partitionColumn,
										 currentJoinNode->partitionMethod);
	}

	return nextJoinNode;
}


/*
 * LocalJoin takes the current partition key column and the candidate table's
 * partition key column and the partition method for each table. The function
 * then evaluates if tables in the join order and the candidate table can be
 * joined locally, without any data transfers. If they can, the function returns
 * a join order node for a local join. Otherwise, the function returns null.
 */
static JoinOrderNode *
LocalJoin(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
		  List *candidateShardList, List *applicableJoinClauses,
		  JoinType joinType)
{
	JoinOrderNode *nextJoinNode = NULL;
	Oid relationId = candidateTable->relationId;
	uint32 tableId = candidateTable->rangeTableId;
	Var *candidatePartitionColumn = PartitionColumn(relationId, tableId);
	Var *currentPartitionColumn = currentJoinNode->partitionColumn;
	char candidatePartitionMethod = PartitionMethod(relationId);
	char currentPartitionMethod = currentJoinNode->partitionMethod;
	bool joinOnPartitionColumns = false;

	/* the partition method should be the same for a local join */
	if (currentPartitionMethod != candidatePartitionMethod)
	{
		return NULL;
	}

	joinOnPartitionColumns = JoinOnColumns(currentPartitionColumn,
										   candidatePartitionColumn,
										   applicableJoinClauses);
	if (joinOnPartitionColumns)
	{
		nextJoinNode = MakeJoinOrderNode(candidateTable, LOCAL_PARTITION_JOIN,
										 currentPartitionColumn,
										 currentPartitionMethod);
	}

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
					List *candidateShardList, List *applicableJoinClauses,
					JoinType joinType)
{
	JoinOrderNode *nextJoinNode = NULL;
	Var *currentPartitionColumn = currentJoinNode->partitionColumn;
	char currentPartitionMethod = currentJoinNode->partitionMethod;

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
	 * If we previously dual-hash re-partitioned the tables for a join, we
	 * currently don't allow a single-repartition join.
	 */
	if (currentPartitionMethod == REDISTRIBUTE_BY_HASH)
	{
		return NULL;
	}

	if (currentPartitionMethod != DISTRIBUTE_BY_HASH)
	{
		OpExpr *joinClause = SinglePartitionJoinClause(currentPartitionColumn,
													   applicableJoinClauses);

		if (joinClause != NULL)
		{
			nextJoinNode = MakeJoinOrderNode(candidateTable, SINGLE_PARTITION_JOIN,
											 currentPartitionColumn,
											 currentPartitionMethod);
		}
	}

	/* evaluate re-partitioning the current table only if the rule didn't apply above */
	if (nextJoinNode == NULL && candidatePartitionMethod != DISTRIBUTE_BY_HASH &&
		candidatePartitionMethod != DISTRIBUTE_BY_NONE)
	{
		OpExpr *joinClause = SinglePartitionJoinClause(candidatePartitionColumn,
													   applicableJoinClauses);

		if (joinClause != NULL)
		{
			nextJoinNode = MakeJoinOrderNode(candidateTable, SINGLE_PARTITION_JOIN,
											 candidatePartitionColumn,
											 candidatePartitionMethod);
		}
	}

	return nextJoinNode;
}


/*
 * SinglePartitionJoinClause walks over the applicable join clause list, and
 * finds an applicable join clause for the given partition column. If no such
 * clause exists, the function returns NULL.
 */
OpExpr *
SinglePartitionJoinClause(Var *partitionColumn, List *applicableJoinClauses)
{
	OpExpr *joinClause = NULL;
	ListCell *applicableJoinClauseCell = NULL;

	foreach(applicableJoinClauseCell, applicableJoinClauses)
	{
		OpExpr *applicableJoinClause = (OpExpr *) lfirst(applicableJoinClauseCell);
		Var *leftColumn = LeftColumn(applicableJoinClause);
		Var *rightColumn = RightColumn(applicableJoinClause);

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
				joinClause = applicableJoinClause;
				break;
			}
			else
			{
				ereport(DEBUG1, (errmsg("single partition column types do not match")));
			}
		}
	}

	return joinClause;
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
				  List *candidateShardList, List *applicableJoinClauses,
				  JoinType joinType)
{
	JoinOrderNode *nextJoinNode = NULL;

	OpExpr *joinClause = DualPartitionJoinClause(applicableJoinClauses);
	if (joinClause)
	{
		Var *nextPartitionColumn = LeftColumn(joinClause);
		nextJoinNode = MakeJoinOrderNode(candidateTable, DUAL_PARTITION_JOIN,
										 nextPartitionColumn, REDISTRIBUTE_BY_HASH);
	}

	return nextJoinNode;
}


/*
 * DualPartitionJoinClause walks over the applicable join clause list, and finds
 * an applicable join clause for dual re-partitioning. If no such clause exists,
 * the function returns NULL.
 */
OpExpr *
DualPartitionJoinClause(List *applicableJoinClauses)
{
	OpExpr *joinClause = NULL;
	ListCell *applicableJoinClauseCell = NULL;

	foreach(applicableJoinClauseCell, applicableJoinClauses)
	{
		OpExpr *applicableJoinClause = (OpExpr *) lfirst(applicableJoinClauseCell);
		Var *leftColumn = LeftColumn(applicableJoinClause);
		Var *rightColumn = RightColumn(applicableJoinClause);

		/* we only need to check that the join column types match */
		if (leftColumn->vartype == rightColumn->vartype)
		{
			joinClause = applicableJoinClause;
			break;
		}
		else
		{
			ereport(DEBUG1, (errmsg("dual partition column types do not match")));
		}
	}

	return joinClause;
}


/*
 * CartesianProduct always evaluates to true since all tables can be combined
 * using a cartesian product operator. This function acts as a catch-all rule,
 * in case none of the join rules apply.
 */
static JoinOrderNode *
CartesianProduct(JoinOrderNode *currentJoinNode, TableEntry *candidateTable,
				 List *candidateShardList, List *applicableJoinClauses,
				 JoinType joinType)
{
	JoinOrderNode *nextJoinNode = MakeJoinOrderNode(candidateTable, CARTESIAN_PRODUCT,
													currentJoinNode->partitionColumn,
													currentJoinNode->partitionMethod);

	return nextJoinNode;
}


/* Constructs and returns a join-order node with the given arguments */
JoinOrderNode *
MakeJoinOrderNode(TableEntry *tableEntry, JoinRuleType joinRuleType,
				  Var *partitionColumn, char partitionMethod)
{
	JoinOrderNode *joinOrderNode = palloc0(sizeof(JoinOrderNode));
	joinOrderNode->tableEntry = tableEntry;
	joinOrderNode->joinRuleType = joinRuleType;
	joinOrderNode->joinType = JOIN_INNER;
	joinOrderNode->partitionColumn = partitionColumn;
	joinOrderNode->partitionMethod = partitionMethod;
	joinOrderNode->joinClauseList = NIL;

	return joinOrderNode;
}


/*
 * ApplicableJoinClauses finds all join clauses that apply between the given
 * left table list and the right table, and returns these found join clauses.
 */
List *
ApplicableJoinClauses(List *leftTableIdList, uint32 rightTableId, List *joinClauseList)
{
	ListCell *joinClauseCell = NULL;
	List *applicableJoinClauses = NIL;

	/* make sure joinClauseList contains only join clauses */
	joinClauseList = JoinClauseList(joinClauseList);

	foreach(joinClauseCell, joinClauseList)
	{
		OpExpr *joinClause = (OpExpr *) lfirst(joinClauseCell);
		Var *joinLeftColumn = LeftColumn(joinClause);
		Var *joinRightColumn = RightColumn(joinClause);

		uint32 joinLeftTableId = joinLeftColumn->varno;
		uint32 joinRightTableId = joinRightColumn->varno;

		bool leftListHasJoinLeft = list_member_int(leftTableIdList, joinLeftTableId);
		bool leftListHasJoinRight = list_member_int(leftTableIdList, joinRightTableId);

		if ((leftListHasJoinLeft && (rightTableId == joinRightTableId)) ||
			(leftListHasJoinRight && (rightTableId == joinLeftTableId)))
		{
			applicableJoinClauses = lappend(applicableJoinClauses, joinClause);
		}
	}

	return applicableJoinClauses;
}


/* Returns the left column in the given join clause. */
Var *
LeftColumn(OpExpr *joinClause)
{
	List *argumentList = joinClause->args;
	Node *leftArgument = (Node *) linitial(argumentList);

	List *varList = pull_var_clause_default(leftArgument);
	Var *leftColumn = NULL;

	Assert(list_length(varList) == 1);
	leftColumn = (Var *) linitial(varList);

	return leftColumn;
}


/* Returns the right column in the given join clause. */
Var *
RightColumn(OpExpr *joinClause)
{
	List *argumentList = joinClause->args;
	Node *rightArgument = (Node *) lsecond(argumentList);

	List *varList = pull_var_clause_default(rightArgument);
	Var *rightColumn = NULL;

	Assert(list_length(varList) == 1);
	rightColumn = (Var *) linitial(varList);

	return rightColumn;
}


/*
 * PartitionColumn builds the partition column for the given relation, and sets
 * the partition column's range table references to the given table identifier.
 *
 * Note that reference tables do not have partition column. Thus, this function
 * returns NULL when called for reference tables.
 */
Var *
PartitionColumn(Oid relationId, uint32 rangeTableId)
{
	Var *partitionKey = DistPartitionKey(relationId);
	Var *partitionColumn = NULL;

	/* short circuit for reference tables */
	if (partitionKey == NULL)
	{
		return partitionColumn;
	}

	partitionColumn = partitionKey;
	partitionColumn->varno = rangeTableId;
	partitionColumn->varnoold = rangeTableId;

	return partitionColumn;
}


/*
 * DistPartitionKey returns the partition key column for the given relation. Note
 * that in the context of distributed join and query planning, the callers of
 * this function *must* set the partition key column's range table reference
 * (varno) to match the table's location in the query range table list.
 *
 * Note that reference tables do not have partition column. Thus, this function
 * returns NULL when called for reference tables.
 */
Var *
DistPartitionKey(Oid relationId)
{
	DistTableCacheEntry *partitionEntry = DistributedTableCacheEntry(relationId);
	Node *variableNode = NULL;
	Var *partitionKey = NULL;

	/* reference tables do not have partition column */
	if (partitionEntry->partitionMethod == DISTRIBUTE_BY_NONE)
	{
		return NULL;
	}

	/* now obtain partition key and build the var node */
	variableNode = stringToNode(partitionEntry->partitionKeyString);

	partitionKey = (Var *) variableNode;
	Assert(IsA(variableNode, Var));

	return partitionKey;
}


/* Returns the partition method for the given relation. */
char
PartitionMethod(Oid relationId)
{
	/* errors out if not a distributed table */
	DistTableCacheEntry *partitionEntry = DistributedTableCacheEntry(relationId);

	char partitionMethod = partitionEntry->partitionMethod;

	return partitionMethod;
}


/* Returns the replication model for the given relation. */
char
TableReplicationModel(Oid relationId)
{
	/* errors out if not a distributed table */
	DistTableCacheEntry *partitionEntry = DistributedTableCacheEntry(relationId);

	char replicationModel = partitionEntry->replicationModel;

	return replicationModel;
}
