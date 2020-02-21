/*-------------------------------------------------------------------------
 *
 * extended_op_node_utils.c implements the logic for building the necessary
 * information that is shared among both the worker and master extended
 * op nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/extended_op_node_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/pg_dist_partition.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "optimizer/restrictinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"


static bool GroupedByPartitionColumn(MultiNode *node, MultiExtendedOp *opNode);
static bool ExtendedOpNodeContainsRepartitionSubquery(MultiExtendedOp *originalOpNode);

static bool HasNonPartitionColumnDistinctAgg(List *targetEntryList, Node *havingQual,
											 List *tableNodeList);
static bool PartitionColumnInTableList(Var *column, List *tableNodeList);
static bool ShouldPullDistinctColumn(bool repartitionSubquery,
									 bool groupedByDisjointPartitionColumn,
									 bool hasNonPartitionColumnDistinctAgg,
									 bool onlyPushableWindowFunctions);
static bool CanPushDownGroupingAndHaving(bool pullUpIntermediateRows,
										 bool groupedByDisjointPartitionColumn,
										 bool hasWindowFuncs,
										 bool onlyPushableWindowFunctions);


/*
 * BuildExtendedOpNodeProperties is a helper function that simply builds
 * the necessary information for processing the extended op node. The return
 * value should be used in a read-only manner.
 */
ExtendedOpNodeProperties
BuildExtendedOpNodeProperties(MultiExtendedOp *extendedOpNode,
							  bool hasNonDistributableAggregates)
{
	ExtendedOpNodeProperties extendedOpNodeProperties;

	List *tableNodeList = FindNodesOfType((MultiNode *) extendedOpNode, T_MultiTable);
	bool groupedByDisjointPartitionColumn =
		GroupedByPartitionColumn((MultiNode *) extendedOpNode, extendedOpNode);

	bool pullUpIntermediateRows = !groupedByDisjointPartitionColumn &&
								  hasNonDistributableAggregates;

	bool repartitionSubquery = ExtendedOpNodeContainsRepartitionSubquery(extendedOpNode);

	List *targetList = extendedOpNode->targetList;
	Node *havingQual = extendedOpNode->havingQual;
	bool hasNonPartitionColumnDistinctAgg =
		HasNonPartitionColumnDistinctAgg(targetList, havingQual, tableNodeList);

	bool pushDownGroupingAndHaving =
		CanPushDownGroupingAndHaving(pullUpIntermediateRows,
									 groupedByDisjointPartitionColumn,
									 extendedOpNode->hasWindowFuncs,
									 extendedOpNode->onlyPushableWindowFunctions);

	bool pullDistinctColumns =
		ShouldPullDistinctColumn(repartitionSubquery,
								 groupedByDisjointPartitionColumn,
								 hasNonPartitionColumnDistinctAgg,
								 extendedOpNode->onlyPushableWindowFunctions);

	extendedOpNodeProperties.groupedByDisjointPartitionColumn =
		groupedByDisjointPartitionColumn;
	extendedOpNodeProperties.repartitionSubquery = repartitionSubquery;
	extendedOpNodeProperties.hasNonPartitionColumnDistinctAgg =
		hasNonPartitionColumnDistinctAgg;
	extendedOpNodeProperties.pullDistinctColumns = pullDistinctColumns;
	extendedOpNodeProperties.pullUpIntermediateRows = pullUpIntermediateRows;
	extendedOpNodeProperties.hasWindowFuncs = extendedOpNode->hasWindowFuncs;
	extendedOpNodeProperties.onlyPushableWindowFunctions =
		extendedOpNode->onlyPushableWindowFunctions;
	extendedOpNodeProperties.pushDownGroupingAndHaving = pushDownGroupingAndHaving;

	return extendedOpNodeProperties;
}


/*
 * GroupedByPartitionColumn returns true if a GROUP BY in the opNode contains
 * the partition column of the underlying relation, which is determined by
 * searching the MultiNode tree for a MultiTable and MultiPartition with
 * a matching column.
 *
 * When there is a re-partition join, the search terminates at the
 * MultiPartition node. Hence we can push down the GROUP BY if the join
 * column is in the GROUP BY.
 */
static bool
GroupedByPartitionColumn(MultiNode *node, MultiExtendedOp *opNode)
{
	if (node == NULL)
	{
		return false;
	}

	if (CitusIsA(node, MultiTable))
	{
		MultiTable *tableNode = (MultiTable *) node;

		Oid relationId = tableNode->relationId;

		if (relationId == SUBQUERY_RELATION_ID ||
			relationId == SUBQUERY_PUSHDOWN_RELATION_ID)
		{
			/* ignore subqueries for now */
			return false;
		}

		char partitionMethod = PartitionMethod(relationId);
		if (partitionMethod != DISTRIBUTE_BY_RANGE &&
			partitionMethod != DISTRIBUTE_BY_HASH)
		{
			/* only range- and hash-distributed tables are strictly partitioned  */
			return false;
		}

		if (GroupedByColumn(opNode->groupClauseList, opNode->targetList,
							tableNode->partitionColumn))
		{
			/* this node is partitioned by a column in the GROUP BY */
			return true;
		}
	}
	else if (CitusIsA(node, MultiPartition))
	{
		MultiPartition *partitionNode = (MultiPartition *) node;

		if (GroupedByColumn(opNode->groupClauseList, opNode->targetList,
							partitionNode->partitionColumn))
		{
			/* this node is partitioned by a column in the GROUP BY */
			return true;
		}
	}
	else if (UnaryOperator(node))
	{
		MultiNode *childNode = ((MultiUnaryNode *) node)->childNode;

		if (GroupedByPartitionColumn(childNode, opNode))
		{
			/* a child node is partitioned by a column in the GROUP BY */
			return true;
		}
	}
	else if (BinaryOperator(node))
	{
		MultiNode *leftChildNode = ((MultiBinaryNode *) node)->leftChildNode;
		MultiNode *rightChildNode = ((MultiBinaryNode *) node)->rightChildNode;

		if (GroupedByPartitionColumn(leftChildNode, opNode) ||
			GroupedByPartitionColumn(rightChildNode, opNode))
		{
			/* a child node is partitioned by a column in the GROUP BY */
			return true;
		}
	}

	return false;
}


/*
 * ExtendedOpNodeContainsRepartitionSubquery is a utility function that
 * returns true if the extended op node contains a re-partition subquery.
 */
static bool
ExtendedOpNodeContainsRepartitionSubquery(MultiExtendedOp *originalOpNode)
{
	MultiNode *parentNode = ParentNode((MultiNode *) originalOpNode);
	MultiNode *childNode = ChildNode((MultiUnaryNode *) originalOpNode);

	if (CitusIsA(parentNode, MultiTable) && CitusIsA(childNode, MultiCollect))
	{
		return true;
	}

	return false;
}


/*
 * HasNonPartitionColumnDistinctAgg returns true if target entry or having qualifier
 * has non-partition column reference in aggregate (distinct) definition. Note that,
 * it only checks aggs subfield of Aggref, it does not check FILTER or SORT clauses.
 * Having any non-column reference like operator expression, function call, or const
 * is considered as a non-partition column. Even if the expression contains partition column
 * like (column + 1), it needs to be evaluated at coordinator, since we can't reliably verify
 * the distinctness of the expression result like (column % 5) or (column + column).
 */
static bool
HasNonPartitionColumnDistinctAgg(List *targetEntryList, Node *havingQual,
								 List *tableNodeList)
{
	List *targetVarList = pull_var_clause((Node *) targetEntryList,
										  PVC_INCLUDE_AGGREGATES |
										  PVC_RECURSE_WINDOWFUNCS);

	/* having clause can't have window functions, no need to recurse for that */
	List *havingVarList = pull_var_clause((Node *) havingQual, PVC_INCLUDE_AGGREGATES);
	List *aggregateCheckList = list_concat(targetVarList, havingVarList);

	ListCell *aggregateCheckCell = NULL;
	foreach(aggregateCheckCell, aggregateCheckList)
	{
		Node *targetNode = lfirst(aggregateCheckCell);
		ListCell *varCell = NULL;
		bool isPartitionColumn = false;

		if (IsA(targetNode, Var))
		{
			continue;
		}

		Assert(IsA(targetNode, Aggref));
		Aggref *targetAgg = (Aggref *) targetNode;
		if (targetAgg->aggdistinct == NIL)
		{
			continue;
		}

		/*
		 * We are dealing with a more complex count distinct, it needs to be
		 * evaluated at coordinator level.
		 */
		if (list_length(targetAgg->args) > 1 || list_length(targetAgg->aggdistinct) > 1)
		{
			return true;
		}

		TargetEntry *firstTargetEntry = linitial_node(TargetEntry, targetAgg->args);
		Node *firstTargetExprNode = strip_implicit_coercions(
			(Node *) firstTargetEntry->expr);
		if (!IsA(firstTargetExprNode, Var))
		{
			return true;
		}

		List *varList = pull_var_clause_default((Node *) targetAgg->args);
		foreach(varCell, varList)
		{
			Node *targetVar = (Node *) lfirst(varCell);

			Assert(IsA(targetVar, Var));

			isPartitionColumn =
				PartitionColumnInTableList((Var *) targetVar, tableNodeList);

			if (!isPartitionColumn)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * PartitionColumnInTableList returns true if provided column is a partition
 * column from provided table node list. It also returns false if a column is
 * partition column of an append distributed table.
 */
static bool
PartitionColumnInTableList(Var *column, List *tableNodeList)
{
	ListCell *tableNodeCell = NULL;
	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = lfirst(tableNodeCell);
		Var *partitionColumn = tableNode->partitionColumn;

		if (partitionColumn != NULL &&
			partitionColumn->varno == column->varno &&
			partitionColumn->varattno == column->varattno)
		{
			Assert(partitionColumn->varno == tableNode->rangeTableId);

			if (PartitionMethod(tableNode->relationId) != DISTRIBUTE_BY_APPEND)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * ShouldPullDistinctColumn returns true if distinct aggregate should pull
 * individual columns from worker to master and evaluate aggregate operation
 * at master.
 *
 * Pull cases are:
 * - repartition subqueries
 * - query has count distinct on a non-partition column on at least one target
 * - count distinct is on a non-partition column and query is not
 *   grouped on partition column
 */
static bool
ShouldPullDistinctColumn(bool repartitionSubquery,
						 bool groupedByDisjointPartitionColumn,
						 bool hasNonPartitionColumnDistinctAgg,
						 bool onlyPushableWindowFunctions)
{
	if (repartitionSubquery)
	{
		return true;
	}

	/* don't pull distinct columns when it can be pushed down */
	if (onlyPushableWindowFunctions && groupedByDisjointPartitionColumn)
	{
		return false;
	}
	else if (hasNonPartitionColumnDistinctAgg)
	{
		return true;
	}

	return false;
}


/*
 * CanPushDownGroupingAndHaving returns whether GROUP BY & HAVING should be
 * pushed down to worker.
 */
static bool
CanPushDownGroupingAndHaving(bool pullUpIntermediateRows,
							 bool groupedByDisjointPartitionColumn,
							 bool hasWindowFuncs, bool onlyPushableWindowFunctions)
{
	/* don't push down if we're pulling up */
	if (pullUpIntermediateRows)
	{
		return false;
	}

	/*
	 * If grouped by a partition column we can push down the having qualifier.
	 *
	 * When a query with subquery is provided, we can't determine if
	 * groupedByDisjointPartitionColumn, therefore we also check if there is a
	 * window function too. If there is a window function we would know that it
	 * is safe to push down (i.e. it is partitioned on distribution column, and
	 * if there is a group by, it contains distribution column).
	 */
	return groupedByDisjointPartitionColumn ||
		   (hasWindowFuncs && onlyPushableWindowFunctions);
}
