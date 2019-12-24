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


/*
 * BuildExtendedOpNodeProperties is a helper function that simply builds
 * the necessary information for processing the extended op node. The return
 * value should be used in a read-only manner.
 */
ExtendedOpNodeProperties
BuildExtendedOpNodeProperties(MultiExtendedOp *extendedOpNode, bool
							  pullUpIntermediateRows)
{
	ExtendedOpNodeProperties extendedOpNodeProperties;

	bool groupedByDisjointPartitionColumn =
		GroupedByPartitionColumn((MultiNode *) extendedOpNode, extendedOpNode);

	/*
	 * TODO: Only window functions that can be pushed down reach here, thus,
	 * using hasWindowFuncs is safe for now. However, this should be fixed
	 * when we support pull-to-master window functions.
	 */
	bool pushDownWindowFunctions = extendedOpNode->hasWindowFuncs;

	extendedOpNodeProperties.groupedByDisjointPartitionColumn =
		groupedByDisjointPartitionColumn;
	extendedOpNodeProperties.pushDownWindowFunctions = pushDownWindowFunctions;
	extendedOpNodeProperties.pullUpIntermediateRows = pullUpIntermediateRows;

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
