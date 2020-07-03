/*-------------------------------------------------------------------------
 *
 * extended_op_node_utils.h
 *	  General Citus planner code.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef EXTENDED_OP_NODE_UTILS_H_
#define EXTENDED_OP_NODE_UTILS_H_

#include "distributed/multi_logical_planner.h"


/*
 * ExtendedOpNodeProperties is a helper structure that is used to
 * share the common information among the worker and coordinator extended
 * op nodes.
 *
 * It is designed to be a read-only singleton object per extended op node
 * generation and processing.
 */
typedef struct ExtendedOpNodeProperties
{
	bool groupedByDisjointPartitionColumn;
	bool repartitionSubquery;
	bool hasNonPartitionColumnDistinctAgg;
	bool pullDistinctColumns;
	bool hasWindowFuncs;
	bool onlyPushableWindowFunctions;
	bool pullUpIntermediateRows;
	bool pushDownGroupingAndHaving;

	/* indicates whether the MultiExtendedOp has a GROUP BY */
	bool hasGroupBy;

	/* indicates whether the MultiExtendedOp has an aggregate on the target list */
	bool hasAggregate;
} ExtendedOpNodeProperties;


extern ExtendedOpNodeProperties BuildExtendedOpNodeProperties(
	MultiExtendedOp *extendedOpNode, bool hasNonDistributableAggregates);


#endif /* EXTENDED_OP_NODE_UTILS_H_ */
