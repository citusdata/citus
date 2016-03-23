/*-------------------------------------------------------------------------
 *
 * citus_nodes.h
 *	  Additional node types, and related infrastructure, for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_NODES_H
#define CITUS_NODES_H


/*
 * Citus Node Tags
 *
 * These have to be distinct from the ideas used in postgres' nodes.h
 */
typedef enum CitusNodeTag
{
	T_MultiNode = 1200, /* FIXME: perhaps use something less predicable? */
	T_MultiTreeRoot,
	T_MultiProject,
	T_MultiCollect,
	T_MultiSelect,
	T_MultiTable,
	T_MultiJoin,
	T_MultiPartition,
	T_MultiCartesianProduct,
	T_MultiExtendedOp,
	T_Job,
	T_MapMergeJob,
	T_MultiPlan,
	T_Task,
	T_ShardInterval,
	T_ShardPlacement
} CitusNodeTag;


/*
 * nodeTag equivalent that returns the node tag for both citus and postgres
 * node tag types. Needs to return int as there's no type that covers both
 * postgres and citus node values.
 */
#define CitusNodeTag(nodeptr)		(*((const int*)(nodeptr)))


/*
 * IsA equivalent that compares node tags as integers, not as enum values.
 */
#define CitusIsA(nodeptr,_type_)	(CitusNodeTag(nodeptr) == T_##_type_)


/* Citus variant of newNode(), don't use directly. */
#define CitusNewNode(size, tag) \
({	Node   *_result; \
	AssertMacro((size) >= sizeof(Node));		/* need the tag, at least */ \
	_result = (Node *) palloc0fast(size); \
	_result->type = (int) (tag); \
	_result; \
})


/*
 * CitusMakeNode is Citus variant of makeNode(). Use it to create nodes of
 * the types listed in the CitusNodeTag enum and plain NodeTag. Initializes
 * memory, besides the node tag, to 0.
 */
#define CitusMakeNode(_type_) ((_type_ *) CitusNewNode(sizeof(_type_),T_##_type_))


#endif /* CITUS_NODES_H */
