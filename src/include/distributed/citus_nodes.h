/*-------------------------------------------------------------------------
 *
 * citus_nodes.h
 *	  Additional node types, and related infrastructure, for Citus.
 *
 * To add a new node type to Citus, perform the following:
 *
 *   * Add a new CitusNodeTag value to use as a tag for the node. Add
 *     the node's name at a corresponding offset within the array named
 *     CitusNodeTagNamesD at the top of citus_nodefuncs.c
 *
 *   * Describe the node in a struct, which must have a CitusNode as
 *     its first element
 *
 *   * Implement an 'outfunc' for the node in citus_outfuncs.c, using
 *     the macros defined within that file. This function will handle
 *     converting the node to a string
 *
 *   * Implement a 'readfunc' for the node in citus_readfuncs.c, using
 *     the macros defined within that file. This function will handle
 *     converting strings into instances of the node
 *
 *   * Use DEFINE_NODE_METHODS within the nodeMethods array (near the
 *     bottom of citus_nodefuncs.c) to register the node in PostgreSQL
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
#define CITUS_NODE_TAG_START	1200
typedef enum CitusNodeTag
{
	T_MultiNode = CITUS_NODE_TAG_START, /* FIXME: perhaps use something less predicable? */
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
	T_ShardPlacement,
	T_RelationShard
} CitusNodeTag;


const char** CitusNodeTagNames;

#if (PG_VERSION_NUM >= 90600)

#include "nodes/extensible.h"

typedef struct CitusNode
{
	ExtensibleNode extensible;
	CitusNodeTag citus_tag; /* for quick type determination */
} CitusNode;

#define CitusNodeTag(nodeptr)		CitusNodeTagI((Node*) nodeptr)

static inline int
CitusNodeTagI(Node *node)
{
	if (!IsA(node, ExtensibleNode))
	{
		return nodeTag(node);
	}

	return ((CitusNode*)(node))->citus_tag;
}

/* Citus variant of newNode(), don't use directly. */
#define CitusNewNode(size, tag) \
({	CitusNode   *_result; \
	AssertMacro((size) >= sizeof(CitusNode));		/* need the tag, at least */ \
	_result = (CitusNode *) palloc0fast(size); \
	_result->extensible.type = T_ExtensibleNode; \
	_result->extensible.extnodename = CitusNodeTagNames[tag - CITUS_NODE_TAG_START]; \
	_result->citus_tag =(int) (tag); \
	_result; \
})


#else

typedef CitusNodeTag CitusNode;
/*
 * nodeTag equivalent that returns the node tag for both citus and postgres
 * node tag types. Needs to return int as there's no type that covers both
 * postgres and citus node values.
 */
#define CitusNodeTag(nodeptr)		(*((const int*)(nodeptr)))


/* Citus variant of newNode(), don't use directly. */
#define CitusNewNode(size, tag) \
({	Node   *_result; \
	AssertMacro((size) >= sizeof(Node));		/* need the tag, at least */ \
	_result = (Node *) palloc0fast(size); \
	_result->type = (int) (tag); \
	_result; \
})

#endif

/*
 * IsA equivalent that compares node tags, including Citus-specific nodes.
 */
#define CitusIsA(nodeptr,_type_)	(CitusNodeTag(nodeptr) == T_##_type_)


/*
 * CitusMakeNode is Citus variant of makeNode(). Use it to create nodes of
 * the types listed in the CitusNodeTag enum and plain NodeTag. Initializes
 * memory, besides the node tag, to 0.
 */
#define CitusMakeNode(_type_) ((_type_ *) CitusNewNode(sizeof(_type_),T_##_type_))


#endif /* CITUS_NODES_H */
