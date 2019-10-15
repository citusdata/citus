/*-------------------------------------------------------------------------
 *
 * query_utils.c
 *
 * Query-walker utility functions to be used to construct a logical plan
 * tree from the given query tree structure.
 *
 * Copyright (c), Citus Data, Inc.
 *
 **----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_class.h"
#include "distributed/query_utils.h"
#include "distributed/version_compat.h"
#include "nodes/nodeFuncs.h"

/*
 * ExtractRangeTableList walks over a tree to gather entries.
 * Execution is parameterized by passing walkerMode flag via ExtractRangeTableWalkerContext
 * as we cannot pass more than one parameter to query_tree_walker
 */
bool
ExtractRangeTableList(Node *node, ExtractRangeTableWalkerContext *context)
{
	/* get parameters from context */
	List **rangeTableRelationList = context->rangeTableList;
	ExtractRangeTableMode walkerMode = context->walkerMode;

	bool walkIsComplete = false;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTable = (RangeTblEntry *) node;

		if (walkerMode == EXTRACT_ALL_ENTRIES ||
			(rangeTable->rtekind == RTE_RELATION && rangeTable->relkind != RELKIND_VIEW))
		{
			(*rangeTableRelationList) = lappend(*rangeTableRelationList, rangeTable);
		}
	}
	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		if (query->hasSubLinks || query->cteList || query->setOperations)
		{
			/* descend into all parts of the query */
			walkIsComplete = query_tree_walker(query,
											   ExtractRangeTableList,
											   context,
											   QTW_EXAMINE_RTES_BEFORE);
		}
		else
		{
			/* descend only into RTEs */
			walkIsComplete = range_table_walker(query->rtable,
												ExtractRangeTableList,
												context,
												QTW_EXAMINE_RTES_BEFORE);
		}
	}
	else
	{
		walkIsComplete = expression_tree_walker(node, ExtractRangeTableList,
												context);
	}

	return walkIsComplete;
}


/*
 * ExtractRangeTableRelationWalker gathers all range table relation entries
 * in a query.
 */
bool
ExtractRangeTableRelationWalker(Node *node, List **rangeTableRelationList)
{
	ExtractRangeTableWalkerContext context;

	context.rangeTableList = rangeTableRelationList;
	context.walkerMode = EXTRACT_RELATION_ENTRIES;

	return ExtractRangeTableList(node, &context);
}


/*
 * ExtractRangeTableEntryWalker walks over a query tree, and finds all range
 * table entries. For recursing into the query tree, this function uses the
 * query tree walker since the expression tree walker doesn't recurse into
 * sub-queries.
 */
bool
ExtractRangeTableEntryWalker(Node *node, List **rangeTableList)
{
	ExtractRangeTableWalkerContext context;

	context.rangeTableList = rangeTableList;
	context.walkerMode = EXTRACT_ALL_ENTRIES;

	return ExtractRangeTableList(node, &context);
}


/*
 * ExtractRangeTableIndexWalker walks over a join tree, and finds all range
 * table indexes in that tree.
 */
bool
ExtractRangeTableIndexWalker(Node *node, List **rangeTableIndexList)
{
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblRef))
	{
		int rangeTableIndex = ((RangeTblRef *) node)->rtindex;
		(*rangeTableIndexList) = lappend_int(*rangeTableIndexList, rangeTableIndex);
	}
	else
	{
		walkerResult = expression_tree_walker(node, ExtractRangeTableIndexWalker,
											  rangeTableIndexList);
	}

	return walkerResult;
}
