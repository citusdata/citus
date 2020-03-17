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


static bool CitusQueryableRangeTableRelation(RangeTblEntry *rangeTableEntry);


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
			(walkerMode == EXTRACT_RELATION_ENTRIES &&
			 CitusQueryableRangeTableRelation(rangeTable)))
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
 * CitusQueryableRangeTableRelation returns true if the input range table
 * entry is a relation and it can be used in a distributed query, including
 * local tables and materialized views as well.
 */
static bool
CitusQueryableRangeTableRelation(RangeTblEntry *rangeTableEntry)
{
	char relationKind = '\0';

	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		/* we're only interested in relations */
		return false;
	}

	relationKind = rangeTableEntry->relkind;
	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE ||
		relationKind == RELKIND_FOREIGN_TABLE || relationKind == RELKIND_MATVIEW)
	{
		/*
		 * RELKIND_VIEW are automatically replaced with a subquery in
		 * the query tree, so we ignore them here.
		 *
		 * RELKIND_MATVIEW is equivalent of a local table in postgres.
		 */
		return true;
	}

	return false;
}


/*
 * ExtractRangeTableRelationWalker gathers all range table relation entries
 * in a query. The caller is responsible for checking whether the returned
 * entries are distributed or not.
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
 * SplitIntoQueries returns a list of queries by splitting
 * the given concatenated query string with delimiter ';'
 */
List *
SplitIntoQueries(char *concatenatedQueryString)
{
	List *queries = NIL;
	rsize_t len = (rsize_t) strlen(concatenatedQueryString);
	char *delimiter = ";";
	char *remaining = concatenatedQueryString;
	char *query = strtok_s(concatenatedQueryString, &len, delimiter, &remaining);
	while (query != NULL)
	{
		queries = lappend(queries, query);
		if (len == 0)
		{
			break;
		}
		query = strtok_s(NULL, &len, delimiter, &remaining);
	}
	return queries;
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
