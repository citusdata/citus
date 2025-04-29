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
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"

#include "distributed/listutils.h"
#include "distributed/query_utils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/version_compat.h"


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


/*
 * ExtractRangeTableIds walks over the given node, and finds all range
 * table entries.
 */
bool
ExtractRangeTableIds(Node *node, ExtractRangeTableIdsContext *context)
{
	List **rangeTableList = context->idList;
	List *rtable = context->rtable;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblRef))
	{
		int rangeTableIndex = ((RangeTblRef *) node)->rtindex;

		RangeTblEntry *rte = rt_fetch(rangeTableIndex, rtable);
		if (rte->rtekind == RTE_SUBQUERY)
		{
			Query *subquery = rte->subquery;
			context->rtable = subquery->rtable;
			ExtractRangeTableIds((Node *) subquery, context);
			context->rtable = rtable; /* restore original rtable */
		}
		else if (rte->rtekind == RTE_RELATION || rte->rtekind == RTE_FUNCTION)
		{
			(*rangeTableList) = lappend(*rangeTableList, rte);
			ereport(DEBUG4, (errmsg("ExtractRangeTableIds: found range table id %d", rte->relid)));
		}
		else
		{
			ereport(DEBUG4, (errmsg("Unsupported RTE kind in ExtractRangeTableIds %d", rte->rtekind)));
		}
		return false;
	}
	else if (IsA(node, Query))
	{
		context->rtable = ((Query *) node)->rtable;
		query_tree_walker((Query *) node, ExtractRangeTableIds, context, 0);
		context->rtable = rtable; /* restore original rtable */
		return false;
	}
	else
	{
		return expression_tree_walker(node, ExtractRangeTableIds, context);
	}
}


/* 
 * CheckIfAllCitusRTEsAreColocated checks if all distributed tables in the
 * given node are colocated. If they are, it sets the value of rte to a
 * representative table.
 */
bool CheckIfAllCitusRTEsAreColocated(Node *node, List *rtable, RangeTblEntry **rte)
{
	ExtractRangeTableIdsContext context;
	List *idList = NIL;
	context.idList = &idList;
	context.rtable = rtable;
	ExtractRangeTableIds(node, &context);

	RangeTblEntry *rteTmp;
	List *citusRelids = NIL;
	ListCell *lc = NULL;

	foreach(lc, idList)
	{
		rteTmp = (RangeTblEntry *) lfirst(lc);
		if (IsCitusTable(rteTmp->relid))
		{
			citusRelids = lappend_int(citusRelids, rteTmp->relid);
			*rte = rteTmp; // set the value of rte, a representative table
		}
	}

	if (!AllDistributedRelationsInListColocated(citusRelids))
	{
		ereport(DEBUG5, (errmsg("The distributed tables are not colocated")));
		return false;
	}

	return true;

}