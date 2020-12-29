/*-------------------------------------------------------------------------
 *
 * var_utils.c
 *	  Utilities regarding vars
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "distributed/pg_version_constants.h"

#include "postgres.h"

#include "distributed/var_utils.h"

#include "parser/parsetree.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "nodes/primnodes.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif

typedef struct PullTableContext
{
	List *rteList;
	List *rtableList;
}PullTableContext;


static bool PullAllRangeTablesEntriesWalker(Node *node,
											PullTableContext *pullTableContext);


/*
 * PullAllRangeTablesEntries collects all the range table entries that are referenced
 * in the given query. The range table entry could be outside of the given query but a var
 * could exist that points to it.
 * rtableList should contain list of rtables up to this query from the top query.
 */
List *
PullAllRangeTablesEntries(Query *query, List *rtableList)
{
	PullTableContext p;
	p.rtableList = rtableList;
	p.rteList = NIL;

	query_or_expression_tree_walker((Node *) query,
									PullAllRangeTablesEntriesWalker,
									(void *) &p,
									0);

	return p.rteList;
}


/*
 * PullAllRangeTablesEntriesWalker descends into the given node and collects
 * all range table entries that are referenced by a Var.
 */
static bool
PullAllRangeTablesEntriesWalker(Node *node, PullTableContext *pullTableContext)
{
	if (node == NULL)
	{
		return false;
	}
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		List *rtableList = pullTableContext->rtableList;
		int index = var->varlevelsup;
		if (index >= list_length(rtableList) || index < 0)
		{
			ereport(ERROR, (errmsg("unexpected state: %d is not between 0-%d", index,
								   list_length(rtableList))));
		}
		List *rtable = (List *) list_nth(rtableList, index);
		RangeTblEntry *rte = (RangeTblEntry *) list_nth(rtable, var->varno - 1);
		pullTableContext->rteList = lappend(pullTableContext->rteList, rte);
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		/* PlaceHolderVar *phv = (PlaceHolderVar *) node; */

		/* we don't want to look into the contained expression */
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */

		Query *query = (Query *) node;
		pullTableContext->rtableList = lcons(query->rtable, pullTableContext->rtableList);
		bool result = query_tree_walker(query, PullAllRangeTablesEntriesWalker,
										(void *) pullTableContext, 0);
		pullTableContext->rtableList = list_delete_first(pullTableContext->rtableList);
		return result;
	}
	return expression_tree_walker(node, PullAllRangeTablesEntriesWalker,
								  (void *) pullTableContext);
}
