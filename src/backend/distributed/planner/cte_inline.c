/*-------------------------------------------------------------------------
 *
 * cte_inline.c
 *	  For multi-shard queries, Citus can only recursively plan CTEs. Instead,
 *	  with the functions defined in this file, the certain CTEs can be inlined
 *	  as subqueries in the query tree. In that case, more optimal distributed
 *	  planning, the query pushdown planning, kicks in and the CTEs can actually
 *	  be pushed down as long as it is safe to pushdown as a subquery.
 *
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pg_version_compat.h"
#include "distributed/pg_version_constants.h"

#include "distributed/cte_inline.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "rewrite/rewriteManip.h"

typedef struct inline_cte_walker_context
{
	const char *ctename;       /* name and relative level of target CTE */
	int levelsup;
	int refcount;              /* number of remaining references */
	Query *ctequery;           /* query to substitute */

	List *aliascolnames;  /* citus addition to Postgres' inline_cte_walker_context */
} inline_cte_walker_context;

/* copy & paste from Postgres source, moved into a function for readability */
static bool PostgreSQLCTEInlineCondition(CommonTableExpr *cte, CmdType cmdType);

/* the following utility functions are copy & paste from PostgreSQL code */
static void inline_cte(Query *mainQuery, CommonTableExpr *cte);
static bool inline_cte_walker(Node *node, inline_cte_walker_context *context);
static bool contain_dml(Node *node);
static bool contain_dml_walker(Node *node, void *context);


/* the following utility functions are related to Citus' logic */
static bool RecursivelyInlineCteWalker(Node *node, void *context);
static void InlineCTEsInQueryTree(Query *query);
static bool QueryTreeContainsInlinableCteWalker(Node *node);


/*
 * RecursivelyInlineCtesInQueryTree gets a query and recursively traverses the
 * tree from top to bottom. On each level, the CTEs that are eligable for
 * inlining are inlined as subqueries. This is useful in distributed planning
 * because Citus' sub(query) planning logic superior to CTE planning, where CTEs
 * are always recursively planned, which might produce very slow executions.
 */
void
RecursivelyInlineCtesInQueryTree(Query *query)
{
	InlineCTEsInQueryTree(query);

	query_tree_walker(query, RecursivelyInlineCteWalker, NULL, 0);
}


/*
 * RecursivelyInlineCteWalker recursively finds all the Query nodes and
 * recursively inline eligable ctes.
 */
static bool
RecursivelyInlineCteWalker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		InlineCTEsInQueryTree(query);

		query_tree_walker(query, RecursivelyInlineCteWalker, NULL, 0);

		/* we're done, no need to recurse anymore for this query */
		return false;
	}

	return expression_tree_walker(node, RecursivelyInlineCteWalker, context);
}


/*
 * InlineCTEsInQueryTree gets a query tree and tries to inline CTEs as subqueries
 * in the query tree.
 *
 * Most of the code is coming from PostgreSQL's CTE inlining logic, there are very
 * few additions that Citus added, which are already commented in the code.
 */
void
InlineCTEsInQueryTree(Query *query)
{
	ListCell *cteCell = NULL;

	/* iterate on the copy of the list because we'll be modifying query->cteList */
	List *copyOfCteList = list_copy(query->cteList);
	foreach(cteCell, copyOfCteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);

		/*
		 * First, make sure that Postgres is OK to inline the CTE. Later, check for
		 * distributed query planning constraints that might prevent inlining.
		 */
		if (PostgreSQLCTEInlineCondition(cte, query->commandType))
		{
			elog(DEBUG1, "CTE %s is going to be inlined via "
						 "distributed planning", cte->ctename);

			/* do the hard work of cte inlining */
			inline_cte(query, cte);

			/* clean-up the necessary fields for distributed planning */
			cte->cterefcount = 0;
			query->cteList = list_delete_ptr(query->cteList, cte);
		}
	}
}


/*
 * QueryTreeContainsInlinableCTE recursively traverses the queryTree, and returns true
 * if any of the (sub)queries in the queryTree contains at least one CTE.
 */
bool
QueryTreeContainsInlinableCTE(Query *queryTree)
{
	return QueryTreeContainsInlinableCteWalker((Node *) queryTree);
}


/*
 * QueryTreeContainsInlinableCteWalker walks over the node, and returns true if any of
 * the (sub)queries in the node contains at least one CTE.
 */
static bool
QueryTreeContainsInlinableCteWalker(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		ListCell *cteCell = NULL;
		foreach(cteCell, query->cteList)
		{
			CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);

			if (PostgreSQLCTEInlineCondition(cte, query->commandType))
			{
				/*
				 * Return true even if we can find a single CTE that is
				 * eligable for inlining.
				 */
				return true;
			}
		}

		return query_tree_walker(query, QueryTreeContainsInlinableCteWalker, NULL, 0);
	}

	return expression_tree_walker(node, QueryTreeContainsInlinableCteWalker, NULL);
}


/*
 * PostgreSQLCTEInlineCondition returns true if the CTE is considered
 * safe to inline by Postgres.
 */
static bool
PostgreSQLCTEInlineCondition(CommonTableExpr *cte, CmdType cmdType)
{
	/*
	 * Consider inlining the CTE (creating RTE_SUBQUERY RTE(s)) instead of
	 * implementing it as a separately-planned CTE.
	 *
	 * We cannot inline if any of these conditions hold:
	 *
	 * 1. The user said not to (the CTEMaterializeAlways option).
	 *
	 * 2. The CTE is recursive.
	 *
	 * 3. The CTE has side-effects; this includes either not being a plain
	 * SELECT, or containing volatile functions.  Inlining might change
	 * the side-effects, which would be bad.
	 *
	 * Otherwise, we have an option whether to inline or not.  That should
	 * always be a win if there's just a single reference, but if the CTE
	 * is multiply-referenced then it's unclear: inlining adds duplicate
	 * computations, but the ability to absorb restrictions from the outer
	 * query level could outweigh that.  We do not have nearly enough
	 * information at this point to tell whether that's true, so we let
	 * the user express a preference.  Our default behavior is to inline
	 * only singly-referenced CTEs, but a CTE marked CTEMaterializeNever
	 * will be inlined even if multiply referenced.
	 */
	if (
		(cte->ctematerialized == CTEMaterializeNever ||
		 (cte->ctematerialized == CTEMaterializeDefault &&
		  cte->cterefcount == 1)) &&
		!cte->cterecursive &&
		cmdType == CMD_SELECT &&
		!contain_dml(cte->ctequery) &&
		!contain_volatile_functions(cte->ctequery))
	{
		return true;
	}

	return false;
}


/* *INDENT-OFF* */
/*
 * inline_cte: convert RTE_CTE references to given CTE into RTE_SUBQUERYs
 */
static void
inline_cte(Query *mainQuery, CommonTableExpr *cte)
{
	struct inline_cte_walker_context context;

	context.ctename = cte->ctename;
	/* Start at levelsup = -1 because we'll immediately increment it */
	context.levelsup = -1;
	context.refcount = cte->cterefcount;
	context.ctequery = castNode(Query, cte->ctequery);
	context.aliascolnames = cte->aliascolnames;

	(void) inline_cte_walker((Node *) mainQuery, &context);

	/* Assert we replaced all references */
	Assert(context.refcount == 0);
}


/*
 * See PostgreSQL's source code at src/backend/optimizer/plan/subselect.c.
 */
static bool
inline_cte_walker(Node *node, inline_cte_walker_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		context->levelsup++;

		(void) query_tree_walker(query, inline_cte_walker, context,
								 QTW_EXAMINE_RTES_AFTER);
		context->levelsup--;

		return false;
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_CTE &&
			strcmp(rte->ctename, context->ctename) == 0 &&
			rte->ctelevelsup == context->levelsup)
		{
			/*
			 * Found a reference to replace.  Generate a copy of the CTE query
			 * with appropriate level adjustment for outer references (e.g.,
			 * to other CTEs).
			 */
			Query *newquery = copyObject(context->ctequery);

			if (context->levelsup > 0)
				IncrementVarSublevelsUp((Node *) newquery, context->levelsup, 1);

			/*
			 * Convert the RTE_CTE RTE into a RTE_SUBQUERY.
			 *
			 * Historically, a FOR UPDATE clause has been treated as extending
			 * into views and subqueries, but not into CTEs.  We preserve this
			 * distinction by not trying to push rowmarks into the new
			 * subquery.
			 */
			rte->rtekind = RTE_SUBQUERY;
			rte->subquery = newquery;
			rte->security_barrier = false;

			List *columnAliasList = context->aliascolnames;
			int columnAliasCount = list_length(columnAliasList);
			int columnIndex = 1;
			for (; columnIndex < list_length(rte->subquery->targetList) + 1; ++columnIndex)
			{
				/*
				 * Rename the column only if a column alias is defined.
				 * Notice that column alias count could be less than actual
				 * column count. We only use provided aliases and keep the
				 * original column names if no alias is defined.
				 */
				if (columnAliasCount >= columnIndex)
				{
					String *columnAlias = (String *) list_nth(columnAliasList, columnIndex - 1);
					Assert(IsA(columnAlias, String));
					TargetEntry *targetEntry =
						list_nth(rte->subquery->targetList, columnIndex - 1);
					Assert(IsA(columnAlias, String));
					targetEntry->resname = strVal(columnAlias);
				}
			}

			/* Zero out CTE-specific fields */
			rte->ctename = NULL;
			rte->ctelevelsup = 0;
			rte->self_reference = false;
			rte->coltypes = NIL;
			rte->coltypmods = NIL;
			rte->colcollations = NIL;

			/* Count the number of replacements we've done */
			context->refcount--;
		}

		return false;
	}

	return expression_tree_walker(node, inline_cte_walker, context);
}


/*
 * contain_dml: is any subquery not a plain SELECT?
 *
 * We reject SELECT FOR UPDATE/SHARE as well as INSERT etc.
 */
static bool
contain_dml(Node *node)
{
	return contain_dml_walker(node, NULL);
}


static bool
contain_dml_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		if (query->commandType != CMD_SELECT ||
			query->rowMarks != NIL)
			return true;

		return query_tree_walker(query, contain_dml_walker, context, 0);
	}
	return expression_tree_walker(node, contain_dml_walker, context);
}

/* *INDENT-ON* */
