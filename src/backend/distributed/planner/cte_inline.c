/*-------------------------------------------------------------------------
 *
 * cte_inline.c
 *	  For multi-shard queries, Citus can only recursively plan CTEs. Instead,
 *	  with the functions defined in this file, the certain CTEs can be inlined
 *	  as subqueries in the query tree. In that case, more optimal distributed
 *	  planning, the query pushdown planning, kicks in and the CTEs can actually
 *	  be pushed down as long as it is safe to pushdown as a subquery.
 *
 *	  Most of the logic in this function is inspired (and some is copy & pasted)
 *	  from PostgreSQL 12's CTE inlining feature.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#endif
#include "rewrite/rewriteManip.h"

#if PG_VERSION_NUM < 120000

/* copy & paste from PG 12 */
#define PG_12_QTW_EXAMINE_RTES_BEFORE 0x10
#define PG_12_QTW_EXAMINE_RTES_AFTER 0x20
bool pg_12_query_tree_walker(Query *query,
							 bool (*walker)(),
							 void *context,
							 int flags);
bool pg_12_range_table_walker(List *rtable,
							  bool (*walker)(),
							  void *context,
							  int flags);
#endif

typedef struct inline_cte_walker_context
{
	const char *ctename;       /* name and relative level of target CTE */
	int levelsup;
	int refcount;              /* number of remaining references */
	Query *ctequery;           /* query to substitute */
} inline_cte_walker_context;

/* copy & paste from Postgres source, moved into a function for readability */
static bool PostgreSQLCTEInlineCondition(CommonTableExpr *cte, CmdType cmdType);

/* the following utility functions are copy & paste from PostgreSQL code */
static void inline_cte(Query *mainQuery, CommonTableExpr *cte);
static bool inline_cte_walker(Node *node, inline_cte_walker_context *context);
static bool contain_dml(Node *node);
static bool contain_dml_walker(Node *node, void *context);


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
#if PG_VERSION_NUM >= 120000
		(cte->ctematerialized == CTEMaterializeNever ||
		 (cte->ctematerialized == CTEMaterializeDefault &&
		  cte->cterefcount == 1)) &&
#else

		/*
		 * If referenced only once inlining would probably perform
		 * better, so for pg < 12, try inlining
		 */
		cte->cterefcount == 1 &&
#endif
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

		/*
		 * Visit the query's RTE nodes after their contents; otherwise
		 * query_tree_walker would descend into the newly inlined CTE query,
		 * which we don't want.
		 */
#if PG_VERSION_NUM < 120000
		(void) pg_12_query_tree_walker(query, inline_cte_walker, context,
									   PG_12_QTW_EXAMINE_RTES_AFTER);
#else
		(void) query_tree_walker(query, inline_cte_walker, context,
								 QTW_EXAMINE_RTES_AFTER);
#endif
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


#if PG_VERSION_NUM < 120000
/*
 * pg_12_query_tree_walker is copied from Postgres 12's source
 * code. The only difference between query_tree_walker the new
 * two flags added in range_table_walker: QTW_EXAMINE_RTES_AFTER
 * and QTW_EXAMINE_RTES_BEFORE.
 */
bool
pg_12_query_tree_walker(Query *query,
				  bool (*walker) (),
				  void *context,
				  int flags)
{
	Assert(query != NULL && IsA(query, Query));

	if (walker((Node *) query->targetList, context))
		return true;
	if (walker((Node *) query->withCheckOptions, context))
		return true;
	if (walker((Node *) query->onConflict, context))
		return true;
	if (walker((Node *) query->returningList, context))
		return true;
	if (walker((Node *) query->jointree, context))
		return true;
	if (walker(query->setOperations, context))
		return true;
	if (walker(query->havingQual, context))
		return true;
	if (walker(query->limitOffset, context))
		return true;
	if (walker(query->limitCount, context))
		return true;
	if (!(flags & QTW_IGNORE_CTE_SUBQUERIES))
	{
		if (walker((Node *) query->cteList, context))
			return true;
	}
	if (!(flags & QTW_IGNORE_RANGE_TABLE))
	{
		if (pg_12_range_table_walker(query->rtable, walker, context, flags))
			return true;
	}
	return false;
}

/*
 * pg_12_range_table_walker is copied from Postgres 12's source
 * code. The only difference between range_table_walker the new
 * two flags added in range_table_walker: QTW_EXAMINE_RTES_AFTER
 * and QTW_EXAMINE_RTES_BEFORE.
 */
bool
pg_12_range_table_walker(List *rtable,
				   bool (*walker) (),
				   void *context,
				   int flags)
{
	ListCell   *rt;

	foreach(rt, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

		/*
		 * Walkers might need to examine the RTE node itself either before or
		 * after visiting its contents (or, conceivably, both).  Note that if
		 * you specify neither flag, the walker won't visit the RTE at all.
		 */
		if (flags & PG_12_QTW_EXAMINE_RTES_BEFORE)
			if (walker(rte, context))
				return true;

		switch (rte->rtekind)
		{
			case RTE_RELATION:
				if (walker(rte->tablesample, context))
					return true;
				break;
			case RTE_CTE:
			case RTE_NAMEDTUPLESTORE:
				/* nothing to do */
				break;
			case RTE_SUBQUERY:
				if (!(flags & QTW_IGNORE_RT_SUBQUERIES))
					if (walker(rte->subquery, context))
						return true;
				break;
			case RTE_JOIN:
				if (!(flags & QTW_IGNORE_JOINALIASES))
					if (walker(rte->joinaliasvars, context))
						return true;
				break;
			case RTE_FUNCTION:
				if (walker(rte->functions, context))
					return true;
				break;
			case RTE_TABLEFUNC:
				if (walker(rte->tablefunc, context))
					return true;
				break;
			case RTE_VALUES:
				if (walker(rte->values_lists, context))
					return true;
				break;
		}

		if (walker(rte->securityQuals, context))
			return true;

		if (flags & PG_12_QTW_EXAMINE_RTES_AFTER)
			if (walker(rte, context))
				return true;
	}
	return false;
}
#endif

/* *INDENT-ON* */
