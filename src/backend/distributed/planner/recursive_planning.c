/*-------------------------------------------------------------------------
 *
 * recursive_planning.c
 *
 * Logic for calling the postgres planner recursively for CTEs and
 * non-pushdownable subqueries in distributed queries.
 *
 * PostgreSQL with Citus can execute 4 types of queries:
 *
 * - Postgres queries on local tables and functions.
 *
 *   These queries can use all SQL features, but they may not reference
 *   distributed tables.
 *
 * - Router queries that can be executed on a single by node by replacing
 *   table names with shard names.
 *
 *   These queries can use nearly all SQL features, but only if they have
 *   a single-valued filter on the distribution column.
 *
 * - Multi-shard queries that can be executed by performing a task for each
 *   shard in a distributed table and performing a merge step.
 *
 *   These queries have limited SQL support. They may only include
 *   subqueries if the subquery can be executed on each shard by replacing
 *   table names with shard names and concatenating the result.
 *
 *   These queries have very limited SQL support and only support basic
 *   inner joins and subqueries without joins.
 *
 * To work around the limitations of these planners, we recursively call
 * the planner for CTEs and unsupported subqueries to obtain a list of
 * subplans.
 *
 * During execution, each subplan is executed separately through the method
 * that is appropriate for that query. The results are written to temporary
 * files on the workers. In the original query, the CTEs and subqueries are
 * replaced by mini-subqueries that read from the temporary files.
 *
 * This allows almost all SQL to be directly or indirectly supported,
 * because if all subqueries that contain distributed tables have been
 * replaced then what remains is a router query which can use nearly all
 * SQL features.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "pg_version_constants.h"

#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/combine_query_planner.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/listutils.h"
#include "distributed/local_distributed_join_planner.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/query_colocation_checker.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/query_utils.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"

/*
 * RecursivePlanningContext is used to recursively plan subqueries
 * and CTEs, pull results to the coordinator, and push it back into
 * the workers.
 */
struct RecursivePlanningContextInternal
{
	int level;
	uint64 planId;
	bool allDistributionKeysInQueryAreEqual; /* used for some optimizations */
	List *subPlanList;
	PlannerRestrictionContext *plannerRestrictionContext;
	bool restrictionEquivalenceCheck;
};

/* track depth of current recursive planner query */
static int recursivePlanningDepth = 0;

/*
 * CteReferenceWalkerContext is used to collect CTE references in
 * CteReferenceListWalker.
 */
typedef struct CteReferenceWalkerContext
{
	int level;
	List *cteReferenceList;
} CteReferenceWalkerContext;

/*
 * VarLevelsUpWalkerContext is used to find Vars in a (sub)query that
 * refer to upper levels and therefore cannot be planned separately.
 */
typedef struct VarLevelsUpWalkerContext
{
	int level;
} VarLevelsUpWalkerContext;


/* local function forward declarations */
static DeferredErrorMessage * RecursivelyPlanSubqueriesAndCTEs(Query *query,
															   RecursivePlanningContext *
															   context);
static bool ShouldRecursivelyPlanNonColocatedSubqueries(Query *subquery,
														RecursivePlanningContext *
														context);
static bool ContainsSubquery(Query *query);
static bool ShouldRecursivelyPlanOuterJoins(RecursivePlanningContext *context);
static void RecursivelyPlanNonColocatedSubqueries(Query *subquery,
												  RecursivePlanningContext *context);
static void RecursivelyPlanNonColocatedJoinWalker(Node *joinNode,
												  ColocatedJoinChecker *
												  colocatedJoinChecker,
												  RecursivePlanningContext *
												  recursivePlanningContext);
static void RecursivelyPlanNonColocatedSubqueriesInWhere(Query *query,
														 ColocatedJoinChecker *
														 colocatedJoinChecker,
														 RecursivePlanningContext *
														 recursivePlanningContext);
static bool RecursivelyPlanRecurringTupleOuterJoinWalker(Node *node, Query *query,
														 RecursivePlanningContext *context);
static void RecursivelyPlanDistributedJoinNode(Node *node, Query *query,
											   RecursivePlanningContext *context);
static bool IsRTERefRecurring(RangeTblRef *rangeTableRef, Query *query);
static List * SublinkListFromWhere(Query *originalQuery);
static bool ExtractSublinkWalker(Node *node, List **sublinkList);
static bool ShouldRecursivelyPlanSublinks(Query *query);
static bool RecursivelyPlanAllSubqueries(Node *node,
										 RecursivePlanningContext *planningContext);
static DeferredErrorMessage * RecursivelyPlanCTEs(Query *query,
												  RecursivePlanningContext *context);
static bool RecursivelyPlanSubqueryWalker(Node *node, RecursivePlanningContext *context);
static bool ShouldRecursivelyPlanSubquery(Query *subquery,
										  RecursivePlanningContext *context);
static bool AllDistributionKeysInSubqueryAreEqual(Query *subquery,
												  PlannerRestrictionContext *
												  restrictionContext);
static bool ShouldRecursivelyPlanSetOperation(Query *query,
											  RecursivePlanningContext *context);
static bool RecursivelyPlanSubquery(Query *subquery,
									RecursivePlanningContext *planningContext);
static void RecursivelyPlanSetOperations(Query *query, Node *node,
										 RecursivePlanningContext *context);
static bool IsLocalTableRteOrMatView(Node *node);
static DistributedSubPlan * CreateDistributedSubPlan(uint32 subPlanId,
													 Query *subPlanQuery);
static bool CteReferenceListWalker(Node *node, CteReferenceWalkerContext *context);
static bool ContainsReferencesToOuterQueryWalker(Node *node,
												 VarLevelsUpWalkerContext *context);
static bool NodeContainsSubqueryReferencingOuterQuery(Node *node);
static void WrapFunctionsInSubqueries(Query *query);
static void TransformFunctionRTE(RangeTblEntry *rangeTblEntry);
static bool ShouldTransformRTE(RangeTblEntry *rangeTableEntry);
static Query * BuildReadIntermediateResultsQuery(List *targetEntryList,
												 List *columnAliasList,
												 Const *resultIdConst, Oid functionOid,
												 bool useBinaryCopyFormat);
static Query * CreateOuterSubquery(RangeTblEntry *rangeTableEntry,
								   List *outerSubqueryTargetList);
static List * GenerateRequiredColNamesFromTargetList(List *targetList);
static char * GetRelationNameAndAliasName(RangeTblEntry *rangeTablentry);

/*
 * GenerateSubplansForSubqueriesAndCTEs is a wrapper around RecursivelyPlanSubqueriesAndCTEs.
 * The function returns the subplans if necessary. For the details of when/how subplans are
 * generated, see RecursivelyPlanSubqueriesAndCTEs().
 *
 * Note that the input originalQuery query is modified if any subplans are generated.
 */
List *
GenerateSubplansForSubqueriesAndCTEs(uint64 planId, Query *originalQuery,
									 PlannerRestrictionContext *plannerRestrictionContext)
{
	RecursivePlanningContext context;

	recursivePlanningDepth++;

	/*
	 * Plan subqueries and CTEs that cannot be pushed down by recursively
	 * calling the planner and add the resulting plans to subPlanList.
	 */
	context.level = 0;
	context.planId = planId;
	context.subPlanList = NIL;
	context.plannerRestrictionContext = plannerRestrictionContext;

	/*
	 * Calculating the distribution key equality upfront is a trade-off for us.
	 *
	 * When the originalQuery contains the distribution key equality, we'd be
	 * able to skip further checks for each lower level subqueries (i.e., if the
	 * all query contains distribution key equality, each subquery also contains
	 * distribution key equality.)
	 *
	 * When the originalQuery doesn't contain the distribution key equality,
	 * calculating this wouldn't help us at all, we should individually check
	 * each each subquery and subquery joins among subqueries.
	 */
	context.allDistributionKeysInQueryAreEqual =
		AllDistributionKeysInQueryAreEqual(originalQuery, plannerRestrictionContext);

	DeferredErrorMessage *error = RecursivelyPlanSubqueriesAndCTEs(originalQuery,
																   &context);
	if (error != NULL)
	{
		recursivePlanningDepth--;
		RaiseDeferredError(error, ERROR);
	}

	if (context.subPlanList && IsLoggableLevel(DEBUG1))
	{
		StringInfo subPlanString = makeStringInfo();
		pg_get_query_def(originalQuery, subPlanString);
		ereport(DEBUG1, (errmsg(
							 "Plan " UINT64_FORMAT
							 " query after replacing subqueries and CTEs: %s", planId,
							 subPlanString->data)));
	}

	recursivePlanningDepth--;

	return context.subPlanList;
}


/*
 * RecursivelyPlanSubqueriesAndCTEs finds subqueries and CTEs that cannot be pushed down to
 * workers directly and instead plans them by recursively calling the planner and
 * adding the subplan to subPlanList.
 *
 * Subplans are executed prior to the distributed plan and the results are written
 * to temporary files on workers.
 *
 * CTE references are replaced by a subquery on the read_intermediate_result
 * function, which reads from the temporary file.
 *
 * If recursive planning results in an error then the error is returned. Otherwise, the
 * subplans will be added to subPlanList.
 */
static DeferredErrorMessage *
RecursivelyPlanSubqueriesAndCTEs(Query *query, RecursivePlanningContext *context)
{
	DeferredErrorMessage *error = RecursivelyPlanCTEs(query, context);
	if (error != NULL)
	{
		return error;
	}

	if (SubqueryPushdown)
	{
		/*
		 * When the subquery_pushdown flag is enabled we make some hacks
		 * to push down subqueries with LIMIT. Recursive planning would
		 * valiantly do the right thing and try to recursively plan the
		 * inner subqueries, but we don't really want it to because those
		 * subqueries might not be supported and would be much slower.
		 *
		 * Instead, we skip recursive planning altogether when
		 * subquery_pushdown is enabled.
		 */
		return NULL;
	}

	/* make sure function calls in joins are executed in the coordinator */
	WrapFunctionsInSubqueries(query);

	/* descend into subqueries */
	query_tree_walker(query, RecursivelyPlanSubqueryWalker, context, 0);

	/*
	 * At this point, all CTEs, leaf subqueries containing local tables and
	 * non-pushdownable subqueries have been replaced. We now check for
	 * combinations of subqueries that cannot be pushed down (e.g.
	 * <subquery on reference table> UNION <subquery on distributed table>).
	 *
	 * This code also runs for the top-level query, which allows us to support
	 * top-level set operations.
	 */

	if (ShouldRecursivelyPlanSetOperation(query, context))
	{
		RecursivelyPlanSetOperations(query, (Node *) query->setOperations, context);
	}

	if (query->havingQual != NULL)
	{
		if (NodeContainsSubqueryReferencingOuterQuery(query->havingQual))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Subqueries in HAVING cannot refer to outer query",
								 NULL, NULL);
		}

		RecursivelyPlanAllSubqueries(query->havingQual, context);
	}

	/*
	 * If the query doesn't have distribution key equality,
	 * recursively plan some of its subqueries.
	 */
	if (ShouldRecursivelyPlanNonColocatedSubqueries(query, context))
	{
		RecursivelyPlanNonColocatedSubqueries(query, context);
	}


	if (ShouldConvertLocalTableJoinsToSubqueries(query->rtable))
	{
		/*
		 * Logical planner cannot handle "local_table" [OUTER] JOIN "dist_table", or
		 * a query with local table/citus local table and subquery. We convert local/citus local
		 * tables to a subquery until they can be planned.
		 */
		RecursivelyPlanLocalTableJoins(query, context);
	}

	/*
	 * Similarly, logical planner cannot handle outer joins when the outer rel
	 * is recurring, such as "<recurring> LEFT JOIN <distributed>". In that case,
	 * we convert distributed table into a subquery and recursively plan inner
	 * side of the outer join. That way, inner rel gets converted into an intermediate
	 * result and logical planner can handle the new query since it's of the from
	 * "<recurring> LEFT JOIN <recurring>".
	 */
	if (ShouldRecursivelyPlanOuterJoins(context))
	{
		RecursivelyPlanRecurringTupleOuterJoinWalker((Node *) query->jointree,
													 query, context);
	}

	/*
	 * If the FROM clause is recurring (does not contain a distributed table),
	 * then we cannot have any distributed tables appearing in subqueries in
	 * the SELECT and WHERE clauses.
	 *
	 * We do the sublink conversations at the end of the recursive planning
	 * because earlier steps might have transformed the query into a
	 * shape that needs recursively planning the sublinks.
	 */
	if (ShouldRecursivelyPlanSublinks(query))
	{
		/* replace all subqueries in the WHERE clause */
		if (query->jointree && query->jointree->quals)
		{
			RecursivelyPlanAllSubqueries((Node *) query->jointree->quals, context);
		}

		/* replace all subqueries in the SELECT clause */
		RecursivelyPlanAllSubqueries((Node *) query->targetList, context);
	}

	return NULL;
}


/*
 * GetPlannerRestrictionContext returns the planner restriction context
 * from the given context.
 */
PlannerRestrictionContext *
GetPlannerRestrictionContext(RecursivePlanningContext *recursivePlanningContext)
{
	return recursivePlanningContext->plannerRestrictionContext;
}


/*
 * ShouldRecursivelyPlanNonColocatedSubqueries returns true if the input query contains joins
 * that are not on the distribution key.
 * *
 * Note that at the point that this function is called, we've already recursively planned all
 * the leaf subqueries. Thus, we're actually checking whether the joins among the subqueries
 * on the distribution key or not.
 */
static bool
ShouldRecursivelyPlanNonColocatedSubqueries(Query *subquery,
											RecursivePlanningContext *context)
{
	/*
	 * If the input query already contains the equality, simply return since it is not
	 * possible to find any non colocated subqueries.
	 */
	if (context->allDistributionKeysInQueryAreEqual)
	{
		return false;
	}

	/*
	 * This check helps us in two ways:
	 *   (i) We're not targeting queries that don't include subqueries at all,
	 *       they should go through regular planning.
	 *  (ii) Lower level subqueries are already recursively planned, so we should
	 *       only bother non-colocated subquery joins, which only happens when
	 *       there are subqueries.
	 */
	if (!ContainsSubquery(subquery))
	{
		return false;
	}

	/* direct joins with local tables are not supported by any of Citus planners */
	if (FindNodeMatchingCheckFunctionInRangeTableList(subquery->rtable,
													  IsLocalTableRteOrMatView))
	{
		return false;
	}

	/*
	 * Finally, check whether this subquery contains distribution key equality or not.
	 */
	if (!AllDistributionKeysInSubqueryAreEqual(subquery,
											   context->plannerRestrictionContext))
	{
		return true;
	}

	return false;
}


/*
 * ContainsSubquery returns true if the input query contains any subqueries
 * in the FROM or WHERE clauses.
 */
static bool
ContainsSubquery(Query *query)
{
	return JoinTreeContainsSubquery(query) || WhereOrHavingClauseContainsSubquery(query);
}


/*
 * ShouldRecursivelyPlanOuterJoins returns true if the JoinRestrictionContext
 * that given RecursivePlanningContext holds implies that the query has outer
 * join(s) that might need to be recursively planned.
 */
static bool
ShouldRecursivelyPlanOuterJoins(RecursivePlanningContext *context)
{
	if (!context || !context->plannerRestrictionContext ||
		!context->plannerRestrictionContext->joinRestrictionContext)
	{
		ereport(ERROR, (errmsg("unexpectedly got NULL pointer in recursive "
							   "planning context")));
	}

	return context->plannerRestrictionContext->joinRestrictionContext->hasOuterJoin;
}


/*
 * RecursivelyPlanNonColocatedSubqueries gets a query which includes one or more
 * other subqueries that are not joined on their distribution keys. The function
 * tries to recursively plan some of the subqueries to make the input query
 * executable by Citus.
 *
 * The function picks an anchor subquery and iterates on the remaining subqueries.
 * Whenever it finds a non colocated subquery with the anchor subquery, the function
 * decides to recursively plan the non colocated subquery.
 *
 * The function first handles subqueries in FROM clause (i.e., jointree->fromlist) and then
 * subqueries in WHERE clause (i.e., jointree->quals).
 *
 * The function does not treat outer joins seperately. Thus, we might end up with
 * a query where the function decides to recursively plan an outer side of an outer
 * join (i.e., LEFT side of LEFT JOIN). For simplicity, we chose to do so and handle
 * outer joins with a seperate pass on the join tree.
 */
static void
RecursivelyPlanNonColocatedSubqueries(Query *subquery, RecursivePlanningContext *context)
{
	FromExpr *joinTree = subquery->jointree;

	/* create the context for the non colocated subquery planning */
	PlannerRestrictionContext *restrictionContext = context->plannerRestrictionContext;
	ColocatedJoinChecker colocatedJoinChecker = CreateColocatedJoinChecker(subquery,
																		   restrictionContext);

	/*
	 * Although this is a rare case, we weren't able to pick an anchor
	 * range table entry, so we cannot continue.
	 */
	if (colocatedJoinChecker.anchorRelationRestrictionList == NIL)
	{
		return;
	}

	/* handle from clause subqueries first */
	RecursivelyPlanNonColocatedJoinWalker((Node *) joinTree, &colocatedJoinChecker,
										  context);

	/* handle subqueries in WHERE clause */
	RecursivelyPlanNonColocatedSubqueriesInWhere(subquery, &colocatedJoinChecker,
												 context);
}


/*
 * RecursivelyPlanNonColocatedJoinWalker gets a join node and walks over it to find
 * subqueries that live under the node.
 *
 * When a subquery found, it's checked whether the subquery is colocated with the
 * anchor subquery specified in the nonColocatedJoinContext. If not,
 * the subquery is recursively planned.
 */
static void
RecursivelyPlanNonColocatedJoinWalker(Node *joinNode,
									  ColocatedJoinChecker *colocatedJoinChecker,
									  RecursivePlanningContext *recursivePlanningContext)
{
	if (joinNode == NULL)
	{
		return;
	}
	else if (IsA(joinNode, FromExpr))
	{
		FromExpr *fromExpr = (FromExpr *) joinNode;
		ListCell *fromExprCell;

		/*
		 * For each element of the from list, check whether the element is
		 * colocated with the anchor subquery by recursing until we
		 * find the subqueries.
		 */
		foreach(fromExprCell, fromExpr->fromlist)
		{
			Node *fromElement = (Node *) lfirst(fromExprCell);

			RecursivelyPlanNonColocatedJoinWalker(fromElement, colocatedJoinChecker,
												  recursivePlanningContext);
		}
	}
	else if (IsA(joinNode, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) joinNode;

		/* recurse into the left subtree */
		RecursivelyPlanNonColocatedJoinWalker(joinExpr->larg, colocatedJoinChecker,
											  recursivePlanningContext);

		/* recurse into the right subtree */
		RecursivelyPlanNonColocatedJoinWalker(joinExpr->rarg, colocatedJoinChecker,
											  recursivePlanningContext);
	}
	else if (IsA(joinNode, RangeTblRef))
	{
		int rangeTableIndex = ((RangeTblRef *) joinNode)->rtindex;
		List *rangeTableList = colocatedJoinChecker->subquery->rtable;
		RangeTblEntry *rte = rt_fetch(rangeTableIndex, rangeTableList);

		/* we're only interested in subqueries for now */
		if (rte->rtekind != RTE_SUBQUERY)
		{
			return;
		}

		/*
		 * If the subquery is not colocated with the anchor subquery,
		 * recursively plan it.
		 */
		Query *subquery = rte->subquery;
		if (!SubqueryColocated(subquery, colocatedJoinChecker))
		{
			RecursivelyPlanSubquery(subquery, recursivePlanningContext);
		}
	}
	else
	{
		pg_unreachable();
	}
}


/*
 * RecursivelyPlanNonColocatedSubqueriesInWhere gets a query and walks over its
 * sublinks to find subqueries that live in WHERE clause.
 *
 * When a subquery found, it's checked whether the subquery is colocated with the
 * anchor subquery specified in the nonColocatedJoinContext. If not,
 * the subquery is recursively planned.
 */
static void
RecursivelyPlanNonColocatedSubqueriesInWhere(Query *query,
											 ColocatedJoinChecker *colocatedJoinChecker,
											 RecursivePlanningContext *
											 recursivePlanningContext)
{
	List *sublinkList = SublinkListFromWhere(query);
	ListCell *sublinkCell = NULL;

	foreach(sublinkCell, sublinkList)
	{
		SubLink *sublink = (SubLink *) lfirst(sublinkCell);
		Query *subselect = (Query *) sublink->subselect;

		/* subselect is probably never NULL, but anyway lets keep the check */
		if (subselect == NULL)
		{
			continue;
		}

		if (!SubqueryColocated(subselect, colocatedJoinChecker))
		{
			RecursivelyPlanSubquery(subselect, recursivePlanningContext);
		}
	}
}


/*
 * Returns true if the given node is recurring, or the node is a 
 * JoinExpr that contains a recurring node.
*/
static bool
JoinExprHasNonRecurringTable(Node *node, Query *query)
{
	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, RangeTblRef))
	{
		return IsRTERefRecurring((RangeTblRef *) node, query);
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) node;

		return JoinExprHasNonRecurringTable(joinExpr->larg, query) ||
			   JoinExprHasNonRecurringTable(joinExpr->rarg, query);
	}
	else
	{
		return false;
	}
}


/*
 * RecursivelyPlanRecurringTupleOuterJoinWalker descends into a join tree and
 * recursively plans all non-recurring (i.e., distributed) rels that that
 * participate in an outer join expression together with a recurring rel,
 * such as <distributed> in "<recurring> LEFT JOIN <distributed>", i.e.,
 * where the recurring rel causes returning recurring tuples from the worker
 * nodes.
 *
 * Returns true if given node is recurring.
 *
 * See RecursivelyPlanDistributedJoinNode() function for the explanation on
 * what does it mean for a node to be "recurring" or "distributed".
 */
static bool
RecursivelyPlanRecurringTupleOuterJoinWalker(Node *node, Query *query,
											 RecursivePlanningContext *
											 recursivePlanningContext)
{
	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, FromExpr))
	{
		FromExpr *fromExpr = (FromExpr *) node;
		ListCell *fromExprCell;

		/* search for join trees in each FROM element */
		foreach(fromExprCell, fromExpr->fromlist)
		{
			Node *fromElement = (Node *) lfirst(fromExprCell);

			RecursivelyPlanRecurringTupleOuterJoinWalker(fromElement, query,
														 recursivePlanningContext);
		}

		/*
		 * Can only appear during the top-level call and top-level callers
		 * are not interested in the return value. Even more, we can't tell
		 * whether a FromExpr is recurring or not.
		 */
		return false;
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) node;

		Node *leftNode = joinExpr->larg;
		Node *rightNode = joinExpr->rarg;

		/*
		 * There may be recursively plannable outer joins deeper in the join tree.
		 *
		 * We first handle the sub join trees and then the top level one since the
		 * top level join expression might not require recursive planning after
		 * handling the sub join trees.
		 */
		bool leftNodeRecurs =
			RecursivelyPlanRecurringTupleOuterJoinWalker(leftNode, query,
														 recursivePlanningContext);
		bool rightNodeRecurs =
			RecursivelyPlanRecurringTupleOuterJoinWalker(rightNode, query,
														 recursivePlanningContext);
		switch (joinExpr->jointype)
		{
			case JOIN_LEFT:
			{
				/* <recurring> left join <distributed> */

				/* Recursively plan the right side of the left left join when the following 
				 * conditions are met:
				 * 1. The left side is recurring
				 * 2. The right side is not recurring
				 * 3. Either of the following:
				 * 		a. The left side is not a RangeTblRef (i.e., it is not a reference/local table)
				 * 		b. The tables in the rigt side are not colocated.
				 * 5. The left side does not have the distribution column (TODO: CHECK THIS)
				 */

				if (leftNodeRecurs && !rightNodeRecurs)				
				{	
					int outerRtIndex = ((RangeTblRef *) leftNode)->rtindex;
					RangeTblEntry *rte = rt_fetch(outerRtIndex, query->rtable);
					RangeTblEntry *innerRte = NULL;
					if (!IsPushdownSafeForRTEInLeftJoin(rte))
					{
						ereport(DEBUG1, (errmsg("recursively planning right side of "
												"the left join since the outer side "
												"is a recurring rel that is not an RTE")));
						RecursivelyPlanDistributedJoinNode(rightNode, query,
														   recursivePlanningContext);
					}
					else if (!CheckIfAllCitusRTEsAreColocated(rightNode, query->rtable, &innerRte))
					{
						ereport(DEBUG1, (errmsg("recursively planning right side of the left join "
												"since tables in the inner side of the left "
												"join are not colocated")));
						RecursivelyPlanDistributedJoinNode(rightNode, query,
									                       recursivePlanningContext);						
					}
				}
				/*
				* rightNodeRecurs if there is a recurring table in the right side. However, if the right side 
				* is a join expression, we need to check if it contains a recurring table. If it does, we need to
				* recursively plan the right side of the left join. Push-down path does not handle the nested joins
				* yet, once we have that, we can remove this check.
				*/
				else if (leftNodeRecurs && rightNodeRecurs && JoinExprHasNonRecurringTable(rightNode, query))
				{
						ereport(DEBUG1, (errmsg("recursively planning right side of the left join "
												"since right side is a joinexpr with non-recurring tables")));
						RecursivelyPlanDistributedJoinNode(rightNode, query,
									   					   recursivePlanningContext);	
				}

				/*
				 * A LEFT JOIN is recurring if the lhs is recurring.
				 * Note that we might have converted the rhs into a recurring
				 * one too if the lhs is recurring, but this anyway has no
				 * effects when deciding whether a LEFT JOIN is recurring.
				 */
				return leftNodeRecurs;
			}

			case JOIN_RIGHT:
			{
				/* <distributed> right join <recurring> */
				if (!leftNodeRecurs && rightNodeRecurs)
				{
					ereport(DEBUG1, (errmsg("recursively planning left side of "
											"the right join since the outer side "
											"is a recurring rel")));
					RecursivelyPlanDistributedJoinNode(leftNode, query,
													   recursivePlanningContext);
				}

				/*
				 * Similar to LEFT JOINs, a RIGHT JOIN is recurring if the rhs
				 * is recurring.
				 */
				return rightNodeRecurs;
			}

			case JOIN_FULL:
			{
				/*
				 * <recurring> full join <distributed>
				 * <distributed> full join <recurring>
				 */
				if (leftNodeRecurs && !rightNodeRecurs)
				{
					ereport(DEBUG1, (errmsg("recursively planning right side of "
											"the full join since the other side "
											"is a recurring rel")));
					RecursivelyPlanDistributedJoinNode(rightNode, query,
													   recursivePlanningContext);
				}
				else if (!leftNodeRecurs && rightNodeRecurs)
				{
					ereport(DEBUG1, (errmsg("recursively planning left side of "
											"the full join since the other side "
											"is a recurring rel")));
					RecursivelyPlanDistributedJoinNode(leftNode, query,
													   recursivePlanningContext);
				}

				/*
				 * An OUTER JOIN is recurring if any sides of the join is
				 * recurring. As in other outer join types, it doesn't matter
				 * whether the other side was / became recurring or not.
				 */
				return leftNodeRecurs || rightNodeRecurs;
			}

			case JOIN_INNER:
			{
				/*
				 * We don't need to recursively plan non-outer joins and we
				 * already descended into sub join trees to handle outer joins
				 * buried in them.
				 */
				return leftNodeRecurs && rightNodeRecurs;
			}

			default:
			{
				ereport(ERROR, (errmsg("got unexpected join type (%d) when recursively "
									   "planning a join",
									   joinExpr->jointype)));
			}
		}
	}
	else if (IsA(node, RangeTblRef))
	{
		return IsRTERefRecurring((RangeTblRef *) node, query);
	}
	else
	{
		ereport(ERROR, errmsg("got unexpected node type (%d) when recursively "
							  "planning a join",
							  nodeTag(node)));
	}
}


/*
 * RecursivelyPlanDistributedJoinNode is a helper function for
 * RecursivelyPlanRecurringTupleOuterJoinWalker that recursively plans given
 * distributed node that is known to be inner side of an outer join.
 *
 * Fails to do so if the distributed join node references the recurring one.
 * In that case, we don't throw an error here but instead we let
 * DeferredErrorIfUnsupportedRecurringTuplesJoin to so for a better error
 * message.
 *
 * We call a node "distributed" if it points to a distributed table or a
 * more complex object (i.e., a join tree or a subquery) that can be pushed
 * down to the worker nodes directly. For a join, this means that it's either
 * an INNER join where any side of it is a distributed table / a distributed
 * sub join tree, or an OUTER join where the outer side is a distributed table
 * / a distributed sub join tree.
 */
static void
RecursivelyPlanDistributedJoinNode(Node *node, Query *query,
								   RecursivePlanningContext *recursivePlanningContext)
{
	if (IsA(node, JoinExpr))
	{
		/*
		 * This, for example, means that RecursivelyPlanRecurringTupleOuterJoinWalker
		 * needs to plan inner side, i.e., "<distributed> INNER JOIN <distributed>",
		 * of the following join:
		 *   <recurring> LEFT JOIN (<distributed> JOIN <distributed>)
		 *
		 * XXX: Ideally, we should handle such a sub join tree by moving
		 *      it into a subquery "as a whole" but this implies that we need to
		 *      rebuild the rtable and re-point all the Vars to the new rtable
		 *      indexes, so we've not implemented that yet.
		 *
		 *      Instead, we recursively plan all the distributed tables in that
		 *      sub join tree. This is much more inefficient than the other
		 *      approach (since we lose the opportunity to push-down the whole
		 *      sub join tree into the workers) but is easier to implement.
		 */

		RecursivelyPlanDistributedJoinNode(((JoinExpr *) node)->larg,
										   query, recursivePlanningContext);

		RecursivelyPlanDistributedJoinNode(((JoinExpr *) node)->rarg,
										   query, recursivePlanningContext);

		return;
	}

	if (!IsA(node, RangeTblRef))
	{
		ereport(ERROR, (errmsg("unexpected join node type (%d)",
							   nodeTag(node))));
	}

	RangeTblRef *rangeTableRef = (RangeTblRef *) node;
	if (IsRTERefRecurring(rangeTableRef, query))
	{
		/*
		 * Not the top-level callers but RecursivelyPlanDistributedJoinNode
		 * might call itself for recurring nodes and need to skip them.
		 */
		return;
	}

	RangeTblEntry *distributedRte = rt_fetch(rangeTableRef->rtindex,
											 query->rtable);
	if (distributedRte->rtekind == RTE_RELATION)
	{
		ereport(DEBUG1, (errmsg("recursively planning distributed relation %s "
								"since it is part of a distributed join node "
								"that is outer joined with a recurring rel",
								GetRelationNameAndAliasName(distributedRte))));

		PlannerRestrictionContext *restrictionContext =
			GetPlannerRestrictionContext(recursivePlanningContext);
		List *requiredAttributes =
			RequiredAttrNumbersForRelation(distributedRte, restrictionContext);

#if PG_VERSION_NUM >= PG_VERSION_16
		RTEPermissionInfo *perminfo = NULL;
		if (distributedRte->perminfoindex)
		{
			perminfo = getRTEPermissionInfo(query->rteperminfos, distributedRte);
		}

		ReplaceRTERelationWithRteSubquery(distributedRte, requiredAttributes,
										  recursivePlanningContext, perminfo);
#else
		ReplaceRTERelationWithRteSubquery(distributedRte, requiredAttributes,
										  recursivePlanningContext, NULL);
#endif
	}
	else if (distributedRte->rtekind == RTE_SUBQUERY)
	{
		/*
		 * We don't try logging the subquery here because RecursivelyPlanSubquery
		 * will anyway do so if the query doesn't reference the outer query.
		 */
		ereport(DEBUG1, (errmsg("recursively planning the distributed subquery "
								"since it is part of a distributed join node "
								"that is outer joined with a recurring rel")));

		bool recursivelyPlanned = RecursivelyPlanSubquery(distributedRte->subquery,
														  recursivePlanningContext);
		if (!recursivelyPlanned)
		{
			/*
			 * RecursivelyPlanSubquery fails to plan a subquery only if it
			 * contains references to the outer query. This means that, we can't
			 * plan such outer joins (like <recurring LEFT OUTER distributed>)
			 * if it's a LATERAL join where the distributed side is a subquery that
			 * references the outer side, as in,
			 *
			 * SELECT * FROM reference
			 * LEFT JOIN LATERAL
			 * (SELECT * FROM distributed WHERE reference.b > distributed.b) q
			 * USING (a);
			 */
			Assert(ContainsReferencesToOuterQuery(distributedRte->subquery));
		}
	}
	else
	{
		/*
		 * We don't expect RecursivelyPlanRecurringTupleOuterJoinWalker to try recursively
		 * plan such an RTE.
		 */
		ereport(ERROR, errmsg("got unexpected RTE type (%d) when recursively "
							  "planning a join",
							  distributedRte->rtekind));
	}
}


/*
 * IsRTERefRecurring returns true if given rte reference points to a recurring
 * rte.
 *
 * If an rte points to a table, then we call it recurring if the table is not
 * a distributed table. Otherwise, e.g., if it points a query, then we call it
 * recurring if none of the rtes that belongs to the query point to a distributed
 * table.
 *
 * Note that it's safe to assume a subquery is not recurring if we have a rte reference
 * to a distributed table somewhere in the query tree. For example, considering
 * the subquery (q) of the the following query:
 *   SELECT * FROM ref LEFT JOIN (SELECT * FROM ref LEFT dist) q,
 * one might think that it's not appropriate to call IsRTERefRecurring for subquery
 * (q). However, this is already not the case because this function is called
 * in the context of recursive planning and hence any query that contains
 * rtes pointing to distributed tables and that cannot be pushed down to worker
 * nodes should've been recursively planned already. This is because, the recursive
 * planner processes the queries in bottom-up fashion. For this reason, the subquery
 * in the example should've already be converted to the following before we check
 * the rte reference that points to the subquery (q):
 *   SELECT * FROM ref LEFT JOIN (SELECT * FROM ref LEFT (SELECT * FROM read_intermediate_result()) dist_1)
 * That way, we wouldn't incorrectly say that (SELECT * FROM ref LEFT dist) is a
 * distributed subquery (due to having a reference to a distributed table).
 */
static bool
IsRTERefRecurring(RangeTblRef *rangeTableRef, Query *query)
{
	int rangeTableIndex = rangeTableRef->rtindex;
	List *rangeTableList = query->rtable;
	RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableIndex, rangeTableList);
	return !FindNodeMatchingCheckFunctionInRangeTableList(list_make1(rangeTableEntry),
														  IsDistributedTableRTE);
}


/*
 * SublinkListFromWhere finds the subquery nodes in the where clause of the given query. Note
 * that the function should be called on the original query given that postgres
 * standard_planner() may convert the subqueries in WHERE clause to joins.
 */
static List *
SublinkListFromWhere(Query *originalQuery)
{
	FromExpr *joinTree = originalQuery->jointree;
	List *sublinkList = NIL;

	if (!joinTree)
	{
		return NIL;
	}

	Node *queryQuals = joinTree->quals;
	ExtractSublinkWalker(queryQuals, &sublinkList);

	return sublinkList;
}


/*
 * ExtractSublinkWalker walks over a quals node, and finds all sublinks
 * in that node.
 */
static bool
ExtractSublinkWalker(Node *node, List **sublinkList)
{
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, SubLink))
	{
		(*sublinkList) = lappend(*sublinkList, node);
	}
	else
	{
		walkerResult = expression_tree_walker(node, ExtractSublinkWalker,
											  sublinkList);
	}

	return walkerResult;
}


/*
 * ShouldRecursivelyPlanSublinks returns true if the query has a recurring
 * FROM clause.
 */
static bool
ShouldRecursivelyPlanSublinks(Query *query)
{
	if (FindNodeMatchingCheckFunctionInRangeTableList(query->rtable,
													  IsDistributedTableRTE))
	{
		/* there is a distributed table in the FROM clause */
		return false;
	}

	return true;
}


/*
 * RecursivelyPlanAllSubqueries descends into an expression tree and recursively
 * plans all subqueries that contain at least one distributed table. The recursive
 * planning starts from the top of the input query.
 */
static bool
RecursivelyPlanAllSubqueries(Node *node, RecursivePlanningContext *planningContext)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		if (FindNodeMatchingCheckFunctionInRangeTableList(query->rtable, IsCitusTableRTE))
		{
			RecursivelyPlanSubquery(query, planningContext);
		}

		return false;
	}

	return expression_tree_walker(node, RecursivelyPlanAllSubqueries, planningContext);
}


/*
 * RecursivelyPlanCTEs plans all CTEs in the query by recursively calling the planner
 * The resulting plan is added to planningContext->subPlanList and CTE references
 * are replaced by subqueries that call read_intermediate_result, which reads the
 * intermediate result of the CTE after it is executed.
 *
 * Recursive and modifying CTEs are not yet supported and return an error.
 */
static DeferredErrorMessage *
RecursivelyPlanCTEs(Query *query, RecursivePlanningContext *planningContext)
{
	ListCell *cteCell = NULL;
	CteReferenceWalkerContext context = { -1, NIL };

	if (query->cteList == NIL)
	{
		/* no CTEs, nothing to do */
		return NULL;
	}

	if (query->hasRecursive)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "recursive CTEs are only supported when they "
							 "contain a filter on the distribution column",
							 NULL, NULL);
	}

	/* get all RTE_CTEs that point to CTEs from cteList */
	CteReferenceListWalker((Node *) query, &context);

	foreach(cteCell, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
		char *cteName = cte->ctename;
		Query *subquery = (Query *) cte->ctequery;
		uint64 planId = planningContext->planId;
		List *cteTargetList = NIL;
		ListCell *rteCell = NULL;
		int replacedCtesCount = 0;

		if (ContainsReferencesToOuterQuery(subquery))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "CTEs that refer to other subqueries are not "
								 "supported in multi-shard queries",
								 NULL, NULL);
		}

		if (cte->cterefcount == 0 && subquery->commandType == CMD_SELECT)
		{
			/*
			 * SELECT CTEs that aren't referenced aren't executed in postgres.
			 * We don't need to generate a subplan for it and can take the rest
			 * of this iteration off.
			 */
			continue;
		}

		uint32 subPlanId = list_length(planningContext->subPlanList) + 1;

		if (IsLoggableLevel(DEBUG1))
		{
			StringInfo subPlanString = makeStringInfo();
			pg_get_query_def(subquery, subPlanString);
			ereport(DEBUG1, (errmsg("generating subplan " UINT64_FORMAT
									"_%u for CTE %s: %s", planId, subPlanId,
									cteName,
									subPlanString->data)));
		}

		/* build a sub plan for the CTE */
		DistributedSubPlan *subPlan = CreateDistributedSubPlan(subPlanId, subquery);
		planningContext->subPlanList = lappend(planningContext->subPlanList, subPlan);

		/* build the result_id parameter for the call to read_intermediate_result */
		char *resultId = GenerateResultId(planId, subPlanId);

		if (subquery->returningList)
		{
			/* modifying CTE with returning */
			cteTargetList = subquery->returningList;
		}
		else
		{
			/* regular SELECT CTE */
			cteTargetList = subquery->targetList;
		}

		/* replace references to the CTE with a subquery that reads results */
		Query *resultQuery = BuildSubPlanResultQuery(cteTargetList, cte->aliascolnames,
													 resultId);

		foreach(rteCell, context.cteReferenceList)
		{
			RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rteCell);

			if (rangeTableEntry->rtekind != RTE_CTE)
			{
				/*
				 * This RTE pointed to a preceding CTE that was already replaced
				 * by a subplan.
				 */
				continue;
			}

			if (strncmp(rangeTableEntry->ctename, cteName, NAMEDATALEN) == 0)
			{
				/* change the RTE_CTE into an RTE_SUBQUERY */
				rangeTableEntry->rtekind = RTE_SUBQUERY;
				rangeTableEntry->ctename = NULL;
				rangeTableEntry->ctelevelsup = 0;

				if (replacedCtesCount == 0)
				{
					/*
					 * Replace the first CTE reference with the result query directly.
					 */
					rangeTableEntry->subquery = resultQuery;
				}
				else
				{
					/*
					 * Replace subsequent CTE references with a copy of the result
					 * query.
					 */
					rangeTableEntry->subquery = copyObject(resultQuery);
				}

				replacedCtesCount++;
			}
		}

		Assert(cte->cterefcount == replacedCtesCount);
	}

	/*
	 * All CTEs are now executed through subplans and RTE_CTEs pointing
	 * to the CTE list have been replaced with subqueries. We can now
	 * clear the cteList.
	 */
	query->cteList = NIL;

	return NULL;
}


/*
 * RecursivelyPlanSubqueryWalker recursively finds all the Query nodes and
 * recursively plans if necessary.
 */
static bool
RecursivelyPlanSubqueryWalker(Node *node, RecursivePlanningContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		context->level += 1;

		/*
		 * First, make sure any subqueries and CTEs within this subquery
		 * are recursively planned if necessary.
		 */
		DeferredErrorMessage *error = RecursivelyPlanSubqueriesAndCTEs(query, context);
		if (error != NULL)
		{
			RaiseDeferredError(error, ERROR);
		}
		context->level -= 1;

		/*
		 * Recursively plan this subquery if it cannot be pushed down and is
		 * eligible for recursive planning.
		 */
		if (ShouldRecursivelyPlanSubquery(query, context))
		{
			RecursivelyPlanSubquery(query, context);
		}

		/* we're done, no need to recurse anymore for this query */
		return false;
	}

	return expression_tree_walker(node, RecursivelyPlanSubqueryWalker, context);
}


/*
 * ShouldRecursivelyPlanSubquery decides whether the input subquery should be recursively
 * planned or not.
 *
 * For the details, see the cases in the function.
 */
static bool
ShouldRecursivelyPlanSubquery(Query *subquery, RecursivePlanningContext *context)
{
	if (FindNodeMatchingCheckFunctionInRangeTableList(subquery->rtable,
													  IsLocalTableRteOrMatView))
	{
		/*
		 * Postgres can always plan queries that don't require distributed planning.
		 * Note that we need to check this first, otherwise the calls to the many other
		 * Citus planner functions would error our due to local relations.
		 *
		 * TODO: We could only successfully create distributed plans with local tables
		 * when the local tables are on the leaf queries and the upper level queries
		 * do not contain any other local tables.
		 */
	}
	else if (CanPushdownSubquery(subquery, false))
	{
		/*
		 * We should do one more check for the distribution key equality.
		 *
		 * If the input query to the planner doesn't contain distribution key equality,
		 * we should further check whether this individual subquery contains or not.
		 *
		 * If all relations are not joined on their distribution keys for the given
		 * subquery, we cannot push push it down and therefore we should try to
		 * recursively plan it.
		 */
		if (!context->allDistributionKeysInQueryAreEqual &&
			!AllDistributionKeysInSubqueryAreEqual(subquery,
												   context->plannerRestrictionContext))
		{
			return true;
		}

		/*
		 * Citus can pushdown this subquery, no need to recursively
		 * plan which is much more expensive than pushdown.
		 */
		return false;
	}

	return true;
}


/*
 * AllDistributionKeysInSubqueryAreEqual is a wrapper function
 * for AllDistributionKeysInQueryAreEqual(). Here, we filter the
 * planner restrictions for the given subquery and do the restriction
 * equality checks on the filtered restriction.
 */
static bool
AllDistributionKeysInSubqueryAreEqual(Query *subquery,
									  PlannerRestrictionContext *restrictionContext)
{
	/* we don't support distribution eq. checks for CTEs yet */
	if (subquery->cteList != NIL)
	{
		return false;
	}

	PlannerRestrictionContext *filteredRestrictionContext =
		FilterPlannerRestrictionForQuery(restrictionContext, subquery);

	bool allDistributionKeysInSubqueryAreEqual =
		AllDistributionKeysInQueryAreEqual(subquery, filteredRestrictionContext);
	if (!allDistributionKeysInSubqueryAreEqual)
	{
		return false;
	}

	return true;
}


/*
 * ShouldRecursivelyPlanSetOperation determines whether the leaf queries of a
 * set operations tree need to be recursively planned in order to support the
 * query as a whole.
 */
static bool
ShouldRecursivelyPlanSetOperation(Query *query, RecursivePlanningContext *context)
{
	SetOperationStmt *setOperations = (SetOperationStmt *) query->setOperations;
	if (setOperations == NULL)
	{
		return false;
	}

	if (context->level == 0)
	{
		/*
		 * We cannot push down top-level set operation. Recursively plan the
		 * leaf nodes such that it becomes a router query.
		 */
		return true;
	}

	if (setOperations->op != SETOP_UNION)
	{
		/*
		 * We can only push down UNION operaionts, plan other set operations
		 * recursively.
		 */
		return true;
	}

	if (DeferErrorIfUnsupportedUnionQuery(query) != NULL)
	{
		/*
		 * If at least one leaf query in the union is recurring, then all
		 * leaf nodes need to be recurring.
		 */
		return true;
	}

	PlannerRestrictionContext *filteredRestrictionContext =
		FilterPlannerRestrictionForQuery(context->plannerRestrictionContext, query);
	if (!SafeToPushdownUnionSubquery(query, filteredRestrictionContext))
	{
		/*
		 * The distribution column is not in the same place in all sides
		 * of the union, meaning we cannot determine distribution column
		 * equivalence. Recursive planning is necessary.
		 */
		return true;
	}

	return false;
}


/*
 * RecursivelyPlanSetOperations descends into a tree of set operations
 * (e.g. UNION, INTERSECTS) and recursively plans all leaf nodes that
 * contain distributed tables.
 */
static void
RecursivelyPlanSetOperations(Query *query, Node *node,
							 RecursivePlanningContext *context)
{
	if (IsA(node, SetOperationStmt))
	{
		SetOperationStmt *setOperations = (SetOperationStmt *) node;

		RecursivelyPlanSetOperations(query, setOperations->larg, context);
		RecursivelyPlanSetOperations(query, setOperations->rarg, context);
	}
	else if (IsA(node, RangeTblRef))
	{
		RangeTblRef *rangeTableRef = (RangeTblRef *) node;
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableRef->rtindex,
												  query->rtable);
		Query *subquery = rangeTableEntry->subquery;

		if (rangeTableEntry->rtekind == RTE_SUBQUERY &&
			FindNodeMatchingCheckFunction((Node *) subquery, IsDistributedTableRTE))
		{
			RecursivelyPlanSubquery(subquery, context);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("unexpected node type (%d) while "
							   "expecting set operations or "
							   "range table references", nodeTag(node))));
	}
}


/*
 * IsLocalTableRteOrMatView gets a node and returns true if the node is a range
 * table entry that points to a postgres local or citus local table or to a
 * materialized view.
 */
static bool
IsLocalTableRteOrMatView(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	if (!IsA(node, RangeTblEntry))
	{
		return false;
	}

	RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;
	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		return false;
	}

	if (rangeTableEntry->relkind == RELKIND_VIEW)
	{
		return false;
	}

	Oid relationId = rangeTableEntry->relid;
	return IsRelationLocalTableOrMatView(relationId);
}


/*
 * IsRelationLocalTableOrMatView returns true if the given relation
 * is a citus local, local, or materialized view.
 */
bool
IsRelationLocalTableOrMatView(Oid relationId)
{
	if (!IsCitusTable(relationId))
	{
		/* postgres local table or a materialized view */
		return true;
	}
	else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		return true;
	}

	/* no local table found */
	return false;
}


/*
 * RecursivelyPlanSubquery recursively plans a query, replaces it with a
 * result query and returns the subplan.
 *
 * Before we recursively plan the given subquery, we should ensure
 * that the subquery doesn't contain any references to the outer
 * queries (i.e., such queries cannot be separately planned). In
 * that case, the function doesn't recursively plan the input query
 * and immediately returns. Later, the planner decides on what to do
 * with the query.
 */
static bool
RecursivelyPlanSubquery(Query *subquery, RecursivePlanningContext *planningContext)
{
	uint64 planId = planningContext->planId;
	Query *debugQuery = NULL;

	if (ContainsReferencesToOuterQuery(subquery))
	{
		elog(DEBUG2, "skipping recursive planning for the subquery since it "
					 "contains references to outer queries");

		return false;
	}

	/*
	 * Subquery will go through the standard planner, thus to properly deparse it
	 * we keep its copy: debugQuery.
	 */
	if (IsLoggableLevel(DEBUG1))
	{
		debugQuery = copyObject(subquery);
	}


	/*
	 * Create the subplan and append it to the list in the planning context.
	 */
	int subPlanId = list_length(planningContext->subPlanList) + 1;

	DistributedSubPlan *subPlan = CreateDistributedSubPlan(subPlanId, subquery);
	planningContext->subPlanList = lappend(planningContext->subPlanList, subPlan);

	/* build the result_id parameter for the call to read_intermediate_result */
	char *resultId = GenerateResultId(planId, subPlanId);

	/*
	 * BuildSubPlanResultQuery() can optionally use provided column aliases.
	 * We do not need to send additional alias list for subqueries.
	 */
	Query *resultQuery = BuildSubPlanResultQuery(subquery->targetList, NIL, resultId);

	if (IsLoggableLevel(DEBUG1))
	{
		StringInfo subqueryString = makeStringInfo();

		pg_get_query_def(debugQuery, subqueryString);

		ereport(DEBUG1, (errmsg("generating subplan " UINT64_FORMAT
								"_%u for subquery %s", planId, subPlanId,
								subqueryString->data)));
	}

	/* finally update the input subquery to point the result query */
	*subquery = *resultQuery;
	return true;
}


/*
 * CreateDistributedSubPlan creates a distributed subplan by recursively calling
 * the planner from the top, which may either generate a local plan or another
 * distributed plan, which can itself contain subplans.
 */
static DistributedSubPlan *
CreateDistributedSubPlan(uint32 subPlanId, Query *subPlanQuery)
{
	int cursorOptions = 0;

	if (ContainsReadIntermediateResultFunction((Node *) subPlanQuery))
	{
		/*
		 * Make sure we go through distributed planning if there are
		 * read_intermediate_result calls, even if there are no distributed
		 * tables in the query anymore.
		 *
		 * We cannot perform this check in the planner itself, since that
		 * would also cause the workers to attempt distributed planning.
		 */
		cursorOptions |= CURSOR_OPT_FORCE_DISTRIBUTED;
	}

	DistributedSubPlan *subPlan = CitusMakeNode(DistributedSubPlan);
	subPlan->plan = planner(subPlanQuery, NULL, cursorOptions, NULL);
	subPlan->subPlanId = subPlanId;

	return subPlan;
}


/*
 * CteReferenceListWalker finds all references to CTEs in the top level of a query
 * and adds them to context->cteReferenceList.
 */
static bool
CteReferenceListWalker(Node *node, CteReferenceWalkerContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;

		if (rangeTableEntry->rtekind == RTE_CTE &&
			rangeTableEntry->ctelevelsup == context->level)
		{
			context->cteReferenceList = lappend(context->cteReferenceList,
												rangeTableEntry);
		}

		/* caller will descend into range table entry */
		return false;
	}
	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		context->level += 1;
		query_tree_walker(query, CteReferenceListWalker, context,
						  QTW_EXAMINE_RTES_BEFORE);
		context->level -= 1;

		return false;
	}
	else
	{
		return expression_tree_walker(node, CteReferenceListWalker, context);
	}
}


/*
 * ContainsReferencesToOuterQuery determines whether the given query contains
 * anything that points outside of the query itself. Such queries cannot be
 * planned recursively.
 */
bool
ContainsReferencesToOuterQuery(Query *query)
{
	VarLevelsUpWalkerContext context = { 0 };
	int flags = 0;

	return query_tree_walker(query, ContainsReferencesToOuterQueryWalker,
							 &context, flags);
}


/*
 * ContainsReferencesToOuterQueryWalker determines whether the given query
 * contains any Vars that point more than context->level levels up.
 *
 * ContainsReferencesToOuterQueryWalker recursively descends into subqueries
 * and increases the level by 1 before recursing.
 */
static bool
ContainsReferencesToOuterQueryWalker(Node *node, VarLevelsUpWalkerContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup > context->level)
		{
			return true;
		}

		return false;
	}
	else if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup > context->level)
		{
			return true;
		}
	}
	else if (IsA(node, GroupingFunc))
	{
		if (((GroupingFunc *) node)->agglevelsup > context->level)
		{
			return true;
		}

		return false;
	}
	else if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup > context->level)
		{
			return true;
		}
	}
	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		int flags = 0;

		context->level += 1;
		bool found = query_tree_walker(query, ContainsReferencesToOuterQueryWalker,
									   context, flags);
		context->level -= 1;

		return found;
	}

	return expression_tree_walker(node, ContainsReferencesToOuterQueryWalker,
								  context);
}


/*
 * NodeContainsSubqueryReferencingOuterQuery determines whether the given node
 * contains anything that points outside of the query itself.
 */
static bool
NodeContainsSubqueryReferencingOuterQuery(Node *node)
{
	List *sublinks = NIL;
	ExtractSublinkWalker(node, &sublinks);

	SubLink *sublink;
	foreach_declared_ptr(sublink, sublinks)
	{
		if (ContainsReferencesToOuterQuery(castNode(Query, sublink->subselect)))
		{
			return true;
		}
	}

	return false;
}


/*
 * ReplaceRTERelationWithRteSubquery replaces the input rte relation target entry
 * with a subquery. The function also pushes down the filters to the subquery.
 *
 * It then recursively plans the subquery. This subquery is wrapped with another subquery
 * as a trick to reduce network cost, because we currently don't have an easy way to
 * skip generating NULL's for non-required columns, and if we create (SELECT a, NULL, NULL FROM table)
 * then this will be sent over network and NULL's also occupy some space. Instead of this we generate:
 * (SELECT t.a, NULL, NULL FROM (SELECT a FROM table) t). The inner subquery will be recursively planned
 * but the outer part will not be yet it will still have the NULL columns so that the query is correct.
 */
void
ReplaceRTERelationWithRteSubquery(RangeTblEntry *rangeTableEntry,
								  List *requiredAttrNumbers,
								  RecursivePlanningContext *context,
								  RTEPermissionInfo *perminfo)
{
	Query *subquery = WrapRteRelationIntoSubquery(rangeTableEntry, requiredAttrNumbers,
												  perminfo);
	List *outerQueryTargetList = CreateAllTargetListForRelation(rangeTableEntry->relid,
																requiredAttrNumbers);

	List *restrictionList =
		GetRestrictInfoListForRelation(rangeTableEntry,
									   context->plannerRestrictionContext);
	List *copyRestrictionList = copyObject(restrictionList);
	Expr *andedBoundExpressions = make_ands_explicit(copyRestrictionList);
	subquery->jointree->quals = (Node *) andedBoundExpressions;

	/*
	 * Originally the quals were pointing to the RTE and its varno
	 * was pointing to its index in rtable. However now we converted the RTE
	 * to a subquery and the quals should be pointing to that subquery, which
	 * is the only RTE in its rtable, hence we update the varnos so that they
	 * point to the subquery RTE.
	 * Originally: rtable: [rte1, current_rte, rte3...]
	 * Now: rtable: [rte1, subquery[current_rte], rte3...] --subquery[current_rte] refers to its rtable.
	 */
	Node *quals = subquery->jointree->quals;
	UpdateVarNosInNode(quals, SINGLE_RTE_INDEX);

	/* replace the function with the constructed subquery */
	rangeTableEntry->rtekind = RTE_SUBQUERY;
#if PG_VERSION_NUM >= PG_VERSION_16
	rangeTableEntry->perminfoindex = 0;
#endif
	rangeTableEntry->subquery = subquery;

	/*
	 * If the relation is inherited, it'll still be inherited as
	 * we've copied it earlier. This is to prevent the newly created
	 * subquery being treated as inherited.
	 */
	rangeTableEntry->inh = false;

	if (IsLoggableLevel(DEBUG1))
	{
		char *relationAndAliasName = GetRelationNameAndAliasName(rangeTableEntry);
		ereport(DEBUG1, (errmsg("Wrapping relation %s to a subquery",
								relationAndAliasName)));
	}

	/* as we created the subquery, now forcefully recursively plan it */
	bool recursivelyPlanned = RecursivelyPlanSubquery(subquery, context);
	if (!recursivelyPlanned)
	{
		ereport(ERROR, (errmsg(
							"unexpected state: query should have been recursively planned")));
	}

	Query *outerSubquery = CreateOuterSubquery(rangeTableEntry, outerQueryTargetList);
	rangeTableEntry->subquery = outerSubquery;
}


/*
 * GetRelationNameAndAliasName returns the relname + alias name if
 * alias name exists otherwise only the relname is returned.
 */
static char *
GetRelationNameAndAliasName(RangeTblEntry *rangeTableEntry)
{
	StringInfo str = makeStringInfo();
	appendStringInfo(str, "\"%s\"", get_rel_name(rangeTableEntry->relid));

	char *aliasName = NULL;
	if (rangeTableEntry->alias)
	{
		aliasName = rangeTableEntry->alias->aliasname;
	}

	if (aliasName)
	{
		appendStringInfo(str, " \"%s\"", aliasName);
	}
	return str->data;
}


/*
 * CreateOuterSubquery creates outer subquery which contains
 * the given range table entry in its rtable.
 */
static Query *
CreateOuterSubquery(RangeTblEntry *rangeTableEntry, List *outerSubqueryTargetList)
{
	List *innerSubqueryColNames = GenerateRequiredColNamesFromTargetList(
		outerSubqueryTargetList);

	Query *outerSubquery = makeNode(Query);
	outerSubquery->commandType = CMD_SELECT;

	/* we copy the input rteRelation to preserve the rteIdentity */
	RangeTblEntry *innerSubqueryRTE = copyObject(rangeTableEntry);

	innerSubqueryRTE->eref->colnames = innerSubqueryColNames;
	outerSubquery->rtable = list_make1(innerSubqueryRTE);

#if PG_VERSION_NUM >= PG_VERSION_16

	/* sanity check */
	Assert(innerSubqueryRTE->rtekind == RTE_SUBQUERY &&
		   innerSubqueryRTE->perminfoindex == 0);
	outerSubquery->rteperminfos = NIL;
#endif


	/* set the FROM expression to the subquery */
	RangeTblRef *newRangeTableRef = makeNode(RangeTblRef);
	newRangeTableRef->rtindex = 1;
	outerSubquery->jointree = makeFromExpr(list_make1(newRangeTableRef), NULL);

	outerSubquery->targetList = outerSubqueryTargetList;
	return outerSubquery;
}


/*
 * GenerateRequiredColNamesFromTargetList generates the required colnames
 * from the given target list.
 */
static List *
GenerateRequiredColNamesFromTargetList(List *targetList)
{
	TargetEntry *entry = NULL;
	List *innerSubqueryColNames = NIL;
	foreach_declared_ptr(entry, targetList)
	{
		if (IsA(entry->expr, Var))
		{
			/*
			 * column names of the inner subquery should only contain the
			 * required columns, as in if we choose 'b' from ('a','b') colnames
			 * should be 'a' not ('a','b')
			 */
			innerSubqueryColNames = lappend(innerSubqueryColNames, makeString(
												entry->resname));
		}
	}
	return innerSubqueryColNames;
}


/*
 * UpdateVarNosInNode iterates the Vars in the
 * given node and updates the varno's as the newVarNo.
 */
void
UpdateVarNosInNode(Node *node, Index newVarNo)
{
	List *varList = pull_var_clause(node, PVC_RECURSE_AGGREGATES |
									PVC_RECURSE_PLACEHOLDERS);
	Var *var = NULL;
	foreach_declared_ptr(var, varList)
	{
		var->varno = newVarNo;
	}
}


/*
 * IsRecursivelyPlannableRelation returns true if the given range table entry
 * is a relation type that can be converted to a subquery.
 */
bool
IsRecursivelyPlannableRelation(RangeTblEntry *rangeTableEntry)
{
	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		return false;
	}
	return rangeTableEntry->relkind == RELKIND_PARTITIONED_TABLE ||
		   rangeTableEntry->relkind == RELKIND_RELATION ||
		   rangeTableEntry->relkind == RELKIND_MATVIEW ||
		   rangeTableEntry->relkind == RELKIND_FOREIGN_TABLE;
}


/*
 * ContainsLocalTableDistributedTableJoin returns true if the input range table list
 * contains a direct join between local RTE and an RTE that contains a distributed
 * or reference table.
 */
bool
ContainsLocalTableDistributedTableJoin(List *rangeTableList)
{
	bool containsLocalTable = false;
	bool containsDistributedTable = false;

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_declared_ptr(rangeTableEntry, rangeTableList)
	{
		if (FindNodeMatchingCheckFunctionInRangeTableList(list_make1(rangeTableEntry),
														  IsDistributedOrReferenceTableRTE))
		{
			containsDistributedTable = true;
		}
		else if (IsRecursivelyPlannableRelation(rangeTableEntry) &&
				 IsLocalTableRteOrMatView((Node *) rangeTableEntry))
		{
			/* we consider citus local tables as local table */
			containsLocalTable = true;
		}
	}

	return containsLocalTable && containsDistributedTable;
}


/*
 * WrapFunctionsInSubqueries iterates over all the immediate Range Table Entries
 * of a query and wraps the functions inside (SELECT * FROM fnc() f)
 * subqueries, so that those functions will be executed on the coordinator if
 * necessary.
 *
 * We wrap all the functions that are used in joins except the ones that are
 * laterally joined or have WITH ORDINALITY clauses.
 * */
static void
WrapFunctionsInSubqueries(Query *query)
{
	List *rangeTableList = query->rtable;
	ListCell *rangeTableCell = NULL;

	/*
	 * If we have only one function call in a query without any joins, we can
	 * easily decide where to execute it.
	 *
	 * If there are some subqueries and/or functions that are joined with a
	 * function, it is not trivial to decide whether we should run this
	 * function in the coordinator or in workers and therefore we may need to
	 * wrap some of those functions in subqueries.
	 *
	 * If we have only one RTE, we leave the parsed query tree as it is. This
	 * also makes sure we do not wrap an already wrapped function call
	 * because we know that there will always be 1 RTE in a wrapped function.
	 * */
	if (list_length(rangeTableList) < 2)
	{
		return;
	}

	/* iterate over all RTEs and wrap them if necessary */
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (ShouldTransformRTE(rangeTableEntry))
		{
			TransformFunctionRTE(rangeTableEntry);
		}
	}
}


/*
 * TransformFunctionRTE wraps a given function RangeTableEntry
 * inside a (SELECT * from function() f) subquery.
 *
 * The said RangeTableEntry is modified and now points to the new subquery.
 * */
static void
TransformFunctionRTE(RangeTblEntry *rangeTblEntry)
{
	Query *subquery = makeNode(Query);
	RangeTblRef *newRangeTableRef = makeNode(RangeTblRef);
	Var *targetColumn = NULL;
	TargetEntry *targetEntry = NULL;
	AttrNumber targetColumnIndex = 0;

	RangeTblFunction *rangeTblFunction = linitial(rangeTblEntry->functions);

	subquery->commandType = CMD_SELECT;

	/* copy the input rangeTblEntry to prevent cycles */
	RangeTblEntry *newRangeTableEntry = copyObject(rangeTblEntry);

	/* set the FROM expression to the subquery */
	subquery->rtable = list_make1(newRangeTableEntry);

#if PG_VERSION_NUM >= PG_VERSION_16

	/* sanity check */
	Assert(newRangeTableEntry->rtekind == RTE_FUNCTION &&
		   newRangeTableEntry->perminfoindex == 0);
	subquery->rteperminfos = NIL;
#endif

	newRangeTableRef->rtindex = 1;
	subquery->jointree = makeFromExpr(list_make1(newRangeTableRef), NULL);

	/* Determine the result type of the function.
	 *
	 * If function return type is not composite or rowtype can't be determined,
	 * tupleDesc is set to null here
	 */
	TupleDesc tupleDesc = (TupleDesc) get_expr_result_tupdesc(rangeTblFunction->funcexpr,
															  true);

	/*
	 * If tupleDesc is not null, we iterate over all the attributes and
	 * create targetEntries
	 * */
	if (tupleDesc)
	{
		/*
		 * A sample function join that end up here:
		 *
		 * CREATE FUNCTION f(..) RETURNS TABLE(c1 int, c2 text) AS .. ;
		 * SELECT .. FROM table JOIN f(..) ON ( .. ) ;
		 *
		 * We will iterate over Tuple Description attributes. i.e (c1 int, c2 text)
		 */
		if (tupleDesc->natts > MaxAttrNumber)
		{
			ereport(ERROR, (errmsg("bad number of tuple descriptor attributes")));
		}
		AttrNumber natts = tupleDesc->natts;
		for (targetColumnIndex = 0; targetColumnIndex < natts;
			 targetColumnIndex++)
		{
			FormData_pg_attribute *attribute = TupleDescAttr(tupleDesc,
															 targetColumnIndex);
			Oid columnType = attribute->atttypid;
			char *columnName = attribute->attname.data;

			/*
			 * The indexing of attributes and TupleDesc and varattno differ
			 *
			 * varattno=0 corresponds to whole row
			 * varattno=1 corresponds to first column that is stored in tupDesc->attrs[0]
			 *
			 * That's why we need to add one to the targetColumnIndex
			 * */
			targetColumn = makeVar(1, targetColumnIndex + 1, columnType, -1, InvalidOid,
								   0);
			targetEntry = makeTargetEntry((Expr *) targetColumn, targetColumnIndex + 1,
										  columnName, false);
			subquery->targetList = lappend(subquery->targetList, targetEntry);
		}
	}

	/*
	 * If tupleDesc is NULL we have 2 different cases:
	 *
	 * 1. The function returns a record but the attributes can not be
	 * determined just by looking at the function definition. In this case the
	 * column names and types must be defined explicitly in the query
	 *
	 * 2. The function returns a non-composite type (e.g. int, text, jsonb ..)
	 * */
	else
	{
		/* create target entries for all columns returned by the function */
		ListCell *functionColumnName = NULL;

		List *functionColumnNames = rangeTblEntry->eref->colnames;
		foreach(functionColumnName, functionColumnNames)
		{
			char *columnName = strVal(lfirst(functionColumnName));
			Oid columnType = InvalidOid;

			/*
			 * If the function returns a set of records, the query needs
			 * to explicitly name column names and types
			 *
			 * Use explicitly defined types in the query if they are
			 * available
			 * */
			if (list_length(rangeTblFunction->funccoltypes) > 0)
			{
				/*
				 * A sample function join that end up here:
				 *
				 * CREATE FUNCTION get_set_of_records() RETURNS SETOF RECORD AS
				 * $cmd$
				 * SELECT x, x+1 FROM generate_series(0,4) f(x)
				 * $cmd$
				 * LANGUAGE SQL;
				 *
				 * SELECT *
				 * FROM table1 JOIN get_set_of_records() AS t2(x int, y int)
				 * ON (id = x);
				 *
				 * Note that the function definition does not have column
				 * names and types. Therefore the user needs to explicitly
				 * state them in the query
				 * */
				columnType = list_nth_oid(rangeTblFunction->funccoltypes,
										  targetColumnIndex);
			}

			/* use the types in the function definition otherwise */
			else
			{
				/*
				 * Only functions returning simple types end up here.
				 * A sample function:
				 *
				 * CREATE FUNCTION add(integer, integer) RETURNS integer AS
				 * 'SELECT $1 + $2;'
				 * LANGUAGE SQL;
				 * SELECT * FROM table JOIN add(3,5) sum ON ( .. ) ;
				 * */
				FuncExpr *funcExpr = (FuncExpr *) rangeTblFunction->funcexpr;
				columnType = funcExpr->funcresulttype;
			}

			/* Note that the column k is associated with varattno/resno of k+1 */
			targetColumn = makeVar(1, targetColumnIndex + 1, columnType, -1,
								   InvalidOid, 0);
			targetEntry = makeTargetEntry((Expr *) targetColumn,
										  targetColumnIndex + 1, columnName, false);
			subquery->targetList = lappend(subquery->targetList, targetEntry);

			targetColumnIndex++;
		}
	}

	/* replace the function with the constructed subquery */
	rangeTblEntry->rtekind = RTE_SUBQUERY;
	rangeTblEntry->subquery = subquery;
}


/*
 * ShouldTransformRTE determines whether a given RTE should bne wrapped in a
 * subquery.
 *
 * Not all functions should be wrapped in a subquery for now. As we support more
 * functions to be used in joins, the constraints here will be relaxed.
 * */
static bool
ShouldTransformRTE(RangeTblEntry *rangeTableEntry)
{
	/*
	 * We should wrap only function rtes that are not LATERAL and
	 * without WITH ORDINALITY clause
	 */
	if (rangeTableEntry->rtekind != RTE_FUNCTION ||
		rangeTableEntry->lateral ||
		rangeTableEntry->funcordinality)
	{
		return false;
	}
	return true;
}


/*
 * BuildSubPlanResultQuery returns a query of the form:
 *
 * SELECT
 *   <target list>
 * FROM
 *   read_intermediate_result('<resultId>', '<copy format'>)
 *   AS res (<column definition list>);
 *
 * The caller can optionally supply a columnAliasList, which is useful for
 * CTEs that have column aliases.
 *
 * If any of the types in the target list cannot be used in the binary copy format,
 * then the copy format 'text' is used, otherwise 'binary' is used.
 */
Query *
BuildSubPlanResultQuery(List *targetEntryList, List *columnAliasList, char *resultId)
{
	Oid functionOid = CitusReadIntermediateResultFuncId();
	bool useBinaryCopyFormat = CanUseBinaryCopyFormatForTargetList(targetEntryList);

	Const *resultIdConst = makeNode(Const);
	resultIdConst->consttype = TEXTOID;
	resultIdConst->consttypmod = -1;
	resultIdConst->constlen = -1;
	resultIdConst->constvalue = CStringGetTextDatum(resultId);
	resultIdConst->constbyval = false;
	resultIdConst->constisnull = false;
	resultIdConst->location = -1;

	return BuildReadIntermediateResultsQuery(targetEntryList, columnAliasList,
											 resultIdConst, functionOid,
											 useBinaryCopyFormat);
}


/*
 * BuildReadIntermediateResultsArrayQuery returns a query of the form:
 *
 * SELECT
 *   <target list>
 * FROM
 *   read_intermediate_results(ARRAY['<resultId>', ...]::text[], '<copy format'>)
 *   AS res (<column definition list>);
 *
 * The caller can optionally supply a columnAliasList, which is useful for
 * CTEs that have column aliases.
 *
 * If useBinaryCopyFormat is true, then 'binary' format is used. Otherwise,
 * 'text' format is used.
 */
Query *
BuildReadIntermediateResultsArrayQuery(List *targetEntryList,
									   List *columnAliasList,
									   List *resultIdList,
									   bool useBinaryCopyFormat)
{
	Oid functionOid = CitusReadIntermediateResultArrayFuncId();

	Const *resultIdConst = makeNode(Const);
	resultIdConst->consttype = TEXTARRAYOID;
	resultIdConst->consttypmod = -1;
	resultIdConst->constlen = -1;
	resultIdConst->constvalue = PointerGetDatum(strlist_to_textarray(resultIdList));
	resultIdConst->constbyval = false;
	resultIdConst->constisnull = false;
	resultIdConst->location = -1;

	return BuildReadIntermediateResultsQuery(targetEntryList, columnAliasList,
											 resultIdConst, functionOid,
											 useBinaryCopyFormat);
}


/*
 * For the given target list, build an empty relation with the same target list.
 * For example, if the target list is (a, b, c), and resultId is "empty", then
 * it returns a Query object for this SQL:
 *      SELECT a, b, c FROM (VALUES (NULL, NULL, NULL)) AS empty(a, b, c) WHERE false;
 */
Query *
BuildEmptyResultQuery(List *targetEntryList, char *resultId)
{
	List *targetList = NIL;
	ListCell *targetEntryCell = NULL;

	List *colTypes = NIL;
	List *colTypMods = NIL;
	List *colCollations = NIL;
	List *colNames = NIL;

	List *valueConsts = NIL;
	List *valueTargetList = NIL;
	List *valueColNames = NIL;

	int targetIndex = 1;

	/* build the target list and column lists needed */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *targetExpr = (Node *) targetEntry->expr;
		char *columnName = targetEntry->resname;
		Oid columnType = exprType(targetExpr);
		Oid columnTypMod = exprTypmod(targetExpr);
		Oid columnCollation = exprCollation(targetExpr);

		if (targetEntry->resjunk)
		{
			continue;
		}

		Var *tgtVar = makeVar(1, targetIndex, columnType, columnTypMod, columnCollation,
							  0);
		TargetEntry *tgtEntry = makeTargetEntry((Expr *) tgtVar, targetIndex, columnName,
												false);
		Const *valueConst = makeConst(columnType, columnTypMod, columnCollation, 0,
									  (Datum) 0, true, false);

		StringInfoData *columnString = makeStringInfo();
		appendStringInfo(columnString, "column%d", targetIndex);

		TargetEntry *valueTgtEntry = makeTargetEntry((Expr *) tgtVar, targetIndex,
													 columnString->data, false);

		valueConsts = lappend(valueConsts, valueConst);
		valueTargetList = lappend(valueTargetList, valueTgtEntry);
		valueColNames = lappend(valueColNames, makeString(columnString->data));

		colNames = lappend(colNames, makeString(columnName));
		colTypes = lappend_oid(colTypes, columnType);
		colTypMods = lappend_oid(colTypMods, columnTypMod);
		colCollations = lappend_oid(colCollations, columnCollation);

		targetList = lappend(targetList, tgtEntry);

		targetIndex++;
	}

	/* Build a RangeTable Entry for the VALUES relation */
	RangeTblEntry *valuesRangeTable = makeNode(RangeTblEntry);
	valuesRangeTable->rtekind = RTE_VALUES;
	valuesRangeTable->values_lists = list_make1(valueConsts);
	valuesRangeTable->colcollations = colCollations;
	valuesRangeTable->coltypes = colTypes;
	valuesRangeTable->coltypmods = colTypMods;
	valuesRangeTable->alias = NULL;
	valuesRangeTable->eref = makeAlias("*VALUES*", valueColNames);
	valuesRangeTable->inFromCl = true;

	RangeTblRef *valuesRTRef = makeNode(RangeTblRef);
	valuesRTRef->rtindex = 1;

	FromExpr *valuesJoinTree = makeNode(FromExpr);
	valuesJoinTree->fromlist = list_make1(valuesRTRef);

	/* build the VALUES query */
	Query *valuesQuery = makeNode(Query);
	valuesQuery->canSetTag = true;
	valuesQuery->commandType = CMD_SELECT;
	valuesQuery->rtable = list_make1(valuesRangeTable);
	#if PG_VERSION_NUM >= PG_VERSION_16
	valuesQuery->rteperminfos = NIL;
	#endif
	valuesQuery->jointree = valuesJoinTree;
	valuesQuery->targetList = valueTargetList;

	/* build the relation selecting from the VALUES */
	RangeTblEntry *emptyRangeTable = makeNode(RangeTblEntry);
	emptyRangeTable->rtekind = RTE_SUBQUERY;
	emptyRangeTable->subquery = valuesQuery;
	emptyRangeTable->alias = makeAlias(resultId, colNames);
	emptyRangeTable->eref = emptyRangeTable->alias;
	emptyRangeTable->inFromCl = true;

	/* build the SELECT query */
	Query *resultQuery = makeNode(Query);
	resultQuery->commandType = CMD_SELECT;
	resultQuery->canSetTag = true;
	resultQuery->rtable = list_make1(emptyRangeTable);
#if PG_VERSION_NUM >= PG_VERSION_16
	resultQuery->rteperminfos = NIL;
#endif
	RangeTblRef *rangeTableRef = makeNode(RangeTblRef);
	rangeTableRef->rtindex = 1;

	/* insert a FALSE qual to ensure 0 rows returned */
	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->fromlist = list_make1(rangeTableRef);
	joinTree->quals = makeBoolConst(false, false);
	resultQuery->jointree = joinTree;
	resultQuery->targetList = targetList;

	return resultQuery;
}


/*
 * BuildReadIntermediateResultsQuery is the common code for generating
 * queries to read from result files. It is used by
 * BuildReadIntermediateResultsArrayQuery and BuildSubPlanResultQuery.
 */
static Query *
BuildReadIntermediateResultsQuery(List *targetEntryList, List *columnAliasList,
								  Const *resultIdConst, Oid functionOid,
								  bool useBinaryCopyFormat)
{
	List *funcColNames = NIL;
	List *funcColTypes = NIL;
	List *funcColTypMods = NIL;
	List *funcColCollations = NIL;
	ListCell *targetEntryCell = NULL;
	List *targetList = NIL;
	int columnNumber = 1;
	Oid copyFormatId = BinaryCopyFormatId();
	int columnAliasCount = list_length(columnAliasList);

	/* build the target list and column definition list */
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *targetExpr = (Node *) targetEntry->expr;
		char *columnName = targetEntry->resname;
		Oid columnType = exprType(targetExpr);
		Oid columnTypMod = exprTypmod(targetExpr);
		Oid columnCollation = exprCollation(targetExpr);

		if (targetEntry->resjunk)
		{
			continue;
		}

		funcColNames = lappend(funcColNames, makeString(columnName));
		funcColTypes = lappend_int(funcColTypes, columnType);
		funcColTypMods = lappend_int(funcColTypMods, columnTypMod);
		funcColCollations = lappend_int(funcColCollations, columnCollation);

		Var *functionColumnVar = makeNode(Var);
		functionColumnVar->varno = 1;
		functionColumnVar->varattno = columnNumber;
		functionColumnVar->vartype = columnType;
		functionColumnVar->vartypmod = columnTypMod;
		functionColumnVar->varcollid = columnCollation;
		functionColumnVar->varlevelsup = 0;
		functionColumnVar->varnosyn = 1;
		functionColumnVar->varattnosyn = columnNumber;
		functionColumnVar->location = -1;

		TargetEntry *newTargetEntry = makeNode(TargetEntry);
		newTargetEntry->expr = (Expr *) functionColumnVar;
		newTargetEntry->resno = columnNumber;

		/*
		 * Rename the column only if a column alias is defined.
		 * Notice that column alias count could be less than actual
		 * column count. We only use provided aliases and keep the
		 * original column names if no alias is defined.
		 */
		if (columnAliasCount >= columnNumber)
		{
			String *columnAlias = (String *) list_nth(columnAliasList, columnNumber - 1);
			Assert(IsA(columnAlias, String));
			newTargetEntry->resname = strVal(columnAlias);
		}
		else
		{
			newTargetEntry->resname = columnName;
		}
		newTargetEntry->resjunk = false;

		targetList = lappend(targetList, newTargetEntry);

		columnNumber++;
	}

	/* build the citus_copy_format parameter for the call to read_intermediate_result */
	if (!useBinaryCopyFormat)
	{
		copyFormatId = TextCopyFormatId();
	}

	Const *resultFormatConst = makeNode(Const);
	resultFormatConst->consttype = CitusCopyFormatTypeId();
	resultFormatConst->consttypmod = -1;
	resultFormatConst->constlen = 4;
	resultFormatConst->constvalue = ObjectIdGetDatum(copyFormatId);
	resultFormatConst->constbyval = true;
	resultFormatConst->constisnull = false;
	resultFormatConst->location = -1;

	/* build the call to read_intermediate_result */
	FuncExpr *funcExpr = makeNode(FuncExpr);
	funcExpr->funcid = functionOid;
	funcExpr->funcretset = true;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = 0;
	funcExpr->funccollid = 0;
	funcExpr->inputcollid = 0;
	funcExpr->location = -1;
	funcExpr->args = list_make2(resultIdConst, resultFormatConst);

	/* build the RTE for the call to read_intermediate_result */
	RangeTblFunction *rangeTableFunction = makeNode(RangeTblFunction);
	rangeTableFunction->funccolcount = list_length(funcColNames);
	rangeTableFunction->funccolnames = funcColNames;
	rangeTableFunction->funccoltypes = funcColTypes;
	rangeTableFunction->funccoltypmods = funcColTypMods;
	rangeTableFunction->funccolcollations = funcColCollations;
	rangeTableFunction->funcparams = NULL;
	rangeTableFunction->funcexpr = (Node *) funcExpr;

	Alias *funcAlias = makeNode(Alias);
	funcAlias->aliasname = "intermediate_result";
	funcAlias->colnames = funcColNames;

	RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
	rangeTableEntry->rtekind = RTE_FUNCTION;
	rangeTableEntry->functions = list_make1(rangeTableFunction);
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->eref = funcAlias;

	/* build the join tree using the read_intermediate_result RTE */
	RangeTblRef *rangeTableRef = makeNode(RangeTblRef);
	rangeTableRef->rtindex = 1;

	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->fromlist = list_make1(rangeTableRef);

	/* build the SELECT query */
	Query *resultQuery = makeNode(Query);
	resultQuery->commandType = CMD_SELECT;
	resultQuery->rtable = list_make1(rangeTableEntry);
#if PG_VERSION_NUM >= PG_VERSION_16
	resultQuery->rteperminfos = NIL;
#endif
	resultQuery->jointree = joinTree;
	resultQuery->targetList = targetList;

	return resultQuery;
}


/*
 * GenerateResultId generates the result ID that is used to identify an intermediate
 * result of the subplan with the given plan ID and subplan ID.
 */
char *
GenerateResultId(uint64 planId, uint32 subPlanId)
{
	StringInfo resultId = makeStringInfo();

	appendStringInfo(resultId, UINT64_FORMAT "_%u", planId, subPlanId);

	return resultId->data;
}


/*
 * GeneratingSubplans returns true if we are currently in the process of
 * generating subplans.
 */
bool
GeneratingSubplans(void)
{
	return recursivePlanningDepth > 0;
}


/*
 * IsPushdownSafeForRTEInLeftJoin returns true if the given range table entry 
 * is safe for pushdown. Currently, we only allow RTE_RELATION and RTE_FUNCTION.
 */
bool IsPushdownSafeForRTEInLeftJoin(RangeTblEntry *rte)
{
	if (rte->rtekind == RTE_RELATION)
	{
		return true;
	}
	/* check if it is a citus table, e.g., ref table */
	else if (rte->rtekind == RTE_FUNCTION)
	{
		RangeTblEntry *newRte = NULL;
		if(!ExtractCitusExtradataContainerRTE(rte, &newRte))
		{
			ereport(DEBUG5, (errmsg("RTE type %d is not safe for pushdown, function but it does not contain citus extradata",
									rte->rtekind)));
			return false;
		}
		return true;
	}
	else
	{
		ereport(DEBUG5, (errmsg("RTE type %d is not safe for pushdown",
							    rte->rtekind)));
		return false;
	}
	
}
