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
 * - Task-tracker queries that can be executed through a tree of
 *   re-partitioning operations.
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

#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/query_colocation_checker.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/log_utils.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "parser/parsetree.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif
#include "utils/builtins.h"
#include "utils/guc.h"


/* track depth of current recursive planner query */
static int recursivePlanningDepth = 0;

/*
 * RecursivePlanningContext is used to recursively plan subqueries
 * and CTEs, pull results to the coordinator, and push it back into
 * the workers.
 */
typedef struct RecursivePlanningContext
{
	int level;
	uint64 planId;
	bool allDistributionKeysInQueryAreEqual; /* used for some optimizations */
	List *subPlanList;
	PlannerRestrictionContext *plannerRestrictionContext;
} RecursivePlanningContext;


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
static List * SublinkList(Query *originalQuery);
static bool ExtractSublinkWalker(Node *node, List **sublinkList);
static bool ShouldRecursivelyPlanAllSubqueriesInWhere(Query *query);
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
static void RecursivelyPlanSetOperations(Query *query, Node *node,
										 RecursivePlanningContext *context);
static bool IsLocalTableRTE(Node *node);
static void RecursivelyPlanSubquery(Query *subquery,
									RecursivePlanningContext *planningContext);
static DistributedSubPlan * CreateDistributedSubPlan(uint32 subPlanId,
													 Query *subPlanQuery);
static bool CteReferenceListWalker(Node *node, CteReferenceWalkerContext *context);
static bool ContainsReferencesToOuterQuery(Query *query);
static bool ContainsReferencesToOuterQueryWalker(Node *node,
												 VarLevelsUpWalkerContext *context);
static void WrapFunctionsInSubqueries(Query *query);
static void TransformFunctionRTE(RangeTblEntry *rangeTblEntry);
static bool ShouldTransformRTE(RangeTblEntry *rangeTableEntry);
static Query * BuildReadIntermediateResultsQuery(List *targetEntryList,
												 List *columnAliasList,
												 Const *resultIdConst, Oid functionOid,
												 bool useBinaryCopyFormat);

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
							 ApplyLogRedaction(subPlanString->data))));
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

	/*
	 * If the FROM clause is recurring (does not contain a distributed table),
	 * then we cannot have any distributed tables appearing in subqueries in
	 * the WHERE clause.
	 */
	if (ShouldRecursivelyPlanAllSubqueriesInWhere(query))
	{
		/* replace all subqueries in the WHERE clause */
		RecursivelyPlanAllSubqueries((Node *) query->jointree->quals, context);
	}

	/*
	 * If the query doesn't have distribution key equality,
	 * recursively plan some of its subqueries.
	 */
	if (ShouldRecursivelyPlanNonColocatedSubqueries(query, context))
	{
		RecursivelyPlanNonColocatedSubqueries(query, context);
	}

	return NULL;
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
	if (FindNodeCheckInRangeTableList(subquery->rtable, IsLocalTableRTE))
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
 * RecursivelyPlanNonColocatedJoinWalker gets a query and walks over its sublinks
 * to find subqueries that live in WHERE clause.
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
	List *sublinkList = SublinkList(query);
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
 * SublinkList finds the subquery nodes in the where clause of the given query. Note
 * that the function should be called on the original query given that postgres
 * standard_planner() may convert the subqueries in WHERE clause to joins.
 */
static List *
SublinkList(Query *originalQuery)
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
 * ShouldRecursivelyPlanAllSubqueriesInWhere returns true if the query has
 * a WHERE clause and a recurring FROM clause (does not contain a distributed
 * table).
 */
static bool
ShouldRecursivelyPlanAllSubqueriesInWhere(Query *query)
{
	FromExpr *joinTree = query->jointree;
	if (joinTree == NULL)
	{
		/* there is no FROM clause */
		return false;
	}

	Node *whereClause = joinTree->quals;
	if (whereClause == NULL)
	{
		/* there is no WHERE clause */
		return false;
	}

	if (FindNodeCheckInRangeTableList(query->rtable, IsDistributedTableRTE))
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

		if (FindNodeCheckInRangeTableList(query->rtable, IsDistributedTableRTE))
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
							 "recursive CTEs are not supported in distributed "
							 "queries",
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
									ApplyLogRedaction(subPlanString->data))));
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
	if (FindNodeCheckInRangeTableList(subquery->rtable, IsLocalTableRTE))
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
	else if (DeferErrorIfCannotPushdownSubquery(subquery, false) == NULL)
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
	else if (TaskExecutorType == MULTI_EXECUTOR_TASK_TRACKER &&
			 SingleRelationRepartitionSubquery(subquery))
	{
		/*
		 * Citus can plan this and execute via repartitioning. Thus,
		 * no need to recursively plan.
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
	if (!SafeToPushdownUnionSubquery(filteredRestrictionContext))
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
			QueryContainsDistributedTableRTE(subquery))
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
 * IsLocalTableRTE gets a node and returns true if the node
 * is a range table relation entry that points to a local
 * relation (i.e., not a distributed relation).
 */
static bool
IsLocalTableRTE(Node *node)
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
	if (IsCitusTable(relationId))
	{
		return false;
	}

	/* local table found */
	return true;
}


/*
 * RecursivelyPlanQuery recursively plans a query, replaces it with a
 * result query and returns the subplan.
 *
 * Before we recursively plan the given subquery, we should ensure
 * that the subquery doesn't contain any references to the outer
 * queries (i.e., such queries cannot be separately planned). In
 * that case, the function doesn't recursively plan the input query
 * and immediately returns. Later, the planner decides on what to do
 * with the query.
 */
static void
RecursivelyPlanSubquery(Query *subquery, RecursivePlanningContext *planningContext)
{
	uint64 planId = planningContext->planId;
	Query *debugQuery = NULL;

	if (ContainsReferencesToOuterQuery(subquery))
	{
		elog(DEBUG2, "skipping recursive planning for the subquery since it "
					 "contains references to outer queries");

		return;
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
								ApplyLogRedaction(subqueryString->data))));
	}

	/* finally update the input subquery to point the result query */
	*subquery = *resultQuery;
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
	subPlan->plan = planner(subPlanQuery, cursorOptions, NULL);
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
 * any Vars that point outside of the query itself. Such queries cannot be
 * planned recursively.
 */
static bool
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
		for (targetColumnIndex = 0; targetColumnIndex < (AttrNumber) tupleDesc->natts;
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
		functionColumnVar->varnoold = 1;
		functionColumnVar->varoattno = columnNumber;
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
			Value *columnAlias = (Value *) list_nth(columnAliasList, columnNumber - 1);
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
