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
 * - Real-time queries that can be executed by performing a task for each
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
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/multi_server_executor.h"
#include "distributed/relation_restriction_equivalence.h"
#include "lib/stringinfo.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/guc.h"


/*
 * RecursivePlanningContext is used to recursively plan subqueries
 * and CTEs, pull results to the coordinator, and push it back into
 * the workers.
 */
typedef struct RecursivePlanningContext
{
	int level;
	uint64 planId;
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


/*
 * QueryWalkerWithRTEIdentity
 */
typedef struct QueryWalkerWithRTEIdentity
{
	int rteIdentity;
	Query *query;
} QueryWalkerWithRTEIdentity;


/* local function forward declarations */
static DeferredErrorMessage * RecursivelyPlanSubqueriesAndCTEs(Query *query,
															   RecursivePlanningContext *
															   context);
static bool ShouldRecursivelyPlanNonColocatedJoins(Query *query,
												   RecursivePlanningContext *context);
static void RecursivelyPlanNonColocatedJoins(Query *query,
											 RecursivePlanningContext *context);
static bool ExtractQueryWalkerWithRTEIdentity(Node *node,
											  QueryWalkerWithRTEIdentity *context);
static void RecursivelyPlanSubqueriesInWhereClause(Query *query,
												   RecursivePlanningContext *context,
												   List *queryAttributeEquivalenceList,
												   RelationRestrictionContext *
												   queryRelationRestrictionContext,
												   RelationRestrictionContext *
												   fromClauseRelationRestrictionContext,
												   Relids rteIdentitiesOfSubqueriesInWhere);

static Relids RTEIdentitiesOfSubqueriesInWhere(Query *query);
static bool ShouldRecursivelyPlanAllSublinks(Query *query);
static bool RecursivelyPlanAllSubqueries(Node *node,
										 RecursivePlanningContext *planningContext);
static DeferredErrorMessage * RecursivelyPlanCTEs(Query *query,
												  RecursivePlanningContext *context);
static bool RecursivelyPlanSubqueryWalker(Node *node, RecursivePlanningContext *context);
static bool ShouldRecursivelyPlanSubquery(Query *subquery);
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
static Query * BuildSubPlanResultQuery(Query *subquery, List *columnAliasList,
									   uint64 planId, uint32 subPlanId);


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
	DeferredErrorMessage *error = NULL;

	/*
	 * Plan subqueries and CTEs that cannot be pushed down by recursively
	 * calling the planner and add the resulting plans to subPlanList.
	 */
	context.level = 0;
	context.planId = planId;
	context.subPlanList = NIL;
	context.plannerRestrictionContext = plannerRestrictionContext;

	error = RecursivelyPlanSubqueriesAndCTEs(originalQuery, &context);
	if (error != NULL)
	{
		RaiseDeferredError(error, ERROR);
	}

	if (context.subPlanList && (log_min_messages <= DEBUG1 || client_min_messages <=
								DEBUG1))
	{
		StringInfo subPlanString = makeStringInfo();
		pg_get_query_def(originalQuery, subPlanString);
		ereport(DEBUG1, (errmsg(
							 "Plan " UINT64_FORMAT
							 " query after replacing subqueries and CTEs: %s", planId,
							 subPlanString->data)));
	}

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
	DeferredErrorMessage *error = NULL;

	error = RecursivelyPlanCTEs(query, context);
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
	 * For certain cases, we should recursively plan all the sublinks to sucessefully
	 * execute the query.
	 */
	if (ShouldRecursivelyPlanAllSublinks(query))
	{
		RecursivelyPlanAllSubqueries(query->jointree->quals, context);
	}

	/*
	 * Each subquery is individually can be executed successfully. However,
	 * they cannot be executed in combination with each other via non-colocated
	 * joins (i.e., joins on non distribution keys).
	 */
	if (ShouldRecursivelyPlanNonColocatedJoins(query, context))
	{
		RecursivelyPlanNonColocatedJoins(query, context);
	}

	return NULL;
}


/*
 * ShouldRecursivelyPlanNonColocatedJoins returns true if not all the distributed
 * relations are joined on their distribution columns.
 *
 * The function returns false if the query contains any local relations since
 * joins among distributed relations and local relations are not supported.
 */
static bool
ShouldRecursivelyPlanNonColocatedJoins(Query *query, RecursivePlanningContext *context)
{
	PlannerRestrictionContext *filteredRestrictionContext = NULL;

	/* we may consider handinling regular non-equi joins seperately */
	if (context->level == 0 && !query->hasSubLinks)
	{
		return false;
	}

	/* set operations are not our target */
	if (ContainsUnionSubquery(query) || query->setOperations != NULL)
	{
		return false;
	}

	/* direct joins with local tables are not supported by any of Citus planners */
	if (FindNodeCheckInRangeTableList(query->rtable, IsLocalTableRTE))
	{
		return false;
	}

	/* trigger recursive planning if there is no partition key equality */
	filteredRestrictionContext =
		FilterPlannerRestrictionForQuery(context->plannerRestrictionContext, query);
	if (!RestrictionEquivalenceForPartitionKeys(filteredRestrictionContext))
	{
		return true;
	}

	return false;
}


/*
 * RecursivelyPlanNonColocatedJoins picks the necessary subquery (or subqueries)
 * and recursively plans to make the input query executable.
 *
 * The non colocated joins could appear in the FROM clause and/or via subqueries
 * in WHERE clause.
 *
 * The function currently only supports recursive planning for subqueries in
 * WHERE clause.
 *
 * TODO: Implement the necessary logic for supporting non colocated joins in
 * the FROM clause.
 */
static void
RecursivelyPlanNonColocatedJoins(Query *query, RecursivePlanningContext *context)
{
	Relids rteIdentitiesOfSubqueriesInWhere = RTEIdentitiesOfSubqueriesInWhere(query);

	PlannerRestrictionContext *queryRestrictionContext =
		FilterPlannerRestrictionForQuery(context->plannerRestrictionContext, query);
	List *queryAttributeEquivalenceList =
		GenerateAllAttributedEquivalances(queryRestrictionContext);
	RelationRestrictionContext *queryRelationRestrictionContext =
		queryRestrictionContext->relationRestrictionContext;
	RelationRestrictionContext *fromClauseRelationRestrictionContext =
		ExcludeRelationRestrictionContext(queryRelationRestrictionContext,
										  rteIdentitiesOfSubqueriesInWhere);

	/*
	 * We have more than one relation in the FROM clause and they don't have a distribution
	 * key equality.
	 */
	if (list_length(fromClauseRelationRestrictionContext->relationRestrictionList) > 1 &&
		!EquivalenceListContainsRelationsEquality(queryAttributeEquivalenceList,
												  fromClauseRelationRestrictionContext))
	{
		/*
		 * TODO: Implement recursive planning on non-colocated joins in the FROM clause. Note that
		 * we should also consider OUTER JOINs that are non-colocated.
		 */
		return;
	}

	/*
	 * At this point we're sure that FROM clause entries are safe to pushdown but not the
	 * whole query. Thus, we should recursively plan some (or all) subqueries in WHERE clause
	 * to be able to execute the whole query.
	 */

	/*
	 * First check whether we'd want to recursively plan all subqueries in WHERE clause.
	 * This could happen if all FROM subqueries are replaced above.
	 */
	if (ShouldRecursivelyPlanAllSublinks(query))
	{
		RecursivelyPlanAllSubqueries(query->jointree->quals, context);
	}
	else
	{
		RecursivelyPlanSubqueriesInWhereClause(query, context,
											   queryAttributeEquivalenceList,
											   queryRelationRestrictionContext,
											   fromClauseRelationRestrictionContext,
											   rteIdentitiesOfSubqueriesInWhere);
	}
}


/*
 * RecursivelyPlanSubqueriesInWhereClause replaces the subqueries in WHERE clause
 * which are not joined on distribution column with the FROM clause subqueries.
 */
static void
RecursivelyPlanSubqueriesInWhereClause(Query *query,
									   RecursivePlanningContext *context,
									   List *queryAttributeEquivalenceList,
									   RelationRestrictionContext *
									   queryRelationRestrictionContext,
									   RelationRestrictionContext *
									   fromClauseRelationRestrictionContext,
									   Relids rteIdentitiesOfSubqueriesInWhere)
{
	int rteIdentity = -1;
	List *fromClauseRelationRestrictionList =
		fromClauseRelationRestrictionContext->relationRestrictionList;

	/*
	 * Iterate on each of the RTE_RELATION in the sublinks. When a non-equi join
	 * among the Ffound, recursively plan
	 */
	while ((rteIdentity =
				bms_next_member(rteIdentitiesOfSubqueriesInWhere, rteIdentity)) >= 0)
	{
		RelationRestrictionContext *relationRestrictionContext =
			FilterRelationRestrictionContext(queryRelationRestrictionContext,
											 bms_make_singleton(rteIdentity));
		RelationRestriction *relationRestriction =
			(RelationRestriction *)
			linitial(relationRestrictionContext->relationRestrictionList);

		List *copyOfFromClauseRelationRestrictionList =
			list_copy(fromClauseRelationRestrictionList);

		List *mergedRelationRestrictionList =
			lappend(copyOfFromClauseRelationRestrictionList, relationRestriction);

		RelationRestrictionContext *mergedRestrictionContext =
			palloc0(sizeof(RelationRestrictionContext));

		mergedRestrictionContext->relationRestrictionList = mergedRelationRestrictionList;

		/*
		 * We found an RTE_RELATION in the subquery in WHERE clause, which doesn't have
		 * a distribution key equality with the from clause sub(queries). Thus, recursively
		 * plan the subquery that this RTE_RELATION resides in.
		 */
		if (!EquivalenceListContainsRelationsEquality(queryAttributeEquivalenceList,
													  mergedRestrictionContext))
		{
			QueryWalkerWithRTEIdentity queryWalkerWithRTEIdentity;

			queryWalkerWithRTEIdentity.rteIdentity = rteIdentity;

			ExtractQueryWalkerWithRTEIdentity(query->jointree->quals,
											  &queryWalkerWithRTEIdentity);

			if (queryWalkerWithRTEIdentity.query != NULL)
			{
				RecursivelyPlanSubquery(queryWalkerWithRTEIdentity.query, context);
			}
		}
	}
}


/*
 * ExtractQueryWalkerWithRTEIdentity walks over a query top-down, finds the query with
 * the given rte identity returns finializes the traversal.
 */
static bool
ExtractQueryWalkerWithRTEIdentity(Node *node, QueryWalkerWithRTEIdentity *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		Relids queryRTEIdentites = QueryRteIdentities(query);

		if (bms_is_member(context->rteIdentity, queryRTEIdentites))
		{
			context->query = query;

			/* we're done */
			return false;
		}

		return query_tree_walker(query, ExtractQueryWalkerWithRTEIdentity, context, 0);
	}

	return expression_tree_walker(node, ExtractQueryWalkerWithRTEIdentity, context);
}


/*
 * RTEIdentitiesOfSubqueriesInWhere returns a bitmap of the rte identities of the
 * relations that appear in the subqueries in WHERE clause for the given query.
 */
static Relids
RTEIdentitiesOfSubqueriesInWhere(Query *query)
{
	List *sublinkList = SublinkList(query);
	Relids allSublinkRteIdentities = NULL;
	ListCell *subLinkCell = NULL;

	foreach(subLinkCell, sublinkList)
	{
		SubLink *sublink = (SubLink *) lfirst(subLinkCell);
		Node *subselect = sublink->subselect;
		Query *subquery = NULL;
		Relids subqueryRteIdentities = NULL;

		if (!IsA(subselect, Query))
		{
			continue;
		}

		subquery = (Query *) subselect;

		subqueryRteIdentities = QueryRteIdentities(subquery);

		allSublinkRteIdentities = bms_union(allSublinkRteIdentities,
											subqueryRteIdentities);
	}

	return allSublinkRteIdentities;
}


/*
 * ShouldRecursivelyPlanAllSublinks returns true when the input query
 * contains only recurring tuples in the FROM clause and has subqueries
 * in WHERE clause. Otherwise, the function returns false.
 */
static bool
ShouldRecursivelyPlanAllSublinks(Query *query)
{
	List *sublinkList = SublinkList(query);

	if (sublinkList != NIL &&
		!FindNodeCheckInRangeTableList(query->rtable, IsDistributedTableRTE))
	{
		return true;
	}

	return false;
}


/*
 * RecursivelyPlanAllSubqueries descends into an expression tree and recursively
 * plans all subqueries found from the top.
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

	if (query->hasModifyingCTE)
	{
		/* we could easily support these, but it's a little scary */
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "data-modifying statements are not supported in "
							 "the WITH clauses of distributed queries",
							 NULL, NULL);
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
		uint32 subPlanId = 0;
		Query *resultQuery = NULL;
		DistributedSubPlan *subPlan = NULL;
		ListCell *rteCell = NULL;
		int replacedCtesCount = 0;

		if (ContainsReferencesToOuterQuery(subquery))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "CTEs that refer to other subqueries are not "
								 "supported in multi-shard queries",
								 NULL, NULL);
		}

		if (cte->cterefcount == 0)
		{
			/*
			 * CTEs that aren't referenced aren't executed in postgres. We
			 * don't need to generate a subplan for it and can take the rest
			 * of this iteration off.
			 */
			continue;
		}

		subPlanId = list_length(planningContext->subPlanList) + 1;

		if (log_min_messages <= DEBUG1 || client_min_messages <= DEBUG1)
		{
			StringInfo subPlanString = makeStringInfo();
			pg_get_query_def(subquery, subPlanString);
			ereport(DEBUG1, (errmsg("generating subplan " UINT64_FORMAT "_%u for "
																		"CTE %s: %s",
									planId, subPlanId, cteName, subPlanString->data)));
		}

		/* build a sub plan for the CTE */
		subPlan = CreateDistributedSubPlan(subPlanId, subquery);
		planningContext->subPlanList = lappend(planningContext->subPlanList, subPlan);

		/* replace references to the CTE with a subquery that reads results */
		resultQuery = BuildSubPlanResultQuery(subquery, cte->aliascolnames, planId,
											  subPlanId);

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
		DeferredErrorMessage *error = NULL;

		context->level += 1;

		/*
		 * First, make sure any subqueries and CTEs within this subquery
		 * are recursively planned if necessary.
		 */
		error = RecursivelyPlanSubqueriesAndCTEs(query, context);
		if (error != NULL)
		{
			RaiseDeferredError(error, ERROR);
		}
		context->level -= 1;

		/*
		 * Recursively plan this subquery if it cannot be pushed down and is
		 * eligible for recursive planning.
		 */
		if (ShouldRecursivelyPlanSubquery(query))
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
ShouldRecursivelyPlanSubquery(Query *subquery)
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
		 * Citus can pushdown this subquery, no need to recursively
		 * plan which is much expensive than pushdown.
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
 * ShouldRecursivelyPlanSetOperation determines whether the leaf queries of a
 * set operations tree need to be recursively planned in order to support the
 * query as a whole.
 */
static bool
ShouldRecursivelyPlanSetOperation(Query *query, RecursivePlanningContext *context)
{
	PlannerRestrictionContext *filteredRestrictionContext = NULL;

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

	filteredRestrictionContext =
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
	RangeTblEntry *rangeTableEntry = NULL;
	Oid relationId = InvalidOid;

	if (node == NULL)
	{
		return false;
	}

	if (!IsA(node, RangeTblEntry))
	{
		return false;
	}

	rangeTableEntry = (RangeTblEntry *) node;
	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		return false;
	}

	if (rangeTableEntry->relkind == RELKIND_VIEW)
	{
		return false;
	}

	relationId = rangeTableEntry->relid;
	if (IsDistributedTable(relationId))
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
	DistributedSubPlan *subPlan = NULL;
	uint64 planId = planningContext->planId;
	int subPlanId = 0;

	Query *resultQuery = NULL;
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
	if (log_min_messages <= DEBUG1 || client_min_messages <= DEBUG1)
	{
		debugQuery = copyObject(subquery);
	}

	/*
	 * Create the subplan and append it to the list in the planning context.
	 */
	subPlanId = list_length(planningContext->subPlanList) + 1;

	subPlan = CreateDistributedSubPlan(subPlanId, subquery);
	planningContext->subPlanList = lappend(planningContext->subPlanList, subPlan);

	/*
	 * BuildSubPlanResultQuery() can optionally use provided column aliases.
	 * We do not need to send additional alias list for subqueries.
	 */
	resultQuery = BuildSubPlanResultQuery(subquery, NIL, planId, subPlanId);

	if (log_min_messages <= DEBUG1 || client_min_messages <= DEBUG1)
	{
		StringInfo subqueryString = makeStringInfo();

		pg_get_query_def(debugQuery, subqueryString);

		ereport(DEBUG1, (errmsg("generating subplan " UINT64_FORMAT "_%u for "
																	"subquery %s",
								planId, subPlanId, subqueryString->data)));
	}

	/* finally update the input subquery to point the result query */
	memcpy(subquery, resultQuery, sizeof(Query));
}


/*
 * CreateDistributedSubPlan creates a distributed subplan by recursively calling
 * the planner from the top, which may either generate a local plan or another
 * distributed plan, which can itself contain subplans.
 */
static DistributedSubPlan *
CreateDistributedSubPlan(uint32 subPlanId, Query *subPlanQuery)
{
	DistributedSubPlan *subPlan = NULL;
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

	subPlan = CitusMakeNode(DistributedSubPlan);
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
		query_tree_walker(query, CteReferenceListWalker, context, QTW_EXAMINE_RTES);
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
		bool found = false;
		int flags = 0;

		context->level += 1;
		found = query_tree_walker(query, ContainsReferencesToOuterQueryWalker,
								  context, flags);
		context->level -= 1;

		return found;
	}

	return expression_tree_walker(node, ContainsReferencesToOuterQueryWalker,
								  context);
}


/*
 * BuildSubPlanResultQuery returns a query of the form:
 *
 * SELECT
 *   <target list>
 * FROM
 *   read_intermediate_result('<planId>_<subPlanId>', '<copy format'>)
 *   AS res (<column definition list>);
 *
 * The target list and column definition list are derived from the given subquery
 * and columm name alias list.
 *
 * If any of the types in the target list cannot be used in the binary copy format,
 * then the copy format 'text' is used, otherwise 'binary' is used.
 */
static Query *
BuildSubPlanResultQuery(Query *subquery, List *columnAliasList, uint64 planId,
						uint32 subPlanId)
{
	Query *resultQuery = NULL;
	char *resultIdString = NULL;
	Const *resultIdConst = NULL;
	Const *resultFormatConst = NULL;
	FuncExpr *funcExpr = NULL;
	Alias *funcAlias = NULL;
	List *funcColNames = NIL;
	List *funcColTypes = NIL;
	List *funcColTypMods = NIL;
	List *funcColCollations = NIL;
	RangeTblFunction *rangeTableFunction = NULL;
	RangeTblEntry *rangeTableEntry = NULL;
	RangeTblRef *rangeTableRef = NULL;
	FromExpr *joinTree = NULL;
	ListCell *targetEntryCell = NULL;
	List *targetList = NIL;
	int columnNumber = 1;
	bool useBinaryCopyFormat = true;
	Oid copyFormatId = BinaryCopyFormatId();
	int columnAliasCount = list_length(columnAliasList);

	/* build the target list and column definition list */
	foreach(targetEntryCell, subquery->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *targetExpr = (Node *) targetEntry->expr;
		char *columnName = targetEntry->resname;
		Oid columnType = exprType(targetExpr);
		Oid columnTypMod = exprTypmod(targetExpr);
		Oid columnCollation = exprCollation(targetExpr);
		Var *functionColumnVar = NULL;
		TargetEntry *newTargetEntry = NULL;

		if (targetEntry->resjunk)
		{
			continue;
		}

		funcColNames = lappend(funcColNames, makeString(columnName));
		funcColTypes = lappend_int(funcColTypes, columnType);
		funcColTypMods = lappend_int(funcColTypMods, columnTypMod);
		funcColCollations = lappend_int(funcColCollations, columnCollation);

		functionColumnVar = makeNode(Var);
		functionColumnVar->varno = 1;
		functionColumnVar->varattno = columnNumber;
		functionColumnVar->vartype = columnType;
		functionColumnVar->vartypmod = columnTypMod;
		functionColumnVar->varcollid = columnCollation;
		functionColumnVar->varlevelsup = 0;
		functionColumnVar->varnoold = 1;
		functionColumnVar->varoattno = columnNumber;
		functionColumnVar->location = -1;

		newTargetEntry = makeNode(TargetEntry);
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

		if (useBinaryCopyFormat && !CanUseBinaryCopyFormatForType(columnType))
		{
			useBinaryCopyFormat = false;
		}

		columnNumber++;
	}

	/* build the result_id parameter for the call to read_intermediate_result */
	resultIdString = GenerateResultId(planId, subPlanId);

	resultIdConst = makeNode(Const);
	resultIdConst->consttype = TEXTOID;
	resultIdConst->consttypmod = -1;
	resultIdConst->constlen = -1;
	resultIdConst->constvalue = CStringGetTextDatum(resultIdString);
	resultIdConst->constbyval = false;
	resultIdConst->constisnull = false;
	resultIdConst->location = -1;

	/* build the citus_copy_format parameter for the call to read_intermediate_result */
	if (!useBinaryCopyFormat)
	{
		copyFormatId = TextCopyFormatId();
	}

	resultFormatConst = makeNode(Const);
	resultFormatConst->consttype = CitusCopyFormatTypeId();
	resultFormatConst->consttypmod = -1;
	resultFormatConst->constlen = 4;
	resultFormatConst->constvalue = ObjectIdGetDatum(copyFormatId);
	resultFormatConst->constbyval = true;
	resultFormatConst->constisnull = false;
	resultFormatConst->location = -1;

	/* build the call to read_intermediate_result */
	funcExpr = makeNode(FuncExpr);
	funcExpr->funcid = CitusReadIntermediateResultFuncId();
	funcExpr->funcretset = true;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = 0;
	funcExpr->funccollid = 0;
	funcExpr->inputcollid = 0;
	funcExpr->location = -1;
	funcExpr->args = list_make2(resultIdConst, resultFormatConst);

	/* build the RTE for the call to read_intermediate_result */
	rangeTableFunction = makeNode(RangeTblFunction);
	rangeTableFunction->funccolcount = list_length(funcColNames);
	rangeTableFunction->funccolnames = funcColNames;
	rangeTableFunction->funccoltypes = funcColTypes;
	rangeTableFunction->funccoltypmods = funcColTypMods;
	rangeTableFunction->funccolcollations = funcColCollations;
	rangeTableFunction->funcparams = NULL;
	rangeTableFunction->funcexpr = (Node *) funcExpr;

	funcAlias = makeNode(Alias);
	funcAlias->aliasname = "intermediate_result";
	funcAlias->colnames = funcColNames;

	rangeTableEntry = makeNode(RangeTblEntry);
	rangeTableEntry->rtekind = RTE_FUNCTION;
	rangeTableEntry->functions = list_make1(rangeTableFunction);
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->eref = funcAlias;

	/* build the join tree using the read_intermediate_result RTE */
	rangeTableRef = makeNode(RangeTblRef);
	rangeTableRef->rtindex = 1;

	joinTree = makeNode(FromExpr);
	joinTree->fromlist = list_make1(rangeTableRef);

	/* build the SELECT query */
	resultQuery = makeNode(Query);
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
