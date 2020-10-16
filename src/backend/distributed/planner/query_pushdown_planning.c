/*-------------------------------------------------------------------------
 *
 * query_pushdown_planning.c
 *
 * Routines for creating pushdown plans for queries. Both select and modify
 * queries can be planned using query pushdown logic passing the checks given
 * in this file.
 *
 * Checks are controlled to understand whether the query can be sent to worker
 * nodes by simply adding shard_id to table names and getting the correct result
 * from them. That means, all the required data is present on the workers.
 *
 * For select queries, Citus try to use query pushdown planner if it has a
 * subquery or function RTEs. For modify queries, Citus try to use query pushdown
 * planner if the query accesses multiple tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/query_utils.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/version_compat.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"


/*
 * RecurringTuplesType is used to distinguish different types of expressions
 * that always produce the same set of tuples when a shard is queried. We make
 * this distinction to produce relevant error messages when recurring tuples
 * are used in a way that would give incorrect results.
 */
typedef enum RecurringTuplesType
{
	RECURRING_TUPLES_INVALID = 0,
	RECURRING_TUPLES_REFERENCE_TABLE,
	RECURRING_TUPLES_FUNCTION,
	RECURRING_TUPLES_EMPTY_JOIN_TREE,
	RECURRING_TUPLES_RESULT_FUNCTION
} RecurringTuplesType;


/* Config variable managed via guc.c */
bool SubqueryPushdown = false; /* is subquery pushdown enabled */


/* Local functions forward declarations */
static bool JoinTreeContainsSubqueryWalker(Node *joinTreeNode, void *context);
static bool IsFunctionRTE(Node *node);
static bool IsOuterJoinExpr(Node *node);
static bool WindowPartitionOnDistributionColumn(Query *query);
static DeferredErrorMessage * DeferErrorIfFromClauseRecurs(Query *queryTree);
static RecurringTuplesType FromClauseRecurringTupleType(Query *queryTree);
static DeferredErrorMessage * DeferredErrorIfUnsupportedRecurringTuplesJoin(
	PlannerRestrictionContext *plannerRestrictionContext);
static DeferredErrorMessage * DeferErrorIfUnsupportedTableCombination(Query *queryTree);
static bool ExtractSetOperationStatmentWalker(Node *node, List **setOperationList);
static RecurringTuplesType FetchFirstRecurType(PlannerInfo *plannerInfo,
											   Relids relids);
static bool ContainsRecurringRTE(RangeTblEntry *rangeTableEntry,
								 RecurringTuplesType *recurType);
static bool ContainsRecurringRangeTable(List *rangeTable, RecurringTuplesType *recurType);
static bool HasRecurringTuples(Node *node, RecurringTuplesType *recurType);
static MultiNode * SubqueryPushdownMultiNodeTree(Query *queryTree);
static void UpdateVarMappingsForExtendedOpNode(List *columnList,
											   List *subqueryTargetEntryList);
static void UpdateColumnToMatchingTargetEntry(Var *column,
											  List *targetEntryList);
static MultiTable * MultiSubqueryPushdownTable(Query *subquery);
static List * CreateSubqueryTargetEntryList(List *columnList);
static bool RelationInfoContainsOnlyRecurringTuples(PlannerInfo *plannerInfo,
													Relids relids);

/*
 * ShouldUseSubqueryPushDown determines whether it's desirable to use
 * subquery pushdown to plan the query based on the original and
 * rewritten query.
 */
bool
ShouldUseSubqueryPushDown(Query *originalQuery, Query *rewrittenQuery,
						  PlannerRestrictionContext *plannerRestrictionContext)
{
	/*
	 * We check the existence of subqueries in FROM clause on the modified query
	 * given that if postgres already flattened the subqueries, MultiNodeTree()
	 * can plan corresponding distributed plan.
	 */
	if (JoinTreeContainsSubquery(rewrittenQuery))
	{
		return true;
	}

	/*
	 * We check the existence of subqueries in WHERE and HAVING clause on the
	 * modified query. In some cases subqueries in the original query are
	 * converted into inner joins and in those cases MultiNodeTree() can plan
	 * the rewritten plan.
	 */
	if (WhereOrHavingClauseContainsSubquery(rewrittenQuery))
	{
		return true;
	}

	/*
	 * We check if postgres planned any semi joins, MultiNodeTree doesn't
	 * support these so we fail. Postgres is able to replace some IN/ANY
	 * subqueries with semi joins and then replace those with inner joins (ones
	 * where the subquery returns unique results). This allows MultiNodeTree to
	 * execute these subqueries (because they are converted to inner joins).
	 * However, even in that case the rewrittenQuery still contains join nodes
	 * with jointype JOIN_SEMI because Postgres doesn't actually update these.
	 * The way we find out instead if it actually planned semi joins, is by
	 * checking the joins that were sent to multi_join_restriction_hook. If no
	 * joins of type JOIN_SEMI are sent it is safe to convert all JOIN_SEMI
	 * nodes to JOIN_INNER nodes (which is what is done in MultiNodeTree).
	 */
	JoinRestrictionContext *joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;
	if (joinRestrictionContext->hasSemiJoin)
	{
		return true;
	}


	/*
	 * We process function RTEs as subqueries, since the join order planner
	 * does not know how to handle them.
	 */
	if (FindNodeMatchingCheckFunction((Node *) originalQuery, IsFunctionRTE))
	{
		return true;
	}

	/*
	 * We handle outer joins as subqueries, since the join order planner
	 * does not know how to handle them.
	 */
	if (FindNodeMatchingCheckFunction((Node *) originalQuery->jointree, IsOuterJoinExpr))
	{
		return true;
	}

	/*
	 * Original query may not have an outer join while rewritten query does.
	 * We should push down in this case.
	 * An example of this is https://github.com/citusdata/citus/issues/2739
	 * where postgres pulls-up the outer-join in the subquery.
	 */
	if (FindNodeMatchingCheckFunction((Node *) rewrittenQuery->jointree, IsOuterJoinExpr))
	{
		return true;
	}

	/*
	 * Some unsupported join clauses in logical planner
	 * may be supported by subquery pushdown planner.
	 */
	List *qualifierList = QualifierList(rewrittenQuery->jointree);
	if (DeferErrorIfUnsupportedClause(qualifierList) != NULL)
	{
		return true;
	}

	/* check if the query has a window function and it is safe to pushdown */
	if (originalQuery->hasWindowFuncs &&
		SafeToPushdownWindowFunction(originalQuery, NULL))
	{
		return true;
	}

	return false;
}


/*
 * JoinTreeContainsSubquery returns true if the input query contains any subqueries
 * in the join tree (e.g., FROM clause).
 */
bool
JoinTreeContainsSubquery(Query *query)
{
	FromExpr *joinTree = query->jointree;

	if (!joinTree)
	{
		return false;
	}

	return JoinTreeContainsSubqueryWalker((Node *) joinTree, query);
}


/*
 * HasEmptyJoinTree returns whether the query selects from anything.
 */
bool
HasEmptyJoinTree(Query *query)
{
	if (query->rtable == NIL)
	{
		return true;
	}

#if PG_VERSION_NUM >= PG_VERSION_12
	else if (list_length(query->rtable) == 1)
	{
		RangeTblEntry *rte = (RangeTblEntry *) linitial(query->rtable);
		if (rte->rtekind == RTE_RESULT)
		{
			return true;
		}
	}
#endif

	return false;
}


/*
 * JoinTreeContainsSubqueryWalker returns true if the input joinTreeNode
 * references to a subquery. Otherwise, recurses into the expression.
 */
static bool
JoinTreeContainsSubqueryWalker(Node *joinTreeNode, void *context)
{
	if (joinTreeNode == NULL)
	{
		return false;
	}

	if (IsA(joinTreeNode, RangeTblRef))
	{
		Query *query = (Query *) context;

		RangeTblRef *rangeTableRef = (RangeTblRef *) joinTreeNode;
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableRef->rtindex, query->rtable);

		if (rangeTableEntry->rtekind == RTE_SUBQUERY)
		{
			return true;
		}

		return false;
	}

	return expression_tree_walker(joinTreeNode, JoinTreeContainsSubqueryWalker, context);
}


/*
 * WhereOrHavingClauseContainsSubquery returns true if the input query contains
 * any subqueries in the WHERE or HAVING clause.
 */
bool
WhereOrHavingClauseContainsSubquery(Query *query)
{
	if (FindNodeMatchingCheckFunction(query->havingQual, IsNodeSubquery))
	{
		return true;
	}

	if (!query->jointree)
	{
		return false;
	}

	/*
	 * We search the whole jointree here, not just the quals. The reason for
	 * this is that the fromlist can contain other FromExpr nodes again or
	 * JoinExpr nodes that also have quals. If that's the case we need to check
	 * those as well if they contain andy subqueries.
	 */
	return FindNodeMatchingCheckFunction((Node *) query->jointree, IsNodeSubquery);
}


/*
 * TargetList returns true if the input query contains
 * any subqueries in the WHERE clause.
 */
bool
TargetListContainsSubquery(Query *query)
{
	return FindNodeMatchingCheckFunction((Node *) query->targetList, IsNodeSubquery);
}


/*
 * IsFunctionRTE determines whether the given node is a function RTE.
 */
static bool
IsFunctionRTE(Node *node)
{
	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTblEntry = (RangeTblEntry *) node;

		if (rangeTblEntry->rtekind == RTE_FUNCTION)
		{
			return true;
		}
	}

	return false;
}


/*
 * IsNodeSubquery returns true if the given node is a Query or SubPlan or a
 * Param node with paramkind PARAM_EXEC.
 *
 * The check for SubPlan is needed when this is used on a already rewritten
 * query. Such a query has SubPlan nodes instead of SubLink nodes (which
 * contain a Query node).
 * The check for PARAM_EXEC is needed because some very simple subqueries like
 * (select 1) are converted to init plans in the rewritten query. In this case
 * the only thing left in the query tree is a Param node with type PARAM_EXEC.
 */
bool
IsNodeSubquery(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query) || IsA(node, SubPlan))
	{
		return true;
	}

	if (!IsA(node, Param))
	{
		return false;
	}
	return ((Param *) node)->paramkind == PARAM_EXEC;
}


/*
 * IsOuterJoinExpr returns whether the given node is an outer join expression.
 */
static bool
IsOuterJoinExpr(Node *node)
{
	bool isOuterJoin = false;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) node;
		JoinType joinType = joinExpr->jointype;
		if (IS_OUTER_JOIN(joinType))
		{
			isOuterJoin = true;
		}
	}

	return isOuterJoin;
}


/*
 * SafeToPushdownWindowFunction checks if the query with window function is supported.
 * Returns the result accordingly and modifies errorDetail if non null.
 */
bool
SafeToPushdownWindowFunction(Query *query, StringInfo *errorDetail)
{
	ListCell *windowClauseCell = NULL;
	List *windowClauseList = query->windowClause;

	/*
	 * We need to check each window clause separately if there is a partition by clause
	 * and if it is partitioned on the distribution column.
	 */
	foreach(windowClauseCell, windowClauseList)
	{
		WindowClause *windowClause = lfirst(windowClauseCell);

		if (!windowClause->partitionClause)
		{
			if (errorDetail)
			{
				*errorDetail = makeStringInfo();
				appendStringInfoString(*errorDetail,
									   "Window functions without PARTITION BY on distribution "
									   "column is currently unsupported");
			}
			return false;
		}
	}

	if (!WindowPartitionOnDistributionColumn(query))
	{
		if (errorDetail)
		{
			*errorDetail = makeStringInfo();
			appendStringInfoString(*errorDetail,
								   "Window functions with PARTITION BY list missing distribution "
								   "column is currently unsupported");
		}
		return false;
	}

	return true;
}


/*
 * WindowPartitionOnDistributionColumn checks if the given subquery has one
 * or more window functions and at least one of them is not partitioned by
 * distribution column. The function returns false if your window function does not
 * have a partition by clause or it does not include the distribution column.
 *
 * Please note that if the query does not have a window function, the function
 * returns true.
 */
static bool
WindowPartitionOnDistributionColumn(Query *query)
{
	List *windowClauseList = query->windowClause;
	ListCell *windowClauseCell = NULL;

	foreach(windowClauseCell, windowClauseList)
	{
		WindowClause *windowClause = lfirst(windowClauseCell);
		List *partitionClauseList = windowClause->partitionClause;
		List *targetEntryList = query->targetList;

		List *groupTargetEntryList =
			GroupTargetEntryList(partitionClauseList, targetEntryList);

		bool partitionOnDistributionColumn =
			TargetListOnPartitionColumn(query, groupTargetEntryList);

		if (!partitionOnDistributionColumn)
		{
			return false;
		}
	}

	return true;
}


/*
 * SubqueryMultiNodeTree gets the query objects and returns logical plan
 * for subqueries.
 *
 * We currently have two different code paths for creating logic plan for subqueries:
 *   (i) subquery pushdown
 *   (ii) single relation repartition subquery
 *
 * In order to create the logical plan, we follow the algorithm below:
 *    -  If subquery pushdown planner can plan the query
 *        -  We're done, we create the multi plan tree and return
 *    -  Else
 *       - If the query is not eligible for single table repartition subquery planning
 *            - Throw the error that the subquery pushdown planner generated
 *       - If it is eligible for single table repartition subquery planning
 *            - Check for the errors for single table repartition subquery planning
 *                - If no errors found, we're done. Create the multi plan and return
 *                - If found errors, throw it
 */
MultiNode *
SubqueryMultiNodeTree(Query *originalQuery, Query *queryTree,
					  PlannerRestrictionContext *plannerRestrictionContext)
{
	MultiNode *multiQueryNode = NULL;

	/*
	 * This is a generic error check that applies to both subquery pushdown
	 * and single table repartition subquery.
	 */
	DeferredErrorMessage *unsupportedQueryError = DeferErrorIfQueryNotSupported(
		originalQuery);
	if (unsupportedQueryError != NULL)
	{
		RaiseDeferredError(unsupportedQueryError, ERROR);
	}

	/*
	 * In principle, we're first trying subquery pushdown planner. If it fails
	 * to create a logical plan, continue with trying the single table
	 * repartition subquery planning.
	 */
	DeferredErrorMessage *subqueryPushdownError = DeferErrorIfUnsupportedSubqueryPushdown(
		originalQuery,
		plannerRestrictionContext);
	if (!subqueryPushdownError)
	{
		multiQueryNode = SubqueryPushdownMultiNodeTree(originalQuery);
	}
	else if (subqueryPushdownError)
	{
		RaiseDeferredErrorInternal(subqueryPushdownError, ERROR);

		List *subqueryEntryList = SubqueryEntryList(queryTree);
		RangeTblEntry *subqueryRangeTableEntry = (RangeTblEntry *) linitial(
			subqueryEntryList);
		Assert(subqueryRangeTableEntry->rtekind == RTE_SUBQUERY);

		Query *subqueryTree = subqueryRangeTableEntry->subquery;

		DeferredErrorMessage *repartitionQueryError =
			DeferErrorIfUnsupportedSubqueryRepartition(subqueryTree);
		if (repartitionQueryError)
		{
			RaiseDeferredErrorInternal(repartitionQueryError, ERROR);
		}

		/* all checks have passed, safe to create the multi plan */
		multiQueryNode = MultiNodeTree(queryTree);
	}

	Assert(multiQueryNode != NULL);

	return multiQueryNode;
}


/*
 * DeferErrorIfContainsUnsupportedSubqueryPushdown iterates on the query's subquery
 * entry list and uses helper functions to check if we can push down subquery
 * to worker nodes. These helper functions returns a deferred error if we
 * cannot push down the subquery.
 */
DeferredErrorMessage *
DeferErrorIfUnsupportedSubqueryPushdown(Query *originalQuery,
										PlannerRestrictionContext *
										plannerRestrictionContext)
{
	bool outerMostQueryHasLimit = false;
	ListCell *subqueryCell = NULL;
	List *subqueryList = NIL;

	if (originalQuery->limitCount != NULL)
	{
		outerMostQueryHasLimit = true;
	}

	/*
	 * We're checking two things here:
	 *    (i)   If the query contains a top level union, ensure that all leaves
	 *          return the partition key at the same position
	 *    (ii)  Else, check whether all relations joined on the partition key or not
	 */
	if (ContainsUnionSubquery(originalQuery))
	{
		if (!SafeToPushdownUnionSubquery(plannerRestrictionContext))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot pushdown the subquery since not all subqueries "
								 "in the UNION have the partition column in the same "
								 "position",
								 "Each leaf query of the UNION should return the "
								 "partition column in the same position and all joins "
								 "must be on the partition column",
								 NULL);
		}
	}
	else if (!RestrictionEquivalenceForPartitionKeys(plannerRestrictionContext))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "complex joins are only supported when all distributed tables are "
							 "co-located and joined on their distribution columns",
							 NULL, NULL);
	}

	/* we shouldn't allow reference tables in the FROM clause when the query has sublinks */
	DeferredErrorMessage *error = DeferErrorIfFromClauseRecurs(originalQuery);
	if (error)
	{
		return error;
	}

	/* we shouldn't allow reference tables in the outer part of outer joins */
	error = DeferredErrorIfUnsupportedRecurringTuplesJoin(plannerRestrictionContext);
	if (error)
	{
		return error;
	}

	/*
	 * We first extract all the queries that appear in the original query. Later,
	 * we delete the original query given that error rules does not apply to the
	 * top level query. For instance, we could support any LIMIT/ORDER BY on the
	 * top level query.
	 */
	ExtractQueryWalker((Node *) originalQuery, &subqueryList);
	subqueryList = list_delete(subqueryList, originalQuery);

	/* iterate on the subquery list and error out accordingly */
	foreach(subqueryCell, subqueryList)
	{
		Query *subquery = lfirst(subqueryCell);
		error = DeferErrorIfCannotPushdownSubquery(subquery,
												   outerMostQueryHasLimit);
		if (error)
		{
			return error;
		}
	}

	return NULL;
}


/*
 * DeferErrorIfFromClauseRecurs returns a deferred error if the
 * given query is not suitable for subquery pushdown.
 *
 * While planning sublinks, we rely on Postgres in the sense that it converts some of
 * sublinks into joins.
 *
 * In some cases, sublinks are pulled up and converted into outer joins. Those cases
 * are already handled with DeferredErrorIfUnsupportedRecurringTuplesJoin().
 *
 * If the sublinks are not pulled up, we should still error out in if the expression
 * in the FROM clause would recur for every shard in a subquery on the WHERE clause.
 *
 * Otherwise, the result would include duplicate rows.
 */
static DeferredErrorMessage *
DeferErrorIfFromClauseRecurs(Query *queryTree)
{
	if (!queryTree->hasSubLinks)
	{
		return NULL;
	}

	RecurringTuplesType recurType = FromClauseRecurringTupleType(queryTree);
	if (recurType == RECURRING_TUPLES_REFERENCE_TABLE)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "Reference tables are not allowed in FROM "
							 "clause when the query has subqueries in "
							 "WHERE clause and it references a column "
							 "from another query", NULL);
	}
	else if (recurType == RECURRING_TUPLES_FUNCTION)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "Functions are not allowed in FROM "
							 "clause when the query has subqueries in "
							 "WHERE clause and it references a column "
							 "from another query", NULL);
	}
	else if (recurType == RECURRING_TUPLES_RESULT_FUNCTION)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "Complex subqueries and CTEs are not allowed in "
							 "the FROM clause when the query has subqueries in the "
							 "WHERE clause and it references a column "
							 "from another query", NULL);
	}
	else if (recurType == RECURRING_TUPLES_EMPTY_JOIN_TREE)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "Subqueries without FROM are not allowed in FROM "
							 "clause when the outer query has subqueries in "
							 "WHERE clause and it references a column "
							 "from another query", NULL);
	}

	/*
	 * We get here when there is neither a distributed table, nor recurring tuples.
	 * That usually means that there isn't a FROM at all (only sublinks), this
	 * implies that queryTree is recurring, but whether this is a problem depends
	 * on outer queries, not on queryTree itself.
	 */

	return NULL;
}


/*
 * FromClauseRecurringTupleType returns tuple recurrence information
 * in query result based on range table entries in from clause.
 *
 * Returned information is used to prepare appropriate deferred error
 * message for subquery pushdown checks.
 */
static RecurringTuplesType
FromClauseRecurringTupleType(Query *queryTree)
{
	RecurringTuplesType recurType = RECURRING_TUPLES_INVALID;

	if (HasEmptyJoinTree(queryTree))
	{
		return RECURRING_TUPLES_EMPTY_JOIN_TREE;
	}

	if (FindNodeMatchingCheckFunctionInRangeTableList(queryTree->rtable,
													  IsDistributedTableRTE))
	{
		/*
		 * There is a distributed table somewhere in the FROM clause.
		 *
		 * In the typical case this means that the query does not recur,
		 * but there are two exceptions:
		 *
		 * - outer joins such as reference_table LEFT JOIN distributed_table
		 * - FROM reference_table WHERE .. (SELECT .. FROM distributed_table) ..
		 *
		 * However, we check all subqueries and joins separately, so we would
		 * find such conditions in other calls.
		 */
		return RECURRING_TUPLES_INVALID;
	}

	/*
	 * Try to figure out which type of recurring tuples we have to produce a
	 * relevant error message. If there are several we'll pick the first one.
	 */
	ContainsRecurringRangeTable(queryTree->rtable, &recurType);

	return recurType;
}


/*
 * DeferredErrorIfUnsupportedRecurringTuplesJoin returns true if there exists a outer join
 * between reference table and distributed tables which does not follow
 * the rules :
 * - Reference tables can not be located in the outer part of the semi join or the
 * anti join. Otherwise, we may have duplicate results. Although getting duplicate
 * results is not possible by checking the equality on the column of the reference
 * table and partition column of distributed table, we still keep these checks.
 * Because, using the reference table in the outer part of the semi join or anti
 * join is not very common.
 * - Reference tables can not be located in the outer part of the left join
 * (Note that PostgreSQL converts right joins to left joins. While converting
 * join types, innerrel and outerrel are also switched.) Otherwise we will
 * definitely have duplicate rows. Beside, reference tables can not be used
 * with full outer joins because of the same reason.
 */
static DeferredErrorMessage *
DeferredErrorIfUnsupportedRecurringTuplesJoin(
	PlannerRestrictionContext *plannerRestrictionContext)
{
	List *joinRestrictionList =
		plannerRestrictionContext->joinRestrictionContext->joinRestrictionList;
	ListCell *joinRestrictionCell = NULL;
	RecurringTuplesType recurType = RECURRING_TUPLES_INVALID;
	foreach(joinRestrictionCell, joinRestrictionList)
	{
		JoinRestriction *joinRestriction = (JoinRestriction *) lfirst(
			joinRestrictionCell);
		JoinType joinType = joinRestriction->joinType;
		PlannerInfo *plannerInfo = joinRestriction->plannerInfo;
		Relids innerrelRelids = joinRestriction->innerrelRelids;
		Relids outerrelRelids = joinRestriction->outerrelRelids;

		if (joinType == JOIN_SEMI || joinType == JOIN_ANTI || joinType == JOIN_LEFT)
		{
			/*
			 * If there are only recurring tuples on the inner side of a join then
			 * we can push it down, regardless of whether the outer side is
			 * recurring or not. Otherwise, we check the outer side for recurring
			 * tuples.
			 */
			if (RelationInfoContainsOnlyRecurringTuples(plannerInfo, innerrelRelids))
			{
				continue;
			}


			/*
			 * If the outer side of the join doesn't have any distributed tables
			 * (e.g., contains only recurring tuples), Citus should not pushdown
			 * the query. The reason is that recurring tuples on every shard would
			 * be added to the result, which is wrong.
			 */
			if (RelationInfoContainsOnlyRecurringTuples(plannerInfo, outerrelRelids))
			{
				/*
				 * Find the first (or only) recurring RTE to give a meaningful
				 * error to the user.
				 */
				recurType = FetchFirstRecurType(plannerInfo, outerrelRelids);

				break;
			}
		}
		else if (joinType == JOIN_FULL)
		{
			if (RelationInfoContainsOnlyRecurringTuples(plannerInfo, innerrelRelids))
			{
				/*
				 * Find the first (or only) recurring RTE to give a meaningful
				 * error to the user.
				 */
				recurType = FetchFirstRecurType(plannerInfo, innerrelRelids);

				break;
			}

			if (RelationInfoContainsOnlyRecurringTuples(plannerInfo, outerrelRelids))
			{
				/*
				 * Find the first (or only) recurring RTE to give a meaningful
				 * error to the user.
				 */
				recurType = FetchFirstRecurType(plannerInfo, outerrelRelids);

				break;
			}
		}
	}

	if (recurType == RECURRING_TUPLES_REFERENCE_TABLE)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "There exist a reference table in the outer "
							 "part of the outer join", NULL);
	}
	else if (recurType == RECURRING_TUPLES_FUNCTION)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "There exist a table function in the outer "
							 "part of the outer join", NULL);
	}
	else if (recurType == RECURRING_TUPLES_EMPTY_JOIN_TREE)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "There exist a subquery without FROM in the outer "
							 "part of the outer join", NULL);
	}
	else if (recurType == RECURRING_TUPLES_RESULT_FUNCTION)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery",
							 "Complex subqueries and CTEs cannot be in the outer "
							 "part of the outer join", NULL);
	}

	return NULL;
}


/*
 * CanPushdownSubquery checks if we can push down the given
 * subquery to worker nodes.
 */
bool
CanPushdownSubquery(Query *subqueryTree, bool outerMostQueryHasLimit)
{
	return DeferErrorIfCannotPushdownSubquery(subqueryTree, outerMostQueryHasLimit) ==
		   NULL;
}


/*
 * DeferErrorIfCannotPushdownSubquery checks if we can push down the given
 * subquery to worker nodes. If we cannot push down the subquery, this function
 * returns a deferred error.
 *
 * We can push down a subquery if it follows rules below:
 * a. If there is an aggregate, it must be grouped on partition column.
 * b. If there is a join, it must be between two regular tables or two subqueries.
 * We don't support join between a regular table and a subquery. And columns on
 * the join condition must be partition columns.
 * c. If there is a distinct clause, it must be on the partition column.
 *
 * This function is very similar to DeferErrorIfQueryNotSupported() in logical
 * planner, but we don't reuse it, because differently for subqueries we support
 * a subset of distinct, union and left joins.
 *
 * Note that this list of checks is not exhaustive, there can be some cases
 * which we let subquery to run but returned results would be wrong. Such as if
 * a subquery has a group by on another subquery which includes order by with
 * limit, we let this query to run, but results could be wrong depending on the
 * features of underlying tables.
 */
DeferredErrorMessage *
DeferErrorIfCannotPushdownSubquery(Query *subqueryTree, bool outerMostQueryHasLimit)
{
	bool preconditionsSatisfied = true;
	char *errorDetail = NULL;
	StringInfo errorInfo = NULL;

	DeferredErrorMessage *deferredError = DeferErrorIfUnsupportedTableCombination(
		subqueryTree);
	if (deferredError)
	{
		return deferredError;
	}

	if (HasEmptyJoinTree(subqueryTree) &&
		contain_mutable_functions((Node *) subqueryTree->targetList))
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries without a FROM clause can only contain immutable "
					  "functions";
	}

	if (subqueryTree->limitOffset)
	{
		preconditionsSatisfied = false;
		errorDetail = "Offset clause is currently unsupported when a subquery "
					  "references a column from another query";
	}

	/* limit is not supported when SubqueryPushdown is not set */
	if (subqueryTree->limitCount && !SubqueryPushdown)
	{
		preconditionsSatisfied = false;
		errorDetail = "Limit in subquery is currently unsupported when a "
					  "subquery references a column from another query";
	}

	/*
	 * Limit is partially supported when SubqueryPushdown is set.
	 * The outermost query must have a limit clause.
	 */
	if (subqueryTree->limitCount && SubqueryPushdown && !outerMostQueryHasLimit)
	{
		preconditionsSatisfied = false;
		errorDetail = "Limit in subquery without limit in the outermost query is "
					  "unsupported";
	}

	if (subqueryTree->setOperations)
	{
		deferredError = DeferErrorIfUnsupportedUnionQuery(subqueryTree);
		if (deferredError)
		{
			return deferredError;
		}
	}

	if (subqueryTree->hasRecursive)
	{
		preconditionsSatisfied = false;
		errorDetail = "Recursive queries are currently unsupported";
	}

	if (subqueryTree->cteList)
	{
		preconditionsSatisfied = false;
		errorDetail = "Common Table Expressions are currently unsupported";
	}

	if (subqueryTree->hasForUpdate)
	{
		preconditionsSatisfied = false;
		errorDetail = "For Update/Share commands are currently unsupported";
	}

	/* group clause list must include partition column */
	if (subqueryTree->groupClause)
	{
		List *groupClauseList = subqueryTree->groupClause;
		List *targetEntryList = subqueryTree->targetList;
		List *groupTargetEntryList = GroupTargetEntryList(groupClauseList,
														  targetEntryList);
		bool groupOnPartitionColumn = TargetListOnPartitionColumn(subqueryTree,
																  groupTargetEntryList);
		if (!groupOnPartitionColumn)
		{
			preconditionsSatisfied = false;
			errorDetail = "Group by list without partition column is currently "
						  "unsupported when a subquery references a column "
						  "from another query";
		}
	}

	/* grouping sets are not allowed in subqueries*/
	if (subqueryTree->groupingSets)
	{
		preconditionsSatisfied = false;
		errorDetail = "could not run distributed query with GROUPING SETS, CUBE, "
					  "or ROLLUP";
	}

	/*
	 * We support window functions when the window function
	 * is partitioned on distribution column.
	 */
	if (subqueryTree->hasWindowFuncs && !SafeToPushdownWindowFunction(subqueryTree,
																	  &errorInfo))
	{
		errorDetail = (char *) errorInfo->data;
		preconditionsSatisfied = false;
	}

	/* we don't support aggregates without group by */
	if (subqueryTree->hasAggs && (subqueryTree->groupClause == NULL))
	{
		preconditionsSatisfied = false;
		errorDetail = "Aggregates without group by are currently unsupported "
					  "when a subquery references a column from another query";
	}

	/* having clause without group by on partition column is not supported */
	if (subqueryTree->havingQual && (subqueryTree->groupClause == NULL))
	{
		preconditionsSatisfied = false;
		errorDetail = "Having qual without group by on partition column is "
					  "currently unsupported when a subquery references "
					  "a column from another query";
	}

	/* distinct clause list must include partition column */
	if (subqueryTree->distinctClause)
	{
		List *distinctClauseList = subqueryTree->distinctClause;
		List *targetEntryList = subqueryTree->targetList;
		List *distinctTargetEntryList = GroupTargetEntryList(distinctClauseList,
															 targetEntryList);
		bool distinctOnPartitionColumn =
			TargetListOnPartitionColumn(subqueryTree, distinctTargetEntryList);
		if (!distinctOnPartitionColumn)
		{
			preconditionsSatisfied = false;
			errorDetail = "Distinct on columns without partition column is "
						  "currently unsupported";
		}
	}

	deferredError = DeferErrorIfFromClauseRecurs(subqueryTree);
	if (deferredError)
	{
		preconditionsSatisfied = false;
		errorDetail = (char *) deferredError->detail;
	}


	/* finally check and return deferred if not satisfied */
	if (!preconditionsSatisfied)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot push down this subquery",
							 errorDetail, NULL);
	}

	return NULL;
}


/*
 * DeferErrorIfUnsupportedTableCombination checks if the given query tree contains any
 * unsupported range table combinations. For this, the function walks over all
 * range tables in the join tree, and checks if they correspond to simple relations
 * or subqueries. It also checks if there is a join between a regular table and
 * a subquery and if join is on more than two range table entries. If any error is found,
 * a deferred error is returned. Else, NULL is returned.
 */
static DeferredErrorMessage *
DeferErrorIfUnsupportedTableCombination(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	List *joinTreeTableIndexList = NIL;
	int joinTreeTableIndex = 0;
	bool unsupportedTableCombination = false;
	char *errorDetail = NULL;

	/*
	 * Extract all range table indexes from the join tree. Note that sub-queries
	 * that get pulled up by PostgreSQL don't appear in this join tree.
	 */
	ExtractRangeTableIndexWalker((Node *) queryTree->jointree,
								 &joinTreeTableIndexList);

	foreach_int(joinTreeTableIndex, joinTreeTableIndexList)
	{
		/*
		 * Join tree's range table index starts from 1 in the query tree. But,
		 * list indexes start from 0.
		 */
		int rangeTableListIndex = joinTreeTableIndex - 1;

		RangeTblEntry *rangeTableEntry =
			(RangeTblEntry *) list_nth(rangeTableList, rangeTableListIndex);

		/*
		 * Check if the range table in the join tree is a simple relation, a
		 * subquery, or immutable function.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION ||
			rangeTableEntry->rtekind == RTE_SUBQUERY
#if PG_VERSION_NUM >= PG_VERSION_12
			|| rangeTableEntry->rtekind == RTE_RESULT
#endif
			)
		{
			/* accepted */
		}
		else if (rangeTableEntry->rtekind == RTE_FUNCTION)
		{
			List *functionList = rangeTableEntry->functions;

			if (list_length(functionList) == 1 &&
				ContainsReadIntermediateResultFunction(linitial(functionList)))
			{
				/*
				 * The read_intermediate_result function is volatile, but we know
				 * it has the same result across all nodes and can therefore treat
				 * it as a reference table.
				 */
			}
			else if (contain_mutable_functions((Node *) functionList))
			{
				unsupportedTableCombination = true;
				errorDetail = "Only immutable functions can be used as a table "
							  "expressions in a multi-shard query";
			}
			else
			{
				/* immutable function RTEs are treated as reference tables */
			}
		}
		else if (rangeTableEntry->rtekind == RTE_CTE)
		{
			unsupportedTableCombination = true;
			errorDetail = "CTEs in subqueries are currently unsupported";
			break;
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			unsupportedTableCombination = true;
			errorDetail = "VALUES in multi-shard queries is currently unsupported";
			break;
		}
		else
		{
			unsupportedTableCombination = true;
			errorDetail = "Table expressions other than relations, subqueries, "
						  "and immutable functions are currently unsupported";
			break;
		}
	}

	/* finally check and error out if not satisfied */
	if (unsupportedTableCombination)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot push down this subquery",
							 errorDetail, NULL);
	}

	return NULL;
}


/*
 * DeferErrorIfUnsupportedUnionQuery is a helper function for ErrorIfCannotPushdownSubquery().
 * The function also errors out for set operations INTERSECT and EXCEPT.
 */
DeferredErrorMessage *
DeferErrorIfUnsupportedUnionQuery(Query *subqueryTree)
{
	List *setOperationStatementList = NIL;
	ListCell *setOperationStatmentCell = NULL;
	RecurringTuplesType recurType = RECURRING_TUPLES_INVALID;

	ExtractSetOperationStatmentWalker((Node *) subqueryTree->setOperations,
									  &setOperationStatementList);
	foreach(setOperationStatmentCell, setOperationStatementList)
	{
		SetOperationStmt *setOperation =
			(SetOperationStmt *) lfirst(setOperationStatmentCell);
		Node *leftArg = setOperation->larg;
		Node *rightArg = setOperation->rarg;
		int leftArgRTI = 0;
		int rightArgRTI = 0;

		if (setOperation->op != SETOP_UNION)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot push down this subquery",
								 "Intersect and Except are currently unsupported",
								 NULL);
		}

		if (IsA(leftArg, RangeTblRef))
		{
			leftArgRTI = ((RangeTblRef *) leftArg)->rtindex;
			Query *leftArgSubquery = rt_fetch(leftArgRTI,
											  subqueryTree->rtable)->subquery;
			recurType = FromClauseRecurringTupleType(leftArgSubquery);
			if (recurType != RECURRING_TUPLES_INVALID)
			{
				break;
			}
		}

		if (IsA(rightArg, RangeTblRef))
		{
			rightArgRTI = ((RangeTblRef *) rightArg)->rtindex;
			Query *rightArgSubquery = rt_fetch(rightArgRTI,
											   subqueryTree->rtable)->subquery;
			recurType = FromClauseRecurringTupleType(rightArgSubquery);
			if (recurType != RECURRING_TUPLES_INVALID)
			{
				break;
			}
		}
	}

	if (recurType == RECURRING_TUPLES_REFERENCE_TABLE)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot push down this subquery",
							 "Reference tables are not supported with union operator",
							 NULL);
	}
	else if (recurType == RECURRING_TUPLES_FUNCTION)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot push down this subquery",
							 "Table functions are not supported with union operator",
							 NULL);
	}
	else if (recurType == RECURRING_TUPLES_EMPTY_JOIN_TREE)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot push down this subquery",
							 "Subqueries without a FROM clause are not supported with "
							 "union operator", NULL);
	}
	else if (recurType == RECURRING_TUPLES_RESULT_FUNCTION)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot push down this subquery",
							 "Complex subqueries and CTEs are not supported within a "
							 "UNION", NULL);
	}


	return NULL;
}


/*
 * ExtractSetOperationStatementWalker walks over a set operations statment,
 * and finds all set operations in the tree.
 */
static bool
ExtractSetOperationStatmentWalker(Node *node, List **setOperationList)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, SetOperationStmt))
	{
		SetOperationStmt *setOperation = (SetOperationStmt *) node;

		(*setOperationList) = lappend(*setOperationList, setOperation);
	}

	bool walkerResult = expression_tree_walker(node,
											   ExtractSetOperationStatmentWalker,
											   setOperationList);

	return walkerResult;
}


/*
 * RelationInfoContainsOnlyRecurringTuples returns false if any of the relations in
 * a RelOptInfo is not recurring.
 */
static bool
RelationInfoContainsOnlyRecurringTuples(PlannerInfo *plannerInfo, Relids relids)
{
	int relationId = -1;

	while ((relationId = bms_next_member(relids, relationId)) >= 0)
	{
		RangeTblEntry *rangeTableEntry = plannerInfo->simple_rte_array[relationId];

		if (FindNodeMatchingCheckFunctionInRangeTableList(list_make1(rangeTableEntry),
														  IsDistributedTableRTE))
		{
			/* we already found a distributed table, no need to check further */
			return false;
		}

		/*
		 * If there are no distributed tables, there should be at least
		 * one recurring rte.
		 */
		RecurringTuplesType recurType PG_USED_FOR_ASSERTS_ONLY;
		Assert(ContainsRecurringRTE(rangeTableEntry, &recurType));
	}

	return true;
}


/*
 * FetchFirstRecurType checks whether the relationInfo
 * contains any recurring table expression, namely a reference table,
 * or immutable function. If found, FetchFirstRecurType
 * returns true.
 *
 * Note that since relation ids of relationInfo indexes to the range
 * table entry list of planner info, planner info is also passed.
 */
static RecurringTuplesType
FetchFirstRecurType(PlannerInfo *plannerInfo, Relids relids)
{
	RecurringTuplesType recurType = RECURRING_TUPLES_INVALID;
	int relationId = -1;

	while ((relationId = bms_next_member(relids, relationId)) >= 0)
	{
		RangeTblEntry *rangeTableEntry = plannerInfo->simple_rte_array[relationId];

		/* relationInfo has this range table entry */
		if (ContainsRecurringRTE(rangeTableEntry, &recurType))
		{
			return recurType;
		}
	}

	return recurType;
}


/*
 * ContainsRecurringRTE returns whether the range table entry contains
 * any entry that generates the same set of tuples when repeating it in
 * a query on different shards.
 */
static bool
ContainsRecurringRTE(RangeTblEntry *rangeTableEntry, RecurringTuplesType *recurType)
{
	return ContainsRecurringRangeTable(list_make1(rangeTableEntry), recurType);
}


/*
 * ContainsRecurringRangeTable returns whether the range table list contains
 * any entry that generates the same set of tuples when repeating it in
 * a query on different shards.
 */
static bool
ContainsRecurringRangeTable(List *rangeTable, RecurringTuplesType *recurType)
{
	return range_table_walker(rangeTable, HasRecurringTuples, recurType,
							  QTW_EXAMINE_RTES_BEFORE);
}


/*
 * HasRecurringTuples returns whether any part of the expression will generate
 * the same set of tuples in every query on shards when executing a distributed
 * query.
 */
static bool
HasRecurringTuples(Node *node, RecurringTuplesType *recurType)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;

		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			Oid relationId = rangeTableEntry->relid;
			if (IsCitusTableType(relationId, REFERENCE_TABLE))
			{
				*recurType = RECURRING_TUPLES_REFERENCE_TABLE;

				/*
				 * Tuples from reference tables will recur in every query on shards
				 * that includes it.
				 */
				return true;
			}
		}
		else if (rangeTableEntry->rtekind == RTE_FUNCTION)
		{
			List *functionList = rangeTableEntry->functions;

			if (list_length(functionList) == 1 &&
				ContainsReadIntermediateResultFunction((Node *) functionList))
			{
				*recurType = RECURRING_TUPLES_RESULT_FUNCTION;
			}
			else
			{
				*recurType = RECURRING_TUPLES_FUNCTION;
			}

			/*
			 * Tuples from functions will recur in every query on shards that includes
			 * it.
			 */
			return true;
		}
#if PG_VERSION_NUM >= PG_VERSION_12
		else if (rangeTableEntry->rtekind == RTE_RESULT)
		{
			*recurType = RECURRING_TUPLES_EMPTY_JOIN_TREE;
			return true;
		}
#endif

		return false;
	}
	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		if (HasEmptyJoinTree(query))
		{
			*recurType = RECURRING_TUPLES_EMPTY_JOIN_TREE;

			/*
			 * Queries with empty join trees will recur in every query on shards
			 * that includes it.
			 */
			return true;
		}

		return query_tree_walker((Query *) node, HasRecurringTuples,
								 recurType, QTW_EXAMINE_RTES_BEFORE);
	}

	return expression_tree_walker(node, HasRecurringTuples, recurType);
}


/*
 * SubqueryPushdownMultiNodeTree creates logical plan for subquery pushdown logic.
 * Note that this logic will be changed in next iterations, so we decoupled it
 * from other parts of code although it causes some code duplication.
 *
 * Current subquery pushdown support in MultiTree logic requires a single range
 * table entry in the top most from clause. Therefore we inject a synthetic
 * query derived from the top level query and make it the only range table
 * entry for the top level query. This way we can push down any subquery joins
 * down to workers without invoking join order planner.
 */
static MultiNode *
SubqueryPushdownMultiNodeTree(Query *originalQuery)
{
	Query *queryTree = copyObject(originalQuery);
	List *targetEntryList = queryTree->targetList;
	MultiCollect *subqueryCollectNode = CitusMakeNode(MultiCollect);

	/* verify we can perform distributed planning on this query */
	DeferredErrorMessage *unsupportedQueryError = DeferErrorIfQueryNotSupported(
		queryTree);
	if (unsupportedQueryError != NULL)
	{
		RaiseDeferredError(unsupportedQueryError, ERROR);
	}

	/*
	 * We would be creating a new Query and pushing down top level query's
	 * contents down to it. Join and filter clauses in higher level query would
	 * be transferred to lower query. Therefore after this function we would
	 * only have a single range table entry in the top level query. We need to
	 * create a target list entry in lower query for each column reference in
	 * upper level query's target list and having clauses. Any column reference
	 * in the upper query will be updated to have varno=1, and varattno=<resno>
	 * of matching target entry in pushed down query.
	 * Consider query
	 *      SELECT s1.a, sum(s2.c)
	 *      FROM (some subquery) s1, (some subquery) s2
	 *      WHERE s1.a = s2.a
	 *      GROUP BY s1.a
	 *      HAVING avg(s2.b);
	 *
	 * We want to prepare a multi tree to avoid subquery joins at top level,
	 * therefore above query is converted to an equivalent
	 *      SELECT worker_column_0, sum(worker_column_1)
	 *      FROM (
	 *              SELECT
	 *                  s1.a AS worker_column_0,
	 *                  s2.c AS worker_column_1,
	 *                  s2.b AS worker_column_2
	 *              FROM (some subquery) s1, (some subquery) s2
	 *              WHERE s1.a = s2.a) worker_subquery
	 *      GROUP BY worker_column_0
	 *      HAVING avg(worker_column_2);
	 *  After this conversion MultiTree is created as follows
	 *
	 *  MultiExtendedOpNode(
	 *      targetList : worker_column_0, sum(worker_column_1)
	 *      groupBy : worker_column_0
	 *      having :  avg(worker_column_2))
	 * --->MultiProject (worker_column_0, worker_column_1, worker_column_2)
	 * --->--->	MultiTable (subquery : worker_subquery)
	 *
	 * Master and worker queries will be created out of this MultiTree at later stages.
	 */

	/*
	 * columnList contains all columns returned by subquery. Subquery target
	 * entry list, subquery range table entry's column name list are derived from
	 * columnList. Columns mentioned in multiProject node and multiExtendedOp
	 * node are indexed with their respective position in columnList.
	 */
	List *targetColumnList = pull_var_clause_default((Node *) targetEntryList);
	List *havingClauseColumnList = pull_var_clause_default(queryTree->havingQual);
	List *columnList = list_concat(targetColumnList, havingClauseColumnList);

	/* create a target entry for each unique column */
	List *subqueryTargetEntryList = CreateSubqueryTargetEntryList(columnList);

	/*
	 * Update varno/varattno fields of columns in columnList to
	 * point to corresponding target entry in subquery target entry list.
	 */
	UpdateVarMappingsForExtendedOpNode(columnList, subqueryTargetEntryList);

	/* new query only has target entries, join tree, and rtable*/
	Query *pushedDownQuery = makeNode(Query);
	pushedDownQuery->commandType = queryTree->commandType;
	pushedDownQuery->targetList = subqueryTargetEntryList;
	pushedDownQuery->jointree = copyObject(queryTree->jointree);
	pushedDownQuery->rtable = copyObject(queryTree->rtable);
	pushedDownQuery->setOperations = copyObject(queryTree->setOperations);
	pushedDownQuery->querySource = queryTree->querySource;
	pushedDownQuery->hasSubLinks = queryTree->hasSubLinks;

	MultiTable *subqueryNode = MultiSubqueryPushdownTable(pushedDownQuery);

	SetChild((MultiUnaryNode *) subqueryCollectNode, (MultiNode *) subqueryNode);
	MultiNode *currentTopNode = (MultiNode *) subqueryCollectNode;

	/* build project node for the columns to project */
	MultiProject *projectNode = MultiProjectNode(targetEntryList);
	SetChild((MultiUnaryNode *) projectNode, currentTopNode);
	currentTopNode = (MultiNode *) projectNode;

	/*
	 * We build the extended operator node to capture aggregate functions, group
	 * clauses, sort clauses, limit/offset clauses, and expressions. We need to
	 * distinguish between aggregates and expressions; and we address this later
	 * in the logical optimizer.
	 */
	MultiExtendedOp *extendedOpNode = MultiExtendedOpNode(queryTree, originalQuery);

	/*
	 * Postgres standard planner converts having qual node to a list of and
	 * clauses and expects havingQual to be of type List when executing the
	 * query later. This function is called on an original query, therefore
	 * havingQual has not been converted yet. Perform conversion here.
	 */
	if (extendedOpNode->havingQual != NULL &&
		!IsA(extendedOpNode->havingQual, List))
	{
		extendedOpNode->havingQual =
			(Node *) make_ands_implicit((Expr *) extendedOpNode->havingQual);
	}

	/*
	 * Group by on primary key allows all columns to appear in the target
	 * list, but once we wrap the join tree into a subquery the GROUP BY
	 * will no longer directly refer to the primary key and referencing
	 * columns that are not in the GROUP BY would result in an error. To
	 * prevent that we wrap all the columns that do not appear in the
	 * GROUP BY in an any_value aggregate.
	 */
	if (extendedOpNode->groupClauseList != NIL)
	{
		extendedOpNode->targetList = (List *) WrapUngroupedVarsInAnyValueAggregate(
			(Node *) extendedOpNode->targetList,
			extendedOpNode->groupClauseList,
			extendedOpNode->targetList, true);

		extendedOpNode->havingQual = WrapUngroupedVarsInAnyValueAggregate(
			(Node *) extendedOpNode->havingQual,
			extendedOpNode->groupClauseList,
			extendedOpNode->targetList, false);
	}

	/*
	 * Postgres standard planner evaluates expressions in the LIMIT/OFFSET clauses.
	 * Since we're using original query here, we should manually evaluate the
	 * expression on the LIMIT and OFFSET clauses. Note that logical optimizer
	 * expects those clauses to be already evaluated.
	 */
	extendedOpNode->limitCount =
		PartiallyEvaluateExpression(extendedOpNode->limitCount, NULL);
	extendedOpNode->limitOffset =
		PartiallyEvaluateExpression(extendedOpNode->limitOffset, NULL);

	SetChild((MultiUnaryNode *) extendedOpNode, currentTopNode);
	currentTopNode = (MultiNode *) extendedOpNode;

	return currentTopNode;
}


/*
 * CreateSubqueryTargetEntryList creates a target entry for each unique column
 * in the column list and returns the target entry list.
 */
static List *
CreateSubqueryTargetEntryList(List *columnList)
{
	AttrNumber resNo = 1;
	List *uniqueColumnList = NIL;
	List *subqueryTargetEntryList = NIL;

	Node *column = NULL;
	foreach_ptr(column, columnList)
	{
		uniqueColumnList = list_append_unique(uniqueColumnList, column);
	}

	foreach_ptr(column, uniqueColumnList)
	{
		TargetEntry *newTargetEntry = makeNode(TargetEntry);

		newTargetEntry->expr = (Expr *) copyObject(column);
		newTargetEntry->resname = WorkerColumnName(resNo);
		newTargetEntry->resjunk = false;
		newTargetEntry->resno = resNo;

		subqueryTargetEntryList = lappend(subqueryTargetEntryList, newTargetEntry);
		resNo++;
	}

	return subqueryTargetEntryList;
}


/*
 * UpdateVarMappingsForExtendedOpNode updates varno/varattno fields of columns
 * in columnList to point to corresponding target in subquery target entry
 * list.
 */
static void
UpdateVarMappingsForExtendedOpNode(List *columnList, List *subqueryTargetEntryList)
{
	Var *column = NULL;
	foreach_ptr(column, columnList)
	{
		/*
		 * As an optimization, subqueryTargetEntryList only consists of
		 * distinct elements. In other words, any duplicate entries in the
		 * target list consolidated into a single element to prevent pulling
		 * unnecessary data from the worker nodes (e.g. SELECT a,a,a,b,b,b FROM x;
		 * is turned into SELECT a,b FROM x_102008).
		 *
		 * Thus, at this point we should iterate on the subqueryTargetEntryList
		 * and ensure that the column on the extended op node points to the
		 * correct target entry.
		 */
		UpdateColumnToMatchingTargetEntry(column, subqueryTargetEntryList);
	}
}


/*
 * UpdateColumnToMatchingTargetEntry sets the variable of given column entry to
 * the matching entry of the targetEntryList. Since data type of the column can
 * be different from the types of the elements of targetEntryList, we use flattenedExpr.
 */
static void
UpdateColumnToMatchingTargetEntry(Var *column, List *targetEntryList)
{
	ListCell *targetEntryCell = NULL;

	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		if (IsA(targetEntry->expr, Var))
		{
			Var *targetEntryVar = (Var *) targetEntry->expr;

			if (IsA(column, Var) && equal(column, targetEntryVar))
			{
				column->varno = 1;
				column->varattno = targetEntry->resno;
				break;
			}
		}
		else
		{
			elog(ERROR, "unrecognized node type on the target list: %d",
				 nodeTag(targetEntry->expr));
		}
	}
}


/*
 * MultiSubqueryPushdownTable creates a MultiTable from the given subquery,
 * populates column list and returns the multitable.
 */
static MultiTable *
MultiSubqueryPushdownTable(Query *subquery)
{
	StringInfo rteName = makeStringInfo();
	List *columnNamesList = NIL;
	ListCell *targetEntryCell = NULL;

	appendStringInfo(rteName, "worker_subquery");

	foreach(targetEntryCell, subquery->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		columnNamesList = lappend(columnNamesList, makeString(targetEntry->resname));
	}

	MultiTable *subqueryTableNode = CitusMakeNode(MultiTable);
	subqueryTableNode->subquery = subquery;
	subqueryTableNode->relationId = SUBQUERY_PUSHDOWN_RELATION_ID;
	subqueryTableNode->rangeTableId = SUBQUERY_RANGE_TABLE_ID;
	subqueryTableNode->partitionColumn = NULL;
	subqueryTableNode->alias = makeNode(Alias);
	subqueryTableNode->alias->aliasname = rteName->data;
	subqueryTableNode->referenceNames = makeNode(Alias);
	subqueryTableNode->referenceNames->aliasname = rteName->data;
	subqueryTableNode->referenceNames->colnames = columnNamesList;

	return subqueryTableNode;
}
