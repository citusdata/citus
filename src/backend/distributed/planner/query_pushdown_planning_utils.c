/*-------------------------------------------------------------------------
 *
 * query_pushdown_planning_utils.c
 *
 * Routines for creating pushdown plans for queries.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/query_pushdown_planning_utils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"

/* Local functions forward declarations */
static bool JoinTreeContainsSubqueryWalker(Node *joinTreeNode, void *context);
static bool IsFunctionRTE(Node *node);
static bool IsNodeQuery(Node *node);
static bool WindowPartitionOnDistributionColumn(Query *query);
static DeferredErrorMessage * DeferErrorIfFromClauseRecurs(Query *queryTree);
static DeferredErrorMessage * DeferredErrorIfUnsupportedRecurringTuplesJoin(
	PlannerRestrictionContext *plannerRestrictionContext);
static DeferredErrorMessage * DeferErrorIfUnsupportedTableCombination(Query *queryTree);
static bool ShouldRecurseForRecurringTuplesJoinChecks(RelOptInfo *relOptInfo);
static bool RelationInfoContainsRecurringTuples(PlannerInfo *plannerInfo,
												RelOptInfo *relationInfo,
												RecurringTuplesType *recurType);
static bool IsRecurringRTE(RangeTblEntry *rangeTableEntry,
						   RecurringTuplesType *recurType);
static bool IsRecurringRangeTable(List *rangeTable, RecurringTuplesType *recurType);


static MultiNode * SubqueryPushdownMultiNodeTree(Query *queryTree);
static void FlattenJoinVars(List *columnList, Query *queryTree);
static void UpdateVarMappingsForExtendedOpNode(List *columnList,
											   List *subqueryTargetEntryList);
static MultiTable * MultiSubqueryPushdownTable(Query *subquery);
static List * CreateSubqueryTargetEntryList(List *columnList);
static Task * QueryPushdownTaskCreate(Query *originalQuery, int shardIndex,
									  RelationRestrictionContext *restrictionContext,
									  uint32 taskId,
									  TaskType taskType);
static void ErrorIfUnsupportedShardDistribution(Query *query);


/*
 * ShouldUseSubqueryPushDown determines whether it's desirable to use
 * subquery pushdown to plan the query based on the original and
 * rewritten query.
 */
bool
ShouldUseSubqueryPushDown(Query *originalQuery, Query *rewrittenQuery)
{
	List *qualifierList = NIL;
	StringInfo errorMessage = NULL;

	/*
	 * We check the existence of subqueries in FROM clause on the modified query
	 * given that if postgres already flattened the subqueries, MultiPlanTree()
	 * can plan corresponding distributed plan.
	 */
	if (JoinTreeContainsSubquery(rewrittenQuery))
	{
		return true;
	}

	/*
	 * We also check the existence of subqueries in WHERE clause. Note that
	 * this check needs to be done on the original query given that
	 * standard_planner() may replace the sublinks with anti/semi joins and
	 * MultiPlanTree() cannot plan such queries.
	 */
	if (WhereClauseContainsSubquery(originalQuery))
	{
		return true;
	}

	/*
	 * We process function RTEs as subqueries, since the join order planner
	 * does not know how to handle them.
	 */
	if (FindNodeCheck((Node *) originalQuery, IsFunctionRTE))
	{
		return true;
	}

	/*
	 * Some unsupported join clauses in logical planner
	 * may be supported by subquery pushdown planner.
	 */
	qualifierList = QualifierList(rewrittenQuery->jointree);
	if (DeferErrorIfUnsupportedClause(qualifierList) != NULL)
	{
		return true;
	}

	/* check if the query has a window function and it is safe to pushdown */
	if (originalQuery->hasWindowFuncs &&
		SafeToPushdownWindowFunction(originalQuery, &errorMessage))
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
 * WhereClauseContainsSubquery returns true if the input query contains
 * any subqueries in the WHERE clause.
 */
bool
WhereClauseContainsSubquery(Query *query)
{
	FromExpr *joinTree = query->jointree;
	Node *queryQuals = NULL;

	if (!joinTree)
	{
		return false;
	}

	queryQuals = joinTree->quals;

	return FindNodeCheck(queryQuals, IsNodeQuery);
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
 * IsNodeQuery returns true if the given node is a Query.
 */
static bool
IsNodeQuery(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	return IsA(node, Query);
}


/*
 * SafeToPushdownWindowFunction checks if the query with window function is supported.
 * It returns the result accordingly and modifies the error detail.
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
			*errorDetail = makeStringInfo();
			appendStringInfoString(*errorDetail,
								   "Window functions without PARTITION BY on distribution "
								   "column is currently unsupported");
			return false;
		}
	}

	if (!WindowPartitionOnDistributionColumn(query))
	{
		*errorDetail = makeStringInfo();
		appendStringInfoString(*errorDetail,
							   "Window functions with PARTITION BY list missing distribution "
							   "column is currently unsupported");
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
		List *groupTargetEntryList = NIL;
		bool partitionOnDistributionColumn = false;
		List *partitionClauseList = windowClause->partitionClause;
		List *targetEntryList = query->targetList;

		groupTargetEntryList =
			GroupTargetEntryList(partitionClauseList, targetEntryList);

		partitionOnDistributionColumn =
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
	DeferredErrorMessage *subqueryPushdownError = NULL;
	DeferredErrorMessage *unsupportedQueryError = NULL;

	/*
	 * This is a generic error check that applies to both subquery pushdown
	 * and single table repartition subquery.
	 */
	unsupportedQueryError = DeferErrorIfQueryNotSupported(originalQuery);
	if (unsupportedQueryError != NULL)
	{
		RaiseDeferredError(unsupportedQueryError, ERROR);
	}

	/*
	 * In principle, we're first trying subquery pushdown planner. If it fails
	 * to create a logical plan, continue with trying the single table
	 * repartition subquery planning.
	 */
	subqueryPushdownError = DeferErrorIfUnsupportedSubqueryPushdown(originalQuery,
																	plannerRestrictionContext);
	if (!subqueryPushdownError)
	{
		multiQueryNode = SubqueryPushdownMultiNodeTree(originalQuery);
	}
	else if (subqueryPushdownError)
	{
		bool singleRelationRepartitionSubquery = false;
		RangeTblEntry *subqueryRangeTableEntry = NULL;
		Query *subqueryTree = NULL;
		DeferredErrorMessage *repartitionQueryError = NULL;
		List *subqueryEntryList = NULL;

		/*
		 * If not eligible for single relation repartition query, we should raise
		 * subquery pushdown error.
		 */
		singleRelationRepartitionSubquery =
			SingleRelationRepartitionSubquery(originalQuery);
		if (!singleRelationRepartitionSubquery)
		{
			RaiseDeferredErrorInternal(subqueryPushdownError, ERROR);
		}

		subqueryEntryList = SubqueryEntryList(queryTree);
		subqueryRangeTableEntry = (RangeTblEntry *) linitial(subqueryEntryList);
		Assert(subqueryRangeTableEntry->rtekind == RTE_SUBQUERY);

		subqueryTree = subqueryRangeTableEntry->subquery;

		repartitionQueryError = DeferErrorIfUnsupportedSubqueryRepartition(subqueryTree);
		if (repartitionQueryError)
		{
			RaiseDeferredErrorInternal(repartitionQueryError, ERROR);
		}

		/* all checks has passed, safe to create the multi plan */
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
	DeferredErrorMessage *error = NULL;

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
							 "joined on their distribution columns with equal operator",
							 NULL, NULL);
	}

	/* we shouldn't allow reference tables in the FROM clause when the query has sublinks */
	error = DeferErrorIfFromClauseRecurs(originalQuery);
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
	RecurringTuplesType recurType = RECURRING_TUPLES_INVALID;

	if (!queryTree->hasSubLinks)
	{
		return NULL;
	}

	if (FindNodeCheckInRangeTableList(queryTree->rtable, IsDistributedTableRTE))
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
		return NULL;
	}


	/*
	 * Try to figure out which type of recurring tuples we have to produce a
	 * relevant error message. If there are several we'll pick the first one.
	 */
	IsRecurringRangeTable(queryTree->rtable, &recurType);

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
		RelOptInfo *innerrel = joinRestriction->innerrel;
		RelOptInfo *outerrel = joinRestriction->outerrel;

		if (joinType == JOIN_SEMI || joinType == JOIN_ANTI || joinType == JOIN_LEFT)
		{
			if (ShouldRecurseForRecurringTuplesJoinChecks(outerrel) &&
				RelationInfoContainsRecurringTuples(plannerInfo, outerrel, &recurType))
			{
				break;
			}
		}
		else if (joinType == JOIN_FULL)
		{
			if ((ShouldRecurseForRecurringTuplesJoinChecks(innerrel) &&
				 RelationInfoContainsRecurringTuples(plannerInfo, innerrel,
													 &recurType)) ||
				(ShouldRecurseForRecurringTuplesJoinChecks(outerrel) &&
				 RelationInfoContainsRecurringTuples(plannerInfo, outerrel, &recurType)))
			{
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
	DeferredErrorMessage *deferredError = NULL;

	deferredError = DeferErrorIfUnsupportedTableCombination(subqueryTree);
	if (deferredError)
	{
		return deferredError;
	}

	if (subqueryTree->rtable == NIL &&
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
	ListCell *joinTreeTableIndexCell = NULL;
	bool unsupportedTableCombination = false;
	char *errorDetail = NULL;

	/*
	 * Extract all range table indexes from the join tree. Note that sub-queries
	 * that get pulled up by PostgreSQL don't appear in this join tree.
	 */
	ExtractRangeTableIndexWalker((Node *) queryTree->jointree, &joinTreeTableIndexList);

	foreach(joinTreeTableIndexCell, joinTreeTableIndexList)
	{
		/*
		 * Join tree's range table index starts from 1 in the query tree. But,
		 * list indexes start from 0.
		 */
		int joinTreeTableIndex = lfirst_int(joinTreeTableIndexCell);
		int rangeTableListIndex = joinTreeTableIndex - 1;

		RangeTblEntry *rangeTableEntry =
			(RangeTblEntry *) list_nth(rangeTableList, rangeTableListIndex);

		/*
		 * Check if the range table in the join tree is a simple relation, a
		 * subquery, or immutable function.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION ||
			rangeTableEntry->rtekind == RTE_SUBQUERY)
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
 * ShouldRecurseForRecurringTuplesJoinChecks is a helper function for deciding
 * on whether the input relOptInfo should be checked for table expressions that
 * generate the same tuples in every query on a shard. We use this to avoid
 * redundant checks and false positives in complex join trees.
 */
static bool
ShouldRecurseForRecurringTuplesJoinChecks(RelOptInfo *relOptInfo)
{
	bool shouldRecurse = true;

	/*
	 * We shouldn't recursively go down for joins since we're already
	 * going to process each join seperately. Otherwise we'd restrict
	 * the coverage. See the below sketch where (h) denotes a hash
	 * distributed relation, (r) denotes a reference table, (L) denotes
	 * LEFT JOIN and (I) denotes INNER JOIN. If we're to recurse into
	 * the inner join, we'd be preventing to push down the following
	 * join tree, which is actually safe to push down.
	 *
	 *                       (L)
	 *                      /  \
	 *                   (I)     h
	 *                  /  \
	 *                r      h
	 */
	if (relOptInfo->reloptkind == RELOPT_JOINREL)
	{
		return false;
	}

	/*
	 * Note that we treat the same query where relations appear in subqueries
	 * differently. (i.e., use SELECT * FROM r; instead of r)
	 *
	 * In that case, to relax some restrictions, we do the following optimization:
	 * If the subplan (i.e., plannerInfo corresponding to the subquery) contains any
	 * joins, we skip reference table checks keeping in mind that the join is already
	 * going to be processed seperately. This optimization should suffice for many
	 * use cases.
	 */
	if (relOptInfo->reloptkind == RELOPT_BASEREL && relOptInfo->subroot != NULL)
	{
		PlannerInfo *subroot = relOptInfo->subroot;

		if (list_length(subroot->join_rel_list) > 0)
		{
			RelOptInfo *subqueryJoin = linitial(subroot->join_rel_list);

			/*
			 * Subqueries without relations (e.g. SELECT 1) are a little funny.
			 * They are treated as having a join, but the join is between 0
			 * relations and won't be in the join restriction list and therefore
			 * won't be revisited in DeferredErrorIfUnsupportedRecurringTuplesJoin.
			 *
			 * We therefore only skip joins with >0 relations.
			 */
			if (bms_num_members(subqueryJoin->relids) > 0)
			{
				shouldRecurse = false;
			}
		}
	}

	return shouldRecurse;
}


/*
 * RelationInfoContainsRecurringTuples checks whether the relationInfo
 * contains any recurring table expression, namely a reference table,
 * or immutable function. If found, RelationInfoContainsRecurringTuples
 * returns true.
 *
 * Note that since relation ids of relationInfo indexes to the range
 * table entry list of planner info, planner info is also passed.
 */
static bool
RelationInfoContainsRecurringTuples(PlannerInfo *plannerInfo, RelOptInfo *relationInfo,
									RecurringTuplesType *recurType)
{
	Relids relids = bms_copy(relationInfo->relids);
	int relationId = -1;

	while ((relationId = bms_first_member(relids)) >= 0)
	{
		RangeTblEntry *rangeTableEntry = plannerInfo->simple_rte_array[relationId];

		/* relationInfo has this range table entry */
		if (IsRecurringRTE(rangeTableEntry, recurType))
		{
			return true;
		}
	}

	return false;
}


/*
 * IsRecurringRTE returns whether the range table entry will generate
 * the same set of tuples when repeating it in a query on different
 * shards.
 */
static bool
IsRecurringRTE(RangeTblEntry *rangeTableEntry, RecurringTuplesType *recurType)
{
	return IsRecurringRangeTable(list_make1(rangeTableEntry), recurType);
}


/*
 * IsRecurringRangeTable returns whether the range table will generate
 * the same set of tuples when repeating it in a query on different
 * shards.
 */
static bool
IsRecurringRangeTable(List *rangeTable, RecurringTuplesType *recurType)
{
	return range_table_walker(rangeTable, HasRecurringTuples, recurType,
							  QTW_EXAMINE_RTES);
}


/*
 * SubqueryPushdownMultiNodeTree creates logical plan for subquery pushdown logic.
 * Note that this logic will be changed in next iterations, so we decoupled it
 * from other parts of code although it causes some code duplication.
 *
 * Current subquery pushdown support in MultiTree logic requires a single range
 * table entry in the top most from clause. Therefore we inject an synthetic
 * query derived from the top level query and make it the only range table
 * entry for the top level query. This way we can push down any subquery joins
 * down to workers without invoking join order planner.
 */
static MultiNode *
SubqueryPushdownMultiNodeTree(Query *queryTree)
{
	List *targetEntryList = queryTree->targetList;
	List *columnList = NIL;
	List *targetColumnList = NIL;
	MultiCollect *subqueryCollectNode = CitusMakeNode(MultiCollect);
	MultiTable *subqueryNode = NULL;
	MultiProject *projectNode = NULL;
	MultiExtendedOp *extendedOpNode = NULL;
	MultiNode *currentTopNode = NULL;
	Query *pushedDownQuery = NULL;
	List *subqueryTargetEntryList = NIL;
	List *havingClauseColumnList = NIL;
	DeferredErrorMessage *unsupportedQueryError = NULL;

	/* verify we can perform distributed planning on this query */
	unsupportedQueryError = DeferErrorIfQueryNotSupported(queryTree);
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
	 *                  s2.b AS as worker_column_2
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
	 * uniqueColumnList contains all columns returned by subquery. Subquery target
	 * entry list, subquery range table entry's column name list are derived from
	 * uniqueColumnList. Columns mentioned in multiProject node and multiExtendedOp
	 * node are indexed with their respective position in uniqueColumnList.
	 */
	targetColumnList = pull_var_clause_default((Node *) targetEntryList);
	havingClauseColumnList = pull_var_clause_default(queryTree->havingQual);
	columnList = list_concat(targetColumnList, havingClauseColumnList);

	FlattenJoinVars(columnList, queryTree);

	/* create a target entry for each unique column */
	subqueryTargetEntryList = CreateSubqueryTargetEntryList(columnList);

	/*
	 * Update varno/varattno fields of columns in columnList to
	 * point to corresponding target entry in subquery target entry list.
	 */
	UpdateVarMappingsForExtendedOpNode(columnList, subqueryTargetEntryList);

	/* new query only has target entries, join tree, and rtable*/
	pushedDownQuery = makeNode(Query);
	pushedDownQuery->commandType = queryTree->commandType;
	pushedDownQuery->targetList = subqueryTargetEntryList;
	pushedDownQuery->jointree = copyObject(queryTree->jointree);
	pushedDownQuery->rtable = copyObject(queryTree->rtable);
	pushedDownQuery->setOperations = copyObject(queryTree->setOperations);
	pushedDownQuery->querySource = queryTree->querySource;

	subqueryNode = MultiSubqueryPushdownTable(pushedDownQuery);

	SetChild((MultiUnaryNode *) subqueryCollectNode, (MultiNode *) subqueryNode);
	currentTopNode = (MultiNode *) subqueryCollectNode;

	/* build project node for the columns to project */
	projectNode = MultiProjectNode(targetEntryList);
	SetChild((MultiUnaryNode *) projectNode, currentTopNode);
	currentTopNode = (MultiNode *) projectNode;

	/*
	 * We build the extended operator node to capture aggregate functions, group
	 * clauses, sort clauses, limit/offset clauses, and expressions. We need to
	 * distinguish between aggregates and expressions; and we address this later
	 * in the logical optimizer.
	 */
	extendedOpNode = MultiExtendedOpNode(queryTree);

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
 * FlattenJoinVars iterates over provided columnList to identify
 * Var's that are referenced from join RTE, and reverts back to their
 * original RTEs.
 *
 * This is required because Postgres allows columns to be referenced using
 * a join alias. Therefore the same column from a table could be referenced
 * twice using its absolute table name (t1.a), and without table name (a).
 * This is a problem when one of them is inside the group by clause and the
 * other is not. Postgres is smart about it to detect that both target columns
 * resolve to the same thing, and allows a single group by clause to cover
 * both target entries when standard planner is called. Since we operate on
 * the original query, we want to make sure we provide correct varno/varattno
 * values to Postgres so that it could produce valid query.
 *
 * Only exception is that, if a join is given an alias name, we do not want to
 * flatten those var's. If we do, deparsing fails since it expects to see a join
 * alias, and cannot access the RTE in the join tree by their names.
 */
static void
FlattenJoinVars(List *columnList, Query *queryTree)
{
	ListCell *columnCell = NULL;
	List *rteList = queryTree->rtable;

	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		RangeTblEntry *columnRte = NULL;
		PlannerInfo *root = NULL;

		Assert(IsA(column, Var));

		/*
		 * if join does not have an alias, it is copied over join rte.
		 * There is no need to find the JoinExpr to check whether it has
		 * an alias defined.
		 *
		 * We use the planner's flatten_join_alias_vars routine to do
		 * the flattening; it wants a PlannerInfo root node, which
		 * fortunately can be mostly dummy.
		 */
		columnRte = rt_fetch(column->varno, rteList);
		if (columnRte->rtekind == RTE_JOIN && columnRte->alias == NULL)
		{
			Var *normalizedVar = NULL;

			if (root == NULL)
			{
				root = makeNode(PlannerInfo);
				root->parse = (queryTree);
				root->planner_cxt = CurrentMemoryContext;
				root->hasJoinRTEs = true;
			}

			normalizedVar = (Var *) flatten_join_alias_vars(root, (Node *) column);

			/*
			 * We need to copy values over existing one to make sure it is updated on
			 * respective places.
			 */
			memcpy(column, normalizedVar, sizeof(Var));
		}
	}
}


/*
 * CreateSubqueryTargetEntryList creates a target entry for each unique column
 * in the column list and returns the target entry list.
 */
static List *
CreateSubqueryTargetEntryList(List *columnList)
{
	AttrNumber resNo = 1;
	ListCell *columnCell = NULL;
	List *uniqueColumnList = NIL;
	List *subqueryTargetEntryList = NIL;

	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		uniqueColumnList = list_append_unique(uniqueColumnList, copyObject(column));
	}

	foreach(columnCell, uniqueColumnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		TargetEntry *newTargetEntry = makeNode(TargetEntry);
		StringInfo columnNameString = makeStringInfo();

		newTargetEntry->expr = (Expr *) copyObject(column);
		appendStringInfo(columnNameString, WORKER_COLUMN_FORMAT, resNo);
		newTargetEntry->resname = columnNameString->data;
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
	ListCell *columnCell = NULL;
	foreach(columnCell, columnList)
	{
		Var *columnOnTheExtendedNode = (Var *) lfirst(columnCell);
		ListCell *targetEntryCell = NULL;
		foreach(targetEntryCell, subqueryTargetEntryList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
			Var *targetColumn = NULL;

			Assert(IsA(targetEntry->expr, Var));
			targetColumn = (Var *) targetEntry->expr;
			if (columnOnTheExtendedNode->varno == targetColumn->varno &&
				columnOnTheExtendedNode->varattno == targetColumn->varattno)
			{
				columnOnTheExtendedNode->varno = 1;
				columnOnTheExtendedNode->varattno = targetEntry->resno;
				break;
			}
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
	MultiTable *subqueryTableNode = NULL;
	StringInfo rteName = makeStringInfo();
	List *columnNamesList = NIL;
	ListCell *targetEntryCell = NULL;

	appendStringInfo(rteName, "worker_subquery");

	foreach(targetEntryCell, subquery->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		columnNamesList = lappend(columnNamesList, makeString(targetEntry->resname));
	}

	subqueryTableNode = CitusMakeNode(MultiTable);
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


/*
 * QueryPushdownSqlTaskList creates a list of SQL tasks to execute the given subquery
 * pushdown job. For this, the it is being checked whether the query is router
 * plannable per target shard interval. For those router plannable worker
 * queries, we create a SQL task and append the task to the task list that is going
 * to be executed.
 */
List *
QueryPushdownSqlTaskList(Query *query, uint64 jobId,
						 RelationRestrictionContext *relationRestrictionContext,
						 List *prunedRelationShardList, TaskType taskType)
{
	List *sqlTaskList = NIL;
	ListCell *restrictionCell = NULL;
	uint32 taskIdIndex = 1; /* 0 is reserved for invalid taskId */
	int shardCount = 0;
	int shardOffset = 0;
	int minShardOffset = 0;
	int maxShardOffset = 0;
	bool *taskRequiredForShardIndex = NULL;
	ListCell *prunedRelationShardCell = NULL;

	/* error if shards are not co-partitioned */
	ErrorIfUnsupportedShardDistribution(query);

	if (list_length(relationRestrictionContext->relationRestrictionList) == 0)
	{
		ereport(ERROR, (errmsg("cannot handle complex subqueries when the "
							   "router executor is disabled")));
	}

	/* defaults to be used if this is a reference table-only query */
	minShardOffset = 0;
	maxShardOffset = 0;

	forboth(prunedRelationShardCell, prunedRelationShardList,
			restrictionCell, relationRestrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;
		List *prunedShardList = (List *) lfirst(prunedRelationShardCell);
		ListCell *shardIntervalCell = NULL;
		DistTableCacheEntry *cacheEntry = NULL;

		cacheEntry = DistributedTableCacheEntry(relationId);
		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE)
		{
			continue;
		}

		/* we expect distributed tables to have the same shard count */
		if (shardCount > 0 && shardCount != cacheEntry->shardIntervalArrayLength)
		{
			ereport(ERROR, (errmsg("shard counts of co-located tables do not "
								   "match")));
		}

		if (taskRequiredForShardIndex == NULL)
		{
			shardCount = cacheEntry->shardIntervalArrayLength;
			taskRequiredForShardIndex = (bool *) palloc0(shardCount);

			/* there is a distributed table, find the shard range */
			minShardOffset = shardCount;
			maxShardOffset = -1;
		}

		foreach(shardIntervalCell, prunedShardList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
			int shardIndex = shardInterval->shardIndex;

			taskRequiredForShardIndex[shardIndex] = true;

			if (shardIndex < minShardOffset)
			{
				minShardOffset = shardIndex;
			}

			if (shardIndex > maxShardOffset)
			{
				maxShardOffset = shardIndex;
			}
		}
	}

	/*
	 * To avoid iterating through all shards indexes we keep the minimum and maximum
	 * offsets of shards that were not pruned away. This optimisation is primarily
	 * relevant for queries on range-distributed tables that, due to range filters,
	 * prune to a small number of adjacent shards.
	 *
	 * In other cases, such as an OR condition on a hash-distributed table, we may
	 * still visit most or all shards even if some of them were pruned away. However,
	 * given that hash-distributed tables typically only have a few shards the
	 * iteration is still very fast.
	 */
	for (shardOffset = minShardOffset; shardOffset <= maxShardOffset; shardOffset++)
	{
		Task *subqueryTask = NULL;

		if (taskRequiredForShardIndex != NULL && !taskRequiredForShardIndex[shardOffset])
		{
			/* this shard index is pruned away for all relations */
			continue;
		}

		subqueryTask = QueryPushdownTaskCreate(query, shardOffset,
											   relationRestrictionContext, taskIdIndex,
											   taskType);
		subqueryTask->jobId = jobId;
		sqlTaskList = lappend(sqlTaskList, subqueryTask);

		++taskIdIndex;
	}

	return sqlTaskList;
}


/*
 * ErrorIfUnsupportedShardDistribution gets list of relations in the given query
 * and checks if two conditions below hold for them, otherwise it errors out.
 * a. Every relation is distributed by range or hash. This means shards are
 * disjoint based on the partition column.
 * b. All relations have 1-to-1 shard partitioning between them. This means
 * shard count for every relation is same and for every shard in a relation
 * there is exactly one shard in other relations with same min/max values.
 */
static void
ErrorIfUnsupportedShardDistribution(Query *query)
{
	Oid firstTableRelationId = InvalidOid;
	List *relationIdList = RelationIdList(query);
	List *nonReferenceRelations = NIL;
	ListCell *relationIdCell = NULL;
	uint32 relationIndex = 0;
	uint32 rangeDistributedRelationCount = 0;
	uint32 hashDistributedRelationCount = 0;

	foreach(relationIdCell, relationIdList)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		char partitionMethod = PartitionMethod(relationId);
		if (partitionMethod == DISTRIBUTE_BY_RANGE)
		{
			rangeDistributedRelationCount++;
			nonReferenceRelations = lappend_oid(nonReferenceRelations,
												relationId);
		}
		else if (partitionMethod == DISTRIBUTE_BY_HASH)
		{
			hashDistributedRelationCount++;
			nonReferenceRelations = lappend_oid(nonReferenceRelations,
												relationId);
		}
		else if (partitionMethod == DISTRIBUTE_BY_NONE)
		{
			/* do not need to handle reference tables */
			continue;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot push down this subquery"),
							errdetail("Currently append partitioned relations "
									  "are not supported")));
		}
	}

	if ((rangeDistributedRelationCount > 0) && (hashDistributedRelationCount > 0))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot push down this subquery"),
						errdetail("A query including both range and hash "
								  "partitioned relations are unsupported")));
	}

	foreach(relationIdCell, nonReferenceRelations)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		bool coPartitionedTables = false;
		Oid currentRelationId = relationId;

		/* get shard list of first relation and continue for the next relation */
		if (relationIndex == 0)
		{
			firstTableRelationId = relationId;
			relationIndex++;

			continue;
		}

		/* check if this table has 1-1 shard partitioning with first table */
		coPartitionedTables = CoPartitionedTables(firstTableRelationId,
												  currentRelationId);
		if (!coPartitionedTables)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot push down this subquery"),
							errdetail("Shards of relations in subquery need to "
									  "have 1-to-1 shard partitioning")));
		}
	}
}


/*
 * SubqueryTaskCreate creates a sql task by replacing the target
 * shardInterval's boundary value.
 */
static Task *
QueryPushdownTaskCreate(Query *originalQuery, int shardIndex,
						RelationRestrictionContext *restrictionContext, uint32 taskId,
						TaskType taskType)
{
	Query *taskQuery = copyObject(originalQuery);

	StringInfo queryString = makeStringInfo();
	ListCell *restrictionCell = NULL;
	Task *subqueryTask = NULL;
	List *taskShardList = NIL;
	List *relationShardList = NIL;
	List *selectPlacementList = NIL;
	uint64 jobId = INVALID_JOB_ID;
	uint64 anchorShardId = INVALID_SHARD_ID;
	bool isModifyWithSubselect = originalQuery->resultRelation > 0;
	RangeTblEntry *resultRangeTable = NULL;
	Oid resultRelationOid = InvalidOid;

	/*
	 * If it is a modify query with sub-select, we need to have result relation
	 * to obtain right lock on it.
	 */
	if (isModifyWithSubselect)
	{
		resultRangeTable = rt_fetch(originalQuery->resultRelation, originalQuery->rtable);
		resultRelationOid = resultRangeTable->relid;
	}

	/*
	 * Find the relevant shard out of each relation for this task.
	 */
	foreach(restrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;
		DistTableCacheEntry *cacheEntry = NULL;
		ShardInterval *shardInterval = NULL;
		RelationShard *relationShard = NULL;

		cacheEntry = DistributedTableCacheEntry(relationId);
		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE)
		{
			/* reference table only has one shard */
			shardInterval = cacheEntry->sortedShardIntervalArray[0];

			/*
			 * Only use reference table as anchor shard if none exists yet or
			 * it is a result relation of modify query with sub-select.
			 */
			if (anchorShardId == INVALID_SHARD_ID ||
				(isModifyWithSubselect && relationId == resultRelationOid))
			{
				anchorShardId = shardInterval->shardId;
			}
		}
		else
		{
			/* use the shard from a specific index */
			shardInterval = cacheEntry->sortedShardIntervalArray[shardIndex];

			/*
			 * We take row exclusive lock for result relation and access share
			 * lock for other relations in the modify executor.
			 */
			if (isModifyWithSubselect)
			{
				if (relationId == resultRelationOid)
				{
					anchorShardId = shardInterval->shardId;
				}
			}
			else
			{
				/* use a shard from a distributed table as the anchor shard */
				anchorShardId = shardInterval->shardId;
			}
		}
		taskShardList = lappend(taskShardList, list_make1(shardInterval));

		relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = shardInterval->relationId;
		relationShard->shardId = shardInterval->shardId;

		relationShardList = lappend(relationShardList, relationShard);
	}

	selectPlacementList = WorkersContainingAllShards(taskShardList);
	if (list_length(selectPlacementList) == 0)
	{
		ereport(ERROR, (errmsg("cannot find a worker that has active placements for all "
							   "shards in the query")));
	}

	/*
	 * Augment the relations in the query with the shard IDs.
	 */
	UpdateRelationToShardNames((Node *) taskQuery, relationShardList);

	/*
	 * Ands are made implicit during shard pruning, as predicate comparison and
	 * refutation depend on it being so. We need to make them explicit again so
	 * that the query string is generated as (...) AND (...) as opposed to
	 * (...), (...).
	 */
	if (taskQuery->jointree->quals != NULL && IsA(taskQuery->jointree->quals, List))
	{
		taskQuery->jointree->quals = (Node *) make_ands_explicit(
			(List *) taskQuery->jointree->quals);
	}

	/* and generate the full query string */
	pg_get_query_def(taskQuery, queryString);
	ereport(DEBUG4, (errmsg("distributed statement: %s", queryString->data)));

	subqueryTask = CreateBasicTask(jobId, taskId, taskType, queryString->data);
	subqueryTask->dependedTaskList = NULL;
	subqueryTask->anchorShardId = anchorShardId;
	subqueryTask->taskPlacementList = selectPlacementList;
	subqueryTask->upsertQuery = false;
	subqueryTask->relationShardList = relationShardList;

	return subqueryTask;
}
