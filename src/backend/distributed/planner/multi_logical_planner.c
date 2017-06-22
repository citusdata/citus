/*-------------------------------------------------------------------------
 *
 * multi_logical_planner.c
 *
 * Routines for constructing a logical plan tree from the given Query tree
 * structure. This new logical plan is based on multi-relational algebra rules.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "distributed/citus_clauses.h"
#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/multi_router_planner.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"


/* Config variable managed via guc.c */
bool SubqueryPushdown = false; /* is subquery pushdown enabled */


/* Struct to differentiate different qualifier types in an expression tree walker */
typedef struct QualifierWalkerContext
{
	List *baseQualifierList;
	List *outerJoinQualifierList;
} QualifierWalkerContext;


/* Function pointer type definition for apply join rule functions */
typedef MultiNode *(*RuleApplyFunction) (MultiNode *leftNode, MultiNode *rightNode,
										 Var *partitionColumn, JoinType joinType,
										 List *joinClauses);

static RuleApplyFunction RuleApplyFunctionArray[JOIN_RULE_LAST] = { 0 }; /* join rules */

/* Local functions forward declarations */
static bool SingleRelationRepartitionSubquery(Query *queryTree);
static DeferredErrorMessage * DeferErrorIfUnsupportedSubqueryPushdown(Query *
																	  originalQuery,
																	  PlannerRestrictionContext
																	  *
																	  plannerRestrictionContext);
static DeferredErrorMessage * DeferErrorIfUnsupportedFilters(Query *subquery);
static bool EqualOpExpressionLists(List *firstOpExpressionList,
								   List *secondOpExpressionList);
static DeferredErrorMessage * DeferErrorIfCannotPushdownSubquery(Query *subqueryTree,
																 bool
																 outerMostQueryHasLimit);
static DeferredErrorMessage * DeferErrorIfUnsupportedUnionQuery(Query *queryTree,
																bool
																outerMostQueryHasLimit);
static bool ExtractSetOperationStatmentWalker(Node *node, List **setOperationList);
static DeferredErrorMessage * DeferErrorIfUnsupportedTableCombination(Query *queryTree);
static bool TargetListOnPartitionColumn(Query *query, List *targetEntryList);
static FieldSelect * CompositeFieldRecursive(Expr *expression, Query *query);
static bool FullCompositeFieldList(List *compositeFieldList);
static MultiNode * MultiPlanTree(Query *queryTree);
static void ErrorIfQueryNotSupported(Query *queryTree);
static bool HasUnsupportedJoinWalker(Node *node, void *context);
static bool ErrorHintRequired(const char *errorHint, Query *queryTree);
static DeferredErrorMessage * DeferErrorIfUnsupportedSubqueryRepartition(Query *
																		 subqueryTree);
static bool HasTablesample(Query *queryTree);
static bool HasOuterJoin(Query *queryTree);
static bool HasOuterJoinWalker(Node *node, void *maxJoinLevel);
static bool HasComplexJoinOrder(Query *queryTree);
static bool HasComplexRangeTableType(Query *queryTree);
static void ValidateClauseList(List *clauseList);
static void ValidateSubqueryPushdownClauseList(List *clauseList);
static bool ExtractFromExpressionWalker(Node *node,
										QualifierWalkerContext *walkerContext);
static List * MultiTableNodeList(List *tableEntryList, List *rangeTableList);
static List * AddMultiCollectNodes(List *tableNodeList);
static MultiNode * MultiJoinTree(List *joinOrderList, List *collectTableList,
								 List *joinClauseList);
static MultiCollect * CollectNodeForTable(List *collectTableList, uint32 rangeTableId);
static MultiSelect * MultiSelectNode(List *whereClauseList);
static bool IsSelectClause(Node *clause);
static bool IsSublinkClause(Node *clause);
static MultiProject * MultiProjectNode(List *targetEntryList);
static MultiExtendedOp * MultiExtendedOpNode(Query *queryTree);

/* Local functions forward declarations for applying joins */
static MultiNode * ApplyJoinRule(MultiNode *leftNode, MultiNode *rightNode,
								 JoinRuleType ruleType, Var *partitionColumn,
								 JoinType joinType, List *joinClauseList);
static RuleApplyFunction JoinRuleApplyFunction(JoinRuleType ruleType);
static MultiNode * ApplyBroadcastJoin(MultiNode *leftNode, MultiNode *rightNode,
									  Var *partitionColumn, JoinType joinType,
									  List *joinClauses);
static MultiNode * ApplyLocalJoin(MultiNode *leftNode, MultiNode *rightNode,
								  Var *partitionColumn, JoinType joinType,
								  List *joinClauses);
static MultiNode * ApplySinglePartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
											Var *partitionColumn, JoinType joinType,
											List *joinClauses);
static MultiNode * ApplyDualPartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
										  Var *partitionColumn, JoinType joinType,
										  List *joinClauses);
static MultiNode * ApplyCartesianProduct(MultiNode *leftNode, MultiNode *rightNode,
										 Var *partitionColumn, JoinType joinType,
										 List *joinClauses);

/*
 * Local functions forward declarations for subquery pushdown. Note that these
 * functions will be removed with upcoming subqery changes.
 */
static Node * ResolveExternalParams(Node *inputNode, ParamListInfo boundParams);
static MultiNode * MultiSubqueryPlanTree(Query *originalQuery,
										 Query *queryTree,
										 PlannerRestrictionContext *
										 plannerRestrictionContext);
static List * SublinkList(Query *originalQuery);
static bool ExtractSublinkWalker(Node *node, List **sublinkList);
static MultiNode * SubqueryPushdownMultiPlanTree(Query *queryTree);

static List * CreateSubqueryTargetEntryList(List *columnList);
static void UpdateVarMappingsForExtendedOpNode(List *columnList,
											   List *subqueryTargetEntryList);
static MultiTable * MultiSubqueryPushdownTable(Query *subquery);


/*
 * MultiLogicalPlanCreate takes in both the original query and its corresponding modified
 * query tree yield by the standard planner. It uses helper functions to create logical
 * plan and adds a root node to top of it. The  original query is only used for subquery
 * pushdown planning.
 *
 * In order to support external parameters for the queries where planning
 * is done on the original query, we need to replace the external parameters
 * manually. To achive that for subquery pushdown planning, we pass boundParams
 * to this function. We need to do that since Citus currently unable to send
 * parameters to the workers on the execution.
 *
 * We also pass queryTree and plannerRestrictionContext to the planner. They
 * are primarily used to decide whether the subquery is safe to pushdown.
 * If not, it helps to produce meaningful error messages for subquery
 * pushdown planning.
 */
MultiTreeRoot *
MultiLogicalPlanCreate(Query *originalQuery, Query *queryTree,
					   PlannerRestrictionContext *plannerRestrictionContext,
					   ParamListInfo boundParams)
{
	MultiNode *multiQueryNode = NULL;
	MultiTreeRoot *rootNode = NULL;

	/*
	 * We check the existence of subqueries in FROM clause on the modified query
	 * given that if postgres already flattened the subqueries, MultiPlanTree()
	 * can plan corresponding distributed plan.
	 *
	 * We also check the existence of subqueries in WHERE clause. Note that
	 * this check needs to be done on the original query given that
	 * standard_planner() may replace the sublinks with anti/semi joins and
	 * MultiPlanTree() cannot plan such queries.
	 */
	if (SubqueryEntryList(queryTree) != NIL || SublinkList(originalQuery) != NIL)
	{
		originalQuery = (Query *) ResolveExternalParams((Node *) originalQuery,
														boundParams);

		multiQueryNode = MultiSubqueryPlanTree(originalQuery, queryTree,
											   plannerRestrictionContext);
	}
	else
	{
		multiQueryNode = MultiPlanTree(queryTree);
	}

	/* add a root node to serve as the permanent handle to the tree */
	rootNode = CitusMakeNode(MultiTreeRoot);
	SetChild((MultiUnaryNode *) rootNode, multiQueryNode);

	return rootNode;
}


/*
 * ResolveExternalParams replaces the external parameters that appears
 * in the query with the corresponding entries in the boundParams.
 *
 * Note that this function is inspired by eval_const_expr() on Postgres.
 * We cannot use that function because it requires access to PlannerInfo.
 */
static Node *
ResolveExternalParams(Node *inputNode, ParamListInfo boundParams)
{
	/* consider resolving external parameters only when boundParams exists */
	if (!boundParams)
	{
		return inputNode;
	}

	if (inputNode == NULL)
	{
		return NULL;
	}

	if (IsA(inputNode, Param))
	{
		Param *paramToProcess = (Param *) inputNode;
		ParamExternData *correspondingParameterData = NULL;
		int numberOfParameters = boundParams->numParams;
		int parameterId = paramToProcess->paramid;
		int16 typeLength = 0;
		bool typeByValue = false;
		Datum constValue = 0;
		bool paramIsNull = false;
		int parameterIndex = 0;

		if (paramToProcess->paramkind != PARAM_EXTERN)
		{
			return inputNode;
		}

		if (parameterId < 0)
		{
			return inputNode;
		}

		/* parameterId starts from 1 */
		parameterIndex = parameterId - 1;
		if (parameterIndex >= numberOfParameters)
		{
			return inputNode;
		}

		correspondingParameterData = &boundParams->params[parameterIndex];

		if (!(correspondingParameterData->pflags & PARAM_FLAG_CONST))
		{
			return inputNode;
		}

		get_typlenbyval(paramToProcess->paramtype, &typeLength, &typeByValue);

		paramIsNull = correspondingParameterData->isnull;
		if (paramIsNull)
		{
			constValue = 0;
		}
		else if (typeByValue)
		{
			constValue = correspondingParameterData->value;
		}
		else
		{
			/*
			 * Out of paranoia ensure that datum lives long enough,
			 * although bind params currently should always live
			 * long enough.
			 */
			constValue = datumCopy(correspondingParameterData->value, typeByValue,
								   typeLength);
		}

		return (Node *) makeConst(paramToProcess->paramtype, paramToProcess->paramtypmod,
								  paramToProcess->paramcollid, typeLength, constValue,
								  paramIsNull, typeByValue);
	}
	else if (IsA(inputNode, Query))
	{
		return (Node *) query_tree_mutator((Query *) inputNode, ResolveExternalParams,
										   boundParams, 0);
	}

	return expression_tree_mutator(inputNode, ResolveExternalParams, boundParams);
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
	Node *queryQuals = NULL;
	List *sublinkList = NIL;

	if (!joinTree)
	{
		return NIL;
	}

	queryQuals = joinTree->quals;
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
 * MultiSubqueryPlanTree gets the query objects and returns logical plan
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
static MultiNode *
MultiSubqueryPlanTree(Query *originalQuery, Query *queryTree,
					  PlannerRestrictionContext *plannerRestrictionContext)
{
	MultiNode *multiQueryNode = NULL;
	DeferredErrorMessage *subqueryPushdownError = NULL;

	/*
	 * This is a generic error check that applies to both subquery pushdown
	 * and single table repartition subquery.
	 */
	ErrorIfQueryNotSupported(originalQuery);

	/*
	 * In principle, we're first trying subquery pushdown planner. If it fails
	 * to create a logical plan, continue with trying the single table
	 * repartition subquery planning.
	 */
	subqueryPushdownError = DeferErrorIfUnsupportedSubqueryPushdown(originalQuery,
																	plannerRestrictionContext);
	if (!subqueryPushdownError)
	{
		multiQueryNode = SubqueryPushdownMultiPlanTree(originalQuery);
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
		multiQueryNode = MultiPlanTree(queryTree);
	}

	Assert(multiQueryNode != NULL);

	return multiQueryNode;
}


/*
 * SingleRelationRepartitionSubquery returns true if it is eligible single
 * repartition query planning in the sense that:
 *   - None of the levels of the subquery contains a join
 *   - Only a single RTE_RELATION exists, which means only a single table
 *     name is specified on the whole query
 *   - No sublinks exists in the subquery
 *
 * Note that the caller should still call DeferErrorIfUnsupportedSubqueryRepartition()
 * to ensure that Citus supports the subquery. Also, this function is designed to run
 * on the original query.
 */
static bool
SingleRelationRepartitionSubquery(Query *queryTree)
{
	List *rangeTableIndexList = NULL;
	RangeTblEntry *rangeTableEntry = NULL;
	List *rangeTableList = queryTree->rtable;
	int rangeTableIndex = 0;

	/* we don't support subqueries in WHERE */
	if (queryTree->hasSubLinks)
	{
		return false;
	}

	/*
	 * Don't allow joins and set operations. If join appears in the queryTree, the
	 * length would be greater than 1. If only set operations exists, the length
	 * would be 0.
	 */
	ExtractRangeTableIndexWalker((Node *) queryTree->jointree,
								 &rangeTableIndexList);
	if (list_length(rangeTableIndexList) != 1)
	{
		return false;
	}

	rangeTableIndex = linitial_int(rangeTableIndexList);
	rangeTableEntry = rt_fetch(rangeTableIndex, rangeTableList);
	if (rangeTableEntry->rtekind == RTE_RELATION)
	{
		return true;
	}
	else if (rangeTableEntry->rtekind == RTE_SUBQUERY)
	{
		Query *subqueryTree = rangeTableEntry->subquery;

		return SingleRelationRepartitionSubquery(subqueryTree);
	}

	return false;
}


/*
 * DeferErrorIfContainsUnsupportedSubqueryPushdown iterates on the query's subquery
 * entry list and uses helper functions to check if we can push down subquery
 * to worker nodes. These helper functions returns a deferred error if we
 * cannot push down the subquery.
 */
static DeferredErrorMessage *
DeferErrorIfUnsupportedSubqueryPushdown(Query *originalQuery,
										PlannerRestrictionContext *
										plannerRestrictionContext)
{
	bool outerMostQueryHasLimit = false;
	ListCell *subqueryCell = NULL;
	List *subqueryList = NIL;
	DeferredErrorMessage *error = NULL;
	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;

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
		if (!SafeToPushdownUnionSubquery(relationRestrictionContext))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot pushdown the subquery since all leaves of "
								 "the UNION does not include partition key at the "
								 "same position",
								 "Each leaf query of the UNION should return "
								 "partition key at the same position on its "
								 "target list.", NULL);
		}
	}
	else if (!RestrictionEquivalenceForPartitionKeys(plannerRestrictionContext))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot pushdown the subquery since all relations are not "
							 "joined using distribution keys",
							 "Each relation should be joined with at least "
							 "one another relation using distribution keys and "
							 "equality operator.", NULL);
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

		error = DeferErrorIfUnsupportedFilters(subquery);
		if (error)
		{
			return error;
		}
	}

	return NULL;
}


/*
 * DeferErrorIfUnsupportedFilters checks if all leaf queries in the given query have
 * same filter on the partition column. Note that if there are queries without
 * any filter on the partition column, they don't break this prerequisite.
 */
static DeferredErrorMessage *
DeferErrorIfUnsupportedFilters(Query *subquery)
{
	List *queryList = NIL;
	ListCell *queryCell = NULL;
	List *subqueryOpExpressionList = NIL;
	List *relationIdList = RelationIdList(subquery);
	Var *partitionColumn = NULL;
	Oid relationId = InvalidOid;

	/*
	 * If there are no appropriate relations, we're going to error out on
	 * DeferErrorIfCannotPushdownSubquery(). It may happen once the subquery
	 * does not include a relation.
	 */
	if (relationIdList == NIL)
	{
		return NULL;
	}

	/*
	 * Get relation id of any relation in the subquery and create partiton column
	 * for this relation. We will use this column to replace columns on operator
	 * expressions on different tables. Then we compare these operator expressions
	 * to see if they consist of same operator and constant value.
	 */
	relationId = linitial_oid(relationIdList);
	partitionColumn = PartitionColumn(relationId, 0);

	ExtractQueryWalker((Node *) subquery, &queryList);
	foreach(queryCell, queryList)
	{
		Query *query = (Query *) lfirst(queryCell);
		List *opExpressionList = NIL;
		List *newOpExpressionList = NIL;

		bool leafQuery = LeafQuery(query);
		if (!leafQuery)
		{
			continue;
		}

		opExpressionList = PartitionColumnOpExpressionList(query);
		if (opExpressionList == NIL)
		{
			continue;
		}

		newOpExpressionList = ReplaceColumnsInOpExpressionList(opExpressionList,
															   partitionColumn);

		if (subqueryOpExpressionList == NIL)
		{
			subqueryOpExpressionList = newOpExpressionList;
		}
		else
		{
			bool equalOpExpressionLists = EqualOpExpressionLists(subqueryOpExpressionList,
																 newOpExpressionList);
			if (!equalOpExpressionLists)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot push down this subquery",
									 "Currently all leaf queries need to "
									 "have same filters on partition column", NULL);
			}
		}
	}

	return NULL;
}


/*
 * EqualOpExpressionLists checks if given two operator expression lists are
 * equal.
 */
static bool
EqualOpExpressionLists(List *firstOpExpressionList, List *secondOpExpressionList)
{
	bool equalOpExpressionLists = false;
	ListCell *firstOpExpressionCell = NULL;
	uint32 equalOpExpressionCount = 0;
	uint32 firstOpExpressionCount = list_length(firstOpExpressionList);
	uint32 secondOpExpressionCount = list_length(secondOpExpressionList);

	if (firstOpExpressionCount != secondOpExpressionCount)
	{
		return false;
	}

	foreach(firstOpExpressionCell, firstOpExpressionList)
	{
		OpExpr *firstOpExpression = (OpExpr *) lfirst(firstOpExpressionCell);
		ListCell *secondOpExpressionCell = NULL;

		foreach(secondOpExpressionCell, secondOpExpressionList)
		{
			OpExpr *secondOpExpression = (OpExpr *) lfirst(secondOpExpressionCell);
			bool equalExpressions = equal(firstOpExpression, secondOpExpression);

			if (equalExpressions)
			{
				equalOpExpressionCount++;
				continue;
			}
		}
	}

	if (equalOpExpressionCount == firstOpExpressionCount)
	{
		equalOpExpressionLists = true;
	}

	return equalOpExpressionLists;
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
 * This function is very similar to ErrorIfQueryNotSupported() in logical
 * planner, but we don't reuse it, because differently for subqueries we support
 * a subset of distinct, union and left joins.
 *
 * Note that this list of checks is not exhaustive, there can be some cases
 * which we let subquery to run but returned results would be wrong. Such as if
 * a subquery has a group by on another subquery which includes order by with
 * limit, we let this query to run, but results could be wrong depending on the
 * features of underlying tables.
 */
static DeferredErrorMessage *
DeferErrorIfCannotPushdownSubquery(Query *subqueryTree, bool outerMostQueryHasLimit)
{
	bool preconditionsSatisfied = true;
	char *errorDetail = NULL;
	DeferredErrorMessage *deferredError = NULL;

	deferredError = DeferErrorIfUnsupportedTableCombination(subqueryTree);
	if (deferredError)
	{
		return deferredError;
	}

	if (subqueryTree->rtable == NIL)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries without relations are unsupported";
	}

	if (subqueryTree->hasWindowFuncs)
	{
		preconditionsSatisfied = false;
		errorDetail = "Window functions are currently unsupported";
	}

	if (subqueryTree->limitOffset)
	{
		preconditionsSatisfied = false;
		errorDetail = "Offset clause is currently unsupported";
	}

	/* limit is not supported when SubqueryPushdown is not set */
	if (subqueryTree->limitCount && !SubqueryPushdown)
	{
		preconditionsSatisfied = false;
		errorDetail = "Limit in subquery is currently unsupported";
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
		deferredError = DeferErrorIfUnsupportedUnionQuery(subqueryTree,
														  outerMostQueryHasLimit);
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
						  "unsupported";
		}
	}

	/* we don't support aggregates without group by */
	if (subqueryTree->hasAggs && (subqueryTree->groupClause == NULL))
	{
		preconditionsSatisfied = false;
		errorDetail = "Aggregates without group by are currently unsupported";
	}

	/* having clause without group by on partition column is not supported */
	if (subqueryTree->havingQual && (subqueryTree->groupClause == NULL))
	{
		preconditionsSatisfied = false;
		errorDetail = "Having qual without group by on partition column is "
					  "currently unsupported";
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
 * DeferErrorIfUnsupportedUnionQuery is a helper function for ErrorIfCannotPushdownSubquery().
 * The function also errors out for set operations INTERSECT and EXCEPT.
 */
static DeferredErrorMessage *
DeferErrorIfUnsupportedUnionQuery(Query *subqueryTree,
								  bool outerMostQueryHasLimit)
{
	List *setOperationStatementList = NIL;
	ListCell *setOperationStatmentCell = NULL;

	ExtractSetOperationStatmentWalker((Node *) subqueryTree->setOperations,
									  &setOperationStatementList);
	foreach(setOperationStatmentCell, setOperationStatementList)
	{
		SetOperationStmt *setOperation =
			(SetOperationStmt *) lfirst(setOperationStatmentCell);

		if (setOperation->op != SETOP_UNION)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot push down this subquery",
								 "Intersect and Except are currently unsupported", NULL);
		}
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
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, SetOperationStmt))
	{
		SetOperationStmt *setOperation = (SetOperationStmt *) node;

		(*setOperationList) = lappend(*setOperationList, setOperation);
	}

	walkerResult = expression_tree_walker(node, ExtractSetOperationStatmentWalker,
										  setOperationList);

	return walkerResult;
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
	bool unsupporteTableCombination = false;
	char *errorDetail = NULL;
	uint32 relationRangeTableCount = 0;
	uint32 subqueryRangeTableCount = 0;

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
		 * Check if the range table in the join tree is a simple relation or a
		 * subquery.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			relationRangeTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_SUBQUERY)
		{
			subqueryRangeTableCount++;
		}
		else
		{
			unsupporteTableCombination = true;
			errorDetail = "Table expressions other than simple relations and "
						  "subqueries are currently unsupported";
			break;
		}
	}

	/* finally check and error out if not satisfied */
	if (unsupporteTableCombination)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot push down this subquery",
							 errorDetail, NULL);
	}

	return NULL;
}


/*
 * TargetListOnPartitionColumn checks if at least one target list entry is on
 * partition column.
 */
static bool
TargetListOnPartitionColumn(Query *query, List *targetEntryList)
{
	bool targetListOnPartitionColumn = false;
	List *compositeFieldList = NIL;

	ListCell *targetEntryCell = NULL;
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Expr *targetExpression = targetEntry->expr;

		bool isPartitionColumn = IsPartitionColumn(targetExpression, query);
		if (isPartitionColumn)
		{
			FieldSelect *compositeField = CompositeFieldRecursive(targetExpression,
																  query);
			if (compositeField)
			{
				compositeFieldList = lappend(compositeFieldList, compositeField);
			}
			else
			{
				targetListOnPartitionColumn = true;
				break;
			}
		}
	}

	/* check composite fields */
	if (!targetListOnPartitionColumn)
	{
		bool fullCompositeFieldList = FullCompositeFieldList(compositeFieldList);
		if (fullCompositeFieldList)
		{
			targetListOnPartitionColumn = true;
		}
	}

	return targetListOnPartitionColumn;
}


/*
 * FullCompositeFieldList gets a composite field list, and checks if all fields
 * of composite type are used in the list.
 */
static bool
FullCompositeFieldList(List *compositeFieldList)
{
	bool fullCompositeFieldList = true;
	bool *compositeFieldArray = NULL;
	uint32 compositeFieldCount = 0;
	uint32 fieldIndex = 0;

	ListCell *fieldSelectCell = NULL;
	foreach(fieldSelectCell, compositeFieldList)
	{
		FieldSelect *fieldSelect = (FieldSelect *) lfirst(fieldSelectCell);
		uint32 compositeFieldIndex = 0;

		Expr *fieldExpression = fieldSelect->arg;
		if (!IsA(fieldExpression, Var))
		{
			continue;
		}

		if (compositeFieldArray == NULL)
		{
			uint32 index = 0;
			Var *compositeColumn = (Var *) fieldExpression;
			Oid compositeTypeId = compositeColumn->vartype;
			Oid compositeRelationId = get_typ_typrelid(compositeTypeId);

			/* get composite type attribute count */
			Relation relation = relation_open(compositeRelationId, AccessShareLock);
			compositeFieldCount = relation->rd_att->natts;
			compositeFieldArray = palloc0(compositeFieldCount * sizeof(bool));
			relation_close(relation, AccessShareLock);

			for (index = 0; index < compositeFieldCount; index++)
			{
				compositeFieldArray[index] = false;
			}
		}

		compositeFieldIndex = fieldSelect->fieldnum - 1;
		compositeFieldArray[compositeFieldIndex] = true;
	}

	for (fieldIndex = 0; fieldIndex < compositeFieldCount; fieldIndex++)
	{
		if (!compositeFieldArray[fieldIndex])
		{
			fullCompositeFieldList = false;
		}
	}

	if (compositeFieldCount == 0)
	{
		fullCompositeFieldList = false;
	}

	return fullCompositeFieldList;
}


/*
 * CompositeFieldRecursive recursively finds composite field in the query tree
 * referred by given expression. If expression does not refer to a composite
 * field, then it returns NULL.
 *
 * If expression is a field select we directly return composite field. If it is
 * a column is referenced from a subquery, then we recursively check that subquery
 * until we reach the source of that column, and find composite field. If this
 * column is referenced from join range table entry, then we resolve which join
 * column it refers and recursively use this column with the same query.
 */
static FieldSelect *
CompositeFieldRecursive(Expr *expression, Query *query)
{
	FieldSelect *compositeField = NULL;
	List *rangetableList = query->rtable;
	Index rangeTableEntryIndex = 0;
	RangeTblEntry *rangeTableEntry = NULL;
	Var *candidateColumn = NULL;

	if (IsA(expression, FieldSelect))
	{
		compositeField = (FieldSelect *) expression;
		return compositeField;
	}

	if (IsA(expression, Var))
	{
		candidateColumn = (Var *) expression;
	}
	else
	{
		return NULL;
	}

	rangeTableEntryIndex = candidateColumn->varno - 1;
	rangeTableEntry = list_nth(rangetableList, rangeTableEntryIndex);

	if (rangeTableEntry->rtekind == RTE_SUBQUERY)
	{
		Query *subquery = rangeTableEntry->subquery;
		List *targetEntryList = subquery->targetList;
		AttrNumber targetEntryIndex = candidateColumn->varattno - 1;
		TargetEntry *subqueryTargetEntry = list_nth(targetEntryList, targetEntryIndex);

		Expr *subqueryExpression = subqueryTargetEntry->expr;
		compositeField = CompositeFieldRecursive(subqueryExpression, subquery);
	}
	else if (rangeTableEntry->rtekind == RTE_JOIN)
	{
		List *joinColumnList = rangeTableEntry->joinaliasvars;
		AttrNumber joinColumnIndex = candidateColumn->varattno - 1;
		Expr *joinColumn = list_nth(joinColumnList, joinColumnIndex);

		compositeField = CompositeFieldRecursive(joinColumn, query);
	}

	return compositeField;
}


/*
 * SubqueryEntryList finds the subquery nodes in the range table entry list, and
 * builds a list of subquery range table entries from these subquery nodes. Range
 * table entry list also includes subqueries which are pulled up. We don't want
 * to add pulled up subqueries to list, so we walk over join tree indexes and
 * check range table entries referenced in the join tree.
 */
List *
SubqueryEntryList(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	List *subqueryEntryList = NIL;
	List *joinTreeTableIndexList = NIL;
	ListCell *joinTreeTableIndexCell = NULL;

	/*
	 * Extract all range table indexes from the join tree. Note that here we
	 * only walk over range table entries at this level and do not recurse into
	 * subqueries.
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

		if (rangeTableEntry->rtekind == RTE_SUBQUERY)
		{
			subqueryEntryList = lappend(subqueryEntryList, rangeTableEntry);
		}
	}

	return subqueryEntryList;
}


/*
 * MultiPlanTree takes in a parsed query tree and uses that tree to construct a
 * logical plan. This plan is based on multi-relational algebra. This function
 * creates the logical plan in several steps.
 *
 * First, the function checks if there is a subquery. If there is a subquery
 * it recursively creates nested multi trees. If this query has a subquery, the
 * function does not create any join trees and jumps to last step.
 *
 * If there is no subquery, the function calculates the join order using tables
 * in the query and join clauses between the tables. Second, the function
 * starts building the logical plan from the bottom-up, and begins with the table
 * and collect nodes. Third, the function builds the join tree using the join
 * order information and table nodes.
 *
 * In the last step, the function adds the select, project, aggregate, sort,
 * group, and limit nodes if they appear in the original query tree.
 */
static MultiNode *
MultiPlanTree(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	List *targetEntryList = queryTree->targetList;
	List *whereClauseList = NIL;
	List *joinClauseList = NIL;
	List *joinOrderList = NIL;
	List *tableEntryList = NIL;
	List *tableNodeList = NIL;
	List *collectTableList = NIL;
	List *subqueryEntryList = NIL;
	MultiNode *joinTreeNode = NULL;
	MultiSelect *selectNode = NULL;
	MultiProject *projectNode = NULL;
	MultiExtendedOp *extendedOpNode = NULL;
	MultiNode *currentTopNode = NULL;

	/* verify we can perform distributed planning on this query */
	ErrorIfQueryNotSupported(queryTree);

	/* extract where clause qualifiers and verify we can plan for them */
	whereClauseList = WhereClauseList(queryTree->jointree);
	ValidateClauseList(whereClauseList);

	/*
	 * If we have a subquery, build a multi table node for the subquery and
	 * add a collect node on top of the multi table node.
	 */
	subqueryEntryList = SubqueryEntryList(queryTree);
	if (subqueryEntryList != NIL)
	{
		RangeTblEntry *subqueryRangeTableEntry = NULL;
		MultiCollect *subqueryCollectNode = CitusMakeNode(MultiCollect);
		MultiTable *subqueryNode = NULL;
		MultiNode *subqueryExtendedNode = NULL;
		Query *subqueryTree = NULL;
		List *whereClauseColumnList = NIL;
		List *targetListColumnList = NIL;
		List *columnList = NIL;
		ListCell *columnCell = NULL;

		/* we only support single subquery in the entry list */
		Assert(list_length(subqueryEntryList) == 1);

		subqueryRangeTableEntry = (RangeTblEntry *) linitial(subqueryEntryList);
		subqueryTree = subqueryRangeTableEntry->subquery;

		/* ensure if subquery satisfies preconditions */
		Assert(DeferErrorIfUnsupportedSubqueryRepartition(subqueryTree) == NULL);

		subqueryNode = CitusMakeNode(MultiTable);
		subqueryNode->relationId = SUBQUERY_RELATION_ID;
		subqueryNode->rangeTableId = SUBQUERY_RANGE_TABLE_ID;
		subqueryNode->partitionColumn = NULL;
		subqueryNode->alias = NULL;
		subqueryNode->referenceNames = NULL;

		/*
		 * We disregard pulled subqueries. This changes order of range table list.
		 * We do not allow subquery joins, so we will have only one range table
		 * entry in range table list after dropping pulled subquery. For this
		 * reason, here we are updating columns in the most outer query for where
		 * clause list and target list accordingly.
		 */
		Assert(list_length(subqueryEntryList) == 1);

		whereClauseColumnList = pull_var_clause_default((Node *) whereClauseList);
		targetListColumnList = pull_var_clause_default((Node *) targetEntryList);

		columnList = list_concat(whereClauseColumnList, targetListColumnList);
		foreach(columnCell, columnList)
		{
			Var *column = (Var *) lfirst(columnCell);
			column->varno = 1;
		}

		/* recursively create child nested multitree */
		subqueryExtendedNode = MultiPlanTree(subqueryTree);

		SetChild((MultiUnaryNode *) subqueryCollectNode, (MultiNode *) subqueryNode);
		SetChild((MultiUnaryNode *) subqueryNode, subqueryExtendedNode);

		currentTopNode = (MultiNode *) subqueryCollectNode;
	}
	else
	{
		bool hasOuterJoin = false;

		/*
		 * We calculate the join order using the list of tables in the query and
		 * the join clauses between them. Note that this function owns the table
		 * entry list's memory, and JoinOrderList() shallow copies the list's
		 * elements.
		 */
		joinClauseList = JoinClauseList(whereClauseList);
		tableEntryList = UsedTableEntryList(queryTree);

		/* build the list of multi table nodes */
		tableNodeList = MultiTableNodeList(tableEntryList, rangeTableList);

		/* add collect nodes on top of the multi table nodes */
		collectTableList = AddMultiCollectNodes(tableNodeList);

		hasOuterJoin = HasOuterJoin(queryTree);
		if (hasOuterJoin)
		{
			/* use the user-defined join order when there are outer joins */
			joinOrderList = FixedJoinOrderList(queryTree->jointree, tableEntryList);
		}
		else
		{
			/* find best join order for commutative inner joins */
			joinOrderList = JoinOrderList(tableEntryList, joinClauseList);
		}

		/* build join tree using the join order and collected tables */
		joinTreeNode = MultiJoinTree(joinOrderList, collectTableList, joinClauseList);

		currentTopNode = joinTreeNode;
	}

	Assert(currentTopNode != NULL);

	/* build select node if the query has selection criteria */
	selectNode = MultiSelectNode(whereClauseList);
	if (selectNode != NULL)
	{
		SetChild((MultiUnaryNode *) selectNode, currentTopNode);
		currentTopNode = (MultiNode *) selectNode;
	}

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
	SetChild((MultiUnaryNode *) extendedOpNode, currentTopNode);
	currentTopNode = (MultiNode *) extendedOpNode;

	return currentTopNode;
}


/*
 * ErrorIfQueryNotSupported checks that we can perform distributed planning for
 * the given query. The checks in this function will be removed as we support
 * more functionality in our distributed planning.
 */
static void
ErrorIfQueryNotSupported(Query *queryTree)
{
	char *errorMessage = NULL;
	bool hasTablesample = false;
	bool hasUnsupportedJoin = false;
	bool hasComplexJoinOrder = false;
	bool hasComplexRangeTableType = false;
	bool preconditionsSatisfied = true;
	const char *errorHint = NULL;
	const char *joinHint = "Consider joining tables on partition column and have "
						   "equal filter on joining columns.";
	const char *filterHint = "Consider using an equality filter on the distributed "
							 "table's partition column.";

	/*
	 * There could be Sublinks in the target list as well. To produce better
	 * error messages we're checking sublinks in the where clause.
	 */
	if (queryTree->hasSubLinks && SublinkList(queryTree) == NIL)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with subquery outside the "
					   "FROM and WHERE clauses";
		errorHint = filterHint;
	}

	if (queryTree->hasWindowFuncs)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with window functions";
		errorHint = filterHint;
	}

	if (queryTree->setOperations)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with UNION, INTERSECT, or "
					   "EXCEPT";
		errorHint = filterHint;
	}

	if (queryTree->hasRecursive)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with RECURSIVE";
		errorHint = filterHint;
	}

	if (queryTree->cteList)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with common table expressions";
		errorHint = filterHint;
	}

	if (queryTree->hasForUpdate)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with FOR UPDATE/SHARE commands";
		errorHint = filterHint;
	}

	if (queryTree->distinctClause)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with DISTINCT clause";
		errorHint = filterHint;
	}

	if (queryTree->groupingSets)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with GROUPING SETS, CUBE, "
					   "or ROLLUP";
		errorHint = filterHint;
	}

	hasTablesample = HasTablesample(queryTree);
	if (hasTablesample)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query which use TABLESAMPLE";
		errorHint = filterHint;
	}

	hasUnsupportedJoin = HasUnsupportedJoinWalker((Node *) queryTree->jointree, NULL);
	if (hasUnsupportedJoin)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with join types other than "
					   "INNER or OUTER JOINS";
		errorHint = joinHint;
	}

	hasComplexJoinOrder = HasComplexJoinOrder(queryTree);
	if (hasComplexJoinOrder)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with complex join orders";
		errorHint = joinHint;
	}

	hasComplexRangeTableType = HasComplexRangeTableType(queryTree);
	if (hasComplexRangeTableType)
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with complex table expressions";
		errorHint = filterHint;
	}


	/* finally check and error out if not satisfied */
	if (!preconditionsSatisfied)
	{
		bool showHint = ErrorHintRequired(errorHint, queryTree);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s", errorMessage),
						showHint ? errhint("%s", errorHint) : 0));
	}
}


/* HasTablesample returns tree if the query contains tablesample */
static bool
HasTablesample(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	ListCell *rangeTableEntryCell = NULL;
	bool hasTablesample = false;

	foreach(rangeTableEntryCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = lfirst(rangeTableEntryCell);
		if (rangeTableEntry->tablesample)
		{
			hasTablesample = true;
			break;
		}
	}

	return hasTablesample;
}


/*
 * HasUnsupportedJoinWalker returns tree if the query contains an unsupported
 * join type. We currently support inner, left, right, full and anti joins.
 * Semi joins are not supported. A full description of these join types is
 * included in nodes/nodes.h.
 */
static bool
HasUnsupportedJoinWalker(Node *node, void *context)
{
	bool hasUnsupportedJoin = false;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) node;
		JoinType joinType = joinExpr->jointype;
		bool outerJoin = IS_OUTER_JOIN(joinType);
		if (!outerJoin && joinType != JOIN_INNER)
		{
			hasUnsupportedJoin = true;
		}
	}

	if (!hasUnsupportedJoin)
	{
		hasUnsupportedJoin = expression_tree_walker(node, HasUnsupportedJoinWalker,
													NULL);
	}

	return hasUnsupportedJoin;
}


/*
 * ErrorHintRequired returns true if error hint shold be displayed with the
 * query error message. Error hint is valid only for queries involving reference
 * and hash partitioned tables. If more than one hash distributed table is
 * present we display the hint only if the tables are colocated. If the query
 * only has reference table(s), then it is handled by router planner.
 */
static bool
ErrorHintRequired(const char *errorHint, Query *queryTree)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	List *colocationIdList = NIL;

	if (errorHint == NULL)
	{
		return false;
	}

	ExtractRangeTableRelationWalker((Node *) queryTree, &rangeTableList);
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rangeTableCell);
		Oid relationId = rte->relid;
		char partitionMethod = PartitionMethod(relationId);
		if (partitionMethod == DISTRIBUTE_BY_NONE)
		{
			continue;
		}
		else if (partitionMethod == DISTRIBUTE_BY_HASH)
		{
			int colocationId = TableColocationId(relationId);
			colocationIdList = list_append_unique_int(colocationIdList, colocationId);
		}
		else
		{
			return false;
		}
	}

	/* do not display the hint if there are more than one colocation group */
	if (list_length(colocationIdList) > 1)
	{
		return false;
	}

	return true;
}


/*
 * DeferErrorIfSubqueryNotSupported checks that we can perform distributed planning for
 * the given subquery. If not, a deferred error is returned. The function recursively
 * does this check to all lower levels of the subquery.
 */
static DeferredErrorMessage *
DeferErrorIfUnsupportedSubqueryRepartition(Query *subqueryTree)
{
	char *errorDetail = NULL;
	bool preconditionsSatisfied = true;
	List *joinTreeTableIndexList = NIL;
	int rangeTableIndex = 0;
	RangeTblEntry *rangeTableEntry = NULL;
	Query *innerSubquery = NULL;

	if (!subqueryTree->hasAggs)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries without aggregates are not supported yet";
	}

	if (subqueryTree->groupClause == NIL)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries without group by clause are not supported yet";
	}

	if (subqueryTree->sortClause != NULL)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries with order by clause are not supported yet";
	}

	if (subqueryTree->limitCount != NULL)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries with limit are not supported yet";
	}

	if (subqueryTree->limitOffset != NULL)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries with offset are not supported yet";
	}

	if (subqueryTree->hasSubLinks)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries other than from-clause subqueries are unsupported";
	}

	/* finally check and return error if conditions are not satisfied */
	if (!preconditionsSatisfied)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot perform distributed planning on this query",
							 errorDetail, NULL);
	}

	/*
	 * Extract all range table indexes from the join tree. Note that sub-queries
	 * that get pulled up by PostgreSQL don't appear in this join tree.
	 */
	ExtractRangeTableIndexWalker((Node *) subqueryTree->jointree,
								 &joinTreeTableIndexList);
	Assert(list_length(joinTreeTableIndexList) == 1);

	/* continue with the inner subquery */
	rangeTableIndex = linitial_int(joinTreeTableIndexList);
	rangeTableEntry = rt_fetch(rangeTableIndex, subqueryTree->rtable);
	if (rangeTableEntry->rtekind == RTE_RELATION)
	{
		return NULL;
	}

	Assert(rangeTableEntry->rtekind == RTE_SUBQUERY);
	innerSubquery = rangeTableEntry->subquery;

	/* recursively continue to the inner subqueries */
	return DeferErrorIfUnsupportedSubqueryRepartition(innerSubquery);
}


/*
 * HasOuterJoin returns true if query has a outer join.
 */
static bool
HasOuterJoin(Query *queryTree)
{
	bool hasOuterJoin = HasOuterJoinWalker((Node *) queryTree->jointree, NULL);

	return hasOuterJoin;
}


/*
 * HasOuterJoinWalker returns true if the query has an outer join. The context
 * parameter should be NULL.
 */
static bool
HasOuterJoinWalker(Node *node, void *context)
{
	bool hasOuterJoin = false;
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
			hasOuterJoin = true;
		}
	}

	if (!hasOuterJoin)
	{
		hasOuterJoin = expression_tree_walker(node, HasOuterJoinWalker, NULL);
	}

	return hasOuterJoin;
}


/*
 * HasComplexJoinOrder returns true if join tree is not a left-handed tree i.e.
 * it has a join expression in at least one right argument.
 */
static bool
HasComplexJoinOrder(Query *queryTree)
{
	bool hasComplexJoinOrder = false;
	List *joinList = NIL;
	ListCell *joinCell = NULL;

	joinList = JoinExprList(queryTree->jointree);
	foreach(joinCell, joinList)
	{
		JoinExpr *joinExpr = lfirst(joinCell);
		if (IsA(joinExpr->rarg, JoinExpr))
		{
			hasComplexJoinOrder = true;
			break;
		}
	}

	return hasComplexJoinOrder;
}


/*
 * HasComplexRangeTableType checks if the given query tree contains any complex
 * range table types. For this, the function walks over all range tables in the
 * join tree, and checks if they correspond to simple relations or subqueries.
 * If they don't, the function assumes the query has complex range tables.
 */
static bool
HasComplexRangeTableType(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	List *joinTreeTableIndexList = NIL;
	ListCell *joinTreeTableIndexCell = NULL;
	bool hasComplexRangeTableType = false;

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
		 * Check if the range table in the join tree is a simple relation or a
		 * subquery.
		 */
		if (rangeTableEntry->rtekind != RTE_RELATION &&
			rangeTableEntry->rtekind != RTE_SUBQUERY)
		{
			hasComplexRangeTableType = true;
		}

		/*
		 * Check if the subquery range table entry includes children inheritance.
		 *
		 * Note that PostgreSQL flattens out simple union all queries into an
		 * append relation, sets "inh" field of RangeTblEntry to true and deletes
		 * set operations. Here we check this for subqueries.
		 */
		if (rangeTableEntry->rtekind == RTE_SUBQUERY && rangeTableEntry->inh)
		{
			hasComplexRangeTableType = true;
		}
	}

	return hasComplexRangeTableType;
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
 * WhereClauseList walks over the FROM expression in the query tree, and builds
 * a list of all clauses from the expression tree. The function checks for both
 * implicitly and explicitly defined clauses, but only selects INNER join
 * explicit clauses, and skips any outer-join clauses. Explicit clauses are
 * expressed as "SELECT ... FROM R1 INNER JOIN R2 ON R1.A = R2.A". Implicit
 * joins differ in that they live in the WHERE clause, and are expressed as
 * "SELECT ... FROM ... WHERE R1.a = R2.a".
 */
List *
WhereClauseList(FromExpr *fromExpr)
{
	FromExpr *fromExprCopy = copyObject(fromExpr);
	QualifierWalkerContext *walkerContext = palloc0(sizeof(QualifierWalkerContext));
	List *whereClauseList = NIL;

	ExtractFromExpressionWalker((Node *) fromExprCopy, walkerContext);
	whereClauseList = walkerContext->baseQualifierList;

	return whereClauseList;
}


/*
 * QualifierList walks over the FROM expression in the query tree, and builds
 * a list of all qualifiers from the expression tree. The function checks for
 * both implicitly and explicitly defined qualifiers. Note that this function
 * is very similar to WhereClauseList(), but QualifierList() also includes
 * outer-join clauses.
 */
List *
QualifierList(FromExpr *fromExpr)
{
	FromExpr *fromExprCopy = copyObject(fromExpr);
	QualifierWalkerContext *walkerContext = palloc0(sizeof(QualifierWalkerContext));
	List *qualifierList = NIL;

	ExtractFromExpressionWalker((Node *) fromExprCopy, walkerContext);
	qualifierList = list_concat(qualifierList, walkerContext->baseQualifierList);
	qualifierList = list_concat(qualifierList, walkerContext->outerJoinQualifierList);

	return qualifierList;
}


/*
 * ValidateClauseList walks over the given list of clauses, and checks that we
 * can recognize all the clauses. This function ensures that we do not drop an
 * unsupported clause type on the floor, and thus prevents erroneous results.
 */
static void
ValidateClauseList(List *clauseList)
{
	ListCell *clauseCell = NULL;
	foreach(clauseCell, clauseList)
	{
		Node *clause = (Node *) lfirst(clauseCell);

		/*
		 * There could never be sublinks here given that it is handled
		 * in subquery pushdown code-path.
		 */
		Assert(!IsSublinkClause(clause));

		if (!(IsSelectClause(clause) || IsJoinClause(clause) || or_clause(clause)))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("unsupported clause type")));
		}
	}
}


/*
 * ValidateSubqueryPushdownClauseList walks over the given list of clauses,
 * and checks that we can recognize all the clauses. This function ensures
 * that we do not drop an unsupported clause type on the floor, and thus
 * prevents erroneous results.
 *
 * Note that this function is slightly different than ValidateClauseList(),
 * additionally allowing sublinks.
 */
static void
ValidateSubqueryPushdownClauseList(List *clauseList)
{
	ListCell *clauseCell = NULL;
	foreach(clauseCell, clauseList)
	{
		Node *clause = (Node *) lfirst(clauseCell);

		if (!(IsSublinkClause(clause) || IsSelectClause(clause) ||
			  IsJoinClause(clause) || or_clause(clause)))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("unsupported clause type")));
		}
	}
}


/*
 * JoinClauseList finds the join clauses from the given where clause expression
 * list, and returns them. The function does not iterate into nested OR clauses
 * and relies on find_duplicate_ors() in the optimizer to pull up factorizable
 * OR clauses.
 */
List *
JoinClauseList(List *whereClauseList)
{
	List *joinClauseList = NIL;
	ListCell *whereClauseCell = NULL;

	foreach(whereClauseCell, whereClauseList)
	{
		Node *whereClause = (Node *) lfirst(whereClauseCell);
		if (IsJoinClause(whereClause))
		{
			joinClauseList = lappend(joinClauseList, whereClause);
		}
	}

	return joinClauseList;
}


/*
 * ExtractFromExpressionWalker walks over a FROM expression, and finds all
 * implicit and explicit qualifiers in the expression. The function looks at
 * join and from expression nodes to find qualifiers, and returns these
 * qualifiers.
 *
 * Note that we don't want outer join clauses in regular outer join planning,
 * but we need outer join clauses in subquery pushdown prerequisite checks.
 * Therefore, outer join qualifiers are returned in a different list than other
 * qualifiers inside the given walker context. For this reason, we return two
 * qualifier lists.
 *
 * Note that we check if the qualifier node in join and from expression nodes
 * is a list node. If it is not a list node which is the case for subqueries,
 * then we run eval_const_expressions(), canonicalize_qual() and make_ands_implicit()
 * on the qualifier node and get a list of flattened implicitly AND'ed qualifier
 * list. Actually in the planer phase of PostgreSQL these functions also run on
 * subqueries but differently from the outermost query, they are run on a copy
 * of parse tree and changes do not get persisted as modifications to the original
 * query tree.
 *
 * Also this function adds SubLinks to the baseQualifierList when they appear on
 * the query's WHERE clause. The callers of the function should consider processing
 * Sublinks as well.
 */
static bool
ExtractFromExpressionWalker(Node *node, QualifierWalkerContext *walkerContext)
{
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	/*
	 * Get qualifier lists of join and from expression nodes. Note that in the
	 * case of subqueries, PostgreSQL can skip simplifying, flattening and
	 * making ANDs implicit. If qualifiers node is not a list, then we run these
	 * preprocess routines on qualifiers node.
	 */
	if (IsA(node, JoinExpr))
	{
		List *joinQualifierList = NIL;
		JoinExpr *joinExpression = (JoinExpr *) node;
		Node *joinQualifiersNode = joinExpression->quals;
		JoinType joinType = joinExpression->jointype;

		if (joinQualifiersNode != NULL)
		{
			if (IsA(joinQualifiersNode, List))
			{
				joinQualifierList = (List *) joinQualifiersNode;
			}
			else
			{
				/* this part of code only run for subqueries */
				Node *joinClause = eval_const_expressions(NULL, joinQualifiersNode);
				joinClause = (Node *) canonicalize_qual((Expr *) joinClause);
				joinQualifierList = make_ands_implicit((Expr *) joinClause);
			}
		}

		/* return outer join clauses in a separate list */
		if (joinType == JOIN_INNER)
		{
			walkerContext->baseQualifierList =
				list_concat(walkerContext->baseQualifierList, joinQualifierList);
		}
		else if (IS_OUTER_JOIN(joinType))
		{
			walkerContext->outerJoinQualifierList =
				list_concat(walkerContext->outerJoinQualifierList, joinQualifierList);
		}
	}
	else if (IsA(node, FromExpr))
	{
		List *fromQualifierList = NIL;
		FromExpr *fromExpression = (FromExpr *) node;
		Node *fromQualifiersNode = fromExpression->quals;

		if (fromQualifiersNode != NULL)
		{
			if (IsA(fromQualifiersNode, List))
			{
				fromQualifierList = (List *) fromQualifiersNode;
			}
			else
			{
				/* this part of code only run for subqueries */
				Node *fromClause = eval_const_expressions(NULL, fromQualifiersNode);
				fromClause = (Node *) canonicalize_qual((Expr *) fromClause);
				fromQualifierList = make_ands_implicit((Expr *) fromClause);
			}

			walkerContext->baseQualifierList =
				list_concat(walkerContext->baseQualifierList, fromQualifierList);
		}
	}

	walkerResult = expression_tree_walker(node, ExtractFromExpressionWalker,
										  (void *) walkerContext);

	return walkerResult;
}


/*
 * IsJoinClause determines if the given node is a join clause according to our
 * criteria. Our criteria defines a join clause as an equi join operator between
 * two columns that belong to two different tables.
 */
bool
IsJoinClause(Node *clause)
{
	bool isJoinClause = false;
	OpExpr *operatorExpression = NULL;
	List *argumentList = NIL;
	Node *leftArgument = NULL;
	Node *rightArgument = NULL;
	List *leftColumnList = NIL;
	List *rightColumnList = NIL;

	if (!IsA(clause, OpExpr))
	{
		return false;
	}

	operatorExpression = (OpExpr *) clause;
	argumentList = operatorExpression->args;

	/* join clauses must have two arguments */
	if (list_length(argumentList) != 2)
	{
		return false;
	}

	/* get left and right side of the expression */
	leftArgument = (Node *) linitial(argumentList);
	rightArgument = (Node *) lsecond(argumentList);

	leftColumnList = pull_var_clause_default(leftArgument);
	rightColumnList = pull_var_clause_default(rightArgument);

	/* each side of the expression should have only one column */
	if ((list_length(leftColumnList) == 1) && (list_length(rightColumnList) == 1))
	{
		Var *leftColumn = (Var *) linitial(leftColumnList);
		Var *rightColumn = (Var *) linitial(rightColumnList);
		bool equiJoin = false;
		bool joinBetweenDifferentTables = false;

		bool equalsOperator = OperatorImplementsEquality(operatorExpression->opno);
		if (equalsOperator)
		{
			equiJoin = true;
		}

		if (leftColumn->varno != rightColumn->varno)
		{
			joinBetweenDifferentTables = true;
		}

		/* codifies our logic for determining if this node is a join clause */
		if (equiJoin && joinBetweenDifferentTables)
		{
			isJoinClause = true;
		}
	}

	return isJoinClause;
}


/*
 * TableEntryList finds the regular relation nodes in the range table entry
 * list, and builds a list of table entries from these regular relation nodes.
 */
List *
TableEntryList(List *rangeTableList)
{
	List *tableEntryList = NIL;
	ListCell *rangeTableCell = NULL;
	uint32 tableId = 1; /* range table indices start at 1 */

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			TableEntry *tableEntry = (TableEntry *) palloc0(sizeof(TableEntry));
			tableEntry->relationId = rangeTableEntry->relid;
			tableEntry->rangeTableId = tableId;

			tableEntryList = lappend(tableEntryList, tableEntry);
		}

		/*
		 * Increment tableId regardless so that table entry's tableId remains
		 * congruent with column's range table reference (varno).
		 */
		tableId++;
	}

	return tableEntryList;
}


/*
 * UsedTableEntryList returns list of relation range table entries
 * that are referenced within the query. Unused entries due to query
 * flattening or re-rewriting are ignored.
 */
List *
UsedTableEntryList(Query *query)
{
	List *tableEntryList = NIL;
	List *rangeTableList = query->rtable;
	List *joinTreeTableIndexList = NIL;
	ListCell *joinTreeTableIndexCell = NULL;

	ExtractRangeTableIndexWalker((Node *) query->jointree, &joinTreeTableIndexList);
	foreach(joinTreeTableIndexCell, joinTreeTableIndexList)
	{
		int joinTreeTableIndex = lfirst_int(joinTreeTableIndexCell);
		RangeTblEntry *rangeTableEntry = rt_fetch(joinTreeTableIndex, rangeTableList);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			TableEntry *tableEntry = (TableEntry *) palloc0(sizeof(TableEntry));
			tableEntry->relationId = rangeTableEntry->relid;
			tableEntry->rangeTableId = joinTreeTableIndex;

			tableEntryList = lappend(tableEntryList, tableEntry);
		}
	}

	return tableEntryList;
}


/*
 * MultiTableNodeList builds a list of MultiTable nodes from the given table
 * entry list. A multi table node represents one entry from the range table
 * list. These entries may belong to the same physical relation in the case of
 * self-joins.
 */
static List *
MultiTableNodeList(List *tableEntryList, List *rangeTableList)
{
	List *tableNodeList = NIL;
	ListCell *tableEntryCell = NULL;

	foreach(tableEntryCell, tableEntryList)
	{
		TableEntry *tableEntry = (TableEntry *) lfirst(tableEntryCell);
		Oid relationId = tableEntry->relationId;
		uint32 rangeTableId = tableEntry->rangeTableId;
		Var *partitionColumn = PartitionColumn(relationId, rangeTableId);
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableId, rangeTableList);

		MultiTable *tableNode = CitusMakeNode(MultiTable);
		tableNode->subquery = NULL;
		tableNode->relationId = relationId;
		tableNode->rangeTableId = rangeTableId;
		tableNode->partitionColumn = partitionColumn;
		tableNode->alias = rangeTableEntry->alias;
		tableNode->referenceNames = rangeTableEntry->eref;

		tableNodeList = lappend(tableNodeList, tableNode);
	}

	return tableNodeList;
}


/* Adds a MultiCollect node on top of each MultiTable node in the given list. */
static List *
AddMultiCollectNodes(List *tableNodeList)
{
	List *collectTableList = NIL;
	ListCell *tableNodeCell = NULL;

	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = (MultiTable *) lfirst(tableNodeCell);

		MultiCollect *collectNode = CitusMakeNode(MultiCollect);
		SetChild((MultiUnaryNode *) collectNode, (MultiNode *) tableNode);

		collectTableList = lappend(collectTableList, collectNode);
	}

	return collectTableList;
}


/*
 * MultiJoinTree takes in the join order information and the list of tables, and
 * builds a join tree by applying the corresponding join rules. The function
 * builds a left deep tree, as expressed by the join order list.
 *
 * The function starts by setting the first table as the top node in the join
 * tree. Then, the function iterates over the list of tables, and builds a new
 * join node between the top of the join tree and the next table in the list.
 * At each iteration, the function sets the top of the join tree to the newly
 * built list. This results in a left deep join tree, and the function returns
 * this tree after every table in the list has been joined.
 */
static MultiNode *
MultiJoinTree(List *joinOrderList, List *collectTableList, List *joinWhereClauseList)
{
	MultiNode *currentTopNode = NULL;
	ListCell *joinOrderCell = NULL;
	bool firstJoinNode = true;

	foreach(joinOrderCell, joinOrderList)
	{
		JoinOrderNode *joinOrderNode = (JoinOrderNode *) lfirst(joinOrderCell);
		uint32 joinTableId = joinOrderNode->tableEntry->rangeTableId;
		MultiCollect *collectNode = CollectNodeForTable(collectTableList, joinTableId);

		if (firstJoinNode)
		{
			currentTopNode = (MultiNode *) collectNode;
			firstJoinNode = false;
		}
		else
		{
			JoinRuleType joinRuleType = joinOrderNode->joinRuleType;
			JoinType joinType = joinOrderNode->joinType;
			Var *partitionColumn = joinOrderNode->partitionColumn;
			MultiNode *newJoinNode = NULL;
			List *joinClauseList = joinOrderNode->joinClauseList;

			/*
			 * Build a join node between the top of our join tree and the next
			 * table in the join order.
			 */
			newJoinNode = ApplyJoinRule(currentTopNode, (MultiNode *) collectNode,
										joinRuleType, partitionColumn, joinType,
										joinClauseList);

			/* the new join node becomes the top of our join tree */
			currentTopNode = newJoinNode;
		}
	}

	/* current top node points to the entire left deep join tree */
	return currentTopNode;
}


/*
 * CollectNodeForTable finds the MultiCollect node whose MultiTable node has the
 * given range table identifier. Note that this function expects each collect
 * node in the given list to have one table node as its child.
 */
static MultiCollect *
CollectNodeForTable(List *collectTableList, uint32 rangeTableId)
{
	MultiCollect *collectNodeForTable = NULL;
	ListCell *collectTableCell = NULL;

	foreach(collectTableCell, collectTableList)
	{
		MultiCollect *collectNode = (MultiCollect *) lfirst(collectTableCell);

		List *tableIdList = OutputTableIdList((MultiNode *) collectNode);
		uint32 tableId = (uint32) linitial_int(tableIdList);
		Assert(list_length(tableIdList) == 1);

		if (tableId == rangeTableId)
		{
			collectNodeForTable = collectNode;
			break;
		}
	}

	Assert(collectNodeForTable != NULL);
	return collectNodeForTable;
}


/*
 * MultiSelectNode extracts the select clauses from the given where clause list,
 * and builds a MultiSelect node from these clauses. If the expression tree does
 * not have any select clauses, the function return null.
 */
static MultiSelect *
MultiSelectNode(List *whereClauseList)
{
	List *selectClauseList = NIL;
	MultiSelect *selectNode = NULL;

	ListCell *whereClauseCell = NULL;
	foreach(whereClauseCell, whereClauseList)
	{
		Node *whereClause = (Node *) lfirst(whereClauseCell);
		if (IsSelectClause(whereClause) || or_clause(whereClause))
		{
			selectClauseList = lappend(selectClauseList, whereClause);
		}
	}

	if (list_length(selectClauseList) > 0)
	{
		selectNode = CitusMakeNode(MultiSelect);
		selectNode->selectClauseList = selectClauseList;
	}

	return selectNode;
}


/*
 * IsSelectClause determines if the given node is a select clause according to
 * our criteria. Our criteria defines a select clause as an expression that has
 * zero or more columns belonging to only one table. The function assumes that
 * no sublinks exists in the clause.
 */
static bool
IsSelectClause(Node *clause)
{
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	Var *firstColumn = NULL;
	Index firstColumnTableId = 0;
	bool isSelectClause = true;

	/* extract columns from the clause */
	columnList = pull_var_clause_default(clause);
	if (list_length(columnList) == 0)
	{
		return true;
	}

	/* get first column's tableId */
	firstColumn = (Var *) linitial(columnList);
	firstColumnTableId = firstColumn->varno;

	/* check if all columns are from the same table */
	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		if (column->varno != firstColumnTableId)
		{
			isSelectClause = false;
		}
	}

	return isSelectClause;
}


/*
 * IsSublinkClause determines if the given node is a sublink or subplan.
 */
static bool
IsSublinkClause(Node *clause)
{
	NodeTag nodeTag = nodeTag(clause);

	if (nodeTag == T_SubLink || nodeTag == T_SubPlan)
	{
		return true;
	}

	return false;
}


/*
 * MultiProjectNode builds the project node using the target entry information
 * from the query tree. The project node only encapsulates projected columns,
 * and does not include aggregates, group clauses, or project expressions.
 */
static MultiProject *
MultiProjectNode(List *targetEntryList)
{
	MultiProject *projectNode = NULL;
	List *uniqueColumnList = NIL;
	List *columnList = NIL;
	ListCell *columnCell = NULL;

	/* extract the list of columns and remove any duplicates */
	columnList = pull_var_clause_default((Node *) targetEntryList);
	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);

		uniqueColumnList = list_append_unique(uniqueColumnList, column);
	}

	/* create project node with list of columns to project */
	projectNode = CitusMakeNode(MultiProject);
	projectNode->columnList = uniqueColumnList;

	return projectNode;
}


/* Builds the extended operator node using fields from the given query tree. */
static MultiExtendedOp *
MultiExtendedOpNode(Query *queryTree)
{
	MultiExtendedOp *extendedOpNode = CitusMakeNode(MultiExtendedOp);
	extendedOpNode->targetList = queryTree->targetList;
	extendedOpNode->groupClauseList = queryTree->groupClause;
	extendedOpNode->sortClauseList = queryTree->sortClause;
	extendedOpNode->limitCount = queryTree->limitCount;
	extendedOpNode->limitOffset = queryTree->limitOffset;
	extendedOpNode->havingQual = queryTree->havingQual;

	return extendedOpNode;
}


/* Helper function to return the parent node of the given node. */
MultiNode *
ParentNode(MultiNode *multiNode)
{
	MultiNode *parentNode = multiNode->parentNode;
	return parentNode;
}


/* Helper function to return the child of the given unary node. */
MultiNode *
ChildNode(MultiUnaryNode *multiNode)
{
	MultiNode *childNode = multiNode->childNode;
	return childNode;
}


/* Helper function to return the grand child of the given unary node. */
MultiNode *
GrandChildNode(MultiUnaryNode *multiNode)
{
	MultiNode *childNode = ChildNode(multiNode);
	MultiNode *grandChildNode = ChildNode((MultiUnaryNode *) childNode);

	return grandChildNode;
}


/* Sets the given child node as a child of the given unary parent node. */
void
SetChild(MultiUnaryNode *parent, MultiNode *child)
{
	parent->childNode = child;
	child->parentNode = (MultiNode *) parent;
}


/* Sets the given child node as a left child of the given parent node. */
void
SetLeftChild(MultiBinaryNode *parent, MultiNode *leftChild)
{
	parent->leftChildNode = leftChild;
	leftChild->parentNode = (MultiNode *) parent;
}


/* Sets the given child node as a right child of the given parent node. */
void
SetRightChild(MultiBinaryNode *parent, MultiNode *rightChild)
{
	parent->rightChildNode = rightChild;
	rightChild->parentNode = (MultiNode *) parent;
}


/* Returns true if the given node is a unary operator. */
bool
UnaryOperator(MultiNode *node)
{
	bool unaryOperator = false;

	if (CitusIsA(node, MultiTreeRoot) || CitusIsA(node, MultiTable) ||
		CitusIsA(node, MultiCollect) || CitusIsA(node, MultiSelect) ||
		CitusIsA(node, MultiProject) || CitusIsA(node, MultiPartition) ||
		CitusIsA(node, MultiExtendedOp))
	{
		unaryOperator = true;
	}

	return unaryOperator;
}


/* Returns true if the given node is a binary operator. */
bool
BinaryOperator(MultiNode *node)
{
	bool binaryOperator = false;

	if (CitusIsA(node, MultiJoin) || CitusIsA(node, MultiCartesianProduct))
	{
		binaryOperator = true;
	}

	return binaryOperator;
}


/*
 * OutputTableIdList finds all table identifiers that are output by the given
 * multi node, and returns these identifiers in a new list.
 */
List *
OutputTableIdList(MultiNode *multiNode)
{
	List *tableIdList = NIL;
	List *tableNodeList = FindNodesOfType(multiNode, T_MultiTable);
	ListCell *tableNodeCell = NULL;

	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = (MultiTable *) lfirst(tableNodeCell);
		int tableId = (int) tableNode->rangeTableId;

		if (tableId != SUBQUERY_RANGE_TABLE_ID)
		{
			tableIdList = lappend_int(tableIdList, tableId);
		}
	}

	return tableIdList;
}


/*
 * FindNodesOfType takes in a given logical plan tree, and recursively traverses
 * the tree in preorder. The function finds all nodes of requested type during
 * the traversal, and returns them in a list.
 */
List *
FindNodesOfType(MultiNode *node, int type)
{
	List *nodeList = NIL;
	int nodeType = T_Invalid;

	/* terminal condition for recursion */
	if (node == NULL)
	{
		return NIL;
	}

	/* current node has expected node type */
	nodeType = CitusNodeTag(node);
	if (nodeType == type)
	{
		nodeList = lappend(nodeList, node);
	}

	if (UnaryOperator(node))
	{
		MultiNode *childNode = ((MultiUnaryNode *) node)->childNode;
		List *childNodeList = FindNodesOfType(childNode, type);

		nodeList = list_concat(nodeList, childNodeList);
	}
	else if (BinaryOperator(node))
	{
		MultiNode *leftChildNode = ((MultiBinaryNode *) node)->leftChildNode;
		MultiNode *rightChildNode = ((MultiBinaryNode *) node)->rightChildNode;

		List *leftChildNodeList = FindNodesOfType(leftChildNode, type);
		List *rightChildNodeList = FindNodesOfType(rightChildNode, type);

		nodeList = list_concat(nodeList, leftChildNodeList);
		nodeList = list_concat(nodeList, rightChildNodeList);
	}

	return nodeList;
}


/*
 * NeedsDistributedPlanning checks if the passed in query is a query running
 * on a distributed table. If it is, we start distributed planning.
 *
 * For distributed relations it also assigns identifiers to the relevant RTEs.
 */
bool
NeedsDistributedPlanning(Query *queryTree)
{
	CmdType commandType = queryTree->commandType;
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasLocalRelation = false;
	bool hasDistributedRelation = false;

	if (commandType != CMD_SELECT && commandType != CMD_INSERT &&
		commandType != CMD_UPDATE && commandType != CMD_DELETE)
	{
		return false;
	}

	/*
	 * We can handle INSERT INTO distributed_table SELECT ... even if the SELECT
	 * part references local tables, so skip the remaining checks.
	 */
	if (InsertSelectIntoDistributedTable(queryTree))
	{
		return true;
	}

	/* extract range table entries for simple relations only */
	ExtractRangeTableRelationWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/* check if relation is local or distributed */
		Oid relationId = rangeTableEntry->relid;

		if (IsDistributedTable(relationId))
		{
			hasDistributedRelation = true;
		}
		else
		{
			hasLocalRelation = true;
		}
	}

	if (hasLocalRelation && hasDistributedRelation)
	{
		ereport(ERROR, (errmsg("cannot plan queries which include both local and "
							   "distributed relations")));
	}

	return hasDistributedRelation;
}


/*
 * ExtractRangeTableRelationWalker gathers all range table entries in a query
 * and filters them to preserve only those of the RTE_RELATION type.
 */
bool
ExtractRangeTableRelationWalker(Node *node, List **rangeTableRelationList)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool walkIsComplete = ExtractRangeTableEntryWalker(node, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION &&
			rangeTableEntry->relkind != RELKIND_VIEW)
		{
			(*rangeTableRelationList) = lappend(*rangeTableRelationList, rangeTableEntry);
		}
	}

	return walkIsComplete;
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
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTable = (RangeTblEntry *) node;
		(*rangeTableList) = lappend(*rangeTableList, rangeTable);
	}
	else if (IsA(node, Query))
	{
		walkIsComplete = query_tree_walker((Query *) node, ExtractRangeTableEntryWalker,
										   rangeTableList, QTW_EXAMINE_RTES);
	}
	else
	{
		walkIsComplete = expression_tree_walker(node, ExtractRangeTableEntryWalker,
												rangeTableList);
	}

	return walkIsComplete;
}


/*
 * pull_var_clause_default calls pull_var_clause with the most commonly used
 * arguments for distributed planning.
 */
List *
pull_var_clause_default(Node *node)
{
#if (PG_VERSION_NUM >= 90600)

	/*
	 * PVC_REJECT_PLACEHOLDERS is now implicit if PVC_INCLUDE_PLACEHOLDERS
	 * isn't specified.
	 */
	List *columnList = pull_var_clause(node, PVC_RECURSE_AGGREGATES);
#else
	List *columnList = pull_var_clause(node, PVC_RECURSE_AGGREGATES,
									   PVC_REJECT_PLACEHOLDERS);
#endif

	return columnList;
}


/*
 * ApplyJoinRule finds the join rule application function that corresponds to
 * the given join rule, and calls this function to create a new join node that
 * joins the left and right nodes together.
 */
static MultiNode *
ApplyJoinRule(MultiNode *leftNode, MultiNode *rightNode, JoinRuleType ruleType,
			  Var *partitionColumn, JoinType joinType, List *joinClauseList)
{
	RuleApplyFunction ruleApplyFunction = NULL;
	MultiNode *multiNode = NULL;

	List *applicableJoinClauses = NIL;
	List *leftTableIdList = OutputTableIdList(leftNode);
	List *rightTableIdList = OutputTableIdList(rightNode);
	int rightTableIdCount PG_USED_FOR_ASSERTS_ONLY = 0;
	uint32 rightTableId = 0;

	rightTableIdCount = list_length(rightTableIdList);
	Assert(rightTableIdCount == 1);

	/* find applicable join clauses between the left and right data sources */
	rightTableId = (uint32) linitial_int(rightTableIdList);
	applicableJoinClauses = ApplicableJoinClauses(leftTableIdList, rightTableId,
												  joinClauseList);

	/* call the join rule application function to create the new join node */
	ruleApplyFunction = JoinRuleApplyFunction(ruleType);
	multiNode = (*ruleApplyFunction)(leftNode, rightNode, partitionColumn,
									 joinType, applicableJoinClauses);

	if (joinType != JOIN_INNER && CitusIsA(multiNode, MultiJoin))
	{
		MultiJoin *joinNode = (MultiJoin *) multiNode;

		/* preserve non-join clauses for OUTER joins */
		joinNode->joinClauseList = list_copy(joinClauseList);
	}

	return multiNode;
}


/*
 * JoinRuleApplyFunction returns a function pointer for the rule application
 * function; this rule application function corresponds to the given rule type.
 * This function also initializes the rule application function array in a
 * static code block, if the array has not been initialized.
 */
static RuleApplyFunction
JoinRuleApplyFunction(JoinRuleType ruleType)
{
	static bool ruleApplyFunctionInitialized = false;
	RuleApplyFunction ruleApplyFunction = NULL;

	if (!ruleApplyFunctionInitialized)
	{
		RuleApplyFunctionArray[BROADCAST_JOIN] = &ApplyBroadcastJoin;
		RuleApplyFunctionArray[LOCAL_PARTITION_JOIN] = &ApplyLocalJoin;
		RuleApplyFunctionArray[SINGLE_PARTITION_JOIN] = &ApplySinglePartitionJoin;
		RuleApplyFunctionArray[DUAL_PARTITION_JOIN] = &ApplyDualPartitionJoin;
		RuleApplyFunctionArray[CARTESIAN_PRODUCT] = &ApplyCartesianProduct;

		ruleApplyFunctionInitialized = true;
	}

	ruleApplyFunction = RuleApplyFunctionArray[ruleType];
	Assert(ruleApplyFunction != NULL);

	return ruleApplyFunction;
}


/*
 * ApplyBroadcastJoin creates a new MultiJoin node that joins the left and the
 * right node. The new node uses the broadcast join rule to perform the join.
 */
static MultiNode *
ApplyBroadcastJoin(MultiNode *leftNode, MultiNode *rightNode,
				   Var *partitionColumn, JoinType joinType,
				   List *applicableJoinClauses)
{
	MultiJoin *joinNode = CitusMakeNode(MultiJoin);
	joinNode->joinRuleType = BROADCAST_JOIN;
	joinNode->joinType = joinType;
	joinNode->joinClauseList = applicableJoinClauses;

	SetLeftChild((MultiBinaryNode *) joinNode, leftNode);
	SetRightChild((MultiBinaryNode *) joinNode, rightNode);

	return (MultiNode *) joinNode;
}


/*
 * ApplyLocalJoin creates a new MultiJoin node that joins the left and the right
 * node. The new node uses the local join rule to perform the join.
 */
static MultiNode *
ApplyLocalJoin(MultiNode *leftNode, MultiNode *rightNode,
			   Var *partitionColumn, JoinType joinType,
			   List *applicableJoinClauses)
{
	MultiJoin *joinNode = CitusMakeNode(MultiJoin);
	joinNode->joinRuleType = LOCAL_PARTITION_JOIN;
	joinNode->joinType = joinType;
	joinNode->joinClauseList = applicableJoinClauses;

	SetLeftChild((MultiBinaryNode *) joinNode, leftNode);
	SetRightChild((MultiBinaryNode *) joinNode, rightNode);

	return (MultiNode *) joinNode;
}


/*
 * ApplySinglePartitionJoin creates a new MultiJoin node that joins the left and
 * right node. The function also adds a MultiPartition node on top of the node
 * (left or right) that is not partitioned on the join column.
 */
static MultiNode *
ApplySinglePartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
						 Var *partitionColumn, JoinType joinType,
						 List *applicableJoinClauses)
{
	OpExpr *joinClause = NULL;
	Var *leftColumn = NULL;
	Var *rightColumn = NULL;
	List *rightTableIdList = NIL;
	uint32 rightTableId = 0;
	uint32 partitionTableId = partitionColumn->varno;

	/* create all operator structures up front */
	MultiJoin *joinNode = CitusMakeNode(MultiJoin);
	MultiCollect *collectNode = CitusMakeNode(MultiCollect);
	MultiPartition *partitionNode = CitusMakeNode(MultiPartition);

	/*
	 * We first find the appropriate join clause. Then, we compare the partition
	 * column against the join clause's columns. If one of the columns matches,
	 * we introduce a (re-)partition operator for the other column.
	 */
	joinClause = SinglePartitionJoinClause(partitionColumn, applicableJoinClauses);
	Assert(joinClause != NULL);

	leftColumn = LeftColumn(joinClause);
	rightColumn = RightColumn(joinClause);

	if (equal(partitionColumn, leftColumn))
	{
		partitionNode->partitionColumn = rightColumn;
		partitionNode->splitPointTableId = partitionTableId;
	}
	else if (equal(partitionColumn, rightColumn))
	{
		partitionNode->partitionColumn = leftColumn;
		partitionNode->splitPointTableId = partitionTableId;
	}

	/* determine the node the partition operator goes on top of */
	rightTableIdList = OutputTableIdList(rightNode);
	rightTableId = (uint32) linitial_int(rightTableIdList);
	Assert(list_length(rightTableIdList) == 1);

	/*
	 * If the right child node is partitioned on the partition key column, we
	 * add the partition operator on the left child node; and vice versa. Then,
	 * we add a collect operator on top of the partition operator, and always
	 * make sure that we have at most one relation on the right-hand side.
	 */
	if (partitionTableId == rightTableId)
	{
		SetChild((MultiUnaryNode *) partitionNode, leftNode);
		SetChild((MultiUnaryNode *) collectNode, (MultiNode *) partitionNode);

		SetLeftChild((MultiBinaryNode *) joinNode, (MultiNode *) collectNode);
		SetRightChild((MultiBinaryNode *) joinNode, rightNode);
	}
	else
	{
		SetChild((MultiUnaryNode *) partitionNode, rightNode);
		SetChild((MultiUnaryNode *) collectNode, (MultiNode *) partitionNode);

		SetLeftChild((MultiBinaryNode *) joinNode, leftNode);
		SetRightChild((MultiBinaryNode *) joinNode, (MultiNode *) collectNode);
	}

	/* finally set join operator fields */
	joinNode->joinRuleType = SINGLE_PARTITION_JOIN;
	joinNode->joinType = joinType;
	joinNode->joinClauseList = applicableJoinClauses;

	return (MultiNode *) joinNode;
}


/*
 * ApplyDualPartitionJoin creates a new MultiJoin node that joins the left and
 * right node. The function also adds two MultiPartition operators on top of
 * both nodes to repartition these nodes' data on the join clause columns.
 */
static MultiNode *
ApplyDualPartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
					   Var *partitionColumn, JoinType joinType,
					   List *applicableJoinClauses)
{
	MultiJoin *joinNode = NULL;
	OpExpr *joinClause = NULL;
	MultiPartition *leftPartitionNode = NULL;
	MultiPartition *rightPartitionNode = NULL;
	MultiCollect *leftCollectNode = NULL;
	MultiCollect *rightCollectNode = NULL;
	Var *leftColumn = NULL;
	Var *rightColumn = NULL;
	List *rightTableIdList = NIL;
	uint32 rightTableId = 0;

	/* find the appropriate join clause */
	joinClause = DualPartitionJoinClause(applicableJoinClauses);
	Assert(joinClause != NULL);

	leftColumn = LeftColumn(joinClause);
	rightColumn = RightColumn(joinClause);

	rightTableIdList = OutputTableIdList(rightNode);
	rightTableId = (uint32) linitial_int(rightTableIdList);
	Assert(list_length(rightTableIdList) == 1);

	leftPartitionNode = CitusMakeNode(MultiPartition);
	rightPartitionNode = CitusMakeNode(MultiPartition);

	/* find the partition node each join clause column belongs to */
	if (leftColumn->varno == rightTableId)
	{
		leftPartitionNode->partitionColumn = rightColumn;
		rightPartitionNode->partitionColumn = leftColumn;
	}
	else
	{
		leftPartitionNode->partitionColumn = leftColumn;
		rightPartitionNode->partitionColumn = rightColumn;
	}

	/* add partition operators on top of left and right nodes */
	SetChild((MultiUnaryNode *) leftPartitionNode, leftNode);
	SetChild((MultiUnaryNode *) rightPartitionNode, rightNode);

	/* add collect operators on top of the two partition operators */
	leftCollectNode = CitusMakeNode(MultiCollect);
	rightCollectNode = CitusMakeNode(MultiCollect);

	SetChild((MultiUnaryNode *) leftCollectNode, (MultiNode *) leftPartitionNode);
	SetChild((MultiUnaryNode *) rightCollectNode, (MultiNode *) rightPartitionNode);

	/* add join operator on top of the two collect operators */
	joinNode = CitusMakeNode(MultiJoin);
	joinNode->joinRuleType = DUAL_PARTITION_JOIN;
	joinNode->joinType = joinType;
	joinNode->joinClauseList = applicableJoinClauses;

	SetLeftChild((MultiBinaryNode *) joinNode, (MultiNode *) leftCollectNode);
	SetRightChild((MultiBinaryNode *) joinNode, (MultiNode *) rightCollectNode);

	return (MultiNode *) joinNode;
}


/* Creates a cartesian product node that joins the left and the right node. */
static MultiNode *
ApplyCartesianProduct(MultiNode *leftNode, MultiNode *rightNode,
					  Var *partitionColumn, JoinType joinType,
					  List *applicableJoinClauses)
{
	MultiCartesianProduct *cartesianNode = CitusMakeNode(MultiCartesianProduct);

	SetLeftChild((MultiBinaryNode *) cartesianNode, leftNode);
	SetRightChild((MultiBinaryNode *) cartesianNode, rightNode);

	return (MultiNode *) cartesianNode;
}


/*
 * SubqueryPushdownMultiTree creates logical plan for subquery pushdown logic.
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
SubqueryPushdownMultiPlanTree(Query *queryTree)
{
	List *targetEntryList = queryTree->targetList;
	List *qualifierList = NIL;
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

	/* verify we can perform distributed planning on this query */
	ErrorIfQueryNotSupported(queryTree);

	/*
	 * Extract qualifiers and verify we can plan for them. Note that since
	 * subquery pushdown join planning is based on restriction equivalence,
	 * checking for these qualifiers may not be necessary.
	 */
	qualifierList = QualifierList(queryTree->jointree);
	ValidateSubqueryPushdownClauseList(qualifierList);

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
 * OperatorImplementsEquality returns true if the given opno represents an
 * equality operator. The function retrieves btree interpretation list for this
 * opno and check if BTEqualStrategyNumber strategy is present.
 */
bool
OperatorImplementsEquality(Oid opno)
{
	bool equalityOperator = false;
	List *btreeIntepretationList = get_op_btree_interpretation(opno);
	ListCell *btreeInterpretationCell = NULL;
	foreach(btreeInterpretationCell, btreeIntepretationList)
	{
		OpBtreeInterpretation *btreeIntepretation = (OpBtreeInterpretation *)
													lfirst(btreeInterpretationCell);
		if (btreeIntepretation->strategy == BTEqualStrategyNumber)
		{
			equalityOperator = true;
			break;
		}
	}

	return equalityOperator;
}
