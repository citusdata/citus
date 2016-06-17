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

#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
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


/* Config variable managed via guc.c */
bool SubqueryPushdown = false; /* is subquery pushdown enabled */


/* Function pointer type definition for apply join rule functions */
typedef MultiNode *(*RuleApplyFunction) (MultiNode *leftNode, MultiNode *rightNode,
										 Var *partitionColumn, JoinType joinType,
										 List *joinClauses);

static RuleApplyFunction RuleApplyFunctionArray[JOIN_RULE_LAST] = { 0 }; /* join rules */

/* Local functions forward declarations */
static MultiNode * MultiPlanTree(Query *queryTree);
static void ErrorIfQueryNotSupported(Query *queryTree);
static bool HasUnsupportedJoinWalker(Node *node, void *context);
static void ErrorIfSubqueryNotSupported(Query *subqueryTree);
#if (PG_VERSION_NUM >= 90500)
static bool HasTablesample(Query *queryTree);
#endif
static bool HasOuterJoin(Query *queryTree);
static bool HasOuterJoinWalker(Node *node, void *maxJoinLevel);
static bool HasComplexJoinOrder(Query *queryTree);
static bool HasComplexRangeTableType(Query *queryTree);
static void ValidateClauseList(List *clauseList);
static bool ExtractFromExpressionWalker(Node *node, List **qualifierList);
static List * MultiTableNodeList(List *tableEntryList, List *rangeTableList);
static List * AddMultiCollectNodes(List *tableNodeList);
static MultiNode * MultiJoinTree(List *joinOrderList, List *collectTableList,
								 List *joinClauseList);
static MultiCollect * CollectNodeForTable(List *collectTableList, uint32 rangeTableId);
static MultiSelect * MultiSelectNode(List *whereClauseList);
static bool IsSelectClause(Node *clause);
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
static MultiNode * SubqueryPushdownMultiPlanTree(Query *queryTree,
												 List *subqueryEntryList);
static void ErrorIfSubqueryJoin(Query *queryTree);
static MultiTable * MultiSubqueryPushdownTable(RangeTblEntry *subqueryRangeTableEntry);


/*
 * MultiLogicalPlanCreate takes in a parsed query tree, uses helper functions to
 * create logical plan and adds a root node to top of it.
 */
MultiTreeRoot *
MultiLogicalPlanCreate(Query *queryTree)
{
	MultiNode *multiQueryNode = NULL;
	MultiTreeRoot *rootNode = NULL;

	List *subqueryEntryList = SubqueryEntryList(queryTree);
	if (subqueryEntryList != NIL)
	{
		if (SubqueryPushdown)
		{
			multiQueryNode = SubqueryPushdownMultiPlanTree(queryTree, subqueryEntryList);
		}
		else
		{
			ErrorIfSubqueryJoin(queryTree);
			multiQueryNode = MultiPlanTree(queryTree);
		}
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

		subqueryRangeTableEntry = (RangeTblEntry *) linitial(subqueryEntryList);
		subqueryTree = subqueryRangeTableEntry->subquery;

		/* check if subquery satisfies preconditons */
		ErrorIfSubqueryNotSupported(subqueryTree);

		/* check if subquery has joining tables */
		ErrorIfSubqueryJoin(subqueryTree);

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
		tableEntryList = TableEntryList(rangeTableList);

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
	char *errorDetail = NULL;
#if (PG_VERSION_NUM >= 90500)
	bool hasTablesample = false;
#endif
	bool hasUnsupportedJoin = false;
	bool hasComplexJoinOrder = false;
	bool hasComplexRangeTableType = false;
	bool preconditionsSatisfied = true;

	if (queryTree->hasSubLinks)
	{
		preconditionsSatisfied = false;
		errorDetail = "Subqueries other than in from-clause are currently unsupported";
	}

	if (queryTree->havingQual != NULL)
	{
		preconditionsSatisfied = false;
		errorDetail = "Having qual is currently unsupported";
	}

	if (queryTree->hasWindowFuncs)
	{
		preconditionsSatisfied = false;
		errorDetail = "Window functions are currently unsupported";
	}

	if (queryTree->limitOffset)
	{
		preconditionsSatisfied = false;
		errorDetail = "Limit Offset clause is currently unsupported";
	}

	if (queryTree->setOperations)
	{
		preconditionsSatisfied = false;
		errorDetail = "Union, Intersect, or Except are currently unsupported";
	}

	if (queryTree->hasRecursive)
	{
		preconditionsSatisfied = false;
		errorDetail = "Recursive queries are currently unsupported";
	}

	if (queryTree->cteList)
	{
		preconditionsSatisfied = false;
		errorDetail = "Common Table Expressions are currently unsupported";
	}

	if (queryTree->hasForUpdate)
	{
		preconditionsSatisfied = false;
		errorDetail = "For Update/Share commands are currently unsupported";
	}

	if (queryTree->distinctClause)
	{
		preconditionsSatisfied = false;
		errorDetail = "Distinct clause is currently unsupported";
	}

#if (PG_VERSION_NUM >= 90500)
	if (queryTree->groupingSets)
	{
		preconditionsSatisfied = false;
		errorDetail = "Grouping sets, cube, and rollup is currently unsupported";
	}

	hasTablesample = HasTablesample(queryTree);
	if (hasTablesample)
	{
		preconditionsSatisfied = false;
		errorDetail = "Tablesample is currently unsupported";
	}
#endif

	hasUnsupportedJoin = HasUnsupportedJoinWalker((Node *) queryTree->jointree, NULL);
	if (hasUnsupportedJoin)
	{
		preconditionsSatisfied = false;
		errorDetail = "Join types other than inner/outer joins are currently unsupported";
	}

	hasComplexJoinOrder = HasComplexJoinOrder(queryTree);
	if (hasComplexJoinOrder)
	{
		preconditionsSatisfied = false;
		errorDetail = "Complex join orders are currently unsupported";
	}

	hasComplexRangeTableType = HasComplexRangeTableType(queryTree);
	if (hasComplexRangeTableType)
	{
		preconditionsSatisfied = false;
		errorDetail = "Complex table expressions are currently unsupported";
	}

	/* finally check and error out if not satisfied */
	if (!preconditionsSatisfied)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this query"),
						errdetail("%s", errorDetail)));
	}
}


#if (PG_VERSION_NUM >= 90500)

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


#endif


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
 * ErrorIfSubqueryNotSupported checks that we can perform distributed planning for
 * the given subquery.
 */
static void
ErrorIfSubqueryNotSupported(Query *subqueryTree)
{
	char *errorDetail = NULL;
	bool preconditionsSatisfied = true;

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

	/* finally check and error out if not satisfied */
	if (!preconditionsSatisfied)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this query"),
						errdetail("%s", errorDetail)));
	}
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
 * implicitly and explicitly defined clauses. Explicit clauses are expressed as
 * "SELECT ... FROM R1 INNER JOIN R2 ON R1.A = R2.A". Implicit joins differ in
 * that they live in the WHERE clause, and are expressed as "SELECT ... FROM
 * ... WHERE R1.a = R2.a".
 */
List *
WhereClauseList(FromExpr *fromExpr)
{
	FromExpr *fromExprCopy = copyObject(fromExpr);
	List *whereClauseList = NIL;

	ExtractFromExpressionWalker((Node *) fromExprCopy, &whereClauseList);

	return whereClauseList;
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

		bool selectClause = IsSelectClause(clause);
		bool joinClause = IsJoinClause(clause);
		bool orClause = or_clause(clause);

		if (!(selectClause || joinClause || orClause))
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
 * explicit qualifiers in the expression. The function looks at join and from
 * expression nodes to find explicit qualifiers, and returns these qualifiers.
 *
 * Note that we check if the qualifier node in join and from expression nodes
 * is a list node. If it is not a list node which is the case for subqueries,
 * then we run eval_const_expressions(), canonicalize_qual() and make_ands_implicit()
 * on the qualifier node and get a list of flattened implicitly AND'ed qualifier
 * list. Actually in the planer phase of PostgreSQL these functions also run on
 * subqueries but differently from the outermost query, they are run on a copy
 * of parse tree and changes do not get persisted as modifications to the original
 * query tree.
 */
static bool
ExtractFromExpressionWalker(Node *node, List **qualifierList)
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

		/*
		 * We only extract qualifiers from inner join clauses, which can be
		 * treated as WHERE clauses.
		 */
		if (joinQualifiersNode != NULL && joinType == JOIN_INNER)
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

			(*qualifierList) = list_concat(*qualifierList, joinQualifierList);
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

			(*qualifierList) = list_concat(*qualifierList, fromQualifierList);
		}
	}

	walkerResult = expression_tree_walker(node, ExtractFromExpressionWalker,
										  (void *) qualifierList);

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
 * zero or more columns belonging to only one table.
 */
static bool
IsSelectClause(Node *clause)
{
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	Var *firstColumn = NULL;
	Index firstColumnTableId = 0;
	bool isSelectClause = true;
	NodeTag nodeTag = nodeTag(clause);

	/* error out for subqueries in WHERE clause */
	if (nodeTag == T_SubLink || nodeTag == T_SubPlan)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this query"),
						errdetail("Subqueries other than in from-clause are currently "
								  "unsupported")));
	}

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
 * NeedsDistributedPlanning checks if the passed in query is a Select query
 * running on partitioned relations. If it is, we start distributed planning.
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

	/* users can't mix local and distributed relations in one query */
	if (hasLocalRelation && hasDistributedRelation)
	{
		ereport(ERROR, (errmsg("cannot plan queries that include both regular and "
							   "partitioned relations")));
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
		if (rangeTableEntry->rtekind == RTE_RELATION)
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
	List *columnList = pull_var_clause(node, PVC_RECURSE_AGGREGATES,
									   PVC_REJECT_PLACEHOLDERS);
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
 */
static MultiNode *
SubqueryPushdownMultiPlanTree(Query *queryTree, List *subqueryEntryList)
{
	List *targetEntryList = queryTree->targetList;
	List *whereClauseList = NIL;
	List *whereClauseColumnList = NIL;
	List *targetListColumnList = NIL;
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	MultiCollect *subqueryCollectNode = CitusMakeNode(MultiCollect);
	MultiTable *subqueryNode = NULL;
	MultiSelect *selectNode = NULL;
	MultiProject *projectNode = NULL;
	MultiExtendedOp *extendedOpNode = NULL;
	MultiNode *currentTopNode = NULL;
	RangeTblEntry *subqueryRangeTableEntry = NULL;

	/* verify we can perform distributed planning on this query */
	ErrorIfQueryNotSupported(queryTree);
	ErrorIfSubqueryJoin(queryTree);

	/* extract where clause qualifiers and verify we can plan for them */
	whereClauseList = WhereClauseList(queryTree->jointree);
	ValidateClauseList(whereClauseList);

	/*
	 * We disregard pulled subqueries. This changes order of range table list.
	 * We do not allow subquery joins, so we will have only one range table
	 * entry in range table list after dropping pulled subquery. For this reason,
	 * here we are updating columns in the most outer query for where clause
	 * list and target list accordingly.
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

	/* create multi node for the subquery */
	subqueryRangeTableEntry = (RangeTblEntry *) linitial(subqueryEntryList);
	subqueryNode = MultiSubqueryPushdownTable(subqueryRangeTableEntry);

	SetChild((MultiUnaryNode *) subqueryCollectNode, (MultiNode *) subqueryNode);
	currentTopNode = (MultiNode *) subqueryCollectNode;

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
 * ErrorIfSubqueryJoin errors out if the given query is a join query. Note that
 * this function will not be required once we implement subquery joins.
 */
static void
ErrorIfSubqueryJoin(Query *queryTree)
{
	List *joinTreeTableIndexList = NIL;
	uint32 joiningRangeTableCount = 0;

	/*
	 * Extract all range table indexes from the join tree. Note that sub-queries
	 * that get pulled up by PostgreSQL don't appear in this join tree.
	 */
	ExtractRangeTableIndexWalker((Node *) queryTree->jointree, &joinTreeTableIndexList);
	joiningRangeTableCount = list_length(joinTreeTableIndexList);

	if (joiningRangeTableCount > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this query"),
						errdetail("Join in subqueries is not supported yet")));
	}
}


/*
 * MultiSubqueryPushdownTable creates a MultiTable from the given subquery range
 * table entry and returns it. Note that this sets subquery field of MultiTable
 * to subquery of the given range table entry.
 */
static MultiTable *
MultiSubqueryPushdownTable(RangeTblEntry *subqueryRangeTableEntry)
{
	Query *subquery = subqueryRangeTableEntry->subquery;

	MultiTable *subqueryTableNode = CitusMakeNode(MultiTable);
	subqueryTableNode->subquery = subquery;
	subqueryTableNode->relationId = HEAP_ANALYTICS_SUBQUERY_RELATION_ID;
	subqueryTableNode->rangeTableId = SUBQUERY_RANGE_TABLE_ID;
	subqueryTableNode->partitionColumn = NULL;
	subqueryTableNode->alias = subqueryRangeTableEntry->alias;
	subqueryTableNode->referenceNames = subqueryRangeTableEntry->eref;

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
