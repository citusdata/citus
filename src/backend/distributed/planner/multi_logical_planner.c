/*-------------------------------------------------------------------------
 *
 * multi_logical_planner.c
 *
 * Routines for constructing a logical plan tree from the given Query tree
 * structure. This new logical plan is based on multi-relational algebra rules.
 *
 * Copyright (c) Citus Data, Inc.
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
#include "distributed/insert_select_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/multi_router_planner.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#else
#include "nodes/relation.h"
#include "optimizer/var.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"


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
static bool AllTargetExpressionsAreColumnReferences(List *targetEntryList);
static FieldSelect * CompositeFieldRecursive(Expr *expression, Query *query);
static bool FullCompositeFieldList(List *compositeFieldList);
static bool HasUnsupportedJoinWalker(Node *node, void *context);
static bool ErrorHintRequired(const char *errorHint, Query *queryTree);
static bool HasTablesample(Query *queryTree);
static bool HasComplexRangeTableType(Query *queryTree);
static bool IsReadIntermediateResultFunction(Node *node);
static bool ExtractFromExpressionWalker(Node *node,
										QualifierWalkerContext *walkerContext);
static List * MultiTableNodeList(List *tableEntryList, List *rangeTableList);
static List * AddMultiCollectNodes(List *tableNodeList);
static MultiNode * MultiJoinTree(List *joinOrderList, List *collectTableList,
								 List *joinClauseList);
static MultiCollect * CollectNodeForTable(List *collectTableList, uint32 rangeTableId);
static MultiSelect * MultiSelectNode(List *whereClauseList);
static bool IsSelectClause(Node *clause);

/* Local functions forward declarations for applying joins */
static MultiNode * ApplyJoinRule(MultiNode *leftNode, MultiNode *rightNode,
								 JoinRuleType ruleType, Var *partitionColumn,
								 JoinType joinType, List *joinClauseList);
static RuleApplyFunction JoinRuleApplyFunction(JoinRuleType ruleType);
static MultiNode * ApplyReferenceJoin(MultiNode *leftNode, MultiNode *rightNode,
									  Var *partitionColumn, JoinType joinType,
									  List *joinClauses);
static MultiNode * ApplyLocalJoin(MultiNode *leftNode, MultiNode *rightNode,
								  Var *partitionColumn, JoinType joinType,
								  List *joinClauses);
static MultiNode * ApplySingleRangePartitionJoin(MultiNode *leftNode,
												 MultiNode *rightNode,
												 Var *partitionColumn, JoinType joinType,
												 List *applicableJoinClauses);
static MultiNode * ApplySingleHashPartitionJoin(MultiNode *leftNode,
												MultiNode *rightNode,
												Var *partitionColumn, JoinType joinType,
												List *applicableJoinClauses);
static MultiJoin * ApplySinglePartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
											Var *partitionColumn, JoinType joinType,
											List *joinClauses);
static MultiNode * ApplyDualPartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
										  Var *partitionColumn, JoinType joinType,
										  List *joinClauses);
static MultiNode * ApplyCartesianProduct(MultiNode *leftNode, MultiNode *rightNode,
										 Var *partitionColumn, JoinType joinType,
										 List *joinClauses);


/*
 * MultiLogicalPlanCreate takes in both the original query and its corresponding modified
 * query tree yield by the standard planner. It uses helper functions to create logical
 * plan and adds a root node to top of it. The  original query is only used for subquery
 * pushdown planning.
 *
 * We also pass queryTree and plannerRestrictionContext to the planner. They
 * are primarily used to decide whether the subquery is safe to pushdown.
 * If not, it helps to produce meaningful error messages for subquery
 * pushdown planning.
 */
MultiTreeRoot *
MultiLogicalPlanCreate(Query *originalQuery, Query *queryTree,
					   PlannerRestrictionContext *plannerRestrictionContext)
{
	MultiNode *multiQueryNode = NULL;
	MultiTreeRoot *rootNode = NULL;

	if (ShouldUseSubqueryPushDown(originalQuery, queryTree))
	{
		multiQueryNode = SubqueryMultiNodeTree(originalQuery, queryTree,
											   plannerRestrictionContext);
	}
	else
	{
		multiQueryNode = MultiNodeTree(queryTree);
	}

	/* add a root node to serve as the permanent handle to the tree */
	rootNode = CitusMakeNode(MultiTreeRoot);
	SetChild((MultiUnaryNode *) rootNode, multiQueryNode);

	return rootNode;
}


/*
 * FindNodeCheck finds a node for which the check function returns true.
 *
 * To call this function directly with an RTE, use:
 * range_table_walker(rte, FindNodeCheck, check, QTW_EXAMINE_RTES_BEFORE)
 */
bool
FindNodeCheck(Node *node, bool (*check)(Node *))
{
	if (node == NULL)
	{
		return false;
	}

	if (check(node))
	{
		return true;
	}

	if (IsA(node, RangeTblEntry))
	{
		/* query_tree_walker descends into RTEs */
		return false;
	}
	else if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, FindNodeCheck, check,
								 QTW_EXAMINE_RTES_BEFORE);
	}

	return expression_tree_walker(node, FindNodeCheck, check);
}


/*
 * SingleRelationRepartitionSubquery returns true if it is eligible single
 * repartition query planning in the sense that:
 *   - None of the levels of the subquery contains a join
 *   - Only a single RTE_RELATION exists, which means only a single table
 *     name is specified on the whole query
 *   - No sublinks exists in the subquery
 *   - No window functions in the subquery
 *
 * Note that the caller should still call DeferErrorIfUnsupportedSubqueryRepartition()
 * to ensure that Citus supports the subquery. Also, this function is designed to run
 * on the original query.
 */
bool
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

	/* we don't support window functions */
	if (queryTree->hasWindowFuncs)
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
 * TargetListOnPartitionColumn checks if at least one target list entry is on
 * partition column.
 */
bool
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
		Oid relationId = InvalidOid;
		Var *column = NULL;

		FindReferencedTableColumn(targetExpression, NIL, query, &relationId, &column);

		/*
		 * If the expression belongs to a reference table continue searching for
		 * other partition keys.
		 */
		if (IsDistributedTable(relationId) && PartitionMethod(relationId) ==
			DISTRIBUTE_BY_NONE)
		{
			continue;
		}

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

	/*
	 * We could still behave as if the target list is on partition column if
	 * all range table entries are reference tables or intermediate results,
	 * and all target expressions are column references to the given query level.
	 */
	if (!targetListOnPartitionColumn)
	{
		if (!FindNodeCheckInRangeTableList(query->rtable, IsDistributedTableRTE) &&
			AllTargetExpressionsAreColumnReferences(targetEntryList))
		{
			targetListOnPartitionColumn = true;
		}
	}

	return targetListOnPartitionColumn;
}


/*
 * AllTargetExpressionsAreColumnReferences returns true if non of the
 * elements in the target entry list belong to an outer query (for
 * example the query is a sublink and references to another query
 * in the from list).
 *
 * The function also returns true if any of the  target entries is not
 * a column itself. This might be too restrictive, but, given that we're
 * handling a very specific type of queries, that seems acceptable for now.
 */
static bool
AllTargetExpressionsAreColumnReferences(List *targetEntryList)
{
	ListCell *targetEntryCell = NULL;

	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);
		Var *candidateColumn = NULL;
		Expr *strippedColumnExpression = (Expr *) strip_implicit_coercions(
			(Node *) targetEntry->expr);

		if (IsA(strippedColumnExpression, Var))
		{
			candidateColumn = (Var *) strippedColumnExpression;
		}
		else if (IsA(strippedColumnExpression, FieldSelect))
		{
			FieldSelect *compositeField = (FieldSelect *) strippedColumnExpression;
			Expr *fieldExpression = compositeField->arg;

			if (IsA(fieldExpression, Var))
			{
				candidateColumn = (Var *) fieldExpression;
			}
		}

		/* we don't support target entries that are not columns */
		if (candidateColumn == NULL)
		{
			return false;
		}

		if (candidateColumn->varlevelsup > 0)
		{
			return false;
		}
	}

	return true;
}


/*
 * FindNodeCheckInRangeTableList finds a node for which the check
 * function returns true.
 *
 * FindNodeCheckInRangeTableList relies on FindNodeCheck() but only
 * considers the range table entries.
 */
bool
FindNodeCheckInRangeTableList(List *rtable, bool (*check)(Node *))
{
	return range_table_walker(rtable, FindNodeCheck, check, QTW_EXAMINE_RTES_BEFORE);
}


/*
 * QueryContainsDistributedTableRTE determines whether the given
 * query contains a distributed table.
 */
bool
QueryContainsDistributedTableRTE(Query *query)
{
	return FindNodeCheck((Node *) query, IsDistributedTableRTE);
}


/*
 * IsDistributedTableRTE gets a node and returns true if the node
 * is a range table relation entry that points to a distributed
 * relation (i.e., excluding reference tables).
 */
bool
IsDistributedTableRTE(Node *node)
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

	relationId = rangeTableEntry->relid;
	if (!IsDistributedTable(relationId) ||
		PartitionMethod(relationId) == DISTRIBUTE_BY_NONE)
	{
		return false;
	}

	return true;
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
 * MultiNodeTree takes in a parsed query tree and uses that tree to construct a
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
MultiNode *
MultiNodeTree(Query *queryTree)
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
	DeferredErrorMessage *unsupportedQueryError = NULL;

	/* verify we can perform distributed planning on this query */
	unsupportedQueryError = DeferErrorIfQueryNotSupported(queryTree);
	if (unsupportedQueryError != NULL)
	{
		RaiseDeferredError(unsupportedQueryError, ERROR);
	}

	/* extract where clause qualifiers and verify we can plan for them */
	whereClauseList = WhereClauseList(queryTree->jointree);
	unsupportedQueryError = DeferErrorIfUnsupportedClause(whereClauseList);
	if (unsupportedQueryError)
	{
		RaiseDeferredErrorInternal(unsupportedQueryError, ERROR);
	}

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
		subqueryExtendedNode = MultiNodeTree(subqueryTree);

		SetChild((MultiUnaryNode *) subqueryCollectNode, (MultiNode *) subqueryNode);
		SetChild((MultiUnaryNode *) subqueryNode, subqueryExtendedNode);

		currentTopNode = (MultiNode *) subqueryCollectNode;
	}
	else
	{
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

		/* find best join order for commutative inner joins */
		joinOrderList = JoinOrderList(tableEntryList, joinClauseList);

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
 * ContainsReadIntermediateResultFunction determines whether an expresion tree contains
 * a call to the read_intermediate_results function.
 */
bool
ContainsReadIntermediateResultFunction(Node *node)
{
	return FindNodeCheck(node, IsReadIntermediateResultFunction);
}


/*
 * IsReadIntermediateResultFunction determines whether a given node is a function call
 * to the read_intermediate_result function.
 */
static bool
IsReadIntermediateResultFunction(Node *node)
{
	if (IsA(node, FuncExpr))
	{
		FuncExpr *funcExpr = (FuncExpr *) node;

		if (funcExpr->funcid == CitusReadIntermediateResultFuncId())
		{
			return true;
		}
	}

	return false;
}


/*
 * ErrorIfQueryNotSupported checks that we can perform distributed planning for
 * the given query. The checks in this function will be removed as we support
 * more functionality in our distributed planning.
 */
DeferredErrorMessage *
DeferErrorIfQueryNotSupported(Query *queryTree)
{
	char *errorMessage = NULL;
	bool hasTablesample = false;
	bool hasUnsupportedJoin = false;
	bool hasComplexRangeTableType = false;
	bool preconditionsSatisfied = true;
	StringInfo errorInfo = NULL;
	const char *errorHint = NULL;
	const char *joinHint = "Consider joining tables on partition column and have "
						   "equal filter on joining columns.";
	const char *filterHint = "Consider using an equality filter on the distributed "
							 "table's partition column.";

	/*
	 * There could be Sublinks in the target list as well. To produce better
	 * error messages we're checking sublinks in the where clause.
	 */
	if (queryTree->hasSubLinks && !WhereClauseContainsSubquery(queryTree))
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query with subquery outside the "
					   "FROM and WHERE clauses";
		errorHint = filterHint;
	}

	if (queryTree->hasWindowFuncs &&
		!SafeToPushdownWindowFunction(queryTree, &errorInfo))
	{
		preconditionsSatisfied = false;
		errorMessage = "could not run distributed query because the window "
					   "function that is used cannot be pushed down";
		errorHint = "Window functions are supported in two ways. Either add "
					"an equality filter on the distributed tables' partition "
					"column or use the window functions with a PARTITION BY "
					"clause containing the distribution column";
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
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 errorMessage, NULL,
							 showHint ? errorHint : NULL);
	}

	return NULL;
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
DeferredErrorMessage *
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
		 * subquery or a function. Note that RTE_FUNCTIONs are handled via (sub)query
		 * pushdown.
		 */
		if (rangeTableEntry->rtekind != RTE_RELATION &&
			rangeTableEntry->rtekind != RTE_SUBQUERY &&
			rangeTableEntry->rtekind != RTE_FUNCTION)
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
 * DeferErrorIfUnsupportedClause walks over the given list of clauses, and
 * checks that we can recognize all the clauses. This function ensures that
 * we do not drop an unsupported clause type on the floor, and thus prevents
 * erroneous results.
 *
 * Returns a deferred error, caller is responsible for raising the error.
 */
DeferredErrorMessage *
DeferErrorIfUnsupportedClause(List *clauseList)
{
	ListCell *clauseCell = NULL;
	foreach(clauseCell, clauseList)
	{
		Node *clause = (Node *) lfirst(clauseCell);

		if (!(IsSelectClause(clause) || IsJoinClause(clause) || or_clause(clause)))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "unsupported clause type", NULL, NULL);
		}
	}
	return NULL;
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
				joinClause = (Node *) canonicalize_qual((Expr *) joinClause, false);
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
				fromClause = (Node *) canonicalize_qual((Expr *) fromClause, false);
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
	Node *strippedLeftArgument = NULL;
	Node *strippedRightArgument = NULL;

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

	strippedLeftArgument = strip_implicit_coercions(leftArgument);
	strippedRightArgument = strip_implicit_coercions(rightArgument);

	/* each side of the expression should have only one column */
	if (IsA(strippedLeftArgument, Var) && IsA(strippedRightArgument, Var))
	{
		Var *leftColumn = (Var *) strippedLeftArgument;
		Var *rightColumn = (Var *) strippedRightArgument;
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
 * MultiProjectNode builds the project node using the target entry information
 * from the query tree. The project node only encapsulates projected columns,
 * and does not include aggregates, group clauses, or project expressions.
 */
MultiProject *
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
MultiExtendedOp *
MultiExtendedOpNode(Query *queryTree)
{
	MultiExtendedOp *extendedOpNode = CitusMakeNode(MultiExtendedOp);
	extendedOpNode->targetList = queryTree->targetList;
	extendedOpNode->groupClauseList = queryTree->groupClause;
	extendedOpNode->sortClauseList = queryTree->sortClause;
	extendedOpNode->limitCount = queryTree->limitCount;
	extendedOpNode->limitOffset = queryTree->limitOffset;
	extendedOpNode->havingQual = queryTree->havingQual;
	extendedOpNode->distinctClause = queryTree->distinctClause;
	extendedOpNode->hasDistinctOn = queryTree->hasDistinctOn;
	extendedOpNode->hasWindowFuncs = queryTree->hasWindowFuncs;
	extendedOpNode->windowClause = queryTree->windowClause;

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
 * ExtractRangeTableRelationWalker gathers all range table relation entries
 * in a query.
 */
bool
ExtractRangeTableRelationWalker(Node *node, List **rangeTableRelationList)
{
	bool walkIsComplete = false;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTable = (RangeTblEntry *) node;

		if (rangeTable->rtekind == RTE_RELATION && rangeTable->relkind != RELKIND_VIEW)
		{
			(*rangeTableRelationList) = lappend(*rangeTableRelationList, rangeTable);

			walkIsComplete = false;
		}
	}
	else if (IsA(node, Query))
	{
		walkIsComplete = query_tree_walker((Query *) node,
										   ExtractRangeTableRelationWalker,
										   rangeTableRelationList,
										   QTW_EXAMINE_RTES_BEFORE);
	}
	else
	{
		walkIsComplete = expression_tree_walker(node, ExtractRangeTableRelationWalker,
												rangeTableRelationList);
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
		Query *query = (Query *) node;

		/*
		 * Since we're only interested in range table entries, we only descend
		 * into all parts of the query when it is necessary. Otherwise, it is
		 * sufficient to descend into range table list since its the only part
		 * of the query that could contain range table entries.
		 */
		if (query->hasSubLinks || query->cteList || query->setOperations)
		{
			/* descend into all parts of the query */
			walkIsComplete = query_tree_walker((Query *) node,
											   ExtractRangeTableEntryWalker,
											   rangeTableList,
											   QTW_EXAMINE_RTES_BEFORE);
		}
		else
		{
			/* descend only into RTEs */
			walkIsComplete = range_table_walker(query->rtable,
												ExtractRangeTableEntryWalker,
												rangeTableList,
												QTW_EXAMINE_RTES_BEFORE);
		}
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
	/*
	 * PVC_REJECT_PLACEHOLDERS is implicit if PVC_INCLUDE_PLACEHOLDERS
	 * isn't specified.
	 */
	List *columnList = pull_var_clause(node, PVC_RECURSE_AGGREGATES |
									   PVC_RECURSE_WINDOWFUNCS);

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
		RuleApplyFunctionArray[REFERENCE_JOIN] = &ApplyReferenceJoin;
		RuleApplyFunctionArray[LOCAL_PARTITION_JOIN] = &ApplyLocalJoin;
		RuleApplyFunctionArray[SINGLE_HASH_PARTITION_JOIN] =
			&ApplySingleHashPartitionJoin;
		RuleApplyFunctionArray[SINGLE_RANGE_PARTITION_JOIN] =
			&ApplySingleRangePartitionJoin;
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
ApplyReferenceJoin(MultiNode *leftNode, MultiNode *rightNode,
				   Var *partitionColumn, JoinType joinType,
				   List *applicableJoinClauses)
{
	MultiJoin *joinNode = CitusMakeNode(MultiJoin);
	joinNode->joinRuleType = REFERENCE_JOIN;
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
 * ApplySingleRangePartitionJoin is a wrapper around ApplySinglePartitionJoin()
 * which sets the joinRuleType properly.
 */
static MultiNode *
ApplySingleRangePartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
							  Var *partitionColumn, JoinType joinType,
							  List *applicableJoinClauses)
{
	MultiJoin *joinNode =
		ApplySinglePartitionJoin(leftNode, rightNode, partitionColumn, joinType,
								 applicableJoinClauses);

	joinNode->joinRuleType = SINGLE_RANGE_PARTITION_JOIN;

	return (MultiNode *) joinNode;
}


/*
 * ApplySingleHashPartitionJoin is a wrapper around ApplySinglePartitionJoin()
 * which sets the joinRuleType properly.
 */
static MultiNode *
ApplySingleHashPartitionJoin(MultiNode *leftNode, MultiNode *rightNode,
							 Var *partitionColumn, JoinType joinType,
							 List *applicableJoinClauses)
{
	MultiJoin *joinNode =
		ApplySinglePartitionJoin(leftNode, rightNode, partitionColumn, joinType,
								 applicableJoinClauses);

	joinNode->joinRuleType = SINGLE_HASH_PARTITION_JOIN;

	return (MultiNode *) joinNode;
}


/*
 * ApplySinglePartitionJoin creates a new MultiJoin node that joins the left and
 * right node. The function also adds a MultiPartition node on top of the node
 * (left or right) that is not partitioned on the join column.
 */
static MultiJoin *
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
	joinNode->joinType = joinType;
	joinNode->joinClauseList = applicableJoinClauses;

	return joinNode;
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
