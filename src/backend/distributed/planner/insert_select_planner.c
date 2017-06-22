/*-------------------------------------------------------------------------
 *
 * insert_select_planner.c
 *
 * Planning logic for INSERT..SELECT.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/errormessage.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "utils/lsyscache.h"


static DeferredErrorMessage * DeferErrorIfCoordinatorInsertSelectUnsupported(
	Query *insertSelectQuery);
static Query * WrapSubquery(Query *subquery);


/*
 * CreatteCoordinatorInsertSelectPlan creates a query plan for a SELECT into a
 * distributed table. The query plan can also be executed on a worker in MX.
 */
MultiPlan *
CreateCoordinatorInsertSelectPlan(Query *parse)
{
	Query *insertSelectQuery = copyObject(parse);
	Query *selectQuery = NULL;

	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractInsertRangeTableEntry(insertSelectQuery);
	Oid targetRelationId = insertRte->relid;

	ListCell *selectTargetCell = NULL;
	ListCell *insertTargetCell = NULL;

	MultiPlan *multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->operation = CMD_INSERT;

	multiPlan->planningError =
		DeferErrorIfCoordinatorInsertSelectUnsupported(insertSelectQuery);

	if (multiPlan->planningError != NULL)
	{
		return multiPlan;
	}

	selectQuery = selectRte->subquery;

	/*
	 * Wrap the SELECT as a subquery if the INSERT...SELECT has CTEs or the SELECT
	 * has top-level set operations.
	 *
	 * We could simply wrap all queries, but that might create a subquery that is
	 * not supported by the logical planner. Since the logical planner also does
	 * not support CTEs and top-level set operations, we can wrap queries containing
	 * those without breaking anything.
	 */
	if (list_length(insertSelectQuery->cteList) > 0)
	{
		selectQuery = WrapSubquery(selectRte->subquery);

		/* copy CTEs from the INSERT ... SELECT statement into outer SELECT */
		selectQuery->cteList = copyObject(insertSelectQuery->cteList);
	}
	else if (selectQuery->setOperations != NULL)
	{
		/* top-level set operations confuse the ReorderInsertSelectTargetLists logic */
		selectQuery = WrapSubquery(selectRte->subquery);
	}

	selectRte->subquery = selectQuery;

	ReorderInsertSelectTargetLists(insertSelectQuery, insertRte, selectRte);

	/* add casts when the SELECT output does not directly match the table */
	forboth(insertTargetCell, insertSelectQuery->targetList,
			selectTargetCell, selectQuery->targetList)
	{
		TargetEntry *insertTargetEntry = (TargetEntry *) lfirst(insertTargetCell);
		TargetEntry *selectTargetEntry = (TargetEntry *) lfirst(selectTargetCell);

		Var *columnVar = NULL;
		Oid columnType = InvalidOid;
		int32 columnTypeMod = 0;
		Oid selectOutputType = InvalidOid;

		/* indirection is not supported, e.g. INSERT INTO table (composite_column.x) */
		if (!IsA(insertTargetEntry->expr, Var))
		{
			ereport(ERROR, (errmsg("can only handle regular columns in the target "
								   "list")));
		}

		columnVar = (Var *) insertTargetEntry->expr;
		columnType = get_atttype(targetRelationId, columnVar->varattno);
		columnTypeMod = get_atttypmod(targetRelationId, columnVar->varattno);
		selectOutputType = columnVar->vartype;

		/*
		 * If the type in the target list does not match the type of the column,
		 * we need to cast to the column type. PostgreSQL would do this
		 * automatically during the insert, but we're passing the SELECT
		 * output directly to COPY.
		 */
		if (columnType != selectOutputType)
		{
			Expr *selectExpression = selectTargetEntry->expr;
			Expr *typeCastedSelectExpr =
				(Expr *) coerce_to_target_type(NULL, (Node *) selectExpression,
											   selectOutputType, columnType,
											   columnTypeMod, COERCION_EXPLICIT,
											   COERCE_IMPLICIT_CAST, -1);

			selectTargetEntry->expr = typeCastedSelectExpr;
		}
	}

	multiPlan->insertSelectSubquery = selectQuery;
	multiPlan->insertTargetList = insertSelectQuery->targetList;
	multiPlan->targetRelationId = targetRelationId;

	return multiPlan;
}


/*
 * DeferErrorIfCoordinatorInsertSelectUnsupported returns an error if executing an
 * INSERT ... SELECT command by pulling results of the SELECT to the coordinator
 * is unsupported because it uses RETURNING, ON CONFLICT, or an append-distributed
 * table.
 */
static DeferredErrorMessage *
DeferErrorIfCoordinatorInsertSelectUnsupported(Query *insertSelectQuery)
{
	RangeTblEntry *insertRte = NULL;
	RangeTblEntry *subqueryRte = NULL;
	Query *subquery = NULL;

	if (list_length(insertSelectQuery->returningList) > 0)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "RETURNING is not supported in INSERT ... SELECT via "
							 "coordinator", NULL, NULL);
	}

	if (insertSelectQuery->onConflict)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "ON CONFLICT is not supported in INSERT ... SELECT via "
							 "coordinator", NULL, NULL);
	}

	insertRte = ExtractInsertRangeTableEntry(insertSelectQuery);
	if (PartitionMethod(insertRte->relid) == DISTRIBUTE_BY_APPEND)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "INSERT ... SELECT into an append-distributed table is "
							 "not supported", NULL, NULL);
	}

	subqueryRte = ExtractSelectRangeTableEntry(insertSelectQuery);
	subquery = (Query *) subqueryRte->subquery;

	if (NeedsDistributedPlanning(subquery) &&
		contain_nextval_expression_walker((Node *) insertSelectQuery->targetList, NULL))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "INSERT ... SELECT cannot generate sequence values when "
							 "selecting from a distributed table",
							 NULL, NULL);
	}

	return NULL;
}


/*
 * WrapSubquery wraps the given query as a subquery in a newly constructed
 * "SELECT * FROM (...subquery...) citus_insert_select_subquery" query.
 */
static Query *
WrapSubquery(Query *subquery)
{
	Query *outerQuery = NULL;
	ParseState *pstate = make_parsestate(NULL);
	Alias *selectAlias = NULL;
	RangeTblEntry *newRangeTableEntry = NULL;
	RangeTblRef *newRangeTableRef = NULL;
	ListCell *selectTargetCell = NULL;
	List *newTargetList = NIL;

	outerQuery = makeNode(Query);
	outerQuery->commandType = CMD_SELECT;

	/* create range table entries */
	selectAlias = makeAlias("citus_insert_select_subquery", NIL);
	newRangeTableEntry = addRangeTableEntryForSubquery(pstate, subquery,
													   selectAlias, false, true);
	outerQuery->rtable = list_make1(newRangeTableEntry);

	/* set the FROM expression to the subquery */
	newRangeTableRef = makeNode(RangeTblRef);
	newRangeTableRef->rtindex = 1;
	outerQuery->jointree = makeFromExpr(list_make1(newRangeTableRef), NULL);

	/* create a target list that matches the SELECT */
	foreach(selectTargetCell, subquery->targetList)
	{
		TargetEntry *selectTargetEntry = (TargetEntry *) lfirst(selectTargetCell);
		Var *newSelectVar = NULL;
		TargetEntry *newSelectTargetEntry = NULL;

		/* exactly 1 entry in FROM */
		int indexInRangeTable = 1;

		if (selectTargetEntry->resjunk)
		{
			continue;
		}

		newSelectVar = makeVar(indexInRangeTable, selectTargetEntry->resno,
							   exprType((Node *) selectTargetEntry->expr),
							   exprTypmod((Node *) selectTargetEntry->expr),
							   exprCollation((Node *) selectTargetEntry->expr), 0);

		newSelectTargetEntry = makeTargetEntry((Expr *) newSelectVar,
											   selectTargetEntry->resno,
											   selectTargetEntry->resname,
											   selectTargetEntry->resjunk);

		newTargetList = lappend(newTargetList, newSelectTargetEntry);
	}

	outerQuery->targetList = newTargetList;

	return outerQuery;
}
