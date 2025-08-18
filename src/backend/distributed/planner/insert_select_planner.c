/*-------------------------------------------------------------------------
 *
 * insert_select_planner.c
 *
 * Planning logic for INSERT..SELECT.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pg_version_constants.h"

#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/errormessage.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/recursive_planning.h"
#include "distributed/repartition_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/version_compat.h"


static void PrepareInsertSelectForCitusPlanner(Query *insertSelectQuery);
static DistributedPlan * CreateInsertSelectPlanInternal(uint64 planId,
														Query *originalQuery,
														PlannerRestrictionContext *
														plannerRestrictionContext,
														ParamListInfo boundParams);
static DistributedPlan * CreateDistributedInsertSelectPlan(Query *originalQuery,
														   PlannerRestrictionContext *
														   plannerRestrictionContext);
static bool InsertSelectHasRouterSelect(Query *originalQuery,
										PlannerRestrictionContext *
										plannerRestrictionContext);
static Task * RouterModifyTaskForShardInterval(Query *originalQuery,
											   CitusTableCacheEntry *targetTableCacheEntry,
											   ShardInterval *shardInterval,
											   PlannerRestrictionContext *
											   plannerRestrictionContext,
											   uint32 taskIdIndex,
											   bool allRelationsJoinedOnPartitionKey,
											   DeferredErrorMessage **routerPlannerError);
static Query * CreateCombineQueryForRouterPlan(DistributedPlan *distPlan);
static List * CreateTargetListForCombineQuery(List *targetList);
static DeferredErrorMessage * DistributedInsertSelectSupported(Query *queryTree,
															   RangeTblEntry *insertRte,
															   RangeTblEntry *subqueryRte,
															   bool allReferenceTables,
															   bool routerSelect,
															   PlannerRestrictionContext *
															   plannerRestrictionContext);
static DeferredErrorMessage * InsertPartitionColumnMatchesSelect(Query *query,
																 RangeTblEntry *insertRte,
																 RangeTblEntry *
																 subqueryRte,
																 Oid *
																 selectPartitionColumnTableId);
static DistributedPlan * CreateNonPushableInsertSelectPlan(uint64 planId, Query *parse,
														   ParamListInfo boundParams);
static DeferredErrorMessage * NonPushableInsertSelectSupported(Query *insertSelectQuery);
static void RelabelTargetEntryList(List *selectTargetList, List *insertTargetList);
static List * AddInsertSelectCasts(List *insertTargetList, List *selectTargetList,
								   Oid targetRelationId);
static Expr * CastExpr(Expr *expr, Oid sourceType, Oid targetType, Oid targetCollation,
					   int targetTypeMod);
static Oid GetNextvalReturnTypeCatalog(void);
static void AppendCastedEntry(TargetEntry *insertEntry, TargetEntry *selectEntry,
							  Oid castFromType, Oid targetType, Oid collation, int32
							  typmod,
							  int targetEntryIndex,
							  List **projectedEntries, List **nonProjectedEntries);
static void SetTargetEntryName(TargetEntry *tle, const char *format, int index);
static void ResetTargetEntryResno(List *targetList);
static void ProcessEntryPair(TargetEntry *insertEntry, TargetEntry *selectEntry,
							 Form_pg_attribute attr, int targetEntryIndex,
							 List **projectedEntries, List **nonProjectedEntries);


/* depth of current insert/select planner. */
static int insertSelectPlannerLevel = 0;


/*
 * InsertSelectIntoCitusTable returns true when the input query is an
 * INSERT INTO ... SELECT kind of query and the target is a citus
 * table.
 *
 * Note that the input query should be the original parsetree of
 * the query (i.e., not passed trough the standard planner).
 */
bool
InsertSelectIntoCitusTable(Query *query)
{
	bool insertSelectQuery = CheckInsertSelectQuery(query);

	if (insertSelectQuery)
	{
		RangeTblEntry *insertRte = ExtractResultRelationRTE(query);
		if (IsCitusTable(insertRte->relid))
		{
			return true;
		}
	}

	return false;
}


/*
 * InsertSelectIntoLocalTable checks whether INSERT INTO ... SELECT inserts
 * into local table. Note that query must be a sample of INSERT INTO ... SELECT
 * type of query.
 */
bool
InsertSelectIntoLocalTable(Query *query)
{
	bool insertSelectQuery = CheckInsertSelectQuery(query);

	if (insertSelectQuery)
	{
		RangeTblEntry *insertRte = ExtractResultRelationRTE(query);
		if (!IsCitusTable(insertRte->relid))
		{
			return true;
		}
	}

	return false;
}


/*
 * CheckInsertSelectQuery returns true when the input query is an INSERT INTO
 * ... SELECT kind of query.
 *
 * This function is inspired from getInsertSelectQuery() on
 * rewrite/rewriteManip.c.
 */
bool
CheckInsertSelectQuery(Query *query)
{
	CmdType commandType = query->commandType;

	if (commandType != CMD_INSERT)
	{
		return false;
	}

	if (query->jointree == NULL || !IsA(query->jointree, FromExpr))
	{
		return false;
	}

	List *fromList = query->jointree->fromlist;
	if (list_length(fromList) != 1)
	{
		return false;
	}

	RangeTblRef *rangeTableReference = linitial(fromList);
	if (!IsA(rangeTableReference, RangeTblRef))
	{
		return false;
	}

	RangeTblEntry *subqueryRte = rt_fetch(rangeTableReference->rtindex, query->rtable);
	if (subqueryRte->rtekind != RTE_SUBQUERY)
	{
		return false;
	}

	/* ensure that there is a query */
	Assert(IsA(subqueryRte->subquery, Query));

	return true;
}


/*
 * CoordinatorInsertSelectExecScan is a wrapper around
 * CoordinatorInsertSelectExecScanInternal which also properly increments
 * or decrements insertSelectExecutorLevel.
 */
DistributedPlan *
CreateInsertSelectPlan(uint64 planId, Query *originalQuery,
					   PlannerRestrictionContext *plannerRestrictionContext,
					   ParamListInfo boundParams)
{
	DistributedPlan *result = NULL;
	insertSelectPlannerLevel++;

	PG_TRY();
	{
		result = CreateInsertSelectPlanInternal(planId, originalQuery,
												plannerRestrictionContext, boundParams);
	}
	PG_CATCH();
	{
		insertSelectPlannerLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	insertSelectPlannerLevel--;
	return result;
}


/*
 * CreateInsertSelectPlan tries to create a distributed plan for an
 * INSERT INTO distributed_table SELECT ... query by push down the
 * command to the workers and if that is not possible it creates a
 * plan for evaluating the SELECT on the coordinator.
 */
static DistributedPlan *
CreateInsertSelectPlanInternal(uint64 planId, Query *originalQuery,
							   PlannerRestrictionContext *plannerRestrictionContext,
							   ParamListInfo boundParams)
{
	DeferredErrorMessage *deferredError = ErrorIfOnConflictNotSupported(originalQuery);
	if (deferredError != NULL)
	{
		/* raising the error as there is no possible solution for the unsupported on conflict statements */
		RaiseDeferredError(deferredError, ERROR);
	}

	DistributedPlan *distributedPlan = CreateDistributedInsertSelectPlan(originalQuery,
																		 plannerRestrictionContext);

	if (distributedPlan->planningError != NULL)
	{
		RaiseDeferredError(distributedPlan->planningError, DEBUG1);

		/*
		 * If INSERT..SELECT cannot be distributed, pull to coordinator or use
		 * repartitioning.
		 */
		distributedPlan = CreateNonPushableInsertSelectPlan(planId, originalQuery,
															boundParams);
	}

	return distributedPlan;
}


/*
 * CreateDistributedInsertSelectPlan creates a DistributedPlan for distributed
 * INSERT ... SELECT queries which could consist of multiple tasks.
 *
 * The function never returns NULL, it errors out if cannot create the DistributedPlan.
 */
static DistributedPlan *
CreateDistributedInsertSelectPlan(Query *originalQuery,
								  PlannerRestrictionContext *plannerRestrictionContext)
{
	List *sqlTaskList = NIL;
	uint32 taskIdIndex = 1;     /* 0 is reserved for invalid taskId */
	uint64 jobId = INVALID_JOB_ID;
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	RangeTblEntry *insertRte = ExtractResultRelationRTEOrError(originalQuery);
	RangeTblEntry *subqueryRte = ExtractSelectRangeTableEntry(originalQuery);
	Oid targetRelationId = insertRte->relid;
	CitusTableCacheEntry *targetCacheEntry = GetCitusTableCacheEntry(targetRelationId);
	int shardCount = targetCacheEntry->shardIntervalArrayLength;
	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	bool allReferenceTables = relationRestrictionContext->allReferenceTables;
	bool routerSelect =
		InsertSelectHasRouterSelect(copyObject(originalQuery),
									plannerRestrictionContext);

	distributedPlan->modLevel = RowModifyLevelForQuery(originalQuery);

	/*
	 * Error semantics for INSERT ... SELECT queries are different than regular
	 * modify queries. Thus, handle separately.
	 */
	distributedPlan->planningError = DistributedInsertSelectSupported(originalQuery,
																	  insertRte,
																	  subqueryRte,
																	  allReferenceTables,
																	  routerSelect,
																	  plannerRestrictionContext);
	if (distributedPlan->planningError)
	{
		return distributedPlan;
	}


	/*
	 * if the query goes to a single node ("router" in Citus' parlance),
	 * we don't need to go through AllDistributionKeysInQueryAreEqual checks.
	 *
	 * For PG16+, this is required as some of the outer JOINs are converted to
	 * "ON(true)" and filters are pushed down to the table scans. As
	 * AllDistributionKeysInQueryAreEqual rely on JOIN filters, it will fail to
	 * detect the router case. However, we can still detect it by checking if
	 * the query is a router query as the router query checks the filters on
	 * the tables.
	 */
	bool allDistributionKeysInQueryAreEqual =
		routerSelect ||
		AllDistributionKeysInQueryAreEqual(originalQuery, plannerRestrictionContext);

	/*
	 * Plan select query for each shard in the target table. Do so by replacing the
	 * partitioning qual parameter added in distributed_planner() using the current shard's
	 * actual boundary values. Also, add the current shard's boundary values to the
	 * top level subquery to ensure that even if the partitioning qual is not distributed
	 * to all the tables, we never run the queries on the shards that don't match with
	 * the current shard boundaries. Finally, perform the normal shard pruning to
	 * decide on whether to push the query to the current shard or not.
	 */
	for (int shardOffset = 0; shardOffset < shardCount; shardOffset++)
	{
		ShardInterval *targetShardInterval =
			targetCacheEntry->sortedShardIntervalArray[shardOffset];

		Task *modifyTask = RouterModifyTaskForShardInterval(originalQuery,
															targetCacheEntry,
															targetShardInterval,
															plannerRestrictionContext,
															taskIdIndex,
															allDistributionKeysInQueryAreEqual,
															&distributedPlan->
															planningError);

		if (distributedPlan->planningError != NULL)
		{
			return distributedPlan;
		}

		/* add the task if it could be created */
		if (modifyTask != NULL)
		{
			modifyTask->modifyWithSubquery = true;

			sqlTaskList = lappend(sqlTaskList, modifyTask);
		}

		taskIdIndex++;
	}

	/* Create the worker job */
	Job *workerJob = CitusMakeNode(Job);
	workerJob->taskList = sqlTaskList;
	workerJob->subqueryPushdown = false;
	workerJob->dependentJobList = NIL;
	workerJob->jobId = jobId;
	workerJob->jobQuery = originalQuery;
	workerJob->requiresCoordinatorEvaluation =
		RequiresCoordinatorEvaluation(originalQuery);

	/* and finally the multi plan */
	distributedPlan->workerJob = workerJob;
	distributedPlan->combineQuery = NULL;
	distributedPlan->expectResults = originalQuery->returningList != NIL;
	distributedPlan->targetRelationId = targetRelationId;

	return distributedPlan;
}


/*
 * InsertSelectHasRouterSelect is a helper function that returns true of the SELECT
 * part of the INSERT .. SELECT query is a router query.
 */
static bool
InsertSelectHasRouterSelect(Query *originalQuery,
							PlannerRestrictionContext *plannerRestrictionContext)
{
	RangeTblEntry *subqueryRte = ExtractSelectRangeTableEntry(originalQuery);
	DistributedPlan *distributedPlan = CreateRouterPlan(subqueryRte->subquery,
														subqueryRte->subquery,
														plannerRestrictionContext);

	return distributedPlan->planningError == NULL;
}


/*
 * CreateInsertSelectIntoLocalTablePlan creates the plan for INSERT .. SELECT queries
 * where the selected table is distributed and the inserted table is not.
 *
 * To create the plan, this function first creates a distributed plan for the SELECT
 * part. Then puts it as a subquery to the original (non-distributed) INSERT query as
 * a subquery. Finally, it puts this INSERT query, which now has a distributed SELECT
 * subquery, in the combineQuery.
 *
 * If the SELECT query is a router query, whose distributed plan does not have a
 * combineQuery, this function also creates a dummy combineQuery for that.
 */
DistributedPlan *
CreateInsertSelectIntoLocalTablePlan(uint64 planId, Query *insertSelectQuery,
									 ParamListInfo boundParams, bool hasUnresolvedParams,
									 PlannerRestrictionContext *plannerRestrictionContext)
{
	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);

	PrepareInsertSelectForCitusPlanner(insertSelectQuery);

	/* get the SELECT query (may have changed after PrepareInsertSelectForCitusPlanner) */
	Query *selectQuery = selectRte->subquery;

	bool allowRecursivePlanning = true;
	DistributedPlan *distPlan = CreateDistributedPlan(planId, allowRecursivePlanning,
													  selectQuery,
													  copyObject(selectQuery),
													  boundParams, hasUnresolvedParams,
													  plannerRestrictionContext);

	/*
	 * We don't expect distPlan to be NULL here because hasUnresolvedParams is
	 * already checked before this function and CreateDistributedPlan only returns
	 * NULL when there are unresolved parameters.
	 */
	Assert(distPlan != NULL);

	if (distPlan->planningError)
	{
		return distPlan;
	}

	if (distPlan->combineQuery == NULL)
	{
		/*
		 * For router queries, we construct a synthetic master query that simply passes
		 * on the results of the remote tasks, which we can then use as the select in
		 * the INSERT .. SELECT.
		 */
		distPlan->combineQuery = CreateCombineQueryForRouterPlan(
			distPlan);
	}

	/*
	 * combineQuery of a distributed select is for combining the results from
	 * worker nodes on the coordinator node. Putting it as a subquery to the
	 * INSERT query, causes the INSERT query to insert the combined select value
	 * from the workers. And making the resulting insert query the combineQuery
	 * let's us execute this insert command.
	 *
	 * So this operation makes the master query insert the result of the
	 * distributed select instead of returning it.
	 */
	selectRte->subquery = distPlan->combineQuery;
	distPlan->combineQuery = insertSelectQuery;

	return distPlan;
}


/*
 * PrepareInsertSelectForCitusPlanner prepares an INSERT..SELECT query tree
 * that was passed to the planner for use by Citus.
 *
 * First, it rebuilds the target lists of the INSERT and the SELECT
 * to be in the same order, which is not guaranteed in the parse tree.
 *
 * Second, some of the constants in the target list will have type
 * "unknown", which would confuse the Citus planner. To address that,
 * we add casts to SELECT target list entries whose type does not correspond
 * to the destination. This also helps us feed the output directly into
 * a COPY stream for INSERT..SELECT via coordinator.
 *
 * In case of UNION or other set operations, the SELECT does not have a
 * clearly defined target list, so we first wrap the UNION in a subquery.
 * UNION queries do not have the "unknown" type problem.
 *
 * Finally, if the INSERT has CTEs, we move those CTEs into the SELECT,
 * such that we can plan the SELECT as an independent query. To ensure
 * the ctelevelsup for CTE RTE's remain the same, we wrap the SELECT into
 * a subquery, unless we already did so in case of a UNION.
 */
static void
PrepareInsertSelectForCitusPlanner(Query *insertSelectQuery)
{
	RangeTblEntry *insertRte = ExtractResultRelationRTEOrError(insertSelectQuery);
	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
	Oid targetRelationId = insertRte->relid;

	bool isWrapped = false;

	if (selectRte->subquery->setOperations != NULL)
	{
		/*
		 * Prepare UNION query for reordering and adding casts by
		 * wrapping it in a subquery to have a single target list.
		 */
		selectRte->subquery = WrapSubquery(selectRte->subquery);
		isWrapped = true;
	}

	/* this is required for correct deparsing of the query */
	ReorderInsertSelectTargetLists(insertSelectQuery, insertRte, selectRte);

	/*
	 * Cast types of insert target list and select projection list to
	 * match the column types of the target relation.
	 */
	selectRte->subquery->targetList =
		AddInsertSelectCasts(insertSelectQuery->targetList,
							 copyObject(selectRte->subquery->targetList),
							 targetRelationId);

	if (list_length(insertSelectQuery->cteList) > 0)
	{
		if (!isWrapped)
		{
			/*
			 * By wrapping the SELECT in a subquery, we can avoid adjusting
			 * ctelevelsup in RTE's that point to the CTEs.
			 */
			selectRte->subquery = WrapSubquery(selectRte->subquery);
		}

		/* copy CTEs from the INSERT ... SELECT statement into outer SELECT */
		selectRte->subquery->cteList = copyObject(insertSelectQuery->cteList);
		selectRte->subquery->hasModifyingCTE = insertSelectQuery->hasModifyingCTE;
		insertSelectQuery->cteList = NIL;
	}
}


/*
 * CreateCombineQueryForRouterPlan is used for creating a dummy combineQuery
 * for a router plan, since router plans normally don't have one.
 */
static Query *
CreateCombineQueryForRouterPlan(DistributedPlan *distPlan)
{
	const Index insertTableId = 1;
	List *tableIdList = list_make1(makeInteger(insertTableId));
	Job *dependentJob = distPlan->workerJob;
	List *dependentTargetList = dependentJob->jobQuery->targetList;

	/* compute column names for the derived table */
	uint32 columnCount = (uint32) list_length(dependentTargetList);
	List *columnNameList = DerivedColumnNameList(columnCount,
												 dependentJob->jobId);

	List *funcColumnNames = NIL;
	List *funcColumnTypes = NIL;
	List *funcColumnTypeMods = NIL;
	List *funcCollations = NIL;

	TargetEntry *targetEntry = NULL;
	foreach_declared_ptr(targetEntry, dependentTargetList)
	{
		Node *expr = (Node *) targetEntry->expr;

		char *name = targetEntry->resname;
		if (name == NULL)
		{
			name = pstrdup("unnamed");
		}

		funcColumnNames = lappend(funcColumnNames, makeString(name));

		funcColumnTypes = lappend_oid(funcColumnTypes, exprType(expr));
		funcColumnTypeMods = lappend_int(funcColumnTypeMods, exprTypmod(expr));
		funcCollations = lappend_oid(funcCollations, exprCollation(expr));
	}

	RangeTblEntry *rangeTableEntry = DerivedRangeTableEntry(NULL,
															columnNameList,
															tableIdList,
															funcColumnNames,
															funcColumnTypes,
															funcColumnTypeMods,
															funcCollations);

	List *targetList = CreateTargetListForCombineQuery(dependentTargetList);

	RangeTblRef *rangeTableRef = makeNode(RangeTblRef);
	rangeTableRef->rtindex = 1;

	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->quals = NULL;
	joinTree->fromlist = list_make1(rangeTableRef);

	Query *combineQuery = makeNode(Query);
	combineQuery->commandType = CMD_SELECT;
	combineQuery->querySource = QSRC_ORIGINAL;
	combineQuery->canSetTag = true;
	combineQuery->rtable = list_make1(rangeTableEntry);

#if PG_VERSION_NUM >= PG_VERSION_16

	/*
	 * This part of the code is more of a sanity check for readability,
	 * it doesn't really do anything.
	 * We know that Only relation RTEs and subquery RTEs that were once relation
	 * RTEs (views) have their perminfoindex set. (see ExecCheckPermissions function)
	 * DerivedRangeTableEntry sets the rtekind to RTE_FUNCTION
	 * Hence we should have no perminfos here.
	 */
	Assert(rangeTableEntry->rtekind == RTE_FUNCTION &&
		   rangeTableEntry->perminfoindex == 0);
	combineQuery->rteperminfos = NIL;
#endif

	combineQuery->targetList = targetList;
	combineQuery->jointree = joinTree;
	return combineQuery;
}


/*
 * CreateTargetListForCombineQuery is used for creating a target list for
 * master query.
 */
static List *
CreateTargetListForCombineQuery(List *targetList)
{
	List *newTargetEntryList = NIL;
	const uint32 masterTableId = 1;
	int columnId = 1;

	/* iterate over original target entries */
	TargetEntry *originalTargetEntry = NULL;
	foreach_declared_ptr(originalTargetEntry, targetList)
	{
		TargetEntry *newTargetEntry = flatCopyTargetEntry(originalTargetEntry);

		Var *column = makeVarFromTargetEntry(masterTableId, originalTargetEntry);
		column->varattno = columnId;
		column->varattnosyn = columnId;
		columnId++;

		if (column->vartype == RECORDOID || column->vartype == RECORDARRAYOID)
		{
			column->vartypmod = BlessRecordExpression(originalTargetEntry->expr);
		}

		Expr *newExpression = (Expr *) column;

		newTargetEntry->expr = newExpression;
		newTargetEntryList = lappend(newTargetEntryList, newTargetEntry);
	}
	return newTargetEntryList;
}


/*
 * DistributedInsertSelectSupported returns NULL if the INSERT ... SELECT query
 * is supported, or a description why not.
 */
static DeferredErrorMessage *
DistributedInsertSelectSupported(Query *queryTree, RangeTblEntry *insertRte,
								 RangeTblEntry *subqueryRte, bool allReferenceTables,
								 bool routerSelect,
								 PlannerRestrictionContext *plannerRestrictionContext)
{
	Oid selectPartitionColumnTableId = InvalidOid;
	Oid targetRelationId = insertRte->relid;
	ListCell *rangeTableCell = NULL;

	/* we only do this check for INSERT ... SELECT queries */
	Assert(InsertSelectIntoCitusTable(queryTree));

	Query *subquery = subqueryRte->subquery;

	if (!NeedsDistributedPlanning(subquery))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "distributed INSERT ... SELECT can only select from "
							 "distributed tables",
							 NULL, NULL);
	}

	RTEListProperties *subqueryRteListProperties = GetRTEListPropertiesForQuery(subquery);
	if (subqueryRteListProperties->hasDistributedTable &&
		(subqueryRteListProperties->hasCitusLocalTable ||
		 subqueryRteListProperties->hasPostgresLocalTable))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "distributed INSERT ... SELECT cannot select from "
							 "distributed tables and local tables at the same time",
							 NULL, NULL);
	}

	if (subqueryRteListProperties->hasDistributedTable &&
		IsCitusTableType(targetRelationId, CITUS_LOCAL_TABLE))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "distributed INSERT ... SELECT cannot insert into a "
							 "local table that is added to metadata",
							 NULL, NULL);
	}

	/*
	 * In some cases, it might be possible to allow postgres local tables
	 * in distributed insert select. However, we want to behave consistent
	 * on all cases including Citus MX, and let insert select via coordinator
	 * to kick-in.
	 */
	if (subqueryRteListProperties->hasPostgresLocalTable)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "distributed INSERT ... SELECT cannot select from "
							 "a local table", NULL, NULL);
		return NULL;
	}

	/* we do not expect to see a view in modify target */
	foreach(rangeTableCell, queryTree->rtable)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION &&
			rangeTableEntry->relkind == RELKIND_VIEW)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot insert into view over distributed table",
								 NULL, NULL);
		}
	}

	if (FindNodeMatchingCheckFunction((Node *) queryTree, CitusIsVolatileFunction))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "volatile functions are not allowed in distributed "
							 "INSERT ... SELECT queries",
							 NULL, NULL);
	}

	DeferredErrorMessage *error = NULL;

	/*
	 * We can skip SQL support related checks for router queries as
	 * they are safe to route with any SQL.
	 */
	if (!routerSelect)
	{
		/* first apply toplevel pushdown checks to SELECT query */
		error =
			DeferErrorIfUnsupportedSubqueryPushdown(subquery, plannerRestrictionContext,
													true);
		if (error)
		{
			return error;
		}

		/* then apply subquery pushdown checks to SELECT query */
		error = DeferErrorIfCannotPushdownSubquery(subquery, false);
		if (error)
		{
			return error;
		}
	}

	if (IsCitusTableType(targetRelationId, CITUS_LOCAL_TABLE))
	{
		/*
		 * If we're inserting into a citus local table, it is ok because we've
		 * checked the non-existence of distributed tables in the subquery.
		 */
	}
	else if (IsCitusTableType(targetRelationId, REFERENCE_TABLE))
	{
		/*
		 * If we're inserting into a reference table, all participating tables
		 * should be reference tables as well.
		 */
		if (!allReferenceTables)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "only reference tables may be queried when targeting "
								 "a reference table with distributed INSERT ... SELECT",
								 NULL, NULL);
		}
	}
	else
	{
		/*
		 * Note that we've already checked the non-existence of Postgres
		 * tables in the subquery.
		 */
		if (subqueryRteListProperties->hasCitusLocalTable ||
			subqueryRteListProperties->hasMaterializedView)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "distributed INSERT ... SELECT cannot select from "
								 "a local relation when inserting into a distributed "
								 "table", NULL, NULL);
		}

		if (HasDistributionKey(targetRelationId))
		{
			/* ensure that INSERT's partition column comes from SELECT's partition column */
			error = InsertPartitionColumnMatchesSelect(queryTree, insertRte, subqueryRte,
													   &selectPartitionColumnTableId);
			if (error)
			{
				return error;
			}
		}
	}

	/* All tables in source list and target table should be colocated. */
	List *distributedRelationIdList = DistributedRelationIdList(subquery);
	distributedRelationIdList = lappend_oid(distributedRelationIdList,
											targetRelationId);

	if (!AllDistributedRelationsInListColocated(distributedRelationIdList))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "INSERT target relation and all source relations of the "
							 "SELECT must be colocated in distributed INSERT ... SELECT",
							 NULL, NULL);
	}

	return NULL;
}


/*
 * RouterModifyTaskForShardInterval creates a modify task by
 * replacing the partitioning qual parameter added in distributed_planner()
 * with the shardInterval's boundary value. Then perform the normal
 * shard pruning on the subquery. Finally, checks if the target shardInterval
 * has exactly same placements with the select task's available anchor
 * placements.
 *
 * The function errors out if the subquery is not router select query (i.e.,
 * subqueries with non equi-joins.).
 */
static Task *
RouterModifyTaskForShardInterval(Query *originalQuery,
								 CitusTableCacheEntry *targetTableCacheEntry,
								 ShardInterval *shardInterval,
								 PlannerRestrictionContext *plannerRestrictionContext,
								 uint32 taskIdIndex,
								 bool safeToPushdownSubquery,
								 DeferredErrorMessage **routerPlannerError)
{
	Query *copiedQuery = copyObject(originalQuery);
	RangeTblEntry *copiedInsertRte = ExtractResultRelationRTEOrError(copiedQuery);
	RangeTblEntry *copiedSubqueryRte = ExtractSelectRangeTableEntry(copiedQuery);
	Query *copiedSubquery = (Query *) copiedSubqueryRte->subquery;

	uint64 shardId = shardInterval->shardId;
	Oid distributedTableId = shardInterval->relationId;

	PlannerRestrictionContext *copyOfPlannerRestrictionContext = palloc0(
		sizeof(PlannerRestrictionContext));

	StringInfo queryString = makeStringInfo();
	ListCell *restrictionCell = NULL;
	List *selectPlacementList = NIL;
	uint64 selectAnchorShardId = INVALID_SHARD_ID;
	List *relationShardList = NIL;
	List *prunedShardIntervalListList = NIL;
	uint64 jobId = INVALID_JOB_ID;
	bool allReferenceTables =
		plannerRestrictionContext->relationRestrictionContext->allReferenceTables;
	List *shardOpExpressions = NIL;
	RestrictInfo *shardRestrictionList = NULL;
	bool multiShardModifyQuery = false;
	List *relationRestrictionList = NIL;

	copyOfPlannerRestrictionContext->relationRestrictionContext =
		CopyRelationRestrictionContext(
			plannerRestrictionContext->relationRestrictionContext);
	copyOfPlannerRestrictionContext->joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;
	copyOfPlannerRestrictionContext->fastPathRestrictionContext =
		plannerRestrictionContext->fastPathRestrictionContext;

	relationRestrictionList =
		copyOfPlannerRestrictionContext->relationRestrictionContext->
		relationRestrictionList;

	/* grab shared metadata lock to stop concurrent placement additions */
	LockShardDistributionMetadata(shardId, ShareLock);

	/*
	 * Replace the partitioning qual parameter value in all baserestrictinfos.
	 * Note that this has to be done on a copy, as the walker modifies in place.
	 */
	foreach(restrictionCell, relationRestrictionList)
	{
		RelationRestriction *restriction = lfirst(restrictionCell);
		List *originalBaseRestrictInfo = restriction->relOptInfo->baserestrictinfo;
		List *extendedBaseRestrictInfo = originalBaseRestrictInfo;
		Index rteIndex = restriction->index;

		if (!safeToPushdownSubquery || allReferenceTables)
		{
			continue;
		}

		shardOpExpressions = ShardIntervalOpExpressions(shardInterval, rteIndex);

		/* means it is a reference table and do not add any shard interval information  */
		if (shardOpExpressions == NIL)
		{
			continue;
		}

		shardRestrictionList = make_simple_restrictinfo(restriction->plannerInfo,
														(Expr *) shardOpExpressions);
		extendedBaseRestrictInfo = lappend(extendedBaseRestrictInfo,
										   shardRestrictionList);

		restriction->relOptInfo->baserestrictinfo = extendedBaseRestrictInfo;
	}

	/*
	 * We also need to add shard interval range to the subquery in case
	 * the partition qual not distributed all tables such as some
	 * subqueries in WHERE clause.
	 *
	 * Note that we need to add the ranges before the shard pruning to
	 * prevent shard pruning logic (i.e, namely UpdateRelationNames())
	 * modifies range table entries, which makes hard to add the quals.
	 */
	RTEListProperties *subqueryRteListProperties = GetRTEListPropertiesForQuery(
		copiedSubquery);
	if (subqueryRteListProperties->hasDistTableWithShardKey)
	{
		AddPartitionKeyNotNullFilterToSelect(copiedSubquery);
	}

	/* mark that we don't want the router planner to generate dummy hosts/queries */
	bool replacePrunedQueryWithDummy = false;

	/*
	 * Use router planner to decide on whether we can push down the query or not.
	 * If we can, we also rely on the side-effects that all RTEs have been updated
	 * to point to the relevant nodes and selectPlacementList is determined.
	 */
	DeferredErrorMessage *planningError = PlanRouterQuery(copiedSubquery,
														  copyOfPlannerRestrictionContext,
														  &selectPlacementList,
														  &selectAnchorShardId,
														  &relationShardList,
														  &prunedShardIntervalListList,
														  replacePrunedQueryWithDummy,
														  &multiShardModifyQuery, NULL,
														  NULL);

	Assert(!multiShardModifyQuery);

	if (planningError)
	{
		*routerPlannerError = planningError;
		return NULL;
	}


	/* ensure that we do not send queries where select is pruned away completely */
	if (list_length(selectPlacementList) == 0)
	{
		ereport(DEBUG2, (errmsg("Skipping target shard interval " UINT64_FORMAT
								" since SELECT query for it pruned away",
								shardId)));

		return NULL;
	}

	/* get the placements for insert target shard and its intersection with select */
	List *insertShardPlacementList = ActiveShardPlacementList(shardId);
	List *intersectedPlacementList = IntersectPlacementList(insertShardPlacementList,
															selectPlacementList);

	/*
	 * If insert target does not have exactly the same placements with the select,
	 * we sholdn't run the query.
	 */
	if (list_length(insertShardPlacementList) != list_length(intersectedPlacementList))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given "
							   "modification"),
						errdetail("Insert query cannot be executed on all placements "
								  "for shard " UINT64_FORMAT "", shardId)));
	}


	/* this is required for correct deparsing of the query */
	ReorderInsertSelectTargetLists(copiedQuery, copiedInsertRte, copiedSubqueryRte);

	/* setting an alias simplifies deparsing of RETURNING */
	if (copiedInsertRte->alias == NULL)
	{
		Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
		copiedInsertRte->alias = alias;
	}

	/* and generate the full query string */
	deparse_shard_query(copiedQuery, distributedTableId, shardInterval->shardId,
						queryString);
	ereport(DEBUG2, (errmsg("distributed statement: %s",
							queryString->data)));

	Task *modifyTask = CreateBasicTask(jobId, taskIdIndex, MODIFY_TASK,
									   queryString->data);
	modifyTask->dependentTaskList = NULL;
	modifyTask->anchorShardId = shardId;
	modifyTask->taskPlacementList = insertShardPlacementList;
	modifyTask->relationShardList = relationShardList;
	modifyTask->replicationModel = targetTableCacheEntry->replicationModel;
	modifyTask->isLocalTableModification = false;

	return modifyTask;
}


/*
 * ReorderInsertSelectTargetLists reorders the target lists of INSERT/SELECT
 * query which is required for deparsing purposes. The reordered query is returned.
 *
 * The necessity for this function comes from the fact that ruleutils.c is not supposed
 * to be used on "rewritten" queries (i.e. ones that have been passed through
 * QueryRewrite()). Query rewriting is the process in which views and such are expanded,
 * and, INSERT/UPDATE targetlists are reordered to match the physical order,
 * defaults etc. For the details of reordeing, see transformInsertRow() and
 * rewriteTargetListIU().
 */
Query *
ReorderInsertSelectTargetLists(Query *originalQuery, RangeTblEntry *insertRte,
							   RangeTblEntry *subqueryRte)
{
	ListCell *insertTargetEntryCell;
	List *newSubqueryTargetlist = NIL;
	List *newInsertTargetlist = NIL;
	List *columnNameList = NIL;
	int resno = 1;
	Index selectTableId = 2;
	int targetEntryIndex = 0;

	Query *subquery = subqueryRte->subquery;

	Oid insertRelationId = insertRte->relid;

	/*
	 * We implement the following algorithm for the reoderding:
	 *  - Iterate over the INSERT target list entries
	 *    - If the target entry includes a Var, find the corresponding
	 *      SELECT target entry on the original query and update resno
	 *    - If the target entry does not include a Var (i.e., defaults
	 *      or constants), create new target entry and add that to
	 *      SELECT target list
	 *    - Create a new INSERT target entry with respect to the new
	 *      SELECT target entry created.
	 */
	foreach(insertTargetEntryCell, originalQuery->targetList)
	{
		TargetEntry *oldInsertTargetEntry = lfirst(insertTargetEntryCell);
		TargetEntry *newSubqueryTargetEntry = NULL;
		AttrNumber originalAttrNo = get_attnum(insertRelationId,
											   oldInsertTargetEntry->resname);

		/* we need to explore the underlying expression */
		Node *expr = strip_implicit_coercions((Node *) oldInsertTargetEntry->expr);

		/* see transformInsertRow() for the details */
		if (IsA(expr, SubscriptingRef) || IsA(expr, FieldStore))
		{
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg(
								"cannot plan distributed INSERT INTO ... SELECT query"),
							errhint("Do not use array references and field stores "
									"on the INSERT target list.")));
		}

		/*
		 * It is safe to pull Var clause and ignore the coercions since that
		 * are already going to be added on the workers implicitly.
		 */
		List *targetVarList = pull_var_clause((Node *) oldInsertTargetEntry->expr,
											  PVC_RECURSE_AGGREGATES);

		int targetVarCount = list_length(targetVarList);

		/* a single INSERT target entry cannot have more than one Var */
		Assert(targetVarCount <= 1);

		if (targetVarCount == 1)
		{
			Var *oldInsertVar = (Var *) linitial(targetVarList);
			TargetEntry *oldSubqueryTle = list_nth(subquery->targetList,
												   oldInsertVar->varattno - 1);

			newSubqueryTargetEntry = copyObject(oldSubqueryTle);

			newSubqueryTargetEntry->resno = resno;
			newSubqueryTargetlist = lappend(newSubqueryTargetlist,
											newSubqueryTargetEntry);
		}
		else
		{
			newSubqueryTargetEntry = makeTargetEntry(oldInsertTargetEntry->expr,
													 resno,
													 oldInsertTargetEntry->resname,
													 oldInsertTargetEntry->resjunk);
			newSubqueryTargetlist = lappend(newSubqueryTargetlist,
											newSubqueryTargetEntry);
		}

		String *columnName = makeString(newSubqueryTargetEntry->resname);
		columnNameList = lappend(columnNameList, columnName);

		/*
		 * The newly created select target entry cannot be a junk entry since junk
		 * entries are not in the final target list and we're processing the
		 * final target list entries.
		 */
		Assert(!newSubqueryTargetEntry->resjunk);

		Var *newInsertVar = makeVar(selectTableId, resno,
									exprType((Node *) newSubqueryTargetEntry->expr),
									exprTypmod((Node *) newSubqueryTargetEntry->expr),
									exprCollation((Node *) newSubqueryTargetEntry->expr),
									0);
		TargetEntry *newInsertTargetEntry = makeTargetEntry((Expr *) newInsertVar,
															originalAttrNo,
															oldInsertTargetEntry->resname,
															oldInsertTargetEntry->resjunk);

		newInsertTargetlist = lappend(newInsertTargetlist, newInsertTargetEntry);
		resno++;
	}

	/*
	 * if there are any remaining target list entries (i.e., GROUP BY column not on the
	 * target list of subquery), update the remaining resnos.
	 */
	int subqueryTargetLength = list_length(subquery->targetList);
	for (; targetEntryIndex < subqueryTargetLength; ++targetEntryIndex)
	{
		TargetEntry *oldSubqueryTle = list_nth(subquery->targetList,
											   targetEntryIndex);

		/*
		 * Skip non-junk entries since we've already processed them above and this
		 * loop only is intended for junk entries.
		 */
		if (!oldSubqueryTle->resjunk)
		{
			continue;
		}

		TargetEntry *newSubqueryTargetEntry = copyObject(oldSubqueryTle);

		newSubqueryTargetEntry->resno = resno;
		newSubqueryTargetlist = lappend(newSubqueryTargetlist,
										newSubqueryTargetEntry);

		resno++;
	}

	originalQuery->targetList = newInsertTargetlist;
	subquery->targetList = newSubqueryTargetlist;
	subqueryRte->eref->colnames = columnNameList;

	return NULL;
}


/*
 * InsertPartitionColumnMatchesSelect returns NULL the partition column in the
 * table targeted by INSERTed matches with the any of the SELECTed table's
 * partition column.  Returns the error description if there's no match.
 *
 * On return without error (i.e., if partition columns match), the function
 * also sets selectPartitionColumnTableId.
 */
static DeferredErrorMessage *
InsertPartitionColumnMatchesSelect(Query *query, RangeTblEntry *insertRte,
								   RangeTblEntry *subqueryRte,
								   Oid *selectPartitionColumnTableId)
{
	ListCell *targetEntryCell = NULL;
	uint32 rangeTableId = 1;
	Oid insertRelationId = insertRte->relid;
	Var *insertPartitionColumn = PartitionColumn(insertRelationId, rangeTableId);
	Query *subquery = subqueryRte->subquery;
	bool targetTableHasPartitionColumn = false;

	foreach(targetEntryCell, query->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		List *insertTargetEntryColumnList = pull_var_clause_default((Node *) targetEntry);
		Var *subqueryPartitionColumn = NULL;

		/*
		 * We only consider target entries that include a single column. Note that this
		 * is slightly different than directly checking the whether the targetEntry->expr
		 * is a var since the var could be wrapped into an implicit/explicit casting.
		 *
		 * Also note that we skip the target entry if it does not contain a Var, which
		 * corresponds to columns with DEFAULT values on the target list.
		 */
		if (list_length(insertTargetEntryColumnList) != 1)
		{
			continue;
		}

		Var *insertVar = (Var *) linitial(insertTargetEntryColumnList);
		AttrNumber originalAttrNo = targetEntry->resno;

		/* skip processing of target table non-partition columns */
		if (originalAttrNo != insertPartitionColumn->varattno)
		{
			continue;
		}

		/* INSERT query includes the partition column */
		targetTableHasPartitionColumn = true;

		TargetEntry *subqueryTargetEntry = list_nth(subquery->targetList,
													insertVar->varattno - 1);
		Expr *selectTargetExpr = subqueryTargetEntry->expr;

		RangeTblEntry *subqueryPartitionColumnRelationIdRTE = NULL;
		List *parentQueryList = list_make2(query, subquery);
		bool skipOuterVars = false;
		FindReferencedTableColumn(selectTargetExpr,
								  parentQueryList, subquery,
								  &subqueryPartitionColumn,
								  &subqueryPartitionColumnRelationIdRTE,
								  skipOuterVars);
		Oid subqueryPartitionColumnRelationId = subqueryPartitionColumnRelationIdRTE ?
												subqueryPartitionColumnRelationIdRTE->
												relid :
												InvalidOid;

		/*
		 * Corresponding (i.e., in the same ordinal position as the target table's
		 * partition column) select target entry does not directly belong a table.
		 * Evaluate its expression type and error out properly.
		 */
		if (subqueryPartitionColumnRelationId == InvalidOid)
		{
			char *errorDetailTemplate = "Subquery contains %s in the "
										"same position as the target table's "
										"partition column.";

			char *exprDescription = "";

			switch (selectTargetExpr->type)
			{
				case T_Const:
				{
					exprDescription = "a constant value";
					break;
				}

				case T_OpExpr:
				{
					exprDescription = "an operator";
					break;
				}

				case T_FuncExpr:
				{
					FuncExpr *subqueryFunctionExpr = (FuncExpr *) selectTargetExpr;

					switch (subqueryFunctionExpr->funcformat)
					{
						case COERCE_EXPLICIT_CALL:
						{
							exprDescription = "a function call";
							break;
						}

						case COERCE_EXPLICIT_CAST:
						{
							exprDescription = "an explicit cast";
							break;
						}

						case COERCE_IMPLICIT_CAST:
						{
							exprDescription = "an implicit cast";
							break;
						}

						default:
						{
							exprDescription = "a function call";
							break;
						}
					}
					break;
				}

				case T_Aggref:
				{
					exprDescription = "an aggregation";
					break;
				}

				case T_CaseExpr:
				{
					exprDescription = "a case expression";
					break;
				}

				case T_CoalesceExpr:
				{
					exprDescription = "a coalesce expression";
					break;
				}

				case T_RowExpr:
				{
					exprDescription = "a row expression";
					break;
				}

				case T_MinMaxExpr:
				{
					exprDescription = "a min/max expression";
					break;
				}

				case T_CoerceViaIO:
				{
					exprDescription = "an explicit coercion";
					break;
				}

				default:
				{
					exprDescription =
						"an expression that is not a simple column reference";
					break;
				}
			}

			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed INSERT INTO ... SELECT "
								 "because the partition columns in the source table "
								 "and subquery do not match",
								 psprintf(errorDetailTemplate, exprDescription),
								 "Ensure the target table's partition column has a "
								 "corresponding simple column reference to a distributed "
								 "table's partition column in the subquery.");
		}

		/*
		 * Insert target expression could only be non-var if the select target
		 * entry does not have the same type (i.e., target column requires casting).
		 */
		if (!IsA(targetEntry->expr, Var))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed INSERT INTO ... SELECT "
								 "because the partition columns in the source table "
								 "and subquery do not match",
								 "The data type of the target table's partition column "
								 "should exactly match the data type of the "
								 "corresponding simple column reference in the subquery.",
								 NULL);
		}

		/* finally, check that the select target column is a partition column */
		if (!IsPartitionColumn(selectTargetExpr, subquery, skipOuterVars))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed INSERT INTO ... SELECT "
								 "because the partition columns in the source table "
								 "and subquery do not match",
								 "The target table's partition column should correspond "
								 "to a partition column in the subquery.",
								 NULL);
		}

		/* finally, check that the select target column is a partition column */
		/* we can set the select relation id */
		*selectPartitionColumnTableId = subqueryPartitionColumnRelationId;

		break;
	}

	if (!targetTableHasPartitionColumn)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot perform distributed INSERT INTO ... SELECT "
							 "because the partition columns in the source table "
							 "and subquery do not match",
							 "the query doesn't include the target table's "
							 "partition column",
							 NULL);
	}

	return NULL;
}


/*
 * CreateNonPushableInsertSelectPlan creates a query plan for a SELECT into a
 * distributed table. The query plan can also be executed on a worker in MX.
 */
static DistributedPlan *
CreateNonPushableInsertSelectPlan(uint64 planId, Query *parse, ParamListInfo boundParams)
{
	Query *insertSelectQuery = copyObject(parse);

	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractResultRelationRTEOrError(insertSelectQuery);
	Oid targetRelationId = insertRte->relid;

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->modLevel = RowModifyLevelForQuery(insertSelectQuery);

	distributedPlan->planningError =
		NonPushableInsertSelectSupported(insertSelectQuery);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	PrepareInsertSelectForCitusPlanner(insertSelectQuery);

	/* get the SELECT query (may have changed after PrepareInsertSelectForCitusPlanner) */
	Query *selectQuery = selectRte->subquery;

	/*
	 * Later we might need to call WrapTaskListForProjection(), which requires
	 * that select target list has unique names, otherwise the outer query
	 * cannot select columns unambiguously. So we relabel select columns to
	 * match target columns.
	 */
	List *insertTargetList = insertSelectQuery->targetList;
	RelabelTargetEntryList(selectQuery->targetList, insertTargetList);

	/*
	 * Make a copy of the select query, since following code scribbles it
	 * but we need to keep the original for EXPLAIN.
	 */
	Query *selectQueryCopy = copyObject(selectQuery);

	/* plan the subquery, this may be another distributed query */
	int cursorOptions = CURSOR_OPT_PARALLEL_OK;
	PlannedStmt *selectPlan = pg_plan_query(selectQueryCopy, NULL, cursorOptions,
											boundParams);

	bool repartitioned = IsRedistributablePlan(selectPlan->planTree) &&
						 IsSupportedRedistributionTarget(targetRelationId);

	/*
	 * It's not possible to generate a distributed plan for a SELECT
	 * having more than one tasks if it references a single-shard table.
	 *
	 * For this reason, right now we don't expect an INSERT .. SELECT
	 * query to go through the repartitioned INSERT .. SELECT logic if the
	 * SELECT query references a single-shard table.
	 */
	Assert(!repartitioned ||
		   !ContainsSingleShardTable(selectQueryCopy));

	distributedPlan->modifyQueryViaCoordinatorOrRepartition = insertSelectQuery;
	distributedPlan->selectPlanForModifyViaCoordinatorOrRepartition = selectPlan;
	distributedPlan->modifyWithSelectMethod = repartitioned ?
											  MODIFY_WITH_SELECT_REPARTITION :
											  MODIFY_WITH_SELECT_VIA_COORDINATOR;
	distributedPlan->expectResults = insertSelectQuery->returningList != NIL;
	distributedPlan->intermediateResultIdPrefix = InsertSelectResultIdPrefix(planId);
	distributedPlan->targetRelationId = targetRelationId;

	return distributedPlan;
}


/*
 * NonPushableInsertSelectSupported returns an error if executing an
 * INSERT ... SELECT command by pulling results of the SELECT to the coordinator
 * or with repartitioning is unsupported because it needs to generate sequence
 * values or insert into an append-distributed table.
 */
static DeferredErrorMessage *
NonPushableInsertSelectSupported(Query *insertSelectQuery)
{
	DeferredErrorMessage *deferredError = ErrorIfOnConflictNotSupported(
		insertSelectQuery);
	if (deferredError)
	{
		return deferredError;
	}

	RangeTblEntry *insertRte = ExtractResultRelationRTE(insertSelectQuery);
	if (IsCitusTableType(insertRte->relid, APPEND_DISTRIBUTED))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "INSERT ... SELECT into an append-distributed table is "
							 "not supported", NULL, NULL);
	}

	return NULL;
}


/*
 * InsertSelectResultPrefix returns the prefix to use for intermediate
 * results of an INSERT ... SELECT via the coordinator that runs in two
 * phases in order to do RETURNING or ON CONFLICT.
 */
char *
InsertSelectResultIdPrefix(uint64 planId)
{
	StringInfo resultIdPrefix = makeStringInfo();

	appendStringInfo(resultIdPrefix, "insert_select_" UINT64_FORMAT, planId);

	return resultIdPrefix->data;
}


/*
 * Return true if the expression tree can change value within a single scan
 * (i.e. the planner must treat it as VOLATILE).
 * We just delegate to PostgreSQL’s helper.
 */
static inline bool
expr_is_volatile(Node *node)
{
	/* contain_volatile_functions() also returns true for set-returning
	 * volatile functions and for nextval()/currval(). */
	return contain_volatile_functions(node);
}


/*
 * WrapSubquery
 *
 * Build a wrapper query:
 *
 *     SELECT <outer-TL>
 *       FROM ( <subquery with any volatile items stripped> )
 *            citus_insert_select_subquery
 *
 * Purpose:
 *   - Preserve column numbering while lifting volatile expressions to the coordinator.
 *   - Volatile (non-deterministic) expressions not used in GROUP BY / ORDER BY
 *     are lifted to the outer SELECT to ensure they are evaluated only once.
 *   - Stable/immutable expressions or volatile ones required by GROUP BY / ORDER BY
 *     stay in the subquery and are accessed via Vars in the outer SELECT.
 */
Query *
WrapSubquery(Query *subquery)
{
	/*
	 * 1. Build the wrapper skeleton: SELECT ... FROM (subquery) alias
	 */
	ParseState *pstate = make_parsestate(NULL);
	Query *outerQuery = makeNode(Query);
	outerQuery->commandType = CMD_SELECT;

	Alias *alias = makeAlias("citus_insert_select_subquery", NIL);
	RangeTblEntry *rte_subq =
		RangeTableEntryFromNSItem(
			addRangeTableEntryForSubquery(pstate,
										  subquery,    /* still points to original subquery */
										  alias,
										  false,       /* not LATERAL */
										  true));      /* in FROM clause */

	outerQuery->rtable = list_make1(rte_subq);

#if PG_VERSION_NUM >= PG_VERSION_16

	/* Ensure RTE_SUBQUERY has proper permission handling */
	Assert(rte_subq->rtekind == RTE_SUBQUERY &&
		   rte_subq->perminfoindex == 0);
	outerQuery->rteperminfos = NIL;
#endif

	RangeTblRef *rtref = makeNode(RangeTblRef);
	rtref->rtindex = 1;  /* Only one RTE, so index is 1 */
	outerQuery->jointree = makeFromExpr(list_make1(rtref), NULL);

	/*
	 * 2. Create new target lists for inner (worker) and outer (coordinator)
	 */
	List *newInnerTL = NIL;
	List *newOuterTL = NIL;
	int nextResno = 1;

	TargetEntry *te = NULL;
	foreach_declared_ptr(te, subquery->targetList)
	{
		if (te->resjunk)
		{
			/* Keep resjunk entries only in subquery (not in outer query) */
			newInnerTL = lappend(newInnerTL, te);
			continue;
		}

		bool isVolatile = expr_is_volatile((Node *) te->expr);
		bool usedInSort = (te->ressortgroupref != 0);

		if (isVolatile && !usedInSort)
		{
			/*
			 * Lift volatile expression to outer query so it's evaluated once.
			 * In inner query, place a NULL of the same type to preserve column position.
			 */
			TargetEntry *outerTE =
				makeTargetEntry(copyObject(te->expr),
								list_length(newOuterTL) + 1,
								te->resname,
								false);
			newOuterTL = lappend(newOuterTL, outerTE);

			Const *nullConst = makeNullConst(exprType((Node *) te->expr),
											 exprTypmod((Node *) te->expr),
											 exprCollation((Node *) te->expr));

			TargetEntry *placeholder =
				makeTargetEntry((Expr *) nullConst,
								nextResno++,          /* preserve column position */
								te->resname,
								false);               /* visible, not resjunk */
			newInnerTL = lappend(newInnerTL, placeholder);
		}
		else
		{
			/*
			 * Either:
			 *   - expression is stable or immutable, or
			 *   - volatile but needed for sorting or grouping
			 *
			 * In both cases, keep it in subquery and reference it using a Var.
			 */
			TargetEntry *innerTE = te;          /* reuse original node */
			innerTE->resno = nextResno++;
			newInnerTL = lappend(newInnerTL, innerTE);

			Var *v = makeVar(/* subquery reference index is 1 */
				rtref->rtindex,     /* same as 1, but self‑documenting */
				innerTE->resno,
				exprType((Node *) innerTE->expr),
				exprTypmod((Node *) innerTE->expr),
				exprCollation((Node *) innerTE->expr),
				0);

			TargetEntry *outerTE =
				makeTargetEntry((Expr *) v,
								list_length(newOuterTL) + 1,
								innerTE->resname,
								false);
			newOuterTL = lappend(newOuterTL, outerTE);
		}
	}

	/*
	 * 3. Assign target lists and return the wrapper query
	 */
	subquery->targetList = newInnerTL;
	outerQuery->targetList = newOuterTL;

	return outerQuery;
}


/*
 * RelabelTargetEntryList relabels select target list to have matching names with
 * insert target list.
 */
static void
RelabelTargetEntryList(List *selectTargetList, List *insertTargetList)
{
	TargetEntry *selectTargetEntry = NULL;
	TargetEntry *insertTargetEntry = NULL;
	forboth_ptr(selectTargetEntry, selectTargetList, insertTargetEntry, insertTargetList)
	{
		selectTargetEntry->resname = insertTargetEntry->resname;
	}
}


/*
 * AddInsertSelectCasts ensures that the columns in the given target lists
 * have the same type as the corresponding columns of the target relation.
 * It adds casts when necessary.
 *
 * Returns the updated selectTargetList.
 */
static List *
AddInsertSelectCasts(List *insertTargetList, List *selectTargetList,
					 Oid targetRelationId)
{
	List *projectedEntries = NIL;
	List *nonProjectedEntries = NIL;

	/*
	 * ReorderInsertSelectTargetLists() ensures that the first few columns of the
	 * SELECT query match the insert targets. It might also include additional
	 * items (for GROUP BY, etc.), so the insertTargetList is shorter.
	 */
	Assert(list_length(insertTargetList) <= list_length(selectTargetList));

	Relation distributedRelation = table_open(targetRelationId, RowExclusiveLock);
	TupleDesc destTupleDescriptor = RelationGetDescr(distributedRelation);

	int targetEntryIndex = 0;
	TargetEntry *insertEntry = NULL;
	TargetEntry *selectEntry = NULL;

	forboth_ptr(insertEntry, insertTargetList, selectEntry, selectTargetList)
	{
		/*
		 * Retrieve the target attribute corresponding to the insert entry.
		 * The attribute is located at (resno - 1) in the tuple descriptor.
		 */
		Form_pg_attribute attr = TupleDescAttr(destTupleDescriptor,
											   insertEntry->resno - 1);

		ProcessEntryPair(insertEntry, selectEntry, attr, targetEntryIndex,
						 &projectedEntries, &nonProjectedEntries);

		targetEntryIndex++;
	}

	/* Append any additional non-projected entries from selectTargetList */
	for (int entryIndex = list_length(insertTargetList);
		 entryIndex < list_length(selectTargetList);
		 entryIndex++)
	{
		nonProjectedEntries = lappend(nonProjectedEntries, list_nth(selectTargetList,
																	entryIndex));
	}

	/* Concatenate projected and non-projected entries and reset resno numbering */
	selectTargetList = list_concat(projectedEntries, nonProjectedEntries);
	ResetTargetEntryResno(selectTargetList);

	table_close(distributedRelation, NoLock);

	return selectTargetList;
}


/*
 * Processes a single pair of insert and select target entries.
 * It compares the source and target types and appends either the
 * original select entry or a casted version to the appropriate list.
 */
static void
ProcessEntryPair(TargetEntry *insertEntry, TargetEntry *selectEntry,
				 Form_pg_attribute attr, int targetEntryIndex,
				 List **projectedEntries, List **nonProjectedEntries)
{
	Oid effectiveSourceType = exprType((Node *) selectEntry->expr);
	Oid targetType = attr->atttypid;

	/*
	 * If the select expression is a NextValueExpr, use its actual return type.
	 *
	 * NextValueExpr represents a call to the nextval() function, which is used to
	 * obtain the next value from a sequence—commonly for populating auto-increment
	 * columns. In many cases, nextval() returns an INT8 (bigint), but the actual
	 * return type may differ depending on database configuration or custom implementations.
	 *
	 * Since the target column might have a different type (e.g., INT4), we need to
	 * obtain the real return type of nextval() to ensure that any type coercion is applied
	 * correctly. This is done by calling GetNextvalReturnTypeCatalog(), which looks up the
	 * function in the catalog and returns its return type. The effectiveSourceType is then
	 * set to this value, ensuring that subsequent comparisons and casts use the correct type.
	 */
	if (IsA(selectEntry->expr, NextValueExpr))
	{
		effectiveSourceType = GetNextvalReturnTypeCatalog();
	}

	if (effectiveSourceType != targetType)
	{
		AppendCastedEntry(insertEntry, selectEntry,
						  effectiveSourceType, targetType,
						  attr->attcollation, attr->atttypmod,
						  targetEntryIndex,
						  projectedEntries, nonProjectedEntries);
	}
	else
	{
		/* Types match, no cast needed */
		*projectedEntries = lappend(*projectedEntries, selectEntry);
	}
}


/*
 * Resets the resno field for each target entry in the list so that
 * they are numbered sequentially.
 */
static void
ResetTargetEntryResno(List *targetList)
{
	int entryResNo = 1;
	ListCell *lc = NULL;
	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		tle->resno = entryResNo++;
	}
}


/*
 * Looks up the nextval(regclass) function in pg_proc, returning its actual
 * rettype. In a standard build, that will be INT8OID, but this is more robust.
 */
static Oid
GetNextvalReturnTypeCatalog(void)
{
	Oid argTypes[1] = { REGCLASSOID };
	List *nameList = list_make1(makeString("nextval"));

	/* Look up the nextval(regclass) function */
	Oid nextvalFuncOid = LookupFuncName(nameList, 1, argTypes, false);
	if (!OidIsValid(nextvalFuncOid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not find function nextval(regclass)")));
	}

	/* Retrieve and validate the return type of the nextval function */
	Oid nextvalReturnType = get_func_rettype(nextvalFuncOid);
	if (!OidIsValid(nextvalReturnType))
	{
		elog(ERROR, "could not determine return type of nextval(regclass)");
	}

	return nextvalReturnType;
}


/**
 * Modifies the given insert entry to match the target column's type and typmod,
 * then creates and appends a new target entry containing a casted expression
 * to the projected list. If the original select entry is used by ORDER BY or GROUP BY,
 * it is marked as junk to avoid ambiguity.
 */
static void
AppendCastedEntry(TargetEntry *insertEntry, TargetEntry *selectEntry,
				  Oid castFromType, Oid targetType, Oid collation, int32 typmod,
				  int targetEntryIndex,
				  List **projectedEntries, List **nonProjectedEntries)
{
	/* Update the insert entry's Var to match the target column's type, typmod, and collation */
	Assert(IsA(insertEntry->expr, Var));
	{
		Var *insertVar = (Var *) insertEntry->expr;
		insertVar->vartype = targetType;
		insertVar->vartypmod = typmod;
		insertVar->varcollid = collation;
	}

	/* Create a new TargetEntry with the casted expression */
	TargetEntry *coercedEntry = copyObject(selectEntry);
	coercedEntry->expr = CastExpr((Expr *) selectEntry->expr,
								  castFromType,
								  targetType,
								  collation,
								  typmod);
	coercedEntry->ressortgroupref = 0;

	/* Assign a unique name to the coerced entry */
	SetTargetEntryName(coercedEntry, "auto_coerced_by_citus_%d", targetEntryIndex);
	*projectedEntries = lappend(*projectedEntries, coercedEntry);

	/* If the original select entry is referenced in ORDER BY or GROUP BY,
	 * mark it as junk and rename it to avoid ambiguity.
	 */
	if (selectEntry->ressortgroupref != 0)
	{
		selectEntry->resjunk = true;
		SetTargetEntryName(selectEntry, "discarded_target_item_%d", targetEntryIndex);
		*nonProjectedEntries = lappend(*nonProjectedEntries, selectEntry);
	}
}


/*
 * CastExpr returns an expression which casts the given expr from sourceType to
 * the given targetType.
 */
static Expr *
CastExpr(Expr *expr, Oid sourceType, Oid targetType, Oid targetCollation,
		 int targetTypeMod)
{
	Oid coercionFuncId = InvalidOid;
	CoercionPathType coercionType = find_coercion_pathway(targetType, sourceType,
														  COERCION_EXPLICIT,
														  &coercionFuncId);

	if (coercionType == COERCION_PATH_FUNC)
	{
		FuncExpr *coerceExpr = makeNode(FuncExpr);
		coerceExpr->funcid = coercionFuncId;
		coerceExpr->args = list_make1(copyObject(expr));
		coerceExpr->funccollid = targetCollation;
		coerceExpr->funcresulttype = targetType;

		return (Expr *) coerceExpr;
	}
	else if (coercionType == COERCION_PATH_RELABELTYPE)
	{
		RelabelType *coerceExpr = makeNode(RelabelType);
		coerceExpr->arg = copyObject(expr);
		coerceExpr->resulttype = targetType;
		coerceExpr->resulttypmod = targetTypeMod;
		coerceExpr->resultcollid = targetCollation;
		coerceExpr->relabelformat = COERCE_IMPLICIT_CAST;
		coerceExpr->location = -1;

		return (Expr *) coerceExpr;
	}
	else if (coercionType == COERCION_PATH_ARRAYCOERCE)
	{
		Oid sourceBaseType = get_base_element_type(sourceType);
		Oid targetBaseType = get_base_element_type(targetType);

		CaseTestExpr *elemExpr = makeNode(CaseTestExpr);
		elemExpr->collation = targetCollation;
		elemExpr->typeId = sourceBaseType;
		elemExpr->typeMod = -1;

		Expr *elemCastExpr = CastExpr((Expr *) elemExpr, sourceBaseType,
									  targetBaseType, targetCollation,
									  targetTypeMod);

		ArrayCoerceExpr *coerceExpr = makeNode(ArrayCoerceExpr);
		coerceExpr->arg = copyObject(expr);
		coerceExpr->elemexpr = elemCastExpr;
		coerceExpr->resultcollid = targetCollation;
		coerceExpr->resulttype = targetType;
		coerceExpr->resulttypmod = targetTypeMod;
		coerceExpr->location = -1;
		coerceExpr->coerceformat = COERCE_IMPLICIT_CAST;

		return (Expr *) coerceExpr;
	}
	else if (coercionType == COERCION_PATH_COERCEVIAIO)
	{
		CoerceViaIO *coerceExpr = makeNode(CoerceViaIO);
		coerceExpr->arg = (Expr *) copyObject(expr);
		coerceExpr->resulttype = targetType;
		coerceExpr->resultcollid = targetCollation;
		coerceExpr->coerceformat = COERCE_IMPLICIT_CAST;
		coerceExpr->location = -1;

		return (Expr *) coerceExpr;
	}
	else
	{
		ereport(ERROR, (errmsg("could not find a conversion path from type %d to %d",
							   sourceType, targetType)));
	}

	return NULL; /* keep compiler happy */
}


/* Helper function to set the target entry name using a formatted string */
static void
SetTargetEntryName(TargetEntry *tle, const char *format, int index)
{
	StringInfo resnameString = makeStringInfo();
	appendStringInfo(resnameString, format, index);
	tle->resname = resnameString->data;
}


/* PlanningInsertSelect returns true if we are planning an INSERT ...SELECT query */
bool
PlanningInsertSelect(void)
{
	return insertSelectPlannerLevel > 0;
}
