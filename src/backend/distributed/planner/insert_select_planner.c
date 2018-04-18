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

#include "catalog/pg_class.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/errormessage.h"
#include "distributed/insert_select_planner.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/query_pushdown_planning_utils.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "utils/lsyscache.h"


static DistributedPlan * CreateDistributedInsertSelectPlan(Query *originalQuery,
														   PlannerRestrictionContext *
														   plannerRestrictionContext);
static Task * RouterModifyTaskForShardInterval(Query *originalQuery,
											   ShardInterval *shardInterval,
											   PlannerRestrictionContext *
											   plannerRestrictionContext,
											   uint32 taskIdIndex,
											   bool allRelationsJoinedOnPartitionKey);
static DeferredErrorMessage * DistributedInsertSelectSupported(Query *queryTree,
															   RangeTblEntry *insertRte,
															   RangeTblEntry *subqueryRte,
															   bool allReferenceTables);
static DeferredErrorMessage * MultiTaskRouterSelectQuerySupported(Query *query);
static bool HasUnsupportedDistinctOn(Query *query);
static DeferredErrorMessage * InsertPartitionColumnMatchesSelect(Query *query,
																 RangeTblEntry *insertRte,
																 RangeTblEntry *
																 subqueryRte,
																 Oid *
																 selectPartitionColumnTableId);
static DistributedPlan * CreateCoordinatorInsertSelectPlan(Query *parse);
static DeferredErrorMessage * CoordinatorInsertSelectSupported(Query *insertSelectQuery);
static Query * WrapSubquery(Query *subquery);
static bool CheckInsertSelectQuery(Query *query);


/*
 * InsertSelectIntoDistributedTable returns true when the input query is an
 * INSERT INTO ... SELECT kind of query and the target is a distributed
 * table.
 *
 * Note that the input query should be the original parsetree of
 * the query (i.e., not passed trough the standard planner).
 */
bool
InsertSelectIntoDistributedTable(Query *query)
{
	bool insertSelectQuery = CheckInsertSelectQuery(query);

	if (insertSelectQuery)
	{
		RangeTblEntry *insertRte = ExtractInsertRangeTableEntry(query);
		if (IsDistributedTable(insertRte->relid))
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
		RangeTblEntry *insertRte = ExtractInsertRangeTableEntry(query);
		if (!IsDistributedTable(insertRte->relid))
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
static bool
CheckInsertSelectQuery(Query *query)
{
	CmdType commandType = query->commandType;
	List *fromList = NULL;
	RangeTblRef *rangeTableReference = NULL;
	RangeTblEntry *subqueryRte = NULL;

	if (commandType != CMD_INSERT)
	{
		return false;
	}

	if (query->jointree == NULL || !IsA(query->jointree, FromExpr))
	{
		return false;
	}

	fromList = query->jointree->fromlist;
	if (list_length(fromList) != 1)
	{
		return false;
	}

	rangeTableReference = linitial(fromList);
	if (!IsA(rangeTableReference, RangeTblRef))
	{
		return false;
	}

	subqueryRte = rt_fetch(rangeTableReference->rtindex, query->rtable);
	if (subqueryRte->rtekind != RTE_SUBQUERY)
	{
		return false;
	}

	/* ensure that there is a query */
	Assert(IsA(subqueryRte->subquery, Query));

	return true;
}


/*
 * CreateInsertSelectPlan tries to create a distributed plan for an
 * INSERT INTO distributed_table SELECT ... query by push down the
 * command to the workers and if that is not possible it creates a
 * plan for evaluating the SELECT on the coordinator.
 */
DistributedPlan *
CreateInsertSelectPlan(Query *originalQuery,
					   PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = NULL;

	distributedPlan = CreateDistributedInsertSelectPlan(originalQuery,
														plannerRestrictionContext);

	if (distributedPlan->planningError != NULL)
	{
		RaiseDeferredError(distributedPlan->planningError, DEBUG1);

		/* if INSERT..SELECT cannot be distributed, pull to coordinator */
		distributedPlan = CreateCoordinatorInsertSelectPlan(originalQuery);
	}

	return distributedPlan;
}


/*
 * CreateDistributedInsertSelectPlan Creates a DistributedPlan for distributed
 * INSERT ... SELECT queries which could consists of multiple tasks.
 *
 * The function never returns NULL, it errors out if cannot create the DistributedPlan.
 */
static DistributedPlan *
CreateDistributedInsertSelectPlan(Query *originalQuery,
								  PlannerRestrictionContext *plannerRestrictionContext)
{
	int shardOffset = 0;
	List *sqlTaskList = NIL;
	uint32 taskIdIndex = 1;     /* 0 is reserved for invalid taskId */
	Job *workerJob = NULL;
	uint64 jobId = INVALID_JOB_ID;
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	RangeTblEntry *insertRte = ExtractInsertRangeTableEntry(originalQuery);
	RangeTblEntry *subqueryRte = ExtractSelectRangeTableEntry(originalQuery);
	Oid targetRelationId = insertRte->relid;
	DistTableCacheEntry *targetCacheEntry = DistributedTableCacheEntry(targetRelationId);
	int shardCount = targetCacheEntry->shardIntervalArrayLength;
	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	bool allReferenceTables = relationRestrictionContext->allReferenceTables;
	bool allDistributionKeysInQueryAreEqual = false;

	distributedPlan->operation = originalQuery->commandType;

	/*
	 * Error semantics for INSERT ... SELECT queries are different than regular
	 * modify queries. Thus, handle separately.
	 */
	distributedPlan->planningError = DistributedInsertSelectSupported(originalQuery,
																	  insertRte,
																	  subqueryRte,
																	  allReferenceTables);
	if (distributedPlan->planningError)
	{
		return distributedPlan;
	}

	allDistributionKeysInQueryAreEqual =
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
	for (shardOffset = 0; shardOffset < shardCount; shardOffset++)
	{
		ShardInterval *targetShardInterval =
			targetCacheEntry->sortedShardIntervalArray[shardOffset];
		Task *modifyTask = NULL;

		modifyTask = RouterModifyTaskForShardInterval(originalQuery, targetShardInterval,
													  plannerRestrictionContext,
													  taskIdIndex,
													  allDistributionKeysInQueryAreEqual);

		/* add the task if it could be created */
		if (modifyTask != NULL)
		{
			modifyTask->modifyWithMultipleTableQuery = true;

			sqlTaskList = lappend(sqlTaskList, modifyTask);
		}

		++taskIdIndex;
	}

	/* Create the worker job */
	workerJob = CitusMakeNode(Job);
	workerJob->taskList = sqlTaskList;
	workerJob->subqueryPushdown = false;
	workerJob->dependedJobList = NIL;
	workerJob->jobId = jobId;
	workerJob->jobQuery = originalQuery;
	workerJob->requiresMasterEvaluation = RequiresMasterEvaluation(originalQuery);

	/* and finally the multi plan */
	distributedPlan->workerJob = workerJob;
	distributedPlan->masterQuery = NULL;
	distributedPlan->routerExecutable = true;
	distributedPlan->hasReturning = false;

	if (list_length(originalQuery->returningList) > 0)
	{
		distributedPlan->hasReturning = true;
	}

	return distributedPlan;
}


/*
 * DistributedInsertSelectSupported returns NULL if the INSERT ... SELECT query
 * is supported, or a description why not.
 */
static DeferredErrorMessage *
DistributedInsertSelectSupported(Query *queryTree, RangeTblEntry *insertRte,
								 RangeTblEntry *subqueryRte, bool allReferenceTables)
{
	Query *subquery = NULL;
	Oid selectPartitionColumnTableId = InvalidOid;
	Oid targetRelationId = insertRte->relid;
	char targetPartitionMethod = PartitionMethod(targetRelationId);
	ListCell *rangeTableCell = NULL;
	DeferredErrorMessage *error = NULL;

	/* we only do this check for INSERT ... SELECT queries */
	AssertArg(InsertSelectIntoDistributedTable(queryTree));

	subquery = subqueryRte->subquery;

	if (!NeedsDistributedPlanning(subquery))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "distributed INSERT ... SELECT can only select from "
							 "distributed tables",
							 NULL, NULL);
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

	if (contain_volatile_functions((Node *) queryTree))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "volatile functions are not allowed in distributed "
							 "INSERT ... SELECT queries",
							 NULL, NULL);
	}

	/* we don't support LIMIT, OFFSET and WINDOW functions */
	error = MultiTaskRouterSelectQuerySupported(subquery);
	if (error)
	{
		return error;
	}

	/*
	 * If we're inserting into a reference table, all participating tables
	 * should be reference tables as well.
	 */
	if (targetPartitionMethod == DISTRIBUTE_BY_NONE)
	{
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
		DeferredErrorMessage *error = NULL;

		/* ensure that INSERT's partition column comes from SELECT's partition column */
		error = InsertPartitionColumnMatchesSelect(queryTree, insertRte, subqueryRte,
												   &selectPartitionColumnTableId);
		if (error)
		{
			return error;
		}

		/*
		 * We expect partition column values come from colocated tables. Note that we
		 * skip this check from the reference table case given that all reference tables
		 * are already (and by default) co-located.
		 */
		if (!TablesColocated(insertRte->relid, selectPartitionColumnTableId))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "INSERT target table and the source relation of the SELECT partition "
								 "column value must be colocated in distributed INSERT ... SELECT",
								 NULL, NULL);
		}
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
RouterModifyTaskForShardInterval(Query *originalQuery, ShardInterval *shardInterval,
								 PlannerRestrictionContext *plannerRestrictionContext,
								 uint32 taskIdIndex,
								 bool safeToPushdownSubquery)
{
	Query *copiedQuery = copyObject(originalQuery);
	RangeTblEntry *copiedInsertRte = ExtractInsertRangeTableEntry(copiedQuery);
	RangeTblEntry *copiedSubqueryRte = ExtractSelectRangeTableEntry(copiedQuery);
	Query *copiedSubquery = (Query *) copiedSubqueryRte->subquery;

	uint64 shardId = shardInterval->shardId;
	Oid distributedTableId = shardInterval->relationId;
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);

	RelationRestrictionContext *copiedRestrictionContext =
		CopyRelationRestrictionContext(
			plannerRestrictionContext->relationRestrictionContext);
	RelationRestrictionContext *originalRestrictionContext =
		CopyRelationRestrictionContext(
			plannerRestrictionContext->relationRestrictionContext);

	StringInfo queryString = makeStringInfo();
	ListCell *restrictionCell = NULL;
	Task *modifyTask = NULL;
	List *selectPlacementList = NIL;
	uint64 selectAnchorShardId = INVALID_SHARD_ID;
	List *relationShardList = NIL;
	uint64 jobId = INVALID_JOB_ID;
	List *insertShardPlacementList = NULL;
	List *intersectedPlacementList = NULL;
	bool upsertQuery = false;
	bool replacePrunedQueryWithDummy = false;
	bool allReferenceTables =
		plannerRestrictionContext->relationRestrictionContext->allReferenceTables;
	List *shardOpExpressions = NIL;
	RestrictInfo *shardRestrictionList = NULL;
	DeferredErrorMessage *planningError = NULL;
	bool multiShardModifyQuery = false;

	/* grab shared metadata lock to stop concurrent placement additions */
	LockShardDistributionMetadata(shardId, ShareLock);

	/*
	 * Replace the partitioning qual parameter value in all baserestrictinfos.
	 * Note that this has to be done on a copy, as the walker modifies in place.
	 */
	foreach(restrictionCell, copiedRestrictionContext->relationRestrictionList)
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

		shardRestrictionList = make_simple_restrictinfo((Expr *) shardOpExpressions);
		extendedBaseRestrictInfo = lappend(extendedBaseRestrictInfo,
										   shardRestrictionList);

		restriction->relOptInfo->baserestrictinfo = extendedBaseRestrictInfo;
	}

	/*
	 * We also need to add  shard interval range to the subquery in case
	 * the partition qual not distributed all tables such as some
	 * subqueries in WHERE clause.
	 *
	 * Note that we need to add the ranges before the shard pruning to
	 * prevent shard pruning logic (i.e, namely UpdateRelationNames())
	 * modifies range table entries, which makes hard to add the quals.
	 */
	if (!allReferenceTables)
	{
		AddShardIntervalRestrictionToSelect(copiedSubquery, shardInterval);
	}

	/* mark that we don't want the router planner to generate dummy hosts/queries */
	replacePrunedQueryWithDummy = false;

	/*
	 * Use router planner to decide on whether we can push down the query or not.
	 * If we can, we also rely on the side-effects that all RTEs have been updated
	 * to point to the relevant nodes and selectPlacementList is determined.
	 */

	plannerRestrictionContext->relationRestrictionContext = copiedRestrictionContext;
	planningError = PlanRouterQuery(copiedSubquery, plannerRestrictionContext,
									&selectPlacementList, &selectAnchorShardId,
									&relationShardList, replacePrunedQueryWithDummy,
									&multiShardModifyQuery);
	plannerRestrictionContext->relationRestrictionContext = originalRestrictionContext;

	Assert(!multiShardModifyQuery);

	if (planningError)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given "
							   "modification"),
						errdetail("Select query cannot be pushed down to the worker.")));
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
	insertShardPlacementList = FinalizedShardPlacementList(shardId);
	intersectedPlacementList = IntersectPlacementList(insertShardPlacementList,
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

	/* set the upsert flag */
	if (originalQuery->onConflict != NULL)
	{
		upsertQuery = true;
	}

	/* setting an alias simplifies deparsing of RETURNING */
	if (copiedInsertRte->alias == NULL)
	{
		Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
		copiedInsertRte->alias = alias;
	}

	/* and generate the full query string */
	deparse_shard_query(copiedQuery, distributedTableId, shardInterval->shardId,
						queryString);
	ereport(DEBUG2, (errmsg("distributed statement: %s", queryString->data)));

	modifyTask = CreateBasicTask(jobId, taskIdIndex, MODIFY_TASK, queryString->data);
	modifyTask->dependedTaskList = NULL;
	modifyTask->anchorShardId = shardId;
	modifyTask->taskPlacementList = insertShardPlacementList;
	modifyTask->upsertQuery = upsertQuery;
	modifyTask->relationShardList = relationShardList;
	modifyTask->replicationModel = cacheEntry->replicationModel;

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
	Query *subquery = NULL;
	ListCell *insertTargetEntryCell;
	List *newSubqueryTargetlist = NIL;
	List *newInsertTargetlist = NIL;
	int resno = 1;
	Index insertTableId = 1;
	Oid insertRelationId = InvalidOid;
	int subqueryTargetLength = 0;
	int targetEntryIndex = 0;

	AssertArg(InsertSelectIntoDistributedTable(originalQuery));

	subquery = subqueryRte->subquery;

	insertRelationId = insertRte->relid;

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
		TargetEntry *newInsertTargetEntry = NULL;
		Var *newInsertVar = NULL;
		TargetEntry *newSubqueryTargetEntry = NULL;
		List *targetVarList = NULL;
		int targetVarCount = 0;
		AttrNumber originalAttrNo = get_attnum(insertRelationId,
											   oldInsertTargetEntry->resname);

		/* see transformInsertRow() for the details */
		if (IsA(oldInsertTargetEntry->expr, ArrayRef) ||
			IsA(oldInsertTargetEntry->expr, FieldStore))
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
		targetVarList = pull_var_clause((Node *) oldInsertTargetEntry->expr,
										PVC_RECURSE_AGGREGATES);

		targetVarCount = list_length(targetVarList);

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

		/*
		 * The newly created select target entry cannot be a junk entry since junk
		 * entries are not in the final target list and we're processing the
		 * final target list entries.
		 */
		Assert(!newSubqueryTargetEntry->resjunk);

		newInsertVar = makeVar(insertTableId, originalAttrNo,
							   exprType((Node *) newSubqueryTargetEntry->expr),
							   exprTypmod((Node *) newSubqueryTargetEntry->expr),
							   exprCollation((Node *) newSubqueryTargetEntry->expr),
							   0);
		newInsertTargetEntry = makeTargetEntry((Expr *) newInsertVar, originalAttrNo,
											   oldInsertTargetEntry->resname,
											   oldInsertTargetEntry->resjunk);

		newInsertTargetlist = lappend(newInsertTargetlist, newInsertTargetEntry);
		resno++;
	}

	/*
	 * if there are any remaining target list entries (i.e., GROUP BY column not on the
	 * target list of subquery), update the remaining resnos.
	 */
	subqueryTargetLength = list_length(subquery->targetList);
	for (; targetEntryIndex < subqueryTargetLength; ++targetEntryIndex)
	{
		TargetEntry *oldSubqueryTle = list_nth(subquery->targetList,
											   targetEntryIndex);
		TargetEntry *newSubqueryTargetEntry = NULL;

		/*
		 * Skip non-junk entries since we've already processed them above and this
		 * loop only is intended for junk entries.
		 */
		if (!oldSubqueryTle->resjunk)
		{
			continue;
		}

		newSubqueryTargetEntry = copyObject(oldSubqueryTle);

		newSubqueryTargetEntry->resno = resno;
		newSubqueryTargetlist = lappend(newSubqueryTargetlist,
										newSubqueryTargetEntry);

		resno++;
	}

	originalQuery->targetList = newInsertTargetlist;
	subquery->targetList = newSubqueryTargetlist;

	return NULL;
}


/*
 * MultiTaskRouterSelectQuerySupported returns NULL if the query may be used
 * as the source for an INSERT ... SELECT or returns a description why not.
 */
static DeferredErrorMessage *
MultiTaskRouterSelectQuerySupported(Query *query)
{
	List *queryList = NIL;
	ListCell *queryCell = NULL;
	StringInfo errorDetail = NULL;
	bool hasUnsupportedDistinctOn = false;

	ExtractQueryWalker((Node *) query, &queryList);
	foreach(queryCell, queryList)
	{
		Query *subquery = (Query *) lfirst(queryCell);

		Assert(subquery->commandType == CMD_SELECT);

		/* pushing down rtes without relations yields (shardCount * expectedRows) */
		if (subquery->rtable == NIL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Subqueries without relations are not allowed in "
								 "distributed INSERT ... SELECT queries",
								 NULL, NULL);
		}

		/* pushing down limit per shard would yield wrong results */
		if (subquery->limitCount != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "LIMIT clauses are not allowed in distributed INSERT "
								 "... SELECT queries",
								 NULL, NULL);
		}

		/* pushing down limit offest per shard would yield wrong results */
		if (subquery->limitOffset != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "OFFSET clauses are not allowed in distributed "
								 "INSERT ... SELECT queries",
								 NULL, NULL);
		}

		/* group clause list must include partition column */
		if (subquery->groupClause)
		{
			List *groupClauseList = subquery->groupClause;
			List *targetEntryList = subquery->targetList;
			List *groupTargetEntryList = GroupTargetEntryList(groupClauseList,
															  targetEntryList);
			bool groupOnPartitionColumn = TargetListOnPartitionColumn(subquery,
																	  groupTargetEntryList);
			if (!groupOnPartitionColumn)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Group by list without distribution column is "
									 "not allowed  in distributed INSERT ... "
									 "SELECT queries",
									 NULL, NULL);
			}
		}

		/*
		 * We support window functions when the window function
		 * is partitioned on distribution column.
		 */
		if (subquery->windowClause && !SafeToPushdownWindowFunction(subquery,
																	&errorDetail))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED, errorDetail->data, NULL,
								 NULL);
		}

		if (subquery->setOperations != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Set operations are not allowed in distributed "
								 "INSERT ... SELECT queries",
								 NULL, NULL);
		}

		/*
		 * We currently do not support grouping sets since it could generate NULL
		 * results even after the restrictions are applied to the query. A solution
		 * would be to add the whole query into a subquery and add the restrictions
		 * on that subquery.
		 */
		if (subquery->groupingSets != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "grouping sets are not allowed in distributed "
								 "INSERT ... SELECT queries",
								 NULL, NULL);
		}

		/*
		 * We don't support DISTINCT ON clauses on non-partition columns.
		 */
		hasUnsupportedDistinctOn = HasUnsupportedDistinctOn(subquery);
		if (hasUnsupportedDistinctOn)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "DISTINCT ON (non-partition column) clauses are not "
								 "allowed in distributed INSERT ... SELECT queries",
								 NULL, NULL);
		}
	}

	return NULL;
}


/*
 * HasUnsupportedDistinctOn returns true if the query has distinct on and
 * distinct targets do not contain partition column.
 */
static bool
HasUnsupportedDistinctOn(Query *query)
{
	ListCell *distinctCell = NULL;

	if (!query->hasDistinctOn)
	{
		return false;
	}

	foreach(distinctCell, query->distinctClause)
	{
		SortGroupClause *distinctClause = lfirst(distinctCell);
		TargetEntry *distinctEntry = get_sortgroupclause_tle(distinctClause,
															 query->targetList);

		if (IsPartitionColumn(distinctEntry->expr, query))
		{
			return false;
		}
	}

	return true;
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
		Var *insertVar = NULL;
		AttrNumber originalAttrNo = InvalidAttrNumber;
		TargetEntry *subqueryTargetEntry = NULL;
		Expr *selectTargetExpr = NULL;
		Oid subqueryPartitionColumnRelationId = InvalidOid;
		Var *subqueryPartitionColumn = NULL;
		List *parentQueryList = NIL;

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

		insertVar = (Var *) linitial(insertTargetEntryColumnList);
		originalAttrNo = targetEntry->resno;

		/* skip processing of target table non-partition columns */
		if (originalAttrNo != insertPartitionColumn->varattno)
		{
			continue;
		}

		/* INSERT query includes the partition column */
		targetTableHasPartitionColumn = true;

		subqueryTargetEntry = list_nth(subquery->targetList,
									   insertVar->varattno - 1);
		selectTargetExpr = subqueryTargetEntry->expr;

		parentQueryList = list_make2(query, subquery);
		FindReferencedTableColumn(selectTargetExpr,
								  parentQueryList, subquery,
								  &subqueryPartitionColumnRelationId,
								  &subqueryPartitionColumn);

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
		if (!IsPartitionColumn(selectTargetExpr, subquery))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed INSERT INTO ... SELECT "
								 "becuase the partition columns in the source table "
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
 * CreatteCoordinatorInsertSelectPlan creates a query plan for a SELECT into a
 * distributed table. The query plan can also be executed on a worker in MX.
 */
static DistributedPlan *
CreateCoordinatorInsertSelectPlan(Query *parse)
{
	Query *insertSelectQuery = copyObject(parse);
	Query *selectQuery = NULL;

	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractInsertRangeTableEntry(insertSelectQuery);
	Oid targetRelationId = insertRte->relid;

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->operation = CMD_INSERT;

	distributedPlan->planningError =
		CoordinatorInsertSelectSupported(insertSelectQuery);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
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

	distributedPlan->insertSelectSubquery = selectQuery;
	distributedPlan->insertTargetList = insertSelectQuery->targetList;
	distributedPlan->targetRelationId = targetRelationId;

	return distributedPlan;
}


/*
 * CoordinatorInsertSelectSupported returns an error if executing an
 * INSERT ... SELECT command by pulling results of the SELECT to the coordinator
 * is unsupported because it uses RETURNING, ON CONFLICT, or an append-distributed
 * table.
 */
static DeferredErrorMessage *
CoordinatorInsertSelectSupported(Query *insertSelectQuery)
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
