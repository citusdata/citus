/*-------------------------------------------------------------------------
 *
 * multi_router_planner.c
 *
 * This file contains functions to plan single shard queries
 * including distributed table modifications.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stddef.h>

#include "access/stratnum.h"
#include "access/xact.h"
#include "distributed/citus_clauses.h"
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "catalog/pg_proc.h"
#include "optimizer/planmain.h"


typedef struct WalkerState
{
	bool containsVar;
	bool varArgument;
	bool badCoalesce;
} WalkerState;


/* planner functions forward declarations */
static MultiPlan * CreateSingleTaskRouterPlan(Query *originalQuery, Query *query,
											  RelationRestrictionContext *
											  restrictionContext);
static MultiPlan * CreateMultiTaskRouterPlan(Query *originalQuery, Query *query,
											 RelationRestrictionContext *
											 restrictionContext);
static Task * CreateMultipleTaskRouterModifyTask(Query *originalQuery, Query *query,
												 ShardInterval *shardInterval,
												 RelationRestrictionContext *
												 restrictionContext,
												 uint32 taskIdIndex);
static bool MasterIrreducibleExpression(Node *expression, bool *varArgument,
										bool *badCoalesce);
static bool MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state);
static char MostPermissiveVolatileFlag(char left, char right);
static bool TargetEntryChangesValue(TargetEntry *targetEntry, Var *column,
									FromExpr *joinTree);
static Task * RouterModifyTask(Query *originalQuery, Query *query);
static ShardInterval * TargetShardIntervalForModify(Query *query);
static List * QueryRestrictList(Query *query);
static bool FastShardPruningPossible(CmdType commandType, char partitionMethod);
static ShardInterval * FastShardPruning(Oid distributedTableId,
										Const *partionColumnValue);
static Oid ExtractFirstDistributedTableId(Query *query);
static Const * ExtractInsertPartitionValue(Query *query, Var *partitionColumn);
static Task * RouterSelectTask(Query *originalQuery, Query *query,
							   RelationRestrictionContext *restrictionContext,
							   List **placementList);
static bool RouterSelectQuery(Query *originalQuery, Query *query,
							  RelationRestrictionContext *restrictionContext,
							  List **placementList, uint64 *anchorShardId);
static List * TargetShardIntervalsForSelect(Query *query,
											RelationRestrictionContext *restrictionContext);
static List * WorkersContainingAllShards(List *prunedShardIntervalsList);
static List * IntersectPlacementList(List *lhsPlacementList, List *rhsPlacementList);
static bool UpdateRelationNames(Node *node,
								RelationRestrictionContext *restrictionContext);
static Job * RouterQueryJob(Query *query, Task *task, List *placementList);
static bool MultiRouterPlannableQuery(Query *query, MultiExecutorType taskExecutorType,
									  RelationRestrictionContext *restrictionContext);
static RelationRestrictionContext * copyRelationRestrictionContext(
	RelationRestrictionContext *oldContext);
static Node * ReplaceHiddenQual(Node *node, void *context);
static void ErrorIfInsertSelectQueryNotSupported(Query *queryTree,
												 RangeTblEntry *insertRte,
												 RangeTblEntry *subqueryRte);
static void ErrorIfMultiTaskRouterSelectQueryUnsupported(Query *query);
static void ErrorIfInsertPartitionColumnDoesNotMatchSelect(Query *query,
														   RangeTblEntry *insertRte,
														   RangeTblEntry *subqueryRte,
														   Oid *
														   selectPartitionColumnTableId);
static void AddHiddenEqualityQual(Query *query, Var *targetPartitionColumnVar);


/*
 * MultiRouterPlanCreate
 */
MultiPlan *
MultiRouterPlanCreate(Query *originalQuery, Query *query,
					  MultiExecutorType taskExecutorType,
					  RelationRestrictionContext *restrictionContext)
{
	MultiPlan *multiPlan = NULL;

	bool routerPlannable = MultiRouterPlannableQuery(query, taskExecutorType,
													 restrictionContext);
	if (!routerPlannable)
	{
		return NULL;
	}

	if (InsertSelectQuery(query))
	{
		multiPlan = CreateMultiTaskRouterPlan(originalQuery, query, restrictionContext);
	}
	else
	{
		multiPlan = CreateSingleTaskRouterPlan(originalQuery, query, restrictionContext);
	}

	return multiPlan;
}


/*
 * CreateSingleTaskRouterPlan creates a physical plan for given query. The created plan is
 * either a modify task that changes a single shard, or a router task that returns
 * query results from a single shard. Supported modify queries (insert/update/delete)
 * are router plannable by default. If query is not router plannable then the function
 * returns NULL.
 */
static MultiPlan *
CreateSingleTaskRouterPlan(Query *originalQuery, Query *query,
						   RelationRestrictionContext *restrictionContext)
{
	CmdType commandType = query->commandType;
	bool modifyTask = false;
	Job *job = NULL;
	Task *task = NULL;
	List *placementList = NIL;
	MultiPlan *multiPlan = NULL;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		modifyTask = true;
	}

	if (modifyTask)
	{
		ErrorIfModifyQueryNotSupported(query);
		task = RouterModifyTask(originalQuery, query);
	}
	else
	{
		Assert(commandType == CMD_SELECT);

		task = RouterSelectTask(originalQuery, query, restrictionContext, &placementList);
	}

	if (task == NULL)
	{
		return NULL;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	job = RouterQueryJob(originalQuery, task, placementList);

	multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->workerJob = job;
	multiPlan->masterQuery = NULL;
	multiPlan->masterTableName = NULL;

	return multiPlan;
}


/*
 * Creates a router plan for INSERT ... SELECT queries which could consists of
 * multiple tasks.
 *
 * The function never returns NULL, it errors out if cannot create the multi plan.
 */
static MultiPlan *
CreateMultiTaskRouterPlan(Query *originalQuery, Query *query,
						  RelationRestrictionContext *restrictionContext)
{
	int shardOffset = 0;
	List *sqlTaskList = NIL;
	uint32 taskIdIndex = 1;     /* 0 is reserved for invalid taskId */
	Job *workerJob = NULL;
	uint64 jobId = INVALID_JOB_ID;
	MultiPlan *multiPlan = NULL;
	RangeTblEntry *insertRte = linitial(query->rtable);
	RangeTblEntry *subqueryRte = lsecond(query->rtable);
	Oid targetRelationId = insertRte->relid;
	DistTableCacheEntry *targetCacheEntry = DistributedTableCacheEntry(targetRelationId);
	int shardCount = targetCacheEntry->shardIntervalArrayLength;

	/*
	 * Error semantics for INSERT ... SELECT queries are different than regular
	 * modify queries. Thus, handle separately.
	 */
	ErrorIfInsertSelectQueryNotSupported(originalQuery, insertRte, subqueryRte);

	/*
	 * Plan select query for each shard in the target table. Do so by
	 * replacing the partitioning qual parameter added in multi_planner()
	 * with actual current shard's boundary values. Then perform the normal
	 * shard pruning.
	 */
	for (shardOffset = 0; shardOffset < shardCount; shardOffset++)
	{
		ShardInterval *targetShardInterval =
			targetCacheEntry->sortedShardIntervalArray[shardOffset];
		Task *modifyTask = NULL;

		modifyTask = CreateMultipleTaskRouterModifyTask(originalQuery, query,
														targetShardInterval,
														restrictionContext,
														taskIdIndex);

		/* add the task if it could be created */
		if (modifyTask != NULL)
		{
			sqlTaskList = lappend(sqlTaskList, modifyTask);
		}

		++taskIdIndex;
	}

	/* there should be a least a single task */
	Assert(sqlTaskList != NIL);

	/* Create the worker job */
	workerJob = CitusMakeNode(Job);
	workerJob->taskList = sqlTaskList;
	workerJob->subqueryPushdown = false;
	workerJob->dependedJobList = NIL;
	workerJob->jobId = jobId;
	workerJob->jobQuery = originalQuery;
	workerJob->requiresMasterEvaluation = false; /* for now we do not support any function evaluation */

	/* and finally the multi plan */
	multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->workerJob = workerJob;
	multiPlan->masterTableName = NULL;
	multiPlan->masterQuery = NULL;

	return multiPlan;
}


/*
 * CreateMultipleTaskRouterModifyTask creates a modify task by
 * replacing the partitioning qual parameter added in multi_planner()
 * with the shardInterval's boundary value. Then perform the normal
 * shard pruning on the subquery. Finally, checks if the target shardInterval
 * has exactly same placements with the select task's available anchor
 * placements.
 *
 * The function errors out if the subquery is not router select query (i.e.,
 * subqueries with non euqi-joins.).
 */
static Task *
CreateMultipleTaskRouterModifyTask(Query *originalQuery, Query *query,
								   ShardInterval *shardInterval,
								   RelationRestrictionContext *restrictionContext,
								   uint32 taskIdIndex)
{
	RangeTblEntry *subqueryRte = lsecond(query->rtable);
	Query *subquery = subqueryRte->subquery;

	Query *copiedQuery = copyObject(originalQuery);
	RangeTblEntry *copiedInsertRte = linitial(copiedQuery->rtable);
	RangeTblEntry *copiedSubqueryRte = lsecond(copiedQuery->rtable);
	Query *copiedSubquery = (Query *) copiedSubqueryRte->subquery;

	uint64 shardId = shardInterval->shardId;
	Oid distributedTableId = shardInterval->relationId;

	RelationRestrictionContext *copiedRestrictionContext =
		copyRelationRestrictionContext(restrictionContext);

	StringInfo queryString = makeStringInfo();
	ListCell *restrictionCell = NULL;
	Task *modifyTask = NULL;
	List *selectPlacementList = NIL;
	uint64 selectAnchorShardId = INVALID_SHARD_ID;
	uint64 jobId = INVALID_JOB_ID;
	List *insertShardPlacementList = NULL;
	List *intersectedPlacementList = NULL;
	bool queryRoutable = false;
	bool upsertQuery = false;

	/* grab shared metadata lock to stop concurrent placement additions */
	LockShardDistributionMetadata(shardId, ShareLock);

	/*
	 * Replace the partitioning qual parameter value in all baserestrictinfos.
	 * Note that this has to be done on a copy, as the walker modifies in place.
	 */
	foreach(restrictionCell, copiedRestrictionContext->relationRestrictionList)
	{
		RelationRestriction *restriction = lfirst(restrictionCell);
		List *originalBaserestrictInfo = restriction->relOptInfo->baserestrictinfo;

		originalBaserestrictInfo =
			(List *) ReplaceHiddenQual((Node *) originalBaserestrictInfo,
									   shardInterval);
	}

	/*
	 * Use router select planner to decide on whether we can push down the query
	 * or not. If we can, we also rely on the side-effects that all RTEs have been
	 * updated to point to the relevant nodes and selectPlacementList is determined.
	 */
	queryRoutable = RouterSelectQuery(copiedSubquery, subquery,
									  copiedRestrictionContext, &selectPlacementList,
									  &selectAnchorShardId);

	if (!queryRoutable)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given "
							   "modification"),
						errdetail("Select query cannot be pushed down to the worker.")));
	}

	/* Ensure that we have INSERTed table's placement exists on the same worker */
	insertShardPlacementList = ShardPlacementList(shardId);
	intersectedPlacementList = IntersectPlacementList(insertShardPlacementList,
													  selectPlacementList);

	if (list_length(insertShardPlacementList) != list_length(intersectedPlacementList))
	{
		ereport(DEBUG2, (errmsg("skipping the task"),
						 errdetail("Insert query hits %d placements, Select query "
								   "hits %d placements and only %d of those placements match.",
								   list_length(insertShardPlacementList),
								   list_length(selectPlacementList),
								   list_length(intersectedPlacementList))));

		return NULL;
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
	ereport(DEBUG4, (errmsg("distributed statement: %s", queryString->data)));

	modifyTask = CreateBasicTask(jobId, taskIdIndex, MODIFY_TASK, queryString->data);
	modifyTask->dependedTaskList = NULL;
	modifyTask->anchorShardId = shardId;
	modifyTask->taskPlacementList = insertShardPlacementList;
	modifyTask->upsertQuery = upsertQuery;

	return modifyTask;
}


/*
 * ErrorIfInsertSelectQueryNotSupported errors out for unsupported
 * INSERT ... SELECT queries.
 */
static void
ErrorIfInsertSelectQueryNotSupported(Query *queryTree, RangeTblEntry *insertRte,
									 RangeTblEntry *subqueryRte)
{
	Query *subquery = NULL;
	Oid selectPartitionColumnTableId = InvalidOid;

	/* we only do this check for INSERT ... SELECT queries */
	AssertArg(InsertSelectQuery(queryTree));

	subquery = subqueryRte->subquery;

	if (contain_mutable_functions((Node *) queryTree))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given "
							   "modification"),
						errdetail(
							"Stable and volatile functions are not allowed in INSERT ... "
							"SELECT queries")));
	}

	if (queryTree->cteList != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given "
							   "modification"),
						errdetail("Common table expressions are not allowed in "
								  "INSERT ... SELECT queries")));
	}

	/* we don't support LIMIT, OFFSET and WINDOW functions */
	ErrorIfMultiTaskRouterSelectQueryUnsupported(subquery);

	/* ensure that INSERT's partition column comes from SELECT's partition column */
	ErrorIfInsertPartitionColumnDoesNotMatchSelect(queryTree, insertRte, subqueryRte,
												   &selectPartitionColumnTableId);

	/* we expect partition column values come from colocated tables */
	if (!TablesColocated(insertRte->relid, selectPartitionColumnTableId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("INSERT target table and the source relation "
							   "of the SELECT partition column value "
							   "must be colocated")));
	}
}


/*
 *  ErrorUnsupportedMultiTaskSelectQuery errors out on queries that we support
 *  for single task router queries, but, cannot allow for multi task router
 *  queries. We do these checks recursively to prevent any wrong results.
 */
static void
ErrorIfMultiTaskRouterSelectQueryUnsupported(Query *query)
{
	List *queryList = NIL;
	ListCell *queryCell = NULL;

	ExtractQueryWalker((Node *) query, &queryList);
	foreach(queryCell, queryList)
	{
		Query *subquery = (Query *) lfirst(queryCell);

		Assert(subquery->commandType == CMD_SELECT);

		if (subquery->limitCount != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given "
								   "modification"),
							errdetail("LIMIT clauses are not allowed in "
									  "INSERT ... SELECT queries")));
		}

		if (subquery->limitOffset != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given "
								   "modification"),
							errdetail("OFFSET clauses are not allowed in "
									  "INSERT ... SELECT queries")));
		}

		if (subquery->windowClause != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given "
								   "modification"),
							errdetail("Window functions are not allowed in "
									  "INSERT ... SELECT queries")));
		}

		/* see comment on AddHiddenPartitionColumnEqualityQual() */
		if (subquery->setOperations != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given "
								   "modification"),
							errdetail("Set operations are not allowed in "
									  "INSERT ... SELECT queries")));
		}
	}
}


/*
 * ErrorIfInsertPartitionColumnDoesNotMatchSelect checks whether the INSERTed table's
 * partition column value matches with the any of the SELECTed table's partition column.
 *
 * On return without error (i.e., if partition columns match), the function also sets
 * selectPartitionColumnTableId.
 */
static void
ErrorIfInsertPartitionColumnDoesNotMatchSelect(Query *query, RangeTblEntry *insertRte,
											   RangeTblEntry *subqueryRte,
											   Oid *selectPartitionColumnTableId)
{
	ListCell *targetEntryCell = NULL;
	uint32 rangeTableId = 1;
	Oid insertRelationId = insertRte->relid;
	Var *insertPartitionColumn = PartitionColumn(insertRelationId, rangeTableId);
	bool partitionColumnsMatch = false;
	Query *subquery = subqueryRte->subquery;

	foreach(targetEntryCell, query->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		if (IsA(targetEntry->expr, Var))
		{
			Var *insertVar = (Var *) targetEntry->expr;
			AttrNumber originalAttrNo = get_attnum(insertRelationId,
												   targetEntry->resname);
			TargetEntry *subqeryTargetEntry = NULL;

			if (originalAttrNo != insertPartitionColumn->varattno)
			{
				continue;
			}

			subqeryTargetEntry = list_nth(subquery->targetList,
										  insertVar->varattno - 1);

			if (!IsA(subqeryTargetEntry->expr, Var))
			{
				partitionColumnsMatch = false;
				break;
			}

			if (!IsPartitionColumnRecursive(subqeryTargetEntry->expr, subquery))
			{
				partitionColumnsMatch = false;
				break;
			}

			partitionColumnsMatch = true;
			*selectPartitionColumnTableId = subqeryTargetEntry->resorigtbl;

			break;
		}
	}

	if (!partitionColumnsMatch)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("SELECT query should return bare partition column on "
							   "the same ordinal position with INSERT query's partition "
							   "column")));
	}
}


/*
 * AddHiddenPartitionColumnEqualityQual() can only be used with
 * INSERT ... SELECT queries.
 *
 * AddHiddenPartitionColumnEqualityQual adds a hidden equality qual
 * to the SELECT query of the given originalQuery. The function currently
 * does NOT add hidden quals if
 *   (i)   CTEs are present on the top level query
 *   (ii)  Set operations are present on the top level query
 *   (iii) Target list does not include a bare partition column.
 *
 * Note that if the input query is not an INSERT .. SELECT the assertion fails.
 */
void
AddHiddenPartitionColumnEqualityQual(Query *originalQuery)
{
	Query *subquery = NULL;
	RangeTblEntry *subqueryEntry = NULL;
	ListCell *targetEntryCell = NULL;
	Var *targetPartitionColumnVar = NULL;
	List *targetList = NULL;

	Assert(InsertSelectQuery(originalQuery));

	/* we currently do not support CTEs */
	if (originalQuery->cteList != NULL)
	{
		return;
	}

	/* TODO: once CTEs are present, this does not work */
	subqueryEntry = (RangeTblEntry *) list_nth(originalQuery->rtable, 1);
	subquery = subqueryEntry->subquery;

	/*
	 * We currently not support the subquery with set operations for three reasons.
	 *
	 *   (i) Adding only a single qual where there are more than one join trees
	 *   leads to an assertion failure on the standard planner (i.e.,
	 *   Assert(parse->jointree->quals == NULL); on plan_set_operations()).
	 *   [THE ABOVE COMMENT IS TO EASE THE REVIEW, REMOVE LATER ON]
	 *
	 *   (ii)  There are potentially multiple jointree quals that we need to add
	 *   the hidden qual, and we haven't implemented that logic yet.
	 *
	 *   (iii) We cannot get the source tables OID via target entries resorigtbl field.
	 *   This makes hard to check the colocation requirement of the source and target
	 *   tables.
	 *
	 * Note that we do not allow set operations on the lower level's of the subquery
	 * as well, which is handled on ErrorIfMultiTaskRouterSelectQueryUnsupported().
	 */
	if (subquery->setOperations != NULL)
	{
		return;
	}

	/* iterate through the target list and find the partition column on the target list */
	targetList = subquery->targetList;
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		if (IsPartitionColumnRecursive(targetEntry->expr, subquery) &&
			IsA(targetEntry->expr, Var))
		{
			targetPartitionColumnVar = (Var *) targetEntry->expr;
			break;
		}
	}

	/*
	 * If we cannot find the bare partition column, no need to add the qual since
	 * we're already going to error out on the multi planner.
	 */
	if (!targetPartitionColumnVar)
	{
		return;
	}

	/* finally add the hidden equality qual of target column to subquery */
	AddHiddenEqualityQual(subquery, targetPartitionColumnVar);
}


/*
 * AddHiddenEqualityQual adds a hidden qual in the following form ($1 = partitionColumn)
 * on the input query and partitionColumn.
 */
static void
AddHiddenEqualityQual(Query *query, Var *partitionColumn)
{
	Param *equalityParameter = makeNode(Param);
	Node *hiddenEqualityQual = NULL;
	Oid partitionColumnCollid = InvalidOid;
	Oid lessThanOperator = InvalidOid;
	Oid equalsOperator = InvalidOid;
	Oid greaterOperator = InvalidOid;
	bool hashable = false;

	AssertArg(query->commandType == CMD_SELECT);

	/* get the necessary equality operator */
	get_sort_group_operators(partitionColumn->vartype, false, true, false,
							 &lessThanOperator, &equalsOperator, &greaterOperator,
							 &hashable);


	partitionColumnCollid = partitionColumn->varcollid;

	equalityParameter->paramkind = PARAM_EXTERN;
	equalityParameter->paramid = HIDDEN_PARAMETER_ID;
	equalityParameter->paramtype = partitionColumn->vartype;
	equalityParameter->paramtypmod = partitionColumn->vartypmod;
	equalityParameter->paramcollid = partitionColumnCollid;
	equalityParameter->location = -1;

	/* create a hidden equality on the on the target partition column */
	hiddenEqualityQual = (Node *)
						 make_opclause(equalsOperator, InvalidOid, false,
									   (Expr *) partitionColumn,
									   (Expr *) equalityParameter,
									   partitionColumnCollid, partitionColumnCollid);

	/* add restriction on partition column */
	if (query->jointree->quals == NULL)
	{
		query->jointree->quals = hiddenEqualityQual;
	}
	else
	{
		query->jointree->quals = make_and_qual(query->jointree->quals,
											   hiddenEqualityQual);
	}
}


/*
 * ErrorIfModifyQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
void
ErrorIfModifyQueryNotSupported(Query *queryTree)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;
	bool specifiesPartitionValue = false;
	ListCell *setTargetCell = NULL;
	List *onConflictSet = NIL;
	Node *arbiterWhere = NULL;
	Node *onConflictWhere = NULL;

	CmdType commandType = queryTree->commandType;
	Assert(commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		   commandType == CMD_DELETE);

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which include subqueries in FROM clauses are rejected below.
	 */
	if (queryTree->hasSubLinks == true)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Subqueries are not supported in distributed"
								  " modifications.")));
	}

	/* reject queries which include CommonTableExpr */
	if (queryTree->cteList != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Common table expressions are not supported in"
								  " distributed modifications.")));
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			queryTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
		else
		{
			/*
			 * Error out for rangeTableEntries that we do not support.
			 * We do not explicitly specify "in FROM clause" in the error detail
			 * for the features that we do not support at all (SUBQUERY, JOIN).
			 * We do not need to check for RTE_CTE because all common table expressions
			 * are rejected above with queryTree->cteList check.
			 */
			char *rangeTableEntryErrorDetail = NULL;
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				rangeTableEntryErrorDetail = "Subqueries are not supported in"
											 " distributed modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_JOIN)
			{
				rangeTableEntryErrorDetail = "Joins are not supported in distributed"
											 " modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_FUNCTION)
			{
				rangeTableEntryErrorDetail = "Functions must not appear in the FROM"
											 " clause of a distributed modifications.";
			}
			else
			{
				rangeTableEntryErrorDetail = "Unrecognized range table entry.";
			}

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given"
								   " modifications"),
							errdetail("%s", rangeTableEntryErrorDetail)));
		}
	}

	/*
	 * Reject queries which involve joins. Note that UPSERTs are exceptional for this case.
	 * Queries like "INSERT INTO table_name ON CONFLICT DO UPDATE (col) SET other_col = ''"
	 * contains two range table entries, and we have to allow them.
	 */
	if (commandType != CMD_INSERT && queryTableCount != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Joins are not supported in distributed "
								  "modifications.")));
	}

	/* reject queries which involve multi-row inserts */
	if (hasValuesScan)
	{
		/*
		 * NB: If you remove this check you must also change the checks further in this
		 * method and ensure that VOLATILE function calls aren't allowed in INSERT
		 * statements. Currently they're allowed but the function call is replaced
		 * with a constant, and if you're inserting multiple rows at once the function
		 * should return a different value for each row.
		 */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Multi-row INSERTs to distributed tables are not "
								  "supported.")));
	}

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		bool hasVarArgument = false; /* A STABLE function is passed a Var argument */
		bool hasBadCoalesce = false; /* CASE/COALESCE passed a mutable function */
		FromExpr *joinTree = queryTree->jointree;
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (commandType == CMD_UPDATE &&
				contain_volatile_functions((Node *) targetEntry->expr))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("functions used in UPDATE queries on distributed "
									   "tables must not be VOLATILE")));
			}

			if (commandType == CMD_UPDATE &&
				TargetEntryChangesValue(targetEntry, partitionColumn,
										queryTree->jointree))
			{
				specifiesPartitionValue = true;
			}

			if (commandType == CMD_INSERT &&
				targetEntry->resno == partitionColumn->varattno &&
				!IsA(targetEntry->expr, Const))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("values given for the partition column must be"
									   " constants or constant expressions")));
			}

			if (commandType == CMD_UPDATE &&
				MasterIrreducibleExpression((Node *) targetEntry->expr,
											&hasVarArgument, &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (joinTree != NULL)
		{
			if (contain_volatile_functions(joinTree->quals))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("functions used in the WHERE clause of "
									   "modification queries on distributed tables "
									   "must not be VOLATILE")));
			}
			else if (MasterIrreducibleExpression(joinTree->quals, &hasVarArgument,
												 &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (hasVarArgument)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("STABLE functions used in UPDATE queries"
								   " cannot be called with column references")));
		}

		if (hasBadCoalesce)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("non-IMMUTABLE functions are not allowed in CASE or"
								   " COALESCE statements")));
		}

		if (contain_mutable_functions((Node *) queryTree->returningList))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("non-IMMUTABLE functions are not allowed in the"
								   " RETURNING clause")));
		}
	}

	if (commandType == CMD_INSERT && queryTree->onConflict != NULL)
	{
		onConflictSet = queryTree->onConflict->onConflictSet;
		arbiterWhere = queryTree->onConflict->arbiterWhere;
		onConflictWhere = queryTree->onConflict->onConflictWhere;
	}

	/*
	 * onConflictSet is expanded via expand_targetlist() on the standard planner.
	 * This ends up adding all the columns to the onConflictSet even if the user
	 * does not explicitly state the columns in the query.
	 *
	 * The following loop simply allows "DO UPDATE SET part_col = table.part_col"
	 * types of elements in the target list, which are added by expand_targetlist().
	 * Any other attempt to update partition column value is forbidden.
	 */
	foreach(setTargetCell, onConflictSet)
	{
		TargetEntry *setTargetEntry = (TargetEntry *) lfirst(setTargetCell);

		if (setTargetEntry->resno == partitionColumn->varattno)
		{
			Expr *setExpr = setTargetEntry->expr;
			if (IsA(setExpr, Var) &&
				((Var *) setExpr)->varattno == partitionColumn->varattno)
			{
				specifiesPartitionValue = false;
			}
			else
			{
				specifiesPartitionValue = true;
			}
		}
		else
		{
			/*
			 * Similarly, allow  "DO UPDATE SET col_1 = table.col_1" types of
			 * target list elements. Note that, the following check allows
			 * "DO UPDATE SET col_1 = table.col_2", which is not harmful.
			 */
			if (IsA(setTargetEntry->expr, Var))
			{
				continue;
			}
			else if (contain_mutable_functions((Node *) setTargetEntry->expr))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("functions used in the DO UPDATE SET clause of "
									   "INSERTs on distributed tables must be marked "
									   "IMMUTABLE")));
			}
		}
	}

	/* error if either arbiter or on conflict WHERE contains a mutable function */
	if (contain_mutable_functions((Node *) arbiterWhere) ||
		contain_mutable_functions((Node *) onConflictWhere))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("functions used in the WHERE clause of the ON CONFLICT "
							   "clause of INSERTs on distributed tables must be marked "
							   "IMMUTABLE")));
	}

	if (specifiesPartitionValue)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("modifying the partition value of rows is not allowed")));
	}
}


/*
 * If the expression contains STABLE functions which accept any parameters derived from a
 * Var returns true and sets varArgument.
 *
 * If the expression contains a CASE or COALESCE which invoke non-IMMUTABLE functions
 * returns true and sets badCoalesce.
 *
 * Assumes the expression contains no VOLATILE functions.
 *
 * Var's are allowed, but only if they are passed solely to IMMUTABLE functions
 *
 * We special-case CASE/COALESCE because those are evaluated lazily. We could evaluate
 * CASE/COALESCE expressions which don't reference Vars, or partially evaluate some
 * which do, but for now we just error out. That makes both the code and user-education
 * easier.
 */
static bool
MasterIrreducibleExpression(Node *expression, bool *varArgument, bool *badCoalesce)
{
	bool result;
	WalkerState data;
	data.containsVar = data.varArgument = data.badCoalesce = false;

	result = MasterIrreducibleExpressionWalker(expression, &data);

	*varArgument |= data.varArgument;
	*badCoalesce |= data.badCoalesce;
	return result;
}


static bool
MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state)
{
	char volatileFlag = 0;
	WalkerState childState = { false, false, false };
	bool containsDisallowedFunction = false;

	if (expression == NULL)
	{
		return false;
	}

	if (IsA(expression, CoalesceExpr))
	{
		CoalesceExpr *expr = (CoalesceExpr *) expression;

		if (contain_mutable_functions((Node *) (expr->args)))
		{
			state->badCoalesce = true;
			return true;
		}
		else
		{
			/*
			 * There's no need to recurse. Since there are no STABLE functions
			 * varArgument will never be set.
			 */
			return false;
		}
	}

	if (IsA(expression, CaseExpr))
	{
		if (contain_mutable_functions(expression))
		{
			state->badCoalesce = true;
			return true;
		}

		return false;
	}

	if (IsA(expression, Var))
	{
		state->containsVar = true;
		return false;
	}

	/*
	 * In order for statement replication to give us consistent results it's important
	 * that we either disallow or evaluate on the master anything which has a volatility
	 * category above IMMUTABLE. Newer versions of postgres might add node types which
	 * should be checked in this function.
	 *
	 * Look through contain_mutable_functions_walker or future PG's equivalent for new
	 * node types before bumping this version number to fix compilation.
	 *
	 * Once you've added them to this check, make sure you also evaluate them in the
	 * executor!
	 */
	StaticAssertStmt(PG_VERSION_NUM < 90700, "When porting to a newer PG this section"
											 " needs to be reviewed.");
	if (IsA(expression, Aggref))
	{
		Aggref *expr = (Aggref *) expression;

		volatileFlag = func_volatile(expr->aggfnoid);
	}
	else if (IsA(expression, WindowFunc))
	{
		WindowFunc *expr = (WindowFunc *) expression;

		volatileFlag = func_volatile(expr->winfnoid);
	}
	else if (IsA(expression, OpExpr))
	{
		OpExpr *expr = (OpExpr *) expression;

		set_opfuncid(expr);
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, FuncExpr))
	{
		FuncExpr *expr = (FuncExpr *) expression;

		volatileFlag = func_volatile(expr->funcid);
	}
	else if (IsA(expression, DistinctExpr))
	{
		/*
		 * to exercise this, you need to create a custom type for which the '=' operator
		 * is STABLE/VOLATILE
		 */
		DistinctExpr *expr = (DistinctExpr *) expression;

		set_opfuncid((OpExpr *) expr);  /* rely on struct equivalence */
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, NullIfExpr))
	{
		/*
		 * same as above, exercising this requires a STABLE/VOLATILE '=' operator
		 */
		NullIfExpr *expr = (NullIfExpr *) expression;

		set_opfuncid((OpExpr *) expr);  /* rely on struct equivalence */
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, ScalarArrayOpExpr))
	{
		/*
		 * to exercise this you need to CREATE OPERATOR with a binary predicate
		 * and use it within an ANY/ALL clause.
		 */
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) expression;

		set_sa_opfuncid(expr);
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, CoerceViaIO))
	{
		/*
		 * to exercise this you need to use a type with a STABLE/VOLATILE intype or
		 * outtype.
		 */
		CoerceViaIO *expr = (CoerceViaIO *) expression;
		Oid iofunc;
		Oid typioparam;
		bool typisvarlena;

		/* check the result type's input function */
		getTypeInputInfo(expr->resulttype,
						 &iofunc, &typioparam);
		volatileFlag = MostPermissiveVolatileFlag(volatileFlag, func_volatile(iofunc));

		/* check the input type's output function */
		getTypeOutputInfo(exprType((Node *) expr->arg),
						  &iofunc, &typisvarlena);
		volatileFlag = MostPermissiveVolatileFlag(volatileFlag, func_volatile(iofunc));
	}
	else if (IsA(expression, ArrayCoerceExpr))
	{
		ArrayCoerceExpr *expr = (ArrayCoerceExpr *) expression;

		if (OidIsValid(expr->elemfuncid))
		{
			volatileFlag = func_volatile(expr->elemfuncid);
		}
	}
	else if (IsA(expression, RowCompareExpr))
	{
		RowCompareExpr *rcexpr = (RowCompareExpr *) expression;
		ListCell *opid;

		foreach(opid, rcexpr->opnos)
		{
			volatileFlag = MostPermissiveVolatileFlag(volatileFlag,
													  op_volatile(lfirst_oid(opid)));
		}
	}
	else if (IsA(expression, Query))
	{
		/* subqueries aren't allowed and fail before control reaches this point */
		Assert(false);
	}

	if (volatileFlag == PROVOLATILE_VOLATILE)
	{
		/* the caller should have already checked for this */
		Assert(false);
	}
	else if (volatileFlag == PROVOLATILE_STABLE)
	{
		containsDisallowedFunction =
			expression_tree_walker(expression,
								   MasterIrreducibleExpressionWalker,
								   &childState);

		if (childState.containsVar)
		{
			state->varArgument = true;
		}

		state->badCoalesce |= childState.badCoalesce;
		state->varArgument |= childState.varArgument;

		return (containsDisallowedFunction || childState.containsVar);
	}

	/* keep traversing */
	return expression_tree_walker(expression,
								  MasterIrreducibleExpressionWalker,
								  state);
}


/*
 * Return the most-pessimistic volatility flag of the two params.
 *
 * for example: given two flags, if one is stable and one is volatile, an expression
 * involving both is volatile.
 */
char
MostPermissiveVolatileFlag(char left, char right)
{
	if (left == PROVOLATILE_VOLATILE || right == PROVOLATILE_VOLATILE)
	{
		return PROVOLATILE_VOLATILE;
	}
	else if (left == PROVOLATILE_STABLE || right == PROVOLATILE_STABLE)
	{
		return PROVOLATILE_STABLE;
	}
	else
	{
		return PROVOLATILE_IMMUTABLE;
	}
}


/*
 * TargetEntryChangesValue determines whether the given target entry may
 * change the value in a given column, given a join tree. The result is
 * true unless the expression refers directly to the column, or the
 * expression is a value that is implied by the qualifiers of the join
 * tree, or the target entry sets a different column.
 */
static bool
TargetEntryChangesValue(TargetEntry *targetEntry, Var *column, FromExpr *joinTree)
{
	bool isColumnValueChanged = true;
	Expr *setExpr = targetEntry->expr;

	if (targetEntry->resno != column->varattno)
	{
		/* target entry of the form SET some_other_col = <x> */
		isColumnValueChanged = false;
	}
	else if (IsA(setExpr, Var))
	{
		Var *newValue = (Var *) setExpr;
		if (newValue->varattno == column->varattno)
		{
			/* target entry of the form SET col = table.col */
			isColumnValueChanged = false;
		}
	}
	else if (IsA(setExpr, Const))
	{
		Const *newValue = (Const *) setExpr;
		List *restrictClauseList = WhereClauseList(joinTree);
		OpExpr *equalityExpr = MakeOpExpression(column, BTEqualStrategyNumber);
		Const *rightConst = (Const *) get_rightop((Expr *) equalityExpr);

		rightConst->constvalue = newValue->constvalue;
		rightConst->constisnull = newValue->constisnull;
		rightConst->constbyval = newValue->constbyval;

		if (predicate_implied_by(list_make1(equalityExpr), restrictClauseList))
		{
			/* target entry of the form SET col = <x> WHERE col = <x> AND ... */
			isColumnValueChanged = false;
		}
	}

	return isColumnValueChanged;
}


/*
 * RouterModifyTask builds a Task to represent a modification performed by
 * the provided query against the provided shard interval. This task contains
 * shard-extended deparsed SQL to be run during execution.
 */
static Task *
RouterModifyTask(Query *originalQuery, Query *query)
{
	ShardInterval *shardInterval = TargetShardIntervalForModify(query);
	uint64 shardId = shardInterval->shardId;
	StringInfo queryString = makeStringInfo();
	Task *modifyTask = NULL;
	bool upsertQuery = false;

	/* grab shared metadata lock to stop concurrent placement additions */
	LockShardDistributionMetadata(shardId, ShareLock);

	if (originalQuery->onConflict != NULL)
	{
		RangeTblEntry *rangeTableEntry = NULL;

		/* set the flag */
		upsertQuery = true;

		/* setting an alias simplifies deparsing of UPSERTs */
		rangeTableEntry = linitial(originalQuery->rtable);
		if (rangeTableEntry->alias == NULL)
		{
			Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
			rangeTableEntry->alias = alias;
		}
	}

	deparse_shard_query(originalQuery, shardInterval->relationId, shardId, queryString);
	ereport(DEBUG4, (errmsg("distributed statement: %s", queryString->data)));

	modifyTask = CitusMakeNode(Task);
	modifyTask->jobId = INVALID_JOB_ID;
	modifyTask->taskId = INVALID_TASK_ID;
	modifyTask->taskType = MODIFY_TASK;
	modifyTask->queryString = queryString->data;
	modifyTask->anchorShardId = shardId;
	modifyTask->dependedTaskList = NIL;
	modifyTask->upsertQuery = upsertQuery;

	return modifyTask;
}


/*
 * TargetShardIntervalForModify determines the single shard targeted by a provided
 * modify command. If no matching shards exist, or if the modification targets more
 * than one shard, this function raises an error depending on the command type.
 */
static ShardInterval *
TargetShardIntervalForModify(Query *query)
{
	List *prunedShardList = NIL;
	int prunedShardCount = 0;
	int shardCount = 0;
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;
	bool fastShardPruningPossible = false;

	Assert(query->commandType != CMD_SELECT);

	/* error out if no shards exist for the table */
	shardCount = cacheEntry->shardIntervalArrayLength;
	if (shardCount == 0)
	{
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}

	fastShardPruningPossible = FastShardPruningPossible(query->commandType,
														partitionMethod);
	if (fastShardPruningPossible)
	{
		uint32 rangeTableId = 1;
		Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
		Const *partitionValue = ExtractInsertPartitionValue(query, partitionColumn);
		ShardInterval *shardInterval = FastShardPruning(distributedTableId,
														partitionValue);

		if (shardInterval != NULL)
		{
			prunedShardList = lappend(prunedShardList, shardInterval);
		}
	}
	else
	{
		List *restrictClauseList = QueryRestrictList(query);
		Index tableId = 1;
		List *shardIntervalList = LoadShardIntervalList(distributedTableId);

		prunedShardList = PruneShardList(distributedTableId, tableId, restrictClauseList,
										 shardIntervalList);
	}

	prunedShardCount = list_length(prunedShardList);
	if (prunedShardCount != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("distributed modifications must target exactly one "
							   "shard")));
	}

	return (ShardInterval *) linitial(prunedShardList);
}


/*
 * UseFastShardPruning returns true if the commandType is INSERT and partition method
 * is hash or range.
 */
static bool
FastShardPruningPossible(CmdType commandType, char partitionMethod)
{
	/* we currently only support INSERTs */
	if (commandType != CMD_INSERT)
	{
		return false;
	}

	/* fast shard pruning is only supported for hash and range partitioned tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod == DISTRIBUTE_BY_RANGE)
	{
		return true;
	}

	return false;
}


/*
 * FastShardPruning is a higher level API for FindShardInterval function. Given the
 * relationId of the distributed table and partitionValue, FastShardPruning function finds
 * the corresponding shard interval that the partitionValue should be in. FastShardPruning
 * returns NULL if no ShardIntervals exist for the given partitionValue.
 */
static ShardInterval *
FastShardPruning(Oid distributedTableId, Const *partitionValue)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	int shardCount = cacheEntry->shardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray = cacheEntry->sortedShardIntervalArray;
	bool useBinarySearch = false;
	char partitionMethod = cacheEntry->partitionMethod;
	FmgrInfo *shardIntervalCompareFunction = cacheEntry->shardIntervalCompareFunction;
	bool hasUniformHashDistribution = cacheEntry->hasUniformHashDistribution;
	FmgrInfo *hashFunction = NULL;
	ShardInterval *shardInterval = NULL;

	/* determine whether to use binary search */
	if (partitionMethod != DISTRIBUTE_BY_HASH || !hasUniformHashDistribution)
	{
		useBinarySearch = true;
	}

	/* we only need hash functions for hash distributed tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		hashFunction = cacheEntry->hashFunction;
	}

	/*
	 * Call FindShardInterval to find the corresponding shard interval for the
	 * given partition value.
	 */
	shardInterval = FindShardInterval(partitionValue->constvalue,
									  sortedShardIntervalArray, shardCount,
									  partitionMethod,
									  shardIntervalCompareFunction, hashFunction,
									  useBinarySearch);

	return shardInterval;
}


/*
 * QueryRestrictList returns the restriction clauses for the query. For a SELECT
 * statement these are the where-clause expressions. For INSERT statements we
 * build an equality clause based on the partition-column and its supplied
 * insert value.
 */
static List *
QueryRestrictList(Query *query)
{
	List *queryRestrictList = NIL;
	CmdType commandType = query->commandType;

	if (commandType == CMD_INSERT)
	{
		/* build equality expression based on partition column value for row */
		Oid distributedTableId = ExtractFirstDistributedTableId(query);
		uint32 rangeTableId = 1;
		Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
		Const *partitionValue = ExtractInsertPartitionValue(query, partitionColumn);

		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);

		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;
		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValue->constvalue;
		rightConst->constisnull = partitionValue->constisnull;
		rightConst->constbyval = partitionValue->constbyval;

		queryRestrictList = list_make1(equalityExpr);
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE ||
			 commandType == CMD_SELECT)
	{
		queryRestrictList = WhereClauseList(query->jointree);
	}

	return queryRestrictList;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
static Oid
ExtractFirstDistributedTableId(Query *query)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (IsDistributedTable(rangeTableEntry->relid))
		{
			distributedTableId = rangeTableEntry->relid;
			break;
		}
	}

	return distributedTableId;
}


/*
 * ExtractPartitionValue extracts the partition column value from a the target
 * of an INSERT command. If a partition value is missing altogether or is
 * NULL, this function throws an error.
 */
static Const *
ExtractInsertPartitionValue(Query *query, Var *partitionColumn)
{
	Const *partitionValue = NULL;
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry != NULL)
	{
		Assert(IsA(targetEntry->expr, Const));

		partitionValue = (Const *) targetEntry->expr;
	}

	if (partitionValue == NULL || partitionValue->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot plan INSERT using row with NULL value "
							   "in partition column")));
	}

	return partitionValue;
}


/* RouterSelectTask builds a Task to represent a single shard select query */
static Task *
RouterSelectTask(Query *originalQuery, Query *query,
				 RelationRestrictionContext *restrictionContext,
				 List **placementList)
{
	Task *task = NULL;
	bool queryRoutable = false;
	StringInfo queryString = makeStringInfo();
	bool upsertQuery = false;
	uint64 shardId = INVALID_SHARD_ID;

	queryRoutable = RouterSelectQuery(originalQuery, query, restrictionContext,
									  placementList, &shardId);


	if (!queryRoutable)
	{
		return NULL;
	}

	pg_get_query_def(originalQuery, queryString);

	task = CitusMakeNode(Task);
	task->jobId = INVALID_JOB_ID;
	task->taskId = INVALID_TASK_ID;
	task->taskType = ROUTER_TASK;
	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->dependedTaskList = NIL;
	task->upsertQuery = upsertQuery;

	return task;
}


/*
 * RouterSelectQuery returns true if the input query can be pushed down to the
 * worker node as it is. Otherwise, the function returns false.
 *
 * On return true, all RTEs have been updated to point to the relevant nodes in
 * the originalQuery. Also, placementList is filled with the list of worker nodes
 * that has all the required shard placements for the query execution. Finally,
 * anchorShardId is set to the first pruned shardId of the given query.
 */
static bool
RouterSelectQuery(Query *originalQuery, Query *query,
				  RelationRestrictionContext *restrictionContext,
				  List **placementList, uint64 *anchorShardId)
{
	List *prunedRelationShardList = TargetShardIntervalsForSelect(query,
																  restrictionContext);
	uint64 shardId = INVALID_SHARD_ID;
	CmdType commandType PG_USED_FOR_ASSERTS_ONLY = query->commandType;
	ListCell *prunedRelationShardListCell = NULL;
	List *workerList = NIL;
	bool shardsPresent = false;
	bool queryRoutable = false;

	*placementList = NIL;

	if (prunedRelationShardList == NULL)
	{
		return queryRoutable;
	}

	Assert(commandType == CMD_SELECT);

	foreach(prunedRelationShardListCell, prunedRelationShardList)
	{
		List *prunedShardList = (List *) lfirst(prunedRelationShardListCell);
		ShardInterval *shardInterval = NULL;

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardList == NIL)
		{
			continue;
		}

		shardsPresent = true;

		/* all relations are now pruned down to 0 or 1 shards */
		Assert(list_length(prunedShardList) <= 1);

		/* anchor shard id */
		if (shardId == INVALID_SHARD_ID)
		{
			shardInterval = (ShardInterval *) linitial(prunedShardList);
			shardId = shardInterval->shardId;
		}
	}

	/*
	 * Determine the worker that has all shard placements if a shard placement found.
	 * If no shard placement exists, we will still run the query but the result will
	 * be empty. We create a dummy shard placement for the first active worker.
	 */
	if (shardsPresent)
	{
		workerList = WorkersContainingAllShards(prunedRelationShardList);
	}
	else
	{
		List *workerNodeList = WorkerNodeList();
		if (workerNodeList != NIL)
		{
			WorkerNode *workerNode = (WorkerNode *) linitial(workerNodeList);
			ShardPlacement *dummyPlacement =
				(ShardPlacement *) CitusMakeNode(ShardPlacement);
			dummyPlacement->nodeName = workerNode->workerName;
			dummyPlacement->nodePort = workerNode->workerPort;

			workerList = lappend(workerList, dummyPlacement);
		}
	}

	if (workerList == NIL)
	{
		ereport(DEBUG2, (errmsg("Found no worker with all shard placements")));

		return queryRoutable;
	}

	UpdateRelationNames((Node *) originalQuery, restrictionContext);

	*placementList = workerList;
	*anchorShardId = shardId;

	/* now that the query is qualified to be routable */
	queryRoutable = true;

	return queryRoutable;
}


/*
 * TargetShardIntervalsForSelect performs shard pruning for all referenced relations
 * in the query and returns list of shards per relation. Shard pruning is done based
 * on provided restriction context per relation. The function bails out and returns NULL
 * if any of the relations pruned down to more than one active shard. It also records
 * pruned shard intervals in relation restriction context to be used later on. Some
 * queries may have contradiction clauses like 'and false' or 'and 1=0', such queries
 * are treated as if all of the shards of joining relations are pruned out.
 */
static List *
TargetShardIntervalsForSelect(Query *query,
							  RelationRestrictionContext *restrictionContext)
{
	List *prunedRelationShardList = NIL;
	ListCell *restrictionCell = NULL;

	Assert(query->commandType == CMD_SELECT);
	Assert(restrictionContext != NULL);

	foreach(restrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;
		Index tableId = relationRestriction->index;
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
		int shardCount = cacheEntry->shardIntervalArrayLength;
		List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
		List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
		List *prunedShardList = NIL;
		int shardIndex = 0;
		List *joinInfoList = relationRestriction->relOptInfo->joininfo;
		List *pseudoRestrictionList = extract_actual_clauses(joinInfoList, true);
		bool whereFalseQuery = false;

		relationRestriction->prunedShardIntervalList = NIL;

		/*
		 * Queries may have contradiction clauses like 'false', or '1=0' in
		 * their filters. Such queries would have pseudo constant 'false'
		 * inside relOptInfo->joininfo list. We treat such cases as if all
		 * shards of the table are pruned out.
		 */
		whereFalseQuery = ContainsFalseClause(pseudoRestrictionList);
		if (!whereFalseQuery && shardCount > 0)
		{
			List *shardIntervalList = NIL;

			for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
			{
				ShardInterval *shardInterval =
					cacheEntry->sortedShardIntervalArray[shardIndex];
				shardIntervalList = lappend(shardIntervalList, shardInterval);
			}

			prunedShardList = PruneShardList(relationId, tableId,
											 restrictClauseList,
											 shardIntervalList);

			/*
			 * Quick bail out. The query can not be router plannable if one
			 * relation has more than one shard left after pruning. Having no
			 * shard left is okay at this point. It will be handled at a later
			 * stage.
			 */
			if (list_length(prunedShardList) > 1)
			{
				return NULL;
			}
		}

		relationRestriction->prunedShardIntervalList = prunedShardList;
		prunedRelationShardList = lappend(prunedRelationShardList, prunedShardList);
	}

	return prunedRelationShardList;
}


/*
 * WorkersContainingAllShards returns list of shard placements that contain all
 * shard intervals provided to the function. It returns NIL if no placement exists.
 * The caller should check if there are any shard intervals exist for placement
 * check prior to calling this function.
 */
static List *
WorkersContainingAllShards(List *prunedShardIntervalsList)
{
	ListCell *prunedShardIntervalCell = NULL;
	bool firstShard = true;
	List *currentPlacementList = NIL;

	foreach(prunedShardIntervalCell, prunedShardIntervalsList)
	{
		List *shardIntervalList = (List *) lfirst(prunedShardIntervalCell);
		ShardInterval *shardInterval = NULL;
		uint64 shardId = INVALID_SHARD_ID;
		List *newPlacementList = NIL;

		if (shardIntervalList == NIL)
		{
			continue;
		}

		Assert(list_length(shardIntervalList) == 1);

		shardInterval = (ShardInterval *) linitial(shardIntervalList);
		shardId = shardInterval->shardId;

		/* retrieve all active shard placements for this shard */
		newPlacementList = FinalizedShardPlacementList(shardId);

		if (firstShard)
		{
			firstShard = false;
			currentPlacementList = newPlacementList;
		}
		else
		{
			/* keep placements that still exists for this shard */
			currentPlacementList = IntersectPlacementList(currentPlacementList,
														  newPlacementList);
		}

		/*
		 * Bail out if placement list becomes empty. This means there is no worker
		 * containing all shards referecend by the query, hence we can not forward
		 * this query directly to any worker.
		 */
		if (currentPlacementList == NIL)
		{
			break;
		}
	}

	return currentPlacementList;
}


/*
 * IntersectPlacementList performs placement pruning based on matching on
 * nodeName:nodePort fields of shard placement data. We start pruning from all
 * placements of the first relation's shard. Then for each relation's shard, we
 * compute intersection of the new shards placement with existing placement list.
 * This operation could have been done using other methods, but since we do not
 * expect very high replication factor, iterating over a list and making string
 * comparisons should be sufficient.
 */
static List *
IntersectPlacementList(List *lhsPlacementList, List *rhsPlacementList)
{
	ListCell *lhsPlacementCell = NULL;
	List *placementList = NIL;

	/* Keep existing placement in the list if it is also present in new placement list */
	foreach(lhsPlacementCell, lhsPlacementList)
	{
		ShardPlacement *lhsPlacement = (ShardPlacement *) lfirst(lhsPlacementCell);
		ListCell *rhsPlacementCell = NULL;
		foreach(rhsPlacementCell, rhsPlacementList)
		{
			ShardPlacement *rhsPlacement = (ShardPlacement *) lfirst(rhsPlacementCell);
			if (rhsPlacement->nodePort == lhsPlacement->nodePort &&
				strncmp(rhsPlacement->nodeName, lhsPlacement->nodeName,
						WORKER_LENGTH) == 0)
			{
				placementList = lappend(placementList, rhsPlacement);
			}
		}
	}

	return placementList;
}


/*
 * ConvertRteToSubqueryWithEmptyResult converts given relation RTE into
 * subquery RTE that returns no results.
 */
static void
ConvertRteToSubqueryWithEmptyResult(RangeTblEntry *rte)
{
	Relation relation = heap_open(rte->relid, NoLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	int columnCount = tupleDescriptor->natts;
	int columnIndex = 0;
	Query *subquery = NULL;
	List *targetList = NIL;
	FromExpr *joinTree = NULL;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FormData_pg_attribute *attributeForm = tupleDescriptor->attrs[columnIndex];
		TargetEntry *targetEntry = NULL;
		StringInfo resname = NULL;
		Const *constValue = NULL;

		if (attributeForm->attisdropped)
		{
			continue;
		}

		resname = makeStringInfo();
		constValue = makeNullConst(attributeForm->atttypid, attributeForm->atttypmod,
								   attributeForm->attcollation);

		appendStringInfo(resname, "%s", attributeForm->attname.data);

		targetEntry = makeNode(TargetEntry);
		targetEntry->expr = (Expr *) constValue;
		targetEntry->resno = columnIndex;
		targetEntry->resname = resname->data;

		targetList = lappend(targetList, targetEntry);
	}

	heap_close(relation, NoLock);

	joinTree = makeNode(FromExpr);
	joinTree->quals = makeBoolConst(false, false);

	subquery = makeNode(Query);
	subquery->commandType = CMD_SELECT;
	subquery->querySource = QSRC_ORIGINAL;
	subquery->canSetTag = true;
	subquery->targetList = targetList;
	subquery->jointree = joinTree;

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->alias = copyObject(rte->eref);
}


/*
 * UpdateRelationNames walks over the query tree and appends shard ids to
 * relations. It uses unique identity value to establish connection between a
 * shard and the range table entry. If the range table id is not given a
 * identity, than the relation is not referenced from the query, no connection
 * could be found between a shard and this relation. Therefore relation is replaced
 * by set of NULL values so that the query would work at worker without any problems.
 *
 */
static bool
UpdateRelationNames(Node *node, RelationRestrictionContext *restrictionContext)
{
	RangeTblEntry *newRte = NULL;
	uint64 shardId = INVALID_SHARD_ID;
	Oid relationId = InvalidOid;
	Oid schemaId = InvalidOid;
	char *relationName = NULL;
	char *schemaName = NULL;
	ListCell *relationRestrictionCell = NULL;
	RelationRestriction *relationRestriction = NULL;
	List *shardIntervalList = NIL;
	ShardInterval *shardInterval = NULL;
	bool replaceRteWithNullValues = false;

	if (node == NULL)
	{
		return false;
	}

	/* want to look at all RTEs, even in subqueries, CTEs and such */
	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, UpdateRelationNames, restrictionContext,
								 QTW_EXAMINE_RTES);
	}

	if (!IsA(node, RangeTblEntry))
	{
		return expression_tree_walker(node, UpdateRelationNames, restrictionContext);
	}


	newRte = (RangeTblEntry *) node;

	if (newRte->rtekind != RTE_RELATION)
	{
		return false;
	}

	/*
	 * Search for the restrictions associated with the RTE. There better be
	 * some, otherwise this query wouldn't be elegible as a router query.
	 *
	 * FIXME: We should probably use a hashtable here, to do efficient
	 * lookup.
	 */
	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);

		if (GetRTEIdentity(relationRestriction->rte) == GetRTEIdentity(newRte))
		{
			break;
		}

		relationRestriction = NULL;
	}

	replaceRteWithNullValues = (relationRestriction == NULL) ||
							   relationRestriction->prunedShardIntervalList == NIL;

	if (replaceRteWithNullValues)
	{
		ConvertRteToSubqueryWithEmptyResult(newRte);
		return false;
	}

	Assert(relationRestriction != NULL);

	shardIntervalList = relationRestriction->prunedShardIntervalList;

	Assert(list_length(shardIntervalList) == 1);
	shardInterval = (ShardInterval *) linitial(shardIntervalList);

	shardId = shardInterval->shardId;
	relationId = shardInterval->relationId;
	relationName = get_rel_name(relationId);
	AppendShardIdToName(&relationName, shardId);

	schemaId = get_rel_namespace(relationId);
	schemaName = get_namespace_name(schemaId);

	ModifyRangeTblExtraData(newRte, CITUS_RTE_SHARD, schemaName, relationName, NIL);

	return false;
}


/*
 * RouterQueryJob creates a Job for the specified query to execute the
 * provided single shard select task.
 */
static Job *
RouterQueryJob(Query *query, Task *task, List *placementList)
{
	Job *job = NULL;
	List *taskList = NIL;
	TaskType taskType = task->taskType;
	bool requiresMasterEvaluation = false;

	/*
	 * We send modify task to the first replica, otherwise we choose the target shard
	 * according to task assignment policy. Placement list for select queries are
	 * provided as function parameter.
	 */
	if (taskType == MODIFY_TASK)
	{
		taskList = FirstReplicaAssignTaskList(list_make1(task));
		requiresMasterEvaluation = RequiresMasterEvaluation(query);
	}
	else
	{
		Assert(placementList != NIL);

		task->taskPlacementList = placementList;
		taskList = list_make1(task);
	}

	job = CitusMakeNode(Job);
	job->dependedJobList = NIL;
	job->jobId = INVALID_JOB_ID;
	job->subqueryPushdown = false;
	job->jobQuery = query;
	job->taskList = taskList;
	job->requiresMasterEvaluation = requiresMasterEvaluation;

	return job;
}


/*
 * MultiRouterPlannableQuery returns true if given query can be router plannable.
 * The query is router plannable if it is a select query issued on a hash
 * partitioned distributed table, and it has a exact match comparison on the
 * partition column. This feature is enabled if task executor is set to real-time
 */
bool
MultiRouterPlannableQuery(Query *query, MultiExecutorType taskExecutorType,
						  RelationRestrictionContext *restrictionContext)
{
	CmdType commandType = query->commandType;
	ListCell *relationRestrictionContextCell = NULL;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return true;
	}

	/* FIXME: I tend to think it's time to remove this */
	if (taskExecutorType != MULTI_EXECUTOR_REAL_TIME)
	{
		return false;
	}

	Assert(commandType == CMD_SELECT);

	if (query->hasForUpdate)
	{
		return false;
	}

	foreach(relationRestrictionContextCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionContextCell);
		RangeTblEntry *rte = relationRestriction->rte;
		if (rte->rtekind == RTE_RELATION)
		{
			/* only hash partitioned tables are supported */
			Oid distributedTableId = rte->relid;
			char partitionMethod = PartitionMethod(distributedTableId);

			if (partitionMethod != DISTRIBUTE_BY_HASH)
			{
				return false;
			}
		}
	}

	return true;
}


/*
 * ReorderInsertSelectTargetLists reorders the target lists of INSERT/SELECT
 * query which is required for deparsing purposes. The reordered query is returned.
 *
 * The necessity for this function comes from the fact that ruleutils.c is not supposed to be
 * used on "rewritten" queries (i.e. ones that have been passed through QueryRewrite()).
 * Query rewriting is the process in which views and such are expanded,
 * and, INSERT/UPDATE targetlists are reordered to match the physical order,
 * defaults etc. For the details of reordeing, see transformInsertRow().
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
	int updatedSubqueryEntryCount = 0;
	Oid insertRelationId = InvalidOid;
	int subqueryTargetLength = 0;

	AssertArg(InsertSelectQuery(originalQuery));

	subquery = subqueryRte->subquery;

	insertRelationId = insertRte->relid;

	/*
	 * We implement the following algorithm for the reoderding:
	 *  - Iterate over the INSERT target list entries
	 *    - If the target entry includes a Var, find the corresponding
	 *      SELECT target entry on the original query and update resno
	 *    - If the target entry does not include a Var (i.e., defaults),
	 *      create new target entry and add that to SELECT target list
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
							errmsg("cannot plan distributed INSERT INTO .. SELECT query"),
							errhint("Do not use array references and field stores "
									"on the INSERT target list.")));
		}

		/*
		 * It is safe to pull Var clause and ignore the coercions since that
		 * are already going to be added on the workers implicitly.
		 */
		targetVarList = pull_var_clause((Node *) oldInsertTargetEntry->expr,
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
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

			updatedSubqueryEntryCount++;
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
	if (subqueryTargetLength != updatedSubqueryEntryCount)
	{
		int targetEntryIndex = updatedSubqueryEntryCount;

		for (; targetEntryIndex < subqueryTargetLength; ++targetEntryIndex)
		{
			TargetEntry *oldSubqueryTle = list_nth(subquery->targetList,
												   targetEntryIndex);
			TargetEntry *newSubqueryTargetEntry = copyObject(oldSubqueryTle);

			Assert(newSubqueryTargetEntry->resjunk == true);

			newSubqueryTargetEntry->resno = resno;
			newSubqueryTargetlist = lappend(newSubqueryTargetlist,
											newSubqueryTargetEntry);

			resno++;
		}
	}

	originalQuery->targetList = newInsertTargetlist;
	subquery->targetList = newSubqueryTargetlist;

	return NULL;
}


/*
 * InsertSelectQuery returns true when the input query
 * is INSERT INTO ... SELECT kind of query.
 */
bool
InsertSelectQuery(Query *query)
{
	CmdType commandType = query->commandType;
	List *rangeTableList = query->rtable;
	RangeTblEntry *subqueryRte = NULL;
	Query *subquery = NULL;

	if (commandType != CMD_INSERT)
	{
		return false;
	}

	rangeTableList = query->rtable;
	if (list_length(rangeTableList) < 2)
	{
		return false;
	}

	subqueryRte = lsecond(query->rtable);
	subquery = subqueryRte->subquery;
	if (subquery == NULL)
	{
		return false;
	}

	return true;
}


/*
 * Copy a RelationRestrictionContext. Note that several subfields are copied
 * shallowly, for lack of copyObject support.
 */
static RelationRestrictionContext *
copyRelationRestrictionContext(RelationRestrictionContext *oldContext)
{
	RelationRestrictionContext *newContext = (RelationRestrictionContext *)
											 palloc(sizeof(RelationRestrictionContext));
	ListCell *relationRestrictionCell = NULL;

	newContext->hasDistributedRelation = oldContext->hasDistributedRelation;
	newContext->hasLocalRelation = oldContext->hasLocalRelation;
	newContext->relationRestrictionList = NIL;

	foreach(relationRestrictionCell, oldContext->relationRestrictionList)
	{
		RelationRestriction *oldRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		RelationRestriction *newRestriction = (RelationRestriction *)
											  palloc0(sizeof(RelationRestriction));

		newRestriction->index = oldRestriction->index;
		newRestriction->relationId = oldRestriction->relationId;
		newRestriction->distributedRelation = oldRestriction->distributedRelation;
		newRestriction->rte = copyObject(oldRestriction->rte);

		/* can't be copied, we copy (flatly) a RelOptInfo, and then decouple baserestrictinfo */
		newRestriction->relOptInfo = palloc(sizeof(RelOptInfo));
		memcpy(newRestriction->relOptInfo, oldRestriction->relOptInfo,
			   sizeof(RelOptInfo));
		newRestriction->relOptInfo->baserestrictinfo = copyObject(
			oldRestriction->relOptInfo->baserestrictinfo);

		/* not copyable, but readonly */
		newRestriction->plannerInfo = oldRestriction->plannerInfo;
		newRestriction->prunedShardIntervalList = copyObject(
			oldRestriction->prunedShardIntervalList);

		newContext->relationRestrictionList =
			lappend(newContext->relationRestrictionList, newRestriction);
	}

	return newContext;
}


/*
 * ReplaceHiddenQual Replace the "hidden" partition restriction clause with
 * the current shard's (passed in context) boundary value.
 *
 * Once we see ($1 = partition column), we replace it with
 * (partCol >= shardMinValue && partCol <= shardMaxValue)
 */
static Node *
ReplaceHiddenQual(Node *node, void *context)
{
	ShardInterval *shardInterval = (ShardInterval *) context;
	Assert(shardInterval->minValueExists);
	Assert(shardInterval->maxValueExists);

	if (node == NULL)
	{
		return NULL;
	}

	/*
	 * Look for operator expressions with two arguments.
	 *
	 * Once Found hidden op, replace with appropriate boundaries for the
	 * current shard interval.
	 *
	 * The boundaries are replaced in the following manner:
	 * (partCol >= shardMinValue && partCol <= shardMaxValue)
	 */
	if (IsA(node, OpExpr) && list_length(((OpExpr *) node)->args) == 2)
	{
		OpExpr *op = (OpExpr *) node;
		Node *leftop = get_leftop((Expr *) op);
		Node *rightop = get_rightop((Expr *) op);
		Param *param = NULL;

		Var *hashedGEColumn = NULL;
		OpExpr *hashedGEOpExpr = NULL;
		Datum shardMinValue = shardInterval->minValue;

		Var *hashedLEColumn = NULL;
		OpExpr *hashedLEOpExpr = NULL;
		Datum shardMaxValue = shardInterval->maxValue;

		List *hashedOperatorList = NIL;

		/* TODO: how can I get those ids */
		Oid integer4GEoperatorId = 525;
		Oid integer4LEoperatorId = 523;

		/* look for the Params */
		if (IsA(leftop, Param))
		{
			param = (Param *) leftop;
		}
		else if (IsA(rightop, Param))
		{
			param = (Param *) rightop;
		}

		/* not an interesting param for our purpose, so return */
		if (!(param && param->paramid == HIDDEN_PARAMETER_ID))
		{
			return node;
		}

		/* generate hashed columns */
		hashedGEColumn = MakeInt4Column();
		hashedLEColumn = MakeInt4Column();

		/* generate the necessary operators */
		hashedGEOpExpr = (OpExpr *) make_opclause(integer4GEoperatorId,
												  InvalidOid, false,
												  (Expr *) hashedGEColumn,
												  (Expr *) MakeInt4Constant(
													  shardMinValue),
												  InvalidOid, InvalidOid);

		hashedLEOpExpr = (OpExpr *) make_opclause(integer4LEoperatorId,
												  InvalidOid, false,
												  (Expr *) hashedLEColumn,
												  (Expr *) MakeInt4Constant(
													  shardMaxValue),
												  InvalidOid, InvalidOid);

		/* update the operators with correct operator numbers and function ids */
		hashedGEOpExpr->opfuncid = get_opcode(hashedGEOpExpr->opno);
		hashedGEOpExpr->opresulttype = get_func_rettype(hashedGEOpExpr->opfuncid);

		hashedLEOpExpr->opfuncid = get_opcode(hashedLEOpExpr->opno);
		hashedLEOpExpr->opresulttype = get_func_rettype(hashedLEOpExpr->opfuncid);

		/* finally add the hashed operators to a list and return it */
		hashedOperatorList = lappend(hashedOperatorList, hashedGEOpExpr);
		hashedOperatorList = lappend(hashedOperatorList, hashedLEOpExpr);

		return (Node *) hashedOperatorList;
	}


	if (IsA(node, Query))
	{
		/* FIXME: probably can remove support for this */
		/* to support CTEs, subqueries, etc */
		return (Node *) query_tree_mutator((Query *) node,
										   ReplaceHiddenQual,
										   context,
										   QTW_EXAMINE_RTES);
	}
	else if (IsA(node, RestrictInfo))
	{
		RestrictInfo *restrictInfo = (RestrictInfo *) node;
		restrictInfo->clause = (Expr *) ReplaceHiddenQual(
			(Node *) restrictInfo->clause, context);

		return (Node *) restrictInfo;
	}

	return expression_tree_mutator(node, ReplaceHiddenQual, context);
}
