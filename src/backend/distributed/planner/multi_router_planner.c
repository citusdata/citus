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
#include "catalog/pg_opfamily.h"
#include "distributed/citus_clauses.h"
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distribution_column.h"
#include "distributed/errormessage.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_pruning.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/joininfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "catalog/pg_proc.h"
#include "optimizer/planmain.h"


typedef struct WalkerState
{
	bool containsVar;
	bool varArgument;
	bool badCoalesce;
} WalkerState;

bool EnableRouterExecution = true;


/* planner functions forward declarations */
static MultiPlan * CreateSingleTaskRouterPlan(Query *originalQuery,
											  Query *query,
											  RelationRestrictionContext *
											  restrictionContext);
static bool SafeToPushDownSubquery(PlannerRestrictionContext *plannerRestrictionContext,
								   Query *originalQuery);
static Task * RouterModifyTaskForShardInterval(Query *originalQuery,
											   ShardInterval *shardInterval,
											   RelationRestrictionContext *
											   restrictionContext,
											   uint32 taskIdIndex,
											   bool allRelationsJoinedOnPartitionKey);
static bool MasterIrreducibleExpression(Node *expression, bool *varArgument,
										bool *badCoalesce);
static bool MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state);
static char MostPermissiveVolatileFlag(char left, char right);
static bool TargetEntryChangesValue(TargetEntry *targetEntry, Var *column,
									FromExpr *joinTree);
static Task * RouterModifyTask(Oid distributedTableId, Query *originalQuery,
							   ShardInterval *shardInterval);
static ShardInterval * TargetShardIntervalForModify(Oid distriubtedTableId, Query *query,
													DeferredErrorMessage **planningError);
static ShardInterval * FindShardForUpdateOrDelete(Query *query,
												  DeferredErrorMessage **planningError);
static List * QueryRestrictList(Query *query, char partitionMethod);
static Expr * ExtractInsertPartitionValue(Query *query, Var *partitionColumn);
static Task * RouterSelectTask(Query *originalQuery,
							   RelationRestrictionContext *restrictionContext,
							   List **placementList);
static bool RelationPrunesToMultipleShards(List *relationShardList);
static List * TargetShardIntervalsForSelect(Query *query,
											RelationRestrictionContext *restrictionContext);
static List * WorkersContainingAllShards(List *prunedShardIntervalsList);
static List * IntersectPlacementList(List *lhsPlacementList, List *rhsPlacementList);
static Job * RouterQueryJob(Query *query, Task *task, List *placementList);
static bool MultiRouterPlannableQuery(Query *query,
									  RelationRestrictionContext *restrictionContext);
static DeferredErrorMessage * InsertSelectQuerySupported(Query *queryTree,
														 RangeTblEntry *insertRte,
														 RangeTblEntry *subqueryRte,
														 bool allReferenceTables);
static DeferredErrorMessage * MultiTaskRouterSelectQuerySupported(Query *query);
static DeferredErrorMessage * InsertPartitionColumnMatchesSelect(Query *query,
																 RangeTblEntry *insertRte,
																 RangeTblEntry *
																 subqueryRte,
																 Oid *
																 selectPartitionColumnTableId);
static DeferredErrorMessage * ErrorIfQueryHasModifyingCTE(Query *queryTree);


/*
 * CreateRouterPlan attempts to create a router executor plan for the given
 * SELECT statement.  If planning fails either NULL is returned, or
 * ->planningError is set to a description of the failure.
 */
MultiPlan *
CreateRouterPlan(Query *originalQuery, Query *query,
				 RelationRestrictionContext *restrictionContext)
{
	Assert(EnableRouterExecution);

	if (MultiRouterPlannableQuery(query, restrictionContext))
	{
		return CreateSingleTaskRouterPlan(originalQuery, query,
										  restrictionContext);
	}

	/*
	 * TODO: Instead have MultiRouterPlannableQuery set an error describing
	 * why router cannot support the query.
	 */
	return NULL;
}


/*
 * CreateModifyPlan attempts to create a plan the given modification
 * statement.  If planning fails ->planningError is set to a description of
 * the failure.
 */
MultiPlan *
CreateModifyPlan(Query *originalQuery, Query *query,
				 PlannerRestrictionContext *plannerRestrictionContext)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(originalQuery);
	ShardInterval *targetShardInterval = NULL;
	Task *task = NULL;
	Job *job = NULL;
	List *placementList = NIL;
	MultiPlan *multiPlan = CitusMakeNode(MultiPlan);

	multiPlan->operation = query->commandType;

	multiPlan->planningError = ModifyQuerySupported(query);
	if (multiPlan->planningError != NULL)
	{
		return multiPlan;
	}

	targetShardInterval = TargetShardIntervalForModify(distributedTableId, query,
													   &multiPlan->planningError);
	if (multiPlan->planningError != NULL)
	{
		return multiPlan;
	}

	task = RouterModifyTask(distributedTableId, originalQuery, targetShardInterval);

	ereport(DEBUG2, (errmsg("Creating router plan")));

	job = RouterQueryJob(originalQuery, task, placementList);
	multiPlan->workerJob = job;
	multiPlan->masterQuery = NULL;
	multiPlan->routerExecutable = true;
	multiPlan->hasReturning = false;

	if (list_length(originalQuery->returningList) > 0)
	{
		multiPlan->hasReturning = true;
	}

	return multiPlan;
}


/*
 * CreateSingleTaskRouterPlan creates a physical plan for given query. The created plan is
 * either a modify task that changes a single shard, or a router task that returns
 * query results from a single worker. Supported modify queries (insert/update/delete)
 * are router plannable by default. If query is not router plannable then either NULL is
 * returned, or the returned plan has planningError set to a description of the problem.
 */
static MultiPlan *
CreateSingleTaskRouterPlan(Query *originalQuery, Query *query,
						   RelationRestrictionContext *restrictionContext)
{
	Job *job = NULL;
	Task *task = NULL;
	List *placementList = NIL;
	MultiPlan *multiPlan = CitusMakeNode(MultiPlan);

	multiPlan->operation = query->commandType;

	/* FIXME: this should probably rather be inlined into CreateRouterPlan */
	multiPlan->planningError = ErrorIfQueryHasModifyingCTE(query);
	if (multiPlan->planningError)
	{
		return multiPlan;
	}

	task = RouterSelectTask(originalQuery, restrictionContext, &placementList);
	if (task == NULL)
	{
		return NULL;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	job = RouterQueryJob(originalQuery, task, placementList);

	multiPlan->workerJob = job;
	multiPlan->masterQuery = NULL;
	multiPlan->routerExecutable = true;
	multiPlan->hasReturning = false;

	return multiPlan;
}


/*
 * Creates a router plan for INSERT ... SELECT queries which could consists of
 * multiple tasks.
 *
 * The function never returns NULL, it errors out if cannot create the multi plan.
 */
MultiPlan *
CreateDistributedInsertSelectPlan(Query *originalQuery,
								  PlannerRestrictionContext *plannerRestrictionContext)
{
	int shardOffset = 0;
	List *sqlTaskList = NIL;
	uint32 taskIdIndex = 1;     /* 0 is reserved for invalid taskId */
	Job *workerJob = NULL;
	uint64 jobId = INVALID_JOB_ID;
	MultiPlan *multiPlan = CitusMakeNode(MultiPlan);
	RangeTblEntry *insertRte = ExtractInsertRangeTableEntry(originalQuery);
	RangeTblEntry *subqueryRte = ExtractSelectRangeTableEntry(originalQuery);
	Oid targetRelationId = insertRte->relid;
	DistTableCacheEntry *targetCacheEntry = DistributedTableCacheEntry(targetRelationId);
	int shardCount = targetCacheEntry->shardIntervalArrayLength;
	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	bool allReferenceTables = relationRestrictionContext->allReferenceTables;
	bool safeToPushDownSubquery = false;

	multiPlan->operation = originalQuery->commandType;

	/*
	 * Error semantics for INSERT ... SELECT queries are different than regular
	 * modify queries. Thus, handle separately.
	 */
	multiPlan->planningError = InsertSelectQuerySupported(originalQuery, insertRte,
														  subqueryRte,
														  allReferenceTables);
	if (multiPlan->planningError)
	{
		return multiPlan;
	}

	safeToPushDownSubquery = SafeToPushDownSubquery(plannerRestrictionContext,
													originalQuery);

	/*
	 * Plan select query for each shard in the target table. Do so by replacing the
	 * partitioning qual parameter added in multi_planner() using the current shard's
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
													  relationRestrictionContext,
													  taskIdIndex,
													  safeToPushDownSubquery);

		/* add the task if it could be created */
		if (modifyTask != NULL)
		{
			modifyTask->insertSelectQuery = true;

			sqlTaskList = lappend(sqlTaskList, modifyTask);
		}

		++taskIdIndex;
	}

	if (MultiTaskQueryLogLevel != MULTI_TASK_QUERY_INFO_OFF &&
		list_length(sqlTaskList) > 1)
	{
		ereport(MultiTaskQueryLogLevel, (errmsg("multi-task query about to be executed"),
										 errhint("Queries are split to multiple tasks "
												 "if they have to be split into several"
												 " queries on the workers.")));
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
	multiPlan->workerJob = workerJob;
	multiPlan->masterQuery = NULL;
	multiPlan->routerExecutable = true;
	multiPlan->hasReturning = false;

	if (list_length(originalQuery->returningList) > 0)
	{
		multiPlan->hasReturning = true;
	}

	return multiPlan;
}


/*
 * SafeToPushDownSubquery returns true if either
 *    (i)  there exists join in the query and all relations joined on their
 *         partition keys
 *    (ii) there exists only union set operations and all relations has
 *         partition keys in the same ordinal position in the query
 */
static bool
SafeToPushDownSubquery(PlannerRestrictionContext *plannerRestrictionContext,
					   Query *originalQuery)
{
	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	bool restrictionEquivalenceForPartitionKeys =
		RestrictionEquivalenceForPartitionKeys(plannerRestrictionContext);

	if (restrictionEquivalenceForPartitionKeys)
	{
		return true;
	}

	if (ContainsUnionSubquery(originalQuery))
	{
		return SafeToPushdownUnionSubquery(relationRestrictionContext);
	}

	return false;
}


/*
 * RouterModifyTaskForShardInterval creates a modify task by
 * replacing the partitioning qual parameter added in multi_planner()
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
								 RelationRestrictionContext *restrictionContext,
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
		CopyRelationRestrictionContext(restrictionContext);

	StringInfo queryString = makeStringInfo();
	ListCell *restrictionCell = NULL;
	Task *modifyTask = NULL;
	List *selectPlacementList = NIL;
	uint64 selectAnchorShardId = INVALID_SHARD_ID;
	List *relationShardList = NIL;
	uint64 jobId = INVALID_JOB_ID;
	List *insertShardPlacementList = NULL;
	List *intersectedPlacementList = NULL;
	bool routerPlannable = false;
	bool upsertQuery = false;
	bool replacePrunedQueryWithDummy = false;
	bool allReferenceTables = restrictionContext->allReferenceTables;
	List *shardOpExpressions = NIL;
	RestrictInfo *shardRestrictionList = NULL;

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
	 * Use router select planner to decide on whether we can push down the query
	 * or not. If we can, we also rely on the side-effects that all RTEs have been
	 * updated to point to the relevant nodes and selectPlacementList is determined.
	 */
	routerPlannable = RouterSelectQuery(copiedSubquery, copiedRestrictionContext,
										&selectPlacementList, &selectAnchorShardId,
										&relationShardList, replacePrunedQueryWithDummy);

	if (!routerPlannable)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given "
							   "modification"),
						errdetail("Select query cannot be pushed down to the worker.")));
	}


	/* ensure that we do not send queries where select is pruned away completely */
	if (list_length(selectPlacementList) == 0)
	{
		ereport(DEBUG2, (errmsg("Skipping target shard interval %ld since "
								"SELECT query for it pruned away", shardId)));

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
								  "for shard %ld", shardId)));
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
 * ShardIntervalOpExpressions returns a list of OpExprs with exactly two
 * items in it. The list consists of shard interval ranges with partition columns
 * such as (partitionColumn >= shardMinValue) and (partitionColumn <= shardMaxValue).
 *
 * The function returns hashed columns generated by MakeInt4Column() for the hash
 * partitioned tables in place of partition columns.
 *
 * The function errors out if the given shard interval does not belong to a hash,
 * range and append distributed tables.
 *
 * NB: If you update this, also look at PrunableExpressionsWalker().
 */
List *
ShardIntervalOpExpressions(ShardInterval *shardInterval, Index rteIndex)
{
	Oid relationId = shardInterval->relationId;
	char partitionMethod = PartitionMethod(shardInterval->relationId);
	Var *partitionColumn = NULL;
	Node *baseConstraint = NULL;

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		partitionColumn = MakeInt4Column();
	}
	else if (partitionMethod == DISTRIBUTE_BY_RANGE || partitionMethod ==
			 DISTRIBUTE_BY_APPEND)
	{
		Assert(rteIndex > 0);
		partitionColumn = PartitionColumn(relationId, rteIndex);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot create shard interval operator expression for "
							   "distributed relations other than hash, range and append distributed "
							   "relations")));
	}

	/* build the base expression for constraint */
	baseConstraint = BuildBaseConstraint(partitionColumn);

	/* walk over shard list and check if shards can be pruned */
	if (shardInterval->minValueExists && shardInterval->maxValueExists)
	{
		UpdateConstraint(baseConstraint, shardInterval);
	}

	return list_make1(baseConstraint);
}


/*
 * AddShardIntervalRestrictionToSelect adds the following range boundaries
 * with the given subquery and shardInterval:
 *
 *    hashfunc(partitionColumn) >= $lower_bound AND
 *    hashfunc(partitionColumn) <= $upper_bound
 *
 * The function expects and asserts that subquery's target list contains a partition
 * column value. Thus, this function should never be called with reference tables.
 */
void
AddShardIntervalRestrictionToSelect(Query *subqery, ShardInterval *shardInterval)
{
	List *targetList = subqery->targetList;
	ListCell *targetEntryCell = NULL;
	Var *targetPartitionColumnVar = NULL;
	Oid integer4GEoperatorId = InvalidOid;
	Oid integer4LEoperatorId = InvalidOid;
	TypeCacheEntry *typeEntry = NULL;
	FuncExpr *hashFunctionExpr = NULL;
	OpExpr *greaterThanAndEqualsBoundExpr = NULL;
	OpExpr *lessThanAndEqualsBoundExpr = NULL;
	List *boundExpressionList = NIL;
	Expr *andedBoundExpressions = NULL;

	/* iterate through the target entries */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		if (IsPartitionColumn(targetEntry->expr, subqery) &&
			IsA(targetEntry->expr, Var))
		{
			targetPartitionColumnVar = (Var *) targetEntry->expr;
			break;
		}
	}

	/* we should have found target partition column */
	Assert(targetPartitionColumnVar != NULL);

	integer4GEoperatorId = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT4OID,
											   INT4OID,
											   BTGreaterEqualStrategyNumber);
	integer4LEoperatorId = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT4OID,
											   INT4OID,
											   BTLessEqualStrategyNumber);

	/* ensure that we find the correct operators */
	Assert(integer4GEoperatorId != InvalidOid);
	Assert(integer4LEoperatorId != InvalidOid);

	/* look up the type cache */
	typeEntry = lookup_type_cache(targetPartitionColumnVar->vartype,
								  TYPECACHE_HASH_PROC_FINFO);

	/* probable never possible given that the tables are already hash partitioned */
	if (!OidIsValid(typeEntry->hash_proc_finfo.fn_oid))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify a hash function for type %s",
							   format_type_be(targetPartitionColumnVar->vartype))));
	}

	/* generate hashfunc(partCol) expression */
	hashFunctionExpr = makeNode(FuncExpr);
	hashFunctionExpr->funcid = CitusWorkerHashFunctionId();
	hashFunctionExpr->args = list_make1(targetPartitionColumnVar);

	/* hash functions always return INT4 */
	hashFunctionExpr->funcresulttype = INT4OID;

	/* generate hashfunc(partCol) >= shardMinValue OpExpr */
	greaterThanAndEqualsBoundExpr =
		(OpExpr *) make_opclause(integer4GEoperatorId,
								 InvalidOid, false,
								 (Expr *) hashFunctionExpr,
								 (Expr *) MakeInt4Constant(shardInterval->minValue),
								 targetPartitionColumnVar->varcollid,
								 targetPartitionColumnVar->varcollid);

	/* update the operators with correct operator numbers and function ids */
	greaterThanAndEqualsBoundExpr->opfuncid =
		get_opcode(greaterThanAndEqualsBoundExpr->opno);
	greaterThanAndEqualsBoundExpr->opresulttype =
		get_func_rettype(greaterThanAndEqualsBoundExpr->opfuncid);

	/* generate hashfunc(partCol) <= shardMinValue OpExpr */
	lessThanAndEqualsBoundExpr =
		(OpExpr *) make_opclause(integer4LEoperatorId,
								 InvalidOid, false,
								 (Expr *) hashFunctionExpr,
								 (Expr *) MakeInt4Constant(shardInterval->maxValue),
								 targetPartitionColumnVar->varcollid,
								 targetPartitionColumnVar->varcollid);

	/* update the operators with correct operator numbers and function ids */
	lessThanAndEqualsBoundExpr->opfuncid = get_opcode(lessThanAndEqualsBoundExpr->opno);
	lessThanAndEqualsBoundExpr->opresulttype =
		get_func_rettype(lessThanAndEqualsBoundExpr->opfuncid);

	/* finally add the operators to a list and make them explicitly anded */
	boundExpressionList = lappend(boundExpressionList, greaterThanAndEqualsBoundExpr);
	boundExpressionList = lappend(boundExpressionList, lessThanAndEqualsBoundExpr);

	andedBoundExpressions = make_ands_explicit(boundExpressionList);

	/* finally add the quals */
	if (subqery->jointree->quals == NULL)
	{
		subqery->jointree->quals = (Node *) andedBoundExpressions;
	}
	else
	{
		subqery->jointree->quals = make_and_qual(subqery->jointree->quals,
												 (Node *) andedBoundExpressions);
	}
}


/*
 * ExtractSelectRangeTableEntry returns the range table entry of the subquery.
 * Note that the function expects and asserts that the input query be
 * an INSERT...SELECT query.
 */
RangeTblEntry *
ExtractSelectRangeTableEntry(Query *query)
{
	List *fromList = NULL;
	RangeTblRef *reference = NULL;
	RangeTblEntry *subqueryRte = NULL;

	Assert(InsertSelectQuery(query));

	/* since we already asserted InsertSelectQuery() it is safe to access both lists */
	fromList = query->jointree->fromlist;
	reference = linitial(fromList);
	subqueryRte = rt_fetch(reference->rtindex, query->rtable);

	return subqueryRte;
}


/*
 * ExtractInsertRangeTableEntry returns the INSERT'ed table's range table entry.
 * Note that the function expects and asserts that the input query be
 * an INSERT...SELECT query.
 */
RangeTblEntry *
ExtractInsertRangeTableEntry(Query *query)
{
	int resultRelation = query->resultRelation;
	List *rangeTableList = query->rtable;
	RangeTblEntry *insertRTE = NULL;

	AssertArg(InsertSelectQuery(query));

	insertRTE = rt_fetch(resultRelation, rangeTableList);

	return insertRTE;
}


/*
 * InsertSelectQueryNotSupported returns NULL if the INSERT ... SELECT query
 * is supported, or a description why not.
 */
static DeferredErrorMessage *
InsertSelectQuerySupported(Query *queryTree, RangeTblEntry *insertRte,
						   RangeTblEntry *subqueryRte, bool allReferenceTables)
{
	Query *subquery = NULL;
	Oid selectPartitionColumnTableId = InvalidOid;
	Oid targetRelationId = insertRte->relid;
	char targetPartitionMethod = PartitionMethod(targetRelationId);
	ListCell *rangeTableCell = NULL;
	DeferredErrorMessage *error = NULL;

	/* we only do this check for INSERT ... SELECT queries */
	AssertArg(InsertSelectQuery(queryTree));

	EnsureCoordinator();

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

	subquery = subqueryRte->subquery;

	if (contain_volatile_functions((Node *) queryTree))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "volatile functions are not allowed in INSERT ... SELECT "
							 "queries",
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
								 "a reference table with INSERT ... SELECT",
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
								 "column value must be colocated",
								 NULL, NULL);
		}
	}

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
								 "INSERT ... SELECT queries",
								 NULL, NULL);
		}

		/* pushing down limit per shard would yield wrong results */
		if (subquery->limitCount != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "LIMIT clauses are not allowed in INSERT ... SELECT "
								 "queries",
								 NULL, NULL);
		}

		/* pushing down limit offest per shard would yield wrong results */
		if (subquery->limitOffset != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "OFFSET clauses are not allowed in INSERT ... SELECT "
								 "queries",
								 NULL, NULL);
		}

		/*
		 * We could potentially support window clauses where the data is partitioned
		 * over distribution column. For simplicity, we currently do not support window
		 * clauses at all.
		 */
		if (subquery->windowClause != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "window functions are not allowed in INSERT ... SELECT "
								 "queries",
								 NULL, NULL);
		}

		if (subquery->setOperations != NULL)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Set operations are not allowed in INSERT ... SELECT "
								 "queries",
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
								 "grouping sets are not allowed in INSERT ... SELECT "
								 "queries",
								 NULL, NULL);
		}

		/*
		 * We cannot support DISTINCT ON clauses since it could be on a non-partition column.
		 * In that case, there is no way that Citus can support this.
		 */
		if (subquery->hasDistinctOn)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "DISTINCT ON clauses are not allowed in "
								 "INSERT ... SELECT queries",
								 NULL, NULL);
		}
	}

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
								 "INSERT INTO ... SELECT partition columns in the source "
								 "table and subquery do not match",
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
								 "INSERT INTO ... SELECT partition columns in the source "
								 "table and subquery do not match",
								 "The data type of the target table's partition column "
								 "should exactly match the data type of the "
								 "corresponding simple column reference in the subquery.",
								 NULL);
		}

		/* finally, check that the select target column is a partition column */
		if (!IsPartitionColumn(selectTargetExpr, subquery))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "INSERT INTO ... SELECT partition columns in the source "
								 "table and subquery do not match",
								 "The target table's partition column should correspond "
								 "to a partition column in the subquery.",
								 NULL);
		}

		/* we can set the select relation id */
		*selectPartitionColumnTableId = subqueryPartitionColumnRelationId;

		break;
	}

	if (!targetTableHasPartitionColumn)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "INSERT INTO ... SELECT partition columns in the source "
							 "table and subquery do not match",
							 "the query doesn't include the target table's "
							 "partition column",
							 NULL);
	}

	return NULL;
}


/*
 * ModifyQuerySupported returns NULL if the query only contains supported
 * features, otherwise it returns an error description.
 */
DeferredErrorMessage *
ModifyQuerySupported(Query *queryTree)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	bool isCoordinator = IsCoordinator();
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
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "subqueries are not supported in distributed modifications",
							 NULL, NULL);
	}

	/* reject queries which include CommonTableExpr */
	if (queryTree->cteList != NIL)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "common table expressions are not supported in distributed "
							 "modifications",
							 NULL, NULL);
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		bool referenceTable = false;

		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			/*
			 * We are sure that the table should be distributed, therefore no need to
			 * call IsDistributedTable() here and DistributedTableCacheEntry will
			 * error out if the table is not distributed
			 */
			DistTableCacheEntry *distTableEntry =
				DistributedTableCacheEntry(rangeTableEntry->relid);

			if (distTableEntry->partitionMethod == DISTRIBUTE_BY_NONE)
			{
				referenceTable = true;
			}

			if (referenceTable && !isCoordinator)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot perform distributed planning for the given"
									 " modification",
									 "Modifications to reference tables are "
									 "supported only from the coordinator.",
									 NULL);
			}

			queryTableCount++;

			/* we do not expect to see a view in modify query */
			if (rangeTableEntry->relkind == RELKIND_VIEW)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot modify views over distributed tables",
									 NULL, NULL);
			}
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

			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed planning for the given "
								 "modifications",
								 rangeTableEntryErrorDetail,
								 NULL);
		}
	}

	/*
	 * Reject queries which involve joins. Note that UPSERTs are exceptional for this case.
	 * Queries like "INSERT INTO table_name ON CONFLICT DO UPDATE (col) SET other_col = ''"
	 * contains two range table entries, and we have to allow them.
	 */
	if (commandType != CMD_INSERT && queryTableCount != 1)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot perform distributed planning for the given"
							 " modification",
							 "Joins are not supported in distributed "
							 "modifications.",
							 NULL);
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
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot perform distributed planning for the given"
							 " modification",
							 "Multi-row INSERTs to distributed tables are not "
							 "supported.",
							 NULL);
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
			bool targetEntryPartitionColumn = false;

			/* reference tables do not have partition column */
			if (partitionColumn == NULL)
			{
				targetEntryPartitionColumn = false;
			}
			else if (targetEntry->resno == partitionColumn->varattno)
			{
				targetEntryPartitionColumn = true;
			}

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (commandType == CMD_UPDATE &&
				contain_volatile_functions((Node *) targetEntry->expr))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in UPDATE queries on distributed "
									 "tables must not be VOLATILE",
									 NULL, NULL);
			}

			if (commandType == CMD_UPDATE && targetEntryPartitionColumn &&
				TargetEntryChangesValue(targetEntry, partitionColumn,
										queryTree->jointree))
			{
				specifiesPartitionValue = true;
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
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in the WHERE clause of modification "
									 "queries on distributed tables must not be VOLATILE",
									 NULL, NULL);
			}
			else if (MasterIrreducibleExpression(joinTree->quals, &hasVarArgument,
												 &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (hasVarArgument)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "STABLE functions used in UPDATE queries "
								 "cannot be called with column references",
								 NULL, NULL);
		}

		if (hasBadCoalesce)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "non-IMMUTABLE functions are not allowed in CASE or "
								 "COALESCE statements",
								 NULL, NULL);
		}

		if (contain_mutable_functions((Node *) queryTree->returningList))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "non-IMMUTABLE functions are not allowed in the "
								 "RETURNING clause",
								 NULL, NULL);
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
		bool setTargetEntryPartitionColumn = false;

		/* reference tables do not have partition column */
		if (partitionColumn == NULL)
		{
			setTargetEntryPartitionColumn = false;
		}
		else if (setTargetEntry->resno == partitionColumn->varattno)
		{
			setTargetEntryPartitionColumn = true;
		}

		if (setTargetEntryPartitionColumn)
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
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in the DO UPDATE SET clause of "
									 "INSERTs on distributed tables must be marked "
									 "IMMUTABLE",
									 NULL, NULL);
			}
		}
	}

	/* error if either arbiter or on conflict WHERE contains a mutable function */
	if (contain_mutable_functions((Node *) arbiterWhere) ||
		contain_mutable_functions((Node *) onConflictWhere))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "functions used in the WHERE clause of the "
							 "ON CONFLICT clause of INSERTs on distributed "
							 "tables must be marked IMMUTABLE",
							 NULL, NULL);
	}

	if (specifiesPartitionValue)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "modifying the partition value of rows is not "
							 "allowed",
							 NULL, NULL);
	}

	return NULL;
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
RouterModifyTask(Oid distributedTableId, Query *originalQuery,
				 ShardInterval *shardInterval)
{
	Task *modifyTask = NULL;
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);

	modifyTask = CitusMakeNode(Task);
	modifyTask->jobId = INVALID_JOB_ID;
	modifyTask->taskId = INVALID_TASK_ID;
	modifyTask->taskType = MODIFY_TASK;
	modifyTask->queryString = NULL;
	modifyTask->anchorShardId = INVALID_SHARD_ID;
	modifyTask->dependedTaskList = NIL;
	modifyTask->replicationModel = cacheEntry->replicationModel;

	if (originalQuery->onConflict != NULL)
	{
		RangeTblEntry *rangeTableEntry = NULL;

		/* set the flag */
		modifyTask->upsertQuery = true;

		/* setting an alias simplifies deparsing of UPSERTs */
		rangeTableEntry = linitial(originalQuery->rtable);
		if (rangeTableEntry->alias == NULL)
		{
			Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
			rangeTableEntry->alias = alias;
		}
	}

	if (shardInterval != NULL)
	{
		uint64 shardId = shardInterval->shardId;
		StringInfo queryString = makeStringInfo();

		/* grab shared metadata lock to stop concurrent placement additions */
		LockShardDistributionMetadata(shardId, ShareLock);

		deparse_shard_query(originalQuery, shardInterval->relationId, shardId,
							queryString);
		ereport(DEBUG4, (errmsg("distributed statement: %s", queryString->data)));

		modifyTask->queryString = queryString->data;
		modifyTask->anchorShardId = shardId;
	}

	return modifyTask;
}


/*
 * TargetShardIntervalForModify determines the single shard targeted by a provided
 * modify command. If no matching shards exist, it throws an error. Otherwise, it
 * delegates to FindShardForInsert or FindShardForUpdateOrDelete based on the
 * command type.
 */
static ShardInterval *
TargetShardIntervalForModify(Oid distributedTableId, Query *query,
							 DeferredErrorMessage **planningError)
{
	ShardInterval *shardInterval = NULL;
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	CmdType commandType = query->commandType;
	int shardCount = 0;

	Assert(commandType != CMD_SELECT);

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

	if (commandType == CMD_INSERT)
	{
		shardInterval = FindShardForInsert(query, planningError);
	}
	else
	{
		shardInterval = FindShardForUpdateOrDelete(query, planningError);
	}

	return shardInterval;
}


/*
 * FindShardForInsert returns the shard interval for an INSERT query or NULL if
 * the partition column value is defined as an expression that still needs to be
 * evaluated. If the partition column value falls within 0 or multiple
 * (overlapping) shards, the planningError is set.
 */
ShardInterval *
FindShardForInsert(Query *query, DeferredErrorMessage **planningError)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;
	uint32 rangeTableId = 1;
	Var *partitionColumn = NULL;
	Expr *partitionValueExpr = NULL;
	Const *partitionValueConst = NULL;
	List *shardIntervalList = NIL;
	List *prunedShardList = NIL;
	int prunedShardCount = 0;

	Assert(query->commandType == CMD_INSERT);

	/* reference tables can only have one shard */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		int shardCount = 0;

		shardIntervalList = LoadShardIntervalList(distributedTableId);
		shardCount = list_length(shardIntervalList);

		if (shardCount != 1)
		{
			ereport(ERROR, (errmsg("reference table cannot have %d shards", shardCount)));
		}

		return (ShardInterval *) linitial(shardIntervalList);
	}

	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);

	partitionValueExpr = ExtractInsertPartitionValue(query, partitionColumn);
	if (!IsA(partitionValueExpr, Const))
	{
		/* shard pruning not possible right now */
		return NULL;
	}

	partitionValueConst = (Const *) partitionValueExpr;
	if (partitionValueConst->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot perform an INSERT with NULL in the partition "
							   "column")));
	}

	if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod == DISTRIBUTE_BY_RANGE)
	{
		Datum partitionValue = partitionValueConst->constvalue;
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
		ShardInterval *shardInterval = FindShardInterval(partitionValue, cacheEntry);

		if (shardInterval != NULL)
		{
			prunedShardList = list_make1(shardInterval);
		}
	}
	else
	{
		List *restrictClauseList = NIL;
		Index tableId = 1;
		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);
		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;

		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValueConst->constvalue;
		rightConst->constisnull = partitionValueConst->constisnull;
		rightConst->constbyval = partitionValueConst->constbyval;

		restrictClauseList = list_make1(equalityExpr);

		prunedShardList = PruneShards(distributedTableId, tableId, restrictClauseList);
	}

	prunedShardCount = list_length(prunedShardList);
	if (prunedShardCount != 1)
	{
		char *partitionKeyString = cacheEntry->partitionKeyString;
		char *partitionColumnName = ColumnNameToColumn(distributedTableId,
													   partitionKeyString);
		StringInfo errorMessage = makeStringInfo();
		StringInfo errorHint = makeStringInfo();
		const char *targetCountType = NULL;

		if (prunedShardCount == 0)
		{
			targetCountType = "no";
		}
		else
		{
			targetCountType = "multiple";
		}

		if (prunedShardCount == 0)
		{
			appendStringInfo(errorHint, "Make sure you have created a shard which "
										"can receive this partition column value.");
		}
		else
		{
			appendStringInfo(errorHint, "Make sure the value for partition column "
										"\"%s\" falls into a single shard.",
							 partitionColumnName);
		}

		appendStringInfo(errorMessage, "cannot run INSERT command which targets %s "
									   "shards", targetCountType);

		(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL,
										 errorHint->data);

		return NULL;
	}

	return (ShardInterval *) linitial(prunedShardList);
}


/*
 * FindShardForUpdateOrDelete finds the shard interval in which an UPDATE or
 * DELETE command should be applied, or sets planningError when the query
 * needs to be applied to multiple or no shards.
 */
static ShardInterval *
FindShardForUpdateOrDelete(Query *query, DeferredErrorMessage **planningError)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;
	CmdType commandType = query->commandType;
	List *restrictClauseList = NIL;
	Index tableId = 1;
	List *prunedShardList = NIL;
	int prunedShardCount = 0;

	Assert(commandType == CMD_UPDATE || commandType == CMD_DELETE);

	restrictClauseList = QueryRestrictList(query, partitionMethod);
	prunedShardList = PruneShards(distributedTableId, tableId, restrictClauseList);

	prunedShardCount = list_length(prunedShardList);
	if (prunedShardCount != 1)
	{
		char *partitionKeyString = cacheEntry->partitionKeyString;
		char *partitionColumnName = ColumnNameToColumn(distributedTableId,
													   partitionKeyString);
		StringInfo errorMessage = makeStringInfo();
		StringInfo errorHint = makeStringInfo();
		const char *commandName = NULL;
		const char *targetCountType = NULL;

		if (commandType == CMD_UPDATE)
		{
			commandName = "UPDATE";
		}
		else
		{
			commandName = "DELETE";
		}

		if (prunedShardCount == 0)
		{
			targetCountType = "no";
		}
		else
		{
			targetCountType = "multiple";
		}

		appendStringInfo(errorHint, "Consider using an equality filter on "
									"partition column \"%s\" to target a "
									"single shard. If you'd like to run a "
									"multi-shard operation, use "
									"master_modify_multiple_shards().",
						 partitionColumnName);

		if (commandType == CMD_DELETE && partitionMethod == DISTRIBUTE_BY_APPEND)
		{
			appendStringInfo(errorHint, " You can also use "
										"master_apply_delete_command() to drop "
										"all shards satisfying delete criteria.");
		}

		appendStringInfo(errorMessage, "cannot run %s command which targets %s shards",
						 commandName, targetCountType);

		(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL,
										 errorHint->data);

		return NULL;
	}

	return (ShardInterval *) linitial(prunedShardList);
}


/*
 * QueryRestrictList returns the restriction clauses for the query. For a SELECT
 * statement these are the where-clause expressions. For INSERT statements we
 * build an equality clause based on the partition-column and its supplied
 * insert value.
 *
 * Since reference tables do not have partition columns, the function returns
 * NIL for reference tables.
 */
static List *
QueryRestrictList(Query *query, char partitionMethod)
{
	List *queryRestrictList = NIL;

	/*
	 * Reference tables do not have the notion of partition column. Thus,
	 * there are no restrictions on the partition column.
	 */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		return queryRestrictList;
	}

	queryRestrictList = WhereClauseList(query->jointree);

	return queryRestrictList;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
Oid
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
static Expr *
ExtractInsertPartitionValue(Query *query, Var *partitionColumn)
{
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot perform an INSERT without a partition column "
							   "value")));
	}

	return targetEntry->expr;
}


/* RouterSelectTask builds a Task to represent a single shard select query */
static Task *
RouterSelectTask(Query *originalQuery, RelationRestrictionContext *restrictionContext,
				 List **placementList)
{
	Task *task = NULL;
	bool queryRoutable = false;
	StringInfo queryString = makeStringInfo();
	bool upsertQuery = false;
	uint64 shardId = INVALID_SHARD_ID;
	List *relationShardList = NIL;
	bool replacePrunedQueryWithDummy = false;

	/* router planner should create task even if it deosn't hit a shard at all */
	replacePrunedQueryWithDummy = true;

	queryRoutable = RouterSelectQuery(originalQuery, restrictionContext,
									  placementList, &shardId, &relationShardList,
									  replacePrunedQueryWithDummy);


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
	task->replicationModel = REPLICATION_MODEL_INVALID;
	task->dependedTaskList = NIL;
	task->upsertQuery = upsertQuery;
	task->relationShardList = relationShardList;

	return task;
}


/*
 * RouterSelectQuery returns true if the input query can be pushed down to the
 * worker node as it is. Otherwise, the function returns false.
 *
 * On return true, all RTEs have been updated to point to the relevant shards in
 * the originalQuery. Also, placementList is filled with the list of worker nodes
 * that has all the required shard placements for the query execution.
 * anchorShardId is set to the first pruned shardId of the given query. Finally,
 * relationShardList is filled with the list of relation-to-shard mappings for
 * the query.
 */
bool
RouterSelectQuery(Query *originalQuery, RelationRestrictionContext *restrictionContext,
				  List **placementList, uint64 *anchorShardId, List **relationShardList,
				  bool replacePrunedQueryWithDummy)
{
	List *prunedRelationShardList = TargetShardIntervalsForSelect(originalQuery,
																  restrictionContext);
	uint64 shardId = INVALID_SHARD_ID;
	CmdType commandType PG_USED_FOR_ASSERTS_ONLY = originalQuery->commandType;
	ListCell *prunedRelationShardListCell = NULL;
	List *workerList = NIL;
	bool shardsPresent = false;

	*placementList = NIL;

	if (prunedRelationShardList == NULL)
	{
		return false;
	}

	Assert(commandType == CMD_SELECT);

	foreach(prunedRelationShardListCell, prunedRelationShardList)
	{
		List *prunedShardList = (List *) lfirst(prunedRelationShardListCell);

		ShardInterval *shardInterval = NULL;
		RelationShard *relationShard = NULL;

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardList == NIL)
		{
			continue;
		}

		shardsPresent = true;

		/* all relations are now pruned down to 0 or 1 shards */
		Assert(list_length(prunedShardList) <= 1);

		shardInterval = (ShardInterval *) linitial(prunedShardList);

		/* anchor shard id */
		if (shardId == INVALID_SHARD_ID)
		{
			shardId = shardInterval->shardId;
		}

		/* add relation to shard mapping */
		relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = shardInterval->relationId;
		relationShard->shardId = shardInterval->shardId;

		*relationShardList = lappend(*relationShardList, relationShard);
	}

	/*
	 * We bail out if there are RTEs that prune multiple shards above, but
	 * there can also be multiple RTEs that reference the same relation.
	 */
	if (RelationPrunesToMultipleShards(*relationShardList))
	{
		return false;
	}

	/*
	 * Determine the worker that has all shard placements if a shard placement found.
	 * If no shard placement exists and replacePrunedQueryWithDummy flag is set, we will
	 * still run the query but the result will be empty. We create a dummy shard
	 * placement for the first active worker.
	 */
	if (shardsPresent)
	{
		workerList = WorkersContainingAllShards(prunedRelationShardList);
	}
	else if (replacePrunedQueryWithDummy)
	{
		List *workerNodeList = ActiveWorkerNodeList();
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
	else
	{
		/*
		 * For INSERT ... SELECT, this query could be still a valid for some other target
		 * shard intervals. Thus, we should return empty list if there aren't any matching
		 * workers, so that the caller can decide what to do with this task.
		 */
		workerList = NIL;

		return true;
	}

	if (workerList == NIL)
	{
		ereport(DEBUG2, (errmsg("Found no worker with all shard placements")));

		return false;
	}

	UpdateRelationToShardNames((Node *) originalQuery, *relationShardList);

	*placementList = workerList;
	*anchorShardId = shardId;

	return true;
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
			prunedShardList = PruneShards(relationId, tableId,
										  restrictClauseList);

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
 * RelationPrunesToMultipleShards returns true if the given list of
 * relation-to-shard mappings contains at least two mappings with
 * the same relation, but different shards.
 */
static bool
RelationPrunesToMultipleShards(List *relationShardList)
{
	ListCell *relationShardCell = NULL;
	RelationShard *previousRelationShard = NULL;

	relationShardList = SortList(relationShardList, CompareRelationShards);

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);

		if (previousRelationShard != NULL &&
			relationShard->relationId == previousRelationShard->relationId &&
			relationShard->shardId != previousRelationShard->shardId)
		{
			return true;
		}

		previousRelationShard = relationShard;
	}

	return false;
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
	bool deferredPruning = false;

	if (taskType == MODIFY_TASK)
	{
		if (task->anchorShardId != INVALID_SHARD_ID)
		{
			/*
			 * We were able to assign a shard ID. Generate task
			 * placement list using the first-replica assignment
			 * policy (modify placements in placement ID order).
			 */
			taskList = FirstReplicaAssignTaskList(list_make1(task));
			requiresMasterEvaluation = RequiresMasterEvaluation(query);
		}
		else
		{
			/*
			 * We were unable to assign a shard ID yet, meaning
			 * the partition column value is an expression.
			 */
			taskList = list_make1(task);
			requiresMasterEvaluation = true;
			deferredPruning = true;
		}
	}
	else
	{
		/*
		 * For selects we get the placement list during shard
		 * pruning.
		 */
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
	job->deferredPruning = deferredPruning;

	return job;
}


/*
 * MultiRouterPlannableQuery returns true if given query can be router plannable.
 * The query is router plannable if it is a modify query, or if its is a select
 * query issued on a hash partitioned distributed table, and it has a filter
 * to reduce number of shard pairs to one, and all shard pairs are located on
 * the same node. Router plannable checks for select queries can be turned off
 * by setting citus.enable_router_execution flag to false.
 */
static bool
MultiRouterPlannableQuery(Query *query, RelationRestrictionContext *restrictionContext)
{
	CmdType commandType = query->commandType;
	ListCell *relationRestrictionContextCell = NULL;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return true;
	}

	Assert(commandType == CMD_SELECT);

	if (!EnableRouterExecution)
	{
		return false;
	}

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

			if (!(partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod ==
				  DISTRIBUTE_BY_NONE || partitionMethod == DISTRIBUTE_BY_RANGE))
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

	AssertArg(InsertSelectQuery(originalQuery));

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
#if (PG_VERSION_NUM >= 90600)
		targetVarList = pull_var_clause((Node *) oldInsertTargetEntry->expr,
										PVC_RECURSE_AGGREGATES);
#else
		targetVarList = pull_var_clause((Node *) oldInsertTargetEntry->expr,
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
#endif

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
 * InsertSelectQuery returns true when the input query
 * is INSERT INTO ... SELECT kind of query.
 *
 * Note that the input query should be the original parsetree of
 * the query (i.e., not passed trough the standard planner).
 *
 * This function is inspired from getInsertSelectQuery() on
 * rewrite/rewriteManip.c.
 */
bool
InsertSelectQuery(Query *query)
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
	Assert(IsA(rangeTableReference, RangeTblRef));

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
 * Copy a RelationRestrictionContext. Note that several subfields are copied
 * shallowly, for lack of copyObject support.
 *
 * Note that CopyRelationRestrictionContext copies the following fields per relation
 * context: index, relationId, distributedRelation, rte, relOptInfo->baserestrictinfo
 * and relOptInfo->joininfo. Also, the function shallowly copies plannerInfo and
 * prunedShardIntervalList which are read-only. All other parts of the relOptInfo
 * is also shallowly copied.
 */
RelationRestrictionContext *
CopyRelationRestrictionContext(RelationRestrictionContext *oldContext)
{
	RelationRestrictionContext *newContext = (RelationRestrictionContext *)
											 palloc(sizeof(RelationRestrictionContext));
	ListCell *relationRestrictionCell = NULL;

	newContext->hasDistributedRelation = oldContext->hasDistributedRelation;
	newContext->hasLocalRelation = oldContext->hasLocalRelation;
	newContext->allReferenceTables = oldContext->allReferenceTables;
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

		newRestriction->relOptInfo->baserestrictinfo =
			copyObject(oldRestriction->relOptInfo->baserestrictinfo);

		newRestriction->relOptInfo->joininfo =
			copyObject(oldRestriction->relOptInfo->joininfo);

		/* not copyable, but readonly */
		newRestriction->plannerInfo = oldRestriction->plannerInfo;
		newRestriction->prunedShardIntervalList = oldRestriction->prunedShardIntervalList;

		newContext->relationRestrictionList =
			lappend(newContext->relationRestrictionList, newRestriction);
	}

	return newContext;
}


/*
 * ErrorIfQueryHasModifyingCTE checks if the query contains modifying common table
 * expressions and errors out if it does.
 */
static DeferredErrorMessage *
ErrorIfQueryHasModifyingCTE(Query *queryTree)
{
	ListCell *cteCell = NULL;

	Assert(queryTree->commandType == CMD_SELECT);

	foreach(cteCell, queryTree->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
		Query *cteQuery = (Query *) cte->ctequery;

		/*
		 * Here we only check for command type of top level query. Normally there can be
		 * nested CTE, however PostgreSQL dictates that data-modifying statements must
		 * be at top level of CTE. Therefore it is OK to just check for top level.
		 * Similarly, we do not need to check for subqueries.
		 */
		if (cteQuery->commandType != CMD_SELECT)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "data-modifying statements are not supported in "
								 "the WITH clauses of distributed queries",
								 NULL, NULL);
		}
	}

	/* everything OK */
	return NULL;
}
