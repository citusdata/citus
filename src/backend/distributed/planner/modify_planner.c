/*-------------------------------------------------------------------------
 *
 * modify_planner.c
 *
 * This file contains functions to plan distributed table modifications.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stddef.h>

#if (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600)
#include "access/stratnum.h"
#else
#include "access/skey.h"
#endif
#include "distributed/citus_nodes.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/modify_planner.h" /* IWYU pragma: keep */
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#if (PG_VERSION_NUM >= 90500)
#include "nodes/makefuncs.h"
#endif
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"


/* planner functions forward declarations */
static void ErrorIfQueryNotSupported(Query *queryTree);
static Task * DistributedModifyTask(Query *query);
#if (PG_VERSION_NUM >= 90500)
static OnConflictExpr * RebuildOnConflict(Oid relationId,
										  OnConflictExpr *originalOnConflict);
#endif
static Job * DistributedModifyJob(Query *query, Task *modifyTask);
static List * QueryRestrictList(Query *query);
static ShardInterval * DistributedModifyShardInterval(Query *query);
static Oid ExtractFirstDistributedTableId(Query *query);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);


/*
 * MultiModifyPlanCreate actually creates the distributed plan for execution
 * of a distribution modification. It expects that the provided MultiTreeRoot
 * is actually a Query object, which it uses directly to produce a MultiPlan.
 */
MultiPlan *
MultiModifyPlanCreate(Query *query)
{
	Task *modifyTask = NULL;
	Job *modifyJob = NULL;
	MultiPlan *multiPlan = NULL;

	ErrorIfQueryNotSupported(query);

	modifyTask = DistributedModifyTask(query);

	modifyJob = DistributedModifyJob(query, modifyTask);

	multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->workerJob = modifyJob;
	multiPlan->masterQuery = NULL;
	multiPlan->masterTableName = NULL;

	return multiPlan;
}


/*
 * ErrorIfQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
static void
ErrorIfQueryNotSupported(Query *queryTree)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	char partitionMethod = PartitionMethod(distributedTableId);
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;
	bool hasNonConstTargetEntryExprs = false;
	bool hasNonConstQualExprs = false;
	bool specifiesPartitionValue = false;
#if (PG_VERSION_NUM >= 90500)
	ListCell *setTargetCell = NULL;
	List *onConflictSet = NIL;
	Node *arbiterWhere = NULL;
	Node *onConflictWhere = NULL;
#endif

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
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Multi-row INSERTs to distributed tables are not "
								  "supported.")));
	}

	/* reject queries with a returning list */
	if (list_length(queryTree->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("RETURNING clauses are not supported in distributed "
								  "modifications.")));
	}

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		FromExpr *joinTree = NULL;
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (!IsA(targetEntry->expr, Const))
			{
				hasNonConstTargetEntryExprs = true;
			}

			if (commandType == CMD_UPDATE &&
				targetEntry->resno == partitionColumn->varattno)
			{
				specifiesPartitionValue = true;
			}
		}

		joinTree = queryTree->jointree;
		if (joinTree != NULL && contain_mutable_functions(joinTree->quals))
		{
			hasNonConstQualExprs = true;
		}
	}

#if (PG_VERSION_NUM >= 90500)
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
				hasNonConstTargetEntryExprs = true;
			}
		}
	}

	/* error if either arbiter or on conflict WHERE contains a mutable function */
	if (contain_mutable_functions((Node *) arbiterWhere) ||
		contain_mutable_functions((Node *) onConflictWhere))
	{
		hasNonConstQualExprs = true;
	}
#endif

	if (hasNonConstTargetEntryExprs || hasNonConstQualExprs)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan sharded modification containing values "
							   "which are not constants or constant expressions")));
	}

	if (specifiesPartitionValue)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("modifying the partition value of rows is not allowed")));
	}
}


/*
 * DistributedModifyTask builds a Task to represent a modification performed by
 * the provided query against the provided shard interval. This task contains
 * shard-extended deparsed SQL to be run during execution.
 */
static Task *
DistributedModifyTask(Query *query)
{
	ShardInterval *shardInterval = DistributedModifyShardInterval(query);
	uint64 shardId = shardInterval->shardId;
	FromExpr *joinTree = NULL;
	StringInfo queryString = makeStringInfo();
	Task *modifyTask = NULL;
	bool upsertQuery = false;

	/* grab shared metadata lock to stop concurrent placement additions */
	LockShardDistributionMetadata(shardId, ShareLock);

	/*
	 * Convert the qualifiers to an explicitly and'd clause, which is needed
	 * before we deparse the query. This applies to SELECT, UPDATE and
	 * DELETE statements.
	 */
	joinTree = query->jointree;
	if ((joinTree != NULL) && (joinTree->quals != NULL))
	{
		Node *whereClause = joinTree->quals;
		if (IsA(whereClause, List))
		{
			joinTree->quals = (Node *) make_ands_explicit((List *) whereClause);
		}
	}

#if (PG_VERSION_NUM >= 90500)
	if (query->onConflict != NULL)
	{
		RangeTblEntry *rangeTableEntry = NULL;
		Oid relationId = shardInterval->relationId;

		/* set the flag */
		upsertQuery = true;

		/* setting an alias simplifies deparsing of UPSERTs */
		rangeTableEntry = linitial(query->rtable);
		if (rangeTableEntry->alias == NULL)
		{
			Alias *alias = makeAlias(UPSERT_ALIAS, NIL);
			rangeTableEntry->alias = alias;
		}

		/* some fields in onConflict expression needs to be updated for deparsing */
		query->onConflict = RebuildOnConflict(relationId, query->onConflict);
	}
#else
	/* always set to false for PG_VERSION_NUM < 90500 */
	upsertQuery = false;
#endif

	deparse_shard_query(query, shardInterval->relationId, shardId, queryString);
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


#if (PG_VERSION_NUM >= 90500)
/*
 * RebuildOnConflict rebuilds OnConflictExpr for correct deparsing. The function
 * makes WHERE clause elements explicit and filters dropped columns
 * from the target list.
 */
static OnConflictExpr *
RebuildOnConflict(Oid relationId, OnConflictExpr *originalOnConflict)
{
	OnConflictExpr *updatedOnConflict = copyObject(originalOnConflict);
	Node *onConflictWhere = updatedOnConflict->onConflictWhere;
	List *onConflictSet = updatedOnConflict->onConflictSet;
	TupleDesc distributedRelationDesc = NULL;
	ListCell *targetEntryCell = NULL;
	List *filteredOnConflictSet = NIL;
	Form_pg_attribute *tableAttributes = NULL;
	Relation distributedRelation = RelationIdGetRelation(relationId);

	/* Convert onConflictWhere qualifiers to an explicitly and'd clause */
	updatedOnConflict->onConflictWhere =
			(Node *) make_ands_explicit((List *) onConflictWhere);

	/*
	 * Here we handle dropped columns on the distributed table. onConflictSet
	 * includes the table attributes even if they are dropped,
	 * since the it is expanded via expand_targetlist() on standard planner.
	 */

	/* get the relation tuple descriptor and table attributes */
	distributedRelationDesc = RelationGetDescr(distributedRelation);
	tableAttributes = distributedRelationDesc->attrs;

	foreach(targetEntryCell, onConflictSet)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		FormData_pg_attribute *tableAttribute = tableAttributes[targetEntry->resno -1];

		/* skip dropped columns */
		if (tableAttribute->attisdropped)
		{
			continue;
		}

		/* we only want to deparse non-dropped columns */
		filteredOnConflictSet = lappend(filteredOnConflictSet, targetEntry);
	}

	/* close distributedRelation to prevent leaks */
	RelationClose(distributedRelation);

	/* set onConflictSet again with the filtered list */
	updatedOnConflict->onConflictSet = filteredOnConflictSet;

	return updatedOnConflict;
}
#endif


/*
 * DistributedModifyJob creates a Job for the specified query to execute the
 * provided modification task. Modification task placements are produced using
 * the "first-replica" algorithm, except modifications run against all matching
 * placements rather than just the first successful one.
 */
Job *
DistributedModifyJob(Query *query, Task *modifyTask)
{
	Job *modifyJob = NULL;
	List *taskList = FirstReplicaAssignTaskList(list_make1(modifyTask));

	modifyJob = CitusMakeNode(Job);
	modifyJob->dependedJobList = NIL;
	modifyJob->jobId = INVALID_JOB_ID;
	modifyJob->subqueryPushdown = false;
	modifyJob->jobQuery = query;
	modifyJob->taskList = taskList;

	return modifyJob;
}


/*
 * DistributedModifyShardInterval determines the single shard targeted by a
 * provided distributed modification command. If no matching shards exist, or
 * if the modification targets more than one one shard, this function raises
 * an error.
 */
static ShardInterval *
DistributedModifyShardInterval(Query *query)
{
	List *restrictClauseList = NIL;
	List *prunedShardList = NIL;
	Index tableId = 1;

	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	List *shardIntervalList = NIL;

	/* error out if no shards exist for the table */
	shardIntervalList = LoadShardIntervalList(distributedTableId);
	if (shardIntervalList == NIL)
	{
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards for modification"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}

	restrictClauseList = QueryRestrictList(query);
	prunedShardList = PruneShardList(distributedTableId, tableId, restrictClauseList,
									 shardIntervalList);

	if (list_length(prunedShardList) != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("distributed modifications must target exactly one "
							   "shard")));
	}

	return (ShardInterval *) linitial(prunedShardList);
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
		Const *partitionValue = ExtractPartitionValue(query, partitionColumn);

		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);

		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;
		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValue->constvalue;
		rightConst->constisnull = partitionValue->constisnull;
		rightConst->constbyval = partitionValue->constbyval;

		queryRestrictList = list_make1(equalityExpr);
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
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
 * of a modification command. If a partition value is missing altogether or is
 * NULL, this function throws an error.
 */
static Const *
ExtractPartitionValue(Query *query, Var *partitionColumn)
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
