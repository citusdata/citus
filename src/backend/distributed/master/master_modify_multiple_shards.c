/*-------------------------------------------------------------------------
 *
 * master_modify_multiple_shards.c
 *	  UDF to run multi shard update/delete queries
 *
 * This file contains master_modify_multiple_shards function, which takes a update
 * or delete query and runs it worker shards of the distributed table. The distributed
 * modify operation can be done within a distributed transaction and committed in
 * one-phase or two-phase fashion, depending on the citus.multi_shard_commit_protocol
 * setting.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_pruning.h"
#include "distributed/worker_protocol.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "nodes/makefuncs.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


static List * ModifyMultipleShardsTaskList(Query *query, List *shardIntervalList);


PG_FUNCTION_INFO_V1(master_modify_multiple_shards);


/*
 * master_modify_multiple_shards takes in a DELETE or UPDATE query string and
 * pushes the query to shards. It finds shards that match the criteria defined
 * in the delete command, generates the same delete query string for each of the
 * found shards with distributed table name replaced with the shard name and
 * sends the queries to the workers. It uses one-phase or two-phase commit
 * transactions depending on citus.copy_transaction_manager value.
 */
Datum
master_modify_multiple_shards(PG_FUNCTION_ARGS)
{
	text *queryText = PG_GETARG_TEXT_P(0);
	char *queryString = text_to_cstring(queryText);
	List *queryTreeList = NIL;
	Oid relationId = InvalidOid;
	Index tableId = 1;
	Query *modifyQuery = NULL;
	Node *queryTreeNode;
	List *restrictClauseList = NIL;
	bool failOK = false;
	List *prunedShardIntervalList = NIL;
	List *taskList = NIL;
	int32 affectedTupleCount = 0;
#if (PG_VERSION_NUM >= 100000)
	RawStmt *rawStmt = (RawStmt *) ParseTreeRawStmt(queryString);
	queryTreeNode = rawStmt->stmt;
#else
	queryTreeNode = ParseTreeNode(queryString);
#endif

	EnsureCoordinator();
	CheckCitusVersion(ERROR);


	if (IsA(queryTreeNode, DeleteStmt))
	{
		DeleteStmt *deleteStatement = (DeleteStmt *) queryTreeNode;
		relationId = RangeVarGetRelid(deleteStatement->relation, NoLock, failOK);
		EnsureTablePermissions(relationId, ACL_DELETE);
	}
	else if (IsA(queryTreeNode, UpdateStmt))
	{
		UpdateStmt *updateStatement = (UpdateStmt *) queryTreeNode;
		relationId = RangeVarGetRelid(updateStatement->relation, NoLock, failOK);
		EnsureTablePermissions(relationId, ACL_UPDATE);
	}
	else if (IsA(queryTreeNode, TruncateStmt))
	{
		TruncateStmt *truncateStatement = (TruncateStmt *) queryTreeNode;
		List *relationList = truncateStatement->relations;
		RangeVar *rangeVar = NULL;

		if (list_length(relationList) != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("master_modify_multiple_shards() can truncate only "
							"one table")));
		}

		rangeVar = (RangeVar *) linitial(relationList);
		relationId = RangeVarGetRelid(rangeVar, NoLock, failOK);
		if (rangeVar->schemaname == NULL)
		{
			Oid schemaOid = get_rel_namespace(relationId);
			char *schemaName = get_namespace_name(schemaOid);
			rangeVar->schemaname = schemaName;
		}

		EnsureTablePermissions(relationId, ACL_TRUNCATE);
	}
	else
	{
		ereport(ERROR, (errmsg("query \"%s\" is not a delete, update, or truncate "
							   "statement", queryString)));
	}

	CheckDistributedTable(relationId);

#if (PG_VERSION_NUM >= 100000)
	queryTreeList = pg_analyze_and_rewrite(rawStmt, queryString, NULL, 0, NULL);
#else
	queryTreeList = pg_analyze_and_rewrite(queryTreeNode, queryString, NULL, 0);
#endif
	modifyQuery = (Query *) linitial(queryTreeList);

	if (modifyQuery->commandType != CMD_UTILITY)
	{
		bool multiShardQuery = true;
		DeferredErrorMessage *error =
			ModifyQuerySupported(modifyQuery, modifyQuery, multiShardQuery, NULL);

		if (error)
		{
			RaiseDeferredError(error, ERROR);
		}
	}

	/* reject queries with a returning list */
	if (list_length(modifyQuery->returningList) > 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("master_modify_multiple_shards() does not support RETURNING")));
	}

	ExecuteMasterEvaluableFunctions(modifyQuery, NULL);

	restrictClauseList = WhereClauseList(modifyQuery->jointree);

	prunedShardIntervalList =
		PruneShards(relationId, tableId, restrictClauseList);

	CHECK_FOR_INTERRUPTS();

	taskList = ModifyMultipleShardsTaskList(modifyQuery, prunedShardIntervalList);
	affectedTupleCount = ExecuteModifyTasksWithoutResults(taskList);

	PG_RETURN_INT32(affectedTupleCount);
}


/*
 * ModifyMultipleShardsTaskList builds a list of tasks to execute a query on a
 * given list of shards.
 */
static List *
ModifyMultipleShardsTaskList(Query *query, List *shardIntervalList)
{
	List *taskList = NIL;
	ListCell *shardIntervalCell = NULL;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		Oid relationId = shardInterval->relationId;
		uint64 shardId = shardInterval->shardId;
		StringInfo shardQueryString = makeStringInfo();
		Task *task = NULL;

		deparse_shard_query(query, relationId, shardId, shardQueryString);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = MODIFY_TASK;
		task->queryString = shardQueryString->data;
		task->dependedTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}
