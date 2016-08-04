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
#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
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


static void LockShardsForModify(List *shardIntervalList);
static bool HasReplication(List *shardIntervalList);
static int SendQueryToShards(Query *query, List *shardIntervalList, Oid relationId);
static int SendQueryToPlacements(char *shardQueryString,
								 ShardConnections *shardConnections,
								 bool returnTupleCount);
static void deparse_truncate_query(Query *query, Oid distrelid, int64 shardid, StringInfo
								   buffer);

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
	bool isTopLevel = true;
	bool failOK = false;
	List *shardIntervalList = NIL;
	List *prunedShardIntervalList = NIL;
	int32 affectedTupleCount = 0;
	bool validateModifyQuery = true;

	PreventTransactionChain(isTopLevel, "master_modify_multiple_shards");

	queryTreeNode = ParseTreeNode(queryString);
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
		EnsureTablePermissions(relationId, ACL_TRUNCATE);
		validateModifyQuery = false;
	}
	else
	{
		ereport(ERROR, (errmsg("query \"%s\" is not a delete, update, or truncate "
							   "statement", queryString)));
	}

	CheckDistributedTable(relationId);

	queryTreeList = pg_analyze_and_rewrite(queryTreeNode, queryString, NULL, 0);
	modifyQuery = (Query *) linitial(queryTreeList);

	if (validateModifyQuery)
	{
		ErrorIfModifyQueryNotSupported(modifyQuery);
	}

	/* reject queries with a returning list */
	if (list_length(modifyQuery->returningList) > 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("master_modify_multiple_shards() does not support RETURNING")));
	}

	ExecuteMasterEvaluableFunctions(modifyQuery);

	shardIntervalList = LoadShardIntervalList(relationId);
	restrictClauseList = WhereClauseList(modifyQuery->jointree);

	prunedShardIntervalList =
		PruneShardList(relationId, tableId, restrictClauseList, shardIntervalList);

	CHECK_FOR_INTERRUPTS();

	LockShardsForModify(prunedShardIntervalList);

	affectedTupleCount = SendQueryToShards(modifyQuery, prunedShardIntervalList,
										   relationId);

	PG_RETURN_INT32(affectedTupleCount);
}


/*
 * LockShardsForModify command locks the replicas of given shard. The
 * lock logic is slightly different from LockShards function. Basically,
 *
 * 1. If citus.all_modifications_commutative is set to true, then all locks
 * are acquired as ShareLock.
 * 2. If citus.all_modifications_commutative is false, then only the shards
 * with 2 or more replicas are locked with ExclusiveLock. Otherwise, the
 * lock is acquired with ShareLock.
 */
static void
LockShardsForModify(List *shardIntervalList)
{
	LOCKMODE lockMode = NoLock;

	if (AllModificationsCommutative)
	{
		lockMode = ShareLock;
	}
	else if (!HasReplication(shardIntervalList))
	{
		lockMode = ShareLock;
	}
	else
	{
		lockMode = ExclusiveLock;
	}

	LockShards(shardIntervalList, lockMode);
}


/*
 * HasReplication checks whether any of the shards in the given list has more
 * than one replica.
 */
static bool
HasReplication(List *shardIntervalList)
{
	ListCell *shardIntervalCell;
	bool hasReplication = false;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		List *shardPlacementList = FinalizedShardPlacementList(shardId);
		if (shardPlacementList->length > 1)
		{
			hasReplication = true;
		}
	}

	return hasReplication;
}


/*
 * SendQueryToShards executes the given query in all placements of the given
 * shard list and returns the total affected tuple count. The execution is done
 * in a distributed transaction and the commit protocol is decided according to
 * the value of citus.multi_shard_commit_protocol parameter. SendQueryToShards
 * does not acquire locks for the shards so it is advised to acquire locks to
 * the shards when necessary before calling SendQueryToShards.
 */
static int
SendQueryToShards(Query *query, List *shardIntervalList, Oid relationId)
{
	int affectedTupleCount = 0;
	char *relationOwner = TableOwner(relationId);
	ListCell *shardIntervalCell = NULL;
	bool truncateCommand = false;
	bool requestTupleCount = true;

	OpenTransactionsToAllShardPlacements(shardIntervalList, relationOwner);

	if (query->commandType == CMD_UTILITY && IsA(query->utilityStmt, TruncateStmt))
	{
		truncateCommand = true;
		requestTupleCount = false;
	}

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(
			shardIntervalCell);
		Oid relationId = shardInterval->relationId;
		uint64 shardId = shardInterval->shardId;
		bool shardConnectionsFound = false;
		ShardConnections *shardConnections = NULL;
		StringInfo shardQueryString = makeStringInfo();
		char *shardQueryStringData = NULL;
		int shardAffectedTupleCount = -1;

		shardConnections = GetShardConnections(shardId, &shardConnectionsFound);
		Assert(shardConnectionsFound);

		if (truncateCommand)
		{
			deparse_truncate_query(query, relationId, shardId, shardQueryString);
		}
		else
		{
			deparse_shard_query(query, relationId, shardId, shardQueryString);
		}

		shardQueryStringData = shardQueryString->data;
		shardAffectedTupleCount = SendQueryToPlacements(shardQueryStringData,
														shardConnections,
														requestTupleCount);
		affectedTupleCount += shardAffectedTupleCount;
	}

	/* check for cancellation one last time before returning */
	CHECK_FOR_INTERRUPTS();

	return affectedTupleCount;
}


/*
 * deparse_truncate_query creates sql representation of a truncate statement. The
 * function only generated basic truncate statement of the form
 * 'truncate table <table_name>' it ignores all options. It also assumes that
 * there is only one relation in the relation list.
 */
void
deparse_truncate_query(Query *query, Oid distrelid, int64 shardid, StringInfo buffer)
{
	TruncateStmt *truncateStatement = NULL;
	RangeVar *relation = NULL;
	char *qualifiedName = NULL;

	Assert(query->commandType == CMD_UTILITY);
	Assert(IsA(query->utilityStmt, TruncateStmt));

	truncateStatement = (TruncateStmt *) query->utilityStmt;

	Assert(list_length(truncateStatement->relations) == 1);

	relation = (RangeVar *) linitial(truncateStatement->relations);
	qualifiedName = quote_qualified_identifier(relation->schemaname,
											   relation->relname);
	appendStringInfo(buffer, "TRUNCATE TABLE %s_" UINT64_FORMAT, qualifiedName, shardid);
}


/*
 * SendQueryToPlacements sends the given query string to all given placement
 * connections of a shard. CommitRemoteTransactions or AbortRemoteTransactions
 * should be called after all queries have been sent successfully.
 */
static int
SendQueryToPlacements(char *shardQueryString, ShardConnections *shardConnections,
					  bool returnTupleCount)
{
	uint64 shardId = shardConnections->shardId;
	List *connectionList = shardConnections->connectionList;
	ListCell *connectionCell = NULL;
	int32 shardAffectedTupleCount = -1;

	Assert(connectionList != NIL);

	if (!returnTupleCount)
	{
		shardAffectedTupleCount = 0;
	}

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		PGresult *result = NULL;
		char *placementAffectedTupleString = NULL;
		int32 placementAffectedTupleCount = -1;

		CHECK_FOR_INTERRUPTS();

		/* send the query */
		result = PQexec(connection, shardQueryString);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			WarnRemoteError(connection, result);
			ereport(ERROR, (errmsg("could not send query to shard placement")));
		}

		placementAffectedTupleString = PQcmdTuples(result);

		if (returnTupleCount)
		{
			placementAffectedTupleCount = pg_atoi(placementAffectedTupleString,
												  sizeof(int32), 0);

			if ((shardAffectedTupleCount == -1) ||
				(shardAffectedTupleCount == placementAffectedTupleCount))
			{
				shardAffectedTupleCount = placementAffectedTupleCount;
			}
			else
			{
				ereport(ERROR,
						(errmsg("modified %d tuples, but expected to modify %d",
								placementAffectedTupleCount, shardAffectedTupleCount),
						 errdetail("Affected tuple counts at placements of shard "
								   UINT64_FORMAT " are different.", shardId)));
			}
		}

		PQclear(result);

		transactionConnection->transactionState = TRANSACTION_STATE_OPEN;
	}

	return shardAffectedTupleCount;
}
