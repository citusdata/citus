/*-------------------------------------------------------------------------
 *
 * master_delete_protocol.c
 *
 * Routine for deleting shards in the distributed cluster. This function takes
 * in a delete command and deletes a shard if and only if all rows in the shard
 * satisfy the conditions in the delete command.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_partition.h"
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


/* Local functions forward declarations */
static void CheckTableCount(Query *deleteQuery);
static void CheckDeleteCriteria(Node *deleteCriteria);
static void CheckPartitionColumn(Oid relationId, Node *whereClause);
static List * ShardsMatchingDeleteCriteria(Oid relationId, List *shardList,
										   Node *deleteCriteria);
static int DropShards(Oid relationId, char *schemaName, char *relationName,
					  List *deletableShardIntervalList);
static bool ExecuteRemoteCommand(const char *nodeName, uint32 nodePort,
								 StringInfo queryString);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_apply_delete_command);
PG_FUNCTION_INFO_V1(master_drop_all_shards);


/*
 * master_apply_delete_command takes in a delete command, finds shards that
 * match the criteria defined in the delete command, drops the found shards from
 * the worker nodes, and updates the corresponding metadata on the master node.
 * This function drops a shard if and only if all rows in the shard satisfy
 * the conditions in the delete command. Note that this function only accepts
 * conditions on the partition key and if no condition is provided then all
 * shards are deleted.
 *
 * We mark shard placements that we couldn't drop as to be deleted later. If a
 * shard satisfies the given conditions, we delete it from shard metadata table
 * even though related shard placements are not deleted.
 */
Datum
master_apply_delete_command(PG_FUNCTION_ARGS)
{
	text *queryText = PG_GETARG_TEXT_P(0);
	char *queryString = text_to_cstring(queryText);
	char *relationName = NULL;
	char *schemaName = NULL;
	Oid relationId = InvalidOid;
	List *shardIntervalList = NIL;
	List *deletableShardIntervalList = NIL;
	List *queryTreeList = NIL;
	Query *deleteQuery = NULL;
	Node *whereClause = NULL;
	Node *deleteCriteria = NULL;
	Node *queryTreeNode = NULL;
	DeleteStmt *deleteStatement = NULL;
	int droppedShardCount = 0;
	LOCKTAG lockTag;
	bool sessionLock = false;
	bool dontWait = false;
	char partitionMethod = 0;
	bool failOK = false;
	bool isTopLevel = true;

	PreventTransactionChain(isTopLevel, "master_apply_delete_command");

	queryTreeNode = ParseTreeNode(queryString);
	if (!IsA(queryTreeNode, DeleteStmt))
	{
		ereport(ERROR, (errmsg("query \"%s\" is not a delete statement",
							   queryString)));
	}

	deleteStatement = (DeleteStmt *) queryTreeNode;

	schemaName = deleteStatement->relation->schemaname;
	relationName = deleteStatement->relation->relname;
	relationId = RangeVarGetRelid(deleteStatement->relation, NoLock, failOK);

	CheckDistributedTable(relationId);
	EnsureTablePermissions(relationId, ACL_DELETE);

	queryTreeList = pg_analyze_and_rewrite(queryTreeNode, queryString, NULL, 0);
	deleteQuery = (Query *) linitial(queryTreeList);
	CheckTableCount(deleteQuery);

	/* get where clause and flatten it */
	whereClause = (Node *) deleteQuery->jointree->quals;
	deleteCriteria = eval_const_expressions(NULL, whereClause);

	partitionMethod = PartitionMethod(relationId);
	if ((partitionMethod == DISTRIBUTE_BY_HASH) && (deleteCriteria != NULL))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from distributed table"),
						errdetail("Delete statements on hash-partitioned tables "
								  "with where clause is not supported")));
	}

	CheckDeleteCriteria(deleteCriteria);
	CheckPartitionColumn(relationId, deleteCriteria);

	/* acquire lock */
	SET_LOCKTAG_ADVISORY(lockTag, MyDatabaseId, relationId, 0, 0);
	LockAcquire(&lockTag, ExclusiveLock, sessionLock, dontWait);

	shardIntervalList = LoadShardIntervalList(relationId);

	/* drop all shards if where clause is not present */
	if (deleteCriteria == NULL)
	{
		deletableShardIntervalList = shardIntervalList;
		ereport(DEBUG2, (errmsg("dropping all shards for \"%s\"", relationName)));
	}
	else
	{
		deletableShardIntervalList = ShardsMatchingDeleteCriteria(relationId,
																  shardIntervalList,
																  deleteCriteria);
	}

	droppedShardCount = DropShards(relationId, schemaName, relationName,
								   deletableShardIntervalList);

	PG_RETURN_INT32(droppedShardCount);
}


/*
 * master_drop_all_shards attempts to drop all shards for a given relation.
 * Unlike master_apply_delete_command, this function can be called even
 * if the table has already been dropped.
 */
Datum
master_drop_all_shards(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *relationNameText = PG_GETARG_TEXT_P(2);

	char *schemaName = NULL;
	char *relationName = NULL;
	bool isTopLevel = true;
	List *shardIntervalList = NIL;
	int droppedShardCount = 0;

	PreventTransactionChain(isTopLevel, "DROP distributed table");

	relationName = get_rel_name(relationId);

	if (relationName != NULL)
	{
		/* ensure proper values are used if the table exists */
		Oid schemaId = get_rel_namespace(relationId);
		schemaName = get_namespace_name(schemaId);

		/*
		 * Only allow the owner to drop all shards, this is more akin to DDL
		 * than DELETE.
		 */
		EnsureTableOwner(relationId);
	}
	else
	{
		/* table has been dropped, rely on user-supplied values */
		schemaName = text_to_cstring(schemaNameText);
		relationName = text_to_cstring(relationNameText);

		/*
		 * Verify that this only is run as superuser - that's how it's used in
		 * our drop event trigger, and we can't verify permissions for an
		 * already dropped relation.
		 */
		if (!superuser())
		{
			ereport(ERROR, (errmsg("cannot drop all shards of a dropped table as "
								   "non-superuser")));
		}
	}

	shardIntervalList = LoadShardIntervalList(relationId);
	droppedShardCount = DropShards(relationId, schemaName, relationName,
								   shardIntervalList);

	PG_RETURN_INT32(droppedShardCount);
}


/*
 * DropShards drops all given shards in a relation. The id, name and schema
 * for the relation are explicitly provided, since this function may be
 * called when the table is already dropped.
 *
 * We mark shard placements that we couldn't drop as to be deleted later, but
 * we do delete the shard metadadata.
 */
static int
DropShards(Oid relationId, char *schemaName, char *relationName,
		   List *deletableShardIntervalList)
{
	ListCell *shardIntervalCell = NULL;
	int droppedShardCount = 0;

	foreach(shardIntervalCell, deletableShardIntervalList)
	{
		List *shardPlacementList = NIL;
		List *droppedPlacementList = NIL;
		List *lingeringPlacementList = NIL;
		ListCell *shardPlacementCell = NULL;
		ListCell *droppedPlacementCell = NULL;
		ListCell *lingeringPlacementCell = NULL;
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		char *shardAlias = NULL;
		char *quotedShardName = NULL;
		StringInfo shardName = makeStringInfo();

		Assert(shardInterval->relationId == relationId);

		/* if shard doesn't have an alias, extend regular table name */
		shardAlias = LoadShardAlias(relationId, shardId);
		if (shardAlias == NULL)
		{
			appendStringInfoString(shardName, relationName);
			AppendShardIdToStringInfo(shardName, shardId);
		}
		else
		{
			appendStringInfoString(shardName, shardAlias);
		}

		quotedShardName = quote_qualified_identifier(schemaName, shardName->data);

		shardPlacementList = ShardPlacementList(shardId);
		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *shardPlacement =
				(ShardPlacement *) lfirst(shardPlacementCell);
			char *workerName = shardPlacement->nodeName;
			uint32 workerPort = shardPlacement->nodePort;
			bool dropSuccessful = false;
			StringInfo workerDropQuery = makeStringInfo();

			char storageType = shardInterval->storageType;
			if (storageType == SHARD_STORAGE_TABLE)
			{
				appendStringInfo(workerDropQuery, DROP_REGULAR_TABLE_COMMAND,
								 quotedShardName);
			}
			else if (storageType == SHARD_STORAGE_COLUMNAR ||
					 storageType == SHARD_STORAGE_FOREIGN)
			{
				appendStringInfo(workerDropQuery, DROP_FOREIGN_TABLE_COMMAND,
								 quotedShardName);
			}

			dropSuccessful = ExecuteRemoteCommand(workerName, workerPort,
												  workerDropQuery);
			if (dropSuccessful)
			{
				droppedPlacementList = lappend(droppedPlacementList, shardPlacement);
			}
			else
			{
				lingeringPlacementList = lappend(lingeringPlacementList, shardPlacement);
			}
		}

		/* make sure we don't process cancel signals */
		HOLD_INTERRUPTS();

		foreach(droppedPlacementCell, droppedPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(droppedPlacementCell);
			char *workerName = placement->nodeName;
			uint32 workerPort = placement->nodePort;

			DeleteShardPlacementRow(shardId, workerName, workerPort);
		}

		/* mark shard placements that we couldn't drop as to be deleted */
		foreach(lingeringPlacementCell, lingeringPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(lingeringPlacementCell);
			char *workerName = placement->nodeName;
			uint32 workerPort = placement->nodePort;
			uint64 oldShardLength = placement->shardLength;

			DeleteShardPlacementRow(shardId, workerName, workerPort);
			InsertShardPlacementRow(shardId, FILE_TO_DELETE, oldShardLength,
									workerName, workerPort);

			ereport(WARNING, (errmsg("could not delete shard \"%s\" on node \"%s:%u\"",
									 shardName->data, workerName, workerPort),
							  errdetail("Marking this shard placement for deletion")));
		}

		DeleteShardRow(shardId);

		if (QueryCancelPending)
		{
			ereport(WARNING, (errmsg("cancel requests are ignored during shard "
									 "deletion")));
			QueryCancelPending = false;
		}

		RESUME_INTERRUPTS();
	}

	droppedShardCount = list_length(deletableShardIntervalList);

	return droppedShardCount;
}


/* Checks that delete is only on one table. */
static void
CheckTableCount(Query *deleteQuery)
{
	int rangeTableCount = list_length(deleteQuery->rtable);
	if (rangeTableCount > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from distributed table"),
						errdetail("Delete on multiple tables is not supported")));
	}
}


/* Checks that delete criteria only consists of simple operator expressions. */
static void
CheckDeleteCriteria(Node *deleteCriteria)
{
	bool simpleOpExpression = true;

	if (deleteCriteria == NULL)
	{
		return;
	}

	if (is_opclause(deleteCriteria))
	{
		simpleOpExpression = SimpleOpExpression((Expr *) deleteCriteria);
	}
	else if (IsA(deleteCriteria, BoolExpr))
	{
		ListCell *opExpressionCell = NULL;
		BoolExpr *deleteCriteriaExpression = (BoolExpr *) deleteCriteria;
		List *opExpressionList = deleteCriteriaExpression->args;

		foreach(opExpressionCell, opExpressionList)
		{
			Expr *opExpression = (Expr *) lfirst(opExpressionCell);
			if (!SimpleOpExpression(opExpression))
			{
				simpleOpExpression = false;
				break;
			}
		}
	}
	else
	{
		simpleOpExpression = false;
	}

	if (!simpleOpExpression)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from distributed table"),
						errdetail("Delete query has a complex operator expression")));
	}
}


/*
 * CheckPartitionColumn checks that the given where clause is based only on the
 * partition key of the given relation id.
 */
static void
CheckPartitionColumn(Oid relationId, Node *whereClause)
{
	Var *partitionColumn = PartitionKey(relationId);
	ListCell *columnCell = NULL;

	List *columnList = pull_var_clause_default(whereClause);
	foreach(columnCell, columnList)
	{
		Var *var = (Var *) lfirst(columnCell);
		if (var->varattno != partitionColumn->varattno)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot delete from distributed table"),
							errdetail("Where clause includes a column other than "
									  "partition column")));
		}
	}
}


/*
 * ShardsMatchingDeleteCriteria selects shards to be deleted from the shard
 * interval list based on the delete criteria, and returns selected shards in
 * another list. We add a shard to the list if and only if all rows in the shard
 * satisfy the delete criteria. Note that this function does not expect
 * deleteCriteria to be NULL.
 */
static List *
ShardsMatchingDeleteCriteria(Oid relationId, List *shardIntervalList,
							 Node *deleteCriteria)
{
	List *dropShardIntervalList = NIL;
	List *deleteCriteriaList = NIL;
	ListCell *shardIntervalCell = NULL;

	/* build the base expression for constraint */
	Index rangeTableIndex = 1;
	Var *partitionColumn = PartitionColumn(relationId, rangeTableIndex);
	Node *baseConstraint = BuildBaseConstraint(partitionColumn);

	Assert(deleteCriteria != NULL);
	deleteCriteriaList = list_make1(deleteCriteria);

	/* walk over shard list and check if shards can be dropped */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		if (shardInterval->minValueExists && shardInterval->maxValueExists)
		{
			List *restrictInfoList = NIL;
			bool dropShard = false;
			BoolExpr *andExpr = NULL;
			Expr *lessThanExpr = NULL;
			Expr *greaterThanExpr = NULL;
			RestrictInfo *lessThanRestrictInfo = NULL;
			RestrictInfo *greaterThanRestrictInfo = NULL;

			/* set the min/max values in the base constraint */
			UpdateConstraint(baseConstraint, shardInterval);

			andExpr = (BoolExpr *) baseConstraint;
			lessThanExpr = (Expr *) linitial(andExpr->args);
			greaterThanExpr = (Expr *) lsecond(andExpr->args);

			lessThanRestrictInfo = make_simple_restrictinfo(lessThanExpr);
			greaterThanRestrictInfo = make_simple_restrictinfo(greaterThanExpr);

			restrictInfoList = lappend(restrictInfoList, lessThanRestrictInfo);
			restrictInfoList = lappend(restrictInfoList, greaterThanRestrictInfo);

			dropShard = predicate_implied_by(deleteCriteriaList, restrictInfoList);
			if (dropShard)
			{
				dropShardIntervalList = lappend(dropShardIntervalList, shardInterval);
				ereport(DEBUG2, (errmsg("delete criteria includes shardId "
										UINT64_FORMAT, shardInterval->shardId)));
			}
		}
	}

	return dropShardIntervalList;
}


/*
 * ExecuteRemoteCommand executes the given SQL command. This command could be an
 * Insert, Update, or Delete statement, or a utility command that returns
 * nothing. If query is successfuly executed, the function returns true.
 * Otherwise, it returns false.
 */
static bool
ExecuteRemoteCommand(const char *nodeName, uint32 nodePort, StringInfo queryString)
{
	char *nodeDatabase = get_database_name(MyDatabaseId);
	int32 connectionId = -1;
	QueryStatus queryStatus = CLIENT_INVALID_QUERY;
	bool querySent = false;
	bool queryReady = false;
	bool queryDone = false;

	connectionId = MultiClientConnect(nodeName, nodePort, nodeDatabase, NULL);
	if (connectionId == INVALID_CONNECTION_ID)
	{
		return false;
	}

	querySent = MultiClientSendQuery(connectionId, queryString->data);
	if (!querySent)
	{
		MultiClientDisconnect(connectionId);
		return false;
	}

	while (!queryReady)
	{
		ResultStatus resultStatus = MultiClientResultStatus(connectionId);
		if (resultStatus == CLIENT_RESULT_READY)
		{
			queryReady = true;
		}
		else if (resultStatus == CLIENT_RESULT_BUSY)
		{
			long sleepIntervalPerCycle = RemoteTaskCheckInterval * 1000L;
			pg_usleep(sleepIntervalPerCycle);
		}
		else
		{
			MultiClientDisconnect(connectionId);
			return false;
		}
	}

	queryStatus = MultiClientQueryStatus(connectionId);
	if (queryStatus == CLIENT_QUERY_DONE)
	{
		queryDone = true;
	}

	MultiClientDisconnect(connectionId);
	return queryDone;
}
