/*-------------------------------------------------------------------------
 *
 * delete_protocol.c
 *
 * Routine for deleting shards in the distributed cluster. This function takes
 * in a delete command and deletes a shard if and only if all rows in the shard
 * satisfy the conditions in the delete command.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include <stddef.h>

#include "postgres.h"

#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"

#include "pg_version_constants.h"

#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/placement_connection.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_cleaner.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"


/* Local functions forward declarations */
static int DropShards(Oid relationId, char *schemaName, char *relationName,
					  List *deletableShardIntervalList, bool dropShardsMetadataOnly);
static List * DropTaskList(Oid relationId, char *schemaName, char *relationName,
						   List *deletableShardIntervalList);
static void ExecuteDropShardPlacementCommandRemotely(ShardPlacement *shardPlacement,
													 const char *shardRelationName,
													 const char *dropShardPlacementCommand);
static char * CreateDropShardPlacementCommand(const char *schemaName,
											  const char *shardRelationName,
											  char storageType);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_apply_delete_command);
PG_FUNCTION_INFO_V1(citus_drop_all_shards);
PG_FUNCTION_INFO_V1(master_drop_all_shards);
PG_FUNCTION_INFO_V1(master_drop_sequences);


/*
 * master_apply_delete_command is a deprecated function for dropping shards
 * in an append-distributed tables.
 */
Datum
master_apply_delete_command(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("master_apply_delete_command has been deprecated")));
}


/*
 * citus_drop_all_shards attempts to drop all shards for a given relation.
 * This function can be called even if the table has already been dropped.
 * In that case, the schema name and relation name arguments are used to
 * determine that table name. Otherwise, the relation ID is used and the
 * other arguments are ignored.
 */
Datum
citus_drop_all_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *relationNameText = PG_GETARG_TEXT_P(2);
	bool dropShardsMetadataOnly = PG_GETARG_BOOL(3);

	char *schemaName = text_to_cstring(schemaNameText);
	char *relationName = text_to_cstring(relationNameText);

	/*
	 * The SQL_DROP trigger calls this function even for tables that are
	 * not distributed. In that case, silently ignore and return -1.
	 */
	if (!IsCitusTableViaCatalog(relationId) || !EnableDDLPropagation)
	{
		PG_RETURN_INT32(-1);
	}

	EnsureCoordinator();
	CheckTableSchemaNameForDrop(relationId, &schemaName, &relationName);

	/*
	 * citus_drop_all_shards is typically called from the DROP TABLE trigger,
	 * but could be called by a user directly. Make sure we have an
	 * AccessExclusiveLock to prevent any other commands from running on this table
	 * concurrently.
	 */
	LockRelationOid(relationId, AccessExclusiveLock);

	List *shardIntervalList = LoadUnsortedShardIntervalListViaCatalog(relationId);
	int droppedShardCount = DropShards(relationId, schemaName, relationName,
									   shardIntervalList, dropShardsMetadataOnly);

	PG_RETURN_INT32(droppedShardCount);
}


/*
 * master_drop_all_shards is a wrapper function for old UDF name.
 */
Datum
master_drop_all_shards(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *relationNameText = PG_GETARG_TEXT_P(2);
	bool dropShardsMetadataOnly = false;

	LOCAL_FCINFO(local_fcinfo, 4);

	InitFunctionCallInfoData(*local_fcinfo, NULL, 4, InvalidOid, NULL, NULL);

	local_fcinfo->args[0].value = ObjectIdGetDatum(relationId);
	local_fcinfo->args[0].isnull = false;
	local_fcinfo->args[1].value = PointerGetDatum(schemaNameText);
	local_fcinfo->args[1].isnull = false;
	local_fcinfo->args[2].value = PointerGetDatum(relationNameText);
	local_fcinfo->args[2].isnull = false;
	local_fcinfo->args[3].value = BoolGetDatum(dropShardsMetadataOnly);
	local_fcinfo->args[3].isnull = false;

	return citus_drop_all_shards(local_fcinfo);
}


/*
 * master_drop_sequences was previously used to drop sequences on workers
 * when using metadata syncing.
 *
 * It may still be called when dropping objects during CREATE EXTENSION,
 * hence the function remains in place.
 */
Datum
master_drop_sequences(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}


/*
 * CheckTableSchemaNameForDrop errors out if the current user does not
 * have permission to un-distribute the given relation, taking into
 * account that it may be called from the drop trigger. If the table exists,
 * the function rewrites the given table and schema name.
 */
void
CheckTableSchemaNameForDrop(Oid relationId, char **schemaName, char **tableName)
{
	char *tempTableName = get_rel_name(relationId);

	if (tempTableName != NULL)
	{
		/* ensure proper values are used if the table exists */
		Oid schemaId = get_rel_namespace(relationId);
		(*schemaName) = get_namespace_name(schemaId);
		(*tableName) = tempTableName;

		EnsureTableOwner(relationId);
	}
}


/*
 * DropShards drops all given shards in a relation. The id, name and schema
 * for the relation are explicitly provided, since this function may be
 * called when the table is already dropped.
 *
 * We mark shard placements that we couldn't drop as to be deleted later, but
 * we do delete the shard metadadata.
 *
 * If dropShardsMetadataOnly is true, then we don't send remote commands to drop the shards:
 * we only remove pg_dist_placement and pg_dist_shard rows.
 */
static int
DropShards(Oid relationId, char *schemaName, char *relationName,
		   List *deletableShardIntervalList, bool dropShardsMetadataOnly)
{
	Assert(OidIsValid(relationId));
	Assert(schemaName != NULL);
	Assert(relationName != NULL);

	UseCoordinatedTransaction();

	/*
	 * We will use below variable across this function to decide if we can
	 * use local execution
	 */
	int32 localGroupId = GetLocalGroupId();

	/* DROP table commands are currently only supported from the coordinator */
	Assert(localGroupId == COORDINATOR_GROUP_ID);

	Use2PCForCoordinatedTransaction();

	List *dropTaskList = DropTaskList(relationId, schemaName, relationName,
									  deletableShardIntervalList);
	bool shouldExecuteTasksLocally = ShouldExecuteTasksLocally(dropTaskList);

	Task *task = NULL;
	foreach_declared_ptr(task, dropTaskList)
	{
		uint64 shardId = task->anchorShardId;

		ShardPlacement *shardPlacement = NULL;
		foreach_declared_ptr(shardPlacement, task->taskPlacementList)
		{
			uint64 shardPlacementId = shardPlacement->placementId;
			int32 shardPlacementGroupId = shardPlacement->groupId;

			bool isLocalShardPlacement = (shardPlacementGroupId == localGroupId);

			/*
			 * If this variable is true, that means the active DROP SCHEMA/DATABASE ... CASCADE
			 * will drop the shard. If we try to drop it over another connection, we will
			 * get into a distributed deadlock. Hence, if this variable is true we should just
			 * delete the shard placement metadata and skip dropping the shard for now.
			 */
			bool skipIfDropSchemaOrDBInProgress = isLocalShardPlacement &&
												  DropSchemaOrDBInProgress() &&
												  localGroupId == COORDINATOR_GROUP_ID;

			/*
			 * We want to send commands to drop shards when both
			 * skipIfDropSchemaOrDBInProgress and dropShardsMetadataOnly are false.
			 */
			bool applyRemoteShardsDrop =
				!skipIfDropSchemaOrDBInProgress && !dropShardsMetadataOnly;

			if (applyRemoteShardsDrop)
			{
				/*
				 * If it is a local placement of a distributed table or a reference table,
				 * then execute the DROP command locally.
				 */
				if (isLocalShardPlacement && shouldExecuteTasksLocally)
				{
					List *singleTaskList = list_make1(task);

					ExecuteLocalUtilityTaskList(singleTaskList);
				}
				else
				{
					/*
					 * Either it was not a local placement or we could not use
					 * local execution even if it was a local placement.
					 * If it is the second case, then it is possibly because in
					 * current transaction, some commands or queries connected
					 * to local group as well.
					 *
					 * Regardless of the node is a remote node or the current node,
					 * try to open a new connection (or use an existing one) to
					 * connect to that node to drop the shard placement over that
					 * remote connection.
					 */
					const char *dropShardPlacementCommand = TaskQueryString(task);
					ExecuteDropShardPlacementCommandRemotely(shardPlacement,
															 relationName,
															 dropShardPlacementCommand);

					if (isLocalShardPlacement)
					{
						SetLocalExecutionStatus(LOCAL_EXECUTION_DISABLED);
					}
				}
			}

			DeleteShardPlacementRow(shardPlacementId);
		}

		/*
		 * Now that we deleted all placements of the shard (or their metadata),
		 * delete the shard metadata as well.
		 */
		DeleteShardRow(shardId);
	}

	int droppedShardCount = list_length(deletableShardIntervalList);

	return droppedShardCount;
}


/*
 * DropTaskList returns a list of tasks to execute a DROP command on shard
 * placements of distributed table. This is handled separately from other
 * DDL commands because we handle it via the DROP trigger, which is called
 * whenever a drop cascades.
 */
static List *
DropTaskList(Oid relationId, char *schemaName, char *relationName,
			 List *deletableShardIntervalList)
{
	/* resulting task list */
	List *taskList = NIL;

	/* enumerate the tasks when putting them to the taskList */
	int taskId = 1;

	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, deletableShardIntervalList)
	{
		Assert(shardInterval->relationId == relationId);

		uint64 shardId = shardInterval->shardId;
		char storageType = shardInterval->storageType;

		char *shardRelationName = pstrdup(relationName);

		/* build shard relation name */
		AppendShardIdToName(&shardRelationName, shardId);

		char *dropShardPlacementCommand =
			CreateDropShardPlacementCommand(schemaName, shardRelationName,
											storageType);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, dropShardPlacementCommand);
		task->dependentTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = ShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * ExecuteDropShardPlacementCommandRemotely executes the given DROP shard command
 * via remote critical connection.
 */
static void
ExecuteDropShardPlacementCommandRemotely(ShardPlacement *shardPlacement,
										 const char *relationName,
										 const char *dropShardPlacementCommand)
{
	Assert(shardPlacement != NULL);
	Assert(relationName != NULL);
	Assert(dropShardPlacementCommand != NULL);

	uint32 connectionFlags = FOR_DDL;
	MultiConnection *connection = GetPlacementConnection(connectionFlags,
														 shardPlacement,
														 NULL);

	/*
	 * This code-path doesn't support optional connections, so we don't expect
	 * NULL connections.
	 */
	Assert(connection != NULL);

	RemoteTransactionBeginIfNecessary(connection);

	if (PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		char *workerName = shardPlacement->nodeName;
		uint32 workerPort = shardPlacement->nodePort;

		/* build shard relation name */
		uint64 shardId = shardPlacement->shardId;
		char *shardRelationName = pstrdup(relationName);

		AppendShardIdToName(&shardRelationName, shardId);

		ereport(WARNING, (errmsg("could not connect to shard \"%s\" on node "
								 "\"%s:%u\"", shardRelationName, workerName,
								 workerPort),
						  errdetail("Marking this shard placement for "
									"deletion")));

		InsertCleanupRecordInCurrentTransaction(CLEANUP_OBJECT_SHARD_PLACEMENT,
												shardRelationName,
												shardPlacement->groupId,
												CLEANUP_DEFERRED_ON_SUCCESS);

		return;
	}

	MarkRemoteTransactionCritical(connection);

	ExecuteCriticalRemoteCommand(connection, dropShardPlacementCommand);
}


/*
 * CreateDropShardPlacementCommand function builds the DROP command to drop
 * the given shard relation by qualifying it with schema name according to
 * shard relation's storage type.
 */
static char *
CreateDropShardPlacementCommand(const char *schemaName, const char *shardRelationName,
								char storageType)
{
	Assert(schemaName != NULL);
	Assert(shardRelationName != NULL);

	StringInfo workerDropQuery = makeStringInfo();

	const char *quotedShardName = quote_qualified_identifier(schemaName,
															 shardRelationName);

	/* build workerDropQuery according to shard storage type */
	if (storageType == SHARD_STORAGE_TABLE)
	{
		appendStringInfo(workerDropQuery, DROP_REGULAR_TABLE_COMMAND,
						 quotedShardName);
	}
	else if (storageType == SHARD_STORAGE_FOREIGN)
	{
		appendStringInfo(workerDropQuery, DROP_FOREIGN_TABLE_COMMAND,
						 quotedShardName);
	}
	else
	{
		/* no other storage type is expected here */
		Assert(false);
	}

	return workerDropQuery->data;
}
