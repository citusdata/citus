/*-------------------------------------------------------------------------
 *
 * stage_protocol.c
 *
 * Routines for staging PostgreSQL table data as shards into the distributed
 * cluster. These user-defined functions are similar to the psql-side \stage
 * command, but also differ from them in that users stage data from tables and
 * not files, and that they can also append to existing shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/tablecmds.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/adaptive_executor.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_planner.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/placement_connection.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"


/* Local functions forward declarations */
static List * RelationShardListForShardCreate(ShardInterval *shardInterval);
static bool WorkerShardStats(ShardPlacement *placement, Oid relationId,
							 const char *shardName, uint64 *shardSize);
static void UpdateTableStatistics(Oid relationId);
static void ReceiveAndUpdateShardsSizes(List *connectionList);
static void UpdateShardSize(uint64 shardId, ShardInterval *shardInterval,
							Oid relationId, List *shardPlacementList,
							uint64 shardSize);
static bool ProcessShardStatisticsRow(PGresult *result, int64 rowIndex, uint64 *shardId,
									  uint64 *shardSize);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_empty_shard);
PG_FUNCTION_INFO_V1(master_append_table_to_shard);
PG_FUNCTION_INFO_V1(citus_update_shard_statistics);
PG_FUNCTION_INFO_V1(master_update_shard_statistics);
PG_FUNCTION_INFO_V1(citus_update_table_statistics);


/*
 * master_create_empty_shard creates an empty shard for the given distributed
 * table. The function first updates metadata on the coordinator node to make
 * this shard visible. Then it creates empty shard on worker node and added
 * shard placement row to metadata table.
 */
Datum
master_create_empty_shard(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *relationNameText = PG_GETARG_TEXT_P(0);
	char *relationName = text_to_cstring(relationNameText);
	uint32 attemptableNodeCount = 0;
	ObjectAddress tableAddress = { 0 };

	uint32 candidateNodeIndex = 0;
	List *candidateNodeList = NIL;
	text *nullMinValue = NULL;
	text *nullMaxValue = NULL;
	char storageType = SHARD_STORAGE_TABLE;

	Oid relationId = ResolveRelationId(relationNameText, false);

	EnsureTablePermissions(relationId, ACL_INSERT);
	CheckDistributedTable(relationId);

	/*
	 * distributed tables might have dependencies on different objects, since we create
	 * shards for a distributed table via multiple sessions these objects will be created
	 * via their own connection and committed immediately so they become visible to all
	 * sessions creating shards.
	 */
	ObjectAddressSet(tableAddress, RelationRelationId, relationId);
	EnsureDependenciesExistOnAllNodes(&tableAddress);
	EnsureReferenceTablesExistOnAllNodes();

	/* don't allow the table to be dropped */
	LockRelationOid(relationId, AccessShareLock);

	/* don't allow concurrent node list changes that require an exclusive lock */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/* set the storage type of foreign tables to 'f' */
	if (IsForeignTable(relationId))
	{
		storageType = SHARD_STORAGE_FOREIGN;
	}

	if (IsCitusTableType(relationId, HASH_DISTRIBUTED))
	{
		ereport(ERROR, (errmsg("relation \"%s\" is a hash partitioned table",
							   relationName),
						errdetail("We currently don't support creating shards "
								  "on hash-partitioned tables")));
	}
	else if (IsCitusTableType(relationId, REFERENCE_TABLE))
	{
		ereport(ERROR, (errmsg("relation \"%s\" is a reference table",
							   relationName),
						errdetail("We currently don't support creating shards "
								  "on reference tables")));
	}
	else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errmsg("relation \"%s\" is a local table",
							   relationName),
						errdetail("We currently don't support creating shards "
								  "on local tables")));
	}

	/* generate new and unique shardId from sequence */
	uint64 shardId = GetNextShardId();

	/* if enough live groups, add an extra candidate node as backup */
	List *workerNodeList = DistributedTablePlacementNodeList(NoLock);

	if (list_length(workerNodeList) > ShardReplicationFactor)
	{
		attemptableNodeCount = ShardReplicationFactor + 1;
	}
	else
	{
		attemptableNodeCount = ShardReplicationFactor;
	}

	/* first retrieve a list of random nodes for shard placements */
	while (candidateNodeIndex < attemptableNodeCount)
	{
		WorkerNode *candidateNode = WorkerGetRoundRobinCandidateNode(workerNodeList,
																	 shardId,
																	 candidateNodeIndex);
		if (candidateNode == NULL)
		{
			ereport(ERROR, (errmsg("could only find %u of %u possible nodes",
								   candidateNodeIndex, attemptableNodeCount)));
		}

		candidateNodeList = lappend(candidateNodeList, candidateNode);
		candidateNodeIndex++;
	}

	InsertShardRow(relationId, shardId, storageType, nullMinValue, nullMaxValue);

	CreateAppendDistributedShardPlacements(relationId, shardId, candidateNodeList,
										   ShardReplicationFactor);

	PG_RETURN_INT64(shardId);
}


/*
 * master_append_table_to_shard is a deprecated function for appending data
 * to a shard in an append-distributed table.
 */
Datum
master_append_table_to_shard(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("master_append_table_to_shard has been deprecated")));
}


/*
 * citus_update_shard_statistics updates metadata (shard size and shard min/max
 * values) of the given shard and returns the updated shard size.
 */
Datum
citus_update_shard_statistics(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);

	uint64 shardSize = UpdateShardStatistics(shardId);

	PG_RETURN_INT64(shardSize);
}


/*
 * citus_update_table_statistics updates metadata (shard size and shard min/max
 * values) of the shards of the given table
 */
Datum
citus_update_table_statistics(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid distributedTableId = PG_GETARG_OID(0);

	/*
	 * Ensure the table still exists by trying to acquire a lock on it
	 * If function returns NULL, it means the table doesn't exist
	 * hence we should skip
	 */
	Relation relation = try_relation_open(distributedTableId, AccessShareLock);

	if (relation != NULL)
	{
		UpdateTableStatistics(distributedTableId);

		/*
		 * We release the lock here since citus_update_table_statistics
		 * is usually used in the following command:
		 * SELECT citus_update_table_statistics(logicalrelid) FROM pg_dist_partition;
		 * In this way we avoid holding the locks on distributed tables for a long time:
		 * If we close the relation with NoLock, the locks on the distributed tables will
		 * be held until the above command is finished (all distributed tables are updated).
		 */
		relation_close(relation, AccessShareLock);
	}
	else
	{
		ereport(NOTICE, (errmsg("relation with OID %u does not exist, skipping",
								distributedTableId)));
	}

	PG_RETURN_VOID();
}


/*
 * master_update_shard_statistics is a wrapper function for old UDF name.
 */
Datum
master_update_shard_statistics(PG_FUNCTION_ARGS)
{
	return citus_update_shard_statistics(fcinfo);
}


/*
 * CheckDistributedTable checks if the given relationId corresponds to a
 * distributed table. If it does not, the function errors out.
 */
void
CheckDistributedTable(Oid relationId)
{
	char *relationName = get_rel_name(relationId);

	/* check that the relationId belongs to a table */
	EnsureRelationKindSupported(relationId);

	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errmsg("relation \"%s\" is not a distributed table",
							   relationName)));
	}
}


/*
 * CreateAppendDistributedShardPlacements creates shards for append distributed
 * tables on worker nodes. After successfully creating shard on the worker,
 * shard placement rows are added to the metadata.
 */
void
CreateAppendDistributedShardPlacements(Oid relationId, int64 shardId,
									   List *workerNodeList, int replicationFactor)
{
	int attemptCount = replicationFactor;
	int workerNodeCount = list_length(workerNodeList);
	int placementsCreated = 0;
	List *foreignConstraintCommandList =
		GetReferencingForeignConstaintCommands(relationId);
	IncludeSequenceDefaults includeSequenceDefaults = NO_SEQUENCE_DEFAULTS;
	bool creatingShellTableOnRemoteNode = false;
	List *ddlCommandList = GetFullTableCreationCommands(relationId,
														includeSequenceDefaults,
														creatingShellTableOnRemoteNode);
	uint32 connectionFlag = FOR_DDL;
	char *relationOwner = TableOwner(relationId);

	/* if we have enough nodes, add an extra placement attempt for backup */
	if (workerNodeCount > replicationFactor)
	{
		attemptCount++;
	}

	for (int attemptNumber = 0; attemptNumber < attemptCount; attemptNumber++)
	{
		int workerNodeIndex = attemptNumber % workerNodeCount;
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, workerNodeIndex);

		if (NodeIsCoordinator(workerNode))
		{
			ereport(NOTICE, (errmsg(
								 "Creating placements for the append partitioned tables on the coordinator is not supported, skipping coordinator ...")));
			continue;
		}

		uint32 nodeGroupId = workerNode->groupId;
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		int shardIndex = -1; /* not used in this code path */
		const ShardState shardState = SHARD_STATE_ACTIVE;
		const uint64 shardSize = 0;
		MultiConnection *connection =
			GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort,
										  relationOwner, NULL);

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			ereport(WARNING, (errmsg("could not connect to node \"%s:%u\"", nodeName,
									 nodePort)));

			continue;
		}

		List *commandList = WorkerCreateShardCommandList(relationId, shardIndex, shardId,
														 ddlCommandList,
														 foreignConstraintCommandList);

		ExecuteCriticalRemoteCommandList(connection, commandList);

		InsertShardPlacementRow(shardId, INVALID_PLACEMENT_ID, shardState, shardSize,
								nodeGroupId);
		placementsCreated++;

		if (placementsCreated >= replicationFactor)
		{
			break;
		}
	}

	/* check if we created enough shard replicas */
	if (placementsCreated < replicationFactor)
	{
		ereport(ERROR, (errmsg("could only create %u of %u of required shard replicas",
							   placementsCreated, replicationFactor)));
	}
}


/*
 * InsertShardPlacementRows inserts shard placements to the metadata table on
 * the coordinator node. Then, returns the list of added shard placements.
 */
List *
InsertShardPlacementRows(Oid relationId, int64 shardId, List *workerNodeList,
						 int workerStartIndex, int replicationFactor)
{
	int workerNodeCount = list_length(workerNodeList);
	int placementsInserted = 0;
	List *insertedShardPlacements = NIL;

	for (int attemptNumber = 0; attemptNumber < replicationFactor; attemptNumber++)
	{
		int workerNodeIndex = (workerStartIndex + attemptNumber) % workerNodeCount;
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, workerNodeIndex);
		uint32 nodeGroupId = workerNode->groupId;
		const ShardState shardState = SHARD_STATE_ACTIVE;
		const uint64 shardSize = 0;

		uint64 shardPlacementId = InsertShardPlacementRow(shardId, INVALID_PLACEMENT_ID,
														  shardState, shardSize,
														  nodeGroupId);
		ShardPlacement *shardPlacement = LoadShardPlacement(shardId, shardPlacementId);
		insertedShardPlacements = lappend(insertedShardPlacements, shardPlacement);

		placementsInserted++;
		if (placementsInserted >= replicationFactor)
		{
			break;
		}
	}

	return insertedShardPlacements;
}


/*
 * CreateShardsOnWorkers creates shards on worker nodes given the shard placements
 * as a parameter The function creates the shards via the executor. This means
 * that it can adopt the number of connections required to create the shards.
 */
void
CreateShardsOnWorkers(Oid distributedRelationId, List *shardPlacements,
					  bool useExclusiveConnection, bool colocatedShard)
{
	IncludeSequenceDefaults includeSequenceDefaults = NO_SEQUENCE_DEFAULTS;
	bool creatingShellTableOnRemoteNode = false;
	List *ddlCommandList = GetFullTableCreationCommands(distributedRelationId,
														includeSequenceDefaults,
														creatingShellTableOnRemoteNode);
	List *foreignConstraintCommandList =
		GetReferencingForeignConstaintCommands(distributedRelationId);

	int taskId = 1;
	List *taskList = NIL;
	int poolSize = 1;

	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, shardPlacements)
	{
		uint64 shardId = shardPlacement->shardId;
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		int shardIndex = -1;
		List *relationShardList = RelationShardListForShardCreate(shardInterval);

		if (colocatedShard)
		{
			shardIndex = ShardIndex(shardInterval);
		}

		List *commandList = WorkerCreateShardCommandList(distributedRelationId,
														 shardIndex,
														 shardId, ddlCommandList,
														 foreignConstraintCommandList);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryStringList(task, commandList);

		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NIL;
		task->anchorShardId = shardId;
		task->relationShardList = relationShardList;
		task->taskPlacementList = list_make1(shardPlacement);

		taskList = lappend(taskList, task);
	}

	if (useExclusiveConnection)
	{
		/*
		 * When the table has local data, we force max parallelization so data
		 * copy is done efficiently. We also prefer to use max parallelization
		 * when we're inside a transaction block because the user might execute
		 * compute heavy commands (e.g., load data or create index) later in the
		 * transaction block.
		 */
		SetLocalForceMaxQueryParallelization();

		/*
		 * TODO: After we fix adaptive executor to record parallel access for
		 * ForceMaxQueryParallelization, we should remove this. This is just
		 * to force adaptive executor to record parallel access to relations.
		 *
		 * Adaptive executor uses poolSize to decide if it should record parallel
		 * access to relations or not, and it ignores ForceMaxQueryParallelization
		 * because of some complications in TRUNCATE.
		 */
		poolSize = MaxAdaptiveExecutorPoolSize;
	}
	bool localExecutionSupported = true;
	ExecuteUtilityTaskListExtended(taskList, poolSize, localExecutionSupported);
}


/*
 * RelationShardListForShardCreate gets a shard interval and returns the placement
 * accesses that would happen when a placement of the shard interval is created.
 */
static List *
RelationShardListForShardCreate(ShardInterval *shardInterval)
{
	Oid relationId = shardInterval->relationId;
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	List *referencedRelationList = cacheEntry->referencedRelationsViaForeignKey;
	List *referencingRelationList = cacheEntry->referencingRelationsViaForeignKey;
	int shardIndex = -1;

	/* list_concat_*() modifies the first arg, so make a copy first */
	List *allForeignKeyRelations = list_copy(referencedRelationList);
	allForeignKeyRelations = list_concat_unique_oid(allForeignKeyRelations,
													referencingRelationList);

	/* record the placement access of the shard itself */
	RelationShard *relationShard = CitusMakeNode(RelationShard);
	relationShard->relationId = relationId;
	relationShard->shardId = shardInterval->shardId;
	List *relationShardList = list_make1(relationShard);

	if (IsCitusTableTypeCacheEntry(cacheEntry, HASH_DISTRIBUTED) &&
		cacheEntry->colocationId != INVALID_COLOCATION_ID)
	{
		shardIndex = ShardIndex(shardInterval);
	}


	/* all foregin key constraint relations */
	Oid fkeyRelationid = InvalidOid;
	foreach_oid(fkeyRelationid, allForeignKeyRelations)
	{
		uint64 fkeyShardId = INVALID_SHARD_ID;

		if (!IsCitusTable(fkeyRelationid))
		{
			/* we're not interested in local tables */
			continue;
		}

		if (IsCitusTableType(fkeyRelationid, REFERENCE_TABLE))
		{
			fkeyShardId = GetFirstShardId(fkeyRelationid);
		}
		else if (IsCitusTableTypeCacheEntry(cacheEntry, HASH_DISTRIBUTED) &&
				 IsCitusTableType(fkeyRelationid, HASH_DISTRIBUTED))
		{
			/* hash distributed tables should be colocated to have fkey */
			Assert(TableColocationId(fkeyRelationid) == cacheEntry->colocationId);

			fkeyShardId =
				ColocatedShardIdInRelation(fkeyRelationid, shardIndex);
		}
		else
		{
			/*
			 * We currently do not support foreign keys from/to local tables or
			 * non-colocated tables when creating shards. Also note that shard
			 * creation via shard moves doesn't happen in a transaction block,
			 * so not relevant here.
			 */
			continue;
		}

		RelationShard *fkeyRelationShard = CitusMakeNode(RelationShard);
		fkeyRelationShard->relationId = fkeyRelationid;
		fkeyRelationShard->shardId = fkeyShardId;

		relationShardList = lappend(relationShardList, fkeyRelationShard);
	}


	/* if partitioned table, make sure to record the parent table */
	if (PartitionTable(relationId))
	{
		RelationShard *parentRelationShard = CitusMakeNode(RelationShard);

		/* partitioned tables are always co-located */
		Assert(shardIndex != -1);

		parentRelationShard->relationId = PartitionParentOid(relationId);
		parentRelationShard->shardId =
			ColocatedShardIdInRelation(parentRelationShard->relationId, shardIndex);

		relationShardList = lappend(relationShardList, parentRelationShard);
	}

	return relationShardList;
}


/*
 * WorkerCreateShardCommandList returns a list of DDL commands for the given
 * shardId to create the shard on the worker node.
 */
List *
WorkerCreateShardCommandList(Oid relationId, int shardIndex, uint64 shardId,
							 List *ddlCommandList,
							 List *foreignConstraintCommandList)
{
	List *commandList = NIL;
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);

	TableDDLCommand *ddlCommand = NULL;
	foreach_ptr(ddlCommand, ddlCommandList)
	{
		Assert(CitusIsA(ddlCommand, TableDDLCommand));
		char *applyDDLCommand = GetShardedTableDDLCommand(ddlCommand, shardId,
														  schemaName);
		commandList = lappend(commandList, applyDDLCommand);
	}

	const char *command = NULL;
	foreach_ptr(command, foreignConstraintCommandList)
	{
		char *escapedCommand = quote_literal_cstr(command);

		uint64 referencedShardId = INVALID_SHARD_ID;

		StringInfo applyForeignConstraintCommand = makeStringInfo();

		/* we need to parse the foreign constraint command to get referencing table id */
		Oid referencedRelationId = ForeignConstraintGetReferencedTableId(command);
		if (referencedRelationId == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cannot create foreign key constraint"),
							errdetail("Referenced relation cannot be found.")));
		}

		Oid referencedSchemaId = get_rel_namespace(referencedRelationId);
		char *referencedSchemaName = get_namespace_name(referencedSchemaId);
		char *escapedReferencedSchemaName = quote_literal_cstr(referencedSchemaName);

		/*
		 * In case of self referencing shards, relation itself might not be distributed
		 * already. Therefore we cannot use ColocatedShardIdInRelation which assumes
		 * given relation is distributed. Besides, since we know foreign key references
		 * itself, referencedShardId is actual shardId anyway. Also, if the referenced
		 * relation is a reference table, we cannot use ColocatedShardIdInRelation since
		 * reference tables only have one shard. Instead, we fetch the one and only shard
		 * from shardlist and use it.
		 */
		if (relationId == referencedRelationId)
		{
			referencedShardId = shardId;
		}
		else if (IsCitusTableType(referencedRelationId, REFERENCE_TABLE))
		{
			referencedShardId = GetFirstShardId(referencedRelationId);
		}
		else
		{
			referencedShardId = ColocatedShardIdInRelation(referencedRelationId,
														   shardIndex);
		}

		appendStringInfo(applyForeignConstraintCommand,
						 WORKER_APPLY_INTER_SHARD_DDL_COMMAND, shardId, escapedSchemaName,
						 referencedShardId, escapedReferencedSchemaName, escapedCommand);

		commandList = lappend(commandList, applyForeignConstraintCommand->data);
	}

	/*
	 * If the shard is created for a partition, send the command to create the
	 * partitioning hierarcy on the shard.
	 */
	if (PartitionTable(relationId))
	{
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		char *attachPartitionCommand = GenerateAttachShardPartitionCommand(shardInterval);

		commandList = lappend(commandList, attachPartitionCommand);
	}

	return commandList;
}


/*
 * UpdateShardStatistics updates metadata (shard size and shard min/max values)
 * of the given shard and returns the updated shard size.
 */
uint64
UpdateShardStatistics(int64 shardId)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid relationId = shardInterval->relationId;
	bool statsOK = false;
	uint64 shardSize = 0;

	/* Build shard qualified name. */
	char *shardName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);

	AppendShardIdToName(&shardName, shardId);

	char *shardQualifiedName = quote_qualified_identifier(schemaName, shardName);

	List *shardPlacementList = ActiveShardPlacementList(shardId);

	/* get shard's statistics from a shard placement */
	ShardPlacement *placement = NULL;
	foreach_ptr(placement, shardPlacementList)
	{
		statsOK = WorkerShardStats(placement, relationId, shardQualifiedName,
								   &shardSize);
		if (statsOK)
		{
			break;
		}
	}

	/*
	 * If for some reason we appended data to a shard, but failed to retrieve
	 * statistics we just WARN here to avoid losing shard-state updates. Note
	 * that this means we will return 0 as the shard fill-factor, and this shard
	 * also won't be pruned as the statistics will be empty. If the failure was
	 * transient, a subsequent append call will fetch the correct statistics.
	 */
	if (!statsOK)
	{
		ereport(WARNING, (errmsg("could not get statistics for shard %s",
								 shardQualifiedName),
						  errdetail("Setting shard statistics to NULL")));
	}

	UpdateShardSize(shardId, shardInterval, relationId, shardPlacementList,
					shardSize);

	return shardSize;
}


/*
 * UpdateTableStatistics updates metadata (shard size and shard min/max values)
 * of the shards of the given table. Follows a similar logic to citus_shard_sizes function.
 */
static void
UpdateTableStatistics(Oid relationId)
{
	List *citusTableIds = NIL;
	citusTableIds = lappend_oid(citusTableIds, relationId);

	/* we want to use a distributed transaction here to detect distributed deadlocks */
	bool useDistributedTransaction = true;

	List *connectionList =
		SendShardStatisticsQueriesInParallel(citusTableIds, useDistributedTransaction);

	ReceiveAndUpdateShardsSizes(connectionList);
}


/*
 * ReceiveAndUpdateShardsSizes receives shard id and size
 * results from the given connection list, and updates
 * respective entries in pg_dist_placement.
 */
static void
ReceiveAndUpdateShardsSizes(List *connectionList)
{
	/*
	 * From the connection list, we will not get all the shards, but
	 * all the placements. We use a hash table to remember already visited shard ids
	 * since we update all the different placements of a shard id at once.
	 */
	HTAB *alreadyVisitedShardPlacements = CreateOidVisitedHashSet();

	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			continue;
		}

		bool raiseInterrupts = true;
		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			continue;
		}

		int64 rowCount = PQntuples(result);
		int64 colCount = PQnfields(result);

		/* Although it is not expected */
		if (colCount != SHARD_SIZES_COLUMN_COUNT)
		{
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "citus_update_table_statistics")));
			continue;
		}

		for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			uint64 shardId = 0;
			uint64 shardSize = 0;

			if (!ProcessShardStatisticsRow(result, rowIndex, &shardId, &shardSize))
			{
				/* this row has no valid shard statistics */
				continue;
			}

			if (OidVisited(alreadyVisitedShardPlacements, shardId))
			{
				/* We have already updated this placement list */
				continue;
			}

			VisitOid(alreadyVisitedShardPlacements, shardId);

			ShardInterval *shardInterval = LoadShardInterval(shardId);
			Oid relationId = shardInterval->relationId;
			List *shardPlacementList = ActiveShardPlacementList(shardId);

			UpdateShardSize(shardId, shardInterval, relationId, shardPlacementList,
							shardSize);
		}
		PQclear(result);
		ForgetResults(connection);
	}
	hash_destroy(alreadyVisitedShardPlacements);
}


/*
 * ProcessShardStatisticsRow processes a row of shard statistics of the input PGresult
 * - it returns true if this row belongs to a valid shard
 * - it returns false if this row has no valid shard statistics (shardId = INVALID_SHARD_ID)
 *
 * Input tuples are assumed to be of the form:
 * (shard_id bigint, shard_name text, shard_size bigint)
 */
static bool
ProcessShardStatisticsRow(PGresult *result, int64 rowIndex, uint64 *shardId,
						  uint64 *shardSize)
{
	*shardId = ParseIntField(result, rowIndex, 0);

	/* check for the dummy entries we put so that UNION ALL wouldn't complain */
	if (*shardId == INVALID_SHARD_ID)
	{
		/* this row has no valid shard statistics */
		return false;
	}

	*shardSize = ParseIntField(result, rowIndex, 2);
	return true;
}


/*
 * UpdateShardSize updates the shardlength (shard size) of the given
 * shard and its placements in pg_dist_placement.
 */
static void
UpdateShardSize(uint64 shardId, ShardInterval *shardInterval, Oid relationId,
				List *shardPlacementList, uint64 shardSize)
{
	ShardPlacement *placement = NULL;

	/* update metadata for each shard placement */
	foreach_ptr(placement, shardPlacementList)
	{
		uint64 placementId = placement->placementId;
		int32 groupId = placement->groupId;

		DeleteShardPlacementRow(placementId);
		InsertShardPlacementRow(shardId, placementId, SHARD_STATE_ACTIVE,
								shardSize, groupId);
	}
}


/*
 * WorkerShardStats queries the worker node, and retrieves shard statistics that
 * we assume have changed after new table data have been appended to the shard.
 */
static bool
WorkerShardStats(ShardPlacement *placement, Oid relationId, const char *shardName,
				 uint64 *shardSize)
{
	StringInfo tableSizeQuery = makeStringInfo();
	PGresult *queryResult = NULL;
	char *tableSizeStringEnd = NULL;

	int connectionFlags = 0;

	MultiConnection *connection = GetPlacementConnection(connectionFlags, placement,
														 NULL);

	/*
	 * This code-path doesn't support optional connections, so we don't expect
	 * NULL connections.
	 */
	Assert(connection != NULL);

	*shardSize = 0;

	char *quotedShardName = quote_literal_cstr(shardName);
	appendStringInfo(tableSizeQuery, SHARD_TABLE_SIZE_QUERY, quotedShardName);

	int executeCommand = ExecuteOptionalRemoteCommand(connection, tableSizeQuery->data,
													  &queryResult);
	if (executeCommand != 0)
	{
		return false;
	}

	char *tableSizeString = PQgetvalue(queryResult, 0, 0);
	if (tableSizeString == NULL)
	{
		PQclear(queryResult);
		ForgetResults(connection);
		return false;
	}

	errno = 0;
	uint64 tableSize = strtou64(tableSizeString, &tableSizeStringEnd, 0);
	if (errno != 0 || (*tableSizeStringEnd) != '\0')
	{
		PQclear(queryResult);
		ForgetResults(connection);
		return false;
	}

	*shardSize = tableSize;

	PQclear(queryResult);
	ForgetResults(connection);

	return true;
}


/*
 * ForeignConstraintGetReferencedTableId parses given foreign constraint query and
 * extracts referenced table id from it.
 */
Oid
ForeignConstraintGetReferencedTableId(const char *queryString)
{
	Node *queryNode = ParseTreeNode(queryString);
	AlterTableStmt *foreignConstraintStmt = (AlterTableStmt *) queryNode;
	AlterTableCmd *command = (AlterTableCmd *) linitial(foreignConstraintStmt->cmds);

	if (command->subtype == AT_AddConstraint)
	{
		Constraint *constraint = (Constraint *) command->def;
		if (constraint->contype == CONSTR_FOREIGN)
		{
			RangeVar *referencedTable = constraint->pktable;

			return RangeVarGetRelid(referencedTable, NoLock,
									foreignConstraintStmt->missing_ok);
		}
	}

	return InvalidOid;
}
