/*-------------------------------------------------------------------------
 *
 * master_stage_protocol.c
 *
 * Routines for staging PostgreSQL table data as shards into the distributed
 * cluster. These user-defined functions are similar to the psql-side \stage
 * command, but also differ from them in that users stage data from tables and
 * not files, and that they can also append to existing shards.
 *
 * Copyright (c) 2013-2016, Citus Data, Inc.
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
#include "distributed/connection_management.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_router_executor.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/placement_connection.h"
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
static void CreateShardsOnWorkersViaExecutor(Oid distributedRelationId,
											 List *shardPlacements,
											 bool useExclusiveConnection, bool
											 colocatedShard);
static void CreateShardsOnWorkersViaCommands(Oid distributedRelationId,
											 List *shardPlacements,
											 bool useExclusiveConnection, bool
											 colocatedShard);
static List * RelationShardListForShardCreate(ShardInterval *shardInterval);
static bool WorkerShardStats(ShardPlacement *placement, Oid relationId,
							 char *shardName, uint64 *shardSize,
							 text **shardMinValue, text **shardMaxValue);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_empty_shard);
PG_FUNCTION_INFO_V1(master_append_table_to_shard);
PG_FUNCTION_INFO_V1(master_update_shard_statistics);


/*
 * master_create_empty_shard creates an empty shard for the given distributed
 * table. The function first updates metadata on the coordinator node to make
 * this shard visible. Then it creates empty shard on worker node and added
 * shard placement row to metadata table.
 */
Datum
master_create_empty_shard(PG_FUNCTION_ARGS)
{
	text *relationNameText = PG_GETARG_TEXT_P(0);
	char *relationName = text_to_cstring(relationNameText);
	uint64 shardId = INVALID_SHARD_ID;
	uint32 attemptableNodeCount = 0;

	uint32 candidateNodeIndex = 0;
	List *candidateNodeList = NIL;
	text *nullMinValue = NULL;
	text *nullMaxValue = NULL;
	char partitionMethod = 0;
	char storageType = SHARD_STORAGE_TABLE;

	Oid relationId = ResolveRelationId(relationNameText, false);
	char relationKind = get_rel_relkind(relationId);
	char replicationModel = REPLICATION_MODEL_INVALID;

	CheckCitusVersion(ERROR);

	EnsureTablePermissions(relationId, ACL_INSERT);
	CheckDistributedTable(relationId);

	/*
	 * Ensure schema exists on each worker node. We can not run this function
	 * transactionally, since we may create shards over separate sessions and
	 * shard creation depends on the schema being present and visible from all
	 * sessions.
	 */
	EnsureSchemaExistsOnAllNodes(relationId);

	/* don't allow the table to be dropped */
	LockRelationOid(relationId, AccessShareLock);

	/* don't allow concurrent node list changes that require an exclusive lock */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/*
	 * We check whether the table is a foreign table or not. If it is, we set
	 * storage type as foreign also. Only exception is if foreign table is a
	 * foreign cstore table, in this case we set storage type as columnar.
	 *
	 * i.e. While setting storage type, columnar has priority over foreign.
	 */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		bool cstoreTable = cstoreTable = CStoreTable(relationId);
		if (cstoreTable)
		{
			storageType = SHARD_STORAGE_COLUMNAR;
		}
		else
		{
			storageType = SHARD_STORAGE_FOREIGN;
		}
	}

	partitionMethod = PartitionMethod(relationId);
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errmsg("relation \"%s\" is a hash partitioned table",
							   relationName),
						errdetail("We currently don't support creating shards "
								  "on hash-partitioned tables")));
	}
	else if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		ereport(ERROR, (errmsg("relation \"%s\" is a reference table",
							   relationName),
						errdetail("We currently don't support creating shards "
								  "on reference tables")));
	}

	replicationModel = TableReplicationModel(relationId);

	EnsureReplicationSettings(relationId, replicationModel);

	/* generate new and unique shardId from sequence */
	shardId = GetNextShardId();

	/* if enough live groups, add an extra candidate node as backup */
	{
		uint32 primaryNodeCount = ActivePrimaryNodeCount();

		attemptableNodeCount = ShardReplicationFactor;
		if (primaryNodeCount > ShardReplicationFactor)
		{
			attemptableNodeCount = ShardReplicationFactor + 1;
		}
	}

	/* first retrieve a list of random nodes for shard placements */
	while (candidateNodeIndex < attemptableNodeCount)
	{
		WorkerNode *candidateNode = NULL;

		if (ShardPlacementPolicy == SHARD_PLACEMENT_LOCAL_NODE_FIRST)
		{
			candidateNode = WorkerGetLocalFirstCandidateNode(candidateNodeList);
		}
		else if (ShardPlacementPolicy == SHARD_PLACEMENT_ROUND_ROBIN)
		{
			List *workerNodeList = ActivePrimaryNodeList();
			candidateNode = WorkerGetRoundRobinCandidateNode(workerNodeList, shardId,
															 candidateNodeIndex);
		}
		else if (ShardPlacementPolicy == SHARD_PLACEMENT_RANDOM)
		{
			candidateNode = WorkerGetRandomCandidateNode(candidateNodeList);
		}
		else
		{
			ereport(ERROR, (errmsg("unrecognized shard placement policy")));
		}

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
 * master_append_table_to_shard appends the given table's contents to the given
 * shard, and updates shard metadata on the master node. If the function fails
 * to append table data to all shard placements, it doesn't update any metadata
 * and errors out. Else if the function fails to append table data to some of
 * the shard placements, it marks those placements as invalid. These invalid
 * placements will get cleaned up during shard rebalancing.
 */
Datum
master_append_table_to_shard(PG_FUNCTION_ARGS)
{
	uint64 shardId = PG_GETARG_INT64(0);
	text *sourceTableNameText = PG_GETARG_TEXT_P(1);
	text *sourceNodeNameText = PG_GETARG_TEXT_P(2);
	uint32 sourceNodePort = PG_GETARG_UINT32(3);

	char *sourceTableName = text_to_cstring(sourceTableNameText);
	char *sourceNodeName = text_to_cstring(sourceNodeNameText);

	Oid shardSchemaOid = 0;
	char *shardSchemaName = NULL;
	char *shardTableName = NULL;
	char *shardQualifiedName = NULL;
	List *shardPlacementList = NIL;
	ListCell *shardPlacementCell = NULL;
	uint64 newShardSize = 0;
	uint64 shardMaxSizeInBytes = 0;
	float4 shardFillLevel = 0.0;
	char partitionMethod = 0;

	ShardInterval *shardInterval = NULL;
	Oid relationId = InvalidOid;
	bool cstoreTable = false;

	char storageType = 0;

	CheckCitusVersion(ERROR);

	shardInterval = LoadShardInterval(shardId);
	relationId = shardInterval->relationId;

	/* don't allow the table to be dropped */
	LockRelationOid(relationId, AccessShareLock);

	cstoreTable = CStoreTable(relationId);
	storageType = shardInterval->storageType;

	EnsureTablePermissions(relationId, ACL_INSERT);

	if (storageType != SHARD_STORAGE_TABLE && !cstoreTable)
	{
		ereport(ERROR, (errmsg("cannot append to shardId " UINT64_FORMAT, shardId),
						errdetail("The underlying shard is not a regular table")));
	}

	partitionMethod = PartitionMethod(relationId);
	if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod == DISTRIBUTE_BY_NONE)
	{
		ereport(ERROR, (errmsg("cannot append to shardId " UINT64_FORMAT, shardId),
						errdetail("We currently don't support appending to shards "
								  "in hash-partitioned or reference tables")));
	}

	/* ensure that the shard placement metadata does not change during the append */
	LockShardDistributionMetadata(shardId, ShareLock);

	/* serialize appends to the same shard */
	LockShardResource(shardId, ExclusiveLock);

	/* get schame name of the target shard */
	shardSchemaOid = get_rel_namespace(relationId);
	shardSchemaName = get_namespace_name(shardSchemaOid);

	/* Build shard table name. */
	shardTableName = get_rel_name(relationId);
	AppendShardIdToName(&shardTableName, shardId);

	shardQualifiedName = quote_qualified_identifier(shardSchemaName, shardTableName);

	shardPlacementList = FinalizedShardPlacementList(shardId);
	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find any shard placements for shardId "
							   UINT64_FORMAT, shardId),
						errhint("Try running master_create_empty_shard() first")));
	}

	BeginOrContinueCoordinatedTransaction();

	/* issue command to append table to each shard placement */
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		MultiConnection *connection = GetPlacementConnection(FOR_DML, shardPlacement,
															 NULL);
		PGresult *queryResult = NULL;
		int executeResult = 0;

		StringInfo workerAppendQuery = makeStringInfo();
		appendStringInfo(workerAppendQuery, WORKER_APPEND_TABLE_TO_SHARD,
						 quote_literal_cstr(shardQualifiedName),
						 quote_literal_cstr(sourceTableName),
						 quote_literal_cstr(sourceNodeName), sourceNodePort);

		RemoteTransactionBeginIfNecessary(connection);

		executeResult = ExecuteOptionalRemoteCommand(connection, workerAppendQuery->data,
													 &queryResult);
		PQclear(queryResult);
		ForgetResults(connection);

		if (executeResult != 0)
		{
			MarkRemoteTransactionFailed(connection, false);
		}
	}

	MarkFailedShardPlacements();

	/* update shard statistics and get new shard size */
	newShardSize = UpdateShardStatistics(shardId);

	/* calculate ratio of current shard size compared to shard max size */
	shardMaxSizeInBytes = (int64) ShardMaxSize * 1024L;
	shardFillLevel = ((float4) newShardSize / (float4) shardMaxSizeInBytes);

	PG_RETURN_FLOAT4(shardFillLevel);
}


/*
 * master_update_shard_statistics updates metadata (shard size and shard min/max
 * values) of the given shard and returns the updated shard size.
 */
Datum
master_update_shard_statistics(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	uint64 shardSize = 0;

	CheckCitusVersion(ERROR);

	shardSize = UpdateShardStatistics(shardId);

	PG_RETURN_INT64(shardSize);
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

	if (!IsDistributedTable(relationId))
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
	int attemptNumber = 0;
	List *foreignConstraintCommandList = GetTableForeignConstraintCommands(relationId);
	bool includeSequenceDefaults = false;
	List *ddlCommandList = GetTableDDLEvents(relationId, includeSequenceDefaults);
	uint32 connectionFlag = FOR_DDL;
	char *relationOwner = TableOwner(relationId);

	/* if we have enough nodes, add an extra placement attempt for backup */
	if (workerNodeCount > replicationFactor)
	{
		attemptCount++;
	}

	for (attemptNumber = 0; attemptNumber < attemptCount; attemptNumber++)
	{
		int workerNodeIndex = attemptNumber % workerNodeCount;
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, workerNodeIndex);
		uint32 nodeGroupId = workerNode->groupId;
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		int shardIndex = -1; /* not used in this code path */
		const RelayFileState shardState = FILE_FINALIZED;
		const uint64 shardSize = 0;
		MultiConnection *connection =
			GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort,
										  relationOwner, NULL);
		List *commandList = NIL;

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			ereport(WARNING, (errmsg("could not connect to node \"%s:%u\"", nodeName,
									 nodePort)));

			continue;
		}

		commandList = WorkerCreateShardCommandList(relationId, shardIndex, shardId,
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
	int attemptNumber = 0;
	int placementsInserted = 0;
	List *insertedShardPlacements = NIL;

	for (attemptNumber = 0; attemptNumber < replicationFactor; attemptNumber++)
	{
		int workerNodeIndex = (workerStartIndex + attemptNumber) % workerNodeCount;
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, workerNodeIndex);
		uint32 nodeGroupId = workerNode->groupId;
		const RelayFileState shardState = FILE_FINALIZED;
		const uint64 shardSize = 0;
		uint64 shardPlacementId = 0;
		ShardPlacement *shardPlacement = NULL;

		shardPlacementId = InsertShardPlacementRow(shardId, INVALID_PLACEMENT_ID,
												   shardState, shardSize, nodeGroupId);
		shardPlacement = LoadShardPlacement(shardId, shardPlacementId);
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
 * as a parameter. Function branches into two: either use the executor or execute the
 * commands one by one.
 */
void
CreateShardsOnWorkers(Oid distributedRelationId, List *shardPlacements,
					  bool useExclusiveConnection, bool colocatedShard)
{
	if (TaskExecutorType == MULTI_EXECUTOR_ADAPTIVE)
	{
		CreateShardsOnWorkersViaExecutor(distributedRelationId, shardPlacements,
										 useExclusiveConnection, colocatedShard);
	}
	else
	{
		CreateShardsOnWorkersViaCommands(distributedRelationId, shardPlacements,
										 useExclusiveConnection, colocatedShard);
	}
}


/*
 * CreateShardsOnWorkersViaExecutor creates the shards via the executor. This means
 * that it can adopt the number of connections required to create the shards.
 */
static void
CreateShardsOnWorkersViaExecutor(Oid distributedRelationId, List *shardPlacements, bool
								 useExclusiveConnection, bool colocatedShard)
{
	bool includeSequenceDefaults = false;
	List *ddlCommandList = GetTableDDLEvents(distributedRelationId,
											 includeSequenceDefaults);
	List *foreignConstraintCommandList =
		GetTableForeignConstraintCommands(distributedRelationId);
	ListCell *shardPlacementCell = NULL;

	int taskId = 1;
	List *taskList = NIL;
	int poolSize = 1;

	foreach(shardPlacementCell, shardPlacements)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		uint64 shardId = shardPlacement->shardId;
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		int shardIndex = -1;
		List *commandList = NIL;
		Task *task = NULL;
		List *relationShardList = RelationShardListForShardCreate(shardInterval);

		if (colocatedShard)
		{
			shardIndex = ShardIndex(shardInterval);
		}

		commandList = WorkerCreateShardCommandList(distributedRelationId, shardIndex,
												   shardId, ddlCommandList,
												   foreignConstraintCommandList);

		task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = StringJoin(commandList, ';');
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependedTaskList = NIL;
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

	ExecuteTaskList(ROW_MODIFY_NONE, taskList, poolSize);
}


/*
 * CreateShardsOnWorkersViaCommands creates shards on worker nodes given the shard
 * placements as a parameter. Function opens connections in transactional way. If the
 * caller needs an exclusive connection (in case of distributing local table with data
 * on it) or creating shards in a transaction, per placement connection is opened
 * for each placement.
 */
static void
CreateShardsOnWorkersViaCommands(Oid distributedRelationId, List *shardPlacements,
								 bool useExclusiveConnection, bool colocatedShard)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedRelationId);

	bool includeSequenceDefaults = false;
	List *ddlCommandList = GetTableDDLEvents(distributedRelationId,
											 includeSequenceDefaults);
	List *foreignConstraintCommandList = GetTableForeignConstraintCommands(
		distributedRelationId);
	List *claimedConnectionList = NIL;
	ListCell *connectionCell = NULL;
	ListCell *shardPlacementCell = NULL;
	int connectionFlags = FOR_DDL;
	bool partitionTable = PartitionTable(distributedRelationId);

	if (useExclusiveConnection)
	{
		connectionFlags |= CONNECTION_PER_PLACEMENT;
	}


	BeginOrContinueCoordinatedTransaction();

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC ||
		cacheEntry->replicationModel == REPLICATION_MODEL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

	/* mark parallel relation accesses before opening connections */
	if (ShouldRecordRelationAccess() && useExclusiveConnection)
	{
		RecordParallelDDLAccess(distributedRelationId);

		/* we should mark the parent as well */
		if (partitionTable)
		{
			Oid parentRelationId = PartitionParentOid(distributedRelationId);
			RecordParallelDDLAccess(parentRelationId);
		}
	}

	foreach(shardPlacementCell, shardPlacements)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		uint64 shardId = shardPlacement->shardId;
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		List *relationShardList = NIL;
		MultiConnection *connection = NULL;
		int shardIndex = -1;
		List *commandList = NIL;

		if (colocatedShard)
		{
			shardIndex = ShardIndex(shardInterval);
		}

		/*
		 * For partitions, make sure that we mark the parent table relation access
		 * with DDL. This is only important for parallel relation access in transaction
		 * blocks, thus check useExclusiveConnection and transaction block as well.
		 */
		if (ShouldRecordRelationAccess() && useExclusiveConnection && partitionTable)
		{
			List *placementAccessList = NIL;

			relationShardList = RelationShardListForShardCreate(shardInterval);

			placementAccessList = BuildPlacementDDLList(shardPlacement->groupId,
														relationShardList);

			connection = GetPlacementListConnection(connectionFlags, placementAccessList,
													NULL);
		}
		else
		{
			connection = GetPlacementConnection(connectionFlags, shardPlacement,
												NULL);
		}

		if (useExclusiveConnection)
		{
			ClaimConnectionExclusively(connection);
			claimedConnectionList = lappend(claimedConnectionList, connection);
		}

		RemoteTransactionBeginIfNecessary(connection);
		MarkRemoteTransactionCritical(connection);

		commandList = WorkerCreateShardCommandList(distributedRelationId, shardIndex,
												   shardId,
												   ddlCommandList,
												   foreignConstraintCommandList);

		ExecuteCriticalRemoteCommandList(connection, commandList);
	}

	/*
	 * We need to unclaim all connections to make them usable again for the copy
	 * command, otherwise copy going to open new connections to placements and
	 * can not see uncommitted changes.
	 */
	foreach(connectionCell, claimedConnectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		UnclaimConnection(connection);
	}
}


/*
 * RelationShardListForShardCreate gets a shard interval and returns the placement
 * accesses that would happen when a placement of the shard interval is created.
 */
static List *
RelationShardListForShardCreate(ShardInterval *shardInterval)
{
	List *relationShardList = NIL;
	RelationShard *relationShard = NULL;
	Oid relationId = shardInterval->relationId;
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
	List *referencedRelationList = cacheEntry->referencedRelationsViaForeignKey;
	List *referencingRelationList = cacheEntry->referencingRelationsViaForeignKey;
	List *allForeignKeyRelations = NIL;
	int shardIndex = -1;
	ListCell *fkeyRelationIdCell = NULL;

	/* list_concat_*() modifies the first arg, so make a copy first */
	allForeignKeyRelations = list_copy(referencedRelationList);
	allForeignKeyRelations = list_concat_unique_oid(allForeignKeyRelations,
													referencingRelationList);

	/* record the placement access of the shard itself */
	relationShard = CitusMakeNode(RelationShard);
	relationShard->relationId = relationId;
	relationShard->shardId = shardInterval->shardId;
	relationShardList = list_make1(relationShard);

	if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH &&
		cacheEntry->colocationId != INVALID_COLOCATION_ID)
	{
		shardIndex = ShardIndex(shardInterval);
	}


	/* all foregin key constraint relations */
	foreach(fkeyRelationIdCell, allForeignKeyRelations)
	{
		Oid fkeyRelationid = lfirst_oid(fkeyRelationIdCell);
		RelationShard *fkeyRelationShard = NULL;
		uint64 fkeyShardId = INVALID_SHARD_ID;

		if (!IsDistributedTable(fkeyRelationid))
		{
			/* we're not interested in local tables */
			continue;
		}

		if (PartitionMethod(fkeyRelationid) == DISTRIBUTE_BY_NONE)
		{
			fkeyShardId = GetFirstShardId(fkeyRelationid);
		}
		else if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH &&
				 PartitionMethod(fkeyRelationid) == DISTRIBUTE_BY_HASH)
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

		fkeyRelationShard = CitusMakeNode(RelationShard);
		fkeyRelationShard->relationId = fkeyRelationid;
		fkeyRelationShard->shardId = fkeyShardId;

		relationShardList = lappend(relationShardList, fkeyRelationShard);
	}


	/* if  partitioned table, make sure to record the parent table */
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
	ListCell *ddlCommandCell = NULL;
	ListCell *foreignConstraintCommandCell = NULL;

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);
		char *escapedDDLCommand = quote_literal_cstr(ddlCommand);
		StringInfo applyDDLCommand = makeStringInfo();

		if (strcmp(schemaName, "public") != 0)
		{
			appendStringInfo(applyDDLCommand, WORKER_APPLY_SHARD_DDL_COMMAND, shardId,
							 escapedSchemaName, escapedDDLCommand);
		}
		else
		{
			appendStringInfo(applyDDLCommand,
							 WORKER_APPLY_SHARD_DDL_COMMAND_WITHOUT_SCHEMA, shardId,
							 escapedDDLCommand);
		}

		commandList = lappend(commandList, applyDDLCommand->data);
	}

	foreach(foreignConstraintCommandCell, foreignConstraintCommandList)
	{
		char *command = (char *) lfirst(foreignConstraintCommandCell);
		char *escapedCommand = quote_literal_cstr(command);

		Oid referencedRelationId = InvalidOid;
		Oid referencedSchemaId = InvalidOid;
		char *referencedSchemaName = NULL;
		char *escapedReferencedSchemaName = NULL;
		uint64 referencedShardId = INVALID_SHARD_ID;

		StringInfo applyForeignConstraintCommand = makeStringInfo();

		/* we need to parse the foreign constraint command to get referencing table id */
		referencedRelationId = ForeignConstraintGetReferencedTableId(command);
		if (referencedRelationId == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cannot create foreign key constraint"),
							errdetail("Referenced relation cannot be found.")));
		}

		referencedSchemaId = get_rel_namespace(referencedRelationId);
		referencedSchemaName = get_namespace_name(referencedSchemaId);
		escapedReferencedSchemaName = quote_literal_cstr(referencedSchemaName);

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
		else if (PartitionMethod(referencedRelationId) == DISTRIBUTE_BY_NONE)
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
	char storageType = shardInterval->storageType;
	char partitionType = PartitionMethod(relationId);
	char *shardQualifiedName = NULL;
	List *shardPlacementList = NIL;
	ListCell *shardPlacementCell = NULL;
	bool statsOK = false;
	uint64 shardSize = 0;
	text *minValue = NULL;
	text *maxValue = NULL;

	/* Build shard qualified name. */
	char *shardName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);

	AppendShardIdToName(&shardName, shardId);

	shardQualifiedName = quote_qualified_identifier(schemaName, shardName);

	shardPlacementList = FinalizedShardPlacementList(shardId);

	/* get shard's statistics from a shard placement */
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);

		statsOK = WorkerShardStats(placement, relationId, shardQualifiedName,
								   &shardSize, &minValue, &maxValue);
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

	/* make sure we don't process cancel signals */
	HOLD_INTERRUPTS();

	/* update metadata for each shard placement we appended to */
	shardPlacementCell = NULL;
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
		uint64 placementId = placement->placementId;
		int32 groupId = placement->groupId;

		DeleteShardPlacementRow(placementId);
		InsertShardPlacementRow(shardId, placementId, FILE_FINALIZED, shardSize,
								groupId);
	}

	/* only update shard min/max values for append-partitioned tables */
	if (partitionType == DISTRIBUTE_BY_APPEND)
	{
		DeleteShardRow(shardId);
		InsertShardRow(relationId, shardId, storageType, minValue, maxValue);
	}

	if (QueryCancelPending)
	{
		ereport(WARNING, (errmsg("cancel requests are ignored during metadata update")));
		QueryCancelPending = false;
	}

	RESUME_INTERRUPTS();

	return shardSize;
}


/*
 * WorkerShardStats queries the worker node, and retrieves shard statistics that
 * we assume have changed after new table data have been appended to the shard.
 */
static bool
WorkerShardStats(ShardPlacement *placement, Oid relationId, char *shardName,
				 uint64 *shardSize, text **shardMinValue, text **shardMaxValue)
{
	char *quotedShardName = NULL;
	bool cstoreTable = false;
	StringInfo tableSizeQuery = makeStringInfo();

	const uint32 unusedTableId = 1;
	char partitionType = PartitionMethod(relationId);
	Var *partitionColumn = NULL;
	char *partitionColumnName = NULL;
	StringInfo partitionValueQuery = makeStringInfo();

	PGresult *queryResult = NULL;
	const int minValueIndex = 0;
	const int maxValueIndex = 1;

	uint64 tableSize = 0;
	char *tableSizeString = NULL;
	char *tableSizeStringEnd = NULL;
	bool minValueIsNull = false;
	bool maxValueIsNull = false;

	int connectionFlags = 0;
	int executeCommand = 0;

	MultiConnection *connection = GetPlacementConnection(connectionFlags, placement,
														 NULL);

	*shardSize = 0;
	*shardMinValue = NULL;
	*shardMaxValue = NULL;

	quotedShardName = quote_literal_cstr(shardName);

	cstoreTable = CStoreTable(relationId);
	if (cstoreTable)
	{
		appendStringInfo(tableSizeQuery, SHARD_CSTORE_TABLE_SIZE_QUERY, quotedShardName);
	}
	else
	{
		appendStringInfo(tableSizeQuery, SHARD_TABLE_SIZE_QUERY, quotedShardName);
	}

	executeCommand = ExecuteOptionalRemoteCommand(connection, tableSizeQuery->data,
												  &queryResult);
	if (executeCommand != 0)
	{
		return false;
	}

	tableSizeString = PQgetvalue(queryResult, 0, 0);
	if (tableSizeString == NULL)
	{
		PQclear(queryResult);
		ForgetResults(connection);
		return false;
	}

	errno = 0;
	tableSize = pg_strtouint64(tableSizeString, &tableSizeStringEnd, 0);
	if (errno != 0 || (*tableSizeStringEnd) != '\0')
	{
		PQclear(queryResult);
		ForgetResults(connection);
		return false;
	}

	*shardSize = tableSize;

	PQclear(queryResult);
	ForgetResults(connection);

	if (partitionType != DISTRIBUTE_BY_APPEND)
	{
		/* we don't need min/max for non-append distributed tables */
		return true;
	}

	/* fill in the partition column name and shard name in the query. */
	partitionColumn = PartitionColumn(relationId, unusedTableId);
	partitionColumnName = get_attname_internal(relationId, partitionColumn->varattno,
											   false);
	appendStringInfo(partitionValueQuery, SHARD_RANGE_QUERY,
					 partitionColumnName, partitionColumnName, shardName);

	executeCommand = ExecuteOptionalRemoteCommand(connection, partitionValueQuery->data,
												  &queryResult);
	if (executeCommand != 0)
	{
		return false;
	}

	minValueIsNull = PQgetisnull(queryResult, 0, minValueIndex);
	maxValueIsNull = PQgetisnull(queryResult, 0, maxValueIndex);

	if (!minValueIsNull && !maxValueIsNull)
	{
		char *minValueResult = PQgetvalue(queryResult, 0, minValueIndex);
		char *maxValueResult = PQgetvalue(queryResult, 0, maxValueIndex);

		*shardMinValue = cstring_to_text(minValueResult);
		*shardMaxValue = cstring_to_text(maxValueResult);
	}

	PQclear(queryResult);
	ForgetResults(connection);

	return true;
}


/*
 * ForeignConstraintGetReferencedTableId parses given foreign constraint query and
 * extracts referenced table id from it.
 */
Oid
ForeignConstraintGetReferencedTableId(char *queryString)
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
