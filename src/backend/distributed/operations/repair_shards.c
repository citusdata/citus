/*-------------------------------------------------------------------------
 *
 * repair_shards.c
 *
 * This file contains functions to repair unhealthy shard placements using data
 * from healthy ones.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include <string.h>
#include <sys/statvfs.h>

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_enum.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/shard_cleaner.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/repair_shards.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* local type declarations */

/*
 * ShardInterval along with to be executed
 * DDL command list.
 */
typedef struct ShardCommandList
{
	ShardInterval *shardInterval;
	List *ddlCommandList;
} ShardCommandList;

/* local function forward declarations */
static void VerifyTablesHaveReplicaIdentity(List *colocatedTableList);
static bool RelationCanPublishAllModifications(Oid relationId);
static bool CanUseLogicalReplication(Oid relationId, char shardReplicationMode);
static void ErrorIfTableCannotBeReplicated(Oid relationId);
static void RepairShardPlacement(int64 shardId, const char *sourceNodeName,
								 int32 sourceNodePort, const char *targetNodeName,
								 int32 targetNodePort);
static void ReplicateColocatedShardPlacement(int64 shardId, char *sourceNodeName,
											 int32 sourceNodePort, char *targetNodeName,
											 int32 targetNodePort,
											 char shardReplicationMode);
static void CopyShardTables(List *shardIntervalList, char *sourceNodeName,
							int32 sourceNodePort, char *targetNodeName,
							int32 targetNodePort, bool useLogicalReplication);
static void CopyShardTablesViaLogicalReplication(List *shardIntervalList,
												 char *sourceNodeName,
												 int32 sourceNodePort,
												 char *targetNodeName,
												 int32 targetNodePort);

static void CopyShardTablesViaBlockWrites(List *shardIntervalList, char *sourceNodeName,
										  int32 sourceNodePort,
										  char *targetNodeName, int32 targetNodePort);
static List * CopyPartitionShardsCommandList(ShardInterval *shardInterval,
											 const char *sourceNodeName,
											 int32 sourceNodePort);
static void EnsureShardCanBeRepaired(int64 shardId, const char *sourceNodeName,
									 int32 sourceNodePort, const char *targetNodeName,
									 int32 targetNodePort);
static void EnsureShardCanBeCopied(int64 shardId, const char *sourceNodeName,
								   int32 sourceNodePort, const char *targetNodeName,
								   int32 targetNodePort);
static List * RecreateTableDDLCommandList(Oid relationId);
static void EnsureTableListOwner(List *tableIdList);
static void EnsureTableListSuitableForReplication(List *tableIdList);

static void DropColocatedShardPlacement(ShardInterval *shardInterval, char *nodeName,
										int32 nodePort);
static void MarkForDropColocatedShardPlacement(ShardInterval *shardInterval,
											   char *nodeName, int32 nodePort);
static void UpdateColocatedShardPlacementMetadataOnWorkers(int64 shardId,
														   char *sourceNodeName,
														   int32 sourceNodePort,
														   char *targetNodeName,
														   int32 targetNodePort);
static void CheckSpaceConstraints(MultiConnection *connection,
								  uint64 colocationSizeInBytes);
static void EnsureEnoughDiskSpaceForShardMove(List *colocatedShardList,
											  char *sourceNodeName, uint32 sourceNodePort,
											  char *targetNodeName, uint32
											  targetNodePort);
static List * RecreateShardDDLCommandList(ShardInterval *shardInterval,
										  const char *sourceNodeName,
										  int32 sourceNodePort);
static List * CopyShardContentsCommandList(ShardInterval *shardInterval,
										   const char *sourceNodeName,
										   int32 sourceNodePort);
static List * PostLoadShardCreationCommandList(ShardInterval *shardInterval,
											   const char *sourceNodeName,
											   int32 sourceNodePort);
static ShardCommandList * CreateShardCommandList(ShardInterval *shardInterval,
												 List *ddlCommandList);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_copy_shard_placement);
PG_FUNCTION_INFO_V1(master_copy_shard_placement);
PG_FUNCTION_INFO_V1(citus_move_shard_placement);
PG_FUNCTION_INFO_V1(master_move_shard_placement);

bool DeferShardDeleteOnMove = true;

double DesiredPercentFreeAfterMove = 10;
bool CheckAvailableSpaceBeforeMove = true;


/*
 * citus_copy_shard_placement implements a user-facing UDF to repair data from
 * a healthy (source) node to an inactive (target) node. To accomplish this it
 * entirely recreates the table structure before copying all data. During this
 * time all modifications are paused to the shard. After successful repair, the
 * inactive placement is marked healthy and modifications may continue. If the
 * repair fails at any point, this function throws an error, leaving the node
 * in an unhealthy state. Please note that citus_copy_shard_placement copies
 * given shard along with its co-located shards.
 */
Datum
citus_copy_shard_placement(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 shardId = PG_GETARG_INT64(0);
	text *sourceNodeNameText = PG_GETARG_TEXT_P(1);
	int32 sourceNodePort = PG_GETARG_INT32(2);
	text *targetNodeNameText = PG_GETARG_TEXT_P(3);
	int32 targetNodePort = PG_GETARG_INT32(4);
	bool doRepair = PG_GETARG_BOOL(5);
	Oid shardReplicationModeOid = PG_GETARG_OID(6);

	char *sourceNodeName = text_to_cstring(sourceNodeNameText);
	char *targetNodeName = text_to_cstring(targetNodeNameText);

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);
	if (doRepair && shardReplicationMode == TRANSFER_MODE_FORCE_LOGICAL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("logical replication cannot be used for repairs")));
	}

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	ErrorIfTableCannotBeReplicated(shardInterval->relationId);

	AcquirePlacementColocationLock(shardInterval->relationId, ExclusiveLock,
								   doRepair ? "repair" : "copy");

	if (doRepair)
	{
		RepairShardPlacement(shardId, sourceNodeName, sourceNodePort, targetNodeName,
							 targetNodePort);
	}
	else
	{
		ReplicateColocatedShardPlacement(shardId, sourceNodeName, sourceNodePort,
										 targetNodeName, targetNodePort,
										 shardReplicationMode);
	}

	PG_RETURN_VOID();
}


/*
 * master_copy_shard_placement is a wrapper function for old UDF name.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
{
	return citus_copy_shard_placement(fcinfo);
}


/*
 * ShardListSizeInBytes returns the size in bytes of a set of shard tables.
 */
uint64
ShardListSizeInBytes(List *shardList, char *workerNodeName, uint32
					 workerNodePort)
{
	uint32 connectionFlag = 0;

	/* we skip child tables of a partitioned table if this boolean variable is true */
	bool optimizePartitionCalculations = true;
	StringInfo tableSizeQuery = GenerateSizeQueryOnMultiplePlacements(shardList,
																	  TOTAL_RELATION_SIZE,
																	  optimizePartitionCalculations);

	MultiConnection *connection = GetNodeConnection(connectionFlag, workerNodeName,
													workerNodePort);
	PGresult *result = NULL;
	int queryResult = ExecuteOptionalRemoteCommand(connection, tableSizeQuery->data,
												   &result);

	if (queryResult != RESPONSE_OKAY)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot get the size because of a connection error")));
	}

	List *sizeList = ReadFirstColumnAsText(result);
	if (list_length(sizeList) != 1)
	{
		ereport(ERROR, (errmsg(
							"received wrong number of rows from worker, expected 1 received %d",
							list_length(sizeList))));
	}

	StringInfo totalSizeStringInfo = (StringInfo) linitial(sizeList);
	char *totalSizeString = totalSizeStringInfo->data;
	uint64 totalSize = SafeStringToUint64(totalSizeString);

	PQclear(result);
	ForgetResults(connection);

	return totalSize;
}


/*
 * CheckSpaceConstraints checks there is enough space to place the colocation
 * on the node that the connection is connected to.
 */
static void
CheckSpaceConstraints(MultiConnection *connection, uint64 colocationSizeInBytes)
{
	uint64 diskAvailableInBytes = 0;
	uint64 diskSizeInBytes = 0;
	bool success =
		GetNodeDiskSpaceStatsForConnection(connection, &diskAvailableInBytes,
										   &diskSizeInBytes);
	if (!success)
	{
		ereport(ERROR, (errmsg("Could not fetch disk stats for node: %s-%d",
							   connection->hostname, connection->port)));
	}

	uint64 diskAvailableInBytesAfterShardMove = 0;
	if (diskAvailableInBytes < colocationSizeInBytes)
	{
		/*
		 * even though the space will be less than "0", we set it to 0 for convenience.
		 */
		diskAvailableInBytes = 0;
	}
	else
	{
		diskAvailableInBytesAfterShardMove = diskAvailableInBytes - colocationSizeInBytes;
	}
	uint64 desiredNewDiskAvailableInBytes = diskSizeInBytes *
											(DesiredPercentFreeAfterMove / 100);
	if (diskAvailableInBytesAfterShardMove < desiredNewDiskAvailableInBytes)
	{
		ereport(ERROR, (errmsg("not enough empty space on node if the shard is moved, "
							   "actual available space after move will be %ld bytes, "
							   "desired available space after move is %ld bytes,"
							   "estimated size increase on node after move is %ld bytes.",
							   diskAvailableInBytesAfterShardMove,
							   desiredNewDiskAvailableInBytes, colocationSizeInBytes),
						errhint(
							"consider lowering citus.desired_percent_disk_available_after_move.")));
	}
}


/*
 * citus_move_shard_placement moves given shard (and its co-located shards) from one
 * node to the other node. To accomplish this it entirely recreates the table structure
 * before copying all data.
 *
 * After that, there are two different paths. First one is blocking shard move in the
 * sense that during shard move all modifications are paused to the shard. The second
 * one relies on logical replication meaning that the writes blocked only for a very
 * short duration almost only when the metadata is actually being updated.
 *
 * After successful move operation, shards in the source node gets deleted. If the move
 * fails at any point, this function throws an error, leaving the cluster without doing
 * any changes in source node or target node.
 */
Datum
citus_move_shard_placement(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 shardId = PG_GETARG_INT64(0);
	char *sourceNodeName = text_to_cstring(PG_GETARG_TEXT_P(1));
	int32 sourceNodePort = PG_GETARG_INT32(2);
	char *targetNodeName = text_to_cstring(PG_GETARG_TEXT_P(3));
	int32 targetNodePort = PG_GETARG_INT32(4);
	Oid shardReplicationModeOid = PG_GETARG_OID(5);


	ListCell *colocatedTableCell = NULL;
	ListCell *colocatedShardCell = NULL;

	Oid relationId = RelationIdForShard(shardId);
	ErrorIfMoveUnsupportedTableType(relationId);
	ErrorIfTargetNodeIsNotSafeToMove(targetNodeName, targetNodePort);

	AcquirePlacementColocationLock(relationId, ExclusiveLock, "move");

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *colocatedTableList = ColocatedTableList(distributedTableId);
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);

	foreach(colocatedTableCell, colocatedTableList)
	{
		Oid colocatedTableId = lfirst_oid(colocatedTableCell);

		/* check that user has owner rights in all co-located tables */
		EnsureTableOwner(colocatedTableId);

		/*
		 * Block concurrent DDL / TRUNCATE commands on the relation. Similarly,
		 * block concurrent citus_move_shard_placement() on any shard of
		 * the same relation. This is OK for now since we're executing shard
		 * moves sequentially anyway.
		 */
		LockRelationOid(colocatedTableId, ShareUpdateExclusiveLock);

		if (IsForeignTable(relationId))
		{
			char *relationName = get_rel_name(colocatedTableId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot repair shard"),
							errdetail("Table %s is a foreign table. Repairing "
									  "shards backed by foreign tables is "
									  "not supported.", relationName)));
		}
	}

	/* we sort colocatedShardList so that lock operations will not cause any deadlocks */
	colocatedShardList = SortList(colocatedShardList, CompareShardIntervalsById);
	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		uint64 colocatedShardId = colocatedShard->shardId;

		EnsureShardCanBeCopied(colocatedShardId, sourceNodeName, sourceNodePort,
							   targetNodeName, targetNodePort);
	}

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);
	if (shardReplicationMode == TRANSFER_MODE_AUTOMATIC)
	{
		VerifyTablesHaveReplicaIdentity(colocatedTableList);
	}

	EnsureEnoughDiskSpaceForShardMove(colocatedShardList, sourceNodeName, sourceNodePort,
									  targetNodeName, targetNodePort);

	/*
	 * At this point of the shard moves, we don't need to block the writes to
	 * shards when logical replication is used.
	 */
	bool useLogicalReplication = CanUseLogicalReplication(distributedTableId,
														  shardReplicationMode);
	if (!useLogicalReplication)
	{
		BlockWritesToShardList(colocatedShardList);
	}
	else
	{
		/*
		 * We prevent multiple shard moves in a transaction that use logical
		 * replication. That's because the first call opens a transaction block
		 * on the worker to drop the old shard placement and replication slot
		 * creation waits for pending transactions to finish, which will not
		 * happen ever. In other words, we prevent a self-deadlock if both
		 * source shard placements are on the same node.
		 */
		if (PlacementMovedUsingLogicalReplicationInTX)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("moving multiple shard placements via logical "
								   "replication in the same transaction is currently "
								   "not supported"),
							errhint("If you wish to move multiple shard placements "
									"in a single transaction set the shard_transfer_mode "
									"to 'block_writes'.")));
		}

		PlacementMovedUsingLogicalReplicationInTX = true;
	}

	/*
	 * CopyColocatedShardPlacement function copies given shard with its co-located
	 * shards.
	 */
	CopyShardTables(colocatedShardList, sourceNodeName, sourceNodePort, targetNodeName,
					targetNodePort, useLogicalReplication);

	ShardInterval *colocatedShard = NULL;
	foreach_ptr(colocatedShard, colocatedShardList)
	{
		uint64 colocatedShardId = colocatedShard->shardId;
		uint32 groupId = GroupForNode(targetNodeName, targetNodePort);
		uint64 placementId = GetNextPlacementId();

		InsertShardPlacementRow(colocatedShardId, placementId,
								SHARD_STATE_ACTIVE, ShardLength(colocatedShardId),
								groupId);
	}

	/* since this is move operation, we remove shards from source node after copy */
	if (DeferShardDeleteOnMove)
	{
		MarkForDropColocatedShardPlacement(shardInterval, sourceNodeName, sourceNodePort);
	}
	else
	{
		DropColocatedShardPlacement(shardInterval, sourceNodeName, sourceNodePort);
	}

	UpdateColocatedShardPlacementMetadataOnWorkers(shardId, sourceNodeName,
												   sourceNodePort, targetNodeName,
												   targetNodePort);

	PG_RETURN_VOID();
}


/*
 * EnsureEnoughDiskSpaceForShardMove checks that there is enough space for
 * shard moves of the given colocated shard list from source node to target node.
 * It tries to clean up old shard placements to ensure there is enough space.
 */
static void
EnsureEnoughDiskSpaceForShardMove(List *colocatedShardList,
								  char *sourceNodeName, uint32 sourceNodePort,
								  char *targetNodeName, uint32 targetNodePort)
{
	if (!CheckAvailableSpaceBeforeMove)
	{
		return;
	}
	uint64 colocationSizeInBytes = ShardListSizeInBytes(colocatedShardList,
														sourceNodeName,
														sourceNodePort);

	uint32 connectionFlag = 0;
	MultiConnection *connection = GetNodeConnection(connectionFlag, targetNodeName,
													targetNodePort);
	CheckSpaceConstraints(connection, colocationSizeInBytes);
}


/*
 * ErrorIfTargetNodeIsNotSafeToMove throws error if the target node is not
 * eligible for moving shards.
 */
void
ErrorIfTargetNodeIsNotSafeToMove(const char *targetNodeName, int targetNodePort)
{
	WorkerNode *workerNode = FindWorkerNode(targetNodeName, targetNodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Moving shards to a non-existing node is not supported"),
						errhint(
							"Add the target node via SELECT citus_add_node('%s', %d);",
							targetNodeName, targetNodePort)));
	}

	if (!workerNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Moving shards to a non-active node is not supported"),
						errhint(
							"Activate the target node via SELECT citus_activate_node('%s', %d);",
							targetNodeName, targetNodePort)));
	}

	if (!workerNode->shouldHaveShards)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Moving shards to a node that shouldn't have a shard is "
							   "not supported"),
						errhint("Allow shards on the target node via "
								"SELECT * FROM citus_set_node_property('%s', %d, 'shouldhaveshards', true);",
								targetNodeName, targetNodePort)));
	}

	if (!NodeIsPrimary(workerNode))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Moving shards to a secondary (e.g., replica) node is "
							   "not supported")));
	}
}


/*
 * master_move_shard_placement is a wrapper around citus_move_shard_placement.
 */
Datum
master_move_shard_placement(PG_FUNCTION_ARGS)
{
	return citus_move_shard_placement(fcinfo);
}


/*
 * ErrorIfMoveUnsupportedTableType is a helper function for rebalance_table_shards
 * and citus_move_shard_placement udf's to error out if relation with relationId
 * is not a distributed table.
 */
void
ErrorIfMoveUnsupportedTableType(Oid relationId)
{
	if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
	{
		return;
	}

	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("table %s is a regular postgres table, you can "
							   "only move shards of a citus table",
							   qualifiedRelationName)));
	}
	else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("table %s is a local table, moving shard of "
							   "a local table added to metadata is currently "
							   "not supported", qualifiedRelationName)));
	}
	else if (IsCitusTableType(relationId, REFERENCE_TABLE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("table %s is a reference table, moving shard of "
							   "a reference table is not supported",
							   qualifiedRelationName)));
	}
}


/*
 * VerifyTablesHaveReplicaIdentity throws an error if any of the tables
 * do not have a replica identity, which is required for logical replication
 * to replicate UPDATE and DELETE commands.
 */
static void
VerifyTablesHaveReplicaIdentity(List *colocatedTableList)
{
	ListCell *colocatedTableCell = NULL;

	foreach(colocatedTableCell, colocatedTableList)
	{
		Oid colocatedTableId = lfirst_oid(colocatedTableCell);

		if (!RelationCanPublishAllModifications(colocatedTableId))
		{
			char *colocatedRelationName = get_rel_name(colocatedTableId);

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot use logical replication to transfer shards of "
								   "the relation %s since it doesn't have a REPLICA "
								   "IDENTITY or PRIMARY KEY", colocatedRelationName),
							errdetail("UPDATE and DELETE commands on the shard will "
									  "error out during logical replication unless "
									  "there is a REPLICA IDENTITY or PRIMARY KEY."),
							errhint("If you wish to continue without a replica "
									"identity set the shard_transfer_mode to "
									"'force_logical' or 'block_writes'.")));
		}
	}
}


/*
 * RelationCanPublishAllModifications returns true if the relation is safe to publish
 * all modification while being replicated via logical replication.
 */
static bool
RelationCanPublishAllModifications(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	bool canPublish = false;

	if (relation == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not open relation with OID %u", relationId)));
	}

	/* if relation has replica identity we are always good */
	if (relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
		OidIsValid(RelationGetReplicaIndex(relation)))
	{
		canPublish = true;
	}

	/* partitioned tables do not contain any data themselves, can always replicate */
	if (PartitionedTable(relationId))
	{
		canPublish = true;
	}

	RelationClose(relation);

	return canPublish;
}


/*
 * BlockWritesToShardList blocks writes to all shards in the given shard
 * list. The function assumes that all the shards in the list are colocated.
 */
void
BlockWritesToShardList(List *shardList)
{
	ShardInterval *shard = NULL;
	foreach_ptr(shard, shardList)
	{
		/*
		 * We need to lock the referenced reference table metadata to avoid
		 * asynchronous shard copy in case of cascading DML operations.
		 */
		LockReferencedReferenceShardDistributionMetadata(shard->shardId,
														 ExclusiveLock);

		LockShardDistributionMetadata(shard->shardId, ExclusiveLock);
	}

	/* following code relies on the list to have at least one shard */
	if (list_length(shardList) == 0)
	{
		return;
	}

	/*
	 * Since the function assumes that the input shards are colocated,
	 * calculating shouldSyncMetadata for a single table is sufficient.
	 */
	ShardInterval *firstShardInterval = (ShardInterval *) linitial(shardList);
	Oid firstDistributedTableId = firstShardInterval->relationId;

	bool shouldSyncMetadata = ShouldSyncTableMetadata(firstDistributedTableId);
	if (shouldSyncMetadata || !IsCoordinator())
	{
		/*
		 * Even if users disable metadata sync, we cannot allow them not to
		 * acquire the remote locks. Hence, we have !IsCoordinator() check.
		 */
		LockShardListMetadataOnWorkers(ExclusiveLock, shardList);
	}
}


/*
 * CanUseLogicalReplication returns true if the given table can be logically replicated.
 */
static bool
CanUseLogicalReplication(Oid relationId, char shardReplicationMode)
{
	if (shardReplicationMode == TRANSFER_MODE_BLOCK_WRITES)
	{
		/* user explicitly chose not to use logical replication */
		return false;
	}

	/*
	 * Logical replication doesn't support replicating foreign tables and views.
	 */
	if (!RegularTable(relationId))
	{
		ereport(LOG, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					  errmsg("Cannot use logical replication for "
							 "shard move since the relation %s is not "
							 "a regular relation",
							 get_rel_name(relationId))));


		return false;
	}

	/* Logical replication doesn't support inherited tables */
	if (IsParentTable(relationId))
	{
		ereport(LOG, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					  errmsg("Cannot use logical replication for "
							 "shard move since the relation %s is an "
							 "inherited relation",
							 get_rel_name(relationId))));

		return false;
	}

	return true;
}


/*
 * ErrorIfTableCannotBeReplicated function errors out if the given table is not suitable
 * for its shard being replicated. There are 2 cases in which shard replication is not
 * allowed:
 *
 * 1) MX tables, since RF=1 is a must MX tables
 * 2) Reference tables, since the shard should already exist in all workers
 */
static void
ErrorIfTableCannotBeReplicated(Oid relationId)
{
	/*
	 * Note that ShouldSyncTableMetadata() returns true for both MX tables
	 * and reference tables.
	 */
	bool shouldSyncMetadata = ShouldSyncTableMetadata(relationId);
	if (!shouldSyncMetadata)
	{
		return;
	}

	CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(relationId);
	char *relationName = get_rel_name(relationId);

	if (IsCitusTableTypeCacheEntry(tableEntry, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						(errmsg("Table %s is a local table. Replicating "
								"shard of a local table added to metadata "
								"currently is not supported",
								quote_literal_cstr(relationName)))));
	}

	/*
	 * ShouldSyncTableMetadata() returns true also for reference table,
	 * we don't want to error in that case since reference tables aren't
	 * automatically replicated to active nodes with no shards, and
	 * master_copy_shard_placement() can be used to create placements in
	 * such nodes.
	 */
	if (tableEntry->replicationModel == REPLICATION_MODEL_STREAMING)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						(errmsg("Table %s is streaming replicated. Shards "
								"of streaming replicated tables cannot "
								"be copied", quote_literal_cstr(relationName)))));
	}
}


/*
 * LookupShardTransferMode maps the oids of citus.shard_transfer_mode enum
 * values to a char.
 */
char
LookupShardTransferMode(Oid shardReplicationModeOid)
{
	char shardReplicationMode = 0;

	Datum enumLabelDatum = DirectFunctionCall1(enum_out, shardReplicationModeOid);
	char *enumLabel = DatumGetCString(enumLabelDatum);

	if (strncmp(enumLabel, "auto", NAMEDATALEN) == 0)
	{
		shardReplicationMode = TRANSFER_MODE_AUTOMATIC;
	}
	else if (strncmp(enumLabel, "force_logical", NAMEDATALEN) == 0)
	{
		shardReplicationMode = TRANSFER_MODE_FORCE_LOGICAL;
	}
	else if (strncmp(enumLabel, "block_writes", NAMEDATALEN) == 0)
	{
		shardReplicationMode = TRANSFER_MODE_BLOCK_WRITES;
	}
	else
	{
		ereport(ERROR, (errmsg("invalid label for enum: %s", enumLabel)));
	}

	return shardReplicationMode;
}


/*
 * RepairShardPlacement repairs given shard from a source node to target node.
 * This function is not co-location aware. It only repairs given shard.
 */
static void
RepairShardPlacement(int64 shardId, const char *sourceNodeName, int32 sourceNodePort,
					 const char *targetNodeName, int32 targetNodePort)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	char *tableOwner = TableOwner(shardInterval->relationId);

	/* prevent table from being dropped */
	LockRelationOid(distributedTableId, AccessShareLock);

	EnsureTableOwner(distributedTableId);

	if (IsForeignTable(distributedTableId))
	{
		char *relationName = get_rel_name(distributedTableId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot repair shard"),
						errdetail("Table %s is a foreign table. Repairing "
								  "shards backed by foreign tables is "
								  "not supported.", relationName)));
	}

	/*
	 * Let's not allow repairing partitions to prevent any edge cases.
	 * We're already not allowing any kind of modifications on the partitions
	 * so their placements are not likely to be marked as INVALID. The only
	 * possible case to mark placement of a partition as invalid is
	 * "ALTER TABLE parent_table DETACH PARTITION partition_table". But,
	 * given that the table would become a regular distributed table if the
	 * command succeeds, we're OK since the regular distributed tables can
	 * be repaired later on.
	 */
	EnsurePartitionTableNotReplicated(distributedTableId);

	/*
	 * We take a lock on the referenced table if there is a foreign constraint
	 * during the copy procedure. If we do not block DMLs on the referenced
	 * table, we cannot avoid the inconsistency between the two copies of the
	 * data. Currently, we do not support replication factor > 1 on the tables
	 * with foreign constraints, so this command will fail for this case anyway.
	 * However, it is taken as a precaution in case we support it one day.
	 */
	LockReferencedReferenceShardDistributionMetadata(shardId, ExclusiveLock);

	/*
	 * We plan to move the placement to the healthy state, so we need to grab a shard
	 * metadata lock (in exclusive mode).
	 */
	LockShardDistributionMetadata(shardId, ExclusiveLock);

	/*
	 * For shard repair, there should be healthy placement in source node and unhealthy
	 * placement in the target node.
	 */
	EnsureShardCanBeRepaired(shardId, sourceNodeName, sourceNodePort, targetNodeName,
							 targetNodePort);

	/*
	 * If the shard belongs to a partitioned table, we need to load the data after
	 * creating the partitions and the partitioning hierarcy.
	 */
	bool partitionedTable = PartitionedTableNoLock(distributedTableId);
	bool includeData = !partitionedTable;

	/* we generate necessary commands to recreate the shard in target node */
	List *ddlCommandList =
		CopyShardCommandList(shardInterval, sourceNodeName, sourceNodePort, includeData);

	List *foreignConstraintCommandList = CopyShardForeignConstraintCommandList(
		shardInterval);
	ddlCommandList = list_concat(ddlCommandList, foreignConstraintCommandList);

	/*
	 * CopyShardCommandList() drops the table which cascades to partitions if the
	 * table is a partitioned table. This means that we need to create both parent
	 * table and its partitions.
	 *
	 * We also skipped copying the data, so include it here.
	 */
	if (partitionedTable)
	{
		char *shardName = ConstructQualifiedShardName(shardInterval);
		StringInfo copyShardDataCommand = makeStringInfo();

		List *partitionCommandList =
			CopyPartitionShardsCommandList(shardInterval, sourceNodeName, sourceNodePort);
		ddlCommandList = list_concat(ddlCommandList, partitionCommandList);

		/* finally copy the data as well */
		appendStringInfo(copyShardDataCommand, WORKER_APPEND_TABLE_TO_SHARD,
						 quote_literal_cstr(shardName), /* table to append */
						 quote_literal_cstr(shardName), /* remote table name */
						 quote_literal_cstr(sourceNodeName), /* remote host */
						 sourceNodePort); /* remote port */
		ddlCommandList = lappend(ddlCommandList, copyShardDataCommand->data);
	}

	EnsureNoModificationsHaveBeenDone();
	SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort, tableOwner,
											  ddlCommandList);

	/* after successful repair, we update shard state as healthy*/
	List *placementList = ShardPlacementListWithoutOrphanedPlacements(shardId);
	ShardPlacement *placement = SearchShardPlacementInListOrError(placementList,
																  targetNodeName,
																  targetNodePort);
	UpdateShardPlacementState(placement->placementId, SHARD_STATE_ACTIVE);
}


/*
 * ReplicateColocatedShardPlacement replicates the given shard and its
 * colocated shards from a source node to target node.
 */
static void
ReplicateColocatedShardPlacement(int64 shardId, char *sourceNodeName,
								 int32 sourceNodePort, char *targetNodeName,
								 int32 targetNodePort, char shardReplicationMode)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *colocatedTableList = ColocatedTableList(distributedTableId);
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);

	EnsureTableListOwner(colocatedTableList);
	EnsureTableListSuitableForReplication(colocatedTableList);

	/*
	 * We sort shardIntervalList so that lock operations will not cause any
	 * deadlocks.
	 */
	colocatedShardList = SortList(colocatedShardList, CompareShardIntervalsById);

	/*
	 * At this point of the shard replication, we don't need to block the writes to
	 * shards when logical replication is used.
	 */
	bool useLogicalReplication = CanUseLogicalReplication(distributedTableId,
														  shardReplicationMode);
	if (!useLogicalReplication)
	{
		BlockWritesToShardList(colocatedShardList);
	}

	ShardInterval *colocatedShard = NULL;
	foreach_ptr(colocatedShard, colocatedShardList)
	{
		uint64 colocatedShardId = colocatedShard->shardId;

		/*
		 * For shard copy, there should be healthy placement in source node and no
		 * placement in the target node.
		 */
		EnsureShardCanBeCopied(colocatedShardId, sourceNodeName, sourceNodePort,
							   targetNodeName, targetNodePort);
	}

	if (shardReplicationMode == TRANSFER_MODE_AUTOMATIC)
	{
		VerifyTablesHaveReplicaIdentity(colocatedTableList);
	}

	if (!IsCitusTableType(distributedTableId, REFERENCE_TABLE))
	{
		/*
		 * When copying a shard to a new node, we should first ensure that reference
		 * tables are present such that joins work immediately after copying the shard.
		 * When copying a reference table, we are probably trying to achieve just that.
		 *
		 * Since this a long-running operation we do this after the error checks, but
		 * before taking metadata locks.
		 */
		EnsureReferenceTablesExistOnAllNodesExtended(shardReplicationMode);
	}

	CopyShardTables(colocatedShardList, sourceNodeName, sourceNodePort,
					targetNodeName, targetNodePort, useLogicalReplication);

	/*
	 * Finally insert the placements to pg_dist_placement and sync it to the
	 * metadata workers.
	 */
	foreach_ptr(colocatedShard, colocatedShardList)
	{
		uint64 colocatedShardId = colocatedShard->shardId;
		uint32 groupId = GroupForNode(targetNodeName, targetNodePort);
		uint64 placementId = GetNextPlacementId();

		InsertShardPlacementRow(colocatedShardId, placementId,
								SHARD_STATE_ACTIVE, ShardLength(colocatedShardId),
								groupId);

		if (ShouldSyncTableMetadata(colocatedShard->relationId))
		{
			char *placementCommand = PlacementUpsertCommand(colocatedShardId, placementId,
															SHARD_STATE_ACTIVE, 0,
															groupId);

			SendCommandToWorkersWithMetadata(placementCommand);
		}
	}
}


/*
 * EnsureTableListOwner ensures current user owns given tables. Superusers
 * are regarded as owners.
 */
static void
EnsureTableListOwner(List *tableIdList)
{
	Oid tableId = InvalidOid;
	foreach_oid(tableId, tableIdList)
	{
		EnsureTableOwner(tableId);
	}
}


/*
 * EnsureTableListSuitableForReplication errors out if given tables are not
 * suitable for replication.
 */
static void
EnsureTableListSuitableForReplication(List *tableIdList)
{
	Oid tableId = InvalidOid;
	foreach_oid(tableId, tableIdList)
	{
		if (IsForeignTable(tableId))
		{
			char *relationName = get_rel_name(tableId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot replicate shard"),
							errdetail("Table %s is a foreign table. Replicating "
									  "shards backed by foreign tables is "
									  "not supported.", relationName)));
		}

		List *foreignConstraintCommandList =
			GetReferencingForeignConstaintCommands(tableId);

		if (foreignConstraintCommandList != NIL &&
			IsCitusTableType(tableId, DISTRIBUTED_TABLE))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot replicate shards with foreign keys")));
		}
	}
}


/*
 * CopyShardTables copies a shard along with its co-located shards from a source
 * node to target node. It does not make any checks about state of the shards.
 * It is caller's responsibility to make those checks if they are necessary.
 */
static void
CopyShardTables(List *shardIntervalList, char *sourceNodeName, int32 sourceNodePort,
				char *targetNodeName, int32 targetNodePort, bool useLogicalReplication)
{
	if (list_length(shardIntervalList) < 1)
	{
		return;
	}

	if (useLogicalReplication)
	{
		CopyShardTablesViaLogicalReplication(shardIntervalList, sourceNodeName,
											 sourceNodePort, targetNodeName,
											 targetNodePort);
	}
	else
	{
		CopyShardTablesViaBlockWrites(shardIntervalList, sourceNodeName, sourceNodePort,
									  targetNodeName, targetNodePort);
	}
}


/*
 * CopyShardTablesViaLogicalReplication copies a shard along with its co-located shards
 * from a source node to target node via logical replication.
 */
static void
CopyShardTablesViaLogicalReplication(List *shardIntervalList, char *sourceNodeName,
									 int32 sourceNodePort, char *targetNodeName,
									 int32 targetNodePort)
{
	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CopyShardTablesViaLogicalReplication",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	/*
	 * Iterate through the colocated shards and create them on the
	 * target node. We do not create the indexes yet.
	 */
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		Oid relationId = shardInterval->relationId;
		uint64 shardId = shardInterval->shardId;
		List *tableRecreationCommandList = RecreateTableDDLCommandList(relationId);
		tableRecreationCommandList =
			WorkerApplyShardDDLCommandList(tableRecreationCommandList, shardId);

		char *tableOwner = TableOwner(shardInterval->relationId);

		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner,
												  tableRecreationCommandList);

		MemoryContextReset(localContext);
	}

	MemoryContextSwitchTo(oldContext);

	/* data copy is done seperately when logical replication is used */
	LogicallyReplicateShards(shardIntervalList, sourceNodeName,
							 sourceNodePort, targetNodeName, targetNodePort);
}


/*
 * CreateShardCommandList creates a struct for shard interval
 * along with DDL commands to be executed.
 */
static ShardCommandList *
CreateShardCommandList(ShardInterval *shardInterval, List *ddlCommandList)
{
	ShardCommandList *shardCommandList = palloc0(
		sizeof(ShardCommandList));
	shardCommandList->shardInterval = shardInterval;
	shardCommandList->ddlCommandList = ddlCommandList;

	return shardCommandList;
}


/*
 * CopyShardTablesViaBlockWrites copies a shard along with its co-located shards
 * from a source node to target node via COPY command. While the command is in
 * progress, the modifications on the source node is blocked.
 */
static void
CopyShardTablesViaBlockWrites(List *shardIntervalList, char *sourceNodeName,
							  int32 sourceNodePort, char *targetNodeName,
							  int32 targetNodePort)
{
	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CopyShardTablesViaBlockWrites",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	/* iterate through the colocated shards and copy each */
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		/*
		 * For each shard we first create the shard table in a separate
		 * transaction and then we copy the data and create the indexes in a
		 * second separate transaction. The reason we don't do both in a single
		 * transaction is so we can see the size of the new shard growing
		 * during the copy when we run get_rebalance_progress in another
		 * session. If we wouldn't split these two phases up, then the table
		 * wouldn't be visible in the session that get_rebalance_progress uses.
		 * So get_rebalance_progress would always report its size as 0.
		 */
		List *ddlCommandList = RecreateShardDDLCommandList(shardInterval, sourceNodeName,
														   sourceNodePort);
		char *tableOwner = TableOwner(shardInterval->relationId);
		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner, ddlCommandList);

		ddlCommandList = NIL;

		/*
		 * Skip copying data for partitioned tables, because they contain no
		 * data themselves. Their partitions do contain data, but those are
		 * different colocated shards that will be copied seperately.
		 */
		if (!PartitionedTable(shardInterval->relationId))
		{
			ddlCommandList = CopyShardContentsCommandList(shardInterval, sourceNodeName,
														  sourceNodePort);
		}
		ddlCommandList = list_concat(
			ddlCommandList,
			PostLoadShardCreationCommandList(shardInterval, sourceNodeName,
											 sourceNodePort));
		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner, ddlCommandList);

		MemoryContextReset(localContext);
	}

	/*
	 * Once all shards are copied, we can recreate relationships between shards.
	 * Create DDL commands to Attach child tables to their parents in a partitioning hierarchy.
	 */
	List *shardIntervalWithDDCommandsList = NIL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		if (PartitionTable(shardInterval->relationId))
		{
			char *attachPartitionCommand =
				GenerateAttachShardPartitionCommand(shardInterval);

			ShardCommandList *shardCommandList = CreateShardCommandList(
				shardInterval,
				list_make1(attachPartitionCommand));
			shardIntervalWithDDCommandsList = lappend(shardIntervalWithDDCommandsList,
													  shardCommandList);
		}
	}

	/*
	 * Iterate through the colocated shards and create DDL commamnds
	 * to create the foreign constraints.
	 */
	foreach_ptr(shardInterval, shardIntervalList)
	{
		List *shardForeignConstraintCommandList = NIL;
		List *referenceTableForeignConstraintList = NIL;

		CopyShardForeignConstraintCommandListGrouped(shardInterval,
													 &shardForeignConstraintCommandList,
													 &referenceTableForeignConstraintList);

		ShardCommandList *shardCommandList = CreateShardCommandList(
			shardInterval,
			list_concat(shardForeignConstraintCommandList,
						referenceTableForeignConstraintList));
		shardIntervalWithDDCommandsList = lappend(shardIntervalWithDDCommandsList,
												  shardCommandList);
	}

	/* Now execute the Partitioning & Foreign constraints creation commads. */
	ShardCommandList *shardCommandList = NULL;
	foreach_ptr(shardCommandList, shardIntervalWithDDCommandsList)
	{
		char *tableOwner = TableOwner(shardCommandList->shardInterval->relationId);
		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner,
												  shardCommandList->ddlCommandList);
	}

	MemoryContextReset(localContext);
	MemoryContextSwitchTo(oldContext);
}


/*
 * CopyPartitionShardsCommandList gets a shardInterval which is a shard that
 * belongs to partitioned table (this is asserted).
 *
 * The function returns a list of commands which re-creates all the partitions
 * of the input shardInterval.
 */
static List *
CopyPartitionShardsCommandList(ShardInterval *shardInterval, const char *sourceNodeName,
							   int32 sourceNodePort)
{
	Oid distributedTableId = shardInterval->relationId;
	List *ddlCommandList = NIL;

	Assert(PartitionedTableNoLock(distributedTableId));

	List *partitionList = PartitionList(distributedTableId);
	Oid partitionOid = InvalidOid;
	foreach_oid(partitionOid, partitionList)
	{
		uint64 partitionShardId =
			ColocatedShardIdInRelation(partitionOid, shardInterval->shardIndex);
		ShardInterval *partitionShardInterval = LoadShardInterval(partitionShardId);
		bool includeData = false;

		List *copyCommandList =
			CopyShardCommandList(partitionShardInterval, sourceNodeName, sourceNodePort,
								 includeData);
		ddlCommandList = list_concat(ddlCommandList, copyCommandList);

		char *attachPartitionCommand =
			GenerateAttachShardPartitionCommand(partitionShardInterval);
		ddlCommandList = lappend(ddlCommandList, attachPartitionCommand);
	}

	return ddlCommandList;
}


/*
 * EnsureShardCanBeRepaired checks if the given shard has a healthy placement in the source
 * node and inactive node on the target node.
 */
static void
EnsureShardCanBeRepaired(int64 shardId, const char *sourceNodeName, int32 sourceNodePort,
						 const char *targetNodeName, int32 targetNodePort)
{
	List *shardPlacementList =
		ShardPlacementListIncludingOrphanedPlacements(shardId);

	ShardPlacement *sourcePlacement = SearchShardPlacementInListOrError(
		shardPlacementList,
		sourceNodeName,
		sourceNodePort);
	if (sourcePlacement->shardState != SHARD_STATE_ACTIVE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source placement must be in active state")));
	}

	ShardPlacement *targetPlacement = SearchShardPlacementInListOrError(
		shardPlacementList,
		targetNodeName,
		targetNodePort);

	/*
	 * shardStateInactive is a legacy state for a placement. As of Citus 11,
	 * we never mark any placement as INACTIVE.
	 *
	 * Still, we prefer to keep this function/code here, as users may need
	 * to recover placements that are marked as inactive pre Citus 11.
	 *
	 */
	int shardStateInactive = 3;
	if (targetPlacement->shardState != shardStateInactive)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("target placement must be in inactive state")));
	}
}


/*
 * EnsureShardCanBeCopied checks if the given shard has a healthy placement in the source
 * node and no placements in the target node.
 */
static void
EnsureShardCanBeCopied(int64 shardId, const char *sourceNodeName, int32 sourceNodePort,
					   const char *targetNodeName, int32 targetNodePort)
{
	List *shardPlacementList = ShardPlacementListIncludingOrphanedPlacements(shardId);

	ShardPlacement *sourcePlacement = SearchShardPlacementInListOrError(
		shardPlacementList,
		sourceNodeName,
		sourceNodePort);
	if (sourcePlacement->shardState != SHARD_STATE_ACTIVE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source placement must be in active state")));
	}

	ShardPlacement *targetPlacement = SearchShardPlacementInList(shardPlacementList,
																 targetNodeName,
																 targetNodePort);
	if (targetPlacement != NULL)
	{
		if (targetPlacement->shardState == SHARD_STATE_TO_DELETE)
		{
			/*
			 * Trigger deletion of orphaned shards and hope that this removes
			 * the shard.
			 */
			DropOrphanedShardsInSeparateTransaction();
			shardPlacementList = ShardPlacementListIncludingOrphanedPlacements(shardId);
			targetPlacement = SearchShardPlacementInList(shardPlacementList,
														 targetNodeName,
														 targetNodePort);

			/*
			 * If it still doesn't remove the shard, then we error.
			 */
			if (targetPlacement != NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg(
									"shard " INT64_FORMAT
									" still exists on the target node as an orphaned shard",
									shardId),
								errdetail(
									"The existing shard is orphaned, but could not be deleted because there are still active queries on it")));
			}
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg(
								"shard " INT64_FORMAT " already exists in the target node",
								shardId)));
		}
	}
}


/*
 * SearchShardPlacementInList searches a provided list for a shard placement with the
 * specified node name and port. This function returns NULL if no such
 * placement exists in the provided list.
 */
ShardPlacement *
SearchShardPlacementInList(List *shardPlacementList, const char *nodeName,
						   uint32 nodePort)
{
	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, shardPlacementList)
	{
		if (strncmp(nodeName, shardPlacement->nodeName, MAX_NODE_LENGTH) == 0 &&
			nodePort == shardPlacement->nodePort)
		{
			return shardPlacement;
		}
	}
	return NULL;
}


/*
 * SearchShardPlacementInListOrError searches a provided list for a shard
 * placement with the specified node name and port. This function throws an
 * error if no such placement exists in the provided list.
 *
 * This is a separate function (instead of using missingOk), so static analysis
 * reasons about NULL returns correctly.
 */
ShardPlacement *
SearchShardPlacementInListOrError(List *shardPlacementList, const char *nodeName,
								  uint32 nodePort)
{
	ShardPlacement *placement = SearchShardPlacementInList(shardPlacementList, nodeName,
														   nodePort);
	if (placement == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("could not find placement matching \"%s:%d\"",
							   nodeName, nodePort),
						errhint("Confirm the placement still exists and try again.")));
	}
	return placement;
}


/*
 * CopyShardCommandList generates command list to copy the given shard placement
 * from the source node to the target node. To do this it recreates the shard
 * on the target, and then copies the data. Caller could optionally skip
 * copying the data by the flag includeDataCopy.
 */
List *
CopyShardCommandList(ShardInterval *shardInterval, const char *sourceNodeName,
					 int32 sourceNodePort, bool includeDataCopy)
{
	List *copyShardToNodeCommandsList = RecreateShardDDLCommandList(
		shardInterval, sourceNodeName, sourceNodePort);
	if (includeDataCopy)
	{
		copyShardToNodeCommandsList = list_concat(
			copyShardToNodeCommandsList,
			CopyShardContentsCommandList(shardInterval, sourceNodeName,
										 sourceNodePort));
	}
	return list_concat(copyShardToNodeCommandsList,
					   PostLoadShardCreationCommandList(shardInterval, sourceNodeName,
														sourceNodePort));
}


/*
 * RecreateShardDDLCommandList generates a command list to recreate a shard,
 * but without any data init and without the post-load table creation commands.
 */
static List *
RecreateShardDDLCommandList(ShardInterval *shardInterval, const char *sourceNodeName,
							int32 sourceNodePort)
{
	int64 shardId = shardInterval->shardId;
	Oid relationId = shardInterval->relationId;

	List *tableRecreationCommandList = RecreateTableDDLCommandList(relationId);
	return WorkerApplyShardDDLCommandList(tableRecreationCommandList, shardId);
}


/*
 * CopyShardContentsCommandList generates a command list to copy the data of the
 * given shard placement from the source node to the target node. This copying
 * requires a precreated table for the shard on the target node to have been
 * created already (using RecreateShardDDLCommandList).
 */
static List *
CopyShardContentsCommandList(ShardInterval *shardInterval, const char *sourceNodeName,
							 int32 sourceNodePort)
{
	char *shardName = ConstructQualifiedShardName(shardInterval);
	StringInfo copyShardDataCommand = makeStringInfo();
	appendStringInfo(copyShardDataCommand, WORKER_APPEND_TABLE_TO_SHARD,
					 quote_literal_cstr(shardName), /* table to append */
					 quote_literal_cstr(shardName), /* remote table name */
					 quote_literal_cstr(sourceNodeName), /* remote host */
					 sourceNodePort); /* remote port */

	return list_make1(copyShardDataCommand->data);
}


/*
 * PostLoadShardCreationCommandList generates a command list to finalize the
 * creation of a shard after the data has been loaded. This creates stuff like
 * the indexes on the table.
 */
static List *
PostLoadShardCreationCommandList(ShardInterval *shardInterval, const char *sourceNodeName,
								 int32 sourceNodePort)
{
	int64 shardId = shardInterval->shardId;
	Oid relationId = shardInterval->relationId;
	bool includeReplicaIdentity = true;
	List *indexCommandList =
		GetPostLoadTableCreationCommands(relationId, true, includeReplicaIdentity);
	return WorkerApplyShardDDLCommandList(indexCommandList, shardId);
}


/*
 * CopyShardForeignConstraintCommandList generates command list to create foreign
 * constraints existing in source shard after copying it to the other node.
 */
List *
CopyShardForeignConstraintCommandList(ShardInterval *shardInterval)
{
	List *colocatedShardForeignConstraintCommandList = NIL;
	List *referenceTableForeignConstraintList = NIL;

	CopyShardForeignConstraintCommandListGrouped(shardInterval,
												 &
												 colocatedShardForeignConstraintCommandList,
												 &referenceTableForeignConstraintList);

	return list_concat(colocatedShardForeignConstraintCommandList,
					   referenceTableForeignConstraintList);
}


/*
 * CopyShardForeignConstraintCommandListGrouped generates command lists
 * to create foreign constraints existing in source shard after copying it to other
 * node in separate groups for foreign constraints in between hash distributed tables
 * and from a hash distributed to reference tables.
 */
void
CopyShardForeignConstraintCommandListGrouped(ShardInterval *shardInterval,
											 List **
											 colocatedShardForeignConstraintCommandList,
											 List **referenceTableForeignConstraintList)
{
	Oid relationId = shardInterval->relationId;
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	int shardIndex = 0;

	List *commandList = GetReferencingForeignConstaintCommands(relationId);

	/* we will only use shardIndex if there is a foreign constraint */
	if (commandList != NIL)
	{
		shardIndex = ShardIndex(shardInterval);
	}

	*colocatedShardForeignConstraintCommandList = NIL;
	*referenceTableForeignConstraintList = NIL;

	const char *command = NULL;
	foreach_ptr(command, commandList)
	{
		char *escapedCommand = quote_literal_cstr(command);

		uint64 referencedShardId = INVALID_SHARD_ID;
		bool colocatedForeignKey = false;

		StringInfo applyForeignConstraintCommand = makeStringInfo();

		/* we need to parse the foreign constraint command to get referenced table id */
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

		if (IsCitusTableType(referencedRelationId, REFERENCE_TABLE))
		{
			referencedShardId = GetFirstShardId(referencedRelationId);
		}
		else if (IsCitusTableType(referencedRelationId, CITUS_LOCAL_TABLE))
		{
			/*
			 * Only reference tables and citus local tables can have foreign
			 * keys to citus local tables but we already do not allow copying
			 * citus local table shards and we don't try to replicate citus
			 * local table shards. So, the referencing table must be a reference
			 * table in this context.
			 */
			Assert(IsCitusTableType(relationId, REFERENCE_TABLE));

			/*
			 * We don't set foreign keys from reference tables to citus local
			 * tables in worker shard placements of reference tables because
			 * we don't have the shard placement for citus local table in worker
			 * nodes.
			 */
			continue;
		}
		else
		{
			referencedShardId = ColocatedShardIdInRelation(referencedRelationId,
														   shardIndex);

			colocatedForeignKey = true;
		}

		appendStringInfo(applyForeignConstraintCommand,
						 WORKER_APPLY_INTER_SHARD_DDL_COMMAND, shardInterval->shardId,
						 escapedSchemaName, referencedShardId,
						 escapedReferencedSchemaName, escapedCommand);

		if (colocatedForeignKey)
		{
			*colocatedShardForeignConstraintCommandList = lappend(
				*colocatedShardForeignConstraintCommandList,
				applyForeignConstraintCommand->data);
		}
		else
		{
			*referenceTableForeignConstraintList = lappend(
				*referenceTableForeignConstraintList,
				applyForeignConstraintCommand->data);
		}
	}
}


/*
 * GetFirstShardId is a helper function which returns the first
 * shardId of the given distributed relation. The function doesn't
 * sort the shardIds, so it is mostly useful for reference tables.
 */
uint64
GetFirstShardId(Oid relationId)
{
	List *shardList = LoadShardList(relationId);
	uint64 *shardIdPointer = (uint64 *) linitial(shardList);

	return (*shardIdPointer);
}


/*
 * ConstuctQualifiedShardName creates the fully qualified name string of the
 * given shard in <schema>.<table_name>_<shard_id> format.
 */
char *
ConstructQualifiedShardName(ShardInterval *shardInterval)
{
	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(shardInterval->relationId);

	char *shardName = pstrdup(tableName);
	AppendShardIdToName(&shardName, shardInterval->shardId);
	shardName = quote_qualified_identifier(schemaName, shardName);

	return shardName;
}


/*
 * RecreateTableDDLCommandList returns a list of DDL statements similar to that
 * returned by GetTableCreationCommands except that the list begins with a "DROP TABLE"
 * or "DROP FOREIGN TABLE" statement to facilitate idempotent recreation of a placement.
 */
static List *
RecreateTableDDLCommandList(Oid relationId)
{
	const char *relationName = get_rel_name(relationId);
	Oid relationSchemaId = get_rel_namespace(relationId);
	const char *relationSchemaName = get_namespace_name(relationSchemaId);
	const char *qualifiedRelationName = quote_qualified_identifier(relationSchemaName,
																   relationName);

	StringInfo dropCommand = makeStringInfo();

	IncludeSequenceDefaults includeSequenceDefaults = NO_SEQUENCE_DEFAULTS;

	/* build appropriate DROP command based on relation kind */
	if (RegularTable(relationId))
	{
		appendStringInfo(dropCommand, DROP_REGULAR_TABLE_COMMAND,
						 qualifiedRelationName);
	}
	else if (IsForeignTable(relationId))
	{
		appendStringInfo(dropCommand, DROP_FOREIGN_TABLE_COMMAND,
						 qualifiedRelationName);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("repair target is not a regular, foreign or partitioned "
							   "table")));
	}

	List *dropCommandList = list_make1(makeTableDDLCommandString(dropCommand->data));
	List *createCommandList = GetPreLoadTableCreationCommands(relationId,
															  includeSequenceDefaults,
															  NULL);
	List *recreateCommandList = list_concat(dropCommandList, createCommandList);

	return recreateCommandList;
}


/*
 * DropColocatedShardPlacement deletes the shard placement metadata for the given shard
 * placement from the pg_dist_placement, and then it drops the shard table
 * from the given node. The function does this for all colocated placements.
 */
static void
DropColocatedShardPlacement(ShardInterval *shardInterval, char *nodeName, int32 nodePort)
{
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
	ListCell *colocatedShardCell = NULL;

	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		char *qualifiedTableName = ConstructQualifiedShardName(colocatedShard);
		StringInfo dropQuery = makeStringInfo();
		uint64 shardId = colocatedShard->shardId;
		List *shardPlacementList =
			ShardPlacementListIncludingOrphanedPlacements(shardId);
		ShardPlacement *placement =
			SearchShardPlacementInListOrError(shardPlacementList, nodeName, nodePort);

		appendStringInfo(dropQuery, DROP_REGULAR_TABLE_COMMAND, qualifiedTableName);

		DeleteShardPlacementRow(placement->placementId);
		SendCommandToWorker(nodeName, nodePort, dropQuery->data);
	}
}


/*
 * MarkForDropColocatedShardPlacement marks the shard placement metadata for
 * the given shard placement to be deleted in pg_dist_placement. The function
 * does this for all colocated placements.
 */
static void
MarkForDropColocatedShardPlacement(ShardInterval *shardInterval, char *nodeName, int32
								   nodePort)
{
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
	ListCell *colocatedShardCell = NULL;

	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		uint64 shardId = colocatedShard->shardId;
		List *shardPlacementList =
			ShardPlacementListIncludingOrphanedPlacements(shardId);
		ShardPlacement *placement =
			SearchShardPlacementInListOrError(shardPlacementList, nodeName, nodePort);

		UpdateShardPlacementState(placement->placementId, SHARD_STATE_TO_DELETE);
	}
}


/*
 * UpdateColocatedShardPlacementMetadataOnWorkers updates the metadata about the
 * placements of the given shard and its colocated shards by changing the nodename and
 * nodeport of the shards from the source nodename/port to target nodename/port.
 *
 * Note that the function does nothing if the given shard belongs to a non-mx table.
 */
static void
UpdateColocatedShardPlacementMetadataOnWorkers(int64 shardId,
											   char *sourceNodeName, int32 sourceNodePort,
											   char *targetNodeName, int32 targetNodePort)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	ListCell *colocatedShardCell = NULL;
	bool shouldSyncMetadata = ShouldSyncTableMetadata(shardInterval->relationId);

	if (!shouldSyncMetadata)
	{
		return;
	}

	uint32 sourceGroupId = GroupForNode(sourceNodeName, sourceNodePort);
	uint32 targetGroupId = GroupForNode(targetNodeName, targetNodePort);

	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);

	/* iterate through the colocated shards and copy each */
	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		StringInfo updateCommand = makeStringInfo();

		appendStringInfo(updateCommand,
						 "SELECT citus_internal_update_placement_metadata(%ld, %d, %d)",
						 colocatedShard->shardId,
						 sourceGroupId, targetGroupId);
		SendCommandToWorkersWithMetadata(updateCommand->data);
	}
}


/*
 * WorkerApplyShardDDLCommandList wraps all DDL commands in ddlCommandList
 * in a call to worker_apply_shard_ddl_command to apply the DDL command to
 * the shard specified by shardId.
 */
List *
WorkerApplyShardDDLCommandList(List *ddlCommandList, int64 shardId)
{
	List *applyDDLCommandList = NIL;

	TableDDLCommand *ddlCommand = NULL;
	foreach_ptr(ddlCommand, ddlCommandList)
	{
		Assert(CitusIsA(ddlCommand, TableDDLCommand));
		char *applyDDLCommand = GetShardedTableDDLCommand(ddlCommand, shardId, NULL);
		applyDDLCommandList = lappend(applyDDLCommandList, applyDDLCommand);
	}

	return applyDDLCommandList;
}
