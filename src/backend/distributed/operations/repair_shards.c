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
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
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

/* local function forward declarations */
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
							int32 targetNodePort);
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

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_copy_shard_placement);
PG_FUNCTION_INFO_V1(master_copy_shard_placement);
PG_FUNCTION_INFO_V1(citus_move_shard_placement);
PG_FUNCTION_INFO_V1(master_move_shard_placement);


bool DeferShardDeleteOnMove = false;


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
	int64 shardId = PG_GETARG_INT64(0);
	text *sourceNodeNameText = PG_GETARG_TEXT_P(1);
	int32 sourceNodePort = PG_GETARG_INT32(2);
	text *targetNodeNameText = PG_GETARG_TEXT_P(3);
	int32 targetNodePort = PG_GETARG_INT32(4);
	bool doRepair = PG_GETARG_BOOL(5);
	Oid shardReplicationModeOid = PG_GETARG_OID(6);

	char *sourceNodeName = text_to_cstring(sourceNodeNameText);
	char *targetNodeName = text_to_cstring(targetNodeNameText);

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);
	if (shardReplicationMode == TRANSFER_MODE_FORCE_LOGICAL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("the force_logical transfer mode is currently "
							   "unsupported")));
	}

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	ErrorIfTableCannotBeReplicated(shardInterval->relationId);

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
 * citus_move_shard_placement moves given shard (and its co-located shards) from one
 * node to the other node. To accomplish this it entirely recreates the table structure
 * before copying all data.
 *
 * After that, there are two different paths. First one is blocking shard move in the
 * sense that during shard move all modifications are paused to the shard. The second
 * one relies on logical replication meaning that the writes blocked only for a very
 * short duration almost only when the metadata is actually being updated. This option
 * is currently only available in Citus Enterprise.
 *
 * After successful move operation, shards in the source node gets deleted. If the move
 * fails at any point, this function throws an error, leaving the cluster without doing
 * any changes in source node or target node.
 */
Datum
citus_move_shard_placement(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	char *sourceNodeName = text_to_cstring(PG_GETARG_TEXT_P(1));
	int32 sourceNodePort = PG_GETARG_INT32(2);
	char *targetNodeName = text_to_cstring(PG_GETARG_TEXT_P(3));
	int32 targetNodePort = PG_GETARG_INT32(4);
	Oid shardReplicationModeOid = PG_GETARG_OID(5);


	ListCell *colocatedTableCell = NULL;
	ListCell *colocatedShardCell = NULL;


	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid relationId = RelationIdForShard(shardId);
	ErrorIfMoveCitusLocalTable(relationId);

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *colocatedTableList = ColocatedTableList(distributedTableId);
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);

	foreach(colocatedTableCell, colocatedTableList)
	{
		Oid colocatedTableId = lfirst_oid(colocatedTableCell);
		char relationKind = '\0';

		/* check that user has owner rights in all co-located tables */
		EnsureTableOwner(colocatedTableId);

		/*
		 * Block concurrent DDL / TRUNCATE commands on the relation. Similarly,
		 * block concurrent citus_move_shard_placement() on any shard of
		 * the same relation. This is OK for now since we're executing shard
		 * moves sequentially anyway.
		 */
		LockRelationOid(colocatedTableId, ShareUpdateExclusiveLock);

		relationKind = get_rel_relkind(colocatedTableId);
		if (relationKind == RELKIND_FOREIGN_TABLE)
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
	if (shardReplicationMode == TRANSFER_MODE_FORCE_LOGICAL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("the force_logical transfer mode is currently "
							   "unsupported")));
	}

	BlockWritesToShardList(colocatedShardList);

	/*
	 * CopyColocatedShardPlacement function copies given shard with its co-located
	 * shards.
	 */
	CopyShardTables(colocatedShardList, sourceNodeName, sourceNodePort, targetNodeName,
					targetNodePort);

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
 * master_move_shard_placement is a wrapper function for old UDF name.
 */
Datum
master_move_shard_placement(PG_FUNCTION_ARGS)
{
	return citus_move_shard_placement(fcinfo);
}


/*
 * ErrorIfMoveCitusLocalTable is a helper function for rebalance_table_shards
 * and citus_move_shard_placement udf's to error out if relation with relationId
 * is a citus local table.
 */
void
ErrorIfMoveCitusLocalTable(Oid relationId)
{
	if (!IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		return;
	}

	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("table %s is a local table, moving shard of "
						   "a local table added to metadata is currently "
						   "not supported", qualifiedRelationName)));
}


/*
 * BlockWritesToColocatedShardList blocks writes to all shards in the given shard
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
	if (shouldSyncMetadata)
	{
		LockShardListMetadataOnWorkers(ExclusiveLock, shardList);
	}
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

	char relationKind = get_rel_relkind(distributedTableId);
	char *tableOwner = TableOwner(shardInterval->relationId);

	/* prevent table from being dropped */
	LockRelationOid(distributedTableId, AccessShareLock);

	EnsureTableOwner(distributedTableId);

	if (relationKind == RELKIND_FOREIGN_TABLE)
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
	SendCommandListToWorkerInSingleTransaction(targetNodeName, targetNodePort, tableOwner,
											   ddlCommandList);

	/* after successful repair, we update shard state as healthy*/
	List *placementList = ShardPlacementList(shardId);
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

	BlockWritesToShardList(colocatedShardList);

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
					targetNodeName, targetNodePort);

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
		char relationKind = get_rel_relkind(tableId);
		if (relationKind == RELKIND_FOREIGN_TABLE)
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
 * CopyColocatedShardPlacement copies a shard along with its co-located shards
 * from a source node to target node. It does not make any checks about state
 * of the shards. It is caller's responsibility to make those checks if they are
 * necessary.
 */
static void
CopyShardTables(List *shardIntervalList, char *sourceNodeName, int32 sourceNodePort,
				char *targetNodeName, int32 targetNodePort)
{
	ShardInterval *shardInterval = NULL;

	/* iterate through the colocated shards and copy each */
	foreach_ptr(shardInterval, shardIntervalList)
	{
		bool includeDataCopy = true;

		if (PartitionedTable(shardInterval->relationId))
		{
			/* partitioned tables contain no data */
			includeDataCopy = false;
		}

		List *ddlCommandList = CopyShardCommandList(shardInterval, sourceNodeName,
													sourceNodePort, includeDataCopy);
		char *tableOwner = TableOwner(shardInterval->relationId);

		SendCommandListToWorkerInSingleTransaction(targetNodeName, targetNodePort,
												   tableOwner, ddlCommandList);
	}


	/*
	 * Once all shards are created, we can recreate relationships between shards.
	 *
	 * Iterate through the colocated shards and create the foreign constraints and
	 * attach child tables to their parents in a partitioning hierarchy.
	 */
	foreach_ptr(shardInterval, shardIntervalList)
	{
		List *shardForeignConstraintCommandList = NIL;
		List *referenceTableForeignConstraintList = NIL;

		char *tableOwner = TableOwner(shardInterval->relationId);

		CopyShardForeignConstraintCommandListGrouped(shardInterval,
													 &shardForeignConstraintCommandList,
													 &referenceTableForeignConstraintList);

		List *commandList = list_concat(shardForeignConstraintCommandList,
										referenceTableForeignConstraintList);

		if (PartitionTable(shardInterval->relationId))
		{
			char *attachPartitionCommand =
				GenerateAttachShardPartitionCommand(shardInterval);

			commandList = lappend(commandList, attachPartitionCommand);
		}

		SendCommandListToWorkerInSingleTransaction(targetNodeName, targetNodePort,
												   tableOwner, commandList);
	}
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
	List *shardPlacementList = ShardPlacementList(shardId);

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
	if (targetPlacement->shardState != SHARD_STATE_INACTIVE)
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
	List *shardPlacementList = ShardPlacementList(shardId);

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
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shard " INT64_FORMAT " already exists in the target node",
							   shardId)));
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
 * from the source node to the target node. Caller could optionally skip copying
 * the data by the flag includeDataCopy.
 */
List *
CopyShardCommandList(ShardInterval *shardInterval, const char *sourceNodeName,
					 int32 sourceNodePort, bool includeDataCopy)
{
	int64 shardId = shardInterval->shardId;
	char *shardName = ConstructQualifiedShardName(shardInterval);
	List *copyShardToNodeCommandsList = NIL;
	StringInfo copyShardDataCommand = makeStringInfo();
	Oid relationId = shardInterval->relationId;

	List *tableRecreationCommandList = RecreateTableDDLCommandList(relationId);
	tableRecreationCommandList =
		WorkerApplyShardDDLCommandList(tableRecreationCommandList, shardId);

	copyShardToNodeCommandsList = list_concat(copyShardToNodeCommandsList,
											  tableRecreationCommandList);

	if (includeDataCopy)
	{
		appendStringInfo(copyShardDataCommand, WORKER_APPEND_TABLE_TO_SHARD,
						 quote_literal_cstr(shardName), /* table to append */
						 quote_literal_cstr(shardName), /* remote table name */
						 quote_literal_cstr(sourceNodeName), /* remote host */
						 sourceNodePort); /* remote port */

		copyShardToNodeCommandsList = lappend(copyShardToNodeCommandsList,
											  copyShardDataCommand->data);
	}

	List *indexCommandList = GetPostLoadTableCreationCommands(relationId, true);
	indexCommandList = WorkerApplyShardDDLCommandList(indexCommandList, shardId);

	copyShardToNodeCommandsList = list_concat(copyShardToNodeCommandsList,
											  indexCommandList);

	return copyShardToNodeCommandsList;
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
	char relationKind = get_rel_relkind(relationId);
	bool includeSequenceDefaults = false;

	/* build appropriate DROP command based on relation kind */
	if (RegularTable(relationId))
	{
		appendStringInfo(dropCommand, DROP_REGULAR_TABLE_COMMAND,
						 qualifiedRelationName);
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
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
		List *shardPlacementList = ShardPlacementList(shardId);
		ShardPlacement *placement =
			SearchShardPlacementInListOrError(shardPlacementList, nodeName, nodePort);

		appendStringInfo(dropQuery, DROP_REGULAR_TABLE_COMMAND, qualifiedTableName);

		DeleteShardPlacementRow(placement->placementId);
		SendCommandToWorker(nodeName, nodePort, dropQuery->data);
	}
}


/*
 * MarkForDropColocatedShardPlacement marks the shard placement metadata for the given
 * shard placement to be deleted in pg_dist_placement. The function does this for all
 * colocated placements.
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
		List *shardPlacementList = ShardPlacementList(shardId);
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

	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);

	/* iterate through the colocated shards and copy each */
	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		StringInfo updateCommand = makeStringInfo();

		appendStringInfo(updateCommand, "UPDATE pg_dist_shard_placement "
										"SET nodename=%s, nodeport=%d WHERE "
										"shardid=%lu AND nodename=%s AND nodeport=%d",
						 quote_literal_cstr(targetNodeName),
						 targetNodePort,
						 colocatedShard->shardId,
						 quote_literal_cstr(sourceNodeName),
						 sourceNodePort);

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
