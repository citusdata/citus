/*-------------------------------------------------------------------------
 *
 * master_repair_shards.c
 *
 * This file contains functions to repair unhealthy shard placements using data
 * from healthy ones.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"

#include <string.h>

#include "catalog/pg_class.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"

/* local function forward declarations */
static char LookupShardTransferMode(Oid shardReplicationModeOid);
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
static List * WorkerApplyShardDDLCommandList(List *ddlCommandList, int64 shardId);
static void EnsureTableListOwner(List *tableIdList);
static void EnsureTableListSuitableForReplication(List *tableIdList);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_copy_shard_placement);
PG_FUNCTION_INFO_V1(master_move_shard_placement);


/*
 * master_copy_shard_placement implements a user-facing UDF to repair data from
 * a healthy (source) node to an inactive (target) node. To accomplish this it
 * entirely recreates the table structure before copying all data. During this
 * time all modifications are paused to the shard. After successful repair, the
 * inactive placement is marked healthy and modifications may continue. If the
 * repair fails at any point, this function throws an error, leaving the node
 * in an unhealthy state. Please note that master_copy_shard_placement copies
 * given shard along with its co-located shards.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
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

	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);
	if (shardReplicationMode == TRANSFER_MODE_FORCE_LOGICAL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("using logical replication in "
							   "master_copy_shard_placement() requires Citus "
							   "Enterprise")));
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
 * master_move_shard_placement moves given shard (and its co-located shards) from one
 * node to the other node.
 */
Datum
master_move_shard_placement(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("master_move_shard_placement() is only supported on "
						   "Citus Enterprise")));
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
	if (shouldSyncMetadata)
	{
		LockShardListMetadataOnWorkers(ExclusiveLock, shardList);
	}
}


/*
 * ErrorIfTableCannotBeReplicated function errors out if the given table is not suitable
 * for its shard being replicated. Shard replications is not allowed only for MX tables,
 * since RF=1 is a must MX tables.
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

	CitusTableCacheEntryRef *tableRef = GetCitusTableCacheEntry(relationId);
	char replicationModel = tableRef->cacheEntry->replicationModel;
	ReleaseTableCacheEntry(tableRef);
	char *relationName = get_rel_name(relationId);

	/*
	 * ShouldSyncTableMetadata() returns true also for reference table,
	 * we don't want to error in that case since reference tables aren't
	 * automatically replicated to active nodes with no shards, and
	 * master_copy_shard_placement() can be used to create placements in
	 * such nodes.
	 */
	if (replicationModel == REPLICATION_MODEL_STREAMING)
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
static char
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
	ShardPlacement *placement = ForceSearchShardPlacementInList(placementList,
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

	if (!IsReferenceTable(distributedTableId))
	{
		/*
		 * When copying a shard to a new node, we should first ensure that reference
		 * tables are present such that joins work immediately after copying the shard.
		 * When copying a reference table, we are probably trying to achieve just that.
		 *
		 * Since this a long-running operation we do this after the error checks, but
		 * before taking metadata locks.
		 */
		EnsureReferenceTablesExistOnAllNodes();
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
			GetTableForeignConstraintCommands(tableId);

		if (foreignConstraintCommandList != NIL &&
			PartitionMethod(tableId) != DISTRIBUTE_BY_NONE)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint"),
							errdetail("This shard has foreign constraints on it. "
									  "Citus currently supports "
									  "foreign key constraints only for "
									  "\"citus.shard_replication_factor = 1\"."),
							errhint("Please change \"citus.shard_replication_factor to "
									"1\". To learn more about using foreign keys with "
									"other replication factors, please contact us at "
									"https://citusdata.com/about/contact_us.")));
		}
	}
}


/*
 * CopyColocatedShardPlacement copies a shard along with its co-located shards from a
 * source node to target node. CopyShardPlacement does not make any checks about state
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
	 *
	 * Note: After implementing foreign constraints from distributed to reference
	 * tables, we have decided to not create foreign constraints from hash
	 * distributed to reference tables at this stage for nonblocking rebalancer.
	 * We just create the co-located ones here. We add the foreign constraints
	 * from hash distributed to reference tables after being completely done with
	 * the copy procedure inside LogicallyReplicateShards. The reason is that,
	 * the reference tables have placements in both source and target workers and
	 * the copied shard would get updated twice because of a cascading DML coming
	 * from both of the placements.
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

	ShardPlacement *sourcePlacement = ForceSearchShardPlacementInList(shardPlacementList,
																	  sourceNodeName,
																	  sourceNodePort);
	if (sourcePlacement->shardState != SHARD_STATE_ACTIVE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source placement must be in active state")));
	}

	ShardPlacement *targetPlacement = ForceSearchShardPlacementInList(shardPlacementList,
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

	ShardPlacement *sourcePlacement = ForceSearchShardPlacementInList(shardPlacementList,
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
 * ForceSearchShardPlacementInList searches a provided list for a shard
 * placement with the specified node name and port. This function throws an
 * error if no such placement exists in the provided list.
 *
 * This is a separate function (instead of using missingOk), so static analysis
 * reasons about NULL returns correctly.
 */
ShardPlacement *
ForceSearchShardPlacementInList(List *shardPlacementList, const char *nodeName,
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

	List *indexCommandList = GetTableIndexAndConstraintCommands(relationId);
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
	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	int shardIndex = 0;

	List *commandList = GetTableForeignConstraintCommands(shardInterval->relationId);

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

		if (PartitionMethod(referencedRelationId) == DISTRIBUTE_BY_NONE)
		{
			referencedShardId = GetFirstShardId(referencedRelationId);
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

	List *dropCommandList = list_make1(dropCommand->data);
	List *createCommandList = GetTableCreationCommands(relationId,
													   includeSequenceDefaults);
	List *recreateCommandList = list_concat(dropCommandList, createCommandList);

	return recreateCommandList;
}


/*
 * WorkerApplyShardDDLCommandList wraps all DDL commands in ddlCommandList
 * in a call to worker_apply_shard_ddl_command to apply the DDL command to
 * the shard specified by shardId.
 */
static List *
WorkerApplyShardDDLCommandList(List *ddlCommandList, int64 shardId)
{
	List *applyDdlCommandList = NIL;

	const char *ddlCommand = NULL;
	foreach_ptr(ddlCommand, ddlCommandList)
	{
		char *escapedDdlCommand = quote_literal_cstr(ddlCommand);

		StringInfo applyDdlCommand = makeStringInfo();
		appendStringInfo(applyDdlCommand,
						 WORKER_APPLY_SHARD_DDL_COMMAND_WITHOUT_SCHEMA,
						 shardId, escapedDdlCommand);

		applyDdlCommandList = lappend(applyDdlCommandList, applyDdlCommand->data);
	}

	return applyDdlCommandList;
}
