/*-------------------------------------------------------------------------
 *
 * shard_transfer.c
 *
 * This file contains functions to transfer shards between nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <string.h>
#include <sys/statvfs.h>

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_enum.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "distributed/adaptive_executor.h"
#include "distributed/backend_data.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_progress.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_split.h"
#include "distributed/shard_transfer.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"

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

static const char *ShardTransferTypeNames[] = {
	[SHARD_TRANSFER_INVALID_FIRST] = "unknown",
	[SHARD_TRANSFER_MOVE] = "move",
	[SHARD_TRANSFER_COPY] = "copy",
};

static const char *ShardTransferTypeNamesCapitalized[] = {
	[SHARD_TRANSFER_INVALID_FIRST] = "unknown",
	[SHARD_TRANSFER_MOVE] = "Move",
	[SHARD_TRANSFER_COPY] = "Copy",
};

static const char *ShardTransferTypeNamesContinuous[] = {
	[SHARD_TRANSFER_INVALID_FIRST] = "unknown",
	[SHARD_TRANSFER_MOVE] = "Moving",
	[SHARD_TRANSFER_COPY] = "Copying",
};

static const char *ShardTransferTypeFunctionNames[] = {
	[SHARD_TRANSFER_INVALID_FIRST] = "unknown",
	[SHARD_TRANSFER_MOVE] = "citus_move_shard_placement",
	[SHARD_TRANSFER_COPY] = "citus_copy_shard_placement",
};

/* local function forward declarations */
static bool CanUseLogicalReplication(Oid relationId, char shardReplicationMode);
static void ErrorIfTableCannotBeReplicated(Oid relationId);
static void ErrorIfTargetNodeIsNotSafeForTransfer(const char *targetNodeName,
												  int targetNodePort,
												  ShardTransferType transferType);
static void ErrorIfSameNode(char *sourceNodeName, int sourceNodePort,
							char *targetNodeName, int targetNodePort,
							const char *operationName);
static void CopyShardTables(List *shardIntervalList, char *sourceNodeName,
							int32 sourceNodePort, char *targetNodeName,
							int32 targetNodePort, bool useLogicalReplication,
							const char *operationName, uint32 optionFlags);
static void CopyShardTablesViaLogicalReplication(List *shardIntervalList,
												 char *sourceNodeName,
												 int32 sourceNodePort,
												 char *targetNodeName,
												 int32 targetNodePort,
												 uint32 optionFlags);

static void CopyShardTablesViaBlockWrites(List *shardIntervalList, char *sourceNodeName,
										  int32 sourceNodePort,
										  char *targetNodeName, int32 targetNodePort,
										  uint32 optionFlags);
static void EnsureShardCanBeCopied(int64 shardId, const char *sourceNodeName,
								   int32 sourceNodePort, const char *targetNodeName,
								   int32 targetNodePort);
static List * RecreateTableDDLCommandList(Oid relationId);
static void EnsureTableListOwner(List *tableIdList);
static void ErrorIfReplicatingDistributedTableWithFKeys(List *tableIdList);

static void DropShardPlacementsFromMetadata(List *shardList,
											char *nodeName,
											int32 nodePort);
static void UpdateColocatedShardPlacementMetadataOnWorkers(int64 shardId,
														   char *sourceNodeName,
														   int32 sourceNodePort,
														   char *targetNodeName,
														   int32 targetNodePort);
static bool IsShardListOnNode(List *colocatedShardList, char *targetNodeName,
							  uint32 targetPort);
static void SetupRebalanceMonitorForShardTransfer(uint64 shardId, Oid distributedTableId,
												  char *sourceNodeName,
												  uint32 sourceNodePort,
												  char *targetNodeName,
												  uint32 targetNodePort,
												  ShardTransferType transferType);
static void CheckSpaceConstraints(MultiConnection *connection,
								  uint64 colocationSizeInBytes);
static void EnsureAllShardsCanBeCopied(List *colocatedShardList,
									   char *sourceNodeName, uint32 sourceNodePort,
									   char *targetNodeName, uint32 targetNodePort);
static void EnsureEnoughDiskSpaceForShardMove(List *colocatedShardList,
											  char *sourceNodeName, uint32 sourceNodePort,
											  char *targetNodeName, uint32 targetNodePort,
											  ShardTransferType transferType);
static bool TransferAlreadyCompleted(List *colocatedShardList,
									 char *sourceNodeName, uint32 sourceNodePort,
									 char *targetNodeName, uint32 targetNodePort,
									 ShardTransferType transferType);
static void LockColocatedRelationsForMove(List *colocatedTableList);
static void ErrorIfForeignTableForShardTransfer(List *colocatedTableList,
												ShardTransferType transferType);
static List * RecreateShardDDLCommandList(ShardInterval *shardInterval,
										  const char *sourceNodeName,
										  int32 sourceNodePort);
static List * PostLoadShardCreationCommandList(ShardInterval *shardInterval,
											   const char *sourceNodeName,
											   int32 sourceNodePort);
static ShardCommandList * CreateShardCommandList(ShardInterval *shardInterval,
												 List *ddlCommandList);
static char * CreateShardCopyCommand(ShardInterval *shard, WorkerNode *targetNode);
static void AcquireShardPlacementLock(uint64_t shardId, int lockMode, Oid relationId,
									  const char *operationName);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_copy_shard_placement);
PG_FUNCTION_INFO_V1(citus_copy_shard_placement_with_nodeid);
PG_FUNCTION_INFO_V1(master_copy_shard_placement);
PG_FUNCTION_INFO_V1(citus_move_shard_placement);
PG_FUNCTION_INFO_V1(citus_move_shard_placement_with_nodeid);
PG_FUNCTION_INFO_V1(master_move_shard_placement);
PG_FUNCTION_INFO_V1(citus_internal_copy_single_shard_placement);
double DesiredPercentFreeAfterMove = 10;
bool CheckAvailableSpaceBeforeMove = true;


/*
 * citus_copy_shard_placement implements a user-facing UDF to copy a placement
 * from a source node to a target node, including all co-located placements.
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
	Oid shardReplicationModeOid = PG_GETARG_OID(5);

	char *sourceNodeName = text_to_cstring(sourceNodeNameText);
	char *targetNodeName = text_to_cstring(targetNodeNameText);

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);

	TransferShards(shardId, sourceNodeName, sourceNodePort,
				   targetNodeName, targetNodePort,
				   shardReplicationMode, SHARD_TRANSFER_COPY, 0);

	PG_RETURN_VOID();
}


/*
 * citus_copy_shard_placement_with_nodeid implements a user-facing UDF to copy a placement
 * from a source node to a target node, including all co-located placements.
 */
Datum
citus_copy_shard_placement_with_nodeid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 shardId = PG_GETARG_INT64(0);
	uint32 sourceNodeId = PG_GETARG_INT32(1);
	uint32 targetNodeId = PG_GETARG_INT32(2);
	Oid shardReplicationModeOid = PG_GETARG_OID(3);

	bool missingOk = false;
	WorkerNode *sourceNode = FindNodeWithNodeId(sourceNodeId, missingOk);
	WorkerNode *targetNode = FindNodeWithNodeId(targetNodeId, missingOk);

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);

	TransferShards(shardId, sourceNode->workerName, sourceNode->workerPort,
				   targetNode->workerName, targetNode->workerPort,
				   shardReplicationMode, SHARD_TRANSFER_COPY, 0);

	PG_RETURN_VOID();
}


/*
 * master_copy_shard_placement is a wrapper function for old UDF name.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
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

	if (doRepair)
	{
		ereport(WARNING, (errmsg("do_repair argument is deprecated")));
	}

	TransferShards(shardId, sourceNodeName, sourceNodePort,
				   targetNodeName, targetNodePort,
				   shardReplicationMode, SHARD_TRANSFER_COPY, 0);


	PG_RETURN_VOID();
}


/*
 * citus_internal_copy_single_shard_placement is an internal function that
 * copies a single shard placement from a source node to a target node.
 * It has two main differences from citus_copy_shard_placement:
 * 1. it copies only a single shard placement, not all colocated shards
 * 2. It allows to defer the constraints creation and this same function
 *   can be used to create the constraints later.
 *
 * The primary use case for this function is to transfer the shards of
 * reference tables. Since all reference tables are colocated together,
 * and each reference table has only one shard, this function can be used
 * to transfer the shards of reference tables in parallel.
 * Furthermore, the reference tables could have relations with
 * other reference tables, so we need to ensure that their constraints
 * are also transferred after copying the shards to the target node.
 * For this reason, we allow the caller to defer the constraints creation.
 *
 * This function is not supposed to be called by the user directly.
 */
Datum
citus_internal_copy_single_shard_placement(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 shardId = PG_GETARG_INT64(0);
	uint32 sourceNodeId = PG_GETARG_INT32(1);
	uint32 targetNodeId = PG_GETARG_INT32(2);
	uint32 flags = PG_GETARG_INT32(3);
	Oid shardReplicationModeOid = PG_GETARG_OID(4);

	bool missingOk = false;
	WorkerNode *sourceNode = FindNodeWithNodeId(sourceNodeId, missingOk);
	WorkerNode *targetNode = FindNodeWithNodeId(targetNodeId, missingOk);

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);

	/*
	 * This is an internal function that is used by the rebalancer.
	 * It is not supposed to be called by the user directly.
	 */
	if (!IsRebalancerInternalBackend())
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("This is an internal Citus function that can only"
							   " be used by a rebalancer task")));
	}

	TransferShards(shardId, sourceNode->workerName, sourceNode->workerPort,
				   targetNode->workerName, targetNode->workerPort,
				   shardReplicationMode, SHARD_TRANSFER_COPY, flags);

	PG_RETURN_VOID();
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

	List *referenceTableIdList = NIL;

	if (HasNodesWithMissingReferenceTables(&referenceTableIdList))
	{
		ereport(ERROR, (errmsg("there are missing reference tables on some nodes"),
						errhint("Copy reference tables first with "
								"replicate_reference_tables() or use "
								"citus_rebalance_start() that will do it automatically."
								)));
	}

	int64 shardId = PG_GETARG_INT64(0);
	char *sourceNodeName = text_to_cstring(PG_GETARG_TEXT_P(1));
	int32 sourceNodePort = PG_GETARG_INT32(2);
	char *targetNodeName = text_to_cstring(PG_GETARG_TEXT_P(3));
	int32 targetNodePort = PG_GETARG_INT32(4);
	Oid shardReplicationModeOid = PG_GETARG_OID(5);

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);
	TransferShards(shardId, sourceNodeName, sourceNodePort,
				   targetNodeName, targetNodePort,
				   shardReplicationMode, SHARD_TRANSFER_MOVE, 0);

	PG_RETURN_VOID();
}


/*
 * citus_move_shard_placement_with_nodeid does the same as citus_move_shard_placement,
 * but accepts node ids as parameters, instead of hostname and port.
 */
Datum
citus_move_shard_placement_with_nodeid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 shardId = PG_GETARG_INT64(0);
	uint32 sourceNodeId = PG_GETARG_INT32(1);
	uint32 targetNodeId = PG_GETARG_INT32(2);
	Oid shardReplicationModeOid = PG_GETARG_OID(3);

	bool missingOk = false;
	WorkerNode *sourceNode = FindNodeWithNodeId(sourceNodeId, missingOk);
	WorkerNode *targetNode = FindNodeWithNodeId(targetNodeId, missingOk);

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);
	TransferShards(shardId, sourceNode->workerName,
				   sourceNode->workerPort, targetNode->workerName,
				   targetNode->workerPort, shardReplicationMode, SHARD_TRANSFER_MOVE, 0);

	PG_RETURN_VOID();
}


/*
 * AcquireShardPlacementLock tries to acquire a lock on the shardid
 * while moving/copying the shard placement. If this
 * is it not possible it fails instantly because this means
 * another move/copy on same shard is currently happening. */
static void
AcquireShardPlacementLock(uint64_t shardId, int lockMode, Oid relationId,
						  const char *operationName)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;

	SET_LOCKTAG_SHARD_MOVE(tag, shardId);

	LockAcquireResult lockAcquired = LockAcquire(&tag, lockMode, sessionLock, dontWait);
	if (!lockAcquired)
	{
		ereport(ERROR, (errmsg("could not acquire the lock required to %s %s",
							   operationName,
							   generate_qualified_relation_name(relationId)),
						errdetail("It means that either a concurrent shard move "
								  "or colocated distributed table creation is "
								  "happening."),
						errhint("Make sure that the concurrent operation has "
								"finished and re-run the command")));
	}
}


/*
 * TransferShards is responsible for handling shard transfers.
 *
 * The optionFlags parameter controls the transfer behavior:
 *
 * - By default, shard colocation groups are treated as a single unit. This works
 *   well for distributed tables, since they can contain multiple colocated shards
 *   on the same node, and shard transfers can still be parallelized at the group level.
 *
 * - Reference tables are different: every reference table belongs to the same
 *   colocation group but has only a single shard. To parallelize reference table
 *   transfers, we must bypass the colocation group. The
 *   SHARD_TRANSFER_SINGLE_SHARD_ONLY flag enables this behavior by transferring
 *   only the specific shardId passed into the function, ignoring colocated shards.
 *
 * - Reference tables may also define foreign key relationships with each other.
 *   Since we cannot create those relationships until all shards have been moved,
 *   the SHARD_TRANSFER_SKIP_CREATE_RELATIONSHIPS flag is used to defer their
 *   creation until shard transfer completes.
 *
 * - After shards are transferred, the SHARD_TRANSFER_CREATE_RELATIONSHIPS_ONLY
 *   flag is used to create the foreign key relationships for already-transferred
 *   reference tables.
 *
 * Currently, optionFlags are only used to customize reference table transfers.
 * For distributed tables, optionFlags should always be set to 0.
 * passing 0 as optionFlags means that the default behavior will be used for
 * all aspects of the shard transfer. That is to consider all colocated shards
 * as a single unit and return after creating the necessary relationships.
 */
void
TransferShards(int64 shardId, char *sourceNodeName,
			   int32 sourceNodePort, char *targetNodeName,
			   int32 targetNodePort, char shardReplicationMode,
			   ShardTransferType transferType, uint32 optionFlags)
{
	/* strings to be used in log messages */
	const char *operationName = ShardTransferTypeNames[transferType];
	const char *operationNameCapitalized =
		ShardTransferTypeNamesCapitalized[transferType];
	const char *operationFunctionName = ShardTransferTypeFunctionNames[transferType];

	/* cannot transfer shard to the same node */
	ErrorIfSameNode(sourceNodeName, sourceNodePort,
					targetNodeName, targetNodePort,
					operationName);

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	/* error if unsupported shard transfer */
	if (transferType == SHARD_TRANSFER_MOVE)
	{
		ErrorIfMoveUnsupportedTableType(distributedTableId);
	}
	else if (transferType == SHARD_TRANSFER_COPY)
	{
		ErrorIfTableCannotBeReplicated(distributedTableId);
		EnsureNoModificationsHaveBeenDone();
	}

	ErrorIfTargetNodeIsNotSafeForTransfer(targetNodeName, targetNodePort, transferType);

	AcquirePlacementColocationLock(distributedTableId, RowExclusiveLock, operationName);

	List *colocatedTableList;
	List *colocatedShardList;

	/*
	 * If SHARD_TRANSFER_SINGLE_SHARD_ONLY is set, we only transfer a single shard
	 * specified by shardId. Otherwise, we transfer all colocated shards.
	 */
	bool isSingleShardOnly = optionFlags & SHARD_TRANSFER_SINGLE_SHARD_ONLY;

	if (isSingleShardOnly)
	{
		colocatedTableList = list_make1_oid(distributedTableId);
		colocatedShardList = list_make1(shardInterval);
	}
	else
	{
		colocatedTableList = ColocatedTableList(distributedTableId);
		colocatedShardList = ColocatedShardIntervalList(shardInterval);
	}

	EnsureTableListOwner(colocatedTableList);

	if (transferType == SHARD_TRANSFER_MOVE)
	{
		/*
		 * Block concurrent DDL / TRUNCATE commands on the relation. while,
		 * allow concurrent citus_move_shard_placement() on the shards of
		 * the same relation.
		 */
		LockColocatedRelationsForMove(colocatedTableList);
	}

	ErrorIfForeignTableForShardTransfer(colocatedTableList, transferType);

	if (transferType == SHARD_TRANSFER_COPY)
	{
		ErrorIfReplicatingDistributedTableWithFKeys(colocatedTableList);
	}

	/*
	 * We sort shardIntervalList so that lock operations will not cause any
	 * deadlocks. But we do not need to do that if the list contain only one
	 * shard.
	 */
	if (!isSingleShardOnly)
	{
		colocatedShardList = SortList(colocatedShardList, CompareShardIntervalsById);
	}

	/* We have pretty much covered the concurrent rebalance operations
	 * and we want to allow concurrent moves within the same colocation group.
	 * but at the same time we want to block the concurrent moves on the same shard
	 * placement. So we lock the shard moves before starting the transfer.
	 */
	foreach_declared_ptr(shardInterval, colocatedShardList)
	{
		int64 shardIdToLock = shardInterval->shardId;
		AcquireShardPlacementLock(shardIdToLock, ExclusiveLock, distributedTableId,
								  operationName);
	}

	bool transferAlreadyCompleted = TransferAlreadyCompleted(colocatedShardList,
															 sourceNodeName,
															 sourceNodePort,
															 targetNodeName,
															 targetNodePort,
															 transferType);

	/*
	 * If we just need to create the shard relationships,We don't need to do anything
	 * else other than calling CopyShardTables with SHARD_TRANSFER_CREATE_RELATIONSHIPS_ONLY
	 * flag.
	 */
	bool createRelationshipsOnly = optionFlags & SHARD_TRANSFER_CREATE_RELATIONSHIPS_ONLY;

	if (createRelationshipsOnly)
	{
		if (!transferAlreadyCompleted)
		{
			/*
			 * if the transfer is not completed, and we are here just to create
			 * the relationships, we can return right away
			 */
			ereport(WARNING, (errmsg("shard is not present on node %s:%d",
									 targetNodeName, targetNodePort),
							  errdetail("%s may have not completed.",
										operationNameCapitalized)));
			return;
		}

		CopyShardTables(colocatedShardList, sourceNodeName, sourceNodePort, targetNodeName
						,
						targetNodePort, (shardReplicationMode ==
										 TRANSFER_MODE_FORCE_LOGICAL),
						operationFunctionName, optionFlags);

		/* We don't need to do anything else, just return */
		return;
	}

	if (transferAlreadyCompleted)
	{
		/* if the transfer is already completed, we can return right away */
		ereport(WARNING, (errmsg("shard is already present on node %s:%d",
								 targetNodeName, targetNodePort),
						  errdetail("%s may have already completed.",
									operationNameCapitalized)));
		return;
	}

	EnsureAllShardsCanBeCopied(colocatedShardList, sourceNodeName, sourceNodePort,
							   targetNodeName, targetNodePort);

	if (shardReplicationMode == TRANSFER_MODE_AUTOMATIC)
	{
		VerifyTablesHaveReplicaIdentity(colocatedTableList);
	}

	EnsureEnoughDiskSpaceForShardMove(colocatedShardList,
									  sourceNodeName, sourceNodePort,
									  targetNodeName, targetNodePort, transferType);

	SetupRebalanceMonitorForShardTransfer(shardId, distributedTableId,
										  sourceNodeName, sourceNodePort,
										  targetNodeName, targetNodePort,
										  transferType);

	UpdatePlacementUpdateStatusForShardIntervalList(
		colocatedShardList,
		sourceNodeName,
		sourceNodePort,
		PLACEMENT_UPDATE_STATUS_SETTING_UP);

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
	else if (transferType == SHARD_TRANSFER_MOVE)
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

	if (transferType == SHARD_TRANSFER_COPY &&
		!IsCitusTableType(distributedTableId, REFERENCE_TABLE))
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

	DropOrphanedResourcesInSeparateTransaction();

	ShardInterval *colocatedShard = NULL;
	foreach_declared_ptr(colocatedShard, colocatedShardList)
	{
		/*
		 * This is to prevent any race condition possibility among the shard moves.
		 * We don't allow the move to happen if the shard we are going to move has an
		 * orphaned placement somewhere that is not cleanup up yet.
		 */
		char *qualifiedShardName = ConstructQualifiedShardName(colocatedShard);
		ErrorIfCleanupRecordForShardExists(qualifiedShardName);
	}

	CopyShardTables(colocatedShardList, sourceNodeName, sourceNodePort, targetNodeName,
					targetNodePort, useLogicalReplication, operationFunctionName,
					optionFlags);

	if (transferType == SHARD_TRANSFER_MOVE)
	{
		/* delete old shards metadata and mark the shards as to be deferred drop */
		int32 sourceGroupId = GroupForNode(sourceNodeName, sourceNodePort);
		InsertCleanupRecordsForShardPlacementsOnNode(colocatedShardList,
													 sourceGroupId);
	}

	/*
	 * Finally insert the placements to pg_dist_placement and sync it to the
	 * metadata workers.
	 */
	colocatedShard = NULL;
	foreach_declared_ptr(colocatedShard, colocatedShardList)
	{
		uint64 colocatedShardId = colocatedShard->shardId;
		uint32 groupId = GroupForNode(targetNodeName, targetNodePort);
		uint64 placementId = GetNextPlacementId();

		InsertShardPlacementRow(colocatedShardId, placementId,
								ShardLength(colocatedShardId),
								groupId);

		if (transferType == SHARD_TRANSFER_COPY &&
			ShouldSyncTableMetadata(colocatedShard->relationId))
		{
			char *placementCommand = PlacementUpsertCommand(colocatedShardId, placementId,
															0, groupId);

			SendCommandToWorkersWithMetadata(placementCommand);
		}
	}

	if (transferType == SHARD_TRANSFER_MOVE)
	{
		/*
		 * Since this is move operation, we remove the placements from the metadata
		 * for the source node after copy.
		 */
		DropShardPlacementsFromMetadata(colocatedShardList,
										sourceNodeName, sourceNodePort);

		UpdateColocatedShardPlacementMetadataOnWorkers(shardId, sourceNodeName,
													   sourceNodePort, targetNodeName,
													   targetNodePort);
	}

	UpdatePlacementUpdateStatusForShardIntervalList(
		colocatedShardList,
		sourceNodeName,
		sourceNodePort,
		PLACEMENT_UPDATE_STATUS_COMPLETED);

	FinalizeCurrentProgressMonitor();
}


/*
 * AdjustShardsForPrimaryCloneNodeSplit is called when a primary-clone node split
 * occurs. It adjusts the shard placements between the primary and clone nodes based
 * on the provided shard lists. Since the clone is an exact replica of the primary
 * but the metadata is not aware of this replication, this function updates the
 * metadata to reflect the new shard distribution.
 *
 * The function handles three types of shards:
 *
 * 1. Shards moving to clone node (cloneShardList):
 *    - Updates shard placement metadata to move placements from primary to clone
 *    - No data movement is needed since the clone already has the data
 *    - Adds cleanup records to remove the shard data from primary at transaction commit
 *
 * 2. Shards staying on primary node (primaryShardList):
 *    - Metadata already correctly reflects these shards on primary
 *    - Adds cleanup records to remove the shard data from clone node
 *
 * 3. Reference tables:
 *    - Inserts new placement records on the clone node
 *    - Data is already present on clone, so only metadata update is needed
 *
 * This function does not perform any actual data movement; it only updates the
 * shard placement metadata and schedules cleanup operations for later execution.
 */
void
AdjustShardsForPrimaryCloneNodeSplit(WorkerNode *primaryNode,
									 WorkerNode *cloneNode,
									 List *primaryShardList,
									 List *cloneShardList)
{
	/* Input validation */
	if (primaryNode == NULL || cloneNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("primary or clone worker node is NULL")));
	}

	if (primaryNode->nodeId == cloneNode->nodeId)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("primary and clone nodes must be different")));
	}

	ereport(NOTICE, (errmsg(
						 "adjusting shard placements for primary %s:%d and clone %s:%d",
						 primaryNode->workerName, primaryNode->workerPort,
						 cloneNode->workerName, cloneNode->workerPort)));

	RegisterOperationNeedingCleanup();

	/*
	 * Process shards that will stay on the primary node.
	 * For these shards, we need to remove their data from the clone node
	 * since the metadata already correctly reflects them on primary.
	 */
	uint64 shardId = 0;
	uint32 primaryGroupId = GroupForNode(primaryNode->workerName, primaryNode->workerPort)
	;
	uint32 cloneGroupId = GroupForNode(cloneNode->workerName, cloneNode->workerPort);

	ereport(NOTICE, (errmsg("processing %d shards for primary node GroupID %d",
							list_length(primaryShardList), primaryGroupId)));


	/*
	 * For each shard staying on primary, insert cleanup records to remove
	 * the shard data from the clone node. The metadata already correctly
	 * reflects these shards on primary, so no metadata changes are needed.
	 */
	foreach_declared_int(shardId, primaryShardList)
	{
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		List *colocatedShardList = ColocatedShardIntervalList(shardInterval);

		char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);
		ereport(LOG, (errmsg(
						  "inserting DELETE shard record for shard %s from clone node GroupID %d",
						  qualifiedShardName, cloneGroupId)));

		InsertCleanupRecordsForShardPlacementsOnNode(colocatedShardList,
													 cloneGroupId);
	}


	/*
	 * Process shards that will move to the clone node.
	 * For these shards, we need to:
	 * 1. Update metadata to move placements from primary to clone
	 * 2. Remove the shard data from primary (via cleanup records)
	 * 3. No data movement needed since clone already has the data
	 */
	ereport(NOTICE, (errmsg("processing %d shards for clone node GroupID %d", list_length(
								cloneShardList), cloneGroupId)));

	foreach_declared_int(shardId, cloneShardList)
	{
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		List *colocatedShardList = ColocatedShardIntervalList(shardInterval);

		/*
		 * Create new shard placement records on the clone node for all
		 * colocated shards. This moves the shard placements from primary
		 * to clone in the metadata.
		 */
		foreach_declared_ptr(shardInterval, colocatedShardList)
		{
			uint64 colocatedShardId = shardInterval->shardId;

			uint64 placementId = GetNextPlacementId();
			InsertShardPlacementRow(colocatedShardId, placementId,
									ShardLength(colocatedShardId),
									cloneGroupId);
		}

		/*
		 * Update the metadata on worker nodes to reflect the new shard
		 * placement distribution between primary and clone nodes.
		 */
		UpdateColocatedShardPlacementMetadataOnWorkers(shardId,
													   primaryNode->workerName,
													   primaryNode->workerPort,
													   cloneNode->workerName,
													   cloneNode->workerPort);

		/*
		 * Remove the shard placement records from primary node metadata
		 * since these shards are now served from the clone node.
		 */
		DropShardPlacementsFromMetadata(colocatedShardList,
										primaryNode->workerName, primaryNode->workerPort);

		char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);
		ereport(LOG, (errmsg(
						  "inserting DELETE shard record for shard %s from primary node GroupID %d",
						  qualifiedShardName, primaryGroupId)));

		/*
		 * Insert cleanup records to remove the shard data from primary node
		 * at transaction commit. This frees up space on the primary node
		 * since the data is now served from the clone node.
		 */
		InsertCleanupRecordsForShardPlacementsOnNode(colocatedShardList,
													 primaryGroupId);
	}

	/*
	 * Handle reference tables - these need to be available on both
	 * primary and clone nodes. Since the clone already has the data,
	 * we just need to insert placement records for the clone node.
	 */
	int colocationId = GetReferenceTableColocationId();

	if (colocationId == INVALID_COLOCATION_ID)
	{
		/* we have no reference table yet. */
		return;
	}
	ShardInterval *shardInterval = NULL;
	List *referenceTableIdList = CitusTableTypeIdList(REFERENCE_TABLE);
	Oid referenceTableId = linitial_oid(referenceTableIdList);
	List *shardIntervalList = LoadShardIntervalList(referenceTableId);
	foreach_declared_ptr(shardInterval, shardIntervalList)
	{
		List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
		ShardInterval *colocatedShardInterval = NULL;

		/*
		 * For each reference table shard, create placement records on the
		 * clone node. The data is already present on the clone, so we only
		 * need to update the metadata to make the clone aware of these shards.
		 */
		foreach_declared_ptr(colocatedShardInterval, colocatedShardList)
		{
			uint64 colocatedShardId = colocatedShardInterval->shardId;

			/*
			 * Insert shard placement record for the clone node and
			 * propagate the metadata change to worker nodes.
			 */
			uint64 placementId = GetNextPlacementId();
			InsertShardPlacementRow(colocatedShardId, placementId,
									ShardLength(colocatedShardId),
									cloneGroupId);

			char *placementCommand = PlacementUpsertCommand(colocatedShardId, placementId,
															0, cloneGroupId);

			SendCommandToWorkersWithMetadata(placementCommand);
		}
	}

	ereport(NOTICE, (errmsg(
						 "shard placement adjustment complete for primary %s:%d and clone %s:%d",
						 primaryNode->workerName, primaryNode->workerPort,
						 cloneNode->workerName, cloneNode->workerPort)));
}


/*
 * Insert deferred cleanup records.
 * The shards will be dropped by background cleaner later.
 */
void
InsertDeferredDropCleanupRecordsForShards(List *shardIntervalList)
{
	ListCell *shardIntervalCell = NULL;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		ListCell *shardPlacementCell = NULL;
		uint64 oldShardId = shardInterval->shardId;

		/* mark for deferred drop */
		List *shardPlacementList = ActiveShardPlacementList(oldShardId);
		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);

			/* get shard name */
			char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);

			/* Log shard in pg_dist_cleanup.
			 * Parent shards are to be dropped only on sucess after split workflow is complete,
			 * so mark the policy as 'CLEANUP_DEFERRED_ON_SUCCESS'.
			 * We also log cleanup record in the current transaction. If the current transaction rolls back,
			 * we do not generate a record at all.
			 */
			InsertCleanupOnSuccessRecordInCurrentTransaction(
				CLEANUP_OBJECT_SHARD_PLACEMENT,
				qualifiedShardName,
				placement->groupId);
		}
	}
}


/*
 * InsertCleanupRecordsForShardPlacementsOnNode inserts deferred cleanup records.
 * The shards will be dropped by background cleaner later.
 * This function does this only for the placements on the given node.
 */
void
InsertCleanupRecordsForShardPlacementsOnNode(List *shardIntervalList,
											 int32 groupId)
{
	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, shardIntervalList)
	{
		/* get shard name */
		char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);

		/* Log shard in pg_dist_cleanup.
		 * Parent shards are to be dropped only on sucess after split workflow is complete,
		 * so mark the policy as 'CLEANUP_DEFERRED_ON_SUCCESS'.
		 * We also log cleanup record in the current transaction. If the current transaction rolls back,
		 * we do not generate a record at all.
		 */
		InsertCleanupOnSuccessRecordInCurrentTransaction(CLEANUP_OBJECT_SHARD_PLACEMENT,
														 qualifiedShardName,
														 groupId);
	}
}


/*
 * IsShardListOnNode determines whether a co-located shard list has
 * active placements on a given node.
 */
static bool
IsShardListOnNode(List *colocatedShardList, char *targetNodeName, uint32 targetNodePort)
{
	WorkerNode *workerNode = FindWorkerNode(targetNodeName, targetNodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Moving shards to a non-existing node is not supported")));
	}

	/*
	 * We exhaustively search all co-located shards
	 */
	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, colocatedShardList)
	{
		uint64 shardId = shardInterval->shardId;
		List *placementList = ActiveShardPlacementListOnGroup(shardId,
															  workerNode->groupId);
		if (placementList == NIL)
		{
			return false;
		}
	}

	return true;
}


/*
 * LockColocatedRelationsForMove takes a list of relations, locks all of them
 * using ShareLock
 */
static void
LockColocatedRelationsForMove(List *colocatedTableList)
{
	Oid colocatedTableId = InvalidOid;
	foreach_declared_oid(colocatedTableId, colocatedTableList)
	{
		LockRelationOid(colocatedTableId, RowExclusiveLock);
	}
}


/*
 * ErrorIfForeignTableForShardTransfer takes a list of relations, errors out if
 * there's a foreign table in the list.
 */
static void
ErrorIfForeignTableForShardTransfer(List *colocatedTableList,
									ShardTransferType transferType)
{
	Oid colocatedTableId = InvalidOid;
	foreach_declared_oid(colocatedTableId, colocatedTableList)
	{
		if (IsForeignTable(colocatedTableId))
		{
			char *relationName = get_rel_name(colocatedTableId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot %s shard",
								   ShardTransferTypeNames[transferType]),
							errdetail("Table %s is a foreign table. "
									  "%s shards backed by foreign tables is "
									  "not supported.", relationName,
									  ShardTransferTypeNamesContinuous[transferType])));
		}
	}
}


/*
 * EnsureAllShardsCanBeCopied is a wrapper around EnsureShardCanBeCopied.
 */
static void
EnsureAllShardsCanBeCopied(List *colocatedShardList,
						   char *sourceNodeName, uint32 sourceNodePort,
						   char *targetNodeName, uint32 targetNodePort)
{
	ShardInterval *colocatedShard = NULL;
	foreach_declared_ptr(colocatedShard, colocatedShardList)
	{
		uint64 colocatedShardId = colocatedShard->shardId;

		/*
		 * To transfer shard, there should be healthy placement in source node and no
		 * placement in the target node.
		 */
		EnsureShardCanBeCopied(colocatedShardId, sourceNodeName, sourceNodePort,
							   targetNodeName, targetNodePort);
	}
}


/*
 * EnsureEnoughDiskSpaceForShardMove checks that there is enough space for
 * shard moves of the given colocated shard list from source node to target node.
 * It tries to clean up old shard placements to ensure there is enough space.
 */
static void
EnsureEnoughDiskSpaceForShardMove(List *colocatedShardList,
								  char *sourceNodeName, uint32 sourceNodePort,
								  char *targetNodeName, uint32 targetNodePort,
								  ShardTransferType transferType)
{
	if (!CheckAvailableSpaceBeforeMove || transferType != SHARD_TRANSFER_MOVE)
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
 * TransferAlreadyCompleted returns true if the given shard transfer is already done.
 * Returns false otherwise.
 */
static bool
TransferAlreadyCompleted(List *colocatedShardList,
						 char *sourceNodeName, uint32 sourceNodePort,
						 char *targetNodeName, uint32 targetNodePort,
						 ShardTransferType transferType)
{
	if (transferType == SHARD_TRANSFER_MOVE &&
		IsShardListOnNode(colocatedShardList, targetNodeName, targetNodePort) &&
		!IsShardListOnNode(colocatedShardList, sourceNodeName, sourceNodePort))
	{
		return true;
	}

	if (transferType == SHARD_TRANSFER_COPY &&
		IsShardListOnNode(colocatedShardList, targetNodeName, targetNodePort) &&
		IsShardListOnNode(colocatedShardList, sourceNodeName, sourceNodePort))
	{
		return true;
	}

	return false;
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

	/* we're interested in whole table, not a particular index */
	Oid indexId = InvalidOid;

	StringInfo tableSizeQuery = GenerateSizeQueryOnMultiplePlacements(shardList,
																	  indexId,
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
 * SetupRebalanceMonitorForShardTransfer prepares the parameters and
 * calls SetupRebalanceMonitor, unless the current transfer is a move
 * initiated by the rebalancer.
 * See comments on SetupRebalanceMonitor
 */
static void
SetupRebalanceMonitorForShardTransfer(uint64 shardId, Oid distributedTableId,
									  char *sourceNodeName, uint32 sourceNodePort,
									  char *targetNodeName, uint32 targetNodePort,
									  ShardTransferType transferType)
{
	if (transferType == SHARD_TRANSFER_MOVE && IsRebalancerInternalBackend())
	{
		/*
		 * We want to be able to track progress of shard moves using
		 * get_rebalancer_progress. If this move is initiated by the rebalancer,
		 * then the rebalancer call has already set up the shared memory that is
		 * used to do that, so we should return here.
		 * But if citus_move_shard_placement is called directly by the user
		 * (or through any other mechanism), then the shared memory is not
		 * set up yet. In that case we do it here.
		 */
		return;
	}

	WorkerNode *sourceNode = FindWorkerNode(sourceNodeName, sourceNodePort);
	WorkerNode *targetNode = FindWorkerNode(targetNodeName, targetNodePort);

	PlacementUpdateEvent *placementUpdateEvent = palloc0(
		sizeof(PlacementUpdateEvent));
	placementUpdateEvent->updateType =
		transferType == SHARD_TRANSFER_COPY ? PLACEMENT_UPDATE_COPY :
		PLACEMENT_UPDATE_MOVE;
	placementUpdateEvent->shardId = shardId;
	placementUpdateEvent->sourceNode = sourceNode;
	placementUpdateEvent->targetNode = targetNode;
	SetupRebalanceMonitor(list_make1(placementUpdateEvent), distributedTableId,
						  REBALANCE_PROGRESS_MOVING,
						  PLACEMENT_UPDATE_STATUS_SETTING_UP);
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
							   "desired available space after move is %ld bytes, "
							   "estimated size increase on node after move is %ld bytes.",
							   diskAvailableInBytesAfterShardMove,
							   desiredNewDiskAvailableInBytes, colocationSizeInBytes),
						errhint(
							"consider lowering citus.desired_percent_disk_available_after_move.")));
	}
}


/*
 * ErrorIfTargetNodeIsNotSafeForTransfer throws error if the target node is not
 * eligible for shard transfers.
 */
static void
ErrorIfTargetNodeIsNotSafeForTransfer(const char *targetNodeName, int targetNodePort,
									  ShardTransferType transferType)
{
	WorkerNode *workerNode = FindWorkerNode(targetNodeName, targetNodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s shards to a non-existing node is not supported",
							   ShardTransferTypeNamesContinuous[transferType]),
						errhint(
							"Add the target node via SELECT citus_add_node('%s', %d);",
							targetNodeName, targetNodePort)));
	}

	if (!workerNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s shards to a non-active node is not supported",
							   ShardTransferTypeNamesContinuous[transferType]),
						errhint(
							"Activate the target node via SELECT citus_activate_node('%s', %d);",
							targetNodeName, targetNodePort)));
	}

	if (transferType == SHARD_TRANSFER_MOVE && !workerNode->shouldHaveShards)
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
						errmsg("%s shards to a secondary (e.g., replica) node is "
							   "not supported",
							   ShardTransferTypeNamesContinuous[transferType])));
	}
}


/*
 * ErrorIfSameNode throws an error if the two host:port combinations
 * are the same.
 */
static void
ErrorIfSameNode(char *sourceNodeName, int sourceNodePort,
				char *targetNodeName, int targetNodePort,
				const char *operationName)
{
	if (strncmp(sourceNodeName, targetNodeName, MAX_NODE_LENGTH) == 0 &&
		sourceNodePort == targetNodePort)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot %s shard to the same node",
							   operationName)));
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
void
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
bool
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
	foreach_declared_ptr(shard, shardList)
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
 * EnsureTableListOwner ensures current user owns given tables. Superusers
 * are regarded as owners.
 */
static void
EnsureTableListOwner(List *tableIdList)
{
	Oid tableId = InvalidOid;
	foreach_declared_oid(tableId, tableIdList)
	{
		EnsureTableOwner(tableId);
	}
}


/*
 * ErrorIfReplicatingDistributedTableWithFKeys errors out if given tables are not
 * suitable for replication.
 */
static void
ErrorIfReplicatingDistributedTableWithFKeys(List *tableIdList)
{
	Oid tableId = InvalidOid;
	foreach_declared_oid(tableId, tableIdList)
	{
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
				char *targetNodeName, int32 targetNodePort, bool useLogicalReplication,
				const char *operationName, uint32 optionFlags)
{
	if (list_length(shardIntervalList) < 1)
	{
		return;
	}

	/* Start operation to prepare for generating cleanup records */
	RegisterOperationNeedingCleanup();

	bool createRelationshipsOnly = optionFlags & SHARD_TRANSFER_CREATE_RELATIONSHIPS_ONLY;

	/*
	 * If we're just going to create relationships only always use
	 * CopyShardTablesViaBlockWrites.
	 */
	if (useLogicalReplication && !createRelationshipsOnly)
	{
		CopyShardTablesViaLogicalReplication(shardIntervalList, sourceNodeName,
											 sourceNodePort, targetNodeName,
											 targetNodePort, optionFlags);
	}
	else
	{
		CopyShardTablesViaBlockWrites(shardIntervalList, sourceNodeName, sourceNodePort,
									  targetNodeName, targetNodePort, optionFlags);
	}

	/*
	 * Drop temporary objects that were marked as CLEANUP_ALWAYS.
	 */
	FinalizeOperationNeedingCleanupOnSuccess(operationName);
}


/*
 * CopyShardTablesViaLogicalReplication copies a shard along with its co-located shards
 * from a source node to target node via logical replication.
 */
static void
CopyShardTablesViaLogicalReplication(List *shardIntervalList, char *sourceNodeName,
									 int32 sourceNodePort, char *targetNodeName,
									 int32 targetNodePort, uint32 optionFlags)
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
	foreach_declared_ptr(shardInterval, shardIntervalList)
	{
		Oid relationId = shardInterval->relationId;
		uint64 shardId = shardInterval->shardId;
		List *tableRecreationCommandList = RecreateTableDDLCommandList(relationId);
		tableRecreationCommandList =
			WorkerApplyShardDDLCommandList(tableRecreationCommandList, shardId);

		char *tableOwner = TableOwner(shardInterval->relationId);

		/* drop the shard we created on the target, in case of failure */
		InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_SHARD_PLACEMENT,
											  ConstructQualifiedShardName(shardInterval),
											  GroupForNode(targetNodeName,
														   targetNodePort),
											  CLEANUP_ON_FAILURE);

		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner,
												  tableRecreationCommandList);

		MemoryContextReset(localContext);
	}

	MemoryContextSwitchTo(oldContext);

	bool skipRelationshipCreation = (optionFlags &
									 SHARD_TRANSFER_SKIP_CREATE_RELATIONSHIPS);

	/* data copy is done seperately when logical replication is used */
	LogicallyReplicateShards(shardIntervalList, sourceNodeName,
							 sourceNodePort, targetNodeName, targetNodePort,
							 skipRelationshipCreation);
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
							  int32 targetNodePort, uint32 optionFlags)
{
	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CopyShardTablesViaBlockWrites",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	WorkerNode *sourceNode = FindWorkerNode(sourceNodeName, sourceNodePort);
	WorkerNode *targetNode = FindWorkerNode(targetNodeName, targetNodePort);
	ShardInterval *shardInterval = NULL;

	bool createRelationshipsOnly = optionFlags & SHARD_TRANSFER_CREATE_RELATIONSHIPS_ONLY;

	/*
	 * If we’re only asked to create the relationships, the shards are already
	 * present and populated on the node. Skip the table‑setup and data‑loading
	 * steps and proceed straight to creating the relationships.
	 */
	if (!createRelationshipsOnly)
	{
		/* iterate through the colocated shards and copy each */
		foreach_declared_ptr(shardInterval, shardIntervalList)
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
			List *ddlCommandList = RecreateShardDDLCommandList(shardInterval,
															   sourceNodeName,
															   sourceNodePort);
			char *tableOwner = TableOwner(shardInterval->relationId);

			/* drop the shard we created on the target, in case of failure */
			InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_SHARD_PLACEMENT,
												  ConstructQualifiedShardName(
													  shardInterval),
												  GroupForNode(targetNodeName,
															   targetNodePort),
												  CLEANUP_ON_FAILURE);

			SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
													  tableOwner, ddlCommandList);
		}

		UpdatePlacementUpdateStatusForShardIntervalList(
			shardIntervalList,
			sourceNodeName,
			sourceNodePort,
			PLACEMENT_UPDATE_STATUS_COPYING_DATA);

		ConflictWithIsolationTestingBeforeCopy();
		CopyShardsToNode(sourceNode, targetNode, shardIntervalList, NULL);
		ConflictWithIsolationTestingAfterCopy();

		UpdatePlacementUpdateStatusForShardIntervalList(
			shardIntervalList,
			sourceNodeName,
			sourceNodePort,
			PLACEMENT_UPDATE_STATUS_CREATING_CONSTRAINTS);

		foreach_declared_ptr(shardInterval, shardIntervalList)
		{
			List *ddlCommandList =
				PostLoadShardCreationCommandList(shardInterval, sourceNodeName,
												 sourceNodePort);
			char *tableOwner = TableOwner(shardInterval->relationId);
			SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
													  tableOwner, ddlCommandList);

			MemoryContextReset(localContext);
		}
	}

	/*
	 * Skip creating shard relationships if the caller has requested that they
	 * not be created.
	 */
	bool skipRelationshipCreation = (optionFlags &
									 SHARD_TRANSFER_SKIP_CREATE_RELATIONSHIPS);

	if (!skipRelationshipCreation)
	{
		/*
		 * Once all shards are copied, we can recreate relationships between shards.
		 * Create DDL commands to Attach child tables to their parents in a partitioning hierarchy.
		 */
		List *shardIntervalWithDDCommandsList = NIL;
		foreach_declared_ptr(shardInterval, shardIntervalList)
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

		UpdatePlacementUpdateStatusForShardIntervalList(
			shardIntervalList,
			sourceNodeName,
			sourceNodePort,
			PLACEMENT_UPDATE_STATUS_CREATING_FOREIGN_KEYS);

		/*
		 * Iterate through the colocated shards and create DDL commamnds
		 * to create the foreign constraints.
		 */
		foreach_declared_ptr(shardInterval, shardIntervalList)
		{
			List *shardForeignConstraintCommandList = NIL;
			List *referenceTableForeignConstraintList = NIL;

			CopyShardForeignConstraintCommandListGrouped(shardInterval,
														 &
														 shardForeignConstraintCommandList,
														 &
														 referenceTableForeignConstraintList);

			ShardCommandList *shardCommandList = CreateShardCommandList(
				shardInterval,
				list_concat(shardForeignConstraintCommandList,
							referenceTableForeignConstraintList));
			shardIntervalWithDDCommandsList = lappend(shardIntervalWithDDCommandsList,
													  shardCommandList);
		}

		/* Now execute the Partitioning & Foreign constraints creation commads. */
		ShardCommandList *shardCommandList = NULL;
		foreach_declared_ptr(shardCommandList, shardIntervalWithDDCommandsList)
		{
			char *tableOwner = TableOwner(shardCommandList->shardInterval->relationId);
			SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
													  tableOwner,
													  shardCommandList->ddlCommandList);
		}

		UpdatePlacementUpdateStatusForShardIntervalList(
			shardIntervalList,
			sourceNodeName,
			sourceNodePort,
			PLACEMENT_UPDATE_STATUS_COMPLETING);
	}
	MemoryContextReset(localContext);
	MemoryContextSwitchTo(oldContext);
}


/*
 * CopyShardsToNode copies the list of shards from the source to the target.
 * When snapshotName is not NULL it will do the COPY using this snapshot name.
 */
void
CopyShardsToNode(WorkerNode *sourceNode, WorkerNode *targetNode, List *shardIntervalList,
				 char *snapshotName)
{
	int taskId = 0;
	List *copyTaskList = NIL;
	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, shardIntervalList)
	{
		/*
		 * Skip copying data for partitioned tables, because they contain no
		 * data themselves. Their partitions do contain data, but those are
		 * different colocated shards that will be copied seperately.
		 */
		if (PartitionedTable(shardInterval->relationId))
		{
			continue;
		}

		List *ddlCommandList = NIL;

		/*
		 * This uses repeatable read because we want to read the table in
		 * the state exactly as it was when the snapshot was created. This
		 * is needed when using this code for the initial data copy when
		 * using logical replication. The logical replication catchup might
		 * fail otherwise, because some of the updates that it needs to do
		 * have already been applied on the target.
		 */
		StringInfo beginTransaction = makeStringInfo();
		appendStringInfo(beginTransaction,
						 "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
		ddlCommandList = lappend(ddlCommandList, beginTransaction->data);

		/* Set snapshot for non-blocking shard split. */
		if (snapshotName != NULL)
		{
			StringInfo snapShotString = makeStringInfo();
			appendStringInfo(snapShotString, "SET TRANSACTION SNAPSHOT %s;",
							 quote_literal_cstr(
								 snapshotName));
			ddlCommandList = lappend(ddlCommandList, snapShotString->data);
		}

		char *copyCommand = CreateShardCopyCommand(
			shardInterval, targetNode);

		ddlCommandList = lappend(ddlCommandList, copyCommand);

		StringInfo commitCommand = makeStringInfo();
		appendStringInfo(commitCommand, "COMMIT;");
		ddlCommandList = lappend(ddlCommandList, commitCommand->data);

		Task *task = CitusMakeNode(Task);
		task->jobId = shardInterval->shardId;
		task->taskId = taskId;
		task->taskType = READ_TASK;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		SetTaskQueryStringList(task, ddlCommandList);

		ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
		SetPlacementNodeMetadata(taskPlacement, sourceNode);

		task->taskPlacementList = list_make1(taskPlacement);

		copyTaskList = lappend(copyTaskList, task);
		taskId++;
	}

	ExecuteTaskListOutsideTransaction(ROW_MODIFY_NONE, copyTaskList,
									  MaxAdaptiveExecutorPoolSize,
									  NULL /* jobIdList (ignored by API implementation) */
									  );
}


/*
 * CreateShardCopyCommand constructs the command to copy a shard to another
 * worker node. This command needs to be run on the node wher you want to copy
 * the shard from.
 */
static char *
CreateShardCopyCommand(ShardInterval *shard,
					   WorkerNode *targetNode)
{
	char *shardName = ConstructQualifiedShardName(shard);
	StringInfo query = makeStringInfo();
	appendStringInfo(query,
					 "SELECT pg_catalog.worker_copy_table_to_node(%s::regclass, %u);",
					 quote_literal_cstr(shardName),
					 targetNode->nodeId);
	return query->data;
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

	/* error if the source shard placement does not exist */
	SearchShardPlacementInListOrError(shardPlacementList, sourceNodeName, sourceNodePort);

	ShardPlacement *targetPlacement = SearchShardPlacementInList(shardPlacementList,
																 targetNodeName,
																 targetNodePort);

	if (targetPlacement != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg(
							"shard " INT64_FORMAT " already exists in the target node",
							shardId)));
	}

	/*
	 * Make sure the relation exists. In some cases the relation is actually dropped but
	 * the metadata remains, such as dropping table while citus.enable_ddl_propagation
	 * is set to off.
	 */
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;
	EnsureRelationExists(distributedTableId);
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
	foreach_declared_ptr(shardPlacement, shardPlacementList)
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

	CopyShardForeignConstraintCommandListGrouped(
		shardInterval,
		&colocatedShardForeignConstraintCommandList,
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
	foreach_declared_ptr(command, commandList)
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

		if (relationId == referencedRelationId)
		{
			referencedShardId = shardInterval->shardId;
		}
		else if (IsCitusTableType(referencedRelationId, REFERENCE_TABLE))
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
	const char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	StringInfo dropCommand = makeStringInfo();

	IncludeSequenceDefaults includeSequenceDefaults = NO_SEQUENCE_DEFAULTS;
	IncludeIdentities includeIdentityDefaults = NO_IDENTITY;

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
						errmsg("target is not a regular, foreign or partitioned "
							   "table")));
	}

	List *dropCommandList = list_make1(makeTableDDLCommandString(dropCommand->data));
	List *createCommandList = GetPreLoadTableCreationCommands(relationId,
															  includeSequenceDefaults,
															  includeIdentityDefaults,
															  NULL);
	List *recreateCommandList = list_concat(dropCommandList, createCommandList);

	return recreateCommandList;
}


/*
 * DropShardPlacementsFromMetadata drops the shard placement metadata for
 * the shard placements of given shard interval list from pg_dist_placement.
 */
static void
DropShardPlacementsFromMetadata(List *shardList,
								char *nodeName, int32 nodePort)
{
	ShardInterval *shardInverval = NULL;
	foreach_declared_ptr(shardInverval, shardList)
	{
		uint64 shardId = shardInverval->shardId;
		List *shardPlacementList = ShardPlacementList(shardId);
		ShardPlacement *placement =
			SearchShardPlacementInListOrError(shardPlacementList, nodeName, nodePort);

		DeleteShardPlacementRow(placement->placementId);
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
						 "SELECT citus_internal.update_placement_metadata(%ld, %d, %d)",
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
	foreach_declared_ptr(ddlCommand, ddlCommandList)
	{
		Assert(CitusIsA(ddlCommand, TableDDLCommand));
		char *applyDDLCommand = GetShardedTableDDLCommand(ddlCommand, shardId, NULL);
		applyDDLCommandList = lappend(applyDDLCommandList, applyDDLCommand);
	}

	return applyDDLCommandList;
}


/*
 * UpdatePlacementUpdateStatusForShardIntervalList updates the status field for shards
 * in the given shardInterval list.
 */
void
UpdatePlacementUpdateStatusForShardIntervalList(List *shardIntervalList,
												char *sourceName, int sourcePort,
												PlacementUpdateStatus status)
{
	List *segmentList = NIL;
	List *rebalanceMonitorList = NULL;

	if (!HasProgressMonitor())
	{
		rebalanceMonitorList = ProgressMonitorList(REBALANCE_ACTIVITY_MAGIC_NUMBER,
												   &segmentList);
	}
	else
	{
		rebalanceMonitorList = list_make1(GetCurrentProgressMonitor());
	}

	ProgressMonitorData *monitor = NULL;
	foreach_declared_ptr(monitor, rebalanceMonitorList)
	{
		PlacementUpdateEventProgress *steps = ProgressMonitorSteps(monitor);

		for (int moveIndex = 0; moveIndex < monitor->stepCount; moveIndex++)
		{
			PlacementUpdateEventProgress *step = steps + moveIndex;
			uint64 currentShardId = step->shardId;
			bool foundInList = false;

			ShardInterval *candidateShard = NULL;
			foreach_declared_ptr(candidateShard, shardIntervalList)
			{
				if (candidateShard->shardId == currentShardId)
				{
					foundInList = true;
					break;
				}
			}

			if (foundInList &&
				strcmp(step->sourceName, sourceName) == 0 &&
				step->sourcePort == sourcePort)
			{
				pg_atomic_write_u64(&step->updateStatus, status);
			}
		}
	}

	DetachFromDSMSegments(segmentList);
}
