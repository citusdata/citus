/*-------------------------------------------------------------------------
 *
 * reference_table_utils.c
 *
 * Declarations for public utility functions related to reference tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* local function forward declarations */
static List * WorkersWithoutReferenceTablePlacement(uint64 shardId, LOCKMODE lockMode);
static StringInfo CopyShardPlacementToWorkerNodeQuery(
	ShardPlacement *sourceShardPlacement,
	WorkerNode *workerNode,
	char transferMode);
static void ReplicateReferenceTableShardToNode(ShardInterval *shardInterval,
											   char *nodeName,
											   int nodePort);
static bool AnyRelationsModifiedInTransaction(List *relationIdList);
static List * ReplicatedMetadataSyncedDistributedTableList(void);
static bool NodeHasAllReferenceTableReplicas(WorkerNode *workerNode);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(upgrade_to_reference_table);
PG_FUNCTION_INFO_V1(replicate_reference_tables);

/*
 * replicate_reference_tables is a UDF to ensure that allreference tables are
 * replicated to all nodes.
 */
Datum
replicate_reference_tables(PG_FUNCTION_ARGS)
{
	Oid shardReplicationModeOid = PG_GETARG_OID(0);
	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);

	/* to prevent concurrent node additions while copying reference tables */
	LockRelationOid(DistNodeRelationId(), ShareLock);
	EnsureReferenceTablesExistOnAllNodesExtended(shardReplicationMode);

	/*
	 * Given the copying of reference tables and updating metadata have been done via a
	 * loopback connection we do not have to retain the lock on pg_dist_node anymore.
	 */
	UnlockRelationOid(DistNodeRelationId(), ShareLock);

	PG_RETURN_VOID();
}


/*
 * EnsureReferenceTablesExistOnAllNodes ensures that a shard placement for every
 * reference table exists on all nodes. If a node does not have a set of shard
 * placements, then citus_copy_shard_placement is called in a subtransaction
 * to pull the data to the new node.
 */
void
EnsureReferenceTablesExistOnAllNodes(void)
{
	EnsureReferenceTablesExistOnAllNodesExtended(TRANSFER_MODE_BLOCK_WRITES);
}


/*
 * EnsureReferenceTablesExistOnAllNodesExtended ensures that a shard placement for every
 * reference table exists on all nodes. If a node does not have a set of shard placements,
 * then citus_copy_shard_placement is called in a subtransaction to pull the data to the
 * new node.
 *
 * The transferMode is passed on to the implementation of the copy to control the locks
 * and transferMode.
 */
void
EnsureReferenceTablesExistOnAllNodesExtended(char transferMode)
{
	List *referenceTableIdList = NIL;
	uint64 shardId = INVALID_SHARD_ID;
	List *newWorkersList = NIL;
	const char *referenceTableName = NULL;
	int colocationId = GetReferenceTableColocationId();

	if (colocationId == INVALID_COLOCATION_ID)
	{
		/* we have no reference table yet. */
		return;
	}

	/*
	 * Most of the time this function should result in a conclusion where we do not need
	 * to copy any reference tables. To prevent excessive locking the majority of the time
	 * we run our precondition checks first with a lower lock. If, after checking with the
	 * lower lock, that we might need to copy reference tables we check with a more
	 * aggressive and self conflicting lock. It is important to be self conflicting in the
	 * second run to make sure that two concurrent calls to this routine will actually not
	 * run concurrently after the initial check.
	 *
	 * If after two iterations of precondition checks we still find the need for copying
	 * reference tables we exit the loop with all locks held. This will prevent concurrent
	 * DROP TABLE and create_reference_table calls so that the list of reference tables we
	 * operate on are stable.
	 *
	 * Since the changes to the reference table placements are made via loopback
	 * connections we release the locks held at the end of this function. Due to Citus
	 * only running transactions in READ COMMITTED mode we can be sure that other
	 * transactions correctly find the metadata entries.
	 */
	LOCKMODE lockmodes[] = { AccessShareLock, ExclusiveLock };
	for (int lockmodeIndex = 0; lockmodeIndex < lengthof(lockmodes); lockmodeIndex++)
	{
		LockColocationId(colocationId, lockmodes[lockmodeIndex]);

		referenceTableIdList = CitusTableTypeIdList(REFERENCE_TABLE);
		if (referenceTableIdList == NIL)
		{
			/*
			 * No reference tables exist, make sure that any locks obtained earlier are
			 * released. It will probably not matter, but we release the locks in the
			 * reverse order we obtained them in.
			 */
			for (int releaseLockmodeIndex = lockmodeIndex; releaseLockmodeIndex >= 0;
				 releaseLockmodeIndex--)
			{
				UnlockColocationId(colocationId, lockmodes[releaseLockmodeIndex]);
			}
			return;
		}

		Oid referenceTableId = linitial_oid(referenceTableIdList);
		referenceTableName = get_rel_name(referenceTableId);
		List *shardIntervalList = LoadShardIntervalList(referenceTableId);
		if (list_length(shardIntervalList) == 0)
		{
			/* check for corrupt metadata */
			ereport(ERROR, (errmsg("reference table \"%s\" does not have a shard",
								   referenceTableName)));
		}

		ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
		shardId = shardInterval->shardId;

		/*
		 * We only take an access share lock, otherwise we'll hold up citus_add_node.
		 * In case of create_reference_table() where we don't want concurrent writes
		 * to pg_dist_node, we have already acquired ShareLock on pg_dist_node.
		 */
		newWorkersList = WorkersWithoutReferenceTablePlacement(shardId, AccessShareLock);
		if (list_length(newWorkersList) == 0)
		{
			/*
			 * All workers alreaddy have a copy of the reference tables, make sure that
			 * any locks obtained earlier are released. It will probably not matter, but
			 * we release the locks in the reverse order we obtained them in.
			 */
			for (int releaseLockmodeIndex = lockmodeIndex; releaseLockmodeIndex >= 0;
				 releaseLockmodeIndex--)
			{
				UnlockColocationId(colocationId, lockmodes[releaseLockmodeIndex]);
			}
			return;
		}
	}

	/*
	 * citus_copy_shard_placement triggers metadata sync-up, which tries to
	 * acquire a ShareLock on pg_dist_node. We do master_copy_shad_placement
	 * in a separate connection. If we have modified pg_dist_node in the
	 * current backend, this will cause a deadlock.
	 */
	if (TransactionModifiedNodeMetadata)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot replicate reference tables in a transaction "
							   "that modified node metadata")));
	}

	/*
	 * Modifications to reference tables in current transaction are not visible
	 * to citus_copy_shard_placement, since it is done in a separate backend.
	 */
	if (AnyRelationsModifiedInTransaction(referenceTableIdList))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot replicate reference tables in a transaction "
							   "that modified a reference table")));
	}

	bool missingOk = false;
	ShardPlacement *sourceShardPlacement = ActiveShardPlacement(shardId, missingOk);
	if (sourceShardPlacement == NULL)
	{
		/* check for corrupt metadata */
		ereport(ERROR, (errmsg("reference table shard "
							   UINT64_FORMAT
							   " does not have an active shard placement",
							   shardId)));
	}

	WorkerNode *newWorkerNode = NULL;
	foreach_ptr(newWorkerNode, newWorkersList)
	{
		ereport(NOTICE, (errmsg("replicating reference table '%s' to %s:%d ...",
								referenceTableName, newWorkerNode->workerName,
								newWorkerNode->workerPort)));

		/*
		 * Call citus_copy_shard_placement using citus extension owner. Current
		 * user might not have permissions to do the copy.
		 */
		const char *userName = CitusExtensionOwnerName();
		int connectionFlags = OUTSIDE_TRANSACTION;

		MultiConnection *connection = GetNodeUserDatabaseConnection(
			connectionFlags, LocalHostName, PostPortNumber,
			userName, NULL);

		if (PQstatus(connection->pgConn) == CONNECTION_OK)
		{
			UseCoordinatedTransaction();

			RemoteTransactionBegin(connection);
			StringInfo placementCopyCommand =
				CopyShardPlacementToWorkerNodeQuery(sourceShardPlacement,
													newWorkerNode,
													transferMode);

			/*
			 * The placement copy command uses distributed execution to copy
			 * the shard. This is allowed when indicating that the backend is a
			 * rebalancer backend.
			 */
			ExecuteCriticalRemoteCommand(connection,
										 "SET LOCAL application_name TO "
										 CITUS_REBALANCER_NAME);
			ExecuteCriticalRemoteCommand(connection, placementCopyCommand->data);
			RemoteTransactionCommit(connection);
		}
		else
		{
			ereport(ERROR, (errmsg("could not open a connection to localhost "
								   "when replicating reference tables"),
							errdetail(
								"citus.replicate_reference_tables_on_activate = false "
								"requires localhost connectivity.")));
		}

		CloseConnection(connection);
	}

	/*
	 * Since reference tables have been copied via a loopback connection we do not have to
	 * retain our locks. Since Citus only runs well in READ COMMITTED mode we can be sure
	 * that other transactions will find the reference tables copied.
	 * We have obtained and held multiple locks, here we unlock them all in the reverse
	 * order we have obtained them in.
	 */
	for (int releaseLockmodeIndex = lengthof(lockmodes) - 1; releaseLockmodeIndex >= 0;
		 releaseLockmodeIndex--)
	{
		UnlockColocationId(colocationId, lockmodes[releaseLockmodeIndex]);
	}
}


List *
GetShardCopyCommandsForMissingReferenceTablePlacements(char transferMode)
{
	int colocationId = GetReferenceTableColocationId();
	if (colocationId == INVALID_COLOCATION_ID)
	{
		/* we have no reference table yet. */
		return NIL;
	}

	/* prevent destructive operations for reference tables */
	LockColocationId(colocationId, AccessShareLock);

	List *referenceTableIdList = CitusTableTypeIdList(REFERENCE_TABLE);
	if (list_length(referenceTableIdList) <= 0)
	{
		/* no reference tables, means no missing ones to copy */
		return NIL;
	}

	Oid referenceTableId = linitial_oid(referenceTableIdList);
	List *shardIntervalList = LoadShardIntervalList(referenceTableId);
	if (list_length(shardIntervalList) == 0)
	{
		const char *referenceTableName = get_rel_name(referenceTableId);

		/* check for corrupt metadata */
		ereport(ERROR, (errmsg("reference table \"%s\" does not have a shard",
							   referenceTableName)));
	}

	ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
	uint64 shardId = shardInterval->shardId;

	List *newWorkersList = WorkersWithoutReferenceTablePlacement(shardId,
																 AccessShareLock);

	const bool missingOk = false;
	ShardPlacement *sourceShardPlacement = ActiveShardPlacement(shardId, missingOk);

	WorkerNode *newWorkerNode = NULL;
	List *commandList = NIL;
	foreach_ptr(newWorkerNode, newWorkersList)
	{
		StringInfo placementCopyCommand =
			CopyShardPlacementToWorkerNodeQuery(sourceShardPlacement, newWorkerNode,
												transferMode);
		commandList = lappend(commandList, placementCopyCommand->data);
	}

	return commandList;
}


/*
 * AnyRelationsModifiedInTransaction returns true if any of the given relations
 * were modified in the current transaction.
 */
static bool
AnyRelationsModifiedInTransaction(List *relationIdList)
{
	Oid relationId = InvalidOid;

	foreach_oid(relationId, relationIdList)
	{
		if (GetRelationDDLAccessMode(relationId) != RELATION_NOT_ACCESSED ||
			GetRelationDMLAccessMode(relationId) != RELATION_NOT_ACCESSED)
		{
			return true;
		}
	}

	return false;
}


/*
 * WorkersWithoutReferenceTablePlacement returns a list of workers (WorkerNode) that
 * do not yet have a placement for the given reference table shard ID, but are
 * supposed to.
 */
static List *
WorkersWithoutReferenceTablePlacement(uint64 shardId, LOCKMODE lockMode)
{
	List *workersWithoutPlacements = NIL;

	List *shardPlacementList = ActiveShardPlacementList(shardId);

	List *workerNodeList = ReferenceTablePlacementNodeList(lockMode);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		ShardPlacement *targetPlacement = SearchShardPlacementInList(shardPlacementList,
																	 nodeName, nodePort);
		if (targetPlacement == NULL)
		{
			workersWithoutPlacements = lappend(workersWithoutPlacements, workerNode);
		}
	}

	return workersWithoutPlacements;
}


/*
 * CopyShardPlacementToWorkerNodeQuery returns the citus_copy_shard_placement
 * command to copy the given shard placement to given node.
 */
static StringInfo
CopyShardPlacementToWorkerNodeQuery(ShardPlacement *sourceShardPlacement,
									WorkerNode *workerNode,
									char transferMode)
{
	StringInfo queryString = makeStringInfo();

	const char *transferModeString =
		transferMode == TRANSFER_MODE_BLOCK_WRITES ? "block_writes" :
		transferMode == TRANSFER_MODE_FORCE_LOGICAL ? "force_logical" :
		"auto";

	appendStringInfo(queryString,
					 "SELECT citus_copy_shard_placement("
					 UINT64_FORMAT ", %s, %d, %s, %d, "
								   "transfer_mode := %s)",
					 sourceShardPlacement->shardId,
					 quote_literal_cstr(sourceShardPlacement->nodeName),
					 sourceShardPlacement->nodePort,
					 quote_literal_cstr(workerNode->workerName),
					 workerNode->workerPort,
					 quote_literal_cstr(transferModeString));

	return queryString;
}


/*
 * upgrade_to_reference_table was removed, but we maintain a dummy implementation
 * to support downgrades.
 */
Datum
upgrade_to_reference_table(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("this function is deprecated and no longer used")));
}


/*
 * ReplicateShardToNode function replicates given shard to the given worker node
 * in a separate transaction. If the worker already has
 * a replica of the shard this is a no-op. This function also modifies metadata
 * by inserting/updating related rows in pg_dist_placement.
 *
 * IMPORTANT: This should only be used to replicate shards of a reference
 * table.
 */
static void
ReplicateReferenceTableShardToNode(ShardInterval *shardInterval, char *nodeName,
								   int nodePort)
{
	uint64 shardId = shardInterval->shardId;

	bool missingOk = false;
	ShardPlacement *sourceShardPlacement = ActiveShardPlacement(shardId, missingOk);
	char *srcNodeName = sourceShardPlacement->nodeName;
	uint32 srcNodePort = sourceShardPlacement->nodePort;
	bool includeDataCopy = true; /* TODO: consider using logical replication */
	List *ddlCommandList = CopyShardCommandList(shardInterval, srcNodeName, srcNodePort,
												includeDataCopy);

	ereport(NOTICE, (errmsg("Replicating reference table \"%s\" to the node %s:%d",
							get_rel_name(shardInterval->relationId), nodeName,
							nodePort)));

	/* send commands to new workers, the current user should be a superuser */
	Assert(superuser());
	WorkerNode *workerNode = FindWorkerNode(nodeName, nodePort);
	SendMetadataCommandListToWorkerListInCoordinatedTransaction(list_make1(workerNode),
																CurrentUserName(),
																ddlCommandList);
	int32 groupId = GroupForNode(nodeName, nodePort);

	uint64 placementId = GetNextPlacementId();
	InsertShardPlacementRow(shardId, placementId, SHARD_STATE_ACTIVE, 0,
							groupId);

	if (ShouldSyncTableMetadata(shardInterval->relationId))
	{
		char *placementCommand = PlacementUpsertCommand(shardId, placementId,
														SHARD_STATE_ACTIVE, 0,
														groupId);

		SendCommandToWorkersWithMetadata(placementCommand);
	}
}


/*
 * CreateReferenceTableColocationId creates a new co-location id for reference tables and
 * writes it into pg_dist_colocation, then returns the created co-location id. Since there
 * can be only one colocation group for all kinds of reference tables, if a co-location id
 * is already created for reference tables, this function returns it without creating
 * anything.
 */
uint32
CreateReferenceTableColocationId()
{
	int shardCount = 1;
	Oid distributionColumnType = InvalidOid;
	Oid distributionColumnCollation = InvalidOid;

	/*
	 * We don't maintain replication factor of reference tables anymore and
	 * just use -1 instead. We don't use this value in any places.
	 */
	int replicationFactor = -1;

	/* check for existing colocations */
	uint32 colocationId =
		ColocationId(shardCount, replicationFactor, distributionColumnType,
					 distributionColumnCollation);

	if (colocationId == INVALID_COLOCATION_ID)
	{
		colocationId = CreateColocationGroup(shardCount, replicationFactor,
											 distributionColumnType,
											 distributionColumnCollation);
	}

	return colocationId;
}


uint32
GetReferenceTableColocationId()
{
	int shardCount = 1;
	Oid distributionColumnType = InvalidOid;
	Oid distributionColumnCollation = InvalidOid;

	/*
	 * We don't maintain replication factor of reference tables anymore and
	 * just use -1 instead. We don't use this value in any places.
	 */
	int replicationFactor = -1;

	/* check for existing colocations */
	uint32 colocationId =
		ColocationId(shardCount, replicationFactor, distributionColumnType,
					 distributionColumnCollation);

	return colocationId;
}


/*
 * DeleteAllReplicatedTablePlacementsFromNodeGroup function iterates over
 * list of reference and replicated hash distributed tables and deletes
 * all placements from pg_dist_placement table for given group.
 */
void
DeleteAllReplicatedTablePlacementsFromNodeGroup(int32 groupId, bool localOnly)
{
	List *referenceTableList = CitusTableTypeIdList(REFERENCE_TABLE);
	List *replicatedMetadataSyncedDistributedTableList =
		ReplicatedMetadataSyncedDistributedTableList();

	List *replicatedTableList =
		list_concat(referenceTableList, replicatedMetadataSyncedDistributedTableList);

	/* if there are no reference tables, we do not need to do anything */
	if (list_length(replicatedTableList) == 0)
	{
		return;
	}

	StringInfo deletePlacementCommand = makeStringInfo();
	Oid replicatedTableId = InvalidOid;
	foreach_oid(replicatedTableId, replicatedTableList)
	{
		List *placements =
			GroupShardPlacementsForTableOnGroup(replicatedTableId, groupId);
		if (list_length(placements) == 0)
		{
			/*
			 * This happens either the node was previously disabled or the table
			 * doesn't have placement on this node.
			 */
			continue;
		}

		GroupShardPlacement *placement = NULL;
		foreach_ptr(placement, placements)
		{
			LockShardDistributionMetadata(placement->shardId, ExclusiveLock);

			DeleteShardPlacementRow(placement->placementId);

			if (!localOnly)
			{
				resetStringInfo(deletePlacementCommand);
				appendStringInfo(deletePlacementCommand,
								 "DELETE FROM pg_catalog.pg_dist_placement "
								 "WHERE placementid = " UINT64_FORMAT,
								 placement->placementId);

				SendCommandToWorkersWithMetadata(deletePlacementCommand->data);
			}
		}
	}
}


/*
 * ReplicatedMetadataSyncedDistributedTableList is a helper function which returns the
 * list of replicated hash distributed tables.
 */
static List *
ReplicatedMetadataSyncedDistributedTableList(void)
{
	List *distributedRelationList = CitusTableTypeIdList(DISTRIBUTED_TABLE);
	List *replicatedHashDistributedTableList = NIL;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, distributedRelationList)
	{
		if (ShouldSyncTableMetadata(relationId) && !SingleReplicatedTable(relationId))
		{
			replicatedHashDistributedTableList =
				lappend_oid(replicatedHashDistributedTableList, relationId);
		}
	}

	return replicatedHashDistributedTableList;
}


/* CompareOids is a comparison function for sort shard oids */
int
CompareOids(const void *leftElement, const void *rightElement)
{
	Oid *leftId = (Oid *) leftElement;
	Oid *rightId = (Oid *) rightElement;

	if (*leftId > *rightId)
	{
		return 1;
	}
	else if (*leftId < *rightId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


/*
 * ReplicateAllReferenceTablesToNode function finds all reference tables and
 * replicates them to the given worker node. It also modifies pg_dist_colocation
 * table to update the replication factor column when necessary. This function
 * skips reference tables if that node already has healthy placement of that
 * reference table to prevent unnecessary data transfer.
 */
void
ReplicateAllReferenceTablesToNode(WorkerNode *workerNode)
{
	int colocationId = GetReferenceTableColocationId();
	if (colocationId == INVALID_COLOCATION_ID)
	{
		/* no reference tables in system */
		return;
	}

	/* prevent changes in table set while replicating reference tables */
	LockColocationId(colocationId, RowExclusiveLock);

	List *referenceTableList = CitusTableTypeIdList(REFERENCE_TABLE);

	/* if there is no reference table, we do not need to replicate anything */
	if (list_length(referenceTableList) > 0)
	{
		List *referenceShardIntervalList = NIL;

		/*
		 * We sort the reference table list to prevent deadlocks in concurrent
		 * ReplicateAllReferenceTablesToAllNodes calls.
		 */
		referenceTableList = SortList(referenceTableList, CompareOids);
		Oid referenceTableId = InvalidOid;
		foreach_oid(referenceTableId, referenceTableList)
		{
			List *shardIntervalList = LoadShardIntervalList(referenceTableId);
			ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);

			List *shardPlacementList =
				ShardPlacementListIncludingOrphanedPlacements(shardInterval->shardId);
			ShardPlacement *targetPlacement =
				SearchShardPlacementInList(shardPlacementList,
										   workerNode->workerName,
										   workerNode->workerPort);
			if (targetPlacement != NULL &&
				targetPlacement->shardState == SHARD_STATE_ACTIVE)
			{
				/* We already have the shard, nothing to do */
				continue;
			}


			referenceShardIntervalList = lappend(referenceShardIntervalList,
												 shardInterval);
		}

		if (ClusterHasKnownMetadataWorkers())
		{
			BlockWritesToShardList(referenceShardIntervalList);
		}

		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, referenceShardIntervalList)
		{
			uint64 shardId = shardInterval->shardId;

			LockShardDistributionMetadata(shardId, ExclusiveLock);

			ReplicateReferenceTableShardToNode(shardInterval,
											   workerNode->workerName,
											   workerNode->workerPort);
		}

		/* create foreign constraints between reference tables */
		foreach_ptr(shardInterval, referenceShardIntervalList)
		{
			List *commandList = CopyShardForeignConstraintCommandList(shardInterval);

			/* send commands to new workers, the current user should be a superuser */
			Assert(superuser());
			SendMetadataCommandListToWorkerListInCoordinatedTransaction(
				list_make1(workerNode),
				CurrentUserName(),
				commandList);
		}
	}
}


/*
 * ErrorIfNotAllNodesHaveReferenceTableReplicas throws an error when one of the
 * nodes in the list does not have reference table replicas.
 */
void
ErrorIfNotAllNodesHaveReferenceTableReplicas(List *workerNodeList)
{
	WorkerNode *workerNode = NULL;

	foreach_ptr(workerNode, workerNodeList)
	{
		if (!NodeHasAllReferenceTableReplicas(workerNode))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("reference tables have not been replicated to "
								   "node %s:%d yet",
								   workerNode->workerName,
								   workerNode->workerPort),
							errdetail("Reference tables are lazily replicated after "
									  "adding a node, but must exist before shards can "
									  "be created on that node."),
							errhint("Run SELECT replicate_reference_tables(); to "
									"ensure reference tables exist on all nodes.")));
		}
	}
}


/*
 * NodeHasAllReferenceTablesReplicas returns whether the given worker node has reference
 * table replicas. If there are no reference tables the function returns true.
 *
 * This function does not do any locking, so the situation could change immediately after,
 * though we can only ever transition from false to true, so only "false" could be the
 * incorrect answer.
 *
 * In the case where the function returns true because no reference tables exist
 * on the node, a reference table could be created immediately after. However, the
 * creation logic guarantees that this reference table will be created on all the
 * nodes, so our answer was correct.
 */
static bool
NodeHasAllReferenceTableReplicas(WorkerNode *workerNode)
{
	List *referenceTableIdList = CitusTableTypeIdList(REFERENCE_TABLE);

	if (list_length(referenceTableIdList) == 0)
	{
		/* no reference tables exist */
		return true;
	}

	Oid referenceTableId = linitial_oid(referenceTableIdList);
	List *shardIntervalList = LoadShardIntervalList(referenceTableId);
	if (list_length(shardIntervalList) != 1)
	{
		/* check for corrupt metadata */
		ereport(ERROR, (errmsg("reference table \"%s\" can only have 1 shard",
							   get_rel_name(referenceTableId))));
	}

	ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
	List *shardPlacementList = ActiveShardPlacementList(shardInterval->shardId);

	ShardPlacement *placement = NULL;
	foreach_ptr(placement, shardPlacementList)
	{
		if (placement->groupId == workerNode->groupId)
		{
			/* our worker has a reference table placement */
			return true;
		}
	}

	return false;
}
