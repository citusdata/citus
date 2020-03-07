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
#include "distributed/master_protocol.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/reference_table_utils.h"
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
static List * WorkersWithoutReferenceTablePlacement(uint64 shardId);
static void CopyShardPlacementToNewWorkerNode(ShardPlacement *sourceShardPlacement,
											  WorkerNode *newWorkerNode);
static void ReplicateSingleShardTableToAllNodes(Oid relationId);
static void ReplicateShardToAllNodes(ShardInterval *shardInterval);
static void ReplicateShardToNode(ShardInterval *shardInterval, char *nodeName,
								 int nodePort);
static void ConvertToReferenceTableMetadata(Oid relationId, uint64 shardId);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(upgrade_to_reference_table);
PG_FUNCTION_INFO_V1(replicate_reference_tables);


/*
 * IsReferenceTable returns whether the given relation ID identifies a reference
 * table.
 */
bool
IsReferenceTable(Oid relationId)
{
	CitusTableCacheEntry *tableEntry = LookupCitusTableCacheEntry(relationId);

	if (!tableEntry->isCitusTable)
	{
		return false;
	}

	if (tableEntry->partitionMethod != DISTRIBUTE_BY_NONE)
	{
		return false;
	}

	return true;
}


/*
 * replicate_reference_tables is a UDF to ensure that allreference tables are
 * replicated to all nodes.
 */
Datum
replicate_reference_tables(PG_FUNCTION_ARGS)
{
	EnsureReferenceTablesExistOnAllNodes();

	PG_RETURN_VOID();
}


/*
 * EnsureReferenceTablesExistOnAllNodes ensures that a shard placement for every
 * reference table exists on all nodes. If a node does not have a set of shard
 * placements, then master_copy_shard_placement is called in a subtransaction
 * to pull the data to the new node.
 */
void
EnsureReferenceTablesExistOnAllNodes(void)
{
	List *referenceTableIdList = ReferenceTableOidList();
	if (list_length(referenceTableIdList) == 0)
	{
		/* no reference tables exist */
		return;
	}

	Oid referenceTableId = linitial_oid(referenceTableIdList);
	List *shardIntervalList = LoadShardIntervalList(referenceTableId);
	if (list_length(shardIntervalList) == 0)
	{
		/* check for corrupt metadata */
		ereport(ERROR, (errmsg("reference table \"%s\" does not have a shard",
							   get_rel_name(referenceTableId))));
	}

	ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
	uint64 shardId = shardInterval->shardId;

	/* prevent this funcion from running concurrently with itself */
	int colocationId = TableColocationId(referenceTableId);
	LockColocationId(colocationId, ExclusiveLock);

	List *newWorkersList = WorkersWithoutReferenceTablePlacement(shardId);
	if (list_length(newWorkersList) == 0)
	{
		/* nothing to do, no need for lock */
		UnlockColocationId(colocationId, ExclusiveLock);
		return;
	}

	/* TODO: ensure reference tables have not been modified in this transaction */

	bool missingOk = false;
	ShardPlacement *sourceShardPlacement = ActiveShardPlacement(shardId, missingOk);
	if (sourceShardPlacement == NULL)
	{
		/* check for corrupt metadata */
		ereport(ERROR, (errmsg("reference table shard " UINT64_FORMAT " does not "
																	  "have an active shard placement",
							   shardId)));
	}

	WorkerNode *newWorkerNode = NULL;
	foreach_ptr(newWorkerNode, newWorkersList)
	{
		CopyShardPlacementToNewWorkerNode(sourceShardPlacement, newWorkerNode);
	}

	/*
	 * Unblock other backends, they will probably observe that there are no
	 * more worker nodes without placements, unless nodes were added concurrently
	 */
	UnlockColocationId(colocationId, ExclusiveLock);
}


/*
 * WorkersWithoutReferenceTablePlacement returns a list of workers (WorkerNode) that
 * do not yet have a placement for the given reference table shard ID, but are
 * supposed to.
 */
static List *
WorkersWithoutReferenceTablePlacement(uint64 shardId)
{
	List *workersWithoutPlacements = NIL;

	List *shardPlacementList = ActiveShardPlacementList(shardId);

	/* we only take an access share lock, otherwise we'll hold up master_add_node */
	List *workerNodeList = ReferenceTablePlacementNodeList(AccessShareLock);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		bool missingWorkerOk = true;
		ShardPlacement *targetPlacement = SearchShardPlacementInList(shardPlacementList,
																	 nodeName, nodePort,
																	 missingWorkerOk);
		if (targetPlacement == NULL)
		{
			workersWithoutPlacements = lappend(workersWithoutPlacements, workerNode);
		}
	}

	return workersWithoutPlacements;
}


/*
 * CopyShardPlacementToNewWorkerNode runs master_copy_shard_placement in a
 * subtransaction by connecting to localhost.
 */
static void
CopyShardPlacementToNewWorkerNode(ShardPlacement *sourceShardPlacement,
								  WorkerNode *newWorkerNode)
{
	int connectionFlags = OUTSIDE_TRANSACTION;
	StringInfo queryString = makeStringInfo();
	const char *userName = CitusExtensionOwnerName();

	MultiConnection *connection = GetNodeUserDatabaseConnection(
		connectionFlags, "localhost", PostPortNumber,
		userName, NULL);

	appendStringInfo(queryString,
					 "SELECT master_copy_shard_placement("
					 UINT64_FORMAT ", %s, %d, %s, %d, do_repair := false)",
					 sourceShardPlacement->shardId,
					 quote_literal_cstr(sourceShardPlacement->nodeName),
					 sourceShardPlacement->nodePort,
					 quote_literal_cstr(newWorkerNode->workerName),
					 newWorkerNode->workerPort);

	ExecuteCriticalRemoteCommand(connection, queryString->data);
}


/*
 * upgrade_to_reference_table accepts a broadcast table which has only one shard and
 * replicates it across all nodes to create a reference table. It also modifies related
 * metadata to mark the table as reference.
 */
Datum
upgrade_to_reference_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	EnsureTableOwner(relationId);

	if (!IsCitusTable(relationId))
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" is not distributed.", relationName),
						errhint("Instead, you can use; "
								"create_reference_table('%s');", relationName)));
	}

	CitusTableCacheEntry *tableEntry = LookupCitusTableCacheEntry(relationId);

	if (tableEntry->partitionMethod == DISTRIBUTE_BY_NONE)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" is already a reference table",
								  relationName)));
	}

	if (tableEntry->replicationModel == REPLICATION_MODEL_STREAMING)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot upgrade to reference table"),
						errdetail("Upgrade is only supported for statement-based "
								  "replicated tables but \"%s\" is streaming replicated",
								  relationName)));
	}

	LockRelationOid(relationId, AccessExclusiveLock);

	List *shardIntervalList = LoadShardIntervalList(relationId);
	if (list_length(shardIntervalList) != 1)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" shard count is not one. Only "
								  "relations with one shard can be upgraded to "
								  "reference tables.", relationName)));
	}

	ReplicateSingleShardTableToAllNodes(relationId);

	PG_RETURN_VOID();
}


/*
 * ReplicateSingleShardTableToAllNodes accepts a broadcast table and replicates
 * it to all worker nodes, and the coordinator if it has been added by the user
 * to pg_dist_node. It assumes that caller of this function ensures that given
 * broadcast table has only one shard.
 */
static void
ReplicateSingleShardTableToAllNodes(Oid relationId)
{
	List *shardIntervalList = LoadShardIntervalList(relationId);
	ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
	uint64 shardId = shardInterval->shardId;

	List *foreignConstraintCommandList = CopyShardForeignConstraintCommandList(
		shardInterval);

	if (foreignConstraintCommandList != NIL || TableReferenced(relationId))
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot upgrade to reference table"),
						errdetail("Relation \"%s\" is part of a foreign constraint. "
								  "Foreign key constraints are not allowed "
								  "from or to reference tables.", relationName)));
	}

	/*
	 * ReplicateShardToAllNodes function opens separate transactions (i.e., not part
	 * of any coordinated transactions) to each worker and replicates given shard to all
	 * workers. If a worker already has a healthy replica of given shard, it skips that
	 * worker to prevent copying unnecessary data.
	 */
	ReplicateShardToAllNodes(shardInterval);

	/*
	 * We need to update metadata tables to mark this table as reference table. We modify
	 * pg_dist_partition, pg_dist_colocation and pg_dist_shard tables in
	 * ConvertToReferenceTableMetadata function.
	 */
	ConvertToReferenceTableMetadata(relationId, shardId);

	/*
	 * After the table has been officially marked as a reference table, we need to create
	 * the reference table itself and insert its pg_dist_partition, pg_dist_shard and
	 * existing pg_dist_placement rows.
	 */
	CreateTableMetadataOnWorkers(relationId);
}


/*
 * ReplicateShardToAllNodes function replicates given shard to all nodes
 * in separate transactions. While replicating, it only replicates the shard to the
 * nodes which does not have a healthy replica of the shard. However, this function
 * does not obtain any lock on shard resource and shard metadata. It is caller's
 * responsibility to take those locks.
 */
static void
ReplicateShardToAllNodes(ShardInterval *shardInterval)
{
	/* prevent concurrent pg_dist_node changes */
	List *workerNodeList = ReferenceTablePlacementNodeList(ShareLock);

	/*
	 * We will iterate over all worker nodes and if a healthy placement does not exist
	 * at given node we will copy the shard to that node. Then we will also modify
	 * the metadata to reflect newly copied shard.
	 */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		ReplicateShardToNode(shardInterval, nodeName, nodePort);
	}
}


/*
 * ReplicateShardToNode function replicates given shard to the given worker node
 * in a separate transaction. While replicating, it only replicates the shard to the
 * workers which does not have a healthy replica of the shard. This function also modifies
 * metadata by inserting/updating related rows in pg_dist_placement.
 */
static void
ReplicateShardToNode(ShardInterval *shardInterval, char *nodeName, int nodePort)
{
	uint64 shardId = shardInterval->shardId;

	bool missingOk = false;
	ShardPlacement *sourceShardPlacement = ActiveShardPlacement(shardId, missingOk);
	char *srcNodeName = sourceShardPlacement->nodeName;
	uint32 srcNodePort = sourceShardPlacement->nodePort;
	bool includeData = true;
	List *ddlCommandList =
		CopyShardCommandList(shardInterval, srcNodeName, srcNodePort, includeData);

	List *shardPlacementList = ShardPlacementList(shardId);
	bool missingWorkerOk = true;
	ShardPlacement *targetPlacement = SearchShardPlacementInList(shardPlacementList,
																 nodeName, nodePort,
																 missingWorkerOk);
	char *tableOwner = TableOwner(shardInterval->relationId);

	/*
	 * Although this function is used for reference tables, and reference table shard
	 * placements always have shardState = SHARD_STATE_ACTIVE, in case of an upgrade
	 * of a non-reference table to reference table, unhealty placements may exist.
	 * In this case, repair the shard placement and update its state in pg_dist_placement.
	 */
	if (targetPlacement == NULL || targetPlacement->shardState != SHARD_STATE_ACTIVE)
	{
		uint64 placementId = 0;
		int32 groupId = 0;

		ereport(NOTICE, (errmsg("Replicating reference table \"%s\" to the node %s:%d",
								get_rel_name(shardInterval->relationId), nodeName,
								nodePort)));

		EnsureNoModificationsHaveBeenDone();
		SendCommandListToWorkerInSingleTransaction(nodeName, nodePort, tableOwner,
												   ddlCommandList);
		if (targetPlacement == NULL)
		{
			groupId = GroupForNode(nodeName, nodePort);

			placementId = GetNextPlacementId();
			InsertShardPlacementRow(shardId, placementId, SHARD_STATE_ACTIVE, 0,
									groupId);
		}
		else
		{
			groupId = targetPlacement->groupId;
			placementId = targetPlacement->placementId;
			UpdateShardPlacementState(placementId, SHARD_STATE_ACTIVE);
		}

		/*
		 * Although ReplicateShardToAllNodes is used only for reference tables,
		 * during the upgrade phase, the placements are created before the table is
		 * marked as a reference table. All metadata (including the placement
		 * metadata) will be copied to workers after all reference table changed
		 * are finished.
		 */
		if (ShouldSyncTableMetadata(shardInterval->relationId))
		{
			char *placementCommand = PlacementUpsertCommand(shardId, placementId,
															SHARD_STATE_ACTIVE, 0,
															groupId);

			SendCommandToWorkersWithMetadata(placementCommand);
		}
	}
}


/*
 * ConvertToReferenceTableMetadata accepts a broadcast table and modifies its metadata to
 * reference table metadata. To do this, this function updates pg_dist_partition,
 * pg_dist_colocation and pg_dist_shard. This function assumes that caller ensures that
 * given broadcast table has only one shard.
 */
static void
ConvertToReferenceTableMetadata(Oid relationId, uint64 shardId)
{
	uint32 currentColocationId = TableColocationId(relationId);
	uint32 newColocationId = CreateReferenceTableColocationId();
	Var *distributionColumn = NULL;
	char shardStorageType = ShardStorageType(relationId);
	text *shardMinValue = NULL;
	text *shardMaxValue = NULL;

	/* delete old metadata rows */
	DeletePartitionRow(relationId);
	DeleteColocationGroupIfNoTablesBelong(currentColocationId);
	DeleteShardRow(shardId);

	/* insert new metadata rows */
	InsertIntoPgDistPartition(relationId, DISTRIBUTE_BY_NONE, distributionColumn,
							  newColocationId, REPLICATION_MODEL_2PC);
	InsertShardRow(relationId, shardId, shardStorageType, shardMinValue, shardMaxValue);
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


/*
 * DeleteAllReferenceTablePlacementsFromNodeGroup function iterates over list of reference
 * tables and deletes all reference table placements from pg_dist_placement table
 * for given group.
 */
void
DeleteAllReferenceTablePlacementsFromNodeGroup(int32 groupId)
{
	List *referenceTableList = ReferenceTableOidList();
	List *referenceShardIntervalList = NIL;

	/* if there are no reference tables, we do not need to do anything */
	if (list_length(referenceTableList) == 0)
	{
		return;
	}

	/*
	 * We sort the reference table list to prevent deadlocks in concurrent
	 * DeleteAllReferenceTablePlacementsFromNodeGroup calls.
	 */
	referenceTableList = SortList(referenceTableList, CompareOids);
	if (ClusterHasKnownMetadataWorkers())
	{
		referenceShardIntervalList = GetSortedReferenceShardIntervals(referenceTableList);

		BlockWritesToShardList(referenceShardIntervalList);
	}

	StringInfo deletePlacementCommand = makeStringInfo();
	Oid referenceTableId = InvalidOid;
	foreach_oid(referenceTableId, referenceTableList)
	{
		List *placements = GroupShardPlacementsForTableOnGroup(referenceTableId,
															   groupId);
		if (list_length(placements) == 0)
		{
			/* this happens if the node was previously disabled */
			continue;
		}

		GroupShardPlacement *placement = (GroupShardPlacement *) linitial(placements);

		LockShardDistributionMetadata(placement->shardId, ExclusiveLock);

		DeleteShardPlacementRow(placement->placementId);

		resetStringInfo(deletePlacementCommand);
		appendStringInfo(deletePlacementCommand,
						 "DELETE FROM pg_dist_placement WHERE placementid = "
						 UINT64_FORMAT,
						 placement->placementId);
		SendCommandToWorkersWithMetadata(deletePlacementCommand->data);
	}
}


/*
 * ReferenceTableOidList function scans pg_dist_partition to create a list of all
 * reference tables. To create the list, it performs sequential scan. Since it is not
 * expected that this function will be called frequently, it is OK not to use index scan.
 * If this function becomes performance bottleneck, it is possible to modify this function
 * to perform index scan.
 */
List *
ReferenceTableOidList()
{
	List *referenceTableList = NIL;

	List *distTableOidList = DistTableOidList();
	Oid relationId = InvalidOid;
	foreach_oid(relationId, distTableOidList)
	{
		CitusTableCacheEntry *cacheEntry = LookupCitusTableCacheEntry(relationId);

		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE)
		{
			referenceTableList = lappend_oid(referenceTableList, relationId);
		}
	}

	return referenceTableList;
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
 * ReferenceTableReplicationFactor returns the replication factor for
 * reference tables.
 */
int
ReferenceTableReplicationFactor(void)
{
	List *nodeList = ReferenceTablePlacementNodeList(NoLock);
	int replicationFactor = list_length(nodeList);
	return replicationFactor;
}
