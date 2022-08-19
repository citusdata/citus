/*-------------------------------------------------------------------------
 *
 * shard_split.c
 *
 * Function definitions for the shard split.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "common/hashfn.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "distributed/utils/array_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "distributed/shared_library_init.h"
#include "distributed/adaptive_executor.h"
#include "distributed/colocation_utils.h"
#include "distributed/hash_helpers.h"
#include "distributed/metadata_cache.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_split.h"
#include "distributed/reference_table_utils.h"
#include "distributed/repair_shards.h"
#include "distributed/resource_lock.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "distributed/shared_library_init.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/utils/distribution_column_map.h"
#include "commands/dbcommands.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/shard_rebalancer.h"
#include "postmaster/postmaster.h"

/*
 * Entry for map that tracks ShardInterval -> Placement Node
 * created by split workflow.
 */
typedef struct ShardCreatedByWorkflowEntry
{
	ShardInterval *shardIntervalKey;
	WorkerNode *workerNodeValue;
} ShardCreatedByWorkflowEntry;

/*
 * Entry for map that trackes dummy shards.
 * Key: node + owner
 * Value: List of dummy shards for that node + owner
 */
typedef struct GroupedDummyShards
{
	NodeAndOwner key;
	List *shardIntervals;
} GroupedDummyShards;

/* Function declarations */
static void ErrorIfCannotSplitShard(SplitOperation splitOperation,
									ShardInterval *sourceShard);
static void ErrorIfCannotSplitShardExtended(SplitOperation splitOperation,
											ShardInterval *shardIntervalToSplit,
											List *shardSplitPointsList,
											List *nodeIdsForPlacementList);
static void ErrorIfModificationAndSplitInTheSameTransaction(SplitOperation
															splitOperation);
static void ErrorIfMultipleNonblockingSplitInTheSameTransaction(SplitOperation
																splitOperation);
static void CreateSplitShardsForShardGroup(HTAB *mapOfShardToPlacementCreatedByWorkflow,
										   List *shardGroupSplitIntervalListList,
										   List *workersForPlacementList);
static void CreateDummyShardsForShardGroup(HTAB *mapOfDummyShardToPlacement,
										   List *sourceColocatedShardIntervalList,
										   List *shardGroupSplitIntervalListList,
										   WorkerNode *sourceWorkerNode,
										   List *workersForPlacementList);
static HTAB * CreateWorkerForPlacementSet(List *workersForPlacementList);
static void CreateAuxiliaryStructuresForShardGroup(List *shardGroupSplitIntervalListList,
												   List *workersForPlacementList,
												   bool includeReplicaIdentity);
static void CreateReplicaIdentitiesForDummyShards(HTAB *mapOfDummyShardToPlacement);
static void CreateObjectOnPlacement(List *objectCreationCommandList,
									WorkerNode *workerNode);
static List *    CreateSplitIntervalsForShardGroup(List *sourceColocatedShardList,
												   List *splitPointsForShard);
static void CreateSplitIntervalsForShard(ShardInterval *sourceShard,
										 List *splitPointsForShard,
										 List **shardSplitChildrenIntervalList);
static void BlockingShardSplit(SplitOperation splitOperation,
							   uint64 splitWorkflowId,
							   List *sourceColocatedShardIntervalList,
							   List *shardSplitPointsList,
							   List *workersForPlacementList,
							   DistributionColumnMap *distributionColumnOverrides);
static void NonBlockingShardSplit(SplitOperation splitOperation,
								  uint64 splitWorkflowId,
								  List *sourceColocatedShardIntervalList,
								  List *shardSplitPointsList,
								  List *workersForPlacementList,
								  DistributionColumnMap *distributionColumnOverrides,
								  uint32 targetColocationId);
static void DoSplitCopy(WorkerNode *sourceShardNode,
						List *sourceColocatedShardIntervalList,
						List *shardGroupSplitIntervalListList,
						List *workersForPlacementList,
						char *snapShotName,
						DistributionColumnMap *distributionColumnOverrides);
static StringInfo CreateSplitCopyCommand(ShardInterval *sourceShardSplitInterval,
										 char *distributionColumnName,
										 List *splitChildrenShardIntervalList,
										 List *workersForPlacementList);
static Task * CreateSplitCopyTask(StringInfo splitCopyUdfCommand, char *snapshotName, int
								  taskId, uint64 jobId);
static void UpdateDistributionColumnsForShardGroup(List *colocatedShardList,
												   DistributionColumnMap *distCols,
												   char distributionMethod,
												   int shardCount,
												   uint32 colocationId);
static void InsertSplitChildrenShardMetadata(List *shardGroupSplitIntervalListList,
											 List *workersForPlacementList);
static void CreatePartitioningHierarchy(List *shardGroupSplitIntervalListList,
										List *workersForPlacementList);
static void CreateForeignKeyConstraints(List *shardGroupSplitIntervalListList,
										List *workersForPlacementList);
static void TryDropSplitShardsOnFailure(HTAB *mapOfShardToPlacementCreatedByWorkflow);
static HTAB * CreateEmptyMapForShardsCreatedByWorkflow();
static Task * CreateTaskForDDLCommandList(List *ddlCommandList, WorkerNode *workerNode);
static StringInfo CreateSplitShardReplicationSetupUDF(
	List *sourceColocatedShardIntervalList, List *shardGroupSplitIntervalListList,
	List *destinationWorkerNodesList,
	DistributionColumnMap *
	distributionColumnOverrides);
static List * ParseReplicationSlotInfoFromResult(PGresult *result);

static List * ExecuteSplitShardReplicationSetupUDF(WorkerNode *sourceWorkerNode,
												   List *sourceColocatedShardIntervalList,
												   List *shardGroupSplitIntervalListList,
												   List *destinationWorkerNodesList,
												   DistributionColumnMap *distributionColumnOverrides);
static void ExecuteSplitShardReleaseSharedMemory(WorkerNode *sourceWorkerNode);
static void AddDummyShardEntryInMap(HTAB *mapOfDummyShards, uint32 targetNodeId,
									ShardInterval *shardInterval);
static void DropDummyShards(HTAB *mapOfDummyShardToPlacement);
static void DropDummyShard(MultiConnection *connection, ShardInterval *shardInterval);
static uint64 GetNextShardIdForSplitChild(void);
static void AcquireNonblockingSplitLock(Oid relationId, SplitMode splitMode);
static List * GetWorkerNodesFromWorkerIds(List *nodeIdsForPlacementList);

/* Customize error message strings based on operation type */
static const char *const SplitOperationName[] =
{
	[SHARD_SPLIT_API] = "split",
	[ISOLATE_TENANT_TO_NEW_SHARD] = "isolate",
	[CREATE_DISTRIBUTED_TABLE] = "create"
};
static const char *const SplitTargetName[] =
{
	[SHARD_SPLIT_API] = "shard",
	[ISOLATE_TENANT_TO_NEW_SHARD] = "tenant",
	[CREATE_DISTRIBUTED_TABLE] = "distributed table"
};

/* Function definitions */

/*
 * ErrorIfCannotSplitShard checks relation kind and invalid shards. It errors
 * out if we are not able to split the given shard.
 */
static void
ErrorIfCannotSplitShard(SplitOperation splitOperation, ShardInterval *sourceShard)
{
	Oid relationId = sourceShard->relationId;
	ListCell *colocatedTableCell = NULL;
	ListCell *colocatedShardCell = NULL;

	/* checks for table ownership and foreign tables */
	List *colocatedTableList = ColocatedTableList(relationId);
	foreach(colocatedTableCell, colocatedTableList)
	{
		Oid colocatedTableId = lfirst_oid(colocatedTableCell);

		/* check that user has owner rights in all co-located tables */
		EnsureTableOwner(colocatedTableId);

		char relationKind = get_rel_relkind(colocatedTableId);
		if (relationKind == RELKIND_FOREIGN_TABLE)
		{
			char *relationName = get_rel_name(colocatedTableId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot %s %s because \"%s\" is a "
								   "foreign table",
								   SplitOperationName[splitOperation],
								   SplitTargetName[splitOperation],
								   relationName),
							errdetail("Splitting shards backed by foreign tables "
									  "is not supported.")));
		}
	}

	/* check shards with inactive placements */
	List *colocatedShardList = ColocatedShardIntervalList(sourceShard);
	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(colocatedShardCell);
		uint64 shardId = shardInterval->shardId;
		ListCell *shardPlacementCell = NULL;

		List *shardPlacementList = ShardPlacementListWithoutOrphanedPlacements(shardId);
		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
			if (placement->shardState != SHARD_STATE_ACTIVE)
			{
				char *relationName = get_rel_name(shardInterval->relationId);
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot %s %s because relation "
									   "\"%s\" has an inactive shard placement "
									   "for the shard %lu",
									   SplitOperationName[splitOperation],
									   SplitTargetName[splitOperation],
									   relationName, shardId),
								errhint("Use master_copy_shard_placement UDF to "
										"repair the inactive shard placement.")));
			}
		}
	}
}


/*
 * Exteded checks before we decide to split the shard.
 * When all consumers (Example : ISOLATE_TENANT_TO_NEW_SHARD) directly call 'SplitShard' API,
 * this method will be merged with 'ErrorIfCannotSplitShard' above.
 */
static void
ErrorIfCannotSplitShardExtended(SplitOperation splitOperation,
								ShardInterval *shardIntervalToSplit,
								List *shardSplitPointsList,
								List *nodeIdsForPlacementList)
{
	/* we should not perform checks for create distributed table operation */
	if (splitOperation == CREATE_DISTRIBUTED_TABLE)
	{
		return;
	}

	CitusTableCacheEntry *cachedTableEntry = GetCitusTableCacheEntry(
		shardIntervalToSplit->relationId);

	/* Perform checks common to both blocking and non-blocking Split API here. */
	if (!IsCitusTableTypeCacheEntry(cachedTableEntry, HASH_DISTRIBUTED))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Cannot %s %s as operation "
							   "is only supported for hash distributed tables.",
							   SplitOperationName[splitOperation],
							   SplitTargetName[splitOperation])));
	}

	uint32 relationReplicationFactor = TableShardReplicationFactor(
		shardIntervalToSplit->relationId);
	if (relationReplicationFactor > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"Operation %s not supported for %s as replication factor '%u' "
							"is greater than 1.",
							SplitOperationName[splitOperation],
							SplitTargetName[splitOperation],
							relationReplicationFactor)));
	}

	int splitPointsCount = list_length(shardSplitPointsList);
	int nodeIdsCount = list_length(nodeIdsForPlacementList);
	int shardsCount = splitPointsCount + 1;
	if (nodeIdsCount != shardsCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg(
					 "Number of worker node ids should be one greater split points. "
					 "NodeId count is '%d' and SplitPoint count is '%d'.",
					 nodeIdsCount,
					 splitPointsCount)));
	}

	if (shardsCount > MAX_SHARD_COUNT)
	{
		ereport(ERROR, (errmsg(
							"Resulting shard count '%d' with split is greater than max shard count '%d' limit.",
							shardsCount, MAX_SHARD_COUNT)));
	}

	Assert(shardIntervalToSplit->minValueExists);
	Assert(shardIntervalToSplit->maxValueExists);

	/* We already verified table is Hash Distributed. We know (minValue, maxValue) are integers. */
	int32 minValue = DatumGetInt32(shardIntervalToSplit->minValue);
	int32 maxValue = DatumGetInt32(shardIntervalToSplit->maxValue);

	/* Fail if Shard Interval cannot be split anymore i.e (min, max) range overlap. */
	if (minValue == maxValue)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg(
					 "Cannot split shard id \"%lu\" as min/max range are equal: ('%d', '%d').",
					 shardIntervalToSplit->shardId,
					 minValue,
					 maxValue)));
	}

	NullableDatum lastShardSplitPoint = { 0, true /*isnull*/ };
	Datum shardSplitPoint;
	foreach_int(shardSplitPoint, shardSplitPointsList)
	{
		int32 shardSplitPointValue = DatumGetInt32(shardSplitPoint);

		/*
		 * 1) All Split points should lie within the shard interval range.
		 * 2) Given our split points inclusive, you cannot specify the max value in a range as a split point.
		 * Example: Shard 81060002 range is from (0,1073741823). '1073741823' as split point is invalid.
		 * '1073741822' is correct and will split shard to: (0, 1073741822) and (1073741823, 1073741823).
		 */
		if (shardSplitPointValue < minValue || shardSplitPointValue > maxValue)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg(
						 "Split point %d is outside the min/max range(%d, %d) for shard id %lu.",
						 shardSplitPointValue,
						 DatumGetInt32(shardIntervalToSplit->minValue),
						 DatumGetInt32(shardIntervalToSplit->maxValue),
						 shardIntervalToSplit->shardId)));
		}
		else if (maxValue == shardSplitPointValue)
		{
			int32 validSplitPoint = shardIntervalToSplit->maxValue - 1;
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg(
						 "Invalid split point %d, as split points should be inclusive. Please use %d instead.",
						 maxValue,
						 validSplitPoint)));
		}

		/* Split points should be in strictly increasing order */
		int32 lastShardSplitPointValue = DatumGetInt32(lastShardSplitPoint.value);
		if (!lastShardSplitPoint.isnull && shardSplitPointValue <=
			lastShardSplitPointValue)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg(
						 "Invalid Split Points '%d' followed by '%d'. "
						 "All split points should be strictly increasing.",
						 lastShardSplitPointValue,
						 shardSplitPointValue)));
		}

		lastShardSplitPoint = (NullableDatum) {
			shardSplitPoint, false
		};
	}
}


/*
 * ErrorIfModificationAndSplitInTheSameTransaction will error if we detect split operation
 * in the same transaction which has modification before.
 */
static void
ErrorIfModificationAndSplitInTheSameTransaction(SplitOperation splitOperation)
{
	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot %s %s after other modifications "
							   "in the same transaction.",
							   SplitOperationName[splitOperation],
							   SplitTargetName[splitOperation])));
	}
}


/*
 * ErrorIfMultipleNonblockingSplitInTheSameTransaction will error if we detect multiple
 * nonblocking split operation in the same transaction.
 */
static void
ErrorIfMultipleNonblockingSplitInTheSameTransaction(SplitOperation splitOperation)
{
	if (PlacementMovedUsingLogicalReplicationInTX)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Multiple '%s %s' operations via logical "
							   "replication in the same transaction is currently "
							   "not supported.",
							   SplitOperationName[splitOperation],
							   SplitTargetName[splitOperation]),
						errhint("If you wish to execute multiple '%s %s' operations "
								"in a single transaction set the shard_transfer_mode "
								"to 'block_writes'.",
								SplitOperationName[splitOperation],
								SplitTargetName[splitOperation])));
	}
}


/*
 * GetWorkerNodesFromWorkerIds returns list of worker nodes given a list
 * of worker ids. It will error if any node id is invalid.
 */
static List *
GetWorkerNodesFromWorkerIds(List *nodeIdsForPlacementList)
{
	List *workersForPlacementList = NIL;
	Datum nodeId;
	foreach_int(nodeId, nodeIdsForPlacementList)
	{
		uint32 nodeIdValue = DatumGetUInt32(nodeId);
		WorkerNode *workerNode = LookupNodeByNodeId(nodeIdValue);

		/* NodeId in Citus are unsigned and range from [1, 4294967296]. */
		if (nodeIdValue < 1 || workerNode == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
							errmsg("Invalid Node Id '%u'.", nodeIdValue)));
		}

		workersForPlacementList =
			lappend(workersForPlacementList, (void *) workerNode);
	}

	return workersForPlacementList;
}


/*
 * SplitShard API to split a given shard (or shard group) based on specified split points
 * to a set of destination nodes.
 * 'splitMode'					: Mode of split operation.
 * 'splitOperation'             : Customer operation that triggered split.
 * 'shardInterval'              : Source shard interval to be split.
 * 'shardSplitPointsList'		: Split Points list for the source 'shardInterval'.
 * 'nodeIdsForPlacementList'	: Placement list corresponding to split children.
 * 'distributionColumnList'     : Maps relation IDs to distribution columns.
 *                                If not specified, the distribution column is read
 *                                from the metadata.
 * 'colocatedShardIntervalList' : Shard interval list for colocation group. (only used for
 *                                create_distributed_table_concurrently).
 * 'targetColocationId'         : Specifies the colocation ID (only used for
 *                                create_distributed_table_concurrently).
 */
void
SplitShard(SplitMode splitMode,
		   SplitOperation splitOperation,
		   uint64 shardIdToSplit,
		   List *shardSplitPointsList,
		   List *nodeIdsForPlacementList,
		   DistributionColumnMap *distributionColumnOverrides,
		   List *colocatedShardIntervalList,
		   uint32 targetColocationId)
{
	ErrorIfModificationAndSplitInTheSameTransaction(splitOperation);
	ErrorIfMultipleNonblockingSplitInTheSameTransaction(splitOperation);

	ShardInterval *shardIntervalToSplit = LoadShardInterval(shardIdToSplit);
	List *colocatedTableList = ColocatedTableList(shardIntervalToSplit->relationId);

	if (splitMode == AUTO_SPLIT)
	{
		VerifyTablesHaveReplicaIdentity(colocatedTableList);
	}

	/* Acquire global lock to prevent concurrent split on the same colocation group or relation */
	Oid relationId = RelationIdForShard(shardIdToSplit);
	AcquirePlacementColocationLock(relationId, ExclusiveLock, "split");

	/* Acquire global lock to prevent concurrent nonblocking splits */
	AcquireNonblockingSplitLock(relationId, splitMode);

	/* sort the tables to avoid deadlocks */
	colocatedTableList = SortList(colocatedTableList, CompareOids);
	Oid colocatedTableId = InvalidOid;
	foreach_oid(colocatedTableId, colocatedTableList)
	{
		/*
		 * Block concurrent DDL / TRUNCATE commands on the relation. Similarly,
		 * block concurrent citus_move_shard_placement() / isolate_tenant_to_new_shard()
		 * on any shard of the same relation.
		 */
		LockRelationOid(colocatedTableId, ShareUpdateExclusiveLock);
	}

	ErrorIfCannotSplitShard(splitOperation, shardIntervalToSplit);
	ErrorIfCannotSplitShardExtended(
		splitOperation,
		shardIntervalToSplit,
		shardSplitPointsList,
		nodeIdsForPlacementList);

	List *workersForPlacementList = GetWorkerNodesFromWorkerIds(nodeIdsForPlacementList);

	List *sourceColocatedShardIntervalList = NIL;
	if (splitOperation != CREATE_DISTRIBUTED_TABLE)
	{
		sourceColocatedShardIntervalList = ColocatedShardIntervalList(
			shardIntervalToSplit);
	}
	else
	{
		sourceColocatedShardIntervalList = colocatedShardIntervalList;
	}

	/* use the user-specified shard ID as the split workflow ID */
	uint64 splitWorkflowId = shardIntervalToSplit->shardId;

	if (splitMode == BLOCKING_SPLIT)
	{
		EnsureReferenceTablesExistOnAllNodesExtended(TRANSFER_MODE_BLOCK_WRITES);
		BlockingShardSplit(
			splitOperation,
			splitWorkflowId,
			sourceColocatedShardIntervalList,
			shardSplitPointsList,
			workersForPlacementList,
			distributionColumnOverrides);
	}
	else
	{
		NonBlockingShardSplit(
			splitOperation,
			splitWorkflowId,
			sourceColocatedShardIntervalList,
			shardSplitPointsList,
			workersForPlacementList,
			distributionColumnOverrides,
			targetColocationId);

		PlacementMovedUsingLogicalReplicationInTX = true;
	}
}


/*
 * ShardIntervalHashCode computes the hash code for a Shardinterval using
 * shardId.
 */
static uint32
ShardIntervalHashCode(const void *key, Size keySize)
{
	const ShardInterval *shardInterval = (const ShardInterval *) key;
	const uint64 *shardId = &(shardInterval->shardId);

	/* standard hash function outlined in Effective Java, Item 8 */
	uint32 result = 17;
	result = 37 * result + tag_hash(shardId, sizeof(uint64));

	return result;
}


/*
 * ShardIntervalHashCompare compares two shard intervals using shard id.
 */
static int
ShardIntervalHashCompare(const void *lhsKey, const void *rhsKey, Size keySize)
{
	const ShardInterval *intervalLhs = (const ShardInterval *) lhsKey;
	const ShardInterval *intervalRhs = (const ShardInterval *) rhsKey;

	int shardIdCompare = 0;

	/* first, compare by shard id */
	if (intervalLhs->shardId < intervalRhs->shardId)
	{
		shardIdCompare = -1;
	}
	else if (intervalLhs->shardId > intervalRhs->shardId)
	{
		shardIdCompare = 1;
	}

	return shardIdCompare;
}


/* Create an empty map that tracks ShardInterval -> Placement Node as created by workflow */
static HTAB *
CreateEmptyMapForShardsCreatedByWorkflow()
{
	HASHCTL info = { 0 };
	info.keysize = sizeof(ShardInterval);
	info.entrysize = sizeof(ShardCreatedByWorkflowEntry);
	info.hash = ShardIntervalHashCode;
	info.match = ShardIntervalHashCompare;
	info.hcxt = CurrentMemoryContext;

	/* we don't have value field as it's a set */
	info.entrysize = info.keysize;
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB *splitChildrenCreatedByWorkflow = hash_create("Shard id to Node Placement Map",
													   32, &info, hashFlags);
	return splitChildrenCreatedByWorkflow;
}


/*
 * SplitShard API to split a given shard (or shard group) in blocking fashion
 * based on specified split points to a set of destination nodes.
 * splitOperation                   : Customer operation that triggered split.
 * splitWorkflowId                  : Number used to identify split workflow in names.
 * sourceColocatedShardIntervalList : Source shard group to be split.
 * shardSplitPointsList             : Split Points list for the source 'shardInterval'.
 * workersForPlacementList          : Placement list corresponding to split children.
 */
static void
BlockingShardSplit(SplitOperation splitOperation,
				   uint64 splitWorkflowId,
				   List *sourceColocatedShardIntervalList,
				   List *shardSplitPointsList,
				   List *workersForPlacementList,
				   DistributionColumnMap *distributionColumnOverrides)
{
	BlockWritesToShardList(sourceColocatedShardIntervalList);

	/* First create shard interval metadata for split children */
	List *shardGroupSplitIntervalListList = CreateSplitIntervalsForShardGroup(
		sourceColocatedShardIntervalList,
		shardSplitPointsList);

	/* Only single placement allowed (already validated RelationReplicationFactor = 1) */
	ShardInterval *firstShard = linitial(sourceColocatedShardIntervalList);
	WorkerNode *sourceShardNode =
		ActiveShardPlacementWorkerNode(firstShard->shardId);

	HTAB *mapOfShardToPlacementCreatedByWorkflow =
		CreateEmptyMapForShardsCreatedByWorkflow();
	PG_TRY();
	{
		/* Physically create split children. */
		CreateSplitShardsForShardGroup(mapOfShardToPlacementCreatedByWorkflow,
									   shardGroupSplitIntervalListList,
									   workersForPlacementList);

		/* For Blocking split, copy isn't snapshotted */
		char *snapshotName = NULL;
		DoSplitCopy(sourceShardNode, sourceColocatedShardIntervalList,
					shardGroupSplitIntervalListList, workersForPlacementList,
					snapshotName, distributionColumnOverrides);

		/* Create auxiliary structures (indexes, stats, replicaindentities, triggers) */
		CreateAuxiliaryStructuresForShardGroup(shardGroupSplitIntervalListList,
											   workersForPlacementList,
											   true /* includeReplicaIdentity*/);

		/*
		 * Up to this point, we performed various subtransactions that may
		 * require additional clean-up in case of failure. The remaining operations
		 * going forward are part of the same distributed transaction.
		 */

		/*
		 * Drop old shards and delete related metadata. Have to do that before
		 * creating the new shard metadata, because there's cross-checks
		 * preventing inconsistent metadata (like overlapping shards).
		 */
		DropShardList(sourceColocatedShardIntervalList);

		/* Insert new shard and placement metdata */
		InsertSplitChildrenShardMetadata(shardGroupSplitIntervalListList,
										 workersForPlacementList);

		/* create partitioning hierarchy, if any */
		CreatePartitioningHierarchy(shardGroupSplitIntervalListList,
									workersForPlacementList);

		/*
		 * Create foreign keys if exists after the metadata changes happening in
		 * DropShardList() and InsertSplitChildrenShardMetadata() because the foreign
		 * key creation depends on the new metadata.
		 */
		CreateForeignKeyConstraints(shardGroupSplitIntervalListList,
									workersForPlacementList);
	}
	PG_CATCH();
	{
		/* end ongoing transactions to enable us to clean up */
		ShutdownAllConnections();

		/* Do a best effort cleanup of shards created on workers in the above block */
		TryDropSplitShardsOnFailure(mapOfShardToPlacementCreatedByWorkflow);

		PG_RE_THROW();
	}
	PG_END_TRY();


	CitusInvalidateRelcacheByRelid(DistShardRelationId());
}


/* Create ShardGroup split children on a list of corresponding workers. */
static void
CreateSplitShardsForShardGroup(HTAB *mapOfShardToPlacementCreatedByWorkflow,
							   List *shardGroupSplitIntervalListList,
							   List *workersForPlacementList)
{
	/*
	 * Iterate over all the shards in the shard group.
	 */
	List *shardIntervalList = NIL;
	foreach_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *shardInterval = NULL;
		WorkerNode *workerPlacementNode = NULL;

		/*
		 * Iterate on split shards DDL command list for a given shard
		 * and create them on corresponding workerPlacementNode.
		 */
		forboth_ptr(shardInterval, shardIntervalList, workerPlacementNode,
					workersForPlacementList)
		{
			/* Populate list of commands necessary to create shard interval on destination */
			List *splitShardCreationCommandList = GetPreLoadTableCreationCommands(
				shardInterval->relationId,
				false, /* includeSequenceDefaults */
				NULL /* auto add columnar options for cstore tables */);
			splitShardCreationCommandList = WorkerApplyShardDDLCommandList(
				splitShardCreationCommandList,
				shardInterval->shardId);

			/* Create new split child shard on the specified placement list */
			CreateObjectOnPlacement(splitShardCreationCommandList, workerPlacementNode);

			ShardCreatedByWorkflowEntry entry;
			entry.shardIntervalKey = shardInterval;
			entry.workerNodeValue = workerPlacementNode;
			bool found = false;
			hash_search(mapOfShardToPlacementCreatedByWorkflow, &entry, HASH_ENTER,
						&found);
			Assert(!found);
		}
	}
}


/* Create a DDL task with corresponding task list on given worker node */
static Task *
CreateTaskForDDLCommandList(List *ddlCommandList, WorkerNode *workerNode)
{
	Task *ddlTask = CitusMakeNode(Task);
	ddlTask->taskType = DDL_TASK;
	ddlTask->replicationModel = REPLICATION_MODEL_INVALID;
	SetTaskQueryStringList(ddlTask, ddlCommandList);

	ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
	SetPlacementNodeMetadata(taskPlacement, workerNode);
	ddlTask->taskPlacementList = list_make1(taskPlacement);

	return ddlTask;
}


/* Create ShardGroup auxiliary structures (indexes, stats, replicaindentities, triggers)
 * on a list of corresponding workers.
 */
static void
CreateAuxiliaryStructuresForShardGroup(List *shardGroupSplitIntervalListList,
									   List *workersForPlacementList, bool
									   includeReplicaIdentity)
{
	List *shardIntervalList = NIL;
	List *ddlTaskExecList = NIL;

	/*
	 * Iterate over all the shards in the shard group.
	 */
	foreach_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *shardInterval = NULL;
		WorkerNode *workerPlacementNode = NULL;

		/*
		 * Iterate on split shard interval list for given shard and create tasks
		 * for every single split shard in a shard group.
		 */
		forboth_ptr(shardInterval, shardIntervalList, workerPlacementNode,
					workersForPlacementList)
		{
			List *ddlCommandList = GetPostLoadTableCreationCommands(
				shardInterval->relationId,
				true /* includeIndexes */,
				includeReplicaIdentity);
			ddlCommandList = WorkerApplyShardDDLCommandList(
				ddlCommandList,
				shardInterval->shardId);

			/*
			 * A task is expected to be instantiated with a non-null 'ddlCommandList'.
			 * The list can be empty, if no auxiliary structures are present.
			 */
			if (ddlCommandList != NULL)
			{
				Task *ddlTask = CreateTaskForDDLCommandList(ddlCommandList,
															workerPlacementNode);

				ddlTaskExecList = lappend(ddlTaskExecList, ddlTask);
			}
		}
	}

	ExecuteTaskListOutsideTransaction(
		ROW_MODIFY_NONE,
		ddlTaskExecList,
		MaxAdaptiveExecutorPoolSize,
		NULL /* jobIdList (ignored by API implementation) */);
}


/*
 * Perform Split Copy from source shard(s) to split children.
 * 'sourceShardNode'					: Source shard worker node.
 * 'sourceColocatedShardIntervalList'	: List of source shard intervals from shard group.
 * 'shardGroupSplitIntervalListList'	: List of shard intervals for split children.
 * 'workersForPlacementList'			: List of workers for split children placement.
 */
static void
DoSplitCopy(WorkerNode *sourceShardNode, List *sourceColocatedShardIntervalList,
			List *shardGroupSplitIntervalListList, List *destinationWorkerNodesList,
			char *snapShotName, DistributionColumnMap *distributionColumnOverrides)
{
	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitShardIntervalList = NIL;

	int taskId = 0;
	List *splitCopyTaskList = NIL;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitShardIntervalList, shardGroupSplitIntervalListList)
	{
		/*
		 * Skip copying data for partitioned tables, because they contain no
		 * data themselves. Their partitions do contain data, but those are
		 * different colocated shards that will be copied seperately.
		 */
		if (PartitionedTable(sourceShardIntervalToCopy->relationId))
		{
			continue;
		}

		Oid relationId = sourceShardIntervalToCopy->relationId;

		Var *distributionColumn =
			GetDistributionColumnWithOverrides(relationId,
											   distributionColumnOverrides);
		Assert(distributionColumn != NULL);

		bool missingOK = false;
		char *distributionColumnName = get_attname(relationId,
												   distributionColumn->varattno,
												   missingOK);

		StringInfo splitCopyUdfCommand = CreateSplitCopyCommand(
			sourceShardIntervalToCopy,
			distributionColumnName,
			splitShardIntervalList,
			destinationWorkerNodesList);

		/* Create copy task. Snapshot name is required for nonblocking splits */
		Task *splitCopyTask = CreateSplitCopyTask(splitCopyUdfCommand, snapShotName,
												  taskId,
												  sourceShardIntervalToCopy->shardId);

		ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
		SetPlacementNodeMetadata(taskPlacement, sourceShardNode);
		splitCopyTask->taskPlacementList = list_make1(taskPlacement);

		splitCopyTaskList = lappend(splitCopyTaskList, splitCopyTask);
		taskId++;
	}

	ExecuteTaskListOutsideTransaction(ROW_MODIFY_NONE, splitCopyTaskList,
									  MaxAdaptiveExecutorPoolSize,
									  NULL /* jobIdList (ignored by API implementation) */);
}


/*
 * Create Copy command for a given shard source shard to be copied to corresponding split children.
 * 'sourceShardSplitInterval' : Source shard interval to be copied.
 * 'splitChildrenShardINnerIntervalList' : List of shard intervals for split children.
 * 'destinationWorkerNodesList' : List of workers for split children placement.
 * Here is an example of a 2 way split copy :
 * SELECT * from worker_split_copy(
 *  81060000, -- source shard id to split copy
 *  ARRAY[
 *       -- split copy info for split children 1
 *      ROW(81060015, -- destination shard id
 *           -2147483648, -- split range begin
 *          1073741823, --split range end
 *          10 -- worker node id)::pg_catalog.split_copy_info,
 *      -- split copy info for split children 2
 *      ROW(81060016,  --destination shard id
 *          1073741824, --split range begin
 *          2147483647, --split range end
 *          11 -- workef node id)::pg_catalog.split_copy_info
 *      ]
 *  );
 */
static StringInfo
CreateSplitCopyCommand(ShardInterval *sourceShardSplitInterval,
					   char *distributionColumnName,
					   List *splitChildrenShardIntervalList,
					   List *destinationWorkerNodesList)
{
	StringInfo splitCopyInfoArray = makeStringInfo();
	appendStringInfo(splitCopyInfoArray, "ARRAY[");

	ShardInterval *splitChildShardInterval = NULL;
	bool addComma = false;
	WorkerNode *destinationWorkerNode = NULL;
	forboth_ptr(splitChildShardInterval, splitChildrenShardIntervalList,
				destinationWorkerNode, destinationWorkerNodesList)
	{
		if (addComma)
		{
			appendStringInfo(splitCopyInfoArray, ",");
		}

		StringInfo splitCopyInfoRow = makeStringInfo();
		appendStringInfo(splitCopyInfoRow,
						 "ROW(%lu, %d, %d, %u)::pg_catalog.split_copy_info",
						 splitChildShardInterval->shardId,
						 DatumGetInt32(splitChildShardInterval->minValue),
						 DatumGetInt32(splitChildShardInterval->maxValue),
						 destinationWorkerNode->nodeId);
		appendStringInfo(splitCopyInfoArray, "%s", splitCopyInfoRow->data);

		addComma = true;
	}
	appendStringInfo(splitCopyInfoArray, "]");

	StringInfo splitCopyUdf = makeStringInfo();
	appendStringInfo(splitCopyUdf, "SELECT pg_catalog.worker_split_copy(%lu, %s, %s);",
					 sourceShardSplitInterval->shardId,
					 quote_literal_cstr(distributionColumnName),
					 splitCopyInfoArray->data);

	return splitCopyUdf;
}


/*
 * CreateSplitCopyTask creates a task for copying data.
 * In the case of Non-blocking split, snapshotted copy task is created with given 'snapshotName'.
 * 'snapshotName' is NULL for Blocking split.
 */
static Task *
CreateSplitCopyTask(StringInfo splitCopyUdfCommand, char *snapshotName, int taskId, uint64
					jobId)
{
	List *ddlCommandList = NIL;
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

	ddlCommandList = lappend(ddlCommandList, splitCopyUdfCommand->data);

	StringInfo commitCommand = makeStringInfo();
	appendStringInfo(commitCommand, "COMMIT;");
	ddlCommandList = lappend(ddlCommandList, commitCommand->data);

	Task *splitCopyTask = CitusMakeNode(Task);
	splitCopyTask->jobId = jobId;
	splitCopyTask->taskId = taskId;
	splitCopyTask->taskType = READ_TASK;
	splitCopyTask->replicationModel = REPLICATION_MODEL_INVALID;
	SetTaskQueryStringList(splitCopyTask, ddlCommandList);

	return splitCopyTask;
}


/*
 * Create an object on a worker node.
 */
static void
CreateObjectOnPlacement(List *objectCreationCommandList,
						WorkerNode *workerPlacementNode)
{
	char *currentUser = CurrentUserName();
	SendCommandListToWorkerOutsideTransaction(workerPlacementNode->workerName,
											  workerPlacementNode->workerPort,
											  currentUser,
											  objectCreationCommandList);
}


/*
 * Create split children intervals for a shardgroup given list of split points.
 * Example:
 * 'sourceColocatedShardIntervalList': Colocated shard S1[-2147483648, 2147483647] & S2[-2147483648, 2147483647]
 * 'splitPointsForShard': [0] (2 way split)
 * 'shardGroupSplitIntervalListList':
 *  [
 *      [ S1_1(-2147483648, 0), S1_2(1, 2147483647) ], // Split Interval List for S1.
 *      [ S2_1(-2147483648, 0), S2_2(1, 2147483647) ]  // Split Interval List for S2.
 *  ]
 */
static List *
CreateSplitIntervalsForShardGroup(List *sourceColocatedShardIntervalList,
								  List *splitPointsForShard)
{
	List *shardGroupSplitIntervalListList = NIL;

	ShardInterval *shardToSplitInterval = NULL;
	foreach_ptr(shardToSplitInterval, sourceColocatedShardIntervalList)
	{
		List *shardSplitIntervalList = NIL;
		CreateSplitIntervalsForShard(shardToSplitInterval, splitPointsForShard,
									 &shardSplitIntervalList);

		shardGroupSplitIntervalListList = lappend(shardGroupSplitIntervalListList,
												  shardSplitIntervalList);
	}

	return shardGroupSplitIntervalListList;
}


/*
 * Create split children intervals given a sourceshard and a list of split points.
 * Example: SourceShard is range [0, 100] and SplitPoints are (15, 30) will give us:
 *  [(0, 15) (16, 30) (31, 100)]
 */
static void
CreateSplitIntervalsForShard(ShardInterval *sourceShard,
							 List *splitPointsForShard,
							 List **shardSplitChildrenIntervalList)
{
	/* For 'N' split points, we will have N+1 shard intervals created. */
	int shardIntervalCount = list_length(splitPointsForShard) + 1;
	ListCell *splitPointCell = list_head(splitPointsForShard);
	int32 splitParentMaxValue = DatumGetInt32(sourceShard->maxValue);
	int32 currentSplitChildMinValue = DatumGetInt32(sourceShard->minValue);

	/* if we are splitting a Citus local table, assume whole shard range */
	if (!sourceShard->maxValueExists)
	{
		splitParentMaxValue = PG_INT32_MAX;
	}

	if (!sourceShard->minValueExists)
	{
		currentSplitChildMinValue = PG_INT32_MIN;
	}

	for (int index = 0; index < shardIntervalCount; index++)
	{
		ShardInterval *splitChildShardInterval = CopyShardInterval(sourceShard);
		splitChildShardInterval->shardIndex = -1;
		splitChildShardInterval->shardId = GetNextShardIdForSplitChild();

		splitChildShardInterval->minValueExists = true;
		splitChildShardInterval->minValue = currentSplitChildMinValue;
		splitChildShardInterval->maxValueExists = true;

		/* Length of splitPointsForShard is one less than 'shardIntervalCount' and we need to account */
		/* for 'splitPointCell' being NULL for last iteration. */
		if (splitPointCell)
		{
			splitChildShardInterval->maxValue = DatumGetInt32((Datum) lfirst(
																  splitPointCell));
			splitPointCell = lnext(splitPointsForShard, splitPointCell);
		}
		else
		{
			splitChildShardInterval->maxValue = splitParentMaxValue;
		}

		currentSplitChildMinValue = splitChildShardInterval->maxValue + 1;
		*shardSplitChildrenIntervalList = lappend(*shardSplitChildrenIntervalList,
												  splitChildShardInterval);
	}
}


/*
 * UpdateDistributionColumnsForShardGroup globally updates the pg_dist_partition metadata
 * for each relation that has a shard in colocatedShardList.
 *
 * This is used primarily for Citus local -> distributed table conversion
 * in create_distributed_table_concurrently.
 *
 * It would be nicer to keep this separate from shard split, but we need to do the
 * update at exactly the right point in the shard split process, namely after
 * replication slot creation and before inserting shard metadata, which itself
 * needs to happen before foreign key creation (mainly because the foreign key
 * functions depend on metadata).
 */
static void
UpdateDistributionColumnsForShardGroup(List *colocatedShardList,
									   DistributionColumnMap *distributionColumnMap,
									   char distributionMethod,
									   int shardCount,
									   uint32 colocationId)
{
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, colocatedShardList)
	{
		Oid relationId = shardInterval->relationId;
		Var *distributionColumn = GetDistributionColumnFromMap(distributionColumnMap,
															   relationId);

		/* we should have an entry for every relation ID in the colocation group */
		Assert(distributionColumn != NULL);

		if (colocationId == INVALID_COLOCATION_ID)
		{
			/*
			 * Getting here with an invalid co-location ID means that no
			 * appropriate co-location group exists yet.
			 */
			colocationId = CreateColocationGroup(shardCount,
												 ShardReplicationFactor,
												 distributionColumn->vartype,
												 distributionColumn->varcollid);
		}

		UpdateDistributionColumnGlobally(relationId, distributionMethod,
										 distributionColumn, colocationId);
	}
}


/*
 * Insert new shard and placement metadata.
 * Sync the Metadata with all nodes if enabled.
 */
static void
InsertSplitChildrenShardMetadata(List *shardGroupSplitIntervalListList,
								 List *workersForPlacementList)
{
	List *shardIntervalList = NIL;
	List *syncedShardList = NIL;

	/*
	 * Iterate over all the shards in the shard group.
	 */
	foreach_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		/*
		 * Iterate on split shards list for a given shard and insert metadata.
		 */
		ShardInterval *shardInterval = NULL;
		WorkerNode *workerPlacementNode = NULL;
		forboth_ptr(shardInterval, shardIntervalList, workerPlacementNode,
					workersForPlacementList)
		{
			InsertShardRow(
				shardInterval->relationId,
				shardInterval->shardId,
				shardInterval->storageType,
				IntegerToText(DatumGetInt32(shardInterval->minValue)),
				IntegerToText(DatumGetInt32(shardInterval->maxValue)));

			InsertShardPlacementRow(
				shardInterval->shardId,
				INVALID_PLACEMENT_ID, /* triggers generation of new id */
				SHARD_STATE_ACTIVE,
				0, /* shard length (zero for HashDistributed Table) */
				workerPlacementNode->groupId);

			if (ShouldSyncTableMetadata(shardInterval->relationId))
			{
				syncedShardList = lappend(syncedShardList, shardInterval);
			}
		}
	}

	/* send commands to synced nodes one by one */
	List *splitOffShardMetadataCommandList = ShardListInsertCommand(syncedShardList);
	char *command = NULL;
	foreach_ptr(command, splitOffShardMetadataCommandList)
	{
		SendCommandToWorkersWithMetadata(command);
	}
}


/*
 * CreatePartitioningHierarchy creates the partitioning
 * hierarchy between the shardList, if any.
 */
static void
CreatePartitioningHierarchy(List *shardGroupSplitIntervalListList,
							List *workersForPlacementList)
{
	/* Create partition heirarchy between shards */
	List *shardIntervalList = NIL;

	/*
	 * Iterate over all the shards in the shard group.
	 */
	foreach_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *shardInterval = NULL;
		WorkerNode *workerPlacementNode = NULL;

		/*
		 * Iterate on split shards list for a given shard and create constraints.
		 */
		forboth_ptr(shardInterval, shardIntervalList, workerPlacementNode,
					workersForPlacementList)
		{
			if (PartitionTable(shardInterval->relationId))
			{
				char *attachPartitionCommand =
					GenerateAttachShardPartitionCommand(shardInterval);

				SendCommandToWorker(
					workerPlacementNode->workerName,
					workerPlacementNode->workerPort,
					attachPartitionCommand);
			}
		}
	}
}


/*
 * Create foreign key constraints on the split children shards.
 */
static void
CreateForeignKeyConstraints(List *shardGroupSplitIntervalListList,
							List *workersForPlacementList)
{
	/* Create constraints between shards */
	List *shardIntervalList = NIL;

	/*
	 * Iterate over all the shards in the shard group.
	 */
	foreach_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *shardInterval = NULL;
		WorkerNode *workerPlacementNode = NULL;

		/*
		 * Iterate on split shards list for a given shard and create constraints.
		 */
		forboth_ptr(shardInterval, shardIntervalList,
					workerPlacementNode, workersForPlacementList)
		{
			List *shardForeignConstraintCommandList = NIL;
			List *referenceTableForeignConstraintList = NIL;

			CopyShardForeignConstraintCommandListGrouped(
				shardInterval,
				&shardForeignConstraintCommandList,
				&referenceTableForeignConstraintList);

			List *constraintCommandList = NIL;
			constraintCommandList = list_concat(constraintCommandList,
												shardForeignConstraintCommandList);
			constraintCommandList = list_concat(constraintCommandList,
												referenceTableForeignConstraintList);

			char *constraintCommand = NULL;
			foreach_ptr(constraintCommand, constraintCommandList)
			{
				SendCommandToWorker(
					workerPlacementNode->workerName,
					workerPlacementNode->workerPort,
					constraintCommand);
			}
		}
	}
}


/*
 * DropShardList drops shards and their metadata from both the coordinator and
 * mx nodes.
 */
void
DropShardList(List *shardIntervalList)
{
	ListCell *shardIntervalCell = NULL;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		ListCell *shardPlacementCell = NULL;
		Oid relationId = shardInterval->relationId;
		uint64 oldShardId = shardInterval->shardId;

		/* delete metadata from synced nodes */
		if (ShouldSyncTableMetadata(relationId))
		{
			ListCell *commandCell = NULL;

			/* send the commands one by one (calls citus_internal_delete_shard_metadata internally) */
			List *shardMetadataDeleteCommandList = ShardDeleteCommandList(shardInterval);
			foreach(commandCell, shardMetadataDeleteCommandList)
			{
				char *command = (char *) lfirst(commandCell);
				SendCommandToWorkersWithMetadata(command);
			}
		}

		/* delete shard placements and drop shards */
		List *shardPlacementList = ActiveShardPlacementList(oldShardId);
		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
			char *workerName = placement->nodeName;
			uint32 workerPort = placement->nodePort;
			StringInfo dropQuery = makeStringInfo();

			DeleteShardPlacementRow(placement->placementId);

			/* get shard name */
			char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);

			char storageType = shardInterval->storageType;
			if (storageType == SHARD_STORAGE_TABLE)
			{
				appendStringInfo(dropQuery, DROP_REGULAR_TABLE_COMMAND,
								 qualifiedShardName);
			}
			else if (storageType == SHARD_STORAGE_FOREIGN)
			{
				appendStringInfo(dropQuery, DROP_FOREIGN_TABLE_COMMAND,
								 qualifiedShardName);
			}

			/* drop old shard */
			SendCommandToWorker(workerName, workerPort, dropQuery->data);
		}

		/* delete shard row */
		DeleteShardRow(oldShardId);
	}
}


/*
 * In case of failure, TryDropSplitShardsOnFailure drops in-progress shard placements from both the
 * coordinator and mx nodes.
 */
static void
TryDropSplitShardsOnFailure(HTAB *mapOfShardToPlacementCreatedByWorkflow)
{
	HASH_SEQ_STATUS status;
	ShardCreatedByWorkflowEntry *entry;

	hash_seq_init(&status, mapOfShardToPlacementCreatedByWorkflow);
	while ((entry = (ShardCreatedByWorkflowEntry *) hash_seq_search(&status)) != 0)
	{
		ShardInterval *shardInterval = entry->shardIntervalKey;
		WorkerNode *workerPlacementNode = entry->workerNodeValue;

		char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);
		StringInfo dropShardQuery = makeStringInfo();

		/* Caller enforces that foreign tables cannot be split (use DROP_REGULAR_TABLE_COMMAND) */
		appendStringInfo(dropShardQuery, DROP_REGULAR_TABLE_COMMAND,
						 qualifiedShardName);

		int connectionFlags = FOR_DDL;
		connectionFlags |= OUTSIDE_TRANSACTION;
		MultiConnection *connnection = GetNodeUserDatabaseConnection(
			connectionFlags,
			workerPlacementNode->workerName,
			workerPlacementNode->workerPort,
			CurrentUserName(),
			NULL /* databaseName */);

		/*
		 * Perform a drop in best effort manner.
		 * The shard may or may not exist and the connection could have died.
		 */
		ExecuteOptionalRemoteCommand(
			connnection,
			dropShardQuery->data,
			NULL /* pgResult */);
	}
}


/*
 * AcquireNonblockingSplitLock does not allow concurrent nonblocking splits, because we share memory and
 * replication slots.
 */
static void
AcquireNonblockingSplitLock(Oid relationId, SplitMode splitMode)
{
	/* we should not be preventing concurrent blocking and nonblocking splits */
	if (splitMode == BLOCKING_SPLIT)
	{
		return;
	}

	LOCKTAG tag;
	SET_LOCKTAG_NONBLOCKING_SPLIT(tag);

	LockAcquireResult lockAcquired = LockAcquire(&tag, ExclusiveLock, false, true);
	if (!lockAcquired)
	{
		ereport(ERROR, (errmsg("could not acquire the lock required to split "
							   "concurrently %s.", generate_qualified_relation_name(
								   relationId)),
						errdetail("It means that either a concurrent shard move "
								  "or distributed table creation is happening."),
						errhint("Make sure that the concurrent operation has "
								"finished and re-run the command")));
	}
}


/*
 * SplitShard API to split a given shard (or shard group) in non-blocking fashion
 * based on specified split points to a set of destination nodes.
 * splitOperation                   : Customer operation that triggered split.
 * splitWorkflowId                  : Number used to identify split workflow in names.
 * sourceColocatedShardIntervalList : Source shard group to be split.
 * shardSplitPointsList             : Split Points list for the source 'shardInterval'.
 * workersForPlacementList          : Placement list corresponding to split children.
 * distributionColumnList           : Maps relation IDs to distribution columns.
 *                                    If not specified, the distribution column is read
 *                                    from the metadata.
 * targetColocationId               : Specifies the colocation ID (only used for
 *                                    create_distributed_table_concurrently).
 */
void
NonBlockingShardSplit(SplitOperation splitOperation,
					  uint64 splitWorkflowId,
					  List *sourceColocatedShardIntervalList,
					  List *shardSplitPointsList,
					  List *workersForPlacementList,
					  DistributionColumnMap *distributionColumnOverrides,
					  uint32 targetColocationId)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);

	/* First create shard interval metadata for split children */
	List *shardGroupSplitIntervalListList = CreateSplitIntervalsForShardGroup(
		sourceColocatedShardIntervalList,
		shardSplitPointsList);

	ShardInterval *firstShard = linitial(sourceColocatedShardIntervalList);
	WorkerNode *sourceShardToCopyNode =
		ActiveShardPlacementWorkerNode(firstShard->shardId);

	/* Create hashmap to group shards for publication-subscription management */
	HTAB *publicationInfoHash = CreateShardSplitInfoMapForPublication(
		sourceColocatedShardIntervalList,
		shardGroupSplitIntervalListList,
		workersForPlacementList);

	DropAllLogicalReplicationLeftovers(SHARD_SPLIT);

	int connectionFlags = FORCE_NEW_CONNECTION;
	MultiConnection *sourceConnection = GetNodeUserDatabaseConnection(
		connectionFlags,
		sourceShardToCopyNode->workerName,
		sourceShardToCopyNode->workerPort,
		superUser,
		databaseName);
	ClaimConnectionExclusively(sourceConnection);

	HTAB *mapOfShardToPlacementCreatedByWorkflow =
		CreateEmptyMapForShardsCreatedByWorkflow();

	HTAB *mapOfDummyShardToPlacement = CreateSimpleHash(NodeAndOwner,
														GroupedShardSplitInfos);
	MultiConnection *sourceReplicationConnection =
		GetReplicationConnection(sourceShardToCopyNode->workerName,
								 sourceShardToCopyNode->workerPort);

	/* Non-Blocking shard split workflow starts here */
	PG_TRY();
	{
		/* 1) Physically create split children. */
		CreateSplitShardsForShardGroup(mapOfShardToPlacementCreatedByWorkflow,
									   shardGroupSplitIntervalListList,
									   workersForPlacementList);

		/*
		 * 2) Create dummy shards due to PG logical replication constraints.
		 *    Refer to the comment section of 'CreateDummyShardsForShardGroup' for indepth
		 *    information.
		 */
		CreateDummyShardsForShardGroup(
			mapOfDummyShardToPlacement,
			sourceColocatedShardIntervalList,
			shardGroupSplitIntervalListList,
			sourceShardToCopyNode,
			workersForPlacementList);

		/*
		 * 3) Create replica identities on dummy shards. This needs to be done
		 * before the subscriptions are created. Otherwise the subscription
		 * creation will get stuck waiting for the publication to send a
		 * replica identity. Since we never actually write data into these
		 * dummy shards there's no point in creating these indexes after the
		 * initial COPY phase, like we do for the replica identities on the
		 * target shards.
		 */
		CreateReplicaIdentitiesForDummyShards(mapOfDummyShardToPlacement);

		/* 4) Create Publications. */
		CreatePublications(sourceConnection, publicationInfoHash);

		/* 5) Execute 'worker_split_shard_replication_setup UDF */
		List *replicationSlotInfoList = ExecuteSplitShardReplicationSetupUDF(
			sourceShardToCopyNode,
			sourceColocatedShardIntervalList,
			shardGroupSplitIntervalListList,
			workersForPlacementList,
			distributionColumnOverrides);

		/*
		 * Subscriber flow starts from here.
		 * Populate 'ShardSplitSubscriberMetadata' for subscription management.
		 */
		List *logicalRepTargetList =
			PopulateShardSplitSubscriptionsMetadataList(
				publicationInfoHash, replicationSlotInfoList,
				shardGroupSplitIntervalListList, workersForPlacementList);

		HTAB *groupedLogicalRepTargetsHash = CreateGroupedLogicalRepTargetsHash(
			logicalRepTargetList);

		/* Create connections to the target nodes */
		CreateGroupedLogicalRepTargetsConnections(
			groupedLogicalRepTargetsHash,
			superUser, databaseName);

		char *logicalRepDecoderPlugin = "citus";

		/*
		 * 6) Create replication slots and keep track of their snapshot.
		 */
		char *snapshot = CreateReplicationSlots(
			sourceConnection,
			sourceReplicationConnection,
			logicalRepTargetList,
			logicalRepDecoderPlugin);

		/*
		 * 7) Create subscriptions. This isn't strictly needed yet at this
		 * stage, but this way we error out quickly if it fails.
		 */
		CreateSubscriptions(
			sourceConnection,
			databaseName,
			logicalRepTargetList);

		/* 8) Do snapshotted Copy */
		DoSplitCopy(sourceShardToCopyNode, sourceColocatedShardIntervalList,
					shardGroupSplitIntervalListList, workersForPlacementList,
					snapshot, distributionColumnOverrides);

		/*
		 * 9) Create replica identities, this needs to be done before enabling
		 * the subscriptions.
		 */
		CreateReplicaIdentities(logicalRepTargetList);

		/*
		 * 10) Enable the subscriptions: Start the catchup phase
		 */
		EnableSubscriptions(logicalRepTargetList);

		/* 11) Wait for subscriptions to be ready */
		WaitForAllSubscriptionsToBecomeReady(groupedLogicalRepTargetsHash);

		/* 12) Wait for subscribers to catchup till source LSN */
		WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);

		/* 13) Create Auxilary structures */
		CreateAuxiliaryStructuresForShardGroup(shardGroupSplitIntervalListList,
											   workersForPlacementList,
											   false /* includeReplicaIdentity*/);

		/* 14) Wait for subscribers to catchup till source LSN */
		WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);

		/* Used for testing */
		ConflictOnlyWithIsolationTesting();

		/* 15) Block writes on source shards */
		BlockWritesToShardList(sourceColocatedShardIntervalList);

		/* 16) Wait for subscribers to catchup till source LSN */
		WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);

		/* 17) Drop Subscribers */
		DropSubscriptions(logicalRepTargetList);

		/* 18) Drop replication slots
		 */
		DropReplicationSlots(sourceConnection, logicalRepTargetList);

		/* 19) Drop Publications */
		DropPublications(sourceConnection, publicationInfoHash);

		/*
		 * 20) Drop old shards and delete related metadata. Have to do that before
		 * creating the new shard metadata, because there's cross-checks
		 * preventing inconsistent metadata (like overlapping shards).
		 */
		DropShardList(sourceColocatedShardIntervalList);

		/*
		 * 21) In case of create_distributed_table_concurrently, which converts
		 * a Citus local table to a distributed table, update the distributed
		 * table metadata now.
		 *
		 * We would rather have this be outside of the scope of NonBlockingShardSplit,
		 * but we cannot make metadata changes before replication slot creation, and
		 * we cannot create the replication slot before creating new shards and
		 * corresponding publications, because the decoder uses a catalog snapshot
		 * from the time of the slot creation, which means it would not be able to see
		 * the shards or publications when replication starts if it was created before.
		 *
		 * We also cannot easily move metadata changes to be after this function,
		 * because CreateForeignKeyConstraints relies on accurate metadata and
		 * we also want to perform the clean-up logic in PG_CATCH in case of
		 * failure.
		 *
		 * Hence, this appears to be the only suitable spot for updating
		 * pg_dist_partition and pg_dist_colocation.
		 */
		if (splitOperation == CREATE_DISTRIBUTED_TABLE)
		{
			/* we currently only use split for hash-distributed tables */
			char distributionMethod = DISTRIBUTE_BY_HASH;
			int shardCount = list_length(shardSplitPointsList) + 1;

			UpdateDistributionColumnsForShardGroup(sourceColocatedShardIntervalList,
												   distributionColumnOverrides,
												   distributionMethod,
												   shardCount,
												   targetColocationId);
		}

		/* 22) Insert new shard and placement metdata */
		InsertSplitChildrenShardMetadata(shardGroupSplitIntervalListList,
										 workersForPlacementList);

		CreatePartitioningHierarchy(shardGroupSplitIntervalListList,
									workersForPlacementList);

		/*
		 * 23) Create foreign keys if exists after the metadata changes happening in
		 * DropShardList() and InsertSplitChildrenShardMetadata() because the foreign
		 * key creation depends on the new metadata.
		 */
		CreateForeignKeyConstraints(shardGroupSplitIntervalListList,
									workersForPlacementList);

		/*
		 * 24) Drop dummy shards.
		 */
		DropDummyShards(mapOfDummyShardToPlacement);

		/*
		 * 24) Release shared memory allocated by worker_split_shard_replication_setup udf
		 * at source node.
		 */
		ExecuteSplitShardReleaseSharedMemory(sourceShardToCopyNode);

		/* 25) Close source connection */
		CloseConnection(sourceConnection);

		/* 26) Close all subscriber connections */
		CloseGroupedLogicalRepTargetsConnections(groupedLogicalRepTargetsHash);

		/* 27) Close connection of template replication slot */
		CloseConnection(sourceReplicationConnection);
	}
	PG_CATCH();
	{
		/* end ongoing transactions to enable us to clean up */
		ShutdownAllConnections();

		/* Do a best effort cleanup of shards created on workers in the above block */
		TryDropSplitShardsOnFailure(mapOfShardToPlacementCreatedByWorkflow);

		DropAllLogicalReplicationLeftovers(SHARD_SPLIT);

		DropDummyShards(mapOfDummyShardToPlacement);

		ExecuteSplitShardReleaseSharedMemory(sourceShardToCopyNode);

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * Given we are using PG logical replication infrastructure there are some constraints
 * that need to met around matching table names in source and target nodes:
 * The restrictions in context of split are:
 * Constraint 1: Dummy source shard(s) from shard group must exist on all destination nodes.
 * Constraint 2: Dummy target shards from shard group must exist on source node.
 * Example :
 * Shard1[1-200] is co-located with Shard2[1-200] in Worker0.
 * We are splitting 2-way to worker0 (same node) and worker1 (different node).
 *
 * Non-Dummy shards (expected from Split):
 * In Worker0 --> Shard1_1 and Shard2_1.
 * In Worker1 --> Shard1_2 and Shard2_2.
 *
 * Dummy shards:
 * From constraint 1, we need to create: Dummy Shard1 and Shard2 in Worker0. Dummy Shard1 and Shard2 in Worker1
 * Note 1 : Given there is an overlap of source and destination in Worker0, Shard1 and Shard2 need not be created.
 * Be very careful here, dropping Shard1, Shard2 with customer data to create dummy Shard1, Shard2 on worker0 is catastrophic.
 *
 * From constraint 2, we need to create: Dummy Shard1_1, Shard2_1, Shard1_2 and Shard2_2 in Worker0.
 * Note 2 : Given there is an overlap of source and destination in Worker0, Shard1_1 and Shard2_1 need not be created.
 */
static void
CreateDummyShardsForShardGroup(HTAB *mapOfDummyShardToPlacement,
							   List *sourceColocatedShardIntervalList,
							   List *shardGroupSplitIntervalListList,
							   WorkerNode *sourceWorkerNode,
							   List *workersForPlacementList)
{
	/*
	 * Statisfy Constraint 1: Create dummy source shard(s) on all destination nodes.
	 * If source node is also in desintation, skip dummy shard creation(see Note 1 from function description).
	 * We are guarenteed to have a single active placement for source shard. This is enforced earlier by ErrorIfCannotSplitShardExtended.
	 */

	/* List 'workersForPlacementList' can have duplicates. We need all unique destination nodes. */
	HTAB *workersForPlacementSet = CreateWorkerForPlacementSet(workersForPlacementList);

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, workersForPlacementSet);
	WorkerNode *workerPlacementNode = NULL;
	while ((workerPlacementNode = (WorkerNode *) hash_seq_search(&status)) != NULL)
	{
		if (workerPlacementNode->nodeId == sourceWorkerNode->nodeId)
		{
			continue;
		}

		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, sourceColocatedShardIntervalList)
		{
			/* Populate list of commands necessary to create shard interval on destination */
			List *splitShardCreationCommandList = GetPreLoadTableCreationCommands(
				shardInterval->relationId,
				false, /* includeSequenceDefaults */
				NULL /* auto add columnar options for cstore tables */);
			splitShardCreationCommandList = WorkerApplyShardDDLCommandList(
				splitShardCreationCommandList,
				shardInterval->shardId);

			/* Create dummy source shard on the specified placement list */
			CreateObjectOnPlacement(splitShardCreationCommandList, workerPlacementNode);

			/* Add dummy source shard entry created for placement node in map */
			AddDummyShardEntryInMap(mapOfDummyShardToPlacement,
									workerPlacementNode->nodeId,
									shardInterval);
		}
	}

	/*
	 * Statisfy Constraint 2: Create dummy target shards from shard group on source node.
	 * If the target shard was created on source node as placement, skip it (See Note 2 from function description).
	 */
	List *shardIntervalList = NULL;
	foreach_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *shardInterval = NULL;
		workerPlacementNode = NULL;
		forboth_ptr(shardInterval, shardIntervalList, workerPlacementNode,
					workersForPlacementList)
		{
			if (workerPlacementNode->nodeId == sourceWorkerNode->nodeId)
			{
				continue;
			}

			List *splitShardCreationCommandList = GetPreLoadTableCreationCommands(
				shardInterval->relationId,
				false, /* includeSequenceDefaults */
				NULL /* auto add columnar options for cstore tables */);
			splitShardCreationCommandList = WorkerApplyShardDDLCommandList(
				splitShardCreationCommandList,
				shardInterval->shardId);

			/* Create dummy split child shard on source worker node */
			CreateObjectOnPlacement(splitShardCreationCommandList, sourceWorkerNode);

			/* Add dummy split child shard entry created on source node */
			AddDummyShardEntryInMap(mapOfDummyShardToPlacement, sourceWorkerNode->nodeId,
									shardInterval);
		}
	}
}


/*
 * CreateWorkerForPlacementSet returns a set with unique worker nodes.
 */
static HTAB *
CreateWorkerForPlacementSet(List *workersForPlacementList)
{
	HASHCTL info = { 0 };
	info.keysize = sizeof(WorkerNode);
	info.hash = WorkerNodeHashCode;
	info.match = WorkerNodeCompare;

	/* we don't have value field as it's a set */
	info.entrysize = info.keysize;

	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB *workerForPlacementSet = hash_create("worker placement set", 32, &info,
											  hashFlags);

	WorkerNode *workerForPlacement = NULL;
	foreach_ptr(workerForPlacement, workersForPlacementList)
	{
		void *hashKey = (void *) workerForPlacement;
		hash_search(workerForPlacementSet, hashKey, HASH_ENTER, NULL);
	}

	return workerForPlacementSet;
}


/*
 * ExecuteSplitShardReplicationSetupUDF executes
 * 'worker_split_shard_replication_setup' UDF on source shard node
 * and returns list of ReplicationSlotInfo.
 */
static List *
ExecuteSplitShardReplicationSetupUDF(WorkerNode *sourceWorkerNode,
									 List *sourceColocatedShardIntervalList,
									 List *shardGroupSplitIntervalListList,
									 List *destinationWorkerNodesList,
									 DistributionColumnMap *distributionColumnOverrides)
{
	StringInfo splitShardReplicationUDF = CreateSplitShardReplicationSetupUDF(
		sourceColocatedShardIntervalList,
		shardGroupSplitIntervalListList,
		destinationWorkerNodesList,
		distributionColumnOverrides);

	/* Force a new connection to execute the UDF */
	int connectionFlags = 0;
	MultiConnection *sourceConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  sourceWorkerNode->
																	  workerName,
																	  sourceWorkerNode->
																	  workerPort,
																	  CitusExtensionOwnerName(),
																	  get_database_name(
																		  MyDatabaseId));
	ClaimConnectionExclusively(sourceConnection);

	PGresult *result = NULL;
	int queryResult = ExecuteOptionalRemoteCommand(sourceConnection,
												   splitShardReplicationUDF->data,
												   &result);

	/*
	 * Result should contain atleast one tuple. The information returned is
	 * set of tuples where each tuple is formatted as:
	 * <targetNodeId, tableOwnerName, replication_slot_name>.
	 */
	if (queryResult != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) < 1 ||
		PQnfields(result) != 3)
	{
		PQclear(result);
		ForgetResults(sourceConnection);
		CloseConnection(sourceConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"Failed to run worker_split_shard_replication_setup UDF. It should successfully execute "
							" for splitting a shard in a non-blocking way. Please retry.")));
	}

	/* Get replication slot information */
	List *replicationSlotInfoList = ParseReplicationSlotInfoFromResult(result);

	PQclear(result);
	ForgetResults(sourceConnection);

	CloseConnection(sourceConnection);
	return replicationSlotInfoList;
}


/*
 * ExecuteSplitShardReleaseSharedMemory releases dynamic shared memory
 * at source node.
 * As a part of non-blocking split workflow, worker_split_shard_replication_setup allocates
 * shared memory to store split information. This has to be released after split completes(or fails).
 */
static void
ExecuteSplitShardReleaseSharedMemory(WorkerNode *sourceWorkerNode)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);

	int connectionFlag = FORCE_NEW_CONNECTION;
	MultiConnection *sourceConnection = GetNodeUserDatabaseConnection(
		connectionFlag,
		sourceWorkerNode->workerName,
		sourceWorkerNode->workerPort,
		superUser,
		databaseName);

	StringInfo splitShardReleaseMemoryUDF = makeStringInfo();
	appendStringInfo(splitShardReleaseMemoryUDF,
					 "SELECT pg_catalog.worker_split_shard_release_dsm();");
	ExecuteCriticalRemoteCommand(sourceConnection, splitShardReleaseMemoryUDF->data);
}


/*
 * CreateSplitShardReplicationSetupUDF creates and returns
 * parameterized 'worker_split_shard_replication_setup' UDF command.
 *
 * 'sourceShardSplitIntervalList'    : Source shard interval to split.
 * 'shardGroupSplitIntervalListList' : List of shard intervals for split children..
 * 'destinationWorkerNodesList'      : List of workers for split children placement.
 *
 * For example consider below input values:
 * sourceColocatedShardIntervalList : [sourceShardInterval]
 * shardGroupSplitIntervalListList  : [<childFirstShardInterval, childSecondShardInterval>]
 * destinationWorkerNodesList       : [worker1, worker2]
 *
 * SELECT * FROM worker_split_shard_replication_setup(
 *  Array[
 *      ROW(sourceShardId, childFirstShardId, childFirstMinRange, childFirstMaxRange, worker1)::citus.split_shard_info,
 *      ROW(sourceShardId, childSecondShardId, childSecondMinRange, childSecondMaxRange, worker2)::citus.split_shard_info
 *  ]);
 */
StringInfo
CreateSplitShardReplicationSetupUDF(List *sourceColocatedShardIntervalList,
									List *shardGroupSplitIntervalListList,
									List *destinationWorkerNodesList,
									DistributionColumnMap *distributionColumnOverrides)
{
	StringInfo splitChildrenRows = makeStringInfo();

	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitChildShardIntervalList = NULL;
	bool addComma = false;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitChildShardIntervalList, shardGroupSplitIntervalListList)
	{
		int64 sourceShardId = sourceShardIntervalToCopy->shardId;
		Oid relationId = sourceShardIntervalToCopy->relationId;

		Var *distributionColumn =
			GetDistributionColumnWithOverrides(relationId,
											   distributionColumnOverrides);

		bool missingOK = false;
		char *distributionColumnName =
			get_attname(relationId, distributionColumn->varattno,
						missingOK);

		ShardInterval *splitChildShardInterval = NULL;
		WorkerNode *destinationWorkerNode = NULL;
		forboth_ptr(splitChildShardInterval, splitChildShardIntervalList,
					destinationWorkerNode, destinationWorkerNodesList)
		{
			if (addComma)
			{
				appendStringInfo(splitChildrenRows, ",");
			}

			StringInfo minValueString = makeStringInfo();
			appendStringInfo(minValueString, "%d", DatumGetInt32(
								 splitChildShardInterval->minValue));

			StringInfo maxValueString = makeStringInfo();
			appendStringInfo(maxValueString, "%d", DatumGetInt32(
								 splitChildShardInterval->maxValue));

			appendStringInfo(splitChildrenRows,
							 "ROW(%lu, %s, %lu, %s, %s, %u)::pg_catalog.split_shard_info",
							 sourceShardId,
							 quote_literal_cstr(distributionColumnName),
							 splitChildShardInterval->shardId,
							 quote_literal_cstr(minValueString->data),
							 quote_literal_cstr(maxValueString->data),
							 destinationWorkerNode->nodeId);

			addComma = true;
		}
	}

	StringInfo splitShardReplicationUDF = makeStringInfo();
	appendStringInfo(splitShardReplicationUDF,
					 "SELECT * FROM pg_catalog.worker_split_shard_replication_setup(ARRAY[%s]);",
					 splitChildrenRows->data);

	return splitShardReplicationUDF;
}


/*
 * ParseReplicationSlotInfoFromResult parses custom datatype 'replication_slot_info'.
 * 'replication_slot_info' is a tuple with below format:
 * <targetNodeId, tableOwnerName, replicationSlotName>
 */
static List *
ParseReplicationSlotInfoFromResult(PGresult *result)
{
	int64 rowCount = PQntuples(result);

	List *replicationSlotInfoList = NIL;
	for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		ReplicationSlotInfo *replicationSlot = (ReplicationSlotInfo *) palloc0(
			sizeof(ReplicationSlotInfo));

		char *targeNodeIdString = PQgetvalue(result, rowIndex, 0 /* nodeId column*/);

		replicationSlot->targetNodeId = strtoul(targeNodeIdString, NULL, 10);

		bool missingOk = false;
		replicationSlot->tableOwnerId = get_role_oid(
			PQgetvalue(result, rowIndex, 1 /* table owner name column */),
			missingOk);

		/* Replication slot name */
		replicationSlot->name = pstrdup(PQgetvalue(result, rowIndex,
												   2 /* slot name column */));

		replicationSlotInfoList = lappend(replicationSlotInfoList, replicationSlot);
	}

	return replicationSlotInfoList;
}


/*
 * AddDummyShardEntryInMap adds shard entry into hash map to keep track
 * of dummy shards that are created. These shards are cleanedup after split completes.
 *
 * This is a cautious measure to keep track of dummy shards created for constraints
 * of logical replication. We cautiously delete only the dummy shards added in the DummyShardHashMap.
 */
static void
AddDummyShardEntryInMap(HTAB *mapOfDummyShardToPlacement, uint32 targetNodeId,
						ShardInterval *shardInterval)
{
	NodeAndOwner key;
	key.nodeId = targetNodeId;
	key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

	bool found = false;
	GroupedDummyShards *nodeMappingEntry =
		(GroupedDummyShards *) hash_search(mapOfDummyShardToPlacement, &key,
										   HASH_ENTER,
										   &found);
	if (!found)
	{
		nodeMappingEntry->shardIntervals = NIL;
	}

	nodeMappingEntry->shardIntervals =
		lappend(nodeMappingEntry->shardIntervals, shardInterval);
}


/*
 * DropDummyShards traverses the dummy shard map and drops shard at given node.
 * It fails if the shard cannot be dropped.
 */
static void
DropDummyShards(HTAB *mapOfDummyShardToPlacement)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, mapOfDummyShardToPlacement);

	GroupedDummyShards *entry = NULL;
	while ((entry = (GroupedDummyShards *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		WorkerNode *shardToBeDroppedNode = FindNodeWithNodeId(nodeId,
															  false /* missingOk */);

		int connectionFlags = FOR_DDL;
		connectionFlags |= OUTSIDE_TRANSACTION;
		MultiConnection *connection = GetNodeUserDatabaseConnection(
			connectionFlags,
			shardToBeDroppedNode->workerName,
			shardToBeDroppedNode->workerPort,
			CurrentUserName(),
			NULL /* databaseName */);

		List *dummyShardIntervalList = entry->shardIntervals;
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, dummyShardIntervalList)
		{
			DropDummyShard(connection, shardInterval);
		}

		CloseConnection(connection);
	}
}


/*
 * DropDummyShard drops a given shard on the node connection.
 * It fails if the shard cannot be dropped.
 */
static void
DropDummyShard(MultiConnection *connection, ShardInterval *shardInterval)
{
	char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);
	StringInfo dropShardQuery = makeStringInfo();

	/* Caller enforces that foreign tables cannot be split (use DROP_REGULAR_TABLE_COMMAND) */
	appendStringInfo(dropShardQuery, DROP_REGULAR_TABLE_COMMAND,
					 qualifiedShardName);

	/*
	 * Since the dummy shard is expected to be present on the given node,
	 * fail if it cannot be dropped during cleanup.
	 */
	ExecuteCriticalRemoteCommand(
		connection,
		dropShardQuery->data);
}


/*
 * CreateReplicaIdentitiesForDummyShards creates replica indentities for split
 * dummy shards.
 */
static void
CreateReplicaIdentitiesForDummyShards(HTAB *mapOfDummyShardToPlacement)
{
	/* Create Replica Identities for dummy shards */
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, mapOfDummyShardToPlacement);

	GroupedDummyShards *entry = NULL;
	while ((entry = (GroupedDummyShards *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		WorkerNode *shardToBeDroppedNode = FindNodeWithNodeId(nodeId,
															  false /* missingOk */);

		List *dummyShardIntervalList = entry->shardIntervals;
		CreateReplicaIdentitiesOnNode(dummyShardIntervalList,
									  shardToBeDroppedNode->workerName,
									  shardToBeDroppedNode->workerPort);
	}
}


/*
 * GetNextShardIdForSplitChild returns shard id to be used for split child.
 * The function connects to the local node through a new connection and gets the next
 * sequence. This prevents self deadlock when 'CREATE_REPLICATION_SLOT' is executed
 * as a part of nonblocking split workflow.
 */
static uint64
GetNextShardIdForSplitChild()
{
	uint64 shardId = 0;

	/*
	 * In regression tests, we would like to generate shard IDs consistently
	 * even if the tests run in parallel. Instead of the sequence, we can use
	 * the next_shard_id GUC to specify which shard ID the current session should
	 * generate next. The GUC is automatically increased by 1 every time a new
	 * shard ID is generated.
	 */
	if (NextShardId > 0)
	{
		shardId = NextShardId;
		NextShardId += 1;

		return shardId;
	}

	StringInfo nextValueCommand = makeStringInfo();
	appendStringInfo(nextValueCommand, "SELECT nextval(%s);", quote_literal_cstr(
						 "pg_catalog.pg_dist_shardid_seq"));

	int connectionFlag = FORCE_NEW_CONNECTION;
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlag,
																LocalHostName,
																PostPortNumber,
																CitusExtensionOwnerName(),
																get_database_name(
																	MyDatabaseId));

	PGresult *result = NULL;
	int queryResult = ExecuteOptionalRemoteCommand(connection, nextValueCommand->data,
												   &result);
	if (queryResult != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1 ||
		PQnfields(result) != 1)
	{
		PQclear(result);
		ForgetResults(connection);
		CloseConnection(connection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"Could not generate next shard id while executing shard splits.")));
	}

	shardId = SafeStringToUint64(PQgetvalue(result, 0, 0 /* nodeId column*/));
	CloseConnection(connection);

	return shardId;
}
