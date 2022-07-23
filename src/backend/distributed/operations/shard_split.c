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
#include "distributed/metadata_cache.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_split.h"
#include "distributed/reference_table_utils.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "distributed/shared_library_init.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_physical_planner.h"
#include "commands/dbcommands.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/deparse_shard_query.h"

/*
 * Entry for map that tracks ShardInterval -> Placement Node
 * created by split workflow.
 */
typedef struct ShardCreatedByWorkflowEntry
{
	ShardInterval *shardIntervalKey;
	WorkerNode *workerNodeValue;
} ShardCreatedByWorkflowEntry;

/* Function declarations */
static void ErrorIfCannotSplitShardExtended(SplitOperation splitOperation,
											ShardInterval *shardIntervalToSplit,
											List *shardSplitPointsList,
											List *nodeIdsForPlacementList);
static void CreateAndCopySplitShardsForShardGroup(
	HTAB *mapOfShardToPlacementCreatedByWorkflow,
	WorkerNode *sourceShardNode,
	List *sourceColocatedShardIntervalList,
	List *shardGroupSplitIntervalListList,
	List *workersForPlacementList);
static void CreateSplitShardsForShardGroup(HTAB *mapOfShardToPlacementCreatedByWorkflow,
										   List *shardGroupSplitIntervalListList,
										   List *workersForPlacementList);
static void CreateDummyShardsForShardGroup(List *sourceColocatedShardIntervalList,
										   List *shardGroupSplitIntervalListList,
										   WorkerNode *sourceWorkerNode,
										   List *workersForPlacementList);
static HTAB * CreateWorkerForPlacementSet(List *workersForPlacementList);
static void CreateAuxiliaryStructuresForShardGroup(List *shardGroupSplitIntervalListList,
												   List *workersForPlacementList);
static void CreateObjectOnPlacement(List *objectCreationCommandList,
									WorkerNode *workerNode);
static List *    CreateSplitIntervalsForShardGroup(List *sourceColocatedShardList,
												   List *splitPointsForShard);
static void CreateSplitIntervalsForShard(ShardInterval *sourceShard,
										 List *splitPointsForShard,
										 List **shardSplitChildrenIntervalList);
static void BlockingShardSplit(SplitOperation splitOperation,
							   ShardInterval *shardIntervalToSplit,
							   List *shardSplitPointsList,
							   List *workersForPlacementList);
static void NonBlockingShardSplit(SplitOperation splitOperation,
								  ShardInterval *shardIntervalToSplit,
								  List *shardSplitPointsList,
								  List *workersForPlacementList);

static void DoSplitCopy(WorkerNode *sourceShardNode,
						List *sourceColocatedShardIntervalList,
						List *shardGroupSplitIntervalListList,
						List *workersForPlacementList,
						char *snapShotName);
static StringInfo CreateSplitCopyCommand(ShardInterval *sourceShardSplitInterval,
										 List *splitChildrenShardIntervalList,
										 List *workersForPlacementList);
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
	List *destinationWorkerNodesList);
static void AddDummyShardEntryInMap(uint32 targetNodeId, ShardInterval *shardInterval);
static void DropDummyShards(void);
static void TryDroppingShard(MultiConnection *connection, ShardInterval *shardInterval);
char * CreateTemplateReplicationSlotAndReturnSnapshot(ShardInterval *shardInterval,
													  WorkerNode *sourceWorkerNode,
													  MultiConnection **
													  templateSlotConnection);

static List * ExecuteSplitShardReplicationSetupUDF(WorkerNode *sourceWorkerNode,
												   List *sourceColocatedShardIntervalList,
												   List *shardGroupSplitIntervalListList,
												   List *destinationWorkerNodesList);
static List * ParseReplicationSlotInfoFromResult(PGresult *result);

/* Customize error message strings based on operation type */
static const char *const SplitOperationName[] =
{
	[SHARD_SPLIT_API] = "split",
	[ISOLATE_TENANT_TO_NEW_SHARD] = "isolate",
};
static const char *const SplitTargetName[] =
{
	[SHARD_SPLIT_API] = "shard",
	[ISOLATE_TENANT_TO_NEW_SHARD] = "tenant",
};

/* Map containing list of dummy shards created on target nodes */
static HTAB *DummyShardInfoHashMap = NULL;

/* Function definitions */

/*
 * ErrorIfCannotSplitShard checks relation kind and invalid shards. It errors
 * out if we are not able to split the given shard.
 */
void
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

	if (extern_IsColumnarTableAmTable(shardIntervalToSplit->relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Cannot %s %s as operation "
							   "is not supported for Columnar tables.",
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
 * SplitShard API to split a given shard (or shard group) based on specified split points
 * to a set of destination nodes.
 * 'splitMode'					: Mode of split operation.
 * 'splitOperation'             : Customer operation that triggered split.
 * 'shardInterval'              : Source shard interval to be split.
 * 'shardSplitPointsList'		: Split Points list for the source 'shardInterval'.
 * 'nodeIdsForPlacementList'	: Placement list corresponding to split children.
 */
void
SplitShard(SplitMode splitMode,
		   SplitOperation splitOperation,
		   uint64 shardIdToSplit,
		   List *shardSplitPointsList,
		   List *nodeIdsForPlacementList)
{
	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot %s %s after other modifications "
							   "in the same transaction.",
							   SplitOperationName[splitOperation],
							   SplitTargetName[splitOperation])));
	}

	ShardInterval *shardIntervalToSplit = LoadShardInterval(shardIdToSplit);
	List *colocatedTableList = ColocatedTableList(shardIntervalToSplit->relationId);

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

	ErrorIfCannotSplitShard(SHARD_SPLIT_API, shardIntervalToSplit);
	ErrorIfCannotSplitShardExtended(
		SHARD_SPLIT_API,
		shardIntervalToSplit,
		shardSplitPointsList,
		nodeIdsForPlacementList);

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

	if (splitMode == BLOCKING_SPLIT)
	{
		EnsureReferenceTablesExistOnAllNodesExtended(TRANSFER_MODE_BLOCK_WRITES);
		BlockingShardSplit(
			splitOperation,
			shardIntervalToSplit,
			shardSplitPointsList,
			workersForPlacementList);
	}
	else
	{
		/*TODO(saawasek): Discussing about existing bug with the assumption of move shard*/
		NonBlockingShardSplit(
			splitOperation,
			shardIntervalToSplit,
			shardSplitPointsList,
			workersForPlacementList);
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
 * 'splitOperation'             : Customer operation that triggered split.
 * 'shardIntervalToSplit'       : Source shard interval to be split.
 * 'shardSplitPointsList'		: Split Points list for the source 'shardInterval'.
 * 'workersForPlacementList'	: Placement list corresponding to split children.
 */
static void
BlockingShardSplit(SplitOperation splitOperation,
				   ShardInterval *shardIntervalToSplit,
				   List *shardSplitPointsList,
				   List *workersForPlacementList)
{
	List *sourceColocatedShardIntervalList = ColocatedShardIntervalList(
		shardIntervalToSplit);

	BlockWritesToShardList(sourceColocatedShardIntervalList);

	/* First create shard interval metadata for split children */
	List *shardGroupSplitIntervalListList = CreateSplitIntervalsForShardGroup(
		sourceColocatedShardIntervalList,
		shardSplitPointsList);

	/* Only single placement allowed (already validated RelationReplicationFactor = 1) */
	List *sourcePlacementList = ActiveShardPlacementList(shardIntervalToSplit->shardId);
	Assert(sourcePlacementList->length == 1);
	ShardPlacement *sourceShardPlacement = (ShardPlacement *) linitial(
		sourcePlacementList);
	WorkerNode *sourceShardToCopyNode = FindNodeWithNodeId(sourceShardPlacement->nodeId,
														   false /* missingOk */);


	HTAB *mapOfShardToPlacementCreatedByWorkflow =
		CreateEmptyMapForShardsCreatedByWorkflow();
	PG_TRY();
	{
		/*
		 * Physically create split children, perform split copy and create auxiliary structures.
		 * This includes: indexes, replicaIdentity. triggers and statistics.
		 * Foreign key constraints are created after Metadata changes (see CreateForeignKeyConstraints).
		 */
		CreateAndCopySplitShardsForShardGroup(
			mapOfShardToPlacementCreatedByWorkflow,
			sourceShardToCopyNode,
			sourceColocatedShardIntervalList,
			shardGroupSplitIntervalListList,
			workersForPlacementList);

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
									   List *workersForPlacementList)
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
				true /* includeReplicaIdentity */);
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
 * Create ShardGroup split children, perform copy and create auxiliary structures
 * on a list of corresponding workers.
 */
static void
CreateAndCopySplitShardsForShardGroup(HTAB *mapOfShardToPlacementCreatedByWorkflow,
									  WorkerNode *sourceShardNode,
									  List *sourceColocatedShardIntervalList,
									  List *shardGroupSplitIntervalListList,
									  List *workersForPlacementList)
{
	CreateSplitShardsForShardGroup(mapOfShardToPlacementCreatedByWorkflow,
								   shardGroupSplitIntervalListList,
								   workersForPlacementList);

	DoSplitCopy(sourceShardNode, sourceColocatedShardIntervalList,
				shardGroupSplitIntervalListList, workersForPlacementList, NULL);

	/* Create auxiliary structures (indexes, stats, replicaindentities, triggers) */
	CreateAuxiliaryStructuresForShardGroup(shardGroupSplitIntervalListList,
										   workersForPlacementList);
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
			char *snapShotName)
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
		if (!PartitionedTable(sourceShardIntervalToCopy->relationId))
		{
			StringInfo splitCopyUdfCommand = CreateSplitCopyCommand(
				sourceShardIntervalToCopy,
				splitShardIntervalList,
				destinationWorkerNodesList);

			List *ddlCommandList = NIL;
			StringInfo beginTransaction = makeStringInfo();
			appendStringInfo(beginTransaction,
							 "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
			ddlCommandList = lappend(ddlCommandList, beginTransaction->data);

			/* Set snapshot */
			if (snapShotName != NULL)
			{
				StringInfo snapShotString = makeStringInfo();
				appendStringInfo(snapShotString, "SET TRANSACTION SNAPSHOT %s;",
								 quote_literal_cstr(
									 snapShotName));
				ddlCommandList = lappend(ddlCommandList, snapShotString->data);
				printf("Sameer final string snapshotted:%s\n", snapShotString->data);
			}

			ddlCommandList = lappend(ddlCommandList, splitCopyUdfCommand->data);

			StringInfo commitCommand = makeStringInfo();
			appendStringInfo(commitCommand, "COMMIT;");
			ddlCommandList = lappend(ddlCommandList, commitCommand->data);

			Task *splitCopyTask = CitusMakeNode(Task);
			splitCopyTask->jobId = sourceShardIntervalToCopy->shardId;
			splitCopyTask->taskId = taskId;
			splitCopyTask->taskType = READ_TASK;
			splitCopyTask->replicationModel = REPLICATION_MODEL_INVALID;
			SetTaskQueryStringList(splitCopyTask, ddlCommandList);

			ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
			SetPlacementNodeMetadata(taskPlacement, sourceShardNode);

			splitCopyTask->taskPlacementList = list_make1(taskPlacement);

			splitCopyTaskList = lappend(splitCopyTaskList, splitCopyTask);
			taskId++;
		}
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
	appendStringInfo(splitCopyUdf, "SELECT pg_catalog.worker_split_copy(%lu, %s);",
					 sourceShardSplitInterval->shardId,
					 splitCopyInfoArray->data);

	return splitCopyUdf;
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
	for (int index = 0; index < shardIntervalCount; index++)
	{
		ShardInterval *splitChildShardInterval = CopyShardInterval(sourceShard);
		splitChildShardInterval->shardIndex = -1;
		splitChildShardInterval->shardId = GetNextShardId();

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
		forboth_ptr(shardInterval, shardIntervalList, workerPlacementNode,
					workersForPlacementList)
		{
			List *shardForeignConstraintCommandList = NIL;
			List *referenceTableForeignConstraintList = NIL;

			CopyShardForeignConstraintCommandListGrouped(shardInterval,
														 &
														 shardForeignConstraintCommandList,
														 &
														 referenceTableForeignConstraintList);

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
 * SplitShard API to split a given shard (or shard group) in blocking fashion
 * based on specified split points to a set of destination nodes.
 * 'splitOperation'             : Customer operation that triggered split.
 * 'shardIntervalToSplit'       : Source shard interval to be split.
 * 'shardSplitPointsList'		: Split Points list for the source 'shardInterval'.
 * 'workersForPlacementList'	: Placement list corresponding to split children.
 */
static void
NonBlockingShardSplit(SplitOperation splitOperation,
					  ShardInterval *shardIntervalToSplit,
					  List *shardSplitPointsList,
					  List *workersForPlacementList)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);
	int connectionFlags = FORCE_NEW_CONNECTION;

	List *sourceColocatedShardIntervalList = ColocatedShardIntervalList(
		shardIntervalToSplit);

	/* First create shard interval metadata for split children */
	List *shardGroupSplitIntervalListList = CreateSplitIntervalsForShardGroup(
		sourceColocatedShardIntervalList,
		shardSplitPointsList);

	/* Only single placement allowed (already validated RelationReplicationFactor = 1) */
	List *sourcePlacementList = ActiveShardPlacementList(shardIntervalToSplit->shardId);
	Assert(sourcePlacementList->length == 1);
	ShardPlacement *sourceShardPlacement = (ShardPlacement *) linitial(
		sourcePlacementList);
	WorkerNode *sourceShardToCopyNode = FindNodeWithNodeId(sourceShardPlacement->nodeId,
														   false /* missingOk */);

	HTAB *shardSplitHashMapForPublication = CreateShardSplitInfoMapForPublication(
		sourceColocatedShardIntervalList,
		shardGroupSplitIntervalListList,
		workersForPlacementList);

	DropAllShardSplitLeftOvers(sourceShardToCopyNode, shardSplitHashMapForPublication);

	MultiConnection *sourceConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  sourceShardToCopyNode
																	  ->
																	  workerName,
																	  sourceShardToCopyNode
																	  ->
																	  workerPort,
																	  superUser,
																	  databaseName);
	ClaimConnectionExclusively(sourceConnection);

	HTAB *mapOfShardToPlacementCreatedByWorkflow =
		CreateEmptyMapForShardsCreatedByWorkflow();
	PG_TRY();
	{
		/* Physically create split children. */
		CreateSplitShardsForShardGroup(mapOfShardToPlacementCreatedByWorkflow,
									   shardGroupSplitIntervalListList,
									   workersForPlacementList);


		CreateDummyShardsForShardGroup(
			sourceColocatedShardIntervalList,
			shardGroupSplitIntervalListList,
			sourceShardToCopyNode,
			workersForPlacementList);

		CreateShardSplitPublications(sourceConnection, shardSplitHashMapForPublication);

		/* Create Template Replication Slot */
		MultiConnection *templateSlotConnection = NULL;
		char *snapShotName = CreateTemplateReplicationSlotAndReturnSnapshot(
			shardIntervalToSplit, sourceShardToCopyNode, &templateSlotConnection);

		/* DoSplitCopy */
		DoSplitCopy(sourceShardToCopyNode, sourceColocatedShardIntervalList,
					shardGroupSplitIntervalListList, workersForPlacementList,
					snapShotName);

		/*worker_split_replication_setup_udf*/
		List *replicationSlotInfoList = ExecuteSplitShardReplicationSetupUDF(
			sourceShardToCopyNode,
			sourceColocatedShardIntervalList,
			shardGroupSplitIntervalListList,
			workersForPlacementList);

		/* Subscriber flow starts from here */
		List *shardSplitSubscribersMetadataList =
			PopulateShardSplitSubscriptionsMetadataList(
				shardSplitHashMapForPublication, replicationSlotInfoList);

		List *targetNodeConnectionList = CreateTargetNodeConnectionsForShardSplit(
			shardSplitSubscribersMetadataList,
			connectionFlags,
			superUser, databaseName);

		/* Create copies of template replication slot */
		char *templateSlotName = ShardSplitTemplateReplicationSlotName(
			shardIntervalToSplit->shardId);
		CreateReplicationSlots(sourceConnection, templateSlotName,
							   shardSplitSubscribersMetadataList);

		CreateShardSplitSubscriptions(targetNodeConnectionList,
									  shardSplitSubscribersMetadataList,
									  sourceShardToCopyNode,
									  superUser,
									  databaseName);

		WaitForShardSplitRelationSubscriptionsBecomeReady(
			shardSplitSubscribersMetadataList);

		XLogRecPtr sourcePosition = GetRemoteLogPosition(sourceConnection);
		WaitForShardSplitRelationSubscriptionsToBeCaughtUp(sourcePosition,
														   shardSplitSubscribersMetadataList);

		CreateAuxiliaryStructuresForShardGroup(shardGroupSplitIntervalListList,
											   workersForPlacementList);

		sourcePosition = GetRemoteLogPosition(sourceConnection);
		WaitForShardSplitRelationSubscriptionsToBeCaughtUp(sourcePosition,
														   shardSplitSubscribersMetadataList);

		BlockWritesToShardList(sourceColocatedShardIntervalList);

		sourcePosition = GetRemoteLogPosition(sourceConnection);
		WaitForShardSplitRelationSubscriptionsToBeCaughtUp(sourcePosition,
														   shardSplitSubscribersMetadataList);

		/* Drop Subscribers */
		DropShardSplitSubsriptions(shardSplitSubscribersMetadataList);

		/* Drop Publications */
		DropShardSplitPublications(sourceConnection, shardSplitHashMapForPublication);

		/*
		 * Drop old shards and delete related metadata. Have to do that before
		 * creating the new shard metadata, because there's cross-checks
		 * preventing inconsistent metadata (like overlapping shards).
		 */
		DropShardList(sourceColocatedShardIntervalList);

		/* Insert new shard and placement metdata */
		InsertSplitChildrenShardMetadata(shardGroupSplitIntervalListList,
										 workersForPlacementList);

		/*
		 * Create foreign keys if exists after the metadata changes happening in
		 * DropShardList() and InsertSplitChildrenShardMetadata() because the foreign
		 * key creation depends on the new metadata.
		 */
		CreateForeignKeyConstraints(shardGroupSplitIntervalListList,
									workersForPlacementList);

		DropDummyShards();

		/* Close source connection */
		CloseConnection(sourceConnection);

		/* Close all subscriber connections */
		CloseShardSplitSubscriberConnections(shardSplitSubscribersMetadataList);

		/* Close connection of template replication slot */
		CloseConnection(templateSlotConnection);
	}
	PG_CATCH();
	{
		/* Do a best effort cleanup of shards created on workers in the above block */
		TryDropSplitShardsOnFailure(mapOfShardToPlacementCreatedByWorkflow);

		DropDummyShards();

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
CreateDummyShardsForShardGroup(List *sourceColocatedShardIntervalList,
							   List *shardGroupSplitIntervalListList,
							   WorkerNode *sourceWorkerNode,
							   List *workersForPlacementList)
{
	/*
	 * Setup a hash map to store list of dummy shards created on nodes.
	 * This will facilitate easy cleanup.
	 */
	DummyShardInfoHashMap = SetupHashMapForShardInfo();

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
			AddDummyShardEntryInMap(workerPlacementNode->nodeId, shardInterval);
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
			AddDummyShardEntryInMap(sourceWorkerNode->nodeId, shardInterval);
		}
	}
}


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
 * CreateTemplateReplicationSlotAndReturnSnapshot creates a replication slot
 * and returns its snapshot. This slot acts as a 'Template' for creating
 * replication slot copies used for logical replication.
 *
 * The snapshot remains valid till the lifetime of the session that creates it.
 */
char *
CreateTemplateReplicationSlotAndReturnSnapshot(ShardInterval *shardInterval,
											   WorkerNode *sourceWorkerNode,
											   MultiConnection **templateSlotConnection)
{
	/*Create Template replication slot */
	int connectionFlags = FORCE_NEW_CONNECTION;
	connectionFlags |= EXCLUSIVE_AND_REPLICATION;

	MultiConnection *sourceConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  sourceWorkerNode->
																	  workerName,
																	  sourceWorkerNode->
																	  workerPort,
																	  CitusExtensionOwnerName(),
																	  get_database_name(
																		  MyDatabaseId));
	ClaimConnectionExclusively(sourceConnection);

	/*
	 * Try to drop leftover template replication slot if any from previous operation
	 * and create new one.
	 */
	char *snapShotName = DropExistingIfAnyAndCreateTemplateReplicationSlot(shardInterval,
																		   sourceConnection);
	*templateSlotConnection = sourceConnection;

	return snapShotName;
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
									 List *destinationWorkerNodesList)
{
	StringInfo splitShardReplicationUDF = CreateSplitShardReplicationSetupUDF(
		sourceColocatedShardIntervalList,
		shardGroupSplitIntervalListList,
		destinationWorkerNodesList);

	/* Force a new connection to execute the UDF */
	int connectionFlags = FORCE_NEW_CONNECTION;
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
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"Failed to run worker_split_shard_replication_setup UDF. It should successfully execute "
							" for splitting a shard in a non-blocking way. Please retry.")));

		PQclear(result);
		ForgetResults(sourceConnection);
		CloseConnection(sourceConnection);
	}

	/* Get replication slot information */
	List *replicationSlotInfoList = ParseReplicationSlotInfoFromResult(result);

	PQclear(result);
	ForgetResults(sourceConnection);

	CloseConnection(sourceConnection);
	return replicationSlotInfoList;
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
									List *destinationWorkerNodesList)
{
	StringInfo splitChildrenRows = makeStringInfo();

	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitChildShardIntervalList = NULL;
	bool addComma = false;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitChildShardIntervalList, shardGroupSplitIntervalListList)
	{
		int64 sourceShardId = sourceShardIntervalToCopy->shardId;

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
							 "ROW(%lu, %lu, %s, %s, %u)::citus.split_shard_info",
							 sourceShardId,
							 splitChildShardInterval->shardId,
							 quote_literal_cstr(minValueString->data),
							 quote_literal_cstr(maxValueString->data),
							 destinationWorkerNode->nodeId);

			addComma = true;
		}
	}

	StringInfo splitShardReplicationUDF = makeStringInfo();
	appendStringInfo(splitShardReplicationUDF,
					 "SELECT * FROM worker_split_shard_replication_setup(ARRAY[%s]);",
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
	int64 colCount = PQnfields(result);

	List *replicationSlotInfoList = NIL;
	for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		ReplicationSlotInfo *replicationSlotInfo = (ReplicationSlotInfo *) palloc0(
			sizeof(ReplicationSlotInfo));

		char *targeNodeIdString = PQgetvalue(result, rowIndex, 0 /* nodeId column*/);

		replicationSlotInfo->targetNodeId = strtoul(targeNodeIdString, NULL, 10);

		/* We're using the pstrdup to copy the data into the current memory context */
		replicationSlotInfo->tableOwnerName = pstrdup(PQgetvalue(result, rowIndex,
																 1 /* table owner name column */));

		/* Replication slot name */
		replicationSlotInfo->slotName = pstrdup(PQgetvalue(result, rowIndex,
														   2 /* slot name column */));

		replicationSlotInfoList = lappend(replicationSlotInfoList, replicationSlotInfo);
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
AddDummyShardEntryInMap(uint32 targetNodeId, ShardInterval *shardInterval)
{
	NodeShardMappingKey key;
	key.nodeId = targetNodeId;
	key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

	bool found = false;
	NodeShardMappingEntry *nodeMappingEntry =
		(NodeShardMappingEntry *) hash_search(DummyShardInfoHashMap, &key, HASH_ENTER,
											  &found);
	if (!found)
	{
		nodeMappingEntry->shardSplitInfoList = NIL;
	}

	nodeMappingEntry->shardSplitInfoList =
		lappend(nodeMappingEntry->shardSplitInfoList, (ShardInterval *) shardInterval);
}


static void
DropDummyShards()
{
	/* Return if no dummy shards are created */
	if (DummyShardInfoHashMap == NULL)
	{
		return;
	}

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, DummyShardInfoHashMap);

	NodeShardMappingEntry *entry = NULL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		WorkerNode *shardToBeDroppedNode = FindNodeWithNodeId(nodeId,
															  false /* missingOk */);

		int connectionFlags = FOR_DDL;
		connectionFlags |= OUTSIDE_TRANSACTION;
		connectionFlags |= FORCE_NEW_CONNECTION;
		MultiConnection *connection = GetNodeUserDatabaseConnection(
			connectionFlags,
			shardToBeDroppedNode->workerName,
			shardToBeDroppedNode->workerPort,
			CurrentUserName(),
			NULL /* databaseName */);

		List *dummyShardIntervalList = entry->shardSplitInfoList;
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, dummyShardIntervalList)
		{
			TryDroppingShard(connection, shardInterval);
		}

		CloseConnection(connection);
	}
}


/*
 * TryDroppingShard drops a given shard on the source node connection.
 */
static void
TryDroppingShard(MultiConnection *connection, ShardInterval *shardInterval)
{
	char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);
	StringInfo dropShardQuery = makeStringInfo();

	/* Caller enforces that foreign tables cannot be split (use DROP_REGULAR_TABLE_COMMAND) */
	appendStringInfo(dropShardQuery, DROP_REGULAR_TABLE_COMMAND,
					 qualifiedShardName);

	/*
	 * Perform a drop in best effort manner.
	 * The shard may or may not exist and the connection could have died.
	 */
	ExecuteOptionalRemoteCommand(
		connection,
		dropShardQuery->data,
		NULL /* pgResult */);
}
