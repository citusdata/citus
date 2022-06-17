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
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "distributed/utils/array_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
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

/*
 * These will be public function when citus-enterprise is merged
 */
static void ErrorIfCannotSplitShard(SplitOperation splitOperation, ShardInterval *sourceShard);
static void InsertSplitOffShardMetadata(List *splitOffShardList,
										List *sourcePlacementList);
static void DropShardList(List *shardIntervalList);
static void CreateForeignConstraints(List *splitOffShardList, List *sourcePlacementList);
static void ExecuteCommandListOnPlacements(List *commandList, List *placementList);

/* Function declarations */
static void ErrorIfCannotSplitShardExtended(SplitOperation splitOperation,
											ShardInterval *shardIntervalToSplit,
											List *shardSplitPointsList,
											List *nodeIdsForPlacementList);
static void CreateSplitShardsForShardGroup(uint32_t sourceShardNodeId,
											List *sourceColocatedShardIntervalList,
											List *shardGroupSplitIntervalListList,
											List *workersForPlacementList,
											List **splitOffShardList);
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
static void DoSplitCopy(uint32_t sourceShardNodeId, List *sourceColocatedShardIntervalList, List *shardGroupSplitIntervalListList, List *workersForPlacementList);
static StringInfo CreateSplitCopyCommand(ShardInterval *sourceShardSplitInterval, List* splitChildrenShardIntervalList, List* workersForPlacementList);

/* Customize error message strings based on operation type */
static const char *const SplitOperationName[] =
{
	[SHARD_SPLIT_API] = "split"
};
static const char *const SplitTargetName[] =
{
	[SHARD_SPLIT_API] = "shard"
};
static const char *const SplitOperationType[] =
{
	[BLOCKING_SPLIT] = "blocking"
};

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

		/*
		 * At the moment, we do not support copying a shard if that shard's
		 * relation is in a colocation group with a partitioned table or partition.
		 */
		if (PartitionedTable(colocatedTableId))
		{
			char *sourceRelationName = get_rel_name(relationId);
			char *colocatedRelationName = get_rel_name(colocatedTableId);

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot %s of '%s', because it "
								   "is a partitioned table",
								   SplitOperationName[splitOperation],
								   colocatedRelationName),
							errdetail("In colocation group of '%s', a partitioned "
									  "relation exists: '%s'. Citus does not support "
									  "%s of partitioned tables.",
									  sourceRelationName,
									  colocatedRelationName,
									  SplitOperationName[splitOperation])));
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
	if (nodeIdsCount != splitPointsCount + 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg(
					 "Number of worker node ids should be one greater split points. "
					 "NodeId count is '%d' and SplitPoint count is '%d'.",
					 nodeIdsCount,
					 splitPointsCount)));
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

		/* All Split points should lie within the shard interval range. */
		int splitPointShardIndex = FindShardIntervalIndex(shardSplitPoint,
														  cachedTableEntry);
		if (shardIntervalToSplit->shardIndex != splitPointShardIndex)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg(
						 "Split point %d is outside the min/max range for shard id %lu.",
						 shardSplitPointValue,
						 shardIntervalToSplit->shardId)));
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

		/*
		 * Given our split points inclusive, you cannot specify the max value in a range as a split point.
		 * Example: Shard 81060002 range is from (0,1073741823). '1073741823' as split point is invalid.
		 * '1073741822' is correct and will split shard to: (0, 1073741822) and (1073741823, 1073741823).
		 */
		if (maxValue == shardSplitPointValue)
		{
			int32 validSplitPoint = shardIntervalToSplit->maxValue - 1;
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg(
						 "Invalid split point %d, as split points should be inclusive. Please use %d instead.",
						 maxValue,
						 validSplitPoint)));
		}

		lastShardSplitPoint = (NullableDatum) {
			shardSplitPoint, false
		};
	}
}


/*
 * SplitShard API to split a given shard (or shard group) in blocking / non-blocking fashion
 * based on specified split points to a set of destination nodes.
 * 'splitOperation'             : Customer operation that triggered split.
 * 'shardInterval'              : Source shard interval to be split.
 * 'shardSplitPointsList'		: Split Points list for the source 'shardInterval'.
 * 'workersForPlacementList'	: Placement list corresponding to split children.
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

	/*
	 * TODO(niupre): When all consumers (Example : ISOLATE_TENANT_TO_NEW_SHARD) directly call 'SplitShard' API,
	 * these two methods will be merged.
	 */
	ErrorIfCannotSplitShard(SHARD_SPLIT_API, shardIntervalToSplit);
	ErrorIfCannotSplitShardExtended(
		SHARD_SPLIT_API,
		shardIntervalToSplit,
		shardSplitPointsList,
		nodeIdsForPlacementList);

	List *workersForPlacementList = NULL;
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
		/* we only support blocking shard split in this code path for now. */
		ereport(ERROR, (errmsg("Invalid split mode %s.", SplitOperationType[splitMode])));
	}
}


/*
 * SplitShard API to split a given shard (or shard group) in blocking fashion
 * based on specified split points to a set of destination nodes.
 * 'splitOperation'             : Customer operation that triggered split.
 * 'shardInterval'              : Source shard interval to be split.
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

	// Only single placement allowed (already validated by caller)
	List *sourcePlacementList = ActiveShardPlacementList(shardIntervalToSplit->shardId);
	Assert(sourcePlacementList->length == 1);
	WorkerNode* sourceShardToCopyNode = (WorkerNode *) linitial(sourcePlacementList);

	/* Physically create split children and perform split copy */
	List *splitOffShardList = NULL;
	CreateSplitShardsForShardGroup(
		sourceShardToCopyNode->nodeId,
		sourceColocatedShardIntervalList,
		shardGroupSplitIntervalListList,
		workersForPlacementList,
		&splitOffShardList);

	/*
	 * Drop old shards and delete related metadata. Have to do that before
	 * creating the new shard metadata, because there's cross-checks
	 * preventing inconsistent metadata (like overlapping shards).
	 */
	DropShardList(sourceColocatedShardIntervalList);

	/* insert new metadata */
	InsertSplitOffShardMetadata(splitOffShardList, sourcePlacementList);

	/*
	 * Create foreign keys if exists after the metadata changes happening in
	 * DropShardList() and InsertSplitOffShardMetadata() because the foreign
	 * key creation depends on the new metadata.
	 */
	CreateForeignConstraints(splitOffShardList, sourcePlacementList);

	CitusInvalidateRelcacheByRelid(DistShardRelationId());
}

/* Create ShardGroup split children on a list of corresponding workers. */
static void
CreateSplitShardsForShardGroup(uint32_t sourceShardNodeId,
							   List *sourceColocatedShardIntervalList,
							   List *shardGroupSplitIntervalListList,
							   List *workersForPlacementList,
							   List **splitOffShardList)
{
	/* Iterate on shard intervals for shard group */
	List *shardIntervalList = NULL;
	foreach_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *shardInterval = NULL;
		WorkerNode *workerPlacementNode = NULL;
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

			(*splitOffShardList) = lappend(*splitOffShardList, shardInterval);
		}
	}

	/* Perform Split Copy */
	DoSplitCopy(sourceShardNodeId, sourceColocatedShardIntervalList, shardGroupSplitIntervalListList, workersForPlacementList);

	// TODO(niupre) : Use Adaptive execution for creating multiple indexes parallely.
}

static void
DoSplitCopy(uint32_t sourceShardNodeId, List *sourceColocatedShardIntervalList, List *shardGroupSplitIntervalListList, List *workersForPlacementList)
{
	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitShardIntervalList = NULL;

	WorkerNode *workerNodeSource = NULL;
	WorkerNode *workerNodeDestination = NULL;
	int taskId = 0;
	List *splitCopyTaskList = NULL;
	forthree_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
		splitShardIntervalList, shardGroupSplitIntervalListList,
		workerNodeDestination, workersForPlacementList)
	{
		StringInfo splitCopyUdfCommand = CreateSplitCopyCommand(sourceShardIntervalToCopy, splitShardIntervalList, workersForPlacementList);

		Task *task = CreateBasicTask(
			sourceShardIntervalToCopy->shardId, /* jobId */
			taskId,
			READ_TASK,
			splitCopyUdfCommand->data);

		ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
		SetPlacementNodeMetadata(taskPlacement, workerNodeSource);

		task->taskPlacementList = list_make1(taskPlacement);

		splitCopyTaskList = lappend(splitCopyTaskList, task);
		taskId++;
	}

	// TODO(niupre) : Pass appropriate MaxParallelShards value from GUC.
	ExecuteTaskListOutsideTransaction(ROW_MODIFY_NONE, splitCopyTaskList, 1, NULL /* jobIdList */);
}

static StringInfo
CreateSplitCopyCommand(ShardInterval *sourceShardSplitInterval, List* splitChildrenShardIntervalList, List* workersForPlacementList)
{
	StringInfo splitCopyInfoArray = makeStringInfo();
	appendStringInfo(splitCopyInfoArray, "ARRAY[");

	ShardInterval *splitChildShardInterval = NULL;
	bool addComma = false;
	WorkerNode *destinationWorkerNode = NULL;
	forboth_ptr(splitChildShardInterval, splitChildrenShardIntervalList, destinationWorkerNode, workersForPlacementList)
	{
		if(addComma)
		{
			appendStringInfo(splitCopyInfoArray, ",");
		}

		StringInfo splitCopyInfoRow = makeStringInfo();
		appendStringInfo(splitCopyInfoRow, "ROW(%lu, %lu, %lu, %u)::citus.split_copy_info",
			splitChildShardInterval->shardId,
			splitChildShardInterval->minValue,
			splitChildShardInterval->maxValue,
			destinationWorkerNode->nodeId);
		appendStringInfo(splitCopyInfoArray, "%s", splitCopyInfoRow->data);

		addComma = true;
	}
	appendStringInfo(splitCopyInfoArray, "]");

	StringInfo splitCopyUdf = makeStringInfo();
	appendStringInfo(splitCopyUdf, "SELECT worker_split_copy(%lu, %s);",
		sourceShardSplitInterval->shardId,
		splitCopyInfoArray->data);

	return splitCopyUdf;
}

/*
 * Create an object (shard/index) on a worker node.
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
 */
static List *
CreateSplitIntervalsForShardGroup(List *sourceColocatedShardIntervalList,
								  List *splitPointsForShard)
{
	List *shardGroupSplitIntervalListList = NULL;

	ShardInterval *shardToSplitInterval = NULL;
	foreach_ptr(shardToSplitInterval, sourceColocatedShardIntervalList)
	{
		List *shardSplitIntervalList = NULL;
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
 * InsertSplitOffShardMetadata inserts new shard and shard placement data into
 * catolog tables both the coordinator and mx nodes.
 */
static void
InsertSplitOffShardMetadata(List *splitOffShardList, List *sourcePlacementList)
{
	List *syncedShardList = NIL;
	ListCell *shardCell = NULL;
	ListCell *commandCell = NULL;

	/* add new metadata */
	foreach(shardCell, splitOffShardList)
	{
		ShardInterval *splitOffShard = (ShardInterval *) lfirst(shardCell);
		Oid relationId = splitOffShard->relationId;
		uint64 shardId = splitOffShard->shardId;
		char storageType = splitOffShard->storageType;
		ListCell *shardPlacementCell = NULL;

		int32 shardMinValue = DatumGetInt32(splitOffShard->minValue);
		int32 shardMaxValue = DatumGetInt32(splitOffShard->maxValue);
		text *shardMinValueText = IntegerToText(shardMinValue);
		text *shardMaxValueText = IntegerToText(shardMaxValue);

		InsertShardRow(relationId, shardId, storageType, shardMinValueText,
					   shardMaxValueText);

		/* split off shard placement metadata */
		foreach(shardPlacementCell, sourcePlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
			uint64 shardSize = 0;

			InsertShardPlacementRow(shardId, INVALID_PLACEMENT_ID, SHARD_STATE_ACTIVE,
									shardSize, placement->groupId);
		}

		if (ShouldSyncTableMetadata(relationId))
		{
			syncedShardList = lappend(syncedShardList, splitOffShard);
		}
	}

	/* send commands to synced nodes one by one */
	List *splitOffShardMetadataCommandList = ShardListInsertCommand(syncedShardList);
	foreach(commandCell, splitOffShardMetadataCommandList)
	{
		char *command = (char *) lfirst(commandCell);
		SendCommandToWorkersWithMetadata(command);
	}
}

/*
 * DropShardList drops shards and their metadata from both the coordinator and
 * mx nodes.
 */
static void
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

			/* send the commands one by one */
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
 * CreateForeignConstraints creates the foreign constraints on the newly
 * created shards via the tenant isolation.
 *
 * The function treats foreign keys to reference tables and foreign keys to
 * co-located distributed tables differently. The former one needs to be
 * executed over a single connection to prevent self-deadlocks. The latter
 * one can be executed in parallel if there are multiple replicas.
 */
static void
CreateForeignConstraints(List *splitOffShardList, List *sourcePlacementList)
{
	ListCell *splitOffShardCell = NULL;

	List *colocatedShardForeignConstraintCommandList = NIL;
	List *referenceTableForeignConstraintList = NIL;

	foreach(splitOffShardCell, splitOffShardList)
	{
		ShardInterval *splitOffShard = (ShardInterval *) lfirst(splitOffShardCell);

		List *currentColocatedForeignKeyList = NIL;
		List *currentReferenceForeignKeyList = NIL;

		CopyShardForeignConstraintCommandListGrouped(splitOffShard,
													 &currentColocatedForeignKeyList,
													 &currentReferenceForeignKeyList);

		colocatedShardForeignConstraintCommandList =
			list_concat(colocatedShardForeignConstraintCommandList,
						currentColocatedForeignKeyList);
		referenceTableForeignConstraintList =
			list_concat(referenceTableForeignConstraintList,
						currentReferenceForeignKeyList);
	}

	/*
	 * We can use parallel connections to while creating co-located foreign keys
	 * if the source placement .
	 * However, foreign keys to reference tables need to be created using a single
	 * connection per worker to prevent self-deadlocks.
	 */
	if (colocatedShardForeignConstraintCommandList != NIL)
	{
		ExecuteCommandListOnPlacements(colocatedShardForeignConstraintCommandList,
									   sourcePlacementList);
	}

	if (referenceTableForeignConstraintList != NIL)
	{
		ListCell *shardPlacementCell = NULL;
		foreach(shardPlacementCell, sourcePlacementList)
		{
			ShardPlacement *shardPlacement =
				(ShardPlacement *) lfirst(shardPlacementCell);

			char *nodeName = shardPlacement->nodeName;
			int32 nodePort = shardPlacement->nodePort;

			/*
			 * We're using the connections that we've used for dropping the
			 * source placements within the same coordinated transaction.
			 */
			char *command = NULL;
			foreach_ptr(command, referenceTableForeignConstraintList)
			{
				SendCommandToWorker(nodeName, nodePort, command);
			}
		}
	}
}

/*
 * ExecuteCommandListOnPlacements runs the given command list on the nodes of
 * the given shard placement list. First, it creates connections. Then it sends
 * commands one by one. For every command, first it send the command to all
 * connections and then checks the results. This helps to run long running
 * commands in parallel. Finally, it sends commit messages to all connections
 * and close them.
 */
static void
ExecuteCommandListOnPlacements(List *commandList, List *placementList)
{
	List *workerConnectionList = NIL;
	ListCell *workerConnectionCell = NULL;
	ListCell *shardPlacementCell = NULL;
	ListCell *commandCell = NULL;

	/* create connections and start transactions */
	foreach(shardPlacementCell, placementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		char *nodeName = shardPlacement->nodeName;
		int32 nodePort = shardPlacement->nodePort;

		int connectionFlags = FORCE_NEW_CONNECTION;
		char *currentUser = CurrentUserName();

		/* create a new connection */
		MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																		  nodeName,
																		  nodePort,
																		  currentUser,
																		  NULL);

		/* mark connection as critical ans start transaction */
		MarkRemoteTransactionCritical(workerConnection);
		RemoteTransactionBegin(workerConnection);

		/* add connection to the list */
		workerConnectionList = lappend(workerConnectionList, workerConnection);
	}

	/* send and check results for every command one by one */
	foreach(commandCell, commandList)
	{
		char *command = lfirst(commandCell);

		/* first only send the command */
		foreach(workerConnectionCell, workerConnectionList)
		{
			MultiConnection *workerConnection =
				(MultiConnection *) lfirst(workerConnectionCell);

			int querySent = SendRemoteCommand(workerConnection, command);
			if (querySent == 0)
			{
				ReportConnectionError(workerConnection, ERROR);
			}
		}

		/* then check the result separately to run long running commands in parallel */
		foreach(workerConnectionCell, workerConnectionList)
		{
			MultiConnection *workerConnection =
				(MultiConnection *) lfirst(workerConnectionCell);
			bool raiseInterrupts = true;

			PGresult *result = GetRemoteCommandResult(workerConnection, raiseInterrupts);
			if (!IsResponseOK(result))
			{
				ReportResultError(workerConnection, result, ERROR);
			}

			PQclear(result);
			ForgetResults(workerConnection);
		}
	}

	/* finally commit each transaction and close connections */
	foreach(workerConnectionCell, workerConnectionList)
	{
		MultiConnection *workerConnection =
			(MultiConnection *) lfirst(workerConnectionCell);

		RemoteTransactionCommit(workerConnection);
		CloseConnection(workerConnection);
	}
}
