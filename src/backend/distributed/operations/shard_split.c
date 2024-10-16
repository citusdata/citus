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

#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "postmaster/postmaster.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/adaptive_executor.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/hash_helpers.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_split.h"
#include "distributed/shard_transfer.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/shared_library_init.h"
#include "distributed/utils/array_type.h"
#include "distributed/utils/distribution_column_map.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"

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
static bool CheckIfRelationWithSameNameExists(ShardInterval *shardInterval,
											  WorkerNode *workerNode);
static void ErrorIfModificationAndSplitInTheSameTransaction(SplitOperation
															splitOperation);
static void CreateSplitShardsForShardGroup(List *shardGroupSplitIntervalListList,
										   List *workersForPlacementList);
static void CreateDummyShardsForShardGroup(HTAB *mapOfPlacementToDummyShardList,
										   List *sourceColocatedShardIntervalList,
										   List *shardGroupSplitIntervalListList,
										   WorkerNode *sourceWorkerNode,
										   List *workersForPlacementList);
static HTAB * CreateWorkerForPlacementSet(List *workersForPlacementList);
static void CreateAuxiliaryStructuresForShardGroup(List *shardGroupSplitIntervalListList,
												   List *workersForPlacementList,
												   bool includeReplicaIdentity);
static void CreateReplicaIdentitiesForDummyShards(HTAB *mapOfPlacementToDummyShardList);
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
static void CreatePartitioningHierarchyForBlockingSplit(
	List *shardGroupSplitIntervalListList,
	List *workersForPlacementList);
static void CreateForeignKeyConstraints(List *shardGroupSplitIntervalListList,
										List *workersForPlacementList);
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
												   DistributionColumnMap *
												   distributionColumnOverrides);
static void ExecuteSplitShardReleaseSharedMemory(MultiConnection *sourceConnection);
static void AddDummyShardEntryInMap(HTAB *mapOfPlacementToDummyShardList, uint32
									targetNodeId,
									ShardInterval *shardInterval);
static uint64 GetNextShardIdForSplitChild(void);
static void AcquireNonblockingSplitLock(Oid relationId);
static List * GetWorkerNodesFromWorkerIds(List *nodeIdsForPlacementList);
static void DropShardListMetadata(List *shardIntervalList);

/* Customize error message strings based on operation type */
static const char *const SplitOperationName[] =
{
	[SHARD_SPLIT_API] = "split",
	[ISOLATE_TENANT_TO_NEW_SHARD] = "isolate",
	[CREATE_DISTRIBUTED_TABLE] = "create"
};
static const char *const SplitOperationAPIName[] =
{
	[SHARD_SPLIT_API] = "citus_split_shard_by_split_points",
	[ISOLATE_TENANT_TO_NEW_SHARD] = "isolate_tenant_to_new_shard",
	[CREATE_DISTRIBUTED_TABLE] = "create_distributed_table_concurrently"
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
}


/*
 * Extended checks before we decide to split the shard.
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
	foreach_declared_int(shardSplitPoint, shardSplitPointsList)
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
 * ErrorIfMultipleNonblockingMoveSplitInTheSameTransaction will error if we detect multiple
 * nonblocking shard movements/splits in the same transaction.
 */
void
ErrorIfMultipleNonblockingMoveSplitInTheSameTransaction(void)
{
	if (PlacementMovedUsingLogicalReplicationInTX)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("multiple shard movements/splits via logical "
							   "replication in the same transaction is currently "
							   "not supported")));
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
	int32 nodeId;
	foreach_declared_int(nodeId, nodeIdsForPlacementList)
	{
		uint32 nodeIdValue = (uint32) nodeId;
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
 * 'distributionColumnOverrides': Maps relation IDs to distribution columns.
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
	const char *operationName = SplitOperationAPIName[splitOperation];

	ErrorIfModificationAndSplitInTheSameTransaction(splitOperation);

	ShardInterval *shardIntervalToSplit = LoadShardInterval(shardIdToSplit);
	List *colocatedTableList = ColocatedTableList(shardIntervalToSplit->relationId);

	if (splitMode == AUTO_SPLIT)
	{
		VerifyTablesHaveReplicaIdentity(colocatedTableList);
	}

	/* Acquire global lock to prevent concurrent split on the same colocation group or relation */
	Oid relationId = RelationIdForShard(shardIdToSplit);
	AcquirePlacementColocationLock(relationId, ExclusiveLock, "split");

	/* sort the tables to avoid deadlocks */
	colocatedTableList = SortList(colocatedTableList, CompareOids);
	Oid colocatedTableId = InvalidOid;
	foreach_declared_oid(colocatedTableId, colocatedTableList)
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

	ErrorIfNotAllNodesHaveReferenceTableReplicas(workersForPlacementList);

	List *sourceColocatedShardIntervalList = NIL;
	if (colocatedShardIntervalList == NIL)
	{
		sourceColocatedShardIntervalList = ColocatedShardIntervalList(
			shardIntervalToSplit);
	}
	else
	{
		sourceColocatedShardIntervalList = colocatedShardIntervalList;
	}

	DropOrphanedResourcesInSeparateTransaction();

	/* use the user-specified shard ID as the split workflow ID */
	uint64 splitWorkflowId = shardIntervalToSplit->shardId;

	/* Start operation to prepare for generating cleanup records */
	RegisterOperationNeedingCleanup();

	if (splitMode == BLOCKING_SPLIT)
	{
		ereport(LOG, (errmsg("performing blocking %s ", operationName)));

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
		ereport(LOG, (errmsg("performing non-blocking %s ", operationName)));

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

	/*
	 * Drop temporary objects that were marked as CLEANUP_ALWAYS.
	 */
	FinalizeOperationNeedingCleanupOnSuccess(operationName);
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
	const char *operationName = SplitOperationAPIName[splitOperation];

	BlockWritesToShardList(sourceColocatedShardIntervalList);

	/* First create shard interval metadata for split children */
	List *shardGroupSplitIntervalListList = CreateSplitIntervalsForShardGroup(
		sourceColocatedShardIntervalList,
		shardSplitPointsList);

	/* Only single placement allowed (already validated RelationReplicationFactor = 1) */
	ShardInterval *firstShard = linitial(sourceColocatedShardIntervalList);
	WorkerNode *sourceShardNode =
		ActiveShardPlacementWorkerNode(firstShard->shardId);

	ereport(LOG, (errmsg("creating child shards for %s", operationName)));

	/* Physically create split children. */
	CreateSplitShardsForShardGroup(shardGroupSplitIntervalListList,
								   workersForPlacementList);

	ereport(LOG, (errmsg("performing copy for %s", operationName)));

	/* For Blocking split, copy isn't snapshotted */
	char *snapshotName = NULL;
	ConflictWithIsolationTestingBeforeCopy();
	DoSplitCopy(sourceShardNode, sourceColocatedShardIntervalList,
				shardGroupSplitIntervalListList, workersForPlacementList,
				snapshotName, distributionColumnOverrides);
	ConflictWithIsolationTestingAfterCopy();

	ereport(LOG, (errmsg(
					  "creating auxillary structures (indexes, stats, replicaindentities, triggers) for %s",
					  operationName)));

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
	 * Delete old shards metadata and mark the shards as to be deferred drop.
	 * Have to do that before creating the new shard metadata,
	 * because there's cross-checks preventing inconsistent metadata
	 * (like overlapping shards).
	 */
	ereport(LOG, (errmsg("marking deferred cleanup of source shard(s) for %s",
						 operationName)));

	InsertDeferredDropCleanupRecordsForShards(sourceColocatedShardIntervalList);

	DropShardListMetadata(sourceColocatedShardIntervalList);

	/* Insert new shard and placement metdata */
	InsertSplitChildrenShardMetadata(shardGroupSplitIntervalListList,
									 workersForPlacementList);

	/* create partitioning hierarchy, if any */
	CreatePartitioningHierarchyForBlockingSplit(
		shardGroupSplitIntervalListList,
		workersForPlacementList);

	ereport(LOG, (errmsg("creating foreign key constraints (if any) for %s",
						 operationName)));

	/*
	 * Create foreign keys if exists after the metadata changes happening in
	 * InsertSplitChildrenShardMetadata() because the foreign
	 * key creation depends on the new metadata.
	 */
	CreateForeignKeyConstraints(shardGroupSplitIntervalListList,
								workersForPlacementList);

	CitusInvalidateRelcacheByRelid(DistShardRelationId());
}


/* Check if a relation with given name already exists on the worker node */
static bool
CheckIfRelationWithSameNameExists(ShardInterval *shardInterval, WorkerNode *workerNode)
{
	char *schemaName = get_namespace_name(
		get_rel_namespace(shardInterval->relationId));
	char *shardName = get_rel_name(shardInterval->relationId);
	AppendShardIdToName(&shardName, shardInterval->shardId);

	StringInfo checkShardExistsQuery = makeStringInfo();

	/*
	 * We pass schemaName and shardName without quote_identifier, since
	 * they are used as strings here.
	 */
	appendStringInfo(checkShardExistsQuery,
					 "SELECT EXISTS (SELECT FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename = %s);",
					 quote_literal_cstr(schemaName),
					 quote_literal_cstr(shardName));

	int connectionFlags = 0;
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																workerNode->workerName,
																workerNode->workerPort,
																CitusExtensionOwnerName(),
																get_database_name(
																	MyDatabaseId));

	PGresult *result = NULL;
	int queryResult = ExecuteOptionalRemoteCommand(connection,
												   checkShardExistsQuery->data, &result);
	if (queryResult != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1)
	{
		ReportResultError(connection, result, ERROR);
	}

	char *existsString = PQgetvalue(result, 0, 0);
	bool tableExists = strcmp(existsString, "t") == 0;

	PQclear(result);
	ForgetResults(connection);

	return tableExists;
}


/* Create ShardGroup split children on a list of corresponding workers. */
static void
CreateSplitShardsForShardGroup(List *shardGroupSplitIntervalListList,
							   List *workersForPlacementList)
{
	/*
	 * Iterate over all the shards in the shard group.
	 */
	List *shardIntervalList = NIL;
	foreach_declared_ptr(shardIntervalList, shardGroupSplitIntervalListList)
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
				false, /* includeIdentityDefaults */
				NULL /* auto add columnar options for cstore tables */);
			splitShardCreationCommandList = WorkerApplyShardDDLCommandList(
				splitShardCreationCommandList,
				shardInterval->shardId);

			/* Log resource for cleanup in case of failure only.
			 * Before we log a record, do a best effort check to see if a shard with same name exists.
			 * This is because, it will cause shard creation to fail and we will end up cleaning the
			 * old shard. We don't want that.
			 */
			bool relationExists = CheckIfRelationWithSameNameExists(shardInterval,
																	workerPlacementNode);

			if (relationExists)
			{
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
								errmsg("relation %s already exists on worker %s:%d",
									   ConstructQualifiedShardName(shardInterval),
									   workerPlacementNode->workerName,
									   workerPlacementNode->workerPort)));
			}

			InsertCleanupRecordInSubtransaction(CLEANUP_OBJECT_SHARD_PLACEMENT,
												ConstructQualifiedShardName(
													shardInterval),
												workerPlacementNode->groupId,
												CLEANUP_ON_FAILURE);

			/* Create new split child shard on the specified placement list */
			CreateObjectOnPlacement(splitShardCreationCommandList,
									workerPlacementNode);
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
	foreach_declared_ptr(shardIntervalList, shardGroupSplitIntervalListList)
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
	MultiConnection *connection =
		GetNodeUserDatabaseConnection(OUTSIDE_TRANSACTION,
									  workerPlacementNode->workerName,
									  workerPlacementNode->workerPort,
									  NULL, NULL);
	SendCommandListToWorkerOutsideTransactionWithConnection(connection,
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
	foreach_declared_ptr(shardToSplitInterval, sourceColocatedShardIntervalList)
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
	foreach_declared_ptr(shardInterval, colocatedShardList)
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
	foreach_declared_ptr(shardIntervalList, shardGroupSplitIntervalListList)
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
	foreach_declared_ptr(command, splitOffShardMetadataCommandList)
	{
		SendCommandToWorkersWithMetadata(command);
	}
}


/*
 * CreatePartitioningHierarchy creates the partitioning
 * hierarchy between the shardList, if any.
 */
static void
CreatePartitioningHierarchyForBlockingSplit(List *shardGroupSplitIntervalListList,
											List *workersForPlacementList)
{
	/* Create partition heirarchy between shards */
	List *shardIntervalList = NIL;

	/*
	 * Iterate over all the shards in the shard group.
	 */
	foreach_declared_ptr(shardIntervalList, shardGroupSplitIntervalListList)
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
	foreach_declared_ptr(shardIntervalList, shardGroupSplitIntervalListList)
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
			foreach_declared_ptr(constraintCommand, constraintCommandList)
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
 * DropShardListMetadata drops shard metadata from both the coordinator and
 * mx nodes.
 */
static void
DropShardListMetadata(List *shardIntervalList)
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

		/* delete shard placements */
		List *shardPlacementList = ActiveShardPlacementList(oldShardId);
		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
			DeleteShardPlacementRow(placement->placementId);
		}

		/* delete shard row */
		DeleteShardRow(oldShardId);
	}
}


/*
 * AcquireNonblockingSplitLock does not allow concurrent nonblocking splits, because we share memory and
 * replication slots.
 */
static void
AcquireNonblockingSplitLock(Oid relationId)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;

	SET_LOCKTAG_CITUS_OPERATION(tag, CITUS_NONBLOCKING_SPLIT);

	LockAcquireResult lockAcquired = LockAcquire(&tag, ExclusiveLock, sessionLock,
												 dontWait);
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
	const char *operationName = SplitOperationAPIName[splitOperation];

	ErrorIfMultipleNonblockingMoveSplitInTheSameTransaction();

	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);

	/* First create shard interval metadata for split children */
	List *shardGroupSplitIntervalListList = CreateSplitIntervalsForShardGroup(
		sourceColocatedShardIntervalList,
		shardSplitPointsList);

	ShardInterval *firstShard = linitial(sourceColocatedShardIntervalList);

	/* Acquire global lock to prevent concurrent nonblocking splits */
	AcquireNonblockingSplitLock(firstShard->relationId);

	WorkerNode *sourceShardToCopyNode =
		ActiveShardPlacementWorkerNode(firstShard->shardId);

	/* Create hashmap to group shards for publication-subscription management */
	HTAB *publicationInfoHash = CreateShardSplitInfoMapForPublication(
		sourceColocatedShardIntervalList,
		shardGroupSplitIntervalListList,
		workersForPlacementList);

	int connectionFlags = FORCE_NEW_CONNECTION;
	MultiConnection *sourceConnection = GetNodeUserDatabaseConnection(
		connectionFlags,
		sourceShardToCopyNode->workerName,
		sourceShardToCopyNode->workerPort,
		superUser,
		databaseName);
	ClaimConnectionExclusively(sourceConnection);

	MultiConnection *sourceReplicationConnection =
		GetReplicationConnection(sourceShardToCopyNode->workerName,
								 sourceShardToCopyNode->workerPort);

	/* Non-Blocking shard split workflow starts here */

	ereport(LOG, (errmsg("creating child shards for %s",
						 operationName)));

	/* 1) Physically create split children. */
	CreateSplitShardsForShardGroup(shardGroupSplitIntervalListList,
								   workersForPlacementList);

	/*
	 * 2) Create dummy shards due to PG logical replication constraints.
	 *    Refer to the comment section of 'CreateDummyShardsForShardGroup' for indepth
	 *    information.
	 */
	HTAB *mapOfPlacementToDummyShardList = CreateSimpleHash(NodeAndOwner,
															GroupedShardSplitInfos);
	CreateDummyShardsForShardGroup(
		mapOfPlacementToDummyShardList,
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
	CreateReplicaIdentitiesForDummyShards(mapOfPlacementToDummyShardList);

	ereport(LOG, (errmsg(
					  "creating replication artifacts (publications, replication slots, subscriptions for %s",
					  operationName)));

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

	/*
	 * We have to create the primary key (or any other replica identity)
	 * before the update/delete operations that are queued will be
	 * replicated. Because if the replica identity does not exist on the
	 * target, the replication would fail.
	 *
	 * So the latest possible moment we could do this is right after the
	 * initial data COPY, but before enabling the susbcriptions. It might
	 * seem like a good idea to it after the initial data COPY, since
	 * it's generally the rule that it's cheaper to build an index at once
	 * than to create it incrementally. This general rule, is why we create
	 * all the regular indexes as late during the move as possible.
	 *
	 * But as it turns out in practice it's not as clear cut, and we saw a
	 * speed degradation in the time it takes to move shards when doing the
	 * replica identity creation after the initial COPY. So, instead we
	 * keep it before the COPY.
	 */
	CreateReplicaIdentities(logicalRepTargetList);

	ereport(LOG, (errmsg("performing copy for %s", operationName)));

	/* 8) Do snapshotted Copy */
	DoSplitCopy(sourceShardToCopyNode, sourceColocatedShardIntervalList,
				shardGroupSplitIntervalListList, workersForPlacementList,
				snapshot, distributionColumnOverrides);

	ereport(LOG, (errmsg("replicating changes for %s", operationName)));

	/*
	 * 9) Logically replicate all the changes and do most of the table DDL,
	 * like index and foreign key creation.
	 */
	CompleteNonBlockingShardTransfer(sourceColocatedShardIntervalList,
									 sourceConnection,
									 publicationInfoHash,
									 logicalRepTargetList,
									 groupedLogicalRepTargetsHash,
									 SHARD_SPLIT);

	/*
	 * 10) Delete old shards metadata and mark the shards as to be deferred drop.
	 * Have to do that before creating the new shard metadata,
	 * because there's cross-checks preventing inconsistent metadata
	 * (like overlapping shards).
	 */
	ereport(LOG, (errmsg("marking deferred cleanup of source shard(s) for %s",
						 operationName)));

	InsertDeferredDropCleanupRecordsForShards(sourceColocatedShardIntervalList);

	DropShardListMetadata(sourceColocatedShardIntervalList);

	/*
	 * 11) In case of create_distributed_table_concurrently, which converts
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

	/* 12) Insert new shard and placement metdata */
	InsertSplitChildrenShardMetadata(shardGroupSplitIntervalListList,
									 workersForPlacementList);

	/* 13) create partitioning hierarchy, if any, this needs to be done
	 * after the metadata is correct, because it fails for some
	 * uninvestigated reason otherwise.
	 */
	CreatePartitioningHierarchy(logicalRepTargetList);

	ereport(LOG, (errmsg("creating foreign key constraints (if any) for %s",
						 operationName)));

	/*
	 * 14) Create foreign keys if exists after the metadata changes happening in
	 * InsertSplitChildrenShardMetadata() because the foreign
	 * key creation depends on the new metadata.
	 */
	CreateUncheckedForeignKeyConstraints(logicalRepTargetList);

	/*
	 * 15) Release shared memory allocated by worker_split_shard_replication_setup udf
	 * at source node.
	 */
	ExecuteSplitShardReleaseSharedMemory(sourceConnection);

	/* 16) Close source connection */
	CloseConnection(sourceConnection);

	/* 17) Close all subscriber connections */
	CloseGroupedLogicalRepTargetsConnections(groupedLogicalRepTargetsHash);

	/* 18) Close connection of template replication slot */
	CloseConnection(sourceReplicationConnection);
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
CreateDummyShardsForShardGroup(HTAB *mapOfPlacementToDummyShardList,
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
		foreach_declared_ptr(shardInterval, sourceColocatedShardIntervalList)
		{
			/* Populate list of commands necessary to create shard interval on destination */
			List *splitShardCreationCommandList = GetPreLoadTableCreationCommands(
				shardInterval->relationId,
				false, /* includeSequenceDefaults */
				false, /* includeIdentityDefaults */
				NULL /* auto add columnar options for cstore tables */);
			splitShardCreationCommandList = WorkerApplyShardDDLCommandList(
				splitShardCreationCommandList,
				shardInterval->shardId);

			/* Log resource for cleanup in case of failure only.
			 * Before we log a record, do a best effort check to see if a shard with same name exists.
			 * This is because, it will cause shard creation to fail and we will end up cleaning the
			 * old shard. We don't want that.
			 */
			bool relationExists = CheckIfRelationWithSameNameExists(shardInterval,
																	workerPlacementNode);

			if (relationExists)
			{
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
								errmsg("relation %s already exists on worker %s:%d",
									   ConstructQualifiedShardName(shardInterval),
									   workerPlacementNode->workerName,
									   workerPlacementNode->workerPort)));
			}

			/* Log shard in pg_dist_cleanup. Given dummy shards are transient resources,
			 * we want to cleanup irrespective of operation success or failure.
			 */
			InsertCleanupRecordInSubtransaction(CLEANUP_OBJECT_SHARD_PLACEMENT,
												ConstructQualifiedShardName(
													shardInterval),
												workerPlacementNode->groupId,
												CLEANUP_ALWAYS);

			/* Create dummy source shard on the specified placement list */
			CreateObjectOnPlacement(splitShardCreationCommandList,
									workerPlacementNode);

			/* Add dummy source shard entry created for placement node in map */
			AddDummyShardEntryInMap(mapOfPlacementToDummyShardList,
									workerPlacementNode->nodeId,
									shardInterval);
		}
	}

	/*
	 * Statisfy Constraint 2: Create dummy target shards from shard group on source node.
	 * If the target shard was created on source node as placement, skip it (See Note 2 from function description).
	 */
	List *shardIntervalList = NULL;
	foreach_declared_ptr(shardIntervalList, shardGroupSplitIntervalListList)
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
				false, /* includeIdentityDefaults */
				NULL /* auto add columnar options for cstore tables */);
			splitShardCreationCommandList = WorkerApplyShardDDLCommandList(
				splitShardCreationCommandList,
				shardInterval->shardId);

			/* Log resource for cleanup in case of failure only.
			 * Before we log a record, do a best effort check to see if a shard with same name exists.
			 * This is because, it will cause shard creation to fail and we will end up cleaning the
			 * old shard. We don't want that.
			 */
			bool relationExists = CheckIfRelationWithSameNameExists(shardInterval,
																	sourceWorkerNode);

			if (relationExists)
			{
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
								errmsg("relation %s already exists on worker %s:%d",
									   ConstructQualifiedShardName(shardInterval),
									   sourceWorkerNode->workerName,
									   sourceWorkerNode->workerPort)));
			}

			/* Log shard in pg_dist_cleanup. Given dummy shards are transient resources,
			 * we want to cleanup irrespective of operation success or failure.
			 */
			InsertCleanupRecordInSubtransaction(CLEANUP_OBJECT_SHARD_PLACEMENT,
												ConstructQualifiedShardName(
													shardInterval),
												sourceWorkerNode->groupId,
												CLEANUP_ALWAYS);

			/* Create dummy split child shard on source worker node */
			CreateObjectOnPlacement(splitShardCreationCommandList, sourceWorkerNode);

			/* Add dummy split child shard entry created on source node */
			AddDummyShardEntryInMap(mapOfPlacementToDummyShardList,
									sourceWorkerNode->nodeId,
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

	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	HTAB *workerForPlacementSet = hash_create("worker placement set", 32, &info,
											  hashFlags);

	WorkerNode *workerForPlacement = NULL;
	foreach_declared_ptr(workerForPlacement, workersForPlacementList)
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
ExecuteSplitShardReleaseSharedMemory(MultiConnection *sourceConnection)
{
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
 *  ], CurrentOperationId);
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
					 "SELECT * FROM pg_catalog.worker_split_shard_replication_setup("
					 "ARRAY[%s], %lu);",
					 splitChildrenRows->data,
					 CurrentOperationId);

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
AddDummyShardEntryInMap(HTAB *mapOfPlacementToDummyShardList, uint32 targetNodeId,
						ShardInterval *shardInterval)
{
	NodeAndOwner key;
	key.nodeId = targetNodeId;
	key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

	bool found = false;
	GroupedDummyShards *nodeMappingEntry =
		(GroupedDummyShards *) hash_search(mapOfPlacementToDummyShardList, &key,
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

	MultiConnection *connection = GetConnectionForLocalQueriesOutsideTransaction(
		CitusExtensionOwnerName());
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
	PQclear(result);
	ForgetResults(connection);

	return shardId;
}
