/*-------------------------------------------------------------------------
 *
 * master_create_shards.c
 *
 * This file contains functions to distribute a table by creating shards for it
 * across a set of worker nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_create_worker_shards);


/*
 * master_create_worker_shards is a user facing function to create worker shards
 * for the given relation in round robin order.
 */
Datum
master_create_worker_shards(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	int32 shardCount = PG_GETARG_INT32(1);
	int32 replicationFactor = PG_GETARG_INT32(2);
	ObjectAddress tableAddress = { 0 };

	Oid distributedTableId = ResolveRelationId(tableNameText, false);

	/* do not add any data */
	bool useExclusiveConnections = false;

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	/*
	 * distributed tables might have dependencies on different objects, since we create
	 * shards for a distributed table via multiple sessions these objects will be created
	 * via their own connection and committed immediately so they become visible to all
	 * sessions creating shards.
	 */
	ObjectAddressSet(tableAddress, RelationRelationId, distributedTableId);
	EnsureDependenciesExistOnAllNodes(&tableAddress);

	EnsureReferenceTablesExistOnAllNodes();

	CreateShardsWithRoundRobinPolicy(distributedTableId, shardCount, replicationFactor,
									 useExclusiveConnections);

	PG_RETURN_VOID();
}


/*
 * CreateShardsWithRoundRobinPolicy creates empty shards for the given table
 * based on the specified number of initial shards. The function first updates
 * metadata on the coordinator node to make this shard (and its placements)
 * visible. Note that the function assumes the table is hash partitioned and
 * calculates the min/max hash token ranges for each shard, giving them an equal
 * split of the hash space. Finally, function creates empty shard placements on
 * worker nodes.
 */
void
CreateShardsWithRoundRobinPolicy(Oid distributedTableId, int32 shardCount,
								 int32 replicationFactor, bool useExclusiveConnections)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);
	bool colocatedShard = false;
	List *insertedShardPlacements = NIL;

	/* make sure table is hash partitioned */
	CheckHashPartitionedTable(distributedTableId);

	/*
	 * In contrast to append/range partitioned tables it makes more sense to
	 * require ownership privileges - shards for hash-partitioned tables are
	 * only created once, not continually during ingest as for the other
	 * partitioning types.
	 */
	EnsureTableOwner(distributedTableId);

	/* we plan to add shards: get an exclusive lock on relation oid */
	LockRelationOid(distributedTableId, ExclusiveLock);

	/* validate that shards haven't already been created for this table */
	List *existingShardList = LoadShardList(distributedTableId);
	if (existingShardList != NIL)
	{
		char *tableName = get_rel_name(distributedTableId);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   tableName)));
	}

	/* make sure that at least one shard is specified */
	if (shardCount <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shard_count must be positive")));
	}

	/* make sure that at least one replica is specified */
	if (replicationFactor <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor must be positive")));
	}

	/* make sure that RF=1 if the table is streaming replicated */
	if (cacheEntry->replicationModel == REPLICATION_MODEL_STREAMING &&
		replicationFactor > 1)
	{
		char *relationName = get_rel_name(cacheEntry->relationId);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("using replication factor %d with the streaming "
							   "replication model is not supported",
							   replicationFactor),
						errdetail("The table %s is marked as streaming replicated and "
								  "the shard replication factor of streaming replicated "
								  "tables must be 1.", relationName),
						errhint("Use replication factor 1.")));
	}

	/* calculate the split of the hash space */
	uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;

	/* don't allow concurrent node list changes that require an exclusive lock */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/* load and sort the worker node list for deterministic placement */
	List *workerNodeList = DistributedTablePlacementNodeList(NoLock);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	int32 workerNodeCount = list_length(workerNodeList);
	if (replicationFactor > workerNodeCount)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor (%d) exceeds number of worker nodes "
							   "(%d)", replicationFactor, workerNodeCount),
						errhint("Add more worker nodes or try again with a lower "
								"replication factor.")));
	}

	/* if we have enough nodes, add an extra placement attempt for backup */
	uint32 placementAttemptCount = (uint32) replicationFactor;
	if (workerNodeCount > replicationFactor)
	{
		placementAttemptCount++;
	}

	/* set shard storage type according to relation type */
	char shardStorageType = ShardStorageType(distributedTableId);

	for (int64 shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		uint32 roundRobinNodeIndex = shardIndex % workerNodeCount;

		/* initialize the hash token space for this shard */
		int32 shardMinHashToken = INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);
		uint64 shardId = GetNextShardId();

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT32_MAX;
		}

		/* insert the shard metadata row along with its min/max values */
		text *minHashTokenText = IntegerToText(shardMinHashToken);
		text *maxHashTokenText = IntegerToText(shardMaxHashToken);

		InsertShardRow(distributedTableId, shardId, shardStorageType,
					   minHashTokenText, maxHashTokenText);

		List *currentInsertedShardPlacements = InsertShardPlacementRows(
			distributedTableId,
			shardId,
			workerNodeList,
			roundRobinNodeIndex,
			replicationFactor);
		insertedShardPlacements = list_concat(insertedShardPlacements,
											  currentInsertedShardPlacements);
	}

	CreateShardsOnWorkers(distributedTableId, insertedShardPlacements,
						  useExclusiveConnections, colocatedShard);
}


/*
 * CreateColocatedShards creates shards for the target relation colocated with
 * the source relation.
 */
void
CreateColocatedShards(Oid targetRelationId, Oid sourceRelationId, bool
					  useExclusiveConnections)
{
	bool colocatedShard = true;
	List *insertedShardPlacements = NIL;

	/* make sure that tables are hash partitioned */
	CheckHashPartitionedTable(targetRelationId);
	CheckHashPartitionedTable(sourceRelationId);

	/*
	 * In contrast to append/range partitioned tables it makes more sense to
	 * require ownership privileges - shards for hash-partitioned tables are
	 * only created once, not continually during ingest as for the other
	 * partitioning types.
	 */
	EnsureTableOwner(targetRelationId);

	/* we plan to add shards: get an exclusive lock on target relation oid */
	LockRelationOid(targetRelationId, ExclusiveLock);

	/* we don't want source table to get dropped before we colocate with it */
	LockRelationOid(sourceRelationId, AccessShareLock);

	/* prevent placement changes of the source relation until we colocate with them */
	List *sourceShardIntervalList = LoadShardIntervalList(sourceRelationId);
	LockShardListMetadata(sourceShardIntervalList, ShareLock);

	/* validate that shards haven't already been created for this table */
	List *existingShardList = LoadShardList(targetRelationId);
	if (existingShardList != NIL)
	{
		char *targetRelationName = get_rel_name(targetRelationId);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   targetRelationName)));
	}

	char targetShardStorageType = ShardStorageType(targetRelationId);

	ShardInterval *sourceShardInterval = NULL;
	foreach_ptr(sourceShardInterval, sourceShardIntervalList)
	{
		uint64 sourceShardId = sourceShardInterval->shardId;
		uint64 newShardId = GetNextShardId();

		int32 shardMinValue = DatumGetInt32(sourceShardInterval->minValue);
		int32 shardMaxValue = DatumGetInt32(sourceShardInterval->maxValue);
		text *shardMinValueText = IntegerToText(shardMinValue);
		text *shardMaxValueText = IntegerToText(shardMaxValue);
		List *sourceShardPlacementList = ShardPlacementList(sourceShardId);

		InsertShardRow(targetRelationId, newShardId, targetShardStorageType,
					   shardMinValueText, shardMaxValueText);

		ShardPlacement *sourcePlacement = NULL;
		foreach_ptr(sourcePlacement, sourceShardPlacementList)
		{
			if (sourcePlacement->shardState == SHARD_STATE_TO_DELETE)
			{
				continue;
			}

			int32 groupId = sourcePlacement->groupId;
			const ShardState shardState = SHARD_STATE_ACTIVE;
			const uint64 shardSize = 0;

			/*
			 * Optimistically add shard placement row the pg_dist_shard_placement, in case
			 * of any error it will be roll-backed.
			 */
			uint64 shardPlacementId = InsertShardPlacementRow(newShardId,
															  INVALID_PLACEMENT_ID,
															  shardState, shardSize,
															  groupId);

			ShardPlacement *shardPlacement = LoadShardPlacement(newShardId,
																shardPlacementId);
			insertedShardPlacements = lappend(insertedShardPlacements, shardPlacement);
		}
	}

	CreateShardsOnWorkers(targetRelationId, insertedShardPlacements,
						  useExclusiveConnections, colocatedShard);
}


/*
 * CreateSingleShardTableWithoutDistKey creates a single shard for the given
 * distributedTableId. The created shard does not have min/max values. Also,
 * the shard is replicated to the all active nodes in the cluster if we are
 * creating a reference table.
 */
void
CreateSingleShardTableWithoutDistKey(Oid distributedTableId, char distributionMethod)
{
	Assert(CitusTableWithoutDistributionKey(distributionMethod));

	/*
	 * In contrast to append/range partitioned tables it makes more sense to
	 * require ownership privileges - shards for reference tables are
	 * only created once, not continually during ingest as for the other
	 * partitioning types such as append and range.
	 */
	EnsureTableOwner(distributedTableId);

	/* we plan to add shards: get an exclusive lock on relation oid */
	LockRelationOid(distributedTableId, ExclusiveLock);

	/* set shard storage type according to relation type */
	char shardStorageType = ShardStorageType(distributedTableId);

	/* validate that shards haven't already been created for this table */
	List *existingShardList = LoadShardList(distributedTableId);
	if (existingShardList != NIL)
	{
		char *tableName = get_rel_name(distributedTableId);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   tableName)));
	}

	List *nodeList = NIL;
	int replicationFactor = 0;

	if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		/*
		 * load and sort the worker node list for deterministic placements
		 * create_reference_table has already acquired pg_dist_node lock
		 */
		nodeList = ReferenceTablePlacementNodeList(ShareLock);
		nodeList = SortList(nodeList, CompareWorkerNodes);

		replicationFactor = ReferenceTableReplicationFactor();
	}
	else if (distributionMethod == CITUS_LOCAL_TABLE)
	{
		nodeList = CitusLocalTablePlacementNodeList();

		/* would always have a single placement */
		replicationFactor = 1;
	}

	/* get the next shard id */
	uint64 shardId = GetNextShardId();

	text *shardMinValue = NULL;
	text *shardMaxValue = NULL;

	InsertShardRow(distributedTableId, shardId, shardStorageType, shardMinValue,
				   shardMaxValue);

	int workerStartIndex = 0;
	List *insertedShardPlacements = InsertShardPlacementRows(distributedTableId, shardId,
															 nodeList, workerStartIndex,
															 replicationFactor);

	bool useExclusiveConnection = false;
	bool colocatedShard = false;

	CreateShardsOnWorkers(distributedTableId, insertedShardPlacements,
						  useExclusiveConnection, colocatedShard);
}


/*
 * CheckHashPartitionedTable looks up the partition information for the given
 * tableId and checks if the table is hash partitioned. If not, the function
 * throws an error.
 */
void
CheckHashPartitionedTable(Oid distributedTableId)
{
	char partitionType = PartitionMethod(distributedTableId);
	if (partitionType != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported table partition type: %c", partitionType)));
	}
}


/* Helper function to convert an integer value to a text type */
text *
IntegerToText(int32 value)
{
	StringInfo valueString = makeStringInfo();
	appendStringInfo(valueString, "%d", value);

	text *valueText = cstring_to_text(valueString->data);

	return valueText;
}
