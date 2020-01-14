/*-------------------------------------------------------------------------
 *
 * shard_cleaner.c
 *	  This implements the background process that cleans shards that are
 *	  left around. Shards that are left around are marked as state 4
 *	  (SHARD_STATE_TO_DELETE) in pg_dist_placement.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/shard_cleaner.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_transaction.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_defer_delete_shards);

/*
 * master_defer_delete_shards implements a user-facing UDF to deleter orphaned shards that
 * are still haning around in the system. These shards are orphaned by previous actions
 * that were not directly able to delete the placements eg. shard moving or dropping of a
 * distributed table while one of the data nodes was not online.
 *
 * This function iterates through placements where shardstate is SHARD_STATE_TO_DELETE
 * (shardstate = 4), drops the corresponding tables from the node and removes the
 * placement information from the catalog.
 *
 * The function takes no arguments and runs cluster wide
 */
Datum
master_defer_delete_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	bool waitForCleanupLock = true;
	int droppedShardCount = 0;
	bool droppedAll = DropMarkedShards(waitForCleanupLock, &droppedShardCount);
	if (!droppedAll)
	{
		ereport(WARNING, (errmsg("not all shards could be dropped")));
	}

	PG_RETURN_INT32(droppedShardCount);
}


/*
 * TryDropMarkedShards is a wrapper around DropMarkedShards that catches
 * any errors to make it safe to use in the maintenance daemon.
 *
 * If dropping any of the shards failed this function returns -1, otherwise it
 * returns the number of dropped shards.
 */
int
TryDropMarkedShards(bool waitForCleanupLock)
{
	int droppedShardCount = 0;
	MemoryContext savedContext = CurrentMemoryContext;

	PG_TRY();
	{
		DropMarkedShards(waitForCleanupLock, &droppedShardCount);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		/* rethrow as WARNING */
		edata->elevel = WARNING;
		ThrowErrorData(edata);
		return -1;
	}
	PG_END_TRY();

	return droppedShardCount;
}


/*
 * DropMarkedShards removes shards that were marked SHARD_STATE_TO_DELETE before.
 *
 * It does so by taking an exclusive lock on the shard and its colocated
 * placements before removing. If the lock cannot be obtained it skips the
 * group and continues with others. The group that has been skipped will be
 * removed at a later time when there are no locks held anymore on those
 * placements.
 *
 * Before doing any of this it will take an exclusive PlacementCleanup lock.
 * This is to ensure that this function is not being run concurrently.
 * Otherwise really bad race conditions are possible, such as removing all
 * placements of a shard. waitForCleanupLock indicates if this function should
 * wait for this lock or error out.
 *
 * The function returns true if all shards were successfuly dropped and fills
 * removedShardCount with the number of shards that were dropped.
 */
bool
DropMarkedShards(bool waitForCleanupLock, int *removedShardCount)
{
	if (removedShardCount != NULL)
	{
		*removedShardCount = 0;
	}
	ListCell *shardPlacementCell = NULL;

	if (!IsCoordinator())
	{
		return false;
	}

	if (waitForCleanupLock)
	{
		LockPlacementCleanup();
	}
	else if (!TryLockPlacementCleanup())
	{
		ereport(WARNING, (errmsg("could not acquire lock to cleanup placements")));
		return false;
	}

	List *shardPlacementList = AllShardPlacementsWithShardPlacementState(
		SHARD_STATE_TO_DELETE);
	foreach(shardPlacementCell, shardPlacementList)
	{
		GroupShardPlacement *placement = (GroupShardPlacement *) lfirst(
			shardPlacementCell);

		if (!PrimaryNodeForGroup(placement->groupId, NULL) ||
			!ShardExists(placement->shardId))
		{
			continue;
		}

		ShardPlacement *shardPlacement = LoadShardPlacement(placement->shardId,
															placement->placementId);
		ShardInterval *shardInterval = LoadShardInterval(shardPlacement->shardId);

		ereport(LOG, (errmsg("dropping shard placement " INT64_FORMAT " of shard "
							 INT64_FORMAT " on %s:%d after it was moved away",
							 shardPlacement->placementId, shardPlacement->shardId,
							 shardPlacement->nodeName, shardPlacement->nodePort)));

		/* prepare sql query to execute to drop the shard */
		StringInfo dropQuery = makeStringInfo();
		char *qualifiedTableName = ConstructQualifiedShardName(shardInterval);
		appendStringInfo(dropQuery, DROP_REGULAR_TABLE_COMMAND, qualifiedTableName);

		List *dropCommandList = list_make2("SET LOCAL lock_timeout TO '1s'",
										   dropQuery->data);

		/* remove the shard from the node and the placement information */
		SendCommandListToWorkerInSingleTransaction(shardPlacement->nodeName,
												   shardPlacement->nodePort,
												   NULL, dropCommandList);

		DeleteShardPlacementRow(placement->placementId);

		if (removedShardCount != NULL)
		{
			(*removedShardCount)++;
		}
	}

	return true;
}
