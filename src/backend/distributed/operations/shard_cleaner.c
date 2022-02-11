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

#include "access/xact.h"
#include "postmaster/postmaster.h"

#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_transaction.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_cleanup_orphaned_shards);
PG_FUNCTION_INFO_V1(isolation_cleanup_orphaned_shards);

static bool TryDropShard(GroupShardPlacement *placement);
static bool TryLockRelationAndPlacementCleanup(Oid relationId, LOCKMODE lockmode);


/*
 * citus_cleanup_orphaned_shards implements a user-facing UDF to delete
 * orphaned shards that are still haning around in the system. These shards are
 * orphaned by previous actions that were not directly able to delete the
 * placements eg. shard moving or dropping of a distributed table while one of
 * the data nodes was not online.
 *
 * This function iterates through placements where shardstate is
 * SHARD_STATE_TO_DELETE (shardstate = 4), drops the corresponding tables from
 * the node and removes the placement information from the catalog.
 *
 * The function takes no arguments and runs cluster wide. It cannot be run in a
 * transaction, because holding the locks it takes for a long time is not good.
 * While the locks are held, it is impossible for the background daemon to
 * cleanup orphaned shards.
 */
Datum
citus_cleanup_orphaned_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	PreventInTransactionBlock(true, "citus_cleanup_orphaned_shards");

	bool waitForLocks = true;
	int droppedShardCount = DropOrphanedShards(waitForLocks);
	if (droppedShardCount > 0)
	{
		ereport(NOTICE, (errmsg("cleaned up %d orphaned shards", droppedShardCount)));
	}

	PG_RETURN_VOID();
}


/*
 * isolation_cleanup_orphaned_shards implements a test UDF that's the same as
 * citus_cleanup_orphaned_shards. The only difference is that this command can
 * be run in transactions, this is to test
 */
Datum
isolation_cleanup_orphaned_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	bool waitForLocks = true;
	int droppedShardCount = DropOrphanedShards(waitForLocks);
	if (droppedShardCount > 0)
	{
		ereport(NOTICE, (errmsg("cleaned up %d orphaned shards", droppedShardCount)));
	}

	PG_RETURN_VOID();
}


/*
 * DropOrphanedShardsInSeparateTransaction cleans up orphaned shards by
 * connecting to localhost. This is done, so that the locks that
 * DropOrphanedShards takes are only held for a short time.
 */
void
DropOrphanedShardsInSeparateTransaction(void)
{
	ExecuteRebalancerCommandInSeparateTransaction("CALL citus_cleanup_orphaned_shards()");
}


/*
 * TryDropOrphanedShards is a wrapper around DropOrphanedShards that catches
 * any errors to make it safe to use in the maintenance daemon.
 *
 * If dropping any of the shards failed this function returns -1, otherwise it
 * returns the number of dropped shards.
 */
int
TryDropOrphanedShards(bool waitForLocks)
{
	int droppedShardCount = 0;
	MemoryContext savedContext = CurrentMemoryContext;
	PG_TRY();
	{
		droppedShardCount = DropOrphanedShards(waitForLocks);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		/* rethrow as WARNING */
		edata->elevel = WARNING;
		ThrowErrorData(edata);
	}
	PG_END_TRY();

	return droppedShardCount;
}


/*
 * DropOrphanedShards removes shards that were marked SHARD_STATE_TO_DELETE before.
 *
 * It does so by trying to take an exclusive lock on the shard and its
 * colocated placements before removing. If the lock cannot be obtained it
 * skips the group and continues with others. The group that has been skipped
 * will be removed at a later time when there are no locks held anymore on
 * those placements.
 *
 * If waitForLocks is false, then if we cannot take a lock on pg_dist_placement
 * we continue without waiting.
 *
 * Before doing any of this it will take an exclusive PlacementCleanup lock.
 * This is to ensure that this function is not being run concurrently.
 * Otherwise really bad race conditions are possible, such as removing all
 * placements of a shard. waitForLocks indicates if this function should
 * wait for this lock or not.
 *
 */
int
DropOrphanedShards(bool waitForLocks)
{
	int removedShardCount = 0;
	ListCell *shardPlacementCell = NULL;

	/*
	 * We should try to take the highest lock that we take
	 * later in this function for pg_dist_placement. We take RowExclusiveLock
	 * in DeleteShardPlacementRow.
	 */
	LOCKMODE lockmode = RowExclusiveLock;

	if (!IsCoordinator())
	{
		return 0;
	}

	if (waitForLocks)
	{
		LockPlacementCleanup();
	}
	else
	{
		Oid distPlacementId = DistPlacementRelationId();
		if (!TryLockRelationAndPlacementCleanup(distPlacementId, lockmode))
		{
			return 0;
		}
	}

	int failedShardDropCount = 0;
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

		if (TryDropShard(placement))
		{
			removedShardCount++;
		}
		else
		{
			failedShardDropCount++;
		}
	}

	if (failedShardDropCount > 0)
	{
		ereport(WARNING, (errmsg("Failed to drop %d orphaned shards out of %d",
								 failedShardDropCount, list_length(shardPlacementList))));
	}

	return removedShardCount;
}


/*
 * TryLockRelationAndPlacementCleanup tries to lock the given relation
 * and the placement cleanup. If it cannot, it returns false.
 *
 */
static bool
TryLockRelationAndPlacementCleanup(Oid relationId, LOCKMODE lockmode)
{
	if (!ConditionalLockRelationOid(relationId, lockmode))
	{
		ereport(DEBUG1, (errmsg(
							 "could not acquire shard lock to cleanup placements")));
		return false;
	}

	if (!TryLockPlacementCleanup())
	{
		ereport(DEBUG1, (errmsg("could not acquire lock to cleanup placements")));
		return false;
	}
	return true;
}


/*
 * TryDropShard tries to drop the given shard placement and returns
 * true on success.
 */
static bool
TryDropShard(GroupShardPlacement *placement)
{
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

	/*
	 * We set a lock_timeout here so that if there are running queries on the
	 * shards we won't get blocked more than 1s and fail.
	 *
	 * The lock timeout also avoids getting stuck in a distributed deadlock, which
	 * can occur because we might be holding pg_dist_placement locks while also
	 * taking locks on the shard placements, and this code interrupts the
	 * distributed deadlock detector.
	 */
	List *dropCommandList = list_make2("SET LOCAL lock_timeout TO '1s'",
									   dropQuery->data);

	/* remove the shard from the node */
	bool success =
		SendOptionalCommandListToWorkerOutsideTransaction(shardPlacement->nodeName,
														  shardPlacement->nodePort,
														  NULL, dropCommandList);
	if (success)
	{
		/* delete the actual placement */
		DeleteShardPlacementRow(placement->placementId);
	}

	return success;
}
