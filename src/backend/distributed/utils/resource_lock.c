/*-------------------------------------------------------------------------
 *
 * resource_lock.c
 *	  Locking Infrastructure for Citus.
 *
 * To avoid introducing a new type of locktag - that then could not be
 * displayed by core functionality - we reuse advisory locks. If we'd just
 * reused them directly we'd run into danger conflicting with user-defined
 * advisory locks, but luckily advisory locks only two values for 'field4' in
 * the locktag.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "miscadmin.h"

#include "distributed/colocation_utils.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "storage/lmgr.h"


/* local function forward declarations */
static LOCKMODE IntToLockMode(int mode);


/* exports for SQL callable functions */
CITUS_FUNCTION(lock_shard_metadata);
CITUS_FUNCTION(lock_shard_resources);


/*
 * lock_shard_metadata allows the shard distribution metadata to be locked
 * remotely to block concurrent writes from workers in MX tables.
 *
 * This function does not sort the array to avoid deadlock, callers
 * must ensure a consistent order.
 */
Datum
lock_shard_metadata(PG_FUNCTION_ARGS)
{
	LOCKMODE lockMode = IntToLockMode(PG_GETARG_INT32(0));
	ArrayType *shardIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);
	Datum *shardIdArrayDatum = NULL;
	int shardIdCount = 0;
	int shardIdIndex = 0;

	CheckCitusVersion(ERROR);

	if (ARR_NDIM(shardIdArrayObject) == 0)
	{
		ereport(ERROR, (errmsg("no locks specified")));
	}

	/* we don't want random users to block writes */
	EnsureSuperUser();

	shardIdCount = ArrayObjectCount(shardIdArrayObject);
	shardIdArrayDatum = DeconstructArrayObject(shardIdArrayObject);

	for (shardIdIndex = 0; shardIdIndex < shardIdCount; shardIdIndex++)
	{
		int64 shardId = DatumGetInt64(shardIdArrayDatum[shardIdIndex]);

		LockShardDistributionMetadata(shardId, lockMode);
	}

	PG_RETURN_VOID();
}


/*
 * lock_shard_resources allows shard resources  to be locked
 * remotely to serialise non-commutative writes on shards.
 *
 * This function does not sort the array to avoid deadlock, callers
 * must ensure a consistent order.
 */
Datum
lock_shard_resources(PG_FUNCTION_ARGS)
{
	LOCKMODE lockMode = IntToLockMode(PG_GETARG_INT32(0));
	ArrayType *shardIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);
	Datum *shardIdArrayDatum = NULL;
	int shardIdCount = 0;
	int shardIdIndex = 0;

	CheckCitusVersion(ERROR);

	if (ARR_NDIM(shardIdArrayObject) == 0)
	{
		ereport(ERROR, (errmsg("no locks specified")));
	}

	/* we don't want random users to block writes */
	EnsureSuperUser();

	shardIdCount = ArrayObjectCount(shardIdArrayObject);
	shardIdArrayDatum = DeconstructArrayObject(shardIdArrayObject);

	for (shardIdIndex = 0; shardIdIndex < shardIdCount; shardIdIndex++)
	{
		int64 shardId = DatumGetInt64(shardIdArrayDatum[shardIdIndex]);

		LockShardResource(shardId, lockMode);
	}

	PG_RETURN_VOID();
}


/*
 * IntToLockMode verifies whether the specified integer is an accepted lock mode
 * and returns it as a LOCKMODE enum.
 */
static LOCKMODE
IntToLockMode(int mode)
{
	if (mode == ExclusiveLock)
	{
		return ExclusiveLock;
	}
	else if (mode == ShareLock)
	{
		return ShareLock;
	}
	else if (mode == AccessShareLock)
	{
		return AccessShareLock;
	}
	else
	{
		elog(ERROR, "unsupported lockmode %d", mode);
	}
}


/*
 * LockShardDistributionMetadata returns after grabbing a lock for distribution
 * metadata related to the specified shard, blocking if required. Any locks
 * acquired using this method are released at transaction end.
 */
void
LockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	SET_LOCKTAG_SHARD_METADATA_RESOURCE(tag, MyDatabaseId, shardId);

	(void) LockAcquire(&tag, lockMode, sessionLock, dontWait);
}


/*
 * TryLockShardDistributionMetadata tries to grab a lock for distribution
 * metadata related to the specified shard, returning false if the lock
 * is currently taken. Any locks acquired using this method are released
 * at transaction end.
 */
bool
TryLockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;
	bool lockAcquired = false;

	SET_LOCKTAG_SHARD_METADATA_RESOURCE(tag, MyDatabaseId, shardId);

	lockAcquired = LockAcquire(&tag, lockMode, sessionLock, dontWait);

	return lockAcquired;
}


/*
 * LockShardResource acquires a lock needed to modify data on a remote shard.
 * This task may be assigned to multiple backends at the same time, so the lock
 * manages any concurrency issues associated with shard file fetching and DML
 * command execution.
 */
void
LockShardResource(uint64 shardId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	AssertArg(shardId != INVALID_SHARD_ID);

	SET_LOCKTAG_SHARD_RESOURCE(tag, MyDatabaseId, shardId);

	(void) LockAcquire(&tag, lockmode, sessionLock, dontWait);
}


/* Releases the lock associated with the relay file fetching/DML task. */
void
UnlockShardResource(uint64 shardId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;

	SET_LOCKTAG_SHARD_RESOURCE(tag, MyDatabaseId, shardId);

	LockRelease(&tag, lockmode, sessionLock);
}


/*
 * LockJobResource acquires a lock for creating resources associated with the
 * given jobId. This resource is typically a job schema (namespace), and less
 * commonly a partition task directory.
 */
void
LockJobResource(uint64 jobId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	SET_LOCKTAG_JOB_RESOURCE(tag, MyDatabaseId, jobId);

	(void) LockAcquire(&tag, lockmode, sessionLock, dontWait);
}


/* Releases the lock for resources associated with the given job id. */
void
UnlockJobResource(uint64 jobId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;

	SET_LOCKTAG_JOB_RESOURCE(tag, MyDatabaseId, jobId);

	LockRelease(&tag, lockmode, sessionLock);
}


/*
 * LockShardListMetadata takes shared locks on the metadata of all shards in
 * shardIntervalList to prevents concurrent placement changes.
 */
void
LockShardListMetadata(List *shardIntervalList, LOCKMODE lockMode)
{
	ListCell *shardIntervalCell = NULL;

	/* lock shards in order of shard id to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->shardId;

		LockShardDistributionMetadata(shardId, lockMode);
	}
}


/*
 * LockShardListResources takes locks on all shards in shardIntervalList to
 * prevent concurrent DML statements on those shards.
 */
void
LockShardListResources(List *shardIntervalList, LOCKMODE lockMode)
{
	ListCell *shardIntervalCell = NULL;

	/* lock shards in order of shard id to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->shardId;

		LockShardResource(shardId, lockMode);
	}
}


/*
 * LockRelationShardResources takes locks on all shards in a list of RelationShards
 * to prevent concurrent DML statements on those shards.
 */
void
LockRelationShardResources(List *relationShardList, LOCKMODE lockMode)
{
	ListCell *relationShardCell = NULL;

	/* lock shards in a consistent order to prevent deadlock */
	relationShardList = SortList(relationShardList, CompareRelationShards);

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);
		uint64 shardId = relationShard->shardId;

		if (shardId != INVALID_SHARD_ID)
		{
			LockShardResource(shardId, lockMode);
		}
	}
}


/*
 * LockParentShardResourceIfPartition checks whether the given shard belongs
 * to a partition. If it does, LockParentShardResourceIfPartition acquires a
 * shard resource lock on the colocated shard of the parent table.
 */
void
LockParentShardResourceIfPartition(uint64 shardId, LOCKMODE lockMode)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid relationId = shardInterval->relationId;

	if (PartitionTable(relationId))
	{
		int shardIndex = ShardIndex(shardInterval);
		Oid parentRelationId = PartitionParentOid(relationId);
		uint64 parentShardId = ColocatedShardIdInRelation(parentRelationId, shardIndex);

		LockShardResource(parentShardId, lockMode);
	}
}


/*
 * LockPartitionsInRelationList iterates over given list and acquires locks on
 * partitions of each partitioned table. It does nothing for non-partitioned tables.
 */
void
LockPartitionsInRelationList(List *relationIdList, LOCKMODE lockmode)
{
	ListCell *relationIdCell = NULL;

	foreach(relationIdCell, relationIdList)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		if (PartitionedTable(relationId))
		{
			LockPartitionRelations(relationId, lockmode);
		}
	}
}


/*
 * LockPartitionRelations acquires relation lock on all partitions of given
 * partitioned relation. This function expects that given relation is a
 * partitioned relation.
 */
void
LockPartitionRelations(Oid relationId, LOCKMODE lockMode)
{
	/*
	 * PartitionList function generates partition list in the same order
	 * as PostgreSQL. Therefore we do not need to sort it before acquiring
	 * locks.
	 */
	List *partitionList = PartitionList(relationId);
	ListCell *partitionCell = NULL;

	foreach(partitionCell, partitionList)
	{
		Oid partitionRelationId = lfirst_oid(partitionCell);
		LockRelationOid(partitionRelationId, lockMode);
	}
}
