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

#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_planner.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_protocol.h"
#include "storage/lmgr.h"


/* local function forward declarations */
static LOCKMODE IntToLockMode(int mode);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(lock_shard_metadata);
PG_FUNCTION_INFO_V1(lock_shard_resources);


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
 * LockRelationDistributionMetadata returns after getting a the lock used for a
 * relation's distribution metadata, blocking if required. Only ExclusiveLock
 * and ShareLock modes are supported. Any locks acquired using this method are
 * released at transaction end.
 */
void
LockRelationDistributionMetadata(Oid relationId, LOCKMODE lockMode)
{
	Assert(lockMode == ExclusiveLock || lockMode == ShareLock);

	(void) LockRelationOid(relationId, lockMode);
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
 * LockMetadataSnapshot acquires a lock needed to serialize changes to pg_dist_node
 * and all other metadata changes. Operations that modify pg_dist_node should acquire
 * AccessExclusiveLock. All other metadata changes should acquire AccessShareLock. Any locks
 * acquired using this method are released at transaction end.
 */
void
LockMetadataSnapshot(LOCKMODE lockMode)
{
	Assert(lockMode == AccessExclusiveLock || lockMode == AccessShareLock);

	(void) LockRelationOid(DistNodeRelationId(), lockMode);
}
