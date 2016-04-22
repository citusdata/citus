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
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "storage/lmgr.h"


/*
 * LockShardDistributionMetadata returns after grabbing a lock for distribution
 * metadata related to the specified shard, blocking if required. ExclusiveLock
 * and ShareLock modes are supported. Any locks acquired using this method are
 * released at transaction end.
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
 * LockShards takes shared locks on the metadata and the data of all shards in
 * shardIntervalList. This prevents concurrent placement changes and concurrent
 * DML statements that require an exclusive lock.
 */
void
LockShards(List *shardIntervalList, LOCKMODE lockMode)
{
	ListCell *shardIntervalCell = NULL;

	/* lock shards in order of shard id to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->shardId;

		/* prevent concurrent changes to number of placements */
		LockShardDistributionMetadata(shardId, lockMode);

		/* prevent concurrent update/delete statements */
		LockShardResource(shardId, lockMode);
	}
}
