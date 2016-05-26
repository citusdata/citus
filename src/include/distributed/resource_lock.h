/*-------------------------------------------------------------------------
 *
 * resource_lock.h
 *	  Locking Infrastructure for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef RESOURCE_LOCK_H
#define RESOURCE_LOCK_H

#include "postgres.h" /* IWYU pragma: keep */
#include "c.h"

#include "nodes/pg_list.h"
#include "storage/lock.h"


/*
 * Postgres' advisory locks use 'field4' to discern between different kind of
 * advisory locks. Only 1 and 2 are used allowing us to define non-conflicting
 * lock methods.
 *
 * In case postgres starts to use additional values, Citus's values
 * will have to be changed. That just requires re-compiling and a restart.
 */
typedef enum AdvisoryLocktagClass
{
	/* values defined in postgres' lockfuncs.c */
	ADV_LOCKTAG_CLASS_INT64 = 1,
	ADV_LOCKTAG_CLASS_INT32 = 2,

	/* Citus lock types */
	ADV_LOCKTAG_CLASS_CITUS_SHARD_METADATA = 4,
	ADV_LOCKTAG_CLASS_CITUS_SHARD = 5,
	ADV_LOCKTAG_CLASS_CITUS_JOB = 6
} AdvisoryLocktagClass;


/* reuse advisory lock, but with different, unused field 4 (4)*/
#define SET_LOCKTAG_SHARD_METADATA_RESOURCE(tag, db, shardid) \
	SET_LOCKTAG_ADVISORY(tag, \
						 db, \
						 (uint32) ((shardid) >> 32), \
						 (uint32) (shardid), \
						 ADV_LOCKTAG_CLASS_CITUS_SHARD_METADATA)

/* reuse advisory lock, but with different, unused field 4 (5)*/
#define SET_LOCKTAG_SHARD_RESOURCE(tag, db, shardid) \
	SET_LOCKTAG_ADVISORY(tag, \
						 db, \
						 (uint32) ((shardid) >> 32), \
						 (uint32) (shardid), \
						 ADV_LOCKTAG_CLASS_CITUS_SHARD)

/* reuse advisory lock, but with different, unused field 4 (6) */
#define SET_LOCKTAG_JOB_RESOURCE(tag, db, jobid) \
	SET_LOCKTAG_ADVISORY(tag, \
						 db, \
						 (uint32) ((jobid) >> 32), \
						 (uint32) (jobid), \
						 ADV_LOCKTAG_CLASS_CITUS_JOB)


/* Lock shard/relation metadata for safe modifications */
extern void LockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode);
extern void LockRelationDistributionMetadata(Oid relationId, LOCKMODE lockMode);

/* Lock shard data, for DML commands or remote fetches */
extern void LockShardResource(uint64 shardId, LOCKMODE lockmode);
extern void UnlockShardResource(uint64 shardId, LOCKMODE lockmode);

/* Lock a job schema or partition task directory */
extern void LockJobResource(uint64 jobId, LOCKMODE lockmode);
extern void UnlockJobResource(uint64 jobId, LOCKMODE lockmode);

extern void LockShards(List *shardIntervalList, LOCKMODE lockMode);

#endif /* RESOURCE_LOCK_H */
