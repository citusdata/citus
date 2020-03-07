/*-------------------------------------------------------------------------
 *
 * resource_lock.h
 *	  Locking Infrastructure for Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef RESOURCE_LOCK_H
#define RESOURCE_LOCK_H

#include "postgres.h" /* IWYU pragma: keep */
#include "c.h"

#include "distributed/worker_transaction.h"
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
	ADV_LOCKTAG_CLASS_CITUS_JOB = 6,
	ADV_LOCKTAG_CLASS_CITUS_REBALANCE_COLOCATION = 7,
	ADV_LOCKTAG_CLASS_CITUS_COLOCATED_SHARDS_METADATA = 8,
} AdvisoryLocktagClass;


/* reuse advisory lock, but with different, unused field 4 (4)*/
#define SET_LOCKTAG_SHARD_METADATA_RESOURCE(tag, db, shardid) \
	SET_LOCKTAG_ADVISORY(tag, \
						 db, \
						 (uint32) ((shardid) >> 32), \
						 (uint32) (shardid), \
						 ADV_LOCKTAG_CLASS_CITUS_SHARD_METADATA)

#define SET_LOCKTAG_COLOCATED_SHARDS_METADATA_RESOURCE(tag, db, colocationId, \
													   shardIntervalIndex) \
	SET_LOCKTAG_ADVISORY(tag, \
						 db, \
						 (uint32) shardIntervalIndex, \
						 (uint32) colocationId, \
						 ADV_LOCKTAG_CLASS_CITUS_COLOCATED_SHARDS_METADATA)

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

/* reuse advisory lock, but with different, unused field 4 (7)
 * Also it has the database hardcoded to MyDatabaseId, to ensure the locks
 * are local to each database */
#define SET_LOCKTAG_REBALANCE_COLOCATION(tag, colocationOrTableId) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((colocationOrTableId) >> 32), \
						 (uint32) (colocationOrTableId), \
						 ADV_LOCKTAG_CLASS_CITUS_REBALANCE_COLOCATION)


/* Lock shard/relation metadata for safe modifications */
extern void LockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode);
extern bool TryLockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode);
extern void LockShardListMetadataOnWorkers(LOCKMODE lockmode, List *shardIntervalList);
extern void BlockWritesToShardList(List *shardList);

/* Lock shard/relation metadata of the referenced reference table if exists */
extern void LockReferencedReferenceShardDistributionMetadata(uint64 shardId,
															 LOCKMODE lock);

/* Lock shard data, for DML commands or remote fetches */
extern void LockShardResource(uint64 shardId, LOCKMODE lockmode);
extern void UnlockShardResource(uint64 shardId, LOCKMODE lockmode);

/* Lock a job schema or partition task directory */
extern void LockJobResource(uint64 jobId, LOCKMODE lockmode);
extern void UnlockJobResource(uint64 jobId, LOCKMODE lockmode);

/* Lock a co-location group */
extern void LockColocationId(int colocationId, LOCKMODE lockMode);
extern void UnlockColocationId(int colocationId, LOCKMODE lockMode);

/* Lock multiple shards for safe modification */
extern void LockShardListMetadata(List *shardIntervalList, LOCKMODE lockMode);
extern void LockShardsInPlacementListMetadata(List *shardPlacementList,
											  LOCKMODE lockMode);
extern void SerializeNonCommutativeWrites(List *shardIntervalList, LOCKMODE lockMode);
extern void LockRelationShardResources(List *relationShardList, LOCKMODE lockMode);
extern List * GetSortedReferenceShardIntervals(List *relationList);

/* Lock parent table's colocated shard resource */
extern void LockParentShardResourceIfPartition(uint64 shardId, LOCKMODE lockMode);

/* Lock mode translation between text and enum */
extern LOCKMODE LockModeTextToLockMode(const char *lockModeName);
extern const char * LockModeToLockModeText(LOCKMODE lockMode);

#endif /* RESOURCE_LOCK_H */
