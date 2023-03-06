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
#include "tcop/utility.h"


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
	ADV_LOCKTAG_CLASS_CITUS_OPERATIONS = 9,
	ADV_LOCKTAG_CLASS_CITUS_CLEANUP_OPERATION_ID = 10,
	ADV_LOCKTAG_CLASS_CITUS_LOGICAL_REPLICATION = 12,
	ADV_LOCKTAG_CLASS_CITUS_REBALANCE_PLACEMENT_COLOCATION = 13,
	ADV_LOCKTAG_CLASS_CITUS_BACKGROUND_TASK = 14
} AdvisoryLocktagClass;

/* CitusOperations has constants for citus operations */
typedef enum CitusOperations
{
	CITUS_TRANSACTION_RECOVERY = 0,
	CITUS_NONBLOCKING_SPLIT = 1,
	CITUS_CREATE_DISTRIBUTED_TABLE_CONCURRENTLY = 2,
	CITUS_CREATE_COLOCATION_DEFAULT = 3,
	CITUS_BACKGROUND_TASK_MONITOR = 4
} CitusOperations;

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

/* reuse advisory lock, but with different, unused field 4 (7)
 * Also it has the database hardcoded to MyDatabaseId, to ensure the locks
 * are local to each database */
#define SET_LOCKTAG_REBALANCE_COLOCATION(tag, colocationOrTableId) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((colocationOrTableId) >> 32), \
						 (uint32) (colocationOrTableId), \
						 ADV_LOCKTAG_CLASS_CITUS_REBALANCE_COLOCATION)

/* reuse advisory lock, but with different, unused field 4 (13)
 * Also it has the database hardcoded to MyDatabaseId, to ensure the locks
 * are local to each database */
#define SET_LOCKTAG_REBALANCE_PLACEMENT_COLOCATION(tag, colocationOrTableId) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((colocationOrTableId) >> 32), \
						 (uint32) (colocationOrTableId), \
						 ADV_LOCKTAG_CLASS_CITUS_REBALANCE_PLACEMENT_COLOCATION)


/* advisory lock for citus operations, also it has the database hardcoded to MyDatabaseId,
 * to ensure the locks are local to each database */
#define SET_LOCKTAG_CITUS_OPERATION(tag, operationId) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) 0, \
						 (uint32) operationId, \
						 ADV_LOCKTAG_CLASS_CITUS_OPERATIONS)

/* reuse advisory lock, but with different, unused field 4 (10)
 * Also it has the database hardcoded to MyDatabaseId, to ensure the locks
 * are local to each database */
#define SET_LOCKTAG_CLEANUP_OPERATION_ID(tag, operationId) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((operationId) >> 32), \
						 (uint32) operationId, \
						 ADV_LOCKTAG_CLASS_CITUS_CLEANUP_OPERATION_ID)

/* reuse advisory lock, but with different, unused field 4 (12)
 * Also it has the database hardcoded to MyDatabaseId, to ensure the locks
 * are local to each database */
#define SET_LOCKTAG_LOGICAL_REPLICATION(tag) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) 0, \
						 (uint32) 0, \
						 ADV_LOCKTAG_CLASS_CITUS_LOGICAL_REPLICATION)

/* reuse advisory lock, but with different, unused field 4 (14)
 * Also it has the database hardcoded to MyDatabaseId, to ensure the locks
 * are local to each database */
#define SET_LOCKTAG_BACKGROUND_TASK(tag, taskId) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((taskId) >> 32), \
						 (uint32) (taskId), \
						 ADV_LOCKTAG_CLASS_CITUS_BACKGROUND_TASK)

/*
 * DistLockConfigs are used to configure the locking behaviour of AcquireDistributedLockOnRelations
 */
enum DistLockConfigs
{
	/*
	 * lock citus tables
	 */
	DIST_LOCK_DEFAULT = 0,

	/*
	 * lock tables that refer to locked citus tables with a foreign key
	 */
	DIST_LOCK_REFERENCING_TABLES = 1,

	/*
	 * throw an error if the lock is not immediately available
	 */
	DIST_LOCK_NOWAIT = 2
};


/* Lock shard/relation metadata for safe modifications */
extern void LockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode);
extern void EnsureShardOwner(uint64 shardId, bool missingOk);
extern void LockShardListMetadataOnWorkers(LOCKMODE lockmode, List *shardIntervalList);
extern void BlockWritesToShardList(List *shardList);

/* Lock shard/relation metadata of the referenced reference table if exists */
extern void LockReferencedReferenceShardDistributionMetadata(uint64 shardId,
															 LOCKMODE lock);

/* Lock shard data, for DML commands or remote fetches */
extern void LockShardResource(uint64 shardId, LOCKMODE lockmode);

/* Lock a co-location group */
extern void LockColocationId(int colocationId, LOCKMODE lockMode);
extern void UnlockColocationId(int colocationId, LOCKMODE lockMode);

/* Lock multiple shards for safe modification */
extern void LockShardListMetadata(List *shardIntervalList, LOCKMODE lockMode);
extern void LockShardListMetadataOnWorkers(LOCKMODE lockmode, List *shardIntervalList);
extern void LockShardsInPlacementListMetadata(List *shardPlacementList,
											  LOCKMODE lockMode);

extern void LockTransactionRecovery(LOCKMODE lockMode);

extern void SerializeNonCommutativeWrites(List *shardIntervalList, LOCKMODE lockMode);
extern void LockRelationShardResources(List *relationShardList, LOCKMODE lockMode);
extern List * GetSortedReferenceShardIntervals(List *relationList);

void AcquireCreateDistributedTableConcurrentlyLock(Oid relationId);

/* Lock parent table's colocated shard resource */
extern void LockParentShardResourceIfPartition(List *shardIntervalList,
											   LOCKMODE lockMode);

/* Lock mode translation between text and enum */
extern LOCKMODE LockModeTextToLockMode(const char *lockModeName);
extern const char * LockModeToLockModeText(LOCKMODE lockMode);
extern void AcquireDistributedLockOnRelations(List *relationList, LOCKMODE lockMode,
											  uint32 configs);
extern void PreprocessLockStatement(LockStmt *stmt, ProcessUtilityContext context);

extern bool EnableAcquiringUnsafeLockFromWorkers;
extern bool SkipAdvisoryLockPermissionChecks;

#endif /* RESOURCE_LOCK_H */
