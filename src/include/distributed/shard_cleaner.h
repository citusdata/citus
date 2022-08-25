/*-------------------------------------------------------------------------
 *
 * shard_cleaner.h
 *	  Type and function declarations used in background shard cleaning
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_SHARD_CLEANER_H
#define CITUS_SHARD_CLEANER_H

/* GUC to configure deferred shard deletion */
extern int DeferShardDeleteInterval;
extern bool DeferShardDeleteOnMove;
extern double DesiredPercentFreeAfterMove;
extern bool CheckAvailableSpaceBeforeMove;

extern bool DeferShardDeleteOnSplit;
extern int NextOperationId;
extern int NextCleanupRecordId;

extern int TryDropOrphanedShards(bool waitForLocks);
extern void DropOrphanedShardsInSeparateTransaction(void);

/* Members for cleanup infrastructure */
typedef uint64 OperationId;
extern OperationId CurrentOperationId;

/*
 * CleanupResource represents the Resource type in cleanup records.
 */
typedef enum CleanupObject
{
	CLEANUP_INVALID = 0,
	CLEANUP_SHARD_PLACEMENT = 1
} CleanupObject;

typedef enum CleanupPolicy
{
	/*
	 * Resources that are transient and always need clean up after the operation is completed.
	 * (Example: Dummy Shards for Non-Blocking splits)
	 */
	CLEANUP_ALWAYS = 0,

	/*
	 * Resources that are cleanup only on failure.
     * (Example: Split Children for Blocking/Non-Blocking splits)
	 */
	CLEANUP_ON_FAILURE = 1,

	/*
	 * Resources that are to be deferred cleanup only on success.
     * (Example: Parent child being split for Blocking/Non-Blocking splits)
	 */
	CLEANUP_DEFERRED_ON_SUCCESS = 2,
} CleanupPolicy;

#define INVALID_OPERATION_ID 0
#define INVALID_CLEANUP_RECORD_ID 0

/* APIs for cleanup infrastructure */
extern OperationId StartNewOperationNeedingCleanup(void);
extern void InsertCleanupRecordInCurrentTransaction(CleanupObject objectType,
													char *objectName,
													int nodeGroupId,
													CleanupPolicy policy);
extern void InsertCleanupRecordInSubtransaction(CleanupObject objectType,
												char *objectName,
												int nodeGroupId,
												CleanupPolicy policy);
extern void CompleteNewOperationNeedingCleanup(bool isSuccess);

#endif /*CITUS_SHARD_CLEANER_H */
