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

#define MAX_BG_TASK_EXECUTORS 1000

/* GUC to configure deferred shard deletion */
extern int DeferShardDeleteInterval;
extern int BackgroundTaskQueueCheckInterval;
extern int MaxBackgroundTaskExecutors;
extern double DesiredPercentFreeAfterMove;
extern bool CheckAvailableSpaceBeforeMove;

extern int NextOperationId;
extern int NextCleanupRecordId;

extern int TryDropOrphanedResources(void);
extern void DropOrphanedResourcesInSeparateTransaction(void);
extern void ErrorIfCleanupRecordForShardExists(char *shardName);

/* Members for cleanup infrastructure */
typedef uint64 OperationId;
extern OperationId CurrentOperationId;

/*
 * CleanupResource represents the Resource type in cleanup records.
 */
typedef enum CleanupObject
{
	CLEANUP_OBJECT_INVALID = 0,
	CLEANUP_OBJECT_SHARD_PLACEMENT = 1,
	CLEANUP_OBJECT_SUBSCRIPTION = 2,
	CLEANUP_OBJECT_REPLICATION_SLOT = 3,
	CLEANUP_OBJECT_PUBLICATION = 4,
	CLEANUP_OBJECT_USER = 5
} CleanupObject;

/*
 * CleanupPolicy represents the policy type for cleanup records.
 */
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
	 * Resources that need 'deferred' clean up only on success .
	 * (Example: Parent child being split for Blocking/Non-Blocking splits)
	 */
	CLEANUP_DEFERRED_ON_SUCCESS = 2,
} CleanupPolicy;

/* Global Constants */
#define INVALID_OPERATION_ID 0
#define INVALID_CLEANUP_RECORD_ID 0

/* APIs for cleanup infrastructure */

/*
 * RegisterOperationNeedingCleanup is be called by an operation to register
 * for cleanup.
 */
extern OperationId RegisterOperationNeedingCleanup(void);

/*
 * InsertCleanupRecordInCurrentTransaction inserts a new pg_dist_cleanup entry
 * as part of the current transaction.
 *
 * This is primarily useful for deferred cleanup (CLEANUP_DEFERRED_ON_SUCCESS)
 * scenarios, since the records would roll back in case of failure.
 */
extern void InsertCleanupRecordInCurrentTransaction(CleanupObject objectType,
													char *objectName,
													int nodeGroupId,
													CleanupPolicy policy);

/*
 * InsertCleanupRecordInSeparateTransaction inserts a new pg_dist_cleanup entry
 * in a separate transaction to ensure the record persists after rollback.
 *
 * This is used in scenarios where we need to cleanup resources on operation
 * completion (CLEANUP_ALWAYS) or on failure (CLEANUP_ON_FAILURE).
 */
extern void InsertCleanupRecordInSubtransaction(CleanupObject objectType,
												char *objectName,
												int nodeGroupId,
												CleanupPolicy policy);

/*
 * FinalizeOperationNeedingCleanupOnSuccess is be called by an operation to signal
 * completion on success. This will trigger cleanup of appropriate resources
 * and cleanup records.
 */
extern void FinalizeOperationNeedingCleanupOnSuccess(const char *operationName);

#endif /*CITUS_SHARD_CLEANER_H */
