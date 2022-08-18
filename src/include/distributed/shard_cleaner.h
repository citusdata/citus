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

/* ----------------
 *		compiler constants for pg_dist_cleanup_record
 * ----------------
 */
#define Natts_pg_dist_cleanup_record 6
#define Anum_pg_dist_cleanup_record_record_id 1
#define Anum_pg_dist_cleanup_record_operation_id 2
#define Anum_pg_dist_cleanup_record_object_type 3
#define Anum_pg_dist_cleanup_record_object_name 4
#define Anum_pg_dist_cleanup_record_node_group_id 5
#define Anum_pg_dist_cleanup_record_is_success 6

#define INVALID_OPERATION_ID 0

/*
 * CleanupObjectType represents an object type that can be used
 * in cleanup records.
 */
typedef enum CleanupObjectType
{
	CLEANUP_SHARD_PLACEMENT = 1
} CleanupObjectType;

typedef uint64 CleanupRecordId;
typedef uint64 OperationId;

extern OperationId CurrentOperationId;


/* GUC to configure deferred shard deletion */
extern int DeferShardDeleteInterval;
extern bool DeferShardDeleteOnMove;
extern double DesiredPercentFreeAfterMove;
extern bool CheckAvailableSpaceBeforeMove;

extern int TryDropOrphanedShards(bool waitForLocks);
extern int DropOrphanedShards(bool waitForLocks);
extern void DropOrphanedShardsInSeparateTransaction(void);

extern OperationId StartOperationRequiringCleanup(void);
extern void InsertCleanupRecordInCurrentTransaction(CleanupObjectType objectType,
													char *objectName, int nodeGroupId);
extern void InsertCleanupRecordInSubtransaction(CleanupObjectType objectType,
												char *objectName, int nodeGroupId);
extern void DeleteMyCleanupOnFailureRecords(void);

#endif /*CITUS_SHARD_CLEANER_H */
