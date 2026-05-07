/*-------------------------------------------------------------------------
 *
 * cluster_changes_block.h
 *
 * Declarations for blocking distributed writes during LTR backup.
 *
 * The cluster changes block feature allows external backup tools to temporarily
 * block distributed 2PC writes across the Citus cluster, take a
 * consistent snapshot on every node, and then release the block.
 *
 * Architecture:
 *   - A dedicated background worker holds the ExclusiveLocks on
 *     pg_dist_transaction / pg_dist_partition / pg_dist_node on
 *     coordinator + all worker nodes.
 *   - Shared memory (ClusterChangesBlockControlData) communicates state
 *     between the UDF caller, the background worker, and the
 *     status/unblock UDFs.
 *   - The background worker auto-releases if timeout expires,
 *     the requestor backend exits, or a worker connection fails.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CLUSTER_CHANGES_BLOCK_H
#define CLUSTER_CHANGES_BLOCK_H

#include "postgres.h"

#include "storage/lwlock.h"


/*
 * ClusterChangesBlockState enumerates the lifecycle states of the cluster changes block.
 */
typedef enum ClusterChangesBlockState
{
	CLUSTER_CHANGES_BLOCK_INACTIVE = 0,  /* no block active */
	CLUSTER_CHANGES_BLOCK_STARTING,      /* background worker is starting up */
	CLUSTER_CHANGES_BLOCK_ACTIVE,        /* locks acquired on all nodes */
	CLUSTER_CHANGES_BLOCK_RELEASING,     /* unblock requested, releasing */
	CLUSTER_CHANGES_BLOCK_ERROR          /* worker hit an error */
} ClusterChangesBlockState;


/*
 * ClusterChangesBlockControlData is the shared memory structure that coordinates
 * the cluster changes block lifecycle between UDF callers and the background worker.
 *
 * Protected by the embedded LWLock (lock).
 */
typedef struct ClusterChangesBlockControlData
{
	/* LWLock tranche info */
	int trancheId;
	char lockTrancheName[NAMEDATALEN];
	LWLock lock;

	/* current state */
	ClusterChangesBlockState state;

	/* PIDs for lifecycle management */
	pid_t workerPid;           /* PID of the background worker holding locks */
	pid_t requestorPid;        /* PID of the backend that requested the block */

	/* timing */
	TimestampTz blockStartTime;  /* when locks were acquired */
	int timeoutMs;               /* auto-release timeout in milliseconds */

	/* cluster info */
	int nodeCount;             /* number of worker nodes locked */

	/* error reporting */
	char errorMessage[256];    /* error message if state == CLUSTER_CHANGES_BLOCK_ERROR */

	/* signal: set to true by unblock UDF to tell worker to release */
	bool releaseRequested;
} ClusterChangesBlockControlData;


/*
 * BLOCK_DISTRIBUTED_WRITES_COMMAND acquires ExclusiveLock on:
 *   1. pg_dist_transaction — blocks 2PC commit decisions
 *   2. pg_dist_partition   — blocks DDL on distributed tables
 *
 * Used by both citus_cluster_changes_block and citus_create_restore_point
 * to quiesce distributed writes on remote metadata nodes.
 *
 * Note: pg_dist_node is only locked locally on the coordinator (node
 * management operations are coordinator-only), so it is intentionally
 * absent from this remote command.
 */
#define BLOCK_DISTRIBUTED_WRITES_COMMAND \
		"LOCK TABLE pg_catalog.pg_dist_transaction IN EXCLUSIVE MODE; " \
		"LOCK TABLE pg_catalog.pg_dist_partition IN EXCLUSIVE MODE"


/* Shared memory sizing and initialization */
extern size_t ClusterChangesBlockShmemSize(void);
extern void ClusterChangesBlockShmemInit(void);

/* Call from _PG_init to chain shmem_startup_hook */
extern void InitializeClusterChangesBlock(void);

/* Background worker entry point */
extern PGDLLEXPORT void CitusClusterChangesBlockWorkerMain(Datum main_arg);

#endif /* CLUSTER_CHANGES_BLOCK_H */
