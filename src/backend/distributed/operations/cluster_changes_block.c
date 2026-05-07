/*-------------------------------------------------------------------------
 *
 * cluster_changes_block.c
 *
 * Implementation of UDFs for blocking distributed writes during LTR backup.
 *
 * This file provides three SQL-callable functions:
 *
 *   citus_cluster_changes_block(timeout_ms int DEFAULT 300000)
 *     -> Spawns a background worker that acquires ExclusiveLock on
 *        pg_dist_transaction, pg_dist_partition, and pg_dist_node on
 *        the coordinator and all worker nodes.  Returns true when
 *        locks are held.
 *
 *   citus_cluster_changes_unblock()
 *     -> Signals the background worker to release all locks and exit.
 *        Returns true on success.
 *
 *   citus_cluster_changes_block_status()
 *     -> Returns a single-row result describing the current block state:
 *        (state text, worker_pid int, requestor_pid int,
 *         block_start_time timestamptz, timeout_ms int, node_count int)
 *
 * Architecture:
 *   The actual lock-holding is done by a dedicated background worker
 *   (CitusClusterChangesBlockWorkerMain).  This ensures locks survive the
 *   caller's session disconnect.  The background worker communicates
 *   its state through shared memory (ClusterChangesBlockControlData).
 *
 *   The worker auto-releases locks when:
 *     - citus_cluster_changes_unblock() sets releaseRequested
 *     - The configured timeout expires
 *     - A worker-node connection fails
 *     - The postmaster dies
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "distributed/background_worker_utils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/cluster_changes_block.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"


/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(citus_cluster_changes_block);
PG_FUNCTION_INFO_V1(citus_cluster_changes_unblock);
PG_FUNCTION_INFO_V1(citus_cluster_changes_block_status);


/* default and maximum timeout for cluster changes block (5 minutes / 30 minutes) */
#define CLUSTER_CHANGES_BLOCK_DEFAULT_TIMEOUT_MS 300000
#define CLUSTER_CHANGES_BLOCK_MAX_TIMEOUT_MS 1800000

/* polling interval while waiting for worker to acquire locks */
#define CLUSTER_CHANGES_BLOCK_POLL_INTERVAL_MS 100

/* interval for worker to check latch / release conditions */
#define CLUSTER_CHANGES_BLOCK_WORKER_CHECK_MS 1000

/* maximum iterations to wait for unblock completion: 300 * 100ms = 30s */
#define CLUSTER_CHANGES_BLOCK_UNBLOCK_MAX_LOOPS 300


/* shared memory pointer */
static ClusterChangesBlockControlData *ClusterChangesBlockControl = NULL;

/* previous shmem_startup_hook */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* SIGTERM flag for worker */
static volatile sig_atomic_t got_sigterm = false;


/* forward declarations */
static List * OpenConnectionsToMetadataWorkersForBlock(LOCKMODE lockMode);
static void BlockDistributedTransactionsLocally(void);
static void BlockDistributedTransactionsOnWorkers(List *connectionList,
												  int timeoutMs,
												  int *nodeCount);
static void cluster_changes_block_worker_sigterm(SIGNAL_ARGS);
static void SetClusterChangesBlockState(ClusterChangesBlockState newState);
static void ResetClusterChangesBlockShmem(ClusterChangesBlockState newState,
										  const char *errorMessage);


/* ----------------------------------------------------------------
 * Shared Memory
 * ---------------------------------------------------------------- */

/*
 * ClusterChangesBlockShmemSize returns the amount of shared memory needed for
 * the cluster changes block control structure.
 */
size_t
ClusterChangesBlockShmemSize(void)
{
	return sizeof(ClusterChangesBlockControlData);
}


/*
 * ClusterChangesBlockShmemInit initializes the shared memory for cluster changes block.
 * Called from citus_shmem_startup_hook or similar.
 */
void
ClusterChangesBlockShmemInit(void)
{
	bool alreadyInitialized = false;

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ClusterChangesBlockControl =
		(ClusterChangesBlockControlData *) ShmemInitStruct("Citus Cluster Changes Block",
														   ClusterChangesBlockShmemSize(),
														   &alreadyInitialized);

	if (!alreadyInitialized)
	{
		ClusterChangesBlockControl->trancheId = LWLockNewTrancheId();
		strlcpy(ClusterChangesBlockControl->lockTrancheName,
				"Citus Cluster Changes Block",
				NAMEDATALEN);
		LWLockRegisterTranche(ClusterChangesBlockControl->trancheId,
							  ClusterChangesBlockControl->lockTrancheName);
		LWLockInitialize(&ClusterChangesBlockControl->lock,
						 ClusterChangesBlockControl->trancheId);

		ClusterChangesBlockControl->state = CLUSTER_CHANGES_BLOCK_INACTIVE;
		ClusterChangesBlockControl->workerPid = 0;
		ClusterChangesBlockControl->requestorPid = 0;
		ClusterChangesBlockControl->blockStartTime = 0;
		ClusterChangesBlockControl->timeoutMs = 0;
		ClusterChangesBlockControl->nodeCount = 0;
		ClusterChangesBlockControl->errorMessage[0] = '\0';
		ClusterChangesBlockControl->releaseRequested = false;
	}

	LWLockRelease(AddinShmemInitLock);
}


/*
 * InitializeClusterChangesBlock installs the shmem_startup_hook to initialize
 * cluster changes block shared memory.  Called from _PG_init.
 */
void
InitializeClusterChangesBlock(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ClusterChangesBlockShmemInit;
}


/* ----------------------------------------------------------------
 * Helper: State management (caller must NOT hold LWLock)
 * ---------------------------------------------------------------- */
static void
SetClusterChangesBlockState(ClusterChangesBlockState newState)
{
	LWLockAcquire(&ClusterChangesBlockControl->lock, LW_EXCLUSIVE);
	ClusterChangesBlockControl->state = newState;
	LWLockRelease(&ClusterChangesBlockControl->lock);
}


/*
 * ResetClusterChangesBlockShmem clears all transient fields in the control
 * structure and sets the state to newState (typically INACTIVE or ERROR).
 *
 * If errorMessage is non-NULL it is copied into the errorMessage slot;
 * otherwise the slot is cleared.  Caller must NOT hold the LWLock.
 *
 * This is the single point that resets the block-control shmem so we don't
 * forget a field on any error/cleanup path.
 */
static void
ResetClusterChangesBlockShmem(ClusterChangesBlockState newState,
							  const char *errorMessage)
{
	LWLockAcquire(&ClusterChangesBlockControl->lock, LW_EXCLUSIVE);
	ClusterChangesBlockControl->state = newState;
	ClusterChangesBlockControl->workerPid = 0;
	ClusterChangesBlockControl->requestorPid = 0;
	ClusterChangesBlockControl->blockStartTime = 0;
	ClusterChangesBlockControl->timeoutMs = 0;
	ClusterChangesBlockControl->nodeCount = 0;
	ClusterChangesBlockControl->releaseRequested = false;
	if (errorMessage != NULL)
	{
		strlcpy(ClusterChangesBlockControl->errorMessage, errorMessage,
				sizeof(ClusterChangesBlockControl->errorMessage));
	}
	else
	{
		ClusterChangesBlockControl->errorMessage[0] = '\0';
	}
	LWLockRelease(&ClusterChangesBlockControl->lock);
}


/* ----------------------------------------------------------------
 * Background Worker: CitusClusterChangesBlockWorkerMain
 *
 * Lifecycle:
 *   1. Connect to database
 *   2. Open connections to metadata worker nodes
 *   3. Acquire local ExclusiveLocks (coordinator)
 *   4. Send LOCK TABLE commands to all metadata workers
 *   5. Update shared memory -> CLUSTER_CHANGES_BLOCK_ACTIVE
 *   6. Wait loop: check latch for release / timeout / postmaster death
 *   7. Close remote connections (rolls back remote transactions,
 *      releasing remote locks)
 *   8. Exit (local locks released when transaction ends)
 * ---------------------------------------------------------------- */

/*
 * Signal handler for SIGTERM in the background worker.
 */
static void
cluster_changes_block_worker_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * CitusClusterChangesBlockWorkerMain is the entry point for the cluster changes block
 * background worker.  It acquires locks on the coordinator and all
 * worker nodes, then holds them until told to release.
 */
void
CitusClusterChangesBlockWorkerMain(Datum main_arg)
{
	Oid databaseOid = DatumGetObjectId(main_arg);

	/*
	 * Extract timeout from bgw_extra.
	 * Layout: [Oid extensionOwner][int timeoutMs]
	 * Always populated by RegisterCitusBackgroundWorker.
	 */
	int timeoutMs = CLUSTER_CHANGES_BLOCK_DEFAULT_TIMEOUT_MS;
	memcpy_s(&timeoutMs, sizeof(int),
			 MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(int));

	pqsignal(SIGTERM, cluster_changes_block_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/* connect to database */
	BackgroundWorkerInitializeConnectionByOid(databaseOid, InvalidOid, 0);

	elog(LOG, "cluster changes block worker started (timeout=%dms)", timeoutMs);

	/* start a transaction — locks live until this transaction ends */
	StartTransactionCommand();

	List *connectionList = NIL;

	PG_TRY();
	{
		/* open connections to metadata worker nodes */
		connectionList = OpenConnectionsToMetadataWorkersForBlock(ShareLock);

		/* begin remote transactions (to bust through pgbouncer) */
		RemoteTransactionListBegin(connectionList);

		/* acquire local locks on coordinator */
		BlockDistributedTransactionsLocally();

		/* acquire remote locks on all metadata workers */
		int nodeCount = 0;
		BlockDistributedTransactionsOnWorkers(connectionList, timeoutMs,
											  &nodeCount);

		/* update shared memory: locks acquired */
		LWLockAcquire(&ClusterChangesBlockControl->lock, LW_EXCLUSIVE);
		ClusterChangesBlockControl->state = CLUSTER_CHANGES_BLOCK_ACTIVE;
		ClusterChangesBlockControl->blockStartTime = GetCurrentTimestamp();
		ClusterChangesBlockControl->nodeCount = nodeCount;
		ClusterChangesBlockControl->workerPid = MyProcPid;
		LWLockRelease(&ClusterChangesBlockControl->lock);

		elog(LOG,
			 "cluster changes block active: locks held on coordinator + %d worker nodes",
			 nodeCount);

		/*
		 * Build a WaitEventSet that monitors:
		 *   - Each remote connection socket (detect dead workers immediately)
		 *   - Latch (for release request / SIGTERM wakeup)
		 *   - Postmaster death
		 *
		 * Slot count: one per connection + latch + postmaster death.
		 */
		int eventSetSize = list_length(connectionList) + 2;
		WaitEventSet *waitEventSet =
			CreateWaitEventSet(WaitEventSetTracker_compat, eventSetSize);

		MultiConnection *conn = NULL;
		foreach_declared_ptr(conn, connectionList)
		{
			int sock = PQsocket(conn->pgConn);
			if (sock == -1)
			{
				FreeWaitEventSet(waitEventSet);
				ereport(ERROR,
						(errmsg("cluster changes block: remote connection already "
								"closed before entering wait loop")));
			}

			int idx = CitusAddWaitEventSetToSet(waitEventSet,
												WL_SOCKET_READABLE,
												sock, NULL,
												(void *) conn);
			if (idx == WAIT_EVENT_SET_INDEX_FAILED)
			{
				FreeWaitEventSet(waitEventSet);
				ereport(ERROR,
						(errmsg("cluster changes block: could not add remote socket "
								"to wait event set")));
			}
		}
		AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH,
						  PGINVALID_SOCKET, NULL, NULL);
		AddWaitEventToSet(waitEventSet, WL_LATCH_SET,
						  PGINVALID_SOCKET, MyLatch, NULL);

		/*
		 * Hold locks until release is requested, timeout expires,
		 * a remote connection fails, or we receive SIGTERM / postmaster death.
		 */
		TimestampTz startTime = GetCurrentTimestamp();
		WaitEvent *events = palloc0(eventSetSize * sizeof(WaitEvent));

		while (!got_sigterm)
		{
			int eventCount = WaitEventSetWait(waitEventSet,
											  CLUSTER_CHANGES_BLOCK_WORKER_CHECK_MS,
											  events, eventSetSize,
											  PG_WAIT_EXTENSION);

			for (int i = 0; i < eventCount; i++)
			{
				WaitEvent *event = &events[i];

				if (event->events & WL_POSTMASTER_DEATH)
				{
					FreeWaitEventSet(waitEventSet);
					pfree(events);
					ResetClusterChangesBlockShmem(
						CLUSTER_CHANGES_BLOCK_ERROR,
						"postmaster died while holding cluster changes block");
					proc_exit(1);
				}

				if (event->events & WL_LATCH_SET)
				{
					ResetLatch(MyLatch);
					continue;
				}

				if (event->events & WL_SOCKET_READABLE)
				{
					/*
					 * An idle locked connection should never receive data.
					 * Any readability means the remote end sent something
					 * unexpected (e.g. termination notice) or the socket
					 * is broken.
					 */
					MultiConnection *failedConn =
						(MultiConnection *) event->user_data;
					bool connectionBad = false;

					if (PQconsumeInput(failedConn->pgConn) == 0)
					{
						connectionBad = true;
					}
					else if (PQstatus(failedConn->pgConn) == CONNECTION_BAD)
					{
						connectionBad = true;
					}

					if (connectionBad)
					{
						FreeWaitEventSet(waitEventSet);
						pfree(events);
						ereport(ERROR,
								(errmsg("cluster changes block: lost connection to "
										"worker node %s:%d",
										failedConn->hostname,
										failedConn->port)));
					}
				}
			}

			CHECK_FOR_INTERRUPTS();

			/* check if release was requested */
			LWLockAcquire(&ClusterChangesBlockControl->lock, LW_SHARED);
			bool shouldRelease = ClusterChangesBlockControl->releaseRequested;
			LWLockRelease(&ClusterChangesBlockControl->lock);

			if (shouldRelease)
			{
				SetClusterChangesBlockState(CLUSTER_CHANGES_BLOCK_RELEASING);
				elog(LOG, "cluster changes block: release requested, shutting down");
				break;
			}

			/* check timeout */
			TimestampTz now = GetCurrentTimestamp();
			long elapsedMs = (long) ((now - startTime) / 1000);
			if (timeoutMs > 0 && elapsedMs >= timeoutMs)
			{
				elog(WARNING,
					 "cluster changes block: timeout reached (%d ms), auto-releasing",
					 timeoutMs);
				break;
			}
		}

		FreeWaitEventSet(waitEventSet);
		pfree(events);

		/* clean up: close remote connections (rolls back remote transactions) */
		foreach_declared_ptr(conn, connectionList)
		{
			ForgetResults(conn);
			CloseConnection(conn);
		}
		connectionList = NIL;
	}
	PG_CATCH();
	{
		/* on error, clean up connections and report */
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		MultiConnection *conn = NULL;
		foreach_declared_ptr(conn, connectionList)
		{
			ForgetResults(conn);
			CloseConnection(conn);
		}

		/*
		 * SIGTERM-induced errors are controlled shutdowns (e.g. postmaster
		 * restart), not real failures — report INACTIVE instead of ERROR
		 * so the state is clean for the next startup.
		 *
		 * Use ResetClusterChangesBlockShmem to clear all transient fields
		 * (workerPid, requestorPid, blockStartTime, timeoutMs, nodeCount,
		 *  releaseRequested) — proc_exit() below skips the post-PG_END_TRY
		 * cleanup, so this is our only chance to clean up.
		 */
		if (got_sigterm)
		{
			ResetClusterChangesBlockShmem(CLUSTER_CHANGES_BLOCK_INACTIVE, NULL);
		}
		else
		{
			ResetClusterChangesBlockShmem(CLUSTER_CHANGES_BLOCK_ERROR,
										  edata->message);
		}

		FreeErrorData(edata);

		AbortCurrentTransaction();

		elog(LOG, "cluster changes block worker exiting due to %s",
			 got_sigterm ? "SIGTERM" : "error");
		proc_exit(got_sigterm ? 0 : 1);
	}
	PG_END_TRY();

	/* abort transaction to release local locks */
	AbortCurrentTransaction();

	/*
	 * Mark shared memory as inactive — but only if WE are still the recorded
	 * worker.  This owner-check defends against a race where this worker is
	 * delayed between the unlocking transaction and this cleanup, and a
	 * concurrent citus_cluster_changes_block() call has already started a
	 * successor worker that updated workerPid.  Without this check the late
	 * cleanup would clobber the successor's ACTIVE state, leaving callers
	 * unable to observe or release the new block.
	 */
	LWLockAcquire(&ClusterChangesBlockControl->lock, LW_SHARED);
	bool stillOwner = (ClusterChangesBlockControl->workerPid == MyProcPid);
	LWLockRelease(&ClusterChangesBlockControl->lock);

	if (stillOwner)
	{
		ResetClusterChangesBlockShmem(CLUSTER_CHANGES_BLOCK_INACTIVE, NULL);
	}
	else
	{
		elog(LOG, "cluster changes block worker (pid %d) skipping shmem reset; "
				  "a successor worker now owns the control block",
			 MyProcPid);
	}

	elog(LOG, "cluster changes block worker finished, all locks released");
	proc_exit(0);
}


/* ----------------------------------------------------------------
 * Lock acquisition helpers
 * ---------------------------------------------------------------- */

/*
 * OpenConnectionsToMetadataWorkersForBlock opens connections to all
 * remote metadata worker nodes using FORCE_NEW_CONNECTION.
 *
 * Unlike citus_create_restore_point (which needs all nodes for
 * pg_create_restore_point), cluster changes block only needs metadata nodes
 * for the LOCK TABLE commands.
 */
static List *
OpenConnectionsToMetadataWorkersForBlock(LOCKMODE lockMode)
{
	List *connectionList = NIL;
	int connectionFlags = FORCE_NEW_CONNECTION;

	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(lockMode);

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		if (!NodeIsPrimaryAndRemote(workerNode) || !workerNode->hasMetadata)
		{
			continue;
		}

		MultiConnection *connection = StartNodeConnection(connectionFlags,
														  workerNode->workerName,
														  workerNode->workerPort);
		MarkRemoteTransactionCritical(connection);
		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	return connectionList;
}


/*
 * BlockDistributedTransactionsLocally acquires ExclusiveLock on
 * pg_dist_node, pg_dist_partition, and pg_dist_transaction on the
 * coordinator.  Same pattern as citus_create_restore_point.
 */
static void
BlockDistributedTransactionsLocally(void)
{
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);
	LockRelationOid(DistPartitionRelationId(), ExclusiveLock);
	LockRelationOid(DistTransactionRelationId(), ExclusiveLock);
}


/*
 * BlockDistributedTransactionsOnWorkers sends LOCK TABLE commands to
 * all connections (which are already filtered to metadata workers)
 * to acquire ExclusiveLocks remotely.
 *
 * A statement_timeout is set on each connection so that lock acquisition
 * does not hang indefinitely if a remote node is unresponsive.
 *
 * Sets *nodeCount to the number of nodes locked.
 */
static void
BlockDistributedTransactionsOnWorkers(List *connectionList, int timeoutMs,
									  int *nodeCount)
{
	/* set statement_timeout on each remote connection to bound lock wait */
	char timeoutCommand[128];
	SafeSnprintf(timeoutCommand, sizeof(timeoutCommand),
				 "SET LOCAL statement_timeout = %d", timeoutMs);

	MultiConnection *connection = NULL;
	foreach_declared_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, timeoutCommand);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	foreach_declared_ptr(connection, connectionList)
	{
		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}
		PQclear(result);
		ForgetResults(connection);
	}

	/* send lock commands in parallel */
	foreach_declared_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection,
										  BLOCK_DISTRIBUTED_WRITES_COMMAND);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* wait for all lock acquisitions */
	foreach_declared_ptr(connection, connectionList)
	{
		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}
		PQclear(result);
		ForgetResults(connection);
	}

	*nodeCount = list_length(connectionList);
}


/* ----------------------------------------------------------------
 * SQL-callable UDFs
 * ---------------------------------------------------------------- */

/*
 * citus_cluster_changes_block blocks distributed 2PC writes across
 * the entire Citus cluster.  A background worker is spawned to hold
 * the locks, so they survive if this session disconnects.
 *
 * Parameters:
 *   timeout_ms (int, default 300000) - auto-release after this many ms
 *
 * Returns: true when locks are held on all nodes.
 */
Datum
citus_cluster_changes_block(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();
	EnsureCoordinator();

	int timeoutMs = PG_GETARG_INT32(0);

	if (timeoutMs <= 0 || timeoutMs > CLUSTER_CHANGES_BLOCK_MAX_TIMEOUT_MS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timeout_ms must be between 1 and %d",
						CLUSTER_CHANGES_BLOCK_MAX_TIMEOUT_MS)));
	}

	/*
	 * Atomically check-and-set under a single LW_EXCLUSIVE acquisition
	 * to prevent TOCTOU races between concurrent callers.
	 */
	LWLockAcquire(&ClusterChangesBlockControl->lock, LW_EXCLUSIVE);
	ClusterChangesBlockState currentState = ClusterChangesBlockControl->state;

	/*
	 * Reject any non-INACTIVE state: spawning a successor worker while a
	 * previous one is still alive (RELEASING) would violate the singleton
	 * invariant and the previous worker's late shmem cleanup could clobber
	 * the successor's state.  ERROR state requires explicit unblock to
	 * clear so the operator notices the failure.
	 */
	if (currentState != CLUSTER_CHANGES_BLOCK_INACTIVE)
	{
		LWLockRelease(&ClusterChangesBlockControl->lock);

		switch (currentState)
		{
			case CLUSTER_CHANGES_BLOCK_STARTING:
			case CLUSTER_CHANGES_BLOCK_ACTIVE:
			{
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("a cluster changes block is already active"),
						 errhint("Use citus_cluster_changes_unblock() to release "
								 "the existing block first.")));
				break;
			}

			case CLUSTER_CHANGES_BLOCK_RELEASING:
			{
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("a cluster changes block release is in progress"),
						 errhint("Wait for the current release to finish, then "
								 "retry citus_cluster_changes_block().")));
				break;
			}

			case CLUSTER_CHANGES_BLOCK_ERROR:
			{
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("the previous cluster changes block ended in error"),
						 errhint("Call citus_cluster_changes_unblock() to clear the "
								 "error state, then retry.")));
				break;
			}

			default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("cluster changes block is in an unexpected state")));
				break;
			}
		}
	}

	ClusterChangesBlockControl->state = CLUSTER_CHANGES_BLOCK_STARTING;
	ClusterChangesBlockControl->requestorPid = MyProcPid;
	ClusterChangesBlockControl->workerPid = 0;
	ClusterChangesBlockControl->releaseRequested = false;
	ClusterChangesBlockControl->errorMessage[0] = '\0';
	ClusterChangesBlockControl->nodeCount = 0;
	ClusterChangesBlockControl->timeoutMs = timeoutMs;
	LWLockRelease(&ClusterChangesBlockControl->lock);

	/* spawn the background worker */
	char workerName[BGW_MAXLEN];
	SafeSnprintf(workerName, BGW_MAXLEN,
				 "Citus Cluster Changes Block Worker: %u", MyDatabaseId);

	CitusBackgroundWorkerConfig config = {
		.workerName = workerName,
		.functionName = "CitusClusterChangesBlockWorkerMain",
		.mainArg = ObjectIdGetDatum(MyDatabaseId),
		.extensionOwner = CitusExtensionOwner(),
		.needsNotification = true,
		.waitForStartup = true,
		.restartTime = CITUS_BGW_NEVER_RESTART,
		.startTime = BgWorkerStart_RecoveryFinished,
		.workerType = "citus_cluster_changes_block",
		.extraData = &timeoutMs,
		.extraDataSize = sizeof(int)
	};

	BackgroundWorkerHandle *handle = RegisterCitusBackgroundWorker(&config);
	if (!handle)
	{
		ResetClusterChangesBlockShmem(CLUSTER_CHANGES_BLOCK_INACTIVE, NULL);
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start cluster changes block background worker"),
				 errhint("Check that max_worker_processes is high enough.")));
	}

	/*
	 * Poll shared memory until the worker reports ACTIVE or ERROR.
	 * The worker was started with waitForStartup=true, so it's
	 * already running at this point.
	 */
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(&ClusterChangesBlockControl->lock, LW_SHARED);
		ClusterChangesBlockState state = ClusterChangesBlockControl->state;
		char errMsg[256];
		if (state == CLUSTER_CHANGES_BLOCK_ERROR)
		{
			strlcpy(errMsg, ClusterChangesBlockControl->errorMessage, sizeof(errMsg));
		}
		LWLockRelease(&ClusterChangesBlockControl->lock);

		if (state == CLUSTER_CHANGES_BLOCK_ACTIVE)
		{
			break;
		}

		if (state == CLUSTER_CHANGES_BLOCK_ERROR)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("cluster changes block worker failed: %s", errMsg)));
		}

		if (state == CLUSTER_CHANGES_BLOCK_INACTIVE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("cluster changes block worker exited unexpectedly")));
		}

		/* still STARTING, check if worker is still alive */
		pid_t bgwPid;
		BgwHandleStatus bgwStatus = GetBackgroundWorkerPid(handle, &bgwPid);
		if (bgwStatus == BGWH_STOPPED)
		{
			ResetClusterChangesBlockShmem(CLUSTER_CHANGES_BLOCK_INACTIVE, NULL);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("cluster changes block worker exited unexpectedly "
							"(crashed or killed)")));
		}

		/* still STARTING, wait a bit */
		int rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   CLUSTER_CHANGES_BLOCK_POLL_INTERVAL_MS,
						   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
		{
			proc_exit(1);
		}
	}

	PG_RETURN_BOOL(true);
}


/*
 * citus_cluster_changes_unblock signals the background worker to
 * release all locks and exit.  Can be called from any session.
 *
 * Returns:
 *   true  if the block was released, or if a previous ERROR state was cleared.
 *   false if no block was active (state was already INACTIVE) or the worker
 *         did not shut down within the unblock timeout.
 */
Datum
citus_cluster_changes_unblock(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();
	EnsureCoordinator();

	LWLockAcquire(&ClusterChangesBlockControl->lock, LW_EXCLUSIVE);
	ClusterChangesBlockState currentState = ClusterChangesBlockControl->state;

	/*
	 * If the previous block ended in ERROR, no worker is alive — just clear
	 * the shmem so a fresh block() call can succeed.  This is the only SQL
	 * path to recover from ERROR without a server restart.
	 */
	if (currentState == CLUSTER_CHANGES_BLOCK_ERROR)
	{
		LWLockRelease(&ClusterChangesBlockControl->lock);
		ResetClusterChangesBlockShmem(CLUSTER_CHANGES_BLOCK_INACTIVE, NULL);
		ereport(NOTICE,
				(errmsg("cleared cluster changes block error state")));
		PG_RETURN_BOOL(true);
	}

	if (currentState != CLUSTER_CHANGES_BLOCK_ACTIVE &&
		currentState != CLUSTER_CHANGES_BLOCK_STARTING)
	{
		LWLockRelease(&ClusterChangesBlockControl->lock);
		PG_RETURN_BOOL(false);
	}

	ClusterChangesBlockControl->releaseRequested = true;

	/* wake the worker via its latch if it has a valid PID */
	pid_t workerPid = ClusterChangesBlockControl->workerPid;
	LWLockRelease(&ClusterChangesBlockControl->lock);

	if (workerPid > 0)
	{
		/*
		 * Send SIGUSR1 to wake the worker's latch so it checks
		 * releaseRequested and exits cleanly via the release path.
		 * This is gentler than SIGTERM and allows the worker to
		 * transition through CLUSTER_CHANGES_BLOCK_RELEASING state.
		 */
		kill(workerPid, SIGUSR1);
	}

	/*
	 * Wait for the worker to finish releasing.
	 * Poll shared memory with a short interval.
	 */
	for (int i = 0; i < CLUSTER_CHANGES_BLOCK_UNBLOCK_MAX_LOOPS; i++)
	{
		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(&ClusterChangesBlockControl->lock, LW_SHARED);
		ClusterChangesBlockState state = ClusterChangesBlockControl->state;
		LWLockRelease(&ClusterChangesBlockControl->lock);

		if (state == CLUSTER_CHANGES_BLOCK_INACTIVE)
		{
			PG_RETURN_BOOL(true);
		}

		int rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   CLUSTER_CHANGES_BLOCK_POLL_INTERVAL_MS,
						   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
		{
			proc_exit(1);
		}
	}

	ereport(WARNING,
			(errmsg("cluster changes block worker did not shut down within 30 seconds")));

	PG_RETURN_BOOL(false);
}


/*
 * citus_cluster_changes_block_status returns a single row describing the current
 * state of the cluster changes block.
 *
 * Returns: (state text, worker_pid int, requestor_pid int,
 *           block_start_time timestamptz, timeout_ms int, node_count int)
 */
Datum
citus_cluster_changes_block_status(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TupleDesc tupleDesc;

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	tupleDesc = BlessTupleDesc(tupleDesc);

	Datum values[6];
	bool nulls[6];
	memset(nulls, 0, sizeof(nulls));

	LWLockAcquire(&ClusterChangesBlockControl->lock, LW_SHARED);

	ClusterChangesBlockState currentState = ClusterChangesBlockControl->state;
	pid_t workerPid = ClusterChangesBlockControl->workerPid;

	LWLockRelease(&ClusterChangesBlockControl->lock);

	/*
	 * Detect stale state: if the worker is supposed to be alive but its
	 * process no longer exists (SIGKILL, OOM-killer, etc.), auto-clean
	 * the shared memory so the system doesn't get permanently stuck.
	 */
	if ((currentState == CLUSTER_CHANGES_BLOCK_ACTIVE ||
		 currentState == CLUSTER_CHANGES_BLOCK_STARTING ||
		 currentState == CLUSTER_CHANGES_BLOCK_RELEASING ||
		 currentState == CLUSTER_CHANGES_BLOCK_ERROR) &&
		workerPid > 0 &&
		kill(workerPid, 0) == -1 && errno == ESRCH)
	{
		/*
		 * Worker is gone — re-acquire exclusive, double-check that the
		 * stale state still matches the dead PID, then route the cleanup
		 * through ResetClusterChangesBlockShmem to keep all reset paths
		 * consistent.  We release the lock between the check and the
		 * helper call, but that gap is harmless: any concurrent block()
		 * is rejected by the != INACTIVE guard, and any concurrent
		 * stale-cleanup or unblock-from-ERROR also targets INACTIVE so
		 * the writes are idempotent.
		 */
		LWLockAcquire(&ClusterChangesBlockControl->lock, LW_EXCLUSIVE);

		bool needsReset =
			(ClusterChangesBlockControl->state == CLUSTER_CHANGES_BLOCK_ACTIVE ||
			 ClusterChangesBlockControl->state == CLUSTER_CHANGES_BLOCK_STARTING ||
			 ClusterChangesBlockControl->state == CLUSTER_CHANGES_BLOCK_RELEASING ||
			 ClusterChangesBlockControl->state == CLUSTER_CHANGES_BLOCK_ERROR) &&
			ClusterChangesBlockControl->workerPid == workerPid;
		LWLockRelease(&ClusterChangesBlockControl->lock);

		if (needsReset)
		{
			elog(WARNING, "cluster changes block: detected stale state (worker PID %d "
						  "no longer exists), auto-cleaning", workerPid);
			ResetClusterChangesBlockShmem(CLUSTER_CHANGES_BLOCK_INACTIVE, NULL);
		}

		LWLockAcquire(&ClusterChangesBlockControl->lock, LW_SHARED);
		currentState = ClusterChangesBlockControl->state;
		LWLockRelease(&ClusterChangesBlockControl->lock);
	}

	LWLockAcquire(&ClusterChangesBlockControl->lock, LW_SHARED);

	/* state */
	const char *stateStr;
	switch (ClusterChangesBlockControl->state)
	{
		case CLUSTER_CHANGES_BLOCK_INACTIVE:
		{
			stateStr = "inactive";
			break;
		}

		case CLUSTER_CHANGES_BLOCK_STARTING:
		{
			stateStr = "starting";
			break;
		}

		case CLUSTER_CHANGES_BLOCK_ACTIVE:
		{
			stateStr = "active";
			break;
		}

		case CLUSTER_CHANGES_BLOCK_RELEASING:
		{
			stateStr = "releasing";
			break;
		}

		case CLUSTER_CHANGES_BLOCK_ERROR:
		{
			stateStr = "error";
			break;
		}

		default:
		{
			stateStr = "unknown";
			break;
		}
	}
	values[0] = CStringGetTextDatum(stateStr);

	/* worker_pid */
	if (ClusterChangesBlockControl->workerPid > 0)
	{
		values[1] = Int32GetDatum(ClusterChangesBlockControl->workerPid);
	}
	else
	{
		nulls[1] = true;
	}

	/* requestor_pid */
	if (ClusterChangesBlockControl->requestorPid > 0)
	{
		values[2] = Int32GetDatum(ClusterChangesBlockControl->requestorPid);
	}
	else
	{
		nulls[2] = true;
	}

	/* block_start_time */
	if (ClusterChangesBlockControl->blockStartTime > 0)
	{
		values[3] = TimestampTzGetDatum(ClusterChangesBlockControl->blockStartTime);
	}
	else
	{
		nulls[3] = true;
	}

	/* timeout_ms */
	values[4] = Int32GetDatum(ClusterChangesBlockControl->timeoutMs);

	/* node_count */
	values[5] = Int32GetDatum(ClusterChangesBlockControl->nodeCount);

	LWLockRelease(&ClusterChangesBlockControl->lock);

	HeapTuple tuple = heap_form_tuple(tupleDesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
