/*-------------------------------------------------------------------------
 *
 * maintenanced.c
 *	  Background worker run for each citus using database in a postgres
 *    cluster.
 *
 * This file provides infrastructure for launching exactly one a background
 * worker for every database in which citus is used.  That background worker
 * can then perform work like deadlock detection, prepared transaction
 * recovery, and cleanup.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include <time.h>

#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_extension.h"
#include "citus_version.h"
#include "catalog/pg_namespace.h"
#include "commands/async.h"
#include "commands/extension.h"
#include "libpq/pqsignal.h"
#include "catalog/namespace.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/distributed_deadlock_detection.h"
#include "distributed/maintenanced.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/shard_cleaner.h"
#include "distributed/metadata_sync.h"
#include "distributed/query_stats.h"
#include "distributed/statistics_collection.h"
#include "distributed/transaction_recovery.h"
#include "distributed/version_compat.h"
#include "nodes/makefuncs.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "nodes/makefuncs.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "tcop/tcopprot.h"
#if PG_VERSION_NUM >= PG_VERSION_13
#include "common/hashfn.h"
#endif
#include "utils/memutils.h"
#include "utils/lsyscache.h"

/*
 * Shared memory data for all maintenance workers.
 */
typedef struct MaintenanceDaemonControlData
{
	/*
	 * Lock protecting the shared memory state.  This is to be taken when
	 * looking up (shared mode) or inserting (exclusive mode) per-database
	 * data in MaintenanceDaemonDBHash.
	 */
	int trancheId;
	char *lockTrancheName;
	LWLock lock;
} MaintenanceDaemonControlData;


/*
 * Per database worker state.
 */
typedef struct MaintenanceDaemonDBData
{
	/* hash key: database to run on */
	Oid databaseOid;

	/* information: which user to use */
	Oid userOid;
	pid_t workerPid;
	bool daemonStarted;
	bool triggerNodeMetadataSync;
	Latch *latch; /* pointer to the background worker's latch */
} MaintenanceDaemonDBData;

/* config variable for distributed deadlock detection timeout */
double DistributedDeadlockDetectionTimeoutFactor = 2.0;
int Recover2PCInterval = 60000;
int DeferShardDeleteInterval = 15000;

/* config variables for metadata sync timeout */
int MetadataSyncInterval = 60000;
int MetadataSyncRetryInterval = 5000;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static MaintenanceDaemonControlData *MaintenanceDaemonControl = NULL;

/*
 * Hash-table of workers, one entry for each database with citus
 * activated.
 */
static HTAB *MaintenanceDaemonDBHash;

static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* set to true when becoming a maintenance daemon */
static bool IsMaintenanceDaemon = false;

static void MaintenanceDaemonSigTermHandler(SIGNAL_ARGS);
static void MaintenanceDaemonSigHupHandler(SIGNAL_ARGS);
static void MaintenanceDaemonShmemExit(int code, Datum arg);
static void MaintenanceDaemonErrorContext(void *arg);
static bool MetadataSyncTriggeredCheckAndReset(MaintenanceDaemonDBData *dbData);
static void WarnMaintenanceDaemonNotStarted(void);


/*
 * InitializeMaintenanceDaemon, called at server start, is responsible for
 * requesting shared memory and related infrastructure required by maintenance
 * daemons.
 */
void
InitializeMaintenanceDaemon(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = MaintenanceDaemonShmemInit;
}


/*
 * InitializeMaintenanceDaemonBackend, called at backend start and
 * configuration changes, is responsible for starting a per-database
 * maintenance worker if necessary.
 */
void
InitializeMaintenanceDaemonBackend(void)
{
	Oid extensionOwner = CitusExtensionOwner();
	bool found;

	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	MaintenanceDaemonDBData *dbData = (MaintenanceDaemonDBData *) hash_search(
		MaintenanceDaemonDBHash,
		&MyDatabaseId,
		HASH_ENTER_NULL,
		&found);

	if (dbData == NULL)
	{
		WarnMaintenanceDaemonNotStarted();
		LWLockRelease(&MaintenanceDaemonControl->lock);

		return;
	}

	if (!found)
	{
		/* ensure the values in MaintenanceDaemonDBData are zero */
		memset(((char *) dbData) + sizeof(Oid), 0,
			   sizeof(MaintenanceDaemonDBData) - sizeof(Oid));
	}

	if (IsMaintenanceDaemon)
	{
		/*
		 * InitializeMaintenanceDaemonBackend is called by the maintenance daemon
		 * itself. In that case, we clearly don't need to start another maintenance
		 * daemon.
		 */
		LWLockRelease(&MaintenanceDaemonControl->lock);
		return;
	}

	if (!found || !dbData->daemonStarted)
	{
		Assert(dbData->workerPid == 0);

		BackgroundWorker worker;
		BackgroundWorkerHandle *handle = NULL;

		memset(&worker, 0, sizeof(worker));

		SafeSnprintf(worker.bgw_name, sizeof(worker.bgw_name),
					 "Citus Maintenance Daemon: %u/%u",
					 MyDatabaseId, extensionOwner);

		/* request ability to connect to target database */
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

		/*
		 * No point in getting started before able to run query, but we do
		 * want to get started on Hot-Standby.
		 */
		worker.bgw_start_time = BgWorkerStart_ConsistentState;

		/* Restart after a bit after errors, but don't bog the system. */
		worker.bgw_restart_time = 5;
		strcpy_s(worker.bgw_library_name,
				 sizeof(worker.bgw_library_name), "citus");
		strcpy_s(worker.bgw_function_name, sizeof(worker.bgw_library_name),
				 "CitusMaintenanceDaemonMain");

		worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
		memcpy_s(worker.bgw_extra, sizeof(worker.bgw_extra), &extensionOwner,
				 sizeof(Oid));
		worker.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		{
			WarnMaintenanceDaemonNotStarted();
			dbData->daemonStarted = false;
			LWLockRelease(&MaintenanceDaemonControl->lock);

			return;
		}

		dbData->daemonStarted = true;
		dbData->userOid = extensionOwner;
		dbData->workerPid = 0;
		dbData->triggerNodeMetadataSync = false;
		LWLockRelease(&MaintenanceDaemonControl->lock);

		pid_t pid;
		WaitForBackgroundWorkerStartup(handle, &pid);

		pfree(handle);
	}
	else
	{
		Assert(dbData->daemonStarted);

		/*
		 * If owner of extension changed, wake up daemon. It'll notice and
		 * restart.
		 */
		if (dbData->userOid != extensionOwner)
		{
			dbData->userOid = extensionOwner;
			if (dbData->latch)
			{
				SetLatch(dbData->latch);
			}
		}
		LWLockRelease(&MaintenanceDaemonControl->lock);
	}
}


/*
 * WarnMaintenanceDaemonNotStarted warns that maintenanced couldn't be started.
 */
static void
WarnMaintenanceDaemonNotStarted(void)
{
	ereport(WARNING, (errmsg("could not start maintenance background worker"),
					  errhint("Increasing max_worker_processes might help.")));
}


/*
 * CitusMaintenanceDaemonMain is the maintenance daemon's main routine, it'll
 * be started by the background worker infrastructure.  If it errors out,
 * it'll be restarted after a few seconds.
 */
void
CitusMaintenanceDaemonMain(Datum main_arg)
{
	Oid databaseOid = DatumGetObjectId(main_arg);
	TimestampTz nextStatsCollectionTime USED_WITH_LIBCURL_ONLY =
		TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 60 * 1000);
	bool retryStatsCollection USED_WITH_LIBCURL_ONLY = false;
	ErrorContextCallback errorCallback;
	TimestampTz lastRecoveryTime = 0;
	TimestampTz lastShardCleanTime = 0;
	TimestampTz lastStatStatementsPurgeTime = 0;
	TimestampTz nextMetadataSyncTime = 0;


	/*
	 * We do metadata sync in a separate background worker. We need its
	 * handle to be able to check its status.
	 */
	BackgroundWorkerHandle *metadataSyncBgwHandle = NULL;

	/*
	 * Look up this worker's configuration.
	 */
	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	MaintenanceDaemonDBData *myDbData = (MaintenanceDaemonDBData *)
										hash_search(MaintenanceDaemonDBHash, &databaseOid,
													HASH_FIND, NULL);
	if (!myDbData)
	{
		/*
		 * When the database crashes, background workers are restarted, but
		 * the state in shared memory is lost. In that case, we exit and
		 * wait for a session to call InitializeMaintenanceDaemonBackend
		 * to properly add it to the hash.
		 */

		proc_exit(0);
	}

	if (myDbData->workerPid != 0)
	{
		/*
		 * Another maintenance daemon is running. This usually happens because
		 * postgres restarts the daemon after an non-zero exit, and
		 * InitializeMaintenanceDaemonBackend started one before postgres did.
		 * In that case, the first one stays and the last one exits.
		 */

		proc_exit(0);
	}

	before_shmem_exit(MaintenanceDaemonShmemExit, main_arg);

	/*
	 * Signal that I am the maintenance daemon now.
	 *
	 * From this point, DROP DATABASE/EXTENSION will send a SIGTERM to me.
	 */
	myDbData->workerPid = MyProcPid;

	/*
	 * Signal that we are running. This in mainly needed in case of restart after
	 * an error, otherwise the daemonStarted flag is already true.
	 */
	myDbData->daemonStarted = true;

	/* wire up signals */
	pqsignal(SIGTERM, MaintenanceDaemonSigTermHandler);
	pqsignal(SIGHUP, MaintenanceDaemonSigHupHandler);
	BackgroundWorkerUnblockSignals();

	myDbData->latch = MyLatch;

	IsMaintenanceDaemon = true;

	LWLockRelease(&MaintenanceDaemonControl->lock);

	/*
	 * Setup error context so log messages can be properly attributed. Some of
	 * them otherwise sound like they might be from a normal user connection.
	 * Do so before setting up signals etc, so we never exit without the
	 * context setup.
	 */
	memset(&errorCallback, 0, sizeof(errorCallback));
	errorCallback.callback = MaintenanceDaemonErrorContext;
	errorCallback.arg = (void *) myDbData;
	errorCallback.previous = error_context_stack;
	error_context_stack = &errorCallback;


	elog(LOG, "starting maintenance daemon on database %u user %u",
		 databaseOid, myDbData->userOid);

	/* connect to database, after that we can actually access catalogs */
	BackgroundWorkerInitializeConnectionByOid(databaseOid, myDbData->userOid, 0);

	/* make worker recognizable in pg_stat_activity */
	pgstat_report_appname("Citus Maintenance Daemon");

	/*
	 * Terminate orphaned metadata sync daemons spawned from previously terminated
	 * or crashed maintenanced instances.
	 */
	SignalMetadataSyncDaemon(databaseOid, SIGTERM);

	/* enter main loop */
	while (!got_SIGTERM)
	{
		int latchFlags = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
		double timeout = 10000.0; /* use this if the deadlock detection is disabled */
		bool foundDeadlock = false;

		CHECK_FOR_INTERRUPTS();

		CitusTableCacheFlushInvalidatedEntries();

		/*
		 * XXX: Each task should clear the metadata cache before every iteration
		 * by calling InvalidateMetadataSystemCache(), because otherwise it
		 * might contain stale OIDs. It appears that in some cases invalidation
		 * messages for a DROP EXTENSION may arrive during these tasks and
		 * this causes us to cache a stale pg_dist_node OID. We'd actually expect
		 * all invalidations to arrive after obtaining a lock in LockCitusExtension.
		 */

		/*
		 * Perform Work. If a specific task needs to be called sooner than
		 * timeout indicates, it's ok to lower it to that value. Expensive
		 * tasks should do their own time math about whether to re-run checks.
		 */

#ifdef HAVE_LIBCURL
		if (EnableStatisticsCollection &&
			GetCurrentTimestamp() >= nextStatsCollectionTime)
		{
			bool statsCollectionSuccess = false;
			InvalidateMetadataSystemCache();
			StartTransactionCommand();

			/*
			 * Lock the extension such that it cannot be dropped or created
			 * concurrently. Skip statistics collection if citus extension is
			 * not accessible.
			 *
			 * Similarly, we skip statistics collection if there exists any
			 * version mismatch or the extension is not fully created yet.
			 */
			if (!LockCitusExtension())
			{
				ereport(DEBUG1, (errmsg("could not lock the citus extension, "
										"skipping statistics collection")));
			}
			else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
			{
				FlushDistTableCache();
				WarnIfSyncDNS();
				statsCollectionSuccess = CollectBasicUsageStatistics();
			}

			/*
			 * If statistics collection was successful the next collection is
			 * 24-hours later. Also, if this was a retry attempt we don't do
			 * any more retries until 24-hours later, so we limit number of
			 * retries to one.
			 */
			if (statsCollectionSuccess || retryStatsCollection)
			{
				nextStatsCollectionTime =
					TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												STATS_COLLECTION_TIMEOUT_MILLIS);
				retryStatsCollection = false;
			}
			else
			{
				nextStatsCollectionTime =
					TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												STATS_COLLECTION_RETRY_TIMEOUT_MILLIS);
				retryStatsCollection = true;
			}

			CommitTransactionCommand();
		}
#endif

		pid_t metadataSyncBgwPid = 0;
		BgwHandleStatus metadataSyncStatus =
			metadataSyncBgwHandle != NULL ?
			GetBackgroundWorkerPid(metadataSyncBgwHandle, &metadataSyncBgwPid) :
			BGWH_STOPPED;

		if (metadataSyncStatus != BGWH_STOPPED &&
			GetCurrentTimestamp() >= nextMetadataSyncTime)
		{
			/*
			 * Metadata sync is still running, recheck in a short while.
			 */
			int nextTimeout = MetadataSyncRetryInterval;
			nextMetadataSyncTime =
				TimestampTzPlusMilliseconds(GetCurrentTimestamp(), nextTimeout);
			timeout = Min(timeout, nextTimeout);
		}
		else if (!RecoveryInProgress() &&
				 metadataSyncStatus == BGWH_STOPPED &&
				 (MetadataSyncTriggeredCheckAndReset(myDbData) ||
				  GetCurrentTimestamp() >= nextMetadataSyncTime))
		{
			if (metadataSyncBgwHandle)
			{
				TerminateBackgroundWorker(metadataSyncBgwHandle);
				pfree(metadataSyncBgwHandle);
				metadataSyncBgwHandle = NULL;
			}

			InvalidateMetadataSystemCache();
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());

			int nextTimeout = MetadataSyncRetryInterval;
			bool syncMetadata = false;

			if (!LockCitusExtension())
			{
				ereport(DEBUG1, (errmsg("could not lock the citus extension, "
										"skipping metadata sync")));
			}
			else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
			{
				bool lockFailure = false;
				syncMetadata = ShouldInitiateMetadataSync(&lockFailure);

				/*
				 * If lock fails, we need to recheck in a short while. If we are
				 * going to sync metadata, we should recheck in a short while to
				 * see if it failed. Otherwise, we can wait longer.
				 */
				nextTimeout = (lockFailure || syncMetadata) ?
							  MetadataSyncRetryInterval :
							  MetadataSyncInterval;
			}

			PopActiveSnapshot();
			CommitTransactionCommand();

			if (syncMetadata)
			{
				metadataSyncBgwHandle =
					SpawnSyncNodeMetadataToNodes(MyDatabaseId, myDbData->userOid);
			}

			nextMetadataSyncTime =
				TimestampTzPlusMilliseconds(GetCurrentTimestamp(), nextTimeout);
			timeout = Min(timeout, nextTimeout);
		}

		/*
		 * If enabled, run 2PC recovery on primary nodes (where !RecoveryInProgress()),
		 * since we'll write to the pg_dist_transaction log.
		 */
		if (Recover2PCInterval > 0 && !RecoveryInProgress() &&
			TimestampDifferenceExceeds(lastRecoveryTime, GetCurrentTimestamp(),
									   Recover2PCInterval))
		{
			int recoveredTransactionCount = 0;

			InvalidateMetadataSystemCache();
			StartTransactionCommand();

			if (!LockCitusExtension())
			{
				ereport(DEBUG1, (errmsg("could not lock the citus extension, "
										"skipping 2PC recovery")));
			}
			else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
			{
				/*
				 * Record last recovery time at start to ensure we run once per
				 * Recover2PCInterval even if RecoverTwoPhaseCommits takes some time.
				 */
				lastRecoveryTime = GetCurrentTimestamp();

				recoveredTransactionCount = RecoverTwoPhaseCommits();
			}

			CommitTransactionCommand();

			if (recoveredTransactionCount > 0)
			{
				ereport(LOG, (errmsg("maintenance daemon recovered %d distributed "
									 "transactions",
									 recoveredTransactionCount)));
			}

			/* make sure we don't wait too long */
			timeout = Min(timeout, Recover2PCInterval);
		}

		/* the config value -1 disables the distributed deadlock detection  */
		if (DistributedDeadlockDetectionTimeoutFactor != -1.0)
		{
			double deadlockTimeout =
				DistributedDeadlockDetectionTimeoutFactor * (double) DeadlockTimeout;

			InvalidateMetadataSystemCache();
			StartTransactionCommand();

			/*
			 * We skip the deadlock detection if citus extension
			 * is not accessible.
			 *
			 * Similarly, we skip to run the deadlock checks if
			 * there exists any version mismatch or the extension
			 * is not fully created yet.
			 */
			if (!LockCitusExtension())
			{
				ereport(DEBUG1, (errmsg("could not lock the citus extension, "
										"skipping deadlock detection")));
			}
			else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
			{
				foundDeadlock = CheckForDistributedDeadlocks();
			}

			CommitTransactionCommand();

			/*
			 * If we find any deadlocks, run the distributed deadlock detection
			 * more often since it is quite possible that there are other
			 * deadlocks need to be resolved.
			 *
			 * Thus, we use 1/20 of the calculated value. With the default
			 * values (i.e., deadlock_timeout 1 seconds,
			 * citus.distributed_deadlock_detection_factor 2), we'd be able to cancel
			 * ~10 distributed deadlocks per second.
			 */
			if (foundDeadlock)
			{
				deadlockTimeout = deadlockTimeout / 20.0;
			}

			/* make sure we don't wait too long */
			timeout = Min(timeout, deadlockTimeout);
		}

		if (!RecoveryInProgress() && DeferShardDeleteInterval > 0 &&
			TimestampDifferenceExceeds(lastShardCleanTime, GetCurrentTimestamp(),
									   DeferShardDeleteInterval))
		{
			int numberOfDroppedShards = 0;

			InvalidateMetadataSystemCache();
			StartTransactionCommand();

			if (!LockCitusExtension())
			{
				ereport(DEBUG1, (errmsg(
									 "could not lock the citus extension, skipping shard cleaning")));
			}
			else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
			{
				/*
				 * Record last shard clean time at start to ensure we run once per
				 * DeferShardDeleteInterval.
				 */
				lastShardCleanTime = GetCurrentTimestamp();

				bool waitForLocks = false;
				numberOfDroppedShards = TryDropOrphanedShards(waitForLocks);
			}

			CommitTransactionCommand();

			if (numberOfDroppedShards > 0)
			{
				ereport(LOG, (errmsg("maintenance daemon dropped %d distributed "
									 "shards previously marked to be removed",
									 numberOfDroppedShards)));
			}

			/* make sure we don't wait too long */
			timeout = Min(timeout, DeferShardDeleteInterval);
		}

		if (StatStatementsPurgeInterval > 0 &&
			StatStatementsTrack != STAT_STATEMENTS_TRACK_NONE &&
			TimestampDifferenceExceeds(lastStatStatementsPurgeTime, GetCurrentTimestamp(),
									   (StatStatementsPurgeInterval * 1000)))
		{
			StartTransactionCommand();

			if (!LockCitusExtension())
			{
				ereport(DEBUG1, (errmsg("could not lock the citus extension, "
										"skipping stat statements purging")));
			}
			else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
			{
				/*
				 * Record last time we perform the purge to ensure we run once per
				 * StatStatementsPurgeInterval.
				 */
				lastStatStatementsPurgeTime = GetCurrentTimestamp();

				CitusQueryStatsSynchronizeEntries();
			}

			CommitTransactionCommand();

			/* make sure we don't wait too long, need to convert seconds to milliseconds */
			timeout = Min(timeout, (StatStatementsPurgeInterval * 1000));
		}

		/*
		 * Wait until timeout, or until somebody wakes us up. Also cast the timeout to
		 * integer where we've calculated it using double for not losing the precision.
		 */
		int rc = WaitLatch(MyLatch, latchFlags, (long) timeout, PG_WAIT_EXTENSION);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			proc_exit(1);
		}

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();

			/* check for changed configuration */
			if (myDbData->userOid != GetSessionUserId())
			{
				/* return code of 1 requests worker restart */
				proc_exit(1);
			}

			/*
			 * Could also add code checking whether extension still exists,
			 * but that'd complicate things a bit, because we'd have to delete
			 * the shared memory entry.  There'd potentially be a race
			 * condition where the extension gets re-created, checking that
			 * this entry still exists, and it getting deleted just after.
			 * Doesn't seem worth catering for that.
			 */
		}

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	if (metadataSyncBgwHandle)
	{
		TerminateBackgroundWorker(metadataSyncBgwHandle);
	}
}


/*
 * MaintenanceDaemonShmemSize computes how much shared memory is required.
 */
size_t
MaintenanceDaemonShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(MaintenanceDaemonControlData));

	/*
	 * We request enough shared memory to have one hash-table entry for each
	 * worker process. We couldn't start more anyway, so there's little point
	 * in allocating more.
	 */
	Size hashSize = hash_estimate_size(max_worker_processes,
									   sizeof(MaintenanceDaemonDBData));
	size = add_size(size, hashSize);

	return size;
}


/*
 * MaintenanceDaemonShmemInit initializes the requested shared memory for the
 * maintenance daemon.
 */
void
MaintenanceDaemonShmemInit(void)
{
	bool alreadyInitialized = false;
	HASHCTL hashInfo;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	MaintenanceDaemonControl =
		(MaintenanceDaemonControlData *) ShmemInitStruct("Citus Maintenance Daemon",
														 MaintenanceDaemonShmemSize(),
														 &alreadyInitialized);

	/*
	 * Might already be initialized on EXEC_BACKEND type platforms that call
	 * shared library initialization functions in every backend.
	 */
	if (!alreadyInitialized)
	{
		MaintenanceDaemonControl->trancheId = LWLockNewTrancheId();
		MaintenanceDaemonControl->lockTrancheName = "Citus Maintenance Daemon";
		LWLockRegisterTranche(MaintenanceDaemonControl->trancheId,
							  MaintenanceDaemonControl->lockTrancheName);

		LWLockInitialize(&MaintenanceDaemonControl->lock,
						 MaintenanceDaemonControl->trancheId);
	}


	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(Oid);
	hashInfo.entrysize = sizeof(MaintenanceDaemonDBData);
	hashInfo.hash = tag_hash;
	int hashFlags = (HASH_ELEM | HASH_FUNCTION);

	MaintenanceDaemonDBHash = ShmemInitHash("Maintenance Database Hash",
											max_worker_processes, max_worker_processes,
											&hashInfo, hashFlags);

	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * MaintenaceDaemonShmemExit is the before_shmem_exit handler for cleaning up MaintenanceDaemonDBHash
 */
static void
MaintenanceDaemonShmemExit(int code, Datum arg)
{
	Oid databaseOid = DatumGetObjectId(arg);

	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	MaintenanceDaemonDBData *myDbData = (MaintenanceDaemonDBData *)
										hash_search(MaintenanceDaemonDBHash, &databaseOid,
													HASH_FIND, NULL);

	/* myDbData is NULL after StopMaintenanceDaemon */
	if (myDbData != NULL)
	{
		/*
		 * Confirm that I am still the registered maintenance daemon before exiting.
		 */
		Assert(myDbData->workerPid == MyProcPid);

		myDbData->daemonStarted = false;
		myDbData->workerPid = 0;
	}

	LWLockRelease(&MaintenanceDaemonControl->lock);
}


/* MaintenanceDaemonSigTermHandler calls proc_exit(0) */
static void
MaintenanceDaemonSigTermHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/*
 * MaintenanceDaemonSigHupHandler set a flag to re-read config file at next
 * convenient time.
 */
static void
MaintenanceDaemonSigHupHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGHUP = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/*
 * MaintenanceDaemonErrorContext adds some context to log messages to make it
 * easier to associate them with the maintenance daemon.
 */
static void
MaintenanceDaemonErrorContext(void *arg)
{
	MaintenanceDaemonDBData *myDbData = (MaintenanceDaemonDBData *) arg;
	errcontext("Citus maintenance daemon for database %u user %u",
			   myDbData->databaseOid, myDbData->userOid);
}


/*
 * LockCitusExtension acquires a lock on the Citus extension or returns
 * false if the extension does not exist or is being dropped.
 */
bool
LockCitusExtension(void)
{
	Oid extensionOid = get_extension_oid("citus", true);
	if (extensionOid == InvalidOid)
	{
		/* citus extension does not exist */
		return false;
	}

	LockDatabaseObject(ExtensionRelationId, extensionOid, 0, AccessShareLock);

	/*
	 * The extension may have been dropped and possibly recreated prior to
	 * obtaining a lock. Check whether we still get the expected OID.
	 */
	Oid recheckExtensionOid = get_extension_oid("citus", true);
	if (recheckExtensionOid != extensionOid)
	{
		return false;
	}

	return true;
}


/*
 * StopMaintenanceDaemon stops the maintenance daemon for the
 * given database and removes it from the maintenance daemon
 * control hash.
 */
void
StopMaintenanceDaemon(Oid databaseId)
{
	bool found = false;
	pid_t workerPid = 0;

	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	MaintenanceDaemonDBData *dbData = (MaintenanceDaemonDBData *) hash_search(
		MaintenanceDaemonDBHash,
		&databaseId,
		HASH_REMOVE, &found);

	if (found)
	{
		workerPid = dbData->workerPid;
	}

	LWLockRelease(&MaintenanceDaemonControl->lock);

	if (workerPid > 0)
	{
		kill(workerPid, SIGTERM);
	}
}


/*
 * TriggerMetadataSync triggers the maintenance daemon to do
 * a node metadata sync for the given database.
 */
void
TriggerNodeMetadataSync(Oid databaseId)
{
	bool found = false;

	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	MaintenanceDaemonDBData *dbData = (MaintenanceDaemonDBData *) hash_search(
		MaintenanceDaemonDBHash,
		&databaseId,
		HASH_FIND, &found);
	if (found)
	{
		dbData->triggerNodeMetadataSync = true;

		/* set latch to wake-up the maintenance loop */
		SetLatch(dbData->latch);
	}

	LWLockRelease(&MaintenanceDaemonControl->lock);
}


/*
 * MetadataSyncTriggeredCheckAndReset checks if metadata sync has been
 * triggered for the given database, and resets the flag.
 */
static bool
MetadataSyncTriggeredCheckAndReset(MaintenanceDaemonDBData *dbData)
{
	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	bool metadataSyncTriggered = dbData->triggerNodeMetadataSync;
	dbData->triggerNodeMetadataSync = false;

	LWLockRelease(&MaintenanceDaemonControl->lock);

	return metadataSyncTriggered;
}
