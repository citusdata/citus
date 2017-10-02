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
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "libpq/pqsignal.h"
#include "catalog/namespace.h"
#include "distributed/distributed_deadlock_detection.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_cache.h"
#include "nodes/makefuncs.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "tcop/tcopprot.h"


/*
 * Shared memory data for all maintenance workers.
 */
typedef struct MaintenanceDaemonControlData
{
	/*
	 * Lock protecting the shared memory state.  This is to be taken when
	 * looking up (shared mode) or inserting (exclusive mode) per-database
	 * data in dbHash.
	 */
	int trancheId;
#if (PG_VERSION_NUM >= 100000)
	char *lockTrancheName;
#else
	LWLockTranche lockTranche;
#endif
	LWLock lock;

	/*
	 * Hash-table of workers, one entry for each database with citus
	 * activated.
	 */
	HTAB *dbHash;
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
	bool daemonStarted;
	pid_t workerPid;
	Latch *latch; /* pointer to the background worker's latch */
} MaintenanceDaemonDBData;

/* config variable for distributed deadlock detection timeout */
double DistributedDeadlockDetectionTimeoutFactor = 2.0;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static MaintenanceDaemonControlData *MaintenanceDaemonControl = NULL;

static volatile sig_atomic_t got_SIGHUP = false;

static void MaintenanceDaemonSigHupHandler(SIGNAL_ARGS);
static size_t MaintenanceDaemonShmemSize(void);
static void MaintenanceDaemonShmemInit(void);
static void MaintenanceDaemonErrorContext(void *arg);
static bool LockCitusExtension(void);


/*
 * InitializeMaintenanceDaemon, called at server start, is responsible for
 * requesting shared memory and related infrastructure required by maintenance
 * daemons.
 */
void
InitializeMaintenanceDaemon(void)
{
	RequestAddinShmemSpace(MaintenanceDaemonShmemSize());

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
	MaintenanceDaemonDBData *dbData = NULL;
	Oid extensionOwner = CitusExtensionOwner();
	bool found;

	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	dbData = (MaintenanceDaemonDBData *) hash_search(MaintenanceDaemonControl->dbHash,
													 &MyDatabaseId,
													 HASH_ENTER_NULL, &found);

	if (dbData == NULL)
	{
		/* FIXME: better message, reference relevant guc in hint */
		ereport(ERROR, (errmsg("ran out of database slots")));
	}

	if (!found || !dbData->daemonStarted)
	{
		BackgroundWorker worker;
		BackgroundWorkerHandle *handle = NULL;
		int pid = 0;

		dbData->userOid = extensionOwner;

		memset(&worker, 0, sizeof(worker));

		snprintf(worker.bgw_name, BGW_MAXLEN,
				 "Citus Maintenance Daemon: %u/%u",
				 MyDatabaseId, extensionOwner);

		/* request ability to connect to target database */
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

		/*
		 * No point in getting started before able to run query, but we do
		 * want to get started on Hot-Stanby standbys.
		 */
		worker.bgw_start_time = BgWorkerStart_ConsistentState;

		/*
		 * Restart after a bit after errors, but don't bog the system.
		 */
		worker.bgw_restart_time = 5;
		sprintf(worker.bgw_library_name, "citus");
		sprintf(worker.bgw_function_name, "CitusMaintenanceDaemonMain");
		worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
		memcpy(worker.bgw_extra, &extensionOwner, sizeof(Oid));
		worker.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		{
			ereport(ERROR, (errmsg("could not start maintenance background worker"),
							errhint("Increasing max_worker_processes might help.")));
		}

		dbData->daemonStarted = true;
		dbData->workerPid = 0;
		LWLockRelease(&MaintenanceDaemonControl->lock);

		WaitForBackgroundWorkerStartup(handle, &pid);
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
 * CitusMaintenanceDaemonMain is the maintenance daemon's main routine, it'll
 * be started by the background worker infrastructure.  If it errors out,
 * it'll be restarted after a few seconds.
 */
void
CitusMaintenanceDaemonMain(Datum main_arg)
{
	Oid databaseOid = DatumGetObjectId(main_arg);
	MaintenanceDaemonDBData *myDbData = NULL;
	ErrorContextCallback errorCallback;

	/*
	 * Look up this worker's configuration.
	 */
	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_SHARED);

	myDbData = (MaintenanceDaemonDBData *)
			   hash_search(MaintenanceDaemonControl->dbHash, &databaseOid,
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

	/* from this point, DROP DATABASE will attempt to kill the worker */
	myDbData->workerPid = MyProcPid;

	/* wire up signals */
	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, MaintenanceDaemonSigHupHandler);
	BackgroundWorkerUnblockSignals();

	myDbData->latch = MyLatch;

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
	BackgroundWorkerInitializeConnectionByOid(databaseOid, myDbData->userOid);

	/* make worker recognizable in pg_stat_activity */
	pgstat_report_appname("Citus Maintenance Daemon");

	/* enter main loop */
	for (;;)
	{
		int rc;
		int latchFlags = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
		double timeout = 10000.0; /* use this if the deadlock detection is disabled */
		bool foundDeadlock = false;

		CHECK_FOR_INTERRUPTS();

		/*
		 * XXX: We clear the metadata cache before every iteration because otherwise
		 * it might contain stale OIDs. It appears that in some cases invalidation
		 * messages for a DROP EXTENSION may arrive during deadlock detection and
		 * this causes us to cache a stale pg_dist_node OID. We'd actually expect
		 * all invalidations to arrive after obtaining a lock in LockCitusExtension.
		 */
		InvalidateMetadataSystemCache();

		/*
		 * Perform Work.  If a specific task needs to be called sooner than
		 * timeout indicates, it's ok to lower it to that value.  Expensive
		 * tasks should do their own time math about whether to re-run checks.
		 */

		/* the config value -1 disables the distributed deadlock detection  */
		if (DistributedDeadlockDetectionTimeoutFactor != -1.0)
		{
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
			timeout =
				DistributedDeadlockDetectionTimeoutFactor * (double) DeadlockTimeout;

			if (foundDeadlock)
			{
				timeout = timeout / 20.0;
			}
		}

		/*
		 * Wait until timeout, or until somebody wakes us up. Also cast the timeout to
		 * integer where we've calculated it using double for not losing the precision.
		 */
#if (PG_VERSION_NUM >= 100000)
		rc = WaitLatch(MyLatch, latchFlags, (long) timeout, PG_WAIT_EXTENSION);
#else
		rc = WaitLatch(MyLatch, latchFlags, (long) timeout);
#endif

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
}


/*
 * MaintenanceDaemonShmemSize computes how much shared memory is required.
 */
static size_t
MaintenanceDaemonShmemSize(void)
{
	Size size = 0;
	Size hashSize = 0;

	size = add_size(size, sizeof(MaintenanceDaemonControlData));

	/*
	 * We request enough shared memory to have one hash-table entry for each
	 * worker process. We couldn't start more anyway, so there's little point
	 * in allocating more.
	 */
	hashSize = hash_estimate_size(max_worker_processes, sizeof(MaintenanceDaemonDBData));
	size = add_size(size, hashSize);

	return size;
}


/*
 * MaintenanceDaemonShmemInit initializes the requested shared memory for the
 * maintenance daemon.
 */
static void
MaintenanceDaemonShmemInit(void)
{
	bool alreadyInitialized = false;
	HASHCTL hashInfo;
	int hashFlags = 0;

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
#if (PG_VERSION_NUM >= 100000)
		MaintenanceDaemonControl->trancheId = LWLockNewTrancheId();
		MaintenanceDaemonControl->lockTrancheName = "Citus Maintenance Daemon";
		LWLockRegisterTranche(MaintenanceDaemonControl->trancheId,
							  MaintenanceDaemonControl->lockTrancheName);
#else

		/* initialize lwlock  */
		LWLockTranche *tranche = &MaintenanceDaemonControl->lockTranche;

		/* start by zeroing out all the memory */
		memset(MaintenanceDaemonControl, 0, MaintenanceDaemonShmemSize());

		/* initialize lock */
		MaintenanceDaemonControl->trancheId = LWLockNewTrancheId();
		tranche->array_base = &MaintenanceDaemonControl->lock;
		tranche->array_stride = sizeof(LWLock);
		tranche->name = "Citus Maintenance Daemon";
		LWLockRegisterTranche(MaintenanceDaemonControl->trancheId, tranche);
#endif

		LWLockInitialize(&MaintenanceDaemonControl->lock,
						 MaintenanceDaemonControl->trancheId);
	}


	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(Oid);
	hashInfo.entrysize = sizeof(MaintenanceDaemonDBData);
	hashInfo.hash = tag_hash;
	hashFlags = (HASH_ELEM | HASH_FUNCTION);

	MaintenanceDaemonControl->dbHash =
		ShmemInitHash("Maintenance Database Hash",
					  max_worker_processes, max_worker_processes,
					  &hashInfo, hashFlags);

	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
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
static bool
LockCitusExtension(void)
{
	Oid recheckExtensionOid = InvalidOid;

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
	recheckExtensionOid = get_extension_oid("citus", true);
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
	MaintenanceDaemonDBData *dbData = NULL;
	pid_t workerPid = 0;

	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	dbData = (MaintenanceDaemonDBData *) hash_search(MaintenanceDaemonControl->dbHash,
													 &databaseId, HASH_REMOVE, &found);
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
