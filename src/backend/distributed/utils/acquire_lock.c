/*-------------------------------------------------------------------------
 *
 * acquire_lock.c
 *	  A dynamic background worker that can help your backend to acquire its locks. This is
 *	  an intrusive way of getting your way. The primary use of this will be to allow
 *	  master_update_node to make progress during failure. When the system cannot possibly
 *	  finish a transaction due to the host required to finish the transaction has failed
 *	  it might be better to actively cancel the backend instead of waiting for it to fail.
 *
 * This file provides infrastructure for launching exactly one a background
 * worker for every database in which citus is used.  That background worker
 * can then perform work like deadlock detection, prepared transaction
 * recovery, and cleanup.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include <unistd.h>

#include "postgres.h"


#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/snapmgr.h"

#include "distributed/citus_acquire_lock.h"
#include "distributed/version_compat.h"

/* forward declaration of background worker entrypoint */
extern void LockAcquireHelperMain(Datum main_arg);

/* forward declaration of helper functions */
static void lock_acquire_helper_sigterm(SIGNAL_ARGS);
static void EnsureStopLockAcquireHelper(void *arg);
static long DeadlineTimestampTzToTimeout(TimestampTz deadline);

/* LockAcquireHelperArgs contains extra arguments to be used to start the worker */
typedef struct LockAcquireHelperArgs
{
	Oid DatabaseId;
	int32 lock_cooldown;
} LockAcquireHelperArgs;

static bool got_sigterm = false;


/*
 * StartLockAcquireHelperBackgroundWorker creates a background worker that will help the
 * backend passed in as an argument to complete. The worker that is started will be
 * terminated once the current memory context gets reset, to make sure it is cleaned up in
 * all situations. It is however advised to call TerminateBackgroundWorker on the handle
 * returned on the first possible moment the help is no longer required.
 */
BackgroundWorkerHandle *
StartLockAcquireHelperBackgroundWorker(int backendToHelp, int32 lock_cooldown)
{
	BackgroundWorkerHandle *handle = NULL;
	LockAcquireHelperArgs args;
	BackgroundWorker worker;
	memset(&args, 0, sizeof(args));
	memset(&worker, 0, sizeof(worker));

	/* collect the extra arguments required for the background worker */
	args.DatabaseId = MyDatabaseId;
	args.lock_cooldown = lock_cooldown;

	/* construct the background worker and start it */
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "Citus Lock Acquire Helper: %d/%u",
			 backendToHelp, MyDatabaseId);
#if PG_VERSION_NUM >= 110000
	snprintf(worker.bgw_type, BGW_MAXLEN, "citus_lock_aqcuire");
#endif

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	snprintf(worker.bgw_library_name, BGW_MAXLEN, "citus");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "LockAcquireHelperMain");
	worker.bgw_main_arg = Int32GetDatum(backendToHelp);
	worker.bgw_notify_pid = 0;

	/*
	 * we check if args fits in bgw_extra to make sure it is safe to copy the data. Once
	 * we exceed the size of data to copy this way we need to look into a different way of
	 * passing the arguments to the worker.
	 */
	Assert(sizeof(worker.bgw_extra) >= sizeof(args));
	memcpy(worker.bgw_extra, &args, sizeof(args));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(ERROR, (errmsg("could not start lock acquiring background worker to "
							   "force the update"),
						errhint("Increasing max_worker_processes might help.")));
	}


	MemoryContextCallback *cb = palloc0(sizeof(MemoryContextCallback));
	cb->func = EnsureStopLockAcquireHelper;
	cb->arg = handle;

	MemoryContextRegisterResetCallback(CurrentMemoryContext, cb);

	return handle;
}


/*
 * EnsureStopLockAcquireHelper is designed to be called as a MemoryContextCallback. It
 * takes a handle to the background worker and Terminates it. It is safe to be called on a
 * handle that has already been terminated due to the guard around the generation number
 * implemented in the handle by postgres.
 */
static void
EnsureStopLockAcquireHelper(void *arg)
{
	BackgroundWorkerHandle *handle = (BackgroundWorkerHandle *) arg;
	TerminateBackgroundWorker(handle);
}


/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
lock_acquire_helper_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * ShouldAcquireLock tests if our backend should still proceed with acquiring the lock,
 * and thus keep terminating conflicting backends. This function returns true until a
 * SIGTERM, background worker termination signal, has been received.
 *
 * The function blocks for at most sleepms when called. During operation without being
 * terminated this is the time between invocations to the backend termination logic.
 */
static bool
ShouldAcquireLock(long sleepms)
{
	int rc;

	/* early escape in case we already got the signal to stop acquiring the lock */
	if (got_sigterm)
	{
		return false;
	}

	rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				   sleepms * 1L, PG_WAIT_EXTENSION);
	ResetLatch(MyLatch);

	/* emergency bailout if postmaster has died */
	if (rc & WL_POSTMASTER_DEATH)
	{
		proc_exit(1);
	}

	CHECK_FOR_INTERRUPTS();

	return !got_sigterm;
}


/*
 * LockAcquireHelperMain runs in a dynamic background worker to help master_update_node to
 * acquire its locks.
 */
void
LockAcquireHelperMain(Datum main_arg)
{
	int backendPid = DatumGetInt32(main_arg);
	StringInfoData sql;
	LockAcquireHelperArgs *args = (LockAcquireHelperArgs *) MyBgworkerEntry->bgw_extra;
	long timeout = 0;
	const TimestampTz connectionStart = GetCurrentTimestamp();
	const TimestampTz deadline = TimestampTzPlusMilliseconds(connectionStart,
															 args->lock_cooldown);

	pqsignal(SIGTERM, lock_acquire_helper_sigterm);

	BackgroundWorkerUnblockSignals();

	elog(LOG, "lock acquiring backend started for backend %d (cooldown %dms)", backendPid,
		 args->lock_cooldown);

	/*
	 * this loop waits till the deadline is reached (eg. lock_cooldown has passed) OR we
	 * no longer need to acquire the lock due to the termination of this backend.
	 * Only after the timeout the code will continue with the section that will acquire
	 * the lock.
	 */
	do {
		timeout = DeadlineTimestampTzToTimeout(deadline);
	} while (timeout > 0 && ShouldAcquireLock(timeout));

	/* connecting to the database */
	BackgroundWorkerInitializeConnectionByOid(args->DatabaseId, InvalidOid, 0);

	/* TODO explain sql below */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT \n"
					 "    DISTINCT conflicting.pid,\n"
					 "    pg_terminate_backend(conflicting.pid)\n"
					 "  FROM pg_locks AS blocked\n"
					 "       JOIN pg_locks AS conflicting\n"
					 "         ON (conflicting.database = blocked.database\n"
					 "             AND conflicting.objid = blocked.objid)\n"
					 " WHERE conflicting.granted = true\n"
					 "   AND blocked.granted = false\n"
					 "   AND blocked.pid = %d;",
					 backendPid);

	while (ShouldAcquireLock(100))
	{
		int row = 0;
		int spiStatus = 0;

		elog(LOG, "canceling competing backends for backend %d", backendPid);

		/*
		 * Begin our transaction
		 */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING, sql.data);

		spiStatus = SPI_execute(sql.data, false, 0);

		if (spiStatus == SPI_OK_SELECT)
		{
			for (row = 0; row < SPI_processed; row++)
			{
				/* TODO count the number of backends canceled and log about it */

				int terminatedPid = 0;
				bool isTerminated = false;
				bool isnull = false;

				terminatedPid = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
															SPI_tuptable->tupdesc,
															1, &isnull));

				isTerminated = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
														  SPI_tuptable->tupdesc,
														  2, &isnull));

				if (isTerminated)
				{
					elog(WARNING, "terminated conflicting backend %d", terminatedPid);
				}
				else
				{
					elog(INFO,
						 "attempt to terminate conflicting backend %d was unsuccessful",
						 terminatedPid);
				}
			}
		}
		else
		{
			elog(FATAL, "cannot cancel competing backends for backend %d", backendPid);
		}

		/*
		 * And finish our transaction.
		 */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_stat(false);
		pgstat_report_activity(STATE_IDLE, NULL);
	}


	elog(LOG, "lock acquiring backend finished for backend %d", backendPid);

	/* safely got to the end, exit without problem */
	proc_exit(0);
}


/*
 * DeadlineTimestampTzToTimeout returns the numer of miliseconds that still need to elapse
 * before the deadline provided as an argument will be reached. The outcome can be used to
 * pass to the Wait of an EventSet to make sure it returns after the timeout has passed.
 */
static long
DeadlineTimestampTzToTimeout(TimestampTz deadline)
{
	long secs = 0;
	int msecs = 0;
	TimestampDifference(GetCurrentTimestamp(), deadline, &secs, &msecs);
	return secs * 1000 + msecs / 1000;
}
