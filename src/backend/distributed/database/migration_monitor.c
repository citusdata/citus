/*-------------------------------------------------------------------------
 *
 * migration_monitor.c
 *
 * Background worker that refreshes the subscription of a migration.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_subscription.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/subscriptioncmds.h"
#include "distributed/database/migration_monitor.h"
#include "distributed/multi_logical_replication.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/syscache.h"


/* private function declarations */
static void migration_monitor_sigterm(SIGNAL_ARGS);
static void RefreshSubscription(char *subscriptionName);

PG_FUNCTION_INFO_V1(citus_internal_start_migration_monitor);
void MigrationMonitorMain(Datum arg);

static volatile sig_atomic_t got_sigterm = false;


/*
 * citus_internal_start_migration_monitor starts a background worker that
 * monitors a migration and refreshes the subscription.
 */
Datum
citus_internal_start_migration_monitor(PG_FUNCTION_ARGS)
{
	text *databaseNameText = PG_GETARG_TEXT_P(0);
	char *databaseName = text_to_cstring(databaseNameText);
	text *subscriptionNameText = PG_GETARG_TEXT_P(1);
	char *subscriptionName = text_to_cstring(subscriptionNameText);

	bool missingOk = false;
	Oid databaseId = get_database_oid(databaseName, missingOk);

	/* cannot use get_subscription_oid because of custom database OID */
	Oid subscriptionId = GetSysCacheOid2(SUBSCRIPTIONNAME, Anum_pg_subscription_oid,
										 databaseId, CStringGetDatum(subscriptionName));
	if (!OidIsValid(subscriptionId))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("subscription \"%s\" does not exist", subscriptionName)));
	}

	pid_t workerPid = StartMigrationMonitor(databaseId, subscriptionId);

	PG_RETURN_INT32(workerPid);
}


/*
 * StartMigrationMonitor registers a background worker in given target
 * database, and returns the background worker handle so that the caller can
 * wait until it is started.
 */
pid_t
StartMigrationMonitor(Oid databaseId, Oid subscriptionId)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	pid_t pid = 0;

	memset(&worker, 0, sizeof(worker));

	elog(LOG, "StartMigrationMonitor %d", subscriptionId);
	uint64 argBits = (uint64) databaseId << 32 | (uint64) subscriptionId;

	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	worker.bgw_main_arg = UInt64GetDatum(argBits);
	worker.bgw_notify_pid = MyProcPid;
	strlcpy(worker.bgw_library_name, "citus",
			sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_function_name, "MigrationMonitorMain",
			sizeof(worker.bgw_function_name));
	strlcpy(worker.bgw_name, "migration monitor",
			sizeof(worker.bgw_name));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(ERROR, (errmsg("failed to start migration monitor")));
	}

	WaitForBackgroundWorkerStartup(handle, &pid);

	return pid;
}


/*
 * MigrationMonitorMain is the main entry-point for the background worker that
 * performs subscription refresh.
 */
void
MigrationMonitorMain(Datum arg)
{
	uint64 argBits = DatumGetUInt64(arg);
	Oid databaseId = (Oid) (argBits >> 32);
	Oid subscriptionId = (Oid) (argBits & 0x00000000FFFFFFFF);

	pqsignal(SIGTERM, migration_monitor_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnectionByOid(databaseId, InvalidOid, 0);

	/* Make background worker recognisable in pg_stat_activity */
	pgstat_report_appname("migration monitor");

	ereport(LOG, (errmsg("migration monitor started for subscription %d "
						 "in database %d",
						 subscriptionId,
						 databaseId)));

	bool isMigrationActive = true;

	while (!got_sigterm && isMigrationActive)
	{
		StartTransactionCommand();

		bool missingOk = true;
		char *subscriptionName = get_subscription_name(subscriptionId, missingOk);
		if (subscriptionName != NULL)
		{
			ereport(LOG, (errmsg("refreshing subscription %s", subscriptionName)));
			RefreshSubscription(subscriptionName);
		}
		else
		{
			isMigrationActive = false;
		}

		CommitTransactionCommand();

		WaitForMiliseconds(10000);
	}

	ereport(LOG, (errmsg("migration monitor stopping")));

	proc_exit(0);
}


/*
 * migration_monitor_sigterm handle SIGTERM signals
 */
static void
migration_monitor_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * RefreshSubscription refreshes the subscription for the current database.
 */
static void
RefreshSubscription(char *subscriptionName)
{
	AlterSubscriptionStmt *alterSubStmt = makeNode(AlterSubscriptionStmt);
	alterSubStmt->kind = ALTER_SUBSCRIPTION_REFRESH;
	alterSubStmt->subname = subscriptionName;

#if (PG_VERSION_NUM >= PG_VERSION_15)
	AlterSubscription(NULL, alterSubStmt, true);
#elif  (PG_VERSION_NUM >= PG_VERSION_14)
	AlterSubscription(alterSubStmt, true);
#else
	AlterSubscription(alterSubStmt);
#endif
}
