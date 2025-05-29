/*-------------------------------------------------------------------------
 *
 * util/restore_interval.c
 *	  Subroutines related to the periodic sync save checkpoint.
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "commands/dbcommands.h"
#include "distributed/listutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_manager.h"
#include "distributed/utils/restore_interval.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"


char *RestorePointInterval = NULL;
char *RestorePointIntervalName = NULL;

RestorePointIntervalMode  restorePointIntervalMode = RESTOREPOINT_INTERVAL_NEVER;

/* the flags for first time of execute proc */
static bool is_first_inday  = true;
static bool is_first_inhour = true;
static bool got_SIGALRM = false;
static bool got_SIGTERM = false;


static void SendRestorePointCmd(Oid DatabaseId, Oid UserOid);

void send_sql_restorepoint_cmd(Datum main_arg);

static void MetadataSyncSigAlrmHandler(SIGNAL_ARGS);
static void MetadataSyncSigTermHandler(SIGNAL_ARGS);


void
send_sql_restorepoint_cmd(Datum main_arg)
{
	StringInfoData sql;
	pg_time_t t = time(NULL);
	struct tm * tt = localtime(&t);
	Oid databaseOid = DatumGetObjectId(main_arg);
	Oid extensionOwner = InvalidOid;
	int spiStatus;

	initStringInfo(&sql);
	if (RestorePointIntervalName != NULL)
		appendStringInfo(&sql,"SELECT citus_create_restore_point('%s_%02d.%02d_%02d:00')",
							RestorePointIntervalName, tt->tm_mon, tt->tm_mday, tt->tm_hour);
	else
		appendStringInfo(&sql,"SELECT citus_create_restore_point('restore_autosave_%02d.%02d_%02d:00')",
							tt->tm_mon, tt->tm_mday, tt->tm_hour);


	/* extension owner is passed via bgw_extra */
	memcpy_s(&extensionOwner, sizeof(extensionOwner),
			 MyBgworkerEntry->bgw_extra, sizeof(Oid));

	pqsignal(SIGTERM, MetadataSyncSigTermHandler);
	pqsignal(SIGALRM, MetadataSyncSigAlrmHandler);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(databaseOid, extensionOwner,BGWORKER_BYPASS_ALLOWCONN);

	/*
	 * Start transaction.
	 */
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, sql.data);
	pgstat_report_appname("restore point");

	spiStatus = SPI_exec(sql.data, 1);
	if (spiStatus != SPI_OK_SELECT)
		elog(FATAL, "cannot cancel competing backends for backend %d", getpid());

	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);

	proc_exit(0);
}


static void 
SendRestorePointCmd(Oid DatabaseId, Oid UserOid)
{
	BackgroundWorkerHandle *handle = NULL;
	BackgroundWorker worker;

	worker.bgw_flags = BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_SHMEM_ACCESS;
	strcpy(worker.bgw_name,"Citus Recovery Background");
	strcpy_s(worker.bgw_library_name, sizeof(worker.bgw_library_name), "citus");
	strcpy_s(worker.bgw_function_name, sizeof(worker.bgw_library_name),
			 "send_sql_restorepoint_cmd");
	strcpy(worker.bgw_type, "pg_restore_interval");

	worker.bgw_main_arg = ObjectIdGetDatum(DatabaseId);
	memcpy_s(worker.bgw_extra, sizeof(worker.bgw_extra), &UserOid,
			 sizeof(UserOid));

	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	BackgroundWorkerUnblockSignals();

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		elog(LOG, "error of RegisterDynamicBackgroundWorker ");
		return;
	}
}


void CheckRestoreInterval(Oid databaseId, Oid userOid)
{
	pg_time_t t = time(NULL);
	struct tm * tt = localtime(&t);

	if (is_first_inday == false)
	{
		if (tt->tm_hour)
			is_first_inday = true;
	}


	if (restorePointIntervalMode == RESTOREPOINT_INTERVAL_DAILY)
	{
		if (is_first_inday && tt->tm_hour == 0 && tt->tm_min == 0)
		{
			is_first_inday = false;
			SendRestorePointCmd(databaseId, userOid);
			return;
		}
	}


	if (is_first_inhour == false)
	{
		if (tt->tm_min)
			is_first_inhour = true;
	}

	if (restorePointIntervalMode == RESTOREPOINT_INTERVAL_HOURLY)
	{
		if ( tt->tm_min == 0 && is_first_inhour)
		{
			is_first_inhour = false;
			SendRestorePointCmd(databaseId, userOid);
			return;
		}
	}


}


bool
GucCheckInterval(char **newval, void **extra, GucSource source)
{
	if (newval == NULL)
		return false;

	if (*newval == NULL)
		return true;

	if (strcmp(*newval,"daily") == 0)
	{
		restorePointIntervalMode = RESTOREPOINT_INTERVAL_DAILY;
		return true;
	}

	if (strcmp(*newval,"hourly") == 0)
	{
			restorePointIntervalMode = RESTOREPOINT_INTERVAL_HOURLY;
			return true;
	}

	if (strcmp(*newval,"never") == 0)
		return true;

	GUC_check_errdetail("The dataime format is dayly hourly or never");
	return false;
}


/*
 * MetadataSyncSigAlrmHandler set a flag to request error at metadata
 * sync daemon. This is used for testing purposes.
 */
static void
MetadataSyncSigAlrmHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGALRM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}

/*
 * MetadataSyncSigTermHandler set a flag to request termination of metadata
 * sync daemon.
 */
static void
MetadataSyncSigTermHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}