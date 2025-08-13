/*-------------------------------------------------------------------------
 *
 * background_worker_utils.c
 *    Common utilities for initializing PostgreSQL background workers
 *    used by Citus distributed infrastructure.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "postmaster/bgworker.h"
#include "storage/proc.h"

#include "distributed/background_worker_utils.h"
#include "distributed/citus_safe_lib.h"

/*
 * InitializeCitusBackgroundWorker initializes a BackgroundWorker struct
 * with common Citus background worker settings.
 */
void
InitializeCitusBackgroundWorker(BackgroundWorker *worker,
								const CitusBackgroundWorkerConfig *config)
{
	Assert(worker != NULL);
	Assert(config != NULL);
	Assert(config->workerName != NULL);
	Assert(config->functionName != NULL);

	/* Initialize the worker structure */
	memset(worker, 0, sizeof(BackgroundWorker));

	/* Set worker name */
	strcpy_s(worker->bgw_name, sizeof(worker->bgw_name), config->workerName);

	/* Set worker type if provided */
	if (config->workerType != NULL)
	{
		strcpy_s(worker->bgw_type, sizeof(worker->bgw_type), config->workerType);
	}

	/* Set standard flags for Citus workers */
	worker->bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	/* Set start time - use custom start time if provided, otherwise use default */
	worker->bgw_start_time = (config->startTime != 0) ? config->startTime :
							 CITUS_BGW_DEFAULT_START_TIME;

	/* Set restart behavior */
	worker->bgw_restart_time = config->restartTime;

	/* Set library and function names */
	strcpy_s(worker->bgw_library_name, sizeof(worker->bgw_library_name), "citus");
	strcpy_s(worker->bgw_function_name, sizeof(worker->bgw_function_name),
			 config->functionName);

	/* Set main argument */
	worker->bgw_main_arg = config->mainArg;

	/* Set extension owner if provided */
	if (OidIsValid(config->extensionOwner))
	{
		memcpy_s(worker->bgw_extra, sizeof(worker->bgw_extra),
				 &config->extensionOwner, sizeof(Oid));
	}

	/* Set additional extra data if provided */
	if (config->extraData != NULL && config->extraDataSize > 0)
	{
		size_t remainingSpace = sizeof(worker->bgw_extra);
		size_t usedSpace = OidIsValid(config->extensionOwner) ? sizeof(Oid) : 0;

		if (usedSpace + config->extraDataSize <= remainingSpace)
		{
			memcpy_s(((char *) worker->bgw_extra) + usedSpace,
					 remainingSpace - usedSpace,
					 config->extraData,
					 config->extraDataSize);
		}
	}

	/* Set notification PID if needed */
	if (config->needsNotification)
	{
		worker->bgw_notify_pid = MyProcPid;
	}
}


/*
 * RegisterCitusBackgroundWorker creates and registers a Citus background worker
 * with the specified configuration. Returns the worker handle on success,
 * NULL on failure.
 */
BackgroundWorkerHandle *
RegisterCitusBackgroundWorker(const CitusBackgroundWorkerConfig *config)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;

	/* Initialize the worker structure */
	InitializeCitusBackgroundWorker(&worker, config);

	/* Register the background worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		return NULL;
	}

	/* Wait for startup if requested */
	if (config->waitForStartup && handle != NULL)
	{
		pid_t pid = 0;
		WaitForBackgroundWorkerStartup(handle, &pid);
	}

	return handle;
}
