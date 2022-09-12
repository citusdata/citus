/*-------------------------------------------------------------------------
 *
 * background_jobs.h
 *	  Functions related to running the background tasks queue monitor.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_BACKGROUND_JOBS_H
#define CITUS_BACKGROUND_JOBS_H

#include "postgres.h"

#include "postmaster/bgworker.h"

#include "distributed/metadata_utility.h"

extern BackgroundWorkerHandle * StartCitusBackgroundTaskQueueMonitor(Oid database,
																	 Oid extensionOwner);
extern void CitusBackgroundTaskQueueMonitorMain(Datum arg);
extern void CitusBackgroundTaskExecuter(Datum main_arg);

extern Datum citus_job_cancel(PG_FUNCTION_ARGS);
extern Datum citus_job_wait(PG_FUNCTION_ARGS);
extern void citus_job_wait_internal(int64 jobid, BackgroundJobStatus *desiredStatus);

#endif /*CITUS_BACKGROUND_JOBS_H */
