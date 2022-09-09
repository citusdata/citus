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

extern BackgroundWorkerHandle * StartCitusBackgroundTaskQueueMonitor(Oid database,
																	 Oid extensionOwner);
extern void CitusBackgroundTaskQueueMonitorMain(Datum arg);
extern void CitusBackgroundTaskExecuter(Datum main_arg);

#endif /*CITUS_BACKGROUND_JOBS_H */
