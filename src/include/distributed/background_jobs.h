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


/*
 * BackgroundExecutorHashEntry hash table entry to refer existing task executors
 */
typedef struct BackgroundExecutorHashEntry
{
	/* hash key must be the first to hash correctly */
	int64 taskid;

	BackgroundWorkerHandle *handle;
	dsm_segment *seg;
	StringInfo message;
} BackgroundExecutorHashEntry;


/*
 * TaskExecutionContext encapsulates info for currently executed task in queue monitor
 */
typedef struct TaskExecutionContext
{
	/* active background executor entry */
	BackgroundExecutorHashEntry *handleEntry;

	/* active background task */
	BackgroundTask *task;

	/* useful to track if task errored */
	bool error;
} TaskExecutionContext;


/*
 * QueueMonitorExecutionContext encapsulates info related to executors and tasks
 * in queue monitor
 */
typedef struct QueueMonitorExecutionContext
{
	/* current total # of parallel task executors */
	int64 currentExecutorCount;

	/* map of current executors */
	HTAB *currentExecutors;

	/* last background allocation failure timestamp */
	TimestampTz backgroundWorkerFailedStartTime;

	/* useful to track if all tasks EWOULDBLOCK'd at current iteration */
	bool allTasksWouldBlock;

	/* context related to current executed task */
	TaskExecutionContext *taskExecutionContext;

	/* context for monitor related allocations */
	MemoryContext ctx;
} QueueMonitorExecutionContext;


extern BackgroundWorkerHandle * StartCitusBackgroundTaskQueueMonitor(Oid database,
																	 Oid extensionOwner);
extern void CitusBackgroundTaskQueueMonitorMain(Datum arg);
extern void CitusBackgroundTaskExecutor(Datum main_arg);

extern Datum citus_job_cancel(PG_FUNCTION_ARGS);
extern Datum citus_job_wait(PG_FUNCTION_ARGS);
extern void citus_job_wait_internal(int64 jobid, BackgroundJobStatus *desiredStatus);

#endif /*CITUS_BACKGROUND_JOBS_H */
