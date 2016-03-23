/*-------------------------------------------------------------------------
 *
 * task_tracker.h
 *
 * Header and type declarations for coordinating execution of tasks and data
 * source transfers on worker nodes.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef TASK_TRACKER_H
#define TASK_TRACKER_H

#include "storage/lwlock.h"
#include "utils/hsearch.h"


#define HIGH_PRIORITY_TASK_TIME 1   /* assignment time for high priority tasks */
#define RESERVED_JOB_ID 1           /* reserved for cleanup and shutdown tasks */
#define SHUTDOWN_MARKER_TASK_ID UINT_MAX /* used to identify task tracker shutdown */
#define MAX_TASK_FAILURE_COUNT 2    /* allowed failure count for one task */
#define LOCAL_HOST_NAME "localhost" /* connect to local backends using this name */
#define TASK_CALL_STRING_SIZE 12288 /* max length of task call string */
#define TEMPLATE0_NAME "template0"  /* skip job schema cleanup for template0 */
#define JOB_SCHEMA_CLEANUP "SELECT worker_cleanup_job_schema_cache()"


/*
 * TaskStatus represents execution status of worker tasks. The assigned and
 * cancel requested statuses are set by the master node; all other statuses are
 * assigned by the task tracker as the worker task makes progress.
 */
typedef enum
{
	TASK_STATUS_INVALID_FIRST = 0,
	TASK_ASSIGNED = 1,          /* master node and task tracker */
	TASK_SCHEDULED = 2,
	TASK_RUNNING = 3,
	TASK_FAILED = 4,
	TASK_PERMANENTLY_FAILED = 5,
	TASK_SUCCEEDED = 6,
	TASK_CANCEL_REQUESTED = 7,  /* master node only */
	TASK_CANCELED = 8,
	TASK_TO_REMOVE = 9,

	/*
	 * The master node's executor uses the following statuses to fully represent
	 * the execution status of worker tasks, as they are perceived by the master
	 * node. These statuses in fact don't belong with the task tracker.
	 */
	TASK_CLIENT_SIDE_QUEUED = 10,
	TASK_CLIENT_SIDE_ASSIGN_FAILED = 11,
	TASK_CLIENT_SIDE_STATUS_FAILED = 12,
	TASK_FILE_TRANSMIT_QUEUED = 13,
	TASK_CLIENT_SIDE_TRANSMIT_FAILED = 14,

	/*
	 * Add new task status types above this comment. Existing types, except for
	 * TASK_STATUS_LAST, should never have their numbers changed.
	 */
	TASK_STATUS_LAST
} TaskStatus;


/*
 * WorkerTask keeps shared memory state for tasks. At a high level, each worker
 * task holds onto three different types of state: (a) state assigned by the
 * master node, (b) state initialized by the protocol process at task assignment
 * time, and (c) state internal to the task tracker process that changes as the
 * task make progress.
 */
typedef struct WorkerTask
{
	uint64 jobId;      /* job id (upper 32-bits reserved); part of hash table key */
	uint32 taskId;     /* task id; part of hash table key */
	uint32 assignedAt; /* task assignment time in epoch seconds */

	char taskCallString[TASK_CALL_STRING_SIZE]; /* query or function call string */
	TaskStatus taskStatus;  /* task's current execution status */
	char databaseName[NAMEDATALEN];   /* name to use for local backend connection */
	int32 connectionId;     /* connection id to local backend */
	uint32 failureCount;    /* number of task failures */
} WorkerTask;


/*
 * WorkerTasksControlData contains task tracker state shared between
 * processes.
 */
typedef struct WorkerTasksSharedStateData
{
	/* Hash table shared by the task tracker and task tracker protocol functions */
	HTAB *taskHash;

	/* Lock protecting workerNodesHash */
	LWLock *taskHashLock;
} WorkerTasksSharedStateData;


/* Config variables managed via guc.c */
extern int TaskTrackerDelay;
extern int MaxTrackedTasksPerNode;
extern int MaxRunningTasksPerNode;

/* State shared by the task tracker and task tracker protocol functions */
extern WorkerTasksSharedStateData *WorkerTasksSharedState;


/* Function declarations local to the worker module */
extern WorkerTask * WorkerTasksHashEnter(uint64 jobId, uint32 taskId);
extern WorkerTask * WorkerTasksHashFind(uint64 jobId, uint32 taskId);

/* Function declarations for starting up and running the task tracker */
extern void TaskTrackerRegister(void);


#endif   /* TASK_TRACKER_H */
