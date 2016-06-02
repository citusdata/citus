/*-------------------------------------------------------------------------
 *
 * multi_server_executor.h
 *	  Type and function declarations for executing remote jobs from a backend;
 *	  the ensemble of these jobs form the distributed execution plan.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_SERVER_EXECUTOR_H
#define MULTI_SERVER_EXECUTOR_H

#include "distributed/multi_physical_planner.h"
#include "distributed/task_tracker.h"
#include "distributed/worker_manager.h"


#define MAX_TASK_EXECUTION_FAILURES 3 /* allowed failure count for one task */
#define MAX_TRACKER_FAILURE_COUNT 3   /* allowed failure count for one tracker */
#define REMOTE_NODE_CONNECT_TIMEOUT 4000 /* async connect timeout in ms */
#define RESERVED_FD_COUNT 64           /* file descriptors unavailable to executor */

/* copy out query results */
#define COPY_QUERY_TO_STDOUT_TEXT "COPY (%s) TO STDOUT"
#define COPY_QUERY_TO_STDOUT_BINARY "COPY (%s) TO STDOUT WITH (FORMAT binary)"
#define COPY_QUERY_TO_FILE_TEXT "COPY (%s) TO '%s'"
#define COPY_QUERY_TO_FILE_BINARY "COPY (%s) TO '%s' WITH (FORMAT binary)"

/* Task tracker executor related defines */
#define TASK_ASSIGNMENT_QUERY "SELECT task_tracker_assign_task \
 ("UINT64_FORMAT ", %u, %s)"
#define TASK_STATUS_QUERY "SELECT task_tracker_task_status("UINT64_FORMAT ", %u)"
#define JOB_CLEANUP_QUERY "SELECT task_tracker_cleanup_job("UINT64_FORMAT ")"
#define JOB_CLEANUP_TASK_ID INT_MAX


/* Enumeration to track one task's execution status */
typedef enum
{
	EXEC_TASK_INVALID_FIRST = 0,
	EXEC_TASK_CONNECT_START = 1,
	EXEC_TASK_CONNECT_POLL = 2,
	EXEC_TASK_FAILED = 3,
	EXEC_FETCH_TASK_LOOP = 4,
	EXEC_FETCH_TASK_START = 5,
	EXEC_FETCH_TASK_RUNNING = 6,
	EXEC_COMPUTE_TASK_START = 7,
	EXEC_COMPUTE_TASK_RUNNING = 8,
	EXEC_COMPUTE_TASK_COPYING = 9,
	EXEC_TASK_DONE = 10,

	/* used for task tracker executor */
	EXEC_TASK_UNASSIGNED = 11,
	EXEC_TASK_QUEUED = 12,
	EXEC_TASK_TRACKER_RETRY = 13,
	EXEC_TASK_TRACKER_FAILED = 14,
	EXEC_SOURCE_TASK_TRACKER_RETRY = 15,
	EXEC_SOURCE_TASK_TRACKER_FAILED = 16
} TaskExecStatus;


/* Enumeration to track file transmits to the master node */
typedef enum
{
	EXEC_TRANSMIT_INVALID_FIRST = 0,
	EXEC_TRANSMIT_UNASSIGNED = 1,
	EXEC_TRANSMIT_QUEUED = 2,
	EXEC_TRANSMIT_COPYING = 3,
	EXEC_TRANSMIT_TRACKER_RETRY = 4,
	EXEC_TRANSMIT_TRACKER_FAILED = 5,
	EXEC_TRANSMIT_DONE = 6
} TransmitExecStatus;


/* Enumeration to track a task tracker's connection status */
typedef enum
{
	TRACKER_STATUS_INVALID_FIRST = 0,
	TRACKER_CONNECT_START = 1,
	TRACKER_CONNECT_POLL = 2,
	TRACKER_CONNECTED = 3,
	TRACKER_CONNECTION_FAILED = 4
} TrackerStatus;


/* Enumeration that represents distributed executor types */
typedef enum
{
	MULTI_EXECUTOR_INVALID_FIRST = 0,
	MULTI_EXECUTOR_REAL_TIME = 1,
	MULTI_EXECUTOR_TASK_TRACKER = 2,
	MULTI_EXECUTOR_ROUTER = 3
} MultiExecutorType;


/* Enumeration that represents a (dis)connect action taken */
typedef enum
{
	CONNECT_ACTION_NONE = 0,
	CONNECT_ACTION_OPENED = 1,
	CONNECT_ACTION_CLOSED = 2
} ConnectAction;


/*
 * TaskExecution holds state that relates to a task's execution. In the case of
 * the real-time executor, this struct encapsulates all information necessary to
 * run the task. The task tracker executor however manages its connection logic
 * elsewhere, and doesn't use connection related fields defined in here.
 */
struct TaskExecution
{
	uint64 jobId;
	uint32 taskId;

	TaskExecStatus *taskStatusArray;
	TransmitExecStatus *transmitStatusArray;
	int32 *connectionIdArray;
	int32 *fileDescriptorArray;
	TimestampTz connectStartTime;
	uint32 nodeCount;
	uint32 currentNodeIndex;
	uint32 querySourceNodeIndex; /* only applies to map fetch tasks */
	int32 dataFetchTaskIndex;
	uint32 failureCount;
};


/*
 * TrackerTaskState represents a task's execution status on a particular task
 * tracker. This state augments task execution state in that it is associated
 * with execution on a particular task tracker.
 */
typedef struct TrackerTaskState
{
	uint64 jobId;
	uint32 taskId;
	TaskStatus status;
	StringInfo taskAssignmentQuery;
} TrackerTaskState;


/*
 * TaskTracker keeps connection and task related state for a task tracker. The
 * task tracker executor then uses this state to open and manage a connection to
 * the task tracker; and assign and check status of tasks over this connection.
 */
typedef struct TaskTracker
{
	uint32 workerPort;              /* node's port; part of hash table key */
	char workerName[WORKER_LENGTH]; /* node's name; part of hash table key */
	TrackerStatus trackerStatus;
	int32 connectionId;
	uint32 connectPollCount;
	uint32 connectionFailureCount;
	uint32 trackerFailureCount;

	HTAB *taskStateHash;
	List *assignedTaskList;
	int32 currentTaskIndex;
	bool connectionBusy;
	TrackerTaskState *connectionBusyOnTask;
} TaskTracker;


/*
 * WorkerNodeState keeps state for a worker node. The real-time executor uses this to
 * keep track of the number of open connections to a worker node.
 */
typedef struct WorkerNodeState
{
	uint32 workerPort;
	char workerName[WORKER_LENGTH];
	uint32 openConnectionCount;
} WorkerNodeState;


/* Config variable managed via guc.c */
extern int RemoteTaskCheckInterval;
extern int MaxAssignTaskBatchSize;
extern int TaskExecutorType;
extern bool BinaryMasterCopyFormat;


/* Function declarations for distributed execution */
extern void MultiRealTimeExecute(Job *job);
extern void MultiTaskTrackerExecute(Job *job);

/* Function declarations common to more than one executor */
extern bool RouterExecutablePlan(MultiPlan *multiPlan, MultiExecutorType executorType);
extern MultiExecutorType JobExecutorType(MultiPlan *multiPlan);
extern void RemoveJobDirectory(uint64 jobId);
extern TaskExecution * InitTaskExecution(Task *task, TaskExecStatus initialStatus);
extern void CleanupTaskExecution(TaskExecution *taskExecution);
extern bool TaskExecutionFailed(TaskExecution *taskExecution);
extern void AdjustStateForFailure(TaskExecution *taskExecution);
extern int MaxMasterConnectionCount(void);


#endif /* MULTI_SERVER_EXECUTOR_H */
