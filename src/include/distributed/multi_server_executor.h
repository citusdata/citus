/*-------------------------------------------------------------------------
 *
 * multi_server_executor.h
 *	  Type and function declarations for executing remote jobs from a backend;
 *	  the ensemble of these jobs form the distributed execution plan.
 *
 * Copyright (c) Citus Data, Inc.
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
#define RESERVED_FD_COUNT 64           /* file descriptors unavailable to executor */

/* copy out query results */
#define EXECUTE_SQL_TASK_TO_FILE_BINARY \
	"SELECT worker_execute_sql_task("UINT64_FORMAT ", %u, %s, true)"
#define EXECUTE_SQL_TASK_TO_FILE_TEXT \
	"SELECT worker_execute_sql_task("UINT64_FORMAT ", %u, %s, false)"

/* Task tracker executor related defines */
#define TASK_ASSIGNMENT_QUERY "SELECT task_tracker_assign_task \
 ("UINT64_FORMAT ", %u, %s);"
#define TASK_STATUS_QUERY "SELECT task_tracker_task_status("UINT64_FORMAT ", %u);"
#define JOB_CLEANUP_QUERY "SELECT task_tracker_cleanup_job("UINT64_FORMAT ")"
#define JOB_CLEANUP_TASK_ID INT_MAX

/* Adaptive executor repartioning related defines */
#define WORKER_CREATE_SCHEMA_QUERY "SELECT worker_create_schema (" UINT64_FORMAT ", %s);"
#define WORKER_REPARTITION_CLEANUP_QUERY "SELECT worker_repartition_cleanup (" \
	UINT64_FORMAT \
	");"


/* Enumeration to track one task's execution status */
typedef enum
{
	/* used for task tracker executor */
	EXEC_TASK_INVALID_FIRST = 0,
	EXEC_TASK_DONE = 1,
	EXEC_TASK_UNASSIGNED = 2,
	EXEC_TASK_QUEUED = 3,
	EXEC_TASK_TRACKER_RETRY = 4,
	EXEC_TASK_TRACKER_FAILED = 5,
	EXEC_SOURCE_TASK_TRACKER_RETRY = 6,
	EXEC_SOURCE_TASK_TRACKER_FAILED = 7,
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
	MULTI_EXECUTOR_ADAPTIVE = 1,
	MULTI_EXECUTOR_TASK_TRACKER = 2,
	MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT = 3
} MultiExecutorType;


/*
 * DistributedExecutionStats holds the execution related stats.
 *
 * totalIntermediateResultSize is a counter to keep the size
 * of the intermediate results of complex subqueries and CTEs
 * so that we can put a limit on the size.
 */
typedef struct DistributedExecutionStats
{
	uint64 totalIntermediateResultSize;
} DistributedExecutionStats;


/*
 * TaskExecution holds state that relates to a task's execution for task-tracker
 * executor.
 */
struct TaskExecution
{
	CitusNode type;
	uint64 jobId;
	uint32 taskId;

	TaskExecStatus *taskStatusArray;
	TransmitExecStatus *transmitStatusArray;
	int32 *connectionIdArray;
	int32 *fileDescriptorArray;
	uint32 nodeCount;
	uint32 currentNodeIndex;
	uint32 querySourceNodeIndex; /* only applies to map fetch tasks */
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
	char *userName;                 /* which user to connect as */
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
	List *connectionBusyOnTaskList;
} TaskTracker;


/* Config variable managed via guc.c */
extern int RemoteTaskCheckInterval;
extern int MaxAssignTaskBatchSize;
extern int TaskExecutorType;
extern bool EnableRepartitionJoins;
extern bool BinaryMasterCopyFormat;
extern int MultiTaskQueryLogLevel;


/* Function declarations for distributed execution */
extern void MultiTaskTrackerExecute(Job *job);

/* Function declarations common to more than one executor */
extern MultiExecutorType JobExecutorType(DistributedPlan *distributedPlan);
extern void RemoveJobDirectory(uint64 jobId);
extern TaskExecution * InitTaskExecution(Task *task, TaskExecStatus initialStatus);
extern bool CheckIfSizeLimitIsExceeded(DistributedExecutionStats *executionStats);
extern void CleanupTaskExecution(TaskExecution *taskExecution);
extern void ErrorSizeLimitIsExceeded(void);
extern bool TaskExecutionFailed(TaskExecution *taskExecution);
extern void AdjustStateForFailure(TaskExecution *taskExecution);
extern int MaxMasterConnectionCount(void);


extern TupleTableSlot * TaskTrackerExecScan(CustomScanState *node);

#endif /* MULTI_SERVER_EXECUTOR_H */
