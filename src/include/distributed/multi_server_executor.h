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

/* copy out query results */
#define EXECUTE_SQL_TASK_TO_FILE_BINARY \
	"SELECT worker_execute_sql_task("UINT64_FORMAT ", %u, %s, true)"
#define EXECUTE_SQL_TASK_TO_FILE_TEXT \
	"SELECT worker_execute_sql_task("UINT64_FORMAT ", %u, %s, false)"

/* Adaptive executor repartioning related defines */
#define WORKER_CREATE_SCHEMA_QUERY "SELECT worker_create_schema (" UINT64_FORMAT ", %s);"
#define WORKER_REPARTITION_CLEANUP_QUERY "SELECT worker_repartition_cleanup (" \
	UINT64_FORMAT \
	");"


/* Enumeration that represents distributed executor types */
typedef enum
{
	MULTI_EXECUTOR_INVALID_FIRST = 0,
	MULTI_EXECUTOR_ADAPTIVE = 1,
	MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT = 2
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

	int32 *connectionIdArray;
	int32 *fileDescriptorArray;
	uint32 nodeCount;
	uint32 currentNodeIndex;
	uint32 querySourceNodeIndex; /* only applies to map fetch tasks */
	uint32 failureCount;
};


/* Config variable managed via guc.c */
extern int RemoteTaskCheckInterval;
extern int MaxAssignTaskBatchSize;
extern int TaskExecutorType;
extern bool EnableRepartitionJoins;
extern bool BinaryMasterCopyFormat;
extern int MultiTaskQueryLogLevel;


/* Function declarations common to more than one executor */
extern MultiExecutorType JobExecutorType(DistributedPlan *distributedPlan);
extern void RemoveJobDirectory(uint64 jobId);
extern bool CheckIfSizeLimitIsExceeded(DistributedExecutionStats *executionStats);
extern void ErrorSizeLimitIsExceeded(void);

#endif /* MULTI_SERVER_EXECUTOR_H */
