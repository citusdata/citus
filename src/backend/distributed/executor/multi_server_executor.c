/*-------------------------------------------------------------------------
 *
 * multi_server_executor.c
 *
 * Function definitions for distributed task execution for real-time
 * and task-tracker executors, and routines common to both. The common
 * routines are implement backend-side logic; and they trigger executions
 * on the client-side via function hooks that they load.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <unistd.h>

#include "distributed/multi_client_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/worker_protocol.h"


int RemoteTaskCheckInterval = 100; /* per cycle sleep interval in millisecs */
int TaskExecutorType = MULTI_EXECUTOR_REAL_TIME; /* distributed executor type */
bool BinaryMasterCopyFormat = false; /* copy data from workers in binary format */


/*
 * JobExecutorType selects the executor type for the given multiPlan using the task
 * executor type config value. The function then checks if the given multiPlan needs
 * more resources than those provided to it by other config values, and issues
 * warnings accordingly. If the selected executor type cannot execute the given
 * multiPlan, the function errors out.
 */
MultiExecutorType
JobExecutorType(MultiPlan *multiPlan)
{
	Job *job = multiPlan->workerJob;
	Query *masterQuery = multiPlan->masterQuery;
	List *workerTaskList = job->taskList;
	List *workerNodeList = WorkerNodeList();
	int taskCount = list_length(workerTaskList);
	int workerNodeCount = list_length(workerNodeList);
	double tasksPerNode = taskCount / ((double) workerNodeCount);
	int dependedJobCount = list_length(job->dependedJobList);

	MultiExecutorType executorType = TaskExecutorType;

	/* check if the first task is a modify task, short-circuit if so */
	if (taskCount > 0)
	{
		Task *firstTask = (Task *) linitial(workerTaskList);

		if (firstTask->taskType == MODIFY_TASK)
		{
			return MULTI_EXECUTOR_ROUTER;
		}
	}

	if (executorType == MULTI_EXECUTOR_REAL_TIME)
	{
		double reasonableConnectionCount = 0;

		/* if we need to open too many connections per worker, warn the user */
		if (tasksPerNode >= MaxConnections)
		{
			ereport(WARNING, (errmsg("this query uses more connections than the "
									 "configured max_connections limit"),
							  errhint("Consider increasing max_connections or setting "
									  "citus.task_executor_type to "
									  "\"task-tracker\".")));
		}

		/*
		 * If we need to open too many outgoing connections, warn the user.
		 * The real-time executor caps the number of tasks it starts by the same limit,
		 * but we still issue this warning because it degrades performance.
		 */
		reasonableConnectionCount = MaxMasterConnectionCount();
		if (taskCount >= reasonableConnectionCount)
		{
			ereport(WARNING, (errmsg("this query uses more file descriptors than the "
									 "configured max_files_per_process limit"),
							  errhint("Consider increasing max_files_per_process or "
									  "setting citus.task_executor_type to "
									  "\"task-tracker\".")));
		}

		/* if we have repartition jobs with real time executor, error out */
		if (dependedJobCount > 0)
		{
			ereport(ERROR, (errmsg("cannot use real time executor with repartition jobs"),
							errhint("Set citus.task_executor_type to "
									"\"task-tracker\".")));
		}
	}
	else if (executorType == MULTI_EXECUTOR_TASK_TRACKER)
	{
		/* if we have more tasks per node than what can be tracked, warn the user */
		if (tasksPerNode >= MaxTrackedTasksPerNode)
		{
			ereport(WARNING, (errmsg("this query assigns more tasks per node than the "
									 "configured max_tracked_tasks_per_node limit")));
		}
	}
	else if (executorType == MULTI_EXECUTOR_ROUTER)
	{
		Task *workerTask = NULL;
		List *workerDependentTaskList = NIL;
		bool masterQueryHasAggregates = false;

		/* if we have repartition jobs with router executor, error out */
		if (dependedJobCount > 0)
		{
			ereport(ERROR, (errmsg("cannot use router executor with repartition jobs"),
							errhint("Set citus.task_executor_type to "
									"\"task-tracker\".")));
		}

		/* if the query hits more than one shards, error out*/
		if (taskCount != 1)
		{
			ereport(ERROR, (errmsg("cannot use router executor with queries that "
								   "hit multiple shards"),
							errhint("Set citus.task_executor_type to \"real-time\" or "
									"\"task-tracker\".")));
		}

		/* if the query has dependent data fetch tasks */
		workerTask = list_nth(workerTaskList, 0);
		workerDependentTaskList = workerTask->dependedTaskList;
		if (list_length(workerDependentTaskList) > 0)
		{
			ereport(ERROR, (errmsg("cannot use router executor with JOINs"),
							errhint("Set citus.task_executor_type to \"real-time\" or "
									"\"task-tracker\".")));
		}

		/* ORDER BY is always applied on the master table with the current planner */
		if (masterQuery != NULL && list_length(masterQuery->sortClause) > 0)
		{
			ereport(ERROR, (errmsg("cannot use router executor with ORDER BY clauses"),
							errhint("Set citus.task_executor_type to \"real-time\" or "
									"\"task-tracker\".")));
		}

		/*
		 * Note that worker query having an aggregate means that the master query should have either
		 * an aggregate or a function expression which has to be executed for the correct results.
		 */
		masterQueryHasAggregates = job->jobQuery->hasAggs;
		if (masterQueryHasAggregates)
		{
			ereport(ERROR, (errmsg("cannot use router executor with aggregates"),
							errhint("Set citus.task_executor_type to \"real-time\" or "
									"\"task-tracker\".")));
		}
	}

	return executorType;
}


/*
 * MaxMasterConnectionCount returns the number of connections a master can open.
 * A master cannot create more than a certain number of file descriptors (FDs).
 * Every task requires 2 FDs, one file and one connection. Some FDs are taken by
 * the VFD pool and there is currently no way to reclaim these before opening a
 * connection. We therefore assume some FDs to be reserved for VFDs, based on
 * observing a typical size of the pool on a Citus master.
 */
int
MaxMasterConnectionCount(void)
{
	return Max((max_files_per_process - RESERVED_FD_COUNT) / 2, 1);
}


/*
 * RemoveJobDirectory gets automatically called at portal drop (end of query) or
 * at transaction abort. The function removes the job directory and releases the
 * associated job resource from the resource manager.
 */
void
RemoveJobDirectory(uint64 jobId)
{
	StringInfo jobDirectoryName = JobDirectoryName(jobId);
	RemoveDirectory(jobDirectoryName);

	ResourceOwnerForgetJobDirectory(CurrentResourceOwner, jobId);
}


/*
 * InitTaskExecution creates a task execution structure for the given task, and
 * initializes execution related fields.
 */
TaskExecution *
InitTaskExecution(Task *task, TaskExecStatus initialTaskExecStatus)
{
	/* each task placement (assignment) corresponds to one worker node */
	uint32 nodeCount = list_length(task->taskPlacementList);
	uint32 nodeIndex = 0;

	TaskExecution *taskExecution = palloc0(sizeof(TaskExecution));
	taskExecution->jobId = task->jobId;
	taskExecution->taskId = task->taskId;
	taskExecution->nodeCount = nodeCount;
	taskExecution->connectPollCount = 0;
	taskExecution->currentNodeIndex = 0;
	taskExecution->dataFetchTaskIndex = -1;
	taskExecution->failureCount = 0;

	taskExecution->taskStatusArray = palloc0(nodeCount * sizeof(TaskExecStatus));
	taskExecution->transmitStatusArray = palloc0(nodeCount * sizeof(TransmitExecStatus));
	taskExecution->connectionIdArray = palloc0(nodeCount * sizeof(int32));
	taskExecution->fileDescriptorArray = palloc0(nodeCount * sizeof(int32));

	for (nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++)
	{
		taskExecution->taskStatusArray[nodeIndex] = initialTaskExecStatus;
		taskExecution->transmitStatusArray[nodeIndex] = EXEC_TRANSMIT_UNASSIGNED;
		taskExecution->connectionIdArray[nodeIndex] = INVALID_CONNECTION_ID;
		taskExecution->fileDescriptorArray[nodeIndex] = -1;
	}

	return taskExecution;
}


/*
 * CleanupTaskExecution iterates over all connections and file descriptors for
 * the given task execution. The function first closes all open connections and
 * file descriptors, and then frees memory allocated for the task execution.
 */
void
CleanupTaskExecution(TaskExecution *taskExecution)
{
	uint32 nodeIndex = 0;
	for (nodeIndex = 0; nodeIndex < taskExecution->nodeCount; nodeIndex++)
	{
		int32 connectionId = taskExecution->connectionIdArray[nodeIndex];
		int32 fileDescriptor = taskExecution->fileDescriptorArray[nodeIndex];

		/* close open connection */
		if (connectionId != INVALID_CONNECTION_ID)
		{
			MultiClientDisconnect(connectionId);
			taskExecution->connectionIdArray[nodeIndex] = INVALID_CONNECTION_ID;
		}

		/* close open file */
		if (fileDescriptor >= 0)
		{
			int closed = close(fileDescriptor);
			taskExecution->fileDescriptorArray[nodeIndex] = -1;

			if (closed < 0)
			{
				ereport(WARNING, (errcode_for_file_access(),
								  errmsg("could not close copy file: %m")));
			}
		}
	}

	/* deallocate memory and reset all fields */
	pfree(taskExecution->taskStatusArray);
	pfree(taskExecution->connectionIdArray);
	pfree(taskExecution->fileDescriptorArray);
	memset(taskExecution, 0, sizeof(TaskExecution));
}


/* Determines if the given task exceeded its failure threshold. */
bool
TaskExecutionFailed(TaskExecution *taskExecution)
{
	if (taskExecution->failureCount >= MAX_TASK_EXECUTION_FAILURES)
	{
		return true;
	}

	return false;
}


/*
 * AdjustStateForFailure increments the failure count for given task execution.
 * The function also determines the next worker node that should be contacted
 * for remote execution.
 */
void
AdjustStateForFailure(TaskExecution *taskExecution)
{
	int maxNodeIndex = taskExecution->nodeCount - 1;
	Assert(maxNodeIndex >= 0);

	if (taskExecution->currentNodeIndex < maxNodeIndex)
	{
		taskExecution->currentNodeIndex++;   /* try next worker node */
	}
	else
	{
		taskExecution->currentNodeIndex = 0; /* go back to the first worker node */
	}

	taskExecution->dataFetchTaskIndex = -1; /* reset data fetch counter */
	taskExecution->failureCount++;          /* record failure */
}
