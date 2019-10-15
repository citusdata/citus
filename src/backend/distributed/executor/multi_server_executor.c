/*-------------------------------------------------------------------------
 *
 * multi_server_executor.c
 *
 * Function definitions for distributed task execution for real-time
 * and task-tracker executors, and routines common to both. The common
 * routines are implement backend-side logic; and they trigger executions
 * on the client-side via function hooks that they load.
 *
 * Copyright (c) 2012-2019, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <unistd.h>

#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "utils/lsyscache.h"

int RemoteTaskCheckInterval = 100; /* per cycle sleep interval in millisecs */
int TaskExecutorType = MULTI_EXECUTOR_ADAPTIVE; /* distributed executor type */
bool BinaryMasterCopyFormat = false; /* copy data from workers in binary format */
bool EnableRepartitionJoins = false;


/*
 * JobExecutorType selects the executor type for the given distributedPlan using the task
 * executor type config value. The function then checks if the given distributedPlan needs
 * more resources than those provided to it by other config values, and issues
 * warnings accordingly. If the selected executor type cannot execute the given
 * distributedPlan, the function errors out.
 */
MultiExecutorType
JobExecutorType(DistributedPlan *distributedPlan)
{
	Job *job = distributedPlan->workerJob;
	List *workerNodeList = NIL;
	int workerNodeCount = 0;
	int taskCount = 0;
	double tasksPerNode = 0.;
	MultiExecutorType executorType = TaskExecutorType;
	bool routerExecutablePlan = distributedPlan->routerExecutable;

	/* check if can switch to router executor */
	if (routerExecutablePlan)
	{
		if (log_min_messages <= DEBUG2 || client_min_messages <= DEBUG2)
		{
			Const *partitionValueConst = job->partitionKeyValue;

			if (partitionValueConst != NULL && !partitionValueConst->constisnull)
			{
				Datum partitionColumnValue = partitionValueConst->constvalue;
				Oid partitionColumnType = partitionValueConst->consttype;
				char *partitionColumnString = DatumToString(partitionColumnValue,
															partitionColumnType);

				ereport(DEBUG2, (errmsg("Plan is router executable"),
								 errdetail("distribution column value: %s",
										   ApplyLogRedaction(partitionColumnString))));
			}
			else
			{
				ereport(DEBUG2, (errmsg("Plan is router executable")));
			}
		}

		if (TaskExecutorType == MULTI_EXECUTOR_ADAPTIVE)
		{
			return TaskExecutorType;
		}

		return MULTI_EXECUTOR_ROUTER;
	}

	if (distributedPlan->insertSelectSubquery != NULL)
	{
		/*
		 * Even if adaptiveExecutorEnabled, we go through
		 * MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT because
		 * the executor already knows how to handle adaptive
		 * executor when necessary.
		 */
		return MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT;
	}

	Assert(distributedPlan->modLevel == ROW_MODIFY_READONLY);

	workerNodeList = ActiveReadableNodeList();
	workerNodeCount = list_length(workerNodeList);
	taskCount = list_length(job->taskList);
	tasksPerNode = taskCount / ((double) workerNodeCount);

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
	}

	if (executorType == MULTI_EXECUTOR_REAL_TIME ||
		executorType == MULTI_EXECUTOR_ADAPTIVE)
	{
		/* if we have repartition jobs with real time executor and repartition
		 * joins are not enabled, error out. Otherwise, switch to task-tracker
		 */
		int dependedJobCount = list_length(job->dependedJobList);
		if (dependedJobCount > 0)
		{
			if (!EnableRepartitionJoins)
			{
				ereport(ERROR, (errmsg(
									"the query contains a join that requires repartitioning"),
								errhint("Set citus.enable_repartition_joins to on "
										"to enable repartitioning")));
			}

			ereport(DEBUG1, (errmsg(
								 "cannot use real time executor with repartition jobs"),
							 errhint("Since you enabled citus.enable_repartition_joins "
									 "Citus chose to use task-tracker.")));
			return MULTI_EXECUTOR_TASK_TRACKER;
		}
	}
	else
	{
		/* if we have more tasks per node than what can be tracked, warn the user */
		if (tasksPerNode >= MaxTrackedTasksPerNode)
		{
			ereport(WARNING, (errmsg("this query assigns more tasks per node than the "
									 "configured max_tracked_tasks_per_node limit")));
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
	StringInfo jobDirectoryName = MasterJobDirectoryName(jobId);
	CitusRemoveDirectory(jobDirectoryName);

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

	TaskExecution *taskExecution = CitusMakeNode(TaskExecution);

	taskExecution->jobId = task->jobId;
	taskExecution->taskId = task->taskId;
	taskExecution->nodeCount = nodeCount;
	taskExecution->connectStartTime = 0;
	taskExecution->currentNodeIndex = 0;
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
	pfree(taskExecution);
}


/* Determines if the given task exceeded its failure threshold. */
bool
TaskExecutionFailed(TaskExecution *taskExecution)
{
	if (taskExecution->criticalErrorOccurred)
	{
		return true;
	}

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

	taskExecution->failureCount++;          /* record failure */
}


/*
 * CheckIfSizeLimitIsExceeded checks if the limit is exceeded by intermediate
 * results, if there is any.
 */
bool
CheckIfSizeLimitIsExceeded(DistributedExecutionStats *executionStats)
{
	uint64 maxIntermediateResultInBytes = 0;

	if (!SubPlanLevel || MaxIntermediateResult < 0)
	{
		return false;
	}

	maxIntermediateResultInBytes = MaxIntermediateResult * 1024L;
	if (executionStats->totalIntermediateResultSize < maxIntermediateResultInBytes)
	{
		return false;
	}

	return true;
}


/*
 * This function is called when the intermediate result size limitation is
 * exceeded. It basically errors out with a detailed explanation.
 */
void
ErrorSizeLimitIsExceeded()
{
	ereport(ERROR, (errmsg("the intermediate result size exceeds "
						   "citus.max_intermediate_result_size (currently %d kB)",
						   MaxIntermediateResult),
					errdetail("Citus restricts the size of intermediate "
							  "results of complex subqueries and CTEs to "
							  "avoid accidentally pulling large result sets "
							  "into once place."),
					errhint("To run the current query, set "
							"citus.max_intermediate_result_size to a higher"
							" value or -1 to disable.")));
}
