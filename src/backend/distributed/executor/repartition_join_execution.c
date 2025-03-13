/*-------------------------------------------------------------------------
 *
 * repartition_join_execution.c
 *
 * This file contains repartition specific logic.
 * ExecuteDependentTasks takes a list of top level tasks. Its logic is as follows:
 * - It generates all the tasks by descending in the tasks tree. Note that each task
 *  has a dependentTaskList.
 * - It generates FetchTask queryStrings with the MapTask queries. It uses the first replicate to
 *  fetch data when replication factor is > 1. Note that if a task fails in any replica adaptive executor
 *  gives an error, so if we come to a fetchTask we know for sure that its dependedMapTask is executed in all
 *  replicas.
 * - It creates schemas in each worker in a single transaction to store intermediate results.
 * - It iterates all tasks and finds the ones whose dependencies are already executed, and executes them with
 *  adaptive executor logic.
 *
 *
 * Repartition queries do not begin a transaction even if we are in
 * a transaction block. As we don't begin a transaction, they won't see the
 * DDLs that happened earlier in the transaction because we don't have that
 * transaction id with repartition queries. Therefore we error in this case.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "utils/builtins.h"

#include "distributed/adaptive_executor.h"
#include "distributed/directed_acyclic_graph_execution.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/repartition_join_execution.h"
#include "distributed/task_execution_utils.h"
#include "distributed/transaction_management.h"
#include "distributed/transmit.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"


static List * ExtractJobsInJobTree(Job *job);
static void TraverseJobTree(Job *curJob, List **jobs);


/*
 * ExecuteDependentTasks executes all tasks except the top level tasks
 * in order from the task tree. At a time, it can execute different tasks from
 * different jobs.
 */
List *
ExecuteDependentTasks(List *topLevelTasks, Job *topLevelJob)
{
	List *allTasks = CreateTaskListForJobTree(topLevelTasks);
	List *jobIds = ExtractJobsInJobTree(topLevelJob);

	ExecuteTasksInDependencyOrder(allTasks, topLevelTasks, jobIds);

	return jobIds;
}


/*
 * ExtractJobsInJobTree returns all job ids in the job tree
 * where the given job is root.
 */
static List *
ExtractJobsInJobTree(Job *job)
{
	List *jobIds = NIL;
	TraverseJobTree(job, &jobIds);
	return jobIds;
}


/*
 * TraverseJobTree does a dfs in the current job and adds
 * all of its job ids.
 */
static void
TraverseJobTree(Job *curJob, List **jobIds)
{
	uint64 *jobIdPointer = palloc(sizeof(uint64));
	*jobIdPointer = curJob->jobId;

	*jobIds = lappend(*jobIds, jobIdPointer);

	Job *childJob = NULL;
	foreach_declared_ptr(childJob, curJob->dependentJobList)
	{
		TraverseJobTree(childJob, jobIds);
	}
}
