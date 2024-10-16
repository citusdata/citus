/*-------------------------------------------------------------------------
 *
 * directed_acyclic_graph_execution_logic.c
 *
 * Logic to run tasks in their dependency order.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"

#include "access/hash.h"

#include "distributed/adaptive_executor.h"
#include "distributed/directed_acyclic_graph_execution.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/transaction_management.h"
#include "distributed/transmit.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"

typedef struct TaskHashKey
{
	uint64 jobId;
	uint32 taskId;

	/*
	 * The padding field is needed to make sure the struct contains no
	 * automatic padding, which is not allowed for hashmap keys.
	 */
	uint32 padding;
}TaskHashKey;

typedef struct TaskHashEntry
{
	TaskHashKey key;
	Task *task;
}TaskHashEntry;

static bool IsAllDependencyCompleted(Task *task, HTAB *completedTasks);
static void AddCompletedTasks(List *curCompletedTasks, HTAB *completedTasks);
static List * FindExecutableTasks(List *allTasks, HTAB *completedTasks);
static List * RemoveMergeTasks(List *taskList);
static bool IsTaskAlreadyCompleted(Task *task, HTAB *completedTasks);

/*
 * ExecuteTasksInDependencyOrder executes the given tasks except the excluded
 * tasks in their dependency order. To do so, it iterates all
 * the tasks and finds the ones that can be executed at that time, it tries to
 * execute all of them in parallel. The parallelism is bound by MaxAdaptiveExecutorPoolSize.
 */
void
ExecuteTasksInDependencyOrder(List *allTasks, List *excludedTasks, List *jobIds)
{
	assert_valid_hash_key3(TaskHashKey, jobId, taskId, padding);
	HTAB *completedTasks = CreateSimpleHash(TaskHashKey, TaskHashEntry);

	/* We only execute depended jobs' tasks, therefore to not execute */
	/* top level tasks, we add them to the completedTasks. */
	AddCompletedTasks(excludedTasks, completedTasks);
	while (true)
	{
		List *curTasks = FindExecutableTasks(allTasks, completedTasks);
		if (list_length(curTasks) == 0)
		{
			break;
		}

		/* merge tasks do not need to be executed */
		List *executableTasks = RemoveMergeTasks(curTasks);
		if (list_length(executableTasks) > 0)
		{
			ExecuteTaskList(ROW_MODIFY_NONE, executableTasks);
		}

		AddCompletedTasks(curTasks, completedTasks);
		curTasks = NIL;
	}
}


/*
 * FindExecutableTasks finds the tasks that can be executed currently,
 * which means that all of their dependencies are executed. If a task
 * is already executed, it is not added to the result.
 */
static List *
FindExecutableTasks(List *allTasks, HTAB *completedTasks)
{
	List *curTasks = NIL;

	Task *task = NULL;
	foreach_declared_ptr(task, allTasks)
	{
		if (IsAllDependencyCompleted(task, completedTasks) &&
			!IsTaskAlreadyCompleted(task, completedTasks))
		{
			curTasks = lappend(curTasks, task);
		}
	}

	return curTasks;
}


/*
 * RemoveMergeTasks returns a copy of taskList that excludes all the
 * merge tasks. We do this because merge tasks are currently only a
 * logical concept that does not need to be executed.
 */
static List *
RemoveMergeTasks(List *taskList)
{
	List *prunedTaskList = NIL;
	Task *task = NULL;

	foreach_declared_ptr(task, taskList)
	{
		if (task->taskType != MERGE_TASK)
		{
			prunedTaskList = lappend(prunedTaskList, task);
		}
	}

	return prunedTaskList;
}


/*
 * AddCompletedTasks adds the givens tasks to completedTasks HTAB.
 */
static void
AddCompletedTasks(List *curCompletedTasks, HTAB *completedTasks)
{
	bool found;

	Task *task = NULL;
	foreach_declared_ptr(task, curCompletedTasks)
	{
		TaskHashKey taskKey = { task->jobId, task->taskId };
		hash_search(completedTasks, &taskKey, HASH_ENTER, &found);
	}
}


/*
 * IsTaskAlreadyCompleted returns true if the given task
 * is found in the completedTasks HTAB.
 */
static bool
IsTaskAlreadyCompleted(Task *task, HTAB *completedTasks)
{
	bool found;

	TaskHashKey taskKey = { task->jobId, task->taskId };
	hash_search(completedTasks, &taskKey, HASH_ENTER, &found);
	return found;
}


/*
 * IsAllDependencyCompleted return true if the given task's
 * dependencies are completed.
 */
static bool
IsAllDependencyCompleted(Task *targetTask, HTAB *completedTasks)
{
	bool found = false;

	Task *task = NULL;
	foreach_declared_ptr(task, targetTask->dependentTaskList)
	{
		TaskHashKey taskKey = { task->jobId, task->taskId };

		hash_search(completedTasks, &taskKey, HASH_FIND, &found);
		if (!found)
		{
			return false;
		}
	}
	return true;
}
