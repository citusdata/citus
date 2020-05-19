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
#include "distributed/hash_helpers.h"

#include "distributed/adaptive_executor.h"
#include "distributed/directed_acyclic_graph_execution.h"
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
}TaskHashKey;

typedef struct TaskHashEntry
{
	TaskHashKey key;
	Task *task;
}TaskHashEntry;

static HASHCTL InitHashTableInfo(void);
static HTAB * CreateTaskHashTable(void);
static bool IsAllDependencyCompleted(Task *task, HTAB *completedTasks);
static void AddCompletedTasks(List *curCompletedTasks, HTAB *completedTasks);
static List * FindExecutableTasks(List *allTasks, HTAB *completedTasks);
static int TaskHashCompare(const void *key1, const void *key2, Size keysize);
static uint32 TaskHash(const void *key, Size keysize);
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
	HTAB *completedTasks = CreateTaskHashTable();

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
		ExecuteTaskListOutsideTransaction(ROW_MODIFY_NONE, curTasks,
										  MaxAdaptiveExecutorPoolSize, jobIds);

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
	foreach_ptr(task, allTasks)
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
 * AddCompletedTasks adds the givens tasks to completedTasks HTAB.
 */
static void
AddCompletedTasks(List *curCompletedTasks, HTAB *completedTasks)
{
	bool found;

	Task *task = NULL;
	foreach_ptr(task, curCompletedTasks)
	{
		TaskHashKey taskKey = { task->jobId, task->taskId };
		hash_search(completedTasks, &taskKey, HASH_ENTER, &found);
	}
}


/*
 * CreateTaskHashTable creates a HTAB with the necessary initialization.
 */
static HTAB *
CreateTaskHashTable()
{
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);
	HASHCTL info = InitHashTableInfo();
	return hash_create("citus task completed list (jobId, taskId)",
					   64, &info, hashFlags);
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
	foreach_ptr(task, targetTask->dependentTaskList)
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


/*
 * InitHashTableInfo returns hash table info, the hash table is
 * configured to be created in the CurrentMemoryContext so that
 * it will be cleaned when this memory context gets freed/reset.
 */
static HASHCTL
InitHashTableInfo()
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TaskHashKey);
	info.entrysize = sizeof(TaskHashEntry);
	info.hash = TaskHash;
	info.match = TaskHashCompare;
	info.hcxt = CurrentMemoryContext;

	return info;
}


static uint32
TaskHash(const void *key, Size keysize)
{
	TaskHashKey *taskKey = (TaskHashKey *) key;
	uint32 hash = 0;

	hash = hash_combine(hash, hash_any((unsigned char *) &taskKey->jobId,
									   sizeof(int64)));
	hash = hash_combine(hash, hash_uint32(taskKey->taskId));

	return hash;
}


static int
TaskHashCompare(const void *key1, const void *key2, Size keysize)
{
	TaskHashKey *taskKey1 = (TaskHashKey *) key1;
	TaskHashKey *taskKey2 = (TaskHashKey *) key2;
	if (taskKey1->jobId != taskKey2->jobId || taskKey1->taskId != taskKey2->taskId)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
