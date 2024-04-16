
#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "pg_version_constants.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/citus_nodes.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/subplan_execution.h"
#include "distributed/task_execution_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"

/* TaskMapKey is used as a key in task hash */
typedef struct TaskMapKey
{
	TaskType taskType;
	uint32 taskId;
	uint64 jobId;
} TaskMapKey;


/*
 * TaskMapEntry is used as entry in task hash. We need to keep a pointer
 * of the task in the entry.
 */
typedef struct TaskMapEntry
{
	TaskMapKey key;
	Task *task;
} TaskMapEntry;

static Task * TaskHashEnter(HTAB *taskHash, Task *task);
static Task * TaskHashLookup(HTAB *trackerHash, TaskType taskType, uint64 jobId,
							 uint32 taskId);

/*
 * CreateTaskListForJobTree visits all tasks in the job tree (by following dependentTaskList),
 * starting with the given job's task list. The function then returns the list.
 */
List *
CreateTaskListForJobTree(List *jobTaskList)
{
	List *taskList = NIL;
	const int topLevelTaskHashSize = 32;
	int taskHashSize = list_length(jobTaskList) * topLevelTaskHashSize;
	assert_valid_hash_key3(TaskMapKey, taskType, taskId, jobId);
	HTAB *taskHash = CreateSimpleHashWithSize(TaskMapKey, TaskMapEntry, taskHashSize);

	/*
	 * We walk over the task tree using breadth-first search. For the search, we
	 * first queue top level tasks in the task tree.
	 */
	List *taskQueue = list_copy(jobTaskList);
	while (taskQueue != NIL)
	{
		/* pop first element from the task queue */
		Task *task = (Task *) linitial(taskQueue);
		taskQueue = list_delete_first(taskQueue);

		taskList = lappend(taskList, task);

		List *dependendTaskList = task->dependentTaskList;

		/*
		 * Push task node's children into the task queue, if and only if
		 * they're not already there. As task dependencies have to form a
		 * directed-acyclic-graph and are processed in a breadth-first search
		 * we can never re-encounter nodes we've already processed.
		 *
		 * While we're checking this, we can also fix the problem that
		 * copyObject() might have duplicated nodes in the graph - if a node
		 * isn't pushed to the graph because it is already planned to be
		 * visited, we can simply replace it with the copy. Note that, here
		 * we only consider dependend tasks. Since currently top level tasks
		 * cannot be on any dependend task list, we do not check them for duplicates.
		 *
		 * taskHash is used to reduce the complexity of keeping track of
		 * the tasks that are already encountered.
		 */
		ListCell *dependentTaskCell = NULL;
		foreach(dependentTaskCell, dependendTaskList)
		{
			Task *dependendTask = lfirst(dependentTaskCell);
			Task *dependendTaskInHash = TaskHashLookup(taskHash,
													   dependendTask->taskType,
													   dependendTask->jobId,
													   dependendTask->taskId);

			/*
			 * If the dependend task encountered for the first time, add it to the hash.
			 * Also, add this task to the task queue. Note that, we do not need to
			 * add the tasks to the queue which are already encountered, because
			 * they are already added to the queue.
			 */
			if (!dependendTaskInHash)
			{
				dependendTaskInHash = TaskHashEnter(taskHash, dependendTask);
				taskQueue = lappend(taskQueue, dependendTaskInHash);
			}

			/* update dependentTaskList element to the one which is in the hash */
			lfirst(dependentTaskCell) = dependendTaskInHash;
		}
	}

	return taskList;
}


/*
 * TaskHashEnter creates a reference to the task entry in the given task
 * hash. The function errors-out if the same key exists multiple times.
 */
static Task *
TaskHashEnter(HTAB *taskHash, Task *task)
{
	bool handleFound = false;

	TaskMapKey taskKey;
	memset(&taskKey, 0, sizeof(TaskMapKey));

	taskKey.taskType = task->taskType;
	taskKey.jobId = task->jobId;
	taskKey.taskId = task->taskId;

	void *hashKey = (void *) &taskKey;
	TaskMapEntry *taskInTheHash = (TaskMapEntry *) hash_search(taskHash, hashKey,
															   HASH_ENTER,
															   &handleFound);

	/* if same node appears twice, we error-out */
	if (handleFound)
	{
		ereport(ERROR, (errmsg("multiple entries for task: \"%d:" UINT64_FORMAT ":%u\"",
							   task->taskType, task->jobId, task->taskId)));
	}

	/* save the pointer to the original task in the hash */
	taskInTheHash->task = task;

	return task;
}


/*
 * TaskHashLookup looks for the tasks that corresponds to the given
 * taskType, jobId and taskId, and returns the found task, NULL otherwise.
 */
static Task *
TaskHashLookup(HTAB *taskHash, TaskType taskType, uint64 jobId, uint32 taskId)
{
	Task *task = NULL;
	bool handleFound = false;

	TaskMapKey taskKey;
	memset(&taskKey, 0, sizeof(TaskMapKey));

	taskKey.taskType = taskType;
	taskKey.jobId = jobId;
	taskKey.taskId = taskId;

	void *hashKey = (void *) &taskKey;
	TaskMapEntry *taskEntry = (TaskMapEntry *) hash_search(taskHash, hashKey, HASH_FIND,
														   &handleFound);

	if (taskEntry != NULL)
	{
		task = taskEntry->task;
	}

	return task;
}
