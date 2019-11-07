#include "postgres.h"
#include "access/hash.h"
#include "distributed/hash_helpers.h"

#include "distributed/multi_physical_planner.h"
#include "distributed/adaptive_executor.h"
#include "distributed/worker_manager.h"
#include "distributed/multi_server_executor.h"
#include "distributed/adaptive_executor_repartitioning.h"
#include "distributed/worker_transaction.h"
#include "distributed/metadata_cache.h"


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


/* TaskMapKey is used as a key in task hash */
typedef struct TaskMapKey
{
	TaskType taskType;
	uint64 jobId;
	uint32 taskId;
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

static void FillTaskGroups(List **allTasks, List **mapTasks, List **outputFetchTasks,
						   List **mergeTasks, List **mergeFetchTasks);
static StringInfo MapFetchTaskQueryString(Task *mapFetchTask, Task *mapTask);
static void PutMapOutputFetchQueryStrings(List **mapOutputFetchTasks);
static void CreateTemporarySchemas(List *mergeTasks);
static List * CreateJobIds(List *mergeTasks);
static void CreateSchemasOnAllWorkers(char *createSchemasCommand);
static char * GenerateCreateSchemasCommand(List *jobIds);
static bool doesJobIDExist(List *jobIds, uint64 jobId);
static HASHCTL InitHashTableInfo();
static HTAB * CreateTaskHashTable();
static void FillTaskKey(TaskHashKey *taskKey, Task *task);
static bool IsAllDependencyCompleted(Task *task, HTAB *completedTasks);
static void AddCompletedTasks(List *curCompletedTasks, HTAB *completedTasks);
static void ExecuteTasksInDependencyOrder(List *allTasks, List *topLevelTasks);
static int TaskHashCompare(const void *key1, const void *key2, Size keysize);
static uint32 TaskHash(const void *key, Size keysize);
static bool IsTaskAlreadyCompleted(Task *task, HTAB *completedTasks);
static List * TaskAndExecutionList(List *jobTaskList);
static HTAB * TaskHashCreate(uint32 taskHashSize);
static Task * TaskHashLookup(HTAB *taskHash, TaskType taskType, uint64 jobId, uint32
							 taskId);
static Task * TaskHashEnter(HTAB *taskHash, Task *task);							 

void
ExecuteDependedTasks(List *topLevelTasks)
{
	List *allTasks = NIL;

	List *mapTasks = NIL;
	List *mapOutputFetchTasks = NIL;
	List *mergeTasks = NIL;
	List *mergeFetchTasks = NIL;

	allTasks = TaskAndExecutionList(topLevelTasks);

	FillTaskGroups(&allTasks, &mapTasks, &mapOutputFetchTasks, &mergeTasks,
				   &mergeFetchTasks);
	PutMapOutputFetchQueryStrings(&mapOutputFetchTasks);

	CreateTemporarySchemas(mergeTasks);

	ExecuteTasksInDependencyOrder(allTasks, topLevelTasks);
}


static void
ExecuteTasksInDependencyOrder(List *allTasks, List *topLevelTasks)
{
	List *curTasks = NIL;
	ListCell *taskCell = NULL;
	TaskHashKey taskKey;

	HTAB *completedTasks = CreateTaskHashTable();
	/* We only execute depended jobs' tasks, therefore to not execute */
	/* top level tasks, we add them to the completedTasks. */
	AddCompletedTasks(topLevelTasks, completedTasks);
	while (true)
	{
		foreach(taskCell, allTasks)
		{
			Task *task = (Task *) lfirst(taskCell);
			FillTaskKey(&taskKey, task);

			if (IsAllDependencyCompleted(task, completedTasks) &&
				!IsTaskAlreadyCompleted(task, completedTasks))
			{
				curTasks = lappend(curTasks, task);
			}
		}

		if (list_length(curTasks) == 0)
		{
			break;
		}
		ExecuteTaskList(ROW_MODIFY_NONE, curTasks, MaxAdaptiveExecutorPoolSize);
		AddCompletedTasks(curTasks, completedTasks);
		curTasks = NIL;
	}
}


static bool
IsTaskAlreadyCompleted(Task *task, HTAB *completedTasks)
{
	TaskHashKey taskKey;
	bool found;

	FillTaskKey(&taskKey, task);
	hash_search(completedTasks, &taskKey, HASH_ENTER, &found);
	return found;
}


static void
AddCompletedTasks(List *curCompletedTasks, HTAB *completedTasks)
{
	ListCell *taskCell = NULL;
	TaskHashKey taskKey;
	bool found;

	foreach(taskCell, curCompletedTasks)
	{
		Task *task = (Task *) lfirst(taskCell);
		FillTaskKey(&taskKey, task);
		hash_search(completedTasks, &taskKey, HASH_ENTER, &found);
	}
}


static bool
IsAllDependencyCompleted(Task *targetTask, HTAB *completedTasks)
{
	ListCell *taskCell = NULL;
	bool found = false;
	TaskHashKey taskKey;

	foreach(taskCell, targetTask->dependedTaskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		FillTaskKey(&taskKey, task);

		hash_search(completedTasks, &taskKey, HASH_FIND, &found);
		if (!found)
		{
			return false;
		}
	}
	return true;
}


static void
FillTaskKey(TaskHashKey *taskKey, Task *task)
{
	taskKey->jobId = task->jobId;
	taskKey->taskId = task->taskId;
}


static HTAB *
CreateTaskHashTable()
{
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);
	HASHCTL info = InitHashTableInfo();
	return hash_create("citus task completed list (jobId, taskId)",
					   64, &info, hashFlags);
}


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

	hash = hash_combine(hash, hash_uint32((uint32) taskKey->jobId));
	hash = hash_combine(hash, hash_uint32(taskKey->taskId));

	return hash;
}


static int
TaskHashCompare(const void *key1, const void *key2, Size keysize)
{
	TaskHashKey *taskKey1 = (TaskHashKey *) key1;
	TaskHashKey *taskKey2 = (TaskHashKey *) key2;
	return taskKey1->jobId != taskKey2->jobId || taskKey1->taskId != taskKey2->taskId;
}

static void
CreateTemporarySchemas(List *mergeTasks)
{
	List *jobIds = CreateJobIds(mergeTasks);
	char *createSchemasCommand = GenerateCreateSchemasCommand(jobIds);
	CreateSchemasOnAllWorkers(createSchemasCommand);
}


static List *
CreateJobIds(List *mergeTasks)
{
	ListCell *taskCell = NULL;
	List *jobIds = NIL;

	foreach(taskCell, mergeTasks)
	{
		Task *task = (Task *) lfirst(taskCell);
		if (!doesJobIDExist(jobIds, task->jobId))
		{
			jobIds = lappend(jobIds, (void *) task->jobId);
		}
	}
	return jobIds;
}


static void
CreateSchemasOnAllWorkers(char *createSchemasCommand)
{
	ListCell *workerNodeCell = NULL;
	char *extensionOwner = CitusExtensionOwnerName();
	List *workerNodeList = ActiveReadableNodeList();
	List *commandList = list_make1(createSchemasCommand);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		SendCommandListToWorkerInSingleTransaction(workerNode->workerName,
												   workerNode->workerPort, extensionOwner,
												   commandList);
	}
}


static char *
GenerateCreateSchemasCommand(List *jobIds)
{
	StringInfo createSchemaCommand = makeStringInfo();
	ListCell *jobIdCell = NULL;

	foreach(jobIdCell, jobIds)
	{
		uint64 jobId = (uint64) lfirst(jobIdCell);
		appendStringInfo(createSchemaCommand, WORKER_CREATE_SCHEMA_QUERY, jobId);
	}
	return createSchemaCommand->data;
}


static bool
doesJobIDExist(List *jobIds, uint64 jobId)
{
	ListCell *jobIdCell = NULL;
	foreach(jobIdCell, jobIds)
	{
		uint64 curJobId = (uint64) lfirst(jobIdCell);
		if (curJobId == jobId)
		{
			return true;
		}
	}
	return false;
}


static void
FillTaskGroups(List **allTasks, List **mapTasks, List **outputFetchTasks,
			   List **mergeTasks, List **mergeFetchTasks)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, *allTasks)
	{
		Task *task = (Task *) lfirst(taskCell);

		if (task->taskType == MAP_TASK)
		{
			*mapTasks = lappend(*mapTasks, task);
		}
		if (task->taskType == MAP_OUTPUT_FETCH_TASK)
		{
			*outputFetchTasks = lappend(*outputFetchTasks, task);
		}
		if (task->taskType == MERGE_TASK)
		{
			*mergeTasks = lappend(*mergeTasks, task);
		}
		if (task->taskType == MERGE_FETCH_TASK)
		{
			*mergeFetchTasks = lappend(*mergeFetchTasks, task);
		}
	}
}


static void
PutMapOutputFetchQueryStrings(List **mapOutputFetchTasks)
{
	ListCell *taskCell = NULL;
	foreach(taskCell, *mapOutputFetchTasks)
	{
		Task *task = (Task *) lfirst(taskCell);
		StringInfo mapFetchTaskQueryString = NULL;
		Task *mapTask = (Task *) linitial(task->dependedTaskList);

		mapFetchTaskQueryString = MapFetchTaskQueryString(task, mapTask);
		task->queryString = mapFetchTaskQueryString->data;
	}
}


/*
 * MapFetchTaskQueryString constructs the map fetch query string from the given
 * map output fetch task and its downstream map task dependency. The constructed
 * query string allows fetching the map task's partitioned output file from the
 * worker node it's created to the worker node that will execute the merge task.
 */
static StringInfo
MapFetchTaskQueryString(Task *mapFetchTask, Task *mapTask)
{
	StringInfo mapFetchQueryString = NULL;
	uint32 partitionFileId = mapFetchTask->partitionId;
	uint32 mergeTaskId = mapFetchTask->upstreamTaskId;

	/* find the node name/port for map task's execution */
	List *mapTaskPlacementList = mapTask->taskPlacementList;
	TaskExecution *mapTaskExecution = mapTask->taskExecution;
	uint32 currentIndex = mapTaskExecution->currentNodeIndex;

	ShardPlacement *mapTaskPlacement = list_nth(mapTaskPlacementList, currentIndex);
	char *mapTaskNodeName = mapTaskPlacement->nodeName;
	uint32 mapTaskNodePort = mapTaskPlacement->nodePort;

	Assert(mapFetchTask->taskType == MAP_OUTPUT_FETCH_TASK);
	Assert(mapTask->taskType == MAP_TASK);

	mapFetchQueryString = makeStringInfo();
	appendStringInfo(mapFetchQueryString, MAP_OUTPUT_FETCH_COMMAND,
					 mapTask->jobId, mapTask->taskId, partitionFileId,
					 mergeTaskId, /* fetch results to merge task */
					 mapTaskNodeName, mapTaskNodePort);

	return mapFetchQueryString;
}


/*
 * TaskAndExecutionList visits all tasks in the job tree, starting with the given
 * job's task list. For each visited task, the function creates a task execution
 * struct, associates the task execution with the task, and adds the task and its
 * execution to a list. The function then returns the list.
 */
static List *
TaskAndExecutionList(List *jobTaskList)
{
	List *taskAndExecutionList = NIL;
	List *taskQueue = NIL;
	const int topLevelTaskHashSize = 32;
	int taskHashSize = list_length(jobTaskList) * topLevelTaskHashSize;
	HTAB *taskHash = TaskHashCreate(taskHashSize);

	/*
	 * We walk over the task tree using breadth-first search. For the search, we
	 * first queue top level tasks in the task tree.
	 */
	taskQueue = list_copy(jobTaskList);
	while (taskQueue != NIL)
	{
		TaskExecution *taskExecution = NULL;
		List *dependendTaskList = NIL;
		ListCell *dependedTaskCell = NULL;

		/* pop first element from the task queue */
		Task *task = (Task *) linitial(taskQueue);
		taskQueue = list_delete_first(taskQueue);

		/* create task execution and associate it with task */
		taskExecution = InitTaskExecution(task, EXEC_TASK_UNASSIGNED);
		task->taskExecution = taskExecution;

		taskAndExecutionList = lappend(taskAndExecutionList, task);

		dependendTaskList = task->dependedTaskList;

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
		foreach(dependedTaskCell, dependendTaskList)
		{
			Task *dependendTask = lfirst(dependedTaskCell);
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

			/* update dependedTaskList element to the one which is in the hash */
			lfirst(dependedTaskCell) = dependendTaskInHash;
		}
	}

	return taskAndExecutionList;
}


/*
 * TaskHashEnter creates a reference to the task entry in the given task
 * hash. The function errors-out if the same key exists multiple times.
 */
static Task *
TaskHashEnter(HTAB *taskHash, Task *task)
{
	void *hashKey = NULL;
	TaskMapEntry *taskInTheHash = NULL;
	bool handleFound = false;

	TaskMapKey taskKey;
	memset(&taskKey, 0, sizeof(TaskMapKey));

	taskKey.taskType = task->taskType;
	taskKey.jobId = task->jobId;
	taskKey.taskId = task->taskId;

	hashKey = (void *) &taskKey;
	taskInTheHash = (TaskMapEntry *) hash_search(taskHash, hashKey, HASH_ENTER,
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
	TaskMapEntry *taskEntry = NULL;
	Task *task = NULL;
	void *hashKey = NULL;
	bool handleFound = false;

	TaskMapKey taskKey;
	memset(&taskKey, 0, sizeof(TaskMapKey));

	taskKey.taskType = taskType;
	taskKey.jobId = jobId;
	taskKey.taskId = taskId;

	hashKey = (void *) &taskKey;
	taskEntry = (TaskMapEntry *) hash_search(taskHash, hashKey, HASH_FIND, &handleFound);

	if (taskEntry != NULL)
	{
		task = taskEntry->task;
	}

	return task;
}


/*
 * TaskHashCreate allocates memory for a task hash, initializes an
 * empty hash, and returns this hash.
 */
static HTAB *
TaskHashCreate(uint32 taskHashSize)
{
	HASHCTL info;
	const char *taskHashName = "Task Hash";
	int hashFlags = 0;
	HTAB *taskHash = NULL;

	/*
	 * Can't create a hashtable of size 0. Normally that shouldn't happen, but
	 * shard pruning currently can lead to this (Job with 0 Tasks). See #833.
	 */
	if (taskHashSize == 0)
	{
		taskHashSize = 2;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TaskMapKey);
	info.entrysize = sizeof(TaskMapEntry);
	info.hash = tag_hash;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	taskHash = hash_create(taskHashName, taskHashSize, &info, hashFlags);

	return taskHash;
}
