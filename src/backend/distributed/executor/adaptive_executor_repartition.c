/*-------------------------------------------------------------------------
 *
 * adaptive_executor_repartition.c
 *
 * Adaptive executor repartition's main entry point is ExecuteDependedTasks.
 * ExecuteDependedTasks takes a list of top level tasks. Its logic is as follows:
 * - It generates all the tasks by descending in the tasks tree. Note that each task
 *  has a dependedTaskList.
 * - It generates FetchTask queryStrings with the MapTask queries. It uses the first replicate to
 *  fetch data when replication factor is > 1. Note that if a task fails in any replica adaptive executor
 *  gives an error, so if we come to a fetchTask we know for sure that its dependedMapTask is executed in all
 *  replicas.
 * - It creates schemas in each worker in a single transaction to store intermediate results.
 * - It iterates all tasks and finds the ones whose dependencies are already executed, and executes them with
 *  adaptive executor logic.
 *
 *
 */

#include "postgres.h"
#include "access/hash.h"
#include "distributed/hash_helpers.h"

#include "distributed/multi_physical_planner.h"
#include "distributed/adaptive_executor.h"
#include "distributed/worker_manager.h"
#include "distributed/multi_server_executor.h"
#include "distributed/adaptive_executor_repartitioning.h"
#include "distributed/worker_transaction.h"
#include "distributed/multi_task_tracker_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/transmit.h"


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

static void FillTaskGroups(List **allTasks, List **outputFetchTasks, List **mergeTasks);
static StringInfo MapFetchTaskQueryString(Task *mapFetchTask, Task *mapTask);
static void PutMapOutputFetchQueryStrings(List **mapOutputFetchTasks);
static void CreateTemporarySchemas(List *mergeTasks);
static List * CreateJobIds(List *mergeTasks);
static void CreateSchemasOnAllWorkers(char *createSchemasCommand);
static char * GenerateCreateSchemasCommand(List *jobIds);
static bool DoesJobIDExist(List *jobIds, uint64 jobId);
static HASHCTL InitHashTableInfo(void);
static HTAB * CreateTaskHashTable(void);
static void FillTaskKey(TaskHashKey *taskKey, Task *task);
static bool IsAllDependencyCompleted(Task *task, HTAB *completedTasks);
static void AddCompletedTasks(List *curCompletedTasks, HTAB *completedTasks);
static void ExecuteTasksInDependencyOrder(List *allTasks, List *topLevelTasks);
static List * FindExecutableTasks(List *allTasks, HTAB *completedTasks);
static int TaskHashCompare(const void *key1, const void *key2, Size keysize);
static uint32 TaskHash(const void *key, Size keysize);
static bool IsTaskAlreadyCompleted(Task *task, HTAB *completedTasks);
static void SendCommandToAllWorkers(List *commandList);


/*
 * ExecuteDependedTasks executes all tasks except the top level tasks
 * in order from the task tree. At a time, it can execute different tasks from
 * different jobs.
 */
void
ExecuteDependedTasks(List *topLevelTasks)
{
	List *allTasks = NIL;

	List *mapOutputFetchTasks = NIL;
	List *mergeTasks = NIL;

	allTasks = TaskAndExecutionList(topLevelTasks);

	FillTaskGroups(&allTasks, &mapOutputFetchTasks, &mergeTasks);
	PutMapOutputFetchQueryStrings(&mapOutputFetchTasks);

	CreateTemporarySchemas(mergeTasks);

	ExecuteTasksInDependencyOrder(allTasks, topLevelTasks);
}


/*
 * FillTaskGroups iterates all tasks and creates a group for outputFetchTasks
 * and mergeTasks.
 */
static void
FillTaskGroups(List **allTasks, List **outputFetchTasks, List **mergeTasks)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, *allTasks)
	{
		Task *task = (Task *) lfirst(taskCell);

		if (task->taskType == MAP_OUTPUT_FETCH_TASK)
		{
			*outputFetchTasks = lappend(*outputFetchTasks, task);
		}
		if (task->taskType == MERGE_TASK)
		{
			*mergeTasks = lappend(*mergeTasks, task);
		}
	}
}


/*
 * PutMapOutputFetchQueryStrings adds the queryStrings for fetchTasks from their mapTasks.
 * Note that it is not created during the planner, but it is probably safe
 * to create the queryStrings for fetchTasks in the planner phase too.
 */
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

	ShardPlacement *mapTaskPlacement = linitial(mapTaskPlacementList);
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
 * CreateTemporarySchemas creates the necessary schemas that will be used
 * later in each worker. Single transaction is used to create the schemas.
 */
static void
CreateTemporarySchemas(List *mergeTasks)
{
	List *jobIds = CreateJobIds(mergeTasks);
	char *createSchemasCommand = GenerateCreateSchemasCommand(jobIds);
	CreateSchemasOnAllWorkers(createSchemasCommand);
}


/*
 * CreateJobIds returns a list of unique job ids that will be used
 * in mergeTasks.
 */
static List *
CreateJobIds(List *mergeTasks)
{
	ListCell *taskCell = NULL;
	List *jobIds = NIL;

	foreach(taskCell, mergeTasks)
	{
		Task *task = (Task *) lfirst(taskCell);
		if (!DoesJobIDExist(jobIds, task->jobId))
		{
			jobIds = lappend(jobIds, (void *) task->jobId);
		}
	}
	return jobIds;
}


/*
 * CreateSchemasOnAllWorkers creates schemas in all workers.
 */
static void
CreateSchemasOnAllWorkers(char *createSchemasCommand)
{
	List *commandList = list_make1(createSchemasCommand);

	SendCommandToAllWorkers(commandList);
}


/*
 * SendCommandToAllWorkers sends the given command to all workers in
 * a single transaction.
 */
static void
SendCommandToAllWorkers(List *commandList)
{
	ListCell *workerNodeCell = NULL;
	char *extensionOwner = CitusExtensionOwnerName();
	List *workerNodeList = ReadWorkerNodes(false);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		SendCommandListToWorkerInSingleTransaction(workerNode->workerName,
												   workerNode->workerPort, extensionOwner,
												   commandList);
	}
}


/*
 * GenerateCreateSchemasCommand returns the command to generate
 * schemas. The returned command is a concatenated which contains
 * exactly list_length(jobIds) subcommands.
 *  E.g create_schema(jobId1); create_schema(jobId2); ...
 * This way we can send the command in just one latency to a worker to
 * create all the necessary schemas.
 */
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
DoesJobIDExist(List *jobIds, uint64 jobId)
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


/*
 * ExecuteTasksInDependencyOrder executes the given tasks except the top
 * level tasks in their dependency order. To do so, it iterates all
 * the tasks and finds the ones that can be executed at that time, it tries to
 * execute all of them in parallel. The parallelism is bound by MaxAdaptiveExecutorPoolSize.
 */
static void
ExecuteTasksInDependencyOrder(List *allTasks, List *topLevelTasks)
{
	HTAB *completedTasks = CreateTaskHashTable();

	/* We only execute depended jobs' tasks, therefore to not execute */
	/* top level tasks, we add them to the completedTasks. */
	AddCompletedTasks(topLevelTasks, completedTasks);
	while (true)
	{
		List *curTasks = FindExecutableTasks(allTasks, completedTasks);
		if (list_length(curTasks) == 0)
		{
			break;
		}

		ExecuteTaskListRepartition(ROW_MODIFY_NONE, curTasks,
								   MaxAdaptiveExecutorPoolSize);
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
	ListCell *taskCell = NULL;
	TaskHashKey taskKey;

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

	return curTasks;
}


/*
 * AddCompletedTasks adds the givens tasks to completedTasks HTAB.
 */
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
	TaskHashKey taskKey;
	bool found;

	FillTaskKey(&taskKey, task);
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


/*
 * CleanUpSchemas removes all the schemas that start with pg_
 * in every worker.
 */
void
CleanUpSchemas()
{
	List *commandList = list_make1(JOB_SCHEMA_CLEANUP);
	SendCommandToAllWorkers(commandList);
}
