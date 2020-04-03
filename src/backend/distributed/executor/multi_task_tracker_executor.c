/*-------------------------------------------------------------------------
 *
 * multi_task_tracker_executor.c
 *
 * Routines for executing remote tasks as part of a distributed execution plan
 * using task trackers. These task trackers receive task assignments from this
 * executor, and they manage task executions on worker nodes. The use of task
 * trackers brings us two benefits: (a) distributed execution plans can scale
 * out to many tasks, as the executor no longer needs to keep a connection open
 * for each task, and (b) distributed execution plans can include map/reduce
 * execution primitives, which involve writing intermediate results to files.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <sys/stat.h>
#include <unistd.h>
#include <math.h>

#include "commands/dbcommands.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_nodes.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_resowner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "distributed/multi_task_tracker_executor.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


int MaxAssignTaskBatchSize = 64; /* maximum number of tasks to assign per round */
int MaxTaskStatusBatchSize = 64; /* maximum number of tasks status checks per round */


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


/* Local functions forward declarations to init tasks and trackers */

static HTAB * TaskHashCreate(uint32 taskHashSize);
static Task * TaskHashEnter(HTAB *taskHash, Task *task);
static Task * TaskHashLookup(HTAB *trackerHash, TaskType taskType, uint64 jobId,
							 uint32 taskId);
static bool TopLevelTask(Task *task);
static bool TransmitExecutionCompleted(TaskExecution *taskExecution);
static HTAB * TrackerHash(const char *taskTrackerHashName, List *workerNodeList,
						  char *userName);
static HTAB * TrackerHashCreate(const char *taskTrackerHashName,
								uint32 taskTrackerHashSize);
static TaskTracker * TrackerHashEnter(HTAB *taskTrackerHash, char *nodeName,
									  uint32 nodePort);
static void TrackerHashConnect(HTAB *taskTrackerHash);
static TrackerStatus TrackerConnectPoll(TaskTracker *taskTracker);
static TaskTracker * ResolveTaskTracker(HTAB *trackerHash, Task *task,
										TaskExecution *taskExecution);
static TaskTracker * ResolveMapTaskTracker(HTAB *trackerHash, Task *task,
										   TaskExecution *taskExecution);
static TaskTracker * TrackerHashLookup(HTAB *trackerHash, const char *nodeName,
									   uint32 nodePort);
static void PrepareMasterJobDirectory(Job *workerJob);

/* Local functions forward declarations to manage tasks and their assignments */
static TaskExecStatus ManageTaskExecution(TaskTracker *taskTracker,
										  TaskTracker *sourceTaskTracker,
										  Task *task, TaskExecution *taskExecution);
static TransmitExecStatus ManageTransmitExecution(TaskTracker *transmitTracker,
												  Task *task,
												  TaskExecution *taskExecution,
												  DistributedExecutionStats *
												  executionStats);
static bool TaskExecutionsCompleted(List *taskList);
static StringInfo MapFetchTaskQueryString(Task *mapFetchTask, Task *mapTask);
static void TrackerQueueSqlTask(TaskTracker *taskTracker, Task *task);
static void TrackerQueueTask(TaskTracker *taskTracker, Task *task);
static StringInfo TaskAssignmentQuery(Task *task, char *queryString);
static TaskStatus TrackerTaskStatus(TaskTracker *taskTracker, Task *task);
static TrackerTaskState * TrackerTaskStateHashLookup(HTAB *taskStateHash, Task *task);
static bool TrackerHealthy(TaskTracker *taskTracker);
static void TrackerQueueFileTransmit(TaskTracker *transmitTracker, Task *task);
static TrackerTaskState * TaskStateHashEnter(HTAB *taskStateHash, uint64 jobId,
											 uint32 taskId);
static int32 TransmitTrackerConnectionId(TaskTracker *transmitTracker, Task *task);

/* Local functions forward declarations to manage task failovers */
static List * ConstrainedTaskList(List *taskAndExecutionList, Task *task);
static List * ConstrainedNonMergeTaskList(List *taskAndExecutionList, Task *task);
static List * UpstreamDependencyList(List *taskAndExecutionList, Task *searchedTask);
static List * ConstrainedMergeTaskList(List *taskAndExecutionList, Task *task);
static List * MergeTaskList(List *taskList);
static void ReassignTaskList(List *taskList);
static void ReassignMapFetchTaskList(List *mapFetchTaskList);

/* Local functions forward declarations to manage task trackers */
static void ManageTaskTracker(TaskTracker *taskTracker);
static bool TrackerConnectionUp(TaskTracker *taskTracker);
static void TrackerReconnectPoll(TaskTracker *taskTracker);
static List * AssignQueuedTasks(TaskTracker *taskTracker);
static List * TaskStatusBatchList(TaskTracker *taskTracker);
static StringInfo TaskStatusBatchQuery(List *taskList);
static void ReceiveTaskStatusBatchQueryResponse(TaskTracker *taskTracker);
static void ManageTransmitTracker(TaskTracker *transmitTracker);
static TrackerTaskState * NextQueuedFileTransmit(HTAB *taskStateHash);

/* Local functions forward declarations to clean up tasks */
static List * JobIdList(Job *job);
static void TrackerCleanupResources(HTAB *taskTrackerHash, HTAB *transmitTrackerHash,
									List *jobIdList, List *taskList);
static void TrackerHashWaitActiveRequest(HTAB *taskTrackerHash);
static void TrackerHashCancelActiveRequest(HTAB *taskTrackerHash);
static Task * JobCleanupTask(uint64 jobId);
static void TrackerHashCleanupJob(HTAB *taskTrackerHash, Task *jobCleanupTask);
static void TrackerHashDisconnect(HTAB *taskTrackerHash);


/*
 * MultiTaskTrackerExecute loops over given tasks, and manages their execution
 * until either one task permanently fails or all tasks successfully complete.
 * The function initializes connections to task trackers on worker nodes, and
 * executes tasks through assigning them to these trackers.
 */
void
MultiTaskTrackerExecute(Job *job)
{
	List *jobTaskList = job->taskList;
	uint32 topLevelTaskCount = 0;
	uint32 failedTaskId = 0;
	bool allTasksCompleted = false;
	bool taskFailed = false;
	bool taskTransmitFailed = false;
	bool clusterFailed = false;
	bool sizeLimitIsExceeded = false;

	DistributedExecutionStats executionStats = { 0 };
	char *extensionOwner = CitusExtensionOwnerName();
	const char *taskTrackerHashName = "Task Tracker Hash";
	const char *transmitTrackerHashName = "Transmit Tracker Hash";

	if (ReadFromSecondaries == USE_SECONDARY_NODES_ALWAYS)
	{
		ereport(ERROR, (errmsg("task tracker queries are not allowed while "
							   "citus.use_secondary_nodes is 'always'"),
						errhint("try setting citus.task_executor_type TO 'adaptive'")));
	}

	/*
	 * We walk over the task tree, and create a task execution struct for each
	 * task. We then associate the task with its execution and get back a list.
	 */
	List *taskAndExecutionList = TaskAndExecutionList(jobTaskList);

	/*
	 * We now count the number of "top level" tasks in the query tree. Once they
	 * complete, we'll need to fetch these tasks' results to the master node.
	 */
	Task *task = NULL;
	foreach_ptr(task, taskAndExecutionList)
	{
		bool topLevelTask = TopLevelTask(task);
		if (topLevelTask)
		{
			topLevelTaskCount++;
		}
	}

	/*
	 * We get the list of worker nodes, and then create two hashes to manage our
	 * connections to these nodes. The first hash manages connections used for
	 * assigning and checking the status of tasks. The second (temporary) hash
	 * helps us in fetching results data from worker nodes to the master node.
	 */
	List *workerNodeList = ActivePrimaryWorkerNodeList(NoLock);
	uint32 taskTrackerCount = (uint32) list_length(workerNodeList);

	/* connect as the current user for running queries */
	HTAB *taskTrackerHash = TrackerHash(taskTrackerHashName, workerNodeList, NULL);

	/* connect as the superuser for fetching result files */
	HTAB *transmitTrackerHash = TrackerHash(transmitTrackerHashName, workerNodeList,
											extensionOwner);

	TrackerHashConnect(taskTrackerHash);
	TrackerHashConnect(transmitTrackerHash);

	/* loop around until all tasks complete, one task fails, or user cancels */
	while (!(allTasksCompleted || taskFailed || taskTransmitFailed ||
			 clusterFailed || QueryCancelPending || sizeLimitIsExceeded))
	{
		TaskTracker *taskTracker = NULL;
		TaskTracker *transmitTracker = NULL;
		HASH_SEQ_STATUS taskStatus;
		HASH_SEQ_STATUS transmitStatus;

		uint32 completedTransmitCount = 0;
		uint32 healthyTrackerCount = 0;
		double acceptableHealthyTrackerCount = 0.0;

		/* first, loop around all tasks and manage them */
		foreach_ptr(task, taskAndExecutionList)
		{
			TaskExecution *taskExecution = task->taskExecution;

			TaskTracker *execTaskTracker = ResolveTaskTracker(taskTrackerHash,
															  task, taskExecution);
			TaskTracker *mapTaskTracker = ResolveMapTaskTracker(taskTrackerHash,
																task, taskExecution);
			Assert(execTaskTracker != NULL);

			/* call the function that performs the core task execution logic */
			TaskExecStatus taskExecutionStatus = ManageTaskExecution(execTaskTracker,
																	 mapTaskTracker,
																	 task, taskExecution);

			/*
			 * If task cannot execute on this task/map tracker, we fail over all
			 * tasks in the same constraint group to the next task/map tracker.
			 */
			if (taskExecutionStatus == EXEC_TASK_TRACKER_FAILED)
			{
				/* mark task tracker as failed, in case it isn't marked already */
				execTaskTracker->trackerFailureCount = MAX_TRACKER_FAILURE_COUNT;

				/*
				 * We may have already started to transmit task results to the
				 * master. When we reassign the transmits, we could leave the
				 * transmit tracker in an invalid state. So, we fail it too.
				 */
				transmitTracker = ResolveTaskTracker(transmitTrackerHash,
													 task, taskExecution);
				transmitTracker->trackerFailureCount = MAX_TRACKER_FAILURE_COUNT;

				List *taskList = ConstrainedTaskList(taskAndExecutionList, task);
				ReassignTaskList(taskList);
			}
			else if (taskExecutionStatus == EXEC_SOURCE_TASK_TRACKER_FAILED)
			{
				/* first resolve the map task this map fetch task depends on */
				Task *mapTask = (Task *) linitial(task->dependentTaskList);
				Assert(task->taskType == MAP_OUTPUT_FETCH_TASK);

				List *mapFetchTaskList = UpstreamDependencyList(taskAndExecutionList,
																mapTask);
				ReassignMapFetchTaskList(mapFetchTaskList);

				List *mapTaskList = ConstrainedTaskList(taskAndExecutionList, mapTask);
				ReassignTaskList(mapTaskList);
			}

			/*
			 * If this task permanently failed, we first need to manually clean
			 * out client-side resources for all task executions. We therefore
			 * record the failure here instead of immediately erroring out.
			 */
			taskFailed = TaskExecutionFailed(taskExecution);
			if (taskFailed)
			{
				failedTaskId = taskExecution->taskId;
				break;
			}
		}

		/* second, loop around "top level" tasks to fetch their results */
		foreach_ptr(task, taskAndExecutionList)
		{
			TaskExecution *taskExecution = task->taskExecution;

			/*
			 * We find the tasks that appear in the top level of the query tree,
			 * and start fetching their results to the master node.
			 */
			bool topLevelTask = TopLevelTask(task);
			if (!topLevelTask)
			{
				continue;
			}

			TaskTracker *execTransmitTracker = ResolveTaskTracker(transmitTrackerHash,
																  task, taskExecution);
			Assert(execTransmitTracker != NULL);

			/* call the function that fetches results for completed SQL tasks */
			TransmitExecStatus transmitExecutionStatus = ManageTransmitExecution(
				execTransmitTracker,
				task,
				taskExecution,
				&
				executionStats);

			/*
			 * If we cannot transmit SQL task's results to the master, we first
			 * force fail the corresponding task tracker. We then fail over all
			 * tasks in the constraint group to the next task/transmit tracker.
			 */
			if (transmitExecutionStatus == EXEC_TRANSMIT_TRACKER_FAILED)
			{
				taskTracker = ResolveTaskTracker(taskTrackerHash,
												 task, taskExecution);
				taskTracker->trackerFailureCount = MAX_TRACKER_FAILURE_COUNT;

				List *taskList = ConstrainedTaskList(taskAndExecutionList, task);
				ReassignTaskList(taskList);
			}

			/* if task failed for good, record failure and break out of loop */
			taskTransmitFailed = TaskExecutionFailed(taskExecution);
			if (taskTransmitFailed)
			{
				failedTaskId = taskExecution->taskId;
				break;
			}

			bool transmitCompleted = TransmitExecutionCompleted(taskExecution);
			if (transmitCompleted)
			{
				completedTransmitCount++;
			}
		}


		if (CheckIfSizeLimitIsExceeded(&executionStats))
		{
			sizeLimitIsExceeded = true;
			break;
		}

		/* third, loop around task trackers and manage them */
		hash_seq_init(&taskStatus, taskTrackerHash);
		hash_seq_init(&transmitStatus, transmitTrackerHash);

		taskTracker = (TaskTracker *) hash_seq_search(&taskStatus);
		while (taskTracker != NULL)
		{
			bool trackerHealthy = TrackerHealthy(taskTracker);
			if (trackerHealthy)
			{
				healthyTrackerCount++;
			}

			ManageTaskTracker(taskTracker);

			taskTracker = (TaskTracker *) hash_seq_search(&taskStatus);
		}

		transmitTracker = (TaskTracker *) hash_seq_search(&transmitStatus);
		while (transmitTracker != NULL)
		{
			ManageTransmitTracker(transmitTracker);

			transmitTracker = (TaskTracker *) hash_seq_search(&transmitStatus);
		}

		/* if more than half the trackers have failed, mark cluster as failed */
		acceptableHealthyTrackerCount = (double) taskTrackerCount / 2.0;
		if (healthyTrackerCount < acceptableHealthyTrackerCount)
		{
			clusterFailed = true;
		}

		/* check if we completed execution; otherwise sleep to avoid tight loop */
		if (completedTransmitCount == topLevelTaskCount)
		{
			allTasksCompleted = true;
		}
		else
		{
			long sleepIntervalPerCycle = RemoteTaskCheckInterval * 1000L;
			pg_usleep(sleepIntervalPerCycle);
		}
	}

	/*
	 * We prevent cancel/die interrupts until we issue cleanup requests to task
	 * trackers and close open connections. Note that for the above while loop,
	 * if the user Ctrl+C's a query and we emit a warning before looping to the
	 * beginning of the while loop, we will get canceled away before we can hold
	 * any interrupts.
	 */
	HOLD_INTERRUPTS();

	List *jobIdList = JobIdList(job);

	TrackerCleanupResources(taskTrackerHash, transmitTrackerHash,
							jobIdList, taskAndExecutionList);

	RESUME_INTERRUPTS();

	/*
	 * If we previously broke out of the execution loop due to a task failure or
	 * user cancellation request, we can now safely emit an error message.
	 */
	if (sizeLimitIsExceeded)
	{
		ErrorSizeLimitIsExceeded();
	}
	else if (taskFailed)
	{
		ereport(ERROR, (errmsg("failed to execute task %u", failedTaskId)));
	}
	else if (clusterFailed)
	{
		ereport(ERROR, (errmsg("majority of nodes failed")));
	}
	else if (QueryCancelPending)
	{
		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * TaskAndExecutionList visits all tasks in the job tree, starting with the given
 * job's task list. For each visited task, the function creates a task execution
 * struct, associates the task execution with the task, and adds the task and its
 * execution to a list. The function then returns the list.
 */
List *
TaskAndExecutionList(List *jobTaskList)
{
	List *taskAndExecutionList = NIL;
	const int topLevelTaskHashSize = 32;
	int taskHashSize = list_length(jobTaskList) * topLevelTaskHashSize;
	HTAB *taskHash = TaskHashCreate(taskHashSize);

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

		/* create task execution and associate it with task */
		TaskExecution *taskExecution = InitTaskExecution(task, EXEC_TASK_UNASSIGNED);
		task->taskExecution = taskExecution;

		taskAndExecutionList = lappend(taskAndExecutionList, task);

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

	return taskAndExecutionList;
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
	int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB *taskHash = hash_create(taskHashName, taskHashSize, &info, hashFlags);

	return taskHash;
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


/*
 * TopLevelTask checks if the given task appears at the top level of the task
 * tree. In doing this, the function assumes the physical planner creates SQL
 * tasks only for the top level job.
 */
static bool
TopLevelTask(Task *task)
{
	bool topLevelTask = false;

	/*
	 * SQL tasks can only appear at the top level in our query tree. Further, no
	 * other task type can appear at the top level in our tree.
	 */
	if (task->taskType == SELECT_TASK)
	{
		topLevelTask = true;
	}

	return topLevelTask;
}


/* Determines if the given transmit task successfully completed executing. */
static bool
TransmitExecutionCompleted(TaskExecution *taskExecution)
{
	bool completed = false;

	for (uint32 nodeIndex = 0; nodeIndex < taskExecution->nodeCount; nodeIndex++)
	{
		TransmitExecStatus *transmitStatusArray = taskExecution->transmitStatusArray;

		TransmitExecStatus transmitStatus = transmitStatusArray[nodeIndex];
		if (transmitStatus == EXEC_TRANSMIT_DONE)
		{
			completed = true;
			break;
		}
	}

	return completed;
}


/*
 * TrackerHash creates a task tracker hash with the given name. The function
 * then inserts one task tracker entry for each node in the given worker node
 * list, and initializes state for each task tracker. The userName argument
 * indicates which user to connect as.
 */
static HTAB *
TrackerHash(const char *taskTrackerHashName, List *workerNodeList, char *userName)
{
	/* create task tracker hash */
	uint32 taskTrackerHashSize = list_length(workerNodeList);
	HTAB *taskTrackerHash = TrackerHashCreate(taskTrackerHashName, taskTrackerHashSize);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		uint32 taskStateCount = 32;
		HASHCTL info;

		/* insert task tracker into the tracker hash */
		TaskTracker *taskTracker = TrackerHashEnter(taskTrackerHash, nodeName, nodePort);


		/* for each task tracker, create hash to track its assigned tasks */
		StringInfo taskStateHashName = makeStringInfo();
		appendStringInfo(taskStateHashName, "Task Tracker \"%s:%u\" Task State Hash",
						 nodeName, nodePort);

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(uint64) + sizeof(uint32);
		info.entrysize = sizeof(TrackerTaskState);
		info.hash = tag_hash;
		info.hcxt = CurrentMemoryContext;
		int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		HTAB *taskStateHash = hash_create(taskStateHashName->data, taskStateCount, &info,
										  hashFlags);
		if (taskStateHash == NULL)
		{
			ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("could not initialize %s", taskStateHashName->data)));
		}

		taskTracker->taskStateHash = taskStateHash;
		taskTracker->userName = userName;
	}

	return taskTrackerHash;
}


/*
 * TrackerHashCreate allocates memory for a task tracker hash, initializes an
 * empty hash, and returns this hash.
 */
static HTAB *
TrackerHashCreate(const char *taskTrackerHashName, uint32 taskTrackerHashSize)
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = WORKER_LENGTH + sizeof(uint32);
	info.entrysize = sizeof(TaskTracker);
	info.hash = tag_hash;
	info.hcxt = CurrentMemoryContext;
	int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB *taskTrackerHash = hash_create(taskTrackerHashName, taskTrackerHashSize,
										&info, hashFlags);
	if (taskTrackerHash == NULL)
	{
		ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("could not initialize task tracker hash")));
	}

	return taskTrackerHash;
}


/*
 * TrackerHashEnter creates a new task tracker entry in the given task tracker
 * hash, and checks that the task tracker entry has been properly created. Note
 * that the caller still needs to set the tracker's task state hash field.
 */
static TaskTracker *
TrackerHashEnter(HTAB *taskTrackerHash, char *nodeName, uint32 nodePort)
{
	bool handleFound = false;

	TaskTracker taskTrackerKey;
	memset(&taskTrackerKey, 0, sizeof(TaskTracker));
	strlcpy(taskTrackerKey.workerName, nodeName, WORKER_LENGTH);
	taskTrackerKey.workerPort = nodePort;

	void *hashKey = (void *) &taskTrackerKey;
	TaskTracker *taskTracker = (TaskTracker *) hash_search(taskTrackerHash, hashKey,
														   HASH_ENTER, &handleFound);

	/* if same node appears twice, we overwrite previous entry */
	if (handleFound)
	{
		ereport(WARNING, (errmsg("multiple entries for task tracker: \"%s:%u\"",
								 nodeName, nodePort)));
	}

	/* init task tracker object with zeroed out task tracker key */
	*taskTracker = taskTrackerKey;
	taskTracker->trackerStatus = TRACKER_CONNECT_START;
	taskTracker->connectionId = INVALID_CONNECTION_ID;
	taskTracker->currentTaskIndex = -1;

	return taskTracker;
}


/*
 * TrackerHashConnect walks over each task tracker in the given hash and tries
 * to open an asynchronous connection to it. The function then returns when we
 * tried connecting to all task trackers and have either succeeded or failed for
 * each one of them.
 */
static void
TrackerHashConnect(HTAB *taskTrackerHash)
{
	uint32 taskTrackerCount = (uint32) hash_get_num_entries(taskTrackerHash);
	uint32 triedTrackerCount = 0;

	/* loop until we tried to connect to all task trackers */
	while (triedTrackerCount < taskTrackerCount)
	{
		HASH_SEQ_STATUS status;

		/* loop over the task tracker hash, and poll all trackers again */
		triedTrackerCount = 0;
		hash_seq_init(&status, taskTrackerHash);

		TaskTracker *taskTracker = (TaskTracker *) hash_seq_search(&status);
		while (taskTracker != NULL)
		{
			TrackerStatus trackerStatus = TrackerConnectPoll(taskTracker);
			if (trackerStatus == TRACKER_CONNECTED ||
				trackerStatus == TRACKER_CONNECTION_FAILED)
			{
				triedTrackerCount++;
			}

			taskTracker = (TaskTracker *) hash_seq_search(&status);
		}

		/* sleep to avoid tight loop */
		long sleepIntervalPerCycle = RemoteTaskCheckInterval * 1000L;
		pg_usleep(sleepIntervalPerCycle);
	}
}


/*
 * TrackerConnectPoll opens an asynchronous connection to the given task tracker
 * and polls this connection's status on every call. The function also sets task
 * tracker's internal state on success, and returns the most recent status for
 * the connection.
 */
static TrackerStatus
TrackerConnectPoll(TaskTracker *taskTracker)
{
	switch (taskTracker->trackerStatus)
	{
		case TRACKER_CONNECT_START:
		{
			char *nodeName = taskTracker->workerName;
			uint32 nodePort = taskTracker->workerPort;
			const char *nodeDatabase = CurrentDatabaseName();
			char *nodeUser = taskTracker->userName;

			int32 connectionId = MultiClientConnectStart(nodeName, nodePort,
														 nodeDatabase, nodeUser);
			if (connectionId != INVALID_CONNECTION_ID)
			{
				taskTracker->connectionId = connectionId;
				taskTracker->trackerStatus = TRACKER_CONNECT_POLL;
			}
			else
			{
				taskTracker->trackerStatus = TRACKER_CONNECTION_FAILED;
			}

			break;
		}

		case TRACKER_CONNECT_POLL:
		{
			int32 connectionId = taskTracker->connectionId;

			ConnectStatus pollStatus = MultiClientConnectPoll(connectionId);
			if (pollStatus == CLIENT_CONNECTION_READY)
			{
				taskTracker->trackerStatus = TRACKER_CONNECTED;
			}
			else if (pollStatus == CLIENT_CONNECTION_BUSY ||
					 pollStatus == CLIENT_CONNECTION_BUSY_READ ||
					 pollStatus == CLIENT_CONNECTION_BUSY_WRITE)
			{
				taskTracker->trackerStatus = TRACKER_CONNECT_POLL;
			}
			else if (pollStatus == CLIENT_CONNECTION_BAD)
			{
				taskTracker->trackerStatus = TRACKER_CONNECTION_FAILED;

				MultiClientDisconnect(connectionId);
				taskTracker->connectionId = INVALID_CONNECTION_ID;
			}

			/* now check if we have been trying to connect for too long */
			taskTracker->connectPollCount++;
			if (pollStatus == CLIENT_CONNECTION_BUSY_READ ||
				pollStatus == CLIENT_CONNECTION_BUSY_WRITE)
			{
				uint32 maxCount =
					ceil(NodeConnectionTimeout * 1.0f / RemoteTaskCheckInterval);
				uint32 currentCount = taskTracker->connectPollCount;
				if (currentCount >= maxCount)
				{
					ereport(WARNING, (errmsg("could not establish asynchronous "
											 "connection after %u ms",
											 NodeConnectionTimeout)));

					taskTracker->trackerStatus = TRACKER_CONNECTION_FAILED;

					MultiClientDisconnect(connectionId);
					taskTracker->connectionId = INVALID_CONNECTION_ID;
				}
			}

			break;
		}

		case TRACKER_CONNECTED:
		case TRACKER_CONNECTION_FAILED:
		{
			/* if connected or failed to connect in previous pass, reset poll count */
			taskTracker->connectPollCount = 0;
			break;
		}

		default:
		{
			int trackerStatus = (int) taskTracker->trackerStatus;
			ereport(FATAL, (errmsg("invalid task tracker status: %d", trackerStatus)));
			break;
		}
	}

	return taskTracker->trackerStatus;
}


/*
 * ResolveTaskTracker is a helper function that resolves the task tracker from
 * the given task and task execution. The function first finds the worker node
 * the given task is scheduled to, and resolves the corresponding task tracker.
 */
static TaskTracker *
ResolveTaskTracker(HTAB *trackerHash, Task *task, TaskExecution *taskExecution)
{
	List *taskPlacementList = task->taskPlacementList;
	uint32 currentIndex = taskExecution->currentNodeIndex;

	ShardPlacement *taskPlacement = list_nth(taskPlacementList, currentIndex);
	char *nodeName = taskPlacement->nodeName;
	uint32 nodePort = taskPlacement->nodePort;

	/* look up in the tracker hash for the found node name/port */
	TaskTracker *taskTracker = TrackerHashLookup(trackerHash, nodeName, nodePort);
	Assert(taskTracker != NULL);

	return taskTracker;
}


/*
 * ResolveMapTaskTracker is a helper function that finds the downstream map task
 * dependency from the given task, and then resolves the task tracker for this
 * map task.
 */
static TaskTracker *
ResolveMapTaskTracker(HTAB *trackerHash, Task *task, TaskExecution *taskExecution)
{
	/* we only resolve source (map) task tracker for map output fetch tasks */
	if (task->taskType != MAP_OUTPUT_FETCH_TASK)
	{
		return NULL;
	}

	Assert(task->dependentTaskList != NIL);
	Task *mapTask = (Task *) linitial(task->dependentTaskList);
	TaskExecution *mapTaskExecution = mapTask->taskExecution;

	TaskTracker *mapTaskTracker = ResolveTaskTracker(trackerHash, mapTask,
													 mapTaskExecution);
	Assert(mapTaskTracker != NULL);

	return mapTaskTracker;
}


/*
 * TrackerHashLookup looks for the task tracker that corresponds to the given
 * node name and port number, and returns the found task tracker if any.
 */
static TaskTracker *
TrackerHashLookup(HTAB *trackerHash, const char *nodeName, uint32 nodePort)
{
	bool handleFound = false;

	TaskTracker taskTrackerKey;
	memset(taskTrackerKey.workerName, 0, WORKER_LENGTH);
	strlcpy(taskTrackerKey.workerName, nodeName, WORKER_LENGTH);
	taskTrackerKey.workerPort = nodePort;

	void *hashKey = (void *) &taskTrackerKey;
	TaskTracker *taskTracker = (TaskTracker *) hash_search(trackerHash, hashKey,
														   HASH_FIND, &handleFound);
	if (taskTracker == NULL || !handleFound)
	{
		ereport(ERROR, (errmsg("could not find task tracker for node \"%s:%u\"",
							   nodeName, nodePort)));
	}

	return taskTracker;
}


/*
 * ManageTaskExecution manages all execution logic for the given task. For this,
 * the function checks if the task's downstream dependencies have completed. If
 * they have, the function assigns the task to the task tracker proxy object,
 * and regularly checks the task's execution status.
 *
 * If the task completes, the function changes task's status. Else if the task
 * observes a connection related failure, the function retries the task on the
 * same task tracker. Else if the task tracker isn't considered as healthy, the
 * function signals to the caller that the task needs to be assigned to another
 * task tracker.
 */
static TaskExecStatus
ManageTaskExecution(TaskTracker *taskTracker, TaskTracker *sourceTaskTracker,
					Task *task, TaskExecution *taskExecution)
{
	TaskExecStatus *taskStatusArray = taskExecution->taskStatusArray;
	uint32 currentNodeIndex = taskExecution->currentNodeIndex;

	TaskExecStatus currentExecutionStatus = taskStatusArray[currentNodeIndex];
	TaskExecStatus nextExecutionStatus = EXEC_TASK_INVALID_FIRST;

	switch (currentExecutionStatus)
	{
		case EXEC_TASK_UNASSIGNED:
		{
			bool trackerHealthy = TrackerHealthy(taskTracker);
			if (!trackerHealthy)
			{
				nextExecutionStatus = EXEC_TASK_TRACKER_FAILED;
				break;
			}

			/*
			 * We first retrieve this task's downstream dependencies, and then check
			 * if these dependencies' executions have completed.
			 */
			bool taskExecutionsCompleted = TaskExecutionsCompleted(
				task->dependentTaskList);
			if (!taskExecutionsCompleted)
			{
				nextExecutionStatus = EXEC_TASK_UNASSIGNED;
				break;
			}

			/* if map fetch task, create query string from completed map task */
			TaskType taskType = task->taskType;
			if (taskType == MAP_OUTPUT_FETCH_TASK)
			{
				Task *mapTask = (Task *) linitial(task->dependentTaskList);
				TaskExecution *mapTaskExecution = mapTask->taskExecution;

				StringInfo mapFetchTaskQueryString = MapFetchTaskQueryString(task,
																			 mapTask);
				SetTaskQueryString(task, mapFetchTaskQueryString->data);
				taskExecution->querySourceNodeIndex = mapTaskExecution->currentNodeIndex;
			}

			/*
			 * We finally queue this task for execution. Note that we queue sql and
			 * other tasks slightly differently.
			 */
			if (taskType == SELECT_TASK)
			{
				TrackerQueueSqlTask(taskTracker, task);
			}
			else
			{
				TrackerQueueTask(taskTracker, task);
			}

			nextExecutionStatus = EXEC_TASK_QUEUED;
			break;
		}

		case EXEC_TASK_QUEUED:
		{
			bool trackerHealthy = TrackerHealthy(taskTracker);
			if (!trackerHealthy)
			{
				nextExecutionStatus = EXEC_TASK_TRACKER_FAILED;
				break;
			}

			TaskStatus remoteTaskStatus = TrackerTaskStatus(taskTracker, task);
			if (remoteTaskStatus == TASK_SUCCEEDED)
			{
				nextExecutionStatus = EXEC_TASK_DONE;
			}
			else if (remoteTaskStatus == TASK_CLIENT_SIDE_ASSIGN_FAILED ||
					 remoteTaskStatus == TASK_CLIENT_SIDE_STATUS_FAILED)
			{
				nextExecutionStatus = EXEC_TASK_TRACKER_RETRY;
			}
			else if (remoteTaskStatus == TASK_PERMANENTLY_FAILED)
			{
				/*
				 * If a map output fetch task failed, we assume the problem lies with
				 * the map task (and the source task tracker it runs on). Otherwise,
				 * we assume the task tracker crashed, and fail over to the next task
				 * tracker.
				 */
				if (task->taskType == MAP_OUTPUT_FETCH_TASK)
				{
					nextExecutionStatus = EXEC_SOURCE_TASK_TRACKER_RETRY;
				}
				else
				{
					nextExecutionStatus = EXEC_TASK_TRACKER_FAILED;
				}
			}
			else
			{
				/* assume task is still in progress */
				nextExecutionStatus = EXEC_TASK_QUEUED;
			}

			break;
		}

		case EXEC_TASK_TRACKER_RETRY:
		{
			/*
			 * This case statement usually handles connection related issues. Some
			 * edge cases however, like a user sending a SIGTERM to the worker node,
			 * keep the connection open but disallow task assignments. We therefore
			 * need to track those as intermittent tracker failures here.
			 */
			bool trackerConnectionUp = TrackerConnectionUp(taskTracker);
			if (trackerConnectionUp)
			{
				taskTracker->trackerFailureCount++;
			}

			bool trackerHealthy = TrackerHealthy(taskTracker);
			if (trackerHealthy)
			{
				TaskStatus remoteTaskStatus = TrackerTaskStatus(taskTracker, task);
				if (remoteTaskStatus == TASK_CLIENT_SIDE_ASSIGN_FAILED)
				{
					nextExecutionStatus = EXEC_TASK_UNASSIGNED;
				}
				else if (remoteTaskStatus == TASK_CLIENT_SIDE_STATUS_FAILED)
				{
					nextExecutionStatus = EXEC_TASK_QUEUED;
				}
			}
			else
			{
				nextExecutionStatus = EXEC_TASK_TRACKER_FAILED;
			}

			break;
		}

		case EXEC_SOURCE_TASK_TRACKER_RETRY:
		{
			Task *mapTask = (Task *) linitial(task->dependentTaskList);
			TaskExecution *mapTaskExecution = mapTask->taskExecution;
			uint32 sourceNodeIndex = mapTaskExecution->currentNodeIndex;

			Assert(sourceTaskTracker != NULL);
			Assert(task->taskType == MAP_OUTPUT_FETCH_TASK);

			/*
			 * As this map fetch task was running, another map fetch that depends on
			 * another map task might have failed. We would have then reassigned the
			 * map task and potentially other map tasks in its constraint group. So
			 * this map fetch's source node might have changed underneath us. If it
			 * did, we don't want to record a failure for the new source tracker.
			 */
			if (taskExecution->querySourceNodeIndex == sourceNodeIndex)
			{
				bool sourceTrackerConnectionUp = TrackerConnectionUp(sourceTaskTracker);
				if (sourceTrackerConnectionUp)
				{
					sourceTaskTracker->trackerFailureCount++;
				}
			}

			bool sourceTrackerHealthy = TrackerHealthy(sourceTaskTracker);
			if (sourceTrackerHealthy)
			{
				/*
				 * We change our status to unassigned. In that status, we queue an
				 * "update map fetch task" on the task tracker, and retry fetching
				 * the map task's output from the same source node.
				 */
				nextExecutionStatus = EXEC_TASK_UNASSIGNED;
			}
			else
			{
				nextExecutionStatus = EXEC_SOURCE_TASK_TRACKER_FAILED;
			}

			break;
		}

		case EXEC_TASK_TRACKER_FAILED:
		case EXEC_SOURCE_TASK_TRACKER_FAILED:
		{
			/*
			 * These two cases exist to signal to the caller that we failed. In both
			 * cases, the caller is responsible for reassigning task(s) and running
			 * the appropriate recovery logic.
			 */
			nextExecutionStatus = EXEC_TASK_UNASSIGNED;
			break;
		}

		case EXEC_TASK_DONE:
		{
			/* we are done with this task's execution */
			nextExecutionStatus = EXEC_TASK_DONE;
			break;
		}

		default:
		{
			/* we fatal here to avoid leaking client-side resources */
			ereport(FATAL, (errmsg("invalid execution status: %d",
								   currentExecutionStatus)));
			break;
		}
	}

	/* update task execution's status for most recent task tracker */
	uint32 nextNodeIndex = taskExecution->currentNodeIndex;
	taskStatusArray[nextNodeIndex] = nextExecutionStatus;

	return nextExecutionStatus;
}


/*
 * ManageTransmitExecution manages logic to fetch the results of the given SQL
 * task to the master node. For this, the function checks if the given SQL task
 * has completed. If it has, the function starts the copy out protocol to fetch
 * the task's results and write them to the local filesystem. When the transmit
 * completes or fails, the function notes that by changing the transmit status.
 */
static TransmitExecStatus
ManageTransmitExecution(TaskTracker *transmitTracker,
						Task *task, TaskExecution *taskExecution,
						DistributedExecutionStats *executionStats)
{
	int32 *fileDescriptorArray = taskExecution->fileDescriptorArray;
	uint32 currentNodeIndex = taskExecution->currentNodeIndex;

	TransmitExecStatus *transmitStatusArray = taskExecution->transmitStatusArray;
	TransmitExecStatus currentTransmitStatus = transmitStatusArray[currentNodeIndex];
	TransmitExecStatus nextTransmitStatus = EXEC_TRANSMIT_INVALID_FIRST;
	Assert(task->taskType == SELECT_TASK);

	switch (currentTransmitStatus)
	{
		case EXEC_TRANSMIT_UNASSIGNED:
		{
			TaskExecStatus *taskStatusArray = taskExecution->taskStatusArray;
			TaskExecStatus currentExecutionStatus = taskStatusArray[currentNodeIndex];

			/* if top level task's in progress, nothing to do */
			if (currentExecutionStatus != EXEC_TASK_DONE)
			{
				nextTransmitStatus = EXEC_TRANSMIT_UNASSIGNED;
				break;
			}

			bool trackerHealthy = TrackerHealthy(transmitTracker);
			if (!trackerHealthy)
			{
				nextTransmitStatus = EXEC_TRANSMIT_TRACKER_FAILED;
				break;
			}

			TrackerQueueFileTransmit(transmitTracker, task);
			nextTransmitStatus = EXEC_TRANSMIT_QUEUED;
			break;
		}

		case EXEC_TRANSMIT_QUEUED:
		{
			bool trackerHealthy = TrackerHealthy(transmitTracker);
			if (!trackerHealthy)
			{
				nextTransmitStatus = EXEC_TRANSMIT_TRACKER_FAILED;
				break;
			}

			TaskStatus taskStatus = TrackerTaskStatus(transmitTracker, task);
			if (taskStatus == TASK_FILE_TRANSMIT_QUEUED)
			{
				/* remain in queued status until tracker assigns this task */
				nextTransmitStatus = EXEC_TRANSMIT_QUEUED;
				break;
			}
			else if (taskStatus == TASK_CLIENT_SIDE_TRANSMIT_FAILED)
			{
				nextTransmitStatus = EXEC_TRANSMIT_TRACKER_RETRY;
				break;
			}

			/* the open connection belongs to this task */
			int32 connectionId = TransmitTrackerConnectionId(transmitTracker, task);
			Assert(connectionId != INVALID_CONNECTION_ID);
			Assert(taskStatus == TASK_ASSIGNED);

			/* start copy protocol */
			QueryStatus queryStatus = MultiClientQueryStatus(connectionId);
			if (queryStatus == CLIENT_QUERY_COPY)
			{
				StringInfo jobDirectoryName = MasterJobDirectoryName(task->jobId);
				StringInfo taskFilename = TaskFilename(jobDirectoryName, task->taskId);

				char *filename = taskFilename->data;
				int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
				int fileMode = (S_IRUSR | S_IWUSR);

				int32 fileDescriptor = BasicOpenFilePerm(filename, fileFlags, fileMode);
				if (fileDescriptor >= 0)
				{
					/*
					 * All files inside the job directory get automatically cleaned
					 * up on transaction commit or abort.
					 */
					fileDescriptorArray[currentNodeIndex] = fileDescriptor;
					nextTransmitStatus = EXEC_TRANSMIT_COPYING;
				}
				else
				{
					ereport(WARNING, (errcode_for_file_access(),
									  errmsg("could not open file \"%s\": %m",
											 filename)));

					nextTransmitStatus = EXEC_TRANSMIT_TRACKER_RETRY;
				}
			}
			else
			{
				nextTransmitStatus = EXEC_TRANSMIT_TRACKER_RETRY;
			}

			/*
			 * We use task tracker logic to manage file transmits as well, but that
			 * abstraction starts to leak after we drop into the copy protocol. To
			 * make our task tracker logic work, we need to "void" the tracker's
			 * connection if the transmit task failed in here.
			 */
			if (nextTransmitStatus == EXEC_TRANSMIT_TRACKER_RETRY)
			{
				transmitTracker->connectionBusy = false;
				transmitTracker->connectionBusyOnTask = NULL;
			}

			break;
		}

		case EXEC_TRANSMIT_COPYING:
		{
			int32 fileDescriptor = fileDescriptorArray[currentNodeIndex];
			int closed = -1;
			uint64 bytesReceived = 0;

			/* the open connection belongs to this task */
			int32 connectionId = TransmitTrackerConnectionId(transmitTracker, task);
			Assert(connectionId != INVALID_CONNECTION_ID);

			CopyStatus copyStatus = MultiClientCopyData(connectionId, fileDescriptor,
														&bytesReceived);

			if (SubPlanLevel > 0)
			{
				executionStats->totalIntermediateResultSize += bytesReceived;
			}

			if (copyStatus == CLIENT_COPY_MORE)
			{
				/* worker node continues to send more data, keep reading */
				nextTransmitStatus = EXEC_TRANSMIT_COPYING;
				break;
			}

			/* we are done copying data */
			if (copyStatus == CLIENT_COPY_DONE)
			{
				closed = close(fileDescriptor);
				fileDescriptorArray[currentNodeIndex] = -1;

				if (closed >= 0)
				{
					nextTransmitStatus = EXEC_TRANSMIT_DONE;
				}
				else
				{
					ereport(WARNING, (errcode_for_file_access(),
									  errmsg("could not close copied file: %m")));

					nextTransmitStatus = EXEC_TRANSMIT_TRACKER_RETRY;
				}
			}
			else if (copyStatus == CLIENT_COPY_FAILED)
			{
				nextTransmitStatus = EXEC_TRANSMIT_TRACKER_RETRY;

				closed = close(fileDescriptor);
				fileDescriptorArray[currentNodeIndex] = -1;

				if (closed < 0)
				{
					ereport(WARNING, (errcode_for_file_access(),
									  errmsg("could not close copy file: %m")));
				}
			}

			/*
			 * We use task tracker logic to manage file transmits as well, but that
			 * abstraction leaks after we drop into the copy protocol. To make it
			 * work, we reset transmit tracker's connection for next file transmit.
			 */
			transmitTracker->connectionBusy = false;
			transmitTracker->connectionBusyOnTask = NULL;

			break;
		}

		case EXEC_TRANSMIT_TRACKER_RETRY:
		{
			/*
			 * The task tracker proxy handles connection errors. On the off chance
			 * that our connection is still up and the transmit tracker misbehaved,
			 * we capture this as an intermittent tracker failure.
			 */
			bool trackerConnectionUp = TrackerConnectionUp(transmitTracker);
			if (trackerConnectionUp)
			{
				transmitTracker->trackerFailureCount++;
			}

			bool trackerHealthy = TrackerHealthy(transmitTracker);
			if (trackerHealthy)
			{
				nextTransmitStatus = EXEC_TRANSMIT_UNASSIGNED;
			}
			else
			{
				nextTransmitStatus = EXEC_TRANSMIT_TRACKER_FAILED;
			}

			break;
		}

		case EXEC_TRANSMIT_TRACKER_FAILED:
		{
			/*
			 * This case exists to signal to the caller that we failed. The caller
			 * is now responsible for reassigning the transmit task (and downstream
			 * SQL task dependencies) and running the appropriate recovery logic.
			 */
			nextTransmitStatus = EXEC_TRANSMIT_UNASSIGNED;
			break;
		}

		case EXEC_TRANSMIT_DONE:
		{
			/* we are done with fetching task results to the master node */
			nextTransmitStatus = EXEC_TRANSMIT_DONE;
			break;
		}

		default:
		{
			/* we fatal here to avoid leaking client-side resources */
			ereport(FATAL, (errmsg("invalid transmit status: %d",
								   currentTransmitStatus)));
			break;
		}
	}

	/* update file transmit status for most recent transmit tracker */
	uint32 nextNodeIndex = taskExecution->currentNodeIndex;
	transmitStatusArray[nextNodeIndex] = nextTransmitStatus;

	return nextTransmitStatus;
}


/*
 * TaskExecutionsCompleted checks if all task executions in the given task list
 * have completed. If they have, the function returns true. Note that this
 * function takes the list of tasks as an optimization over separately
 * extracting a list of task executions, but it should only operate on task
 * executions to preserve the abstraction.
 */
static bool
TaskExecutionsCompleted(List *taskList)
{
	bool taskExecutionsComplete = true;

	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		TaskExecution *taskExecution = task->taskExecution;
		uint32 nodeIndex = taskExecution->currentNodeIndex;

		TaskExecStatus taskStatus = taskExecution->taskStatusArray[nodeIndex];
		if (taskStatus != EXEC_TASK_DONE)
		{
			taskExecutionsComplete = false;
			break;
		}
	}

	return taskExecutionsComplete;
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

	StringInfo mapFetchQueryString = makeStringInfo();
	appendStringInfo(mapFetchQueryString, MAP_OUTPUT_FETCH_COMMAND,
					 mapTask->jobId, mapTask->taskId, partitionFileId,
					 mergeTaskId, /* fetch results to merge task */
					 mapTaskNodeName, mapTaskNodePort);

	return mapFetchQueryString;
}


/*
 * TrackerQueueSqlTask wraps a copy out command around the given task's query,
 * creates a task assignment query from this copy out command, and then queues
 * this assignment query in the given tracker's internal hash. The queued query
 * will be assigned to the remote task tracker at a later time.
 */
static void
TrackerQueueSqlTask(TaskTracker *taskTracker, Task *task)
{
	HTAB *taskStateHash = taskTracker->taskStateHash;

	/*
	 * We first wrap the original query string in a worker_execute_sql_task
	 * call. This allows for the query's results to persist on the worker node
	 * after the query completes and for the executor to later fetch this
	 * persisted data using COPY ... (format 'transmit')
	 */

	StringInfo sqlTaskQueryString = makeStringInfo();
	char *escapedTaskQueryString = quote_literal_cstr(TaskQueryStringForAllPlacements(
														  task));

	if (BinaryMasterCopyFormat)
	{
		appendStringInfo(sqlTaskQueryString, EXECUTE_SQL_TASK_TO_FILE_BINARY,
						 task->jobId, task->taskId, escapedTaskQueryString);
	}
	else
	{
		appendStringInfo(sqlTaskQueryString, EXECUTE_SQL_TASK_TO_FILE_TEXT,
						 task->jobId, task->taskId, escapedTaskQueryString);
	}

	/* wrap a task assignment query outside the copy out query */
	StringInfo taskAssignmentQuery = TaskAssignmentQuery(task, sqlTaskQueryString->data);

	TrackerTaskState *taskState = TaskStateHashEnter(taskStateHash, task->jobId,
													 task->taskId);
	taskState->status = TASK_CLIENT_SIDE_QUEUED;
	taskState->taskAssignmentQuery = taskAssignmentQuery;
}


/*
 * TrackerQueueTask creates a task assignment query from the given task's query
 * string, and then queues this assignment query in the given tracker's internal
 * hash. The queued query will be assigned to the remote task tracker at a later
 * time.
 */
static void
TrackerQueueTask(TaskTracker *taskTracker, Task *task)
{
	HTAB *taskStateHash = taskTracker->taskStateHash;

	/* wrap a task assignment query outside the original query */
	StringInfo taskAssignmentQuery =
		TaskAssignmentQuery(task, TaskQueryStringForAllPlacements(task));

	TrackerTaskState *taskState = TaskStateHashEnter(taskStateHash, task->jobId,
													 task->taskId);
	taskState->status = TASK_CLIENT_SIDE_QUEUED;
	taskState->taskAssignmentQuery = taskAssignmentQuery;
}


/*
 * TaskAssignmentQuery escapes the given query string with quotes, and wraps
 * this escaped query string inside a task assignment command. This way, the
 * query can be assigned to the remote task tracker.
 */
static StringInfo
TaskAssignmentQuery(Task *task, char *queryString)
{
	/* quote the original query as a string literal */
	char *escapedQueryString = quote_literal_cstr(queryString);

	StringInfo taskAssignmentQuery = makeStringInfo();
	appendStringInfo(taskAssignmentQuery, TASK_ASSIGNMENT_QUERY,
					 task->jobId, task->taskId, escapedQueryString);

	return taskAssignmentQuery;
}


/*
 * TrackerTaskStatus returns the remote execution status of the given task. Note
 * that the task must have already been queued with the task tracker for status
 * checking to happen.
 */
static TaskStatus
TrackerTaskStatus(TaskTracker *taskTracker, Task *task)
{
	HTAB *taskStateHash = taskTracker->taskStateHash;

	TrackerTaskState *taskState = TrackerTaskStateHashLookup(taskStateHash, task);
	if (taskState == NULL)
	{
		const char *nodeName = taskTracker->workerName;
		uint32 nodePort = taskTracker->workerPort;

		ereport(ERROR, (errmsg("could not find task state for job " UINT64_FORMAT
							   " and task %u", task->jobId, task->taskId),
						errdetail("Task tracker: \"%s:%u\"", nodeName, nodePort)));
	}

	return taskState->status;
}


/*
 * TrackerTaskStateHashLookup looks for the task state entry for the given task
 * in the task tracker's state hash. The function then returns the found task
 * state entry, if any.
 */
static TrackerTaskState *
TrackerTaskStateHashLookup(HTAB *taskStateHash, Task *task)
{
	bool handleFound = false;

	TrackerTaskState taskStateKey;
	taskStateKey.jobId = task->jobId;
	taskStateKey.taskId = task->taskId;

	void *hashKey = (void *) &taskStateKey;
	TrackerTaskState *taskState = (TrackerTaskState *) hash_search(taskStateHash, hashKey,
																   HASH_FIND,
																   &handleFound);

	return taskState;
}


/* Checks if the given task tracker is considered as healthy. */
static bool
TrackerHealthy(TaskTracker *taskTracker)
{
	bool trackerHealthy = false;

	if (taskTracker->trackerFailureCount < MAX_TRACKER_FAILURE_COUNT &&
		taskTracker->connectionFailureCount < MAX_TRACKER_FAILURE_COUNT)
	{
		trackerHealthy = true;
	}

	return trackerHealthy;
}


/*
 * TrackerQueueFileTransmit queues a file transmit request in the given task
 * tracker's internal hash. The queued request will be served at a later time.
 */
static void
TrackerQueueFileTransmit(TaskTracker *transmitTracker, Task *task)
{
	HTAB *transmitStateHash = transmitTracker->taskStateHash;

	TrackerTaskState *transmitState = TaskStateHashEnter(transmitStateHash, task->jobId,
														 task->taskId);
	transmitState->status = TASK_FILE_TRANSMIT_QUEUED;
}


/*
 * TaskStateHashEnter creates a new task state entry in the given task state
 * hash, and checks that the task entry has been properly created.
 */
static TrackerTaskState *
TaskStateHashEnter(HTAB *taskStateHash, uint64 jobId, uint32 taskId)
{
	bool handleFound = false;

	TrackerTaskState taskStateKey;
	taskStateKey.jobId = jobId;
	taskStateKey.taskId = taskId;

	void *hashKey = (void *) &taskStateKey;
	TrackerTaskState *taskState = (TrackerTaskState *) hash_search(taskStateHash, hashKey,
																   HASH_ENTER,
																   &handleFound);

	/* if same task queued twice, we overwrite previous entry */
	if (handleFound)
	{
		ereport(DEBUG1, (errmsg("multiple task state entries for job "
								UINT64_FORMAT " and task %u", jobId, taskId)));
	}

	/* init task state object */
	taskState->status = TASK_STATUS_INVALID_FIRST;
	taskState->taskAssignmentQuery = NULL;

	return taskState;
}


/*
 * TransmitTrackerConnectionId checks if the given tracker is transmitting the
 * given task's results to the master node. If it is, the function returns the
 * connectionId used in transmitting task results. If not, the function returns
 * an invalid connectionId.
 */
static int32
TransmitTrackerConnectionId(TaskTracker *transmitTracker, Task *task)
{
	int32 connectionId = INVALID_CONNECTION_ID;

	TrackerTaskState *transmitState = transmitTracker->connectionBusyOnTask;
	if (transmitState != NULL)
	{
		/* we are transmitting results for this particular task */
		if (transmitState->jobId == task->jobId &&
			transmitState->taskId == task->taskId)
		{
			connectionId = transmitTracker->connectionId;
		}
	}

	return connectionId;
}


/*
 * ConstrainedTaskList finds the given task's constraint group within the given
 * task and execution list. We define a constraint group as all tasks that need
 * to be assigned (or reassigned) to the same task tracker for query execution
 * to complete. At a high level, compute tasks are part of the same constraint
 * group. Also, the transitive closure of tasks that have the same merge task
 * dependency are part of one constraint group.
 */
static List *
ConstrainedTaskList(List *taskAndExecutionList, Task *task)
{
	List *constrainedTaskList = NIL;

	/*
	 * We first check if this task depends on any merge tasks. If it does *not*,
	 * the task's dependency list becomes our tiny constraint group.
	 */
	List *mergeTaskList = ConstrainedMergeTaskList(taskAndExecutionList, task);
	if (mergeTaskList == NIL)
	{
		constrainedTaskList = ConstrainedNonMergeTaskList(taskAndExecutionList, task);

		return constrainedTaskList;
	}

	/* we first add merge tasks and their dependencies to our constraint group */
	Task *mergeTask = NULL;
	foreach_ptr(mergeTask, mergeTaskList)
	{
		List *dependentTaskList = mergeTask->dependentTaskList;

		constrainedTaskList = lappend(constrainedTaskList, mergeTask);
		constrainedTaskList = TaskListConcatUnique(constrainedTaskList,
												   dependentTaskList);
	}

	/*
	 * We now pick the first merge task as our constraining task, and walk over
	 * the task list looking for any tasks that depend on the constraining merge
	 * task. Note that finding a task's upstream dependencies necessitates that
	 * we walk over all the tasks. If we want to optimize this later on, we can
	 * precompute a task list that excludes map fetch tasks.
	 */
	Task *constrainingTask = (Task *) linitial(mergeTaskList);

	List *upstreamTaskList = UpstreamDependencyList(taskAndExecutionList,
													constrainingTask);
	Assert(upstreamTaskList != NIL);

	Task *upstreamTask = NULL;
	foreach_ptr(upstreamTask, upstreamTaskList)
	{
		List *dependentTaskList = upstreamTask->dependentTaskList;

		/*
		 * We already added merge tasks to our constrained list. We therefore use
		 * concat unique to ensure they don't get appended for a second time.
		 */
		constrainedTaskList = TaskListAppendUnique(constrainedTaskList, upstreamTask);
		constrainedTaskList = TaskListConcatUnique(constrainedTaskList,
												   dependentTaskList);
	}

	return constrainedTaskList;
}


/*
 * ConstrainedNonMergeTaskList finds the constraint group for the given task,
 * assuming that the given task doesn't have any merge task dependencies. This
 * constraint group includes compute task.
 */
static List *
ConstrainedNonMergeTaskList(List *taskAndExecutionList, Task *task)
{
	Task *upstreamTask = NULL;
	List *dependentTaskList = NIL;

	TaskType taskType = task->taskType;
	if (taskType == SELECT_TASK || taskType == MAP_TASK)
	{
		upstreamTask = task;
		dependentTaskList = upstreamTask->dependentTaskList;
	}
	Assert(upstreamTask != NULL);

	List *constrainedTaskList = list_make1(upstreamTask);
	constrainedTaskList = list_concat(constrainedTaskList, dependentTaskList);

	return constrainedTaskList;
}


/*
 * UpstreamDependencyList looks for the given task's upstream task dependencies
 * in the given task and execution list. For this, the function walks across all
 * tasks in the task list. This walk is expensive due to the number of map fetch
 * tasks involved; and this function should be called sparingly.
 */
static List *
UpstreamDependencyList(List *taskAndExecutionList, Task *searchedTask)
{
	List *upstreamTaskList = NIL;

	Task *upstreamTask = NULL;
	foreach_ptr(upstreamTask, taskAndExecutionList)
	{
		List *dependentTaskList = upstreamTask->dependentTaskList;

		/*
		 * The given task and its upstream dependency cannot be of the same type.
		 * We perform this check as an optimization. This way, we can quickly
		 * skip over upstream map fetch tasks if we aren't looking for them.
		 */
		if (upstreamTask->taskType == searchedTask->taskType)
		{
			continue;
		}

		/*
		 * We walk over the upstream task's dependency list, and check if any of
		 * them is the task we are looking for.
		 */
		Task *dependentTask = NULL;
		foreach_ptr(dependentTask, dependentTaskList)
		{
			if (TasksEqual(dependentTask, searchedTask))
			{
				upstreamTaskList = lappend(upstreamTaskList, upstreamTask);
			}
		}
	}

	return upstreamTaskList;
}


/*
 * ConstrainedMergeTaskList finds any merge task dependencies for the given task.
 * Note that a given task may have zero, one, or two merge task dependencies. To
 * resolve all dependencies, the function first looks at the task's type. Then,
 * the function may need to find the task's parent, and resolve any merge task
 * dependencies from that parent task.
 */
static List *
ConstrainedMergeTaskList(List *taskAndExecutionList, Task *task)
{
	List *constrainedMergeTaskList = NIL;
	TaskType taskType = task->taskType;

	/*
	 * We find the list of constraining merge tasks for the given task. If the
	 * given task is a SQL or map task, we simply need to find its merge task
	 * dependencies -- if any.
	 */
	if (taskType == SELECT_TASK || taskType == MAP_TASK)
	{
		constrainedMergeTaskList = MergeTaskList(task->dependentTaskList);
	}
	else if (taskType == MAP_OUTPUT_FETCH_TASK)
	{
		List *taskList = UpstreamDependencyList(taskAndExecutionList, task);
		Task *mergeTask = (Task *) linitial(taskList);

		/*
		 * Once we resolve the merge task, we use the exact same logic as below
		 * to find any other merge task in our constraint group.
		 */
		List *upstreamTaskList = UpstreamDependencyList(taskAndExecutionList, mergeTask);
		Task *upstreamTask = (Task *) linitial(upstreamTaskList);

		constrainedMergeTaskList = MergeTaskList(upstreamTask->dependentTaskList);
	}
	else if (taskType == MERGE_TASK)
	{
		List *upstreamTaskList = UpstreamDependencyList(taskAndExecutionList, task);

		/*
		 * A merge task can have multiple SQL/map task parents. We now get only
		 * one of those parents. We then search if the parent depends on another
		 * merge task besides us.
		 */
		Assert(upstreamTaskList != NIL);
		Task *upstreamTask = (Task *) linitial(upstreamTaskList);

		constrainedMergeTaskList = MergeTaskList(upstreamTask->dependentTaskList);
	}

	return constrainedMergeTaskList;
}


/*
 * MergeTaskList walks over the given task list, finds the merge tasks in the
 * list, and returns the found tasks in a new list.
 */
static List *
MergeTaskList(List *taskList)
{
	List *mergeTaskList = NIL;

	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		if (task->taskType == MERGE_TASK)
		{
			mergeTaskList = lappend(mergeTaskList, task);
		}
	}

	return mergeTaskList;
}


/*
 * ReassignTaskList walks over all tasks in the given task list, and reassigns
 * each task's execution and transmit to the next worker node. This ensures that
 * all tasks within the same constraint group are failed over to the next node
 * together. The function also increments each task's failure counter.
 */
static void
ReassignTaskList(List *taskList)
{
	List *completedTaskList = NIL;

	/*
	 * As an optimization, we first find the SQL tasks whose results we already
	 * fetched to the master node. We don't need to re-execute these SQL tasks.
	 */
	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		TaskExecution *taskExecution = task->taskExecution;

		bool transmitCompleted = TransmitExecutionCompleted(taskExecution);
		if ((task->taskType == SELECT_TASK) && transmitCompleted)
		{
			completedTaskList = lappend(completedTaskList, task);
		}
	}

	taskList = TaskListDifference(taskList, completedTaskList);

	foreach_ptr(task, taskList)
	{
		TaskExecution *taskExecution = task->taskExecution;

		uint32 currentNodeIndex = taskExecution->currentNodeIndex;
		TaskExecStatus *taskStatusArray = taskExecution->taskStatusArray;
		TransmitExecStatus *transmitStatusArray = taskExecution->transmitStatusArray;

		/*
		 * We reset current task statuses in case we fail on all other worker
		 * nodes and come back to this one.
		 */
		taskStatusArray[currentNodeIndex] = EXEC_TASK_UNASSIGNED;
		transmitStatusArray[currentNodeIndex] = EXEC_TRANSMIT_UNASSIGNED;

		/* update node index to try next worker node */
		AdjustStateForFailure(taskExecution);
	}
}


/*
 * ReassignMapFetchTaskList walks over tasks in the given task list, and resets
 * their task execution status. This ensures that all map output fetch tasks are
 * retried after the node executing the map task has been failed over.
 */
static void
ReassignMapFetchTaskList(List *mapFetchTaskList)
{
	Task *mapFetchTask = NULL;
	foreach_ptr(mapFetchTask, mapFetchTaskList)
	{
		TaskExecution *mapFetchTaskExecution = mapFetchTask->taskExecution;

		TaskExecStatus *taskStatusArray = mapFetchTaskExecution->taskStatusArray;
		uint32 currentNodeIndex = mapFetchTaskExecution->currentNodeIndex;

		/*
		 * We reassign to same task tracker knowing that the source task tracker
		 * (that we failed to fetch map output from) has changed.
		 */
		taskStatusArray[currentNodeIndex] = EXEC_TASK_UNASSIGNED;
	}
}


/*
 * ManageTaskTracker manages tasks assigned to the given task tracker. For this,
 * the function coordinates access to the underlying connection. The function
 * also: (1) synchronously assigns locally queued tasks to the task tracker, (2)
 * issues an asynchronous task status query for one assigned task at a time, and
 * (3) retrieves status query results for the previously issued status query.
 */
static void
ManageTaskTracker(TaskTracker *taskTracker)
{
	bool trackerHealthy = TrackerHealthy(taskTracker);
	if (!trackerHealthy)
	{
		return;
	}

	bool trackerConnectionUp = TrackerConnectionUp(taskTracker);
	if (!trackerConnectionUp)
	{
		TrackerReconnectPoll(taskTracker);  /* try an async reconnect */
		return;
	}

	/*
	 * (1) We first synchronously assign any pending new tasks. We also make
	 * sure not to block execution on one task tracker for a long time.
	 */
	if (!taskTracker->connectionBusy)
	{
		List *previousTaskList = taskTracker->assignedTaskList;
		List *newTaskList = AssignQueuedTasks(taskTracker);

		taskTracker->assignedTaskList = list_concat(previousTaskList, newTaskList);
	}

	/*
	 * (2) We find assigned tasks. We then send an asynchronous query to check
	 * the tasks' statuses.
	 */
	if (!taskTracker->connectionBusy)
	{
		List *taskStatusBatchList = TaskStatusBatchList(taskTracker);

		/* if we have running tasks, check their status */
		if (taskStatusBatchList)
		{
			int32 connectionId = taskTracker->connectionId;

			StringInfo taskStatusBatchQuery = TaskStatusBatchQuery(taskStatusBatchList);

			bool querySent = MultiClientSendQuery(connectionId,
												  taskStatusBatchQuery->data);
			if (querySent)
			{
				taskTracker->connectionBusy = true;
				taskTracker->connectionBusyOnTaskList = taskStatusBatchList;
			}
			else
			{
				/* mark only first task in list as failed */
				TrackerTaskState *taskState = (TrackerTaskState *) linitial(
					taskStatusBatchList);
				taskState->status = TASK_CLIENT_SIDE_STATUS_FAILED;

				list_free(taskStatusBatchList);

				taskTracker->connectionBusy = false;
				taskTracker->connectionBusyOnTaskList = NIL;
			}

			pfree(taskStatusBatchQuery);
		}
	}

	/*
	 * (3) check if results are ready for previously issued task status query
	 */
	if (taskTracker->connectionBusy)
	{
		int32 connectionId = taskTracker->connectionId;

		/* if connection is available, update task status accordingly */
		ResultStatus resultStatus = MultiClientResultStatus(connectionId);
		if (resultStatus == CLIENT_RESULT_READY)
		{
			ReceiveTaskStatusBatchQueryResponse(taskTracker);
		}
		else if (resultStatus == CLIENT_RESULT_UNAVAILABLE)
		{
			TrackerTaskState *taskState = (TrackerTaskState *) linitial(
				taskTracker->connectionBusyOnTaskList);
			Assert(taskState != NULL);
			taskState->status = TASK_CLIENT_SIDE_STATUS_FAILED;
		}

		/* if connection is available, give it back to the task tracker */
		if (resultStatus != CLIENT_RESULT_BUSY)
		{
			list_free(taskTracker->connectionBusyOnTaskList);

			taskTracker->connectionBusy = false;
			taskTracker->connectionBusyOnTaskList = NIL;
		}
	}
}


/*
 * TrackerConnectionUp checks the most recent connection status for the given
 * task tracker. The function returns true if the connection is still up.
 */
static bool
TrackerConnectionUp(TaskTracker *taskTracker)
{
	bool connectionUp = false;

	/* if we think we have a connection, check its most recent status */
	if (taskTracker->trackerStatus == TRACKER_CONNECTED)
	{
		connectionUp = MultiClientConnectionUp(taskTracker->connectionId);
	}

	return connectionUp;
}


/*
 * TrackerReconnectPoll checks if we have an open connection to the given task
 * tracker. If not, the function opens an asynchronous connection to the task
 * tracker and polls this connection's status on every call. The function also
 * sets the task tracker's internal state.
 */
static void
TrackerReconnectPoll(TaskTracker *taskTracker)
{
	TrackerStatus currentStatus = taskTracker->trackerStatus;
	if (currentStatus == TRACKER_CONNECTED)
	{
		bool connectionUp = MultiClientConnectionUp(taskTracker->connectionId);
		if (connectionUp)
		{
			taskTracker->trackerStatus = TRACKER_CONNECTED;
		}
		else
		{
			taskTracker->trackerStatus = TRACKER_CONNECTION_FAILED;

			/* we lost the connection underneath us, clean it up */
			MultiClientDisconnect(taskTracker->connectionId);
			taskTracker->connectionId = INVALID_CONNECTION_ID;
		}
	}
	else if (currentStatus == TRACKER_CONNECT_START ||
			 currentStatus == TRACKER_CONNECT_POLL)
	{
		taskTracker->trackerStatus = TrackerConnectPoll(taskTracker);
	}
	else if (currentStatus == TRACKER_CONNECTION_FAILED)
	{
		taskTracker->connectionFailureCount++;
		taskTracker->connectPollCount = 0;

		taskTracker->trackerStatus = TRACKER_CONNECT_START;
	}
}


/*
 * AssignQueuedTasks walks over the given task tracker's task state hash, finds
 * queued tasks in this hash, and synchronously assigns them to the given task
 * tracker. The function then returns the list of newly assigned tasks.
 */
static List *
AssignQueuedTasks(TaskTracker *taskTracker)
{
	HTAB *taskStateHash = taskTracker->taskStateHash;
	List *assignedTaskList = NIL;
	uint32 taskAssignmentCount = 0;
	List *tasksToAssignList = NIL;
	StringInfo assignTaskBatchQuery = makeStringInfo();
	int32 connectionId = taskTracker->connectionId;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, taskStateHash);

	TrackerTaskState *taskState = (TrackerTaskState *) hash_seq_search(&status);
	while (taskState != NULL)
	{
		if (taskState->status == TASK_CLIENT_SIDE_QUEUED)
		{
			StringInfo taskAssignmentQuery = taskState->taskAssignmentQuery;

			appendStringInfo(assignTaskBatchQuery, "%s", taskAssignmentQuery->data);

			tasksToAssignList = lappend(tasksToAssignList, taskState);
			taskAssignmentCount++;
			if (taskAssignmentCount >= MaxAssignTaskBatchSize)
			{
				hash_seq_term(&status);
				break;
			}
		}

		taskState = (TrackerTaskState *) hash_seq_search(&status);
	}

	if (taskAssignmentCount > 0)
	{
		void *queryResult = NULL;
		int rowCount = 0;
		int columnCount = 0;

		bool batchSuccess = MultiClientSendQuery(connectionId,
												 assignTaskBatchQuery->data);

		foreach_ptr(taskState, tasksToAssignList)
		{
			if (!batchSuccess)
			{
				taskState->status = TASK_CLIENT_SIDE_ASSIGN_FAILED;
				continue;
			}

			BatchQueryStatus queryStatus = MultiClientBatchResult(connectionId,
																  &queryResult,
																  &rowCount,
																  &columnCount);
			if (queryStatus == CLIENT_BATCH_QUERY_CONTINUE)
			{
				taskState->status = TASK_ASSIGNED;
				assignedTaskList = lappend(assignedTaskList, taskState);
			}
			else
			{
				taskState->status = TASK_CLIENT_SIDE_ASSIGN_FAILED;
				batchSuccess = false;
			}

			MultiClientClearResult(queryResult);
		}

		/* call MultiClientBatchResult one more time to finish reading results */
		MultiClientBatchResult(connectionId, &queryResult, &rowCount, &columnCount);
		Assert(queryResult == NULL);

		pfree(assignTaskBatchQuery);
		list_free(tasksToAssignList);
	}

	return assignedTaskList;
}


/*
 * TaskStatusBatchList returns a list containing up to MaxTaskStatusBatchSize
 * tasks from the list of assigned tasks. When the number of tasks is greater
 * than the maximum, the next call of this function will continue in the
 * assigned task list after the last task that was added to the current list.
 *
 * In some cases the list may be empty even if tasks have been assigned due to
 * wrap-around, namely if we first generate a batch of MaxTaskStatusBatchSize,
 * but none of the remaining tasks in assignedTaskList are running.
 */
static List *
TaskStatusBatchList(TaskTracker *taskTracker)
{
	int32 assignedTaskIndex = 0;
	List *assignedTaskList = taskTracker->assignedTaskList;
	List *taskStatusBatchList = NIL;

	int32 assignedTaskCount = list_length(assignedTaskList);
	if (assignedTaskCount == 0)
	{
		return NIL;
	}

	int32 lastTaskIndex = (assignedTaskCount - 1);
	int32 currentTaskIndex = taskTracker->currentTaskIndex;
	if (currentTaskIndex >= lastTaskIndex)
	{
		currentTaskIndex = -1;
	}

	TrackerTaskState *assignedTask = NULL;
	foreach_ptr(assignedTask, assignedTaskList)
	{
		TaskStatus taskStatus = assignedTask->status;

		bool taskRunning = false;
		if (taskStatus == TASK_ASSIGNED || taskStatus == TASK_SCHEDULED ||
			taskStatus == TASK_RUNNING || taskStatus == TASK_FAILED)
		{
			taskRunning = true;
		}

		if (taskRunning && (assignedTaskIndex > currentTaskIndex))
		{
			taskStatusBatchList = lappend(taskStatusBatchList, assignedTask);
			if (list_length(taskStatusBatchList) >= MaxTaskStatusBatchSize)
			{
				break;
			}
		}

		assignedTaskIndex++;
	}

	/* continue where we left off next time this function is called */
	taskTracker->currentTaskIndex = assignedTaskIndex;

	return taskStatusBatchList;
}


/*
 * TaskStatusBatchQuery builds a command string containing multiple
 * task_tracker_task_status queries from a TrackerTaskState list.
 */
static StringInfo
TaskStatusBatchQuery(List *taskList)
{
	StringInfo taskStatusBatchQuery = makeStringInfo();

	TrackerTaskState *taskState = NULL;
	foreach_ptr(taskState, taskList)
	{
		appendStringInfo(taskStatusBatchQuery, TASK_STATUS_QUERY,
						 taskState->jobId, taskState->taskId);
	}

	return taskStatusBatchQuery;
}


/*
 * ReceiveTaskStatusBatchQueryResponse assumes that a batch of task status
 * queries have been previously sent to the given task tracker, and receives
 * and processes the responses for these status queries. If a status check fails
 * only one task status is marked as failed and the remainder is considered not
 * executed.
 */
static void
ReceiveTaskStatusBatchQueryResponse(TaskTracker *taskTracker)
{
	List *checkedTaskList = taskTracker->connectionBusyOnTaskList;
	int32 connectionId = taskTracker->connectionId;
	int rowCount = 0;
	int columnCount = 0;
	void *queryResult = NULL;

	TrackerTaskState *checkedTask = NULL;
	foreach_ptr(checkedTask, checkedTaskList)
	{
		TaskStatus taskStatus = TASK_STATUS_INVALID_FIRST;

		BatchQueryStatus queryStatus = MultiClientBatchResult(connectionId, &queryResult,
															  &rowCount, &columnCount);
		if (queryStatus == CLIENT_BATCH_QUERY_CONTINUE)
		{
			char *valueString = MultiClientGetValue(queryResult, 0, 0);
			if (valueString == NULL || (*valueString) == '\0')
			{
				taskStatus = TASK_PERMANENTLY_FAILED;
			}
			else
			{
				char *valueStringEnd = NULL;
				errno = 0;

				taskStatus = strtoul(valueString, &valueStringEnd, 0);
				if (errno != 0 || (*valueStringEnd) != '\0')
				{
					/* we couldn't parse received integer */
					taskStatus = TASK_PERMANENTLY_FAILED;
				}

				Assert(taskStatus > TASK_STATUS_INVALID_FIRST);
				Assert(taskStatus < TASK_STATUS_LAST);
			}
		}
		else
		{
			taskStatus = TASK_CLIENT_SIDE_STATUS_FAILED;
		}

		checkedTask->status = taskStatus;

		MultiClientClearResult(queryResult);

		if (queryStatus == CLIENT_BATCH_QUERY_FAILED)
		{
			/* remaining queries were not executed */
			break;
		}
	}

	/* call MultiClientBatchResult one more time to finish reading results */
	MultiClientBatchResult(connectionId, &queryResult, &rowCount, &columnCount);
	Assert(queryResult == NULL);
}


/*
 * ManageTransmitTracker manages access to the connection we opened to the worker
 * node. If the connection is idle, and we have file transmit requests pending,
 * the function picks a pending file transmit request, and starts the Copy Out
 * protocol to copy the file's contents.
 */
static void
ManageTransmitTracker(TaskTracker *transmitTracker)
{
	bool trackerHealthy = TrackerHealthy(transmitTracker);
	if (!trackerHealthy)
	{
		return;
	}

	bool trackerConnectionUp = TrackerConnectionUp(transmitTracker);
	if (!trackerConnectionUp)
	{
		TrackerReconnectPoll(transmitTracker);  /* try an async reconnect */
		return;
	}

	/* connection belongs to another file transmit */
	if (transmitTracker->connectionBusy)
	{
		return;
	}

	TrackerTaskState *transmitState = NextQueuedFileTransmit(
		transmitTracker->taskStateHash);
	if (transmitState != NULL)
	{
		int32 connectionId = transmitTracker->connectionId;
		StringInfo jobDirectoryName = JobDirectoryName(transmitState->jobId);
		StringInfo taskFilename = TaskFilename(jobDirectoryName, transmitState->taskId);
		char *userName = CurrentUserName();

		StringInfo fileTransmitQuery = makeStringInfo();
		appendStringInfo(fileTransmitQuery, TRANSMIT_WITH_USER_COMMAND,
						 taskFilename->data, quote_literal_cstr(userName));

		bool fileTransmitStarted = MultiClientSendQuery(connectionId,
														fileTransmitQuery->data);
		if (fileTransmitStarted)
		{
			transmitState->status = TASK_ASSIGNED;

			transmitTracker->connectionBusy = true;
			transmitTracker->connectionBusyOnTask = transmitState;
		}
		else
		{
			transmitState->status = TASK_CLIENT_SIDE_TRANSMIT_FAILED;

			transmitTracker->connectionBusy = false;
			transmitTracker->connectionBusyOnTask = NULL;
		}
	}
}


/*
 * NextQueuedFileTransmit walks over all tasks in the given hash, and looks for
 * a file transmit task that has been queued, but not served yet.
 */
static TrackerTaskState *
NextQueuedFileTransmit(HTAB *taskStateHash)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, taskStateHash);

	TrackerTaskState *taskState = (TrackerTaskState *) hash_seq_search(&status);
	while (taskState != NULL)
	{
		if (taskState->status == TASK_FILE_TRANSMIT_QUEUED)
		{
			hash_seq_term(&status);
			break;
		}

		taskState = (TrackerTaskState *) hash_seq_search(&status);
	}

	return taskState;
}


/*
 * JobIdList walks over all jobs in the given job tree and retrieves each job's
 * identifier. The function then inserts these job identifiers in a new list and
 * returns this list.
 */
static List *
JobIdList(Job *job)
{
	List *jobIdList = NIL;

	/*
	 * We walk over the job tree using breadth-first search. For this, we first
	 * queue the root node, and then start traversing our search space.
	 */
	List *jobQueue = list_make1(job);
	while (jobQueue != NIL)
	{
		uint64 *jobIdPointer = (uint64 *) palloc0(sizeof(uint64));

		Job *currJob = (Job *) linitial(jobQueue);
		jobQueue = list_delete_first(jobQueue);

		(*jobIdPointer) = currJob->jobId;
		jobIdList = lappend(jobIdList, jobIdPointer);

		/* prevent dependentJobList being modified on list_concat() call */
		List *jobChildrenList = list_copy(currJob->dependentJobList);
		if (jobChildrenList != NIL)
		{
			jobQueue = list_concat(jobQueue, jobChildrenList);
		}
	}

	return jobIdList;
}


/*
 * TrackerCleanupResources cleans up remote and local resources associated with
 * the query. To clean up remote resources, the function cancels ongoing transmit
 * tasks. It also waits for ongoing requests to the task trackers to complete
 * before assigning "job clean up" tasks to them. To reclaim local resources,
 * the function closes open file descriptors and disconnects from task trackers.
 */
static void
TrackerCleanupResources(HTAB *taskTrackerHash, HTAB *transmitTrackerHash,
						List *jobIdList, List *taskList)
{
	/*
	 * We are done with query execution. We now wait for open requests to the task
	 * trackers to complete and cancel any open requests to the transmit trackers.
	 */
	TrackerHashWaitActiveRequest(taskTrackerHash);
	TrackerHashCancelActiveRequest(transmitTrackerHash);

	/* only close open files; open connections are owned by trackers */
	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		TaskExecution *taskExecution = task->taskExecution;

		CleanupTaskExecution(taskExecution);
		task->taskExecution = NULL;
	}

	/*
	 * For each executed job, we create a special task to clean up its resources
	 * on worker nodes, and send this clean-up task to all task trackers.
	 */
	uint64 *jobIdPointer = NULL;
	foreach_ptr(jobIdPointer, jobIdList)
	{
		Task *jobCleanupTask = JobCleanupTask(*jobIdPointer);
		TrackerHashCleanupJob(taskTrackerHash, jobCleanupTask);
	}

	TrackerHashDisconnect(taskTrackerHash);
	TrackerHashDisconnect(transmitTrackerHash);
}


/*
 * TrackerHashWaitActiveRequest walks over task trackers in the given hash, and
 * checks if they have an ongoing request. If they do, the function waits for
 * the request to complete. If the request completes successfully, the function
 * frees the connection for future tasks.
 */
static void
TrackerHashWaitActiveRequest(HTAB *taskTrackerHash)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, taskTrackerHash);

	TaskTracker *taskTracker = (TaskTracker *) hash_seq_search(&status);
	while (taskTracker != NULL)
	{
		bool trackerConnectionUp = TrackerConnectionUp(taskTracker);

		/* if we have an ongoing request, block until we have a response */
		if (trackerConnectionUp && taskTracker->connectionBusy)
		{
			QueryStatus queryStatus = MultiClientQueryStatus(taskTracker->connectionId);
			if (queryStatus == CLIENT_QUERY_DONE)
			{
				taskTracker->connectionBusy = false;
				taskTracker->connectionBusyOnTask = NULL;
				taskTracker->connectionBusyOnTaskList = NIL;
			}
		}

		taskTracker = (TaskTracker *) hash_seq_search(&status);
	}
}


/*
 * TrackerHashCancelActiveRequest walks over task trackers in the given hash,
 * and checks if they have an ongoing request. If they do, the function sends a
 * cancel message on that connection.
 */
static void
TrackerHashCancelActiveRequest(HTAB *taskTrackerHash)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, taskTrackerHash);

	TaskTracker *taskTracker = (TaskTracker *) hash_seq_search(&status);
	while (taskTracker != NULL)
	{
		bool trackerConnectionUp = TrackerConnectionUp(taskTracker);

		/* if we have an ongoing request, send cancel message */
		if (trackerConnectionUp && taskTracker->connectionBusy)
		{
			MultiClientCancel(taskTracker->connectionId);
		}

		taskTracker = (TaskTracker *) hash_seq_search(&status);
	}
}


/*
 * JobCleanupTask creates a special task to clean up all resources associated
 * with a given job on the worker node. The function then returns this task.
 */
static Task *
JobCleanupTask(uint64 jobId)
{
	StringInfo jobCleanupQuery = makeStringInfo();
	appendStringInfo(jobCleanupQuery, JOB_CLEANUP_QUERY, jobId);

	Task *jobCleanupTask = CitusMakeNode(Task);
	jobCleanupTask->jobId = jobId;
	jobCleanupTask->taskId = JOB_CLEANUP_TASK_ID;
	jobCleanupTask->replicationModel = REPLICATION_MODEL_INVALID;
	SetTaskQueryString(jobCleanupTask, jobCleanupQuery->data);

	return jobCleanupTask;
}


/*
 * TrackerHashCleanupJob walks over task trackers in the given hash, and assigns
 * a job cleanup task to the tracker if the tracker's connection is available.
 * The function then walks over task trackers to which it sent a cleanup task,
 * checks the request's status, and emits an appropriate status message.
 */
static void
TrackerHashCleanupJob(HTAB *taskTrackerHash, Task *jobCleanupTask)
{
	uint64 jobId = jobCleanupTask->jobId;
	List *taskTrackerList = NIL;
	const long statusCheckInterval = 10000; /* microseconds */
	bool timedOut = false;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, taskTrackerHash);

	/* walk over task trackers and try to issue job clean up requests */
	TaskTracker *taskTracker = (TaskTracker *) hash_seq_search(&status);
	while (taskTracker != NULL)
	{
		bool trackerConnectionUp = TrackerConnectionUp(taskTracker);
		if (trackerConnectionUp)
		{
			bool jobCleanupQuerySent = false;

			/* if we have a clear connection, send cleanup job */
			if (!taskTracker->connectionBusy)
			{
				/* assign through task tracker to manage resource utilization */
				StringInfo jobCleanupQuery = TaskAssignmentQuery(
					jobCleanupTask, TaskQueryStringForAllPlacements(jobCleanupTask));

				jobCleanupQuerySent = MultiClientSendQuery(taskTracker->connectionId,
														   jobCleanupQuery->data);
			}

			/*
			 * If cleanup query was sent, mark that the connection is busy and
			 * hold onto the task tracker to check status.
			 */
			if (jobCleanupQuerySent)
			{
				taskTracker->connectionBusy = true;
				taskTrackerList = lappend(taskTrackerList, taskTracker);
			}
			else
			{
				const char *nodeName = taskTracker->workerName;
				uint32 nodePort = taskTracker->workerPort;

				ereport(WARNING, (errmsg("could not assign cleanup query for job "
										 UINT64_FORMAT " to node \"%s:%u\"",
										 jobId, nodeName, nodePort)));
			}
		}

		taskTracker = (TaskTracker *) hash_seq_search(&status);
	}

	/* record the time when we start waiting for cleanup jobs to be sent */
	TimestampTz startTime = GetCurrentTimestamp();

	/*
	 * Walk over task trackers to which we sent clean up requests. Perform
	 * these checks until it times out.
	 *
	 * We want to determine timedOut flag after the loop start to make sure
	 * we iterate one more time after time out occurs. This is necessary to report
	 * warning messages for timed out cleanup jobs.
	 */
	List *remainingTaskTrackerList = taskTrackerList;
	while (list_length(remainingTaskTrackerList) > 0 && !timedOut)
	{
		List *activeTackTrackerList = remainingTaskTrackerList;

		remainingTaskTrackerList = NIL;

		pg_usleep(statusCheckInterval);
		TimestampTz currentTime = GetCurrentTimestamp();
		timedOut = TimestampDifferenceExceeds(startTime, currentTime,
											  NodeConnectionTimeout);

		foreach_ptr(taskTracker, activeTackTrackerList)
		{
			int32 connectionId = taskTracker->connectionId;
			char *nodeName = taskTracker->workerName;
			uint32 nodePort = taskTracker->workerPort;

			ResultStatus resultStatus = MultiClientResultStatus(connectionId);
			if (resultStatus == CLIENT_RESULT_READY)
			{
				QueryStatus queryStatus = MultiClientQueryStatus(connectionId);
				if (queryStatus == CLIENT_QUERY_DONE)
				{
					ereport(DEBUG4, (errmsg("completed cleanup query for job "
											UINT64_FORMAT, jobId)));

					/* clear connection for future cleanup queries */
					taskTracker->connectionBusy = false;
				}
				else if (timedOut)
				{
					ereport(WARNING, (errmsg("could not receive response for cleanup "
											 "query status for job " UINT64_FORMAT
											 " on node \"%s:%u\" with status %d",
											 jobId,
											 nodeName, nodePort, (int) queryStatus),
									  errhint("Manually clean job resources on node "
											  "\"%s:%u\" by running \"%s\" ", nodeName,
											  nodePort, TaskQueryStringForAllPlacements(
												  jobCleanupTask))));
				}
				else
				{
					remainingTaskTrackerList = lappend(remainingTaskTrackerList,
													   taskTracker);
				}
			}
			else if (resultStatus == CLIENT_RESULT_UNAVAILABLE || timedOut)
			{
				/* CLIENT_RESULT_UNAVAILABLE is returned if the connection failed somehow */
				ereport(WARNING, (errmsg("could not receive response for cleanup query "
										 "result for job " UINT64_FORMAT
										 " on node \"%s:%u\" with status %d",
										 jobId, nodeName,
										 nodePort, (int) resultStatus),
								  errhint("Manually clean job resources on node "
										  "\"%s:%u\" by running \"%s\" ", nodeName,
										  nodePort, TaskQueryStringForAllPlacements(
											  jobCleanupTask))));
			}
			else
			{
				remainingTaskTrackerList = lappend(remainingTaskTrackerList, taskTracker);
			}
		}
	}
}


/*
 * TrackerHashDisconnect walks over task trackers in the given hash, and closes
 * open connections to them.
 */
static void
TrackerHashDisconnect(HTAB *taskTrackerHash)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, taskTrackerHash);

	TaskTracker *taskTracker = (TaskTracker *) hash_seq_search(&status);
	while (taskTracker != NULL)
	{
		if (taskTracker->connectionId != INVALID_CONNECTION_ID)
		{
			MultiClientDisconnect(taskTracker->connectionId);
			taskTracker->connectionId = INVALID_CONNECTION_ID;
		}

		taskTracker = (TaskTracker *) hash_seq_search(&status);
	}
}


/*
 * TaskTrackerExecScan is a callback function which returns next tuple from a
 * task-tracker execution. In the first call, it executes distributed task-tracker
 * plan and loads results from temporary files into custom scan's tuple store.
 * Then, it returns tuples one by one from this tuple store.
 */
TupleTableSlot *
TaskTrackerExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (!scanState->finishedRemoteScan)
	{
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Job *workerJob = distributedPlan->workerJob;
		Query *jobQuery = workerJob->jobQuery;

		ErrorIfTransactionAccessedPlacementsLocally();
		DisableLocalExecution();

		if (ContainsReadIntermediateResultFunction((Node *) jobQuery))
		{
			ereport(ERROR, (errmsg("Complex subqueries and CTEs are not supported when "
								   "task_executor_type is set to 'task-tracker'")));
		}

		/* we are taking locks on partitions of partitioned tables */
		LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);

		PrepareMasterJobDirectory(workerJob);
		MultiTaskTrackerExecute(workerJob);

		LoadTuplesIntoTupleStore(scanState, workerJob);

		scanState->finishedRemoteScan = true;
	}

	TupleTableSlot *resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * PrepareMasterJobDirectory creates a directory on the master node to keep job
 * execution results. We also register this directory for automatic cleanup on
 * portal delete.
 */
static void
PrepareMasterJobDirectory(Job *workerJob)
{
	StringInfo jobDirectoryName = MasterJobDirectoryName(workerJob->jobId);
	CitusCreateDirectory(jobDirectoryName);

	ResourceOwnerEnlargeJobDirectories(CurrentResourceOwner);
	ResourceOwnerRememberJobDirectory(CurrentResourceOwner, workerJob->jobId);
}
