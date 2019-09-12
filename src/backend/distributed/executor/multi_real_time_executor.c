/*-------------------------------------------------------------------------
 *
 * multi_real_time_executor.c
 *
 * Routines for executing remote tasks as part of a distributed execution plan
 * in real-time. These routines open up a separate connection for each task they
 * need to execute, and therefore return their results faster. However, they can
 * only handle as many tasks as the number of file descriptors (connections)
 * available. They also can't handle execution primitives that need to write
 * their results to intermediate files.
 *
 * Copyright (c) 2013-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/connection_management.h"
#include "distributed/local_executor.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "storage/fd.h"
#include "utils/timestamp.h"


/*
 * GUC that determines whether a SELECT in a transaction block should also run in
 * a transaction block on the worker even if no writes have occurred yet.
 */
bool SelectOpensTransactionBlock;


/* Local functions forward declarations */
static ConnectAction ManageTaskExecution(Task *task, TaskExecution *taskExecution,
										 TaskExecutionStatus *executionStatus,
										 DistributedExecutionStats *executionStats);
static bool TaskExecutionReadyToStart(TaskExecution *taskExecution);
static bool TaskExecutionCompleted(TaskExecution *taskExecution);
static void CancelTaskExecutionIfActive(TaskExecution *taskExecution);
static void CancelRequestIfActive(TaskExecStatus taskStatus, int connectionId);

/* Worker node state hash functions */
static HTAB * WorkerHash(const char *workerHashName, List *workerNodeList);
static HTAB * WorkerHashCreate(const char *workerHashName, uint32 workerHashSize);
static WorkerNodeState * WorkerHashEnter(HTAB *workerHash,
										 char *nodeName, uint32 nodePort);
static WorkerNodeState * WorkerHashLookup(HTAB *workerHash,
										  const char *nodeName, uint32 nodePort);
static WorkerNodeState * LookupWorkerForTask(HTAB *workerHash, Task *task,
											 TaskExecution *taskExecution);

/* Throttling functions */
static bool WorkerConnectionsExhausted(WorkerNodeState *workerNodeState);
static bool MasterConnectionsExhausted(HTAB *workerHash);
static uint32 TotalOpenConnectionCount(HTAB *workerHash);
static void UpdateConnectionCounter(WorkerNodeState *workerNode,
									ConnectAction connectAction);


/*
 * MultiRealTimeExecute loops over the given tasks, and manages their execution
 * until either one task permanently fails or all tasks successfully complete.
 * The function opens up a connection for each task it needs to execute, and
 * manages these tasks' execution in real-time.
 */
void
MultiRealTimeExecute(Job *job)
{
	List *taskList = job->taskList;
	List *taskExecutionList = NIL;
	ListCell *taskExecutionCell = NULL;
	ListCell *taskCell = NULL;
	uint32 failedTaskId = 0;
	bool allTasksCompleted = false;
	bool taskCompleted = false;
	bool taskFailed = false;
	bool sizeLimitIsExceeded = false;
	DistributedExecutionStats executionStats = { 0 };

	List *workerNodeList = NIL;
	HTAB *workerHash = NULL;
	const char *workerHashName = "Worker node hash";
	WaitInfo *waitInfo = MultiClientCreateWaitInfo(list_length(taskList));

	workerNodeList = ActiveReadableNodeList();
	workerHash = WorkerHash(workerHashName, workerNodeList);

	if (IsMultiStatementTransaction() && SelectOpensTransactionBlock)
	{
		BeginOrContinueCoordinatedTransaction();
	}

	RecordParallelRelationAccessForTaskList(taskList);

	/* initialize task execution structures for remote execution */
	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		TaskExecution *taskExecution = NULL;

		taskExecution = InitTaskExecution(task, EXEC_TASK_CONNECT_START);
		taskExecutionList = lappend(taskExecutionList, taskExecution);
	}

	/* loop around until all tasks complete, one task fails, or user cancels */
	while (!(allTasksCompleted || taskFailed || QueryCancelPending ||
			 sizeLimitIsExceeded))
	{
		uint32 taskCount = list_length(taskList);
		uint32 completedTaskCount = 0;

		/* loop around all tasks and manage them */
		taskCell = NULL;
		taskExecutionCell = NULL;

		MultiClientResetWaitInfo(waitInfo);

		forboth(taskCell, taskList, taskExecutionCell, taskExecutionList)
		{
			Task *task = (Task *) lfirst(taskCell);
			TaskExecution *taskExecution = (TaskExecution *) lfirst(taskExecutionCell);
			ConnectAction connectAction = CONNECT_ACTION_NONE;
			WorkerNodeState *workerNodeState = NULL;
			TaskExecutionStatus executionStatus;

			workerNodeState = LookupWorkerForTask(workerHash, task, taskExecution);

			/* in case the task is about to start, throttle if necessary */
			if (TaskExecutionReadyToStart(taskExecution) &&
				(WorkerConnectionsExhausted(workerNodeState) ||
				 MasterConnectionsExhausted(workerHash)))
			{
				continue;
			}

			/* call the function that performs the core task execution logic */
			connectAction = ManageTaskExecution(task, taskExecution, &executionStatus,
												&executionStats);

			/* update the connection counter for throttling */
			UpdateConnectionCounter(workerNodeState, connectAction);

			/*
			 * If this task failed, we need to iterate over task executions, and
			 * manually clean out their client-side resources. Hence, we record
			 * the failure here instead of immediately erroring out.
			 */
			taskFailed = TaskExecutionFailed(taskExecution);
			if (taskFailed)
			{
				failedTaskId = taskExecution->taskId;
				break;
			}

			taskCompleted = TaskExecutionCompleted(taskExecution);
			if (taskCompleted)
			{
				completedTaskCount++;
			}
			else
			{
				uint32 currentIndex = taskExecution->currentNodeIndex;
				int32 *connectionIdArray = taskExecution->connectionIdArray;
				int32 connectionId = connectionIdArray[currentIndex];

				/*
				 * If not done with the task yet, make note of what this task
				 * and its associated connection is waiting for.
				 */
				MultiClientRegisterWait(waitInfo, executionStatus, connectionId);
			}
		}

		/* in case the task has intermediate results */
		if (CheckIfSizeLimitIsExceeded(&executionStats))
		{
			sizeLimitIsExceeded = true;
			break;
		}

		/*
		 * Check if all tasks completed; otherwise wait as appropriate to
		 * avoid a tight loop. That means we immediately continue if tasks are
		 * ready to be processed further, and block when we're waiting for
		 * network IO.
		 */
		if (completedTaskCount == taskCount)
		{
			allTasksCompleted = true;
		}
		else
		{
			MultiClientWait(waitInfo);
		}

#ifdef WIN32

		/*
		 * Don't call CHECK_FOR_INTERRUPTS because we want to clean up after ourselves,
		 * calling pgwin32_dispatch_queued_signals sets QueryCancelPending so we leave
		 * the loop.
		 */
		pgwin32_dispatch_queued_signals();
#endif
	}

	MultiClientFreeWaitInfo(waitInfo);

	/*
	 * We prevent cancel/die interrupts until we clean up connections to worker
	 * nodes. Note that for the above while loop, if the user Ctrl+C's a query
	 * and we emit a warning before looping to the beginning of the while loop,
	 * we will get canceled away before we can hold any interrupts.
	 */
	HOLD_INTERRUPTS();

	/* cancel any active task executions */
	taskExecutionCell = NULL;
	foreach(taskExecutionCell, taskExecutionList)
	{
		TaskExecution *taskExecution = (TaskExecution *) lfirst(taskExecutionCell);
		CancelTaskExecutionIfActive(taskExecution);
	}

	/*
	 * If cancel might have been sent, give remote backends some time to flush
	 * their responses. This avoids some broken pipe logs on the backend-side.
	 *
	 * FIXME: This shouldn't be dependant on RemoteTaskCheckInterval; they're
	 * unrelated type of delays.
	 */
	if (taskFailed || QueryCancelPending)
	{
		long sleepInterval = RemoteTaskCheckInterval * 1000L;
		pg_usleep(sleepInterval);
	}

	/* close connections and open files */
	taskExecutionCell = NULL;
	foreach(taskExecutionCell, taskExecutionList)
	{
		TaskExecution *taskExecution = (TaskExecution *) lfirst(taskExecutionCell);
		CleanupTaskExecution(taskExecution);
	}

	RESUME_INTERRUPTS();

	/*
	 * If we previously broke out of the execution loop due to a task failure or
	 * user cancellation request, we can now safely emit an error message (all
	 * client-side resources have been cleared).
	 */
	if (sizeLimitIsExceeded)
	{
		ErrorSizeLimitIsExceeded();
	}
	else if (taskFailed)
	{
		ereport(ERROR, (errmsg("failed to execute task %u", failedTaskId)));
	}
	else if (QueryCancelPending)
	{
		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * ManageTaskExecution manages all execution logic for the given task. For this,
 * the function starts a new "execution" on a node, and tracks this execution's
 * progress. On failure, the function restarts this execution on another node.
 * Note that this function directly manages a task's execution by opening up a
 * separate connection to the worker node for each execution. The function
 * returns a ConnectAction enum indicating whether a connection has been opened
 * or closed in this call.  Via the executionStatus parameter this function returns
 * what a Task is blocked on.
 */
static ConnectAction
ManageTaskExecution(Task *task, TaskExecution *taskExecution,
					TaskExecutionStatus *executionStatus,
					DistributedExecutionStats *executionStats)
{
	TaskExecStatus *taskStatusArray = taskExecution->taskStatusArray;
	int32 *connectionIdArray = taskExecution->connectionIdArray;
	int32 *fileDescriptorArray = taskExecution->fileDescriptorArray;
	uint32 currentIndex = taskExecution->currentNodeIndex;
	TaskExecStatus currentStatus = taskStatusArray[currentIndex];
	List *taskPlacementList = task->taskPlacementList;
	ShardPlacement *taskPlacement = list_nth(taskPlacementList, currentIndex);
	ConnectAction connectAction = CONNECT_ACTION_NONE;

	/* as most state transitions don't require blocking, default to not waiting */
	*executionStatus = TASK_STATUS_READY;

	switch (currentStatus)
	{
		case EXEC_TASK_CONNECT_START:
		{
			int32 connectionId = INVALID_CONNECTION_ID;
			List *relationShardList = task->relationShardList;
			List *placementAccessList = NIL;

			/* create placement accesses for placements that appear in a subselect */
			placementAccessList = BuildPlacementSelectList(taskPlacement->groupId,
														   relationShardList);

			/*
			 * Should at least have an entry for the anchor shard. If this is not the
			 * case, we should have errored out in the physical planner. We are
			 * rechecking here until we find the root cause of
			 * https://github.com/citusdata/citus/issues/2092.
			 */
			if (placementAccessList == NIL)
			{
				ereport(WARNING, (errmsg("could not find any placements for task %d",
										 task->taskId)));
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;

				break;
			}

			connectionId = MultiClientPlacementConnectStart(placementAccessList,
															NULL);
			connectionIdArray[currentIndex] = connectionId;

			/* if valid, poll the connection until the connection is initiated */
			if (connectionId != INVALID_CONNECTION_ID)
			{
				taskStatusArray[currentIndex] = EXEC_TASK_CONNECT_POLL;
				taskExecution->connectStartTime = GetCurrentTimestamp();
				connectAction = CONNECT_ACTION_OPENED;
			}
			else
			{
				*executionStatus = TASK_STATUS_ERROR;
				AdjustStateForFailure(taskExecution);
				break;
			}

			break;
		}

		case EXEC_TASK_CONNECT_POLL:
		{
			int32 connectionId = connectionIdArray[currentIndex];
			ConnectStatus pollStatus = MultiClientConnectPoll(connectionId);

			/*
			 * If the connection is established, we change our state based on
			 * whether a coordinated transaction has been started.
			 */
			if (pollStatus == CLIENT_CONNECTION_READY)
			{
				if (InCoordinatedTransaction())
				{
					taskStatusArray[currentIndex] = EXEC_BEGIN_START;
				}
				else
				{
					taskStatusArray[currentIndex] = EXEC_COMPUTE_TASK_START;
				}
			}
			else if (pollStatus == CLIENT_CONNECTION_BUSY)
			{
				/* immediately retry */
				taskStatusArray[currentIndex] = EXEC_TASK_CONNECT_POLL;
			}
			else if (pollStatus == CLIENT_CONNECTION_BUSY_READ)
			{
				*executionStatus = TASK_STATUS_SOCKET_READ;
				taskStatusArray[currentIndex] = EXEC_TASK_CONNECT_POLL;
			}
			else if (pollStatus == CLIENT_CONNECTION_BUSY_WRITE)
			{
				*executionStatus = TASK_STATUS_SOCKET_WRITE;
				taskStatusArray[currentIndex] = EXEC_TASK_CONNECT_POLL;
			}
			else if (pollStatus == CLIENT_CONNECTION_BAD)
			{
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
			}

			/* now check if we have been trying to connect for too long */
			if (pollStatus == CLIENT_CONNECTION_BUSY_READ ||
				pollStatus == CLIENT_CONNECTION_BUSY_WRITE)
			{
				if (TimestampDifferenceExceeds(taskExecution->connectStartTime,
											   GetCurrentTimestamp(),
											   NodeConnectionTimeout))
				{
					ereport(WARNING, (errmsg("could not establish asynchronous "
											 "connection after %u ms",
											 NodeConnectionTimeout)));

					taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
				}
			}

			break;
		}

		case EXEC_TASK_FAILED:
		{
			bool raiseError = false;
			bool isCritical = false;

			/*
			 * On task failure, we close the connection. We also reset our execution
			 * status assuming that we might fail on all other worker nodes and come
			 * back to this failed node. In that case, we will retry compute task(s)
			 * on this node again.
			 */
			int32 connectionId = connectionIdArray[currentIndex];
			MultiConnection *connection = MultiClientGetConnection(connectionId);

			/* next time we try this worker node, start from the beginning */
			taskStatusArray[currentIndex] = EXEC_TASK_CONNECT_START;

			/* try next worker node */
			AdjustStateForFailure(taskExecution);

			/*
			 * Add a delay in MultiClientWait, to avoid potentially excerbating problems
			 * by looping quickly
			 */
			*executionStatus = TASK_STATUS_ERROR;

			if (connection == NULL)
			{
				/*
				 * The task failed before we even managed to connect. This happens when
				 * the metadata is out of sync due to a rebalance. It may be that only
				 * one placement was moved, in that case the other one might still work.
				 */
				break;
			}

			isCritical = connection->remoteTransaction.transactionCritical;
			if (isCritical)
			{
				/* cannot recover when error occurs in a critical transaction */
				taskExecution->criticalErrorOccurred = true;
			}

			/*
			 * Mark the connection as failed in case it was already used to perform
			 * writes. We do not error out here, because the abort handler currently
			 * cannot handle connections with COPY (SELECT ...) TO STDOUT commands
			 * in progress.
			 */
			raiseError = false;
			MarkRemoteTransactionFailed(connection, raiseError);

			MultiClientDisconnect(connectionId);
			connectionIdArray[currentIndex] = INVALID_CONNECTION_ID;

			connectAction = CONNECT_ACTION_CLOSED;

			break;
		}

		case EXEC_BEGIN_START:
		{
			int32 connectionId = connectionIdArray[currentIndex];
			MultiConnection *connection = MultiClientGetConnection(connectionId);
			RemoteTransaction *transaction = &connection->remoteTransaction;

			/*
			 * If BEGIN was not yet sent on this connection, send it now.
			 * Otherwise, continue with the task.
			 */
			if (transaction->transactionState == REMOTE_TRANS_INVALID)
			{
				StartRemoteTransactionBegin(connection);
				taskStatusArray[currentIndex] = EXEC_BEGIN_RUNNING;
				break;
			}
			else
			{
				taskStatusArray[currentIndex] = EXEC_COMPUTE_TASK_START;
				break;
			}
		}

		case EXEC_BEGIN_RUNNING:
		{
			int32 connectionId = connectionIdArray[currentIndex];
			MultiConnection *connection = MultiClientGetConnection(connectionId);
			RemoteTransaction *transaction = &connection->remoteTransaction;

			/* check if query results are in progress or unavailable */
			ResultStatus resultStatus = MultiClientResultStatus(connectionId);
			if (resultStatus == CLIENT_RESULT_BUSY)
			{
				*executionStatus = TASK_STATUS_SOCKET_READ;
				taskStatusArray[currentIndex] = EXEC_BEGIN_RUNNING;
				break;
			}
			else if (resultStatus == CLIENT_RESULT_UNAVAILABLE)
			{
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
				break;
			}

			/* read the results from BEGIN and update the transaction state */
			FinishRemoteTransactionBegin(connection);

			if (transaction->transactionFailed)
			{
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
				break;
			}
			else
			{
				taskStatusArray[currentIndex] = EXEC_COMPUTE_TASK_START;
				break;
			}
		}

		case EXEC_COMPUTE_TASK_START:
		{
			int32 connectionId = connectionIdArray[currentIndex];
			bool querySent = false;

			/* construct new query to copy query results to stdout */
			char *queryString = task->queryString;
			StringInfo computeTaskQuery = makeStringInfo();
			if (BinaryMasterCopyFormat)
			{
				appendStringInfo(computeTaskQuery, COPY_QUERY_TO_STDOUT_BINARY,
								 queryString);
			}
			else
			{
				appendStringInfo(computeTaskQuery, COPY_QUERY_TO_STDOUT_TEXT,
								 queryString);
			}

			querySent = MultiClientSendQuery(connectionId, computeTaskQuery->data);
			if (querySent)
			{
				taskStatusArray[currentIndex] = EXEC_COMPUTE_TASK_RUNNING;
			}
			else
			{
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
			}

			break;
		}

		case EXEC_COMPUTE_TASK_RUNNING:
		{
			int32 connectionId = connectionIdArray[currentIndex];
			ResultStatus resultStatus = MultiClientResultStatus(connectionId);
			QueryStatus queryStatus = CLIENT_INVALID_QUERY;

			/* check if query results are in progress or unavailable */
			if (resultStatus == CLIENT_RESULT_BUSY)
			{
				taskStatusArray[currentIndex] = EXEC_COMPUTE_TASK_RUNNING;
				*executionStatus = TASK_STATUS_SOCKET_READ;
				break;
			}
			else if (resultStatus == CLIENT_RESULT_UNAVAILABLE)
			{
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
				break;
			}

			Assert(resultStatus == CLIENT_RESULT_READY);

			/* check if our request to copy query results has been acknowledged */
			queryStatus = MultiClientQueryStatus(connectionId);
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
					fileDescriptorArray[currentIndex] = fileDescriptor;
					taskStatusArray[currentIndex] = EXEC_COMPUTE_TASK_COPYING;
				}
				else
				{
					ereport(WARNING, (errcode_for_file_access(),
									  errmsg("could not open file \"%s\": %m",
											 filename)));

					taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
				}
			}
			else if (queryStatus == CLIENT_QUERY_FAILED)
			{
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
			}
			else
			{
				ereport(FATAL, (errmsg("invalid query status: %d", queryStatus)));
			}

			break;
		}

		case EXEC_COMPUTE_TASK_COPYING:
		{
			int32 connectionId = connectionIdArray[currentIndex];
			int32 fileDesc = fileDescriptorArray[currentIndex];
			int closed = -1;
			uint64 bytesReceived = 0;

			/* copy data from worker node, and write to local file */
			CopyStatus copyStatus = MultiClientCopyData(connectionId, fileDesc,
														&bytesReceived);

			if (SubPlanLevel > 0)
			{
				executionStats->totalIntermediateResultSize += bytesReceived;
			}

			/* if worker node will continue to send more data, keep reading */
			if (copyStatus == CLIENT_COPY_MORE)
			{
				taskStatusArray[currentIndex] = EXEC_COMPUTE_TASK_COPYING;
				*executionStatus = TASK_STATUS_SOCKET_READ;
			}
			else if (copyStatus == CLIENT_COPY_DONE)
			{
				closed = close(fileDesc);
				fileDescriptorArray[currentIndex] = -1;

				if (closed >= 0)
				{
					taskStatusArray[currentIndex] = EXEC_TASK_DONE;

					/* we are done executing; we no longer need the connection */
					MultiClientReleaseConnection(connectionId);
					connectionIdArray[currentIndex] = INVALID_CONNECTION_ID;
					connectAction = CONNECT_ACTION_CLOSED;
				}
				else
				{
					ereport(WARNING, (errcode_for_file_access(),
									  errmsg("could not close copied file: %m")));

					taskStatusArray[currentIndex] = EXEC_TASK_FAILED;
				}
			}
			else if (copyStatus == CLIENT_COPY_FAILED)
			{
				taskStatusArray[currentIndex] = EXEC_TASK_FAILED;

				closed = close(fileDesc);
				fileDescriptorArray[currentIndex] = -1;

				if (closed < 0)
				{
					ereport(WARNING, (errcode_for_file_access(),
									  errmsg("could not close copy file: %m")));
				}
			}

			break;
		}

		case EXEC_TASK_DONE:
		{
			/* we are done with this task's execution */
			break;
		}

		default:
		{
			/* we fatal here to avoid leaking client-side resources */
			ereport(FATAL, (errmsg("invalid execution status: %d", currentStatus)));
			break;
		}
	}

	return connectAction;
}


/* Determines if the given task is ready to start. */
static bool
TaskExecutionReadyToStart(TaskExecution *taskExecution)
{
	bool readyToStart = false;
	TaskExecStatus *taskStatusArray = taskExecution->taskStatusArray;
	uint32 currentIndex = taskExecution->currentNodeIndex;
	TaskExecStatus taskStatus = taskStatusArray[currentIndex];

	if (taskStatus == EXEC_TASK_CONNECT_START)
	{
		readyToStart = true;
	}

	return readyToStart;
}


/* Determines if the given task successfully completed executing. */
static bool
TaskExecutionCompleted(TaskExecution *taskExecution)
{
	bool completed = false;
	uint32 nodeIndex = 0;

	for (nodeIndex = 0; nodeIndex < taskExecution->nodeCount; nodeIndex++)
	{
		TaskExecStatus taskStatus = taskExecution->taskStatusArray[nodeIndex];
		if (taskStatus == EXEC_TASK_DONE)
		{
			completed = true;
			break;
		}
	}

	return completed;
}


/* Iterates over all open connections, and cancels any active requests. */
static void
CancelTaskExecutionIfActive(TaskExecution *taskExecution)
{
	uint32 nodeIndex = 0;
	for (nodeIndex = 0; nodeIndex < taskExecution->nodeCount; nodeIndex++)
	{
		int32 connectionId = taskExecution->connectionIdArray[nodeIndex];
		if (connectionId != INVALID_CONNECTION_ID)
		{
			TaskExecStatus *taskStatusArray = taskExecution->taskStatusArray;
			TaskExecStatus taskStatus = taskStatusArray[nodeIndex];

			CancelRequestIfActive(taskStatus, connectionId);
		}
	}
}


/* Helper function to cancel an ongoing request, if any. */
static void
CancelRequestIfActive(TaskExecStatus taskStatus, int connectionId)
{
	/*
	 * We use the task status to determine if we have an active request being
	 * processed by the worker node. If we do, we send a cancellation request.
	 */
	if (taskStatus == EXEC_COMPUTE_TASK_RUNNING)
	{
		ResultStatus resultStatus = MultiClientResultStatus(connectionId);
		if (resultStatus == CLIENT_RESULT_BUSY)
		{
			MultiClientCancel(connectionId);
		}
	}
	else if (taskStatus == EXEC_COMPUTE_TASK_COPYING)
	{
		MultiClientCancel(connectionId);
	}
}


/*
 * WorkerHash creates a worker node hash with the given name. The function
 * then inserts one entry for each worker node in the given worker node
 * list.
 */
static HTAB *
WorkerHash(const char *workerHashName, List *workerNodeList)
{
	uint32 workerHashSize = list_length(workerNodeList);
	HTAB *workerHash = WorkerHashCreate(workerHashName, workerHashSize);

	ListCell *workerNodeCell = NULL;
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		WorkerHashEnter(workerHash, nodeName, nodePort);
	}

	return workerHash;
}


/*
 * WorkerHashCreate allocates memory for a worker node hash, initializes an
 * empty hash, and returns this hash.
 */
static HTAB *
WorkerHashCreate(const char *workerHashName, uint32 workerHashSize)
{
	HASHCTL info;
	int hashFlags = 0;
	HTAB *workerHash = NULL;

	memset(&info, 0, sizeof(info));
	info.keysize = WORKER_LENGTH + sizeof(uint32);
	info.entrysize = sizeof(WorkerNodeState);
	info.hash = tag_hash;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	workerHash = hash_create(workerHashName, workerHashSize, &info, hashFlags);
	if (workerHash == NULL)
	{
		ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("could not initialize worker node hash")));
	}

	return workerHash;
}


/*
 * WorkerHashEnter creates a new worker node entry in the given worker node
 * hash, and checks that the worker node entry has been properly created.
 */
static WorkerNodeState *
WorkerHashEnter(HTAB *workerHash, char *nodeName, uint32 nodePort)
{
	bool handleFound = false;
	WorkerNodeState *workerNodeState = NULL;
	WorkerNodeState workerNodeKey;

	memset(&workerNodeKey, 0, sizeof(WorkerNodeState));
	strlcpy(workerNodeKey.workerName, nodeName, WORKER_LENGTH);
	workerNodeKey.workerPort = nodePort;

	workerNodeState = (WorkerNodeState *) hash_search(workerHash, (void *) &workerNodeKey,
													  HASH_ENTER, &handleFound);
	if (handleFound)
	{
		ereport(WARNING, (errmsg("multiple worker node state entries for node: \"%s:%u\"",
								 nodeName, nodePort)));
	}

	memcpy(workerNodeState, &workerNodeKey, sizeof(WorkerNodeState));
	workerNodeState->openConnectionCount = 0;

	return workerNodeState;
}


/*
 * WorkerHashLookup looks for the worker node state that corresponds to the given
 * node name and port number, and returns the found worker node state if any.
 */
static WorkerNodeState *
WorkerHashLookup(HTAB *workerHash, const char *nodeName, uint32 nodePort)
{
	bool handleFound = false;
	WorkerNodeState *workerNodeState = NULL;
	WorkerNodeState workerNodeKey;

	memset(&workerNodeKey, 0, sizeof(WorkerNodeState));
	strlcpy(workerNodeKey.workerName, nodeName, WORKER_LENGTH);
	workerNodeKey.workerPort = nodePort;

	workerNodeState = (WorkerNodeState *) hash_search(workerHash, (void *) &workerNodeKey,
													  HASH_FIND, &handleFound);
	if (workerNodeState == NULL)
	{
		ereport(ERROR, (errmsg("could not find worker node state for node \"%s:%u\"",
							   nodeName, nodePort)));
	}

	return workerNodeState;
}


/*
 * LookupWorkerForTask looks for the worker node state of the current worker
 * node of a task execution.
 */
static WorkerNodeState *
LookupWorkerForTask(HTAB *workerHash, Task *task, TaskExecution *taskExecution)
{
	uint32 currentIndex = taskExecution->currentNodeIndex;
	List *taskPlacementList = task->taskPlacementList;
	ShardPlacement *taskPlacement = list_nth(taskPlacementList, currentIndex);
	char *nodeName = taskPlacement->nodeName;
	uint32 nodePort = taskPlacement->nodePort;

	WorkerNodeState *workerNodeState = WorkerHashLookup(workerHash, nodeName, nodePort);

	return workerNodeState;
}


/*
 * WorkerConnectionsExhausted determines if the current query has exhausted the
 * maximum number of open connections that can be made to a worker.
 *
 * Note that the function takes sequential exection of the queries into account
 * as well. In other words, in the sequential mode, the connections are considered
 * to be exahusted when there is already a connection opened to the given worker.
 */
static bool
WorkerConnectionsExhausted(WorkerNodeState *workerNodeState)
{
	bool reachedLimit = false;

	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION &&
		workerNodeState->openConnectionCount >= 1)
	{
		reachedLimit = true;
	}
	else if (MultiShardConnectionType == PARALLEL_CONNECTION &&
			 workerNodeState->openConnectionCount >= MaxConnections)
	{
		/*
		 * A worker cannot accept more than max_connections connections. If we have a
		 * small number of workers with many shards, then a single query could exhaust
		 * max_connections unless we throttle here. We use the value of max_connections
		 * on the master as a proxy for the worker configuration to avoid introducing a
		 * new configuration value.
		 */
		reachedLimit = true;
	}

	return reachedLimit;
}


/*
 * MasterConnectionsExhausted determines if the current query has exhausted
 * the maximum number of connections the master process can make.
 */
static bool
MasterConnectionsExhausted(HTAB *workerHash)
{
	bool reachedLimit = false;

	uint32 maxConnectionCount = MaxMasterConnectionCount();
	uint32 totalConnectionCount = TotalOpenConnectionCount(workerHash);
	if (totalConnectionCount >= maxConnectionCount)
	{
		reachedLimit = true;
	}

	return reachedLimit;
}


/*
 * TotalOpenConnectionCount counts the total number of open connections across all the
 * workers.
 */
static uint32
TotalOpenConnectionCount(HTAB *workerHash)
{
	uint32 connectionCount = 0;
	WorkerNodeState *workerNodeState = NULL;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, workerHash);

	workerNodeState = (WorkerNodeState *) hash_seq_search(&status);
	while (workerNodeState != NULL)
	{
		connectionCount += workerNodeState->openConnectionCount;
		workerNodeState = (WorkerNodeState *) hash_seq_search(&status);
	}

	return connectionCount;
}


/*
 * UpdateConnectionCounter updates the connection counter for a given worker
 * node based on the specified connect action.
 */
static void
UpdateConnectionCounter(WorkerNodeState *workerNode, ConnectAction connectAction)
{
	if (connectAction == CONNECT_ACTION_OPENED)
	{
		workerNode->openConnectionCount++;
	}
	else if (connectAction == CONNECT_ACTION_CLOSED)
	{
		workerNode->openConnectionCount--;
	}
}


/*
 * RealTimeExecScan is a callback function which returns next tuple from a real-time
 * execution. In the first call, it executes distributed real-time plan and loads
 * results from temporary files into custom scan's tuple store. Then, it returns
 * tuples one by one from this tuple store.
 */
TupleTableSlot *
RealTimeExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Job *workerJob = distributedPlan->workerJob;

		ErrorIfLocalExecutionHappened();
		DisableLocalExecution();

		/* we are taking locks on partitions of partitioned tables */
		LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);

		PrepareMasterJobDirectory(workerJob);

		ExecuteSubPlans(distributedPlan);
		MultiRealTimeExecute(workerJob);

		LoadTuplesIntoTupleStore(scanState, workerJob);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * PrepareMasterJobDirectory creates a directory on the master node to keep job
 * execution results. We also register this directory for automatic cleanup on
 * portal delete.
 */
void
PrepareMasterJobDirectory(Job *workerJob)
{
	StringInfo jobDirectoryName = MasterJobDirectoryName(workerJob->jobId);
	CitusCreateDirectory(jobDirectoryName);

	ResourceOwnerEnlargeJobDirectories(CurrentResourceOwner);
	ResourceOwnerRememberJobDirectory(CurrentResourceOwner, workerJob->jobId);
}
