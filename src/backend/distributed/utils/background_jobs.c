/*-------------------------------------------------------------------------
 *
 * background_jobs.c
 *	  Background jobs run as a background worker, spawned from the
 *	  maintenance daemon. Jobs have tasks, tasks can depend on other
 *	  tasks before execution.
 *
 * This file contains the code for two separate background workers to
 * achieve the goal of running background tasks asynchronously from the
 * main database workload. This first background worker is the
 * Background Tasks Queue Monitor. This background worker keeps track of
 * tasks recorded in pg_dist_background_task and ensures execution based
 * on a statemachine. When a task needs to be executed it starts a
 * Background Task Executor that executes the sql statement defined in the
 * task. The output of the Executor is shared with the Monitor via a
 * shared memory queue.
 *
 * To make sure there is only ever exactly one monitor running per database
 * it takes an exclusive lock on the CITUS_BACKGROUND_TASK_MONITOR
 * operation. This lock is consulted from the maintenance daemon to only
 * spawn a new monitor when the lock is not held.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "safe_mem_lib.h"

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "libpq/pqsignal.h"
#include "parser/analyze.h"
#include "pgstat.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"

#include "distributed/background_jobs.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/shard_cleaner.h"
#include "distributed/resource_lock.h"

/* Table-of-contents constants for our dynamic shared memory segment. */
#define CITUS_BACKGROUND_TASK_MAGIC 0x51028081
#define CITUS_BACKGROUND_TASK_KEY_DATABASE 0
#define CITUS_BACKGROUND_TASK_KEY_USERNAME 1
#define CITUS_BACKGROUND_TASK_KEY_COMMAND 2
#define CITUS_BACKGROUND_TASK_KEY_QUEUE 3
#define CITUS_BACKGROUND_TASK_KEY_TASK_ID 4
#define CITUS_BACKGROUND_TASK_NKEYS 5

static BackgroundWorkerHandle * StartCitusBackgroundTaskExecutor(char *database,
																 char *user,
																 char *command,
																 int64 taskId,
																 dsm_segment **pSegment);
static void ExecuteSqlString(const char *sql);
static shm_mq_result ConsumeTaskWorkerOutput(shm_mq_handle *responseq, StringInfo message,
											 bool *hadError);
static void UpdateDependingTasks(BackgroundTask *task);
static int64 CalculateBackoffDelay(int retryCount);
static HTAB * CreateBackgroundExecutorHash(MemoryContext context);
static BackgroundExecutorHashEntry * GetNextTaskExecutorFromHashTable(
	HTAB *runningTaskBackgroundExecutors, int64 lastMonitoredTaskId);
static void StartMonitorTransaction(MemoryContext ctx);
static void CommitMonitorTransaction(MemoryContext ctx);
static shm_mq_result ReadFromExecutorQueue(dsm_segment *seg, StringInfo message,
										   bool *hadError);

PG_FUNCTION_INFO_V1(citus_job_cancel);
PG_FUNCTION_INFO_V1(citus_job_wait);


/*
 * pg_catalog.citus_job_cancel(jobid bigint) void
 *   cancels a scheduled/running job
 *
 * When cancelling a job there are two phases.
 *  1. scan all associated tasks and transition all tasks that are not already in their
 *     terminal state to cancelled. Except if the task is currently running.
 *  2. for all running tasks we send a cancelation signal to the backend running the
 *     query. The background executor/monitor will transition this task to cancelled.
 *
 * We apply the same policy checks as pg_cancel_backend to check if a user can cancel a
 * job.
 */
Datum
citus_job_cancel(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 jobid = PG_GETARG_INT64(0);

	/* Cancel all tasks that were scheduled before */
	List *pids = CancelTasksForJob(jobid);

	/* send cancellation to any running backends */
	int pid = 0;
	foreach_int(pid, pids)
	{
		Datum pidDatum = Int32GetDatum(pid);
		Datum signalSuccessDatum = DirectFunctionCall1(pg_cancel_backend, pidDatum);
		bool signalSuccess = DatumGetBool(signalSuccessDatum);
		if (!signalSuccess)
		{
			ereport(WARNING, (errmsg("could not send signal to process %d: %m", pid)));
		}
	}

	UpdateBackgroundJob(jobid);

	PG_RETURN_VOID();
}


/*
 * pg_catalog.citus_job_wait(jobid bigint,
 *                            desired_status citus_job_status DEFAULT NULL) boolean
 *   waits till a job reaches a desired status, or can't reach the status anymore because
 *   it reached a (different) terminal state. When no desired_status is given it will
 *   assume any terminal state as its desired status. The function returns if the
 *   desired_state was reached.
 *
 * The current implementation is a polling implementation with an interval of 1 second.
 * Ideally we would have some synchronization between the background tasks queue monitor
 * and any backend calling this function to receive a signal when the job changes state.
 */
Datum
citus_job_wait(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 jobid = PG_GETARG_INT64(0);

	/* parse the optional desired_status argument */
	bool hasDesiredStatus = !PG_ARGISNULL(1);
	BackgroundJobStatus desiredStatus = { 0 };
	if (hasDesiredStatus)
	{
		desiredStatus = BackgroundJobStatusByOid(PG_GETARG_OID(1));
	}

	citus_job_wait_internal(jobid, hasDesiredStatus ? &desiredStatus : NULL);

	PG_RETURN_VOID();
}


/*
 * citus_job_wait_internal imaplements the waiting on a job for reuse in other areas where
 * we want to wait on jobs. eg the background rebalancer.
 *
 * When a desiredStatus is provided it will provide an error when a different state is
 * reached and the state cannot ever reach the desired state anymore.
 */
void
citus_job_wait_internal(int64 jobid, BackgroundJobStatus *desiredStatus)
{
	/*
	 * Since we are wait polling we will actually allocate memory on every poll. To make
	 * sure we don't put unneeded pressure on the memory we create a context that we clear
	 * every iteration.
	 */
	MemoryContext waitContext = AllocSetContextCreate(CurrentMemoryContext,
													  "JobsWaitContext",
													  ALLOCSET_DEFAULT_MINSIZE,
													  ALLOCSET_DEFAULT_INITSIZE,
													  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext oldContext = MemoryContextSwitchTo(waitContext);

	while (true)
	{
		MemoryContextReset(waitContext);

		BackgroundJob *job = GetBackgroundJobByJobId(jobid);
		if (!job)
		{
			ereport(ERROR, (errmsg("no job found for job with jobid: %ld", jobid)));
		}

		if (desiredStatus && job->state == *desiredStatus)
		{
			/* job has reached its desired status, done waiting */
			break;
		}

		if (IsBackgroundJobStatusTerminal(job->state))
		{
			if (desiredStatus)
			{
				/*
				 * We have reached a terminal state, which is not the desired state we
				 * were waiting for, otherwise we would have escaped earlier. Since it is
				 * a terminal state we know that we can never reach the desired state.
				 */

				Oid reachedStatusOid = BackgroundJobStatusOid(job->state);
				Datum reachedStatusNameDatum = DirectFunctionCall1(enum_out,
																   reachedStatusOid);
				char *reachedStatusName = DatumGetCString(reachedStatusNameDatum);

				Oid desiredStatusOid = BackgroundJobStatusOid(*desiredStatus);
				Datum desiredStatusNameDatum = DirectFunctionCall1(enum_out,
																   desiredStatusOid);
				char *desiredStatusName = DatumGetCString(desiredStatusNameDatum);

				ereport(ERROR,
						(errmsg("Job reached terminal state \"%s\" instead of desired "
								"state \"%s\"", reachedStatusName, desiredStatusName)));
			}

			/* job has reached its terminal state, done waiting */
			break;
		}

		/* sleep for a while, before rechecking the job status */
		const long delay_ms = 1000;
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 delay_ms,
						 WAIT_EVENT_PG_SLEEP);

		ResetLatch(MyLatch);
	}

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(waitContext);
}


/*
 * StartCitusBackgroundTaskQueueMonitor spawns a new background worker connected to the
 * current database and owner. This background worker consumes the tasks that are ready
 * for execution.
 */
BackgroundWorkerHandle *
StartCitusBackgroundTaskQueueMonitor(Oid database, Oid extensionOwner)
{
	BackgroundWorker worker = { 0 };
	BackgroundWorkerHandle *handle = NULL;

	/* Configure a worker. */
	memset(&worker, 0, sizeof(worker));
	SafeSnprintf(worker.bgw_name, BGW_MAXLEN,
				 "Citus Background Task Queue Monitor: %u/%u",
				 database, extensionOwner);
	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* don't restart, we manage restarts from maintenance daemon */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy_s(worker.bgw_library_name, sizeof(worker.bgw_library_name), "citus");
	strcpy_s(worker.bgw_function_name, sizeof(worker.bgw_library_name),
			 "CitusBackgroundTaskQueueMonitorMain");
	worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
	memcpy_s(worker.bgw_extra, sizeof(worker.bgw_extra), &extensionOwner,
			 sizeof(Oid));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		return NULL;
	}

	pid_t pid;
	WaitForBackgroundWorkerStartup(handle, &pid);

	return handle;
}


/*
 * context for any log/error messages emitted from the background task queue monitor.
 */
typedef struct CitusBackgroundTaskQueueMonitorErrorCallbackContext
{
	const char *database;
} CitusBackgroundTaskQueueMonitorCallbackContext;


/*
 * CitusBackgroundTaskQueueMonitorErrorCallback is a callback handler that gets called for
 * any ereport to add extra context to the message.
 */
static void
CitusBackgroundTaskQueueMonitorErrorCallback(void *arg)
{
	CitusBackgroundTaskQueueMonitorCallbackContext *context =
		(CitusBackgroundTaskQueueMonitorCallbackContext *) arg;
	errcontext("Citus Background Task Queue Monitor: %s", context->database);
}


/*
 * CreateBackgroundExecutorHash constructs a hash table which maps from task
 * id to a handle of BackgroundExecutor, allocated in given context
 */
static HTAB *
CreateBackgroundExecutorHash(MemoryContext context)
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(BackgroundExecutorHashEntry);
	info.hash = tag_hash;
	info.hcxt = context;
	int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB *backgroundExecutorHash = hash_create("Background Executor Hash", 32, &info,
											   hashFlags);

	return backgroundExecutorHash;
}


/*
 * GetNextTaskExecutorFromHashTable returns the next executor from given hash table to be
 * run by task queue monitor. It will try to find the next executor in round-robin fashion
 * so that tasks will not starve to wait for their execution.
 */
static BackgroundExecutorHashEntry *
GetNextTaskExecutorFromHashTable(HTAB *runningTaskBackgroundExecutors, int64
								 lastMonitoredTaskId)
{
	BackgroundExecutorHashEntry *firstReadyBackgroundExecutorHashEntry = NULL;
	BackgroundExecutorHashEntry *nextReadyBackgroundExecutorHashEntry = NULL;

	HASH_SEQ_STATUS status;
	BackgroundExecutorHashEntry *backgroundExecutorHashEntry;
	hash_seq_init(&status, runningTaskBackgroundExecutors);

	while ((backgroundExecutorHashEntry = hash_seq_search(&status)) != NULL)
	{
		int64 taskid = backgroundExecutorHashEntry->taskid;

		if (firstReadyBackgroundExecutorHashEntry == NULL)
		{
			firstReadyBackgroundExecutorHashEntry = backgroundExecutorHashEntry;
		}

		if (taskid > lastMonitoredTaskId)
		{
			if (nextReadyBackgroundExecutorHashEntry == NULL)
			{
				nextReadyBackgroundExecutorHashEntry = backgroundExecutorHashEntry;
			}
			else if (taskid < nextReadyBackgroundExecutorHashEntry->taskid)
			{
				nextReadyBackgroundExecutorHashEntry = backgroundExecutorHashEntry;
			}
		}
	}

	if (nextReadyBackgroundExecutorHashEntry)
	{
		/* we return next background executor */
		return nextReadyBackgroundExecutorHashEntry;
	}
	else if (firstReadyBackgroundExecutorHashEntry)
	{
		/* we return to the beginning of the background executors */
		return firstReadyBackgroundExecutorHashEntry;
	}

	/* No background executor found */
	return NULL;
}


/*
 * StartMonitorTransaction switches into given ctx to make allocations persist in it
 */
static void
StartMonitorTransaction(MemoryContext ctx)
{
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	MemoryContextSwitchTo(ctx);
}


/*
 * CommitMonitorTransaction changes given ctx with TopTransactionContext to not deallocate
 * memory in ctx on transaction commits
 */
static void
CommitMonitorTransaction(MemoryContext ctx)
{
	MemoryContextSwitchTo(TopTransactionContext);
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(ctx);
}


/*
 * CitusBackgroundTaskQueueMonitorMain is the main entry point for the background worker
 * running the background tasks queue monitor.
 *
 * It's mainloop reads a runnable task from pg_dist_background_task and progressing the
 * tasks and jobs state machines associated with the task. When no new task can be found
 * it will exit(0) and lets the maintenance daemon poll for new tasks.
 *
 * The main loop is implemented as asynchronous loop stepping through the task
 * and update its state before going to the next. Loop assigns runnable tasks to new task
 * executors as much as possible. If the max task executor limit is hit, the tasks will be
 * waiting in runnable status until currently running tasks finish. Each parallel worker
 * executes one task at a time without blocking each other by using nonblocking api.
 */
void
CitusBackgroundTaskQueueMonitorMain(Datum arg)
{
	Oid databaseOid = DatumGetObjectId(arg);

	/* extension owner is passed via bgw_extra */
	Oid extensionOwner = InvalidOid;
	memcpy_s(&extensionOwner, sizeof(extensionOwner),
			 MyBgworkerEntry->bgw_extra, sizeof(Oid));

	BackgroundWorkerUnblockSignals();

	/* connect to database, after that we can actually access catalogs */
	BackgroundWorkerInitializeConnectionByOid(databaseOid, extensionOwner, 0);

	/*
	 * save old context until monitor loop exits, we use backgroundTaskContext for
	 * all allocations.
	 */
	MemoryContext oldContext = CurrentMemoryContext;
	MemoryContext backgroundTaskContext = AllocSetContextCreate(TopMemoryContext,
																"BackgroundTaskContext",
																ALLOCSET_DEFAULT_SIZES);

	StartMonitorTransaction(backgroundTaskContext);

	const char *databasename = get_database_name(MyDatabaseId);
	databasename = pstrdup(databasename);

	/* setup error context to indicate the errors came from a running background task */
	ErrorContextCallback errorCallback = { 0 };
	struct CitusBackgroundTaskQueueMonitorErrorCallbackContext context = {
		.database = databasename,
	};
	errorCallback.callback = CitusBackgroundTaskQueueMonitorErrorCallback;
	errorCallback.arg = (void *) &context;
	errorCallback.previous = error_context_stack;
	error_context_stack = &errorCallback;

	CommitMonitorTransaction(backgroundTaskContext);

	/*
	 * There should be exactly one background task monitor running, running multiple would
	 * cause conflicts on processing the tasks in the catalog table as well as violate
	 * parallelism guarantees. To make sure there is at most, exactly one backend running
	 * we take a session lock on the CITUS_BACKGROUND_TASK_MONITOR operation.
	 *
	 * TODO now that we have a lock, we should install a term handler to terminate any
	 * 'child' backend when we are terminated. Otherwise we will still have a situation
	 * where the actual task could be running multiple times.
	 */
	LOCKTAG tag = { 0 };
	SET_LOCKTAG_CITUS_OPERATION(tag, CITUS_BACKGROUND_TASK_MONITOR);
	const bool sessionLock = true;
	const bool dontWait = true;
	LockAcquireResult locked =
		LockAcquire(&tag, AccessExclusiveLock, sessionLock, dontWait);
	if (locked == LOCKACQUIRE_NOT_AVAIL)
	{
		ereport(ERROR, (errmsg("background task queue monitor already running for "
							   "database")));
	}

	/* make worker recognizable in pg_stat_activity */
	pgstat_report_appname("citus background task queue monitor");

	ereport(DEBUG1, (errmsg("started citus background task queue monitor")));

	/*
	 * First we find all jobs that are running, we need to check if they are still running
	 * if not reset their state back to scheduled.
	 */
	StartMonitorTransaction(backgroundTaskContext);
	ResetRunningBackgroundTasks();
	CommitMonitorTransaction(backgroundTaskContext);

	TimestampTz backgroundWorkerFailedStartTime = 0;

	/*
	 * create a map to concurrent store background workers created for tasks
	 */
	HTAB *backgroundExecutorHandles = CreateBackgroundExecutorHash(backgroundTaskContext);

	/*
	 * Useful to keep track last monitored task id to ensure round-robin task execution
	 */
	int64 lastMonitoredTaskId = -1;

	/*
	 * current total # of parallel task executors
	 */
	int64 currentExecutorCount = 0;

	/*
	 * current running or runnable task and its related params used in monitor FSM
	 */
	BackgroundTask *task = NULL;
	BackgroundWorkerHandle *handle = NULL;
	BackgroundExecutorHashEntry *handleEntry = NULL;
	dsm_segment *seg = NULL;
	StringInfo message = NULL;
	shm_mq_result mq_res = SHM_MQ_WOULD_BLOCK;
	bool hadError = false;
	char *databaseName = NULL;
	char *userName = NULL;

	/*
	 * current and previous execution state for task monitor FSM
	 */
	BackgroundMonitorExecutionStates monitorExecutionState = MonitorStart;
	BackgroundMonitorExecutionStates prevMonitorExecutionState = MonitorStart;

	/* loop exits if there is no runnable or running tasks left (no ready task) */
	while (monitorExecutionState != NoReadyTaskFound)
	{
		switch (monitorExecutionState)
		{
			case MonitorStart:
			{
				/* initial state */

				InvalidateMetadataSystemCache();

				monitorExecutionState = FetchNewTask;
				prevMonitorExecutionState = MonitorStart;
				break;
			}

			case FetchNewTask:
			{
				/* try to fetch a runnable task */

				StartMonitorTransaction(backgroundTaskContext);
				task = GetRunnableBackgroundTask();

				if (!task)
				{
					monitorExecutionState = TryToExecuteReadyTask;
				}
				else
				{
					/* we load the database name and username here as we are still in a transaction */
					databaseName = get_database_name(MyDatabaseId);
					userName = GetUserNameFromId(task->owner, false);

					monitorExecutionState = AssignNewTask;
				}

				prevMonitorExecutionState = FetchNewTask;
				CommitMonitorTransaction(backgroundTaskContext);
				break;
			}

			case TryToExecuteReadyTask:
			{
				/* we try to find an existing task executor in our hash table */

				handleEntry = GetNextTaskExecutorFromHashTable(backgroundExecutorHandles,
															   lastMonitoredTaskId);

				if (handleEntry)
				{
					StartMonitorTransaction(backgroundTaskContext);
					task = GetBackgroundTaskByTaskId(handleEntry->taskid);
					CommitMonitorTransaction(backgroundTaskContext);

					handle = handleEntry->handle;
					seg = handleEntry->seg;
					message = handleEntry->message;
					lastMonitoredTaskId = handleEntry->taskid;

					monitorExecutionState = ExecutorReady;
				}
				else
				{
					monitorExecutionState = NoReadyTaskFound;
				}

				prevMonitorExecutionState = TryToExecuteReadyTask;
				break;
			}

			case AssignNewTask:
			{
				/* we try to allocate a new task executor for a runnable task */

				if (currentExecutorCount == MaxBackgroundTaskExecutors)
				{
					/*
					 * we hit to maximum allowable task executor count. Warn once and restart the loop
					 * after a short sleep.
					 */
					ereport(WARNING, (errmsg("unable to start background worker for "
											 "background task execution"),
									  errdetail(
										  "Already reached the maximum number of task "
										  "executors: %ld", currentExecutorCount)));

					const long delay_ms = 1000;
					(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT |
									 WL_EXIT_ON_PM_DEATH,
									 delay_ms, WAIT_EVENT_PG_SLEEP);
					ResetLatch(MyLatch);
					CHECK_FOR_INTERRUPTS();

					monitorExecutionState = TryToExecuteReadyTask;
				}
				else
				{
					handle = StartCitusBackgroundTaskExecutor(databaseName, userName,
															  task->command,
															  task->taskid, &seg);

					if (handle == NULL)
					{
						/*
						 * we are unable to start a background worker for the task execution.
						 * Probably we are out of background workers. Warn once and restart the loop
						 * after a short sleep.
						 */
						if (backgroundWorkerFailedStartTime == 0)
						{
							ereport(WARNING, (errmsg(
												  "unable to start background worker for "
												  "background task execution")));
							backgroundWorkerFailedStartTime = GetCurrentTimestamp();
						}

						const long delay_ms = 1000;
						(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT |
										 WL_EXIT_ON_PM_DEATH,
										 delay_ms, WAIT_EVENT_PG_SLEEP);
						ResetLatch(MyLatch);
						CHECK_FOR_INTERRUPTS();

						monitorExecutionState = TryToExecuteReadyTask;
					}
					else
					{
						/*
						 * we allocated a task executor. Then set shared variables used in FSM.
						 * Also add new task executor into our map.
						 */
						message = makeStringInfo();

						bool handleEntryFound = false;
						handleEntry = hash_search(backgroundExecutorHandles,
												  &task->taskid, HASH_ENTER,
												  &handleEntryFound);
						Assert(!handleEntryFound);
						handleEntry->handle = handle;
						handleEntry->seg = seg;
						handleEntry->message = message;
						currentExecutorCount++;

						lastMonitoredTaskId = handleEntry->taskid;

						monitorExecutionState = ExecutorReady;
					}
				}

				prevMonitorExecutionState = AssignNewTask;
				break;
			}

			case ExecutorReady:
			{
				/*
				 * the step is just after we setup shared FSM variables. We are here either after
				 * finding an existing task executor from hash map, or after allocating a task
				 * executor for a runnable task.
				 */

				if (backgroundWorkerFailedStartTime > 0)
				{
					/*
					 * We had a delay in starting the background worker for task execution. Report
					 * the actual delay and reset the time. This allows a subsequent task to
					 * report again if it can't start a background worker directly.
					 */
					long secs = 0;
					int microsecs = 0;
					TimestampDifference(backgroundWorkerFailedStartTime,
										GetCurrentTimestamp(),
										&secs, &microsecs);
					ereport(LOG, (errmsg(
									  "able to start a background worker with %ld seconds"
									  "delay", secs)));

					backgroundWorkerFailedStartTime = 0;
				}

				ereport(LOG, (errmsg("found task with jobid/taskid: %ld/%ld",
									 task->jobid, task->taskid)));

				prevMonitorExecutionState = ExecutorReady;
				monitorExecutionState = TaskConcurrentCancelCheck;
				break;
			}

			case TaskConcurrentCancelCheck:
			{
				/*
				 * Here we take exclusive lock on pg_dist_background_task table to prevent a
				 * concurrent modification. We first check if a task is concurrently cancelled
				 * or deleted. Accordingly, we decide to make the task status as cancelled or running.
				 */

				Assert(prevMonitorExecutionState == ExecutorReady);

				StartMonitorTransaction(backgroundTaskContext);

				/*
				 * Reload task while holding a new ExclusiveLock on the table. A separate process
				 * could have cancelled or removed the task by now, they would not see the pid and
				 * status update, so it is our responsibility to stop the backend and _not_ write
				 * the pid and running status.
				 *
				 * The lock will release on transaction commit.
				 */
				LockRelationOid(DistBackgroundTaskRelationId(), ExclusiveLock);

				task = GetBackgroundTaskByTaskId(task->taskid);

				if (!task || task->status == BACKGROUND_TASK_STATUS_CANCELLING)
				{
					monitorExecutionState = TaskCancelling;
				}
				else
				{
					monitorExecutionState = TaskRunning;
				}

				prevMonitorExecutionState = TaskConcurrentCancelCheck;
				break;
			}

			case TaskCancelling:
			{
				/*
				 * being in that step means that a concurrent cancel happened. we should make task status
				 * as cancelled. We also want to reflect cancel message by consuming task executor
				 * queue.
				 */
				prevMonitorExecutionState = TaskCancelling;
				monitorExecutionState = TryConsumeTaskWorker;
				break;
			}

			case TaskRunning:
			{
				/*
				 * Now that we have verified the task has not been cancelled and still exist we
				 * update it to reflect the new state. If task is already in running status,
				 * the operation is idempotent. But for runnable tasks, we make their status
				 * as running.
				 */

				pid_t pid = 0;
				GetBackgroundWorkerPid(handle, &pid);
				task->status = BACKGROUND_TASK_STATUS_RUNNING;
				SET_NULLABLE_FIELD(task, pid, pid);

				/* Update task status to indicate it is running */
				UpdateBackgroundTask(task);
				UpdateBackgroundJob(task->jobid);

				prevMonitorExecutionState = TaskRunning;
				monitorExecutionState = TryConsumeTaskWorker;
				break;
			}

			case TryConsumeTaskWorker:
			{
				/*
				 * we consume task executor response queue.
				 * possible response codes can lead us different steps below.
				 */
				hadError = false;
				mq_res = ReadFromExecutorQueue(seg, message, &hadError);

				if (prevMonitorExecutionState == TaskCancelling)
				{
					monitorExecutionState = TaskCancelled;
				}
				else if (hadError)
				{
					monitorExecutionState = TaskHadError;
				}
				else if (mq_res == SHM_MQ_WOULD_BLOCK)
				{
					monitorExecutionState = TaskWouldBlock;
				}
				else
				{
					Assert(mq_res == SHM_MQ_DETACHED);
					monitorExecutionState = TaskSucceeded;
				}

				prevMonitorExecutionState = TryConsumeTaskWorker;
				break;
			}

			case TaskHadError:
			{
				/*
				 * When we had an error in response queue, we need to decide if we want to retry (keep the
				 * runnable state), or move to error state
				 */

				if (!task->retry_count)
				{
					SET_NULLABLE_FIELD(task, retry_count, 1);
				}
				else
				{
					(*task->retry_count)++;
				}

				/*
				 * based on the retry count we either transition the task to its error
				 * state, or we calculate a new backoff time for future execution.
				 */
				int64 delayMs = CalculateBackoffDelay(*(task->retry_count));
				if (delayMs < 0)
				{
					task->status = BACKGROUND_TASK_STATUS_ERROR;
					UNSET_NULLABLE_FIELD(task, not_before);
				}
				else
				{
					TimestampTz notBefore = TimestampTzPlusMilliseconds(
						GetCurrentTimestamp(), delayMs);
					SET_NULLABLE_FIELD(task, not_before, notBefore);

					task->status = BACKGROUND_TASK_STATUS_RUNNABLE;
				}

				prevMonitorExecutionState = TaskHadError;
				monitorExecutionState = TaskEnded;
				break;
			}

			case TaskCancelled:
			{
				/* we update task status as cancelled */

				task->status = BACKGROUND_TASK_STATUS_CANCELLED;

				prevMonitorExecutionState = TaskCancelled;
				monitorExecutionState = TaskEnded;
				break;
			}

			case TaskWouldBlock:
			{
				/* still running the task, let the loop choose a new task */

				/* lower cpu consumption on EWOULD_BLOCK */
				const long delay_ms = 2;
				(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT |
								 WL_EXIT_ON_PM_DEATH,
								 delay_ms, WAIT_EVENT_PG_SLEEP);
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();

				CommitMonitorTransaction(backgroundTaskContext);

				prevMonitorExecutionState = TaskWouldBlock;
				monitorExecutionState = MonitorStart;
				break;
			}

			case TaskSucceeded:
			{
				/* update task status as done. */
				task->status = BACKGROUND_TASK_STATUS_DONE;

				prevMonitorExecutionState = TaskSucceeded;
				monitorExecutionState = TaskEnded;
				break;
			}

			case TaskEnded:
			{
				/*
				 * here is a common end state for fail, success and cancel cases.
				 * we update task and job fields. We also update depending jobs.
				 * At the end, do cleanup.
				 */

				UNSET_NULLABLE_FIELD(task, pid);
				task->message = (message) ? message->data : NULL;

				UpdateBackgroundTask(task);
				UpdateDependingTasks(task);
				UpdateBackgroundJob(task->jobid);

				hash_search(backgroundExecutorHandles, &task->taskid, HASH_REMOVE, NULL);
				TerminateBackgroundWorker(handle);
				dsm_detach(seg);
				currentExecutorCount--;

				CommitMonitorTransaction(backgroundTaskContext);

				prevMonitorExecutionState = TaskEnded;
				monitorExecutionState = MonitorStart;
				break;
			}

			default:
			{
				break;
			}
		}
	}

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(backgroundTaskContext);
}


/*
 * ReadFromExecutorQueue reads from task executor's response queue into the message.
 * It also sets hadError flag if an error response is encountered in the queue.
 */
static shm_mq_result
ReadFromExecutorQueue(dsm_segment *seg, StringInfo message, bool *hadError)
{
	shm_toc *toc = shm_toc_attach(CITUS_BACKGROUND_TASK_MAGIC,
								  dsm_segment_address(seg));
	shm_mq *mq = shm_toc_lookup(toc, CITUS_BACKGROUND_TASK_KEY_QUEUE, false);
	shm_mq_handle *responseq = shm_mq_attach(mq, seg, NULL);

	/*
	 * Consume background executor's queue and get a response code.
	 */
	shm_mq_result mq_res = ConsumeTaskWorkerOutput(responseq, message, hadError);
	return mq_res;
}


/*
 * CalculateBackoffDelay calculates the time to backoff between retries.
 *
 * Per try we increase the delay as follows:
 *   retry 1: 5 sec
 *   retry 2: 20 sec
 *   retry 3-32 (30 tries in total): 1 min
 *
 * returns -1 when retrying should stop.
 *
 * In the future we would like a callback on the job_type that could
 * distinguish the retry count and delay + potential jitter on a
 * job_type basis. For now we only assume this to be used by the
 * rebalancer and settled on the retry scheme above.
 */
static int64
CalculateBackoffDelay(int retryCount)
{
	if (retryCount == 1)
	{
		return 5 * 1000;
	}
	else if (retryCount == 2)
	{
		return 20 * 1000;
	}
	else if (retryCount <= 32)
	{
		return 60 * 1000;
	}
	return -1;
}


#if PG_VERSION_NUM < PG_VERSION_15
static const char *
error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
		{
			prefix = gettext_noop("DEBUG");
			break;
		}

		case LOG:
		case LOG_SERVER_ONLY:
		{
			prefix = gettext_noop("LOG");
			break;
		}

		case INFO:
		{
			prefix = gettext_noop("INFO");
			break;
		}

		case NOTICE:
		{
			prefix = gettext_noop("NOTICE");
			break;
		}

		case WARNING:
		{
			prefix = gettext_noop("WARNING");
			break;
		}

#if PG_VERSION_NUM >= PG_VERSION_14
		case WARNING_CLIENT_ONLY:
		{
			prefix = gettext_noop("WARNING");
			break;
		}
#endif

		case ERROR:
		{
			prefix = gettext_noop("ERROR");
			break;
		}

		case FATAL:
		{
			prefix = gettext_noop("FATAL");
			break;
		}

		case PANIC:
		{
			prefix = gettext_noop("PANIC");
			break;
		}

		default:
		{
			prefix = "???";
			break;
		}
	}

	return prefix;
}


#endif


/*
 * bgw_generate_returned_message -
 *      generates the message to be inserted into the job_run_details table
 *      first part is comming from error_severity (elog.c)
 */
static void
bgw_generate_returned_message(StringInfoData *display_msg, ErrorData edata)
{
	const char *prefix = error_severity(edata.elevel);
	appendStringInfo(display_msg, "%s: %s", prefix, edata.message);
	if (edata.detail != NULL)
	{
		appendStringInfo(display_msg, "\nDETAIL: %s", edata.detail);
	}

	if (edata.hint != NULL)
	{
		appendStringInfo(display_msg, "\nHINT: %s", edata.hint);
	}

	if (edata.context != NULL)
	{
		appendStringInfo(display_msg, "\nCONTEXT: %s", edata.context);
	}
}


/*
 * UpdateDependingTasks updates all depending tasks, based on the type of terminal state
 * the current task reached.
 */
static void
UpdateDependingTasks(BackgroundTask *task)
{
	switch (task->status)
	{
		case BACKGROUND_TASK_STATUS_DONE:
		{
			UnblockDependingBackgroundTasks(task);
			break;
		}

		case BACKGROUND_TASK_STATUS_ERROR:
		{
			/* when we error this task, we need to unschedule all dependant tasks */
			UnscheduleDependentTasks(task);
			break;
		}

		default:
		{
			/* nothing to do for other states */
			break;
		}
	}
}


/*
 * ConsumeTaskWorkerOutput consumes the output of an executor and mutates the
 * BackgroundTask object to reflect changes like the message and status on the task.
 */
static shm_mq_result
ConsumeTaskWorkerOutput(shm_mq_handle *responseq, StringInfo message, bool *hadError)
{
	shm_mq_result res;

	/*
	 * Message-parsing routines operate on a null-terminated StringInfo,
	 * so we must construct one.
	 */
	StringInfoData msg = { 0 };
	initStringInfo(&msg);

	for (;;)
	{
		resetStringInfo(&msg);

		/*
		 * non-blocking receive to not block other bg workers
		 */
		Size nbytes = 0;
		void *data = NULL;
		const bool noWait = true;
		res = shm_mq_receive(responseq, &nbytes, &data, noWait);

		if (res != SHM_MQ_SUCCESS)
		{
			break;
		}

		appendBinaryStringInfo(&msg, data, nbytes);

		/*
		 * msgtype seems to be documented on
		 * https://www.postgresql.org/docs/current/protocol-message-formats.html
		 *
		 * Here we mostly handle the same message types as supported in pg_cron as the
		 * executor is highly influenced by the implementation there.
		 */
		char msgtype = pq_getmsgbyte(&msg);
		switch (msgtype)
		{
			case 'E': /* ErrorResponse */
			{
				if (hadError)
				{
					*hadError = true;
				}
			}

			/* FALLTHROUGH */

			case 'N': /* NoticeResponse */
			{
				ErrorData edata = { 0 };
				StringInfoData display_msg = { 0 };

				pq_parse_errornotice(&msg, &edata);
				initStringInfo(&display_msg);
				bgw_generate_returned_message(&display_msg, edata);

				/* we keep only the last message */
				resetStringInfo(message);
				appendStringInfoString(message, display_msg.data);
				appendStringInfoChar(message, '\n');

				pfree(display_msg.data);

				break;
			}

			case 'C': /* CommandComplete */
			{
				const char *tag = pq_getmsgstring(&msg);

				char *nonconst_tag = pstrdup(tag);

				/* append the nonconst_tag to the task's message */
				appendStringInfoString(message, nonconst_tag);
				appendStringInfoChar(message, '\n');

				pfree(nonconst_tag);

				break;
			}

			case 'A':
			case 'D':
			case 'G':
			case 'H':
			case 'T':
			case 'W':
			case 'Z':
			{
				break;
			}

			default:
			{
				elog(WARNING, "unknown message type: %c (%zu bytes)",
					 msg.data[0], nbytes);
				break;
			}
		}
	}

	if (res == SHM_MQ_DETACHED)
	{
		pfree(msg.data);
	}
	return res;
}


/*
 * StoreArgumentsInDSM creates a dynamic shared memory segment to pass the query and its
 * environment to the executor.
 */
static dsm_segment *
StoreArgumentsInDSM(char *database, char *username, char *command, int64 taskId)
{
	/*
	 * Create the shared memory that we will pass to the background
	 * worker process.  We use DSM_CREATE_NULL_IF_MAXSEGMENTS so that we
	 * do not ERROR here.  This way, we can mark the job as failed and
	 * keep the launcher process running normally.
	 */
	shm_toc_estimator e = { 0 };
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, strlen(database) + 1);
	shm_toc_estimate_chunk(&e, strlen(username) + 1);
	shm_toc_estimate_chunk(&e, strlen(command) + 1);
#define QUEUE_SIZE ((Size) 65536)
	shm_toc_estimate_chunk(&e, QUEUE_SIZE);
	shm_toc_estimate_chunk(&e, sizeof(int64));
	shm_toc_estimate_keys(&e, CITUS_BACKGROUND_TASK_NKEYS);
	Size segsize = shm_toc_estimate(&e);

	dsm_segment *seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);
	if (seg == NULL)
	{
		ereport(ERROR,
				(errmsg("max number of DSM segments may has been reached")));

		return NULL;
	}

	shm_toc *toc = shm_toc_create(CITUS_BACKGROUND_TASK_MAGIC, dsm_segment_address(seg),
								  segsize);

	Size size = strlen(database) + 1;
	char *databaseTarget = shm_toc_allocate(toc, size);
	strcpy_s(databaseTarget, size, database);
	shm_toc_insert(toc, CITUS_BACKGROUND_TASK_KEY_DATABASE, databaseTarget);

	size = strlen(username) + 1;
	char *usernameTarget = shm_toc_allocate(toc, size);
	strcpy_s(usernameTarget, size, username);
	shm_toc_insert(toc, CITUS_BACKGROUND_TASK_KEY_USERNAME, usernameTarget);

	size = strlen(command) + 1;
	char *commandTarget = shm_toc_allocate(toc, size);
	strcpy_s(commandTarget, size, command);
	shm_toc_insert(toc, CITUS_BACKGROUND_TASK_KEY_COMMAND, commandTarget);

	shm_mq *mq = shm_mq_create(shm_toc_allocate(toc, QUEUE_SIZE), QUEUE_SIZE);
	shm_toc_insert(toc, CITUS_BACKGROUND_TASK_KEY_QUEUE, mq);
	shm_mq_set_receiver(mq, MyProc);

	int64 *taskIdTarget = shm_toc_allocate(toc, sizeof(int64));
	*taskIdTarget = taskId;
	shm_toc_insert(toc, CITUS_BACKGROUND_TASK_KEY_TASK_ID, taskIdTarget);
	shm_mq_attach(mq, seg, NULL);

	return seg;
}


/*
 * StartCitusBackgroundTaskExecutor start a new background worker for the execution of a
 * background task. Callers interested in the shared memory segment that is created
 * between the background worker and the current backend can pass in a segOut to get a
 * pointer to the dynamic shared memory.
 */
static BackgroundWorkerHandle *
StartCitusBackgroundTaskExecutor(char *database, char *user, char *command, int64 taskId,
								 dsm_segment **pSegment)
{
	dsm_segment *seg = StoreArgumentsInDSM(database, user, command, taskId);

	/* Configure a worker. */
	BackgroundWorker worker = { 0 };
	memset(&worker, 0, sizeof(worker));
	SafeSnprintf(worker.bgw_name, BGW_MAXLEN,
				 "Citus Background Task Queue Executor: %s/%s",
				 database, user);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* don't restart, we manage restarts from maintenance daemon */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy_s(worker.bgw_library_name, sizeof(worker.bgw_library_name), "citus");
	strcpy_s(worker.bgw_function_name, sizeof(worker.bgw_library_name),
			 "CitusBackgroundTaskExecutor");
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid = MyProcPid;

	BackgroundWorkerHandle *handle = NULL;
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		dsm_detach(seg);
		return NULL;
	}

	pid_t pid = { 0 };
	WaitForBackgroundWorkerStartup(handle, &pid);

	if (pSegment)
	{
		*pSegment = seg;
	}

	return handle;
}


/*
 * context for any log/error messages emitted from the background task executor.
 */
typedef struct CitusBackgroundJobExecutorErrorCallbackContext
{
	const char *database;
	const char *username;
} CitusBackgroundJobExecutorErrorCallbackContext;


/*
 * CitusBackgroundJobExecutorErrorCallback is a callback handler that gets called for any
 * ereport to add extra context to the message.
 */
static void
CitusBackgroundJobExecutorErrorCallback(void *arg)
{
	CitusBackgroundJobExecutorErrorCallbackContext *context =
		(CitusBackgroundJobExecutorErrorCallbackContext *) arg;
	errcontext("Citus Background Task Queue Executor: %s/%s", context->database,
			   context->username);
}


/*
 * CitusBackgroundTaskExecutor is the main function of the background tasks queue
 * executor. This backend attaches to a shared memory segment as identified by the
 * main_arg of the background worker.
 *
 * This is mostly based on the background worker logic in pg_cron
 */
void
CitusBackgroundTaskExecutor(Datum main_arg)
{
	/*
	 * TODO figure out if we need this signal handler that is in pgcron
	 * pqsignal(SIGTERM, pg_cron_background_worker_sigterm);
	 */
	BackgroundWorkerUnblockSignals();

	/* Set up a memory context and resource owner. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "citus background job");
	CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
												 "citus background job execution",
												 ALLOCSET_DEFAULT_SIZES);

	/* Set up a dynamic shared memory segment. */
	dsm_segment *seg = dsm_attach(DatumGetInt32(main_arg));
	if (seg == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to map dynamic shared memory segment")));
	}

	shm_toc *toc = shm_toc_attach(CITUS_BACKGROUND_TASK_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));
	}

	char *database = shm_toc_lookup(toc, CITUS_BACKGROUND_TASK_KEY_DATABASE, false);
	char *username = shm_toc_lookup(toc, CITUS_BACKGROUND_TASK_KEY_USERNAME, false);
	char *command = shm_toc_lookup(toc, CITUS_BACKGROUND_TASK_KEY_COMMAND, false);
	int64 *taskId = shm_toc_lookup(toc, CITUS_BACKGROUND_TASK_KEY_TASK_ID, false);
	shm_mq *mq = shm_toc_lookup(toc, CITUS_BACKGROUND_TASK_KEY_QUEUE, false);

	shm_mq_set_sender(mq, MyProc);
	shm_mq_handle *responseq = shm_mq_attach(mq, seg, NULL);
	pq_redirect_to_shm_mq(seg, responseq);

	/* setup error context to indicate the errors came from a running background task */
	ErrorContextCallback errorCallback = { 0 };
	CitusBackgroundJobExecutorErrorCallbackContext context = {
		.database = database,
		.username = username,
	};
	errorCallback.callback = CitusBackgroundJobExecutorErrorCallback;
	errorCallback.arg = (void *) &context;
	errorCallback.previous = error_context_stack;
	error_context_stack = &errorCallback;

	BackgroundWorkerInitializeConnection(database, username, 0);

	/* make sure we are the only backend running for this task */
	LOCKTAG locktag = { 0 };
	SET_LOCKTAG_BACKGROUND_TASK(locktag, *taskId);
	const bool sessionLock = true;
	const bool dontWait = true;
	LockAcquireResult locked =
		LockAcquire(&locktag, AccessExclusiveLock, sessionLock, dontWait);
	if (locked == LOCKACQUIRE_NOT_AVAIL)
	{
		ereport(ERROR, (errmsg("unable to acquire background task lock for taskId: %ld",
							   *taskId),
						errdetail("this indicates that an other backend is already "
								  "executing this task")));
	}

	/* Prepare to execute the query. */
	SetCurrentStatementStartTimestamp();
	debug_query_string = command;
	char *appname = psprintf("citus background task queue executor (taskId %ld)",
							 *taskId);
	pgstat_report_appname(appname);
	pgstat_report_activity(STATE_RUNNING, command);
	StartTransactionCommand();
	if (StatementTimeout > 0)
	{
		enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout);
	}
	else
	{
		disable_timeout(STATEMENT_TIMEOUT, false);
	}

	/* Execute the query. */
	ExecuteSqlString(command);

	/* Post-execution cleanup. */
	disable_timeout(STATEMENT_TIMEOUT, false);
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, command);
	pgstat_report_stat(true);

	/* Signal that we are done. */
	ReadyForQuery(DestRemote);

	dsm_detach(seg);
	proc_exit(0);
}


/*
 * Execute given SQL string without SPI or a libpq session.
 */
static void
ExecuteSqlString(const char *sql)
{
	/*
	 * Parse the SQL string into a list of raw parse trees.
	 *
	 * Because we allow statements that perform internal transaction control,
	 * we can't do this in TopTransactionContext; the parse trees might get
	 * blown away before we're done executing them.
	 */
	MemoryContext parsecontext = AllocSetContextCreate(CurrentMemoryContext,
													   "query parse/plan",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext oldcontext = MemoryContextSwitchTo(parsecontext);
	List *raw_parsetree_list = pg_parse_query(sql);
	int commands_remaining = list_length(raw_parsetree_list);
	bool isTopLevel = commands_remaining == 1;
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Do parse analysis, rule rewrite, planning, and execution for each raw
	 * parsetree.  We must fully execute each query before beginning parse
	 * analysis on the next one, since there may be interdependencies.
	 */
	RawStmt *parsetree = NULL;
	foreach_ptr(parsetree, raw_parsetree_list)
	{
		/*
		 * We don't allow transaction-control commands like COMMIT and ABORT
		 * here.  The entire SQL statement is executed as a single transaction
		 * which commits if no errors are encountered.
		 */
		if (IsA(parsetree, TransactionStmt))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg(
								"transaction control statements are not allowed in background job")));
		}

		/*
		 * Get the command name for use in status display (it also becomes the
		 * default completion tag, down inside PortalRun).  Set ps_status and
		 * do any special start-of-SQL-command processing needed by the
		 * destination.
		 */
		CommandTag commandTag = CreateCommandTag(parsetree->stmt);
		set_ps_display(GetCommandTagName(commandTag));
		BeginCommand(commandTag, DestNone);

		/* Set up a snapshot if parse analysis/planning will need one. */
		bool snapshot_set = false;
		if (analyze_requires_snapshot(parsetree))
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_set = true;
		}

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * As with parsing, we need to make sure this data outlives the
		 * transaction, because of the possibility that the statement might
		 * perform internal transaction control.
		 */
		oldcontext = MemoryContextSwitchTo(parsecontext);

#if PG_VERSION_NUM >= 150000
		List *querytree_list =
			pg_analyze_and_rewrite_fixedparams(parsetree, sql, NULL, 0, NULL);
#else
		List *querytree_list =
			pg_analyze_and_rewrite(parsetree, sql, NULL, 0, NULL);
#endif

		List *plantree_list = pg_plan_queries(querytree_list, sql, 0, NULL);

		/* Done with the snapshot used for parsing/planning */
		if (snapshot_set)
		{
			PopActiveSnapshot();
		}

		/* If we got a cancel signal in analysis or planning, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Execute the query using the unnamed portal.
		 */
		Portal portal = CreatePortal("", true, true);

		/* Don't display the portal in pg_cursors */
		portal->visible = false;
		PortalDefineQuery(portal, NULL, sql, commandTag, plantree_list, NULL);
		PortalStart(portal, NULL, 0, InvalidSnapshot);
		int16 format[] = { 1 };
		PortalSetResultFormat(portal, lengthof(format), format);        /* binary format */

		commands_remaining--;
		DestReceiver *receiver = CreateDestReceiver(DestNone);

		/*
		 * Only once the portal and destreceiver have been established can
		 * we return to the transaction context.  All that stuff needs to
		 * survive an internal commit inside PortalRun!
		 */
		MemoryContextSwitchTo(oldcontext);

		/* Here's where we actually execute the command. */
		QueryCompletion qc = { 0 };
		(void) PortalRun(portal, FETCH_ALL, isTopLevel, true, receiver, receiver, &qc);

		/* Clean up the receiver. */
		(*receiver->rDestroy)(receiver);

		/*
		 * Send a CommandComplete message even if we suppressed the query
		 * results.  The user backend will report these in the absence of
		 * any true query results.
		 */
		EndCommand(&qc, DestRemote, false);

		/* Clean up the portal. */
		PortalDrop(portal, false);
	}

	/* Be sure to advance the command counter after the last script command */
	CommandCounterIncrement();
}
