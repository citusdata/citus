/*-------------------------------------------------------------------------
 *
 * task_tracker.c
 *
 * The task tracker background process runs on every worker node. The process
 * wakes up at regular intervals, reads information from a shared hash, and
 * checks if any new tasks are assigned to this node. If they are, the process
 * runs task-specific logic, and sends queries to the postmaster for execution.
 * The task tracker then tracks the execution of these queries, and updates the
 * shared hash with task progress information.
 *
 * The task tracker is started by the postmaster when the startup process
 * finishes. The process remains alive until the postmaster commands it to
 * terminate. Normal termination is by SIGTERM, which instructs the task tracker
 * to exit(0). Emergency termination is by SIGQUIT; like any backend, the task
 * tracker will simply abort and exit on SIGQUIT.
 *
 * For details on how the task tracker manages resources during process start-up
 * and shutdown, please see the writeboard on our Basecamp project website.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include <unistd.h>

#include "commands/dbcommands.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/task_tracker.h"
#include "distributed/transmit.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "libpq/hba.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"


int TaskTrackerDelay = 200;       /* process sleep interval in millisecs */
int MaxRunningTasksPerNode = 16;  /* max number of running tasks */
int MaxTrackedTasksPerNode = 1024; /* max number of tracked tasks */
int MaxTaskStringSize = 12288; /* max size of a worker task call string in bytes */
WorkerTasksSharedStateData *WorkerTasksSharedState; /* shared memory state */

/* Hash table shared by the task tracker and task tracker protocol functions */
HTAB *TaskTrackerTaskHash = NULL; /* shared memory */

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Flags set by interrupt handlers for later service in the main loop */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* initialization forward declarations */
static Size TaskTrackerShmemSize(void);
static void TaskTrackerShmemInit(void);

/* Signal handler forward declarations */
static void TrackerSigHupHandler(SIGNAL_ARGS);
static void TrackerShutdownHandler(SIGNAL_ARGS);

/* Local functions forward declarations */
static void TrackerCleanupJobSchemas(void);
static void TrackerCleanupConnections(HTAB *WorkerTasksHash);
static void TrackerRegisterShutDown(HTAB *WorkerTasksHash);
static void TrackerDelayLoop(void);
static List * SchedulableTaskList(HTAB *WorkerTasksHash);
static WorkerTask * SchedulableTaskPriorityQueue(HTAB *WorkerTasksHash);
static uint32 CountTasksMatchingCriteria(HTAB *WorkerTasksHash,
										 bool (*CriteriaFunction)(WorkerTask *));
static bool RunningTask(WorkerTask *workerTask);
static bool SchedulableTask(WorkerTask *workerTask);
static int CompareTasksByTime(const void *first, const void *second);
static void ScheduleWorkerTasks(HTAB *WorkerTasksHash, List *schedulableTaskList);
static void ManageWorkerTasksHash(HTAB *WorkerTasksHash);
static void ManageWorkerTask(WorkerTask *workerTask, HTAB *WorkerTasksHash);
static void RemoveWorkerTask(WorkerTask *workerTask, HTAB *WorkerTasksHash);
static void CreateJobDirectoryIfNotExists(uint64 jobId);
static void MarkWorkerTaskAsFailed(WorkerTask *workerTask);
static int32 ConnectToLocalBackend(const char *databaseName, const char *userName);


/* Organize, at startup, that the task tracker is started */
void
TaskTrackerRegister(void)
{
	BackgroundWorker worker;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = TaskTrackerShmemInit;

	if (IsUnderPostmaster)
	{
		return;
	}

	/* organize and register initialization of required shared memory */
	RequestAddinShmemSpace(TaskTrackerShmemSize());

	/* and that the task tracker is started as background worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 1;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "citus");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "TaskTrackerMain");
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "task tracker");

	RegisterBackgroundWorker(&worker);
}


/* Main entry point for task tracker process. */
void
TaskTrackerMain(Datum main_arg)
{
	sigjmp_buf local_sigjmp_buf;
	static bool processStartUp = true;

	/* Properly accept or ignore signals the postmaster might send us */
	pqsignal(SIGHUP, TrackerSigHupHandler); /* set flag to read config file */
	pqsignal(SIGTERM, TrackerShutdownHandler); /* request shutdown */

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	MemoryContext TaskTrackerContext = AllocSetContextCreateExtended(TopMemoryContext,
																	 "Task Tracker",
																	 ALLOCSET_DEFAULT_MINSIZE,
																	 ALLOCSET_DEFAULT_INITSIZE,
																	 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(TaskTrackerContext);

	/*
	 * If an exception is encountered, processing resumes here. The motivation
	 * for this code block is outlined in postgres.c, and the code itself is
	 * heavily based on bgwriter.c.
	 *
	 * In most error scenarios, we will not drop here: the task tracker process
	 * offloads all work to backend processes, and checks the completion of work
	 * through the client executor library. We will therefore only come here if
	 * we have inconsistencies in the shared hash and need to signal an error.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since we are not using PG_TRY, we must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are just a minimal subset of AbortTransaction().
		 * We do not have many resources to worry about; we only have a shared
		 * hash and an LWLock guarding that hash.
		 */
		LWLockReleaseAll();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context, and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TaskTrackerContext);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(TaskTrackerContext);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely to
		 * be repeated, and we don't want to be filling the error logs as fast
		 * as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * We run validation and cache cleanup functions as this process is starting
	 * up. If these functions throw an error, we won't try running them again.
	 */
	if (processStartUp)
	{
		processStartUp = false;

		/* clean up old files in the job cache */
		TrackerCleanupJobDirectories();

		/* clean up schemas in the job cache */
		TrackerCleanupJobSchemas();
	}

	/* Loop forever */
	for (;;)
	{
		/*
		 * Emergency bailout if postmaster has died. This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 *
		 * XXX: Note that PostgreSQL background processes no longer nap between
		 * their loops, but instead uses latches to wake up when necessary. We
		 * should switch to using latches in here too, and have the task tracker
		 * assign function notify us when there is a new task.
		 */
		if (!PostmasterIsAlive())
		{
			exit(1);
		}

		/* Process any requests or signals received recently */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;

			/* reload postgres configuration files */
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (got_SIGTERM)
		{
			/*
			 * From here on, reporting errors should end with exit(1), and not
			 * send control back to the sigsetjmp block above.
			 */
			ExitOnAnyError = true;

			/* Close open connections to local backends */
			TrackerCleanupConnections(TaskTrackerTaskHash);

			/* Add a sentinel task to the shared hash to mark shutdown */
			TrackerRegisterShutDown(TaskTrackerTaskHash);

			/* Normal exit from the task tracker is here */
			proc_exit(0);
		}

		/* Call the function that does the actual work */
		ManageWorkerTasksHash(TaskTrackerTaskHash);

		/* Sleep for the configured time */
		TrackerDelayLoop();
	}
}


/*
 * WorkerTasksHashEnter creates a new worker task in the shared hash, and
 * performs checks for this task. Note that the caller still needs to initialize
 * the worker task's fields, and hold the appopriate locks for the shared hash.
 */
WorkerTask *
WorkerTasksHashEnter(uint64 jobId, uint32 taskId)
{
	bool handleFound = false;

	WorkerTask searchTask;
	searchTask.jobId = jobId;
	searchTask.taskId = taskId;

	void *hashKey = (void *) &searchTask;
	WorkerTask *workerTask = (WorkerTask *) hash_search(TaskTrackerTaskHash, hashKey,
														HASH_ENTER_NULL, &handleFound);
	if (workerTask == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("out of shared memory"),
						errhint("Try increasing citus.max_tracked_tasks_per_node.")));
	}

	/* check that we do not have the same task assigned twice to this node */
	if (handleFound)
	{
		ereport(ERROR, (errmsg("cannot assign an already assigned task"),
						errdetail("Task jobId: " UINT64_FORMAT " and taskId: %u",
								  jobId, taskId)));
	}

	return workerTask;
}


/*
 * WorkerTasksHashFind looks up the worker task with the given identifiers in
 * the shared hash. Note that the caller still needs to hold the appropriate
 * locks for the shared hash.
 */
WorkerTask *
WorkerTasksHashFind(uint64 jobId, uint32 taskId)
{
	WorkerTask searchTask;
	searchTask.jobId = jobId;
	searchTask.taskId = taskId;

	void *hashKey = (void *) &searchTask;
	WorkerTask *workerTask = (WorkerTask *) hash_search(TaskTrackerTaskHash, hashKey,
														HASH_FIND, NULL);

	return workerTask;
}


/*
 * TrackerCleanupJobDirectories cleans up all files in the job cache directory
 * as part of this process's start-up logic. The task tracker process manages
 * both tasks in the shared hash and these tasks' output files. When the task
 * tracker needs to shutdown, all shared hash entries are deleted, but the
 * associated files cannot be cleaned up safely. We therefore perform this
 * cleanup when the process restarts.
 */
void
TrackerCleanupJobDirectories(void)
{
	/* use the default tablespace in {datadir}/base */
	StringInfo jobCacheDirectory = makeStringInfo();
	appendStringInfo(jobCacheDirectory, "base/%s", PG_JOB_CACHE_DIR);

	CitusRemoveDirectory(jobCacheDirectory->data);
	CitusCreateDirectory(jobCacheDirectory);

	FreeStringInfo(jobCacheDirectory);
}


/*
 * TrackerCleanupJobSchemas creates and assigns tasks to remove job schemas and
 * all tables within these schemas. These job schemas are currently created by
 * merge tasks, and may linger if the database shuts down before the jobs get
 * cleaned up. This function then runs during process start-up, and creates one
 * task per database to remove lingering job schemas, if any.
 */
static void
TrackerCleanupJobSchemas(void)
{
	/*
	 * XXX: We previously called DatabaseNameList() to read the list of database
	 * names here. This function read the database names from the flat database
	 * file; this file was deprecated on Aug 31, 2009. We hence need to rewrite
	 * this function to read from pg_database directly.
	 */
	List *databaseNameList = NIL;
	ListCell *databaseNameCell = NULL;
	const uint64 jobId = RESERVED_JOB_ID;
	uint32 taskIndex = 1;

	LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_EXCLUSIVE);

	foreach(databaseNameCell, databaseNameList)
	{
		char *databaseName = (char *) lfirst(databaseNameCell);

		/* template0 database does not accept connections */
		int skipDatabaseName = strncmp(databaseName, TEMPLATE0_NAME, NAMEDATALEN);
		if (skipDatabaseName == 0)
		{
			continue;
		}

		/*
		 * We create cleanup tasks since we can't remove schemas within the task
		 * tracker process. We also assign high priorities to these tasks so
		 * that they get scheduled before everyone else.
		 */
		WorkerTask *cleanupTask = WorkerTasksHashEnter(jobId, taskIndex);
		cleanupTask->assignedAt = HIGH_PRIORITY_TASK_TIME;
		cleanupTask->taskStatus = TASK_ASSIGNED;

		strlcpy(cleanupTask->taskCallString, JOB_SCHEMA_CLEANUP, MaxTaskStringSize);
		strlcpy(cleanupTask->databaseName, databaseName, NAMEDATALEN);

		/* zero out all other fields */
		cleanupTask->connectionId = INVALID_CONNECTION_ID;
		cleanupTask->failureCount = 0;

		taskIndex++;
	}

	LWLockRelease(&WorkerTasksSharedState->taskHashLock);

	if (databaseNameList != NIL)
	{
		list_free_deep(databaseNameList);
	}
}


/*
 * TrackerCleanupConnections closes all open connections to backends during
 * process shutdown. This signals to the backends that their connections are
 * gone and stops them from logging pipe-related warning messages.
 */
static void
TrackerCleanupConnections(HTAB *WorkerTasksHash)
{
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, WorkerTasksHash);

	WorkerTask *currentTask = (WorkerTask *) hash_seq_search(&status);
	while (currentTask != NULL)
	{
		if (currentTask->connectionId != INVALID_CONNECTION_ID)
		{
			MultiClientDisconnect(currentTask->connectionId);
			currentTask->connectionId = INVALID_CONNECTION_ID;
		}

		currentTask = (WorkerTask *) hash_seq_search(&status);
	}
}


/*
 * TrackerRegisterShutDown enters a special marker task to the shared hash. This
 * marker task indicates to "task protocol processes" that we are shutting down
 * and that they shouldn't accept new task assignments.
 */
static void
TrackerRegisterShutDown(HTAB *WorkerTasksHash)
{
	uint64 jobId = RESERVED_JOB_ID;
	uint32 taskId = SHUTDOWN_MARKER_TASK_ID;

	LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_EXCLUSIVE);

	WorkerTask *shutdownMarkerTask = WorkerTasksHashEnter(jobId, taskId);
	shutdownMarkerTask->taskStatus = TASK_SUCCEEDED;
	shutdownMarkerTask->connectionId = INVALID_CONNECTION_ID;

	LWLockRelease(&WorkerTasksSharedState->taskHashLock);
}


/* Sleeps either for the configured time or until a signal is received. */
static void
TrackerDelayLoop(void)
{
	const long SignalCheckInterval = 1000000L; /* check signal every second */

	/*
	 * On some platforms, signals do not interrupt the sleep. To ensure we
	 * respond promptly when someone signals us, we break down the sleep into
	 * 1-second increments, and check for interrupts after each nap.
	 */
	long trackerDelay = TaskTrackerDelay * 1000L;
	while (trackerDelay > (SignalCheckInterval - 1))
	{
		if (got_SIGHUP || got_SIGTERM)
		{
			break;
		}
		pg_usleep(SignalCheckInterval);
		trackerDelay -= SignalCheckInterval;
	}
	if (!(got_SIGHUP || got_SIGTERM))
	{
		pg_usleep(trackerDelay);
	}
}


/* ------------------------------------------------------------
 * Signal handling and shared hash initialization functions follow
 * ------------------------------------------------------------
 */

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
TrackerSigHupHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGHUP = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/* SIGTERM: set flag for main loop to exit normally */
static void
TrackerShutdownHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/* Estimates the shared memory size used for keeping track of tasks. */
static Size
TaskTrackerShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(WorkerTasksSharedStateData));

	Size hashSize = hash_estimate_size(MaxTrackedTasksPerNode, WORKER_TASK_SIZE);
	size = add_size(size, hashSize);

	return size;
}


/* Initializes the shared memory used for keeping track of tasks. */
static void
TaskTrackerShmemInit(void)
{
	bool alreadyInitialized = false;
	HASHCTL info;

	long maxTableSize = (long) MaxTrackedTasksPerNode;
	long initTableSize = maxTableSize / 8;

	/*
	 * Allocate the control structure for the hash table that maps unique task
	 * identifiers (uint64:uint32) to general task information, as well as the
	 * parameters needed to run the task.
	 */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint64) + sizeof(uint32);
	info.entrysize = WORKER_TASK_SIZE;
	info.hash = tag_hash;
	int hashFlags = (HASH_ELEM | HASH_FUNCTION);

	/*
	 * Currently the lock isn't required because allocation only happens at
	 * startup in postmaster, but it doesn't hurt, and makes things more
	 * consistent with other extensions.
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* allocate struct containing task tracker related shared state */
	WorkerTasksSharedState =
		(WorkerTasksSharedStateData *) ShmemInitStruct("Worker Task Control",
													   sizeof(WorkerTasksSharedStateData),
													   &alreadyInitialized);

	if (!alreadyInitialized)
	{
		WorkerTasksSharedState->taskHashTrancheId = LWLockNewTrancheId();
		WorkerTasksSharedState->taskHashTrancheName = "Worker Task Hash Tranche";
		LWLockRegisterTranche(WorkerTasksSharedState->taskHashTrancheId,
							  WorkerTasksSharedState->taskHashTrancheName);

		LWLockInitialize(&WorkerTasksSharedState->taskHashLock,
						 WorkerTasksSharedState->taskHashTrancheId);

		WorkerTasksSharedState->conninfosValid = true;
	}

	/*  allocate hash table */
	TaskTrackerTaskHash = ShmemInitHash("Worker Task Hash", initTableSize, maxTableSize,
										&info, hashFlags);

	LWLockRelease(AddinShmemInitLock);

	Assert(TaskTrackerTaskHash != NULL);
	Assert(WorkerTasksSharedState->taskHashTrancheId != 0);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/* ------------------------------------------------------------
 * Task scheduling and management functions follow
 * ------------------------------------------------------------
 */

/*
 * SchedulableTaskList calculates the number of tasks to schedule at this given
 * moment, and creates a deep-copied list containing that many tasks. The tasks
 * in the list are sorted according to a priority criteria, currently the task's
 * assignment time. Note that this function expects the caller to hold a read
 * lock over the shared hash.
 */
static List *
SchedulableTaskList(HTAB *WorkerTasksHash)
{
	List *schedulableTaskList = NIL;

	uint32 runningTaskCount = CountTasksMatchingCriteria(WorkerTasksHash, &RunningTask);
	if (runningTaskCount >= MaxRunningTasksPerNode)
	{
		return NIL;  /* we already have enough tasks running */
	}

	uint32 schedulableTaskCount = CountTasksMatchingCriteria(WorkerTasksHash,
															 &SchedulableTask);
	if (schedulableTaskCount == 0)
	{
		return NIL;  /* we do not have any new tasks to schedule */
	}

	uint32 tasksToScheduleCount = MaxRunningTasksPerNode - runningTaskCount;
	if (tasksToScheduleCount > schedulableTaskCount)
	{
		tasksToScheduleCount = schedulableTaskCount;
	}

	/* get all schedulable tasks ordered according to a priority criteria */
	WorkerTask *schedulableTaskQueue = SchedulableTaskPriorityQueue(WorkerTasksHash);

	for (uint32 queueIndex = 0; queueIndex < tasksToScheduleCount; queueIndex++)
	{
		WorkerTask *schedulableTask = (WorkerTask *) palloc0(WORKER_TASK_SIZE);
		WorkerTask *queuedTask = WORKER_TASK_AT(schedulableTaskQueue, queueIndex);
		schedulableTask->jobId = queuedTask->jobId;
		schedulableTask->taskId = queuedTask->taskId;

		schedulableTaskList = lappend(schedulableTaskList, schedulableTask);
	}

	/* free priority queue */
	pfree(schedulableTaskQueue);

	return schedulableTaskList;
}


/*
 * SchedulableTaskPriorityQueue allocates an array containing all schedulable
 * tasks in the shared hash, orders these tasks according to a sorting criteria,
 * and returns the sorted array.
 */
static WorkerTask *
SchedulableTaskPriorityQueue(HTAB *WorkerTasksHash)
{
	HASH_SEQ_STATUS status;
	uint32 queueIndex = 0;

	/* our priority queue size equals to the number of schedulable tasks */
	uint32 queueSize = CountTasksMatchingCriteria(WorkerTasksHash, &SchedulableTask);
	if (queueSize == 0)
	{
		return NULL;
	}

	/* allocate an array of tasks for our priority queue */
	WorkerTask *priorityQueue = (WorkerTask *) palloc0(WORKER_TASK_SIZE * queueSize);

	/* copy tasks in the shared hash to the priority queue */
	hash_seq_init(&status, WorkerTasksHash);

	WorkerTask *currentTask = (WorkerTask *) hash_seq_search(&status);
	while (currentTask != NULL)
	{
		if (SchedulableTask(currentTask))
		{
			/* tasks in the priority queue only need the first three fields */
			WorkerTask *queueTask = WORKER_TASK_AT(priorityQueue, queueIndex);

			queueTask->jobId = currentTask->jobId;
			queueTask->taskId = currentTask->taskId;
			queueTask->assignedAt = currentTask->assignedAt;

			queueIndex++;
		}

		currentTask = (WorkerTask *) hash_seq_search(&status);
	}

	/* now order elements in the queue according to our sorting criterion */
	qsort(priorityQueue, queueSize, WORKER_TASK_SIZE, CompareTasksByTime);

	return priorityQueue;
}


/* Counts the number of tasks that match the given criteria function. */
static uint32
CountTasksMatchingCriteria(HTAB *WorkerTasksHash,
						   bool (*CriteriaFunction)(WorkerTask *))
{
	HASH_SEQ_STATUS status;
	uint32 taskCount = 0;

	hash_seq_init(&status, WorkerTasksHash);

	WorkerTask *currentTask = (WorkerTask *) hash_seq_search(&status);
	while (currentTask != NULL)
	{
		bool matchesCriteria = (*CriteriaFunction)(currentTask);
		if (matchesCriteria)
		{
			taskCount++;
		}

		currentTask = (WorkerTask *) hash_seq_search(&status);
	}

	return taskCount;
}


/* Checks if the worker task is running. */
static bool
RunningTask(WorkerTask *workerTask)
{
	TaskStatus currentStatus = workerTask->taskStatus;
	if (currentStatus == TASK_RUNNING)
	{
		return true;
	}

	return false;
}


/* Checks if the worker task can be scheduled to run. */
static bool
SchedulableTask(WorkerTask *workerTask)
{
	TaskStatus currentStatus = workerTask->taskStatus;
	if (currentStatus == TASK_ASSIGNED)
	{
		return true;
	}

	return false;
}


/* Comparison function to compare two worker tasks by their assignment times. */
static int
CompareTasksByTime(const void *first, const void *second)
{
	WorkerTask *firstTask = (WorkerTask *) first;
	WorkerTask *secondTask = (WorkerTask *) second;

	/* tasks that are assigned earlier have higher priority */
	int timeDiff = firstTask->assignedAt - secondTask->assignedAt;
	return timeDiff;
}


/*
 * ScheduleWorkerTasks takes a list of tasks to schedule, and for each task in
 * the list, finds and schedules the corresponding task from the shared hash.
 * Note that this function expects the caller to hold an exclusive lock over the
 * shared hash.
 */
static void
ScheduleWorkerTasks(HTAB *WorkerTasksHash, List *schedulableTaskList)
{
	ListCell *schedulableTaskCell = NULL;
	foreach(schedulableTaskCell, schedulableTaskList)
	{
		WorkerTask *schedulableTask = (WorkerTask *) lfirst(schedulableTaskCell);
		void *hashKey = (void *) schedulableTask;

		WorkerTask *taskToSchedule = (WorkerTask *) hash_search(WorkerTasksHash, hashKey,
																HASH_FIND, NULL);

		/* if task is null, the shared hash is in an incosistent state */
		if (taskToSchedule == NULL)
		{
			ereport(ERROR, (errmsg("could not find the worker task to schedule"),
							errdetail("Task jobId: " UINT64_FORMAT " and taskId: %u",
									  schedulableTask->jobId, schedulableTask->taskId)));
		}

		/*
		 * After determining the set of tasks to schedule, we release the hash's
		 * shared lock for a short time period. We then re-acquire the lock in
		 * exclusive mode. We therefore need to check if this task has been
		 * canceled in the meantime.
		 */
		if (taskToSchedule->taskStatus != TASK_CANCEL_REQUESTED)
		{
			Assert(SchedulableTask(taskToSchedule));

			taskToSchedule->taskStatus = TASK_SCHEDULED;
		}
		else
		{
			ereport(INFO, (errmsg("the worker task to schedule has been canceled"),
						   errdetail("Task jobId: " UINT64_FORMAT " and taskId: %u",
									 schedulableTask->jobId, schedulableTask->taskId)));
		}
	}
}


/* Manages the scheduling and execution of all tasks in the shared hash. */
static void
ManageWorkerTasksHash(HTAB *WorkerTasksHash)
{
	HASH_SEQ_STATUS status;

	/* ask the scheduler if we have new tasks to schedule */
	LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_SHARED);
	List *schedulableTaskList = SchedulableTaskList(WorkerTasksHash);
	LWLockRelease(&WorkerTasksSharedState->taskHashLock);

	LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_EXCLUSIVE);

	if (!WorkerTasksSharedState->conninfosValid)
	{
		InvalidateConnParamsHashEntries();
	}

	/* schedule new tasks if we have any */
	if (schedulableTaskList != NIL)
	{
		ScheduleWorkerTasks(WorkerTasksHash, schedulableTaskList);
		list_free_deep(schedulableTaskList);
	}

	/* now iterate over all tasks, and manage them */
	hash_seq_init(&status, WorkerTasksHash);

	WorkerTask *currentTask = (WorkerTask *) hash_seq_search(&status);
	while (currentTask != NULL)
	{
		ManageWorkerTask(currentTask, WorkerTasksHash);

		/*
		 * Typically, we delete worker tasks in the task tracker protocol
		 * process. This task however was canceled mid-query, and the protocol
		 * process asked us to remove it from the shared hash.
		 */
		if (currentTask->taskStatus == TASK_TO_REMOVE)
		{
			RemoveWorkerTask(currentTask, WorkerTasksHash);
		}

		currentTask = (WorkerTask *) hash_seq_search(&status);
	}

	LWLockRelease(&WorkerTasksSharedState->taskHashLock);
}


/*
 * ManageWorkerTask manages the execution of the worker task. More specifically,
 * the function connects to a local backend, sends the query associated with the
 * task, and oversees the query's execution. Note that this function expects the
 * caller to hold an exclusive lock over the shared hash.
 */
static void
ManageWorkerTask(WorkerTask *workerTask, HTAB *WorkerTasksHash)
{
	switch (workerTask->taskStatus)
	{
		case TASK_ASSIGNED:
		{
			break;  /* nothing to do until the task gets scheduled */
		}

		case TASK_SCHEDULED:
		{
			/* create the job output directory if it does not exist */
			CreateJobDirectoryIfNotExists(workerTask->jobId);

			/* the task is ready to run; connect to local backend */
			workerTask->connectionId = ConnectToLocalBackend(workerTask->databaseName,
															 workerTask->userName);

			if (workerTask->connectionId != INVALID_CONNECTION_ID)
			{
				bool taskSent = MultiClientSendQuery(workerTask->connectionId,
													 workerTask->taskCallString);
				if (taskSent)
				{
					workerTask->taskStatus = TASK_RUNNING;
				}
				else
				{
					MarkWorkerTaskAsFailed(workerTask);

					MultiClientDisconnect(workerTask->connectionId);
					workerTask->connectionId = INVALID_CONNECTION_ID;
				}
			}
			else
			{
				MarkWorkerTaskAsFailed(workerTask);
			}

			break;
		}

		case TASK_RUNNING:
		{
			int32 connectionId = workerTask->connectionId;
			ResultStatus resultStatus = MultiClientResultStatus(connectionId);

			/* check if query results are ready, in progress, or unavailable */
			if (resultStatus == CLIENT_RESULT_READY)
			{
				QueryStatus queryStatus = MultiClientQueryStatus(connectionId);
				if (queryStatus == CLIENT_QUERY_DONE)
				{
					workerTask->taskStatus = TASK_SUCCEEDED;
				}
				else if (queryStatus == CLIENT_QUERY_FAILED)
				{
					MarkWorkerTaskAsFailed(workerTask);
				}
				else
				{
					ereport(FATAL, (errmsg("invalid query status: %d", queryStatus)));
				}
			}
			else if (resultStatus == CLIENT_RESULT_BUSY)
			{
				workerTask->taskStatus = TASK_RUNNING;
			}
			else if (resultStatus == CLIENT_RESULT_UNAVAILABLE)
			{
				MarkWorkerTaskAsFailed(workerTask);
			}

			/* clean up the connection if we are done with the task */
			if (resultStatus != CLIENT_RESULT_BUSY)
			{
				MultiClientDisconnect(workerTask->connectionId);
				workerTask->connectionId = INVALID_CONNECTION_ID;
			}

			break;
		}

		case TASK_FAILED:
		{
			if (workerTask->failureCount < MAX_TASK_FAILURE_COUNT)
			{
				workerTask->taskStatus = TASK_ASSIGNED;
			}
			else
			{
				workerTask->taskStatus = TASK_PERMANENTLY_FAILED;
			}

			break;
		}

		case TASK_PERMANENTLY_FAILED:
		case TASK_SUCCEEDED:
		{
			break;
		}

		case TASK_CANCEL_REQUESTED:
		{
			/*
			 * If this task is responsible for cleaning up the shared hash, we
			 * give the task more time instead of canceling it. The reason this
			 * task is marked for cancellation is that its file descriptor needs
			 * to be reclaimed after the clean up completes.
			 */
			if (workerTask->taskId == JOB_CLEANUP_TASK_ID)
			{
				workerTask->taskStatus = TASK_CANCELED;
				break;
			}

			if (workerTask->connectionId != INVALID_CONNECTION_ID)
			{
				int32 connectionId = workerTask->connectionId;

				ResultStatus status = MultiClientResultStatus(connectionId);
				if (status == CLIENT_RESULT_BUSY)
				{
					MultiClientCancel(connectionId);
				}
			}

			/* give the backend some time to flush its response */
			workerTask->taskStatus = TASK_CANCELED;
			break;
		}

		case TASK_CANCELED:
		{
			if (workerTask->connectionId != INVALID_CONNECTION_ID)
			{
				MultiClientDisconnect(workerTask->connectionId);
				workerTask->connectionId = INVALID_CONNECTION_ID;
			}

			if (workerTask->taskId == JOB_CLEANUP_TASK_ID)
			{
				StringInfo jobDirectoryName = JobDirectoryName(workerTask->jobId);
				CitusRemoveDirectory(jobDirectoryName->data);
			}

			workerTask->taskStatus = TASK_TO_REMOVE;
			break;
		}

		case TASK_TO_REMOVE:
		default:
		{
			/* we fatal here to avoid leaking client-side resources */
			ereport(FATAL, (errmsg("invalid task status: %d", workerTask->taskStatus)));
			break;
		}
	}

	Assert(workerTask->failureCount <= MAX_TASK_FAILURE_COUNT);
}


/*
 * MarkWorkerTaskAsFailed marks the given worker task as failed
 * and increases the failure count. Failure count is used to
 * determine if the task should be marked as permanently failed.
 */
static void
MarkWorkerTaskAsFailed(WorkerTask *workerTask)
{
	workerTask->taskStatus = TASK_FAILED;
	workerTask->failureCount++;
}


/* Wrapper function to remove the worker task from the shared hash. */
static void
RemoveWorkerTask(WorkerTask *workerTask, HTAB *WorkerTasksHash)
{
	void *hashKey = (void *) workerTask;

	WorkerTask *taskRemoved = hash_search(WorkerTasksHash, hashKey, HASH_REMOVE, NULL);
	if (taskRemoved == NULL)
	{
		ereport(FATAL, (errmsg("worker task hash corrupted")));
	}
}


/* Wrapper function to create the job directory if it does not already exist. */
static void
CreateJobDirectoryIfNotExists(uint64 jobId)
{
	StringInfo jobDirectoryName = JobDirectoryName(jobId);

	bool jobDirectoryExists = DirectoryExists(jobDirectoryName);
	if (!jobDirectoryExists)
	{
		CitusCreateDirectory(jobDirectoryName);
	}

	FreeStringInfo(jobDirectoryName);
}


/* Wrapper function to inititate connection to local backend. */
static int32
ConnectToLocalBackend(const char *databaseName, const char *userName)
{
	const char *nodeName = LOCAL_HOST_NAME;
	const uint32 nodePort = PostPortNumber;

	/*
	 * Our client library currently only handles TCP sockets. We therefore do
	 * not use Unix domain sockets here.
	 */
	int32 connectionId = MultiClientConnect(nodeName, nodePort, databaseName, userName);

	return connectionId;
}
