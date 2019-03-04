/*-------------------------------------------------------------------------
 *
 * task_tracker_protocol.c
 *
 * The task tracker background process runs on every worker node. The following
 * routines allow for the master node to assign tasks to the task tracker, check
 * these tasks' statuses, and remove these tasks when they are no longer needed.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include <time.h>

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/schemacmds.h"
#include "commands/trigger.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/task_tracker.h"
#include "distributed/task_tracker_protocol.h"
#include "distributed/worker_protocol.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"


/* Local functions forward declarations */
static bool TaskTrackerRunning(void);
static void CreateJobSchema(StringInfo schemaName);
static void CreateTask(uint64 jobId, uint32 taskId, char *taskCallString);
static void UpdateTask(WorkerTask *workerTask, char *taskCallString);
static void CleanupTask(WorkerTask *workerTask);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(task_tracker_assign_task);
PG_FUNCTION_INFO_V1(task_tracker_task_status);
PG_FUNCTION_INFO_V1(task_tracker_cleanup_job);
PG_FUNCTION_INFO_V1(task_tracker_conninfo_cache_invalidate);


/*
 * task_tracker_assign_task creates a new task in the shared hash or updates an
 * already existing task. The function also creates a schema for the job if it
 * doesn't already exist.
 */
Datum
task_tracker_assign_task(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 taskId = PG_GETARG_UINT32(1);
	text *taskCallStringText = PG_GETARG_TEXT_P(2);

	StringInfo jobSchemaName = JobSchemaName(jobId);
	bool schemaExists = false;

	WorkerTask *workerTask = NULL;
	char *taskCallString = text_to_cstring(taskCallStringText);
	uint32 taskCallStringLength = strlen(taskCallString);

	bool taskTrackerRunning = false;

	CheckCitusVersion(ERROR);

	/* check that we have a running task tracker on this host */
	taskTrackerRunning = TaskTrackerRunning();
	if (!taskTrackerRunning)
	{
		ereport(ERROR, (errcode(ERRCODE_CANNOT_CONNECT_NOW),
						errmsg("the task tracker has been disabled or shut down")));
	}

	/* check that we have enough space in our shared hash for this string */
	if (taskCallStringLength >= MaxTaskStringSize)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("task string length (%d) exceeds maximum assignable "
							   "size (%d)", taskCallStringLength, MaxTaskStringSize),
						errhint("Consider increasing citus.max_task_string_size.")));
	}

	/*
	 * If the schema does not exist, we create it. However, the schema does not
	 * become visible to other processes until the transaction commits, and we
	 * therefore do not release the resource lock in this case. Otherwise, the
	 * schema is already visible, and we immediately release the resource lock.
	 */
	LockJobResource(jobId, AccessExclusiveLock);
	schemaExists = JobSchemaExists(jobSchemaName);
	if (!schemaExists)
	{
		/* lock gets automatically released upon return from this function */
		CreateJobSchema(jobSchemaName);
	}
	else
	{
		Oid schemaId = get_namespace_oid(jobSchemaName->data, false);

		EnsureSchemaOwner(schemaId);

		UnlockJobResource(jobId, AccessExclusiveLock);
	}

	LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_EXCLUSIVE);

	/* check if we already have the task in our shared hash */
	workerTask = WorkerTasksHashFind(jobId, taskId);
	if (workerTask == NULL)
	{
		CreateTask(jobId, taskId, taskCallString);
	}
	else
	{
		UpdateTask(workerTask, taskCallString);
	}

	LWLockRelease(&WorkerTasksSharedState->taskHashLock);

	PG_RETURN_VOID();
}


/* Returns the task status of an already existing task. */
Datum
task_tracker_task_status(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 taskId = PG_GETARG_UINT32(1);

	WorkerTask *workerTask = NULL;
	uint32 taskStatus = 0;
	char *userName = CurrentUserName();
	bool taskTrackerRunning = false;

	CheckCitusVersion(ERROR);

	taskTrackerRunning = TaskTrackerRunning();

	if (taskTrackerRunning)
	{
		LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_SHARED);

		workerTask = WorkerTasksHashFind(jobId, taskId);
		if (workerTask == NULL ||
			(!superuser() && strncmp(userName, workerTask->userName, NAMEDATALEN) != 0))
		{
			ereport(ERROR, (errmsg("could not find the worker task"),
							errdetail("Task jobId: " UINT64_FORMAT " and taskId: %u",
									  jobId, taskId)));
		}

		taskStatus = (uint32) workerTask->taskStatus;

		LWLockRelease(&WorkerTasksSharedState->taskHashLock);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_CANNOT_CONNECT_NOW),
						errmsg("the task tracker has been disabled or shut down")));
	}

	PG_RETURN_UINT32(taskStatus);
}


/*
 * task_tracker_cleanup_job finds all tasks for the given job, and cleans up
 * files, connections, and shared hash enties associated with these tasks.
 */
Datum
task_tracker_cleanup_job(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);

	bool schemaExists = false;
	HASH_SEQ_STATUS status;
	WorkerTask *currentTask = NULL;
	StringInfo jobDirectoryName = NULL;
	StringInfo jobSchemaName = NULL;

	CheckCitusVersion(ERROR);

	jobSchemaName = JobSchemaName(jobId);

	/*
	 * We'll keep this lock for a while, but that's ok because nothing
	 * else should be happening on this job.
	 */
	LockJobResource(jobId, AccessExclusiveLock);

	schemaExists = JobSchemaExists(jobSchemaName);
	if (schemaExists)
	{
		Oid schemaId = get_namespace_oid(jobSchemaName->data, false);

		EnsureSchemaOwner(schemaId);
	}

	/*
	 * We first clean up any open connections, and remove tasks belonging to
	 * this job from the shared hash.
	 */
	LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_EXCLUSIVE);

	hash_seq_init(&status, TaskTrackerTaskHash);

	currentTask = (WorkerTask *) hash_seq_search(&status);
	while (currentTask != NULL)
	{
		if (currentTask->jobId == jobId)
		{
			CleanupTask(currentTask);
		}

		currentTask = (WorkerTask *) hash_seq_search(&status);
	}

	LWLockRelease(&WorkerTasksSharedState->taskHashLock);

	/*
	 * We then delete the job directory and schema, if they exist. This cleans
	 * up all intermediate files and tables allocated for the job. Note that the
	 * schema drop call can block if another process is creating the schema or
	 * writing to a table within the schema.
	 */
	jobDirectoryName = JobDirectoryName(jobId);
	CitusRemoveDirectory(jobDirectoryName);

	RemoveJobSchema(jobSchemaName);
	UnlockJobResource(jobId, AccessExclusiveLock);

	PG_RETURN_VOID();
}


/*
 * task_tracker_conninfo_cache_invalidate is a trigger function that signals to
 * the task tracker to refresh its conn params cache after a authinfo change.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
task_tracker_conninfo_cache_invalidate(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	/* no-op in community edition */

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * TaskTrackerRunning checks if the task tracker process is running. To do this,
 * the function checks if the task tracker is configured to start up, and infers
 * from shared memory that the tracker hasn't received a shut down request.
 */
static bool
TaskTrackerRunning(void)
{
	WorkerTask *workerTask = NULL;
	bool postmasterAlive = true;
	bool taskTrackerRunning = true;

	/* if postmaster shut down, infer task tracker shut down from it */
	postmasterAlive = PostmasterIsAlive();
	if (!postmasterAlive)
	{
		return false;
	}

	/*
	 * When the task tracker receives a termination signal, it inserts a special
	 * marker task to the shared hash. We need to look up this marker task since
	 * the postmaster doesn't send a terminate signal to running backends.
	 */
	LWLockAcquire(&WorkerTasksSharedState->taskHashLock, LW_SHARED);

	workerTask = WorkerTasksHashFind(RESERVED_JOB_ID, SHUTDOWN_MARKER_TASK_ID);
	if (workerTask != NULL)
	{
		taskTrackerRunning = false;
	}

	LWLockRelease(&WorkerTasksSharedState->taskHashLock);

	return taskTrackerRunning;
}


/*
 * CreateJobSchema creates a job schema with the given schema name. Note that
 * this function ensures that our pg_ prefixed schema names can be created.
 * Further note that the created schema does not become visible to other
 * processes until the transaction commits.
 */
static void
CreateJobSchema(StringInfo schemaName)
{
	const char *queryString = NULL;
	bool oldAllowSystemTableMods = false;

	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	CreateSchemaStmt *createSchemaStmt = NULL;
	RoleSpec currentUserRole = { 0 };

	/* allow schema names that start with pg_ */
	oldAllowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;

	/* ensure we're allowed to create this schema */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* build a CREATE SCHEMA statement */
	currentUserRole.type = T_RoleSpec;
	currentUserRole.roletype = ROLESPEC_CSTRING;
	currentUserRole.rolename = GetUserNameFromId(savedUserId, false);
	currentUserRole.location = -1;

	createSchemaStmt = makeNode(CreateSchemaStmt);
	createSchemaStmt->schemaname = schemaName->data;
	createSchemaStmt->schemaElts = NIL;

	/* actually create schema with the current user as owner */
	createSchemaStmt->authrole = &currentUserRole;
	CreateSchemaCommand(createSchemaStmt, queryString, -1, -1);

	CommandCounterIncrement();

	/* and reset environment */
	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	allowSystemTableMods = oldAllowSystemTableMods;
}


/*
 * CreateTask creates a new task in shared hash, initializes the task, and sets
 * the task to assigned state. Note that this function expects the caller to
 * hold an exclusive lock over the shared hash.
 */
static void
CreateTask(uint64 jobId, uint32 taskId, char *taskCallString)
{
	WorkerTask *workerTask = NULL;
	uint32 assignmentTime = 0;
	char *databaseName = CurrentDatabaseName();
	char *userName = CurrentUserName();

	/* increase task priority for cleanup tasks */
	assignmentTime = (uint32) time(NULL);
	if (taskId == JOB_CLEANUP_TASK_ID)
	{
		assignmentTime = HIGH_PRIORITY_TASK_TIME;
	}

	/* enter the worker task into shared hash and initialize the task */
	workerTask = WorkerTasksHashEnter(jobId, taskId);
	workerTask->assignedAt = assignmentTime;
	strlcpy(workerTask->taskCallString, taskCallString, MaxTaskStringSize);

	workerTask->taskStatus = TASK_ASSIGNED;
	workerTask->connectionId = INVALID_CONNECTION_ID;
	workerTask->failureCount = 0;
	strlcpy(workerTask->databaseName, databaseName, NAMEDATALEN);
	strlcpy(workerTask->userName, userName, NAMEDATALEN);
}


/*
 * UpdateTask updates the call string text for an already existing task. Note
 * that this function expects the caller to hold an exclusive lock over the
 * shared hash.
 */
static void
UpdateTask(WorkerTask *workerTask, char *taskCallString)
{
	TaskStatus taskStatus = TASK_STATUS_INVALID_FIRST;

	taskStatus = workerTask->taskStatus;
	Assert(taskStatus != TASK_STATUS_INVALID_FIRST);

	/*
	 * 1. If the task has succeeded or has been canceled, we don't do anything.
	 * 2. If the task has permanently failed, we update the task call string,
	 * reset the failure count, and change the task's status to schedulable.
	 * 3. If the task is in conduit, we update the task call string, and reset
	 * the failure count.
	 */
	if (taskStatus == TASK_SUCCEEDED || taskStatus == TASK_CANCEL_REQUESTED ||
		taskStatus == TASK_CANCELED)
	{
		/* nothing to do */
	}
	else if (taskStatus == TASK_PERMANENTLY_FAILED)
	{
		strlcpy(workerTask->taskCallString, taskCallString, MaxTaskStringSize);
		workerTask->failureCount = 0;
		workerTask->taskStatus = TASK_ASSIGNED;
	}
	else
	{
		strlcpy(workerTask->taskCallString, taskCallString, MaxTaskStringSize);
		workerTask->failureCount = 0;
	}
}


/* Cleans up connection and shared hash entry associated with the given task. */
static void
CleanupTask(WorkerTask *workerTask)
{
	WorkerTask *taskRemoved = NULL;
	void *hashKey = (void *) workerTask;

	/*
	 * If the connection is still valid, the master node decided to terminate
	 * the task prematurely. This can happen when the user wants to cancel the
	 * query, or when a speculatively executed task finishes elsewhere and the
	 * query completes.
	 */
	if (workerTask->connectionId != INVALID_CONNECTION_ID)
	{
		/*
		 * The task tracker process owns the connections to local backends, and
		 * we cannot interefere with those connections from another process. We
		 * therefore ask the task tracker to clean up the connection and to
		 * remove the task from the shared hash. Note that one of the cleaned up
		 * tasks will always be the clean-up task itself.
		 */
		ereport(DEBUG3, (errmsg("requesting cancel for worker task"),
						 errdetail("Task jobId: " UINT64_FORMAT " and taskId: %u",
								   workerTask->jobId, workerTask->taskId)));

		workerTask->taskStatus = TASK_CANCEL_REQUESTED;
		return;
	}

	/* remove the task from the shared hash */
	taskRemoved = hash_search(TaskTrackerTaskHash, hashKey, HASH_REMOVE, NULL);
	if (taskRemoved == NULL)
	{
		ereport(FATAL, (errmsg("worker task hash corrupted")));
	}
}
