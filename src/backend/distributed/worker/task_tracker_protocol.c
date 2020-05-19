/*-------------------------------------------------------------------------
 *
 * task_tracker_protocol.c
 *
 * The task tracker background process runs on every worker node. The following
 * routines allow for the master node to assign tasks to the task tracker, check
 * these tasks' statuses, and remove these tasks when they are no longer needed.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"


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
	ereport(ERROR, (errmsg("not supposed to get here, did you cheat?")));

	PG_RETURN_NULL();
}


/* Returns the task status of an already existing task. */
Datum
task_tracker_task_status(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("not supposed to get here, did you cheat?")));

	PG_RETURN_UINT32(0);
}


/*
 * task_tracker_cleanup_job finds all tasks for the given job, and cleans up
 * files, connections, and shared hash enties associated with these tasks.
 */
Datum
task_tracker_cleanup_job(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("not supposed to get here, did you cheat?")));

	PG_RETURN_NULL();
}


/*
 * task_tracker_conninfo_cache_invalidate is a trigger function that signals to
 * the task tracker to refresh its conn params cache after an authinfo change.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
task_tracker_conninfo_cache_invalidate(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("not supposed to get here, did you cheat?")));

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}
