/*-------------------------------------------------------------------------
 *
 * task_tracker_protocol.c
 *
 * The methods in the file are deprecated.
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


/* This UDF is deprecated.*/
Datum
task_tracker_assign_task(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("This UDF is deprecated.")));

	PG_RETURN_NULL();
}


/* This UDF is deprecated.*/
Datum
task_tracker_task_status(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("This UDF is deprecated.")));

	PG_RETURN_UINT32(0);
}


/* This UDF is deprecated.*/
Datum
task_tracker_cleanup_job(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("This UDF is deprecated.")));

	PG_RETURN_NULL();
}


/* This UDF is deprecated.*/
Datum
task_tracker_conninfo_cache_invalidate(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(PointerGetDatum(NULL));
}
