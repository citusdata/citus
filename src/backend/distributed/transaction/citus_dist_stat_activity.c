/*-------------------------------------------------------------------------
 *
 * citus_dist_stat_activity.c
 *
 *	The methods in the file are deprecated.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

PG_FUNCTION_INFO_V1(citus_dist_stat_activity);
PG_FUNCTION_INFO_V1(citus_worker_stat_activity);

/* This UDF is deprecated. */
Datum
citus_dist_stat_activity(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("This UDF is deprecated.")));

	PG_RETURN_NULL();
}


/* This UDF is deprecated. */
Datum
citus_worker_stat_activity(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("This UDF is deprecated.")));

	PG_RETURN_NULL();
}
