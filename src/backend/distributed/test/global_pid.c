/*-------------------------------------------------------------------------
 *
 * test/src/global_pid.c
 *
 * This file contains functions to test the global pid.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "distributed/backend_data.h"
#include "distributed/metadata_cache.h"

PG_FUNCTION_INFO_V1(test_assign_global_pid);


/*
 * test_assign_global_pid is the wrapper UDF for AssignGlobalPID and is only meant for use
 * in tests.
 */
Datum
test_assign_global_pid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	AssignGlobalPID(application_name);

	PG_RETURN_VOID();
}
