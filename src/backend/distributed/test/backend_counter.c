/*-------------------------------------------------------------------------
 *
 * test/src/backend_counter.c
 *
 * This file contains functions to test the active backend counter
 * within Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "distributed/backend_data.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(get_all_active_client_backend_count);


/*
 * get_all_active_client_backend_count returns the active
 * client backend count.
 */
Datum
get_all_active_client_backend_count(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT32(GetExternalClientBackendCount());
}
