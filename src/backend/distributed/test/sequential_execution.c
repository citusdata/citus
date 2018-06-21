/*-------------------------------------------------------------------------
 *
 * test/src/sequential_execution.c
 *
 * This file contains functions to test setting citus.multi_shard_modify_mode
 * GUC.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "distributed/multi_executor.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(set_local_multi_shard_modify_mode_to_sequential);


/*
 * set_local_multi_shard_modify_mode_to_sequential is a SQL
 * interface for testing SetLocalMultiShardModifyModeToSequential().
 */
Datum
set_local_multi_shard_modify_mode_to_sequential(PG_FUNCTION_ARGS)
{
	SetLocalMultiShardModifyModeToSequential();

	PG_RETURN_VOID();
}
