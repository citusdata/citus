/*-------------------------------------------------------------------------
 *
 * worker_split_shard_release_dsm.c
 *    This file contains functions to release dynamic shared memory segment
 *    allocated during split workflow.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_shared_memory.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(worker_split_shard_release_dsm);

Datum
worker_split_shard_release_dsm(PG_FUNCTION_ARGS)
{
	ReleaseSharedMemoryOfShardSplitInfo();
	PG_RETURN_VOID();
}
