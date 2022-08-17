/*-------------------------------------------------------------------------
 *
 * worker_cleanup_artifact_udf.c
 *    This file contains functions to clean up artifacts and metadata.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/listutils.h"
#include "distributed/remote_commands.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "commands/dbcommands.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(worker_cleanup_artifacts);

Datum
worker_cleanup_artifacts(PG_FUNCTION_ARGS)
{
    char *first = PG_GETARG_CSTRING(0);
    printf("foobar %s\n", first);
	PG_RETURN_VOID();
}