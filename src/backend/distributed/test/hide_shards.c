/*-------------------------------------------------------------------------
 *
 * hide_shards.c
 *
 * This file contains functions to provide helper UDFs for hiding
 * shards from the applications.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "distributed/metadata_utility.h"
#include "distributed/worker_shard_visibility.h"


PG_FUNCTION_INFO_V1(set_backend_type);

/*
 * set_backend_type is an external API to set the MyBackendType and
 * re-checks the shard visibility.
 */
Datum
set_backend_type(PG_FUNCTION_ARGS)
{
	EnsureSuperUser();

	MyBackendType = PG_GETARG_INT32(0);

	elog(NOTICE, "backend type switched to: %s",
		 GetBackendTypeDesc(MyBackendType));

	ResetHideShardsDecision();

	PG_RETURN_VOID();
}
