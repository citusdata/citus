/*-------------------------------------------------------------------------
 *
 * worker_create_truncate_trigger_protocol.c
 *
 * Routines for creating truncate triggers on distributed tables on worker nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"


PG_FUNCTION_INFO_V1(worker_create_truncate_trigger);


/*
 * worker_create_truncate_trigger creates a truncate trigger for the given distributed
 * table on current metadata worker. The function is intended to be called by the
 * coordinator node during metadata propagation of mx tables or during the upgrades from
 * citus version <=5.2 to >=6.1. The function requires superuser permissions.
 */
Datum
worker_create_truncate_trigger(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	EnsureSuperUser();
	CheckCitusVersion(ERROR);

	/* Create the truncate trigger */
	CreateTruncateTrigger(relationId);

	PG_RETURN_VOID();
}
