/*-------------------------------------------------------------------------
 *
 * test/src/invalidate_inactive_shared_connections.c
 *
 * This file contains functions to invalidate inactive shared connections.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "distributed/shared_connection_stats.h"
#include "distributed/listutils.h"
#include "nodes/parsenodes.h"
#include "utils/guc.h"

PG_FUNCTION_INFO_V1(invalidate_inactive_shared_connections);

/*
 * invalidate_inactive_shared_connections invalidates inactive
 * and not used shared connections by removing from the global hash.
 */
Datum
invalidate_inactive_shared_connections(PG_FUNCTION_ARGS)
{
	RemoveInactiveNodesFromSharedConnections();
	PG_RETURN_VOID();
}
