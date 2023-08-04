/*-------------------------------------------------------------------------
 *
 * database_lock.c
 *    Functions for locking a database.
 *
 * Copyright (c) Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "distributed/metadata_cache.h"
#include "storage/lmgr.h"


static void CitusDatabaseLock(Oid databaseId);


PG_FUNCTION_INFO_V1(citus_database_lock_by_name);


/*
 * citus_database_lock locks the given database in access exclusive mode
 * to temporarily block new connections.
 */
Datum
citus_database_lock_by_name(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Name databaseName = PG_GETARG_NAME(0);
	bool missingOk = false;
	Oid databaseId = get_database_oid(NameStr(*databaseName), missingOk);

	CitusDatabaseLock(databaseId);

	PG_RETURN_VOID();
}


/*
 * CitusDatabaseLock locks a database for new connections.
 */
static void
CitusDatabaseLock(Oid databaseId)
{
	LockSharedObject(DatabaseRelationId, databaseId, 0, ExclusiveLock);
}
