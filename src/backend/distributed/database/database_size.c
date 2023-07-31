/*-------------------------------------------------------------------------
 *
 * database_size.c
 *    Functions for getting the size of a database.
 *
 * Copyright (c) Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/database/database_sharding.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_transaction.h"
#include "utils/builtins.h"


static int64 GetLocalDatabaseSize(Oid databaseId);
static int64 CitusDatabaseShardSize(DatabaseShard *dbShard);
static int64 CitusDatabaseSizeOnNodeList(Oid databaseId, List *workerNodeList);


PG_FUNCTION_INFO_V1(citus_database_size_oid);
PG_FUNCTION_INFO_V1(citus_database_size_name);


/*
 * citus_database_size_oid returns the size of a Citus database
 * with the given oid.
 */
Datum
citus_database_size_oid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid databaseId = PG_GETARG_OID(0);
	int64 size = CitusDatabaseSize(databaseId);

	PG_RETURN_INT64(size);
}


/*
 * citus_database_size_name returns the size of a Citus database
 * with the given name.
 */
Datum
citus_database_size_name(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Name databaseName = PG_GETARG_NAME(0);
	bool missingOk = false;
	Oid databaseId = get_database_oid(NameStr(*databaseName), missingOk);

	int64 size = CitusDatabaseSize(databaseId);

	PG_RETURN_INT64(size);
}


/*
 * CitusDatabaseSize returns the size of a Citus database.
 */
int64
CitusDatabaseSize(Oid databaseId)
{
	DatabaseShard *dbShard = GetDatabaseShardByOid(databaseId);
	if (dbShard != NULL)
	{
		/* for known database shards, get the remote size */
		return CitusDatabaseShardSize(dbShard);
	}

	if (databaseId == MyDatabaseId)
	{
		/* for the current database, get the size from all nodes */
		List *workerNodes = TargetWorkerSetNodeList(ALL_SHARD_NODES, RowShareLock);
		return CitusDatabaseSizeOnNodeList(databaseId, workerNodes);
	}

	/* for other databases, get the local size */
	/* TODO: get it from main database? */
	return GetLocalDatabaseSize(databaseId);
}


/*
 * GetLocalDatabaseSize returns the local database size by calling pg_database_size.
 */
static int64
GetLocalDatabaseSize(Oid databaseId)
{
	Datum databaseIdDatum = ObjectIdGetDatum(databaseId);
	Datum sizeDatum = DirectFunctionCall1(pg_database_size_oid, databaseIdDatum);
	return DatumGetInt64(sizeDatum);
}


/*
 * CitusDatabaseShardSize gets the database size for a specific
 * shard.
 */
static int64
CitusDatabaseShardSize(DatabaseShard *dbShard)
{
	WorkerNode *workerNode = LookupNodeForGroup(dbShard->nodeGroupId);

	return CitusDatabaseSizeOnNodeList(dbShard->databaseOid, list_make1(workerNode));
}


/*
 * CitusDatabaseSizeOnNodeList returns the sum of the sizes
 * for a given database from all nodes in the list.
 */
static int64
CitusDatabaseSizeOnNodeList(Oid databaseId, List *workerNodeList)
{
	int64 size = 0;

	bool raiseInterrupts = true;

	char *databaseName = get_database_name(databaseId);
	char *command = psprintf("SELECT pg_catalog.pg_database_size(%s)",
							 quote_literal_cstr(databaseName));

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		if (workerNode->groupId == GetLocalGroupId())
		{
			return GetLocalDatabaseSize(databaseId);
		}

		int connectionFlags = 0;
		MultiConnection *connection = GetNodeConnection(connectionFlags,
														workerNode->workerName,
														workerNode->workerPort);

		int querySent = SendRemoteCommand(connection, command);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		if (PQntuples(result) != 1 || PQnfields(result) != 1)
		{
			PQclear(result);
			ClearResults(connection, raiseInterrupts);

			ereport(ERROR, (errmsg("unexpected number of columns returned by: %s",
								   command)));
		}

		if (!PQgetisnull(result, 0, 0))
		{
			char *sizeString = PQgetvalue(result, 0, 0);
			size += SafeStringToUint64(sizeString);
		}

		PQclear(result);
		ClearResults(connection, raiseInterrupts);
	}

	return size;
}
