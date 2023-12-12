#include "postgres.h"


#include "fmgr.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"


#include "access/genam.h"
#include "commands/dbcommands.h"
#include "distributed/argutils.h"
#include "distributed/connection_management.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_database.h"
#include "distributed/remote_commands.h"


static int GroupLookupFromDatabase(int64 databaseOid, bool missingOk);
PG_FUNCTION_INFO_V1(citus_internal_pg_database_size_by_db_name);
PG_FUNCTION_INFO_V1(citus_internal_pg_database_size_by_db_oid);
PG_FUNCTION_INFO_V1(citus_internal_database_size);

/*
 * This function obtains the size of a database given its name,
 * similar to the function pg_database_size(name).
 * However, since we need to override pg_database_size,
 * we create this wrapper function to achieve the same functionality.
 */
Datum
citus_internal_pg_database_size_by_db_name(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "dbName");

	Name dbName = PG_GETARG_NAME(0);
	Datum size = DirectFunctionCall1(pg_database_size_name, NameGetDatum(dbName));

	PG_RETURN_DATUM(size);
}


/*
 * This function obtains the size of a database given its oid,
 * similar to the function pg_database_size(oid).
 * However, since we need to override pg_database_size,
 * we create this wrapper function to achieve the same functionality.
 */
Datum
citus_internal_pg_database_size_by_db_oid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "dbOid");

	Oid dbOid = PG_GETARG_OID(0);
	Datum size = DirectFunctionCall1(pg_database_size_oid, ObjectIdGetDatum(dbOid));

	PG_RETURN_DATUM(size);
}


Datum
citus_internal_database_size(PG_FUNCTION_ARGS)
{
	uint32 connectionFlag = 0;


	PGresult *result = NULL;
	bool failOnError = true;

	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "dbName");

	Name dbName = PG_GETARG_NAME(0);
	elog(INFO, "citus_internal_database_name: %s", dbName->data);

	StringInfo databaseSizeQuery = makeStringInfo();
	appendStringInfo(databaseSizeQuery,
					 "SELECT citus_internal.pg_database_size_local('%s')",
					 dbName->data);

	/*get database oid */
	bool missingOk = true;
	Oid databaseOid = get_database_oid(dbName->data, missingOk);
	elog(INFO, "citus_internal_database_oid: %d", databaseOid);

	/*get group id */
	int groupId = GroupLookupFromDatabase(databaseOid, missingOk);
	if (groupId < 0)
	{
		ereport(ERROR, (errmsg("could not find valid entry for database %d ",
							   databaseOid)));
		PG_RETURN_INT64(-1);
	}
	elog(INFO, "group id: %d", groupId);

	WorkerNode *workerNode = LookupNodeForGroup(groupId);

	char *workerNodeName = workerNode->workerName;
	uint32 workerNodePort = workerNode->workerPort;

	elog(INFO, "workerNodeName: %s", workerNodeName);
	elog(INFO, "workerNodePort: %d", workerNodePort);

	if (groupId == GetLocalGroupId())
	{
		/*local database */
		elog(INFO, "local database");
		PG_RETURN_INT64(DirectFunctionCall1(citus_internal_pg_database_size_by_db_name,
											NameGetDatum(dbName)));
	}
	else
	{
		elog(INFO, "remote database");

		/*remote database */
		MultiConnection *connection = GetNodeConnection(connectionFlag, workerNodeName,
														workerNodePort);
		int queryResult = ExecuteOptionalRemoteCommand(connection,
													   databaseSizeQuery->data,
													   &result);
		int64 size = 0;

		if (queryResult != 0)
		{
			ereport(WARNING, (errcode(ERRCODE_CONNECTION_FAILURE),
							  errmsg("could not connect to %s:%d to get size of "
									 "database \"%s\"",
									 workerNodeName, workerNodePort,
									 dbName->data)));
		}
		else
		{
			size = ParseIntField(result, 0, 0);
		}
		PQclear(result);
		ClearResults(connection, failOnError);
		PG_RETURN_INT64(size);
	}
}


/*
 * Retrieves the groupId of a distributed database
 * using databaseOid from the pg_dist_database table.
 */
static int
GroupLookupFromDatabase(int64 databaseOid, bool missingOk)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Form_pg_dist_database databaseForm = NULL;
	Relation pgDistDatabase = table_open(PgDistDatabaseRelationId(), AccessShareLock);
	int groupId = -1;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_database_databaseid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(databaseOid));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistDatabase,
													InvalidOid, true,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple) && !missingOk)
	{
		ereport(ERROR, (errmsg("could not find valid entry for database "
							   UINT64_FORMAT, databaseOid)));
	}

	if (!HeapTupleIsValid(heapTuple))
	{
		groupId = -2;
	}
	else
	{
		databaseForm = (Form_pg_dist_database) GETSTRUCT(heapTuple);
		groupId = databaseForm->groupid;
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistDatabase, NoLock);

	return groupId;
}
