/*-------------------------------------------------------------------------
 *
 * test/src/connection_cache.c
 *
 * This file contains functions to exercise Citus's connection hash
 * functionality for purposes of unit testing.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "libpq-fe.h"

#include <stddef.h>
#include <string.h>
#include <sys/socket.h>

#include "catalog/pg_type.h"
#include "distributed/connection_cache.h"
#include "distributed/test_helper_functions.h" /* IWYU pragma: keep */
#include "utils/lsyscache.h"


/* local function forward declarations */
static Datum ExtractIntegerDatum(char *input);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(initialize_remote_temp_table);
PG_FUNCTION_INFO_V1(count_remote_temp_table_rows);
PG_FUNCTION_INFO_V1(get_and_purge_connection);
PG_FUNCTION_INFO_V1(set_connection_status_bad);


/*
 * initialize_remote_temp_table connects to a specified host on a specified
 * port and creates a temporary table with 100 rows. Because the table is
 * temporary, it will be visible if a connection is reused but not if a new
 * connection is opened to the node.
 */
Datum
initialize_remote_temp_table(PG_FUNCTION_ARGS)
{
	char *nodeName = PG_GETARG_CSTRING(0);
	int32 nodePort = PG_GETARG_INT32(1);
	PGresult *result = NULL;

	PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		PG_RETURN_BOOL(false);
	}

	result = PQexec(connection, POPULATE_TEMP_TABLE);
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		WarnRemoteError(connection, result);
	}

	PQclear(result);

	PG_RETURN_BOOL(true);
}


/*
 * count_remote_temp_table_rows just returns the integer count of rows in the
 * table created by initialize_remote_temp_table. If no such table exists, this
 * function emits a warning and returns -1.
 */
Datum
count_remote_temp_table_rows(PG_FUNCTION_ARGS)
{
	char *nodeName = PG_GETARG_CSTRING(0);
	int32 nodePort = PG_GETARG_INT32(1);
	Datum count = Int32GetDatum(-1);
	PGresult *result = NULL;

	PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		PG_RETURN_DATUM(count);
	}

	result = PQexec(connection, COUNT_TEMP_TABLE);
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		WarnRemoteError(connection, result);
	}
	else
	{
		char *countText = PQgetvalue(result, 0, 0);
		count = ExtractIntegerDatum(countText);
	}

	PQclear(result);

	PG_RETURN_DATUM(count);
}


/*
 * get_and_purge_connection first gets a connection using the provided hostname
 * and port before immediately passing that connection to PurgeConnection.
 * Simply a wrapper around PurgeConnection that uses hostname/port rather than
 * PGconn.
 */
Datum
get_and_purge_connection(PG_FUNCTION_ARGS)
{
	char *nodeName = PG_GETARG_CSTRING(0);
	int32 nodePort = PG_GETARG_INT32(1);

	PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		PG_RETURN_BOOL(false);
	}

	PurgeConnection(connection);

	PG_RETURN_BOOL(true);
}


/*
 * set_connection_status_bad does not remove the given connection from the connection hash.
 * It simply shuts down the underlying socket. On success, it returns true.
 */
Datum
set_connection_status_bad(PG_FUNCTION_ARGS)
{
	char *nodeName = PG_GETARG_CSTRING(0);
	int32 nodePort = PG_GETARG_INT32(1);
	int socket = -1;
	int shutdownStatus = 0;
	int pqStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		PG_RETURN_BOOL(false);
	}

	/* Prevent further reads/writes... */
	socket = PQsocket(connection);
	shutdownStatus = shutdown(socket, SHUT_RDWR);
	if (shutdownStatus != 0)
	{
		ereport(ERROR, (errcode_for_socket_access(), errmsg("shutdown failed")));
	}

	/* ... and make libpq notice by reading data. */
	pqStatus = PQconsumeInput(connection);

	Assert(pqStatus == 0); /* expect failure */

	PG_RETURN_BOOL(true);
}


/*
 * ExtractIntegerDatum transforms an integer in textual form into a Datum.
 */
static Datum
ExtractIntegerDatum(char *input)
{
	Oid typIoFunc = InvalidOid;
	Oid typIoParam = InvalidOid;
	Datum intDatum = 0;
	FmgrInfo fmgrInfo;
	memset(&fmgrInfo, 0, sizeof(fmgrInfo));

	getTypeInputInfo(INT4OID, &typIoFunc, &typIoParam);
	fmgr_info(typIoFunc, &fmgrInfo);

	intDatum = InputFunctionCall(&fmgrInfo, input, typIoFunc, -1);

	return intDatum;
}
