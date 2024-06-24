/*-------------------------------------------------------------------------
 *
 * test/src/make_external_connection.c
 *
 * This file contains UDF to connect to a node without using the Citus
 * internal application_name.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/function_utils.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/run_from_same_connection.h"
#include "distributed/version_compat.h"


PG_FUNCTION_INFO_V1(make_external_connection_to_node);


/*
 * make_external_connection_to_node opens a conneciton to a node
 * and keeps it until the end of the session.
 */
Datum
make_external_connection_to_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	char *nodeName = text_to_cstring(PG_GETARG_TEXT_P(0));
	uint32 nodePort = PG_GETARG_UINT32(1);
	char *userName = text_to_cstring(PG_GETARG_TEXT_P(2));
	char *databaseName = text_to_cstring(PG_GETARG_TEXT_P(3));

	StringInfo connectionString = makeStringInfo();
	appendStringInfo(connectionString,
					 "host=%s port=%d user=%s dbname=%s",
					 nodeName, nodePort, userName, databaseName);

	PGconn *pgConn = PQconnectdb(connectionString->data);

	if (PQstatus(pgConn) != CONNECTION_OK)
	{
		PQfinish(pgConn);

		ereport(ERROR, (errmsg("connection failed")));
	}

	PG_RETURN_VOID();
}
