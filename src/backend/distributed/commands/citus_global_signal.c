/*-------------------------------------------------------------------------
 *
 * citus_global_signal.c
 *    Commands for Citus' overriden versions of pg_cancel_backend
 *    and pg_terminate_backend statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "distributed/backend_data.h"
#include "distributed/metadata_cache.h"
#include "distributed/worker_manager.h"
#include "lib/stringinfo.h"
#include "signal.h"

static bool CitusSignalBackend(uint64 globalPID, uint64 timeout, int sig);

PG_FUNCTION_INFO_V1(pg_cancel_backend);
PG_FUNCTION_INFO_V1(pg_terminate_backend);

/*
 * pg_cancel_backend overrides the Postgres' pg_cancel_backend to cancel
 * a query with a global pid so a query can be cancelled from another node.
 *
 * To cancel a query that is on another node, a pg_cancel_backend command is sent
 * to that node. This new command is sent with pid instead of global pid, so original
 * pg_cancel_backend function is used.
 */
Datum
pg_cancel_backend(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 globalPID = PG_GETARG_INT64(0);

	int sig = SIGINT;
	uint64 timeout = 0;
	bool success = CitusSignalBackend(globalPID, timeout, sig);

	PG_RETURN_BOOL(success);
}


/*
 * pg_terminate_backend overrides the Postgres' pg_terminate_backend to terminate
 * a query with a global pid so a query can be terminated from another node.
 *
 * To terminate a query that is on another node, a pg_terminate_backend command is sent
 * to that node. This new command is sent with pid instead of global pid, so original
 * pg_terminate_backend function is used.
 */
Datum
pg_terminate_backend(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 globalPID = PG_GETARG_INT64(0);
	uint64 timeout = PG_GETARG_INT64(1);

	int sig = SIGTERM;
	bool success = CitusSignalBackend(globalPID, timeout, sig);

	PG_RETURN_BOOL(success);
}


/*
 * CitusSignalBackend gets a global pid and and ends the original query with the global pid
 * that might have started in another node by connecting to that node and running either
 * pg_cancel_backend or pg_terminate_backend based on the withTerminate argument.
 */
static bool
CitusSignalBackend(uint64 globalPID, uint64 timeout, int sig)
{
	Assert((sig == SIGINT) || (sig == SIGTERM));

#if PG_VERSION_NUM < PG_VERSION_14
	if (timeout != 0)
	{
		elog(ERROR, "timeout parameter is only supported on Postgres 14 or later");
	}
#endif

	int nodeId = ExtractNodeIdFromGlobalPID(globalPID);
	int processId = ExtractProcessIdFromGlobalPID(globalPID);

	WorkerNode *workerNode = FindNodeWithNodeId(nodeId);

	StringInfo cancelQuery = makeStringInfo();

	if (sig == SIGINT)
	{
		appendStringInfo(cancelQuery, "SELECT pg_cancel_backend(%d::integer)", processId);
	}
	else
	{
#if PG_VERSION_NUM >= PG_VERSION_14
		appendStringInfo(cancelQuery,
						 "SELECT pg_terminate_backend(%d::integer, %lu::bigint)",
						 processId, timeout);
#else
		appendStringInfo(cancelQuery, "SELECT pg_terminate_backend(%d::integer)",
						 processId);
#endif
	}

	StringInfo queryResult = makeStringInfo();

	bool reportResultError = true;

	bool success = ExecuteRemoteQueryOrCommand(workerNode->workerName,
											   workerNode->workerPort, cancelQuery->data,
											   queryResult, reportResultError);

	if (success && queryResult && strcmp(queryResult->data, "f") == 0)
	{
		success = false;
	}

	return success;
}
