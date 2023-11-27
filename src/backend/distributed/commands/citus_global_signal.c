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

#include "signal.h"

#include "lib/stringinfo.h"

#include "pg_version_constants.h"

#include "distributed/backend_data.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"

static bool CitusSignalBackend(uint64 globalPID, uint64 timeout, int sig);

PG_FUNCTION_INFO_V1(citus_cancel_backend);
PG_FUNCTION_INFO_V1(citus_terminate_backend);

/*
 * pg_cancel_backend overrides the Postgres' pg_cancel_backend to cancel
 * a query with a global pid so a query can be cancelled from another node.
 *
 * To cancel a query that is on another node, a pg_cancel_backend command is sent
 * to that node. This new command is sent with pid instead of global pid, so original
 * pg_cancel_backend function is used.
 */
Datum
citus_cancel_backend(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 pid = PG_GETARG_INT64(0);

	int sig = SIGINT;
	uint64 timeout = 0;
	bool success = CitusSignalBackend(pid, timeout, sig);

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
citus_terminate_backend(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 pid = PG_GETARG_INT64(0);
	uint64 timeout = PG_GETARG_INT64(1);

	int sig = SIGTERM;
	bool success = CitusSignalBackend(pid, timeout, sig);

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

	bool missingOk = false;
	int nodeId = ExtractNodeIdFromGlobalPID(globalPID, missingOk);
	int processId = ExtractProcessIdFromGlobalPID(globalPID);

	WorkerNode *workerNode = FindNodeWithNodeId(nodeId, missingOk);

	StringInfo cancelQuery = makeStringInfo();

	if (sig == SIGINT)
	{
		appendStringInfo(cancelQuery, "SELECT pg_cancel_backend(%d::integer)", processId);
	}
	else
	{
		appendStringInfo(cancelQuery,
						 "SELECT pg_terminate_backend(%d::integer, %lu::bigint)",
						 processId, timeout);
	}

	int connectionFlags = 0;
	MultiConnection *connection = GetNodeConnection(connectionFlags,
													workerNode->workerName,
													workerNode->workerPort);

	if (!SendRemoteCommand(connection, cancelQuery->data))
	{
		/* if we cannot connect, we warn and report false */
		ReportConnectionError(connection, WARNING);
		return false;
	}

	bool raiseInterrupts = true;
	PGresult *queryResult = GetRemoteCommandResult(connection, raiseInterrupts);

	/* if remote node throws an error, we also throw an error */
	if (!IsResponseOK(queryResult))
	{
		ReportResultError(connection, queryResult, ERROR);
	}

	StringInfo queryResultString = makeStringInfo();
	bool success = EvaluateSingleQueryResult(connection, queryResult, queryResultString);
	if (success && strcmp(queryResultString->data, "f") == 0)
	{
		/* worker node returned "f" */
		success = false;
	}

	PQclear(queryResult);

	bool raiseErrors = false;
	ClearResults(connection, raiseErrors);

	return success;
}
