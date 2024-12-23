/*-------------------------------------------------------------------------
 *
 * test/src/run_from_same_connection.c
 *
 * This file contains UDF to run consecutive commands on worker node from the
 * same connection. UDFs will be used to test MX functionalities in isolation
 * tests.
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


#define ALTER_CURRENT_PROCESS_ID \
	"ALTER SYSTEM SET citus.isolation_test_session_process_id TO %d"
#define ALTER_CURRENT_WORKER_PROCESS_ID \
	"ALTER SYSTEM SET citus.isolation_test_session_remote_process_id TO %ld"
#define GET_PROCESS_ID "SELECT process_id FROM get_current_transaction_id()"


static bool allowNonIdleRemoteTransactionOnXactHandling = false;
static MultiConnection *singleConnection = NULL;


/*
 * Config variables which will be used by isolation framework to check transactions
 * initiated from worker nodes.
 */
int IsolationTestSessionRemoteProcessID = -1;
int IsolationTestSessionProcessID = -1;


static int64 GetRemoteProcessId(void);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(start_session_level_connection_to_node);
PG_FUNCTION_INFO_V1(run_commands_on_session_level_connection_to_node);
PG_FUNCTION_INFO_V1(stop_session_level_connection_to_node);
PG_FUNCTION_INFO_V1(override_backend_data_gpid);


/*
 * AllowNonIdleTransactionOnXactHandling allows connection opened with
 * SESSION_LIFESPAN remain opened even if it is not idle.
 */
bool
AllowNonIdleTransactionOnXactHandling(void)
{
	return allowNonIdleRemoteTransactionOnXactHandling;
}


/*
 * start_session_level_connection_to_node helps us to open and keep connections
 * open while sending consecutive commands, even if they are outside the transaction.
 * To use the connection opened with an open transaction, we have implemented a hacky
 * solution by setting a static flag, allowNonIdleRemoteTransactionOnXactHandling, on
 * this file to true. That gives us to chance to keep that connection open.
 *
 * Note that, this UDF shouldn't be used outside the isolation tests.
 */
Datum
start_session_level_connection_to_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	uint32 nodePort = PG_GETARG_UINT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	int connectionFlags = 0;

	if (singleConnection != NULL && (strcmp(singleConnection->hostname,
											nodeNameString) != 0 ||
									 singleConnection->port != nodePort))
	{
		elog(ERROR,
			 "can not connect different worker nodes from the same session using start_session_level_connection_to_node");
	}

	/*
	 * In order to keep connection open even with an open transaction,
	 * allowSessionLifeSpanWithOpenTransaction is set to true.
	 */
	if (singleConnection == NULL)
	{
		singleConnection = GetNodeConnection(connectionFlags, nodeNameString, nodePort);
		allowNonIdleRemoteTransactionOnXactHandling = true;
	}

	if (PQstatus(singleConnection->pgConn) != CONNECTION_OK)
	{
		elog(ERROR, "failed to connect to %s:%d", nodeNameString, (int) nodePort);
	}

	/* pretend we are a regular client to avoid citus-initiated backend checks */
	const char *setAppName =
		"SET application_name TO run_commands_on_session_level_connection_to_node";

	ExecuteCriticalRemoteCommand(singleConnection, setAppName);

	/*
	 * We are hackily overriding the remote processes' gpid value such that
	 * blocked process detection continues to work.
	 */
	StringInfo overrideBackendDataCommandOriginator = makeStringInfo();
	appendStringInfo(overrideBackendDataCommandOriginator,
					 "SELECT override_backend_data_gpid(%lu);",
					 GetGlobalPID());
	ExecuteCriticalRemoteCommand(singleConnection,
								 overrideBackendDataCommandOriginator->data);


	PG_RETURN_VOID();
}


/*
 * run_commands_on_session_level_connection_to_node runs to consecutive commands
 * from the same connection opened by start_session_level_connection_to_node.
 *
 * Since transactions can be initiated from worker nodes with MX, we need to
 * keep them open on the worker node to check whether there exist a waiting
 * transaction in test steps. In order to release the locks taken in the
 * transaction we need to send related unlock commands from the same connection
 * as well.
 */
Datum
run_commands_on_session_level_connection_to_node(PG_FUNCTION_ARGS)
{
	text *queryText = PG_GETARG_TEXT_P(0);
	char *queryString = text_to_cstring(queryText);

	StringInfo processStringInfo = makeStringInfo();
	StringInfo workerProcessStringInfo = makeStringInfo();
	MultiConnection *localConnection = GetNodeConnection(0, LOCAL_HOST_NAME,
														 PostPortNumber);

	if (!singleConnection)
	{
		elog(ERROR,
			 "start_session_level_connection_to_node must be called first to open a session level connection");
	}

	appendStringInfo(processStringInfo, ALTER_CURRENT_PROCESS_ID, MyProcPid);
	appendStringInfo(workerProcessStringInfo, ALTER_CURRENT_WORKER_PROCESS_ID,
					 GetRemoteProcessId());

	ExecuteCriticalRemoteCommand(singleConnection, queryString);

	/*
	 * Since we cannot run `ALTER SYSTEM` command within a transaction, we are
	 * calling it from a self-connected session.
	 */
	ExecuteCriticalRemoteCommand(localConnection, processStringInfo->data);
	ExecuteCriticalRemoteCommand(localConnection, workerProcessStringInfo->data);

	CloseConnection(localConnection);

	/* Call pg_reload_conf UDF to update changed GUCs above on each backend */
	Oid pgReloadConfOid = FunctionOid("pg_catalog", "pg_reload_conf", 0);
	OidFunctionCall0(pgReloadConfOid);


	PG_RETURN_VOID();
}


/*
 * override_backend_data_gpid is a wrapper around SetBackendDataGpid().
 * Also sets distributedCommandOriginator to true since the only caller of
 * this method calls this function actually wants this backend to
 * be treated as a distributed command originator with the given global pid.
 */
Datum
override_backend_data_gpid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 gpid = PG_GETARG_INT64(0);

	SetBackendDataGlobalPID(gpid);
	SetBackendDataDistributedCommandOriginator(true);

	PG_RETURN_VOID();
}


/*
 * stop_session_level_connection_to_node closes the connection opened by the
 * start_session_level_connection_to_node and set the flag to false which
 * allows connection API to keep connections with open transaction.
 */
Datum
stop_session_level_connection_to_node(PG_FUNCTION_ARGS)
{
	allowNonIdleRemoteTransactionOnXactHandling = false;

	if (singleConnection != NULL)
	{
		CloseConnection(singleConnection);
		singleConnection = NULL;
	}

	PG_RETURN_VOID();
}


/*
 * GetRemoteProcessId() get the process id of remote transaction opened
 * by the connection.
 */
static int64
GetRemoteProcessId()
{
	StringInfo queryStringInfo = makeStringInfo();
	PGresult *result = NULL;

	appendStringInfo(queryStringInfo, GET_PROCESS_ID);

	int queryResult = ExecuteOptionalRemoteCommand(singleConnection,
												   queryStringInfo->data, &result);
	if (queryResult != RESPONSE_OKAY)
	{
		PG_RETURN_VOID();
	}

	int64 rowCount = PQntuples(result);
	if (rowCount != 1)
	{
		PG_RETURN_VOID();
	}

	int64 resultValue = ParseIntField(result, 0, 0);

	PQclear(result);
	ClearResults(singleConnection, false);

	return resultValue;
}
