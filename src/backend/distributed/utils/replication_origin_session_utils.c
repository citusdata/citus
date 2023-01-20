/*-------------------------------------------------------------------------
 *
 * replication_origin_session_utils.c
 *   Functions for managing replication origin session.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "distributed/replication_origin_session_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/metadata_cache.h"
#include "utils/builtins.h"
#include "miscadmin.h"

static bool IsRemoteReplicationOriginSessionSetup(MultiConnection *connection);

static bool ExecuteRemoteCommandAndCheckResult(MultiConnection *connection, char *command,
											   char *expected);

static inline bool IsLocalReplicationOriginSessionActive(void);

PG_FUNCTION_INFO_V1(replication_origin_session_start_no_publish);
PG_FUNCTION_INFO_V1(replication_origin_session_end_no_publish);
PG_FUNCTION_INFO_V1(replication_origin_session_is_no_publish);

/*
 * This variable is used to remember the replication origin id of the current session
 * before resetting it to DoNotReplicateId in SetupReplicationOriginLocalSession.
 */
static RepOriginId originalOriginId = InvalidRepOriginId;

/*
 * Setting that controls whether replication origin sessions are enabled.
 */
bool isReplicationOriginSessionFeatureEnabled = false;


/* replication_origin_session_start_no_publish starts a new replication origin session
 * in the local node. This function is used to avoid publishing the WAL records to the
 * replication slot by setting replication origin to DoNotReplicateId in WAL records.
 * It remembers the previous replication origin for the current session which will be
 * used to reset the replication origin to the previous value when the session ends.
 */
Datum
replication_origin_session_start_no_publish(PG_FUNCTION_ARGS)
{
	if (!isReplicationOriginSessionFeatureEnabled)
	{
		PG_RETURN_VOID();
	}
	SetupReplicationOriginLocalSession();
	PG_RETURN_VOID();
}


/* replication_origin_session_end_no_publish ends the current replication origin session
 * in the local node. This function is used to reset the replication origin to the
 * earlier value of replication origin.
 */
Datum
replication_origin_session_end_no_publish(PG_FUNCTION_ARGS)
{
	if (!isReplicationOriginSessionFeatureEnabled)
	{
		PG_RETURN_VOID();
	}
	ResetReplicationOriginLocalSession();
	PG_RETURN_VOID();
}


/* replication_origin_session_is_no_publish checks if the current replication origin
 * session is active in the local node.
 */
Datum
replication_origin_session_is_no_publish(PG_FUNCTION_ARGS)
{
	if (!isReplicationOriginSessionFeatureEnabled)
	{
		PG_RETURN_BOOL(false);
	}
	bool result = IsLocalReplicationOriginSessionActive();
	PG_RETURN_BOOL(result);
}


/* IsLocalReplicationOriginSessionActive checks if the current replication origin
 * session is active in the local node.
 */
inline bool
IsLocalReplicationOriginSessionActive(void)
{
	return (replorigin_session_origin != InvalidRepOriginId);
}


/*
 * SetupReplicationOriginLocalSession sets up a new replication origin session in a
 * local session.
 */
void
SetupReplicationOriginLocalSession(void)
{
	if (!isReplicationOriginSessionFeatureEnabled)
	{
		return;
	}

	/*elog(LOG, "Setting up local replication origin session"); */
	if (!IsLocalReplicationOriginSessionActive())
	{
		originalOriginId = replorigin_session_origin;
		replorigin_session_origin = DoNotReplicateId;

		/* Register a call back for ResetReplicationOriginLocalSession function for error cases */
		MemoryContextCallback *replicationOriginResetCallback = palloc0(
			sizeof(MemoryContextCallback));
		replicationOriginResetCallback->func =
			ResetReplicationOriginLocalSessionCallbackHandler;
		replicationOriginResetCallback->arg = NULL;
		MemoryContextRegisterResetCallback(CurrentMemoryContext,
										   replicationOriginResetCallback);
	}
}


/*
 * ResetReplicationOriginLocalSession resets the replication origin session in a
 * local node.
 */
void
ResetReplicationOriginLocalSession(void)
{
	/*elog(LOG, "Resetting local replication origin session"); */
	if (!isReplicationOriginSessionFeatureEnabled)
	{
		return;
	}

	if (IsLocalReplicationOriginSessionActive())
	{
		replorigin_session_origin = originalOriginId;
	}
}


/*
 * ResetReplicationOriginLocalSessionCallbackHandler is a callback function that
 * resets the replication origin session in a local node. This is used to register
 * with MemoryContextRegisterResetCallback to reset the replication origin session
 * in case of any error for the given memory context.
 */
void
ResetReplicationOriginLocalSessionCallbackHandler(void *arg)
{
	ResetReplicationOriginLocalSession();
}


/*
 * SetupReplicationOriginRemoteSession sets up a new replication origin session in a
 * remote session. The identifier is used to create a unique replication origin name
 * for the session in the remote node.
 */
void
SetupReplicationOriginRemoteSession(MultiConnection *connection)
{
	if (!isReplicationOriginSessionFeatureEnabled)
	{
		return;
	}
	if (connection != NULL && !IsRemoteReplicationOriginSessionSetup(connection))
	{
		/*elog(LOG, "After IsReplicationOriginSessionSetup session %s,%d", connection->hostname, connection->port); */
		StringInfo replicationOriginSessionSetupQuery = makeStringInfo();
		appendStringInfo(replicationOriginSessionSetupQuery,
						 "select pg_catalog.replication_origin_session_start_no_publish();");
		ExecuteCriticalRemoteCommand(connection,
									 replicationOriginSessionSetupQuery->data);
		connection->isReplicationOriginSessionSetup = true;
	}
}


/*
 * ResetReplicationOriginRemoteSession resets the replication origin session in a
 * remote node.
 */
void
ResetReplicationOriginRemoteSession(MultiConnection *connection)
{
	if (!isReplicationOriginSessionFeatureEnabled)
	{
		return;
	}
	if (connection != NULL && connection->isReplicationOriginSessionSetup)
	{
		/*elog(LOG, "Resetting remote replication origin session %s,%d", connection->hostname, connection->port); */
		StringInfo replicationOriginSessionResetQuery = makeStringInfo();
		appendStringInfo(replicationOriginSessionResetQuery,
						 "select pg_catalog.replication_origin_session_end_no_publish();");
		ExecuteCriticalRemoteCommand(connection,
									 replicationOriginSessionResetQuery->data);
		connection->isReplicationOriginSessionSetup = false;
	}
}


/*
 * IsRemoteReplicationOriginSessionSetup(MultiConnection *connection) checks if the replication origin is setup
 * already in the local or remote session.
 */
static bool
IsRemoteReplicationOriginSessionSetup(MultiConnection *connection)
{
	/*elog(LOG, "IsReplicationOriginSessionSetup: %s,%d", connection->hostname, connection->port); */
	if (connection->isReplicationOriginSessionSetup)
	{
		return true;
	}

	StringInfo isReplicationOriginSessionSetupQuery = makeStringInfo();
	appendStringInfo(isReplicationOriginSessionSetupQuery,
					 "SELECT pg_catalog.replication_origin_session_is_no_publish()");
	bool result =
		ExecuteRemoteCommandAndCheckResult(connection,
										   isReplicationOriginSessionSetupQuery->data,
										   "t");

	connection->isReplicationOriginSessionSetup = result;
	return result;
}


/*
 * ExecuteRemoteCommandAndCheckResult executes the given command in the remote node and
 * checks if the result is equal to the expected result. If the result is equal to the
 * expected result, the function returns true, otherwise it returns false.
 */
static bool
ExecuteRemoteCommandAndCheckResult(MultiConnection *connection, char *command,
								   char *expected)
{
	if (!SendRemoteCommand(connection, command))
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

	/* Evaluate the queryResult and store it into the queryResultString */
	bool success = EvaluateSingleQueryResult(connection, queryResult, queryResultString);
	bool result = false;
	if (success && strcmp(queryResultString->data, expected) == 0)
	{
		result = true;
	}

	PQclear(queryResult);
	ForgetResults(connection);

	return result;
}
