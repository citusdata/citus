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

static void SetupMemoryContextResetReplicationOriginHandler(void);

static void SetupReplicationOriginSessionHelper(bool isContexResetSetupNeeded);

static inline bool IsLocalReplicationOriginSessionActive(void);

PG_FUNCTION_INFO_V1(citus_internal_start_replication_origin_tracking);
PG_FUNCTION_INFO_V1(citus_internal_stop_replication_origin_tracking);
PG_FUNCTION_INFO_V1(citus_internal_is_replication_origin_tracking_active);

/*
 * This variable is used to remember the replication origin id of the current session
 * before resetting it to DoNotReplicateId in SetupReplicationOriginLocalSession.
 */
static RepOriginId OriginalOriginId = InvalidRepOriginId;

/*
 * Setting that controls whether replication origin tracking is enabled
 */
bool enable_change_data_capture = false;


/* citus_internal_start_replication_origin_tracking starts a new replication origin session
 * in the local node. This function is used to avoid publishing the WAL records to the
 * replication slot by setting replication origin to DoNotReplicateId in WAL records.
 * It remembers the previous replication origin for the current session which will be
 * used to reset the replication origin to the previous value when the session ends.
 */
Datum
citus_internal_start_replication_origin_tracking(PG_FUNCTION_ARGS)
{
	if (!enable_change_data_capture)
	{
		PG_RETURN_VOID();
	}
	SetupReplicationOriginSessionHelper(false);
	PG_RETURN_VOID();
}


/* citus_internal_stop_replication_origin_tracking ends the current replication origin session
 * in the local node. This function is used to reset the replication origin to the
 * earlier value of replication origin.
 */
Datum
citus_internal_stop_replication_origin_tracking(PG_FUNCTION_ARGS)
{
	ResetReplicationOriginLocalSession();
	PG_RETURN_VOID();
}


/* citus_internal_is_replication_origin_tracking_active checks if the current replication origin
 * session is active in the local node.
 */
Datum
citus_internal_is_replication_origin_tracking_active(PG_FUNCTION_ARGS)
{
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
 * SetupMemoryContextResetReplicationOriginHandler registers a callback function
 * that resets the replication origin session in case of any error for the current
 * memory context.
 */
static void
SetupMemoryContextResetReplicationOriginHandler()
{
	MemoryContextCallback *replicationOriginResetCallback = palloc0(
		sizeof(MemoryContextCallback));
	replicationOriginResetCallback->func =
		ResetReplicationOriginLocalSessionCallbackHandler;
	replicationOriginResetCallback->arg = NULL;
	MemoryContextRegisterResetCallback(CurrentMemoryContext,
									   replicationOriginResetCallback);
}


/*
 * SetupReplicationOriginSessionHelper sets up a new replication origin session in a
 * local session. It takes an argument isContexResetSetupNeeded to decide whether
 * to register a callback function that resets the replication origin session in case
 * of any error for the current memory context.
 */
static void
SetupReplicationOriginSessionHelper(bool isContexResetSetupNeeded)
{
	if (!enable_change_data_capture)
	{
		elog(NOTICE, "change data capture is not enabled");
		return;
	}
	OriginalOriginId = replorigin_session_origin;
	replorigin_session_origin = DoNotReplicateId;
	if (isContexResetSetupNeeded)
	{
		SetupMemoryContextResetReplicationOriginHandler();
	}
}


/*
 * SetupReplicationOriginLocalSession sets up a new replication origin session in a
 * local session.
 */
void
SetupReplicationOriginLocalSession()
{
	SetupReplicationOriginSessionHelper(true);
}


/*
 * ResetReplicationOriginLocalSession resets the replication origin session in a
 * local node.
 */
void
ResetReplicationOriginLocalSession(void)
{
	if (replorigin_session_origin != DoNotReplicateId)
	{
		return;
	}

	replorigin_session_origin = OriginalOriginId;
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
	if (!enable_change_data_capture)
	{
		return;
	}
	if (connection != NULL && !IsRemoteReplicationOriginSessionSetup(connection))
	{
		StringInfo replicationOriginSessionSetupQuery = makeStringInfo();
		appendStringInfo(replicationOriginSessionSetupQuery,
						 "select pg_catalog.citus_internal_start_replication_origin_tracking();");
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
	if (connection != NULL && connection->isReplicationOriginSessionSetup)
	{
		/*elog(LOG, "Resetting remote replication origin session %s,%d", connection->hostname, connection->port); */
		StringInfo replicationOriginSessionResetQuery = makeStringInfo();
		appendStringInfo(replicationOriginSessionResetQuery,
						 "select pg_catalog.citus_internal_stop_replication_origin_tracking();");
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
					 "SELECT pg_catalog.citus_internal_is_replication_origin_tracking_active()");
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
