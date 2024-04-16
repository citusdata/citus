/*-------------------------------------------------------------------------
 *
 * replication_origin_session_utils.c
 *   Functions for managing replication origin session.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "utils/builtins.h"

#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/replication_origin_session_utils.h"

static bool IsRemoteReplicationOriginSessionSetup(MultiConnection *connection);

static void SetupMemoryContextResetReplicationOriginHandler(void);

static void SetupReplicationOriginSessionHelper(bool isContexResetSetupNeeded);

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
bool EnableChangeDataCapture = false;


/* citus_internal_start_replication_origin_tracking starts a new replication origin session
 * in the local node. This function is used to avoid publishing the WAL records to the
 * replication slot by setting replication origin to DoNotReplicateId in WAL records.
 * It remembers the previous replication origin for the current session which will be
 * used to reset the replication origin to the previous value when the session ends.
 */
Datum
citus_internal_start_replication_origin_tracking(PG_FUNCTION_ARGS)
{
	if (!EnableChangeDataCapture)
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


/* IsLocalReplicationOriginSessionActive checks if the current replication origin
 * session is active in the local node.
 */
static inline bool
IsLocalReplicationOriginSessionActive(void)
{
	return (replorigin_session_origin == DoNotReplicateId);
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
	if (!EnableChangeDataCapture)
	{
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
	if (!EnableChangeDataCapture)
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
		StringInfo replicationOriginSessionResetQuery = makeStringInfo();
		appendStringInfo(replicationOriginSessionResetQuery,
						 "select pg_catalog.citus_internal_stop_replication_origin_tracking();");
		ExecuteCriticalRemoteCommand(connection,
									 replicationOriginSessionResetQuery->data);
		connection->isReplicationOriginSessionSetup = false;
	}
}


/*
 * IsRemoteReplicationOriginSessionSetup checks if the replication origin is setup
 * already in the remote session by calliing the UDF
 * citus_internal_is_replication_origin_tracking_active(). This is also remembered
 * in the connection object to avoid calling the UDF again next time.
 */
static bool
IsRemoteReplicationOriginSessionSetup(MultiConnection *connection)
{
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
