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

static bool isReplicationOriginSessionSetup(MultiConnection *connection);
static bool isReplicationOriginCreated(MultiConnection *connection, char *originName,
									   RepOriginId *originId);
static RepOriginId CreateReplicationOriginSession(MultiConnection *connection,
												  char *originName);
static void ReplicationOriginSessionSetupHelper(MultiConnection *connection,
												char *originName);
static bool ExecuteRemoteCommandAndCheckResult(MultiConnection *connection, char *command,
											   char *expected);

/* SetupReplicationOriginRemoteSession sets up a new replication origin session in a
 * remote session. The identifier is used to create a unique replication origin name
 * for the session in the remote node.
 */
void
SetupReplicationOriginRemoteSession(MultiConnection *connection, char *identifier)
{
	if (!isReplicationOriginSessionSetup(connection))
	{
		StringInfo originNameString = makeStringInfo();
		appendStringInfo(originNameString, "citus_internal_%d_%s", GetLocalNodeId(),
						 identifier);
		char *originName = quote_literal_cstr(originNameString->data);
		ReplicationOriginSessionSetupHelper(connection, originName);
	}
}


/* ResetReplicationOriginRemoteSession resets the replication origin session in a
 * remote node.
 */
void
ResetReplicationOriginRemoteSession(MultiConnection *connection, char *identifier)
{
	/* Reset the replication origin in remote session*/
	StringInfo replicationOriginSessionResetQuery = makeStringInfo();
	appendStringInfo(replicationOriginSessionResetQuery,
					 "select pg_catalog.pg_replication_origin_session_reset();");
	ExecuteCriticalRemoteCommand(connection,
								 replicationOriginSessionResetQuery->data);

	/* Drop the replication origin entry created in remote session.*/
	StringInfo originNameString = makeStringInfo();
	appendStringInfo(originNameString, "citus_internal_%d_%s", GetLocalNodeId(),
					 identifier);
	StringInfo replicationOriginSessionDropQuery = makeStringInfo();
	appendStringInfo(replicationOriginSessionDropQuery,
					 "select pg_catalog.pg_replication_origin_drop(%s);",
					 quote_literal_cstr(originNameString->data));

	ExecuteCriticalRemoteCommand(connection,
								 replicationOriginSessionDropQuery->data);
}


/* SetupReplicationOriginLocalSession sets up a new replication origin session in a
 * local session.
 */
void
SetupReplicationOriginLocalSession(void)
{
	if (!isReplicationOriginSessionSetup(NULL))
	{
		StringInfo originNameString = makeStringInfo();
		appendStringInfo(originNameString, "citus_internal_%d", GetLocalNodeId());
		ReplicationOriginSessionSetupHelper(NULL, originNameString->data);
	}
}


/* ResetReplicationOriginLocalSession resets the replication origin session in a
 * local node.
 */
void
ResetReplicationOriginLocalSession(void)
{
	/*Reset Replication Origin in local session */
	if (replorigin_session_origin != InvalidRepOriginId)
	{
		replorigin_session_reset();
		replorigin_session_origin = InvalidRepOriginId;
	}
}


/* isReplicationOriginSessionSetup checks if the replication origin is setup
 * already in the local or remote session.
 */
static bool
isReplicationOriginSessionSetup(MultiConnection *connection)
{
	bool result = false;
	if (connection == NULL)
	{
		result = (replorigin_session_origin != InvalidRepOriginId);
	}
	else
	{
		/*Setup Replication Origin in remote session */
		StringInfo isReplicationOriginSessionSetupQuery = makeStringInfo();
		appendStringInfo(isReplicationOriginSessionSetupQuery,
						 "SELECT pg_catalog.pg_replication_origin_session_is_setup()");
		result =
			ExecuteRemoteCommandAndCheckResult(connection,
											   isReplicationOriginSessionSetupQuery->data,
											   "t");
	}
	return result;
}


/* isReplicationOriginCreated checks if the replication origin is created
 * in the local or remote session.*/
static bool
isReplicationOriginCreated(MultiConnection *connection, char *originName,
						   RepOriginId *originId)
{
	bool result = false;
	if (connection == NULL)
	{
		*originId = replorigin_by_name(originName, true);
		result = (*originId != InvalidRepOriginId);
	}
	else
	{
		/*Setup Replication Origin in remote session */
		StringInfo isReplicationOriginSessionSetupQuery = makeStringInfo();
		appendStringInfo(isReplicationOriginSessionSetupQuery,
						 "SELECT pg_catalog.pg_replication_origin_oid(%s);",
						 originName);

		/* If the replication origin was already created the above command
		 * will return the id of the entry in pg_replication_origin table.
		 * So to check if the entry is there alreay, the retuen value should
		 * be non-empty and the condition below checks that. */
		result = !ExecuteRemoteCommandAndCheckResult(connection,
													 isReplicationOriginSessionSetupQuery
													 ->data, "");
	}
	return result;
}


/* CreateReplicationOriginSession creates a new replication origin if it does
 * not already exist already. To make the replication origin name unique
 * for different nodes, origin node's id is appended to the prefix citus_internal_.*/
static RepOriginId
CreateReplicationOriginSession(MultiConnection *connection, char *originName)
{
	RepOriginId originId = InvalidRepOriginId;
	if (connection == NULL)
	{
		originId = replorigin_create(originName);
	}
	else
	{
		StringInfo replicationOriginCreateQuery = makeStringInfo();
		appendStringInfo(replicationOriginCreateQuery,
						 "select pg_catalog.pg_replication_origin_create(%s);",
						 originName);
		ExecuteCriticalRemoteCommand(connection, replicationOriginCreateQuery->data);
	}
	return originId;
}


/* ReplicationOriginSessionSetupHelper sets up a new replication origin session in a
 * local or remote session.
 */
static void
ReplicationOriginSessionSetupHelper(MultiConnection *connection,
									char *originName)
{
	RepOriginId originId = InvalidRepOriginId;
	if (!isReplicationOriginCreated(connection, originName, &originId))
	{
		originId = CreateReplicationOriginSession(connection, originName);
	}

	if (connection == NULL)
	{
		/*Setup Replication Origin in local session */
		replorigin_session_setup(originId);
		replorigin_session_origin = originId;
	}
	else
	{
		/*Setup Replication Origin in remote session */
		StringInfo replicationOriginSessionSetupQuery = makeStringInfo();
		appendStringInfo(replicationOriginSessionSetupQuery,
						 "select pg_catalog.pg_replication_origin_session_setup(%s);",
						 originName);
		ExecuteCriticalRemoteCommand(connection,
									 replicationOriginSessionSetupQuery->data);
	}
}


/* ExecuteRemoteCommandAndCheckResult executes the given command in the remote node and
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
	bool raiseErrors = false;
	ClearResults(connection, raiseErrors);

	return result;
}
