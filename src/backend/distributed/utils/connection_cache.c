/*-------------------------------------------------------------------------
 *
 * connection_cache.c
 *
 * Legacy connection caching layer. Will be removed entirely.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h" /* IWYU pragma: keep */
#include "c.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "commands/dbcommands.h"
#include "distributed/connection_management.h"
#include "distributed/connection_cache.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


/* local function forward declarations */
static void ReportRemoteError(PGconn *connection, PGresult *result, bool raiseError);


/*
 * GetOrEstablishConnection returns a PGconn which can be used to execute
 * queries on a remote PostgreSQL server. If no suitable connection to the
 * specified node on the specified port yet exists, the function establishes
 * a new connection and adds it to the connection cache before returning it.
 *
 * Returned connections are guaranteed to be in the CONNECTION_OK state. If the
 * requested connection cannot be established, or if it was previously created
 * but is now in an unrecoverable bad state, this function returns NULL.
 *
 * This function throws an error if a hostname over 255 characters is provided.
 */
PGconn *
GetOrEstablishConnection(char *nodeName, int32 nodePort)
{
	int connectionFlags = SESSION_LIFESPAN;
	PGconn *connection = NULL;
	MultiConnection *multiConnection =
		GetNodeConnection(connectionFlags, nodeName, nodePort);

	if (PQstatus(multiConnection->pgConn) == CONNECTION_OK)
	{
		connection = multiConnection->pgConn;
	}
	else
	{
		ReportConnectionError(multiConnection, WARNING);
		CloseConnection(multiConnection);
		connection = NULL;
	}

	return connection;
}


/*
 * Utility method to simplify populating a connection cache key with relevant
 * fields from a provided connection.
 */
void
BuildKeyForConnection(PGconn *connection, NodeConnectionKey *connectionKey)
{
	char *nodeNameString = NULL;
	char *nodePortString = NULL;
	char *nodeUserString = NULL;

	nodeNameString = ConnectionGetOptionValue(connection, "host");
	if (nodeNameString == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("connection is missing host option")));
	}

	nodePortString = ConnectionGetOptionValue(connection, "port");
	if (nodePortString == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("connection is missing port option")));
	}

	nodeUserString = ConnectionGetOptionValue(connection, "user");
	if (nodeUserString == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("connection is missing user option")));
	}

	MemSet(connectionKey, 0, sizeof(NodeConnectionKey));
	strlcpy(connectionKey->nodeName, nodeNameString, MAX_NODE_LENGTH + 1);
	connectionKey->nodePort = pg_atoi(nodePortString, sizeof(int32), 0);
	strlcpy(connectionKey->nodeUser, nodeUserString, NAMEDATALEN);

	pfree(nodeNameString);
	pfree(nodePortString);
	pfree(nodeUserString);
}


/*
 * WarnRemoteError retrieves error fields from a remote result and produces an
 * error report at the WARNING level after amending the error with a CONTEXT
 * field containing the remote node host and port information.
 */
void
WarnRemoteError(PGconn *connection, PGresult *result)
{
	ReportRemoteError(connection, result, false);
}


/*
 * ReraiseRemoteError retrieves error fields from a remote result and re-raises
 * the error after amending it with a CONTEXT field containing the remote node
 * host and port information.
 */
void
ReraiseRemoteError(PGconn *connection, PGresult *result)
{
	ReportRemoteError(connection, result, true);
}


/*
 * ReportRemoteError is an internal helper function which implements logic
 * needed by both WarnRemoteError and ReraiseRemoteError. They wrap this
 * function to provide explicit names for the possible behaviors.
 */
static void
ReportRemoteError(PGconn *connection, PGresult *result, bool raiseError)
{
	char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char *messagePrimary = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	char *messageDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	char *messageHint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
	char *messageContext = PQresultErrorField(result, PG_DIAG_CONTEXT);

	char *nodeName = ConnectionGetOptionValue(connection, "host");
	char *nodePort = ConnectionGetOptionValue(connection, "port");
	int sqlState = ERRCODE_CONNECTION_FAILURE;
	int errorLevel = WARNING;

	if (sqlStateString != NULL)
	{
		sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
								 sqlStateString[3], sqlStateString[4]);
	}

	/*
	 * If the PGresult did not contain a message, the connection may provide a
	 * suitable top level one. At worst, this is an empty string.
	 */
	if (messagePrimary == NULL)
	{
		char *lastNewlineIndex = NULL;

		messagePrimary = PQerrorMessage(connection);
		lastNewlineIndex = strrchr(messagePrimary, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}
	}

	/*
	 * If requested, actually raise an error.
	 */
	if (raiseError)
	{
		errorLevel = ERROR;
	}

	if (sqlState == ERRCODE_CONNECTION_FAILURE)
	{
		ereport(errorLevel, (errcode(sqlState),
							 errmsg("connection failed to %s:%s", nodeName, nodePort),
							 errdetail("%s", messagePrimary)));
	}
	else
	{
		ereport(errorLevel, (errcode(sqlState), errmsg("%s", messagePrimary),
							 messageDetail ? errdetail("%s", messageDetail) : 0,
							 messageHint ? errhint("%s", messageHint) : 0,
							 messageContext ? errcontext("%s", messageContext) : 0,
							 errcontext("while executing command on %s:%s",
										nodeName, nodePort)));
	}
}


/*
 * ConnectToNode opens a connection to a remote PostgreSQL server. The function
 * configures the connection's fallback application name to 'citus' and sets
 * the remote encoding to match the local one.  All parameters are required to
 * be non NULL.
 *
 * This is only a thin layer over connection_management.[ch], and will be
 * removed soon.
 */
PGconn *
ConnectToNode(char *nodeName, int32 nodePort, char *nodeUser)
{
	/* don't want already established connections */
	int connectionFlags = FORCE_NEW_CONNECTION;
	PGconn *connection = NULL;
	MultiConnection *multiConnection =
		GetNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort, nodeUser,
									  NULL);

	if (PQstatus(multiConnection->pgConn) == CONNECTION_OK)
	{
		connection = multiConnection->pgConn;
	}
	else
	{
		ReportConnectionError(multiConnection, WARNING);
		CloseConnection(multiConnection);
		connection = NULL;
	}

	return connection;
}


/*
 * ConnectionGetOptionValue inspects the provided connection for an option with
 * a given keyword and returns a new palloc'd string with that options's value.
 * The function returns NULL if the connection has no setting for an option with
 * the provided keyword.
 */
char *
ConnectionGetOptionValue(PGconn *connection, char *optionKeyword)
{
	char *optionValue = NULL;
	PQconninfoOption *conninfoOptions = PQconninfo(connection);
	PQconninfoOption *option = NULL;

	for (option = conninfoOptions; option->keyword != NULL; option++)
	{
		if (strncmp(option->keyword, optionKeyword, NAMEDATALEN) == 0)
		{
			optionValue = pstrdup(option->val);
		}
	}

	PQconninfoFree(conninfoOptions);

	return optionValue;
}
