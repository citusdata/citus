/*-------------------------------------------------------------------------
 *
 * remote_commands.c
 *   Helpers to make it easier to execute command on remote nodes.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"


/* GUC, determining whether statements sent to remote nodes are logged */
bool LogRemoteCommands = false;


/* simple helpers */

/*
 * IsResponseOK checks whether the result is a successful one.
 */
bool
IsResponseOK(PGresult *result)
{
	ExecStatusType resultStatus = PQresultStatus(result);

	if (resultStatus == PGRES_SINGLE_TUPLE || resultStatus == PGRES_TUPLES_OK ||
		resultStatus == PGRES_COMMAND_OK)
	{
		return true;
	}

	return false;
}


/*
 * Clear connection from current activity.
 *
 * FIXME: This probably should use PQcancel() if results would require network
 * IO.
 */
void
ForgetResults(MultiConnection *connection)
{
	while (true)
	{
		PGresult *result = NULL;
		result = PQgetResult(connection->conn);
		if (result == NULL)
		{
			break;
		}
		if (PQresultStatus(result) == PGRES_COPY_IN)
		{
			PQputCopyEnd(connection->conn, NULL);

			/* FIXME: mark connection as failed? */
		}
		PQclear(result);
	}
}


/*
 * SqlStateMatchesCategory returns true if the given sql state (which may be
 * NULL if unknown) is in the given error category. Note that we use
 * ERRCODE_TO_CATEGORY macro to determine error category of the sql state and
 * expect the caller to use the same macro for the error category.
 */
bool
SqlStateMatchesCategory(char *sqlStateString, int category)
{
	bool sqlStateMatchesCategory = false;
	int sqlState = 0;
	int sqlStateCategory = 0;

	if (sqlStateString == NULL)
	{
		return false;
	}

	sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
							 sqlStateString[3], sqlStateString[4]);

	sqlStateCategory = ERRCODE_TO_CATEGORY(sqlState);
	if (sqlStateCategory == category)
	{
		sqlStateMatchesCategory = true;
	}

	return sqlStateMatchesCategory;
}


/* report errors & warnings */

/*
 * Report libpq failure that's not associated with a result.
 */
void
ReportConnectionError(MultiConnection *connection, int elevel)
{
	char *nodeName = connection->hostname;
	int nodePort = connection->port;

	ereport(elevel, (errmsg("connection error: %s:%d", nodeName, nodePort),
					 errdetail("%s", PQerrorMessage(connection->conn))));
}


/*
 * Report libpq failure associated with a result.
 */
void
ReportResultError(MultiConnection *connection, PGresult *result, int elevel)
{
	char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char *messagePrimary = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	char *messageDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	char *messageHint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
	char *messageContext = PQresultErrorField(result, PG_DIAG_CONTEXT);

	char *nodeName = connection->hostname;
	int nodePort = connection->port;
	int sqlState = ERRCODE_INTERNAL_ERROR;

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

		messagePrimary = PQerrorMessage(connection->conn);
		lastNewlineIndex = strrchr(messagePrimary, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}
	}

	ereport(elevel, (errcode(sqlState), errmsg("%s", messagePrimary),
					 messageDetail ? errdetail("%s", messageDetail) : 0,
					 messageHint ? errhint("%s", messageHint) : 0,
					 messageContext ? errcontext("%s", messageContext) : 0,
					 errcontext("while executing command on %s:%d",
								nodeName, nodePort)));
}


/*
 * Log commands send to remote nodes if citus.log_remote_commands wants us to
 * do so.
 */
void
LogRemoteCommand(MultiConnection *connection, const char *command)
{
	if (!LogRemoteCommands)
	{
		return;
	}

	ereport(LOG, (errmsg("issuing %s", command),
				  errdetail("on server %s:%d", connection->hostname, connection->port)));
}


/* wrappers around libpq functions, with command logging support */

/*
 * Tiny PQsendQuery wrapper that logs remote commands, and accepts a
 * MultiConnection instead of a plain PGconn.
 */
int
SendRemoteCommand(MultiConnection *connection, const char *command)
{
	LogRemoteCommand(connection, command);
	return PQsendQuery(connection->conn, command);
}
