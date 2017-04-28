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
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/latch.h"


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
 * ForgetResults clears a connection from pending activity.
 *
 * XXX: In the future it might be a good idea to use use PQcancel() if results
 * would require network IO.
 */
void
ForgetResults(MultiConnection *connection)
{
	while (true)
	{
		PGresult *result = NULL;
		const bool dontRaiseErrors = false;

		result = GetRemoteCommandResult(connection, dontRaiseErrors);
		if (result == NULL)
		{
			break;
		}
		if (PQresultStatus(result) == PGRES_COPY_IN)
		{
			PQputCopyEnd(connection->pgConn, NULL);

			/* TODO: mark transaction as failed, once we can. */
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
					 errdetail("%s", PQerrorMessage(connection->pgConn))));
}


/*
 * ReportResultError reports libpq failure associated with a result.
 */
void
ReportResultError(MultiConnection *connection, PGresult *result, int elevel)
{
	/* we release PQresult when throwing an error because the caller can't */
	PG_TRY();
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
			sqlState = MAKE_SQLSTATE(sqlStateString[0],
									 sqlStateString[1],
									 sqlStateString[2],
									 sqlStateString[3],
									 sqlStateString[4]);
		}

		/*
		 * If the PGresult did not contain a message, the connection may provide a
		 * suitable top level one. At worst, this is an empty string.
		 */
		if (messagePrimary == NULL)
		{
			char *lastNewlineIndex = NULL;

			messagePrimary = PQerrorMessage(connection->pgConn);
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
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * LogRemoteCommand logs commands send to remote nodes if
 * citus.log_remote_commands wants us to do so.
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
 * ExecuteCriticalRemoteCommand executes a remote command that is critical
 * to the transaction. If the command fails then the transaction aborts.
 */
void
ExecuteCriticalRemoteCommand(MultiConnection *connection, const char *command)
{
	int querySent = 0;
	PGresult *result = NULL;
	bool raiseInterrupts = true;

	querySent = SendRemoteCommand(connection, command);
	if (querySent == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	PQclear(result);
	ForgetResults(connection);
}


/*
 * ExecuteOptionalRemoteCommand executes a remote command. If the command fails a WARNING
 * is emitted but execution continues.
 *
 * could return 0, QUERY_SEND_FAILED, or RESPONSE_NOT_OKAY
 * result is only set if there was no error
 */
int
ExecuteOptionalRemoteCommand(MultiConnection *connection, const char *command,
							 PGresult **result)
{
	int querySent = 0;
	PGresult *localResult = NULL;
	bool raiseInterrupts = true;

	querySent = SendRemoteCommand(connection, command);
	if (querySent == 0)
	{
		ReportConnectionError(connection, WARNING);
		return QUERY_SEND_FAILED;
	}

	localResult = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(localResult))
	{
		ReportResultError(connection, localResult, WARNING);
		PQclear(localResult);
		ForgetResults(connection);
		return RESPONSE_NOT_OKAY;
	}

	*result = localResult;
	return 0;
}


/*
 * SendRemoteCommand is a PQsendQuery wrapper that logs remote commands, and
 * accepts a MultiConnection instead of a plain PGconn.  It makes sure it can
 * send commands asynchronously without blocking (at the potential expense of
 * an additional memory allocation).
 */
int
SendRemoteCommandParams(MultiConnection *connection, const char *command,
						int parameterCount, const Oid *parameterTypes,
						const char *const *parameterValues)
{
	PGconn *pgConn = connection->pgConn;
	bool wasNonblocking = false;
	int rc = 0;

	LogRemoteCommand(connection, command);

	/*
	 * Don't try to send command if connection is entirely gone
	 * (PQisnonblocking() would crash).
	 */
	if (!pgConn)
	{
		return 0;
	}

	wasNonblocking = PQisnonblocking(pgConn);

	/* make sure not to block anywhere */
	if (!wasNonblocking)
	{
		PQsetnonblocking(pgConn, true);
	}

	rc = PQsendQueryParams(pgConn, command, parameterCount, parameterTypes,
						   parameterValues, NULL, NULL, 0);

	/* reset nonblocking connection to its original state */
	if (!wasNonblocking)
	{
		PQsetnonblocking(pgConn, false);
	}

	return rc;
}


/*
 * SendRemoteCommand is a PQsendQuery wrapper that logs remote commands, and
 * accepts a MultiConnection instead of a plain PGconn.  It makes sure it can
 * send commands asynchronously without blocking (at the potential expense of
 * an additional memory allocation).
 */
int
SendRemoteCommand(MultiConnection *connection, const char *command)
{
	return SendRemoteCommandParams(connection, command, 0, NULL, NULL);
}


/*
 * ReadFirstColumnAsText reads the first column of result tuples from the given
 * PGresult struct and returns them in a StringInfo list.
 */
List *
ReadFirstColumnAsText(PGresult *queryResult)
{
	List *resultRowList = NIL;
	const int columnIndex = 0;
	int64 rowIndex = 0;
	int64 rowCount = 0;

	ExecStatusType status = PQresultStatus(queryResult);
	if (status == PGRES_TUPLES_OK)
	{
		rowCount = PQntuples(queryResult);
	}

	for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		char *rowValue = PQgetvalue(queryResult, rowIndex, columnIndex);

		StringInfo rowValueString = makeStringInfo();
		appendStringInfoString(rowValueString, rowValue);

		resultRowList = lappend(resultRowList, rowValueString);
	}

	return resultRowList;
}


/*
 * GetRemoteCommandResult is a wrapper around PQgetResult() that handles interrupts.
 *
 * If raiseInterrupts is true and an interrupt arrives, e.g. the query is
 * being cancelled, CHECK_FOR_INTERRUPTS() will be called, which then throws
 * an error.
 *
 * If raiseInterrupts is false and an interrupt arrives that'd otherwise raise
 * an error, GetRemotecommandResult returns NULL, and the transaction is
 * marked as having failed. While that's not a perfect way to signal failure,
 * callers will usually treat that as an error, and it's easy to use.
 *
 * Handling of interrupts is important to allow queries being cancelled while
 * waiting on remote nodes. In a distributed deadlock scenario cancelling
 * might be the only way to resolve the deadlock.
 */
PGresult *
GetRemoteCommandResult(MultiConnection *connection, bool raiseInterrupts)
{
	PGconn *pgConn = connection->pgConn;
	int socket = 0;
	int waitFlags = WL_POSTMASTER_DEATH | WL_LATCH_SET;
	bool wasNonblocking = false;
	PGresult *result = NULL;
	bool failed = false;

	/*
	 * Short circuit tests around the more expensive parts of this
	 * routine. This'd also trigger a return in the, unlikely, case of a
	 * failed/nonexistant connection.
	 */
	if (!PQisBusy(pgConn))
	{
		return PQgetResult(connection->pgConn);
	}

	socket = PQsocket(pgConn);
	wasNonblocking = PQisnonblocking(pgConn);

	/* make sure not to block anywhere */
	if (!wasNonblocking)
	{
		PQsetnonblocking(pgConn, true);
	}

	if (raiseInterrupts)
	{
		CHECK_FOR_INTERRUPTS();
	}

	/* make sure command has been sent out */
	while (!failed)
	{
		int rc = 0;

		ResetLatch(MyLatch);

		/* try to send all the data */
		rc = PQflush(pgConn);

		/* stop writing if all data has been sent, or there was none to send */
		if (rc == 0)
		{
			break;
		}

		/* if sending failed, there's nothing more we can do */
		if (rc == -1)
		{
			failed = true;
			break;
		}

		/* this means we have to wait for data to go out */
		Assert(rc == 1);

		rc = WaitLatchOrSocket(MyLatch, waitFlags | WL_SOCKET_WRITEABLE, socket, 0);

		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (rc & WL_LATCH_SET)
		{
			/* if allowed raise errors */
			if (raiseInterrupts)
			{
				CHECK_FOR_INTERRUPTS();
			}

			/*
			 * If raising errors allowed, or called within in a section with
			 * interrupts held, return NULL instead, and mark the transaction
			 * as failed.
			 */
			if (InterruptHoldoffCount > 0 && (QueryCancelPending || ProcDiePending))
			{
				connection->remoteTransaction.transactionFailed = true;
				failed = true;
				break;
			}
		}
	}

	/* wait for the result of the command to come in */
	while (!failed)
	{
		int rc = 0;

		ResetLatch(MyLatch);

		/* if reading fails, there's not much we can do */
		if (PQconsumeInput(pgConn) == 0)
		{
			failed = true;
			break;
		}

		/* check if all the necessary data is now available */
		if (!PQisBusy(pgConn))
		{
			result = PQgetResult(connection->pgConn);
			break;
		}

		rc = WaitLatchOrSocket(MyLatch, waitFlags | WL_SOCKET_READABLE, socket, 0);

		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (rc & WL_LATCH_SET)
		{
			/* if allowed raise errors */
			if (raiseInterrupts)
			{
				CHECK_FOR_INTERRUPTS();
			}

			/*
			 * If raising errors allowed, or called within in a section with
			 * interrupts held, return NULL instead, and mark the transaction
			 * as failed.
			 */
			if (InterruptHoldoffCount > 0 && (QueryCancelPending || ProcDiePending))
			{
				connection->remoteTransaction.transactionFailed = true;
				failed = true;
				break;
			}
		}
	}

	if (!wasNonblocking)
	{
		PQsetnonblocking(pgConn, false);
	}

	return result;
}
