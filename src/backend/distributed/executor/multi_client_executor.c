/*-------------------------------------------------------------------------
 *
 * multi_client_executor.c
 *
 * This file contains the libpq-specific parts of executing queries on remote
 * nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "distributed/metadata_cache.h"
#include "distributed/connection_management.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/remote_commands.h"
#include "distributed/subplan_execution.h"

#include <errno.h>
#include <unistd.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif


/* Local pool to track active connections */
static MultiConnection *ClientConnectionArray[MAX_CONNECTION_COUNT];

/*
 * The value at any position on ClientPollingStatusArray is only defined when
 * the corresponding ClientConnectionArray entry exists.
 */
static PostgresPollingStatusType ClientPollingStatusArray[MAX_CONNECTION_COUNT];


/* AllocateConnectionId returns a connection id from the connection pool. */
static int32
AllocateConnectionId(void)
{
	int32 connectionId = INVALID_CONNECTION_ID;

	/* allocate connectionId from connection pool */
	for (int32 connIndex = 0; connIndex < MAX_CONNECTION_COUNT; connIndex++)
	{
		MultiConnection *connection = ClientConnectionArray[connIndex];
		if (connection == NULL)
		{
			connectionId = connIndex;
			break;
		}
	}

	return connectionId;
}


/*
 * MultiClientConnect synchronously tries to establish a connection. If it
 * succeeds, it returns the connection id. Otherwise, it reports connection
 * error and returns INVALID_CONNECTION_ID.
 *
 * nodeDatabase and userName can be NULL, in which case values from the
 * current session are used.
 */
int32
MultiClientConnect(const char *nodeName, uint32 nodePort, const char *nodeDatabase,
				   const char *userName)
{
	int32 connectionId = AllocateConnectionId();
	int connectionFlags = FORCE_NEW_CONNECTION; /* no cached connections for now */

	if (connectionId == INVALID_CONNECTION_ID)
	{
		ereport(WARNING, (errmsg("could not allocate connection in connection pool")));
		return connectionId;
	}

	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot open new connections after the first modification "
							   "command within a transaction")));
	}

	/* establish synchronous connection to worker node */
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags, nodeName,
																nodePort,
																userName, nodeDatabase);

	ConnStatusType connStatusType = PQstatus(connection->pgConn);

	if (connStatusType == CONNECTION_OK)
	{
		ClientConnectionArray[connectionId] = connection;
	}
	else
	{
		ReportConnectionError(connection, WARNING);
		CloseConnection(connection);
		connectionId = INVALID_CONNECTION_ID;
	}

	return connectionId;
}


/* MultiClientDisconnect disconnects the connection. */
void
MultiClientDisconnect(int32 connectionId)
{
	const int InvalidPollingStatus = -1;

	Assert(connectionId != INVALID_CONNECTION_ID);
	MultiConnection *connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	CloseConnection(connection);

	ClientConnectionArray[connectionId] = NULL;
	ClientPollingStatusArray[connectionId] = InvalidPollingStatus;
}


/* MultiClientSendQuery sends the given query over the given connection. */
bool
MultiClientSendQuery(int32 connectionId, const char *query)
{
	bool success = true;

	Assert(connectionId != INVALID_CONNECTION_ID);
	MultiConnection *connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	int querySent = SendRemoteCommand(connection, query);
	if (querySent == 0)
	{
		char *errorMessage = pchomp(PQerrorMessage(connection->pgConn));

		/*
		 * query might include the user query coming from the taskTracker
		 * code path, that's why we hash it, too. Otherwise, this code
		 * path is generally exercised for the kind of errors that
		 * we cannot send the queries that Citus itself produced.
		 */
		ereport(WARNING, (errmsg("could not send remote query \"%s\"",
								 ApplyLogRedaction(query)),
						  errdetail("Client error: %s",
									ApplyLogRedaction(errorMessage))));

		success = false;
	}

	return success;
}


/* MultiClientResultStatus checks result status for an asynchronous query. */
ResultStatus
MultiClientResultStatus(int32 connectionId)
{
	ResultStatus resultStatus = CLIENT_INVALID_RESULT_STATUS;

	Assert(connectionId != INVALID_CONNECTION_ID);
	MultiConnection *connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	ConnStatusType connStatusType = PQstatus(connection->pgConn);
	if (connStatusType == CONNECTION_BAD)
	{
		ereport(WARNING, (errmsg("could not maintain connection to worker node")));
		return CLIENT_RESULT_UNAVAILABLE;
	}

	/* consume input to allow status change */
	int consumed = PQconsumeInput(connection->pgConn);
	if (consumed != 0)
	{
		int connectionBusy = PQisBusy(connection->pgConn);
		if (connectionBusy == 0)
		{
			resultStatus = CLIENT_RESULT_READY;
		}
		else
		{
			resultStatus = CLIENT_RESULT_BUSY;
		}
	}
	else
	{
		ereport(WARNING, (errmsg("could not consume data from worker node")));
		resultStatus = CLIENT_RESULT_UNAVAILABLE;
	}

	return resultStatus;
}


/* MultiClientQueryStatus returns the query status. */
QueryStatus
MultiClientQueryStatus(int32 connectionId)
{
	int tupleCount PG_USED_FOR_ASSERTS_ONLY = 0;
	bool copyResults = false;
	QueryStatus queryStatus = CLIENT_INVALID_QUERY;
	bool raiseInterrupts = true;

	Assert(connectionId != INVALID_CONNECTION_ID);
	MultiConnection *connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	ConnStatusType connStatusType = PQstatus(connection->pgConn);
	if (connStatusType == CONNECTION_BAD)
	{
		ereport(WARNING, (errmsg("could not maintain connection to worker node")));
		return CLIENT_QUERY_FAILED;
	}

	/*
	 * We now read the result object and check its status. If the result object
	 * isn't ready yet (the caller didn't wait for the connection to be ready),
	 * we will block on this call.
	 */
	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
	ExecStatusType resultStatus = PQresultStatus(result);

	if (resultStatus == PGRES_COMMAND_OK)
	{
		queryStatus = CLIENT_QUERY_DONE;
	}
	else if (resultStatus == PGRES_TUPLES_OK)
	{
		queryStatus = CLIENT_QUERY_DONE;

		/*
		 * We use the client executor to only issue a select query that returns
		 * a void value. We therefore should not have more than one value here.
		 */
		tupleCount = PQntuples(result);
		Assert(tupleCount <= 1);
	}
	else if (resultStatus == PGRES_COPY_OUT)
	{
		queryStatus = CLIENT_QUERY_COPY;
		copyResults = true;
	}
	else
	{
		queryStatus = CLIENT_QUERY_FAILED;
		if (resultStatus == PGRES_COPY_IN)
		{
			copyResults = true;
		}

		ReportResultError(connection, result, WARNING);
	}

	/* clear the result object */
	PQclear(result);

	/*
	 * When using the async query mechanism, we need to keep reading results
	 * until we get null. The exception to this rule is the copy protocol.
	 */
	if (!copyResults)
	{
		ForgetResults(connection);
	}

	return queryStatus;
}


/* MultiClientCopyData copies data from the file. */
CopyStatus
MultiClientCopyData(int32 connectionId, int32 fileDescriptor, uint64 *returnBytesReceived)
{
	char *receiveBuffer = NULL;
	const int asynchronous = 1;
	CopyStatus copyStatus = CLIENT_INVALID_COPY;

	Assert(connectionId != INVALID_CONNECTION_ID);
	MultiConnection *connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	/*
	 * Consume input to handle the case where previous copy operation might have
	 * received zero bytes.
	 */
	int consumed = PQconsumeInput(connection->pgConn);
	if (consumed == 0)
	{
		ereport(WARNING, (errmsg("could not read data from worker node")));
		return CLIENT_COPY_FAILED;
	}

	/* receive copy data message in an asynchronous manner */
	int receiveLength = PQgetCopyData(connection->pgConn, &receiveBuffer, asynchronous);
	while (receiveLength > 0)
	{
		/* received copy data; append these data to file */
		errno = 0;

		if (returnBytesReceived)
		{
			*returnBytesReceived += receiveLength;
		}

		int appended = write(fileDescriptor, receiveBuffer, receiveLength);
		if (appended != receiveLength)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
			{
				errno = ENOSPC;
			}
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not append to copied file: %m")));
		}

		PQfreemem(receiveBuffer);

		receiveLength = PQgetCopyData(connection->pgConn, &receiveBuffer, asynchronous);
	}

	/* we now check the last received length returned by copy data */
	if (receiveLength == 0)
	{
		/* we cannot read more data without blocking */
		copyStatus = CLIENT_COPY_MORE;
	}
	else if (receiveLength == -1)
	{
		/* received copy done message */
		bool raiseInterrupts = true;
		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		ExecStatusType resultStatus = PQresultStatus(result);

		if (resultStatus == PGRES_COMMAND_OK)
		{
			copyStatus = CLIENT_COPY_DONE;
		}
		else
		{
			copyStatus = CLIENT_COPY_FAILED;

			ReportResultError(connection, result, WARNING);
		}

		PQclear(result);
	}
	else if (receiveLength == -2)
	{
		/* received an error */
		copyStatus = CLIENT_COPY_FAILED;

		ReportConnectionError(connection, WARNING);
	}

	/* if copy out completed, make sure we drain all results from libpq */
	if (receiveLength < 0)
	{
		ForgetResults(connection);
	}

	return copyStatus;
}
