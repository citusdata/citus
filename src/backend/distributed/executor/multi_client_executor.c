/*-------------------------------------------------------------------------
 *
 * multi_client_executor.c
 *
 * This file contains the libpq-specific parts of executing queries on remote
 * nodes.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
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
#include "distributed/multi_client_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/remote_commands.h"

#include <errno.h>
#include <unistd.h>

#include <poll.h>
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif


/* Local pool to track active connections */
static MultiConnection *ClientConnectionArray[MAX_CONNECTION_COUNT];

/*
 * The value at any position on ClientPollingStatusArray is only defined when
 * the corresponding ClientConnectionArray entry exists.
 */
static PostgresPollingStatusType ClientPollingStatusArray[MAX_CONNECTION_COUNT];


/* Local functions forward declarations */
static bool ClientConnectionReady(MultiConnection *connection,
								  PostgresPollingStatusType pollingStatus);


/* AllocateConnectionId returns a connection id from the connection pool. */
static int32
AllocateConnectionId(void)
{
	int32 connectionId = INVALID_CONNECTION_ID;
	int32 connIndex = 0;

	/* allocate connectionId from connection pool */
	for (connIndex = 0; connIndex < MAX_CONNECTION_COUNT; connIndex++)
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
	MultiConnection *connection = NULL;
	ConnStatusType connStatusType = CONNECTION_OK;
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
	connection = GetNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
											   userName, nodeDatabase);

	connStatusType = PQstatus(connection->pgConn);

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


/*
 * MultiClientConnectStart asynchronously tries to establish a connection. If it
 * succeeds, it returns the connection id. Otherwise, it reports connection
 * error and returns INVALID_CONNECTION_ID.
 */
int32
MultiClientConnectStart(const char *nodeName, uint32 nodePort, const char *nodeDatabase,
						const char *userName)
{
	MultiConnection *connection = NULL;
	ConnStatusType connStatusType = CONNECTION_OK;
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

	/* prepare asynchronous request for worker node connection */
	connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
												 userName, nodeDatabase);
	connStatusType = PQstatus(connection->pgConn);

	/*
	 * If prepared, we save the connection, and set its initial polling status
	 * to PGRES_POLLING_WRITING as specified in "Database Connection Control
	 * Functions" section of the PostgreSQL documentation.
	 */
	if (connStatusType != CONNECTION_BAD)
	{
		ClientConnectionArray[connectionId] = connection;
		ClientPollingStatusArray[connectionId] = PGRES_POLLING_WRITING;
	}
	else
	{
		ReportConnectionError(connection, WARNING);
		CloseConnection(connection);

		connectionId = INVALID_CONNECTION_ID;
	}

	return connectionId;
}


/* MultiClientConnectPoll returns the status of client connection. */
ConnectStatus
MultiClientConnectPoll(int32 connectionId)
{
	MultiConnection *connection = NULL;
	PostgresPollingStatusType pollingStatus = PGRES_POLLING_OK;
	ConnectStatus connectStatus = CLIENT_INVALID_CONNECT;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	pollingStatus = ClientPollingStatusArray[connectionId];
	if (pollingStatus == PGRES_POLLING_OK)
	{
		connectStatus = CLIENT_CONNECTION_READY;
	}
	else if (pollingStatus == PGRES_POLLING_READING)
	{
		bool readReady = ClientConnectionReady(connection, PGRES_POLLING_READING);
		if (readReady)
		{
			ClientPollingStatusArray[connectionId] = PQconnectPoll(connection->pgConn);
			connectStatus = CLIENT_CONNECTION_BUSY;
		}
		else
		{
			connectStatus = CLIENT_CONNECTION_BUSY_READ;
		}
	}
	else if (pollingStatus == PGRES_POLLING_WRITING)
	{
		bool writeReady = ClientConnectionReady(connection, PGRES_POLLING_WRITING);
		if (writeReady)
		{
			ClientPollingStatusArray[connectionId] = PQconnectPoll(connection->pgConn);
			connectStatus = CLIENT_CONNECTION_BUSY;
		}
		else
		{
			connectStatus = CLIENT_CONNECTION_BUSY_WRITE;
		}
	}
	else if (pollingStatus == PGRES_POLLING_FAILED)
	{
		ReportConnectionError(connection, WARNING);

		connectStatus = CLIENT_CONNECTION_BAD;
	}

	return connectStatus;
}


/* MultiClientDisconnect disconnects the connection. */
void
MultiClientDisconnect(int32 connectionId)
{
	MultiConnection *connection = NULL;
	const int InvalidPollingStatus = -1;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	CloseConnection(connection);

	ClientConnectionArray[connectionId] = NULL;
	ClientPollingStatusArray[connectionId] = InvalidPollingStatus;
}


/*
 * MultiClientConnectionUp checks if the connection status is up, in other words,
 * it is not bad.
 */
bool
MultiClientConnectionUp(int32 connectionId)
{
	MultiConnection *connection = NULL;
	ConnStatusType connStatusType = CONNECTION_OK;
	bool connectionUp = true;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	connStatusType = PQstatus(connection->pgConn);
	if (connStatusType == CONNECTION_BAD)
	{
		connectionUp = false;
	}

	return connectionUp;
}


/* MultiClientExecute synchronously executes a query over the given connection. */
bool
MultiClientExecute(int32 connectionId, const char *query, void **queryResult,
				   int *rowCount, int *columnCount)
{
	bool querySent = false;
	bool queryOK = false;

	querySent = MultiClientSendQuery(connectionId, query);
	if (!querySent)
	{
		return false;
	}

	queryOK = MultiClientQueryResult(connectionId, queryResult, rowCount, columnCount);

	return queryOK;
}


/* MultiClientSendQuery sends the given query over the given connection. */
bool
MultiClientSendQuery(int32 connectionId, const char *query)
{
	MultiConnection *connection = NULL;
	bool success = true;
	int querySent = 0;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	querySent = PQsendQuery(connection->pgConn, query);
	if (querySent == 0)
	{
		char *errorMessage = pchomp(PQerrorMessage(connection->pgConn));
		ereport(WARNING, (errmsg("could not send remote query \"%s\"", query),
						  errdetail("Client error: %s", errorMessage)));

		success = false;
	}

	return success;
}


/* MultiClientCancel cancels the running query on the given connection. */
bool
MultiClientCancel(int32 connectionId)
{
	MultiConnection *connection = NULL;
	PGcancel *cancelObject = NULL;
	int cancelSent = 0;
	bool canceled = true;
	char errorBuffer[STRING_BUFFER_SIZE];

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	cancelObject = PQgetCancel(connection->pgConn);

	cancelSent = PQcancel(cancelObject, errorBuffer, sizeof(errorBuffer));
	if (cancelSent == 0)
	{
		ereport(WARNING, (errmsg("could not issue cancel request"),
						  errdetail("Client error: %s", errorBuffer)));

		canceled = false;
	}

	PQfreeCancel(cancelObject);

	return canceled;
}


/* MultiClientResultStatus checks result status for an asynchronous query. */
ResultStatus
MultiClientResultStatus(int32 connectionId)
{
	MultiConnection *connection = NULL;
	int consumed = 0;
	ConnStatusType connStatusType = CONNECTION_OK;
	ResultStatus resultStatus = CLIENT_INVALID_RESULT_STATUS;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	connStatusType = PQstatus(connection->pgConn);
	if (connStatusType == CONNECTION_BAD)
	{
		ereport(WARNING, (errmsg("could not maintain connection to worker node")));
		return CLIENT_RESULT_UNAVAILABLE;
	}

	/* consume input to allow status change */
	consumed = PQconsumeInput(connection->pgConn);
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


/* MultiClientQueryResult gets results for an asynchronous query. */
bool
MultiClientQueryResult(int32 connectionId, void **queryResult, int *rowCount,
					   int *columnCount)
{
	MultiConnection *connection = NULL;
	PGresult *result = NULL;
	ConnStatusType connStatusType = CONNECTION_OK;
	ExecStatusType resultStatus = PGRES_COMMAND_OK;
	bool raiseInterrupts = true;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	connStatusType = PQstatus(connection->pgConn);
	if (connStatusType == CONNECTION_BAD)
	{
		ereport(WARNING, (errmsg("could not maintain connection to worker node")));
		return false;
	}

	result = GetRemoteCommandResult(connection, raiseInterrupts);
	resultStatus = PQresultStatus(result);
	if (resultStatus == PGRES_TUPLES_OK)
	{
		(*queryResult) = (void **) result;
		(*rowCount) = PQntuples(result);
		(*columnCount) = PQnfields(result);
	}
	else
	{
		ReportResultError(connection, result, WARNING);
		PQclear(result);

		return false;
	}

	/* clear extra result objects */
	ForgetResults(connection);

	return true;
}


/*
 * MultiClientBatchResult returns results for a "batch" of queries, meaning a
 * string containing multiple select statements separated by semicolons. This
 * function should be called multiple times to retrieve the results for all the
 * queries, until CLIENT_BATCH_QUERY_DONE is returned (even if a failure occurs).
 * If a query in the batch fails, the remaining queries will not be executed. On
 * success, queryResult, rowCount and columnCount will be set to the appropriate
 * values. After use, queryResult should be cleared using ClientClearResult.
 */
BatchQueryStatus
MultiClientBatchResult(int32 connectionId, void **queryResult, int *rowCount,
					   int *columnCount)
{
	MultiConnection *connection = NULL;
	PGresult *result = NULL;
	ConnStatusType connStatusType = CONNECTION_OK;
	ExecStatusType resultStatus = PGRES_COMMAND_OK;
	BatchQueryStatus queryStatus = CLIENT_INVALID_BATCH_QUERY;
	bool raiseInterrupts = true;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	/* set default result */
	(*queryResult) = NULL;
	(*rowCount) = -1;
	(*columnCount) = -1;

	connStatusType = PQstatus(connection->pgConn);
	if (connStatusType == CONNECTION_BAD)
	{
		ereport(WARNING, (errmsg("could not maintain connection to worker node")));
		return CLIENT_BATCH_QUERY_FAILED;
	}

	result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (result == NULL)
	{
		return CLIENT_BATCH_QUERY_DONE;
	}

	resultStatus = PQresultStatus(result);
	if (resultStatus == PGRES_TUPLES_OK)
	{
		(*queryResult) = (void **) result;
		(*rowCount) = PQntuples(result);
		(*columnCount) = PQnfields(result);
		queryStatus = CLIENT_BATCH_QUERY_CONTINUE;
	}
	else if (resultStatus == PGRES_COMMAND_OK)
	{
		(*queryResult) = (void **) result;
		queryStatus = CLIENT_BATCH_QUERY_CONTINUE;
	}
	else
	{
		ReportResultError(connection, result, WARNING);
		PQclear(result);
		queryStatus = CLIENT_BATCH_QUERY_FAILED;
	}

	return queryStatus;
}


/* MultiClientGetValue returns the value of field at the given position. */
char *
MultiClientGetValue(void *queryResult, int rowIndex, int columnIndex)
{
	char *value = PQgetvalue((PGresult *) queryResult, rowIndex, columnIndex);
	return value;
}


/* MultiClientValueIsNull returns whether the value at the given position is null. */
bool
MultiClientValueIsNull(void *queryResult, int rowIndex, int columnIndex)
{
	bool isNull = PQgetisnull((PGresult *) queryResult, rowIndex, columnIndex);
	return isNull;
}


/* MultiClientClearResult free's the memory associated with a PGresult. */
void
MultiClientClearResult(void *queryResult)
{
	PQclear((PGresult *) queryResult);
}


/* MultiClientQueryStatus returns the query status. */
QueryStatus
MultiClientQueryStatus(int32 connectionId)
{
	MultiConnection *connection = NULL;
	PGresult *result = NULL;
	int tupleCount PG_USED_FOR_ASSERTS_ONLY = 0;
	bool copyResults = false;
	ConnStatusType connStatusType = CONNECTION_OK;
	ExecStatusType resultStatus = PGRES_COMMAND_OK;
	QueryStatus queryStatus = CLIENT_INVALID_QUERY;
	bool raiseInterrupts = true;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	connStatusType = PQstatus(connection->pgConn);
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
	result = GetRemoteCommandResult(connection, raiseInterrupts);
	resultStatus = PQresultStatus(result);

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
MultiClientCopyData(int32 connectionId, int32 fileDescriptor)
{
	MultiConnection *connection = NULL;
	char *receiveBuffer = NULL;
	int consumed = 0;
	int receiveLength = 0;
	const int asynchronous = 1;
	CopyStatus copyStatus = CLIENT_INVALID_COPY;

	Assert(connectionId != INVALID_CONNECTION_ID);
	connection = ClientConnectionArray[connectionId];
	Assert(connection != NULL);

	/*
	 * Consume input to handle the case where previous copy operation might have
	 * received zero bytes.
	 */
	consumed = PQconsumeInput(connection->pgConn);
	if (consumed == 0)
	{
		ereport(WARNING, (errmsg("could not read data from worker node")));
		return CLIENT_COPY_FAILED;
	}

	/* receive copy data message in an asynchronous manner */
	receiveLength = PQgetCopyData(connection->pgConn, &receiveBuffer, asynchronous);
	while (receiveLength > 0)
	{
		/* received copy data; append these data to file */
		int appended = -1;
		errno = 0;

		appended = write(fileDescriptor, receiveBuffer, receiveLength);
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


/*
 * MultiClientCreateWaitInfo creates a WaitInfo structure, capable of keeping
 * track of what maxConnections connections are waiting for; to allow
 * efficiently waiting for all of them at once.
 *
 * Connections can be added using MultiClientRegisterWait(). All added
 * connections can then be waited upon together using MultiClientWait().
 */
WaitInfo *
MultiClientCreateWaitInfo(int maxConnections)
{
	WaitInfo *waitInfo = palloc(sizeof(WaitInfo));

	waitInfo->maxWaiters = maxConnections;
	waitInfo->pollfds = palloc(maxConnections * sizeof(struct pollfd));

	/* initialize remaining fields */
	MultiClientResetWaitInfo(waitInfo);

	return waitInfo;
}


/* MultiClientResetWaitInfo clears all pending waits from a WaitInfo. */
void
MultiClientResetWaitInfo(WaitInfo *waitInfo)
{
	waitInfo->registeredWaiters = 0;
	waitInfo->haveReadyWaiter = false;
	waitInfo->haveFailedWaiter = false;
}


/* MultiClientFreeWaitInfo frees a resources associated with a waitInfo struct. */
void
MultiClientFreeWaitInfo(WaitInfo *waitInfo)
{
	pfree(waitInfo->pollfds);
	pfree(waitInfo);
}


/*
 * MultiClientRegisterWait adds a connection to be waited upon, waiting for
 * executionStatus.
 */
void
MultiClientRegisterWait(WaitInfo *waitInfo, TaskExecutionStatus executionStatus,
						int32 connectionId)
{
	MultiConnection *connection = NULL;
	struct pollfd *pollfd = NULL;

	Assert(waitInfo->registeredWaiters < waitInfo->maxWaiters);

	if (executionStatus == TASK_STATUS_READY)
	{
		waitInfo->haveReadyWaiter = true;
		return;
	}
	else if (executionStatus == TASK_STATUS_ERROR)
	{
		waitInfo->haveFailedWaiter = true;
		return;
	}

	connection = ClientConnectionArray[connectionId];
	pollfd = &waitInfo->pollfds[waitInfo->registeredWaiters];
	pollfd->fd = PQsocket(connection->pgConn);
	if (executionStatus == TASK_STATUS_SOCKET_READ)
	{
		pollfd->events = POLLERR | POLLIN;
	}
	else if (executionStatus == TASK_STATUS_SOCKET_WRITE)
	{
		pollfd->events = POLLERR | POLLOUT;
	}
	waitInfo->registeredWaiters++;
}


/*
 * MultiClientWait waits until at least one connection added with
 * MultiClientRegisterWait is ready to be processed again.
 */
void
MultiClientWait(WaitInfo *waitInfo)
{
	/*
	 * If we had a failure, we always want to sleep for a bit, to prevent
	 * flooding the other system, probably making the situation worse.
	 */
	if (waitInfo->haveFailedWaiter)
	{
		long sleepIntervalPerCycle = RemoteTaskCheckInterval * 1000L;

		pg_usleep(sleepIntervalPerCycle);
		return;
	}

	/* if there are tasks that already need attention again, don't wait */
	if (waitInfo->haveReadyWaiter)
	{
		return;
	}

	while (true)
	{
		/*
		 * Wait for activity on any of the sockets. Limit the maximum time
		 * spent waiting in one wait cycle, as insurance against edge
		 * cases. For efficiency we don't want wake up quite as often as
		 * citus.remote_task_check_interval, so rather arbitrarily sleep ten
		 * times as long.
		 */
		int rc = poll(waitInfo->pollfds, waitInfo->registeredWaiters,
					  RemoteTaskCheckInterval * 10);

		if (rc < 0)
		{
			/*
			 * Signals that arrive can interrupt our poll(). In that case just
			 * check for interrupts, and try again. Every other error is
			 * unexpected and treated as such.
			 */
			if (errno == EAGAIN || errno == EINTR)
			{
				CHECK_FOR_INTERRUPTS();

				/* maximum wait starts at max again, but that's ok, it's just a stopgap */
				continue;
			}
			else
			{
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("poll failed: %m")));
			}
		}
		else if (rc == 0)
		{
			ereport(DEBUG5,
					(errmsg("waiting for activity on tasks took longer than %ld ms",
							(long) RemoteTaskCheckInterval * 10)));
		}

		/*
		 * At least one fd changed received a readiness notification, time to
		 * process tasks again.
		 */
		return;
	}
}


/*
 * ClientConnectionReady checks if the given connection is ready for non-blocking
 * reads or writes. This function is loosely based on pqSocketCheck() at fe-misc.c
 * and libpq_select() at libpqwalreceiver.c.
 */
static bool
ClientConnectionReady(MultiConnection *connection,
					  PostgresPollingStatusType pollingStatus)
{
	bool clientConnectionReady = false;
	int pollResult = 0;
	int fileDescriptorCount = 1;
	int immediateTimeout = 0;
	int pollEventMask = 0;
	struct pollfd pollFileDescriptor;

	if (pollingStatus == PGRES_POLLING_READING)
	{
		pollEventMask = POLLERR | POLLIN;
	}
	else if (pollingStatus == PGRES_POLLING_WRITING)
	{
		pollEventMask = POLLERR | POLLOUT;
	}

	pollFileDescriptor.fd = PQsocket(connection->pgConn);
	pollFileDescriptor.events = pollEventMask;
	pollFileDescriptor.revents = 0;

	pollResult = poll(&pollFileDescriptor, fileDescriptorCount, immediateTimeout);

	if (pollResult > 0)
	{
		clientConnectionReady = true;
	}
	else if (pollResult == 0)
	{
		clientConnectionReady = false;
	}
	else if (pollResult < 0)
	{
		if (errno == EINTR)
		{
			/*
			 * If a signal was caught, we return false so the caller polls the
			 * connection again.
			 */
			clientConnectionReady = false;
		}
		else
		{
			/*
			 * poll() can set errno to EFAULT (when socket is not
			 * contained in the calling program's address space), EBADF (invalid
			 * file descriptor), EINVAL (invalid arguments to select or poll),
			 * and ENOMEM (no space to allocate file descriptor tables). Out of
			 * these, only ENOMEM is likely here, and it is a fatal error, so we
			 * error out.
			 */
			Assert(errno == ENOMEM);
			ereport(ERROR, (errcode_for_socket_access(),
							errmsg("poll() failed: %m")));
		}
	}

	return clientConnectionReady;
}
