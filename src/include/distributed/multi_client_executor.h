/*-------------------------------------------------------------------------
 *
 * multi_client_executor.h
 *	  Type and function pointer declarations for executing client-side (libpq)
 *	  logic.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_CLIENT_EXECUTOR_H
#define MULTI_CLIENT_EXECUTOR_H


#define INVALID_CONNECTION_ID -1  /* identifies an invalid connection */
#define CLIENT_CONNECT_TIMEOUT 5  /* connection timeout in seconds */
#define MAX_CONNECTION_COUNT 2048 /* simultaneous client connection count */
#define STRING_BUFFER_SIZE 1024   /* buffer size for character arrays */
#define CONN_INFO_TEMPLATE "host=%s port=%u dbname=%s connect_timeout=%u"


/* Enumeration to track one client connection's status */
typedef enum
{
	CLIENT_INVALID_CONNECT = 0,
	CLIENT_CONNECTION_BAD = 1,
	CLIENT_CONNECTION_BUSY = 2,
	CLIENT_CONNECTION_READY = 3
} ConnectStatus;


/* Enumeration to see if we can read query results without blocking */
typedef enum
{
	CLIENT_INVALID_RESULT_STATUS = 0,
	CLIENT_RESULT_UNAVAILABLE = 1,
	CLIENT_RESULT_BUSY = 2,
	CLIENT_RESULT_READY = 3
} ResultStatus;


/* Enumeration to track one execution query's status on the client */
typedef enum
{
	CLIENT_INVALID_QUERY = 0,
	CLIENT_QUERY_FAILED = 1,
	CLIENT_QUERY_DONE = 2,
	CLIENT_QUERY_COPY = 3
} QueryStatus;


/* Enumeration to track one copy query's status on the client */
typedef enum
{
	CLIENT_INVALID_COPY = 0,
	CLIENT_COPY_MORE = 1,
	CLIENT_COPY_FAILED = 2,
	CLIENT_COPY_DONE = 3
} CopyStatus;


/* Enumeration to track the status of a query in a batch on the client */
typedef enum
{
	CLIENT_INVALID_BATCH_QUERY = 0,
	CLIENT_BATCH_QUERY_FAILED = 1,
	CLIENT_BATCH_QUERY_CONTINUE = 2,
	CLIENT_BATCH_QUERY_DONE = 3
} BatchQueryStatus;


/* Function declarations for executing client-side (libpq) logic. */
extern int32 MultiClientConnect(const char *nodeName, uint32 nodePort,
								const char *nodeDatabase);
extern int32 MultiClientConnectStart(const char *nodeName, uint32 nodePort,
									 const char *nodeDatabase);
extern ConnectStatus MultiClientConnectPoll(int32 connectionId);
extern void MultiClientDisconnect(int32 connectionId);
extern bool MultiClientConnectionUp(int32 connectionId);
extern bool MultiClientSendQuery(int32 connectionId, const char *query);
extern bool MultiClientCancel(int32 connectionId);
extern ResultStatus MultiClientResultStatus(int32 connectionId);
extern QueryStatus MultiClientQueryStatus(int32 connectionId);
extern CopyStatus MultiClientCopyData(int32 connectionId, int32 fileDescriptor);
extern bool MultiClientQueryResult(int32 connectionId, void **queryResult,
								   int *rowCount, int *columnCount);
extern BatchQueryStatus MultiClientBatchResult(int32 connectionId, void **queryResult,
											   int *rowCount, int *columnCount);
extern char * MultiClientGetValue(void *queryResult, int rowIndex, int columnIndex);
extern void MultiClientClearResult(void *queryResult);


#endif /* MULTI_CLIENT_EXECUTOR_H */
