/*-------------------------------------------------------------------------
 *
 * multi_client_executor.h
 *	  Type and function pointer declarations for executing client-side (libpq)
 *	  logic.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_CLIENT_EXECUTOR_H
#define MULTI_CLIENT_EXECUTOR_H


#include "distributed/connection_management.h"
#include "nodes/pg_list.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#define INVALID_CONNECTION_ID -1  /* identifies an invalid connection */
#define MAX_CONNECTION_COUNT 2048 /* simultaneous client connection count */
#define STRING_BUFFER_SIZE 1024   /* buffer size for character arrays */


/* Enumeration to track one client connection's status */
typedef enum
{
	CLIENT_INVALID_CONNECT = 0,
	CLIENT_CONNECTION_BAD = 1,
	CLIENT_CONNECTION_BUSY = 2,
	CLIENT_CONNECTION_BUSY_READ = 3,
	CLIENT_CONNECTION_BUSY_WRITE = 4,
	CLIENT_CONNECTION_READY = 5
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

struct pollfd; /* forward declared, to avoid having to include poll.h */

typedef struct WaitInfo
{
	int maxWaiters;
#ifdef HAVE_POLL
	struct pollfd *pollfds;
#else
	fd_set readFileDescriptorSet;
	fd_set writeFileDescriptorSet;
	fd_set exceptionFileDescriptorSet;
	int maxConnectionFileDescriptor;
#endif /* HAVE_POLL*/
	int registeredWaiters;
	bool haveReadyWaiter;
	bool haveFailedWaiter;
} WaitInfo;


/* Function declarations for executing client-side (libpq) logic. */
extern int32 MultiClientConnect(const char *nodeName, uint32 nodePort,
								const char *nodeDatabase, const char *nodeUser);
extern void MultiClientDisconnect(int32 connectionId);
extern bool MultiClientSendQuery(int32 connectionId, const char *query);
extern ResultStatus MultiClientResultStatus(int32 connectionId);
extern QueryStatus MultiClientQueryStatus(int32 connectionId);
extern CopyStatus MultiClientCopyData(int32 connectionId, int32 fileDescriptor,
									  uint64 *returnBytesReceived);


#endif /* MULTI_CLIENT_EXECUTOR_H */
