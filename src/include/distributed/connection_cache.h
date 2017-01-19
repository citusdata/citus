/*-------------------------------------------------------------------------
 *
 * connection_cache.h
 *
 * Declarations for public functions and types related to connection hash
 * functionality.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONNECTION_CACHE_H
#define CONNECTION_CACHE_H

#include "c.h"
#include "libpq-fe.h"

#include "utils/hsearch.h"
#include "distributed/connection_management.h"

/*
 * NodeConnectionKey acts as the key to index into the (process-local) hash
 * keeping track of open connections. Node name and port are sufficient.
 */
typedef struct NodeConnectionKey
{
	char nodeName[MAX_NODE_LENGTH + 1]; /* hostname of host to connect to */
	int32 nodePort;                     /* port of host to connect to */
	char nodeUser[NAMEDATALEN + 1];     /* user name to connect as */
} NodeConnectionKey;


/* NodeConnectionEntry keeps track of connections themselves. */
typedef struct NodeConnectionEntry
{
	NodeConnectionKey cacheKey; /* hash entry key */
	MultiConnection *connection; /* connection to remote server, if any */
} NodeConnectionEntry;


/* function declarations for obtaining and using a connection */
extern PGconn * GetOrEstablishConnection(char *nodeName, int32 nodePort);
extern void BuildKeyForConnection(PGconn *connection, NodeConnectionKey *connectionKey);
extern void WarnRemoteError(PGconn *connection, PGresult *result);
extern void ReraiseRemoteError(PGconn *connection, PGresult *result);
extern PGconn * ConnectToNode(char *nodeName, int nodePort, char *nodeUser);
extern char * ConnectionGetOptionValue(PGconn *connection, char *optionKeyword);


#endif /* CONNECTION_CACHE_H */
