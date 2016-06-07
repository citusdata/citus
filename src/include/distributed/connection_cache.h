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

#include "nodes/pg_list.h"
#include "utils/hsearch.h"

/* maximum duration to wait for connection */
#define CLIENT_CONNECT_TIMEOUT_SECONDS "5"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255
#define MAX_PORT_LENGTH 10

/* times to attempt connection (or reconnection) */
#define MAX_CONNECT_ATTEMPTS 2

/* SQL statement for testing */
#define TEST_SQL "DO $$ BEGIN RAISE EXCEPTION 'Raised remotely!'; END $$"


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
	PGconn *connection;         /* connection to remote server, if any */
} NodeConnectionEntry;


/* function declarations for obtaining and using a connection */
extern PGconn * GetOrEstablishConnection(char *nodeName, int32 nodePort);
extern void PurgeConnection(PGconn *connection);
extern bool SqlStateMatchesCategory(char *sqlStateString, int category);
extern void WarnRemoteError(PGconn *connection, PGresult *result);
extern void ReraiseRemoteError(PGconn *connection, PGresult *result);
extern PGconn * ConnectToNode(char *nodeName, int nodePort, char *nodeUser);
extern char * ConnectionGetOptionValue(PGconn *connection, char *optionKeyword);
#endif /* CONNECTION_CACHE_H */
