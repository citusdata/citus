/*-------------------------------------------------------------------------
 *
 * connection_cache.c
 *
 * This file contains functions to implement a connection hash.
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
#include <string.h>

#include "commands/dbcommands.h"
#include "distributed/connection_cache.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


/*
 * NodeConnectionHash is the connection hash itself. It begins uninitialized.
 * The first call to GetOrEstablishConnection triggers hash creation.
 */
static HTAB *NodeConnectionHash = NULL;


/* local function forward declarations */
static HTAB * CreateNodeConnectionHash(void);
static PGconn * ConnectToNode(char *nodeName, char *nodePort);
static char * ConnectionGetOptionValue(PGconn *connection, char *optionKeyword);


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
	PGconn *connection = NULL;
	NodeConnectionKey nodeConnectionKey;
	NodeConnectionEntry *nodeConnectionEntry = NULL;
	bool entryFound = false;
	bool needNewConnection = true;

	/* check input */
	if (strnlen(nodeName, MAX_NODE_LENGTH + 1) > MAX_NODE_LENGTH)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("hostname exceeds the maximum length of %d",
							   MAX_NODE_LENGTH)));
	}

	/* if first call, initialize the connection hash */
	if (NodeConnectionHash == NULL)
	{
		NodeConnectionHash = CreateNodeConnectionHash();
	}

	memset(&nodeConnectionKey, 0, sizeof(nodeConnectionKey));
	strncpy(nodeConnectionKey.nodeName, nodeName, MAX_NODE_LENGTH);
	nodeConnectionKey.nodePort = nodePort;

	nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
									  HASH_FIND, &entryFound);
	if (entryFound)
	{
		connection = nodeConnectionEntry->connection;
		if (PQstatus(connection) == CONNECTION_OK)
		{
			needNewConnection = false;
		}
		else
		{
			PurgeConnection(connection);
		}
	}

	if (needNewConnection)
	{
		StringInfo nodePortString = makeStringInfo();
		appendStringInfo(nodePortString, "%d", nodePort);

		connection = ConnectToNode(nodeName, nodePortString->data);
		if (connection != NULL)
		{
			nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
											  HASH_ENTER, &entryFound);
			nodeConnectionEntry->connection = connection;
		}
	}

	return connection;
}


/*
 * PurgeConnection removes the given connection from the connection hash and
 * closes it using PQfinish. If our hash does not contain the given connection,
 * this method simply prints a warning and exits.
 */
void
PurgeConnection(PGconn *connection)
{
	NodeConnectionKey nodeConnectionKey;
	NodeConnectionEntry *nodeConnectionEntry = NULL;
	bool entryFound = false;
	char *nodeNameString = NULL;
	char *nodePortString = NULL;

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

	memset(&nodeConnectionKey, 0, sizeof(nodeConnectionKey));
	strncpy(nodeConnectionKey.nodeName, nodeNameString, MAX_NODE_LENGTH);
	nodeConnectionKey.nodePort = pg_atoi(nodePortString, sizeof(int32), 0);

	pfree(nodeNameString);
	pfree(nodePortString);

	nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
									  HASH_REMOVE, &entryFound);
	if (entryFound)
	{
		/*
		 * It's possible the provided connection matches the host and port for
		 * an entry in the hash without being precisely the same connection. In
		 * that case, we will want to close the hash's connection (because the
		 * entry has already been removed) in addition to the provided one.
		 */
		if (nodeConnectionEntry->connection != connection)
		{
			ereport(WARNING, (errmsg("hash entry for \"%s:%d\" contained different "
									 "connection than that provided by caller",
									 nodeConnectionKey.nodeName,
									 nodeConnectionKey.nodePort)));
			PQfinish(nodeConnectionEntry->connection);
		}
	}
	else
	{
		ereport(WARNING, (errcode(ERRCODE_NO_DATA),
						  errmsg("could not find hash entry for connection to \"%s:%d\"",
								 nodeConnectionKey.nodeName,
								 nodeConnectionKey.nodePort)));
	}

	PQfinish(connection);
}


/*
 * ReportRemoteError retrieves various error fields from the a remote result and
 * produces an error report at the WARNING level.
 */
void
ReportRemoteError(PGconn *connection, PGresult *result)
{
	char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char *remoteMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	char *nodeName = ConnectionGetOptionValue(connection, "host");
	char *nodePort = ConnectionGetOptionValue(connection, "port");
	char *errorPrefix = "Connection failed to";
	int sqlState = ERRCODE_CONNECTION_FAILURE;

	if (sqlStateString != NULL)
	{
		sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
								 sqlStateString[3], sqlStateString[4]);

		/* use more specific error prefix for result failures */
		if (sqlState != ERRCODE_CONNECTION_FAILURE)
		{
			errorPrefix = "Bad result from";
		}
	}

	/*
	 * If the PGresult did not contain a message, the connection may provide a
	 * suitable top level one. At worst, this is an empty string.
	 */
	if (remoteMessage == NULL)
	{
		char *lastNewlineIndex = NULL;

		remoteMessage = PQerrorMessage(connection);
		lastNewlineIndex = strrchr(remoteMessage, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}
	}

	ereport(WARNING, (errcode(sqlState),
					  errmsg("%s %s:%s", errorPrefix, nodeName, nodePort),
					  errdetail("Remote message: %s", remoteMessage)));
}


/*
 * CreateNodeConnectionHash returns a newly created hash table suitable for
 * storing unlimited connections indexed by node name and port.
 */
static HTAB *
CreateNodeConnectionHash(void)
{
	HTAB *nodeConnectionHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(NodeConnectionKey);
	info.entrysize = sizeof(NodeConnectionEntry);
	info.hash = tag_hash;
	info.hcxt = CacheMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	nodeConnectionHash = hash_create("citus connection cache", 32, &info, hashFlags);

	return nodeConnectionHash;
}


/*
 * ConnectToNode opens a connection to a remote PostgreSQL server. The function
 * configures the connection's fallback application name to 'citus' and sets
 * the remote encoding to match the local one. This function requires that the
 * port be specified as a string for easier use with libpq functions.
 *
 * We attempt to connect up to MAX_CONNECT_ATTEMPT times. After that we give up
 * and return NULL.
 */
static PGconn *
ConnectToNode(char *nodeName, char *nodePort)
{
	PGconn *connection = NULL;
	const char *clientEncoding = GetDatabaseEncodingName();
	const char *dbname = get_database_name(MyDatabaseId);
	int attemptIndex = 0;

	const char *keywordArray[] = {
		"host", "port", "fallback_application_name",
		"client_encoding", "connect_timeout", "dbname", NULL
	};
	const char *valueArray[] = {
		nodeName, nodePort, "citus", clientEncoding,
		CLIENT_CONNECT_TIMEOUT_SECONDS, dbname, NULL
	};

	Assert(sizeof(keywordArray) == sizeof(valueArray));

	for (attemptIndex = 0; attemptIndex < MAX_CONNECT_ATTEMPTS; attemptIndex++)
	{
		connection = PQconnectdbParams(keywordArray, valueArray, false);
		if (PQstatus(connection) == CONNECTION_OK)
		{
			break;
		}
		else
		{
			/* warn if still erroring on final attempt */
			if (attemptIndex == MAX_CONNECT_ATTEMPTS - 1)
			{
				ReportRemoteError(connection, NULL);
			}

			PQfinish(connection);
			connection = NULL;
		}
	}

	return connection;
}


/*
 * ConnectionGetOptionValue inspects the provided connection for an option with
 * a given keyword and returns a new palloc'd string with that options's value.
 * The function returns NULL if the connection has no setting for an option with
 * the provided keyword.
 */
static char *
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
