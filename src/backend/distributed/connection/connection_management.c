/*-------------------------------------------------------------------------
 *
 * connection_management.c
 *   Central management of connections and their life-cycle
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"
#include "miscadmin.h"
#include "pg_config.h"
#include "pgstat.h"
#include "safe_lib.h"

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "mb/pg_wchar.h"
#include "portability/instr_time.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "distributed/backend_data.h"
#include "distributed/cancel_utils.h"
#include "distributed/connection_management.h"
#include "distributed/error_codes.h"
#include "distributed/errormessage.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/memutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/placement_connection.h"
#include "distributed/remote_commands.h"
#include "distributed/run_from_same_connection.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/time_constants.h"
#include "distributed/version_compat.h"
#include "distributed/worker_log_messages.h"


int NodeConnectionTimeout = 30000;
int MaxCachedConnectionsPerWorker = 1;
int MaxCachedConnectionLifetime = 10 * MS_PER_MINUTE;

HTAB *ConnectionHash = NULL;
HTAB *ConnParamsHash = NULL;

MemoryContext ConnectionContext = NULL;

static uint32 ConnectionHashHash(const void *key, Size keysize);
static int ConnectionHashCompare(const void *a, const void *b, Size keysize);
static void StartConnectionEstablishment(MultiConnection *connectionn,
										 ConnectionHashKey *key);
static MultiConnection * FindAvailableConnection(dlist_head *connections, uint32 flags);
static void ErrorIfMultipleMetadataConnectionExists(dlist_head *connections);
static void FreeConnParamsHashEntryFields(ConnParamsHashEntry *entry);
static void AfterXactHostConnectionHandling(ConnectionHashEntry *entry, bool isCommit);
static bool ShouldShutdownConnection(MultiConnection *connection, const int
									 cachedConnectionCount);
static bool RemoteTransactionIdle(MultiConnection *connection);
static int EventSetSizeForConnectionList(List *connections);


/* types for async connection management */
enum MultiConnectionPhase
{
	MULTI_CONNECTION_PHASE_CONNECTING,
	MULTI_CONNECTION_PHASE_CONNECTED,
	MULTI_CONNECTION_PHASE_ERROR,
};
typedef struct MultiConnectionPollState
{
	enum MultiConnectionPhase phase;
	MultiConnection *connection;
	PostgresPollingStatusType pollmode;
} MultiConnectionPollState;


/* helper functions for async connection management */
static bool MultiConnectionStatePoll(MultiConnectionPollState *connectionState);
static WaitEventSet * WaitEventSetFromMultiConnectionStates(List *connections,
															int *waitCount);
static void CloseNotReadyMultiConnectionStates(List *connectionStates);
static uint32 MultiConnectionStateEventMask(MultiConnectionPollState *connectionState);
static void CitusPQFinish(MultiConnection *connection);
static ConnParamsHashEntry * FindOrCreateConnParamsEntry(ConnectionHashKey *key);

/*
 * Initialize per-backend connection management infrastructure.
 */
void
InitializeConnectionManagement(void)
{
	HASHCTL info, connParamsInfo;

	/*
	 * Create a single context for connection and transaction related memory
	 * management. Doing so, instead of allocating in TopMemoryContext, makes
	 * it easier to associate used memory.
	 */
	ConnectionContext = AllocSetContextCreateInternal(TopMemoryContext,
													  "Connection Context",
													  ALLOCSET_DEFAULT_MINSIZE,
													  ALLOCSET_DEFAULT_INITSIZE,
													  ALLOCSET_DEFAULT_MAXSIZE);

	/* create (host,port,user,database) -> [connection] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionHashKey);
	info.entrysize = sizeof(ConnectionHashEntry);
	info.hash = ConnectionHashHash;
	info.match = ConnectionHashCompare;
	info.hcxt = ConnectionContext;
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	/* connParamsInfo is same as info, except for entrysize */
	connParamsInfo = info;
	connParamsInfo.entrysize = sizeof(ConnParamsHashEntry);

	ConnectionHash = hash_create("citus connection cache (host,port,user,database)",
								 64, &info, hashFlags);

	ConnParamsHash = hash_create("citus connparams cache (host,port,user,database)",
								 64, &connParamsInfo, hashFlags);
}


/*
 * InvalidateConnParamsHashEntries sets every hash entry's isValid flag to false.
 */
void
InvalidateConnParamsHashEntries(void)
{
	if (ConnParamsHash != NULL)
	{
		ConnParamsHashEntry *entry = NULL;
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, ConnParamsHash);
		while ((entry = (ConnParamsHashEntry *) hash_seq_search(&status)) != NULL)
		{
			entry->isValid = false;
		}
	}
}


/*
 * AfterXactConnectionHandling performs connection management activity after the end of a transaction. Both
 * COMMIT and ABORT paths are handled here.
 *
 * This is called by Citus' global transaction callback.
 */
void
AfterXactConnectionHandling(bool isCommit)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		if (!entry->isValid)
		{
			/* skip invalid connection hash entries */
			continue;
		}

		AfterXactHostConnectionHandling(entry, isCommit);

		/*
		 * NB: We leave the hash entry in place, even if there's no individual
		 * connections in it anymore. There seems no benefit in deleting it,
		 * and it'll save a bit of work in the next transaction.
		 */
	}
}


/*
 * GetNodeConnection() establishes a connection to remote node, using default
 * user and database.
 *
 * See StartNodeUserDatabaseConnection for details.
 */
MultiConnection *
GetNodeConnection(uint32 flags, const char *hostname, int32 port)
{
	return GetNodeUserDatabaseConnection(flags, hostname, port, NULL, NULL);
}


/*
 * StartNodeConnection initiates a connection to remote node, using default
 * user and database.
 *
 * See StartNodeUserDatabaseConnection for details.
 */
MultiConnection *
StartNodeConnection(uint32 flags, const char *hostname, int32 port)
{
	MultiConnection *connection = StartNodeUserDatabaseConnection(flags, hostname, port,
																  NULL, NULL);

	/*
	 * connection can only be NULL for optional connections, which we don't
	 * support in this codepath.
	 */
	Assert((flags & OPTIONAL_CONNECTION) == 0);
	Assert(connection != NULL);
	return connection;
}


/*
 * GetNodeUserDatabaseConnection establishes connection to remote node.
 *
 * See StartNodeUserDatabaseConnection for details.
 */
MultiConnection *
GetNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port,
							  const char *user, const char *database)
{
	MultiConnection *connection = StartNodeUserDatabaseConnection(flags, hostname, port,
																  user, database);

	/*
	 * connection can only be NULL for optional connections, which we don't
	 * support in this codepath.
	 */
	Assert((flags & OPTIONAL_CONNECTION) == 0);
	Assert(connection != NULL);

	FinishConnectionEstablishment(connection);

	return connection;
}


/*
 * GetConnectionForLocalQueriesOutsideTransaction returns a localhost connection for
 * subtransaction. To avoid creating excessive connections, we reuse an
 * existing connection.
 */
MultiConnection *
GetConnectionForLocalQueriesOutsideTransaction(char *userName)
{
	int connectionFlag = OUTSIDE_TRANSACTION;
	MultiConnection *connection =
		GetNodeUserDatabaseConnection(connectionFlag, LocalHostName, PostPortNumber,
									  userName, get_database_name(MyDatabaseId));

	return connection;
}


/*
 * StartNodeUserDatabaseConnection() initiates a connection to a remote node.
 *
 * If user or database are NULL, the current session's defaults are used. The
 * following flags influence connection establishment behaviour:
 * - FORCE_NEW_CONNECTION - a new connection is required
 *
 * The returned connection has only been initiated, not fully
 * established. That's useful to allow parallel connection establishment. If
 * that's not desired use the Get* variant.
 */
MultiConnection *
StartNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port,
								const char *user, const char *database)
{
	ConnectionHashKey key;
	bool found;

	/* do some minimal input checks */
	if (strlen(hostname) > MAX_NODE_LENGTH)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("hostname exceeds the maximum length of %d",
							   MAX_NODE_LENGTH)));
	}

	strlcpy(key.hostname, hostname, MAX_NODE_LENGTH);

	key.port = port;
	if (user)
	{
		strlcpy(key.user, user, NAMEDATALEN);
	}
	else
	{
		strlcpy(key.user, CurrentUserName(), NAMEDATALEN);
	}
	if (database)
	{
		strlcpy(key.database, database, NAMEDATALEN);
	}
	else
	{
		strlcpy(key.database, CurrentDatabaseName(), NAMEDATALEN);
	}

	if (flags & REQUIRE_REPLICATION_CONNECTION_PARAM)
	{
		key.replicationConnParam = true;
	}
	else
	{
		key.replicationConnParam = false;
	}

	if (CurrentCoordinatedTransactionState == COORD_TRANS_NONE)
	{
		CurrentCoordinatedTransactionState = COORD_TRANS_IDLE;
	}

	/*
	 * Lookup relevant hash entry. We always enter. If only a cached
	 * connection is desired, and there's none, we'll simply leave the
	 * connection list empty.
	 */

	ConnectionHashEntry *entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found || !entry->isValid)
	{
		/*
		 * We are just building hash entry or previously it was left in an
		 * invalid state as we couldn't allocate memory for it.
		 * So initialize entry->connections list here.
		 */
		entry->isValid = false;
		entry->connections = MemoryContextAlloc(ConnectionContext,
												sizeof(dlist_head));
		dlist_init(entry->connections);

		/*
		 * If MemoryContextAlloc errors out -e.g. during an OOM-, entry->connections
		 * stays as NULL. So entry->isValid should be set to true right after we
		 * initialize entry->connections properly.
		 */
		entry->isValid = true;
	}

	/* if desired, check whether there's a usable connection */
	if (!(flags & FORCE_NEW_CONNECTION))
	{
		/* check connection cache for a connection that's not already in use */
		MultiConnection *connection = FindAvailableConnection(entry->connections, flags);
		if (connection)
		{
			return connection;
		}
	}
	else if (flags & REQUIRE_METADATA_CONNECTION)
	{
		/* FORCE_NEW_CONNECTION and REQUIRE_METADATA_CONNECTION are incompatible */
		ereport(ERROR, (errmsg("metadata connections cannot be forced to open "
							   "a new connection")));
	}


	/*
	 * Either no caching desired, or no pre-established, non-claimed,
	 * connection present. Initiate connection establishment.
	 */
	MultiConnection *connection = MemoryContextAllocZero(ConnectionContext,
														 sizeof(MultiConnection));
	connection->initializationState = POOL_STATE_NOT_INITIALIZED;
	dlist_push_tail(entry->connections, &connection->connectionNode);

	/* these two flags are by nature cannot happen at the same time */
	Assert(!((flags & WAIT_FOR_CONNECTION) && (flags & OPTIONAL_CONNECTION)));

	if (flags & WAIT_FOR_CONNECTION)
	{
		WaitLoopForSharedConnection(hostname, port);
	}
	else if (flags & OPTIONAL_CONNECTION)
	{
		/*
		 * We can afford to skip establishing an optional connection. For
		 * non-optional connections, we first retry for some time. If we still
		 * cannot reserve the right to establish a connection, we prefer to
		 * error out.
		 */
		if (!TryToIncrementSharedConnectionCounter(hostname, port))
		{
			/* do not track the connection anymore */
			dlist_delete(&connection->connectionNode);
			pfree(connection);

			return NULL;
		}
	}
	else
	{
		/*
		 * The caller doesn't want the connection manager to wait
		 * until a connection slot is available on the remote node.
		 * In the end, we might fail to establish connection to the
		 * remote node as it might not have any space in
		 * max_connections for this connection establishment.
		 *
		 * Still, we keep track of the connection counter.
		 */
		IncrementSharedConnectionCounter(hostname, port);
	}


	/*
	 * We've already incremented the counter above, so we should decrement
	 * when we're done with the connection.
	 */
	connection->initializationState = POOL_STATE_COUNTER_INCREMENTED;

	StartConnectionEstablishment(connection, &key);

	ResetShardPlacementAssociation(connection);


	if ((flags & REQUIRE_METADATA_CONNECTION))
	{
		connection->useForMetadataOperations = true;
	}

	/* fully initialized the connection, record it */
	connection->initializationState = POOL_STATE_INITIALIZED;

	return connection;
}


/*
 * FindAvailableConnection searches the given list of connections for one that
 * is not claimed exclusively.
 *
 * If no connection is available, FindAvailableConnection returns NULL.
 */
static MultiConnection *
FindAvailableConnection(dlist_head *connections, uint32 flags)
{
	List *metadataConnectionCandidateList = NIL;

	dlist_iter iter;
	dlist_foreach(iter, connections)
	{
		MultiConnection *connection =
			dlist_container(MultiConnection, connectionNode, iter.cur);

		if (flags & OUTSIDE_TRANSACTION)
		{
			/* don't return connections that are used in transactions */
			if (connection->remoteTransaction.transactionState !=
				REMOTE_TRANS_NOT_STARTED)
			{
				continue;
			}
		}

		/* don't return claimed connections */
		if (connection->claimedExclusively)
		{
			/* connection is in use for an ongoing operation */
			continue;
		}

		if (connection->forceCloseAtTransactionEnd &&
			!connection->remoteTransaction.beginSent)
		{
			/*
			 * This is a connection that should be closed, probably because
			 * of old connection options or removing a node. This will
			 * automatically be closed at the end of the transaction. But, if we are still
			 * inside a transaction, we should keep using this connection as long as a remote
			 * transaction is in progress over the connection. The main use for this case
			 * is having some commands inside a transaction block after removing nodes. And, we
			 * currently allow very limited operations after removing a node inside a
			 * transaction block (e.g., no placement access can happen).
			 */
			continue;
		}

		if (connection->initializationState != POOL_STATE_INITIALIZED)
		{
			/*
			 * If the connection has not been initialized, it should not be
			 * considered as available.
			 */
			continue;
		}

		if ((flags & REQUIRE_METADATA_CONNECTION) &&
			!connection->useForMetadataOperations)
		{
			/*
			 * The caller requested a metadata connection, and this is not the
			 * metadata connection. Still, this is a candidate for becoming a
			 * metadata connection.
			 */
			metadataConnectionCandidateList =
				lappend(metadataConnectionCandidateList, connection);
			continue;
		}

		return connection;
	}

	if ((flags & REQUIRE_METADATA_CONNECTION) &&
		list_length(metadataConnectionCandidateList) > 0)
	{
		/*
		 * Caller asked a metadata connection, and we couldn't find a connection
		 * that has already been used for metadata operations.
		 *
		 * So, we pick the first connection as the metadata connection.
		 */
		MultiConnection *metadataConnection =
			linitial(metadataConnectionCandidateList);

		Assert(!metadataConnection->claimedExclusively);

		/* remember that we use this connection for metadata operations */
		metadataConnection->useForMetadataOperations = true;

		/*
		 * We cannot have multiple metadata connections. If we see
		 * this error, it is likely that there is a bug in connection
		 * management.
		 */
		ErrorIfMultipleMetadataConnectionExists(connections);

		return metadataConnection;
	}

	return NULL;
}


/*
 * ErrorIfMultipleMetadataConnectionExists throws an error if the
 * input connection dlist contains more than one metadata connections.
 */
static void
ErrorIfMultipleMetadataConnectionExists(dlist_head *connections)
{
	bool foundMetadataConnection = false;
	dlist_iter iter;
	dlist_foreach(iter, connections)
	{
		MultiConnection *connection =
			dlist_container(MultiConnection, connectionNode, iter.cur);

		if (connection->useForMetadataOperations)
		{
			if (foundMetadataConnection)
			{
				ereport(ERROR, (errmsg("cannot have multiple metadata connections")));
			}

			foundMetadataConnection = true;
		}
	}
}


/*
 * CloseAllConnectionsAfterTransaction sets the forceClose flag of all the
 * connections. This is mainly done when citus.node_conninfo changes.
 */
void
CloseAllConnectionsAfterTransaction(void)
{
	if (ConnectionHash == NULL)
	{
		return;
	}
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		if (!entry->isValid)
		{
			/* skip invalid connection hash entries */
			continue;
		}

		dlist_iter iter;

		dlist_head *connections = entry->connections;
		dlist_foreach(iter, connections)
		{
			MultiConnection *connection =
				dlist_container(MultiConnection, connectionNode, iter.cur);

			connection->forceCloseAtTransactionEnd = true;
		}
	}
}


/*
 * ConnectionAvailableToNode returns a MultiConnection if the session has at least
 * one connection established and avaliable to use to the give node. Else, returns
 * false.
 */
MultiConnection *
ConnectionAvailableToNode(char *hostName, int nodePort, const char *userName,
						  const char *database)
{
	ConnectionHashKey key;
	bool found = false;

	strlcpy(key.hostname, hostName, MAX_NODE_LENGTH);
	key.port = nodePort;
	strlcpy(key.user, userName, NAMEDATALEN);
	strlcpy(key.database, database, NAMEDATALEN);
	key.replicationConnParam = false;

	ConnectionHashEntry *entry =
		(ConnectionHashEntry *) hash_search(ConnectionHash, &key, HASH_FIND, &found);

	if (!found || !entry->isValid)
	{
		return false;
	}

	int flags = 0;
	MultiConnection *connection = FindAvailableConnection(entry->connections, flags);

	return connection;
}


/*
 * CloseNodeConnectionsAfterTransaction sets the forceClose flag of the connections
 * to a particular node as true such that the connections are no longer cached. This
 * is mainly used when a worker leaves the cluster.
 */
void
CloseNodeConnectionsAfterTransaction(char *nodeName, int nodePort)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		if (!entry->isValid)
		{
			/* skip invalid connection hash entries */
			continue;
		}

		dlist_iter iter;

		if (strcmp(entry->key.hostname, nodeName) != 0 || entry->key.port != nodePort)
		{
			continue;
		}

		dlist_head *connections = entry->connections;
		dlist_foreach(iter, connections)
		{
			MultiConnection *connection =
				dlist_container(MultiConnection, connectionNode, iter.cur);

			connection->forceCloseAtTransactionEnd = true;
		}
	}
}


/*
 * Close a previously established connection.
 */
void
CloseConnection(MultiConnection *connection)
{
	ConnectionHashKey key;
	bool found;

	/* close connection */
	CitusPQFinish(connection);

	strlcpy(key.hostname, connection->hostname, MAX_NODE_LENGTH);
	key.port = connection->port;
	key.replicationConnParam = connection->requiresReplication;
	strlcpy(key.user, connection->user, NAMEDATALEN);
	strlcpy(key.database, connection->database, NAMEDATALEN);

	hash_search(ConnectionHash, &key, HASH_FIND, &found);

	if (found)
	{
		/* unlink from list of open connections */
		dlist_delete(&connection->connectionNode);

		/* same for transaction state and shard/placement machinery */
		CloseShardPlacementAssociation(connection);
		ResetRemoteTransaction(connection);

		/* we leave the per-host entry alive */
		pfree(connection);
	}
	else
	{
		ereport(ERROR, (errmsg("closing untracked connection")));
	}
}


/*
 * ShutdownAllConnections shutdowns all the MultiConnections in the
 * ConnectionHash.
 *
 * This function is intended to be called atexit() of the backend, so
 * that the cached connections are closed properly. Calling this function
 * at another point in the code could be dangerous, so think twice if you
 * need to call this function.
 */
void
ShutdownAllConnections(void)
{
	ConnectionHashEntry *entry = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != NULL)
	{
		if (!entry->isValid)
		{
			/* skip invalid connection hash entries */
			continue;
		}

		dlist_iter iter;

		dlist_foreach(iter, entry->connections)
		{
			MultiConnection *connection =
				dlist_container(MultiConnection, connectionNode, iter.cur);

			ShutdownConnection(connection);
		}
	}
}


/*
 * ShutdownConnection, if necessary cancels the currently running statement,
 * and then closes the underlying libpq connection.  The MultiConnection
 * itself is left intact.
 *
 * NB: Cancelling a statement requires network IO, and currently is not
 * interruptible. Unfortunately libpq does not provide a non-blocking
 * implementation of PQcancel(), so we don't have much choice for now.
 */
void
ShutdownConnection(MultiConnection *connection)
{
	/*
	 * Only cancel statement if there's currently one running, and the
	 * connection is in an OK state.
	 */
	if (PQstatus(connection->pgConn) == CONNECTION_OK &&
		PQtransactionStatus(connection->pgConn) == PQTRANS_ACTIVE)
	{
		SendCancelationRequest(connection);
	}
	CitusPQFinish(connection);
}


/*
 * MultiConnectionStatePoll executes a PQconnectPoll on the connection to progress the
 * connection establishment. The return value of this function indicates if the
 * MultiConnectionPollState has been changed, which could require a change to the WaitEventSet
 */
static bool
MultiConnectionStatePoll(MultiConnectionPollState *connectionState)
{
	MultiConnection *connection = connectionState->connection;
	ConnStatusType status = PQstatus(connection->pgConn);
	PostgresPollingStatusType oldPollmode = connectionState->pollmode;

	Assert(connectionState->phase == MULTI_CONNECTION_PHASE_CONNECTING);

	if (status == CONNECTION_OK)
	{
		connectionState->phase = MULTI_CONNECTION_PHASE_CONNECTED;
		return true;
	}
	else if (status == CONNECTION_BAD)
	{
		/* FIXME: retries? */
		connectionState->phase = MULTI_CONNECTION_PHASE_ERROR;
		return true;
	}
	else
	{
		connectionState->phase = MULTI_CONNECTION_PHASE_CONNECTING;
	}

	connectionState->pollmode = PQconnectPoll(connection->pgConn);

	/*
	 * FIXME: Do we want to add transparent retry support here?
	 */
	if (connectionState->pollmode == PGRES_POLLING_FAILED)
	{
		connectionState->phase = MULTI_CONNECTION_PHASE_ERROR;
		return true;
	}
	else if (connectionState->pollmode == PGRES_POLLING_OK)
	{
		connectionState->phase = MULTI_CONNECTION_PHASE_CONNECTED;
		return true;
	}
	else
	{
		Assert(connectionState->pollmode == PGRES_POLLING_WRITING ||
			   connectionState->pollmode == PGRES_POLLING_READING);
	}

	return (oldPollmode != connectionState->pollmode) ? true : false;
}


/*
 * EventSetSizeForConnectionList calculates the space needed for a WaitEventSet based on a
 * list of connections.
 */
inline static int
EventSetSizeForConnectionList(List *connections)
{
	/* we need space for 2 postgres events in the waitset on top of the connections */
	return list_length(connections) + 2;
}


/*
 * WaitEventSetFromMultiConnectionStates takes a list of MultiConnectionStates and adds
 * all sockets of the connections that are still in the connecting phase to a WaitSet,
 * taking into account the maximum number of connections that could be added in total to
 * a WaitSet.
 *
 * waitCount populates the number of connections added to the WaitSet in case when a
 * non-NULL pointer is provided.
 */
static WaitEventSet *
WaitEventSetFromMultiConnectionStates(List *connections, int *waitCount)
{
	const int eventSetSize = EventSetSizeForConnectionList(connections);
	int numEventsAdded = 0;

	if (waitCount)
	{
		*waitCount = 0;
	}

	WaitEventSet *waitEventSet = CreateWaitEventSet(WaitEventSetTracker_compat,
													eventSetSize);
	EnsureReleaseResource((MemoryContextCallbackFunction) (&FreeWaitEventSet),
						  waitEventSet);

	/*
	 * Put the wait events for the signal latch and postmaster death at the end such that
	 * event index + pendingConnectionsStartIndex = the connection index in the array.
	 */
	AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(waitEventSet, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
	numEventsAdded += 2;

	MultiConnectionPollState *connectionState = NULL;
	foreach_declared_ptr(connectionState, connections)
	{
		if (numEventsAdded >= eventSetSize)
		{
			/* room for events to schedule is exhausted */
			break;
		}

		if (connectionState->phase != MULTI_CONNECTION_PHASE_CONNECTING)
		{
			/* connections that are not connecting will not be added to the WaitSet */
			continue;
		}

		int sock = PQsocket(connectionState->connection->pgConn);

		int eventMask = MultiConnectionStateEventMask(connectionState);

		int waitEventSetIndex =
			CitusAddWaitEventSetToSet(waitEventSet, eventMask, sock,
									  NULL, (void *) connectionState);
		if (waitEventSetIndex == WAIT_EVENT_SET_INDEX_FAILED)
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("connection establishment for node %s:%d failed",
								   connectionState->connection->hostname,
								   connectionState->connection->port),
							errhint("Check both the local and remote server logs for the "
									"connection establishment errors.")));
		}

		numEventsAdded++;

		if (waitCount)
		{
			*waitCount = *waitCount + 1;
		}
	}

	return waitEventSet;
}


/*
 * MultiConnectionStateEventMask returns the eventMask use by the WaitEventSet for the
 * for the socket associated with the connection based on the pollmode PQconnectPoll
 * returned in its last invocation
 */
static uint32
MultiConnectionStateEventMask(MultiConnectionPollState *connectionState)
{
	uint32 eventMask = 0;
	if (connectionState->pollmode == PGRES_POLLING_READING)
	{
		eventMask |= WL_SOCKET_READABLE;
	}
	else
	{
		eventMask |= WL_SOCKET_WRITEABLE;
	}
	return eventMask;
}


/*
 * FinishConnectionListEstablishment takes a list of MultiConnection and finishes the
 * connections establishment asynchronously for all connections not already fully
 * connected.
 */
void
FinishConnectionListEstablishment(List *multiConnectionList)
{
	instr_time connectionStart;
	INSTR_TIME_SET_CURRENT(connectionStart);

	List *connectionStates = NULL;

	WaitEventSet *waitEventSet = NULL;
	bool waitEventSetRebuild = true;
	int waitCount = 0;

	MultiConnection *connection = NULL;
	foreach_declared_ptr(connection, multiConnectionList)
	{
		MultiConnectionPollState *connectionState =
			palloc0(sizeof(MultiConnectionPollState));

		connectionState->connection = connection;

		/*
		 * before we can build the waitset to wait for asynchronous IO we need to know the
		 * pollmode to use for the sockets. This is populated by executing one round of
		 * PQconnectPoll. This updates the MultiConnectionPollState struct with its phase and
		 * its next poll mode.
		 */
		MultiConnectionStatePoll(connectionState);

		connectionStates = lappend(connectionStates, connectionState);
		if (connectionState->phase == MULTI_CONNECTION_PHASE_CONNECTING)
		{
			waitCount++;
		}
	}

	/* prepare space for socket events */
	WaitEvent *events = (WaitEvent *) palloc0(EventSetSizeForConnectionList(
												  connectionStates) *
											  sizeof(WaitEvent));

	/*
	 * for high connection counts with lots of round trips we could potentially have a lot
	 * of (big) waitsets that we'd like to clean right after we have used them. To do this
	 * we switch to a temporary memory context for this loop which gets reset at the end
	 */
	MemoryContext oldContext = MemoryContextSwitchTo(
		AllocSetContextCreate(CurrentMemoryContext,
							  "connection establishment temporary context",
							  ALLOCSET_DEFAULT_SIZES));
	while (waitCount > 0)
	{
		long timeout = MillisecondsToTimeout(connectionStart, NodeConnectionTimeout);

		if (waitEventSetRebuild)
		{
			MemoryContextReset(CurrentMemoryContext);
			waitEventSet = WaitEventSetFromMultiConnectionStates(connectionStates,
																 &waitCount);
			waitEventSetRebuild = false;

			if (waitCount <= 0)
			{
				break;
			}
		}

		int eventCount = WaitEventSetWait(waitEventSet, timeout, events, waitCount,
										  WAIT_EVENT_CLIENT_READ);

		for (int eventIndex = 0; eventIndex < eventCount; eventIndex++)
		{
			WaitEvent *event = &events[eventIndex];
			MultiConnectionPollState *connectionState =
				(MultiConnectionPollState *) event->user_data;

			if (event->events & WL_POSTMASTER_DEATH)
			{
				ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
			}

			if (event->events & WL_LATCH_SET)
			{
				ResetLatch(MyLatch);

				CHECK_FOR_INTERRUPTS();

				if (IsHoldOffCancellationReceived())
				{
					/*
					 * because we can't break from 2 loops easily we need to not forget to
					 * reset the memory context
					 */
					MemoryContextDelete(MemoryContextSwitchTo(oldContext));
					return;
				}

				continue;
			}
			bool beforePollSocket = PQsocket(connectionState->connection->pgConn);
			bool connectionStateChanged = MultiConnectionStatePoll(connectionState);

			if (beforePollSocket != PQsocket(connectionState->connection->pgConn))
			{
				/* rebuild the wait events if MultiConnectionStatePoll() changed the socket */
				waitEventSetRebuild = true;
			}

			if (connectionStateChanged)
			{
				if (connectionState->phase != MULTI_CONNECTION_PHASE_CONNECTING)
				{
					/* we cannot stop waiting for connection, so rebuild the event set */
					waitEventSetRebuild = true;
				}
				else
				{
					/* connection state changed, reset the event mask */
					uint32 eventMask = MultiConnectionStateEventMask(connectionState);
					bool success =
						CitusModifyWaitEvent(waitEventSet, event->pos,
											 eventMask, NULL);
					if (!success)
					{
						ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
										errmsg("connection establishment for node %s:%d "
											   "failed", connection->hostname,
											   connection->port),
										errhint("Check both the local and remote server "
												"logs for the connection establishment "
												"errors.")));
					}
				}

				/*
				 * The state has changed to connected, update the connection's
				 * state as well.
				 */
				if (connectionState->phase == MULTI_CONNECTION_PHASE_CONNECTED)
				{
					MarkConnectionConnected(connectionState->connection);
				}
			}
		}

		if (eventCount == 0)
		{
			/*
			 * timeout has occurred on waitset, double check the timeout since
			 * connectionStart and if passed close all non-finished connections
			 */

			if (MillisecondsPassedSince(connectionStart) >= NodeConnectionTimeout)
			{
				/*
				 * showing as a warning, can't be an error. In some cases queries can
				 * proceed with only some of the connections being fully established.
				 * Queries that can't will error then and there
				 */
				ereport(WARNING, (errmsg("could not establish connection after %u ms",
										 NodeConnectionTimeout)));

				/*
				 * Close all connections that have not been fully established.
				 */
				CloseNotReadyMultiConnectionStates(connectionStates);

				break;
			}
		}
	}
	MemoryContextDelete(MemoryContextSwitchTo(oldContext));
}


/*
 * MillisecondsPassedSince returns the number of milliseconds elapsed between an
 * instr_time & the current time.
 */
double
MillisecondsPassedSince(instr_time moment)
{
	instr_time timeSinceMoment;
	INSTR_TIME_SET_CURRENT(timeSinceMoment);
	INSTR_TIME_SUBTRACT(timeSinceMoment, moment);
	return INSTR_TIME_GET_MILLISEC(timeSinceMoment);
}


/*
 * MillisecondsToTimeout returns the numer of milliseconds that still need to elapse
 * before msAfterStart milliseconds have passed since start. The outcome can be used to
 * pass to the Wait of an EventSet to make sure it returns after the timeout has passed.
 */
long
MillisecondsToTimeout(instr_time start, long msAfterStart)
{
	return msAfterStart - MillisecondsPassedSince(start);
}


/*
 * CloseNotReadyMultiConnectionStates calls CloseConnection for all MultiConnection's
 * tracked in the MultiConnectionPollState list passed in, only if the connection is not yet
 * fully established.
 *
 * This function removes the pointer to the MultiConnection data after the Connections are
 * closed since they should not be used anymore.
 */
static void
CloseNotReadyMultiConnectionStates(List *connectionStates)
{
	MultiConnectionPollState *connectionState = NULL;
	foreach_declared_ptr(connectionState, connectionStates)
	{
		MultiConnection *connection = connectionState->connection;

		if (connectionState->phase != MULTI_CONNECTION_PHASE_CONNECTING)
		{
			continue;
		}

		/* close connection, otherwise we take up resource on the other side */
		CitusPQFinish(connection);
	}
}


/*
 * CitusPQFinish is a wrapper around PQfinish and does book keeping on shared connection
 * counters.
 */
static void
CitusPQFinish(MultiConnection *connection)
{
	if (connection->pgConn != NULL)
	{
		PQfinish(connection->pgConn);
		connection->pgConn = NULL;
	}

	/* behave idempotently, there is no gurantee that CitusPQFinish() is called once */
	if (connection->initializationState >= POOL_STATE_COUNTER_INCREMENTED)
	{
		DecrementSharedConnectionCounter(connection->hostname, connection->port);
		connection->initializationState = POOL_STATE_NOT_INITIALIZED;
	}
}


/*
 * Close connections on timeout in FinishConnectionListEstablishment
 * Synchronously finish connection establishment of an individual connection.
 * This function is a convenience wrapped around FinishConnectionListEstablishment.
 */
void
FinishConnectionEstablishment(MultiConnection *connection)
{
	FinishConnectionListEstablishment(list_make1(connection));
}


/*
 * ForceConnectionCloseAtTransactionEnd marks connection to be closed at the end of the
 * transaction.
 */
void
ForceConnectionCloseAtTransactionEnd(MultiConnection *connection)
{
	connection->forceCloseAtTransactionEnd = true;
}


/*
 * ClaimConnectionExclusively signals that this connection is actively being
 * used. That means it'll not be, again, returned by
 * StartNodeUserDatabaseConnection() et al until releases with
 * UnclaimConnection().
 */
void
ClaimConnectionExclusively(MultiConnection *connection)
{
	Assert(!connection->claimedExclusively);
	connection->claimedExclusively = true;
}


/*
 * UnclaimConnection signals that this connection is not being used
 * anymore. That means it again may be returned by
 * StartNodeUserDatabaseConnection() et al.
 */
void
UnclaimConnection(MultiConnection *connection)
{
	connection->claimedExclusively = false;
}


static uint32
ConnectionHashHash(const void *key, Size keysize)
{
	ConnectionHashKey *entry = (ConnectionHashKey *) key;

	uint32 hash = string_hash(entry->hostname, NAMEDATALEN);
	hash = hash_combine(hash, hash_uint32(entry->port));
	hash = hash_combine(hash, string_hash(entry->user, NAMEDATALEN));
	hash = hash_combine(hash, string_hash(entry->database, NAMEDATALEN));
	hash = hash_combine(hash, hash_uint32(entry->replicationConnParam));

	return hash;
}


static int
ConnectionHashCompare(const void *a, const void *b, Size keysize)
{
	ConnectionHashKey *ca = (ConnectionHashKey *) a;
	ConnectionHashKey *cb = (ConnectionHashKey *) b;

	if (strncmp(ca->hostname, cb->hostname, MAX_NODE_LENGTH) != 0 ||
		ca->port != cb->port ||
		ca->replicationConnParam != cb->replicationConnParam ||
		strncmp(ca->user, cb->user, NAMEDATALEN) != 0 ||
		strncmp(ca->database, cb->database, NAMEDATALEN) != 0)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}


/*
 * Asynchronously establish connection to a remote node, but don't wait for
 * that to finish. DNS lookups etc. are performed synchronously though.
 */
static void
StartConnectionEstablishment(MultiConnection *connection, ConnectionHashKey *key)
{
	static uint64 connectionId = 1;

	ConnParamsHashEntry *entry = FindOrCreateConnParamsEntry(key);

	strlcpy(connection->hostname, key->hostname, MAX_NODE_LENGTH);
	connection->port = key->port;
	strlcpy(connection->database, key->database, NAMEDATALEN);
	strlcpy(connection->user, key->user, NAMEDATALEN);
	connection->requiresReplication = key->replicationConnParam;

	connection->pgConn = PQconnectStartParams((const char **) entry->keywords,
											  (const char **) entry->values,
											  false);
	INSTR_TIME_SET_CURRENT(connection->connectionEstablishmentStart);

	/* do not increment for restarted connections */
	if (connection->connectionId == 0)
	{
		connection->connectionId = connectionId++;
	}

	/*
	 * To avoid issues with interrupts not getting caught all our connections
	 * are managed in a non-blocking manner. remote_commands.c provides
	 * wrappers emulating blocking behaviour.
	 */
	PQsetnonblocking(connection->pgConn, true);

	SetCitusNoticeReceiver(connection);
}


/*
 * FindOrCreateConnParamsEntry searches ConnParamsHash for the given key,
 * if it is not found, it is created.
 */
static ConnParamsHashEntry *
FindOrCreateConnParamsEntry(ConnectionHashKey *key)
{
	bool found = false;

	/* search our cache for precomputed connection settings */
	ConnParamsHashEntry *entry = hash_search(ConnParamsHash, key, HASH_ENTER, &found);
	if (!found || !entry->isValid)
	{
		if (!found)
		{
			/*
			 * Zero out entry, but not the key part.
			 * Avoids leaving invalid pointers in hash table if GetConnParam throws with MemoryContextAllocZero.
			 */
			memset(((char *) entry) + sizeof(ConnectionHashKey), 0,
				   sizeof(ConnParamsHashEntry) - sizeof(ConnectionHashKey));
		}

		/* avoid leaking memory in the keys and values arrays */
		if (found && !entry->isValid)
		{
			FreeConnParamsHashEntryFields(entry);
		}

		/* if not found or not valid, compute them from GUC, runtime, etc. */
		GetConnParams(key, &entry->keywords, &entry->values, &entry->runtimeParamStart,
					  ConnectionContext);

		entry->isValid = true;
	}

	return entry;
}


/*
 * FreeConnParamsHashEntryFields frees any dynamically allocated memory reachable
 * from the fields of the provided ConnParamsHashEntry. This includes all runtime
 * libpq keywords and values, as well as the actual arrays storing them.
 */
static void
FreeConnParamsHashEntryFields(ConnParamsHashEntry *entry)
{
	/*
	 * if there was a memory error during the initialization of ConnParamHashEntry in
	 * GetConnParams the keywords or values might not have been initialized completely.
	 * We check if they have been initialized before freeing them.
	 *
	 * We only iteratively free the lists starting at the index pointed to by
	 * entry->runtimeParamStart as all entries before are settings that are managed
	 * separately.
	 */

	if (entry->keywords != NULL)
	{
		char **keyword = &entry->keywords[entry->runtimeParamStart];
		while (*keyword != NULL)
		{
			pfree(*keyword);
			keyword++;
		}
		pfree(entry->keywords);
		entry->keywords = NULL;
	}

	if (entry->values != NULL)
	{
		char **value = &entry->values[entry->runtimeParamStart];
		while (*value != NULL)
		{
			pfree(*value);
			value++;
		}
		pfree(entry->values);
		entry->values = NULL;
	}

	entry->runtimeParamStart = 0;
}


/*
 * AfterXactHostConnectionHandling closes all remote connections if not necessary anymore (i.e. not session
 * lifetime), or if in a failed state.
 */
static void
AfterXactHostConnectionHandling(ConnectionHashEntry *entry, bool isCommit)
{
	if (!entry || !entry->isValid)
	{
		/* callers only pass valid hash entries but let's be on the safe side */
		ereport(ERROR, (errmsg("connection hash entry is NULL or invalid")));
	}

	dlist_mutable_iter iter;
	int cachedConnectionCount = 0;

	dlist_foreach_modify(iter, entry->connections)
	{
		MultiConnection *connection =
			dlist_container(MultiConnection, connectionNode, iter.cur);

		/*
		 * To avoid leaking connections we warn if connections are
		 * still claimed exclusively. We can only do so if the transaction is
		 * committed, as it's normal that code didn't have chance to clean
		 * up after errors.
		 */
		if (isCommit && connection->claimedExclusively)
		{
			ereport(WARNING,
					(errmsg("connection claimed exclusively at transaction commit")));
		}


		if (ShouldShutdownConnection(connection, cachedConnectionCount))
		{
			ShutdownConnection(connection);

			/* remove from transactionlist before free-ing */
			ResetRemoteTransaction(connection);

			/* unlink from list */
			dlist_delete(iter.cur);

			pfree(connection);
		}
		else
		{
			/*
			 * reset healthy session lifespan connections.
			 */
			ResetRemoteTransaction(connection);

			UnclaimConnection(connection);


			cachedConnectionCount++;
		}
	}
}


/*
 * ShouldShutdownConnection returns true if either one of the followings is true:
 * - The connection is citus initiated.
 * - Current cached connections is already at MaxCachedConnectionsPerWorker
 * - Connection is forced to close at the end of transaction
 * - Connection is not in OK state
 * - Connection has a replication origin setup
 * - A transaction is still in progress (usually because we are cancelling a distributed transaction)
 * - A connection reached its maximum lifetime
 */
static bool
ShouldShutdownConnection(MultiConnection *connection, const int cachedConnectionCount)
{
	/*
	 * When we are in a backend that was created to serve an internal connection
	 * from the coordinator or another worker, we disable connection caching to avoid
	 * escalating the number of cached connections. We can recognize such backends
	 * from their application name.
	 */
	return (IsCitusInternalBackend() || IsRebalancerInternalBackend()) ||
		   connection->initializationState != POOL_STATE_INITIALIZED ||
		   cachedConnectionCount >= MaxCachedConnectionsPerWorker ||
		   connection->forceCloseAtTransactionEnd ||
		   PQstatus(connection->pgConn) != CONNECTION_OK ||
		   !RemoteTransactionIdle(connection) ||
		   connection->requiresReplication ||
		   connection->isReplicationOriginSessionSetup ||
		   (MaxCachedConnectionLifetime >= 0 &&
			MillisecondsToTimeout(connection->connectionEstablishmentStart,
								  MaxCachedConnectionLifetime) <= 0);
}


/*
 * RestartConnection starts a new connection attempt for the given
 * MultiConnection.
 *
 * The internal state of the MultiConnection is preserved. For example, we
 * assume that we already went through all the other initialization steps in
 * StartNodeUserDatabaseConnection, such as incrementing shared connection
 * counters.
 *
 * This function should be used cautiously. If a connection is already
 * involved in a remote transaction, we cannot restart the underlying
 * connection. The caller is responsible for enforcing the restrictions
 * on this.
 */
void
RestartConnection(MultiConnection *connection)
{
	/* we cannot restart any connection that refers to a placement */
	Assert(dlist_is_empty(&connection->referencedPlacements));

	/* we cannot restart any connection that is part of a transaction */
	Assert(connection->remoteTransaction.transactionState == REMOTE_TRANS_NOT_STARTED);

	ConnectionHashKey key;
	strlcpy(key.hostname, connection->hostname, MAX_NODE_LENGTH);
	key.port = connection->port;
	strlcpy(key.user, connection->user, NAMEDATALEN);
	strlcpy(key.database, connection->database, NAMEDATALEN);
	key.replicationConnParam = connection->requiresReplication;

	/*
	 * With low-level APIs, we shutdown and restart the connection.
	 * The main trick here is that we are using the same MultiConnection *
	 * such that all the state of the connection is preserved.
	 */
	ShutdownConnection(connection);
	StartConnectionEstablishment(connection, &key);

	/*
	 * We are restarting an already initialized connection which has
	 * gone through StartNodeUserDatabaseConnection(). That's why we
	 * can safely mark the state initialized.
	 *
	 * Not that we have to do this because ShutdownConnection() sets the
	 * state to not initialized.
	 */
	connection->initializationState = POOL_STATE_INITIALIZED;
	connection->connectionState = MULTI_CONNECTION_CONNECTING;
}


/*
 * RemoteTransactionIdle function returns true if we manually
 * set flag on run_commands_on_session_level_connection_to_node to true to
 * force connection API keeping connection open or the status of the connection
 * is idle.
 */
static bool
RemoteTransactionIdle(MultiConnection *connection)
{
	/*
	 * This is a very special case where we're running isolation tests on MX.
	 * We don't care whether the transaction is idle or not when we're
	 * running MX isolation tests. Thus, let the caller act as if the remote
	 * transactions is idle.
	 */
	if (AllowNonIdleTransactionOnXactHandling())
	{
		return true;
	}

	return PQtransactionStatus(connection->pgConn) == PQTRANS_IDLE;
}


/*
 * MarkConnectionConnected is a helper function which sets the  connection
 * connectionState to MULTI_CONNECTION_CONNECTED, and also updates connection
 * establishment time when necessary.
 */
void
MarkConnectionConnected(MultiConnection *connection)
{
	connection->connectionState = MULTI_CONNECTION_CONNECTED;

	if (INSTR_TIME_IS_ZERO(connection->connectionEstablishmentEnd))
	{
		INSTR_TIME_SET_CURRENT(connection->connectionEstablishmentEnd);
	}
}


/*
 * CitusAddWaitEventSetToSet is a wrapper around Postgres' AddWaitEventToSet().
 *
 * AddWaitEventToSet() may throw hard errors. For example, when the
 * underlying socket for a connection is closed by the remote server
 * and already reflected by the OS, however Citus hasn't had a chance
 * to get this information. In that case, if replication factor is >1,
 * Citus can failover to other nodes for executing the query. Even if
 * replication factor = 1, Citus can give much nicer errors.
 *
 * So CitusAddWaitEventSetToSet simply puts ModifyWaitEvent into a
 * PG_TRY/PG_CATCH block in order to catch any hard errors, and
 * returns this information to the caller.
 */
int
CitusAddWaitEventSetToSet(WaitEventSet *set, uint32 events, pgsocket fd,
						  Latch *latch, void *user_data)
{
	volatile int waitEventSetIndex = WAIT_EVENT_SET_INDEX_NOT_INITIALIZED;
	MemoryContext savedContext = CurrentMemoryContext;

	PG_TRY();
	{
		waitEventSetIndex =
			AddWaitEventToSet(set, events, fd, latch, (void *) user_data);
	}
	PG_CATCH();
	{
		/*
		 * We might be in an arbitrary memory context when the
		 * error is thrown and we should get back to one we had
		 * at PG_TRY() time, especially because we are not
		 * re-throwing the error.
		 */
		MemoryContextSwitchTo(savedContext);

		FlushErrorState();

		/* let the callers know about the failure */
		waitEventSetIndex = WAIT_EVENT_SET_INDEX_FAILED;
	}
	PG_END_TRY();

	return waitEventSetIndex;
}


/*
 * CitusModifyWaitEvent is a wrapper around Postgres' ModifyWaitEvent().
 *
 * ModifyWaitEvent may throw hard errors. For example, when the underlying
 * socket for a connection is closed by the remote server and already
 * reflected by the OS, however Citus hasn't had a chance to get this
 * information. In that case, if replication factor is >1, Citus can
 * failover to other nodes for executing the query. Even if replication
 * factor = 1, Citus can give much nicer errors.
 *
 * So CitusModifyWaitEvent simply puts ModifyWaitEvent into a PG_TRY/PG_CATCH
 * block in order to catch any hard errors, and returns this information to the
 * caller.
 */
bool
CitusModifyWaitEvent(WaitEventSet *set, int pos, uint32 events, Latch *latch)
{
	volatile bool success = true;
	MemoryContext savedContext = CurrentMemoryContext;

	PG_TRY();
	{
		ModifyWaitEvent(set, pos, events, latch);
	}
	PG_CATCH();
	{
		/*
		 * We might be in an arbitrary memory context when the
		 * error is thrown and we should get back to one we had
		 * at PG_TRY() time, especially because we are not
		 * re-throwing the error.
		 */
		MemoryContextSwitchTo(savedContext);

		FlushErrorState();

		/* let the callers know about the failure */
		success = false;
	}
	PG_END_TRY();

	return success;
}
