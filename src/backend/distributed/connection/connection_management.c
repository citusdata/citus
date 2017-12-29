/*-------------------------------------------------------------------------
 *
 * connection_management.c
 *   Central management of connections and their life-cycle
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif


#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/hash_helpers.h"
#include "distributed/placement_connection.h"
#include "mb/pg_wchar.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


int NodeConnectionTimeout = 5000;
int CitusSSLMode = CITUS_SSL_MODE_PREFER;
HTAB *ConnectionHash = NULL;
MemoryContext ConnectionContext = NULL;


static uint32 ConnectionHashHash(const void *key, Size keysize);
static int ConnectionHashCompare(const void *a, const void *b, Size keysize);
static MultiConnection * StartConnectionEstablishment(ConnectionHashKey *key);
static void AfterXactHostConnectionHandling(ConnectionHashEntry *entry, bool isCommit);
static MultiConnection * FindAvailableConnection(dlist_head *connections, uint32 flags);


/*
 * Initialize per-backend connection management infrastructure.
 */
void
InitializeConnectionManagement(void)
{
	HASHCTL info;
	uint32 hashFlags = 0;


	/*
	 * Create a single context for connection and transaction related memory
	 * management. Doing so, instead of allocating in TopMemoryContext, makes
	 * it easier to associate used memory.
	 */
	ConnectionContext = AllocSetContextCreate(TopMemoryContext, "Connection Context",
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
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	ConnectionHash = hash_create("citus connection cache (host,port,user,database)",
								 64, &info, hashFlags);
}


/*
 * Perform connection management activity after the end of a transaction. Both
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
	return StartNodeUserDatabaseConnection(flags, hostname, port, NULL, NULL);
}


/*
 * GetNodeUserDatabaseConnection establishes connection to remote node.
 *
 * See StartNodeUserDatabaseConnection for details.
 */
MultiConnection *
GetNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port, const
							  char *user, const char *database)
{
	MultiConnection *connection;

	connection = StartNodeUserDatabaseConnection(flags, hostname, port, user, database);

	FinishConnectionEstablishment(connection);

	return connection;
}


/*
 * StartNodeUserDatabaseConnection() initiates a connection to a remote node.
 *
 * If user or database are NULL, the current session's defaults are used. The
 * following flags influence connection establishment behaviour:
 * - SESSION_LIFESPAN - the connection should persist after transaction end
 * - FORCE_NEW_CONNECTION - a new connection is required
 *
 * The returned connection has only been initiated, not fully
 * established. That's useful to allow parallel connection establishment. If
 * that's not desired use the Get* variant.
 */
MultiConnection *
StartNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port, const
								char *user, const char *database)
{
	ConnectionHashKey key;
	ConnectionHashEntry *entry = NULL;
	MultiConnection *connection;
	bool found;

	/* do some minimal input checks */
	strlcpy(key.hostname, hostname, MAX_NODE_LENGTH);
	if (strlen(hostname) > MAX_NODE_LENGTH)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("hostname exceeds the maximum length of %d",
							   MAX_NODE_LENGTH)));
	}

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
		strlcpy(key.database, get_database_name(MyDatabaseId), NAMEDATALEN);
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

	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->connections = MemoryContextAlloc(ConnectionContext,
												sizeof(dlist_head));
		dlist_init(entry->connections);
	}

	/* if desired, check whether there's a usable connection */
	if (!(flags & FORCE_NEW_CONNECTION))
	{
		/* check connection cache for a connection that's not already in use */
		connection = FindAvailableConnection(entry->connections, flags);
		if (connection)
		{
			if (flags & SESSION_LIFESPAN)
			{
				connection->sessionLifespan = true;
			}

			return connection;
		}
	}

	/*
	 * Either no caching desired, or no pre-established, non-claimed,
	 * connection present. Initiate connection establishment.
	 */
	connection = StartConnectionEstablishment(&key);

	dlist_push_tail(entry->connections, &connection->connectionNode);
	ResetShardPlacementAssociation(connection);

	if (flags & SESSION_LIFESPAN)
	{
		connection->sessionLifespan = true;
	}

	return connection;
}


/* StartNodeUserDatabaseConnection() helper */
static MultiConnection *
FindAvailableConnection(dlist_head *connections, uint32 flags)
{
	dlist_iter iter;

	dlist_foreach(iter, connections)
	{
		MultiConnection *connection =
			dlist_container(MultiConnection, connectionNode, iter.cur);

		/* don't return claimed connections */
		if (connection->claimedExclusively)
		{
			continue;
		}

		return connection;
	}

	return NULL;
}


/*
 * CloseNodeConnectionsAfterTransaction sets the sessionLifespan flag of the connections
 * to a particular node as false. This is mainly used when a worker leaves the cluster.
 */
void
CloseNodeConnectionsAfterTransaction(char *nodeName, int nodePort)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		dlist_iter iter;
		dlist_head *connections = NULL;

		if (strcmp(entry->key.hostname, nodeName) != 0 || entry->key.port != nodePort)
		{
			continue;
		}

		connections = entry->connections;
		dlist_foreach(iter, connections)
		{
			MultiConnection *connection =
				dlist_container(MultiConnection, connectionNode, iter.cur);

			connection->sessionLifespan = false;
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
	PQfinish(connection->pgConn);
	connection->pgConn = NULL;

	strlcpy(key.hostname, connection->hostname, MAX_NODE_LENGTH);
	key.port = connection->port;
	strlcpy(key.user, connection->user, NAMEDATALEN);
	strlcpy(key.database, connection->database, NAMEDATALEN);

	hash_search(ConnectionHash, &key, HASH_FIND, &found);

	if (found)
	{
		/* unlink from list of open connections */
		dlist_delete(&connection->connectionNode);

		/* same for transaction state and shard/placement machinery */
		CloseRemoteTransaction(connection);
		CloseShardPlacementAssociation(connection);

		/* we leave the per-host entry alive */
		pfree(connection);
	}
	else
	{
		ereport(ERROR, (errmsg("closing untracked connection")));
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
		char errorMessage[256] = { 0 };
		PGcancel *cancel = PQgetCancel(connection->pgConn);

		if (!PQcancel(cancel, errorMessage, sizeof(errorMessage)))
		{
			ereport(WARNING, (errmsg("could not cancel connection: %s",
									 errorMessage)));
		}
		PQfreeCancel(cancel);
	}
	PQfinish(connection->pgConn);
	connection->pgConn = NULL;
}


/*
 * FinishConnectionListEstablishment is a wrapper around FinishConnectionEstablishment.
 * The function iterates over the multiConnectionList and finishes the connection
 * establishment for each multi connection.
 */
void
FinishConnectionListEstablishment(List *multiConnectionList)
{
	ListCell *multiConnectionCell = NULL;

	foreach(multiConnectionCell, multiConnectionList)
	{
		MultiConnection *multiConnection = (MultiConnection *) lfirst(
			multiConnectionCell);

		/* TODO: consider making connection establishment fully in parallel */
		FinishConnectionEstablishment(multiConnection);
	}
}


/*
 * Synchronously finish connection establishment of an individual connection.
 *
 * TODO: Replace with variant waiting for multiple connections.
 */
void
FinishConnectionEstablishment(MultiConnection *connection)
{
	static int checkIntervalMS = 200;

	/*
	 * Loop until connection is established, or failed (possibly just timed
	 * out).
	 */
	while (true)
	{
		ConnStatusType status = PQstatus(connection->pgConn);
		PostgresPollingStatusType pollmode;

		if (status == CONNECTION_OK)
		{
			return;
		}

		/* FIXME: retries? */
		if (status == CONNECTION_BAD)
		{
			return;
		}

		pollmode = PQconnectPoll(connection->pgConn);

		/*
		 * FIXME: Do we want to add transparent retry support here?
		 */
		if (pollmode == PGRES_POLLING_FAILED)
		{
			return;
		}
		else if (pollmode == PGRES_POLLING_OK)
		{
			return;
		}
		else
		{
			Assert(pollmode == PGRES_POLLING_WRITING ||
				   pollmode == PGRES_POLLING_READING);
		}

		/* Loop, to handle poll() being interrupted by signals (EINTR) */
		while (true)
		{
			int pollResult = 0;

			/* we use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
			struct pollfd pollFileDescriptor;

			pollFileDescriptor.fd = PQsocket(connection->pgConn);
			if (pollmode == PGRES_POLLING_READING)
			{
				pollFileDescriptor.events = POLLIN;
			}
			else
			{
				pollFileDescriptor.events = POLLOUT;
			}
			pollFileDescriptor.revents = 0;

			/*
			 * Only sleep for a limited amount of time, so we can react to
			 * interrupts in time, even if the platform doesn't interrupt
			 * poll() after signal arrival.
			 */
			pollResult = poll(&pollFileDescriptor, 1, checkIntervalMS);
#else /* !HAVE_POLL */
			fd_set readFileDescriptorSet;
			fd_set writeFileDescriptorSet;
			fd_set exceptionFileDescriptorSet;
			int selectTimeoutUS = checkIntervalMS * 1000;
			struct timeval selectTimeout = { 0, selectTimeoutUS };
			int selectFileDescriptor = PQsocket(connection->pgConn);

			FD_ZERO(&readFileDescriptorSet);
			FD_ZERO(&writeFileDescriptorSet);
			FD_ZERO(&exceptionFileDescriptorSet);

			if (pollmode == PGRES_POLLING_READING)
			{
				FD_SET(selectFileDescriptor, &readFileDescriptorSet);
			}
			else if (pollmode == PGRES_POLLING_WRITING)
			{
				FD_SET(selectFileDescriptor, &writeFileDescriptorSet);
			}

			pollResult = (select) (selectFileDescriptor + 1, &readFileDescriptorSet,
								   &writeFileDescriptorSet, &exceptionFileDescriptorSet,
								   &selectTimeout);
#endif /* HAVE_POLL */

			if (pollResult == 0)
			{
				/*
				 * Timeout exceeded. Two things to do:
				 * - check whether any interrupts arrived and handle them
				 * - check whether establishment for connection already has
				 *   lasted for too long, stop waiting if so.
				 */
				CHECK_FOR_INTERRUPTS();

				if (TimestampDifferenceExceeds(connection->connectionStart,
											   GetCurrentTimestamp(),
											   NodeConnectionTimeout))
				{
					ereport(WARNING, (errmsg("could not establish connection after %u ms",
											 NodeConnectionTimeout)));

					/* close connection, otherwise we take up resource on the other side */
					PQfinish(connection->pgConn);
					connection->pgConn = NULL;
					break;
				}
			}
			else if (pollResult > 0)
			{
				/*
				 * IO possible, continue connection establishment. We could
				 * check for timeouts here as well, but if there's progress
				 * there seems little point.
				 */
				break;
			}
			else if (pollResult != EINTR)
			{
				/* Retrying, signal interrupted. So check. */
				CHECK_FOR_INTERRUPTS();
			}
			else
			{
				/*
				 * We ERROR here, instead of just returning a failed
				 * connection, because this shouldn't happen, and indicates a
				 * programming error somewhere, not a network etc. issue.
				 */
				ereport(ERROR, (errcode_for_socket_access(),
								errmsg("poll() failed: %m")));
			}
		}
	}
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
	uint32 hash = 0;

	hash = string_hash(entry->hostname, NAMEDATALEN);
	hash = hash_combine(hash, hash_uint32(entry->port));
	hash = hash_combine(hash, string_hash(entry->user, NAMEDATALEN));
	hash = hash_combine(hash, string_hash(entry->database, NAMEDATALEN));

	return hash;
}


static int
ConnectionHashCompare(const void *a, const void *b, Size keysize)
{
	ConnectionHashKey *ca = (ConnectionHashKey *) a;
	ConnectionHashKey *cb = (ConnectionHashKey *) b;

	if (strncmp(ca->hostname, cb->hostname, MAX_NODE_LENGTH) != 0 ||
		ca->port != cb->port ||
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
static MultiConnection *
StartConnectionEstablishment(ConnectionHashKey *key)
{
	char nodePortString[12];
	const char *clientEncoding = GetDatabaseEncodingName();
	MultiConnection *connection = NULL;
	const char *sslmode = CitusSSLModeString();

	const char *keywords[] = {
		"host", "port", "dbname", "user", "sslmode",
		"client_encoding", "fallback_application_name",
		NULL
	};
	const char *values[] = {
		key->hostname, nodePortString, key->database, key->user, sslmode,
		clientEncoding, "citus", NULL
	};

	connection = MemoryContextAllocZero(ConnectionContext, sizeof(MultiConnection));
	sprintf(nodePortString, "%d", key->port);

	strlcpy(connection->hostname, key->hostname, MAX_NODE_LENGTH);
	connection->port = key->port;
	strlcpy(connection->database, key->database, NAMEDATALEN);
	strlcpy(connection->user, key->user, NAMEDATALEN);

	connection->pgConn = PQconnectStartParams(keywords, values, false);
	connection->connectionStart = GetCurrentTimestamp();

	/*
	 * To avoid issues with interrupts not getting caught all our connections
	 * are managed in a non-blocking manner. remote_commands.c provides
	 * wrappers emulating blocking behaviour.
	 */
	PQsetnonblocking(connection->pgConn, true);

	return connection;
}


/*
 * CitusSSLModeString returns the current value of citus.sslmode.
 */
char *
CitusSSLModeString(void)
{
	switch (CitusSSLMode)
	{
		case CITUS_SSL_MODE_DISABLE:
		{
			return "disable";
		}

		case CITUS_SSL_MODE_ALLOW:
		{
			return "allow";
		}

		case CITUS_SSL_MODE_PREFER:
		{
			return "prefer";
		}

		case CITUS_SSL_MODE_REQUIRE:
		{
			return "require";
		}

		case CITUS_SSL_MODE_VERIFY_CA:
		{
			return "verify-ca";
		}

		case CITUS_SSL_MODE_VERIFY_FULL:
		{
			return "verify-full";
		}

		default:
		{
			ereport(ERROR, (errmsg("unrecognized value for citus.sslmode")));
		}
	}
}


/*
 * Close all remote connections if necessary anymore (i.e. not session
 * lifetime), or if in a failed state.
 */
static void
AfterXactHostConnectionHandling(ConnectionHashEntry *entry, bool isCommit)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, entry->connections)
	{
		MultiConnection *connection =
			dlist_container(MultiConnection, connectionNode, iter.cur);

		/*
		 * To avoid code leaking connections we warn if connections are
		 * still claimed exclusively. We can only do so if the transaction
		 * committed, as it's normal that code didn't have chance to clean
		 * up after errors.
		 */
		if (isCommit && connection->claimedExclusively)
		{
			ereport(WARNING,
					(errmsg("connection claimed exclusively at transaction commit")));
		}

		/*
		 * Preserve session lifespan connections if they are still healthy.
		 */
		if (!connection->sessionLifespan ||
			PQstatus(connection->pgConn) != CONNECTION_OK ||
			PQtransactionStatus(connection->pgConn) != PQTRANS_IDLE)
		{
			ShutdownConnection(connection);

			/* unlink from list */
			dlist_delete(iter.cur);

			pfree(connection);
		}
		else
		{
			/* reset per-transaction state */
			ResetRemoteTransaction(connection);
			ResetShardPlacementAssociation(connection);

			/* reset copy state */
			connection->copyBytesWrittenSinceLastFlush = 0;

			UnclaimConnection(connection);
		}
	}
}
