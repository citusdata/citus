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
#include "mb/pg_wchar.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


HTAB *ConnectionHash = NULL;
MemoryContext ConnectionContext = NULL;

static uint32 ConnectionHashHash(const void *key, Size keysize);
static int ConnectionHashCompare(const void *a, const void *b, Size keysize);
static MultiConnection * StartConnectionEstablishment(ConnectionHashKey *key);
static MultiConnection * FindAvailableConnection(dlist_head *connections, uint32 flags);
static void AfterXactResetHostConnections(ConnectionHashEntry *entry, bool isCommit);


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
 * GetNodeConnection() establishes a connection to remote node, using default
 * user and database.
 *
 * See StartNodeUserDatabaseConnection for details.
 */
MultiConnection *
GetNodeConnection(const char *hostname, int32 port, uint32 flags)
{
	return GetNodeUserDatabaseConnection(hostname, port, NULL, NULL, flags);
}


/*
 * StartNodeConnection initiate a connection to remote node, using default
 * user and database.
 *
 * See StartNodeUserDatabaseConnection for details.
 */
MultiConnection *
StartNodeConnection(const char *hostname, int32 port, uint32 flags)
{
	return StartNodeUserDatabaseConnection(hostname, port, NULL, NULL, flags);
}


/*
 * GetNodeUserDatabaseConnection establishes connection to remote node.
 *
 * See StartNodeUserDatabaseConnection for details.
 */
MultiConnection *
GetNodeUserDatabaseConnection(const char *hostname, int32 port, const
							  char *user, const char *database, uint32 flags)
{
	MultiConnection *connection;

	connection = StartNodeUserDatabaseConnection(hostname, port, user, database, flags);

	FinishConnectionEstablishment(connection);

	return connection;
}


/*
 * StartNodeUserDatabaseConnection() initiates a connection to a remote node.
 *
 * The returned connection has only been initiated, not fully
 * established. That's useful to allow parallel connection establishment. If
 * that's not desired use the Get* variant.
 */
MultiConnection *
StartNodeUserDatabaseConnection(const char *hostname, int32 port, const
								char *user, const char *database, uint32 flags)
{
	ConnectionHashKey key;
	ConnectionHashEntry *entry = NULL;
	MultiConnection *connection = NULL;
	bool found = false;

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

	/*
	 * Lookup relevant hash entry or enter a new one.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->connections = MemoryContextAlloc(ConnectionContext,
												sizeof(dlist_head));
		dlist_init(entry->connections);
	}

	connection = FindAvailableConnection(entry->connections, flags);
	if (connection == NULL)
	{
		MemoryContext oldContext = NULL;

		/*
		 * Either no caching desired, or no pre-established, non-claimed,
		 * connection present. Initiate connection establishment.
		 */
		connection = StartConnectionEstablishment(&key);

		oldContext = MemoryContextSwitchTo(ConnectionContext);
		dlist_push_tail(entry->connections, &connection->node);

		MemoryContextSwitchTo(oldContext);
	}

	if (flags & IN_TRANSACTION)
	{
		connection->activeInTransaction = true;
	}

	if (flags & CLAIM_EXCLUSIVELY)
	{
		ClaimConnectionExclusively(connection);
	}

	return connection;
}


/*
 * FindAvailableConnection finds an available connection from the given list
 * of connections. A connection is available if it has not been claimed
 * exclusively and, if the IN_TRANSACTION flag is not set, is not in a
 * transaction.
 */
static MultiConnection *
FindAvailableConnection(dlist_head *connections, uint32 flags)
{
	dlist_iter iter;

	/* check connection cache for a connection that's not already in use */
	dlist_foreach(iter, connections)
	{
		MultiConnection *connection =
			dlist_container(MultiConnection, node, iter.cur);

		/* don't return claimed connections */
		if (connection->claimedExclusively)
		{
			continue;
		}

		/*
		 * Don't return connections that are active in the coordinated transaction if
		 * not explicitly requested.
		 */
		if (!(flags & IN_TRANSACTION) && connection->activeInTransaction)
		{
			continue;
		}

		return connection;
	}

	return NULL;
}


/*
 * Close a previously established connection.
 */
void
CloseConnectionByPGconn(PGconn *pqConn)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		dlist_head *connections = entry->connections;
		dlist_iter iter;

		/* check connection cache for a connection that's not already in use */
		dlist_foreach(iter, connections)
		{
			MultiConnection *connection =
				dlist_container(MultiConnection, node, iter.cur);

			if (connection->conn == pqConn)
			{
				/* unlink from list */
				dlist_delete(&connection->node);

				/* we leave the per-host entry alive */
				pfree(connection);
			}
		}
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
		ConnStatusType status = PQstatus(connection->conn);
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

		pollmode = PQconnectPoll(connection->conn);

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
			struct pollfd pollFileDescriptor;
			int pollResult = 0;

			pollFileDescriptor.fd = PQsocket(connection->conn);
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

			if (pollResult == 0)
			{
				/*
				 * Timeout exceeded. Two things to do:
				 * - check whether any interrupts arrived and handle them
				 * - check whether establishment for connection alreadly has
				 *   lasted for too long, stop waiting if so.
				 */
				CHECK_FOR_INTERRUPTS();

				if (TimestampDifferenceExceeds(connection->connectionStart,
											   GetCurrentTimestamp(),
											   CLIENT_CONNECT_TIMEOUT_SECONDS_INT * 1000))
				{
					ereport(WARNING, (errmsg("could not establish connection after %u ms",
											 CLIENT_CONNECT_TIMEOUT_SECONDS_INT * 1000)));

					/* close connection, otherwise we take up resource on the other side */
					PQfinish(connection->conn);
					connection->conn = NULL;
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
 * anymore. That means it again may be returned by returned by
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

	if (strncmp(ca->hostname, cb->hostname, NAMEDATALEN) != 0 ||
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

	const char *keywords[] = {
		"host", "port", "dbname", "user",
		"client_encoding", "fallback_application_name",
		NULL
	};
	const char *values[] = {
		key->hostname, nodePortString, key->database, key->user,
		clientEncoding, "citus", NULL
	};

	connection = MemoryContextAllocZero(ConnectionContext, sizeof(MultiConnection));
	sprintf(nodePortString, "%d", key->port);

	strlcpy(connection->hostname, key->hostname, MAX_NODE_LENGTH);
	connection->port = key->port;
	strlcpy(connection->database, key->database, NAMEDATALEN);
	strlcpy(connection->user, key->user, NAMEDATALEN);

	connection->conn = PQconnectStartParams(keywords, values, false);
	connection->connectionStart = GetCurrentTimestamp();

	return connection;
}


/*
 * Perform connection management activity after the end of a transaction. Both
 * COMMIT and ABORT paths are handled here.
 *
 * This is called by Citus' global transaction callback.
 */
void
AfterXactResetConnections(bool isCommit)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		AfterXactResetHostConnections(entry, isCommit);

		/*
		 * NB: We leave the hash entry in place, even if there's no individual
		 * connections in it anymore. There seems no benefit in deleting it,
		 * and it'll save a bit of work in the next transaction.
		 */
	}
}


/*
 * Close all remote connections if necessary anymore (i.e. not cached),
 * or if in a failed state.
 */
static void
AfterXactResetHostConnections(ConnectionHashEntry *entry, bool isCommit)
{
	dlist_mutable_iter iter;
	bool cachedConnection = false;

	dlist_foreach_modify(iter, entry->connections)
	{
		MultiConnection *connection = dlist_container(MultiConnection, node, iter.cur);

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
		 * Close connection if there was an error or we already cached
		 * a connection for this node.
		 */
		if (cachedConnection ||
			PQstatus(connection->conn) != CONNECTION_OK ||
			PQtransactionStatus(connection->conn) != PQTRANS_IDLE)
		{
			PQfinish(connection->conn);
			connection->conn = NULL;

			/* unlink from list */
			dlist_delete(iter.cur);

			pfree(connection);
		}
		else
		{
			/* reset connection state */
			UnclaimConnection(connection);
			connection->activeInTransaction = false;

			/* close remaining connections */
			cachedConnection = true;
		}
	}
}
