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
AtEOXact_Connections(bool isCommit)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	/*
	 * Close all remote connections if necessary anymore (i.e. not session
	 * lifetime), or if in a failed state.
	 */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		ListCell *previousCell = NULL;
		ListCell *nextCell = NULL;
		ListCell *connectionCell = NULL;

		/*
		 * Have to iterate "manually", to be able to delete connections in the
		 * middle of the list.
		 */
		for (connectionCell = list_head(entry->connections);
			 connectionCell != NULL;
			 connectionCell = nextCell)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

			nextCell = lnext(connectionCell);

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
			 * Only let a connection life longer than a single transaction if
			 * instructed to do so by the caller. We also skip doing so if
			 * it's in a state that wouldn't allow us to run queries again.
			 */
			if (!connection->sessionLifespan ||
				PQstatus(connection->conn) != CONNECTION_OK ||
				PQtransactionStatus(connection->conn) != PQTRANS_IDLE)
			{
				PQfinish(connection->conn);
				connection->conn = NULL;

				entry->connections =
					list_delete_cell(entry->connections, connectionCell, previousCell);

				pfree(connection);
			}
			else
			{
				/* reset per-transaction state */
				connection->activeInTransaction = false;

				UnclaimConnection(connection);

				previousCell = connectionCell;
			}
		}

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
 * StartNodeConnection initiate a connection to remote node, using default
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
 * - NEW_CONNECTION - it is permitted to establish a new connection
 * - CACHED_CONNECTION - it is permitted to re-use an established connection
 * - SESSION_LIFESPAN - the connection should persist after transaction end
 * - FOR_DML - only meaningful for placement associated connections
 * - FOR_DDL - only meaningful for placement associated connections
 * - CRITICAL_CONNECTION - transaction failures on this connection fail the entire
 *   coordinated transaction
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
	MemoryContext oldContext;
	bool found;

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
		entry->connections = NIL;
	}

	if (flags & CACHED_CONNECTION)
	{
		ListCell *connectionCell = NULL;

		/* check connection cache for a connection that's not already in use */
		foreach(connectionCell, entry->connections)
		{
			connection = (MultiConnection *) lfirst(connectionCell);

			/* don't return claimed connections */
			if (!connection->claimedExclusively)
			{
				if (flags & SESSION_LIFESPAN)
				{
					connection->sessionLifespan = true;
				}
				connection->activeInTransaction = true;

				/*
				 * Check whether we're right now allowed to open new
				 * connections. A cached connection counts as new if it hasn't
				 * been used in this transaction.
				 *
				 * FIXME: This should be removed soon, once all connections go
				 * through this API.
				 */
				if (!connection->activeInTransaction &&
					XactModificationLevel > XACT_MODIFICATION_DATA)
				{
					ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
									errmsg("cannot open new connections after the first "
										   "modification command within a transaction")));
				}

				return connection;
			}

			/*
			 * One could argue for erroring out when the connection is in a
			 * failed state. But that'd be a bad idea for two reasons:
			 *
			 * 1) Generally starting a connection might fail, after calling
			 *    this function, so calling code needs to handle that anyway.
			 * 2) This might be used in code that transparently handles
			 *    connection failure.
			 */
		}

		/* no connection available, done if a new connection isn't desirable */
		if (!(flags & NEW_CONNECTION))
		{
			return NULL;
		}
	}

	/*
	 * Check whether we're right now allowed to open new connections.
	 *
	 * FIXME: This should be removed soon, once all connections go through
	 * this API.
	 */
	if (XactModificationLevel > XACT_MODIFICATION_DATA)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot open new connections after the first modification "
							   "command within a transaction")));
	}

	/*
	 * Either no caching desired, or no pre-established, non-claimed,
	 * connection present. Initiate connection establishment.
	 */
	connection = StartConnectionEstablishment(&key);

	oldContext = MemoryContextSwitchTo(ConnectionContext);
	entry->connections = lappend(entry->connections, connection);
	MemoryContextSwitchTo(oldContext);

	if (flags & SESSION_LIFESPAN)
	{
		connection->sessionLifespan = true;
	}

	connection->activeInTransaction = true;

	return connection;
}


/*
 * Synchronously finish connection establishment of an individual connection.
 *
 * TODO: Replace with variant waiting for multiple connections.
 */
void
FinishConnectionEstablishment(MultiConnection *connection)
{
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

			pollResult = poll(&pollFileDescriptor, 1, CLIENT_CONNECT_TIMEOUT_SECONDS_INT);

			if (pollResult == 0)
			{
				/* timeout exceeded */
			}
			else if (pollResult > 0)
			{
				/* IO possible, continue connection establishment */
				break;
			}
			else if (pollResult != EINTR)
			{
				/* retrying, signal */
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

	return connection;
}
