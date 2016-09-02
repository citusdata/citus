/*-------------------------------------------------------------------------
 *
 * multi_shard_transaction.c
 *     This file contains functions for managing 1PC or 2PC transactions
 *     across many shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "libpq-fe.h"
#include "postgres.h"

#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/multi_shard_transaction.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"


#define INITIAL_CONNECTION_CACHE_SIZE 1001


/* Local functions forward declarations */
static void RegisterShardPlacementXactCallback(void);


/* Global variables used in commit handler */
static List *shardPlacementConnectionList = NIL;
static bool isXactCallbackRegistered = false;


/*
 * OpenTransactionsToAllShardPlacements opens connections to all placements of
 * the given shard Id Pointer List and returns the hash table containing the connections.
 * The resulting hash table maps shardIds to ShardConnection structs.
 */
HTAB *
OpenTransactionsToAllShardPlacements(List *shardIntervalList, char *userName)
{
	HTAB *shardConnectionHash = CreateShardConnectionHash();
	ListCell *shardIntervalCell = NULL;
	ListCell *connectionCell = NULL;
	List *connectionList = NIL;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;

		OpenConnectionsToShardPlacements(shardId, shardConnectionHash, userName);
	}

	connectionList = ConnectionList(shardConnectionHash);

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		PGresult *result = NULL;

		result = PQexec(connection, "BEGIN");
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReraiseRemoteError(connection, result);
		}
	}

	shardPlacementConnectionList = ConnectionList(shardConnectionHash);

	RegisterShardPlacementXactCallback();

	return shardConnectionHash;
}


/*
 * CreateShardConnectionHash constructs a hash table used for shardId->Connection
 * mapping.
 */
HTAB *
CreateShardConnectionHash(void)
{
	HTAB *shardConnectionsHash = NULL;
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardConnections);
	info.hash = tag_hash;
	info.hcxt = TopTransactionContext;

	hashFlags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;
	shardConnectionsHash = hash_create("Shard Connections Hash",
									   INITIAL_CONNECTION_CACHE_SIZE, &info,
									   hashFlags);

	return shardConnectionsHash;
}


/*
 * OpenConnectionsToShardPlacements opens connections to all placements of the
 * shard with the given shardId and populates the shardConnectionHash table
 * accordingly.
 */
void
OpenConnectionsToShardPlacements(uint64 shardId, HTAB *shardConnectionHash,
								 char *userName)
{
	bool shardConnectionsFound = false;

	/* get existing connections to the shard placements, if any */
	ShardConnections *shardConnections = GetShardConnections(shardConnectionHash,
															 shardId,
															 &shardConnectionsFound);

	List *shardPlacementList = FinalizedShardPlacementList(shardId);
	ListCell *shardPlacementCell = NULL;
	List *connectionList = NIL;

	Assert(!shardConnectionsFound);

	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find any shard placements for the shard "
							   UINT64_FORMAT, shardId)));
	}

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(
			shardPlacementCell);
		char *workerName = shardPlacement->nodeName;
		uint32 workerPort = shardPlacement->nodePort;
		PGconn *connection = ConnectToNode(workerName, workerPort, userName);
		TransactionConnection *transactionConnection = NULL;

		if (connection == NULL)
		{
			List *abortConnectionList = ConnectionList(shardConnectionHash);
			CloseConnections(abortConnectionList);

			ereport(ERROR, (errmsg("could not establish a connection to all "
								   "placements of shard %lu", shardId)));
		}

		transactionConnection = palloc0(sizeof(TransactionConnection));

		transactionConnection->connectionId = shardConnections->shardId;
		transactionConnection->transactionState = TRANSACTION_STATE_INVALID;
		transactionConnection->connection = connection;

		connectionList = lappend(connectionList, transactionConnection);
	}

	shardConnections->connectionList = connectionList;
}


/*
 * GetShardConnections finds existing connections for a shard in the hash.
 * If not found, then a ShardConnections structure with empty connectionList
 * is returned.
 */
ShardConnections *
GetShardConnections(HTAB *shardConnectionHash, int64 shardId,
					bool *shardConnectionsFound)
{
	ShardConnections *shardConnections = NULL;

	shardConnections = (ShardConnections *) hash_search(shardConnectionHash,
														&shardId,
														HASH_ENTER,
														shardConnectionsFound);
	if (!*shardConnectionsFound)
	{
		shardConnections->shardId = shardId;
		shardConnections->connectionList = NIL;
	}

	return shardConnections;
}


/*
 * ConnectionList flattens the connection hash to a list of placement connections.
 */
List *
ConnectionList(HTAB *connectionHash)
{
	List *connectionList = NIL;
	HASH_SEQ_STATUS status;
	ShardConnections *shardConnections = NULL;

	hash_seq_init(&status, connectionHash);

	shardConnections = (ShardConnections *) hash_seq_search(&status);
	while (shardConnections != NULL)
	{
		List *shardConnectionsList = list_copy(shardConnections->connectionList);
		connectionList = list_concat(connectionList, shardConnectionsList);

		shardConnections = (ShardConnections *) hash_seq_search(&status);
	}

	return connectionList;
}


/*
 * EnableXactCallback ensures the XactCallback for committing/aborting
 * remote worker transactions is registered.
 */
void
RegisterShardPlacementXactCallback(void)
{
	if (!isXactCallbackRegistered)
	{
		RegisterXactCallback(CompleteShardPlacementTransactions, NULL);
		isXactCallbackRegistered = true;
	}
}


/*
 * CompleteShardPlacementTransactions commits or aborts pending shard placement
 * transactions when the local transaction commits or aborts.
 */
void
CompleteShardPlacementTransactions(XactEvent event, void *arg)
{
	if (shardPlacementConnectionList == NIL)
	{
		/* nothing to do */
		return;
	}
	else if (event == XACT_EVENT_PRE_COMMIT)
	{
		/*
		 * Any failure here will cause local changes to be rolled back,
		 * and remote changes to either roll back (1PC) or, in case of
		 * connection or node failure, leave a prepared transaction
		 * (2PC).
		 */

		if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
		{
			PrepareRemoteTransactions(shardPlacementConnectionList);
		}

		return;
	}
	else if (event == XACT_EVENT_COMMIT)
	{
		/*
		 * A failure here will cause some remote changes to either
		 * roll back (1PC) or, in case of connection or node failure,
		 * leave a prepared transaction (2PC). However, the local
		 * changes have already been committed.
		 */

		CommitRemoteTransactions(shardPlacementConnectionList, false);
	}
	else if (event == XACT_EVENT_ABORT)
	{
		/*
		 * A failure here will cause some remote changes to either
		 * roll back (1PC) or, in case of connection or node failure,
		 * leave a prepared transaction (2PC). The local changes have
		 * already been rolled back.
		 */

		AbortRemoteTransactions(shardPlacementConnectionList);
	}
	else
	{
		return;
	}

	CloseConnections(shardPlacementConnectionList);
	shardPlacementConnectionList = NIL;
}


/*
 * CloseConnections closes all connections in connectionList.
 */
void
CloseConnections(List *connectionList)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;

		PQfinish(connection);
	}
}
