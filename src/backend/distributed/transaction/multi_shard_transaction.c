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

#include "distributed/colocation_utils.h"
#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
#include "distributed/connection_management.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "utils/memutils.h"


#define INITIAL_CONNECTION_CACHE_SIZE 1001


/* per-transaction state */
static HTAB *shardConnectionHash = NULL;


/*
 * OpenTransactionsToAllShardPlacements opens connections to all placements
 * using the provided shard identifier list. Connections accumulate in a global
 * shardConnectionHash variable for use (and re-use) within this transaction.
 */
void
OpenTransactionsToAllShardPlacements(List *shardIntervalList, char *userName)
{
	ListCell *shardIntervalCell = NULL;

	if (shardConnectionHash == NULL)
	{
		shardConnectionHash = CreateShardConnectionHash(TopTransactionContext);
	}

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;

		BeginTransactionOnShardPlacements(shardId, userName);
	}
}


/*
 * CreateShardConnectionHash constructs a hash table which maps from shard
 * identifier to connection lists, passing the provided MemoryContext to
 * hash_create for hash allocations.
 */
HTAB *
CreateShardConnectionHash(MemoryContext memoryContext)
{
	HTAB *shardConnectionsHash = NULL;
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardConnections);
	info.hcxt = memoryContext;
	hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	shardConnectionsHash = hash_create("Shard Connections Hash",
									   INITIAL_CONNECTION_CACHE_SIZE, &info,
									   hashFlags);

	return shardConnectionsHash;
}


/*
 * BeginTransactionOnShardPlacements opens new connections (if necessary) to
 * all placements of a shard (specified by shard identifier). After sending a
 * BEGIN command on all connections, they are added to shardConnectionHash for
 * use within this transaction. Exits early if connections already exist for
 * the specified shard, and errors if no placements can be found, a connection
 * cannot be made, or if the BEGIN command fails.
 */
void
BeginTransactionOnShardPlacements(uint64 shardId, char *userName)
{
	List *shardPlacementList = NIL;
	ListCell *placementCell = NULL;

	ShardConnections *shardConnections = NULL;
	bool shardConnectionsFound = false;

	MemoryContext oldContext = NULL;
	shardPlacementList = FinalizedShardPlacementList(shardId);

	if (shardPlacementList == NIL)
	{
		/* going to have to have some placements to do any work */
		ereport(ERROR, (errmsg("could not find any shard placements for the shard "
							   UINT64_FORMAT, shardId)));
	}

	BeginOrContinueCoordinatedTransaction();
	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

	/* get existing connections to the shard placements, if any */
	shardConnections = GetShardConnections(shardId, &shardConnectionsFound);
	if (shardConnectionsFound)
	{
		/* exit early if we've already established shard transactions */
		return;
	}

	foreach(placementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(placementCell);
		MultiConnection *connection = NULL;
		TransactionConnection *transactionConnection = NULL;
		WorkerNode *workerNode = FindWorkerNode(shardPlacement->nodeName,
												shardPlacement->nodePort);
		int connectionFlags = FORCE_NEW_CONNECTION;

		if (workerNode == NULL)
		{
			ereport(ERROR, (errmsg("could not find worker node %s:%d",
								   shardPlacement->nodeName, shardPlacement->nodePort)));
		}

		/* XXX: It'd be nicer to establish connections asynchronously here */
		connection = GetNodeUserDatabaseConnection(connectionFlags,
												   shardPlacement->nodeName,
												   shardPlacement->nodePort,
												   userName,
												   NULL);
		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			ereport(ERROR, (errmsg("could not establish a connection to all "
								   "placements of shard %lu", shardId)));
		}

		/* entries must last through the whole top-level transaction */
		oldContext = MemoryContextSwitchTo(TopTransactionContext);

		transactionConnection = palloc0(sizeof(TransactionConnection));

		transactionConnection->groupId = workerNode->groupId;
		transactionConnection->connectionId = shardConnections->shardId;
		transactionConnection->transactionState = TRANSACTION_STATE_OPEN;
		transactionConnection->connection = connection->pgConn;
		transactionConnection->nodeName = shardPlacement->nodeName;
		transactionConnection->nodePort = shardPlacement->nodePort;

		shardConnections->connectionList = lappend(shardConnections->connectionList,
												   transactionConnection);

		MemoryContextSwitchTo(oldContext);

		/*
		 * Every individual failure should cause entire distributed
		 * transaction to fail.
		 */
		MarkRemoteTransactionCritical(connection);
		if (MultiShardCommitProtocol > COMMIT_PROTOCOL_BARE)
		{
			/* issue BEGIN */
			RemoteTransactionBegin(connection);
		}
	}
}


/*
 * GetShardConnections finds existing connections for a shard in the global
 * connection hash. If not found, then a ShardConnections structure with empty
 * connectionList is returned and the shardConnectionsFound output parameter
 * will be set to false.
 */
ShardConnections *
GetShardConnections(int64 shardId, bool *shardConnectionsFound)
{
	ShardConnections *shardConnections = NULL;

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	List *colocatedShardIds = ColocatedShardIntervalList(shardInterval);
	ShardInterval *baseShardInterval = LowestShardIntervalById(colocatedShardIds);
	int64 baseShardId = baseShardInterval->shardId;

	shardConnections = GetShardHashConnections(shardConnectionHash, baseShardId,
											   shardConnectionsFound);

	return shardConnections;
}


/*
 * GetShardHashConnections finds existing connections for a shard in the
 * provided hash. If not found, then a ShardConnections structure with empty
 * connectionList is returned.
 */
ShardConnections *
GetShardHashConnections(HTAB *connectionHash, int64 shardId, bool *connectionsFound)
{
	ShardConnections *shardConnections = NULL;

	shardConnections = (ShardConnections *) hash_search(connectionHash, &shardId,
														HASH_ENTER, connectionsFound);
	if (!*connectionsFound)
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

	if (connectionHash == NULL)
	{
		return NIL;
	}

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
 * ResetShardPlacementTransactionState performs cleanup after the end of a
 * transaction.
 */
void
ResetShardPlacementTransactionState(void)
{
	/*
	 * Now that transaction management does most of our work, nothing remains
	 * but to reset the connection hash, which wouldn't be valid next time
	 * round.
	 */
	shardConnectionHash = NULL;

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_BARE)
	{
		MultiShardCommitProtocol = SavedMultiShardCommitProtocol;
	}
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

		CloseConnectionByPGconn(connection);
	}
}
