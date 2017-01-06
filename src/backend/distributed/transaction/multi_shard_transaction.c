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
	List *newConnectionList = NIL;

	if (shardConnectionHash == NULL)
	{
		shardConnectionHash = CreateShardConnectionHash(TopTransactionContext);
	}

	BeginOrContinueCoordinatedTransaction();
	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

	/* open connections to shards which don't have connections yet */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		ShardConnections *shardConnections = NULL;
		bool shardConnectionsFound = false;
		List *shardPlacementList = NIL;
		ListCell *placementCell = NULL;

		shardConnections = GetShardConnections(shardId, &shardConnectionsFound);
		if (shardConnectionsFound)
		{
			continue;
		}

		shardPlacementList = FinalizedShardPlacementList(shardId);
		if (shardPlacementList == NIL)
		{
			/* going to have to have some placements to do any work */
			ereport(ERROR, (errmsg("could not find any shard placements for the shard "
								   UINT64_FORMAT, shardId)));
		}

		foreach(placementCell, shardPlacementList)
		{
			ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(placementCell);
			MultiConnection *connection = NULL;
			MemoryContext oldContext = NULL;

			WorkerNode *workerNode = FindWorkerNode(shardPlacement->nodeName,
													shardPlacement->nodePort);
			if (workerNode == NULL)
			{
				ereport(ERROR, (errmsg("could not find worker node %s:%d",
									   shardPlacement->nodeName,
									   shardPlacement->nodePort)));
			}

			connection = StartNodeUserDatabaseConnection(FORCE_NEW_CONNECTION,
														 shardPlacement->nodeName,
														 shardPlacement->nodePort,
														 userName,
														 NULL);

			/* we need to preserve the connection list for the next statement */
			oldContext = MemoryContextSwitchTo(TopTransactionContext);

			shardConnections->connectionList = lappend(shardConnections->connectionList,
													   connection);

			MemoryContextSwitchTo(oldContext);

			newConnectionList = lappend(newConnectionList, connection);

			/*
			 * Every individual failure should cause entire distributed
			 * transaction to fail.
			 */
			MarkRemoteTransactionCritical(connection);
		}
	}

	/* finish connection establishment newly opened connections */
	FinishConnectionListEstablishment(newConnectionList);

	/* the special BARE mode (for e.g. VACUUM/ANALYZE) skips BEGIN */
	if (MultiShardCommitProtocol > COMMIT_PROTOCOL_BARE)
	{
		RemoteTransactionsBeginIfNecessary(newConnectionList);
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
		SavedMultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;
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
