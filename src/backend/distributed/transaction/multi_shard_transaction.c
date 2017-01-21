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
#include "distributed/connection_management.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/placement_connection.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "utils/memutils.h"


#define INITIAL_SHARD_CONNECTION_HASH_SIZE 128


/*
 * OpenTransactionsToAllShardPlacements opens connections to all placements
 * using the provided shard identifier list and returns it as a shard ID ->
 * ShardConnections hash. connectionFlags can be used to specify whether
 * the command is FOR_DML or FOR_DDL.
 */
HTAB *
OpenTransactionsToAllShardPlacements(List *shardIntervalList, int connectionFlags)
{
	HTAB *shardConnectionHash = NULL;
	ListCell *shardIntervalCell = NULL;
	List *newConnectionList = NIL;

	shardConnectionHash = CreateShardConnectionHash(CurrentMemoryContext);

	/* open connections to shards which don't have connections yet */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		ShardConnections *shardConnections = NULL;
		bool shardConnectionsFound = false;
		List *shardPlacementList = NIL;
		ListCell *placementCell = NULL;

		shardConnections = GetShardHashConnections(shardConnectionHash, shardId,
												   &shardConnectionsFound);
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

			WorkerNode *workerNode = FindWorkerNode(shardPlacement->nodeName,
													shardPlacement->nodePort);
			if (workerNode == NULL)
			{
				ereport(ERROR, (errmsg("could not find worker node %s:%d",
									   shardPlacement->nodeName,
									   shardPlacement->nodePort)));
			}

			connection = StartPlacementConnection(connectionFlags,
												  shardPlacement,
												  NULL);

			ClaimConnectionExclusively(connection);

			shardConnections->connectionList = lappend(shardConnections->connectionList,
													   connection);

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

	return shardConnectionHash;
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
									   INITIAL_SHARD_CONNECTION_HASH_SIZE, &info,
									   hashFlags);

	return shardConnectionsHash;
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
 * ShardConnectionList returns the list of ShardConnections in connectionHash.
 */
List *
ShardConnectionList(HTAB *connectionHash)
{
	List *shardConnectionsList = NIL;
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
		shardConnectionsList = lappend(shardConnectionsList, shardConnections);

		shardConnections = (ShardConnections *) hash_seq_search(&status);
	}

	return shardConnectionsList;
}


/*
 * ResetShardPlacementTransactionState performs cleanup after the end of a
 * transaction.
 */
void
ResetShardPlacementTransactionState(void)
{
	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_BARE)
	{
		MultiShardCommitProtocol = SavedMultiShardCommitProtocol;
		SavedMultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;
	}
}


/*
 * UnclaimAllShardConnections unclaims all connections in the given
 * shard connections hash after previously claiming them exclusively
 * in OpenTransactionsToAllShardPlacements.
 */
void
UnclaimAllShardConnections(HTAB *shardConnectionHash)
{
	HASH_SEQ_STATUS status;
	ShardConnections *shardConnections = NULL;

	hash_seq_init(&status, shardConnectionHash);

	while ((shardConnections = hash_seq_search(&status)) != 0)
	{
		List *connectionList = shardConnections->connectionList;
		ListCell *connectionCell = NULL;

		foreach(connectionCell, connectionList)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

			UnclaimConnection(connection);
		}
	}
}
