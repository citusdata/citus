/*-------------------------------------------------------------------------
 *
 * placement_connection.c
 *   Per-Placement connection & transaction handling
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "distributed/connection_management.h"
#include "distributed/placement_connection.h"
#include "distributed/metadata_cache.h"
#include "distributed/hash_helpers.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


HTAB *ConnectionPlacementHash = NULL;
HTAB *ConnectionShardHash = NULL;


static void AssociatePlacementWithShard(ConnectionPlacementHashEntry *placementEntry,
										ShardPlacement *placement);


/*
 * GetPlacementConnection establishes a connection for a placement.
 *
 * See StartPlacementConnection for details.
 */
MultiConnection *
GetPlacementConnection(uint32 flags, ShardPlacement *placement)
{
	MultiConnection *connection = StartPlacementConnection(flags, placement);

	FinishConnectionEstablishment(connection);
	return connection;
}


/*
 * StartPlacementConnection() initiates a connection to a remote node,
 * associated with the placement and transaction.
 *
 * The connection is established as the current user & database.
 *
 * See StartNodeUserDatabaseConnection for details.
 *
 * Flags have the corresponding meaning from StartNodeUserDatabaseConnection,
 * except that two additional flags have an effect:
 * - FOR_DML - signal that connection is going to be used for DML (modifications)
 * - FOR_DDL - signal that connection is going to be used for DDL
 *
 * Only one connection associated with the placement may have FOR_DML or
 * FOR_DDL set. This restriction prevents deadlocks and wrong results due to
 * in-progress transactions.
 */
MultiConnection *
StartPlacementConnection(uint32 flags, ShardPlacement *placement)
{
	ConnectionPlacementHashKey key;
	ConnectionPlacementHashEntry *placementEntry = NULL;
	MemoryContext oldContext = NULL;
	bool found = false;
	ConnectionReference *returnConnectionReference = NULL;
	ListCell *referenceCell = NULL;

	key.placementid = placement->placementId;

	/* FIXME: not implemented */
	Assert(flags & NEW_CONNECTION);

	/*
	 * Lookup relevant hash entry. We always enter. If only a cached
	 * connection is desired, and there's none, we'll simply leave the
	 * connection list empty.
	 */

	placementEntry = hash_search(ConnectionPlacementHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		placementEntry->connectionReferences = NIL;
		placementEntry->failed = false;
	}


	/*
	 * Check whether any of the connections already associated with the
	 * placement can be reused, or violates FOR_DML/FOR_DDL constraints.
	 */
	foreach(referenceCell, placementEntry->connectionReferences)
	{
		ConnectionReference *connectionReference = NULL;
		bool useConnection = false;
		MultiConnection *connection = NULL;

		connectionReference = (ConnectionReference *) lfirst(referenceCell);
		connection = connectionReference->connection;

		/* use the connection, unless in a state that's not useful for us */
		if (connection->claimedExclusively ||
			!((flags & CACHED_CONNECTION)) ||
			returnConnectionReference != NULL)
		{
			useConnection = false;
		}
		else
		{
			useConnection = true;
		}

		/*
		 * If not using the connection, verify that FOR_DML/DDL flags are
		 * compatible.
		 */
		if (useConnection)
		{
			returnConnectionReference = connectionReference;
		}
		else if (connectionReference->hadDDL)
		{
			/* XXX: errcode & errmsg */
			ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
							errmsg("cannot establish new placement connection when other "
								   "placement executed DDL")));
		}
		else if (connectionReference->hadDML)
		{
			/* XXX: errcode & errmsg */
			ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
							errmsg("cannot establish new placement connection when other "
								   "placement executed DML")));
		}
	}

	/* no connection available, done if a new connection isn't desirable */
	if (!(flags & NEW_CONNECTION))
	{
		return NULL;
	}


	/*
	 * Either no caching desired, or no connection present. Start connection
	 * establishment.
	 */
	if (returnConnectionReference == NULL)
	{
		MultiConnection *connection = StartNodeConnection(flags, placement->nodeName,
														  placement->nodePort);

		oldContext = MemoryContextSwitchTo(ConnectionContext);
		returnConnectionReference =
			(ConnectionReference *) palloc(sizeof(ConnectionReference));
		returnConnectionReference->connection = connection;
		returnConnectionReference->hadDDL = false;
		returnConnectionReference->hadDML = false;
		placementEntry->connectionReferences =
			lappend(placementEntry->connectionReferences, returnConnectionReference);
		MemoryContextSwitchTo(oldContext);

		AssociatePlacementWithShard(placementEntry, placement);
	}

	if (flags & FOR_DDL)
	{
		returnConnectionReference->hadDDL = true;
	}
	if (flags & FOR_DML)
	{
		returnConnectionReference->hadDML = true;
	}
	if (flags & CRITICAL_CONNECTION)
	{
		RemoteTransaction *transaction =
			&returnConnectionReference->connection->remoteTransaction;
		transaction->criticalTransaction = true;
	}

	return returnConnectionReference->connection;
}


void
InitPlacementConnectionManagement(void)
{
	HASHCTL info;
	uint32 hashFlags = 0;

	/* create (placementid) -> [connection] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionPlacementHashKey);
	info.entrysize = sizeof(ConnectionPlacementHashEntry);
	info.hash = tag_hash;
	info.hcxt = ConnectionContext;
	hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ConnectionPlacementHash = hash_create("citus connection cache (placementid)",
										  64, &info, hashFlags);

	/* create (shardId) -> [ConnectionShardHashEntry] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionShardHashKey);
	info.entrysize = sizeof(ConnectionShardHashEntry);
	info.hash = tag_hash;
	info.hcxt = ConnectionContext;
	hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ConnectionShardHash = hash_create("citus connection cache (shardid)",
									  64, &info, hashFlags);
}


/*
 * Disassociate connections from placements and shards. This will be called at
 * the end of XACT_EVENT_COMMIT and XACT_EVENT_ABORT.
 */
void
ResetPlacementConnectionManagement(void)
{
	/* Simply delete all entries*/
	hash_delete_all(ConnectionPlacementHash);
	hash_delete_all(ConnectionShardHash);
}


/*
 * Check which placements have to be marked as invalid, and/or whether
 * sufficiently many placements have failed to abort the entire coordinated
 * transaction.
 *
 * This will usually be called twice. Once before the remote commit is done,
 * and once after. This is so we can abort before executing remote commits,
 * and so we can handle remote transactions that failed during commit.
 *
 * When preCommit or using2PC is true, failures on transactions marked as
 * critical will abort the entire coordinated transaction. Otherwise we can't
 * anymore, because some remote transactions might have already committed.
 */
void
CheckForFailedPlacements(bool preCommit, bool using2PC)
{
	HASH_SEQ_STATUS status;
	ConnectionShardHashEntry *shardEntry = NULL;

	hash_seq_init(&status, ConnectionShardHash);
	while ((shardEntry = (ConnectionShardHashEntry *) hash_seq_search(&status)) != 0)
	{
		ListCell *placementCell = NULL;
		int failures = 0;
		int successes = 0;

		foreach(placementCell, shardEntry->placementConnections)
		{
			ConnectionPlacementHashEntry *placementEntry =
				(ConnectionPlacementHashEntry *) lfirst(placementCell);
			ListCell *referenceCell = NULL;

			foreach(referenceCell, placementEntry->connectionReferences)
			{
				ConnectionReference *reference =
					(ConnectionReference *) lfirst(referenceCell);
				MultiConnection *connection = reference->connection;

				/*
				 * If neither DDL nor DML were executed, there's no need for
				 * invalidation.
				 */
				if (!reference->hadDDL && !reference->hadDML)
				{
					continue;
				}

				if (connection->remoteTransaction.transactionFailed)
				{
					placementEntry->failed = true;

					/*
					 * Raise an error if failure was on a required connection,
					 * unless we're post-commit and not using 2PC. In that
					 * case escalating failures here could leave inconsistent
					 * shards in place, which are not marked as invalid.
					 *
					 * XXX: should we warn?
					 */
					if (preCommit || using2PC)
					{
						/* to raise ERROR if a required connection */
						MarkRemoteTransactionFailed(connection, true);
					}
				}
			}

			if (placementEntry->failed)
			{
				failures++;
			}
			else
			{
				successes++;
			}
		}

		if (failures > 0 && successes == 0)
		{
			/*
			 * FIXME: arguably we should only error out here if we're
			 * pre-commit or using 2PC. Otherwise we can end up with a state
			 * where parts of the transaction is committed and others aren't,
			 * without correspondingly marking things as invalid (which we
			 * can't, as we would have already committed).
			 */

			/* FIXME: better message */
			ereport(ERROR, (errmsg("could not commit transaction on any active nodes")));
		}

		foreach(placementCell, shardEntry->placementConnections)
		{
			ConnectionPlacementHashEntry *placementEntry =
				(ConnectionPlacementHashEntry *) lfirst(placementCell);

			if (placementEntry->failed)
			{
				UpdateShardPlacementState(placementEntry->key.placementid, FILE_INACTIVE);
			}
		}
	}
}


/* Record shard->placement relation */
static void
AssociatePlacementWithShard(ConnectionPlacementHashEntry *placementEntry,
							ShardPlacement *placement)
{
	ConnectionShardHashKey shardKey;
	ConnectionShardHashEntry *shardEntry = NULL;
	bool found = false;
	MemoryContext oldContext = NULL;
	shardKey.shardId = placement->shardId;
	shardEntry = hash_search(ConnectionShardHash, &shardKey, HASH_ENTER, &found);
	if (!found)
	{
		shardEntry->placementConnections = NIL;
	}

	oldContext = MemoryContextSwitchTo(ConnectionContext);
	shardEntry->placementConnections =
		list_append_unique_ptr(shardEntry->placementConnections, placementEntry);
	MemoryContextSwitchTo(oldContext);
}
