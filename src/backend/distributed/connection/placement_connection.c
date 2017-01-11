/*-------------------------------------------------------------------------
 *
 * placement_connection.c
 *   Per placement connection handling.
 *
 * Copyright (c) 2016-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/metadata_cache.h"
#include "distributed/placement_connection.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/*
 * A connection reference is used to register that a connection has been used
 * to read or modify a shard placement as a particular user.
 */
typedef struct ConnectionReference
{
	/*
	 * The user used to read/modify the placement. We cannot reuse connections
	 * that were performed using a different role, since it would not have the
	 * right permissions.
	 */
	const char *userName;

	/* the connection */
	MultiConnection *connection;

	/*
	 * Information about what the connection is used for. There can only be
	 * one connection executing DDL/DML for a placement to avoid deadlock
	 * issues/read-your-own-writes violations.  The difference between DDL/DML
	 * currently is only used to emit more precise error messages.
	 */
	bool hadDML;
	bool hadDDL;

	/* membership in ConnectionPlacementHashKey->connectionReferences */
	dlist_node placementNode;

	/* membership in MultiConnection->referencedPlacements */
	dlist_node connectionNode;
} ConnectionReference;


/*
 * Hash table mapping placements to a list of connections.
 *
 * This stores a list of connections for each placement, because multiple
 * connections to the same placement may exist at the same time. E.g. a
 * real-time executor query may reference the same placement in several
 * sub-tasks.
 *
 * We keep track about a connection having executed DML or DDL, since we can
 * only ever allow a single transaction to do either to prevent deadlocks and
 * consistency violations (e.g. read-your-own-writes).
 */

/* hash key */
typedef struct ConnectionPlacementHashKey
{
	uint64 placementId;
} ConnectionPlacementHashKey;

/* hash entry */
typedef struct ConnectionPlacementHashEntry
{
	ConnectionPlacementHashKey key;

	/* did any remote transactions fail? */
	bool failed;

	/* list of connections to remote nodes */
	dlist_head connectionReferences;

	/* if non-NULL, connection executing DML/DDL*/
	ConnectionReference *modifyingConnection;

	/* membership in ConnectionShardHashEntry->placementConnections */
	dlist_node shardNode;
} ConnectionPlacementHashEntry;

/* hash table */
static HTAB *ConnectionPlacementHash;


/*
 * Hash table mapping shard ids to placements.
 *
 * This is used to track whether placements of a shard have to be marked
 * invalid after a failure, or whether a coordinated transaction has to be
 * aborted, to avoid all placements of a shard to be marked invalid.
 */

/* hash key */
typedef struct ConnectionShardHashKey
{
	uint64 shardId;
} ConnectionShardHashKey;

/* hash entry */
typedef struct ConnectionShardHashEntry
{
	ConnectionShardHashKey key;
	dlist_head placementConnections;
} ConnectionShardHashEntry;

/* hash table */
static HTAB *ConnectionShardHash;


static ConnectionReference * CheckExistingConnections(uint32 flags, const char *userName,
													  ConnectionPlacementHashEntry *
													  placementEntry);
static bool CanUseExistingConnection(uint32 flags, const char *userName,
									 ConnectionReference *connectionReference);
static void AssociatePlacementWithShard(ConnectionPlacementHashEntry *placementEntry,
										ShardPlacement *placement);
static bool CheckShardPlacements(ConnectionShardHashEntry *shardEntry, bool preCommit,
								 bool using2PC);


/*
 * GetPlacementConnection establishes a connection for a placement.
 *
 * See StartPlacementConnection for details.
 */
MultiConnection *
GetPlacementConnection(uint32 flags, ShardPlacement *placement, const char *userName)
{
	MultiConnection *connection = StartPlacementConnection(flags, placement, userName);

	FinishConnectionEstablishment(connection);
	return connection;
}


/*
 * StartPlacementConnection initiates a connection to a remote node,
 * associated with the placement and transaction.
 *
 * The connection is established for the current database. If userName is NULL
 * the current user is used, otherwise the provided one.
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
StartPlacementConnection(uint32 flags, ShardPlacement *placement, const char *userName)
{
	ConnectionPlacementHashKey key;
	ConnectionPlacementHashEntry *placementEntry = NULL;
	ConnectionReference *returnConnectionReference = NULL;
	char *freeUserName = NULL;
	bool found = false;

	if (userName == NULL)
	{
		userName = freeUserName = CurrentUserName();
	}

	key.placementId = placement->placementId;

	/* lookup relevant hash entry */
	placementEntry = hash_search(ConnectionPlacementHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		dlist_init(&placementEntry->connectionReferences);
		placementEntry->failed = false;
		placementEntry->modifyingConnection = NULL;
	}

	/*
	 * Check whether any of the connections already associated with the
	 * placement can be reused, or violates FOR_DML/FOR_DDL constraints.
	 */
	returnConnectionReference = CheckExistingConnections(flags, userName, placementEntry);

	/*
	 * Either no caching desired, or no connection present. Start connection
	 * establishment.  Allocations are performed in transaction context, so we
	 * don't have to care about freeing in case of an early disconnect.
	 */
	if (returnConnectionReference == NULL)
	{
		MultiConnection *connection = StartNodeConnection(flags, placement->nodeName,
														  placement->nodePort);

		returnConnectionReference = (ConnectionReference *)
									MemoryContextAlloc(TopTransactionContext,
													   sizeof(ConnectionReference));
		returnConnectionReference->connection = connection;
		returnConnectionReference->hadDDL = false;
		returnConnectionReference->hadDML = false;
		returnConnectionReference->userName =
			MemoryContextStrdup(TopTransactionContext, userName);
		dlist_push_tail(&placementEntry->connectionReferences,
						&returnConnectionReference->placementNode);

		/* record association with shard, for invalidation */
		AssociatePlacementWithShard(placementEntry, placement);

		/* record association with connection, to handle connection closure */
		dlist_push_tail(&connection->referencedPlacements,
						&returnConnectionReference->connectionNode);
	}

	if (flags & FOR_DDL)
	{
		placementEntry->modifyingConnection = returnConnectionReference;
		returnConnectionReference->hadDDL = true;
	}
	if (flags & FOR_DML)
	{
		placementEntry->modifyingConnection = returnConnectionReference;
		returnConnectionReference->hadDML = true;
	}

	if (freeUserName)
	{
		pfree(freeUserName);
	}

	return returnConnectionReference->connection;
}


/*
 * CheckExistingConnections check whether any of the existing connections is
 * usable. If so, return it, otherwise return NULL.
 *
 * A connection is usable if it is not in use, the user matches, DDL/DML usage
 * match and cached connection are allowed.  If no existing connection
 * matches, but a new connection would conflict (e.g. a connection already
 * exists but isn't usable, and the desired connection needs to execute
 * DML/DML) an error is thrown.
 */
static ConnectionReference *
CheckExistingConnections(uint32 flags, const char *userName,
						 ConnectionPlacementHashEntry *placementEntry)
{
	dlist_iter it;

	/*
	 * If there's a connection that has executed DML/DDL, always return it if
	 * possible. That's because we only can execute DML/DDL over that
	 * connection.  Checking this first also allows us to detect conflicts due
	 * to opening a second connection that wants to execute DML/DDL.
	 */
	if (placementEntry->modifyingConnection)
	{
		ConnectionReference *connectionReference = placementEntry->modifyingConnection;

		if (CanUseExistingConnection(flags, userName, connectionReference))
		{
			return connectionReference;
		}

		if (connectionReference->hadDDL)
		{
			/* would deadlock otherwise */
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot establish new placement connection when DDL has been "
							"executed on existing placement connection")));
		}
		else if (connectionReference->hadDML)
		{
			/* we'd not see the other connection's contents */
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot establish new placement connection when DML has been "
							"executed on existing placement connection")));
		}
		else
		{
			/* modifying xact should have executed DML/DDL */
			Assert(false);
		}
	}

	/*
	 * Search existing connections for a reusable connection.
	 */
	dlist_foreach(it, &placementEntry->connectionReferences)
	{
		ConnectionReference *connectionReference =
			dlist_container(ConnectionReference, placementNode, it.cur);

		if (CanUseExistingConnection(flags, userName, connectionReference))
		{
			return connectionReference;
		}

		/*
		 * If not using the connection, verify that FOR_DML/DDL flags are
		 * compatible.
		 */
		if (flags & FOR_DDL)
		{
			/* would deadlock otherwise */
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot establish new placement connection for DDL when "
							"other connections exist")));
		}
		else if (flags & FOR_DML)
		{
			/*
			 * We could allow this case (as presumably the SELECT is done, but
			 * it'd be a bit prone to programming errors, so we disallow for
			 * now.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot establish new placement connection for DML when "
							"other connections exist")));
		}
	}

	/* establish a new connection */
	return NULL;
}


/*
 * CanUseExistingConnection is a helper function for CheckExistingConnections()
 * that checks whether an existing connection can be reused.
 */
static bool
CanUseExistingConnection(uint32 flags, const char *userName,
						 ConnectionReference *connectionReference)
{
	MultiConnection *connection = connectionReference->connection;

	if (!connection)
	{
		/* if already closed connection obviously not usable */
		return false;
	}
	else if (connection->claimedExclusively)
	{
		/* already used */
		return false;
	}
	else if (flags & FORCE_NEW_CONNECTION)
	{
		/* no connection reuse desired */
		return false;
	}
	else if (strcmp(connectionReference->userName, userName) != 0)
	{
		/* connection for different user, check for conflict */
		return false;
	}
	else
	{
		return true;
	}
}


/*
 * AssociatePlacementWithShard records shard->placement relation in
 * ConnectionShardHash.
 *
 * That association is later used, in CheckForFailedPlacements, to invalidate
 * shard placements if necessary.
 */
static void
AssociatePlacementWithShard(ConnectionPlacementHashEntry *placementEntry,
							ShardPlacement *placement)
{
	ConnectionShardHashKey shardKey;
	ConnectionShardHashEntry *shardEntry = NULL;
	bool found = false;
	dlist_iter placementIter;

	shardKey.shardId = placement->shardId;
	shardEntry = hash_search(ConnectionShardHash, &shardKey, HASH_ENTER, &found);
	if (!found)
	{
		dlist_init(&shardEntry->placementConnections);
	}

	/*
	 * Check if placement is already associated with shard (happens if there's
	 * multiple connections for a placement).  There'll usually only be few
	 * placement per shard, so the price of iterating isn't large.
	 */
	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);

		if (placementEntry->key.placementId == placement->placementId)
		{
			return;
		}
	}

	/* otherwise add */
	dlist_push_tail(&shardEntry->placementConnections, &placementEntry->shardNode);
}


/*
 * CloseShardPlacementAssociation handles a connection being closed before
 * transaction end.
 *
 * This should only be called by connection_management.c.
 */
void
CloseShardPlacementAssociation(struct MultiConnection *connection)
{
	dlist_iter placementIter;

	/* set connection to NULL for all references to the connection */
	dlist_foreach(placementIter, &connection->referencedPlacements)
	{
		ConnectionReference *reference =
			dlist_container(ConnectionReference, connectionNode, placementIter.cur);

		reference->connection = NULL;

		/*
		 * Note that we don't reset ConnectionPlacementHashEntry's
		 * modifyingConnection here, that'd more complicated than it seems
		 * worth.  That means we'll error out spuriously if a DML/DDL
		 * executing connection is closed earlier in a transaction.
		 */
	}
}


/*
 * ResetShardPlacementAssociation resets the association of connections to
 * shard placements at the end of a transaction.
 *
 * This should only be called by connection_management.c.
 */
void
ResetShardPlacementAssociation(struct MultiConnection *connection)
{
	dlist_init(&connection->referencedPlacements);
}


/*
 * ResetPlacementConnectionManagement() disassociates connections from
 * placements and shards. This will be called at the end of XACT_EVENT_COMMIT
 * and XACT_EVENT_ABORT.
 */
void
ResetPlacementConnectionManagement(void)
{
	/* Simply delete all entries */
	hash_delete_all(ConnectionPlacementHash);
	hash_delete_all(ConnectionShardHash);

	/*
	 * NB: memory for ConnectionReference structs and subordinate data is
	 * deleted by virtue of being allocated in TopTransactionContext.
	 */
}


/*
 * CheckForFailedPlacements checks which placements have to be marked as
 * invalid, and/or whether sufficiently many placements have failed to abort
 * the entire coordinated transaction.
 *
 * This will usually be called twice. Once before the remote commit is done,
 * and once after. This is so we can abort before executing remote commits,
 * and so we can handle remote transactions that failed during commit.
 *
 * When preCommit or using2PC is true, failures on transactions marked as
 * critical will abort the entire coordinated transaction. If not we can't
 * roll back, because some remote transactions might have already committed.
 */
void
CheckForFailedPlacements(bool preCommit, bool using2PC)
{
	HASH_SEQ_STATUS status;
	ConnectionShardHashEntry *shardEntry = NULL;
	int successes = 0;
	int attempts = 0;

	hash_seq_init(&status, ConnectionShardHash);
	while ((shardEntry = (ConnectionShardHashEntry *) hash_seq_search(&status)) != 0)
	{
		attempts++;
		if (CheckShardPlacements(shardEntry, preCommit, using2PC))
		{
			successes++;
		}
	}

	/*
	 * If no shards could be modified at all, error out. Doesn't matter if
	 * we're post-commit - there's nothing to invalidate.
	 */
	if (attempts > 0 && successes == 0)
	{
		ereport(ERROR, (errmsg("could not commit transaction on any active node")));
	}
}


/*
 * CheckShardPlacements is a helper function for CheckForFailedPlacements that
 * performs the per-shard work.
 */
static bool
CheckShardPlacements(ConnectionShardHashEntry *shardEntry,
					 bool preCommit, bool using2PC)
{
	int failures = 0;
	int successes = 0;
	dlist_iter placementIter;

	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);
		ConnectionReference *modifyingConnection = placementEntry->modifyingConnection;
		MultiConnection *connection = NULL;

		/* we only consider shards that are modified */
		if (modifyingConnection == NULL)
		{
			continue;
		}

		connection = modifyingConnection->connection;

		if (!connection || connection->remoteTransaction.transactionFailed)
		{
			placementEntry->failed = true;
			failures++;
		}
		else
		{
			successes++;
		}
	}

	if (failures > 0 && successes == 0)
	{
		int elevel = 0;

		/*
		 * Only error out if we're pre-commit or using 2PC. Can't error
		 * otherwise as we can end up with a state where some shard
		 * modifications have already committed successfully.  If no
		 * modifications at all succeed, CheckForFailedPlacements() will error
		 * out.  This sucks.
		 */
		if (preCommit || using2PC)
		{
			elevel = ERROR;
		}
		else
		{
			elevel = WARNING;
		}

		ereport(elevel,
				(errmsg("could not commit transaction for shard " INT64_FORMAT
						" on any active node",
						shardEntry->key.shardId)));
		return false;
	}

	/* mark all failed placements invalid */
	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);

		if (placementEntry->failed)
		{
			UpdateShardPlacementState(placementEntry->key.placementId, FILE_INACTIVE);
		}
	}

	return true;
}


/*
 * InitPlacementConnectionManagement performs initialization of the
 * infrastructure in this file at server start.
 */
void
InitPlacementConnectionManagement(void)
{
	HASHCTL info;
	uint32 hashFlags = 0;

	/* create (placementId) -> [ConnectionReference] hash */
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
