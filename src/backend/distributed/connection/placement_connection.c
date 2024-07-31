/*-------------------------------------------------------------------------
 *
 * placement_connection.c
 *   Per placement connection handling.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/hash.h"
#include "common/hashfn.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "pg_version_constants.h"

#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/distributed_planner.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"


/*
 * A connection reference is used to register that a connection has been used
 * to read or modify either a) a shard placement as a particular user b) a
 * group of colocated placements (which depend on whether the reference is
 * from ConnectionPlacementHashEntry or ColocatedPlacementHashEntry).
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

	/* colocation group of the placement, if any */
	uint32 colocationGroupId;
	uint32 representativeValue;

	/* placementId of the placement, used only for append distributed tables */
	uint64 placementId;

	/* membership in MultiConnection->referencedPlacements */
	dlist_node connectionNode;
} ConnectionReference;


struct ColocatedPlacementsHashEntry;


/*
 * Hash table mapping placements to a list of connections.
 *
 * This stores a list of connections for each placement, because multiple
 * connections to the same placement may exist at the same time. E.g. an
 * adaptive executor query may reference the same placement in several
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

	/* primary connection used to access the placement */
	ConnectionReference *primaryConnection;

	/* are any other connections reading from the placements? */
	bool hasSecondaryConnections;

	/* entry for the set of co-located placements */
	struct ColocatedPlacementsHashEntry *colocatedEntry;

	/* membership in ConnectionShardHashEntry->placementConnections */
	dlist_node shardNode;
} ConnectionPlacementHashEntry;

/* hash table */
static HTAB *ConnectionPlacementHash;


/*
 * A hash-table mapping colocated placements to connections. Colocated
 * placements being the set of placements on a single node that represent the
 * same value range. This is needed because connections for colocated
 * placements (i.e. the corresponding placements for different colocated
 * distributed tables) need to share connections.  Otherwise things like
 * foreign keys can very easily lead to unprincipled deadlocks.  This means
 * that there can only be one DML/DDL connection for a set of colocated
 * placements.
 *
 * A set of colocated placements is identified, besides node identifying
 * information, by the associated colocation group id and the placement's
 * 'representativeValue' which currently is the lower boundary of it's
 * hash-range.
 *
 * Note that this hash-table only contains entries for hash-partitioned
 * tables, because others so far don't support colocation.
 */

/* hash key */
typedef struct ColocatedPlacementsHashKey
{
	/* to identify host - database can't differ */
	uint32 nodeId;

	/* colocation group, or invalid */
	uint32 colocationGroupId;

	/* to represent the value range */
	uint32 representativeValue;
} ColocatedPlacementsHashKey;

/* hash entry */
typedef struct ColocatedPlacementsHashEntry
{
	ColocatedPlacementsHashKey key;

	/* primary connection used to access the co-located placements */
	ConnectionReference *primaryConnection;

	/* are any other connections reading from the placements? */
	bool hasSecondaryConnections;
}  ColocatedPlacementsHashEntry;

static HTAB *ColocatedPlacementsHash;


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


static MultiConnection * FindPlacementListConnection(int flags, List *placementAccessList,
													 const char *userName);
static ConnectionPlacementHashEntry * FindOrCreatePlacementEntry(
	ShardPlacement *placement);
static bool CanUseExistingConnection(uint32 flags, const char *userName,
									 ConnectionReference *placementConnection);
static bool ConnectionAccessedDifferentPlacement(MultiConnection *connection,
												 ShardPlacement *placement);
static void AssociatePlacementWithShard(ConnectionPlacementHashEntry *placementEntry,
										ShardPlacement *placement);
static bool HasModificationFailedForShard(ConnectionShardHashEntry *shardEntry);
static uint32 ColocatedPlacementsHashHash(const void *key, Size keysize);
static int ColocatedPlacementsHashCompare(const void *a, const void *b, Size keysize);


/*
 * GetPlacementConnection establishes a connection for a placement.
 *
 * See StartPlacementConnection for details.
 */
MultiConnection *
GetPlacementConnection(uint32 flags, ShardPlacement *placement, const char *userName)
{
	MultiConnection *connection = StartPlacementConnection(flags, placement, userName);

	if (connection == NULL)
	{
		/* connection can only be NULL for optional connections */
		Assert((flags & OPTIONAL_CONNECTION));

		return NULL;
	}

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
 * FOR_DDL set. For hash-partitioned tables only one connection for a set of
 * colocated placements may have FOR_DML/DDL set.  This restriction prevents
 * deadlocks and wrong results due to in-progress transactions.
 */
MultiConnection *
StartPlacementConnection(uint32 flags, ShardPlacement *placement, const char *userName)
{
	ShardPlacementAccess *placementAccess =
		(ShardPlacementAccess *) palloc0(sizeof(ShardPlacementAccess));

	placementAccess->placement = placement;

	if (flags & FOR_DDL)
	{
		placementAccess->accessType = PLACEMENT_ACCESS_DDL;
	}
	else if (flags & FOR_DML)
	{
		placementAccess->accessType = PLACEMENT_ACCESS_DML;
	}
	else
	{
		placementAccess->accessType = PLACEMENT_ACCESS_SELECT;
	}

	return StartPlacementListConnection(flags, list_make1(placementAccess), userName);
}


/*
 * StartPlacementListConnection returns a connection to a remote node suitable for
 * a placement accesses (SELECT, DML, DDL) or throws an error if no suitable
 * connection can be established if would cause a self-deadlock or consistency
 * violation.
 */
MultiConnection *
StartPlacementListConnection(uint32 flags, List *placementAccessList,
							 const char *userName)
{
	char *freeUserName = NULL;

	if (userName == NULL)
	{
		userName = freeUserName = CurrentUserName();
	}

	MultiConnection *chosenConnection = FindPlacementListConnection(flags,
																	placementAccessList,
																	userName);
	if (chosenConnection == NULL)
	{
		/* use the first placement from the list to extract nodename and nodeport */
		ShardPlacementAccess *placementAccess =
			(ShardPlacementAccess *) linitial(placementAccessList);
		ShardPlacement *placement = placementAccess->placement;
		char *nodeName = placement->nodeName;
		int nodePort = placement->nodePort;

		/*
		 * No suitable connection in the placement->connection mapping, get one from
		 * the node->connection pool.
		 */
		chosenConnection = StartNodeUserDatabaseConnection(flags, nodeName, nodePort,
														   userName, NULL);
		if (chosenConnection == NULL)
		{
			/* connection can only be NULL for optional connections */
			Assert((flags & OPTIONAL_CONNECTION));

			return NULL;
		}

		if ((flags & REQUIRE_CLEAN_CONNECTION) &&
			ConnectionAccessedDifferentPlacement(chosenConnection, placement))
		{
			/*
			 * Cached connection accessed a non-co-located placement in the same
			 * table or co-location group, while the caller asked for a clean
			 * connection. Open a new connection instead.
			 *
			 * We use this for situations in which we want to use a different
			 * connection for every placement, such as COPY. If we blindly returned
			 * a cached connection that already modified a different, non-co-located
			 * placement B in the same table or in a table with the same co-location
			 * ID as the current placement, then we'd no longer able to write to
			 * placement B later in the COPY.
			 */
			chosenConnection = StartNodeUserDatabaseConnection(flags |
															   FORCE_NEW_CONNECTION,
															   nodeName, nodePort,
															   userName, NULL);

			if (chosenConnection == NULL)
			{
				/* connection can only be NULL for optional connections */
				Assert((flags & OPTIONAL_CONNECTION));

				return NULL;
			}

			Assert(!ConnectionAccessedDifferentPlacement(chosenConnection, placement));
		}
	}

	/* remember which connection we're going to use to access the placements */
	AssignPlacementListToConnection(placementAccessList, chosenConnection);

	if (freeUserName)
	{
		pfree(freeUserName);
	}

	return chosenConnection;
}


/*
 * AssignPlacementListToConnection assigns a set of shard placement accesses to a
 * given connection, meaning that connection must be used for all (conflicting)
 * accesses of the same shard placements to make sure reads see writes and to
 * make sure we don't take conflicting locks.
 */
void
AssignPlacementListToConnection(List *placementAccessList, MultiConnection *connection)
{
	const char *userName = connection->user;

	ShardPlacementAccess *placementAccess = NULL;
	foreach_declared_ptr(placementAccess, placementAccessList)
	{
		ShardPlacement *placement = placementAccess->placement;
		ShardPlacementAccessType accessType = placementAccess->accessType;


		if (placement->shardId == INVALID_SHARD_ID)
		{
			/*
			 * When a SELECT prunes down to 0 shard, we use a dummy placement
			 * which is only used to route the query to a worker node, but
			 * the SELECT doesn't actually access any shard placement.
			 *
			 * FIXME: this can be removed if we evaluate empty SELECTs locally.
			 */
			continue;
		}

		ConnectionPlacementHashEntry *placementEntry = FindOrCreatePlacementEntry(
			placement);
		ConnectionReference *placementConnection = placementEntry->primaryConnection;

		if (placementConnection->connection == connection)
		{
			/* using the connection that was already assigned to the placement */
		}
		else if (placementConnection->connection == NULL)
		{
			/* placement does not have a connection assigned yet */
			placementConnection->connection = connection;
			placementConnection->hadDDL = false;
			placementConnection->hadDML = false;
			placementConnection->userName = MemoryContextStrdup(TopTransactionContext,
																userName);
			placementConnection->placementId = placementAccess->placement->placementId;

			/* record association with connection */
			dlist_push_tail(&connection->referencedPlacements,
							&placementConnection->connectionNode);
		}
		else
		{
			/* using a different connection than the one assigned to the placement */

			if (accessType != PLACEMENT_ACCESS_SELECT)
			{
				/*
				 * We previously read from the placement, but now we're writing to
				 * it (if we had written to the placement, we would have either chosen
				 * the same connection, or errored out). Update the connection reference
				 * to point to the connection used for writing. We don't need to remember
				 * the existing connection since we won't be able to reuse it for
				 * accessing the placement. However, we do register that it exists in
				 * hasSecondaryConnections.
				 */
				placementConnection->connection = connection;
				placementConnection->userName = MemoryContextStrdup(TopTransactionContext,
																	userName);

				Assert(!placementConnection->hadDDL);
				Assert(!placementConnection->hadDML);

				/* record association with connection */
				dlist_push_tail(&connection->referencedPlacements,
								&placementConnection->connectionNode);
			}

			/*
			 * There are now multiple connections that read from the placement
			 * and DDL commands are forbidden.
			 */
			placementEntry->hasSecondaryConnections = true;

			if (placementEntry->colocatedEntry != NULL)
			{
				/* we also remember this for co-located placements */
				placementEntry->colocatedEntry->hasSecondaryConnections = true;
			}
		}

		/*
		 * Remember that we used the current connection for writes.
		 */
		if (accessType == PLACEMENT_ACCESS_DDL)
		{
			placementConnection->hadDDL = true;
		}

		if (accessType == PLACEMENT_ACCESS_DML)
		{
			placementConnection->hadDML = true;
		}

		/* record the relation access */
		Oid relationId = RelationIdForShard(placement->shardId);
		RecordRelationAccessIfNonDistTable(relationId, accessType);
	}
}


/*
 * GetConnectionIfPlacementAccessedInXact returns the connection over which
 * the placement has been access in the transaction. If not found, returns
 * NULL.
 */
MultiConnection *
GetConnectionIfPlacementAccessedInXact(int flags, List *placementAccessList,
									   const char *userName)
{
	char *freeUserName = NULL;

	if (userName == NULL)
	{
		userName = freeUserName = CurrentUserName();
	}

	MultiConnection *connection = FindPlacementListConnection(flags, placementAccessList,
															  userName);

	if (freeUserName != NULL)
	{
		pfree(freeUserName);
	}

	return connection;
}


/*
 * FindPlacementListConnection determines whether there is a connection that must
 * be used to perform the given placement accesses.
 *
 * If a placement was only read in this transaction, then the same connection must
 * be used for DDL to prevent self-deadlock. If a placement was modified in this
 * transaction, then the same connection must be used for all subsequent accesses
 * to ensure read-your-writes consistency and prevent self-deadlock. If those
 * conditions cannot be met, because a connection is in use or the placements in
 * the placement access list were modified over multiple connections, then this
 * function throws an error.
 *
 * The function returns the connection that needs to be used, if such a connection
 * exists.
 */
static MultiConnection *
FindPlacementListConnection(int flags, List *placementAccessList, const char *userName)
{
	bool foundModifyingConnection = false;
	MultiConnection *chosenConnection = NULL;

	/*
	 * Go through all placement accesses to find a suitable connection.
	 *
	 * If none of the placements have been accessed in this transaction, connection
	 * remains NULL.
	 *
	 * If one or more of the placements have been modified in this transaction, then
	 * use the connection that performed the write. If placements have been written
	 * over multiple connections or the connection is not available, error out.
	 *
	 * If placements have only been read in this transaction, then use the last
	 * suitable connection found for a placement in the placementAccessList.
	 */
	ShardPlacementAccess *placementAccess = NULL;
	foreach_declared_ptr(placementAccess, placementAccessList)
	{
		ShardPlacement *placement = placementAccess->placement;
		ShardPlacementAccessType accessType = placementAccess->accessType;


		if (placement->shardId == INVALID_SHARD_ID)
		{
			/*
			 * When a SELECT prunes down to 0 shard, we use a dummy placement.
			 * In that case, we can fall back to the default connection.
			 *
			 * FIXME: this can be removed if we evaluate empty SELECTs locally.
			 */
			continue;
		}

		ConnectionPlacementHashEntry *placementEntry = FindOrCreatePlacementEntry(
			placement);
		ColocatedPlacementsHashEntry *colocatedEntry = placementEntry->colocatedEntry;
		ConnectionReference *placementConnection = placementEntry->primaryConnection;

		/* note: the Asserts below are primarily for clarifying the conditions */

		if (placementConnection->connection == NULL)
		{
			/* no connection has been chosen for the placement */
		}
		else if (accessType == PLACEMENT_ACCESS_DDL &&
				 placementEntry->hasSecondaryConnections)
		{
			/*
			 * If a placement has been read over multiple connections (typically as
			 * a result of a reference table join) then a DDL command on the placement
			 * would create a self-deadlock.
			 */

			Assert(placementConnection != NULL);

			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot perform DDL on placement " UINT64_FORMAT
							", which has been read over multiple connections",
							placement->placementId)));
		}
		else if (accessType == PLACEMENT_ACCESS_DDL && colocatedEntry != NULL &&
				 colocatedEntry->hasSecondaryConnections)
		{
			/*
			 * If a placement has been read over multiple (uncommitted) connections
			 * then a DDL command on a co-located placement may create a self-deadlock
			 * if there exist some relationship between the co-located placements
			 * (e.g. foreign key, partitioning).
			 */

			Assert(placementConnection != NULL);

			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot perform DDL on placement " UINT64_FORMAT
							" since a co-located placement has been read over multiple connections",
							placement->placementId)));
		}
		else if (foundModifyingConnection)
		{
			/*
			 * We already found a connection that performed writes on of the placements
			 * and must use it.
			 */

			if ((placementConnection->hadDDL || placementConnection->hadDML) &&
				placementConnection->connection != chosenConnection)
			{
				/*
				 * The current placement may have been modified over a different
				 * connection. Neither connection is guaranteed to see all uncomitted
				 * writes and therefore we cannot proceed.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						 errmsg("cannot perform query with placements that were "
								"modified over multiple connections")));
			}
		}
		else if (accessType == PLACEMENT_ACCESS_SELECT &&
				 placementEntry->hasSecondaryConnections &&
				 !placementConnection->hadDDL && !placementConnection->hadDML)
		{
			/*
			 * Two separate connections have already selected from this placement
			 * and it was not modified. There is no benefit to using this connection.
			 */
		}
		else if (CanUseExistingConnection(flags, userName, placementConnection))
		{
			/*
			 * There is an existing connection for the placement and we can use it.
			 */

			Assert(placementConnection != NULL);
			chosenConnection = placementConnection->connection;

			if (placementConnection->hadDDL || placementConnection->hadDML)
			{
				/* this connection performed writes, we must use it */
				foundModifyingConnection = true;
			}
		}
		else if (placementConnection->hadDDL || placementConnection->hadDML)
		{
			if (strcmp(placementConnection->userName, userName) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						 errmsg("cannot perform query on placements that were "
								"modified in this transaction by a different "
								"user")));
			}
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot perform query, because modifications were "
							"made over a connection that cannot be used at "
							"this time. This is most likely a Citus bug so "
							"please report it"
							)));
		}
	}

	return chosenConnection;
}


/*
 * FindOrCreatePlacementEntry finds a placement entry in either the
 * placement->connection hash or the co-located placements->connection hash,
 * or adds a new entry if the placement has not yet been accessed in the
 * current transaction.
 */
static ConnectionPlacementHashEntry *
FindOrCreatePlacementEntry(ShardPlacement *placement)
{
	ConnectionPlacementHashKey connKey;
	bool found = false;

	connKey.placementId = placement->placementId;

	ConnectionPlacementHashEntry *placementEntry = hash_search(ConnectionPlacementHash,
															   &connKey, HASH_ENTER,
															   &found);
	if (!found)
	{
		/* no connection has been chosen for this placement */
		placementEntry->failed = false;
		placementEntry->primaryConnection = NULL;
		placementEntry->hasSecondaryConnections = false;
		placementEntry->colocatedEntry = NULL;

		if (placement->partitionMethod == DISTRIBUTE_BY_HASH ||
			placement->partitionMethod == DISTRIBUTE_BY_NONE)
		{
			ColocatedPlacementsHashKey coloKey;

			coloKey.nodeId = placement->nodeId;
			coloKey.colocationGroupId = placement->colocationGroupId;
			coloKey.representativeValue = placement->representativeValue;

			/* look for a connection assigned to co-located placements */
			ColocatedPlacementsHashEntry *colocatedEntry = hash_search(
				ColocatedPlacementsHash, &coloKey, HASH_ENTER,
				&found);
			if (!found)
			{
				void *conRef = MemoryContextAllocZero(TopTransactionContext,
													  sizeof(ConnectionReference));

				ConnectionReference *connectionReference = (ConnectionReference *) conRef;

				/*
				 * Store the co-location group information such that we can later
				 * determine whether a connection accessed different placements
				 * of the same co-location group.
				 */
				connectionReference->colocationGroupId = placement->colocationGroupId;
				connectionReference->representativeValue = placement->representativeValue;

				/*
				 * Create a connection reference that can be used for the entire
				 * set of co-located placements.
				 */
				colocatedEntry->primaryConnection = connectionReference;

				colocatedEntry->hasSecondaryConnections = false;
			}

			/*
			 * Assign the connection reference for the set of co-located placements
			 * to the current placement.
			 */
			placementEntry->primaryConnection = colocatedEntry->primaryConnection;
			placementEntry->colocatedEntry = colocatedEntry;
		}
		else
		{
			void *conRef = MemoryContextAllocZero(TopTransactionContext,
												  sizeof(ConnectionReference));

			placementEntry->primaryConnection = (ConnectionReference *) conRef;
		}
	}

	/* record association with shard, for invalidation */
	AssociatePlacementWithShard(placementEntry, placement);

	return placementEntry;
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
 * ConnectionAccessedDifferentPlacement returns true if the connection accessed another
 * placement in the same colocation group with a different representative value,
 * meaning it's not strictly colocated.
 */
static bool
ConnectionAccessedDifferentPlacement(MultiConnection *connection,
									 ShardPlacement *placement)
{
	dlist_iter placementIter;

	dlist_foreach(placementIter, &connection->referencedPlacements)
	{
		ConnectionReference *connectionReference =
			dlist_container(ConnectionReference, connectionNode, placementIter.cur);

		/* handle append and range distributed tables */
		if (placement->partitionMethod != DISTRIBUTE_BY_HASH &&
			placement->placementId != connectionReference->placementId)
		{
			return true;
		}

		/* handle hash distributed tables */
		if (placement->colocationGroupId != INVALID_COLOCATION_ID &&
			placement->colocationGroupId == connectionReference->colocationGroupId &&
			placement->representativeValue != connectionReference->representativeValue)
		{
			/* non-co-located placements from the same co-location group */
			return true;
		}
	}

	return false;
}


/*
 * ConnectionModifiedPlacement returns true if any DML or DDL is executed over
 * the connection on any placement/table.
 */
bool
ConnectionModifiedPlacement(MultiConnection *connection)
{
	dlist_iter placementIter;

	if (connection->remoteTransaction.transactionState == REMOTE_TRANS_NOT_STARTED)
	{
		/*
		 * When StartPlacementListConnection() is called, we set the
		 * hadDDL/hadDML even before the actual command is sent to
		 * remote nodes. And, if this function is called at that
		 * point, we should not assume that the connection has already
		 * done any modifications.
		 */
		return false;
	}

	if (dlist_is_empty(&connection->referencedPlacements))
	{
		/*
		 * When referencesPlacements are empty, it means that we come here
		 * from an API that uses a node connection (e.g., not placement connection),
		 * which doesn't set placements.
		 * In that case, the command sent could be either write or read, so we assume
		 * it is write to be on the safe side.
		 */
		return true;
	}

	dlist_foreach(placementIter, &connection->referencedPlacements)
	{
		ConnectionReference *connectionReference =
			dlist_container(ConnectionReference, connectionNode, placementIter.cur);

		if (connectionReference->hadDDL || connectionReference->hadDML)
		{
			return true;
		}
	}

	return false;
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
	bool found = false;
	dlist_iter placementIter;

	shardKey.shardId = placement->shardId;
	ConnectionShardHashEntry *shardEntry = hash_search(ConnectionShardHash, &shardKey,
													   HASH_ENTER, &found);
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
		ConnectionPlacementHashEntry *currPlacementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);

		if (currPlacementEntry->key.placementId == placement->placementId)
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
		 * primaryConnection here, that'd be more complicated than it seems
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
 * ResetPlacementConnectionManagement() dissociates connections from
 * placements and shards. This will be called at the end of XACT_EVENT_COMMIT
 * and XACT_EVENT_ABORT.
 */
void
ResetPlacementConnectionManagement(void)
{
	/* Simply delete all entries */
	hash_delete_all(ConnectionPlacementHash);
	hash_delete_all(ConnectionShardHash);
	hash_delete_all(ColocatedPlacementsHash);

	/*
	 * NB: memory for ConnectionReference structs and subordinate data is
	 * deleted by virtue of being allocated in TopTransactionContext.
	 */
}


/*
 * ErrorIfPostCommitFailedShardPlacements throws an error if any of the placements
 * that modified the database and involved in the transaction has failed.
 *
 * Note that Citus already fails queries/commands in case of any failures during query
 * processing. However, there are certain failures that can only be detected on the
 * COMMIT time. And, this check mainly ensures to catch errors that happens on the
 * COMMIT time on the placements.
 *
 * The most common example for this case is the deferred errors that are thrown by
 * triggers or constraints at the COMMIT time.
 */
void
ErrorIfPostCommitFailedShardPlacements(void)
{
	HASH_SEQ_STATUS status;
	ConnectionShardHashEntry *shardEntry = NULL;

	hash_seq_init(&status, ConnectionShardHash);
	while ((shardEntry = (ConnectionShardHashEntry *) hash_seq_search(&status)) != 0)
	{
		if (HasModificationFailedForShard(shardEntry))
		{
			ereport(ERROR,
					(errmsg("could not commit transaction for shard " INT64_FORMAT
							" on at least one active node", shardEntry->key.shardId)));
		}
	}
}


/*
 * HasModificationFailedForShard is a helper function for
 * ErrorIfPostCommitFailedShardPlacements that performs the per-shard work.
 *
 * The function returns true if any placement of the input shard is modified
 * and any failures has happened (either connection failures or transaction
 * failures).
 */
static bool
HasModificationFailedForShard(ConnectionShardHashEntry *shardEntry)
{
	dlist_iter placementIter;

	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);
		ConnectionReference *primaryConnection = placementEntry->primaryConnection;

		/* we only consider shards that are modified */
		if (primaryConnection == NULL ||
			!(primaryConnection->hadDDL || primaryConnection->hadDML))
		{
			continue;
		}

		MultiConnection *connection = primaryConnection->connection;

		if (!connection || connection->remoteTransaction.transactionFailed)
		{
			return true;
		}
	}

	return false;
}


/*
 * InitPlacementConnectionManagement performs initialization of the
 * infrastructure in this file at server start.
 */
void
InitPlacementConnectionManagement(void)
{
	HASHCTL info;

	/* create (placementId) -> [ConnectionReference] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionPlacementHashKey);
	info.entrysize = sizeof(ConnectionPlacementHashEntry);
	info.hash = tag_hash;
	info.hcxt = ConnectionContext;
	uint32 hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ConnectionPlacementHash = hash_create("citus connection cache (placementid)",
										  64, &info, hashFlags);

	/* create (colocated placement identity) -> [ConnectionReference] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ColocatedPlacementsHashKey);
	info.entrysize = sizeof(ColocatedPlacementsHashEntry);
	info.hash = ColocatedPlacementsHashHash;
	info.match = ColocatedPlacementsHashCompare;
	info.hcxt = ConnectionContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	ColocatedPlacementsHash = hash_create("citus connection cache (colocated placements)",
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
 * UseConnectionPerPlacement returns whether we should use as separate connection
 * per placement even if another connection is idle. We mostly use this in testing
 * scenarios.
 */
bool
UseConnectionPerPlacement(void)
{
	return ForceMaxQueryParallelization &&
		   MultiShardConnectionType != SEQUENTIAL_CONNECTION;
}


static uint32
ColocatedPlacementsHashHash(const void *key, Size keysize)
{
	ColocatedPlacementsHashKey *entry = (ColocatedPlacementsHashKey *) key;

	uint32 hash = hash_uint32(entry->nodeId);
	hash = hash_combine(hash, hash_uint32(entry->colocationGroupId));
	hash = hash_combine(hash, hash_uint32(entry->representativeValue));

	return hash;
}


static int
ColocatedPlacementsHashCompare(const void *a, const void *b, Size keysize)
{
	ColocatedPlacementsHashKey *ca = (ColocatedPlacementsHashKey *) a;
	ColocatedPlacementsHashKey *cb = (ColocatedPlacementsHashKey *) b;

	if (ca->nodeId != cb->nodeId ||
		ca->colocationGroupId != cb->colocationGroupId ||
		ca->representativeValue != cb->representativeValue)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
