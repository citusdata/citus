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

#include "access/hash.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/distributed_planner.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


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
 * that there can only one DML/DDL connection for a set of colocated
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
	int nodeId;

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
static bool CheckShardPlacements(ConnectionShardHashEntry *shardEntry);
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
 * GetPlacementListConnection establishes a connection for a set of placement
 * accesses.
 *
 * See StartPlacementListConnection for details.
 */
MultiConnection *
GetPlacementListConnection(uint32 flags, List *placementAccessList, const char *userName)
{
	MultiConnection *connection = StartPlacementListConnection(flags, placementAccessList,
															   userName);

	FinishConnectionEstablishment(connection);
	return connection;
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
	MultiConnection *chosenConnection = NULL;

	if (userName == NULL)
	{
		userName = freeUserName = CurrentUserName();
	}

	chosenConnection = FindPlacementListConnection(flags, placementAccessList, userName);
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

		if ((flags & CONNECTION_PER_PLACEMENT) &&
			ConnectionAccessedDifferentPlacement(chosenConnection, placement))
		{
			/*
			 * Cached connection accessed a non-co-located placement in the same
			 * table or co-location group, while the caller asked for a connection
			 * per placement. Open a new connection instead.
			 *
			 * We use this for situations in which we want to use a different
			 * connection for every placement, such as COPY. If we blindly returned
			 * a cached conection that already modified a different, non-co-located
			 * placement B in the same table or in a table with the same co-location
			 * ID as the current placement, then we'd no longer able to write to
			 * placement B later in the COPY.
			 */
			chosenConnection = StartNodeUserDatabaseConnection(flags |
															   FORCE_NEW_CONNECTION,
															   nodeName, nodePort,
															   userName, NULL);

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
	ListCell *placementAccessCell = NULL;
	char *userName = connection->user;

	foreach(placementAccessCell, placementAccessList)
	{
		ShardPlacementAccess *placementAccess =
			(ShardPlacementAccess *) lfirst(placementAccessCell);
		ShardPlacement *placement = placementAccess->placement;
		ShardPlacementAccessType accessType = placementAccess->accessType;

		ConnectionPlacementHashEntry *placementEntry = NULL;
		ConnectionReference *placementConnection = NULL;

		Oid relationId = InvalidOid;

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

		placementEntry = FindOrCreatePlacementEntry(placement);
		placementConnection = placementEntry->primaryConnection;

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
		relationId = RelationIdForShard(placement->shardId);
		RecordRelationAccessIfReferenceTable(relationId, accessType);
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
	MultiConnection *connection = NULL;
	char *freeUserName = NULL;

	if (userName == NULL)
	{
		userName = freeUserName = CurrentUserName();
	}

	connection = FindPlacementListConnection(flags, placementAccessList,
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
	ListCell *placementAccessCell = NULL;
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
	foreach(placementAccessCell, placementAccessList)
	{
		ShardPlacementAccess *placementAccess =
			(ShardPlacementAccess *) lfirst(placementAccessCell);
		ShardPlacement *placement = placementAccess->placement;
		ShardPlacementAccessType accessType = placementAccess->accessType;

		ConnectionPlacementHashEntry *placementEntry = NULL;
		ColocatedPlacementsHashEntry *colocatedEntry = NULL;
		ConnectionReference *placementConnection = NULL;

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

		placementEntry = FindOrCreatePlacementEntry(placement);
		colocatedEntry = placementEntry->colocatedEntry;
		placementConnection = placementEntry->primaryConnection;

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
		else if (placementConnection->hadDDL)
		{
			/*
			 * There is an existing connection, but we cannot use it and it executed
			 * DDL. Any subsequent operation needs to be able to see the results of
			 * the DDL command and thus cannot proceed if it cannot use the connection.
			 */

			Assert(placementConnection != NULL);
			Assert(!CanUseExistingConnection(flags, userName, placementConnection));

			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot establish a new connection for "
							"placement " UINT64_FORMAT
							", since DDL has been executed on a connection that is in use",
							placement->placementId)));
		}
		else if (placementConnection->hadDML)
		{
			/*
			 * There is an existing connection, but we cannot use it and it executed
			 * DML. Any subsequent operation needs to be able to see the results of
			 * the DML command and thus cannot proceed if it cannot use the connection.
			 *
			 * Note that this is not meaningfully different from the previous case. We
			 * just produce a different error message based on whether DDL was or only
			 * DML was executed.
			 */

			Assert(placementConnection != NULL);
			Assert(!CanUseExistingConnection(flags, userName, placementConnection));
			Assert(!placementConnection->hadDDL);

			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot establish a new connection for "
							"placement " UINT64_FORMAT
							", since DML has been executed on a connection that is in use",
							placement->placementId)));
		}
		else if (accessType == PLACEMENT_ACCESS_DDL)
		{
			/*
			 * There is an existing connection, but we cannot use it and we want to
			 * execute DDL. The operation on the existing connection might conflict
			 * with the DDL statement.
			 */

			Assert(placementConnection != NULL);
			Assert(!CanUseExistingConnection(flags, userName, placementConnection));
			Assert(!placementConnection->hadDDL);
			Assert(!placementConnection->hadDML);

			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot perform a parallel DDL command because multiple "
							"placements have been accessed over the same connection")));
		}
		else
		{
			/*
			 * The placement has a connection assigned to it, but it cannot be used,
			 * most likely because it has been claimed exclusively. Fortunately, it
			 * has only been used for reads and we're not performing a DDL command.
			 * We can therefore use a different connection for this placement.
			 */

			Assert(placementConnection != NULL);
			Assert(!CanUseExistingConnection(flags, userName, placementConnection));
			Assert(!placementConnection->hadDDL);
			Assert(!placementConnection->hadDML);
			Assert(accessType != PLACEMENT_ACCESS_DDL);
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
	ConnectionPlacementHashEntry *placementEntry = NULL;
	bool found = false;

	connKey.placementId = placement->placementId;

	placementEntry = hash_search(ConnectionPlacementHash, &connKey, HASH_ENTER, &found);
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
			ColocatedPlacementsHashEntry *colocatedEntry = NULL;

			coloKey.nodeId = placement->nodeId;
			coloKey.colocationGroupId = placement->colocationGroupId;
			coloKey.representativeValue = placement->representativeValue;

			/* look for a connection assigned to co-located placements */
			colocatedEntry = hash_search(ColocatedPlacementsHash, &coloKey, HASH_ENTER,
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

	if (connection->remoteTransaction.transactionState == REMOTE_TRANS_INVALID)
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
 * ConnectionUsedForAnyPlacements returns true if the connection
 * has not been associated with any placement.
 */
bool
ConnectionUsedForAnyPlacements(MultiConnection *connection)
{
	return !dlist_is_empty(&connection->referencedPlacements);
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
		 * primaryConnection here, that'd more complicated than it seems
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
	hash_delete_all(ColocatedPlacementsHash);
	ResetRelationAccessHash();

	/*
	 * NB: memory for ConnectionReference structs and subordinate data is
	 * deleted by virtue of being allocated in TopTransactionContext.
	 */
}


/*
 * MarkFailedShardPlacements looks through every connection in the connection shard hash
 * and marks the placements associated with failed connections invalid.
 *
 * Every shard must have at least one placement connection which did not fail. If all
 * modifying connections for a shard failed then the transaction will be aborted.
 *
 * This will be called just before commit, so we can abort before executing remote
 * commits. It should also be called after modification statements, to ensure that we
 * don't run future statements against placements which are not up to date.
 */
void
MarkFailedShardPlacements()
{
	HASH_SEQ_STATUS status;
	ConnectionShardHashEntry *shardEntry = NULL;

	hash_seq_init(&status, ConnectionShardHash);
	while ((shardEntry = (ConnectionShardHashEntry *) hash_seq_search(&status)) != 0)
	{
		if (!CheckShardPlacements(shardEntry))
		{
			ereport(ERROR,
					(errmsg("could not make changes to shard " INT64_FORMAT
							" on any node",
							shardEntry->key.shardId)));
		}
	}
}


/*
 * PostCommitMarkFailedShardPlacements marks placements invalid and checks whether
 * sufficiently many placements have failed to abort the entire coordinated
 * transaction.
 *
 * This will be called just after a coordinated commit so we can handle remote
 * transactions which failed during commit.
 *
 * When using2PC is set as least one placement must succeed per shard. If all placements
 * fail for a shard the entire transaction is aborted. If using2PC is not set then a only
 * a warning will be emitted; we cannot abort because some remote transactions might have
 * already been committed.
 */
void
PostCommitMarkFailedShardPlacements(bool using2PC)
{
	HASH_SEQ_STATUS status;
	ConnectionShardHashEntry *shardEntry = NULL;
	int successes = 0;
	int attempts = 0;

	int elevel = using2PC ? ERROR : WARNING;

	hash_seq_init(&status, ConnectionShardHash);
	while ((shardEntry = (ConnectionShardHashEntry *) hash_seq_search(&status)) != 0)
	{
		attempts++;
		if (CheckShardPlacements(shardEntry))
		{
			successes++;
		}
		else
		{
			/*
			 * Only error out if we're using 2PC. If we're not using 2PC we can't error
			 * out otherwise we can end up with a state where some shard modifications
			 * have already committed successfully.
			 */
			ereport(elevel,
					(errmsg("could not commit transaction for shard " INT64_FORMAT
							" on any active node",
							shardEntry->key.shardId)));
		}
	}

	/*
	 * If no shards could be modified at all, error out. Doesn't matter whether
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
CheckShardPlacements(ConnectionShardHashEntry *shardEntry)
{
	int failures = 0;
	int successes = 0;
	dlist_iter placementIter;

	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);
		ConnectionReference *primaryConnection = placementEntry->primaryConnection;
		MultiConnection *connection = NULL;

		/* we only consider shards that are modified */
		if (primaryConnection == NULL ||
			!(primaryConnection->hadDDL || primaryConnection->hadDML))
		{
			continue;
		}

		connection = primaryConnection->connection;

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
		return false;
	}

	/* mark all failed placements invalid */
	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);

		if (placementEntry->failed)
		{
			uint64 shardId = shardEntry->key.shardId;
			uint64 placementId = placementEntry->key.placementId;
			GroupShardPlacement *shardPlacement =
				LoadGroupShardPlacement(shardId, placementId);

			/*
			 * We only set shard state if its current state is FILE_FINALIZED, which
			 * prevents overwriting shard state if it is already set at somewhere else.
			 */
			if (shardPlacement->shardState == FILE_FINALIZED)
			{
				UpdateShardPlacementState(placementEntry->key.placementId, FILE_INACTIVE);
			}
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

	/* (relationId) = [relationAccessMode] hash */
	AllocateRelationAccessHash();
}


static uint32
ColocatedPlacementsHashHash(const void *key, Size keysize)
{
	ColocatedPlacementsHashKey *entry = (ColocatedPlacementsHashKey *) key;
	uint32 hash = 0;

	hash = hash_uint32(entry->nodeId);
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


/*
 * AnyConnectionAccessedPlacements simply checks the number of entries in
 * ConnectionPlacementHash. This is useful to detect whether we're in a
 * distirbuted transaction and already executed at least one command that
 * accessed to a placement.
 */
bool
AnyConnectionAccessedPlacements(void)
{
	/* this is initialized on PG_INIT */
	Assert(ConnectionPlacementHash != NULL);

	return hash_get_num_entries(ConnectionPlacementHash) > 0;
}
