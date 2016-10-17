/*-------------------------------------------------------------------------
 * placement_connection.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLACEMENT_CONNECTION_H
#define PLACEMENT_CONNECTION_H


#include "distributed/connection_management.h"
#include "utils/hsearch.h"


/* forward declare, to avoid dependency on ShardPlacement definition */
struct ShardPlacement;

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
	uint32 placementid;
} ConnectionPlacementHashKey;

/* information about a connection reference to a table */
typedef struct ConnectionReference
{
	MultiConnection *connection;
	bool hadDML;
	bool hadDDL;
} ConnectionReference;

/* hash entry */
typedef struct ConnectionPlacementHashEntry
{
	ConnectionPlacementHashKey key;

	bool failed;

	List *connectionReferences;
} ConnectionPlacementHashEntry;

/* hash table */
extern HTAB *ConnectionPlacementHash;


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
	ConnectionShardHashKey *key;
	List *placementConnections;
} ConnectionShardHashEntry;

/* hash table itself */
extern HTAB *ConnectionShardHash;


/* Higher level connection handling API. */
extern MultiConnection * GetPlacementConnection(uint32 flags,
												struct ShardPlacement *placement);
extern MultiConnection * StartPlacementConnection(uint32 flags,
												  struct ShardPlacement *placement);

extern void CheckForFailedPlacements(bool preCommit, bool using2PC);

extern void InitPlacementConnectionManagement(void);
extern void ResetPlacementConnectionManagement(void);

#endif /* PLACEMENT_CONNECTION_H */
