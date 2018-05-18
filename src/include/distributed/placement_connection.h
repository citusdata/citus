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

/* forward declare, to avoid dependency on ShardPlacement definition */
struct ShardPlacement;

/* represents the way in which a placement is accessed */
typedef enum ShardPlacementAccessType
{
	/* read from placement */
	PLACEMENT_ACCESS_SELECT,

	/* modify rows in placement */
	PLACEMENT_ACCESS_DML,

	/* modify placement schema */
	PLACEMENT_ACCESS_DDL
} ShardPlacementAccessType;

/* represents access to a placement */
typedef struct ShardPlacementAccess
{
	/* placement that is accessed */
	struct ShardPlacement *placement;

	/* the way in which the placement is accessed */
	ShardPlacementAccessType accessType;
} ShardPlacementAccess;

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
extern HTAB *ConnectionPlacementHash;


extern MultiConnection * GetPlacementConnection(uint32 flags,
												struct ShardPlacement *placement,
												const char *userName);
extern MultiConnection * StartPlacementConnection(uint32 flags,
												  struct ShardPlacement *placement,
												  const char *userName);

extern MultiConnection * GetPlacementListConnection(uint32 flags,
													List *placementAccessList,
													const char *userName);
extern MultiConnection * StartPlacementListConnection(uint32 flags,
													  List *placementAccessList,
													  const char *userName);

extern void ResetPlacementConnectionManagement(void);
extern void MarkFailedShardPlacements(void);
extern void PostCommitMarkFailedShardPlacements(bool using2PC);

extern void CloseShardPlacementAssociation(struct MultiConnection *connection);
extern void ResetShardPlacementAssociation(struct MultiConnection *connection);

extern void InitPlacementConnectionManagement(void);

#endif /* PLACEMENT_CONNECTION_H */
