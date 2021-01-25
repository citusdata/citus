/*-------------------------------------------------------------------------
 * placement_connection.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLACEMENT_CONNECTION_H
#define PLACEMENT_CONNECTION_H


#include "distributed/connection_management.h"
#include "distributed/placement_access.h"


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


extern MultiConnection * GetPlacementConnection(uint32 flags,
												struct ShardPlacement *placement,
												const char *userName);
extern MultiConnection * StartPlacementConnection(uint32 flags,
												  struct ShardPlacement *placement,
												  const char *userName);
extern MultiConnection *  GetConnectionIfPlacementAccessedInXact(int flags,
																 List *placementAccessList,
																 const char *userName);
extern MultiConnection * StartPlacementListConnection(uint32 flags,
													  List *placementAccessList,
													  const char *userName);
extern ShardPlacementAccess * CreatePlacementAccess(ShardPlacement *placement,
													ShardPlacementAccessType accessType);
extern void AssignPlacementListToConnection(List *placementAccessList,
											MultiConnection *connection);

extern void ResetPlacementConnectionManagement(void);
extern void MarkFailedShardPlacements(void);
extern void PostCommitMarkFailedShardPlacements(bool using2PC);

extern void CloseShardPlacementAssociation(struct MultiConnection *connection);
extern void ResetShardPlacementAssociation(struct MultiConnection *connection);

extern void InitPlacementConnectionManagement(void);

extern bool ConnectionModifiedPlacement(MultiConnection *connection);
extern bool UseConnectionPerPlacement(void);

#endif /* PLACEMENT_CONNECTION_H */
