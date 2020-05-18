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
extern bool ConnectionUsedForAnyPlacements(MultiConnection *connection);

extern bool UseConnectionPerPlacement(void);

#endif /* PLACEMENT_CONNECTION_H */
