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

extern MultiConnection * GetPlacementConnection(uint32 flags,
												struct ShardPlacement *placement,
												const char *userName);
extern MultiConnection * StartPlacementConnection(uint32 flags,
												  struct ShardPlacement *placement,
												  const char *userName);

extern void ResetPlacementConnectionManagement(void);
extern void MarkFailedShardPlacements(void);
extern void PostCommitMarkFailedShardPlacements(bool using2PC);

extern void CloseShardPlacementAssociation(struct MultiConnection *connection);
extern void ResetShardPlacementAssociation(struct MultiConnection *connection);

extern void InitPlacementConnectionManagement(void);

#endif /* PLACEMENT_CONNECTION_H */
