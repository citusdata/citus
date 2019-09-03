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


extern MultiConnection * GetPlacementConnection(uint32 flags,
												struct ShardPlacement *placement,
												const char *userName);
extern MultiConnection * StartPlacementConnection(uint32 flags,
												  struct ShardPlacement *placement,
												  const char *userName);
extern MultiConnection *  GetConnectionIfPlacementAccessedInXact(int flags,
																 List *placementAccessList,
																 const char *userName);
extern MultiConnection * GetPlacementListConnection(uint32 flags,
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
extern bool AnyConnectionAccessedPlacements(void);

extern bool ConnectionModifiedPlacement(MultiConnection *connection);
extern bool ConnectionUsedForAnyPlacements(MultiConnection *connection);

#endif /* PLACEMENT_CONNECTION_H */
