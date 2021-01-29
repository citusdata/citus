/*-------------------------------------------------------------------------
 *
 * locally_reserved_shared_connection_stats.h
 *   Management of connection reservations in shard memory pool
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCALLY_RESERVED_SHARED_CONNECTIONS_H_
#define LOCALLY_RESERVED_SHARED_CONNECTIONS_H_

#include "distributed/connection_management.h"


extern void InitializeLocallyReservedSharedConnections(void);
extern bool CanUseReservedConnection(const char *hostName, int nodePort,
									 Oid userId, Oid databaseOid);
extern void MarkReservedConnectionUsed(const char *hostName, int nodePort,
									   Oid userId, Oid databaseOid);
extern void DeallocateReservedConnections(void);
extern void EnsureConnectionPossibilityForRemotePrimaryNodes(void);
extern bool TryConnectionPossibilityForLocalPrimaryNode(void);
extern bool IsReservationPossible(void);

#endif /* LOCALLY_RESERVED_SHARED_CONNECTIONS_H_ */
