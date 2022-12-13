/*-------------------------------------------------------------------------
 *
 * replication_origin_utils.h
 *   Utilities related to replication origin.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REPLICATION_ORIGIN_SESSION_UTILS_H
#define REPLICATION_ORIGIN_SESSION_UTILS_H

#include "postgres.h"
#include "replication/origin.h"
#include "distributed/connection_management.h"

void SetupReplicationOriginRemoteSession(MultiConnection *connection, char *identifier);
void ResetReplicationOriginRemoteSession(MultiConnection *connection, char *identifier);

void SetupReplicationOriginLocalSession(void);
void ResetReplicationOriginLocalSession(void);


#endif /* REPLICATION_ORIGIN_SESSION_UTILS_H */
