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

extern void InitializeReplicationOriginSessionUtils(void);

extern void SetupReplicationOriginRemoteSession(MultiConnection *connection);
extern void ResetReplicationOriginRemoteSession(MultiConnection *connection);

extern void SetupReplicationOriginLocalSession(void);
extern void ResetReplicationOriginLocalSession(void);
extern void ResetReplicationOriginLocalSessionCallbackHandler(void *arg);


extern bool EnableChangeDataCapture;


#endif /* REPLICATION_ORIGIN_SESSION_UTILS_H */
