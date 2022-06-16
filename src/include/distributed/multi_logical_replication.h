/*-------------------------------------------------------------------------
 *
 * multi_logical_replication.h
 *
 *    Declarations for public functions and variables used in logical replication
 *    on the distributed tables while moving shards.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTI_LOGICAL_REPLICATION_H_
#define MULTI_LOGICAL_REPLICATION_H_


#include "nodes/pg_list.h"


/* Config variables managed via guc.c */
extern int LogicalReplicationTimeout;

extern bool PlacementMovedUsingLogicalReplicationInTX;


extern void LogicallyReplicateShards(List *shardList, char *sourceNodeName,
									 int sourceNodePort, char *targetNodeName,
									 int targetNodePort);

#define SHARD_MOVE_PUBLICATION_PREFIX "citus_shard_move_publication_"
#define SHARD_MOVE_SUBSCRIPTION_PREFIX "citus_shard_move_subscription_"
#define SHARD_MOVE_SUBSCRIPTION_ROLE_PREFIX "citus_shard_move_subscription_role_"

#endif /* MULTI_LOGICAL_REPLICATION_H_ */
