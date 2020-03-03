/*-------------------------------------------------------------------------
 *
 * worker_shard_visibility.h
 *   Hide shard names on MX worker nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_SHARD_VISIBILITY_H
#define WORKER_SHARD_VISIBILITY_H

#include "nodes/nodes.h"

extern bool OverrideTableVisibility;


extern void ReplaceTableVisibleFunction(Node *inputNode);
extern bool RelationIsAKnownShard(Oid shardRelationOid, bool onlySearchPath);
extern Oid GetOwnerRelationOid(Oid shardRelationId, bool onlySearchPath);


#endif /* WORKER_SHARD_VISIBILITY_H */
