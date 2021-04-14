/*-------------------------------------------------------------------------
 *
 * shard_utils.h
 *   Utilities related to shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARD_UTILS_H
#define SHARD_UTILS_H

#include "postgres.h"

extern Oid GetTableLocalShardOid(Oid citusTableOid, uint64 shardId);
extern char * GetLongestShardName(Oid citusTableOid, char *finalRelationName);
extern char * GetLongestShardNameForLocalPartition(Oid parentTableOid,
												   char *partitionRelationName);

#endif /* SHARD_UTILS_H */
