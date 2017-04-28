/*-------------------------------------------------------------------------
 *
 * shard_pruning.h
 *   Shard pruning infrastructure.
 *
 * Copyright (c) 2014-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARD_PRUNING_H_
#define SHARD_PRUNING_H_

#include "distributed/metadata_cache.h"
#include "nodes/primnodes.h"

#define INVALID_SHARD_INDEX -1

/* Function declarations for shard pruning */
extern List * PruneShards(Oid relationId, Index rangeTableId, List *whereClauseList);
extern bool ContainsFalseClause(List *whereClauseList);

#endif /* SHARD_PRUNING_H_ */
