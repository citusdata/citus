/*-------------------------------------------------------------------------
 *
 * shard_pruning.h
 *   Shard pruning infrastructure.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARD_PRUNING_H_
#define SHARD_PRUNING_H_

#include "distributed/metadata_cache.h"
#include "nodes/primnodes.h"

#define INVALID_SHARD_INDEX -1

/* Function declarations for shard pruning */
extern List * PruneShards(Oid relationId, Index rangeTableId, List *whereClauseList,
						  Const **partitionValueConst);
extern bool ContainsFalseClause(List *whereClauseList);
extern Const * TransformPartitionRestrictionValue(Var *partitionColumn,
												  Const *restrictionValue,
												  bool missingOk);
#endif /* SHARD_PRUNING_H_ */
