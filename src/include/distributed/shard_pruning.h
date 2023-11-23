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

#include "nodes/primnodes.h"

#include "distributed/metadata_cache.h"

#define INVALID_SHARD_INDEX -1

/* Function declarations for shard pruning */
extern List * PruneShards(Oid relationId, Index rangeTableId, List *whereClauseList,
						  Const **partitionValueConst);
extern bool ContainsFalseClause(List *whereClauseList);
extern List * get_all_actual_clauses(List *restrictinfo_list);
extern Const * TransformPartitionRestrictionValue(Var *partitionColumn,
												  Const *restrictionValue,
												  bool missingOk);
bool VarConstOpExprClause(OpExpr *opClause, Var **varClause, Const **constantClause);

#endif /* SHARD_PRUNING_H_ */
