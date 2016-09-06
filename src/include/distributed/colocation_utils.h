/*-------------------------------------------------------------------------
 *
 * colocation_utils.h
 *
 * Declarations for public utility functions related to co-located tables.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLOCATION_UTILS_H_
#define COLOCATION_UTILS_H_

#include "distributed/shardinterval_utils.h"
#include "nodes/pg_list.h"

#define INVALID_COLOCATION_ID 0

extern uint64 TableColocationId(Oid distributedTableId);
extern bool TablesColocated(Oid leftDistributedTableId, Oid rightDistributedTableId);
extern bool ShardsColocated(ShardInterval *leftShardInterval,
							ShardInterval *rightShardInterval);
extern List * ColocatedTableList(Oid distributedTableId);
extern List * ColocatedShardPlacementList(ShardInterval *shardInterval);

#endif /* COLOCATION_UTILS_H_ */
