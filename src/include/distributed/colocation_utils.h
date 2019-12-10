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

extern uint32 TableColocationId(Oid distributedTableId);
extern bool TablesColocated(Oid leftDistributedTableId, Oid rightDistributedTableId);
extern bool ShardsColocated(ShardInterval *leftShardInterval,
							ShardInterval *rightShardInterval);
extern List * ColocatedTableList(Oid distributedTableId);
extern List * ColocatedShardIntervalList(ShardInterval *shardInterval);
extern Oid ColocatedTableId(Oid colocationId);
extern uint64 ColocatedShardIdInRelation(Oid relationId, int shardIndex);
uint32 ColocationId(int shardCount, int replicationFactor, Oid distributionColumnType,
					Oid distributionColumnCollation);
extern uint32 CreateColocationGroup(int shardCount, int replicationFactor,
									Oid distributionColumnType,
									Oid distributionColumnCollation);
extern uint32 GetNextColocationId(void);
extern void CheckReplicationModel(Oid sourceRelationId, Oid targetRelationId);
extern void CheckDistributionColumnType(Oid sourceRelationId, Oid targetRelationId);

extern void DeleteColocationGroupIfNoTablesBelong(uint32 colocationId);

#endif /* COLOCATION_UTILS_H_ */
