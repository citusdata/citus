/*
 * multi_partitioning_utils.h
 *	  Utility functions declarations for declarative partitioning
 *
 * Copyright (c) Citus Data, Inc.
 */
#ifndef MULTI_PARTITIONING_UTILS_H_
#define MULTI_PARTITIONING_UTILS_H_


#include "nodes/pg_list.h"

#include "distributed/metadata_utility.h"


extern bool PartitionedTable(Oid relationId);
extern bool PartitionedTableNoLock(Oid relationId);
extern bool PartitionTable(Oid relationId);
extern bool PartitionTableNoLock(Oid relationId);
extern bool IsChildTable(Oid relationId);
extern bool IsParentTable(Oid relationId);
extern Oid PartitionParentOid(Oid partitionOid);
extern Oid PartitionWithLongestNameRelationId(Oid parentRelationId);
extern List * PartitionList(Oid parentRelationId);
extern char * GenerateDetachPartitionCommand(Oid partitionTableId);
extern List * GenerateDetachPartitionCommandRelationIdList(List *relationIds);
extern char * GenerateAttachShardPartitionCommand(ShardInterval *shardInterval);
extern char * GenerateAlterTableAttachPartitionCommand(Oid partitionTableId);
extern List * GenerateAttachPartitionCommandRelationIdList(List *relationIds);
extern char * GeneratePartitioningInformation(Oid tableId);
extern void FixPartitionConstraintsOnWorkers(Oid relationId);
extern void FixLocalPartitionConstraints(Oid relationId, int64 shardId);
extern void FixPartitionShardIndexNames(Oid relationId, Oid parentIndexOid);
extern List * ListShardsUnderParentRelation(Oid relationId);

#endif /* MULTI_PARTITIONING_UTILS_H_ */
