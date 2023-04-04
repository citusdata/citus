/*-------------------------------------------------------------------------
 *
 * shard_transfer.h
 *	  Code used to move shards around.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/shard_rebalancer.h"
#include "nodes/pg_list.h"

typedef enum
{
	SHARD_TRANSFER_INVALID_FIRST = 0,
	SHARD_TRANSFER_MOVE = 1,
	SHARD_TRANSFER_COPY = 2
} ShardTransferType;

extern void TransferShards(int64 shardId,
						   char *sourceNodeName, int32 sourceNodePort,
						   char *targetNodeName, int32 targetNodePort,
						   char shardReplicationMode, ShardTransferType transferType);
extern uint64 ShardListSizeInBytes(List *colocatedShardList,
								   char *workerNodeName, uint32 workerNodePort);
extern void ErrorIfMoveUnsupportedTableType(Oid relationId);
extern void CopyShardsToNode(WorkerNode *sourceNode, WorkerNode *targetNode,
							 List *shardIntervalList, char *snapshotName);
extern void VerifyTablesHaveReplicaIdentity(List *colocatedTableList);
extern bool RelationCanPublishAllModifications(Oid relationId);
extern void UpdatePlacementUpdateStatusForShardIntervalList(List *shardIntervalList,
															char *sourceName,
															int sourcePort,
															PlacementUpdateStatus status);
extern void InsertDeferredDropCleanupRecordsForShards(List *shardIntervalList);
extern void InsertCleanupRecordsForShardPlacementsOnNode(List *shardIntervalList,
														 int32 groupId);
