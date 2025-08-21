/*-------------------------------------------------------------------------
 *
 * shard_transfer.h
 *	  Code used to move shards around.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/pg_list.h"

#include "distributed/shard_rebalancer.h"

extern Datum citus_move_shard_placement(PG_FUNCTION_ARGS);
extern Datum citus_move_shard_placement_with_nodeid(PG_FUNCTION_ARGS);

typedef enum
{
	SHARD_TRANSFER_INVALID_FIRST = 0,
	SHARD_TRANSFER_MOVE = 1,
	SHARD_TRANSFER_COPY = 2
} ShardTransferType;

/*
 * ShardTransferOperationMode is used to pass flags to the shard transfer
 * function. The flags are used to control the behavior of the transfer
 * function.
 * Currently, optionFlags are only used to customize reference table transfers.
 * For distributed tables, optionFlags should always be set to 0.
 */
typedef enum
{
	/*
	 * This flag instructs the transfer function to only transfer single shard
	 * rather than transfer all the colocated shards for the shard interval.
	 * Using this flag mean we might break the colocated shard
	 * relationship on the source node. So this is only usefull when setting up
	 * the new node and we are sure that the node would not be used until we have
	 * transfered all the shards.
	 * The reason we need this flag is that we want to be able to transfer
	 * colocated shards in parallel and for now it is only used for the reference
	 * table shards.
	 * Finally if you are using this flag, you should also use consider defering
	 * the creation of the relationships on the source node until all colocated
	 * shards are transfered (see: SHARD_TRANSFER_SKIP_CREATE_RELATIONSHIPS).
	 */
	SHARD_TRANSFER_SINGLE_SHARD_ONLY = 1 << 0,

	/* With this flag the shard transfer function does not create any constrainsts
	 * or foreign relations defined on the shard, This can be used to defer the
	 * creation of the relationships until all the shards are transfered.
	 * This is usefull when we are transfering colocated shards in parallel and
	 * we want to avoid the creation of the relationships on the source node
	 * until all the shards are transfered.
	 */
	SHARD_TRANSFER_SKIP_CREATE_RELATIONSHIPS = 1 << 1,

	/* This flag is used to indicate that the shard transfer function should
	 * only create the relationships on the target node and not transfer any data.
	 * This is can be used to create the relationships that were defered
	 * during the transfering of shards.
	 */
	SHARD_TRANSFER_CREATE_RELATIONSHIPS_ONLY = 1 << 2
} ShardTransferOperationMode;


extern void TransferShards(int64 shardId,
						   char *sourceNodeName, int32 sourceNodePort,
						   char *targetNodeName, int32 targetNodePort,
						   char shardReplicationMode, ShardTransferType transferType,
						   uint32 optionFlags);
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

extern void AdjustShardsForPrimaryCloneNodeSplit(WorkerNode *primaryNode,
												 WorkerNode *cloneNode,
												 List *primaryShardList,
												 List *cloneShardList);
