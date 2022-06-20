/*-------------------------------------------------------------------------
 *
 * worker_split_copy.h
 *
 * API for worker shard split copy.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_SPLIT_COPY_H_
#define WORKER_SPLIT_COPY_H_

typedef struct SplitCopyInfo
{
	uint64 destinationShardId;				/* destination shard id */
	int32 destinationShardMinHashValue;     /* min hash value of destination shard */
	int32 destinationShardMaxHashValue;     /* max hash value of destination shard */
	uint32_t destinationShardNodeId; 		/* node where split child shard is to be placed */
} SplitCopyInfo;

extern DestReceiver* CreateSplitCopyDestReceiver(EState *executorState, uint64 sourceShardIdToCopy, List* splitCopyInfoList);

#endif /* WORKER_SPLIT_COPY_H_ */
