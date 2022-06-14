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

typedef struct FullRelationName
{
	char *schemaName;
	char *relationName;
} FullRelationName;

typedef struct SplitCopyInfo
{
	FullRelationName *destinationShard;		/* destination shard name */
	int32 shardMinValue;         			/* min hash value of destination shard */
	int32 shardMaxValue;         			/* max hash value of destination shard */
	uint32_t nodeId; 						/* node where split child shard is to be placed */
} SplitCopyInfo;

extern DestReceiver* CreateSplitCopyDestReceiver(FullRelationName *sourceShard, List* splitCopyInfoList);

#endif /* WORKER_SPLIT_COPY_H_ */
