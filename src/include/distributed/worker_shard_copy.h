/*-------------------------------------------------------------------------
 *
 * worker_shard_copy.c
 *	 Copy data to destination shard in a push approach.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_SHARD_COPY_H_
#define WORKER_SHARD_COPY_H_

struct FullRelationName;

extern DestReceiver * CreateShardCopyDestReceiver(EState *executorState,
												  char *destinationShardFullyQualifiedName,
												  uint32_t destinationNodeId);

#endif /* WORKER_SHARD_COPY_H_ */
