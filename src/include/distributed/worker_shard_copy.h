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

/* GUC, determining whether Binary Copy is enabled */
extern bool EnableBinaryProtocol;

extern DestReceiver * CreateShardCopyDestReceiver(EState *executorState,
												  List *destinationShardFullyQualifiedName,
												  uint32_t destinationNodeId);

#endif /* WORKER_SHARD_COPY_H_ */
