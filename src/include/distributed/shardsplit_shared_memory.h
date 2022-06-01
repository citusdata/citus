/*-------------------------------------------------------------------------
 *
 * shardsplit_shared_memory.h
 *    API's for creating and accessing shared memory segments to store
 *    shard split information. 'split_shard_replication_setup' UDF creates the
 *    shared memory and populates the contents. WAL sender processes are consumer
 *    of split information for appropriate tuple routing.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDSPLIT_SHARED_MEMORY_H
#define SHARDSPLIT_SHARED_MEMORY_H

#include "c.h"
#include "fmgr.h"
#include "distributed/shard_split.h"

/*
 * Header of the shared memory segment where shard split information is stored.
 */
typedef struct ShardSplitInfoSMHeader
{
	int shardSplitInfoCount;          /* number of elements in the shared memory */
} ShardSplitInfoSMHeader;


/* Functions for creating and accessing shared memory segments */
extern ShardSplitInfo * CreateSharedMemoryForShardSplitInfo(int shardSplitInfoCount,
															dsm_handle *dsmHandle);

extern ShardSplitInfo * GetShardSplitInfoSMArrayForSlot(char *slotName,
														int *shardSplitInfoCount);


/* Functions related to encoding-decoding for replication slot name */
char * encode_replication_slot(uint32_t nodeId,
							   dsm_handle dsmHandle,
							   uint32_t tableOwnerId);
void decode_replication_slot(char *slotName,
							 uint32_t *nodeId,
							 dsm_handle *dsmHandle);

#endif /* SHARDSPLIT_SHARED_MEMORY_H */
