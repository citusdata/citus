/*-------------------------------------------------------------------------
 *
 * shardsplit_shared_memory.h
 *    API's for creating and accessing shared memory segments to store
 *    shard split information. 'worker_split_shard_replication_setup' UDF creates the
 *    shared memory and populates the contents. WAL sender processes are consumer
 *    of split information for appropriate tuple routing.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDSPLIT_SHARED_MEMORY_H
#define SHARDSPLIT_SHARED_MEMORY_H

#include "distributed/shard_split.h"

/*
 * Header of the shared memory segment where shard split information is stored.
 */
typedef struct ShardSplitInfoSMHeader
{
	int count;          /* number of elements in the shared memory */
	ShardSplitInfo splitInfoArray[FLEXIBLE_ARRAY_MEMBER];
} ShardSplitInfoSMHeader;

/*
 * Shard split information is populated and stored in shared memory in the form of one dimensional
 * array by 'worker_split_shard_replication_setup'. Information belonging to same replication
 * slot is grouped together and stored contiguously within this array.
 * 'SourceToDestinationShardMap' maps list of child(destination) shards that should be processed by a replication
 * slot corresponding to a parent(source) shard. When a parent shard receives a change, the decoder can use this map
 * to traverse only the list of child shards corresponding the given parent.
 */
typedef struct SourceToDestinationShardMapEntry
{
	Oid sourceShardKey;
	List *shardSplitInfoList;
} SourceToDestinationShardMapEntry;

typedef struct ShardSplitShmemData
{
	int trancheId;
	NamedLWLockTranche namedLockTranche;
	LWLock lock;

	dsm_handle dsmHandle;
} ShardSplitShmemData;

/* Functions for creating and accessing shared memory used for dsm handle managment */
void InitializeShardSplitSMHandleManagement(void);

void StoreShardSplitSharedMemoryHandle(dsm_handle dsmHandle);
dsm_handle GetShardSplitSharedMemoryHandle(void);

/* Functions for creating and accessing shared memory segments consisting shard split information */
extern ShardSplitInfoSMHeader * CreateSharedMemoryForShardSplitInfo(int
																	shardSplitInfoCount,
																	dsm_handle *dsmHandle);
extern void ReleaseSharedMemoryOfShardSplitInfo(void);

extern ShardSplitInfoSMHeader *  GetShardSplitInfoSMHeader(void);

extern HTAB * PopulateSourceToDestinationShardMapForSlot(char *slotName);

char * encode_replication_slot(uint32_t nodeId, uint32_t tableOwnerId);
#endif /* SHARDSPLIT_SHARED_MEMORY_H */
