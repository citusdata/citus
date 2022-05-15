/*-------------------------------------------------------------------------
 *
 * shardsplit_sharedmemory.h
 *    API's for creating and accessing shared memory segments to store
 *    shard split information. 'setup_shard_replication' UDF creates the
 *    shared memory, populates the contents and WAL sender processes are
 *    the consumers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDSPLIT_SHARED_MEMORY_H
#define SHARDSPLIT_SHARED_MEMORY_H

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "distributed/shard_split.h"

/*
 * Header of the shared memory segment where shard split information is stored.
 */
typedef struct ShardSplitInfoSMHeader
{
	uint64 processId;       /* process id creating the shared memory segment */
	int stepCount;          /* number of elements in the shared memory */
} ShardSplitInfoSMHeader;


/* Functions for creating and accessing shared memory segments */
extern ShardSplitInfoSMHeader * CreateShardSplitInfoSharedMemory(int stepCount,
																 Size stepSize,
																 dsm_handle *dsmHandle);

extern ShardSplitInfo * GetSharedMemoryForShardSplitInfo(int shardSplitInfoCount,
														 dsm_handle *dsmHandle);

extern ShardSplitInfo * GetShardSplitInfoSMArrayForSlot(char *slotName, int *arraySize);

extern dsm_handle GetSMHandleFromSlotName(char *slotName);

/*
 * ShardSplitInfoSMSteps returns a pointer to the array of shard split info
 * steps that are stored in shared memory.
 */
extern void * ShardSplitInfoSMSteps(ShardSplitInfoSMHeader *shardSplitInfoSMHeader);

extern ShardSplitInfoSMHeader * GetShardSplitInfoSMHeaderFromDSMHandle(dsm_handle
																	   dsmHandle,
																	   dsm_segment **
																	   attachedSegment);

/*
 * An UPADATE request for a partition key, is realized as 'DELETE' on
 * old shard and 'INSERT' on new shard. So a commit of UPDATE has to be
 * seggrated in two replication messages. WAL sender belonging to a
 * replication slot can send only one message and hence to handle UPDATE we
 * have to create one extra replication slot per node that handles the deletion
 * part of an UPDATE.
 *
 * SLOT_HANDING_INSERT_AND_DELETE - Responsible for handling INSERT and DELETE
 *                                 operations.
 * SLOT_HANDLING_DELETE_OF_UPDATE - Responsible for only handling DELETE on old shard
 *                                 for an UPDATE. Its a no-op for INSERT and DELETE
 *                                 operations.
 */
enum ReplicationSlotType
{
	SLOT_HANDLING_INSERT_AND_DELETE,
	SLOT_HANDLING_DELETE_OF_UPDATE
};

/* Functions related to encoding-decoding for replication slot name */
char * encode_replication_slot(uint64_t nodeId,
							   uint32 slotType,
							   dsm_handle dsmHandle);
void decode_replication_slot(char *slotName,
							 uint64_t *nodeId,
							 uint32_t *slotType,
							 dsm_handle *dsmHandle);

#endif /* SHARDSPLIT_SHARED_MEMORY_H */
