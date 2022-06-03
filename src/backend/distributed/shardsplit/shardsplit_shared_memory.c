/*-------------------------------------------------------------------------
 *
 * shardsplit_shared_memory.c
 *    API's for creating and accessing shared memory segments to store
 *    shard split information. 'setup_shard_replication' UDF creates the
 *    shared memory, populates the contents and WAL sender processes are
 *    the consumers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/citus_safe_lib.h"

/* Function declarations */
static ShardSplitInfoSMHeader * AllocateSharedMemoryForShardSplitInfo(int
																	  shardSplitInfoCount,
																	  Size
																	  shardSplitInfoSize,
																	  dsm_handle *
																	  dsmHandle);

static void * ShardSplitInfoSMData(ShardSplitInfoSMHeader *shardSplitInfoSMHeader);

static ShardSplitInfoSMHeader * GetShardSplitInfoSMHeaderFromDSMHandle(dsm_handle
																	   dsmHandle);

/*
 * GetShardSplitInfoSMHeaderFromDSMHandle returns the header of the shared memory
 * segment. It pins the mapping till lifetime of the backend process accessing it.
 */
static ShardSplitInfoSMHeader *
GetShardSplitInfoSMHeaderFromDSMHandle(dsm_handle dsmHandle)
{
	dsm_segment *dsmSegment = dsm_find_mapping(dsmHandle);

	if (dsmSegment == NULL)
	{
		dsmSegment = dsm_attach(dsmHandle);
	}

	if (dsmSegment == NULL)
	{
		ereport(ERROR,
				(errmsg("could not attach to dynamic shared memory segment "
						"corresponding to handle:%u", dsmHandle)));
	}

	/*
	 * Detatching segment associated with resource owner with 'dsm_pin_mapping' call before the
	 * resource owner releases, to avoid warning being logged and potential leaks.
	 */
	dsm_pin_mapping(dsmSegment);

	ShardSplitInfoSMHeader *header = (ShardSplitInfoSMHeader *) dsm_segment_address(
		dsmSegment);

	return header;
}


/*
 * GetShardSplitInfoSMArrayForSlot returns pointer to the array of
 * 'ShardSplitInfo' struct stored in the shared memory segment.
 */
ShardSplitInfo *
GetShardSplitInfoSMArrayForSlot(char *slotName, int *shardSplitInfoCount)
{
	if (slotName == NULL ||
		shardSplitInfoCount == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("Expected slot name and array size arguments")));
	}

	dsm_handle dsmHandle;
	uint32_t nodeId = 0;
	decode_replication_slot(slotName, &nodeId, &dsmHandle);

	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		GetShardSplitInfoSMHeaderFromDSMHandle(dsmHandle);
	*shardSplitInfoCount = shardSplitInfoSMHeader->shardSplitInfoCount;

	ShardSplitInfo *shardSplitInfoArray =
		(ShardSplitInfo *) ShardSplitInfoSMData(shardSplitInfoSMHeader);

	return shardSplitInfoArray;
}


/*
 * AllocateSharedMemoryForShardSplitInfo is used to allocate and store
 * information about the shard undergoing a split. The function allocates dynamic
 * shared memory segment consisting of a header and an array of ShardSplitInfo structure.
 * The contents of this shared memory segment are consumed by WAL sender process
 * during catch up phase of replication through logical decoding plugin.
 *
 * The shared memory segment exists till the catch up phase completes or the
 * postmaster shutsdown.
 */
static ShardSplitInfoSMHeader *
AllocateSharedMemoryForShardSplitInfo(int shardSplitInfoCount, Size shardSplitInfoSize,
									  dsm_handle *dsmHandle)
{
	if (shardSplitInfoCount <= 0 || shardSplitInfoSize <= 0)
	{
		ereport(ERROR,
				(errmsg("count and size of each step should be "
						"positive values")));
	}

	Size totalSize = sizeof(ShardSplitInfoSMHeader) + shardSplitInfoCount *
					 shardSplitInfoSize;
	dsm_segment *dsmSegment = dsm_create(totalSize, DSM_CREATE_NULL_IF_MAXSEGMENTS);

	if (dsmSegment == NULL)
	{
		ereport(ERROR,
				(errmsg("could not create a dynamic shared memory segment to "
						"keep shard split info")));
	}

	*dsmHandle = dsm_segment_handle(dsmSegment);

	/*
	 * Pin the segment till Postmaster shutsdown since we need this
	 * segment even after the session ends for replication catchup phase.
	 */
	dsm_pin_segment(dsmSegment);

	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		GetShardSplitInfoSMHeaderFromDSMHandle(*dsmHandle);

	shardSplitInfoSMHeader->shardSplitInfoCount = shardSplitInfoCount;

	return shardSplitInfoSMHeader;
}


/*
 * CreateSharedMemoryForShardSplitInfo is a wrapper function which creates shared memory
 * for storing shard split infomation. The function returns pointer to the first element
 * within this array.
 *
 * shardSplitInfoCount - number of 'ShardSplitInfo ' elements to be allocated
 * dsmHandle           - handle of the allocated shared memory segment
 */
ShardSplitInfo *
CreateSharedMemoryForShardSplitInfo(int shardSplitInfoCount, dsm_handle *dsmHandle)
{
	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		AllocateSharedMemoryForShardSplitInfo(shardSplitInfoCount,
											  sizeof(ShardSplitInfo),
											  dsmHandle);
	ShardSplitInfo *shardSplitInfoSMArray =
		(ShardSplitInfo *) ShardSplitInfoSMData(shardSplitInfoSMHeader);

	return shardSplitInfoSMArray;
}


/*
 * ShardSplitInfoSMData returns a pointer to the array of 'ShardSplitInfo'.
 * This is simply the data right after the header, so this function is trivial.
 * The main purpose of this function is to make the intent clear to readers
 * of the code.
 */
static void *
ShardSplitInfoSMData(ShardSplitInfoSMHeader *shardSplitInfoSMHeader)
{
	return shardSplitInfoSMHeader + 1;
}


/*
 * encode_replication_slot returns an encoded replication slot name
 * in the following format.
 * Slot Name = citus_split_nodeId_sharedMemoryHandle_tableOwnerOid
 * Max supported length of replication slot name is 64 bytes.
 */
char *
encode_replication_slot(uint32_t nodeId,
						dsm_handle dsmHandle,
						uint32_t tableOwnerId)
{
	StringInfo slotName = makeStringInfo();
	appendStringInfo(slotName, "citus_split_%u_%u_%u", nodeId, dsmHandle, tableOwnerId);
	return slotName->data;
}


/*
 * decode_replication_slot decodes the replication slot name
 * into node id, shared memory handle.
 */
void
decode_replication_slot(char *slotName,
						uint32_t *nodeId,
						dsm_handle *dsmHandle)
{
	int index = 0;
	char *strtokPosition = NULL;
	char *dupSlotName = pstrdup(slotName);
	char *slotNameString = strtok_r(dupSlotName, "_", &strtokPosition);
	while (slotNameString != NULL)
	{
		/* third part of the slot name is NodeId */
		if (index == 2)
		{
			*nodeId = strtoul(slotNameString, NULL, 10);
		}

		/* fourth part of the name is memory handle */
		else if (index == 3)
		{
			*dsmHandle = strtoul(slotNameString, NULL, 10);
		}

		slotNameString = strtok_r(NULL, "_", &strtokPosition);
		index++;

		/*Ignoring TableOwnerOid*/
	}

	/*
	 * Replication slot name is encoded as citus_split_nodeId_sharedMemoryHandle_tableOwnerOid.
	 * Hence the number of tokens would be strictly five considering "_" as delimiter.
	 */
	if (index != 5)
	{
		ereport(ERROR,
				(errmsg("Invalid Replication Slot name encoding: %s", slotName)));
	}
}
