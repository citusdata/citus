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
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "distributed/colocation_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/citus_safe_lib.h"

/*
 * GetShardSplitInfoSMHeaderFromDSMHandle returns the header of the shared memory
 * segment beloing to 'dsmHandle'. It pins the shared memory segment mapping till
 * lifetime of the backend process accessing it.
 */
ShardSplitInfoSMHeader *
GetShardSplitInfoSMHeaderFromDSMHandle(dsm_handle dsmHandle,
									   dsm_segment **attachedSegment)
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

	/* Remain attached until end of backend or DetachSession(). */
	dsm_pin_mapping(dsmSegment);

	ShardSplitInfoSMHeader *header = (ShardSplitInfoSMHeader *) dsm_segment_address(
		dsmSegment);

	*attachedSegment = dsmSegment;

	return header;
}


/*
 * GetShardSplitInfoSMArrayForSlot returns pointer to the array of
 * 'ShardSplitInfo' struct stored in the shared memory segment.
 */
ShardSplitInfo *
GetShardSplitInfoSMArrayForSlot(char *slotName, int *arraySize)
{
	if (slotName == NULL ||
		arraySize == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("Expected slot name and array size arguments")));
	}

	dsm_handle dsmHandle = GetSMHandleFromSlotName(slotName);
	dsm_segment *dsmSegment = NULL;
	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		GetShardSplitInfoSMHeaderFromDSMHandle(dsmHandle,
											   &dsmSegment);
	*arraySize = shardSplitInfoSMHeader->stepCount;

	ShardSplitInfo *shardSplitInfoArray =
		(ShardSplitInfo *) ShardSplitInfoSMSteps(shardSplitInfoSMHeader);

	return shardSplitInfoArray;
}


/*
 * GetSMHandleFromSlotName function returns the shared memory handle
 * from the replication slot name. Replication slot name is encoded as
 * "NODEID_SlotType_SharedMemoryHANDLE".
 */
dsm_handle
GetSMHandleFromSlotName(char *slotName)
{
	if (slotName == NULL)
	{
		ereport(ERROR,
				errmsg("Invalid NULL replication slot name."));
	}

	uint64_t nodeId = 0;
	uint32_t type = 0;
	dsm_handle handle = 0;
	decode_replication_slot(slotName, &nodeId, &type, &handle);

	return handle;
}


/*
 * CreateShardSplitInfoSharedMemory is used to create a place to store
 * information about the shard undergoing a split. The function creates dynamic
 * shared memory segment consisting of a header regarding the processId and an
 * array of "steps" which store ShardSplitInfo. The contents of this shared
 * memory segment are consumed by WAL sender process during catch up phase of
 * replication through logical decoding plugin.
 *
 * The shared memory segment exists till the catch up phase completes or the
 * postmaster shutsdown.
 */
ShardSplitInfoSMHeader *
CreateShardSplitInfoSharedMemory(int stepCount, Size stepSize, dsm_handle *dsmHandle)
{
	if (stepSize <= 0 || stepCount <= 0)
	{
		ereport(ERROR,
				(errmsg("number of steps and size of each step should be "
						"positive values")));
	}

	Size totalSize = sizeof(ShardSplitInfoSMHeader) + stepSize * stepCount;
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
		GetShardSplitInfoSMHeaderFromDSMHandle(*dsmHandle, &dsmSegment);

	shardSplitInfoSMHeader->stepCount = stepCount;
	shardSplitInfoSMHeader->processId = MyProcPid;

	return shardSplitInfoSMHeader;
}


/*
 * GetSharedMemoryForShardSplitInfo is a wrapper function which creates shared memory
 * for storing shard split infomation. The function returns pointer the first element
 * within this array.
 *
 * shardSplitInfoCount - number of 'ShardSplitInfo ' elements to be allocated
 * dsmHandle           - handle of the allocated shared memory segment
 */
ShardSplitInfo *
GetSharedMemoryForShardSplitInfo(int shardSplitInfoCount, dsm_handle *dsmHandle)
{
	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		CreateShardSplitInfoSharedMemory(shardSplitInfoCount,
										 sizeof(ShardSplitInfo),
										 dsmHandle);
	ShardSplitInfo *shardSplitInfoSMArray =
		(ShardSplitInfo *) ShardSplitInfoSMSteps(shardSplitInfoSMHeader);

	return shardSplitInfoSMArray;
}


/*
 * ShardSplitInfoSMSteps returns a pointer to the array of 'ShardSplitInfo'
 * steps that are stored in shared memory segment. This is simply the data
 * right after the header, so this function is trivial. The main purpose of
 * this function is to make the intent clear to readers of the code.
 */
void *
ShardSplitInfoSMSteps(ShardSplitInfoSMHeader *shardSplitInfoSMHeader)
{
	return shardSplitInfoSMHeader + 1;
}


/*
 * encode_replication_slot returns an encoded replication slot name
 * in the following format.
 * Slot Name = NodeId_ReplicationSlotType_SharedMemoryHandle
 */
char *
encode_replication_slot(uint64_t nodeId,
						uint32 slotType,
						dsm_handle dsmHandle)
{
	StringInfo slotName = makeStringInfo();
	appendStringInfo(slotName, "%ld_%u_%u", nodeId, slotType, dsmHandle);
	return slotName->data;
}


/*
 * decode_replication_slot decodes the replication slot name
 * into node id, slotType, shared memory handle.
 */
void
decode_replication_slot(char *slotName,
						uint64_t *nodeId,
						uint32_t *slotType,
						dsm_handle *dsmHandle)
{
	if (slotName == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid null replication slot name.")));
	}

	int index = 0;
	char *strtokPosition = NULL;
	char *dupSlotName = pstrdup(slotName);
	char *slotNameString = strtok_r(dupSlotName, "_", &strtokPosition);
	while (slotNameString != NULL)
	{
		if (index == 0)
		{
			*nodeId = SafeStringToUint64(slotNameString);
		}
		else if (index == 1)
		{
			*slotType = strtoul(slotNameString, NULL, 10);
		}
		else if (index == 2)
		{
			*dsmHandle = strtoul(slotNameString, NULL, 10);
		}
		slotNameString = strtok_r(NULL, "_", &strtokPosition);
		index++;
	}
}
