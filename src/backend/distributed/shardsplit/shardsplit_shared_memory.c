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
#include "storage/ipc.h"
#include "utils/memutils.h"

const char *sharedMemoryNameForHandleManagement =
	"SHARED_MEMORY_FOR_SPLIT_SHARD_HANDLE_MANAGEMENT";

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static
void ShardSplitShmemInit(void);

/* Function declarations */
static ShardSplitInfoSMHeader * AllocateSharedMemoryForShardSplitInfo(int
																	  shardSplitInfoCount,
																	  Size
																	  shardSplitInfoSize,
																	  dsm_handle *
																	  dsmHandle);

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
 * GetShardSplitInfoSMHeader returns pointer to the header of shared memory segment.
 */
ShardSplitInfoSMHeader *
GetShardSplitInfoSMHeader(char *slotName)
{
	if (slotName == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("Expected slot name but found NULL")));
	}

	dsm_handle dsmHandle = GetSharedMemoryHandle();

	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		GetShardSplitInfoSMHeaderFromDSMHandle(dsmHandle);

	return shardSplitInfoSMHeader;
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

	Size totalSize = offsetof(ShardSplitInfoSMHeader, splitInfoArray) +
					 shardSplitInfoCount *
					 shardSplitInfoSize;
	dsm_segment *dsmSegment = dsm_create(totalSize, DSM_CREATE_NULL_IF_MAXSEGMENTS);

	if (dsmSegment == NULL)
	{
		ereport(ERROR,
				(errmsg("could not create a dynamic shared memory segment to "
						"store shard split info")));
	}

	*dsmHandle = dsm_segment_handle(dsmSegment);

	/*
	 * Pin the segment till Postmaster shutsdown since we need this
	 * segment even after the session ends for replication catchup phase.
	 */
	dsm_pin_segment(dsmSegment);

	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		GetShardSplitInfoSMHeaderFromDSMHandle(*dsmHandle);

	shardSplitInfoSMHeader->count = shardSplitInfoCount;

	return shardSplitInfoSMHeader;
}


/*
 * CreateSharedMemoryForShardSplitInfo is a wrapper function which creates shared memory
 * for storing shard split infomation. The function returns pointer to the header of
 * shared memory segment.
 *
 * shardSplitInfoCount - number of 'ShardSplitInfo ' elements to be allocated
 * dsmHandle           - handle of the allocated shared memory segment
 */
ShardSplitInfoSMHeader *
CreateSharedMemoryForShardSplitInfo(int shardSplitInfoCount, dsm_handle *dsmHandle)
{
	ShardSplitInfoSMHeader *shardSplitInfoSMHeader =
		AllocateSharedMemoryForShardSplitInfo(shardSplitInfoCount,
											  sizeof(ShardSplitInfo),
											  dsmHandle);
	return shardSplitInfoSMHeader;
}


/*
 * encode_replication_slot returns an encoded replication slot name
 * in the following format.
 * Slot Name = citus_split_nodeId_tableOwnerOid
 * Max supported length of replication slot name is 64 bytes.
 */
char *
encode_replication_slot(uint32_t nodeId,
						dsm_handle dsmHandle,
						uint32_t tableOwnerId)
{
	StringInfo slotName = makeStringInfo();
	appendStringInfo(slotName, "citus_split_%u_%u", nodeId, tableOwnerId);

	if (slotName->len > NAMEDATALEN)
	{
		ereport(ERROR,
				(errmsg(
					 "Replication Slot name:%s having length:%d is greater than maximum allowed length:%d",
					 slotName->data, slotName->len, NAMEDATALEN)));
	}

	return slotName->data;
}


/*
 * InitializeShardSplitSMHandleManagement requests the necessary shared memory
 * from Postgres and sets up the shared memory startup hook.
 * This memory is used to store handle of other shared memories allocated during split workflow.
 */
void
InitializeShardSplitSMHandleManagement(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ShardSplitShmemInit;
}


static void
ShardSplitShmemInit(void)
{
	bool alreadyInitialized = false;
	ShardSplitShmemData *smData = ShmemInitStruct(sharedMemoryNameForHandleManagement,
												  sizeof(ShardSplitShmemData),
												  &alreadyInitialized);

	if (!alreadyInitialized)
	{
		char *trancheName = "Split_Shard_Setup_Tranche";

		NamedLWLockTranche *namedLockTranche =
			&smData->namedLockTranche;

		/* start by zeroing out all the memory */
		memset(smData, 0,
			   sizeof(ShardSplitShmemData));

		namedLockTranche->trancheId = LWLockNewTrancheId();

		LWLockRegisterTranche(namedLockTranche->trancheId, trancheName);
		LWLockInitialize(&smData->lock,
						 namedLockTranche->trancheId);

		smData->dsmHandle = DSM_HANDLE_INVALID;
	}

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * StoreSharedMemoryHandle stores a handle of shared memory
 * allocated and populated by 'worker_split_shard_replication_setup' UDF.
 */
void
StoreSharedMemoryHandle(dsm_handle dsmHandle)
{
	bool found = false;
	ShardSplitShmemData *smData = ShmemInitStruct(sharedMemoryNameForHandleManagement,
												  sizeof(ShardSplitShmemData),
												  &found);
	if (!found)
	{
		ereport(ERROR,
				errmsg(
					"Shared memory for handle management should have been initialized during boot"));
	}

	LWLockAcquire(&smData->lock, LW_EXCLUSIVE);

	/*
	 * In a normal situation, previously stored handle should have been invalidated
	 * before the current function is called.
	 * If this handle is still valid, it means cleanup of previous split shard
	 * workflow failed. Log a waring and continue the current shard split operation.
	 */
	if (smData->dsmHandle != DSM_HANDLE_INVALID)
	{
		ereport(WARNING,
				errmsg(
					"As a part of split shard workflow,unexpectedly found a valid"
					" shared memory handle while storing a new one."));
	}

	/* Store the incoming handle */
	smData->dsmHandle = dsmHandle;

	LWLockRelease(&smData->lock);
}


/*
 * GetSharedMemoryHandle returns the shared memory handle stored
 * by 'worker_split_shard_replication_setup' UDF. This handle
 * is requested by wal sender processes during logical replication phase.
 */
dsm_handle
GetSharedMemoryHandle(void)
{
	bool found = false;
	ShardSplitShmemData *smData = ShmemInitStruct(sharedMemoryNameForHandleManagement,
												  sizeof(ShardSplitShmemData),
												  &found);
	if (!found)
	{
		ereport(ERROR,
				errmsg(
					"Shared memory for handle management should have been initialized during boot"));
	}

	LWLockAcquire(&smData->lock, LW_SHARED);
	dsm_handle dsmHandle = smData->dsmHandle;
	LWLockRelease(&smData->lock);

	return dsmHandle;
}


/*
 * PopulateShardSplitInfoForReplicationSlot function traverses 'ShardSplitInfo' array
 * stored within shared memory segment. It returns the starting and ending index position
 * of a given slot within this array. When the given replication slot processes a commit,
 * traversal is only limited within this bound thus enhancing performance.
 */
ShardSplitInfoForReplicationSlot *
PopulateShardSplitInfoForReplicationSlot(char *slotName)
{
	ShardSplitInfoSMHeader *smHeader = GetShardSplitInfoSMHeader(slotName);

	MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

	ShardSplitInfoForReplicationSlot *infoForReplicationSlot =
		(ShardSplitInfoForReplicationSlot *) palloc(
			sizeof(ShardSplitInfoForReplicationSlot));
	infoForReplicationSlot->shardSplitInfoHeader = smHeader;
	infoForReplicationSlot->startIndex = -1;
	infoForReplicationSlot->endIndex = -1;

	int index = 0;
	while (index < smHeader->count)
	{
		if (strcmp(smHeader->splitInfoArray[index].slotName, slotName) == 0)
		{
			/* Found the starting index from where current slot information begins */
			infoForReplicationSlot->startIndex = index;

			/* Slide forward to get the end index */
			index++;
			while (index < smHeader->count && strcmp(
					   smHeader->splitInfoArray[index].slotName, slotName) == 0)
			{
				index++;
			}

			infoForReplicationSlot->endIndex = index - 1;

			/*
			 * 'ShardSplitInfo' with same slot name are stored contiguously in shared memory segment.
			 * After the current 'index' position, we should not encounter any 'ShardSplitInfo' with incoming slot name.
			 * If this happens, there is shared memory corruption. Its worth to go ahead and assert for this assumption.
			 * TODO: Traverse further and assert
			 */
		}

		index++;
	}

	if (infoForReplicationSlot->startIndex == -1)
	{
		ereport(ERROR,
				(errmsg("Unexpectedly could not find information "
						"corresponding to replication slot name:%s in shared memory.",
						slotName)));
	}

	MemoryContextSwitchTo(oldContext);

	return infoForReplicationSlot;
}
