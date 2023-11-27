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

#include "common/hashfn.h"
#include "storage/ipc.h"
#include "utils/memutils.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_shared_memory.h"

const char *SharedMemoryNameForHandleManagement =
	"Shared memory handle for shard split";

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Function declarations */
static ShardSplitInfoSMHeader * AllocateSharedMemoryForShardSplitInfo(int
																	  shardSplitInfoCount,
																	  Size
																	  shardSplitInfoSize,
																	  dsm_handle *
																	  dsmHandle);
static ShardSplitInfoSMHeader * GetShardSplitInfoSMHeaderFromDSMHandle(dsm_handle
																	   dsmHandle);
static dsm_handle GetShardSplitSharedMemoryHandle(void);
static void ShardSplitShmemInit(void);

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

	if (header == NULL)
	{
		ereport(ERROR,
				(errmsg("Could not get shared memory segment header "
						"corresponding to handle for split workflow:%u", dsmHandle)));
	}

	return header;
}


/*
 * GetShardSplitInfoSMHeader returns pointer to the header of shared memory segment.
 */
ShardSplitInfoSMHeader *
GetShardSplitInfoSMHeader()
{
	dsm_handle dsmHandle = GetShardSplitSharedMemoryHandle();

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
				(errmsg("shardSplitInfoCount and size of each step should be "
						"positive values")));
	}

	Size totalSize = offsetof(ShardSplitInfoSMHeader, splitInfoArray) +
					 (shardSplitInfoCount * shardSplitInfoSize);
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
 * ReleaseSharedMemoryOfShardSplitInfo releases(unpins) the dynamic shared memory segment
 * allocated by 'worker_split_shard_replication_setup'. This shared memory was pinned
 * to Postmaster process and is valid till Postmaster shutsdown or
 * explicitly unpinned by calling 'dsm_unpin_segment'.
 */
void
ReleaseSharedMemoryOfShardSplitInfo()
{
	/* Get handle of dynamic shared memory segment*/
	dsm_handle dsmHandle = GetShardSplitSharedMemoryHandle();

	if (dsmHandle == DSM_HANDLE_INVALID)
	{
		return;
	}

	/*
	 * Unpin the dynamic shared memory segment. 'dsm_pin_segment' was
	 * called previously by 'AllocateSharedMemoryForShardSplitInfo'.
	 */
	dsm_unpin_segment(dsmHandle);

	/*
	 * As dynamic shared memory is unpinned, store an invalid handle in static
	 * shared memory used for handle management.
	 */
	StoreShardSplitSharedMemoryHandle(DSM_HANDLE_INVALID);
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
	ShardSplitShmemData *smData = ShmemInitStruct(SharedMemoryNameForHandleManagement,
												  sizeof(ShardSplitShmemData),
												  &alreadyInitialized);

	if (!alreadyInitialized)
	{
		char *trancheName = "Split Shard Setup Tranche";

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
 * StoreShardSplitSharedMemoryHandle stores a handle of shared memory
 * allocated and populated by 'worker_split_shard_replication_setup' UDF.
 * This handle is stored in a different statically allocated shared memory
 * segment with name 'Shared memory handle for shard split'.
 */
void
StoreShardSplitSharedMemoryHandle(dsm_handle dsmHandle)
{
	bool found = false;
	ShardSplitShmemData *smData = ShmemInitStruct(SharedMemoryNameForHandleManagement,
												  sizeof(ShardSplitShmemData),
												  &found);
	if (!found)
	{
		ereport(ERROR,
				errmsg(
					"Shared memory for handle management should have been initialized during boot"));
	}

	/*
	 * We only support non concurrent split. However, it is fine to take a
	 * lock and store the handle incase concurrent splits are introduced in future.
	 */
	LWLockAcquire(&smData->lock, LW_EXCLUSIVE);

	/*
	 * In a normal situation, previously stored handle should have been invalidated
	 * before the current function is called.
	 * If this handle is still valid, it means cleanup of previous split shard
	 * workflow failed. Log a waring and continue the current shard split operation.
	 * Skip warning if new handle to be stored is invalid. We store invalid handle
	 * when shared memory is released by calling worker_split_shard_release_dsm.
	 */
	if (smData->dsmHandle != DSM_HANDLE_INVALID && dsmHandle != DSM_HANDLE_INVALID)
	{
		ereport(WARNING,
				errmsg(
					"Previous split shard worflow was not successfully and could not complete the cleanup phase."
					" Continuing with the current split shard workflow."));
	}

	/* Store the incoming handle */
	smData->dsmHandle = dsmHandle;

	LWLockRelease(&smData->lock);
}


/*
 * GetShardSplitSharedMemoryHandle returns the handle of dynamic shared memory segment stored
 * by 'worker_split_shard_replication_setup' UDF. This handle is requested by WAL sender processes
 * during logical replication phase or during cleanup.
 */
dsm_handle
GetShardSplitSharedMemoryHandle(void)
{
	bool found = false;
	ShardSplitShmemData *smData = ShmemInitStruct(SharedMemoryNameForHandleManagement,
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
 * PopulateSourceToDestinationShardMapForSlot populates 'SourceToDestinationShard' hash map for a given slot.
 * Key of the map is Oid of source shard which is undergoing a split and value is a list of corresponding child shards.
 * To populate the map, the function traverses 'ShardSplitInfo' array stored within shared memory segment.
 */
HTAB *
PopulateSourceToDestinationShardMapForSlot(char *slotName, MemoryContext cxt)
{
	HASHCTL info;
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(SourceToDestinationShardMapEntry);
	info.hash = uint32_hash;
	info.hcxt = cxt;

	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	HTAB *sourceShardToDesShardMap = hash_create("SourceToDestinationShardMap", 128,
												 &info, hashFlags);

	MemoryContext oldContext = MemoryContextSwitchTo(cxt);

	ShardSplitInfoSMHeader *smHeader = GetShardSplitInfoSMHeader();
	for (int index = 0; index < smHeader->count; index++)
	{
		if (strcmp(smHeader->splitInfoArray[index].slotName, slotName) == 0)
		{
			Oid sourceShardOid = smHeader->splitInfoArray[index].sourceShardOid;
			bool found = false;
			SourceToDestinationShardMapEntry *entry =
				(SourceToDestinationShardMapEntry *) hash_search(
					sourceShardToDesShardMap, &sourceShardOid, HASH_ENTER, &found);

			if (!found)
			{
				entry->shardSplitInfoList = NIL;
				entry->sourceShardKey = sourceShardOid;
			}

			ShardSplitInfo *shardSplitInfoForSlot = (ShardSplitInfo *) palloc0(
				sizeof(ShardSplitInfo));
			*shardSplitInfoForSlot = smHeader->splitInfoArray[index];

			entry->shardSplitInfoList = lappend(entry->shardSplitInfoList,
												(ShardSplitInfo *) shardSplitInfoForSlot);
		}
	}

	MemoryContextSwitchTo(oldContext);
	return sourceShardToDesShardMap;
}
