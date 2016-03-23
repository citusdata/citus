/*-------------------------------------------------------------------------
 *
 * multi_resowner.c
 *	  Citus resource owner integration
 *
 * An extension can't directly add members to ResourceOwnerData. Instead we
 * have to use the resource owner callback mechanism. Right now it's
 * sufficient to have an array of referenced resources - there bascially are
 * never more than a handful of entries, if that. If that changes we should
 * probably rather use a hash table using the pointer value of the resource
 * owner as key.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/multi_server_executor.h"
#include "utils/memutils.h"
#include "utils/resowner_private.h"
#include "distributed/multi_resowner.h"


typedef struct JobDirectoryEntry
{
	ResourceOwner owner;
	uint64 jobId;
} JobDirectoryEntry;


static bool RegisteredResownerCallback = false;
JobDirectoryEntry *RegisteredJobDirectories = NULL;
size_t NumRegisteredJobDirectories = 0;
size_t NumAllocatedJobDirectories = 0;


/*
 * Resource owner callback - release resources still held by the resource
 * owner.
 */
static void
MultiResourceOwnerReleaseCallback(ResourceReleasePhase phase,
								  bool isCommit,
								  bool isTopLevel,
								  void *arg)
{
	int lastJobIndex = NumRegisteredJobDirectories - 1;
	int jobIndex = 0;

	if (phase == RESOURCE_RELEASE_AFTER_LOCKS)
	{
		/*
		 * Remove all remaining job directories, after locks have been
		 * released.
		 */
		for (jobIndex = lastJobIndex; jobIndex >= 0; jobIndex--)
		{
			JobDirectoryEntry *entry = &RegisteredJobDirectories[jobIndex];

			if (entry->owner == CurrentResourceOwner)
			{
				RemoveJobDirectory(entry->jobId);
			}
		}
	}
}


/*
 * ResourceOwnerEnlargeJobDirectories makes sure that there is space to
 * reference at least one more job directory for the resource owner. Note that
 * we only expect one job directory per portal, but we still use an array
 * here.
 *
 * This function is separate from the one actually inserting an entry because
 * if we run out of memory, it's critical to do so *before* acquiring the
 * resource.
 */
void
ResourceOwnerEnlargeJobDirectories(ResourceOwner owner)
{
	int newMax = 0;

	/* ensure callback is registered */
	if (!RegisteredResownerCallback)
	{
		RegisterResourceReleaseCallback(MultiResourceOwnerReleaseCallback, NULL);
		RegisteredResownerCallback = true;
	}

	if (RegisteredJobDirectories == NULL)
	{
		newMax = 16;
		RegisteredJobDirectories =
			(JobDirectoryEntry *) MemoryContextAlloc(TopMemoryContext,
													 newMax * sizeof(JobDirectoryEntry));
		NumAllocatedJobDirectories = newMax;
	}
	else if (NumRegisteredJobDirectories + 1 > NumAllocatedJobDirectories)
	{
		newMax = NumAllocatedJobDirectories * 2;
		RegisteredJobDirectories =
			(JobDirectoryEntry *) repalloc(RegisteredJobDirectories,
										   newMax * sizeof(JobDirectoryEntry));
		NumAllocatedJobDirectories = newMax;
	}
}


/* Remembers that a temporary job directory is owned by a resource owner. */
void
ResourceOwnerRememberJobDirectory(ResourceOwner owner, uint64 jobId)
{
	JobDirectoryEntry *entry = NULL;

	Assert(NumRegisteredJobDirectories + 1 <= NumAllocatedJobDirectories);
	entry = &RegisteredJobDirectories[NumRegisteredJobDirectories];
	entry->owner = owner;
	entry->jobId = jobId;
	NumRegisteredJobDirectories++;
}


/* Forgets that a temporary job directory is owned by a resource owner. */
void
ResourceOwnerForgetJobDirectory(ResourceOwner owner, uint64 jobId)
{
	int lastJobIndex = NumRegisteredJobDirectories - 1;
	int jobIndex = 0;

	for (jobIndex = lastJobIndex; jobIndex >= 0; jobIndex--)
	{
		JobDirectoryEntry *entry = &RegisteredJobDirectories[jobIndex];

		if (entry->owner == owner && entry->jobId == jobId)
		{
			/* move all later entries one up */
			while (jobIndex < lastJobIndex)
			{
				RegisteredJobDirectories[jobIndex] =
					RegisteredJobDirectories[jobIndex + 1];
				jobIndex++;
			}
			NumRegisteredJobDirectories = lastJobIndex;
			return;
		}
	}

	elog(ERROR, "jobId " UINT64_FORMAT " is not owned by resource owner %p",
		 jobId, owner);
}
