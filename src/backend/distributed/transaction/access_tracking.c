/*-------------------------------------------------------------------------
 *
 * access_tracking.c
 *
 *   Transaction access tracking for Citus. The functions in this file
 *   are intended to track the relation accesses within a transaction. The
 *   logic here is mostly useful when a reference table is referred by
 *   a distributed table via a foreign key. Whenever such a pair of tables
 *   are acccesed inside a transaction, Citus should detect and act
 *   accordingly.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"


#include "access/transam.h"
#include "access/xact.h"
#include "distributed/access_tracking.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/lsyscache.h"


/* the following macros are copied from lmgr/lock.c */
#define FAST_PATH_BITS_PER_SLOT 3
#define FAST_PATH_LOCKNUMBER_OFFSET 1
#define FAST_PATH_MASK ((1 << FAST_PATH_BITS_PER_SLOT) - 1)
#define FAST_PATH_GET_BITS(proc, n) \
	(((proc)->fpLockBits >> (FAST_PATH_BITS_PER_SLOT * n)) & FAST_PATH_MASK)


/* this function is only exported in the regression tests */
PG_FUNCTION_INFO_V1(relation_accessed_in_transaction_block);


/*
 * relation_accessed_in_transaction_block returns true if the given relation
 * is accessed in the current transaction block.
 */
Datum
relation_accessed_in_transaction_block(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	bool relationAccessed =
		RelationAccessedInTransactionBlock(list_make1_oid(relationId));

	return BoolGetDatum(relationAccessed);
}


/*
 * RelationAccessedInTransactionBlock returns true if any element of the
 * relation id list that is provided is accessed in the current transaction
 * block.
 *
 * Note that the function returns true as soon as any relation is found.
 */
bool
RelationAccessedInTransactionBlock(List *relationIds)
{
	int fastPathSlot = 0;
	int partitionNum = 0;
	SHM_QUEUE *procLocks = NULL;
	PROCLOCK *proclock = NULL;
	LOCK *lock = NULL;

	/* if we're not in a transaction block, no further checks needed */
	if (!IsTransactionBlock())
	{
		return false;
	}

	/*
	 * We check relation accesses in two steps. In the first step, we check the
	 * fast path locks that MyProc holds. Later, we iterate over all the procLoks
	 * that MyProc holds. With these two iterations, we are able to access all
	 * the locks that are held by the current backend.
	 */
	LWLockAcquire(&MyProc->backendLock, LW_SHARED);

	for (fastPathSlot = 0; fastPathSlot < FP_LOCK_SLOTS_PER_BACKEND; ++fastPathSlot)
	{
		uint32 lockbits = FAST_PATH_GET_BITS(MyProc, fastPathSlot);
		Oid lockedRelationId = InvalidOid;

		/* skip unallocated slots */
		if (!lockbits)
		{
			continue;
		}

		lockedRelationId = MyProc->fpRelId[fastPathSlot];

		/* we don't need to check catalog tables */
		if (lockedRelationId > FirstNormalObjectId &&
			list_member_oid(relationIds, lockedRelationId))
		{
			LWLockRelease(&MyProc->backendLock);

			return true;
		}
	}

	LWLockRelease(&MyProc->backendLock);

	/* now iterate over the procLocks */
	for (partitionNum = 0; partitionNum < NUM_LOCK_PARTITIONS; ++partitionNum)
	{
		LWLockAcquire(LockHashPartitionLockByIndex(partitionNum), LW_SHARED);

		procLocks = &(MyProc->myProcLocks[partitionNum]);

		proclock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
											 offsetof(PROCLOCK, procLink));

		while (proclock)
		{
			Assert(proclock->tag.myProc == MyProc);

			lock = proclock->tag.myLock;

			/* we're only interested in relation locks */
			if (lock->tag.locktag_type == LOCKTAG_RELATION)
			{
				Oid lockedRelationId = lock->tag.locktag_field2;

				/* we don't need to check catalog tables */
				if (lockedRelationId > FirstNormalObjectId &&
					list_member_oid(relationIds, lockedRelationId))
				{
					LWLockRelease(LockHashPartitionLockByIndex(partitionNum));

					return true;
				}
			}

			proclock = (PROCLOCK *)
					   SHMQueueNext(procLocks, &proclock->procLink,
									offsetof(PROCLOCK, procLink));
		}

		LWLockRelease(LockHashPartitionLockByIndex(partitionNum));
	}

	return false;
}
