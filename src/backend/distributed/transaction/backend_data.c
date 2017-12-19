/*-------------------------------------------------------------------------
 *
 * backend_data.c
 *
 *  Infrastructure for managing per backend data that can efficiently
 *  accessed by all sessions.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "distributed/backend_data.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/transaction_identifier.h"
#include "nodes/execnodes.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "utils/timestamp.h"


/*
 * Each backend's data reside in the shared memory
 * on the BackendManagementShmemData.
 */
typedef struct BackendManagementShmemData
{
	int trancheId;
#if (PG_VERSION_NUM >= 100000)
	NamedLWLockTranche namedLockTranche;
#else
	LWLockTranche lockTranche;
#endif
	LWLock lock;

	/*
	 * We prefer to use an atomic integer over sequences for two
	 * reasons (i) orders of magnitude performance difference
	 * (ii) allowing read-only replicas to be able to generate ids
	 */
	pg_atomic_uint64 nextTransactionNumber;

	BackendData backends[FLEXIBLE_ARRAY_MEMBER];
} BackendManagementShmemData;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static BackendManagementShmemData *backendManagementShmemData = NULL;
static BackendData *MyBackendData = NULL;


static void BackendManagementShmemInit(void);
static size_t BackendManagementShmemSize(void);


PG_FUNCTION_INFO_V1(assign_distributed_transaction_id);
PG_FUNCTION_INFO_V1(get_current_transaction_id);
PG_FUNCTION_INFO_V1(get_all_active_transactions);


/*
 * assign_distributed_transaction_id updates the shared memory allocated for this backend
 * and sets initiatorNodeIdentifier, transactionNumber, timestamp fields with the given
 * inputs. Also, the function sets the database id and process id via the information that
 * Postgres provides.
 *
 * This function is only intended for internal use for managing distributed transactions.
 * Users should not use this function for any purpose.
 */
Datum
assign_distributed_transaction_id(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/* MyBackendData should always be avaliable, just out of paranoia */
	if (!MyBackendData)
	{
		ereport(ERROR, (errmsg("backend is not ready for distributed transactions")));
	}

	/*
	 * Note that we don't need to lock shared memory (i.e., LockBackendSharedMemory()) here
	 * since this function is executed after AssignDistributedTransactionId() issued on the
	 * initiator node, which already takes the required lock to enforce the consistency.
	 */

	SpinLockAcquire(&MyBackendData->mutex);

	/* if an id is already assigned, release the lock and error */
	if (MyBackendData->transactionId.transactionNumber != 0)
	{
		SpinLockRelease(&MyBackendData->mutex);

		ereport(ERROR, (errmsg("the backend has already been assigned a "
							   "transaction id")));
	}

	MyBackendData->databaseId = MyDatabaseId;

	MyBackendData->transactionId.initiatorNodeIdentifier = PG_GETARG_INT32(0);
	MyBackendData->transactionId.transactionNumber = PG_GETARG_INT64(1);
	MyBackendData->transactionId.timestamp = PG_GETARG_TIMESTAMPTZ(2);
	MyBackendData->transactionId.transactionOriginator = false;

	SpinLockRelease(&MyBackendData->mutex);

	PG_RETURN_VOID();
}


/*
 * get_current_transaction_id returns a tuple with (databaseId, processId,
 * initiatorNodeIdentifier, transactionNumber, timestamp) that exists in the
 * shared memory associated with this backend. Note that if the backend
 * is not in a transaction, the function returns uninitialized data where
 * transactionNumber equals to 0.
 */
Datum
get_current_transaction_id(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;

	const int attributeCount = 5;
	Datum values[attributeCount];
	bool isNulls[attributeCount];

	DistributedTransactionId *distributedTransctionId = NULL;

	CheckCitusVersion(ERROR);

	/* build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDescriptor) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	/* MyBackendData should always be avaliable, just out of paranoia */
	if (!MyBackendData)
	{
		ereport(ERROR, (errmsg("backend is not ready for distributed transactions")));
	}

	distributedTransctionId = GetCurrentDistributedTransactionId();

	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	/* first two fields do not change for this backend, so get directly */
	values[0] = ObjectIdGetDatum(MyDatabaseId);
	values[1] = Int32GetDatum(MyProcPid);

	values[2] = Int32GetDatum(distributedTransctionId->initiatorNodeIdentifier);
	values[3] = UInt64GetDatum(distributedTransctionId->transactionNumber);

	/* provide a better output */
	if (distributedTransctionId->initiatorNodeIdentifier != 0)
	{
		values[4] = TimestampTzGetDatum(distributedTransctionId->timestamp);
	}
	else
	{
		isNulls[4] = true;
	}

	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(heapTuple));
}


/*
 * get_all_active_transactions returns all the avaliable information about all
 * the active backends.
 */
Datum
get_all_active_transactions(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *returnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = NULL;
	MemoryContext perQueryContext = NULL;
	MemoryContext oldContext = NULL;

	int backendIndex = 0;

	const int attributeCount = 5;
	Datum values[attributeCount];
	bool isNulls[attributeCount];

	CheckCitusVersion(ERROR);

	/* check to see if caller supports us returning a tuplestore */
	if (returnSetInfo == NULL || !IsA(returnSetInfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context " \
						"that cannot accept a set")));
	}

	if (!(returnSetInfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}

	/* build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDescriptor) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	perQueryContext = returnSetInfo->econtext->ecxt_per_query_memory;

	oldContext = MemoryContextSwitchTo(perQueryContext);

	tupleStore = tuplestore_begin_heap(true, false, work_mem);
	returnSetInfo->returnMode = SFRM_Materialize;
	returnSetInfo->setResult = tupleStore;
	returnSetInfo->setDesc = tupleDescriptor;

	MemoryContextSwitchTo(oldContext);

	/*
	 * We don't want to initialize memory while spinlock is held so we
	 * prefer to do it here. This initialization is done only for the first
	 * row.
	 */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	/* we're reading all distributed transactions, prevent new backends */
	LockBackendSharedMemory(LW_SHARED);

	for (backendIndex = 0; backendIndex < MaxBackends; ++backendIndex)
	{
		BackendData *currentBackend =
			&backendManagementShmemData->backends[backendIndex];

		SpinLockAcquire(&currentBackend->mutex);

		/* we're only interested in active backends */
		if (currentBackend->transactionId.transactionNumber == 0)
		{
			SpinLockRelease(&currentBackend->mutex);
			continue;
		}

		values[0] = ObjectIdGetDatum(currentBackend->databaseId);
		values[1] = Int32GetDatum(ProcGlobal->allProcs[backendIndex].pid);
		values[2] = Int32GetDatum(currentBackend->transactionId.initiatorNodeIdentifier);
		values[3] = UInt64GetDatum(currentBackend->transactionId.transactionNumber);
		values[4] = TimestampTzGetDatum(currentBackend->transactionId.timestamp);

		SpinLockRelease(&currentBackend->mutex);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);

		/*
		 * We don't want to initialize memory while spinlock is held so we
		 * prefer to do it here. This initialization is done for the rows
		 * starting from the second one.
		 */
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));
	}

	UnlockBackendSharedMemory();

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}


/*
 * InitializeBackendManagement requests the necessary shared memory
 * from Postgres and sets up the shared memory startup hook.
 */
void
InitializeBackendManagement(void)
{
	/* allocate shared memory */
	if (!IsUnderPostmaster)
	{
		RequestAddinShmemSpace(BackendManagementShmemSize());
	}

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = BackendManagementShmemInit;
}


/*
 * BackendManagementShmemInit is the callback that is to be called on shared
 * memory startup hook. The function sets up the necessary shared memory
 * segment for the backend manager.
 */
static void
BackendManagementShmemInit(void)
{
	bool alreadyInitialized = false;

	/* we may update the shmem, acquire lock exclusively */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	backendManagementShmemData =
		(BackendManagementShmemData *) ShmemInitStruct(
			"Backend Management Shmem",
			BackendManagementShmemSize(),
			&alreadyInitialized);

	if (!alreadyInitialized)
	{
		int backendIndex = 0;
		char *trancheName = "Backend Management Tranche";

#if (PG_VERSION_NUM >= 100000)
		NamedLWLockTranche *namedLockTranche =
			&backendManagementShmemData->namedLockTranche;

#else
		LWLockTranche *lockTranche = &backendManagementShmemData->lockTranche;
#endif

		/* start by zeroing out all the memory */
		memset(backendManagementShmemData, 0,
			   BackendManagementShmemSize());

#if (PG_VERSION_NUM >= 100000)
		namedLockTranche->trancheId = LWLockNewTrancheId();

		LWLockRegisterTranche(namedLockTranche->trancheId, trancheName);
		LWLockInitialize(&backendManagementShmemData->lock,
						 namedLockTranche->trancheId);
#else
		backendManagementShmemData->trancheId = LWLockNewTrancheId();

		/* we only need a single lock */
		lockTranche->array_base = &backendManagementShmemData->lock;
		lockTranche->array_stride = sizeof(LWLock);
		lockTranche->name = trancheName;

		LWLockRegisterTranche(backendManagementShmemData->trancheId, lockTranche);
		LWLockInitialize(&backendManagementShmemData->lock,
						 backendManagementShmemData->trancheId);
#endif

		/* start the distributed transaction ids from 1 */
		pg_atomic_init_u64(&backendManagementShmemData->nextTransactionNumber, 1);

		/*
		 * We need to init per backend's spinlock before any backend
		 * starts its execution.
		 */
		for (backendIndex = 0; backendIndex < MaxBackends; ++backendIndex)
		{
			SpinLockInit(&backendManagementShmemData->backends[backendIndex].mutex);
		}
	}

	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * BackendManagementShmemSize returns the size that should be allocated
 * on the shared memory for backend management.
 */
static size_t
BackendManagementShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(BackendManagementShmemData));
	size = add_size(size, mul_size(sizeof(BackendData), MaxBackends));

	return size;
}


/*
 *  InitializeBackendData is called per backend and does the
 *  required initialization.
 */
void
InitializeBackendData(void)
{
	MyBackendData = &backendManagementShmemData->backends[MyProc->pgprocno];

	Assert(MyBackendData);

	LockBackendSharedMemory(LW_EXCLUSIVE);

	SpinLockAcquire(&MyBackendData->mutex);

	MyBackendData->databaseId = MyDatabaseId;
	MyBackendData->transactionId.initiatorNodeIdentifier = 0;
	MyBackendData->transactionId.transactionOriginator = false;
	MyBackendData->transactionId.transactionNumber = 0;
	MyBackendData->transactionId.timestamp = 0;

	SpinLockRelease(&MyBackendData->mutex);

	UnlockBackendSharedMemory();
}


/*
 * UnSetDistributedTransactionId simply acquires the mutex and resets the backend's
 * distributed transaction data in shared memory to the initial values.
 */
void
UnSetDistributedTransactionId(void)
{
	/* backend does not exist if the extension is not created */
	if (MyBackendData)
	{
		SpinLockAcquire(&MyBackendData->mutex);

		MyBackendData->databaseId = 0;
		MyBackendData->transactionId.initiatorNodeIdentifier = 0;
		MyBackendData->transactionId.transactionOriginator = false;
		MyBackendData->transactionId.transactionNumber = 0;
		MyBackendData->transactionId.timestamp = 0;

		SpinLockRelease(&MyBackendData->mutex);
	}
}


/*
 * LockBackendSharedMemory is a simple wrapper around LWLockAcquire on the
 * shared memory lock.
 *
 * We use the backend shared memory lock for preventing new backends to be part
 * of a new distributed transaction or an existing backend to leave a distributed
 * transaction while we're reading the all backends' data.
 *
 * The primary goal is to provide consistent view of the current distributed
 * transactions while doing the deadlock detection.
 */
void
LockBackendSharedMemory(LWLockMode lockMode)
{
	LWLockAcquire(&backendManagementShmemData->lock, lockMode);
}


/*
 * UnlockBackendSharedMemory is a simple wrapper around LWLockRelease on the
 * shared memory lock.
 */
void
UnlockBackendSharedMemory(void)
{
	LWLockRelease(&backendManagementShmemData->lock);
}


/*
 * GetCurrentDistributedTransactionId reads the backend's distributed transaction id and
 * returns a copy of it.
 *
 * When called from a parallel worker, it uses the parent's transaction ID per the logic
 * in GetBackendDataForProc.
 */
DistributedTransactionId *
GetCurrentDistributedTransactionId(void)
{
	DistributedTransactionId *currentDistributedTransactionId =
		(DistributedTransactionId *) palloc(sizeof(DistributedTransactionId));
	BackendData backendData;

	GetBackendDataForProc(MyProc, &backendData);

	currentDistributedTransactionId->initiatorNodeIdentifier =
		backendData.transactionId.initiatorNodeIdentifier;
	currentDistributedTransactionId->transactionOriginator =
		backendData.transactionId.transactionOriginator;
	currentDistributedTransactionId->transactionNumber =
		backendData.transactionId.transactionNumber;
	currentDistributedTransactionId->timestamp =
		backendData.transactionId.timestamp;

	return currentDistributedTransactionId;
}


/*
 * AssignDistributedTransactionId generates a new distributed transaction id and
 * sets it for the current backend. It also sets the databaseId and
 * processId fields.
 *
 * This function should only be called on BeginCoordinatedTransaction(). Any other
 * callers is very likely to break the distributed transction management.
 */
void
AssignDistributedTransactionId(void)
{
	pg_atomic_uint64 *transactionNumberSequence =
		&backendManagementShmemData->nextTransactionNumber;

	uint64 nextTransactionNumber = pg_atomic_fetch_add_u64(transactionNumberSequence, 1);
	int localGroupId = GetLocalGroupId();
	TimestampTz currentTimestamp = GetCurrentTimestamp();

	SpinLockAcquire(&MyBackendData->mutex);

	MyBackendData->databaseId = MyDatabaseId;

	MyBackendData->transactionId.initiatorNodeIdentifier = localGroupId;
	MyBackendData->transactionId.transactionOriginator = true;
	MyBackendData->transactionId.transactionNumber =
		nextTransactionNumber;
	MyBackendData->transactionId.timestamp = currentTimestamp;

	SpinLockRelease(&MyBackendData->mutex);
}


/*
 * CurrentDistributedTransactionNumber returns the transaction number of the
 * current distributed transaction. The caller must make sure a distributed
 * transaction is in progress.
 */
uint64
CurrentDistributedTransactionNumber(void)
{
	Assert(MyBackendData != NULL);

	return MyBackendData->transactionId.transactionNumber;
}


/*
 * GetBackendDataForProc writes the backend data for the given process to
 * result. If the process is part of a lock group (parallel query) it
 * returns the leader data instead.
 */
void
GetBackendDataForProc(PGPROC *proc, BackendData *result)
{
	BackendData *backendData = NULL;
	int pgprocno = proc->pgprocno;

	if (proc->lockGroupLeader != NULL)
	{
		pgprocno = proc->lockGroupLeader->pgprocno;
	}

	backendData = &backendManagementShmemData->backends[pgprocno];

	SpinLockAcquire(&backendData->mutex);

	memcpy(result, backendData, sizeof(BackendData));

	SpinLockRelease(&backendData->mutex);
}


/*
 * CancelTransactionDueToDeadlock cancels the input proc and also marks the backend
 * data with this information.
 */
void
CancelTransactionDueToDeadlock(PGPROC *proc)
{
	BackendData *backendData = &backendManagementShmemData->backends[proc->pgprocno];

	/* backend might not have used citus yet and thus not initialized backend data */
	if (!backendData)
	{
		return;
	}

	SpinLockAcquire(&backendData->mutex);

	/* send a SIGINT only if the process is still in a distributed transaction */
	if (backendData->transactionId.transactionNumber != 0)
	{
		backendData->cancelledDueToDeadlock = true;
		SpinLockRelease(&backendData->mutex);

		if (kill(proc->pid, SIGINT) != 0)
		{
			ereport(WARNING,
					(errmsg("attempted to cancel this backend (pid: %d) to resolve a "
							"distributed deadlock but the backend could not "
							"be cancelled", proc->pid)));
		}
	}
	else
	{
		SpinLockRelease(&backendData->mutex);
	}
}


/*
 * MyBackendGotCancelledDueToDeadlock returns whether the current distributed
 * transaction was cancelled due to a deadlock. If the backend is not in a
 * distributed transaction, the function returns false.
 */
bool
MyBackendGotCancelledDueToDeadlock(void)
{
	bool cancelledDueToDeadlock = false;

	/* backend might not have used citus yet and thus not initialized backend data */
	if (!MyBackendData)
	{
		return false;
	}

	SpinLockAcquire(&MyBackendData->mutex);

	if (IsInDistributedTransaction(MyBackendData))
	{
		cancelledDueToDeadlock = MyBackendData->cancelledDueToDeadlock;
	}

	SpinLockRelease(&MyBackendData->mutex);

	return cancelledDueToDeadlock;
}


/*
 * ActiveDistributedTransactionNumbers returns a list of pointers to
 * transaction numbers of distributed transactions that are in progress
 * and were started by the node on which it is called.
 */
List *
ActiveDistributedTransactionNumbers(void)
{
	List *activeTransactionNumberList = NIL;
	int curBackend = 0;

	/* build list of starting procs */
	for (curBackend = 0; curBackend < MaxBackends; curBackend++)
	{
		PGPROC *currentProc = &ProcGlobal->allProcs[curBackend];
		BackendData currentBackendData;
		uint64 *transactionNumber = NULL;

		if (currentProc->pid == 0)
		{
			/* unused PGPROC slot */
			continue;
		}

		GetBackendDataForProc(currentProc, &currentBackendData);

		if (!IsInDistributedTransaction(&currentBackendData))
		{
			/* not a distributed transaction */
			continue;
		}

		if (!currentBackendData.transactionId.transactionOriginator)
		{
			/* not a coordinator process */
			continue;
		}

		transactionNumber = (uint64 *) palloc0(sizeof(uint64));
		*transactionNumber = currentBackendData.transactionId.transactionNumber;

		activeTransactionNumberList = lappend(activeTransactionNumberList,
											  transactionNumber);
	}

	return activeTransactionNumberList;
}
