/*-------------------------------------------------------------------------
 *
 * backend_data.c
 *
 *  Infrastructure for managing per backend data that can efficiently
 *  accessed by all sessions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"
#include "nodes/execnodes.h"
#include "postmaster/autovacuum.h" /* to access autovacuum_max_workers */
#if PG_VERSION_NUM >= 120000
#include "replication/walsender.h"
#endif
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "utils/timestamp.h"


#define GET_ACTIVE_TRANSACTION_QUERY "SELECT * FROM get_all_active_transactions();"
#define ACTIVE_TRANSACTION_COLUMN_COUNT 6

/*
 * Each backend's data reside in the shared memory
 * on the BackendManagementShmemData.
 */
typedef struct BackendManagementShmemData
{
	int trancheId;
	NamedLWLockTranche namedLockTranche;
	LWLock lock;

	/*
	 * We prefer to use an atomic integer over sequences for two
	 * reasons (i) orders of magnitude performance difference
	 * (ii) allowing read-only replicas to be able to generate ids
	 */
	pg_atomic_uint64 nextTransactionNumber;

	BackendData backends[FLEXIBLE_ARRAY_MEMBER];
} BackendManagementShmemData;


static void StoreAllActiveTransactions(Tuplestorestate *tupleStore, TupleDesc
									   tupleDescriptor);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static BackendManagementShmemData *backendManagementShmemData = NULL;
static BackendData *MyBackendData = NULL;


static void BackendManagementShmemInit(void);
static size_t BackendManagementShmemSize(void);


PG_FUNCTION_INFO_V1(assign_distributed_transaction_id);
PG_FUNCTION_INFO_V1(get_current_transaction_id);
PG_FUNCTION_INFO_V1(get_global_active_transactions);
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
	Oid userId = GetUserId();

	/* prepare data before acquiring spinlock to protect against errors */
	int32 initiatorNodeIdentifier = PG_GETARG_INT32(0);
	uint64 transactionNumber = PG_GETARG_INT64(1);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(2);

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
	MyBackendData->userId = userId;

	MyBackendData->transactionId.initiatorNodeIdentifier = initiatorNodeIdentifier;
	MyBackendData->transactionId.transactionNumber = transactionNumber;
	MyBackendData->transactionId.timestamp = timestamp;
	MyBackendData->transactionId.transactionOriginator = false;

	MyBackendData->citusBackend.initiatorNodeIdentifier =
		MyBackendData->transactionId.initiatorNodeIdentifier;
	MyBackendData->citusBackend.transactionOriginator = false;

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

	Datum values[5];
	bool isNulls[5];

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
 * get_global_active_transactions returns all the available information about all
 * the active backends from each node of the cluster. If you call that function from
 * the coordinator, it will returns back active transaction from the coordinator as
 * well. Yet, if you call it from the worker, result won't include the transactions
 * on the coordinator node, since worker nodes are not aware of the coordinator.
 */
Datum
get_global_active_transactions(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = NULL;
	List *workerNodeList = ActivePrimaryWorkerNodeList(NoLock);
	ListCell *workerNodeCell = NULL;
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;
	StringInfo queryToSend = makeStringInfo();

	CheckCitusVersion(ERROR);
	tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	appendStringInfo(queryToSend, GET_ACTIVE_TRANSACTION_QUERY);

	/* add active transactions for local node */
	StoreAllActiveTransactions(tupleStore, tupleDescriptor);

	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		if (workerNode->groupId == GetLocalGroupId())
		{
			/* we already get these transactions via GetAllActiveTransactions() */
			continue;
		}

		connection = StartNodeConnection(connectionFlags, nodeName, nodePort);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int querySent = false;

		querySent = SendRemoteCommand(connection, queryToSend->data);
		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
		}
	}

	/* receive query results */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		PGresult *result = NULL;
		bool raiseInterrupts = true;
		Datum values[ACTIVE_TRANSACTION_COLUMN_COUNT];
		bool isNulls[ACTIVE_TRANSACTION_COLUMN_COUNT];
		int64 rowIndex = 0;
		int64 rowCount = 0;
		int64 colCount = 0;

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			continue;
		}

		result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			continue;
		}

		rowCount = PQntuples(result);
		colCount = PQnfields(result);

		/* Although it is not expected */
		if (colCount != ACTIVE_TRANSACTION_COLUMN_COUNT)
		{
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "get_all_active_transactions")));
			continue;
		}

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			memset(values, 0, sizeof(values));
			memset(isNulls, false, sizeof(isNulls));

			values[0] = ParseIntField(result, rowIndex, 0);
			values[1] = ParseIntField(result, rowIndex, 1);
			values[2] = ParseIntField(result, rowIndex, 2);
			values[3] = ParseBoolField(result, rowIndex, 3);
			values[4] = ParseIntField(result, rowIndex, 4);
			values[5] = ParseTimestampTzField(result, rowIndex, 5);

			tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
		}

		PQclear(result);
		ForgetResults(connection);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}


/*
 * get_all_active_transactions returns all the avaliable information about all
 * the active backends.
 */
Datum
get_all_active_transactions(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = NULL;

	CheckCitusVersion(ERROR);
	tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	StoreAllActiveTransactions(tupleStore, tupleDescriptor);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}


/*
 * StoreAllActiveTransactions gets active transaction from the local node and inserts
 * them into the given tuplestore.
 */
static void
StoreAllActiveTransactions(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	int backendIndex = 0;
	Datum values[ACTIVE_TRANSACTION_COLUMN_COUNT];
	bool isNulls[ACTIVE_TRANSACTION_COLUMN_COUNT];
	bool showAllTransactions = superuser();
	const Oid userId = GetUserId();

	/*
	 * We don't want to initialize memory while spinlock is held so we
	 * prefer to do it here. This initialization is done only for the first
	 * row.
	 */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	if (is_member_of_role(userId, DEFAULT_ROLE_MONITOR))
	{
		showAllTransactions = true;
	}

	/* we're reading all distributed transactions, prevent new backends */
	LockBackendSharedMemory(LW_SHARED);

	for (backendIndex = 0; backendIndex < MaxBackends; ++backendIndex)
	{
		BackendData *currentBackend =
			&backendManagementShmemData->backends[backendIndex];
		bool coordinatorOriginatedQuery = false;

		/* to work on data after releasing g spinlock to protect against errors */
		Oid databaseId = InvalidOid;
		int backendPid = -1;
		int initiatorNodeIdentifier = -1;
		uint64 transactionNumber = 0;
		TimestampTz transactionIdTimestamp = 0;

		SpinLockAcquire(&currentBackend->mutex);

		/* we're only interested in backends initiated by Citus */
		if (currentBackend->citusBackend.initiatorNodeIdentifier < 0)
		{
			SpinLockRelease(&currentBackend->mutex);
			continue;
		}

		/*
		 * Unless the user has a role that allows seeing all transactions (superuser,
		 * pg_monitor), skip over transactions belonging to other users.
		 */
		if (!showAllTransactions && currentBackend->userId != userId)
		{
			SpinLockRelease(&currentBackend->mutex);
			continue;
		}

		databaseId = currentBackend->databaseId;
		backendPid = ProcGlobal->allProcs[backendIndex].pid;
		initiatorNodeIdentifier = currentBackend->citusBackend.initiatorNodeIdentifier;

		/*
		 * We prefer to use worker_query instead of transactionOriginator in the user facing
		 * functions since its more intuitive. Thus, we negate the result before returning.
		 *
		 * We prefer to use citusBackend's transactionOriginator field over transactionId's
		 * field with the same name. The reason is that it also covers backends that are not
		 * inside a distributed transaction.
		 */
		coordinatorOriginatedQuery = currentBackend->citusBackend.transactionOriginator;

		transactionNumber = currentBackend->transactionId.transactionNumber;
		transactionIdTimestamp = currentBackend->transactionId.timestamp;

		SpinLockRelease(&currentBackend->mutex);

		values[0] = ObjectIdGetDatum(databaseId);
		values[1] = Int32GetDatum(backendPid);
		values[2] = Int32GetDatum(initiatorNodeIdentifier);
		values[3] = !coordinatorOriginatedQuery;
		values[4] = UInt64GetDatum(transactionNumber);
		values[5] = TimestampTzGetDatum(transactionIdTimestamp);

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
		int totalProcs = 0;
		char *trancheName = "Backend Management Tranche";

		NamedLWLockTranche *namedLockTranche =
			&backendManagementShmemData->namedLockTranche;

		/* start by zeroing out all the memory */
		memset(backendManagementShmemData, 0,
			   BackendManagementShmemSize());

		namedLockTranche->trancheId = LWLockNewTrancheId();

		LWLockRegisterTranche(namedLockTranche->trancheId, trancheName);
		LWLockInitialize(&backendManagementShmemData->lock,
						 namedLockTranche->trancheId);

		/* start the distributed transaction ids from 1 */
		pg_atomic_init_u64(&backendManagementShmemData->nextTransactionNumber, 1);

		/*
		 * We need to init per backend's spinlock before any backend
		 * starts its execution. Note that we initialize TotalProcs (e.g., not
		 * MaxBackends) since some of the blocking processes could be prepared
		 * transactions, which aren't covered by MaxBackends.
		 *
		 * We also initiate initiatorNodeIdentifier to -1, which can never be
		 * used as a node id.
		 */
		totalProcs = TotalProcCount();
		for (backendIndex = 0; backendIndex < totalProcs; ++backendIndex)
		{
			BackendData *backendData =
				&backendManagementShmemData->backends[backendIndex];
			backendData->citusBackend.initiatorNodeIdentifier = -1;
			SpinLockInit(&backendData->mutex);
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
	int totalProcs = TotalProcCount();

	size = add_size(size, sizeof(BackendManagementShmemData));
	size = add_size(size, mul_size(sizeof(BackendData), totalProcs));

	return size;
}


/*
 * TotalProcCount returns the total processes that could run via the current
 * postgres server. See the details in the function comments.
 *
 * There is one thing we should warn the readers. Citus enforces to be loaded
 * as the first extension in shared_preload_libraries. However, if any other
 * extension overrides MaxConnections, autovacuum_max_workers or
 * max_worker_processes, our reasoning in this function may not work as expected.
 * Given that it is not a usual pattern for extension, we consider Citus' behaviour
 * good enough for now.
 */
int
TotalProcCount(void)
{
	int maxBackends = 0;
	int totalProcs = 0;

#ifdef WIN32

	/* autovacuum_max_workers is not PGDLLIMPORT, so use a high estimate for windows */
	int estimatedMaxAutovacuumWorkers = 30;
	maxBackends =
		MaxConnections + estimatedMaxAutovacuumWorkers + 1 + max_worker_processes;
#else

	/*
	 * We're simply imitating Postgrsql's InitializeMaxBackends(). Given that all
	 * the items used here PGC_POSTMASTER, should be safe to access them
	 * anytime during the execution even before InitializeMaxBackends() is called.
	 */
	maxBackends = MaxConnections + autovacuum_max_workers + 1 + max_worker_processes;
#endif

	/*
	 * We prefer to maintain space for auxiliary procs or preperad transactions in
	 * the backend space because they could be blocking processes and our current
	 * implementation of distributed deadlock detection could process them
	 * as a regular backend. In the future, we could consider changing deadlock
	 * detection algorithm to ignore auxiliary procs or prepared transactions and
	 * save some space.
	 */
	totalProcs = maxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;

#if PG_VERSION_NUM >= 120000
	totalProcs += max_wal_senders;
#endif

	return totalProcs;
}


/*
 * InitializeBackendData initialises MyBackendData to the shared memory segment
 * belonging to the current backend.
 *
 * The function is called through CitusHasBeenLoaded when we first detect that
 * the Citus extension is present, and after any subsequent invalidation of
 * pg_dist_partition (see InvalidateMetadataSystemCache()).
 *
 * We only need to initialise MyBackendData once. The only goal here is to make
 * sure that we don't use the backend data from a previous backend with the same
 * pgprocno. Resetting the backend data after a distributed transaction happens
 * on COMMIT/ABORT through transaction callbacks.
 */
void
InitializeBackendData(void)
{
	if (MyBackendData != NULL)
	{
		/*
		 * We already initialized MyBackendData before. We definitely should
		 * not initialise it again, because we might be in the middle of a
		 * distributed transaction.
		 */
		return;
	}

	MyBackendData = &backendManagementShmemData->backends[MyProc->pgprocno];

	Assert(MyBackendData);

	LockBackendSharedMemory(LW_EXCLUSIVE);

	/* zero out the backend data */
	UnSetDistributedTransactionId();

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
		MyBackendData->userId = 0;
		MyBackendData->transactionId.initiatorNodeIdentifier = 0;
		MyBackendData->transactionId.transactionOriginator = false;
		MyBackendData->transactionId.transactionNumber = 0;
		MyBackendData->transactionId.timestamp = 0;

		MyBackendData->citusBackend.initiatorNodeIdentifier = -1;
		MyBackendData->citusBackend.transactionOriginator = false;

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
 * callers is very likely to break the distributed transaction management.
 */
void
AssignDistributedTransactionId(void)
{
	pg_atomic_uint64 *transactionNumberSequence =
		&backendManagementShmemData->nextTransactionNumber;

	uint64 nextTransactionNumber = pg_atomic_fetch_add_u64(transactionNumberSequence, 1);
	int localGroupId = GetLocalGroupId();
	TimestampTz currentTimestamp = GetCurrentTimestamp();
	Oid userId = GetUserId();

	SpinLockAcquire(&MyBackendData->mutex);

	MyBackendData->databaseId = MyDatabaseId;
	MyBackendData->userId = userId;

	MyBackendData->transactionId.initiatorNodeIdentifier = localGroupId;
	MyBackendData->transactionId.transactionOriginator = true;
	MyBackendData->transactionId.transactionNumber = nextTransactionNumber;
	MyBackendData->transactionId.timestamp = currentTimestamp;

	MyBackendData->citusBackend.initiatorNodeIdentifier = localGroupId;
	MyBackendData->citusBackend.transactionOriginator = true;

	SpinLockRelease(&MyBackendData->mutex);
}


/*
 * MarkCitusInitiatedCoordinatorBackend sets that coordinator backend is
 * initiated by Citus.
 */
void
MarkCitusInitiatedCoordinatorBackend(void)
{
	/*
	 * GetLocalGroupId may throw exception which can cause leaving spin lock
	 * unreleased. Calling GetLocalGroupId function before the lock to avoid this.
	 */
	int localGroupId = GetLocalGroupId();

	SpinLockAcquire(&MyBackendData->mutex);

	MyBackendData->citusBackend.initiatorNodeIdentifier = localGroupId;
	MyBackendData->citusBackend.transactionOriginator = true;

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


/*
 * GetMyProcLocalTransactionId() is a wrapper for
 * getting lxid of MyProc.
 */
LocalTransactionId
GetMyProcLocalTransactionId(void)
{
	return MyProc->lxid;
}
