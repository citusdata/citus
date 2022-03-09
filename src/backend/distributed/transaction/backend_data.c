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

#include "distributed/pg_version_constants.h"

#include "miscadmin.h"
#include "unistd.h"

#include "safe_lib.h"

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
#include "distributed/shared_connection_stats.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_manager.h"
#include "nodes/execnodes.h"
#include "postmaster/autovacuum.h" /* to access autovacuum_max_workers */
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "utils/timestamp.h"


#define GET_ACTIVE_TRANSACTION_QUERY "SELECT * FROM get_all_active_transactions();"
#define ACTIVE_TRANSACTION_COLUMN_COUNT 7
#define GLOBAL_PID_NODE_ID_MULTIPLIER 10000000000

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

	/*
	 * Total number of external client backends that are authenticated.
	 *
	 * Note that the counter does not consider any background workers
	 * or such, and also exludes internal connections between nodes.
	 */
	pg_atomic_uint32 externalClientBackendCounter;

	BackendData backends[FLEXIBLE_ARRAY_MEMBER];
} BackendManagementShmemData;


static void StoreAllActiveTransactions(Tuplestorestate *tupleStore, TupleDesc
									   tupleDescriptor);
static bool UserHasPermissionToViewStatsOf(Oid currentUserId, Oid backendOwnedId);
static uint64 CalculateGlobalPID(int32 nodeId, pid_t pid);
static uint64 GenerateGlobalPID(void);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static BackendManagementShmemData *backendManagementShmemData = NULL;
static BackendData *MyBackendData = NULL;


static void BackendManagementShmemInit(void);
static size_t BackendManagementShmemSize(void);


PG_FUNCTION_INFO_V1(assign_distributed_transaction_id);
PG_FUNCTION_INFO_V1(get_current_transaction_id);
PG_FUNCTION_INFO_V1(get_global_active_transactions);
PG_FUNCTION_INFO_V1(get_all_active_transactions);
PG_FUNCTION_INFO_V1(citus_calculate_gpid);
PG_FUNCTION_INFO_V1(citus_backend_gpid);
PG_FUNCTION_INFO_V1(citus_nodeid_for_gpid);
PG_FUNCTION_INFO_V1(citus_pid_for_gpid);


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

	/* prepare data before acquiring spinlock to protect against errors */
	int32 initiatorNodeIdentifier = PG_GETARG_INT32(0);
	uint64 transactionNumber = PG_GETARG_INT64(1);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(2);

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

	MyBackendData->transactionId.initiatorNodeIdentifier = initiatorNodeIdentifier;
	MyBackendData->transactionId.transactionNumber = transactionNumber;
	MyBackendData->transactionId.timestamp = timestamp;
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
	CheckCitusVersion(ERROR);

	TupleDesc tupleDescriptor = NULL;

	Datum values[5];
	bool isNulls[5];


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

	DistributedTransactionId *distributedTransctionId =
		GetCurrentDistributedTransactionId();

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

	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

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
	CheckCitusVersion(ERROR);

	TupleDesc tupleDescriptor = NULL;
	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	List *connectionList = NIL;
	StringInfo queryToSend = makeStringInfo();

	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	appendStringInfo(queryToSend, GET_ACTIVE_TRANSACTION_QUERY);

	/* add active transactions for local node */
	StoreAllActiveTransactions(tupleStore, tupleDescriptor);

	int32 localGroupId = GetLocalGroupId();

	/* open connections in parallel */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = 0;

		if (workerNode->groupId == localGroupId)
		{
			/* we already get these transactions via GetAllActiveTransactions() */
			continue;
		}

		MultiConnection *connection = StartNodeConnection(connectionFlags, nodeName,
														  nodePort);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, queryToSend->data);
		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
		}
	}

	/* receive query results */
	foreach_ptr(connection, connectionList)
	{
		bool raiseInterrupts = true;
		Datum values[ACTIVE_TRANSACTION_COLUMN_COUNT];
		bool isNulls[ACTIVE_TRANSACTION_COLUMN_COUNT];

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			continue;
		}

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			continue;
		}

		int64 rowCount = PQntuples(result);
		int64 colCount = PQnfields(result);

		/* Although it is not expected */
		if (colCount != ACTIVE_TRANSACTION_COLUMN_COUNT)
		{
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "get_all_active_transactions")));
			continue;
		}

		for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			memset(values, 0, sizeof(values));
			memset(isNulls, false, sizeof(isNulls));

			values[0] = ParseIntField(result, rowIndex, 0);
			values[1] = ParseIntField(result, rowIndex, 1);
			values[2] = ParseIntField(result, rowIndex, 2);
			values[3] = ParseBoolField(result, rowIndex, 3);
			values[4] = ParseIntField(result, rowIndex, 4);
			values[5] = ParseTimestampTzField(result, rowIndex, 5);
			values[6] = ParseIntField(result, rowIndex, 6);

			tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
		}

		PQclear(result);
		ForgetResults(connection);
	}

	PG_RETURN_VOID();
}


/*
 * get_all_active_transactions returns all the avaliable information about all
 * the active backends.
 */
Datum
get_all_active_transactions(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	StoreAllActiveTransactions(tupleStore, tupleDescriptor);

	PG_RETURN_VOID();
}


/*
 * StoreAllActiveTransactions gets active transaction from the local node and inserts
 * them into the given tuplestore.
 */
static void
StoreAllActiveTransactions(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	Datum values[ACTIVE_TRANSACTION_COLUMN_COUNT];
	bool isNulls[ACTIVE_TRANSACTION_COLUMN_COUNT];
	bool showAllBackends = superuser();
	const Oid userId = GetUserId();

	if (!showAllBackends && is_member_of_role(userId, ROLE_PG_MONITOR))
	{
		showAllBackends = true;
	}

	/* we're reading all distributed transactions, prevent new backends */
	LockBackendSharedMemory(LW_SHARED);

	for (int backendIndex = 0; backendIndex < TotalProcCount(); ++backendIndex)
	{
		bool showCurrentBackendDetails = showAllBackends;
		BackendData *currentBackend =
			&backendManagementShmemData->backends[backendIndex];
		PGPROC *currentProc = &ProcGlobal->allProcs[backendIndex];

		/* to work on data after releasing g spinlock to protect against errors */
		uint64 transactionNumber = 0;

		SpinLockAcquire(&currentBackend->mutex);

		if (currentProc->pid == 0)
		{
			/* unused PGPROC slot */
			SpinLockRelease(&currentBackend->mutex);
			continue;
		}

		/*
		 * Unless the user has a role that allows seeing all transactions (superuser,
		 * pg_monitor), we only follow pg_stat_statements owner checks.
		 */
		if (!showCurrentBackendDetails &&
			UserHasPermissionToViewStatsOf(userId, currentProc->roleId))
		{
			showCurrentBackendDetails = true;
		}

		Oid databaseId = currentBackend->databaseId;
		int backendPid = ProcGlobal->allProcs[backendIndex].pid;

		/*
		 * We prefer to use worker_query instead of distributedCommandOriginator in
		 * the user facing functions since its more intuitive. Thus,
		 * we negate the result before returning.
		 */
		bool distributedCommandOriginator =
			currentBackend->distributedCommandOriginator;

		transactionNumber = currentBackend->transactionId.transactionNumber;
		TimestampTz transactionIdTimestamp = currentBackend->transactionId.timestamp;

		SpinLockRelease(&currentBackend->mutex);

		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		/*
		 * We imitate pg_stat_activity such that if a user doesn't have enough
		 * privileges, we only show the minimal information including the pid,
		 * global pid and distributedCommandOriginator.
		 *
		 * pid is already can be found in pg_stat_activity for any process, and
		 * the rest doesn't reveal anything critial for under priviledge users
		 * but still could be useful for monitoring purposes of Citus.
		 */
		if (showCurrentBackendDetails)
		{
			bool missingOk = true;
			int initiatorNodeId =
				ExtractNodeIdFromGlobalPID(currentBackend->globalPID, missingOk);

			values[0] = ObjectIdGetDatum(databaseId);
			values[1] = Int32GetDatum(backendPid);
			values[2] = Int32GetDatum(initiatorNodeId);
			values[3] = !distributedCommandOriginator;
			values[4] = UInt64GetDatum(transactionNumber);
			values[5] = TimestampTzGetDatum(transactionIdTimestamp);
			values[6] = UInt64GetDatum(currentBackend->globalPID);
		}
		else
		{
			isNulls[0] = true;
			values[1] = Int32GetDatum(backendPid);
			isNulls[2] = true;
			values[3] = !distributedCommandOriginator;
			isNulls[4] = true;
			isNulls[5] = true;
			values[6] = UInt64GetDatum(currentBackend->globalPID);
		}

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
 * UserHasPermissionToViewStatsOf returns true if currentUserId can
 * see backends of backendOwnedId.
 *
 * We follow the same approach with pg_stat_activity.
 */
static
bool
UserHasPermissionToViewStatsOf(Oid currentUserId, Oid backendOwnedId)
{
	if (has_privs_of_role(currentUserId, backendOwnedId))
	{
		return true;
	}

	if (is_member_of_role(currentUserId,
#if PG_VERSION_NUM >= PG_VERSION_14
						  ROLE_PG_READ_ALL_STATS))
#else
						  DEFAULT_ROLE_READ_ALL_STATS))
#endif
	{
		return true;
	}

	return false;
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

		/* there are no active backends yet, so start with zero */
		pg_atomic_init_u32(&backendManagementShmemData->externalClientBackendCounter, 0);

		/*
		 * We need to init per backend's spinlock before any backend
		 * starts its execution. Note that we initialize TotalProcs (e.g., not
		 * MaxBackends) since some of the blocking processes could be prepared
		 * transactions, which aren't covered by MaxBackends.
		 *
		 * We also initiate initiatorNodeIdentifier to -1, which can never be
		 * used as a node id.
		 */
		int totalProcs = TotalProcCount();
		for (int backendIndex = 0; backendIndex < totalProcs; ++backendIndex)
		{
			BackendData *backendData =
				&backendManagementShmemData->backends[backendIndex];
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

	totalProcs += max_wal_senders;

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
	UnSetGlobalPID();

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

		MyBackendData->cancelledDueToDeadlock = false;
		MyBackendData->transactionId.initiatorNodeIdentifier = 0;
		MyBackendData->transactionId.transactionOriginator = false;
		MyBackendData->transactionId.transactionNumber = 0;
		MyBackendData->transactionId.timestamp = 0;

		SpinLockRelease(&MyBackendData->mutex);
	}
}


/*
 * UnSetGlobalPID resets the global pid for the current backend.
 */
void
UnSetGlobalPID(void)
{
	/* backend does not exist if the extension is not created */
	if (MyBackendData)
	{
		SpinLockAcquire(&MyBackendData->mutex);

		MyBackendData->globalPID = 0;
		MyBackendData->databaseId = 0;
		MyBackendData->userId = 0;
		MyBackendData->distributedCommandOriginator = false;

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
 * This function should only be called on UseCoordinatedTransaction(). Any other
 * callers is very likely to break the distributed transaction management.
 */
void
AssignDistributedTransactionId(void)
{
	pg_atomic_uint64 *transactionNumberSequence =
		&backendManagementShmemData->nextTransactionNumber;

	uint64 nextTransactionNumber = pg_atomic_fetch_add_u64(transactionNumberSequence, 1);
	int32 localGroupId = GetLocalGroupId();
	TimestampTz currentTimestamp = GetCurrentTimestamp();

	SpinLockAcquire(&MyBackendData->mutex);

	MyBackendData->transactionId.initiatorNodeIdentifier = localGroupId;
	MyBackendData->transactionId.transactionOriginator = true;
	MyBackendData->transactionId.transactionNumber = nextTransactionNumber;
	MyBackendData->transactionId.timestamp = currentTimestamp;

	SpinLockRelease(&MyBackendData->mutex);
}


/*
 * AssignGlobalPID assigns a global process id for the current backend.
 * If this is a Citus initiated backend, which means it is distributed part of a distributed
 * query, then this function assigns the global pid extracted from the application name.
 * If not, this function assigns a new generated global pid.
 */
void
AssignGlobalPID(void)
{
	uint64 globalPID = INVALID_CITUS_INTERNAL_BACKEND_GPID;
	bool distributedCommandOriginator = false;

	if (!IsCitusInternalBackend())
	{
		globalPID = GenerateGlobalPID();
		distributedCommandOriginator = true;
	}
	else
	{
		globalPID = ExtractGlobalPID(application_name);
	}

	Oid userId = GetUserId();

	SpinLockAcquire(&MyBackendData->mutex);

	MyBackendData->globalPID = globalPID;
	MyBackendData->distributedCommandOriginator = distributedCommandOriginator;
	MyBackendData->databaseId = MyDatabaseId;
	MyBackendData->userId = userId;

	SpinLockRelease(&MyBackendData->mutex);
}


/*
 * SetBackendDataDistributedCommandOriginator is used to set the distributedCommandOriginator
 * field on MyBackendData.
 */
void
SetBackendDataDistributedCommandOriginator(bool distributedCommandOriginator)
{
	if (!MyBackendData)
	{
		return;
	}
	SpinLockAcquire(&MyBackendData->mutex);
	MyBackendData->distributedCommandOriginator =
		distributedCommandOriginator;
	SpinLockRelease(&MyBackendData->mutex);
}


/*
 * GetGlobalPID returns the global process id of the current backend.
 */
uint64
GetGlobalPID(void)
{
	uint64 globalPID = INVALID_CITUS_INTERNAL_BACKEND_GPID;

	if (MyBackendData)
	{
		SpinLockAcquire(&MyBackendData->mutex);
		globalPID = MyBackendData->globalPID;
		SpinLockRelease(&MyBackendData->mutex);
	}

	return globalPID;
}


/*
 * citus_calculate_gpid calculates the gpid for any given process on any
 * given node.
 */
Datum
citus_calculate_gpid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int32 nodeId = PG_GETARG_INT32(0);
	int32 pid = PG_GETARG_INT32(1);

	PG_RETURN_UINT64(CalculateGlobalPID(nodeId, pid));
}


/*
 * CalculateGlobalPID gets a nodeId and pid, and returns the global pid
 * that can be assigned for a process with the given input.
 */
static uint64
CalculateGlobalPID(int32 nodeId, pid_t pid)
{
	/*
	 * We try to create a human readable global pid that consists of node id and process id.
	 * By multiplying node id with 10^10 and adding pid we generate a number where the smallest
	 * 10 digit represent the pid and the remaining digits are the node id.
	 *
	 * Both node id and pid are 32 bit. We use 10^10 to fit all possible pids. Some very large
	 * node ids might cause overflow. But even for the applications that scale around 50 nodes every
	 * day it'd take about 100K years. So we are not worried.
	 */
	return (((uint64) nodeId) * GLOBAL_PID_NODE_ID_MULTIPLIER) + pid;
}


/*
 * GenerateGlobalPID generates the global process id for the current backend.
 * See CalculateGlobalPID for the details.
 */
static uint64
GenerateGlobalPID(void)
{
	return CalculateGlobalPID(GetLocalNodeId(), getpid());
}


/*
 * citus_backend_gpid similar to pg_backend_pid, but returns Citus
 * assigned gpid.
 */
Datum
citus_backend_gpid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_RETURN_UINT64(GetGlobalPID());
}


/*
 * citus_nodeid_for_gpid returns node id for the global process with given global pid
 */
Datum
citus_nodeid_for_gpid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 globalPID = PG_GETARG_INT64(0);

	bool missingOk = false;
	PG_RETURN_INT32(ExtractNodeIdFromGlobalPID(globalPID, missingOk));
}


/*
 * citus_pid_for_gpid returns process id for the global process with given global pid
 */
Datum
citus_pid_for_gpid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 globalPID = PG_GETARG_INT64(0);

	PG_RETURN_INT32(ExtractProcessIdFromGlobalPID(globalPID));
}


/*
 * ExtractGlobalPID extracts the global process id from the application name and returns it
 * if the application name is not compatible with Citus' application names returns 0.
 */
uint64
ExtractGlobalPID(char *applicationName)
{
	/* does application name exist */
	if (!applicationName)
	{
		return INVALID_CITUS_INTERNAL_BACKEND_GPID;
	}

	/* we create our own copy of application name incase the original changes */
	char *applicationNameCopy = pstrdup(applicationName);

	uint64 prefixLength = strlen(CITUS_APPLICATION_NAME_PREFIX);

	/* does application name start with Citus's application name prefix */
	if (strncmp(applicationNameCopy, CITUS_APPLICATION_NAME_PREFIX, prefixLength) != 0)
	{
		return INVALID_CITUS_INTERNAL_BACKEND_GPID;
	}

	/* are the remaining characters of the application name numbers */
	uint64 numberOfRemainingChars = strlen(applicationNameCopy) - prefixLength;
	if (numberOfRemainingChars <= 0 ||
		!strisdigit_s(applicationNameCopy + prefixLength, numberOfRemainingChars))
	{
		return INVALID_CITUS_INTERNAL_BACKEND_GPID;
	}

	char *globalPIDString = &applicationNameCopy[prefixLength];
	uint64 globalPID = strtoul(globalPIDString, NULL, 10);

	return globalPID;
}


/*
 * ExtractNodeIdFromGlobalPID extracts the node id from the global pid.
 * Global pid is constructed by multiplying node id with GLOBAL_PID_NODE_ID_MULTIPLIER
 * and adding process id. So integer division of global pid by GLOBAL_PID_NODE_ID_MULTIPLIER
 * gives us the node id.
 */
int
ExtractNodeIdFromGlobalPID(uint64 globalPID, bool missingOk)
{
	int nodeId = (int) (globalPID / GLOBAL_PID_NODE_ID_MULTIPLIER);

	if (!missingOk &&
		nodeId == GLOBAL_PID_NODE_ID_FOR_NODES_NOT_IN_METADATA)
	{
		ereport(ERROR, (errmsg("originator node of the query with the global pid "
							   "%lu is not in Citus' metadata", globalPID),
						errhint("connect to the node directly run pg_cancel_backend(pid) "
								"or pg_terminate_backend(pid)")));
	}

	return nodeId;
}


/*
 * ExtractProcessIdFromGlobalPID extracts the process id from the global pid.
 * Global pid is constructed by multiplying node id with GLOBAL_PID_NODE_ID_MULTIPLIER
 * and adding process id. So global pid mod GLOBAL_PID_NODE_ID_MULTIPLIER gives us the
 * process id.
 */
int
ExtractProcessIdFromGlobalPID(uint64 globalPID)
{
	return (int) (globalPID % GLOBAL_PID_NODE_ID_MULTIPLIER);
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
	int pgprocno = proc->pgprocno;

	if (proc->lockGroupLeader != NULL)
	{
		pgprocno = proc->lockGroupLeader->pgprocno;
	}

	BackendData *backendData = &backendManagementShmemData->backends[pgprocno];

	SpinLockAcquire(&backendData->mutex);

	*result = *backendData;

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
 * We keep some session level state to keep track of if we were cancelled
 * because of a distributed deadlock. When clearState is true, this function
 * also resets that state. So after calling this function with clearState true,
 * a second would always return false.
 */
bool
MyBackendGotCancelledDueToDeadlock(bool clearState)
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
	if (clearState)
	{
		MyBackendData->cancelledDueToDeadlock = false;
	}

	SpinLockRelease(&MyBackendData->mutex);

	return cancelledDueToDeadlock;
}


/*
 * MyBackendIsInDisributedTransaction returns true if MyBackendData
 * is in a distributed transaction.
 */
bool
MyBackendIsInDisributedTransaction(void)
{
	/* backend might not have used citus yet and thus not initialized backend data */
	if (!MyBackendData)
	{
		return false;
	}

	return IsInDistributedTransaction(MyBackendData);
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

	/* build list of starting procs */
	for (int curBackend = 0; curBackend < MaxBackends; curBackend++)
	{
		PGPROC *currentProc = &ProcGlobal->allProcs[curBackend];
		BackendData currentBackendData;

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

		uint64 *transactionNumber = (uint64 *) palloc0(sizeof(uint64));
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


/*
 * GetExternalClientBackendCount returns externalClientBackendCounter in
 * the shared memory.
 */
int
GetExternalClientBackendCount(void)
{
	uint32 activeBackendCount =
		pg_atomic_read_u32(&backendManagementShmemData->externalClientBackendCounter);

	return activeBackendCount;
}


/*
 * IncrementExternalClientBackendCounter increments externalClientBackendCounter in
 * the shared memory by one.
 */
uint32
IncrementExternalClientBackendCounter(void)
{
	return pg_atomic_add_fetch_u32(
		&backendManagementShmemData->externalClientBackendCounter, 1);
}


/*
 * DecrementExternalClientBackendCounter decrements externalClientBackendCounter in
 * the shared memory by one.
 */
void
DecrementExternalClientBackendCounter(void)
{
	pg_atomic_sub_fetch_u32(&backendManagementShmemData->externalClientBackendCounter, 1);
}
