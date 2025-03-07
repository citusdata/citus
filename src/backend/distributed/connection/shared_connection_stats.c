/*-------------------------------------------------------------------------
 *
 * shared_connection_stats.c
 *   Keeps track of the number of connections to remote nodes across
 *   backends. The primary goal is to prevent excessive number of
 *   connections (typically > max_connections) to any worker node.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "storage/ipc.h"
#include "utils/builtins.h"

#include "pg_version_constants.h"

#include "distributed/backend_data.h"
#include "distributed/cancel_utils.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/locally_reserved_shared_connections.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/time_constants.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_manager.h"


#define REMOTE_CONNECTION_STATS_COLUMNS 4


/*
 * The data structure used to store data in shared memory. This data structure is only
 * used for storing the lock. The actual statistics about the connections are stored
 * in the hashmap, which is allocated separately, as Postgres provides different APIs
 * for allocating hashmaps in the shared memory.
 */
typedef struct ConnectionStatsSharedData
{
	int sharedConnectionHashTrancheId;
	char *sharedConnectionHashTrancheName;

	LWLock sharedConnectionHashLock;
	ConditionVariable waitersConditionVariable;
} ConnectionStatsSharedData;


typedef struct SharedConnStatsHashKey
{
	/*
	 * We keep the entries in the shared memory even after master_update_node()
	 * as there might be some cached connections to the old node.
	 * That's why, we prefer to use "hostname/port" over nodeId.
	 */
	char hostname[MAX_NODE_LENGTH];
	int32 port;

	/*
	 * Given that citus.shared_max_pool_size can be defined per database, we
	 * should keep track of shared connections per database.
	 */
	Oid databaseOid;
} SharedConnStatsHashKey;

/* hash entry for per worker stats */
typedef struct SharedConnStatsHashEntry
{
	SharedConnStatsHashKey key;

	int connectionCount;
} SharedConnStatsHashEntry;


/*
 * Controlled via a GUC, never access directly, use GetMaxSharedPoolSize().
 *  "0" means adjust MaxSharedPoolSize automatically by using MaxConnections.
 * "-1" means do not apply connection throttling
 * Anything else means use that number
 */
int MaxSharedPoolSize = 0;

/*
 * Controlled via a GUC, never access directly, use GetLocalSharedPoolSize().
 *  "0" means adjust LocalSharedPoolSize automatically by using MaxConnections.
 * "-1" means do not use any remote connections for local tasks
 * Anything else means use that number
 */
int LocalSharedPoolSize = 0;

/* number of connections reserved for Citus */
int MaxClientConnections = ALLOW_ALL_EXTERNAL_CONNECTIONS;


/* the following two structs are used for accessing shared memory */
static HTAB *SharedConnStatsHash = NULL;
static ConnectionStatsSharedData *ConnectionStatsSharedState = NULL;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


/* local function declarations */
static void StoreAllRemoteConnectionStats(Tuplestorestate *tupleStore, TupleDesc
										  tupleDescriptor);
static void LockConnectionSharedMemory(LWLockMode lockMode);
static void UnLockConnectionSharedMemory(void);
static bool ShouldWaitForConnection(int currentConnectionCount);
static uint32 SharedConnectionHashHash(const void *key, Size keysize);
static int SharedConnectionHashCompare(const void *a, const void *b, Size keysize);


PG_FUNCTION_INFO_V1(citus_remote_connection_stats);

/*
 * citus_remote_connection_stats returns all the avaliable information about all
 * the remote connections (a.k.a., connections to remote nodes).
 */
Datum
citus_remote_connection_stats(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	StoreAllRemoteConnectionStats(tupleStore, tupleDescriptor);

	PG_RETURN_VOID();
}


/*
 * StoreAllRemoteConnectionStats gets connections established from the current node
 * and inserts them into the given tuplestore.
 *
 * We don't need to enforce any access privileges as the number of backends
 * on any node is already visible on pg_stat_activity to all users.
 */
static void
StoreAllRemoteConnectionStats(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	Datum values[REMOTE_CONNECTION_STATS_COLUMNS];
	bool isNulls[REMOTE_CONNECTION_STATS_COLUMNS];

	/* we're reading all shared connections, prevent any changes */
	LockConnectionSharedMemory(LW_SHARED);

	HASH_SEQ_STATUS status;
	SharedConnStatsHashEntry *connectionEntry = NULL;

	hash_seq_init(&status, SharedConnStatsHash);
	while ((connectionEntry = (SharedConnStatsHashEntry *) hash_seq_search(&status)) != 0)
	{
		/* get ready for the next tuple */
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		char *databaseName = get_database_name(connectionEntry->key.databaseOid);
		if (databaseName == NULL)
		{
			/* database might have been dropped */
			continue;
		}

		values[0] = PointerGetDatum(cstring_to_text(connectionEntry->key.hostname));
		values[1] = Int32GetDatum(connectionEntry->key.port);
		values[2] = PointerGetDatum(cstring_to_text(databaseName));
		values[3] = Int32GetDatum(connectionEntry->connectionCount);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	UnLockConnectionSharedMemory();
}


/*
 * GetMaxClientConnections returns the value of citus.max_client_connections,
 * or max_connections when it is -1 or when connecting as superuser.
 *
 * The latter is done because citus.max_client_connections does not apply to
 * superuser.
 */
int
GetMaxClientConnections(void)
{
	if (MaxClientConnections == ALLOW_ALL_EXTERNAL_CONNECTIONS || superuser())
	{
		return MaxConnections;
	}

	return MaxClientConnections;
}


/*
 * GetMaxSharedPoolSize is a wrapper around MaxSharedPoolSize which is controlled
 * via a GUC.
 *  "0" means adjust MaxSharedPoolSize automatically by using MaxConnections
 * "-1" means do not apply connection throttling
 * Anything else means use that number
 */
int
GetMaxSharedPoolSize(void)
{
	if (MaxSharedPoolSize == ADJUST_POOLSIZE_AUTOMATICALLY)
	{
		return GetMaxClientConnections();
	}

	return MaxSharedPoolSize;
}


/*
 * GetLocalSharedPoolSize is a wrapper around LocalSharedPoolSize which is
 * controlled via a GUC.
 *  "0" means adjust MaxSharedPoolSize automatically by using MaxConnections
 * "-1" means do not use any remote connections for local tasks
 * Anything else means use that number
 */
int
GetLocalSharedPoolSize(void)
{
	if (LocalSharedPoolSize == ADJUST_POOLSIZE_AUTOMATICALLY)
	{
		return GetMaxClientConnections() * 0.5;
	}

	return LocalSharedPoolSize;
}


/*
 * WaitLoopForSharedConnection tries to increment the shared connection
 * counter for the given hostname/port and the current database in
 * SharedConnStatsHash.
 *
 * The function implements a retry mechanism via a condition variable.
 */
void
WaitLoopForSharedConnection(const char *hostname, int port)
{
	while (!TryToIncrementSharedConnectionCounter(hostname, port))
	{
		CHECK_FOR_INTERRUPTS();

		WaitForSharedConnection();
	}

	ConditionVariableCancelSleep();
}


/*
 * TryToIncrementSharedConnectionCounter tries to increment the shared
 * connection counter for the given nodeId and the current database in
 * SharedConnStatsHash.
 *
 * If the function returns true, the caller is allowed (and expected)
 * to establish a new connection to the given node. Else, the caller
 * is not allowed to establish a new connection.
 */
bool
TryToIncrementSharedConnectionCounter(const char *hostname, int port)
{
	if (GetMaxSharedPoolSize() == DISABLE_CONNECTION_THROTTLING)
	{
		/* connection throttling disabled */
		return true;
	}

	bool counterIncremented = false;
	SharedConnStatsHashKey connKey;

	strlcpy(connKey.hostname, hostname, MAX_NODE_LENGTH);
	if (strlen(hostname) > MAX_NODE_LENGTH)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("hostname exceeds the maximum length of %d",
							   MAX_NODE_LENGTH)));
	}

	/*
	 * The local session might already have some reserved connections to the given
	 * node. In that case, we don't need to go through the shared memory.
	 */
	Oid userId = GetUserId();
	if (CanUseReservedConnection(hostname, port, userId, MyDatabaseId))
	{
		MarkReservedConnectionUsed(hostname, port, userId, MyDatabaseId);

		return true;
	}

	connKey.port = port;
	connKey.databaseOid = MyDatabaseId;

	/*
	 * Handle adaptive connection management for the local node slightly different
	 * as local node can failover to local execution.
	 */
	bool connectionToLocalNode = false;
	int activeBackendCount = 0;
	WorkerNode *workerNode = FindWorkerNode(hostname, port);
	if (workerNode)
	{
		connectionToLocalNode = (workerNode->groupId == GetLocalGroupId());
		if (connectionToLocalNode &&
			GetLocalSharedPoolSize() == DISABLE_REMOTE_CONNECTIONS_FOR_LOCAL_QUERIES)
		{
			/*
			 * This early return is required as LocalNodeParallelExecutionFactor
			 * is ignored for the first connection below. This check makes the
			 * user experience is more accurate and also makes it easy for
			 * having regression tests which emulates the local node adaptive
			 * connection management.
			 */
			return false;
		}

		activeBackendCount = GetExternalClientBackendCount();
	}

	LockConnectionSharedMemory(LW_EXCLUSIVE);

	/*
	 * As the hash map is allocated in shared memory, it doesn't rely on palloc for
	 * memory allocation, so we could get NULL via HASH_ENTER_NULL when there is no
	 * space in the shared memory. That's why we prefer continuing the execution
	 * instead of throwing an error.
	 */
	bool entryFound = false;
	SharedConnStatsHashEntry *connectionEntry =
		hash_search(SharedConnStatsHash, &connKey, HASH_ENTER_NULL, &entryFound);

	/*
	 * It is possible to throw an error at this point, but that doesn't help us in anyway.
	 * Instead, we try our best, let the connection establishment continue by-passing the
	 * connection throttling.
	 */
	if (!connectionEntry)
	{
		UnLockConnectionSharedMemory();
		return true;
	}

	if (!entryFound)
	{
		/* we successfully allocated the entry for the first time, so initialize it */
		connectionEntry->connectionCount = 1;

		counterIncremented = true;
	}
	else if (connectionToLocalNode)
	{
		/*
		 * For local nodes, solely relying on citus.max_shared_pool_size or
		 * max_connections might not be sufficient. The former gives us
		 * a preview of the future (e.g., we let the new connections to establish,
		 * but they are not established yet). The latter gives us the close to
		 * precise view of the past (e.g., the active number of client backends).
		 *
		 * Overall, we want to limit both of the metrics. The former limit typically
		 * kicks in under regular loads, where the load of the database increases in
		 * a reasonable pace. The latter limit typically kicks in when the database
		 * is issued lots of concurrent sessions at the same time, such as benchmarks.
		 */
		if (activeBackendCount + 1 > GetLocalSharedPoolSize())
		{
			counterIncremented = false;
		}
		else if (connectionEntry->connectionCount + 1 > GetLocalSharedPoolSize())
		{
			counterIncremented = false;
		}
		else
		{
			connectionEntry->connectionCount++;
			counterIncremented = true;
		}
	}
	else if (connectionEntry->connectionCount + 1 > GetMaxSharedPoolSize())
	{
		/* there is no space left for this connection */
		counterIncremented = false;
	}
	else
	{
		connectionEntry->connectionCount++;
		counterIncremented = true;
	}

	UnLockConnectionSharedMemory();

	return counterIncremented;
}


/*
 * IncrementSharedConnectionCounter increments the shared counter
 * for the given hostname and port.
 */
void
IncrementSharedConnectionCounter(const char *hostname, int port)
{
	SharedConnStatsHashKey connKey;

	if (MaxSharedPoolSize == DISABLE_CONNECTION_THROTTLING)
	{
		/* connection throttling disabled */
		return;
	}

	strlcpy(connKey.hostname, hostname, MAX_NODE_LENGTH);
	if (strlen(hostname) > MAX_NODE_LENGTH)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("hostname exceeds the maximum length of %d",
							   MAX_NODE_LENGTH)));
	}

	connKey.port = port;
	connKey.databaseOid = MyDatabaseId;

	LockConnectionSharedMemory(LW_EXCLUSIVE);

	/*
	 * As the hash map is allocated in shared memory, it doesn't rely on palloc for
	 * memory allocation, so we could get NULL via HASH_ENTER_NULL. That's why we prefer
	 * continuing the execution instead of throwing an error.
	 */
	bool entryFound = false;
	SharedConnStatsHashEntry *connectionEntry =
		hash_search(SharedConnStatsHash, &connKey, HASH_ENTER_NULL, &entryFound);

	/*
	 * It is possible to throw an error at this point, but that doesn't help us in anyway.
	 * Instead, we try our best, let the connection establishment continue by-passing the
	 * connection throttling.
	 */
	if (!connectionEntry)
	{
		UnLockConnectionSharedMemory();

		ereport(DEBUG4, (errmsg("No entry found for node %s:%d while incrementing "
								"connection counter", hostname, port)));

		return;
	}

	if (!entryFound)
	{
		/* we successfully allocated the entry for the first time, so initialize it */
		connectionEntry->connectionCount = 0;
	}

	connectionEntry->connectionCount += 1;

	UnLockConnectionSharedMemory();
}


/*
 * DecrementSharedConnectionCounter decrements the shared counter
 * for the given hostname and port for the given count.
 */
void
DecrementSharedConnectionCounter(const char *hostname, int port)
{
	SharedConnStatsHashKey connKey;

	/*
	 * Do not call GetMaxSharedPoolSize() here, since it may read from
	 * the catalog and we may be in the process exit handler.
	 */
	if (MaxSharedPoolSize == DISABLE_CONNECTION_THROTTLING)
	{
		/* connection throttling disabled */
		return;
	}

	strlcpy(connKey.hostname, hostname, MAX_NODE_LENGTH);
	if (strlen(hostname) > MAX_NODE_LENGTH)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("hostname exceeds the maximum length of %d",
							   MAX_NODE_LENGTH)));
	}

	connKey.port = port;
	connKey.databaseOid = MyDatabaseId;

	LockConnectionSharedMemory(LW_EXCLUSIVE);

	bool entryFound = false;
	SharedConnStatsHashEntry *connectionEntry =
		hash_search(SharedConnStatsHash, &connKey, HASH_FIND, &entryFound);

	/* this worker node is removed or updated, no need to care */
	if (!entryFound)
	{
		UnLockConnectionSharedMemory();

		/* wake up any waiters in case any backend is waiting for this node */
		WakeupWaiterBackendsForSharedConnection();

		ereport(DEBUG4, (errmsg("No entry found for node %s:%d while decrementing "
								"connection counter", hostname, port)));

		return;
	}

	/* we should never go below 0 */
	Assert(connectionEntry->connectionCount > 0);

	connectionEntry->connectionCount -= 1;

	if (connectionEntry->connectionCount == 0)
	{
		/*
		 * We don't have to remove at this point as the node might be still active
		 * and will have new connections open to it. Still, this seems like a convenient
		 * place to remove the entry, as connectionCount == 0 implies that the server is
		 * not busy, and given the default value of MaxCachedConnectionsPerWorker = 1,
		 * we're unlikely to trigger this often.
		 */
		hash_search(SharedConnStatsHash, &connKey, HASH_REMOVE, &entryFound);
	}

	UnLockConnectionSharedMemory();

	WakeupWaiterBackendsForSharedConnection();
}


/*
 * LockConnectionSharedMemory is a utility function that should be used when
 * accessing to the SharedConnStatsHash, which is in the shared memory.
 */
static void
LockConnectionSharedMemory(LWLockMode lockMode)
{
	LWLockAcquire(&ConnectionStatsSharedState->sharedConnectionHashLock, lockMode);
}


/*
 * UnLockConnectionSharedMemory is a utility function that should be used after
 * LockConnectionSharedMemory().
 */
static void
UnLockConnectionSharedMemory(void)
{
	LWLockRelease(&ConnectionStatsSharedState->sharedConnectionHashLock);
}


/*
 * WakeupWaiterBackendsForSharedConnection is a wrapper around the condition variable
 * broadcast operation.
 *
 * We use a single condition variable, for all worker nodes, to implement the connection
 * throttling mechanism. Combination of all the backends are allowed to establish
 * MaxSharedPoolSize number of connections per worker node. If a backend requires a
 * non-optional connection (see WAIT_FOR_CONNECTION for details), it is not allowed
 * to establish it immediately if the total connections are equal to MaxSharedPoolSize.
 * Instead, the backend waits on the condition variable. When any other backend
 * terminates an existing connection to any remote node, this function is called.
 * The main goal is to trigger all waiting backends to try getting a connection slot
 * in MaxSharedPoolSize. The ones which can get connection slot are allowed to continue
 * with the connection establishments. Others should wait another backend to call
 * this function.
 */
void
WakeupWaiterBackendsForSharedConnection(void)
{
	ConditionVariableBroadcast(&ConnectionStatsSharedState->waitersConditionVariable);
}


/*
 * WaitForSharedConnection is a wrapper around the condition variable sleep operation.
 *
 * For the details of the use of the condition variable, see
 * WakeupWaiterBackendsForSharedConnection().
 */
void
WaitForSharedConnection(void)
{
	ConditionVariableSleep(&ConnectionStatsSharedState->waitersConditionVariable,
						   PG_WAIT_EXTENSION);
}


/*
 * InitializeSharedConnectionStats requests the necessary shared memory
 * from Postgres and sets up the shared memory startup hook.
 */
void
InitializeSharedConnectionStats(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = SharedConnectionStatsShmemInit;
}


/*
 * SharedConnectionStatsShmemSize returns the size that should be allocated
 * on the shared memory for shared connection stats.
 */
size_t
SharedConnectionStatsShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(ConnectionStatsSharedData));

	Size hashSize = hash_estimate_size(MaxWorkerNodesTracked,
									   sizeof(SharedConnStatsHashEntry));

	size = add_size(size, hashSize);

	return size;
}


/*
 * SharedConnectionStatsShmemInit initializes the shared memory used
 * for keeping track of connection stats across backends.
 */
void
SharedConnectionStatsShmemInit(void)
{
	bool alreadyInitialized = false;
	HASHCTL info;

	/* create (hostname, port, database) -> [counter] */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(SharedConnStatsHashKey);
	info.entrysize = sizeof(SharedConnStatsHashEntry);
	info.hash = SharedConnectionHashHash;
	info.match = SharedConnectionHashCompare;
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	/*
	 * Currently the lock isn't required because allocation only happens at
	 * startup in postmaster, but it doesn't hurt, and makes things more
	 * consistent with other extensions.
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ConnectionStatsSharedState =
		(ConnectionStatsSharedData *) ShmemInitStruct(
			"Shared Connection Stats Data",
			sizeof(ConnectionStatsSharedData),
			&alreadyInitialized);

	if (!alreadyInitialized)
	{
		ConnectionStatsSharedState->sharedConnectionHashTrancheId = LWLockNewTrancheId();
		ConnectionStatsSharedState->sharedConnectionHashTrancheName =
			"Shared Connection Tracking Hash Tranche";
		LWLockRegisterTranche(ConnectionStatsSharedState->sharedConnectionHashTrancheId,
							  ConnectionStatsSharedState->sharedConnectionHashTrancheName);

		LWLockInitialize(&ConnectionStatsSharedState->sharedConnectionHashLock,
						 ConnectionStatsSharedState->sharedConnectionHashTrancheId);

		ConditionVariableInit(&ConnectionStatsSharedState->waitersConditionVariable);
	}

	/* allocate hash table */
	SharedConnStatsHash =
		ShmemInitHash("Shared Conn. Stats Hash", MaxWorkerNodesTracked,
					  MaxWorkerNodesTracked, &info, hashFlags);

	LWLockRelease(AddinShmemInitLock);

	Assert(SharedConnStatsHash != NULL);
	Assert(ConnectionStatsSharedState->sharedConnectionHashTrancheId != 0);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * AdaptiveConnectionManagementFlag returns the appropriate connection flag,
 * regarding the adaptive connection management, based on the given
 * activeConnectionCount to remote nodes.
 *
 * This function should only be called if the code-path is capable of handling
 * optional connections.
 */
int
AdaptiveConnectionManagementFlag(bool connectToLocalNode, int activeConnectionCount)
{
	if (UseConnectionPerPlacement())
	{
		/*
		 * User wants one connection per placement, so no throttling is desired
		 * and we do not set any flags.
		 *
		 * The primary reason for this is that allowing multiple backends to use
		 * connection per placement could lead to unresolved self deadlocks. In other
		 * words, each backend may stuck waiting for other backends to get a slot
		 * in the shared connection counters.
		 */
		return 0;
	}
	else if (connectToLocalNode)
	{
		/*
		 * Connection to local node is always optional because the executor is capable
		 * of falling back to local execution.
		 */
		return OPTIONAL_CONNECTION;
	}
	else if (ShouldWaitForConnection(activeConnectionCount))
	{
		/*
		 * We need this connection to finish the execution. If it is not
		 * available based on the current number of connections to the worker
		 * then wait for it.
		 */
		return WAIT_FOR_CONNECTION;
	}
	else
	{
		/*
		 * The execution can be finished the execution with a single connection,
		 * remaining are optional. If the execution can get more connections,
		 * it can increase the parallelism.
		 */
		return OPTIONAL_CONNECTION;
	}
}


/*
 * ShouldWaitForConnection returns true if the workerPool should wait to
 * get the next connection until one slot is empty within
 * citus.max_shared_pool_size on the worker. Note that, if there is an
 * empty slot, the connection will not wait anyway.
 */
static bool
ShouldWaitForConnection(int currentConnectionCount)
{
	if (currentConnectionCount == 0)
	{
		/*
		 * We definitely need at least 1 connection to finish the execution.
		 * All single shard queries hit here with the default settings.
		 */
		return true;
	}

	if (currentConnectionCount < MaxCachedConnectionsPerWorker)
	{
		/*
		 * Until this session caches MaxCachedConnectionsPerWorker connections,
		 * this might lead some optional connections to be considered as non-optional
		 * when MaxCachedConnectionsPerWorker > 1.
		 *
		 * However, once the session caches MaxCachedConnectionsPerWorker (which is
		 * the second transaction executed in the session), Citus would utilize the
		 * cached connections as much as possible.
		 */
		return true;
	}

	return false;
}


static uint32
SharedConnectionHashHash(const void *key, Size keysize)
{
	SharedConnStatsHashKey *entry = (SharedConnStatsHashKey *) key;

	uint32 hash = string_hash(entry->hostname, NAMEDATALEN);
	hash = hash_combine(hash, hash_uint32(entry->port));
	hash = hash_combine(hash, hash_uint32(entry->databaseOid));

	return hash;
}


static int
SharedConnectionHashCompare(const void *a, const void *b, Size keysize)
{
	SharedConnStatsHashKey *ca = (SharedConnStatsHashKey *) a;
	SharedConnStatsHashKey *cb = (SharedConnStatsHashKey *) b;

	if (strncmp(ca->hostname, cb->hostname, MAX_NODE_LENGTH) != 0 ||
		ca->port != cb->port ||
		ca->databaseOid != cb->databaseOid)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
