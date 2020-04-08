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
#include "pgstat.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "distributed/cancel_utils.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/time_constants.h"
#include "distributed/tuplestore.h"
#include "utils/builtins.h"
#include "utils/hashutils.h"
#include "utils/hsearch.h"
#include "storage/ipc.h"


#define REMOTE_CONNECTION_STATS_COLUMNS 4

#define ADJUST_POOLSIZE_AUTOMATICALLY 0
#define DISABLE_CONNECTION_THROTTLING -1

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
	 * In some cases, Citus allows opening connections to hosts where
	 * there is no notion of "WorkerNode", such as task-tracker daemon.
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


/* the following two structs are used for accessing shared memory */
static HTAB *SharedConnStatsHash = NULL;
static ConnectionStatsSharedData *ConnectionStatsSharedState = NULL;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


/* local function declarations */
static void StoreAllConnections(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor);
static void LockConnectionSharedMemory(LWLockMode lockMode);
static void UnLockConnectionSharedMemory(void);
static void SharedConnectionStatsShmemInit(void);
static size_t SharedConnectionStatsShmemSize(void);
static int SharedConnectionHashCompare(const void *a, const void *b, Size keysize);
static uint32 SharedConnectionHashHash(const void *key, Size keysize);


PG_FUNCTION_INFO_V1(citus_remote_connection_stats);


/*
 * citus_remote_connection_stats returns all the avaliable information about all
 * the remote connections (a.k.a., connections to remote nodes).
 */
Datum
citus_remote_connection_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;

	CheckCitusVersion(ERROR);
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	StoreAllConnections(tupleStore, tupleDescriptor);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}


/*
 * StoreAllConnections gets connections established from the current node
 * and inserts them into the given tuplestore.
 *
 * We don't need to enforce any access privileges as the number of backends
 * on any node is already visible on pg_stat_activity to all users.
 */
static void
StoreAllConnections(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
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
 * RemoveInactiveNodesFromSharedConnections goes over the SharedConnStatsHash
 * and removes the inactive entries.
 */
void
RemoveInactiveNodesFromSharedConnections(void)
{
	/* we're modifying connections, prevent any changes */
	LockConnectionSharedMemory(LW_EXCLUSIVE);

	HASH_SEQ_STATUS status;
	SharedConnStatsHashEntry *connectionEntry = NULL;

	/*
	 * In the first iteration, try to remove worker nodes that doesn't have any active
	 * conections and the node does not exits in the metadata anymore.
	 */
	hash_seq_init(&status, SharedConnStatsHash);
	while ((connectionEntry = (SharedConnStatsHashEntry *) hash_seq_search(&status)) != 0)
	{
		SharedConnStatsHashKey connectionKey = connectionEntry->key;
		WorkerNode *workerNode =
			FindWorkerNode(connectionKey.hostname, connectionKey.port);

		if (connectionEntry->connectionCount == 0 &&
			(workerNode == NULL || !workerNode->isActive))
		{
			hash_search(SharedConnStatsHash, &connectionKey, HASH_REMOVE, NULL);
		}
	}

	int entryCount = hash_get_num_entries(SharedConnStatsHash);
	if (entryCount + 1 < MaxWorkerNodesTracked)
	{
		/* we're good, we have at least one more space for a new worker */
		UnLockConnectionSharedMemory();

		return;
	}

	/*
	 * We aimed to remove nodes that don't have any open connections. If we
	 * failed to find one, we have to be more aggressive and remove at least
	 * one of the inactive ones.
	 */
	hash_seq_init(&status, SharedConnStatsHash);
	while ((connectionEntry = (SharedConnStatsHashEntry *) hash_seq_search(&status)) != 0)
	{
		SharedConnStatsHashKey connectionKey = connectionEntry->key;
		WorkerNode *workerNode =
			FindWorkerNode(connectionKey.hostname, connectionKey.port);

		if (workerNode == NULL || !workerNode->isActive)
		{
			hash_search(SharedConnStatsHash, &connectionKey, HASH_REMOVE, NULL);

			hash_seq_term(&status);

			break;
		}
	}


	UnLockConnectionSharedMemory();
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
		return MaxConnections;
	}

	return MaxSharedPoolSize;
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

	connKey.port = port;
	connKey.databaseOid = MyDatabaseId;

	LockConnectionSharedMemory(LW_EXCLUSIVE);

	/*
	 * As the hash map is  allocated in shared memory, it doesn't rely on palloc for
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

	if (GetMaxSharedPoolSize() == DISABLE_CONNECTION_THROTTLING)
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
	 * As the hash map is  allocated in shared memory, it doesn't rely on palloc for
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
 * for the given hostname and port.
 */
void
DecrementSharedConnectionCounter(const char *hostname, int port)
{
	SharedConnStatsHashKey connKey;

	if (GetMaxSharedPoolSize() == DISABLE_CONNECTION_THROTTLING)
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

		return;
	}

	/* we should never go below 0 */
	Assert(connectionEntry->connectionCount > 0);

	connectionEntry->connectionCount -= 1;

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


void
WakeupWaiterBackendsForSharedConnection(void)
{
	ConditionVariableBroadcast(&ConnectionStatsSharedState->waitersConditionVariable);
}


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
	/* allocate shared memory */
	if (!IsUnderPostmaster)
	{
		RequestAddinShmemSpace(SharedConnectionStatsShmemSize());
	}

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = SharedConnectionStatsShmemInit;
}


/*
 * SharedConnectionStatsShmemSize returns the size that should be allocated
 * on the shared memory for shared connection stats.
 */
static size_t
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
static void
SharedConnectionStatsShmemInit(void)
{
	bool alreadyInitialized = false;
	HASHCTL info;

	/* create (nodeId,database) -> [counter] */
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

	/*  allocate hash table */
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
