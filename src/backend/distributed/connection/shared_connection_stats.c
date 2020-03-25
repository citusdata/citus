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
#include "distributed/metadata_cache.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/tuplestore.h"
#include "utils/builtins.h"
#include "utils/hashutils.h"
#include "utils/hsearch.h"
#include "storage/ipc.h"


#define REMOTE_CONNECTION_STATS_COLUMNS 4


/*
 * The data structure used to store data in shared memory. This data structure only
 * used for storing the lock. The actual statistics about the connections are stored
 * in the hashmap, which is allocated separately, as Postgres provides different APIs
 * for allocating hashmaps in the shared memory.
 */
typedef struct ConnectionStatsSharedData
{
	int sharedConnectionHashTrancheId;
	char *sharedConnectionHashTrancheName;
	LWLock sharedConnectionHashLock;
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
 * Controlled via a GUC.
 *
 * By default, Citus tracks 1024 worker nodes, which is already
 * very unlikely number of worker nodes. Given that the shared
 * memory required per worker is pretty small (~120 Bytes), we think it
 * is a good default that wouldn't hurt any users in any dimension.
 */
int MaxTrackedWorkerNodes = 1024;

static int MaxSharedPoolSize = 100;

/* the following two structs used for accessing shared memory */
static HTAB *SharedConnStatsHash = NULL;
static ConnectionStatsSharedData *ConnectionStatsSharedState = NULL;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


/* local function declarations */
static void StoreAllConnections(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor);
static void UnLockConnectionSharedMemory(void);
static void LockConnectionSharedMemory(LWLockMode lockMode);
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

	/* we're reading all distributed transactions, prevent new backends */
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

		values[0] = PointerGetDatum(cstring_to_text(connectionEntry->key.hostname));
		values[1] = Int32GetDatum(connectionEntry->key.port);
		values[2] = PointerGetDatum(cstring_to_text(databaseName));
		values[3] = Int32GetDatum(connectionEntry->connectionCount);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	UnLockConnectionSharedMemory();
}


/*
 * WaitOrErrorForSharedConnection tries to increment the shared connection
 * counter for the given hostname/port and the current database in
 * SharedConnStatsHash.
 *
 * The function implements a retry mechanism. If the function cannot increment
 * the counter withing the specificed amount of the time, it throws an error.
 */
void
WaitOrErrorForSharedConnection(const char *hostname, int port)
{
	int counter = 0;

	while (!TryToIncrementSharedConnectionCounter(hostname, port))
	{
		int latchFlags = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
		double timeoutMsec = 100.0;

		CHECK_FOR_INTERRUPTS();

		int rc = WaitLatch(MyLatch, latchFlags, (long) timeoutMsec, PG_WAIT_EXTENSION);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}
		else if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();

			if (IsHoldOffCancellationReceived())
			{
				/* in case the interrupts are hold, we still want to cancel */
				ereport(ERROR, (errmsg("canceling statement due to user request")));
			}
		}
		else if (rc & WL_TIMEOUT)
		{
			++counter;
			if (counter == 10)
			{
				ereport(ERROR, (errmsg("citus.max_shared_pool_size connections are "
									   "already established to the node %s:%d,"
									   "so cannot establish any more connections",
									   hostname, port),
								errhint("consider increasing "
										"citus.max_shared_pool_size")));
			}
		}
	}
}


/*
 * TryToIncrementSharedConnectionCounter tries to increment the shared
 * connection counter for the given nodeId and the current database in
 * SharedConnStatsHash.
 *
 * The function first checks whether the number of connections is less than
 * citus.max_shared_pool_size. If so, the function increments the counter
 * by one and returns true. Else, the function returns false.
 */
bool
TryToIncrementSharedConnectionCounter(const char *hostname, int port)
{
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

	bool entryFound = false;
	SharedConnStatsHashEntry *connectionEntry =
		hash_search(SharedConnStatsHash, &connKey, HASH_ENTER, &entryFound);

	if (!entryFound)
	{
		connectionEntry->connectionCount = 1;

		counterIncremented = true;
	}
	else if (connectionEntry->connectionCount + 1 >= MaxSharedPoolSize)
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
	size = add_size(size, mul_size(sizeof(LWLock), MaxTrackedWorkerNodes));

	Size hashSize = hash_estimate_size(MaxTrackedWorkerNodes,
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
	}

	/*  allocate hash table */
	SharedConnStatsHash =
		ShmemInitHash("Shared Conn. Stats Hash", MaxTrackedWorkerNodes,
					  MaxTrackedWorkerNodes, &info, hashFlags);

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
