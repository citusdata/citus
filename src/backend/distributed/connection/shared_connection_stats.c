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
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/tuplestore.h"
#include "utils/hashutils.h"
#include "utils/hsearch.h"
#include "storage/ipc.h"


#define REMOTE_CONNECTION_STATS_COLUMNS 6


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
	 * Using nodeId (over hostname/hostport) make the tracking resiliant to
	 * master_update_node(). Plus, requires a little less memory.
	 */
	uint32 nodeId;

	/*
	 * Given that citus.shared_max_pool_size can be defined per database, we
	 * should keep track of shared connections per database.
	 */
	char database[NAMEDATALEN];
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

		values[0] = Int32GetDatum(connectionEntry->key.nodeId);
		values[1] = PointerGetDatum(connectionEntry->key.database);
		values[2] = Int32GetDatum(connectionEntry->connectionCount);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	UnLockConnectionSharedMemory();
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

	uint32 hash = hash_uint32(entry->nodeId);
	hash = hash_combine(hash, string_hash(entry->database, NAMEDATALEN));

	return hash;
}


static int
SharedConnectionHashCompare(const void *a, const void *b, Size keysize)
{
	SharedConnStatsHashKey *ca = (SharedConnStatsHashKey *) a;
	SharedConnStatsHashKey *cb = (SharedConnStatsHashKey *) b;

	if (ca->nodeId != cb->nodeId ||
		strncmp(ca->database, cb->database, NAMEDATALEN) != 0)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
