/*-------------------------------------------------------------------------
 *
 * stat_counters.c
 *
 * This file contains functions to track various statistic counters for
 * Citus.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "common/hashfn.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "distributed/stat_counters.h"


/*
 * Shared hash constants.
 *
 * The places where STAT_COUNTERS_MAX_DATABASES is used do not impose a hard
 * limit on the number of databases that can be tracked but in ShmemInitHash()
 * it's documented that the access efficiency will degrade if it is exceeded
 * substantially.
 *
 * XXX: Consider using dshash_table instead (shared) HTAB.
 */
#define STAT_COUNTERS_INIT_DATABASES 8
#define STAT_COUNTERS_MAX_DATABASES 1024


typedef pg_atomic_uint64 CitusAtomicStatCounters[N_CITUS_STAT_COUNTERS];

/* shared hash table key */
typedef struct StatCountersHashKey
{
	Oid dbid;
} StatCountersHashKey;

/* shared hash table entry */
typedef struct StatCountersHashEntry
{
	StatCountersHashKey key;
	CitusAtomicStatCounters counters;
} StatCountersHashEntry;

/* shared memory state */
typedef struct StatCountersState
{
	LWLockId lock;
} StatCountersState;


/* GUC value for citus.enable_stat_counters */
bool EnableStatCounters = true;


/* shared memory variables */
static StatCountersState *StatCountersSharedState = NULL;
static HTAB *StatCountersHash = NULL;


/* shared memory init & management */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static void SharedStatCountersArrayShmemInit(void);

/* shared hash utilities */
static uint32 CitusStatCountersHashFn(const void *key, Size keysize);
static int CitusStatCountersMatchFn(const void *key1, const void *key2, Size keysize);

/* other helper functions */
static void StoreAllStatCounters(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor);
static Tuplestorestate * SetupStatCountersTuplestore(FunctionCallInfo fcinfo,
													 TupleDesc *tupleDescriptor);
static void ResetStatCounters(void);


PG_FUNCTION_INFO_V1(citus_stat_counters);
PG_FUNCTION_INFO_V1(citus_stat_counters_reset);


/*
 * citus_stat_counters returns all Citus stat counters.
 */
Datum
citus_stat_counters(PG_FUNCTION_ARGS)
{
	if (!EnableStatCounters)
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("citus stat counters are not enabled, consider setting "
						"citus.enable_stat_counters to true and restarting the "
						"server")));

		PG_RETURN_VOID();
	}

	if (!StatCountersSharedState || !StatCountersHash)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shared memory is not initialized for citus stat counters")));
	}


	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupStatCountersTuplestore(fcinfo, &tupleDescriptor);

	StoreAllStatCounters(tupleStore, tupleDescriptor);

	PG_RETURN_VOID();
}


/*
 * citus_stat_counters_reset resets all the Citus stat counters.
 */
Datum
citus_stat_counters_reset(PG_FUNCTION_ARGS)
{
	if (!EnableStatCounters)
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("citus stat counters are not enabled, consider setting "
						"citus.enable_stat_counters to true and restarting the "
						"server")));

		PG_RETURN_VOID();
	}

	if (!StatCountersSharedState || !StatCountersHash)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shared memory is not initialized for citus stat counters")));
	}

	ResetStatCounters();

	PG_RETURN_VOID();
}


/*
 * InitializeStatCountersArrayMem saves the previous shmem_startup_hook and sets
 * up a new shmem_startup_hook for initializing the shared memory used for
 * keeping track of stat counters across backends.
 */
void
InitializeStatCountersArrayMem(void)
{
/* on the versions older than PG 15, we use shmem_request_hook_type */
#if PG_VERSION_NUM < PG_VERSION_15

	if (!IsUnderPostmaster)
	{
		RequestAddinShmemSpace(StatCountersArrayShmemSize());

		elog(LOG, "requesting named LWLockTranch for %s",
			 STAT_COUNTERS_STATE_LOCK_TRANCHE_NAME);
		RequestNamedLWLockTranche(STAT_COUNTERS_STATE_LOCK_TRANCHE_NAME, 1);
	}
#endif

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = SharedStatCountersArrayShmemInit;
}


/*
 * StatCountersArrayShmemSize calculates and returns shared memory size
 * required to keep stat counters.
 */
Size
StatCountersArrayShmemSize(void)
{
	Size size = MAXALIGN(sizeof(StatCountersState));
	size = add_size(size, hash_estimate_size(STAT_COUNTERS_MAX_DATABASES,
											 sizeof(StatCountersHashEntry)));

	return size;
}


/*
 * IncrementStatCounter increments the stat counter for the given statId
 * of the current database.
 */
void
IncrementStatCounter(int statId)
{
	if (!EnableStatCounters)
	{
		return;
	}

	if (!StatCountersSharedState || !StatCountersHash)
	{
		return;
	}

	StatCountersHashKey key;
	key.dbid = MyDatabaseId;

	LWLockAcquire(StatCountersSharedState->lock, LW_SHARED);

	StatCountersHashEntry *dbEntry = (StatCountersHashEntry *) hash_search(
		StatCountersHash,
		(void *) &key,
		HASH_FIND,
		NULL);
	if (!dbEntry)
	{
		/* promote the lock to exclusive to insert the new entry for this database */
		LWLockRelease(StatCountersSharedState->lock);
		LWLockAcquire(StatCountersSharedState->lock, LW_EXCLUSIVE);

		bool councurrentlyInserted = false;
		dbEntry = (StatCountersHashEntry *) hash_search(StatCountersHash, (void *) &key,
														HASH_ENTER,
														&councurrentlyInserted);

		/*
		 * Initialize the counters to 0 if someone else didn't race while we
		 * were promoting the lock.
		 */
		if (!councurrentlyInserted)
		{
			for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
			{
				pg_atomic_init_u64(&dbEntry->counters[statIdx], 0);
			}
		}

		/* downgrade the lock to shared */
		LWLockRelease(StatCountersSharedState->lock);
		LWLockAcquire(StatCountersSharedState->lock, LW_SHARED);
	}

	pg_atomic_fetch_add_u64(&dbEntry->counters[statId], 1);

	LWLockRelease(StatCountersSharedState->lock);
}


/*
 * SetupStatCountersTuplestore returns a Tuplestorestate for returning the
 * stat counters.
 */
static Tuplestorestate *
SetupStatCountersTuplestore(FunctionCallInfo fcinfo, TupleDesc *tupleDescriptor)
{
	ReturnSetInfo *resultSet = (ReturnSetInfo *) fcinfo->resultinfo;
	switch (get_call_result_type(fcinfo, NULL, tupleDescriptor))
	{
		case TYPEFUNC_COMPOSITE:
		{
			/* success */
			break;
		}

		case TYPEFUNC_RECORD:
		{
			/* failed to determine actual type of RECORD */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
			break;
		}

		default:
		{
			/* result type isn't composite */
			elog(ERROR, "return type must be a row type");
			break;
		}
	}

	MemoryContext perQueryContext = resultSet->econtext->ecxt_per_query_memory;

	MemoryContext oldContext = MemoryContextSwitchTo(perQueryContext);

	bool randomAccess = true;
	bool interTransactions = false;
	Tuplestorestate *tupstore = tuplestore_begin_heap(randomAccess, interTransactions,
													  work_mem);

	resultSet->returnMode = SFRM_Materialize;
	resultSet->setResult = tupstore;
	resultSet->setDesc = *tupleDescriptor;

	MemoryContextSwitchTo(oldContext);

	return tupstore;
}


/*
 * StoreAllStatCounters returns all the available information about all the stat
 * into the given tuple store.
 */
static void
StoreAllStatCounters(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	LWLockAcquire(StatCountersSharedState->lock, LW_SHARED);

	HASH_SEQ_STATUS hashSeq;
	hash_seq_init(&hashSeq, StatCountersHash);

	StatCountersHashEntry *entry;
	while ((entry = hash_seq_search(&hashSeq)) != NULL)
	{
		Datum values[6] = { 0 };
		bool isNulls[6] = { 0 };
		MemSet(values, 0, sizeof(values));
		MemSet(isNulls, false, sizeof(isNulls));

		values[0] = ObjectIdGetDatum(entry->key.dbid);

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			uint64 statCounter = pg_atomic_read_u64(&entry->counters[statIdx]);
			values[statIdx + 1] = UInt64GetDatum(statCounter);
		}

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	LWLockRelease(StatCountersSharedState->lock);
}


/*
 * SharedStatCountersArrayShmemInit initializes the shared memory used
 * for keeping track of stat counters.
 */
static void
SharedStatCountersArrayShmemInit(void)
{
	/* initialize the shared memory only if the stat counters are enabled */
	if (EnableStatCounters)
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

		bool alreadyInitialized;
		StatCountersSharedState = ShmemInitStruct(STAT_COUNTERS_STATE_LOCK_TRANCHE_NAME,
												  sizeof(StatCountersState),
												  &alreadyInitialized);


		if (!alreadyInitialized)
		{
			StatCountersSharedState->lock = &(GetNamedLWLockTranche(
												  STAT_COUNTERS_STATE_LOCK_TRANCHE_NAME))
											->lock;
		}

		HASHCTL info;
		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(StatCountersHashKey);
		info.entrysize = sizeof(StatCountersHashEntry);
		info.hash = CitusStatCountersHashFn;
		info.match = CitusStatCountersMatchFn;

		StatCountersHash = ShmemInitHash("Citus Stat Counters Hash",
										 STAT_COUNTERS_INIT_DATABASES,
										 STAT_COUNTERS_MAX_DATABASES, &info,
										 HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

		LWLockRelease(AddinShmemInitLock);
	}

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * CitusStatCountersHashFn is the hash function for the stat counters hash table.
 */
static uint32
CitusStatCountersHashFn(const void *key, Size keysize)
{
	const StatCountersHashKey *k = (const StatCountersHashKey *) key;

	return hash_uint32((uint32) k->dbid);
}


/*
 * CitusStatCountersMatchFn is the match function for the stat counters hash table.
 */
static int
CitusStatCountersMatchFn(const void *key1, const void *key2, Size keysize)
{
	const StatCountersHashKey *k1 = (const StatCountersHashKey *) key1;
	const StatCountersHashKey *k2 = (const StatCountersHashKey *) key2;

	if (k1->dbid == k2->dbid)
	{
		return 0;
	}

	return 1;
}


/*
 * ResetStatCounters resets all the stat counters.
 */
static void
ResetStatCounters(void)
{
	LWLockAcquire(StatCountersSharedState->lock, LW_SHARED);

	HASH_SEQ_STATUS hashSeq;
	hash_seq_init(&hashSeq, StatCountersHash);

	StatCountersHashEntry *entry;
	while ((entry = hash_seq_search(&hashSeq)) != NULL)
	{
		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			pg_atomic_write_u64(&entry->counters[statIdx], 0);
		}
	}

	LWLockRelease(StatCountersSharedState->lock);
}
