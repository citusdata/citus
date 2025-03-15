/*-------------------------------------------------------------------------
 *
 * stat_counters.c
 *
 * This file contains functions to track various statistic counters for
 * Citus.
 *
 * XXX: Need to handle dropped databases.
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


/* file path for dumping the stat counters */
#define CITUS_STAT_COUNTERS_DUMP_FILE "pg_stat/citus_stat_counters.stat"
#define CITUS_STAT_COUNTERS_TMP_DUMP_FILE (CITUS_STAT_COUNTERS_DUMP_FILE ".tmp")

/*
 * Shared hash constants.
 *
 * The places where STAT_COUNTERS_MAX_DATABASES is used do not impose a hard
 * limit on the number of databases that can be tracked but in ShmemInitHash()
 * it's documented that the access efficiency will degrade if it is exceeded
 * substantially.
 *
 * XXX: Consider using dshash_table instead of (shared) HTAB if that becomes
 *      a concern.
 */
#define STAT_COUNTERS_INIT_DATABASES 8
#define STAT_COUNTERS_MAX_DATABASES 1024


/* shared hash table entry */
typedef struct StatCountersHashEntry
{
	/* must be the first field since this is used as the key */
	Oid dbId;

	pg_atomic_uint64 counters[N_CITUS_STAT_COUNTERS];
} StatCountersHashEntry;

/* shared memory state */
typedef struct StatCountersState
{
	LWLockId lock;
} StatCountersState;


/* GUC value for citus.enable_stat_counters */
bool EnableStatCounters = true;

/* stat counters dump file version */
static const uint32 CITUS_STAT_COUNTERS_FILE_VERSION = 1;

/* shared memory variables */
static StatCountersState *StatCountersSharedState = NULL;
static HTAB *StatCountersSharedHash = NULL;


/* shared memory init & management */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static void SharedStatCountersArrayShmemInit(void);
static void CitusStatCountersShmemShutdown(int code, Datum arg);

/* shared hash callbacks & utilities */
static uint32 CitusStatCountersHashFn(const void *key, Size keysize);
static int CitusStatCountersMatchFn(const void *key1, const void *key2, Size keysize);
static StatCountersHashEntry * CitusStatCountersHashEntryAllocIfNotExists(Oid dbId, bool init);

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

	if (!StatCountersSharedState || !StatCountersSharedHash)
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

	if (!StatCountersSharedState || !StatCountersSharedHash)
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

	if (!StatCountersSharedState || !StatCountersSharedHash)
	{
		return;
	}

	LWLockAcquire(StatCountersSharedState->lock, LW_SHARED);

	Oid dbId = MyDatabaseId;

	StatCountersHashEntry *dbEntry = (StatCountersHashEntry *) hash_search(
		StatCountersSharedHash,
		(void *) &dbId,
		HASH_FIND,
		NULL);
	if (!dbEntry)
	{
		/* promote the lock to exclusive to insert the new entry for this database */
		LWLockRelease(StatCountersSharedState->lock);
		LWLockAcquire(StatCountersSharedState->lock, LW_EXCLUSIVE);

		/*
		 * Seems we don't have an entry for this database, so need to
		 * initialize the counters as well.
		 */
		bool init = true;
		dbEntry = CitusStatCountersHashEntryAllocIfNotExists(dbId, init);

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

	HASH_SEQ_STATUS hashSeqState;
	hash_seq_init(&hashSeqState, StatCountersSharedHash);

	StatCountersHashEntry *entry;
	while ((entry = hash_seq_search(&hashSeqState)) != NULL)
	{
		Datum values[6] = { 0 };
		bool isNulls[6] = { 0 };
		MemSet(values, 0, sizeof(values));
		MemSet(isNulls, false, sizeof(isNulls));

		values[0] = ObjectIdGetDatum(entry->dbId);

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
	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}

	if (!EnableStatCounters)
	{
		return;
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	bool alreadyInitialized = false;
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
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(StatCountersHashEntry);
	info.hash = CitusStatCountersHashFn;
	info.match = CitusStatCountersMatchFn;

	StatCountersSharedHash = ShmemInitHash("Citus Stat Counters Hash",
										STAT_COUNTERS_INIT_DATABASES,
										STAT_COUNTERS_MAX_DATABASES, &info,
										HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
	{
		on_shmem_exit(CitusStatCountersShmemShutdown, (Datum) 0);
	}

	if (alreadyInitialized)
	{
		return;
	}

	/* Load stat file, don't care about locking */
	FILE *file = AllocateFile(CITUS_STAT_COUNTERS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
		{
			return;         /* ignore not-found error */
		}
		goto error;
	}

	uint32 fileVersion = 0;
	if (fread(&fileVersion, sizeof(uint32), 1, file) != 1)
	{
		goto error;
	}

	/* check if version is correct, atm we only have one */
	if (fileVersion != CITUS_STAT_COUNTERS_FILE_VERSION)
	{
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("citus_stat_counters file version mismatch: expected %u, got %u",
						CITUS_STAT_COUNTERS_FILE_VERSION, fileVersion)));

		goto error;
	}

	/* get number of entries */
	int32 numEntries = 0;
	if (fread(&numEntries, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	for (int i = 0; i < numEntries; i++)
	{
		Oid dbId = InvalidOid;
		if (fread(&dbId, sizeof(Oid), 1, file) != 1)
		{
			goto error;
		}

		uint64 statCounters[N_CITUS_STAT_COUNTERS] = { 0 };
		if (fread(&statCounters, sizeof(uint64), N_CITUS_STAT_COUNTERS, file) != N_CITUS_STAT_COUNTERS)
		{
			goto error;
		}

		/*
		 * We don't need to initialize the counters since we'll set them to the
		 * values read from the file soon.
		 */
		bool init = false;
		StatCountersHashEntry *entry = CitusStatCountersHashEntryAllocIfNotExists(dbId, init);

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			pg_atomic_write_u64(&entry->counters[statIdx], statCounters[statIdx]);
		}
	}

	FreeFile(file);

	/*
	 * Remove the file so it's not included in backups/replicas, etc. A new file will be
	 * written on next shutdown.
	 */
	unlink(CITUS_STAT_COUNTERS_DUMP_FILE);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read citus_stat_counters file \"%s\": %m",
					CITUS_STAT_COUNTERS_DUMP_FILE)));

	if (file)
	{
		FreeFile(file);
	}

	/* delete bogus file, don't care of errors in this case */
	unlink(CITUS_STAT_COUNTERS_DUMP_FILE);
}


/*
 * CitusStatCountersShmemShutdown is called on shutdown to dump the stat counters
 * to CITUS_STAT_COUNTERS_DUMP_FILE.
 */
static void
CitusStatCountersShmemShutdown(int code, Datum arg)
{
	/* don't try to dump during a crash */
	if (code)
	{
		return;
	}

	if (!EnableStatCounters || !StatCountersSharedState)
	{
		return;
	}

	FILE *file = AllocateFile(CITUS_STAT_COUNTERS_TMP_DUMP_FILE , PG_BINARY_W);
	if (file == NULL)
	{
		goto error;
	}

	if (fwrite(&CITUS_STAT_COUNTERS_FILE_VERSION, sizeof(uint32), 1, file) != 1)
	{
		goto error;
	}

	int32 numEntries = hash_get_num_entries(StatCountersSharedHash);
	if (fwrite(&numEntries, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	StatCountersHashEntry *entry = NULL;

	HASH_SEQ_STATUS hashSeqState;
	hash_seq_init(&hashSeqState, StatCountersSharedHash);
	while ((entry = hash_seq_search(&hashSeqState)) != NULL)
	{
		if (fwrite(&entry->dbId, sizeof(Oid), 1, file) != 1)
		{
			/* we assume hash_seq_term won't change errno */
			hash_seq_term(&hashSeqState);
			goto error;
		}

		uint64 statCounters[N_CITUS_STAT_COUNTERS] = { 0 };

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			statCounters[statIdx] = pg_atomic_read_u64(&entry->counters[statIdx]);
		}

		if (fwrite(&statCounters, sizeof(uint64), N_CITUS_STAT_COUNTERS, file) != N_CITUS_STAT_COUNTERS)
		{
			/* we assume hash_seq_term won't change errno */
			hash_seq_term(&hashSeqState);
			goto error;
		}
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/* rename the file inplace */
	if (rename(CITUS_STAT_COUNTERS_TMP_DUMP_FILE , CITUS_STAT_COUNTERS_DUMP_FILE) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename citus_stat_counters file \"%s\": %m",
					    CITUS_STAT_COUNTERS_TMP_DUMP_FILE )));
	}

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write citus_stat_counters file \"%s\": %m",
				    CITUS_STAT_COUNTERS_DUMP_FILE)));

	if (file)
	{
		FreeFile(file);
	}

	unlink(CITUS_STAT_COUNTERS_DUMP_FILE);
}

/*
 * CitusStatCountersHashFn is the hash function for the stat counters hash table.
 */
static uint32
CitusStatCountersHashFn(const void *key, Size keysize)
{
	const Oid *k = (const Oid *) key;

	return hash_uint32((uint32) *k);
}


/*
 * CitusStatCountersMatchFn is the match function for the stat counters hash table.
 */
static int
CitusStatCountersMatchFn(const void *key1, const void *key2, Size keysize)
{
	const Oid *k1 = (const Oid *) key1;
	const Oid *k2 = (const Oid *) key2;

	if (*k1 == *k2)
	{
		return 0;
	}

	return 1;
}

/*
 * CitusStatCountersHashEntryAllocIfNotExists allocates a new entry in the
 * stat counters hash table if it doesn't already exist.
 *
 * Assumes that the caller holds LW_EXCLUSIVE on StatCountersSharedState->lock.
 */
static StatCountersHashEntry *
CitusStatCountersHashEntryAllocIfNotExists(Oid dbId, bool init)
{
	bool councurrentlyInserted = false;
	StatCountersHashEntry * dbEntry =
		(StatCountersHashEntry *) hash_search(StatCountersSharedHash, (void *) &dbId,
											  HASH_ENTER,
											  &councurrentlyInserted);

	/*
	* Initialize the counters to 0 if someone else didn't race while we
	* were promoting the lock.
	*/
	if (!councurrentlyInserted && init)
	{
		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			pg_atomic_init_u64(&dbEntry->counters[statIdx], 0);
		}
	}

	return dbEntry;
}


/*
 * ResetStatCounters resets all the stat counters.
 */
static void
ResetStatCounters(void)
{
	LWLockAcquire(StatCountersSharedState->lock, LW_SHARED);

	HASH_SEQ_STATUS hashSeqState;
	hash_seq_init(&hashSeqState, StatCountersSharedHash);

	StatCountersHashEntry *entry;
	while ((entry = hash_seq_search(&hashSeqState)) != NULL)
	{
		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			pg_atomic_write_u64(&entry->counters[statIdx], 0);
		}
	}

	LWLockRelease(StatCountersSharedState->lock);
}
