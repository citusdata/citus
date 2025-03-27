/*-------------------------------------------------------------------------
 *
 * stat_counters.c
 *
 * This file contains functions to track various statistic counters for
 * Citus.
 *
 * Each backend increments the local counters (PendingStatCounters) and
 * flushes them to the shared memory at the end of the flush interval
 * (StatCountersFlushInterval). The shared memory is used to keep track
 * of the stat counters across backends for all databases.
 *
 * We don't have a good way to enforce that we flush the pending counters
 * always at the end of the flush interval because we only check for the
 * interval at the time of incrementing a counter. And, if a backend exits
 * before the interval elapses, then we still flush the pending counters at
 * the time of exit via CitusStatCountersFlushAtExit().
 *
 * Also, we store the stat counters in CITUS_STAT_COUNTERS_DUMP_FILE on
 * shutdown via postmaster and restore them on startup via the first backend
 * that initializes the shared memory for stat counters.
 *
 * And for the dropped databases, Citus maintenance daemon removes the stat
 * counters from the shared memory periodically, which is controlled by
 * StatCountersPurgeInterval. When the feature is disabled, removal of the
 * stat counters is automatically disabled. Instead of inserting a cleanup
 * record into pg_dist_cleanup, we choose to remove the stat counters via
 * the maintenance daemon because inserting a cleanup record wouldn't work
 * when the current database is dropped. And if there were other databases
 * where Citus is installed, then they would still continue to have the
 * dropped database's stat counters in the shared memory.
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "common/hashfn.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/syscache.h"

#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
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

	TimestampTz statsResetTimestamp;
} StatCountersHashEntry;

/* shared memory state */
typedef struct StatCountersState
{
	LWLockId lock;
} StatCountersState;


/* GUC value for citus.stat_counters_flush_interval */
int StatCountersFlushInterval = DEFAULT_STAT_COUNTERS_FLUSH_INTERVAL;

/* GUC value for citus.stat_counters_purge_interval */
int StatCountersPurgeInterval = DEFAULT_STAT_COUNTERS_PURGE_INTERVAL;

/* stat counters dump file version */
static const uint32 CITUS_STAT_COUNTERS_FILE_VERSION = 1;

/* shared memory init */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* shared memory variables */
static StatCountersState *CitusStatCountersSharedState = NULL;
static HTAB *CitusStatCountersSharedHash = NULL;

/* local stat counters that are pending to be flushed */
static uint64 PendingStatCounters[N_CITUS_STAT_COUNTERS] = { 0 };


/* helper functions for citus_stat_counters() and citus_stat_counters_reset() */
static Tuplestorestate * SetupStatCountersTuplestore(FunctionCallInfo fcinfo,
													 TupleDesc *tupleDescriptor);
static void StoreStatCountersForDbId(Tuplestorestate *tupleStore,
									 TupleDesc tupleDescriptor,
									 Oid dbId);
static void StoreStatCountersFromValues(Tuplestorestate *tupleStore,
										TupleDesc tupleDescriptor,
										pg_atomic_uint64 *counters,
										TimestampTz statsResetTimestamp);
static void ResetStatCountersForDbId(Oid dbId);
static void ResetStatCounterArray(pg_atomic_uint64 *counters);

/* helper functions to increment and flush stat counters */
static bool ShouldByPassLocalCounters(void);
static void IncrementSharedStatCounterForMyDb(int statId);
static void FlushPendingCountersIfNeeded(bool force);

/* shared memory init & management */
static void CitusStatCountersShmemInit(void);
static void CitusStatCountersShmemShutdown(int code, Datum arg);

/* shared hash utilities */
static StatCountersHashEntry * CitusStatCountersHashEntryAllocIfNotExists(Oid dbId);


PG_FUNCTION_INFO_V1(citus_stat_counters);
PG_FUNCTION_INFO_V1(citus_stat_counters_reset);


/*
 * citus_stat_counters returns the stat counters for the specified database or for
 * all databases if InvalidOid is provided.
 */
Datum
citus_stat_counters(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("database oid cannot be NULL")));
	}

	Oid dbId = PG_GETARG_OID(0);

	if (!IsCitusStatCountersEnabled())
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Citus stat counters are not enabled, consider setting "
						"citus.stat_counters_flush_interval to a non-negative value "
						"and restarting the server")));

	}

	if (!CitusStatCountersSharedState || !CitusStatCountersSharedHash)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("shared memory is not initialized for Citus stat counters")));
	}

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupStatCountersTuplestore(fcinfo, &tupleDescriptor);

	StoreStatCountersForDbId(tupleStore, tupleDescriptor, dbId);

	PG_RETURN_VOID();
}


/*
 * citus_stat_counters_reset resets the stat counters for the specified database
 * or for all databases if InvalidOid is provided.
 */
Datum
citus_stat_counters_reset(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("database oid cannot be NULL")));
	}

	Oid dbId = PG_GETARG_OID(0);

	if (!IsCitusStatCountersEnabled())
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Citus stat counters are not enabled, consider setting "
						"citus.stat_counters_flush_interval to a non-negative value "
						"and restarting the server")));

	}

	if (!CitusStatCountersSharedState || !CitusStatCountersSharedHash)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("shared memory is not initialized for Citus stat counters")));
	}

	ResetStatCountersForDbId(dbId);

	PG_RETURN_VOID();
}


/*
 * IsCitusStatCountersEnabled returns whether the stat counters are enabled.
 */
bool
IsCitusStatCountersEnabled(void)
{
	Assert(StatCountersFlushInterval >= DISABLE_STAT_COUNTERS_FLUSH_INTERVAL);
	return StatCountersFlushInterval != DISABLE_STAT_COUNTERS_FLUSH_INTERVAL;
}


/*
 * IncrementStatCounterForMyDb increments the stat counter for the given statId
 * of the current database.
 */
void
IncrementStatCounterForMyDb(int statId)
{
	if (!IsCitusStatCountersEnabled())
	{
		return;
	}

	if (ShouldByPassLocalCounters())
	{
		IncrementSharedStatCounterForMyDb(statId);
	}
	else
	{
		PendingStatCounters[statId]++;

		bool force = false;
		FlushPendingCountersIfNeeded(force);
	}
}


/*
 * InitializeStatCountersArrayMem saves the previous shmem_startup_hook and sets
 * up a new shmem_startup_hook to initialize the shared memory used for
 * keeping track of stat counters across backends for all databases.
 */
void
InitializeStatCountersArrayMem(void)
{
	Assert(IsCitusStatCountersEnabled());

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
	shmem_startup_hook = CitusStatCountersShmemInit;
}


/*
 * CitusStatCountersFlushAtExit is called on backend exit to flush the local
 * stat counters to the shared memory, if there are any pending.
 */
void
CitusStatCountersFlushAtExit(int code, Datum arg)
{
	Assert(IsCitusStatCountersEnabled());

	bool force = true;
	FlushPendingCountersIfNeeded(force);
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
 * CitusStatCountersRemoveDroppedDatabases removes the stat counters for the
 * databases that are dropped.
 */
void
CitusStatCountersRemoveDroppedDatabases(void)
{
	Assert(IsCitusStatCountersEnabled());

	List *droppedDatabaseIdList = NIL;

	LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);

	HASH_SEQ_STATUS hashSeqState = { 0 };
	hash_seq_init(&hashSeqState, CitusStatCountersSharedHash);

	StatCountersHashEntry *dbEntry = NULL;
	while ((dbEntry = hash_seq_search(&hashSeqState)) != NULL)
	{
		HeapTuple dbTuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbEntry->dbId));
		if (!HeapTupleIsValid(dbTuple))
		{
			droppedDatabaseIdList = lappend_oid(droppedDatabaseIdList, dbEntry->dbId);
		}
		else
		{
			ReleaseSysCache(dbTuple);
		}
	}

	LWLockRelease(CitusStatCountersSharedState->lock);

	if (list_length(droppedDatabaseIdList) == 0)
	{
		return;
	}

	/* acquire exclusive lock to quickly remove the entries */
	LWLockAcquire(CitusStatCountersSharedState->lock, LW_EXCLUSIVE);

	Oid droppedDatabaseId = InvalidOid;
	foreach_declared_oid(droppedDatabaseId, droppedDatabaseIdList)
	{
		hash_search(CitusStatCountersSharedHash, (void *) &droppedDatabaseId,
					HASH_REMOVE, NULL);
	}

	LWLockRelease(CitusStatCountersSharedState->lock);
}


/*
 * SetupStatCountersTuplestore returns a Tuplestorestate for returning the
 * stat counters and setups the provided TupleDesc.
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
 * StoreStatCountersForDbId fetches the stat counters for the specified database
 * and stores them into the given tuple store.
 */
static void
StoreStatCountersForDbId(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor, Oid dbId)
{
	LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);

	StatCountersHashEntry *dbEntry = hash_search(CitusStatCountersSharedHash,
												 (void *) &dbId,
												 HASH_FIND, NULL);
	if (dbEntry)
	{
		StoreStatCountersFromValues(tupleStore, tupleDescriptor, dbEntry->counters,
									dbEntry->statsResetTimestamp);
	}
	else
	{
		/* use a zeroed entry if the database doesn't exist */
		TimestampTz zeroedTimestamp = 0;

		pg_atomic_uint64 zeroedCounters[N_CITUS_STAT_COUNTERS] = { 0 };
		ResetStatCounterArray(zeroedCounters);

		StoreStatCountersFromValues(tupleStore, tupleDescriptor, zeroedCounters,
									zeroedTimestamp);
	}

	LWLockRelease(CitusStatCountersSharedState->lock);
}


/*
 * StoreStatCountersFromValues stores the stat counters stored in given
 * counter array and the timestamp into the given tuple store.
 *
 * Given counter array is assumed to be of length N_CITUS_STAT_COUNTERS.
 */
static void
StoreStatCountersFromValues(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor,
							pg_atomic_uint64 *counters, TimestampTz statsResetTimestamp)
{
	Datum values[N_CITUS_STAT_COUNTERS + 1] = { 0 };
	bool isNulls[N_CITUS_STAT_COUNTERS + 1] = { 0 };

	for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
	{
		values[statIdx] = UInt64GetDatum(pg_atomic_read_u64(&counters[statIdx]));
	}

	if (statsResetTimestamp == 0)
	{
		isNulls[N_CITUS_STAT_COUNTERS] = true;
	}
	else
	{
		values[N_CITUS_STAT_COUNTERS] = TimestampTzGetDatum(statsResetTimestamp);
	}

	tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
}


/*
 * ResetStatCountersForDbId resets the stat counters for the specified database or
 * for all databases if InvalidOid is provided.
 */
static void
ResetStatCountersForDbId(Oid dbId)
{
	LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);

	if (OidIsValid(dbId))
	{
		StatCountersHashEntry *dbEntry = hash_search(CitusStatCountersSharedHash,
													 (void *) &dbId,
													 HASH_FIND, NULL);

		/* skip if we don't have an entry for this database */
		if (dbEntry)
		{
			ResetStatCounterArray(dbEntry->counters);
			dbEntry->statsResetTimestamp = GetCurrentTimestamp();
		}
	}
	else
	{
		HASH_SEQ_STATUS hashSeqState = { 0 };
		hash_seq_init(&hashSeqState, CitusStatCountersSharedHash);

		StatCountersHashEntry *dbEntry = NULL;
		while ((dbEntry = hash_seq_search(&hashSeqState)) != NULL)
		{
			ResetStatCounterArray(dbEntry->counters);
			dbEntry->statsResetTimestamp = GetCurrentTimestamp();
		}
	}

	LWLockRelease(CitusStatCountersSharedState->lock);
}


/*
 * ResetStatCounterArray resets the stat counters stored in the given
 * counter array.
 *
 * Given counter array is assumed to be of length N_CITUS_STAT_COUNTERS.
 */
static void
ResetStatCounterArray(pg_atomic_uint64 *counters)
{
	for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
	{
		pg_atomic_write_u64(&counters[statIdx], 0);
	}
}


/*
 * ShouldByPassLocalCounters returns true if we should immediately increment
 * the shared memory counters without buffering them in the local counters.
 */
static bool
ShouldByPassLocalCounters(void)
{
	Assert(IsCitusStatCountersEnabled());
	return StatCountersFlushInterval == 0;
}


/*
 * IncrementSharedStatCounterForMyDb increments the stat counter for the given statId
 * of the current database in the shared memory.
 */
static void
IncrementSharedStatCounterForMyDb(int statId)
{
	Oid dbId = MyDatabaseId;

	LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);

	/*
	 * XXX: Can we cache the entry for the current database?
	 *
	 *      From one perspective, doing so should be fine since dropping a
	 *      database succeeds only after all the backends connected to it
	 *      are disconnected, so we cannot have a backend that has a dangling
	 *      entry.
	 *
	 *      On the other hand, we're not sure if a potential hash table resize
	 *      would invalidate the cached entry.
	 */
	StatCountersHashEntry *dbEntry = (StatCountersHashEntry *) hash_search(
		CitusStatCountersSharedHash,
		(void *) &dbId,
		HASH_FIND,
		NULL);
	if (!dbEntry)
	{
		/* promote the lock to exclusive to insert the new entry for this database */
		LWLockRelease(CitusStatCountersSharedState->lock);
		LWLockAcquire(CitusStatCountersSharedState->lock, LW_EXCLUSIVE);

		dbEntry = CitusStatCountersHashEntryAllocIfNotExists(dbId);

		/* downgrade the lock to shared */
		LWLockRelease(CitusStatCountersSharedState->lock);
		LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);
	}

	pg_atomic_fetch_add_u64(&dbEntry->counters[statId], 1);

	LWLockRelease(CitusStatCountersSharedState->lock);
}


/*
 * FlushPendingCountersIfNeeded flushes PendingStatCounters to the shared
 * memory if StatCountersFlushInterval has elapsed since the last flush
 * or if force is true.
 */
static void
FlushPendingCountersIfNeeded(bool force)
{
	static instr_time LastFlushTime = { 0 };

	if (!force)
	{
		if (INSTR_TIME_IS_ZERO(LastFlushTime))
		{
			/* assume that the first increment is the start of the flush interval */
			INSTR_TIME_SET_CURRENT(LastFlushTime);
			return;
		}

		instr_time now = { 0 };
		INSTR_TIME_SET_CURRENT(now);
		INSTR_TIME_SUBTRACT(now, LastFlushTime);
		if (INSTR_TIME_GET_MILLISEC(now) < StatCountersFlushInterval)
		{
			return;
		}
	}

	Oid dbId = MyDatabaseId;

	LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);

	/* XXX: Same here, can we cache the entry for the current database? */
	StatCountersHashEntry *dbEntry = (StatCountersHashEntry *) hash_search(
		CitusStatCountersSharedHash,
		(void *) &dbId,
		HASH_FIND,
		NULL);
	if (!dbEntry)
	{
		/* promote the lock to exclusive to insert the new entry for this database */
		LWLockRelease(CitusStatCountersSharedState->lock);
		LWLockAcquire(CitusStatCountersSharedState->lock, LW_EXCLUSIVE);

		dbEntry = CitusStatCountersHashEntryAllocIfNotExists(dbId);

		/* downgrade the lock to shared */
		LWLockRelease(CitusStatCountersSharedState->lock);
		LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);
	}

	for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
	{
		if (PendingStatCounters[statIdx] == 0)
		{
			/* nothing to flush for this stat, avoid unnecessary lock contention */
			continue;
		}

		pg_atomic_fetch_add_u64(&dbEntry->counters[statIdx],
								PendingStatCounters[statIdx]);

		PendingStatCounters[statIdx] = 0;
	}

	LWLockRelease(CitusStatCountersSharedState->lock);

	INSTR_TIME_SET_CURRENT(LastFlushTime);
}


/*
 * CitusStatCountersShmemInit initializes the shared memory used
 * for keeping track of stat counters and restores the stat counters from
 * the dump file.
 */
static void
CitusStatCountersShmemInit(void)
{
	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}

	Assert(IsCitusStatCountersEnabled());

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	bool alreadyInitialized = false;
	CitusStatCountersSharedState = ShmemInitStruct(STAT_COUNTERS_STATE_LOCK_TRANCHE_NAME,
												   sizeof(StatCountersState),
												   &alreadyInitialized);


	if (!alreadyInitialized)
	{
		CitusStatCountersSharedState->lock = &(GetNamedLWLockTranche(
												   STAT_COUNTERS_STATE_LOCK_TRANCHE_NAME))
											 ->lock;
	}

	HASHCTL hashInfo = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(StatCountersHashEntry),
		.hash = uint32_hash,
	};

	CitusStatCountersSharedHash = ShmemInitHash("Citus stat counters Hash",
												STAT_COUNTERS_INIT_DATABASES,
												STAT_COUNTERS_MAX_DATABASES, &hashInfo,
												HASH_ELEM | HASH_FUNCTION);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
	{
		/* postmaster dumps the stat counters on shutdown */
		on_shmem_exit(CitusStatCountersShmemShutdown, (Datum) 0);
	}

	if (alreadyInitialized)
	{
		/*
		 * alreadyInitialized being true doesn't necessarily mean that the
		 * backend that initialized the shared memory has completed restoring
		 * the stat counters from the dump file.
		 *
		 * However, it's still fine to return here as commented below.
		 */
		return;
	}

	/*
	 * We know that only one backend can be here at this point, so we're
	 * the only one that restores the stat counters from the dump file.
	 *
	 * However, we don't block other backends from accessing the shared
	 * memory while we're restoring the stat counters - need to be careful.
	 */
	FILE *file = AllocateFile(CITUS_STAT_COUNTERS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
		{
			/* ignore not-found error */
			return;
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
		if (fread(&statCounters, sizeof(uint64), N_CITUS_STAT_COUNTERS, file) !=
			N_CITUS_STAT_COUNTERS)
		{
			goto error;
		}

		LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);

		StatCountersHashEntry *dbEntry = (StatCountersHashEntry *) hash_search(
			CitusStatCountersSharedHash,
			(void *) &dbId,
			HASH_FIND,
			NULL);
		if (!dbEntry)
		{
			/* promote the lock to exclusive to insert the new entry for this database */
			LWLockRelease(CitusStatCountersSharedState->lock);
			LWLockAcquire(CitusStatCountersSharedState->lock, LW_EXCLUSIVE);

			dbEntry = CitusStatCountersHashEntryAllocIfNotExists(dbId);

			/* downgrade the lock to shared */
			LWLockRelease(CitusStatCountersSharedState->lock);
			LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);
		}

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			/*
			 * If no other backend has created the entry for us, then we do that
			 * via the above call made to CitusStatCountersHashEntryAllocIfNotExists()
			 * and there we init the counters if we just created the entry.
			 *
			 * So here we always "fetch_add" instead of "write" or "init" to avoid
			 * overwriting the counters-increments made by other backends so far, if
			 * it's not us who created the entry.
			 */
			pg_atomic_fetch_add_u64(&dbEntry->counters[statIdx], statCounters[statIdx]);
		}

		LWLockRelease(CitusStatCountersSharedState->lock);
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
 * CitusStatCountersShmemShutdown is called on shutdown by postmaster to dump the
 * stat counters to CITUS_STAT_COUNTERS_DUMP_FILE.
 */
static void
CitusStatCountersShmemShutdown(int code, Datum arg)
{
	/* don't try to dump during a crash */
	if (code)
	{
		return;
	}

	Assert(IsCitusStatCountersEnabled());

	LWLockAcquire(CitusStatCountersSharedState->lock, LW_SHARED);

	FILE *file = AllocateFile(CITUS_STAT_COUNTERS_TMP_DUMP_FILE, PG_BINARY_W);
	if (file == NULL)
	{
		goto error;
	}

	if (fwrite(&CITUS_STAT_COUNTERS_FILE_VERSION, sizeof(uint32), 1, file) != 1)
	{
		goto error;
	}

	int32 numEntries = hash_get_num_entries(CitusStatCountersSharedHash);
	if (fwrite(&numEntries, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	StatCountersHashEntry *dbEntry = NULL;

	HASH_SEQ_STATUS hashSeqState = { 0 };
	hash_seq_init(&hashSeqState, CitusStatCountersSharedHash);
	while ((dbEntry = hash_seq_search(&hashSeqState)) != NULL)
	{
		if (fwrite(&dbEntry->dbId, sizeof(Oid), 1, file) != 1)
		{
			/* we assume hash_seq_term won't change errno */
			hash_seq_term(&hashSeqState);
			goto error;
		}

		uint64 statCounters[N_CITUS_STAT_COUNTERS] = { 0 };

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			statCounters[statIdx] = pg_atomic_read_u64(&dbEntry->counters[statIdx]);
		}

		if (fwrite(&statCounters, sizeof(uint64), N_CITUS_STAT_COUNTERS, file) !=
			N_CITUS_STAT_COUNTERS)
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
	if (rename(CITUS_STAT_COUNTERS_TMP_DUMP_FILE, CITUS_STAT_COUNTERS_DUMP_FILE) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename citus_stat_counters file \"%s\" to \"%s\": %m",
						CITUS_STAT_COUNTERS_TMP_DUMP_FILE,
						CITUS_STAT_COUNTERS_DUMP_FILE)));
	}

	LWLockRelease(CitusStatCountersSharedState->lock);

	return;

error:

	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write citus_stat_counters file \"%s\": %m",
					CITUS_STAT_COUNTERS_TMP_DUMP_FILE)));

	if (file)
	{
		FreeFile(file);
	}

	LWLockRelease(CitusStatCountersSharedState->lock);

	unlink(CITUS_STAT_COUNTERS_DUMP_FILE);
}


/*
 * CitusStatCountersHashEntryAllocIfNotExists allocates a new entry in the
 * stat counters hash table if it doesn't already exist.
 *
 * Assumes that the caller has exclusive access to the hash table.
 */
static StatCountersHashEntry *
CitusStatCountersHashEntryAllocIfNotExists(Oid dbId)
{
	bool councurrentlyInserted = false;
	StatCountersHashEntry *dbEntry =
		(StatCountersHashEntry *) hash_search(CitusStatCountersSharedHash, (void *) &dbId,
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

		dbEntry->statsResetTimestamp = 0;
	}

	return dbEntry;
}
