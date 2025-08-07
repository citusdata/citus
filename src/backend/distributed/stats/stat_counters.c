/*-------------------------------------------------------------------------
 *
 * stat_counters.c
 *
 * This file contains functions to track various statistic counters for
 * Citus.
 *
 * We create an array of "BackendStatsSlot"s in shared memory, one for
 * each backend. Each backend increments its own stat counters in its
 * own slot via IncrementStatCounterForMyDb(). And when a backend exits,
 * it saves its stat counters from its slot via
 * SaveBackendStatsIntoSavedBackendStatsHash() into a hash table in
 * shared memory, whose entries are "SavedBackendStatsHashEntry"s and
 * the key is the database id. In other words, each entry of the hash
 * table is used to aggregate the stat counters for backends that were
 * connected to that database and exited since the last server restart.
 * Plus, each entry is responsible for keeping track of the reset
 * timestamp for both active and exited backends too.
 * Note that today we don't evict the entries of the said hash table
 * that point to dropped databases because the wrapper view anyway
 * filters them out (thanks to LEFT JOIN) and we don't expect a
 * performance hit due to that unless users have a lot of databases
 * that are dropped and recreated frequently.
 *
 * The reason why we save the stat counters for exited backends in the
 * shared hash table is that we cannot guarantee that the backend slot
 * that was used by an exited backend will be reused by another backend
 * connected to the same database. For this reason, we need to save the
 * stat counters for exited backends into a shared hash table so that we
 * can reset the counters within the corresponding backend slots while
 * the backends exit.
 *
 * When citus_stat_counters() is called, we first aggregate the stat
 * counters from the backend slots of all the active backends and then
 * we add the aggregated stat counters from the exited backends that
 * are stored in the shared hash table. Also, we don't persist backend
 * stats on server shutdown, but we might want to do that in the future.
 *
 * Similarly, when citus_stat_counters_reset() is called, we reset the
 * stat counters for the active backends and the exited backends that are
 * stored in the shared hash table. Then, it also updates the
 * resetTimestamp in the shared hash table entry appropriately. So,
 * similarly, when citus_stat_counters() is called, we just report
 * resetTimestamp as stats_reset column.
 *
 * Caveats:
 *
 * There is chance that citus_stat_counters_reset() might race with a
 * backend that is trying to increment one of the counters in its slot
 * and as a result it can effectively fail to reset that counter due to
 * the reasons documented in IncrementStatCounterForMyDb() function.
 * However, this should be a very rare case and we can live with that
 * for now.
 *
 * Also, citus_stat_counters() might observe the counters for a backend
 * twice or perhaps unsee it if it's concurrently exiting, depending on
 * the order we call CollectActiveBackendStatsIntoHTAB() and
 * CollectSavedBackendStatsIntoHTAB() in citus_stat_counters(). However,
 * the next call to citus_stat_counters() will see the correct values
 * for the counters, so we can live with that for now.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "common/hashfn.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/hsearch.h"

#include "pg_version_compat.h"

#include "distributed/argutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/stats/stat_counters.h"
#include "distributed/tuplestore.h"


/*
 * saved backend stats - hash table constants
 *
 * Configurations used to create the hash table for saved backend stats.
 * The places where SAVED_BACKEND_STATS_HASH_MAX_DATABASES is used do not
 * impose a hard limit on the number of databases that can be tracked but
 * in ShmemInitHash() it's documented that the access efficiency will degrade
 * if it is exceeded substantially.
 *
 * XXX: Consider using dshash_table instead of (shared) HTAB if that becomes
 *      a concern.
 */
#define SAVED_BACKEND_STATS_HASH_INIT_DATABASES 8
#define SAVED_BACKEND_STATS_HASH_MAX_DATABASES 1024


/* fixed size array types to store the stat counters */
typedef pg_atomic_uint64 AtomicStatCounters[N_CITUS_STAT_COUNTERS];
typedef uint64 StatCounters[N_CITUS_STAT_COUNTERS];

/*
 * saved backend stats - hash entry definition
 *
 * This is used to define & access the shared hash table used to aggregate the stat
 * counters for the backends exited so far since last server restart. It's also
 * responsible for keeping track of the reset timestamp.
 */
typedef struct SavedBackendStatsHashEntry
{
	/* hash entry key, must always be the first */
	Oid databaseId;

	/*
	 * Needs to be locked whenever we read / write counters or resetTimestamp
	 * in this struct since we don't use atomic counters for this struct. Plus,
	 * we want to update the stat counters and resetTimestamp atomically.
	 */
	slock_t mutex;

	/*
	 * While "counters" only represents the stat counters for exited backends,
	 * the "resetTimestamp" doesn't only represent the reset timestamp for exited
	 * backends' stat counters but also for the active backends.
	 */
	StatCounters counters;
	TimestampTz resetTimestamp;
} SavedBackendStatsHashEntry;

/*
 * Hash entry definition used for the local hash table created by
 * citus_stat_counters() at the runtime to aggregate the stat counters
 * across all backends.
 */
typedef struct DatabaseStatsHashEntry
{
	/* hash entry key, must always be the first */
	Oid databaseId;

	StatCounters counters;
	TimestampTz resetTimestamp;
} DatabaseStatsHashEntry;

/* definition of a one per-backend stat counters slot in shared memory */
typedef struct BackendStatsSlot
{
	AtomicStatCounters counters;
} BackendStatsSlot;


/*
 * GUC variable
 *
 * This only controls whether we track the stat counters or not, via
 * IncrementStatCounterForMyDb() and
 * SaveBackendStatsIntoSavedBackendStatsHash(). In other words, even
 * when the GUC is disabled, we still allocate the shared memory
 * structures etc. and citus_stat_counters() / citus_stat_counters_reset()
 * will still work.
 */
bool EnableStatCounters = ENABLE_STAT_COUNTERS_DEFAULT;

/* saved backend stats - shared memory variables */
static LWLockId *SharedSavedBackendStatsHashLock = NULL;
static HTAB *SharedSavedBackendStatsHash = NULL;

/* per-backend stat counter slots - shared memory array */
BackendStatsSlot *SharedBackendStatsSlotArray = NULL;

/*
 * We don't expect the callsites that check this (via
 * EnsureStatCountersShmemInitDone()) to be executed before
 * StatCountersShmemInit() is done. Plus, once StatCountersShmemInit()
 * is done, we also don't expect shared memory variables to be
 * initialized improperly. However, we still set this to true only
 * once StatCountersShmemInit() is done and if all three of the shared
 * memory variables above are initialized properly. And in the callsites
 * where these shared memory variables are accessed, we check this
 * variable first just to be on the safe side.
 */
static bool StatCountersShmemInitDone = false;

/* saved shmem_startup_hook */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


/* shared memory init & management */
static void StatCountersShmemInit(void);
static Size SharedBackendStatsSlotArrayShmemSize(void);

/* helper functions for citus_stat_counters() */
static void CollectActiveBackendStatsIntoHTAB(Oid databaseId, HTAB *databaseStats);
static void CollectSavedBackendStatsIntoHTAB(Oid databaseId, HTAB *databaseStats);
static DatabaseStatsHashEntry * DatabaseStatsHashEntryFindOrCreate(Oid databaseId,
																   HTAB *databaseStats);
static void StoreDatabaseStatsIntoTupStore(HTAB *databaseStats,
										   Tuplestorestate *tupleStore,
										   TupleDesc tupleDescriptor);

/* helper functions for citus_stat_counters_reset() */
static bool ResetActiveBackendStats(Oid databaseId);
static void ResetSavedBackendStats(Oid databaseId, bool force);

/* saved backend stats */
static SavedBackendStatsHashEntry * SavedBackendStatsHashEntryCreateIfNotExists(Oid
																				databaseId);


/* sql exports */
PG_FUNCTION_INFO_V1(citus_stat_counters);
PG_FUNCTION_INFO_V1(citus_stat_counters_reset);


/*
 * EnsureStatCountersShmemInitDone returns true if the shared memory
 * data structures used for keeping track of stat counters have been
 * properly initialized, otherwise, returns false and emits a warning.
 */
static inline bool
EnsureStatCountersShmemInitDone(void)
{
	if (!StatCountersShmemInitDone)
	{
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shared memory for stat counters was not properly initialized")));
		return false;
	}

	return true;
}


/*
 * citus_stat_counters returns stats counters for the given database id.
 *
 * This only returns rows for the databases which have been connected to
 * by at least one backend since the last server restart (even if no
 * observations have been made for none of the counters or if they were
 * reset) and it considers such a database even if it has been dropped later.
 *
 * When InvalidOid is provided, all such databases are considered; otherwise
 * only the database with the given id is considered.
 *
 * So, as an outcome, when a database id that is different than InvalidOid
 * is provided and no backend has connected to it since the last server
 * restart, or, if we didn't ever have such a database, then the function
 * returns an empty set.
 *
 * Finally, stats_reset column is set to NULL if the stat counters for the
 * database were never reset since the last server restart.
 */
Datum
citus_stat_counters(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/*
	 * Function's sql definition allows Postgres to silently
	 * ignore NULL, but we still check.
	 */
	PG_ENSURE_ARGNOTNULL(0, "database_id");
	Oid databaseId = PG_GETARG_OID(0);

	/* just to be on the safe side */
	if (!EnsureStatCountersShmemInitDone())
	{
		PG_RETURN_VOID();
	}

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	HASHCTL info;
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION);
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.hash = oid_hash;
	info.entrysize = sizeof(DatabaseStatsHashEntry);

	HTAB *databaseStats = hash_create("Citus Database Stats Collect Hash", 8, &info,
									  hashFlags);

	CollectActiveBackendStatsIntoHTAB(databaseId, databaseStats);
	CollectSavedBackendStatsIntoHTAB(databaseId, databaseStats);

	StoreDatabaseStatsIntoTupStore(databaseStats, tupleStore, tupleDescriptor);

	hash_destroy(databaseStats);

	PG_RETURN_VOID();
}


/*
 * citus_stat_counters_reset resets Citus stat counters for given database
 * id or for the current database if InvalidOid is provided.
 *
 * If a valid database id is provided, stat counters for that database are
 * reset, even if it was dropped later.
 *
 * Otherwise, if the provided database id is not valid, then the function
 * effectively does nothing.
 */
Datum
citus_stat_counters_reset(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/*
	 * Function's sql definition allows Postgres to silently
	 * ignore NULL, but we still check.
	 */
	PG_ENSURE_ARGNOTNULL(0, "database_id");
	Oid databaseId = PG_GETARG_OID(0);

	/*
	 * If the database id is InvalidOid, then we assume that
	 * the caller wants to reset the stat counters for the
	 * current database.
	 */
	if (databaseId == InvalidOid)
	{
		databaseId = MyDatabaseId;
	}

	/* just to be on the safe side */
	if (!EnsureStatCountersShmemInitDone())
	{
		PG_RETURN_VOID();
	}

	bool foundAnyBackendsForDb = ResetActiveBackendStats(databaseId);

	/*
	 * Even when we don't have an entry for the given database id in the
	 * saved backend stats hash table, we still want to create one for
	 * it to save the resetTimestamp if we currently have at least backend
	 * connected to it. By providing foundAnyBackendsForDb, we effectively
	 * let the function do that. Since ResetActiveBackendStats() doesn't
	 * filter the active backends, foundAnyBackendsForDb being true
	 * not always means that at least one backend is connected to it right
	 * now, but it means that we had such a backend at some point in time
	 * since the last server restart. If all backends refered to in the
	 * shared array are already exited, then we should already have an
	 * entry for it in the saved backend stats hash table, so providing
	 * a "true" wouldn't do anything in that case. Otherwise, if at least
	 * one backend is still connected to it, providing a "true" will
	 * effectively create a new entry for it if it doesn't exist yet,
	 * which is what we actually want to do.
	 *
	 * That way, we can save the resetTimestamp for the active backends
	 * into the relevant entry of the saved backend stats hash table.
	 * Note that we don't do that for the databases that don't have
	 * any active backends connected to them because we actually don't
	 * reset anything for such databases.
	 */
	ResetSavedBackendStats(databaseId, foundAnyBackendsForDb);

	PG_RETURN_VOID();
}


/*
 * InitializeStatCountersShmem saves the previous shmem_startup_hook and sets
 * up a new shmem_startup_hook for initializing the shared memory data structures
 * used for keeping track of stat counters.
 */
void
InitializeStatCountersShmem(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = StatCountersShmemInit;
}


/*
 * StatCountersShmemSize calculates and returns shared memory size
 * required for the shared memory data structures used for keeping track of
 * stat counters.
 */
Size
StatCountersShmemSize(void)
{
	Size backendStatsSlotArraySize = SharedBackendStatsSlotArrayShmemSize();
	Size savedBackendStatsHashLockSize = MAXALIGN(sizeof(LWLockId));
	Size savedBackendStatsHashSize = hash_estimate_size(
		SAVED_BACKEND_STATS_HASH_MAX_DATABASES, sizeof(SavedBackendStatsHashEntry));

	return add_size(add_size(backendStatsSlotArraySize, savedBackendStatsHashLockSize),
					savedBackendStatsHashSize);
}


/*
 * IncrementStatCounterForMyDb increments the stat counter for the given statId
 * for this backend.
 */
void
IncrementStatCounterForMyDb(int statId)
{
	if (!EnableStatCounters)
	{
		return;
	}

	/* just to be on the safe side */
	if (!EnsureStatCountersShmemInitDone())
	{
		return;
	}

	int myBackendSlotIdx = getProcNo_compat(MyProc);
	BackendStatsSlot *myBackendStatsSlot =
		&SharedBackendStatsSlotArray[myBackendSlotIdx];

	/*
	 * When there cannot be any other writers, incrementing an atomic
	 * counter via pg_atomic_read_u64() and pg_atomic_write_u64() is
	 * same as incrementing it via pg_atomic_fetch_add_u64(). Plus, the
	 * former is cheaper than the latter because the latter has to do
	 * extra work to deal with concurrent writers.
	 *
	 * In our case, the only concurrent writer could be the backend that
	 * is executing citus_stat_counters_reset(). So, there is chance that
	 * we read the counter value, then it gets reset by a concurrent call
	 * made to citus_stat_counters_reset() and then we write the
	 * incremented value back, by effectively overriding the reset value.
	 * But this should be a rare case and we can live with that, for the
	 * sake of lock-free implementation of this function.
	 */
	pg_atomic_uint64 *statPtr = &myBackendStatsSlot->counters[statId];
	pg_atomic_write_u64(statPtr, pg_atomic_read_u64(statPtr) + 1);
}


/*
 * SaveBackendStatsIntoSavedBackendStatsHash saves the stat counters
 * for this backend into the saved backend stats hash table.
 *
 * So, this is only supposed to be called when a backend exits.
 *
 * Also, we do our best to avoid throwing errors in this function because
 * this function is called when a backend is exiting and throwing errors
 * at that point will cause the backend to crash.
 */
void
SaveBackendStatsIntoSavedBackendStatsHash(void)
{
	if (!EnableStatCounters)
	{
		return;
	}

	/* just to be on the safe side */
	if (!EnsureStatCountersShmemInitDone())
	{
		return;
	}

	Oid databaseId = MyDatabaseId;

	LWLockAcquire(*SharedSavedBackendStatsHashLock, LW_SHARED);

	SavedBackendStatsHashEntry *dbSavedBackendStatsEntry =
		(SavedBackendStatsHashEntry *) hash_search(
			SharedSavedBackendStatsHash,
			(void *) &databaseId,
			HASH_FIND,
			NULL);
	if (!dbSavedBackendStatsEntry)
	{
		/* promote the lock to exclusive to insert the new entry for this database */
		LWLockRelease(*SharedSavedBackendStatsHashLock);
		LWLockAcquire(*SharedSavedBackendStatsHashLock, LW_EXCLUSIVE);

		dbSavedBackendStatsEntry =
			SavedBackendStatsHashEntryCreateIfNotExists(databaseId);

		LWLockRelease(*SharedSavedBackendStatsHashLock);

		if (!dbSavedBackendStatsEntry)
		{
			/*
			 * Couldn't allocate a new hash entry because we're out of
			 * (shared) memory. In that case, we just log a warning and
			 * return, instead of throwing an error due to the reasons
			 * mentioned in function's comment.
			 */
			ereport(WARNING,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("failed to allocate saved backend stats hash entry")));
			return;
		}

		/* re-acquire the shared lock */
		LWLockAcquire(*SharedSavedBackendStatsHashLock, LW_SHARED);
	}

	int myBackendSlotIdx = getProcNo_compat(MyProc);
	BackendStatsSlot *myBackendStatsSlot =
		&SharedBackendStatsSlotArray[myBackendSlotIdx];

	SpinLockAcquire(&dbSavedBackendStatsEntry->mutex);

	for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
	{
		dbSavedBackendStatsEntry->counters[statIdx] +=
			pg_atomic_read_u64(&myBackendStatsSlot->counters[statIdx]);

		/*
		 * Given that this function is only called when a backend exits, later on
		 * another backend might be assigned to the same slot. So, we reset each
		 * stat counter of this slot to 0 after saving it.
		 */
		pg_atomic_write_u64(&myBackendStatsSlot->counters[statIdx], 0);
	}

	SpinLockRelease(&dbSavedBackendStatsEntry->mutex);

	LWLockRelease(*SharedSavedBackendStatsHashLock);
}


/*
 * StatCountersShmemInit initializes the shared memory data structures used
 * for keeping track of stat counters.
 */
static void
StatCountersShmemInit(void)
{
	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	bool sharedBackendStatsSlotArrayAlreadyInit = false;
	SharedBackendStatsSlotArray = (BackendStatsSlot *)
								  ShmemInitStruct("Citus Shared Backend Stats Slot Array",
												  SharedBackendStatsSlotArrayShmemSize(),
												  &sharedBackendStatsSlotArrayAlreadyInit);

	bool sharedSavedBackendStatsHashLockAlreadyInit = false;
	SharedSavedBackendStatsHashLock = ShmemInitStruct(
		SAVED_BACKEND_STATS_HASH_LOCK_TRANCHE_NAME,
		sizeof(LWLockId),
		&
		sharedSavedBackendStatsHashLockAlreadyInit);

	HASHCTL hashInfo = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(SavedBackendStatsHashEntry),
		.hash = oid_hash,
	};
	SharedSavedBackendStatsHash = ShmemInitHash("Citus Shared Saved Backend Stats Hash",
												SAVED_BACKEND_STATS_HASH_INIT_DATABASES,
												SAVED_BACKEND_STATS_HASH_MAX_DATABASES,
												&hashInfo,
												HASH_ELEM | HASH_FUNCTION);

	Assert(sharedBackendStatsSlotArrayAlreadyInit ==
		   sharedSavedBackendStatsHashLockAlreadyInit);
	if (!sharedBackendStatsSlotArrayAlreadyInit)
	{
		for (int backendSlotIdx = 0; backendSlotIdx < MaxBackends; ++backendSlotIdx)
		{
			BackendStatsSlot *backendStatsSlot =
				&SharedBackendStatsSlotArray[backendSlotIdx];

			for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
			{
				pg_atomic_init_u64(&backendStatsSlot->counters[statIdx], 0);
			}
		}

		*SharedSavedBackendStatsHashLock = &(
			GetNamedLWLockTranche(
				SAVED_BACKEND_STATS_HASH_LOCK_TRANCHE_NAME)
			)->lock;
	}

	LWLockRelease(AddinShmemInitLock);

	/*
	 * At this point, they should have been set to non-null values already,
	 * but we still check them just to be sure.
	 */
	if (SharedBackendStatsSlotArray &&
		SharedSavedBackendStatsHashLock &&
		SharedSavedBackendStatsHash)
	{
		StatCountersShmemInitDone = true;
	}
}


/*
 * SharedBackendStatsSlotArrayShmemSize returns the size of the shared
 * backend stats slot array.
 */
static Size
SharedBackendStatsSlotArrayShmemSize(void)
{
	return mul_size(sizeof(BackendStatsSlot), MaxBackends);
}


/*
 * CollectActiveBackendStatsIntoHTAB aggregates the stat counters for the
 * given database id from all the active backends into the databaseStats
 * hash table. The function doesn't actually filter the slots of active
 * backends but it's just fine to read the stat counters from all because
 * exited backends anyway zero out their stat counters when they exit.
 *
 * If the database id is InvalidOid, then all the active backends will be
 * considered regardless of the database they are connected to.
 *
 * Otherwise, if the database id is different than InvalidOid, then only
 * the active backends whose PGPROC->databaseId is the same as the given
 * database id will be considered, if any.
 */
static void
CollectActiveBackendStatsIntoHTAB(Oid databaseId, HTAB *databaseStats)
{
	for (int backendSlotIdx = 0; backendSlotIdx < MaxBackends; ++backendSlotIdx)
	{
		PGPROC *backendProc = GetPGProcByNumber(backendSlotIdx);

		if (backendProc->pid == 0)
		{
			/* unused slot */
			continue;
		}

		Oid procDatabaseId = backendProc->databaseId;
		if (procDatabaseId == InvalidOid)
		{
			/*
			 * Not connected to any database, something like logical replication
			 * launcher, autovacuum launcher or such.
			 */
			continue;
		}

		if (databaseId != InvalidOid && databaseId != procDatabaseId)
		{
			/* not a database we are interested in */
			continue;
		}

		DatabaseStatsHashEntry *dbStatsEntry =
			DatabaseStatsHashEntryFindOrCreate(procDatabaseId, databaseStats);

		BackendStatsSlot *backendStatsSlot =
			&SharedBackendStatsSlotArray[backendSlotIdx];

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			dbStatsEntry->counters[statIdx] +=
				pg_atomic_read_u64(&backendStatsSlot->counters[statIdx]);
		}
	}
}


/*
 * CollectSavedBackendStatsIntoHTAB fetches the saved stat counters and
 * resetTimestamp for the given database id from the saved backend stats
 * hash table and saves them into the databaseStats hash table.
 *
 * If the database id is InvalidOid, then all the databases that present
 * in the saved backend stats hash table will be considered.
 *
 * Otherwise, if the database id is different than InvalidOid, then only
 * the entry that belongs to given database will be considered, if there
 * is such an entry.
 */
static void
CollectSavedBackendStatsIntoHTAB(Oid databaseId, HTAB *databaseStats)
{
	LWLockAcquire(*SharedSavedBackendStatsHashLock, LW_SHARED);

	if (databaseId != InvalidOid)
	{
		SavedBackendStatsHashEntry *dbSavedBackendStatsEntry =
			(SavedBackendStatsHashEntry *) hash_search(
				SharedSavedBackendStatsHash,
				(void *) &databaseId,
				HASH_FIND,
				NULL);

		if (dbSavedBackendStatsEntry)
		{
			DatabaseStatsHashEntry *dbStatsEntry =
				DatabaseStatsHashEntryFindOrCreate(databaseId, databaseStats);

			SpinLockAcquire(&dbSavedBackendStatsEntry->mutex);

			for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
			{
				dbStatsEntry->counters[statIdx] +=
					dbSavedBackendStatsEntry->counters[statIdx];
			}

			dbStatsEntry->resetTimestamp =
				dbSavedBackendStatsEntry->resetTimestamp;

			SpinLockRelease(&dbSavedBackendStatsEntry->mutex);
		}
	}
	else
	{
		HASH_SEQ_STATUS hashSeqStatus;
		hash_seq_init(&hashSeqStatus, SharedSavedBackendStatsHash);

		SavedBackendStatsHashEntry *dbSavedBackendStatsEntry = NULL;
		while ((dbSavedBackendStatsEntry = hash_seq_search(&hashSeqStatus)) != NULL)
		{
			DatabaseStatsHashEntry *dbStatsEntry =
				DatabaseStatsHashEntryFindOrCreate(dbSavedBackendStatsEntry->databaseId,
												   databaseStats);

			SpinLockAcquire(&dbSavedBackendStatsEntry->mutex);

			for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
			{
				dbStatsEntry->counters[statIdx] +=
					dbSavedBackendStatsEntry->counters[statIdx];
			}

			dbStatsEntry->resetTimestamp =
				dbSavedBackendStatsEntry->resetTimestamp;

			SpinLockRelease(&dbSavedBackendStatsEntry->mutex);
		}
	}

	LWLockRelease(*SharedSavedBackendStatsHashLock);
}


/*
 * DatabaseStatsHashEntryFindOrCreate creates a new entry in databaseStats
 * hash table for the given database id if it doesn't already exist and
 * initializes it, or just returns the existing entry if it does.
 */
static DatabaseStatsHashEntry *
DatabaseStatsHashEntryFindOrCreate(Oid databaseId, HTAB *databaseStats)
{
	bool found = false;
	DatabaseStatsHashEntry *dbStatsEntry = (DatabaseStatsHashEntry *)
										   hash_search(databaseStats, &databaseId,
													   HASH_ENTER, &found);

	if (!found)
	{
		MemSet(dbStatsEntry->counters, 0, sizeof(StatCounters));
		dbStatsEntry->resetTimestamp = 0;
	}

	return dbStatsEntry;
}


/*
 * StoreDatabaseStatsIntoTupStore stores the database stats from the
 * databaseStats hash table into given tuple store.
 */
static void
StoreDatabaseStatsIntoTupStore(HTAB *databaseStats, Tuplestorestate *tupleStore,
							   TupleDesc tupleDescriptor)
{
	HASH_SEQ_STATUS hashSeqStatus;
	hash_seq_init(&hashSeqStatus, databaseStats);

	DatabaseStatsHashEntry *dbStatsEntry = NULL;
	while ((dbStatsEntry = hash_seq_search(&hashSeqStatus)) != NULL)
	{
		/* +2 for database_id (first) and the stats_reset (last) column */
		Datum values[N_CITUS_STAT_COUNTERS + 2] = { 0 };
		bool isNulls[N_CITUS_STAT_COUNTERS + 2] = { 0 };

		values[0] = ObjectIdGetDatum(dbStatsEntry->databaseId);

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			uint64 statCounter = dbStatsEntry->counters[statIdx];
			values[statIdx + 1] = UInt64GetDatum(statCounter);
		}

		/* set stats_reset column to NULL if it was never reset */
		if (dbStatsEntry->resetTimestamp == 0)
		{
			isNulls[N_CITUS_STAT_COUNTERS + 1] = true;
		}
		else
		{
			values[N_CITUS_STAT_COUNTERS + 1] =
				TimestampTzGetDatum(dbStatsEntry->resetTimestamp);
		}

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
}


/*
 * ResetActiveBackendStats resets the stat counters for the given database
 * id for all the active backends. The function doesn't actually filter the
 * slots of active backends but it's just fine to reset the stat counters
 * for all because doing so just means resetting the stat counters for
 * exited backends once again, which were already reset when they exited.
 *
 * Only active backends whose PGPROC->databaseId is the same as the given
 * database id will be considered, if any.
 *
 * Returns true if any active backend was found.
 */
static bool
ResetActiveBackendStats(Oid databaseId)
{
	bool foundAny = false;

	for (int backendSlotIdx = 0; backendSlotIdx < MaxBackends; ++backendSlotIdx)
	{
		PGPROC *backendProc = GetPGProcByNumber(backendSlotIdx);

		if (backendProc->pid == 0)
		{
			/* unused slot */
			continue;
		}

		Oid procDatabaseId = backendProc->databaseId;
		if (procDatabaseId == InvalidOid)
		{
			/*
			 * not connected to any database, something like logical replication
			 * launcher, autovacuum launcher, etc.
			 */
			continue;
		}

		if (databaseId != procDatabaseId)
		{
			/* not a database we are interested in */
			continue;
		}

		foundAny = true;

		BackendStatsSlot *backendStatsSlot =
			&SharedBackendStatsSlotArray[backendSlotIdx];

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			pg_atomic_write_u64(&backendStatsSlot->counters[statIdx], 0);
		}
	}

	return foundAny;
}


/*
 * ResetSavedBackendStats resets the saved stat counters for the given
 * database id and sets the resetTimestamp for it to the current timestamp.
 *
 * If force is true, then we first make sure that we have an entry for
 * the given database id in the saved backend stats hash table.
 */
static void
ResetSavedBackendStats(Oid databaseId, bool force)
{
	LWLockAcquire(*SharedSavedBackendStatsHashLock, LW_SHARED);

	SavedBackendStatsHashEntry *dbSavedBackendStatsEntry =
		(SavedBackendStatsHashEntry *) hash_search(
			SharedSavedBackendStatsHash,
			(void *) &databaseId,
			HASH_FIND,
			NULL);

	if (!dbSavedBackendStatsEntry && force)
	{
		/* promote the lock to exclusive to insert the new entry for this database */
		LWLockRelease(*SharedSavedBackendStatsHashLock);
		LWLockAcquire(*SharedSavedBackendStatsHashLock, LW_EXCLUSIVE);

		dbSavedBackendStatsEntry =
			SavedBackendStatsHashEntryCreateIfNotExists(databaseId);

		LWLockRelease(*SharedSavedBackendStatsHashLock);

		if (!dbSavedBackendStatsEntry)
		{
			/*
			 * Couldn't allocate a new hash entry because we're out of
			 * (shared) memory.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("failed to allocate saved backend stats hash entry")));
			return;
		}

		/* re-acquire the shared lock */
		LWLockAcquire(*SharedSavedBackendStatsHashLock, LW_SHARED);
	}

	/*
	 * Actually reset the stat counters for the exited backends and set
	 * the resetTimestamp to the current timestamp if we already had
	 * an entry for it or if we just created it.
	 */
	if (dbSavedBackendStatsEntry)
	{
		SpinLockAcquire(&dbSavedBackendStatsEntry->mutex);

		memset(dbSavedBackendStatsEntry->counters, 0, sizeof(StatCounters));
		dbSavedBackendStatsEntry->resetTimestamp = GetCurrentTimestamp();

		SpinLockRelease(&dbSavedBackendStatsEntry->mutex);
	}

	LWLockRelease(*SharedSavedBackendStatsHashLock);
}


/*
 * SavedBackendStatsHashEntryCreateIfNotExists creates a new entry in the
 * saved backend stats hash table for the given database id if it doesn't
 * already exist and initializes it.
 *
 * Assumes that the caller has exclusive access to the hash table since it
 * performs HASH_ENTER_NULL.
 *
 * Returns NULL if the entry didn't exist and couldn't be allocated since
 * we're out of (shared) memory.
 */
static SavedBackendStatsHashEntry *
SavedBackendStatsHashEntryCreateIfNotExists(Oid databaseId)
{
	bool found = false;
	SavedBackendStatsHashEntry *dbSavedBackendStatsEntry =
		(SavedBackendStatsHashEntry *) hash_search(SharedSavedBackendStatsHash,
												   (void *) &databaseId,
												   HASH_ENTER_NULL,
												   &found);

	if (!dbSavedBackendStatsEntry)
	{
		/*
		 * As we provided HASH_ENTER_NULL, returning NULL means OOM.
		 * In that case, we return and let the caller decide what to do.
		 */
		return NULL;
	}

	if (!found)
	{
		memset(dbSavedBackendStatsEntry->counters, 0, sizeof(StatCounters));

		dbSavedBackendStatsEntry->resetTimestamp = 0;

		SpinLockInit(&dbSavedBackendStatsEntry->mutex);
	}

	return dbSavedBackendStatsEntry;
}
