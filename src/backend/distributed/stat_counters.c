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
 * SaveBackendStatsIntoExitedBackendStatsHash() into a hash table in
 * shared memory, whose entries are "ExitedBackendStatsHashEntry"s and
 * the key is the database id. In other words, each entry of the hash
 * table is used to aggregate the stat counters for backends that were
 * connected to that database and exited since the last server restart.
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
 * stored in the shared hash table. Also, although we have resetTimestamp
 * as part of "ExitedBackendStatsHashEntry"s, it doesn't only represent
 * the reset timestamp for exited backends' stat counters but also for
 * the active backends. This is because, when citus_stat_counters_reset()
 * is called, we reset the stat counters for both active backends and the
 * exited backends whose stats are stored in the shared hash table.
 * Similarly, when citus_stat_counters() is called, we just report
 * resetTimestamp as stats_reset column.
 *
 * As a last note, there is chance that citus_stat_counters_reset() might
 * race with a backend that is trying to increment one of the counters in
 * its slot and as a result it can effectively fail to reset
 * that counter due to the reasons documented in IncrementStatCounterForMyDb()
 * function. However, this should be a very rare case and we can live with
 * that for now.
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
#include "distributed/stat_counters.h"
#include "distributed/tuplestore.h"


/*
 * exited backend stats - hash table constants
 *
 * Configurations used to create the hash table for exited backend stats.
 * The places where EXITED_BACKEND_STATS_HASH_MAX_DATABASES is used do not
 * impose a hard limit on the number of databases that can be tracked but
 * in ShmemInitHash() it's documented that the access efficiency will degrade
 * if it is exceeded substantially.
 *
 * XXX: Consider using dshash_table instead of (shared) HTAB if that becomes
 *      a concern.
 */
#define EXITED_BACKEND_STATS_HASH_INIT_DATABASES 8
#define EXITED_BACKEND_STATS_HASH_MAX_DATABASES 1024


/* fixed size array types to store the stat counters */
typedef pg_atomic_uint64 AtomicStatCounters[N_CITUS_STAT_COUNTERS];
typedef uint64 StatCounters[N_CITUS_STAT_COUNTERS];

/*
 * exited backend stats - hash entry definition
 *
 * This is used to define & access the shared hash table used to aggregate the stat
 * counters for the backends exited so far since last server restart.
 */
typedef struct ExitedBackendStatsHashEntry
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
	 * resetTimestamp doesn't only represent the reset timestamp for exited
	 * backends' stat counters but also for the active backends. This is because,
	 * when citus_stat_counters_reset() is called, we reset the stat counters for
	 * both active backends and the exited backends whose stats are aggregated
	 * in the shared hash table. Similarly, when citus_stat_counters() is called,
	 * we just report this as stats_reset column.
	 */
	StatCounters counters;
	TimestampTz resetTimestamp;
} ExitedBackendStatsHashEntry;

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
	/*
	 * Since we sue atomic counters for the stat counters in this struct,
	 * we don't alway need to lock this struct when we read / write the
	 * counters. So, the purpose of this mutex is just to prevent
	 * citus_stat_counters() to observe the same counter value twice or
	 * perhaps losing it while a backend was "saving" its stat counters
	 * into the exited backend stats hash table. For this reason, we
	 * either acquire the mutex while saving the stat counters into the
	 * shared memory or when we want to prevent them being saved & reset
	 * subsequently.
	 */
	slock_t saveMutex;

	AtomicStatCounters counters;
} BackendStatsSlot;


/*
 * GUC variable
 *
 * This only controls whether we track the stat counters or not, via
 * IncrementStatCounterForMyDb() and
 * SaveBackendStatsIntoExitedBackendStatsHash(). In other words, even
 * when the GUC is disabled, we still allocate the shared memory
 * structures etc. and citus_stat_counters() / citus_stat_counters_reset()
 * will still work.
 */
bool EnableStatCounters = ENABLE_STAT_COUNTERS_DEFAULT;

/* exited backend stats - shared memory variables */
static LWLockId *SharedExitedBackendStatsHashLock = NULL;
static HTAB *SharedExitedBackendStatsHash = NULL;

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
static void CollectDbStatsFromActiveBackendsIntoHTAB(Oid databaseId, HTAB *databaseStats);
static void CollectDbStatsFromExitedBackendsIntoHTAB(Oid databaseId, HTAB *databaseStats);
static void StoreDatabaseStatsIntoTupStore(HTAB *databaseStats,
										   Tuplestorestate *tupleStore,
										   TupleDesc tupleDescriptor);

/* helper functions for citus_stat_counters_reset() */
static void ResetStatCountersForActiveBackends(Oid databaseId);
static void ResetStatCountersForExitedBackends(Oid databaseId);

/* exited backend stats */
static ExitedBackendStatsHashEntry * ExitedBackendStatsHashEntryAllocIfNotExists(Oid
																				 dbId);


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
 * Finally, counters with 0 are returned as NULLs to make this function
 * suitable for building citus_stat_counters view.
 *
 * Throws an error if provided database id is NULL.
 */
Datum
citus_stat_counters(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

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

	CollectDbStatsFromActiveBackendsIntoHTAB(databaseId, databaseStats);
	CollectDbStatsFromExitedBackendsIntoHTAB(databaseId, databaseStats);

	StoreDatabaseStatsIntoTupStore(databaseStats, tupleStore, tupleDescriptor);

	hash_destroy(databaseStats);

	PG_RETURN_VOID();
}


/*
 * citus_stat_counters_reset resets Citus stat counters for given database
 * id.
 *
 * If a valid database id is provided, only the stat counters for that
 * database are reset, even if it was dropped later.
 *
 * If InvalidOid is provided, stat counters for all databases are reset.
 *
 * Otherwise, if the provided database id is not valid and is different than
 * InvalidOid, then the function effectively does nothing.
 *
 * Throws an error if provided database id is NULL.
 */
Datum
citus_stat_counters_reset(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "database_id");
	Oid databaseId = PG_GETARG_OID(0);

	/* just to be on the safe side */
	if (!EnsureStatCountersShmemInitDone())
	{
		PG_RETURN_VOID();
	}

	ResetStatCountersForActiveBackends(databaseId);
	ResetStatCountersForExitedBackends(databaseId);

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
	Size exitedBackendStatsHashLockSize = MAXALIGN(sizeof(LWLockId));
	Size exitedBackendStatsHashSize = hash_estimate_size(
		EXITED_BACKEND_STATS_HASH_MAX_DATABASES, sizeof(ExitedBackendStatsHashEntry));

	return add_size(add_size(backendStatsSlotArraySize, exitedBackendStatsHashLockSize),
					exitedBackendStatsHashSize);
}


/*
 * IncrementStatCounter increments the stat counter for the given statId
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
	 * Since we're sure that there cannot be any other backends that calls
	 * this function to increment the same stat counter at the same time,
	 * we use pg_atomic_read_u64() and pg_atomic_write_u64() to increment
	 * the stat counter instead of pg_atomic_fetch_add_u64().
	 * Moreover, the latter is more expensive than the former because it
	 * has to deal with concurrent writers, which we cannot have any.
	 *
	 * So, there is chance that we read the counter value, then it gets
	 * reset by a concurrent call made to citus_stat_counters_reset() and
	 * then we write the incremented value back, by effectively overriding
	 * the reset value. But this should be a rare case and we can live with
	 * that.
	 */
	pg_atomic_uint64 *statPtr = &myBackendStatsSlot->counters[statId];
	pg_atomic_write_u64(statPtr, pg_atomic_read_u64(statPtr) + 1);
}


/*
 * SaveBackendStatsIntoExitedBackendStatsHash saves the stat counters
 * for this backend into the exited backend stats hash table.
 *
 * So, this is only supposed to be called when a backend exits.
 *
 * Also, we do our best to avoid throwing errors in this function because
 * this function is called when a backend is exiting and throwing errors
 * at that point will cause the backend to crash.
 */
void
SaveBackendStatsIntoExitedBackendStatsHash(void)
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

	LWLockAcquire(*SharedExitedBackendStatsHashLock, LW_SHARED);

	ExitedBackendStatsHashEntry *dbExitedBackendStatsEntry =
		(ExitedBackendStatsHashEntry *) hash_search(
			SharedExitedBackendStatsHash,
			(void *) &databaseId,
			HASH_FIND,
			NULL);
	if (!dbExitedBackendStatsEntry)
	{
		/* promote the lock to exclusive to insert the new entry for this database */
		LWLockRelease(*SharedExitedBackendStatsHashLock);
		LWLockAcquire(*SharedExitedBackendStatsHashLock, LW_EXCLUSIVE);

		dbExitedBackendStatsEntry =
			ExitedBackendStatsHashEntryAllocIfNotExists(databaseId);

		LWLockRelease(*SharedExitedBackendStatsHashLock);

		if (!dbExitedBackendStatsEntry)
		{
			/*
			 * Couldn't allocate a new hash entry because we're out of
			 * (shared) memory. In that case, we just log a warning and
			 * return, instead of throwing an error due to the reasons
			 * mentioned in function's comment.
			 */
			ereport(WARNING,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("failed to allocate exited backend stats hash entry")));
			return;
		}

		/* re-acquire the shared lock */
		LWLockAcquire(*SharedExitedBackendStatsHashLock, LW_SHARED);
	}

	int myBackendSlotIdx = getProcNo_compat(MyProc);
	BackendStatsSlot *myBackendStatsSlot =
		&SharedBackendStatsSlotArray[myBackendSlotIdx];

	/*
	 * TODO: check lock order everywhere
	 *
	 * We want to prevent a concurrent call to citus_stat_counters() from
	 * observing the same counter value twice or perhaps losing it while
	 * we're saving our stat counters into the exited backend stats hash.
	 * So here we acquire saveMutex of the backend stats slot as well.
	 */
	SpinLockAcquire(&myBackendStatsSlot->saveMutex);
	SpinLockAcquire(&dbExitedBackendStatsEntry->mutex);

	for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
	{
		dbExitedBackendStatsEntry->counters[statIdx] +=
			pg_atomic_read_u64(&myBackendStatsSlot->counters[statIdx]);

		/*
		 * Given that this function is only called when a backend exits, later on
		 * another backend might be assigned to the same slot. So, we reset each
		 * stat counter of this slot to 0 after saving it.
		 */
		pg_atomic_write_u64(&myBackendStatsSlot->counters[statIdx], 0);
	}

	SpinLockRelease(&dbExitedBackendStatsEntry->mutex);
	SpinLockRelease(&myBackendStatsSlot->saveMutex);

	LWLockRelease(*SharedExitedBackendStatsHashLock);
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

	bool sharedExitedBackendStatsHashLockAlreadyInit = false;
	SharedExitedBackendStatsHashLock = ShmemInitStruct(
		EXITED_BACKEND_STATS_HASH_LOCK_TRANCHE_NAME,
		sizeof(LWLockId),
		&
		sharedExitedBackendStatsHashLockAlreadyInit);

	HASHCTL hashInfo = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(ExitedBackendStatsHashEntry),
		.hash = oid_hash,
	};
	SharedExitedBackendStatsHash = ShmemInitHash("Citus Shared Exited Backend Stats Hash",
												 EXITED_BACKEND_STATS_HASH_INIT_DATABASES,
												 EXITED_BACKEND_STATS_HASH_MAX_DATABASES,
												 &hashInfo,
												 HASH_ELEM | HASH_FUNCTION);

	Assert(sharedBackendStatsSlotArrayAlreadyInit ==
		   sharedExitedBackendStatsHashLockAlreadyInit);
	if (!sharedBackendStatsSlotArrayAlreadyInit)
	{
		for (int backendSlotIdx = 0; backendSlotIdx < MaxBackends; ++backendSlotIdx)
		{
			BackendStatsSlot *backendStatsSlot =
				&SharedBackendStatsSlotArray[backendSlotIdx];

			SpinLockInit(&backendStatsSlot->saveMutex);

			for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
			{
				pg_atomic_init_u64(&backendStatsSlot->counters[statIdx], 0);
			}
		}

		*SharedExitedBackendStatsHashLock = &(
			GetNamedLWLockTranche(
				EXITED_BACKEND_STATS_HASH_LOCK_TRANCHE_NAME)
			)->lock;
	}

	LWLockRelease(AddinShmemInitLock);

	/*
	 * At this point, they should have been set to non-null values already,
	 * but we still check them just to be sure.
	 */
	if (SharedBackendStatsSlotArray &&
		SharedExitedBackendStatsHashLock &&
		SharedExitedBackendStatsHash)
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
 * CollectDbStatsFromActiveBackendsIntoHTAB aggregates the stat counters
 * for the given database id from all the active backends into the
 * databaseStats hash table.
 *
 * If the database id is InvalidOid, then all the active backends will be
 * considered regardless of the database they are connected to.
 *
 * Otherwise, if the database id is different than InvalidOid, then only
 * the active backends whose PGPROC->databaseId is the same as the given
 * database id will be considered, if any.
 */
static void
CollectDbStatsFromActiveBackendsIntoHTAB(Oid databaseId, HTAB *databaseStats)
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

		bool found = false;
		DatabaseStatsHashEntry *dbStatsEntry = (DatabaseStatsHashEntry *)
											   hash_search(databaseStats, &procDatabaseId,
														   HASH_ENTER, &found);
		if (!found)
		{
			MemSet(dbStatsEntry->counters, 0, sizeof(StatCounters));
			dbStatsEntry->resetTimestamp = 0;
		}

		BackendStatsSlot *backendStatsSlot =
			&SharedBackendStatsSlotArray[backendSlotIdx];

		/*
		 * Since we want to wait for a concurrent call to
		 * SaveBackendStatsIntoExitedBackendStatsHash() to finish, we
		 * acquire the saveMutex here. We want to wait for it because
		 * otherwise we might observe the same counter value twice or
		 * perhaps lose it.
		 */
		SpinLockAcquire(&backendStatsSlot->saveMutex);

		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			dbStatsEntry->counters[statIdx] +=
				pg_atomic_read_u64(&backendStatsSlot->counters[statIdx]);
		}

		SpinLockRelease(&backendStatsSlot->saveMutex);
	}
}


/*
 * CollectDbStatsFromExitedBackendsIntoHTAB fetches the stat counters for the
 * given database id from the exited backend stats hash table and saves them
 * into the databaseStats hash table.
 *
 * If the database id is InvalidOid, then all the databases that present in the
 * exited backend stats hash table will be considered.
 *
 * Otherwise, if the database id is different than InvalidOid, then only the entry
 * that belongs to given database will be considered, if there is such an entry.
 */
static void
CollectDbStatsFromExitedBackendsIntoHTAB(Oid databaseId, HTAB *databaseStats)
{
	LWLockAcquire(*SharedExitedBackendStatsHashLock, LW_SHARED);

	if (databaseId != InvalidOid)
	{
		ExitedBackendStatsHashEntry *dbExitedBackendStatsEntry =
			(ExitedBackendStatsHashEntry *) hash_search(
				SharedExitedBackendStatsHash,
				(void *) &databaseId,
				HASH_FIND,
				NULL);

		if (dbExitedBackendStatsEntry)
		{
			DatabaseStatsHashEntry *dbStatsEntry = (DatabaseStatsHashEntry *)
												   hash_search(databaseStats, &databaseId,
															   HASH_ENTER, NULL);

			SpinLockAcquire(&dbExitedBackendStatsEntry->mutex);

			for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
			{
				dbStatsEntry->counters[statIdx] +=
					dbExitedBackendStatsEntry->counters[statIdx];
			}

			dbStatsEntry->resetTimestamp =
				dbExitedBackendStatsEntry->resetTimestamp;

			SpinLockRelease(&dbExitedBackendStatsEntry->mutex);
		}
	}
	else
	{
		HASH_SEQ_STATUS hashSeqStatus;
		hash_seq_init(&hashSeqStatus, SharedExitedBackendStatsHash);

		ExitedBackendStatsHashEntry *dbExitedBackendStatsEntry = NULL;
		while ((dbExitedBackendStatsEntry = hash_seq_search(&hashSeqStatus)) != NULL)
		{
			DatabaseStatsHashEntry *dbStatsEntry = (DatabaseStatsHashEntry *)
												   hash_search(databaseStats,
															   &dbExitedBackendStatsEntry
															   ->databaseId, HASH_ENTER,
															   NULL);

			SpinLockAcquire(&dbExitedBackendStatsEntry->mutex);

			for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
			{
				dbStatsEntry->counters[statIdx] +=
					dbExitedBackendStatsEntry->counters[statIdx];
			}

			dbStatsEntry->resetTimestamp =
				dbExitedBackendStatsEntry->resetTimestamp;

			SpinLockRelease(&dbExitedBackendStatsEntry->mutex);
		}
	}

	LWLockRelease(*SharedExitedBackendStatsHashLock);
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
			if (statCounter == 0)
			{
				isNulls[statIdx + 1] = true;
			}
			else
			{
				values[statIdx + 1] = UInt64GetDatum(statCounter);
			}
		}

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
 * ResetStatCountersForActiveBackends resets the stat counters for the
 * given database id for all the active backends.
 *
 * If the database id is InvalidOid, then all the active backends will be
 * considered regardless of the database they are connected to.
 *
 * Otherwise, if the database id is different than InvalidOid, then only
 * the active backends whose PGPROC->databaseId is the same as the given
 * database id will be considered, if any.
 */
static void
ResetStatCountersForActiveBackends(Oid databaseId)
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
			 * not connected to any database, something like logical replication
			 * launcher, autovacuum launcher, etc.
			 */
			continue;
		}

		if (databaseId != InvalidOid && databaseId != procDatabaseId)
		{
			/* not a database we are interested in */
			continue;
		}

		BackendStatsSlot *backendStatsSlot =
			&SharedBackendStatsSlotArray[backendSlotIdx];

		/*
		 * Similar to IncrementStatCounterForMyDb(), we don't have to acquire
		 * saveMutex here because we're not doing something that could cause
		 * the same counter value to be observed twice.
		 */
		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			pg_atomic_write_u64(&backendStatsSlot->counters[statIdx], 0);
		}
	}
}


/*
 * ResetStatCountersForExitedBackends resets the stat counters for the
 * given database id for all the exited backends.
 *
 * If the database id is InvalidOid, then all the databases that present in
 * the exited backend stats hash table will be considered.
 *
 * Otherwise, if the database id is different than InvalidOid, then only the
 * entry that belongs to given database will be considered, if there is such
 * an entry.
 */
static void
ResetStatCountersForExitedBackends(Oid databaseId)
{
	LWLockAcquire(*SharedExitedBackendStatsHashLock, LW_SHARED);

	if (databaseId != InvalidOid)
	{
		ExitedBackendStatsHashEntry *dbExitedBackendStatsEntry =
			(ExitedBackendStatsHashEntry *) hash_search(
				SharedExitedBackendStatsHash,
				(void *) &databaseId,
				HASH_FIND,
				NULL);

		if (dbExitedBackendStatsEntry)
		{
			SpinLockAcquire(&dbExitedBackendStatsEntry->mutex);

			memset(dbExitedBackendStatsEntry->counters, 0, sizeof(StatCounters));
			dbExitedBackendStatsEntry->resetTimestamp = GetCurrentTimestamp();

			SpinLockRelease(&dbExitedBackendStatsEntry->mutex);
		}
	}
	else
	{
		HASH_SEQ_STATUS hashSeqStatus;
		hash_seq_init(&hashSeqStatus, SharedExitedBackendStatsHash);

		ExitedBackendStatsHashEntry *dbExitedBackendStatsEntry = NULL;
		while ((dbExitedBackendStatsEntry = hash_seq_search(&hashSeqStatus)) != NULL)
		{
			SpinLockAcquire(&dbExitedBackendStatsEntry->mutex);

			memset(dbExitedBackendStatsEntry->counters, 0, sizeof(StatCounters));
			dbExitedBackendStatsEntry->resetTimestamp = GetCurrentTimestamp();

			SpinLockRelease(&dbExitedBackendStatsEntry->mutex);
		}
	}

	LWLockRelease(*SharedExitedBackendStatsHashLock);
}


/*
 * ExitedBackendStatsHashEntryAllocIfNotExists allocates a new entry in the
 * exited backend stats hash table for the given database id if it doesn't
 * already exist.
 *
 * Assumes that the caller has exclusive access to the hash table.
 *
 * Returns NULL if the entry didn't exist and couldn't be allocated since
 * we're out of (shared) memory.
 */
static ExitedBackendStatsHashEntry *
ExitedBackendStatsHashEntryAllocIfNotExists(Oid databaseId)
{
	bool councurrentlyInserted = false;
	ExitedBackendStatsHashEntry *dbExitedBackendStatsEntry =
		(ExitedBackendStatsHashEntry *) hash_search(SharedExitedBackendStatsHash,
													(void *) &databaseId,
													HASH_ENTER_NULL,
													&councurrentlyInserted);

	if (!dbExitedBackendStatsEntry)
	{
		/*
		 * As we provided HASH_ENTER_NULL, returning NULL means OOM.
		 * In that case, we return and let the caller decide what to do.
		 */
		return NULL;
	}

	/*
	 * Act only if someone else didn't race creating the entry before the
	 * caller grabbed exclusive access to the hash table.
	 */
	if (!councurrentlyInserted)
	{
		memset(dbExitedBackendStatsEntry->counters, 0, sizeof(StatCounters));

		dbExitedBackendStatsEntry->resetTimestamp = 0;

		SpinLockInit(&dbExitedBackendStatsEntry->mutex);
	}

	return dbExitedBackendStatsEntry;
}
