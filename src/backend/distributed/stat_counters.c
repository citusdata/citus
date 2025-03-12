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

#include "storage/ipc.h"
#include "utils/builtins.h"

#include "distributed/backend_data.h"
#include "distributed/stat_counters.h"

typedef uint64 CitusStatCounters[N_CITUS_STAT_COUNTERS];

/* GUC value for citus.stat_counter_slots */
int StatCounterSlots = DEFAULT_STAT_COUNTER_SLOTS;

/* shared memory init & management */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static void SharedStatCountersArrayShmemInit(void);

/* pointer to the shared memory array for stat counters */
CitusAtomicStatCounters *SharedStatCountersArray = NULL;

/* other helper functions */
static void StoreAllStatCounters(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor);
static void AggregateStatCountersInto(CitusStatCounters *aggregatedStatCounters);
static Tuplestorestate * SetupStatCountersTuplestore(FunctionCallInfo fcinfo,
													 TupleDesc *tupleDescriptor);
static void ResetStatCounters(void);
static int GetStatCounterSlots(void);
static bool IsStatCountersEnabled(void);

/*
 * Keep this in sync with StatType enum in stat_counters.h.
 * For each StatType enum a StatMapping entry should exist.
 */
static char StatMapping[N_CITUS_STAT_COUNTERS][MAX_STAT_NAME_LENGTH] = {
	[STAT_CONNECTION_ESTABLISHMENT_SUCCEEDED] = "connection_establishment_succeeded",
	[STAT_CONNECTION_ESTABLISHMENT_FAILED] = "connection_establishment_failed",
	[STAT_CONNECTION_REUSED] = "connection_reused",

	[STAT_QUERY_EXECUTION_SINGLE_SHARD] = "query_execution_single_shard",
	[STAT_QUERY_EXECUTION_MULTI_SHARD] = "query_execution_multi_shard",
};


PG_FUNCTION_INFO_V1(citus_stat_counters);
PG_FUNCTION_INFO_V1(citus_stat_counters_reset);


/*
 * citus_stat_counters returns all the available information about all
 * Citus stat counters.
 */
Datum
citus_stat_counters(PG_FUNCTION_ARGS)
{
	if (!IsStatCountersEnabled())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("citus stat counters are not enabled")));
		PG_RETURN_VOID();
	}

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupStatCountersTuplestore(fcinfo, &tupleDescriptor);

	StoreAllStatCounters(tupleStore, tupleDescriptor);

	PG_RETURN_VOID();
}


/*
 * citus_stat_counters_reset resets all the Citus stat counters for all backends.
 */
Datum
citus_stat_counters_reset(PG_FUNCTION_ARGS)
{
	if (!IsStatCountersEnabled())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("citus stat counters are not enabled")));
		PG_RETURN_VOID();
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
	return mul_size(sizeof(CitusAtomicStatCounters), GetStatCounterSlots());
}


/*
 * IncrementStatCounter increments the stat counter for the given statId
 * for this backend.
 */
void
IncrementStatCounter(int statId)
{
	if (!IsStatCountersEnabled())
	{
		return;
	}

#if PG_VERSION_NUM >= 170000
	int backendSlotIdx = MyProcNumber;
#else
	int backendSlotIdx = MyBackendId - 1;
#endif

	backendSlotIdx = backendSlotIdx % GetStatCounterSlots();

	pg_atomic_fetch_add_u64(&SharedStatCountersArray[backendSlotIdx][statId], 1);
}


/*
 * SetupStatCountersTuplestore returns a Tuplestorestate for returning the
 * stat counters aggregated across all the backends.
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
 * counters aggregated across all the backends into the given tuple store.
 */
static void
StoreAllStatCounters(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	Datum values[2] = { 0 };
	bool isNulls[2] = { 0 };

	CitusStatCounters aggregatedStatCounters;
	MemSet(aggregatedStatCounters, 0, sizeof(CitusStatCounters));

	AggregateStatCountersInto(&aggregatedStatCounters);

	for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
	{
		values[0] = PointerGetDatum(cstring_to_text(StatMapping[statIdx]));
		values[1] = Int64GetDatum(aggregatedStatCounters[statIdx]);
		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
}


/*
 * AggregateStatCountersInto aggregates the stat counters of all the backends into
 * the given aggregatedStatCounters.
 */
static void
AggregateStatCountersInto(CitusStatCounters *aggregatedStatCounters)
{
	for (int backendSlotIdx = 0; backendSlotIdx < GetStatCounterSlots(); ++backendSlotIdx)
	{
		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			(*aggregatedStatCounters)[statIdx] +=
				pg_atomic_read_u64(&SharedStatCountersArray[backendSlotIdx][statIdx]);
		}
	}
}


/*
 * ResetStatCounters resets all the stat counters for all the backends.
 */
static void
ResetStatCounters(void)
{
	/*
	 * Some stats might be lost between reading the stats for all the backend processes
	 * and resetting the stats. However, we are okay with this since we don't want to block
	 * the client backends that might be incrementing the stats.
	 */
	for (int backendSlotIdx = 0; backendSlotIdx < GetStatCounterSlots(); ++backendSlotIdx)
	{
		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			pg_atomic_write_u64(&SharedStatCountersArray[backendSlotIdx][statIdx], 0);
		}
	}
}


/*
 * SharedStatCountersArrayShmemInit initializes the shared memory used
 * for keeping track of stat counters across backends.
 */
static void
SharedStatCountersArrayShmemInit(void)
{
	/* initialize the shared memory only if the stat counters are enabled */
	if (IsStatCountersEnabled())
	{
		/* validate that we have names for the stat counters as well */
		for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
		{
			if (strlen(StatMapping[statIdx]) == 0)
			{
				ereport(PANIC, (errmsg("Stat mapping for index %d not found", statIdx)));
			}
		}

		bool alreadyInitialized;

		size_t statCountersSharedArrayShmemSize = StatCountersArrayShmemSize();

		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

		SharedStatCountersArray = (CitusAtomicStatCounters *)
								  ShmemInitStruct("Citus Stat Counters Array",
												  statCountersSharedArrayShmemSize,
												  &alreadyInitialized);

		if (!alreadyInitialized)
		{
			for (int backendSlotIdx = 0; backendSlotIdx < GetStatCounterSlots();
				 ++backendSlotIdx)
			{
				for (int statIdx = 0; statIdx < N_CITUS_STAT_COUNTERS; statIdx++)
				{
					pg_atomic_init_u64(&SharedStatCountersArray[backendSlotIdx][statIdx],
									   0);
				}
			}
		}

		LWLockRelease(AddinShmemInitLock);
	}

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * IsStatCountersEnabled returns whether the stat counters are enabled.
 */
static bool
IsStatCountersEnabled(void)
{
	return StatCounterSlots >= 0;
}


/*
 * GetStatCounterSlots returns the number of slots to store stat counters.
 *
 * Guarantees to return the same value in the run-time because StatCounterSlots
 * enforces PGC_POSTMASTER.
 */
static int
GetStatCounterSlots(void)
{
	if (StatCounterSlots == 0)
	{
		return MaxBackends;
	}
	else if (StatCounterSlots > 0)
	{
		return StatCounterSlots;
	}
	else
	{
		Assert(StatCounterSlots == -1);
		return 0;
	}
}
