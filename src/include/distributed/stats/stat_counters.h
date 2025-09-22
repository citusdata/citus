/*-------------------------------------------------------------------------
 *
 * stat_counters.h
 *
 * This file contains the exported functions to track various statistic
 * counters for Citus.
 *
 * -------------------------------------------------------------------------
 */

#ifndef STAT_COUNTERS_H
#define STAT_COUNTERS_H


/* saved backend stats - constants */
#define SAVED_BACKEND_STATS_HASH_LOCK_TRANCHE_NAME \
	"citus_stat_counters saved backend stats hash"

/* default value for the GUC variable */
#define ENABLE_STAT_COUNTERS_DEFAULT false


/*
 * Must be in the same order as the output columns defined in citus_stat_counters() UDF,
 * see src/backend/distributed/sql/udfs/citus_stat_counters/latest.sql
 */
typedef enum
{
	/*
	 * These are mainly tracked by connection_management.c and
	 * adaptive_executor.c.
	 */
	STAT_CONNECTION_ESTABLISHMENT_SUCCEEDED,
	STAT_CONNECTION_ESTABLISHMENT_FAILED,
	STAT_CONNECTION_REUSED,

	/*
	 * These are maintained by ExecCustomScan methods implemented
	 * for CustomScan nodes provided by Citus to account for actual
	 * execution of the queries and subplans. By maintaining these
	 * counters in ExecCustomScan callbacks, we ensure avoid
	 * incrementing them for plain EXPLAIN (i.e., without ANALYZE).
	 * queries. And, prefering the executor methods rather than the
	 * planner methods helps us capture the execution of prepared
	 * statements too.
	 */
	STAT_QUERY_EXECUTION_SINGLE_SHARD,
	STAT_QUERY_EXECUTION_MULTI_SHARD,

	/* do not use this and ensure it is the last entry */
	N_CITUS_STAT_COUNTERS
} StatType;


/* GUC variable */
extern bool EnableStatCounters;


/* shared memory init */
extern void InitializeStatCountersShmem(void);
extern Size StatCountersShmemSize(void);

/* main entry point for the callers who want to increment the stat counters */
extern void IncrementStatCounterForMyDb(int statId);

/*
 * Exported to define a before_shmem_exit() callback that saves
 * the stat counters for exited backends into the shared memory.
 */
extern void SaveBackendStatsIntoSavedBackendStatsHash(void);

#endif /* STAT_COUNTERS_H */
