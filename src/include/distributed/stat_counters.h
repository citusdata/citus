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

#include <port/atomics.h>

#define STAT_COUNTERS_STATE_LOCK_TRANCHE_NAME "citus_stat_counters_lock_tranche"

/*
 * Must be in the same order as the columns defined in citus_stat_counters view,
 * see src/backend/distributed/sql/udfs/citus_stat_counters/latest.sql
 */
typedef enum
{
	STAT_CONNECTION_ESTABLISHMENT_SUCCEEDED,
	STAT_CONNECTION_ESTABLISHMENT_FAILED,
	STAT_CONNECTION_REUSED,

	STAT_QUERY_EXECUTION_SINGLE_SHARD,
	STAT_QUERY_EXECUTION_MULTI_SHARD,

	/* do not use this and ensure it is the last entry */
	N_CITUS_STAT_COUNTERS
} StatType;

extern bool EnableStatCounters;

extern void InitializeStatCountersArrayMem(void);
extern Size StatCountersArrayShmemSize(void);
extern void IncrementStatCounter(int statId);

#endif /* STAT_COUNTERS_H */
