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

#define DEFAULT_STAT_COUNTER_SLOTS 16

#define MAX_STAT_NAME_LENGTH 255

typedef enum
{
	STAT_CONNECTION_ESTABLISHMENT_SUCCEEDED,
	STAT_CONNECTION_ESTABLISHMENT_FAILED,
	STAT_CONNECTION_REUSED,

	/* do not use this and ensure it is the last entry */
	N_CITUS_STAT_COUNTERS
} StatType;

extern int StatCounterSlots;

typedef pg_atomic_uint64 CitusAtomicStatCounters[N_CITUS_STAT_COUNTERS];

extern void InitializeStatCountersArrayMem(void);
extern Size StatCountersArrayShmemSize(void);
extern void IncrementStatCounter(int statId);

#endif /* STAT_COUNTERS_H */
