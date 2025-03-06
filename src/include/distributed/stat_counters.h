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

#if PG_VERSION_NUM >= 170000
#include <storage/proc.h>
#else
#include <storage/backendid.h>
#endif
#include <port/atomics.h>

#define MAX_STAT_NAME_LENGTH 255
#define MAX_STAT_COUNT 4

/* do not use MAX_STAT_INDEX and ensure it is the last entry */
typedef enum
{
	STAT_CONNECTION_ESTABLISHMENT_SUCCEEDED,
	STAT_CONNECTION_ESTABLISHMENT_FAILED,
	STAT_CONNECTION_REUSED,

	/* this must be the last value in the StatType */
	MAX_STAT_INDEX
} StatType;

typedef pg_atomic_uint64 CitusAtomicStatCounters[MAX_STAT_COUNT];

extern void InitializeStatCountersArrayMem(void);
extern Size StatCountersArrayShmemSize(void);
extern void IncrementStatCounter(int statId);

#endif /* STAT_COUNTERS_H */
