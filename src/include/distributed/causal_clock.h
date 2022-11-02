/*
 * causal_clock.h
 *
 * Data structure definitions for managing hybrid logical clock and
 * related function declarations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CAUSAL_CLOCK_H
#define CAUSAL_CLOCK_H

#include "distributed/type_utils.h"

/*
 * Clock components - Unsigned 64 bit <LC, C>
 * Logical clock   (LC): 42 bits
 * Counter          (C): 22 bits
 *
 * 2^42 milliseconds - 4398046511104 milliseconds, which is ~139 years.
 * 2^22 ticks - maximum of four million operations per millisecond.
 *
 */

#define LOGICAL_BITS 42
#define COUNTER_BITS 22
#define LOGICAL_MASK ((1U << COUNTER_BITS) - 1)

#define MAX_LOGICAL ((1LU << LOGICAL_BITS) - 1)
#define MAX_COUNTER LOGICAL_MASK

extern bool EnableClusterClock;

extern void LogicalClockShmemInit(void);
extern size_t LogicalClockShmemSize(void);
extern void InitializeClusterClockMem(void);
extern ClusterClock * GetEpochTimeAsClock(void);

#endif /* CAUSAL_CLOCK_H */
