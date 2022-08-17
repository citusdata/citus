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

extern size_t LogicalClockShmemSize(void);

/*
 * Clock components - Unsigned 64 bit <LC, C>
 * Logical clock   (LC): 42 bits
 * Counter          (C): 22 bits
 *
 * 2^42 milliseconds - 4398046511104 milliseconds, which is ~139 years.
 * 2^22 ticks - maximum of four million operations per millisecond.
 *
 */

#define COUNTER_BITS 22
#define LOGICAL_MASK ((1U << COUNTER_BITS) - 1)
#define MAX_COUNTER LOGICAL_MASK

#define GET_LOGICAL(x) ((x) >> COUNTER_BITS)
#define GET_COUNTER(x) ((x) & LOGICAL_MASK)

/* concatenate logical and counter to form a 64 bit clock value */
#define SET_CLOCK(var, lc, c) var = (((lc) << COUNTER_BITS) | (c))

extern bool EnableClusterClock;
extern void LogicalClockShmemInit(void);
extern void InitializeClusterClockMem(void);
extern void PrepareAndSetTransactionClock(List *transactionNodeList);
extern void InitClockAtBoot(void);

#endif /* CAUSAL_CLOCK_H */
