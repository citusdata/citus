/*
 * cluster_clock.h
 *
 * Data structure definitions for managing cluster clock and
 * related function declarations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CLUSTER_CLOCK_H
#define CLUSTER_CLOCK_H

extern uint64 GetEpochTimeMs(void);
extern bool GetLocalClockValue(uint64 *savedValue);
extern void PersistLocalClockValue(int code, Datum argUnused);
extern void SetTransactionClusterClock(List *connectionList);

#endif /* CLUSTER_CLOCK_H */
