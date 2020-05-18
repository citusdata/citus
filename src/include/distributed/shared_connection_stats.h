/*-------------------------------------------------------------------------
 *
 * shared_connection_stats.h
 *   Central management of connections and their life-cycle
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARED_CONNECTION_STATS_H
#define SHARED_CONNECTION_STATS_H

extern int MaxSharedPoolSize;


extern void InitializeSharedConnectionStats(void);
extern void WaitForSharedConnection(void);
extern void WakeupWaiterBackendsForSharedConnection(void);
extern int GetMaxSharedPoolSize(void);
extern bool TryToIncrementSharedConnectionCounter(const char *hostname, int port);
extern void WaitLoopForSharedConnection(const char *hostname, int port);
extern void DecrementSharedConnectionCounter(const char *hostname, int port);
extern void IncrementSharedConnectionCounter(const char *hostname, int port);
extern int ConnectionFlagForSharedConnectionStats(int currentSessionConnectionCount);

#endif /* SHARED_CONNECTION_STATS_H */
