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

#define ADJUST_POOLSIZE_AUTOMATICALLY 0
#define DISABLE_CONNECTION_THROTTLING -1
#define DISABLE_REMOTE_CONNECTIONS_FOR_LOCAL_QUERIES -1
#define ALLOW_ALL_EXTERNAL_CONNECTIONS -1


extern int MaxSharedPoolSize;
extern int LocalSharedPoolSize;
extern int MaxClientConnections;


extern void InitializeSharedConnectionStats(void);
extern void WaitForSharedConnection(void);
extern void WakeupWaiterBackendsForSharedConnection(void);
extern size_t SharedConnectionStatsShmemSize(void);
extern void SharedConnectionStatsShmemInit(void);
extern int GetMaxClientConnections(void);
extern int GetMaxSharedPoolSize(void);
extern int GetLocalSharedPoolSize(void);
extern bool TryToIncrementSharedConnectionCounter(const char *hostname, int port);
extern void WaitLoopForSharedConnection(const char *hostname, int port);
extern void DecrementSharedConnectionCounter(const char *hostname, int port);
extern void IncrementSharedConnectionCounter(const char *hostname, int port);
extern int AdaptiveConnectionManagementFlag(bool connectToLocalNode, int
											activeConnectionCount);

#endif /* SHARED_CONNECTION_STATS_H */
