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

enum SharedPoolCounterMode
{
	/*
	 * Use this flag to reserve a connection from a maintenance quota
	 */
	MAINTENANCE_CONNECTION = 1 << 0
};

extern int MaxSharedPoolSize;
extern int MaxMaintenanceSharedPoolSize;
extern int MaintenanceConnectionPoolTimeout;
extern int LocalSharedPoolSize;
extern int MaxClientConnections;


extern void InitializeSharedConnectionStats(void);
extern void WaitForSharedConnection(uint32);
extern void WakeupWaiterBackendsForSharedConnection(uint32);
extern size_t SharedConnectionStatsShmemSize(void);
extern void SharedConnectionStatsShmemInit(void);
extern int GetMaxClientConnections(void);
extern int GetMaxSharedPoolSize(void);
extern int GetMaxMaintenanceSharedPoolSize(void);
extern int GetLocalSharedPoolSize(void);
extern bool TryToIncrementSharedConnectionCounter(uint32 flags, const char *hostname,
												  int port);
extern void WaitLoopForSharedConnection(uint32 flags, const char *hostname, int port);
extern void DecrementSharedConnectionCounter(uint32 externalFlags, const char *hostname,
											 int port);
extern void IncrementSharedConnectionCounter(uint32 flags, const char *hostname,
											 int port);
extern int AdaptiveConnectionManagementFlag(bool connectToLocalNode,
											int activeConnectionCount);

#endif /* SHARED_CONNECTION_STATS_H */
