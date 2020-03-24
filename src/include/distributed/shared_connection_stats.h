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

extern int MaxTrackedWorkerNodes;

extern void InitializeSharedConnectionStats(void);
extern bool TryToIncrementSharedConnectionCounter(const char *hostname, int port);

#endif /* SHARED_CONNECTION_STATS_H */
