/*-------------------------------------------------------------------------
 *
 * cluster_version.h
 *	  Declarations for the cluster-wide Citus version cache kept in shared
 *	  memory.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CLUSTER_VERSION_H
#define CLUSTER_VERSION_H

#include "postgres.h"


/* GUC: maintenance daemon refresh interval for the cached version, in ms (-1 disables) */
extern int ClusterVersionRefreshInterval;

/* shared memory management, called from _PG_init / citus_shmem_request */
extern Size ClusterVersionShmemSize(void);
extern void InitializeClusterVersionShmem(void);
extern void ClusterVersionShmemInit(void);

/*
 * Invalidates the cached cluster minimum version. Called from the pg_dist_node
 * relcache invalidation callback whenever the set of nodes changes, and
 * periodically by the maintenance daemon to bound staleness.
 */
extern void InvalidateClusterVersionCache(void);

#endif /* CLUSTER_VERSION_H */
