/*-------------------------------------------------------------------------
 *
 * cluster_version.c
 *
 * UDFs to reason about the Citus version running across the whole cluster,
 * backed by a shared-memory cache of the computed minimum.
 *
 * The minimum cluster version is computed by asking every active primary node
 * for its own loaded Citus version (citus_version_num()) and taking the
 * smallest value. Because that fan-out is relatively expensive, the result is
 * cached in node-local shared memory. The cache is invalidated whenever
 * pg_dist_node changes (see InvalidateClusterVersionCache), so node additions,
 * removals and upgrades force a recompute on the next read.
 *
 * Note: PostgreSQL shared memory is node-local, so the cache only lives on the
 * node that computed it; every node maintains its own cache independently.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

#include "citus_version.h"

#include "distributed/cluster_version.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"

#define CLUSTER_VERSION_QUERY "SELECT citus_version_num()"


/*
 * GUC controlling how often the maintenance daemon invalidates the cached
 * cluster minimum version so the next reader recomputes it, in milliseconds.
 * This bounds cache staleness and lets in-place version changes (which do not
 * touch pg_dist_node) be picked up. -1 disables the periodic invalidation.
 */
int ClusterVersionRefreshInterval = 60 * 1000;


/*
 * Shared-memory cache of the cluster-wide minimum Citus version. cacheValid is
 * cleared without holding the lock from the pg_dist_node invalidation callback,
 * mirroring InvalidateNodeRelationCacheCallback; a stale read is harmless
 * because the next reader simply recomputes.
 */
typedef struct ClusterVersionShmemData
{
	NamedLWLockTranche namedLockTranche;
	LWLock lock;
	int32 cachedMinVersionNum;
	bool cacheValid;
} ClusterVersionShmemData;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ClusterVersionShmemData *ClusterVersionShmem = NULL;


PG_FUNCTION_INFO_V1(citus_minimum_cluster_version);

static int32 ComputeMinimumClusterCitusVersion(void);
static int32 RemoteNodeCitusVersion(WorkerNode *workerNode);
static text * CitusVersionNumToText(int32 versionNum);


/*
 * citus_minimum_cluster_version returns the oldest (smallest) Citus version
 * that is running on any active primary node in the cluster, formatted as a
 * human-readable "major.minor.patch" string (e.g. "15.0.0"). The value is
 * served from the shared-memory cache when valid, and recomputed otherwise.
 * Only the integer encoding is cached and compared; the result is decoded to
 * text for presentation.
 */
Datum
citus_minimum_cluster_version(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/* fast path: return the cached value if it is still valid */
	LWLockAcquire(&ClusterVersionShmem->lock, LW_SHARED);
	bool cacheValid = ClusterVersionShmem->cacheValid;
	int32 cachedVersion = ClusterVersionShmem->cachedMinVersionNum;
	LWLockRelease(&ClusterVersionShmem->lock);

	if (cacheValid)
	{
		PG_RETURN_TEXT_P(CitusVersionNumToText(cachedVersion));
	}

	/* slow path: recompute via fan-out and refresh the cache */
	int32 minimumVersion = ComputeMinimumClusterCitusVersion();

	LWLockAcquire(&ClusterVersionShmem->lock, LW_EXCLUSIVE);
	ClusterVersionShmem->cachedMinVersionNum = minimumVersion;
	ClusterVersionShmem->cacheValid = true;
	LWLockRelease(&ClusterVersionShmem->lock);

	PG_RETURN_TEXT_P(CitusVersionNumToText(minimumVersion));
}


/*
 * CitusVersionNumToText decodes a citus_version_num() style integer
 * (major * 10000 + minor * 100 + patch) back into a "major.minor.patch" string,
 * matching how Citus versions are written (e.g. 120105 -> "12.1.5").
 */
static text *
CitusVersionNumToText(int32 versionNum)
{
	int32 major = versionNum / 10000;
	int32 minor = (versionNum / 100) % 100;
	int32 patch = versionNum % 100;

	return cstring_to_text(psprintf("%d.%d.%d", major, minor, patch));
}


/*
 * ComputeMinimumClusterCitusVersion walks over all active primary nodes and
 * returns the smallest citus_version_num() among them. The local node is
 * answered from the CITUS_VERSION_NUM constant, every other node is queried
 * over a connection. If a remote node cannot be reached we error out, because a
 * minimum that silently ignores unreachable nodes could be used to make an
 * unsafe decision.
 */
static int32
ComputeMinimumClusterCitusVersion(void)
{
	/* the local node always contributes its own loaded version */
	int32 minimumVersion = CITUS_VERSION_NUM;

	int32 localGroupId = GetLocalGroupId();
	List *nodeList = ActivePrimaryNodeList(NoLock);

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, nodeList)
	{
		int32 nodeVersion = 0;

		if (workerNode->groupId == localGroupId)
		{
			nodeVersion = CITUS_VERSION_NUM;
		}
		else
		{
			nodeVersion = RemoteNodeCitusVersion(workerNode);
		}

		if (nodeVersion < minimumVersion)
		{
			minimumVersion = nodeVersion;
		}
	}

	return minimumVersion;
}


/*
 * RemoteNodeCitusVersion opens a connection to the given node and returns the
 * result of citus_version_num() executed on it.
 */
static int32
RemoteNodeCitusVersion(WorkerNode *workerNode)
{
	int connectionFlags = 0;
	MultiConnection *connection = GetNodeConnection(connectionFlags,
												   workerNode->workerName,
												   workerNode->workerPort);

	PGresult *result = NULL;
	int executionResult = ExecuteOptionalRemoteCommand(connection,
													   CLUSTER_VERSION_QUERY, &result);

	if (executionResult != RESPONSE_OKAY || result == NULL || PQntuples(result) != 1)
	{
		PQclear(result);
		ForgetResults(connection);

		ereport(ERROR, (errmsg("could not get Citus version from node \"%s:%d\"",
							   workerNode->workerName, workerNode->workerPort),
						errhint("Ensure the node is reachable and running Citus.")));
	}

	int32 nodeVersion = (int32) ParseIntField(result, 0, 0);

	PQclear(result);
	ForgetResults(connection);

	return nodeVersion;
}


/*
 * ClusterVersionShmemSize returns the amount of shared memory needed for the
 * cluster version cache.
 */
Size
ClusterVersionShmemSize(void)
{
	return sizeof(ClusterVersionShmemData);
}


/*
 * InitializeClusterVersionShmem chains the shared memory startup hook used to
 * allocate the cluster version cache. Called from _PG_init.
 */
void
InitializeClusterVersionShmem(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ClusterVersionShmemInit;
}


/*
 * ClusterVersionShmemInit allocates and initializes the shared memory used for
 * the cluster version cache.
 */
void
ClusterVersionShmemInit(void)
{
	bool alreadyInitialized = false;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ClusterVersionShmem = (ClusterVersionShmemData *)
						  ShmemInitStruct("Citus Cluster Version Shmem",
										  ClusterVersionShmemSize(),
										  &alreadyInitialized);

	if (!alreadyInitialized)
	{
		ClusterVersionShmem->namedLockTranche.trancheName =
			"Citus Cluster Version Tranche";
		ClusterVersionShmem->namedLockTranche.trancheId = LWLockNewTrancheId();
		LWLockRegisterTranche(ClusterVersionShmem->namedLockTranche.trancheId,
							  ClusterVersionShmem->namedLockTranche.trancheName);
		LWLockInitialize(&ClusterVersionShmem->lock,
						 ClusterVersionShmem->namedLockTranche.trancheId);

		ClusterVersionShmem->cachedMinVersionNum = 0;
		ClusterVersionShmem->cacheValid = false;
	}

	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * InvalidateClusterVersionCache marks the cached minimum version as stale so
 * that the next reader recomputes it. It is called from the pg_dist_node
 * relcache invalidation callback. We intentionally do not take the LWLock here,
 * matching InvalidateNodeRelationCacheCallback, to keep the callback safe and
 * cheap; a racy stale read only costs one extra recompute.
 */
void
InvalidateClusterVersionCache(void)
{
	if (ClusterVersionShmem != NULL)
	{
		ClusterVersionShmem->cacheValid = false;
	}
}
