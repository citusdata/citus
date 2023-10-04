/*-------------------------------------------------------------------------
 *
 * rebalancer_placement_isolation.h
 *	  Routines to determine which worker node should be used to separate
 *	  a colocated set of shard placements that need separate nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLACEMENT_ISOLATION_H
#define PLACEMENT_ISOLATION_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

#include "distributed/metadata_utility.h"

struct RebalancerPlacementIsolationContext;
typedef struct RebalancerPlacementIsolationContext RebalancerPlacementIsolationContext;

extern RebalancerPlacementIsolationContext * PrepareRebalancerPlacementIsolationContext(
	List *activeWorkerNodeList,
	List
	*
	activeShardPlacementList,
	WorkerNode
	*
	drainWorkerNode,
	FmgrInfo
	*
	shardAllowedOnNodeUDF);
extern bool RebalancerPlacementIsolationContextPlacementIsAllowedOnWorker(
	RebalancerPlacementIsolationContext *context,
	uint64 shardId,
	uint64
	placementId,
	WorkerNode *
	workerNode);

#endif /* PLACEMENT_ISOLATION_H */
