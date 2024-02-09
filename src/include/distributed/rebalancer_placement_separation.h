/*-------------------------------------------------------------------------
 *
 * rebalancer_placement_separation.h
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

struct RebalancerPlacementSeparationContext;
typedef struct RebalancerPlacementSeparationContext RebalancerPlacementSeparationContext;

/* *INDENT-OFF* */
extern RebalancerPlacementSeparationContext * PrepareRebalancerPlacementSeparationContext(
	List *activeWorkerNodeList,
	List *activeShardPlacementList,
	FmgrInfo shardAllowedOnNodeUDF);
extern bool RebalancerPlacementSeparationContextPlacementIsAllowedOnWorker(
	RebalancerPlacementSeparationContext *context,
	uint64 shardId,
	uint64 placementId,
	WorkerNode *workerNode);
/* *INDENT-ON* */

#endif /* PLACEMENT_ISOLATION_H */
