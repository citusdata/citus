/*-------------------------------------------------------------------------
 *
 * rebalancer_placement_isolation.c
 *	  Routines to determine which worker node should be used to separate
 *	  a colocated set of shard placements that need separate nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

#include "distributed/colocation_utils.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/rebalancer_placement_isolation.h"
#include "distributed/shard_rebalancer.h"


struct RebalancerPlacementIsolationContext
{
	HTAB *nodePlacementGroupHash;
};


/*
 * Entry of the hash table that maps each primary worker node to a shard
 * placement group that is determined to be separated from other shards in
 * the cluster via that node.
 */
typedef struct
{
	/* hash key -- group id of the node */
	int32 nodeGroupId;

	/*
	 * Whether given node is allowed to have any shards.
	 *
	 * This is not just WorkerNode->shouldHaveShards but also takes into account
	 * whether the node is being drained.
	 */
	bool shouldHaveShards;

	/*
	 * Whether given node is allowed to separate any shard placement groups.
	 *
	 * This is set only if we're draining a single node because otherwise
	 * we have the control to separate shard placement groups on any node.
	 *
	 * However if we're draining a single node, we cannot separate shard
	 * placement groups on the node that already has some placements because
	 * we cannot move the existing placements from a node that we're not
	 * draining to another node when we're draining a single node.
	 */
	bool allowedToSeparateAnyPlacementGroup;

	/*
	 * Shard placement group that is assigned to this node to be separated
	 * from others in the cluster.
	 *
	 * NULL if no shard placement group is assigned yet.
	 */
	ShardPlacementGroup *assignedPlacementGroup;
} NodePlacementGroupHashEntry;

/*
 * Routines to prepare a hash table where each entry is of type
 * NodePlacementGroupHashEntry.
 */
static void NodePlacementGroupHashInit(HTAB *nodePlacementGroupHash,
									   List *workerNodeList,
									   WorkerNode *drainWorkerNode);
static void NodePlacementGroupHashAssignNodes(HTAB *nodePlacementGroupHash,
											  List *workerNodeList,
											  List *shardPlacementList);
static bool NodePlacementGroupHashAssignNode(HTAB *nodePlacementGroupHash,
											 int32 nodeGroupId,
											 ShardPlacement *shardPlacement);
static NodePlacementGroupHashEntry * NodePlacementGroupHashGetNodeWithGroupId(
	HTAB *nodePlacementGroupHash,
	int32
	nodeGroupId);


/* other helpers */
static int WorkerNodeListGetNodeWithGroupId(List *workerNodeList, int32 nodeGroupId);


/*
 * PrepareRebalancerPlacementIsolationContext creates RebalancerPlacementIsolationContext
 * that keeps track of which worker nodes are used to separate which shard placement groups
 * that need separate nodes.
 */
RebalancerPlacementIsolationContext *
PrepareRebalancerPlacementIsolationContext(List *activeWorkerNodeList,
										   List *activeShardPlacementList,
										   WorkerNode *drainWorkerNode)
{
	HTAB *nodePlacementGroupHash =
		CreateSimpleHashWithNameAndSize(uint32, NodePlacementGroupHashEntry,
										"NodePlacementGroupHash",
										list_length(activeWorkerNodeList));

	activeWorkerNodeList = SortList(activeWorkerNodeList, CompareWorkerNodes);
	activeShardPlacementList = SortList(activeShardPlacementList, CompareShardPlacements);

	NodePlacementGroupHashInit(nodePlacementGroupHash, activeWorkerNodeList,
							   drainWorkerNode);

	NodePlacementGroupHashAssignNodes(nodePlacementGroupHash,
									  activeWorkerNodeList,
									  activeShardPlacementList);

	RebalancerPlacementIsolationContext *context =
		palloc(sizeof(RebalancerPlacementIsolationContext));
	context->nodePlacementGroupHash = nodePlacementGroupHash;

	return context;
}


/*
 * NodePlacementGroupHashInit initializes given hash table where each
 * entry is of type NodePlacementGroupHashEntry by using given list
 * of worker nodes and the worker node that is being drained, if specified.
 */
static void
NodePlacementGroupHashInit(HTAB *nodePlacementGroupHash, List *workerNodeList,
						   WorkerNode *drainWorkerNode)
{
	bool drainSingleNode = drainWorkerNode != NULL;

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		NodePlacementGroupHashEntry *nodePlacementGroupHashEntry =
			hash_search(nodePlacementGroupHash, &workerNode->groupId, HASH_ENTER,
						NULL);

		nodePlacementGroupHashEntry->nodeGroupId = workerNode->groupId;

		bool shouldHaveShards = workerNode->shouldHaveShards;
		if (drainWorkerNode && drainWorkerNode->groupId == workerNode->groupId)
		{
			shouldHaveShards = false;
		}

		nodePlacementGroupHashEntry->shouldHaveShards = shouldHaveShards;
		nodePlacementGroupHashEntry->allowedToSeparateAnyPlacementGroup =
			shouldHaveShards;
		nodePlacementGroupHashEntry->assignedPlacementGroup = NULL;

		/*
		 * For the rest of the comment, assume that:
		 *   Node D: the node we're draining
		 *   Node I: a node that is not D and that has a shard placement group
		 *           that needs a separate node
		 *   Node R: a node that is not D and that has some regular shard
		 *           placements
		 *
		 * If we're draining a single node, then we don't know whether other
		 * nodes have any regular shard placements or any that need a separate
		 * node because in that case GetRebalanceSteps() would provide a list of
		 * shard placements that are stored on D, not a list that contains all
		 * the placements accross the cluster (because we want to limit node
		 * draining to that node in that case). Note that when all shard
		 * placements in the cluster are provided, NodePlacementGroupHashAssignNodes()
		 * would already be aware of which node is used to separate which shard
		 * placement group or which node is used to store some regular shard
		 * placements. That is why we skip below code if we're not draining a
		 * single node. It's not only inefficient to run below code when we're
		 * not draining a single node, but also it's not correct because otherwise
		 * rebalancer might decide to move some shard placements between any
		 * nodes in the cluster and it would be incorrect to assume that current
		 * placement distribution would be the same after the rebalancer plans the
		 * moves.
		 *
		 * Below we find out the assigned placement groups for nodes of type
		 * I because we want to avoid from moving the placements (if any) from
		 * node D to node I. We also set allowedToSeparateAnyPlacementGroup to
		 * false for the nodes that already have some shard placements because
		 * we want to avoid from moving the placements that need a separate node
		 * (if any) from node D to node R.
		 */
		if (!(shouldHaveShards && drainSingleNode))
		{
			continue;
		}

		ShardPlacementGroup *separatedShardPlacementGroup =
			NodeGroupGetSeparatedShardPlacementGroup(
				nodePlacementGroupHashEntry->nodeGroupId);
		if (separatedShardPlacementGroup)
		{
			nodePlacementGroupHashEntry->assignedPlacementGroup =
				separatedShardPlacementGroup;
		}
		else
		{
			nodePlacementGroupHashEntry->allowedToSeparateAnyPlacementGroup =
				!NodeGroupHasShardPlacements(nodePlacementGroupHashEntry->nodeGroupId);
		}
	}
}


/*
 * NodePlacementGroupHashAssignNodes assigns all active shard placements in
 * the cluster that need separate nodes to individual worker nodes.
 */
static void
NodePlacementGroupHashAssignNodes(HTAB *nodePlacementGroupHash,
								  List *workerNodeList,
								  List *shardPlacementList)
{
	List *availableWorkerList = list_copy(workerNodeList);
	List *unassignedShardPlacementList = NIL;

	/*
	 * Assign as much as possible shard placement groups to worker nodes where
	 * they are stored already.
	 */
	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, shardPlacementList)
	{
		ShardInterval *shardInterval = LoadShardInterval(shardPlacement->shardId);
		if (!shardInterval->needsSeparateNode)
		{
			continue;
		}

		int32 assignGroupId = shardPlacement->groupId;
		if (NodePlacementGroupHashAssignNode(nodePlacementGroupHash,
											 assignGroupId,
											 shardPlacement))
		{
			int currentPlacementNodeIdx =
				WorkerNodeListGetNodeWithGroupId(availableWorkerList,
												 assignGroupId);
			availableWorkerList = list_delete_nth_cell(availableWorkerList,
													   currentPlacementNodeIdx);
		}
		else
		{
			unassignedShardPlacementList =
				lappend(unassignedShardPlacementList, shardPlacement);
		}
	}

	/*
	 * For the shard placement groups that could not be assigned to their
	 * current node, assign them to any other node that is available.
	 */
	int availableNodeIdx = 0;
	ShardPlacement *unassignedShardPlacement = NULL;
	foreach_ptr(unassignedShardPlacement, unassignedShardPlacementList)
	{
		bool separated = false;
		while (!separated && availableNodeIdx < list_length(availableWorkerList))
		{
			WorkerNode *availableWorkerNode =
				(WorkerNode *) list_nth(availableWorkerList, availableNodeIdx);
			availableNodeIdx++;

			if (NodePlacementGroupHashAssignNode(nodePlacementGroupHash,
												 availableWorkerNode->groupId,
												 unassignedShardPlacement))
			{
				separated = true;
				break;
			}
		}

		if (!separated)
		{
			ereport(WARNING, (errmsg("could not separate all shard placements "
									 "that need a separate node")));
			return;
		}
	}
}


/*
 * NodePlacementGroupHashAssignNode is an helper to
 * NodePlacementGroupHashAssignNodes that tries to assign given
 * shard placement to given node and returns true if it succeeds.
 */
static bool
NodePlacementGroupHashAssignNode(HTAB *nodePlacementGroupHash,
								 int32 nodeGroupId,
								 ShardPlacement *shardPlacement)
{
	NodePlacementGroupHashEntry *nodePlacementGroupHashEntry =
		NodePlacementGroupHashGetNodeWithGroupId(nodePlacementGroupHash, nodeGroupId);

	ShardPlacementGroup *placementGroup =
		GetShardPlacementGroupForPlacement(shardPlacement->shardId,
										   shardPlacement->placementId);

	if (nodePlacementGroupHashEntry->assignedPlacementGroup)
	{
		/*
		 * Right now callers of this function call it once for each distinct
		 * shard placement group, hence we assume that placementGroup and
		 * nodePlacementGroupHashEntry->assignedPlacementGroup cannot be the
		 * same already, unless no shard placement group is assigned to this
		 * node yet.
		 */
		Assert(!ShardPlacementGroupsSame(placementGroup,
										 nodePlacementGroupHashEntry->
										 assignedPlacementGroup));
		return false;
	}

	if (!nodePlacementGroupHashEntry->allowedToSeparateAnyPlacementGroup)
	{
		return false;
	}

	if (!nodePlacementGroupHashEntry->shouldHaveShards)
	{
		return false;
	}

	nodePlacementGroupHashEntry->assignedPlacementGroup = placementGroup;

	return true;
}


/*
 * RebalancerPlacementIsolationContextPlacementIsAllowedOnWorker returns true
 * if shard placement with given shardId & placementId is allowed to be stored
 * on given worker node.
 */
bool
RebalancerPlacementIsolationContextPlacementIsAllowedOnWorker(
	RebalancerPlacementIsolationContext *context,
	uint64 shardId,
	uint64 placementId,
	WorkerNode *workerNode)
{
	HTAB *nodePlacementGroupHash = context->nodePlacementGroupHash;
	NodePlacementGroupHashEntry *nodePlacementGroupHashEntry =
		NodePlacementGroupHashGetNodeWithGroupId(nodePlacementGroupHash,
												 workerNode->groupId);

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	if (!shardInterval->needsSeparateNode)
	{
		/*
		 * It doesn't need a separate node, but is the node used to separate
		 * a shard placement group? If so, we cannot store it on this node.
		 */
		return nodePlacementGroupHashEntry->shouldHaveShards &&
			   nodePlacementGroupHashEntry->assignedPlacementGroup == NULL;
	}

	/*
	 * Given shard placement needs a separate node.
	 * Check if given worker node is the one that is assigned to separate it.
	 */
	ShardPlacementGroup *placementGroup =
		GetShardPlacementGroupForPlacement(shardId, placementId);
	return nodePlacementGroupHashEntry->assignedPlacementGroup != NULL &&
		   ShardPlacementGroupsSame(nodePlacementGroupHashEntry->assignedPlacementGroup,
									placementGroup);
}


/*
 * NodePlacementGroupHashGetNodeWithGroupId searches given hash table for
 * NodePlacementGroupHashEntry with given node id and returns it.
 *
 * Throws an error if no such entry is found.
 */
static NodePlacementGroupHashEntry *
NodePlacementGroupHashGetNodeWithGroupId(HTAB *nodePlacementGroupHash, int32 nodeGroupId)
{
	NodePlacementGroupHashEntry *nodePlacementGroupHashEntry =
		hash_search(nodePlacementGroupHash, &nodeGroupId, HASH_FIND, NULL);

	if (nodePlacementGroupHashEntry == NULL)
	{
		ereport(ERROR, (errmsg("no such node is found")));
	}

	return nodePlacementGroupHashEntry;
}


/*
 * WorkerNodeListGetNodeWithGroupId returns the index of worker node with given id
 * in given worker node list.
 *
 * Throws an error if no such node is found.
 */
static int
WorkerNodeListGetNodeWithGroupId(List *workerNodeList, int32 nodeGroupId)
{
	int workerNodeIndex = 0;
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		if (workerNode->groupId == nodeGroupId)
		{
			return workerNodeIndex;
		}

		workerNodeIndex++;
	}

	ereport(ERROR, (errmsg("no such node is found")));
}
