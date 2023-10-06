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
} NodeToPlacementGroupHashEntry;

/*
 * Routines to prepare a hash table where each entry is of type
 * NodeToPlacementGroupHashEntry.
 */
static void NodeToPlacementGroupHashInit(HTAB *nodePlacementGroupHash,
										 List *activeWorkerNodeList,
										 List *rebalancePlacementList,
										 WorkerNode *drainWorkerNode);
static void NodeToPlacementGroupHashAssignNodes(HTAB *nodePlacementGroupHash,
												List *activeWorkerNodeList,
												List *rebalancePlacementList,
												FmgrInfo *shardAllowedOnNodeUDF);
static bool NodeToPlacementGroupHashAssignNode(HTAB *nodePlacementGroupHash,
											   int32 nodeGroupId,
											   ShardPlacement *shardPlacement,
											   FmgrInfo *shardAllowedOnNodeUDF);
static NodeToPlacementGroupHashEntry * NodeToPlacementGroupHashGetNodeWithGroupId(
	HTAB *nodePlacementGroupHash,
	int32
	nodeGroupId);


/* other helpers */
static List * PlacementListGetUniqueNodeGroupIds(List *placementList);
static int WorkerNodeListGetNodeWithGroupId(List *workerNodeList, int32 nodeGroupId);


/*
 * PrepareRebalancerPlacementIsolationContext creates RebalancerPlacementIsolationContext
 * that keeps track of which worker nodes are used to separate which shard placement groups
 * that need separate nodes.
 */
RebalancerPlacementIsolationContext *
PrepareRebalancerPlacementIsolationContext(List *activeWorkerNodeList,
										   List *rebalancePlacementList,
										   WorkerNode *drainWorkerNode,
										   FmgrInfo *shardAllowedOnNodeUDF)
{
	HTAB *nodePlacementGroupHash =
		CreateSimpleHashWithNameAndSize(uint32, NodeToPlacementGroupHashEntry,
										"NodeToPlacementGroupHash",
										list_length(activeWorkerNodeList));

	activeWorkerNodeList = SortList(activeWorkerNodeList, CompareWorkerNodes);
	rebalancePlacementList = SortList(rebalancePlacementList, CompareShardPlacements);

	NodeToPlacementGroupHashInit(nodePlacementGroupHash, activeWorkerNodeList,
								 rebalancePlacementList, drainWorkerNode);

	NodeToPlacementGroupHashAssignNodes(nodePlacementGroupHash,
										activeWorkerNodeList,
										rebalancePlacementList,
										shardAllowedOnNodeUDF);

	RebalancerPlacementIsolationContext *context =
		palloc(sizeof(RebalancerPlacementIsolationContext));
	context->nodePlacementGroupHash = nodePlacementGroupHash;

	return context;
}


/*
 * NodeToPlacementGroupHashInit initializes given hash table where each
 * entry is of type NodeToPlacementGroupHashEntry by using given list
 * of worker nodes and the worker node that is being drained, if specified.
 */
static void
NodeToPlacementGroupHashInit(HTAB *nodePlacementGroupHash, List *activeWorkerNodeList,
							 List *rebalancePlacementList, WorkerNode *drainWorkerNode)
{
	List *placementListUniqueNodeGroupIds =
		PlacementListGetUniqueNodeGroupIds(rebalancePlacementList);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, activeWorkerNodeList)
	{
		NodeToPlacementGroupHashEntry *nodePlacementGroupHashEntry =
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
		 * Lets call set of the nodes that placements in rebalancePlacementList
		 * are stored on as D and the others as S. In other words, D is the set
		 * of the nodes that we're allowed to move the placements "from" or
		 * "to (*)" (* = if we're not draining it) and S is the set of the nodes
		 * that we're only allowed to move the placements "to" but not "from".
		 *
		 * This means that, for a node of type S, the fact that whether the node
		 * is used to separate a placement group or not cannot be changed in the
		 * runtime.
		 *
		 * For this reason, below we find out the assigned placement groups for
		 * nodes of type S because we want to avoid from moving the placements
		 * (if any) from a node of type D to a node that is used to separate a
		 * placement group within S. We also set allowedToSeparateAnyPlacementGroup
		 * to false for the nodes that already have some shard placements within S
		 * because we want to avoid from moving the placements that need a separate
		 * node (if any) from node D to node S.
		 *
		 * We skip below code for nodes of type D not because optimization purposes
		 * but because it would be "incorrect" to assume that "current placement
		 * distribution for a node of type D would be the same" after the rebalancer
		 * plans the moves.
		 */

		if (!shouldHaveShards)
		{
			/* we can't assing any shard placement groups to the node anyway */
			continue;
		}

		if (list_length(placementListUniqueNodeGroupIds) == list_length(
				activeWorkerNodeList))
		{
			/*
			 * list_member_oid() check would return true for all placements then.
			 * This means that all the nodes are of type D.
			 */
			Assert(list_member_oid(placementListUniqueNodeGroupIds, workerNode->groupId));
			continue;
		}

		if (list_member_oid(placementListUniqueNodeGroupIds, workerNode->groupId))
		{
			/* node is of type D */
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
 * NodeToPlacementGroupHashAssignNodes assigns all active shard placements in
 * the cluster that need separate nodes to individual worker nodes.
 */
static void
NodeToPlacementGroupHashAssignNodes(HTAB *nodePlacementGroupHash,
									List *activeWorkerNodeList,
									List *rebalancePlacementList,
									FmgrInfo *shardAllowedOnNodeUDF)
{
	List *availableWorkerList = list_copy(activeWorkerNodeList);
	List *unassignedPlacementList = NIL;

	/*
	 * Assign as much as possible shard placement groups to worker nodes where
	 * they are stored already.
	 */
	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, rebalancePlacementList)
	{
		ShardInterval *shardInterval = LoadShardInterval(shardPlacement->shardId);
		if (!shardInterval->needsSeparateNode)
		{
			continue;
		}

		int32 shardPlacementGroupId = shardPlacement->groupId;
		if (NodeToPlacementGroupHashAssignNode(nodePlacementGroupHash,
											   shardPlacementGroupId,
											   shardPlacement,
											   shardAllowedOnNodeUDF))
		{
			/*
			 * NodeToPlacementGroupHashAssignNode() succeeds for each worker node
			 * once, hence we must not have removed the worker node from the list
			 * yet, and WorkerNodeListGetNodeWithGroupId() ensures that already.
			 */
			int currentPlacementNodeIdx =
				WorkerNodeListGetNodeWithGroupId(availableWorkerList,
												 shardPlacementGroupId);
			availableWorkerList = list_delete_nth_cell(availableWorkerList,
													   currentPlacementNodeIdx);
		}
		else
		{
			unassignedPlacementList =
				lappend(unassignedPlacementList, shardPlacement);
		}
	}

	bool emitWarning = false;

	/*
	 * For the shard placement groups that could not be assigned to their
	 * current node, assign them to any other node that is available.
	 */
	ShardPlacement *unassignedShardPlacement = NULL;
	foreach_ptr(unassignedShardPlacement, unassignedPlacementList)
	{
		bool separated = false;

		WorkerNode *availableWorkerNode = NULL;
		foreach_ptr(availableWorkerNode, availableWorkerList)
		{
			if (NodeToPlacementGroupHashAssignNode(nodePlacementGroupHash,
												   availableWorkerNode->groupId,
												   unassignedShardPlacement,
												   shardAllowedOnNodeUDF))
			{
				separated = true;
				break;
			}
		}

		if (!separated)
		{
			emitWarning = true;
		}
	}

	if (emitWarning)
	{
		ereport(WARNING, (errmsg("could not separate all shard placements "
								 "that need a separate node")));
	}
}


/*
 * NodeToPlacementGroupHashAssignNode is an helper to
 * NodeToPlacementGroupHashAssignNodes that tries to assign given
 * shard placement to given node and returns true if it succeeds.
 */
static bool
NodeToPlacementGroupHashAssignNode(HTAB *nodePlacementGroupHash,
								   int32 nodeGroupId,
								   ShardPlacement *shardPlacement,
								   FmgrInfo *shardAllowedOnNodeUDF)
{
	NodeToPlacementGroupHashEntry *nodePlacementGroupHashEntry =
		NodeToPlacementGroupHashGetNodeWithGroupId(nodePlacementGroupHash, nodeGroupId);

	if (nodePlacementGroupHashEntry->assignedPlacementGroup)
	{
		/*
		 * Right now callers of this function call it once for each distinct
		 * shard placement group, hence we assume that shard placement group
		 * that given shard placement belongs to and
		 * nodePlacementGroupHashEntry->assignedPlacementGroup cannot be the
		 * same, without checking.
		 */
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

	WorkerNode *workerNode = PrimaryNodeForGroup(nodeGroupId, NULL);
	Datum allowed = FunctionCall2(shardAllowedOnNodeUDF, shardPlacement->shardId,
								  workerNode->nodeId);
	if (!DatumGetBool(allowed))
	{
		return false;
	}

	nodePlacementGroupHashEntry->assignedPlacementGroup =
		GetShardPlacementGroupForPlacement(shardPlacement->shardId,
										   shardPlacement->placementId);

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
	NodeToPlacementGroupHashEntry *nodePlacementGroupHashEntry =
		NodeToPlacementGroupHashGetNodeWithGroupId(nodePlacementGroupHash,
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
	if (nodePlacementGroupHashEntry->assignedPlacementGroup == NULL)
	{
		/* the node is not supposed to separate a placement group */
		return false;
	}

	ShardPlacementGroup *placementGroup =
		GetShardPlacementGroupForPlacement(shardId, placementId);
	return ShardPlacementGroupsSame(nodePlacementGroupHashEntry->assignedPlacementGroup,
									placementGroup);
}


/*
 * NodeToPlacementGroupHashGetNodeWithGroupId searches given hash table for
 * NodeToPlacementGroupHashEntry with given node id and returns it.
 *
 * Throws an error if no such entry is found.
 */
static NodeToPlacementGroupHashEntry *
NodeToPlacementGroupHashGetNodeWithGroupId(HTAB *nodePlacementGroupHash,
										   int32 nodeGroupId)
{
	NodeToPlacementGroupHashEntry *nodePlacementGroupHashEntry =
		hash_search(nodePlacementGroupHash, &nodeGroupId, HASH_FIND, NULL);

	if (nodePlacementGroupHashEntry == NULL)
	{
		ereport(ERROR, (errmsg("no such node is found")));
	}

	return nodePlacementGroupHashEntry;
}


/*
 * PlacementListGetUniqueNodeGroupIds returns a list of unique node group ids
 * that are used by given list of shard placements.
 */
static List *
PlacementListGetUniqueNodeGroupIds(List *placementList)
{
	List *placementListUniqueNodeGroupIds = NIL;

	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, placementList)
	{
		placementListUniqueNodeGroupIds =
			list_append_unique_oid(placementListUniqueNodeGroupIds,
								   shardPlacement->groupId);
	}

	return placementListUniqueNodeGroupIds;
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
