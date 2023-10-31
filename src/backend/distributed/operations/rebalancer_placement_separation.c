/*-------------------------------------------------------------------------
 *
 * rebalancer_placement_separation.c
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
#include "distributed/rebalancer_placement_separation.h"
#include "distributed/shard_rebalancer.h"


typedef struct RebalancerPlacementSeparationContext
{
	/*
	 * Hash table where each entry is of the form NodeToPlacementGroupHashEntry,
	 * meaning that each entry maps the node with nodeGroupId to
	 * a NodeToPlacementGroupHashEntry.
	 */
	HTAB *nodePlacementGroupHash;
} RebalancerPlacementSeparationContext;


/*
 * Entry of the hash table that maps each primary worker node to a shard
 * placement group that is determined to be separated from other shards in
 * the cluster via that node.
 */
typedef struct NodeToPlacementGroupHashEntry
{
	/* hash key -- group id of the node */
	int32 nodeGroupId;

	/*
	 * Whether given node is allowed to have any shards.
	 *
	 * Inherited from WorkerNode->shouldHaveShards.
	 */
	bool shouldHaveShards;

	/*
	 * Whether given node has some shard placements that cannot be moved away.
	 *
	 * For the nodes that this rebalancer-run is not allowed to move the
	 * placements away from, InitRebalancerPlacementSeparationContext() sets
	 * this to true if the node has some shard placements already. And if the
	 * node has a single shard placement group that needs a separate node, it
	 * also sets assignedPlacementGroup.
	 *
	 * We do so to prevent TryAssignPlacementGroupsToNodeGroups() making
	 * incorrect assignments later on.
	 *
	 * See InitRebalancerPlacementSeparationContext() for more details.
	 */
	bool hasPlacementsThatCannotBeMovedAway;

	/*
	 * Shardgroup placement that is assigned to this node to be separated
	 * from others in the cluster.
	 *
	 * NULL if no shardgroup placement is not assigned yet.
	 */
	ShardgroupPlacement *assignedPlacementGroup;
} NodeToPlacementGroupHashEntry;

/*
 * Routines to prepare RebalancerPlacementSeparationContext.
 */
static void InitRebalancerPlacementSeparationContext(
	RebalancerPlacementSeparationContext *context,
	List *activeWorkerNodeList,
	List *rebalancePlacementList);
static void TryAssignPlacementGroupsToNodeGroups(
	RebalancerPlacementSeparationContext *context,
	List *activeWorkerNodeList,
	List *rebalancePlacementList,
	FmgrInfo shardAllowedOnNodeUDF);
static bool TryAssignPlacementGroupToNodeGroup(
	RebalancerPlacementSeparationContext *context,
	int32 candidateNodeGroupId,
	ShardPlacement *shardPlacement,
	FmgrInfo shardAllowedOnNodeUDF);


/* other helpers */
static List * PlacementListGetUniqueNodeGroupIds(List *placementList);


/*
 * PrepareRebalancerPlacementSeparationContext creates RebalancerPlacementSeparationContext
 * that keeps track of which worker nodes are used to separate which shardgroup placements
 * that need separate nodes.
 */
RebalancerPlacementSeparationContext *
PrepareRebalancerPlacementSeparationContext(List *activeWorkerNodeList,
											List *rebalancePlacementList,
											FmgrInfo shardAllowedOnNodeUDF)
{
	HTAB *nodePlacementGroupHash =
		CreateSimpleHashWithNameAndSize(int32, NodeToPlacementGroupHashEntry,
										"NodeToPlacementGroupHash",
										list_length(activeWorkerNodeList));

	RebalancerPlacementSeparationContext *context =
		palloc0(sizeof(RebalancerPlacementSeparationContext));
	context->nodePlacementGroupHash = nodePlacementGroupHash;

	activeWorkerNodeList = SortList(activeWorkerNodeList, CompareWorkerNodes);
	rebalancePlacementList = SortList(rebalancePlacementList, CompareShardPlacements);

	InitRebalancerPlacementSeparationContext(context, activeWorkerNodeList,
											 rebalancePlacementList);

	TryAssignPlacementGroupsToNodeGroups(context,
										 activeWorkerNodeList,
										 rebalancePlacementList,
										 shardAllowedOnNodeUDF);

	return context;
}


/*
 * InitRebalancerPlacementSeparationContext initializes given
 * RebalancerPlacementSeparationContext by using given list
 * of worker nodes and the worker node that is being drained,
 * if specified.
 */
static void
InitRebalancerPlacementSeparationContext(RebalancerPlacementSeparationContext *context,
										 List *activeWorkerNodeList,
										 List *rebalancePlacementList)
{
	HTAB *nodePlacementGroupHash = context->nodePlacementGroupHash;

	List *placementListUniqueNodeGroupIds =
		PlacementListGetUniqueNodeGroupIds(rebalancePlacementList);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, activeWorkerNodeList)
	{
		NodeToPlacementGroupHashEntry *nodePlacementGroupHashEntry =
			hash_search(nodePlacementGroupHash, &workerNode->groupId, HASH_ENTER,
						NULL);

		nodePlacementGroupHashEntry->shouldHaveShards = workerNode->shouldHaveShards;
		nodePlacementGroupHashEntry->hasPlacementsThatCannotBeMovedAway = false;
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
		 * placement group within S. We also set hasPlacementsThatCannotBeMovedAway
		 * to true for the nodes that already have some shard placements within S
		 * because we want to avoid from moving the placements that need a separate
		 * node (if any) from node D to node S.
		 *
		 * We skip below code for nodes of type D not because optimization purposes
		 * but because it would be "incorrect" to assume that "current placement
		 * distribution for a node of type D would be the same" after the rebalancer
		 * plans the moves.
		 */

		if (!workerNode->shouldHaveShards)
		{
			/* we can't assing any shardgroup placements to the node anyway */
			continue;
		}

		if (list_member_int(placementListUniqueNodeGroupIds, workerNode->groupId))
		{
			/* node is of type D */
			continue;
		}

		/* node is of type S */
		nodePlacementGroupHashEntry->hasPlacementsThatCannotBeMovedAway =
			NodeGroupHasDistributedTableShardPlacements(
				nodePlacementGroupHashEntry->nodeGroupId);
		nodePlacementGroupHashEntry->assignedPlacementGroup =
			NodeGroupGetSeparatedShardgroupPlacement(
				nodePlacementGroupHashEntry->nodeGroupId);
	}
}


/*
 * TryAssignPlacementGroupsToNodeGroups tries to assign placements that need
 * separate nodes within given placement list to individual worker nodes.
 */
static void
TryAssignPlacementGroupsToNodeGroups(RebalancerPlacementSeparationContext *context,
									 List *activeWorkerNodeList,
									 List *rebalancePlacementList,
									 FmgrInfo shardAllowedOnNodeUDF)
{
	List *unassignedPlacementList = NIL;

	/*
	 * Assign as much as possible shardgroup placements to worker nodes where
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

		int32 currentNodeGroupId = shardPlacement->groupId;
		if (!TryAssignPlacementGroupToNodeGroup(context,
												currentNodeGroupId,
												shardPlacement,
												shardAllowedOnNodeUDF))
		{
			unassignedPlacementList =
				lappend(unassignedPlacementList, shardPlacement);
		}
	}

	bool emitWarning = false;

	/*
	 * For the shardgroup placements that could not be assigned to their
	 * current node, assign them to any other node.
	 */
	ShardPlacement *unassignedShardPlacement = NULL;
	foreach_ptr(unassignedShardPlacement, unassignedPlacementList)
	{
		bool separated = false;

		WorkerNode *activeWorkerNode = NULL;
		foreach_ptr(activeWorkerNode, activeWorkerNodeList)
		{
			if (TryAssignPlacementGroupToNodeGroup(context,
												   activeWorkerNode->groupId,
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
 * TryAssignPlacementGroupToNodeGroup is an helper to
 * TryAssignPlacementGroupsToNodeGroups that tries to assign given
 * shard placement to given node and returns true if it succeeds.
 */
static bool
TryAssignPlacementGroupToNodeGroup(RebalancerPlacementSeparationContext *context,
								   int32 candidateNodeGroupId,
								   ShardPlacement *shardPlacement,
								   FmgrInfo shardAllowedOnNodeUDF)
{
	HTAB *nodePlacementGroupHash = context->nodePlacementGroupHash;

	bool found = false;
	NodeToPlacementGroupHashEntry *nodePlacementGroupHashEntry =
		hash_search(nodePlacementGroupHash, &candidateNodeGroupId, HASH_FIND, &found);

	if (!found)
	{
		ereport(ERROR, (errmsg("no such node is found")));
	}

	if (nodePlacementGroupHashEntry->assignedPlacementGroup)
	{
		/*
		 * Right now callers of this function call it once for each distinct
		 * shardgroup placement, hence we assume that shardgroup placement
		 * that given shard placement belongs to and
		 * nodePlacementGroupHashEntry->assignedPlacementGroup cannot be the
		 * same, without checking.
		 */
		return false;
	}

	if (nodePlacementGroupHashEntry->hasPlacementsThatCannotBeMovedAway)
	{
		return false;
	}

	if (!nodePlacementGroupHashEntry->shouldHaveShards)
	{
		return false;
	}

	WorkerNode *workerNode = PrimaryNodeForGroup(candidateNodeGroupId, NULL);
	Datum allowed = FunctionCall2(&shardAllowedOnNodeUDF, shardPlacement->shardId,
								  workerNode->nodeId);
	if (!DatumGetBool(allowed))
	{
		return false;
	}

	nodePlacementGroupHashEntry->assignedPlacementGroup =
		GetShardgroupPlacementForPlacement(shardPlacement->shardId,
										   shardPlacement->placementId);

	return true;
}


/*
 * RebalancerPlacementSeparationContextPlacementIsAllowedOnWorker returns true
 * if shard placement with given shardId & placementId is allowed to be stored
 * on given worker node.
 */
bool
RebalancerPlacementSeparationContextPlacementIsAllowedOnWorker(
	RebalancerPlacementSeparationContext *context,
	uint64 shardId,
	uint64 placementId,
	WorkerNode *workerNode)
{
	HTAB *nodePlacementGroupHash = context->nodePlacementGroupHash;

	bool found = false;
	NodeToPlacementGroupHashEntry *nodePlacementGroupHashEntry =
		hash_search(nodePlacementGroupHash, &(workerNode->groupId), HASH_FIND, &found);

	if (!found)
	{
		ereport(ERROR, (errmsg("no such node is found")));
	}

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	if (!shardInterval->needsSeparateNode)
	{
		/*
		 * It doesn't need a separate node, but is the node used to separate
		 * a shardgroup placement? If so, we cannot store it on this node.
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

	ShardgroupPlacement *placementGroup =
		GetShardgroupPlacementForPlacement(shardId, placementId);
	return ShardgroupPlacementsSame(nodePlacementGroupHashEntry->assignedPlacementGroup,
									placementGroup);
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
			list_append_unique_int(placementListUniqueNodeGroupIds,
								   shardPlacement->groupId);
	}

	return placementListUniqueNodeGroupIds;
}
