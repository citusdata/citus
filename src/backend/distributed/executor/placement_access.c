/*-------------------------------------------------------------------------
 *
 * citus_custom_scan.c
 *
 * Definitions of the functions used in generating the placement accesses
 * for distributed query execution.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/placement_access.h"

static List * BuildPlacementSelectList(int32 groupId, List *relationShardList);
static List * BuildPlacementDDLList(int32 groupId, List *relationShardList);
static List * BuildPlacementAccessList(int32 groupId, List *relationShardList,
									   ShardPlacementAccessType accessType);


/*
 * PlacementAccessListForTask returns a list of placement accesses for a given
 * task and task placement.
 */
List *
PlacementAccessListForTask(Task *task, ShardPlacement *taskPlacement)
{
	List *placementAccessList = NIL;
	List *relationShardList = task->relationShardList;
	bool addAnchorAccess = false;
	ShardPlacementAccessType accessType = PLACEMENT_ACCESS_SELECT;

	if (task->taskType == MODIFY_TASK)
	{
		/* DML command */
		addAnchorAccess = true;
		accessType = PLACEMENT_ACCESS_DML;
	}
	else if (task->taskType == DDL_TASK || task->taskType == VACUUM_ANALYZE_TASK)
	{
		/* DDL command */
		addAnchorAccess = true;
		accessType = PLACEMENT_ACCESS_DDL;
	}
	else if (relationShardList == NIL)
	{
		/* SELECT query that does not touch any shard placements */
		addAnchorAccess = true;
		accessType = PLACEMENT_ACCESS_SELECT;
	}

	if (addAnchorAccess)
	{
		ShardPlacementAccess *placementAccess =
			CreatePlacementAccess(taskPlacement, accessType);

		placementAccessList = lappend(placementAccessList, placementAccess);
	}

	/*
	 * We've already added anchor shardId's placement access to the list. Now,
	 * add the other placements in the relationShardList.
	 */
	if (accessType == PLACEMENT_ACCESS_DDL)
	{
		/*
		 * All relations appearing inter-shard DDL commands should be marked
		 * with DDL access.
		 */
		List *relationShardAccessList =
			BuildPlacementDDLList(taskPlacement->groupId, relationShardList);

		placementAccessList = list_concat(placementAccessList, relationShardAccessList);
	}
	else
	{
		/*
		 * In case of SELECTs or DML's, we add SELECT placement accesses to the
		 * elements in relationShardList. For SELECT queries, it is trivial, since
		 * the query is literally accesses the relationShardList in the same query.
		 *
		 * For DMLs, create placement accesses for placements that appear in a
		 * subselect.
		 */
		List *relationShardAccessList =
			BuildPlacementSelectList(taskPlacement->groupId, relationShardList);

		placementAccessList = list_concat(placementAccessList, relationShardAccessList);
	}

	return placementAccessList;
}


/*
 * BuildPlacementSelectList builds a list of SELECT placement accesses
 * which can be used to call StartPlacementListConnection or
 * GetPlacementListConnection. If the node group does not have a placement
 * (e.g. in case of a broadcast join) then the shard is skipped.
 */
static List *
BuildPlacementSelectList(int32 groupId, List *relationShardList)
{
	return BuildPlacementAccessList(groupId, relationShardList, PLACEMENT_ACCESS_SELECT);
}


/*
 * BuildPlacementDDLList is a warpper around BuildPlacementAccessList() for DDL access.
 */
static List *
BuildPlacementDDLList(int32 groupId, List *relationShardList)
{
	return BuildPlacementAccessList(groupId, relationShardList, PLACEMENT_ACCESS_DDL);
}


/*
 * BuildPlacementAccessList returns a list of placement accesses for the given
 * relationShardList and the access type.
 */
static List *
BuildPlacementAccessList(int32 groupId, List *relationShardList,
						 ShardPlacementAccessType accessType)
{
	List *placementAccessList = NIL;

	RelationShard *relationShard = NULL;
	foreach_declared_ptr(relationShard, relationShardList)
	{
		ShardPlacement *placement = ActiveShardPlacementOnGroup(groupId,
																relationShard->shardId);
		if (placement == NULL)
		{
			continue;
		}

		ShardPlacementAccess *placementAccess = CreatePlacementAccess(placement,
																	  accessType);
		placementAccessList = lappend(placementAccessList, placementAccess);
	}

	return placementAccessList;
}


/*
 * CreatePlacementAccess returns a new ShardPlacementAccess for the given placement
 * and access type.
 */
ShardPlacementAccess *
CreatePlacementAccess(ShardPlacement *placement, ShardPlacementAccessType accessType)
{
	ShardPlacementAccess *placementAccess = (ShardPlacementAccess *) palloc0(
		sizeof(ShardPlacementAccess));
	placementAccess->placement = placement;
	placementAccess->accessType = accessType;

	return placementAccess;
}
