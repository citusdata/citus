/*-------------------------------------------------------------------------
 *
 * master_expire_table_cache.c
 *	  UDF to refresh shard cache at workers
 *
 * This file contains master_expire_table_cache function. The function
 * accepts a table name and drops tables cached shards from all workers.
 * It does not change existing shard placement. Only drops cached copies
 * of shards.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"

#include "catalog/pg_class.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


static List * FindAbsentShardPlacementsOnWorker(WorkerNode *workerNode,
												ShardInterval **shardIntervalArray,
												List **placementListArray,
												int shardCount);
static void DropShardsFromWorker(WorkerNode *workerNode, Oid relationId,
								 List *shardIntervalList);

PG_FUNCTION_INFO_V1(master_expire_table_cache);


/*
 * master_expire_table_cache drops table's caches shards in all workers. The function
 * expects a passed table to be a small distributed table meaning it has less than
 * large_table_shard_count.
 */
Datum
master_expire_table_cache(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	DistTableCacheEntry *cacheEntry = NULL;
	List *workerNodeList = NIL;
	ListCell *workerNodeCell = NULL;
	int shardCount = 0;
	ShardInterval **shardIntervalArray = NULL;
	List **placementListArray = NULL;
	int shardIndex = 0;

	CheckCitusVersion(ERROR);

	cacheEntry = DistributedTableCacheEntry(relationId);
	workerNodeList = ActiveWorkerNodeList();
	shardCount = cacheEntry->shardIntervalArrayLength;
	shardIntervalArray = cacheEntry->sortedShardIntervalArray;

	if (shardCount == 0)
	{
		ereport(WARNING, (errmsg("Table has no shards, no action is taken")));
		PG_RETURN_VOID();
	}

	if (shardCount >= LargeTableShardCount)
	{
		ereport(ERROR, (errmsg("Must be called on tables smaller than %d shards",
							   LargeTableShardCount)));
	}

	placementListArray = palloc(shardCount * sizeof(List *));

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		ShardInterval *shardInterval = shardIntervalArray[shardIndex];
		placementListArray[shardIndex] =
			FinalizedShardPlacementList(shardInterval->shardId);
	}

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		List *shardDropList = FindAbsentShardPlacementsOnWorker(workerNode,
																shardIntervalArray,
																placementListArray,
																shardCount);
		DropShardsFromWorker(workerNode, relationId, shardDropList);
	}

	pfree(placementListArray);

	PG_RETURN_VOID();
}


/*
 * FindAbsentShardPlacementsOnWorker compiles shard interval list of shards
 * that do not have registered placement at given worker node.
 */
List *
FindAbsentShardPlacementsOnWorker(WorkerNode *workerNode,
								  ShardInterval **shardIntervalArray,
								  List **placementListArray, int shardCount)
{
	List *absentShardIntervalList = NIL;

	int shardIndex = 0;
	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		ShardInterval *shardInterval = shardIntervalArray[shardIndex];
		List *placementList = placementListArray[shardIndex];

		ListCell *placementCell = NULL;
		foreach(placementCell, placementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);

			/*
			 * Append shard interval to absent list if none of its placements is on
			 * the worker.
			 */
			if (placement->nodePort == workerNode->workerPort &&
				strncmp(placement->nodeName, workerNode->workerName, WORKER_LENGTH) == 0)
			{
				break;
			}
			else if (lnext(placementCell) == NULL)
			{
				absentShardIntervalList = lappend(absentShardIntervalList, shardInterval);
			}
		}
	}

	return absentShardIntervalList;
}


/*
 * DropShardsFromWorker drops provided shards belonging to a relation from
 * given worker. It does not change any metadata at the master.
 */
static void
DropShardsFromWorker(WorkerNode *workerNode, Oid relationId, List *shardIntervalList)
{
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *relationName = get_rel_name(relationId);
	char relationKind = get_rel_relkind(relationId);
	StringInfo workerCommand = makeStringInfo();
	StringInfo shardNames = makeStringInfo();
	ListCell *shardIntervalCell = NULL;

	if (shardIntervalList == NIL)
	{
		return;
	}

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		char *shardName = pstrdup(relationName);
		char *quotedShardName = NULL;

		AppendShardIdToName(&shardName, shardInterval->shardId);
		quotedShardName = quote_qualified_identifier(schemaName, shardName);
		appendStringInfo(shardNames, "%s", quotedShardName);

		/* append a comma after the shard name if there are more shards */
		if (lnext(shardIntervalCell) != NULL)
		{
			appendStringInfo(shardNames, ", ");
		}
	}

	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(workerCommand, DROP_REGULAR_TABLE_COMMAND, shardNames->data);
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		appendStringInfo(workerCommand, DROP_FOREIGN_TABLE_COMMAND, shardNames->data);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("expire target is not a regular or foreign table")));
	}

	ExecuteRemoteCommand(workerNode->workerName, workerNode->workerPort, workerCommand);
}
