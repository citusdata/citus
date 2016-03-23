/*-------------------------------------------------------------------------
 *
 * master_repair_shards.c
 *
 * This file contains functions to repair unhealthy shard placements using data
 * from healthy ones.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"

#include <string.h>

#include "catalog/pg_class.h"
#include "distributed/connection_cache.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* local function forward declarations */
static ShardPlacement * SearchShardPlacementInList(List *shardPlacementList,
												   text *nodeName, uint32 nodePort);
static List * RecreateTableDDLCommandList(Oid relationId);
static bool CopyDataFromFinalizedPlacement(Oid distributedTableId, int64 shardId,
										   ShardPlacement *healthyPlacement,
										   ShardPlacement *placementToRepair);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_copy_shard_placement);


/*
 * master_copy_shard_placement implements a user-facing UDF to copy data from
 * a healthy (source) node to an inactive (target) node. To accomplish this it
 * entirely recreates the table structure before copying all data. During this
 * time all modifications are paused to the shard. After successful repair, the
 * inactive placement is marked healthy and modifications may continue. If the
 * repair fails at any point, this function throws an error, leaving the node
 * in an unhealthy state.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	text *sourceNodeName = PG_GETARG_TEXT_P(1);
	int32 sourceNodePort = PG_GETARG_INT32(2);
	text *targetNodeName = PG_GETARG_TEXT_P(3);
	int32 targetNodePort = PG_GETARG_INT32(4);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *shardPlacementList = NIL;
	ShardPlacement *sourcePlacement = NULL;
	ShardPlacement *targetPlacement = NULL;
	WorkerNode *targetNode = NULL;
	List *ddlCommandList = NIL;
	bool dataCopied = false;
	char relationKind = '\0';

	/*
	 * By taking an exclusive lock on the shard, we both stop all modifications
	 * (INSERT, UPDATE, or DELETE) and prevent concurrent repair operations from
	 * being able to operate on this shard.
	 */
	LockShardResource(shardId, ExclusiveLock);

	/*
	 * We've stopped data modifications of this shard, but we plan to move
	 * a placement to the healthy state, so we need to grab a shard metadata
	 * lock (in exclusive mode) as well.
	 */
	LockShardDistributionMetadata(shardId, ExclusiveLock);

	shardPlacementList = ShardPlacementList(shardId);
	sourcePlacement = SearchShardPlacementInList(shardPlacementList, sourceNodeName,
												 sourceNodePort);
	if (sourcePlacement->shardState != FILE_FINALIZED)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source placement must be in finalized state")));
	}

	targetPlacement = SearchShardPlacementInList(shardPlacementList, targetNodeName,
												 targetNodePort);
	if (targetPlacement->shardState != FILE_INACTIVE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("target placement must be in inactive state")));
	}

	relationKind = get_rel_relkind(distributedTableId);
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot repair shard"),
						errdetail("Repairing shards backed by foreign tables is "
								  "not supported.")));
	}

	targetNode = palloc0(sizeof(WorkerNode));
	targetNode->inWorkerFile = true;
	strlcpy(targetNode->workerName, targetPlacement->nodeName, WORKER_LENGTH);
	targetNode->workerPort = targetPlacement->nodePort;

	/* retrieve DDL commands needed to drop and recreate table*/
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId);

	/* remove existing (unhealthy) placement row; CreateShardPlacements will recreate */
	DeleteShardPlacementRow(targetPlacement->shardId, targetPlacement->nodeName,
							targetPlacement->nodePort);

	/* finally, drop/recreate remote table and add back row (in healthy state) */
	CreateShardPlacements(shardId, ddlCommandList, list_make1(targetNode), 0, 1);

	HOLD_INTERRUPTS();

	dataCopied = CopyDataFromFinalizedPlacement(distributedTableId, shardId,
												sourcePlacement, targetPlacement);
	if (!dataCopied)
	{
		ereport(ERROR, (errmsg("could not copy shard data"),
						errhint("Consult recent messages in the server logs for "
								"details.")));
	}

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * SearchShardPlacementInList searches a provided list for a shard placement
 * with the specified node name and port. This function throws an error if no
 * such placement exists in the provided list.
 */
static ShardPlacement *
SearchShardPlacementInList(List *shardPlacementList, text *nodeNameText, uint32 nodePort)
{
	ListCell *shardPlacementCell = NULL;
	ShardPlacement *matchingPlacement = NULL;
	char *nodeName = text_to_cstring(nodeNameText);

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = lfirst(shardPlacementCell);

		if (strncmp(nodeName, shardPlacement->nodeName, MAX_NODE_LENGTH) == 0 &&
			nodePort == shardPlacement->nodePort)
		{
			matchingPlacement = shardPlacement;
			break;
		}
	}

	if (matchingPlacement == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("could not find placement matching \"%s:%d\"",
							   nodeName, nodePort),
						errhint("Confirm the placement still exists and try again.")));
	}

	return matchingPlacement;
}


/*
 * RecreateTableDDLCommandList returns a list of DDL statements similar to that
 * returned by GetTableDDLEvents except that the list begins with a "DROP TABLE"
 * or "DROP FOREIGN TABLE" statement to facilitate total recreation of a placement.
 */
static List *
RecreateTableDDLCommandList(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	StringInfo dropCommand = makeStringInfo();
	List *createCommandList = NIL;
	List *dropCommandList = NIL;
	List *recreateCommandList = NIL;
	char relationKind = get_rel_relkind(relationId);

	/* build appropriate DROP command based on relation kind */
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(dropCommand, DROP_REGULAR_TABLE_COMMAND,
						 quote_identifier(relationName));
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		appendStringInfo(dropCommand, DROP_FOREIGN_TABLE_COMMAND,
						 quote_identifier(relationName));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("repair target is not a regular or foreign table")));
	}

	dropCommandList = list_make1(dropCommand->data);

	createCommandList = GetTableDDLEvents(relationId);

	recreateCommandList = list_concat(dropCommandList, createCommandList);

	return recreateCommandList;
}


/*
 * CopyDataFromFinalizedPlacement copies a the data for a shard (identified by
 * a relation and shard identifier) from a healthy placement to one needing
 * repair. The unhealthy placement must already have an empty relation in place
 * to receive rows from the healthy placement. This function returns a boolean
 * indicating success or failure.
 */
static bool
CopyDataFromFinalizedPlacement(Oid distributedTableId, int64 shardId,
							   ShardPlacement *healthyPlacement,
							   ShardPlacement *placementToRepair)
{
	char *relationName = get_rel_name(distributedTableId);
	const char *shardName = NULL;
	StringInfo copyRelationQuery = makeStringInfo();
	List *queryResultList = NIL;
	bool copySuccessful = false;

	AppendShardIdToName(&relationName, shardId);
	shardName = quote_identifier(relationName);

	appendStringInfo(copyRelationQuery, WORKER_APPEND_TABLE_TO_SHARD,
					 quote_literal_cstr(shardName), /* table to append */
					 quote_literal_cstr(shardName), /* remote table name */
					 quote_literal_cstr(healthyPlacement->nodeName), /* remote host */
					 healthyPlacement->nodePort); /* remote port */

	queryResultList = ExecuteRemoteQuery(placementToRepair->nodeName,
										 placementToRepair->nodePort, copyRelationQuery);
	if (queryResultList != NIL)
	{
		copySuccessful = true;
	}

	return copySuccessful;
}
