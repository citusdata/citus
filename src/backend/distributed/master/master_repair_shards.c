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
#include "distributed/colocation_utils.h"
#include "distributed/connection_cache.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* local function forward declarations */
static void EnsureShardCanBeRepaired(int64 shardId, char *sourceNodeName,
									 int32 sourceNodePort, char *targetNodeName,
									 int32 targetNodePort);
static void EnsureShardCanBeMoved(int64 shardId, char *sourceNodeName,
								  int32 sourceNodePort);
static ShardPlacement * SearchShardPlacementInList(List *shardPlacementList,
												   char *nodeName, uint32 nodePort,
												   bool missingOk);
static void CopyShardPlacement(int64 shardId, char *sourceNodeName, int32 sourceNodePort,
							   char *targetNodeName, int32 targetNodePort,
							   bool doRepair);
static List * CopyShardCommandList(ShardInterval *shardInterval, char *sourceNodeName,
								   int32 sourceNodePort);
static char * ConstructQualifiedShardName(ShardInterval *shardInterval);
static List * RecreateTableDDLCommandList(Oid relationId);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_copy_shard_placement);
PG_FUNCTION_INFO_V1(master_move_shard_placement);


/*
 * master_copy_shard_placement implements a user-facing UDF to repair data from
 * a healthy (source) node to an inactive (target) node. To accomplish this it
 * entirely recreates the table structure before copying all data. During this
 * time all modifications are paused to the shard. After successful repair, the
 * inactive placement is marked healthy and modifications may continue. If the
 * repair fails at any point, this function throws an error, leaving the node
 * in an unhealthy state. Please note that master_copy_shard_placement copies
 * given shard along with its co-located shards.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	text *sourceNodeNameText = PG_GETARG_TEXT_P(1);
	int32 sourceNodePort = PG_GETARG_INT32(2);
	text *targetNodeNameText = PG_GETARG_TEXT_P(3);
	int32 targetNodePort = PG_GETARG_INT32(4);
	bool doRepair = true;

	char *sourceNodeName = text_to_cstring(sourceNodeNameText);
	char *targetNodeName = text_to_cstring(targetNodeNameText);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;
	List *colocatedTableList = ColocatedTableList(distributedTableId);
	ListCell *colocatedTableCell = NULL;
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
	ListCell *colocatedShardCell = NULL;

	foreach(colocatedTableCell, colocatedTableList)
	{
		Oid colocatedTableId = lfirst_oid(colocatedTableCell);
		char relationKind = '\0';

		/* check that user has owner rights in all co-located tables */
		EnsureTableOwner(colocatedTableId);

		relationKind = get_rel_relkind(colocatedTableId);
		if (relationKind == RELKIND_FOREIGN_TABLE)
		{
			char *relationName = get_rel_name(colocatedTableId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot repair shard"),
							errdetail("Table %s is a foreign table. Repairing "
									  "shards backed by foreign tables is "
									  "not supported.", relationName)));
		}
	}

	/* we sort colocatedShardList so that lock operations will not cause any deadlocks */
	colocatedShardList = SortList(colocatedShardList, CompareShardIntervalsById);
	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		uint64 colocatedShardId = colocatedShard->shardId;

		/*
		 * We've stopped data modifications of this shard, but we plan to move
		 * a placement to the healthy state, so we need to grab a shard metadata
		 * lock (in exclusive mode) as well.
		 */
		LockShardDistributionMetadata(colocatedShardId, ExclusiveLock);

		/*
		 * If our aim is repairing, we should be sure that there is an unhealthy
		 * placement in target node. We use EnsureShardCanBeRepaired function
		 * to be sure that there is an unhealthy placement in target node. If
		 * we just want to copy the shard without any repair, it is enough to use
		 * EnsureShardCanBeCopied which just checks there is a placement in source
		 * and no placement in target node.
		 */
		if (doRepair)
		{
			/*
			 * After #810 is fixed, we should remove this check and call EnsureShardCanBeRepaired
			 * for all shard ids
			 */
			if (colocatedShardId == shardId)
			{
				EnsureShardCanBeRepaired(colocatedShardId, sourceNodeName, sourceNodePort,
										 targetNodeName, targetNodePort);
			}
			else
			{
				EnsureShardCanBeMoved(colocatedShardId, sourceNodeName, sourceNodePort);
			}
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("master_copy_shard_placement() without repair option "
								   "is only supported on Citus Enterprise")));
		}
	}


	/* CopyShardPlacement function copies given shard with its co-located shards */
	CopyShardPlacement(shardId, sourceNodeName, sourceNodePort, targetNodeName,
					   targetNodePort, doRepair);

	PG_RETURN_VOID();
}


/*
 * master_move_shard_placement moves given shard (and its co-located shards) from one
 * node to the other node.
 */
Datum
master_move_shard_placement(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("master_move_shard_placement() is only supported on "
						   "Citus Enterprise")));
}


/*
 * EnsureShardCanBeRepaired checks if the given shard has a healthy placement in the source
 * node and inactive node on the target node.
 */
static void
EnsureShardCanBeRepaired(int64 shardId, char *sourceNodeName, int32 sourceNodePort,
						 char *targetNodeName, int32 targetNodePort)
{
	List *shardPlacementList = ShardPlacementList(shardId);
	ShardPlacement *sourcePlacement = NULL;
	ShardPlacement *targetPlacement = NULL;
	bool missingSourceOk = false;
	bool missingTargetOk = false;

	sourcePlacement = SearchShardPlacementInList(shardPlacementList, sourceNodeName,
												 sourceNodePort, missingSourceOk);
	if (sourcePlacement->shardState != FILE_FINALIZED)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source placement must be in finalized state")));
	}

	targetPlacement = SearchShardPlacementInList(shardPlacementList, targetNodeName,
												 targetNodePort, missingTargetOk);
	if (targetPlacement->shardState != FILE_INACTIVE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("target placement must be in inactive state")));
	}
}


/*
 * EnsureShardCanBeMoved checks if the given shard has a placement in the source node but
 * not on the target node. It is important to note that SearchShardPlacementInList
 * function already generates error if given shard does not have a placement in the
 * source node. Therefore we do not perform extra check.
 */
static void
EnsureShardCanBeMoved(int64 shardId, char *sourceNodeName, int32 sourceNodePort)
{
	List *shardPlacementList = ShardPlacementList(shardId);
	bool missingSourceOk = false;

	/* Actual check is done in SearchShardPlacementInList  */
	SearchShardPlacementInList(shardPlacementList, sourceNodeName, sourceNodePort,
							   missingSourceOk);
}


/*
 * SearchShardPlacementInList searches a provided list for a shard placement with the
 * specified node name and port. If missingOk is set to true, this function returns NULL
 * if no such placement exists in the provided list, otherwise it throws an error.
 */
static ShardPlacement *
SearchShardPlacementInList(List *shardPlacementList, char *nodeName, uint32 nodePort, bool
						   missingOk)
{
	ListCell *shardPlacementCell = NULL;
	ShardPlacement *matchingPlacement = NULL;

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
		if (missingOk)
		{
			return NULL;
		}

		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("could not find placement matching \"%s:%d\"",
							   nodeName, nodePort),
						errhint("Confirm the placement still exists and try again.")));
	}

	return matchingPlacement;
}


/*
 * CopyShardPlacement copies a shard along with its co-located shards from a source node
 * to target node. CopyShardPlacement does not make any checks about state of the shards.
 * It is caller's responsibility to make those checks if they are necessary.
 */
static void
CopyShardPlacement(int64 shardId, char *sourceNodeName, int32 sourceNodePort,
				   char *targetNodeName, int32 targetNodePort, bool doRepair)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
	ListCell *colocatedShardCell = NULL;
	List *ddlCommandList = NIL;
	char *nodeUser = CitusExtensionOwnerName();

	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		List *colocatedShardDdlList = NIL;

		colocatedShardDdlList = CopyShardCommandList(colocatedShard, sourceNodeName,
													 sourceNodePort);

		ddlCommandList = list_concat(ddlCommandList, colocatedShardDdlList);
	}

	HOLD_INTERRUPTS();

	SendCommandListToWorkerInSingleTransaction(targetNodeName, targetNodePort, nodeUser,
											   ddlCommandList);

	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShard = (ShardInterval *) lfirst(colocatedShardCell);
		uint64 colocatedShardId = colocatedShard->shardId;

		/*
		 * If we call this function for repair purposes, the caller should have
		 * removed the old shard placement metadata.
		 */
		if (doRepair)
		{
			List *shardPlacementList = ShardPlacementList(colocatedShardId);
			bool missingSourceOk = false;

			ShardPlacement *placement = SearchShardPlacementInList(shardPlacementList,
																   targetNodeName,
																   targetNodePort,
																   missingSourceOk);

			UpdateShardPlacementState(placement->placementId, FILE_FINALIZED);
		}
		else
		{
			InsertShardPlacementRow(colocatedShardId, INVALID_PLACEMENT_ID,
									FILE_FINALIZED, ShardLength(colocatedShardId),
									targetNodeName,
									targetNodePort);
		}
	}

	RESUME_INTERRUPTS();
}


/*
 * CopyShardCommandList generates command list to copy the given shard placement
 * from the source node to the target node.
 */
static List *
CopyShardCommandList(ShardInterval *shardInterval,
					 char *sourceNodeName, int32 sourceNodePort)
{
	char *shardName = ConstructQualifiedShardName(shardInterval);
	List *ddlCommandList = NIL;
	ListCell *ddlCommandCell = NULL;
	List *copyShardToNodeCommandsList = NIL;
	StringInfo copyShardDataCommand = makeStringInfo();

	ddlCommandList = RecreateTableDDLCommandList(shardInterval->relationId);

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = lfirst(ddlCommandCell);
		char *escapedDdlCommand = quote_literal_cstr(ddlCommand);

		StringInfo applyDdlCommand = makeStringInfo();
		appendStringInfo(applyDdlCommand,
						 WORKER_APPLY_SHARD_DDL_COMMAND_WITHOUT_SCHEMA,
						 shardInterval->shardId, escapedDdlCommand);

		copyShardToNodeCommandsList = lappend(copyShardToNodeCommandsList,
											  applyDdlCommand->data);
	}

	appendStringInfo(copyShardDataCommand, WORKER_APPEND_TABLE_TO_SHARD,
					 quote_literal_cstr(shardName), /* table to append */
					 quote_literal_cstr(shardName), /* remote table name */
					 quote_literal_cstr(sourceNodeName), /* remote host */
					 sourceNodePort); /* remote port */

	copyShardToNodeCommandsList = lappend(copyShardToNodeCommandsList,
										  copyShardDataCommand->data);

	return copyShardToNodeCommandsList;
}


/*
 * ConstuctQualifiedShardName creates the fully qualified name string of the
 * given shard in <schema>.<table_name>_<shard_id> format.
 *
 * FIXME: Copied from Citus-MX, should be removed once those changes checked-in to Citus.
 */
static char *
ConstructQualifiedShardName(ShardInterval *shardInterval)
{
	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(shardInterval->relationId);
	char *shardName = NULL;

	shardName = pstrdup(tableName);
	AppendShardIdToName(&shardName, shardInterval->shardId);
	shardName = quote_qualified_identifier(schemaName, shardName);

	return shardName;
}


/*
 * RecreateTableDDLCommandList returns a list of DDL statements similar to that
 * returned by GetTableDDLEvents except that the list begins with a "DROP TABLE"
 * or "DROP FOREIGN TABLE" statement to facilitate total recreation of a placement.
 */
static List *
RecreateTableDDLCommandList(Oid relationId)
{
	const char *relationName = get_rel_name(relationId);
	Oid relationSchemaId = get_rel_namespace(relationId);
	const char *relationSchemaName = get_namespace_name(relationSchemaId);
	const char *qualifiedRelationName = quote_qualified_identifier(relationSchemaName,
																   relationName);

	StringInfo dropCommand = makeStringInfo();
	List *createCommandList = NIL;
	List *dropCommandList = NIL;
	List *recreateCommandList = NIL;
	char relationKind = get_rel_relkind(relationId);

	/* build appropriate DROP command based on relation kind */
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(dropCommand, DROP_REGULAR_TABLE_COMMAND,
						 qualifiedRelationName);
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		appendStringInfo(dropCommand, DROP_FOREIGN_TABLE_COMMAND,
						 qualifiedRelationName);
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
