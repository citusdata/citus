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
#include "distributed/connection_management.h"
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
static void RepairShardPlacement(int64 shardId, char *sourceNodeName,
								 int32 sourceNodePort, char *targetNodeName,
								 int32 targetNodePort);
static void EnsureShardCanBeRepaired(int64 shardId, char *sourceNodeName,
									 int32 sourceNodePort, char *targetNodeName,
									 int32 targetNodePort);
static List * RecreateTableDDLCommandList(Oid relationId);
static List * WorkerApplyShardDDLCommandList(List *ddlCommandList, int64 shardId);

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
	bool doRepair = PG_GETARG_BOOL(5);

	char *sourceNodeName = text_to_cstring(sourceNodeNameText);
	char *targetNodeName = text_to_cstring(targetNodeNameText);

	if (!doRepair)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("master_copy_shard_placement() "
							   "with do not repair functionality "
							   "is only supported on Citus Enterprise")));
	}

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	/* RepairShardPlacement function repairs only given shard */
	RepairShardPlacement(shardId, sourceNodeName, sourceNodePort, targetNodeName,
						 targetNodePort);

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
 * RepairShardPlacement repairs given shard from a source node to target node.
 * This function is not co-location aware. It only repairs given shard.
 */
static void
RepairShardPlacement(int64 shardId, char *sourceNodeName, int32 sourceNodePort,
					 char *targetNodeName, int32 targetNodePort)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	char relationKind = get_rel_relkind(distributedTableId);
	char *tableOwner = TableOwner(shardInterval->relationId);
	bool missingOk = false;

	List *ddlCommandList = NIL;
	List *foreignConstraintCommandList = NIL;
	List *placementList = NIL;
	ShardPlacement *placement = NULL;

	EnsureTableOwner(distributedTableId);

	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		char *relationName = get_rel_name(distributedTableId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot repair shard"),
						errdetail("Table %s is a foreign table. Repairing "
								  "shards backed by foreign tables is "
								  "not supported.", relationName)));
	}

	/*
	 * We plan to move the placement to the healthy state, so we need to grab a shard
	 * metadata lock (in exclusive mode).
	 */
	LockShardDistributionMetadata(shardId, ExclusiveLock);

	/*
	 * For shard repair, there should be healthy placement in source node and unhealthy
	 * placement in the target node.
	 */
	EnsureShardCanBeRepaired(shardId, sourceNodeName, sourceNodePort, targetNodeName,
							 targetNodePort);

	/* we generate necessary commands to recreate the shard in target node */
	ddlCommandList = CopyShardCommandList(shardInterval, sourceNodeName, sourceNodePort);
	foreignConstraintCommandList = CopyShardForeignConstraintCommandList(shardInterval);
	ddlCommandList = list_concat(ddlCommandList, foreignConstraintCommandList);
	SendCommandListToWorkerInSingleTransaction(targetNodeName, targetNodePort, tableOwner,
											   ddlCommandList);

	/* after successful repair, we update shard state as healthy*/
	placementList = ShardPlacementList(shardId);
	placement = SearchShardPlacementInList(placementList, targetNodeName, targetNodePort,
										   missingOk);
	UpdateShardPlacementState(placement->placementId, FILE_FINALIZED);
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
 * SearchShardPlacementInList searches a provided list for a shard placement with the
 * specified node name and port. If missingOk is set to true, this function returns NULL
 * if no such placement exists in the provided list, otherwise it throws an error.
 */
ShardPlacement *
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
 * CopyShardCommandList generates command list to copy the given shard placement
 * from the source node to the target node.
 */
List *
CopyShardCommandList(ShardInterval *shardInterval,
					 char *sourceNodeName, int32 sourceNodePort)
{
	int64 shardId = shardInterval->shardId;
	char *shardName = ConstructQualifiedShardName(shardInterval);
	List *tableRecreationCommandList = NIL;
	List *indexCommandList = NIL;
	List *copyShardToNodeCommandsList = NIL;
	StringInfo copyShardDataCommand = makeStringInfo();
	Oid relationId = shardInterval->relationId;

	tableRecreationCommandList = RecreateTableDDLCommandList(relationId);
	tableRecreationCommandList =
		WorkerApplyShardDDLCommandList(tableRecreationCommandList, shardId);

	copyShardToNodeCommandsList = list_concat(copyShardToNodeCommandsList,
											  tableRecreationCommandList);

	appendStringInfo(copyShardDataCommand, WORKER_APPEND_TABLE_TO_SHARD,
					 quote_literal_cstr(shardName), /* table to append */
					 quote_literal_cstr(shardName), /* remote table name */
					 quote_literal_cstr(sourceNodeName), /* remote host */
					 sourceNodePort); /* remote port */

	copyShardToNodeCommandsList = lappend(copyShardToNodeCommandsList,
										  copyShardDataCommand->data);

	indexCommandList = GetTableIndexAndConstraintCommands(relationId);
	indexCommandList = WorkerApplyShardDDLCommandList(indexCommandList, shardId);

	copyShardToNodeCommandsList = list_concat(copyShardToNodeCommandsList,
											  indexCommandList);

	return copyShardToNodeCommandsList;
}


/*
 * CopyShardForeignConstraintCommandList generates command list to create foreign
 * constraints existing in source shard after copying it to the other node.
 */
List *
CopyShardForeignConstraintCommandList(ShardInterval *shardInterval)
{
	List *copyShardForeignConstraintCommandList = NIL;

	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	int shardIndex = 0;

	List *commandList = GetTableForeignConstraintCommands(shardInterval->relationId);
	ListCell *commandCell = NULL;

	/* we will only use shardIndex if there is a foreign constraint */
	if (commandList != NIL)
	{
		shardIndex = ShardIndex(shardInterval);
	}

	foreach(commandCell, commandList)
	{
		char *command = (char *) lfirst(commandCell);
		char *escapedCommand = quote_literal_cstr(command);

		Oid referencedRelationId = InvalidOid;
		Oid referencedSchemaId = InvalidOid;
		char *referencedSchemaName = NULL;
		char *escapedReferencedSchemaName = NULL;
		uint64 referencedShardId = INVALID_SHARD_ID;

		StringInfo applyForeignConstraintCommand = makeStringInfo();

		/* we need to parse the foreign constraint command to get referencing table id */
		referencedRelationId = ForeignConstraintGetReferencedTableId(command);
		if (referencedRelationId == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cannot create foreign key constraint"),
							errdetail("Referenced relation cannot be found.")));
		}

		referencedSchemaId = get_rel_namespace(referencedRelationId);
		referencedSchemaName = get_namespace_name(referencedSchemaId);
		escapedReferencedSchemaName = quote_literal_cstr(referencedSchemaName);
		referencedShardId = ColocatedShardIdInRelation(referencedRelationId, shardIndex);

		appendStringInfo(applyForeignConstraintCommand,
						 WORKER_APPLY_INTER_SHARD_DDL_COMMAND, shardInterval->shardId,
						 escapedSchemaName, referencedShardId,
						 escapedReferencedSchemaName, escapedCommand);

		copyShardForeignConstraintCommandList = lappend(
			copyShardForeignConstraintCommandList,
			applyForeignConstraintCommand->data);
	}

	return copyShardForeignConstraintCommandList;
}


/*
 * ConstuctQualifiedShardName creates the fully qualified name string of the
 * given shard in <schema>.<table_name>_<shard_id> format.
 */
char *
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
 * returned by GetTableCreationCommands except that the list begins with a "DROP TABLE"
 * or "DROP FOREIGN TABLE" statement to facilitate idempotent recreation of a placement.
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
	bool includeSequenceDefaults = false;

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
	createCommandList = GetTableCreationCommands(relationId, includeSequenceDefaults);
	recreateCommandList = list_concat(dropCommandList, createCommandList);

	return recreateCommandList;
}


/*
 * WorkerApplyShardDDLCommandList wraps all DDL commands in ddlCommandList
 * in a call to worker_apply_shard_ddl_command to apply the DDL command to
 * the shard specified by shardId.
 */
static List *
WorkerApplyShardDDLCommandList(List *ddlCommandList, int64 shardId)
{
	List *applyDdlCommandList = NIL;
	ListCell *ddlCommandCell = NULL;

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = lfirst(ddlCommandCell);
		char *escapedDdlCommand = quote_literal_cstr(ddlCommand);

		StringInfo applyDdlCommand = makeStringInfo();
		appendStringInfo(applyDdlCommand,
						 WORKER_APPLY_SHARD_DDL_COMMAND_WITHOUT_SCHEMA,
						 shardId, escapedDdlCommand);

		applyDdlCommandList = lappend(applyDdlCommandList, applyDdlCommand->data);
	}

	return applyDdlCommandList;
}
