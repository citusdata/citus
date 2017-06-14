/*-------------------------------------------------------------------------
 *
 * master_stage_protocol.c
 *
 * Routines for staging PostgreSQL table data as shards into the distributed
 * cluster. These user-defined functions are similar to the psql-side \stage
 * command, but also differ from them in that users stage data from tables and
 * not files, and that they can also append to existing shards.
 *
 * Copyright (c) 2013-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/tablecmds.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/multi_client_executor.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/placement_connection.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/tqual.h"


/* Local functions forward declarations */
static bool WorkerShardStats(ShardPlacement *placement, Oid relationId,
							 char *shardName, uint64 *shardSize,
							 text **shardMinValue, text **shardMaxValue);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_empty_shard);
PG_FUNCTION_INFO_V1(master_append_table_to_shard);
PG_FUNCTION_INFO_V1(master_update_shard_statistics);


/*
 * master_create_empty_shard creates an empty shard for the given distributed
 * table. For this, the function first gets a list of candidate nodes, connects
 * to these nodes, and issues DDL commands on the nodes to create empty shard
 * placements. The function then updates metadata on the master node to make
 * this shard (and its placements) visible.
 */
Datum
master_create_empty_shard(PG_FUNCTION_ARGS)
{
	text *relationNameText = PG_GETARG_TEXT_P(0);
	char *relationName = text_to_cstring(relationNameText);
	List *workerNodeList = NIL;
	uint64 shardId = INVALID_SHARD_ID;
	List *ddlEventList = NULL;
	uint32 attemptableNodeCount = 0;
	uint32 liveNodeCount = 0;

	uint32 candidateNodeIndex = 0;
	List *candidateNodeList = NIL;
	text *nullMinValue = NULL;
	text *nullMaxValue = NULL;
	char partitionMethod = 0;
	char storageType = SHARD_STORAGE_TABLE;

	Oid relationId = ResolveRelationId(relationNameText);
	char relationKind = get_rel_relkind(relationId);
	char *relationOwner = TableOwner(relationId);
	char replicationModel = REPLICATION_MODEL_INVALID;
	bool includeSequenceDefaults = false;

	CheckCitusVersion(ERROR);

	workerNodeList = ActiveWorkerNodeList();

	EnsureTablePermissions(relationId, ACL_INSERT);
	CheckDistributedTable(relationId);

	/*
	 * We check whether the table is a foreign table or not. If it is, we set
	 * storage type as foreign also. Only exception is if foreign table is a
	 * foreign cstore table, in this case we set storage type as columnar.
	 *
	 * i.e. While setting storage type, columnar has priority over foreign.
	 */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		bool cstoreTable = cstoreTable = CStoreTable(relationId);
		if (cstoreTable)
		{
			storageType = SHARD_STORAGE_COLUMNAR;
		}
		else
		{
			storageType = SHARD_STORAGE_FOREIGN;
		}
	}

	partitionMethod = PartitionMethod(relationId);
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errmsg("relation \"%s\" is a hash partitioned table",
							   relationName),
						errdetail("We currently don't support creating shards "
								  "on hash-partitioned tables")));
	}
	else if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		ereport(ERROR, (errmsg("relation \"%s\" is a reference table",
							   relationName),
						errdetail("We currently don't support creating shards "
								  "on reference tables")));
	}

	replicationModel = TableReplicationModel(relationId);

	EnsureReplicationSettings(relationId, replicationModel);

	/* generate new and unique shardId from sequence */
	shardId = GetNextShardId();

	/* get table DDL commands to replay on the worker node */
	ddlEventList = GetTableDDLEvents(relationId, includeSequenceDefaults);

	/* if enough live nodes, add an extra candidate node as backup */
	attemptableNodeCount = ShardReplicationFactor;
	liveNodeCount = WorkerGetLiveNodeCount();
	if (liveNodeCount > ShardReplicationFactor)
	{
		attemptableNodeCount = ShardReplicationFactor + 1;
	}

	/* first retrieve a list of random nodes for shard placements */
	while (candidateNodeIndex < attemptableNodeCount)
	{
		WorkerNode *candidateNode = NULL;

		if (ShardPlacementPolicy == SHARD_PLACEMENT_LOCAL_NODE_FIRST)
		{
			candidateNode = WorkerGetLocalFirstCandidateNode(candidateNodeList);
		}
		else if (ShardPlacementPolicy == SHARD_PLACEMENT_ROUND_ROBIN)
		{
			candidateNode = WorkerGetRoundRobinCandidateNode(workerNodeList, shardId,
															 candidateNodeIndex);
		}
		else if (ShardPlacementPolicy == SHARD_PLACEMENT_RANDOM)
		{
			candidateNode = WorkerGetRandomCandidateNode(candidateNodeList);
		}
		else
		{
			ereport(ERROR, (errmsg("unrecognized shard placement policy")));
		}

		if (candidateNode == NULL)
		{
			ereport(ERROR, (errmsg("could only find %u of %u possible nodes",
								   candidateNodeIndex, attemptableNodeCount)));
		}

		candidateNodeList = lappend(candidateNodeList, candidateNode);
		candidateNodeIndex++;
	}

	CreateShardPlacements(relationId, shardId, ddlEventList, relationOwner,
						  candidateNodeList, 0, ShardReplicationFactor);

	InsertShardRow(relationId, shardId, storageType, nullMinValue, nullMaxValue);

	PG_RETURN_INT64(shardId);
}


/*
 * master_append_table_to_shard appends the given table's contents to the given
 * shard, and updates shard metadata on the master node. If the function fails
 * to append table data to all shard placements, it doesn't update any metadata
 * and errors out. Else if the function fails to append table data to some of
 * the shard placements, it marks those placements as invalid. These invalid
 * placements will get cleaned up during shard rebalancing.
 */
Datum
master_append_table_to_shard(PG_FUNCTION_ARGS)
{
	uint64 shardId = PG_GETARG_INT64(0);
	text *sourceTableNameText = PG_GETARG_TEXT_P(1);
	text *sourceNodeNameText = PG_GETARG_TEXT_P(2);
	uint32 sourceNodePort = PG_GETARG_UINT32(3);

	char *sourceTableName = text_to_cstring(sourceTableNameText);
	char *sourceNodeName = text_to_cstring(sourceNodeNameText);

	Oid shardSchemaOid = 0;
	char *shardSchemaName = NULL;
	char *shardTableName = NULL;
	char *shardQualifiedName = NULL;
	List *shardPlacementList = NIL;
	ListCell *shardPlacementCell = NULL;
	uint64 newShardSize = 0;
	uint64 shardMaxSizeInBytes = 0;
	float4 shardFillLevel = 0.0;
	char partitionMethod = 0;

	ShardInterval *shardInterval = NULL;
	Oid relationId = InvalidOid;
	bool cstoreTable = false;

	char storageType = 0;

	CheckCitusVersion(ERROR);

	shardInterval = LoadShardInterval(shardId);
	relationId = shardInterval->relationId;
	cstoreTable = CStoreTable(relationId);
	storageType = shardInterval->storageType;

	EnsureTablePermissions(relationId, ACL_INSERT);

	if (storageType != SHARD_STORAGE_TABLE && !cstoreTable)
	{
		ereport(ERROR, (errmsg("cannot append to shardId " UINT64_FORMAT, shardId),
						errdetail("The underlying shard is not a regular table")));
	}

	partitionMethod = PartitionMethod(relationId);
	if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod == DISTRIBUTE_BY_NONE)
	{
		ereport(ERROR, (errmsg("cannot append to shardId " UINT64_FORMAT, shardId),
						errdetail("We currently don't support appending to shards "
								  "in hash-partitioned or reference tables")));
	}

	/* ensure that the shard placement metadata does not change during the append */
	LockShardDistributionMetadata(shardId, ShareLock);

	/* serialize appends to the same shard */
	LockShardResource(shardId, ExclusiveLock);

	/* get schame name of the target shard */
	shardSchemaOid = get_rel_namespace(relationId);
	shardSchemaName = get_namespace_name(shardSchemaOid);

	/* Build shard table name. */
	shardTableName = get_rel_name(relationId);
	AppendShardIdToName(&shardTableName, shardId);

	shardQualifiedName = quote_qualified_identifier(shardSchemaName, shardTableName);

	shardPlacementList = FinalizedShardPlacementList(shardId);
	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find any shard placements for shardId "
							   UINT64_FORMAT, shardId),
						errhint("Try running master_create_empty_shard() first")));
	}

	BeginOrContinueCoordinatedTransaction();

	/* issue command to append table to each shard placement */
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		MultiConnection *connection = GetPlacementConnection(FOR_DML, shardPlacement,
															 NULL);
		PGresult *queryResult = NULL;
		int executeResult = 0;

		StringInfo workerAppendQuery = makeStringInfo();
		appendStringInfo(workerAppendQuery, WORKER_APPEND_TABLE_TO_SHARD,
						 quote_literal_cstr(shardQualifiedName),
						 quote_literal_cstr(sourceTableName),
						 quote_literal_cstr(sourceNodeName), sourceNodePort);

		RemoteTransactionBeginIfNecessary(connection);

		executeResult = ExecuteOptionalRemoteCommand(connection, workerAppendQuery->data,
													 &queryResult);
		PQclear(queryResult);
		ForgetResults(connection);

		if (executeResult != 0)
		{
			MarkRemoteTransactionFailed(connection, false);
		}
	}

	MarkFailedShardPlacements();

	/* update shard statistics and get new shard size */
	newShardSize = UpdateShardStatistics(shardId);

	/* calculate ratio of current shard size compared to shard max size */
	shardMaxSizeInBytes = (int64) ShardMaxSize * 1024L;
	shardFillLevel = ((float4) newShardSize / (float4) shardMaxSizeInBytes);

	PG_RETURN_FLOAT4(shardFillLevel);
}


/*
 * master_update_shard_statistics updates metadata (shard size and shard min/max
 * values) of the given shard and returns the updated shard size.
 */
Datum
master_update_shard_statistics(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	uint64 shardSize = 0;

	CheckCitusVersion(ERROR);

	shardSize = UpdateShardStatistics(shardId);

	PG_RETURN_INT64(shardSize);
}


/*
 * CheckDistributedTable checks if the given relationId corresponds to a
 * distributed table. If it does not, the function errors out.
 */
void
CheckDistributedTable(Oid relationId)
{
	char *relationName = get_rel_name(relationId);

	/* check that the relationId belongs to a table */
	char tableType = get_rel_relkind(relationId);
	if (!(tableType == RELKIND_RELATION || tableType == RELKIND_FOREIGN_TABLE))
	{
		ereport(ERROR, (errmsg("relation \"%s\" is not a table", relationName)));
	}

	if (!IsDistributedTable(relationId))
	{
		ereport(ERROR, (errmsg("relation \"%s\" is not a distributed table",
							   relationName)));
	}
}


/*
 * CreateShardPlacements attempts to create a certain number of placements
 * (provided by the replicationFactor argument) on the provided list of worker
 * nodes. Beginning at the provided start index, DDL commands are attempted on
 * worker nodes (via WorkerCreateShards). If there are more worker nodes than
 * required for replication, one remote failure is tolerated. If the provided
 * replication factor is not attained, an error is raised (placements remain on
 * nodes if some DDL commands had been successful).
 */
void
CreateShardPlacements(Oid relationId, int64 shardId, List *ddlEventList,
					  char *newPlacementOwner, List *workerNodeList,
					  int workerStartIndex, int replicationFactor)
{
	int attemptCount = replicationFactor;
	int workerNodeCount = list_length(workerNodeList);
	int placementsCreated = 0;
	int attemptNumber = 0;

	/* if we have enough nodes, add an extra placement attempt for backup */
	if (workerNodeCount > replicationFactor)
	{
		attemptCount++;
	}

	for (attemptNumber = 0; attemptNumber < attemptCount; attemptNumber++)
	{
		int workerNodeIndex = (workerStartIndex + attemptNumber) % workerNodeCount;
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, workerNodeIndex);
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		List *foreignConstraintCommandList = GetTableForeignConstraintCommands(
			relationId);
		int shardIndex = -1; /* not used in this code path */
		bool created = false;

		created = WorkerCreateShard(relationId, nodeName, nodePort, shardIndex,
									shardId, newPlacementOwner, ddlEventList,
									foreignConstraintCommandList);
		if (created)
		{
			const RelayFileState shardState = FILE_FINALIZED;
			const uint64 shardSize = 0;

			InsertShardPlacementRow(shardId, INVALID_PLACEMENT_ID, shardState, shardSize,
									workerNode->groupId);
			placementsCreated++;
		}
		else
		{
			ereport(WARNING, (errmsg("could not create shard on \"%s:%u\"",
									 nodeName, nodePort)));
		}

		if (placementsCreated >= replicationFactor)
		{
			break;
		}
	}

	/* check if we created enough shard replicas */
	if (placementsCreated < replicationFactor)
	{
		ereport(ERROR, (errmsg("could only create %u of %u of required shard replicas",
							   placementsCreated, replicationFactor)));
	}
}


/*
 * WorkerCreateShard applies DDL commands for the given shardId to create the
 * shard on the worker node. Note that this function opens a new connection for
 * each DDL command, and could leave the shard in an half-initialized state.
 */
bool
WorkerCreateShard(Oid relationId, char *nodeName, uint32 nodePort,
				  int shardIndex, uint64 shardId, char *newShardOwner,
				  List *ddlCommandList, List *foreignConstraintCommandList)
{
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	bool shardCreated = true;
	ListCell *ddlCommandCell = NULL;
	ListCell *foreignConstraintCommandCell = NULL;

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);
		char *escapedDDLCommand = quote_literal_cstr(ddlCommand);
		List *queryResultList = NIL;
		StringInfo applyDDLCommand = makeStringInfo();

		if (strcmp(schemaName, "public") != 0)
		{
			appendStringInfo(applyDDLCommand, WORKER_APPLY_SHARD_DDL_COMMAND, shardId,
							 escapedSchemaName, escapedDDLCommand);
		}
		else
		{
			appendStringInfo(applyDDLCommand,
							 WORKER_APPLY_SHARD_DDL_COMMAND_WITHOUT_SCHEMA, shardId,
							 escapedDDLCommand);
		}

		queryResultList = ExecuteRemoteQuery(nodeName, nodePort, newShardOwner,
											 applyDDLCommand);
		if (queryResultList == NIL)
		{
			shardCreated = false;
			break;
		}
	}

	foreach(foreignConstraintCommandCell, foreignConstraintCommandList)
	{
		char *command = (char *) lfirst(foreignConstraintCommandCell);
		char *escapedCommand = quote_literal_cstr(command);

		Oid referencedRelationId = InvalidOid;
		Oid referencedSchemaId = InvalidOid;
		char *referencedSchemaName = NULL;
		char *escapedReferencedSchemaName = NULL;
		uint64 referencedShardId = INVALID_SHARD_ID;

		List *queryResultList = NIL;
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

		/*
		 * In case of self referencing shards, relation itself might not be distributed
		 * already. Therefore we cannot use ColocatedShardIdInRelation which assumes
		 * given relation is distributed. Besides, since we know foreign key references
		 * itself, referencedShardId is actual shardId anyway.
		 */
		if (relationId == referencedRelationId)
		{
			referencedShardId = shardId;
		}
		else
		{
			referencedShardId = ColocatedShardIdInRelation(referencedRelationId,
														   shardIndex);
		}

		appendStringInfo(applyForeignConstraintCommand,
						 WORKER_APPLY_INTER_SHARD_DDL_COMMAND, shardId, escapedSchemaName,
						 referencedShardId, escapedReferencedSchemaName, escapedCommand);

		queryResultList = ExecuteRemoteQuery(nodeName, nodePort, newShardOwner,
											 applyForeignConstraintCommand);
		if (queryResultList == NIL)
		{
			shardCreated = false;
			break;
		}
	}

	return shardCreated;
}


/*
 * UpdateShardStatistics updates metadata (shard size and shard min/max values)
 * of the given shard and returns the updated shard size.
 */
uint64
UpdateShardStatistics(int64 shardId)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid relationId = shardInterval->relationId;
	char storageType = shardInterval->storageType;
	char partitionType = PartitionMethod(relationId);
	char *shardQualifiedName = NULL;
	List *shardPlacementList = NIL;
	ListCell *shardPlacementCell = NULL;
	bool statsOK = false;
	uint64 shardSize = 0;
	text *minValue = NULL;
	text *maxValue = NULL;

	/* Build shard qualified name. */
	char *shardName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);

	AppendShardIdToName(&shardName, shardId);

	shardQualifiedName = quote_qualified_identifier(schemaName, shardName);

	shardPlacementList = FinalizedShardPlacementList(shardId);

	/* get shard's statistics from a shard placement */
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);

		statsOK = WorkerShardStats(placement, relationId, shardQualifiedName,
								   &shardSize, &minValue, &maxValue);
		if (statsOK)
		{
			break;
		}
	}

	/*
	 * If for some reason we appended data to a shard, but failed to retrieve
	 * statistics we just WARN here to avoid losing shard-state updates. Note
	 * that this means we will return 0 as the shard fill-factor, and this shard
	 * also won't be pruned as the statistics will be empty. If the failure was
	 * transient, a subsequent append call will fetch the correct statistics.
	 */
	if (!statsOK)
	{
		ereport(WARNING, (errmsg("could not get statistics for shard %s",
								 shardQualifiedName),
						  errdetail("Setting shard statistics to NULL")));
	}

	/* make sure we don't process cancel signals */
	HOLD_INTERRUPTS();

	/* update metadata for each shard placement we appended to */
	shardPlacementCell = NULL;
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
		uint64 placementId = placement->placementId;
		uint32 groupId = placement->groupId;

		DeleteShardPlacementRow(placementId);
		InsertShardPlacementRow(shardId, placementId, FILE_FINALIZED, shardSize,
								groupId);
	}

	/* only update shard min/max values for append-partitioned tables */
	if (partitionType == DISTRIBUTE_BY_APPEND)
	{
		DeleteShardRow(shardId);
		InsertShardRow(relationId, shardId, storageType, minValue, maxValue);
	}

	if (QueryCancelPending)
	{
		ereport(WARNING, (errmsg("cancel requests are ignored during metadata update")));
		QueryCancelPending = false;
	}

	RESUME_INTERRUPTS();

	return shardSize;
}


/*
 * WorkerShardStats queries the worker node, and retrieves shard statistics that
 * we assume have changed after new table data have been appended to the shard.
 */
static bool
WorkerShardStats(ShardPlacement *placement, Oid relationId, char *shardName,
				 uint64 *shardSize, text **shardMinValue, text **shardMaxValue)
{
	char *quotedShardName = NULL;
	bool cstoreTable = false;
	StringInfo tableSizeQuery = makeStringInfo();

	const uint32 unusedTableId = 1;
	char partitionType = PartitionMethod(relationId);
	Var *partitionColumn = NULL;
	char *partitionColumnName = NULL;
	StringInfo partitionValueQuery = makeStringInfo();

	PGresult *queryResult = NULL;
	const int minValueIndex = 0;
	const int maxValueIndex = 1;

	uint64 tableSize = 0;
	char *tableSizeString = NULL;
	char *tableSizeStringEnd = NULL;
	bool minValueIsNull = false;
	bool maxValueIsNull = false;

	int connectionFlags = 0;
	int executeCommand = 0;

	MultiConnection *connection = GetPlacementConnection(connectionFlags, placement,
														 NULL);

	*shardSize = 0;
	*shardMinValue = NULL;
	*shardMaxValue = NULL;

	quotedShardName = quote_literal_cstr(shardName);

	cstoreTable = CStoreTable(relationId);
	if (cstoreTable)
	{
		appendStringInfo(tableSizeQuery, SHARD_CSTORE_TABLE_SIZE_QUERY, quotedShardName);
	}
	else
	{
		appendStringInfo(tableSizeQuery, SHARD_TABLE_SIZE_QUERY, quotedShardName);
	}

	executeCommand = ExecuteOptionalRemoteCommand(connection, tableSizeQuery->data,
												  &queryResult);
	if (executeCommand != 0)
	{
		return false;
	}

	tableSizeString = PQgetvalue(queryResult, 0, 0);
	if (tableSizeString == NULL)
	{
		PQclear(queryResult);
		ForgetResults(connection);
		return false;
	}

	errno = 0;
	tableSize = strtoull(tableSizeString, &tableSizeStringEnd, 0);
	if (errno != 0 || (*tableSizeStringEnd) != '\0')
	{
		PQclear(queryResult);
		ForgetResults(connection);
		return false;
	}

	*shardSize = tableSize;

	PQclear(queryResult);
	ForgetResults(connection);

	if (partitionType != DISTRIBUTE_BY_APPEND)
	{
		/* we don't need min/max for non-append distributed tables */
		return true;
	}

	/* fill in the partition column name and shard name in the query. */
	partitionColumn = PartitionColumn(relationId, unusedTableId);
	partitionColumnName = get_attname(relationId, partitionColumn->varattno);

	appendStringInfo(partitionValueQuery, SHARD_RANGE_QUERY,
					 partitionColumnName, partitionColumnName, shardName);

	executeCommand = ExecuteOptionalRemoteCommand(connection, partitionValueQuery->data,
												  &queryResult);
	if (executeCommand != 0)
	{
		return false;
	}

	minValueIsNull = PQgetisnull(queryResult, 0, minValueIndex);
	maxValueIsNull = PQgetisnull(queryResult, 0, maxValueIndex);

	if (!minValueIsNull && !maxValueIsNull)
	{
		char *minValueResult = PQgetvalue(queryResult, 0, minValueIndex);
		char *maxValueResult = PQgetvalue(queryResult, 0, maxValueIndex);

		*shardMinValue = cstring_to_text(minValueResult);
		*shardMaxValue = cstring_to_text(maxValueResult);
	}

	PQclear(queryResult);
	ForgetResults(connection);

	return true;
}


/*
 * ForeignConstraintGetReferencedTableId parses given foreign constraint query and
 * extracts referenced table id from it.
 */
Oid
ForeignConstraintGetReferencedTableId(char *queryString)
{
	Node *queryNode = ParseTreeNode(queryString);
	AlterTableStmt *foreignConstraintStmt = (AlterTableStmt *) queryNode;
	AlterTableCmd *command = (AlterTableCmd *) linitial(foreignConstraintStmt->cmds);

	if (command->subtype == AT_AddConstraint)
	{
		Constraint *constraint = (Constraint *) command->def;
		if (constraint->contype == CONSTR_FOREIGN)
		{
			RangeVar *referencedTable = constraint->pktable;

			return RangeVarGetRelid(referencedTable, NoLock,
									foreignConstraintStmt->missing_ok);
		}
	}

	return InvalidOid;
}
