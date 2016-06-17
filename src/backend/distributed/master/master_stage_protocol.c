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

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/resource_lock.h"
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
static bool WorkerCreateShard(char *nodeName, uint32 nodePort, uint64 shardId,
							  char *newShardOwner, List *ddlCommandList);
static bool WorkerShardStats(char *nodeName, uint32 nodePort, Oid relationId,
							 char *shardName, uint64 *shardSize,
							 text **shardMinValue, text **shardMaxValue);
static uint64 WorkerTableSize(char *nodeName, uint32 nodePort, Oid relationId,
							  char *tableName);
static StringInfo WorkerPartitionValue(char *nodeName, uint32 nodePort, Oid relationId,
									   char *shardName, char *selectQuery);


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
	List *workerNodeList = WorkerNodeList();
	Datum shardIdDatum = 0;
	int64 shardId = INVALID_SHARD_ID;
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

	/* generate new and unique shardId from sequence */
	shardIdDatum = master_get_new_shardid(NULL);
	shardId = DatumGetInt64(shardIdDatum);

	/* get table DDL commands to replay on the worker node */
	ddlEventList = GetTableDDLEvents(relationId);

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

	CreateShardPlacements(shardId, ddlEventList, relationOwner,
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
	List *succeededPlacementList = NIL;
	List *failedPlacementList = NIL;
	ListCell *shardPlacementCell = NULL;
	ListCell *failedPlacementCell = NULL;
	uint64 newShardSize = 0;
	uint64 shardMaxSizeInBytes = 0;
	float4 shardFillLevel = 0.0;
	char partitionMethod = 0;

	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid relationId = shardInterval->relationId;
	bool cstoreTable = CStoreTable(relationId);

	char storageType = shardInterval->storageType;

	EnsureTablePermissions(relationId, ACL_INSERT);

	if (storageType != SHARD_STORAGE_TABLE && !cstoreTable)
	{
		ereport(ERROR, (errmsg("cannot append to shardId " UINT64_FORMAT, shardId),
						errdetail("The underlying shard is not a regular table")));
	}

	partitionMethod = PartitionMethod(relationId);
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errmsg("cannot append to shardId " UINT64_FORMAT, shardId),
						errdetail("We currently don't support appending to shards "
								  "in hash-partitioned tables")));
	}

	/*
	 * We lock on the shardId, but do not unlock. When the function returns, and
	 * the transaction for this function commits, this lock will automatically
	 * be released. This ensures appends to a shard happen in a serial manner.
	 */
	LockShardResource(shardId, AccessExclusiveLock);

	/* get schame name of the target shard */
	shardSchemaOid = get_rel_namespace(relationId);
	shardSchemaName = get_namespace_name(shardSchemaOid);

	/* if shard doesn't have an alias, extend regular table name */
	shardTableName = LoadShardAlias(relationId, shardId);
	if (shardTableName == NULL)
	{
		shardTableName = get_rel_name(relationId);
		AppendShardIdToName(&shardTableName, shardId);
	}

	shardQualifiedName = quote_qualified_identifier(shardSchemaName, shardTableName);

	shardPlacementList = FinalizedShardPlacementList(shardId);
	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find any shard placements for shardId "
							   UINT64_FORMAT, shardId),
						errhint("Try running master_create_empty_shard() first")));
	}

	/* issue command to append table to each shard placement */
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		char *workerName = shardPlacement->nodeName;
		uint32 workerPort = shardPlacement->nodePort;
		List *queryResultList = NIL;

		StringInfo workerAppendQuery = makeStringInfo();
		appendStringInfo(workerAppendQuery, WORKER_APPEND_TABLE_TO_SHARD,
						 quote_literal_cstr(shardQualifiedName),
						 quote_literal_cstr(sourceTableName),
						 quote_literal_cstr(sourceNodeName), sourceNodePort);

		/* inserting data should be performed by the current user */
		queryResultList = ExecuteRemoteQuery(workerName, workerPort, NULL,
											 workerAppendQuery);
		if (queryResultList != NIL)
		{
			succeededPlacementList = lappend(succeededPlacementList, shardPlacement);
		}
		else
		{
			failedPlacementList = lappend(failedPlacementList, shardPlacement);
		}
	}

	/* before updating metadata, check that we appended to at least one shard */
	if (succeededPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not append table to any shard placement")));
	}

	/* make sure we don't process cancel signals */
	HOLD_INTERRUPTS();

	/* mark shard placements that we couldn't append to as inactive */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(failedPlacementCell);
		char *workerName = placement->nodeName;
		uint32 workerPort = placement->nodePort;
		uint64 oldShardLength = placement->shardLength;

		DeleteShardPlacementRow(shardId, workerName, workerPort);
		InsertShardPlacementRow(shardId, FILE_INACTIVE, oldShardLength,
								workerName, workerPort);

		ereport(WARNING, (errmsg("could not append table to shard \"%s\" on node "
								 "\"%s:%u\"", shardQualifiedName, workerName,
								 workerPort),
						  errdetail("Marking this shard placement as inactive")));
	}

	RESUME_INTERRUPTS();

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
CreateShardPlacements(int64 shardId, List *ddlEventList, char *newPlacementOwner,
					  List *workerNodeList, int workerStartIndex, int replicationFactor)
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

		bool created = WorkerCreateShard(nodeName, nodePort, shardId, newPlacementOwner,
										 ddlEventList);
		if (created)
		{
			const RelayFileState shardState = FILE_FINALIZED;
			const uint64 shardSize = 0;

			InsertShardPlacementRow(shardId, shardState, shardSize, nodeName, nodePort);
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
static bool
WorkerCreateShard(char *nodeName, uint32 nodePort, uint64 shardId,
				  char *newShardOwner, List *ddlCommandList)
{
	bool shardCreated = true;
	ListCell *ddlCommandCell = NULL;

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);
		char *escapedDDLCommand = quote_literal_cstr(ddlCommand);
		List *queryResultList = NIL;

		StringInfo applyDDLCommand = makeStringInfo();
		appendStringInfo(applyDDLCommand, WORKER_APPLY_SHARD_DDL_COMMAND,
						 shardId, escapedDDLCommand);

		queryResultList = ExecuteRemoteQuery(nodeName, nodePort, newShardOwner,
											 applyDDLCommand);
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

	/* if shard doesn't have an alias, extend regular table name */
	shardQualifiedName = LoadShardAlias(relationId, shardId);
	if (shardQualifiedName == NULL)
	{
		char *shardName = get_rel_name(relationId);

		Oid schemaId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(schemaId);

		AppendShardIdToName(&shardName, shardId);

		shardQualifiedName = quote_qualified_identifier(schemaName, shardName);
	}

	shardPlacementList = FinalizedShardPlacementList(shardId);

	/* get shard's statistics from a shard placement */
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
		char *workerName = placement->nodeName;
		uint32 workerPort = placement->nodePort;

		statsOK = WorkerShardStats(workerName, workerPort, relationId, shardQualifiedName,
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
		char *workerName = placement->nodeName;
		uint32 workerPort = placement->nodePort;

		DeleteShardPlacementRow(shardId, workerName, workerPort);
		InsertShardPlacementRow(shardId, FILE_FINALIZED, shardSize,
								workerName, workerPort);
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
WorkerShardStats(char *nodeName, uint32 nodePort, Oid relationId, char *shardName,
				 uint64 *shardSize, text **shardMinValue, text **shardMaxValue)
{
	bool shardStatsOK = true;

	PG_TRY();
	{
		uint64 tableSize = WorkerTableSize(nodeName, nodePort, relationId, shardName);
		StringInfo minValue = WorkerPartitionValue(nodeName, nodePort, relationId,
												   shardName, SHARD_MIN_VALUE_QUERY);
		StringInfo maxValue = WorkerPartitionValue(nodeName, nodePort, relationId,
												   shardName, SHARD_MAX_VALUE_QUERY);

		(*shardSize) = tableSize;
		(*shardMinValue) = cstring_to_text_with_len(minValue->data, minValue->len);
		(*shardMaxValue) = cstring_to_text_with_len(maxValue->data, maxValue->len);
	}
	PG_CATCH();
	{
		shardStatsOK = false;
	}
	PG_END_TRY();

	return shardStatsOK;
}


/*
 * WorkerTableSize queries the worker node to extract the disk space used by the
 * given relation. The function assumes the relation represents a regular table or
 * a cstore_fdw table.
 */
static uint64
WorkerTableSize(char *nodeName, uint32 nodePort, Oid relationId, char *tableName)
{
	uint64 tableSize = 0;
	List *queryResultList = NIL;
	StringInfo tableSizeString = NULL;
	char *tableSizeStringEnd = NULL;
	char *quotedTableName = quote_literal_cstr(tableName);
	bool cstoreTable = CStoreTable(relationId);
	StringInfo tableSizeQuery = makeStringInfo();


	if (cstoreTable)
	{
		appendStringInfo(tableSizeQuery, SHARD_CSTORE_TABLE_SIZE_QUERY, quotedTableName);
	}
	else
	{
		appendStringInfo(tableSizeQuery, SHARD_TABLE_SIZE_QUERY, quotedTableName);
	}

	queryResultList = ExecuteRemoteQuery(nodeName, nodePort, NULL, tableSizeQuery);
	if (queryResultList == NIL)
	{
		ereport(ERROR, (errmsg("could not receive table size from node "
							   "\"%s:%u\"", nodeName, nodePort)));
	}

	tableSizeString = (StringInfo) linitial(queryResultList);

	errno = 0;
	tableSize = strtoull(tableSizeString->data, &tableSizeStringEnd, 0);
	if (errno != 0 || (*tableSizeStringEnd) != '\0')
	{
		ereport(ERROR, (errmsg("could not extract table size for table \"%s\"",
							   quotedTableName)));
	}

	return tableSize;
}


/*
 * WorkerPartitionValue helps in extracting partition column's min or max value
 * from the given shard. For this, the function resolves the given distributed
 * relation's partition column, connects to the worker node, and runs a select
 * query on the given shard.
 */
static StringInfo
WorkerPartitionValue(char *nodeName, uint32 nodePort, Oid relationId,
					 char *shardName, char *selectQuery)
{
	StringInfo partitionValue = NULL;
	List *queryResultList = NIL;
	uint32 unusedTableId = 1;

	Var *partitionColumn = PartitionColumn(relationId, unusedTableId);
	char *partitionColumnName = get_attname(relationId, partitionColumn->varattno);

	StringInfo partitionValueQuery = makeStringInfo();
	appendStringInfo(partitionValueQuery, selectQuery, partitionColumnName, shardName);

	/*
	 * Note that the following call omits the partition column value's size, and
	 * simply casts the results to a (char *). If the user partitioned the table
	 * on a binary byte array, this approach fails and should be fixed.
	 */
	queryResultList = ExecuteRemoteQuery(nodeName, nodePort, NULL, partitionValueQuery);
	if (queryResultList == NIL)
	{
		ereport(ERROR, (errmsg("could not receive shard min/max values from node "
							   "\"%s:%u\"", nodeName, nodePort)));
	}

	partitionValue = (StringInfo) linitial(queryResultList);
	return partitionValue;
}
