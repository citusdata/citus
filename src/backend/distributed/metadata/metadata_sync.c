/*-------------------------------------------------------------------------
 *
 * metadata_sync.c
 *
 * Routines for synchronizing metadata to all workers.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_foreign_server.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/distribution_column.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_node.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


static char * LocalGroupIdUpdateCommand(uint32 groupId);
static void MarkNodeHasMetadata(char *nodeName, int32 nodePort);
static char * TruncateTriggerCreateCommand(Oid relationId);


PG_FUNCTION_INFO_V1(start_metadata_sync_to_node);


/*
 * start_metadata_sync_to_node function creates the metadata in a worker for preparing the
 * worker for accepting MX-table queries. The function first sets the localGroupId of the
 * worker so that the worker knows which tuple in pg_dist_node table represents itself.
 * After that, SQL statetemens for re-creating metadata about mx distributed
 * tables are sent to the worker. Finally, the hasmetadata column of the target node in
 * pg_dist_node is marked as true.
 */
Datum
start_metadata_sync_to_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	char *extensionOwner = CitusExtensionOwnerName();

	WorkerNode *workerNode = NULL;
	char *localGroupIdUpdateCommand = NULL;
	List *recreateMetadataSnapshotCommandList = NIL;
	List *dropMetadataCommandList = NIL;
	List *createMetadataCommandList = NIL;

	EnsureSuperUser();

	PreventTransactionChain(true, "start_metadata_sync_to_node");

	workerNode = FindWorkerNode(nodeNameString, nodePort);

	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("you cannot sync metadata to a non-existent node"),
						errhint("First, add the node with SELECT master_add_node(%s,%d)",
								nodeNameString, nodePort)));
	}

	/* generate and add the local group id's update query */
	localGroupIdUpdateCommand = LocalGroupIdUpdateCommand(workerNode->groupId);

	/* generate the queries which drop the metadata */
	dropMetadataCommandList = MetadataDropCommands();

	/* generate the queries which create the metadata from scratch */
	createMetadataCommandList = MetadataCreateCommands();

	recreateMetadataSnapshotCommandList = lappend(recreateMetadataSnapshotCommandList,
												  localGroupIdUpdateCommand);
	recreateMetadataSnapshotCommandList = list_concat(recreateMetadataSnapshotCommandList,
													  dropMetadataCommandList);
	recreateMetadataSnapshotCommandList = list_concat(recreateMetadataSnapshotCommandList,
													  createMetadataCommandList);

	/*
	 * Send the snapshot recreation commands in a single remote transaction and
	 * error out in any kind of failure. Note that it is not required to send
	 * createMetadataSnapshotCommandList in the same transaction that we send
	 * nodeDeleteCommand and nodeInsertCommand commands below.
	 */
	SendCommandListToWorkerInSingleTransaction(nodeNameString, nodePort, extensionOwner,
											   recreateMetadataSnapshotCommandList);

	MarkNodeHasMetadata(nodeNameString, nodePort);

	PG_RETURN_VOID();
}


/*
 * ShouldSyncTableMetadata checks if a distributed table has streaming replication model
 * and hash distribution. In that case the distributed table is considered an MX table,
 * and its metadata is required to exist on the worker nodes.
 */
bool
ShouldSyncTableMetadata(Oid relationId)
{
	DistTableCacheEntry *tableEntry = DistributedTableCacheEntry(relationId);
	bool usesHashDistribution = (tableEntry->partitionMethod == DISTRIBUTE_BY_HASH);
	bool usesStreamingReplication =
		(tableEntry->replicationModel == REPLICATION_MODEL_STREAMING);

	if (usesStreamingReplication && usesHashDistribution)
	{
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * MetadataCreateCommands returns list of queries that are
 * required to create the current metadata snapshot of the node that the
 * function is called. The metadata snapshot commands includes the
 * following queries:
 *
 * (i)   Query that populates pg_dist_node table
 * (ii)  Queries that create the clustered tables
 * (iii) Queries that populate pg_dist_partition table referenced by (ii)
 * (iv)  Queries that populate pg_dist_shard table referenced by (iii)
 * (v)   Queries that populate pg_dist_shard_placement table referenced by (iv)
 */
List *
MetadataCreateCommands(void)
{
	List *metadataSnapshotCommandList = NIL;
	List *distributedTableList = DistributedTableList();
	List *workerNodeList = WorkerNodeList();
	ListCell *distributedTableCell = NULL;
	char *nodeListInsertCommand = NULL;

	/* generate insert command for pg_dist_node table */
	nodeListInsertCommand = NodeListInsertCommand(workerNodeList);
	metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
										  nodeListInsertCommand);

	/* iterate over the distributed tables */
	foreach(distributedTableCell, distributedTableList)
	{
		DistTableCacheEntry *cacheEntry =
			(DistTableCacheEntry *) lfirst(distributedTableCell);
		List *clusteredTableDDLEvents = NIL;
		List *shardIntervalList = NIL;
		List *shardCreateCommandList = NIL;
		Oid clusteredTableId = cacheEntry->relationId;

		/* add only clustered tables */
		if (!ShouldSyncTableMetadata(clusteredTableId))
		{
			continue;
		}

		/* add the DDL events first */
		clusteredTableDDLEvents = GetDistributedTableDDLEvents(cacheEntry);
		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  clusteredTableDDLEvents);

		/* add the pg_dist_shard{,placement} entries */
		shardIntervalList = LoadShardIntervalList(clusteredTableId);
		shardCreateCommandList = ShardListInsertCommand(shardIntervalList);

		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  shardCreateCommandList);
	}

	return metadataSnapshotCommandList;
}


/*
 * MetadataDropCommands returns list of queries that are required to
 * drop all the metadata of the node that are related to clustered tables.
 * The drop metadata snapshot commands includes the following queries:
 *
 * (i)   Queries that delete all the rows from pg_dist_node table
 * (ii)  Queries that drop the clustered tables and remove its references from
 *       the pg_dist_partition. Note that distributed relation ids are gathered
 *       from the worker itself to prevent dropping any non-distributed tables
 *       with the same name.
 * (iii) Queries that delete all the rows from pg_dist_shard table referenced by (ii)
 * (iv) Queries that delete all the rows from pg_dist_shard_placement table
 *      referenced by (iii)
 */
List *
MetadataDropCommands(void)
{
	List *dropSnapshotCommandList = NIL;
	char *removeTablesCommand = NULL;
	char *removeNodesCommand = NULL;

	removeNodesCommand = DELETE_ALL_NODES;
	dropSnapshotCommandList = lappend(dropSnapshotCommandList,
									  removeNodesCommand);

	removeTablesCommand = REMOVE_ALL_CLUSTERED_TABLES_COMMAND;
	dropSnapshotCommandList = lappend(dropSnapshotCommandList,
									  removeTablesCommand);

	return dropSnapshotCommandList;
}


/*
 * NodeListInsertCommand generates a single multi-row INSERT command that can be
 * executed to insert the nodes that are in workerNodeList to pg_dist_node table.
 */
char *
NodeListInsertCommand(List *workerNodeList)
{
	ListCell *workerNodeCell = NULL;
	StringInfo nodeListInsertCommand = makeStringInfo();
	int workerCount = list_length(workerNodeList);
	int processedWorkerNodeCount = 0;

	/* if there are no workers, return NULL */
	if (workerCount == 0)
	{
		return nodeListInsertCommand->data;
	}

	/* generate the query without any values yet */
	appendStringInfo(nodeListInsertCommand,
					 "INSERT INTO pg_dist_node "
					 "(nodeid, groupid, nodename, nodeport, noderack, hasmetadata) "
					 "VALUES ");

	/* iterate over the worker nodes, add the values */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *hasMetadaString = workerNode->hasMetadata ? "TRUE" : "FALSE";

		appendStringInfo(nodeListInsertCommand,
						 "(%d, %d, %s, %d, '%s', %s)",
						 workerNode->nodeId,
						 workerNode->groupId,
						 quote_literal_cstr(workerNode->workerName),
						 workerNode->workerPort,
						 workerNode->workerRack,
						 hasMetadaString);

		processedWorkerNodeCount++;
		if (processedWorkerNodeCount != workerCount)
		{
			appendStringInfo(nodeListInsertCommand, ",");
		}
	}

	return nodeListInsertCommand->data;
}


/*
 * DistributionCreateCommands generates a commands that can be
 * executed to replicate the metadata for a distributed table.
 */
char *
DistributionCreateCommand(DistTableCacheEntry *cacheEntry)
{
	StringInfo insertDistributionCommand = makeStringInfo();
	Oid relationId = cacheEntry->relationId;
	char distributionMethod = cacheEntry->partitionMethod;
	char *partitionKeyString = cacheEntry->partitionKeyString;
	char *qualifiedRelationName =
		generate_qualified_relation_name(relationId);
	char *partitionKeyColumnName = ColumnNameToColumn(relationId, partitionKeyString);
	uint32 colocationId = cacheEntry->colocationId;
	char replicationModel = cacheEntry->replicationModel;

	appendStringInfo(insertDistributionCommand,
					 "INSERT INTO pg_dist_partition "
					 "(logicalrelid, partmethod, partkey, colocationid, repmodel) "
					 "VALUES "
					 "(%s::regclass, '%c', column_name_to_column(%s,%s), %d, '%c')",
					 quote_literal_cstr(qualifiedRelationName),
					 distributionMethod,
					 quote_literal_cstr(qualifiedRelationName),
					 quote_literal_cstr(partitionKeyColumnName),
					 colocationId,
					 replicationModel);

	return insertDistributionCommand->data;
}


/*
 * DistributionDeleteCommand generates a command that can be executed
 * to drop a distributed table and its metadata on a remote node.
 */
char *
DistributionDeleteCommand(char *schemaName, char *tableName)
{
	char *distributedRelationName = NULL;
	StringInfo deleteDistributionCommand = makeStringInfo();

	distributedRelationName = quote_qualified_identifier(schemaName, tableName);

	appendStringInfo(deleteDistributionCommand,
					 "SELECT worker_drop_distributed_table(%s::regclass)",
					 quote_literal_cstr(distributedRelationName));

	return deleteDistributionCommand->data;
}


/*
 * TableOwnerResetCommand generates a commands that can be executed
 * to reset the table owner.
 */
char *
TableOwnerResetCommand(Oid relationId)
{
	StringInfo ownerResetCommand = makeStringInfo();
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	char *tableOwnerName = TableOwner(relationId);

	appendStringInfo(ownerResetCommand,
					 "ALTER TABLE %s OWNER TO %s",
					 qualifiedRelationName,
					 quote_identifier(tableOwnerName));

	return ownerResetCommand->data;
}


/*
 * ShardListInsertCommand generates a singe command that can be
 * executed to replicate shard and shard placement metadata for the
 * given shard intervals. The function assumes that each shard has a
 * single placement, and asserts this information.
 */
List *
ShardListInsertCommand(List *shardIntervalList)
{
	List *commandList = NIL;
	ListCell *shardIntervalCell = NULL;
	StringInfo insertPlacementCommand = makeStringInfo();
	StringInfo insertShardCommand = makeStringInfo();
	int shardCount = list_length(shardIntervalList);
	int processedShardCount = 0;
	int processedShardPlacementCount = 0;

	/* if there are no shards, return empty list */
	if (shardCount == 0)
	{
		return commandList;
	}

	/* generate the shard placement query without any values yet */
	appendStringInfo(insertPlacementCommand,
					 "INSERT INTO pg_dist_shard_placement "
					 "(shardid, shardstate, shardlength,"
					 " nodename, nodeport, placementid) "
					 "VALUES ");

	/* add placements to insertPlacementCommand */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;

		List *shardPlacementList = FinalizedShardPlacementList(shardId);
		ShardPlacement *placement = NULL;

		/* the function only handles single placement per shard */
		Assert(list_length(shardPlacementList) == 1);

		placement = (ShardPlacement *) linitial(shardPlacementList);

		appendStringInfo(insertPlacementCommand,
						 "(%lu, 1, %lu, %s, %d, %lu)",
						 shardId,
						 placement->shardLength,
						 quote_literal_cstr(placement->nodeName),
						 placement->nodePort,
						 placement->placementId);

		processedShardPlacementCount++;
		if (processedShardPlacementCount != shardCount)
		{
			appendStringInfo(insertPlacementCommand, ",");
		}
	}

	/* add the command to the list that we'll return */
	commandList = lappend(commandList, insertPlacementCommand->data);

	/* now, generate the shard query without any values yet */
	appendStringInfo(insertShardCommand,
					 "INSERT INTO pg_dist_shard "
					 "(logicalrelid, shardid, shardstorage,"
					 " shardminvalue, shardmaxvalue) "
					 "VALUES ");

	/* now add shards to insertShardCommand */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		Oid distributedRelationId = shardInterval->relationId;
		char *qualifiedRelationName = generate_qualified_relation_name(
			distributedRelationId);

		int minHashToken = DatumGetInt32(shardInterval->minValue);
		int maxHashToken = DatumGetInt32(shardInterval->maxValue);

		appendStringInfo(insertShardCommand,
						 "(%s::regclass, %lu, '%c', '%d', '%d')",
						 quote_literal_cstr(qualifiedRelationName),
						 shardId,
						 shardInterval->storageType,
						 minHashToken,
						 maxHashToken);

		processedShardCount++;
		if (processedShardCount != shardCount)
		{
			appendStringInfo(insertShardCommand, ",");
		}
	}

	/* finally add the command to the list that we'll return */
	commandList = lappend(commandList, insertShardCommand->data);

	return commandList;
}


/*
 * NodeDeleteCommand generate a command that can be
 * executed to delete the metadata for a worker node.
 */
char *
NodeDeleteCommand(uint32 nodeId)
{
	StringInfo nodeDeleteCommand = makeStringInfo();

	appendStringInfo(nodeDeleteCommand,
					 "DELETE FROM pg_dist_node "
					 "WHERE nodeid = %u", nodeId);

	return nodeDeleteCommand->data;
}


/*
 * GetDistributedTableDDLEvents returns the full set of DDL commands necessary to
 * create this relation on a worker. This includes setting up any sequences,
 * setting the owner of the table, and inserting into metadata tables.
 */
List *
GetDistributedTableDDLEvents(DistTableCacheEntry *cacheEntry)
{
	char *ownerResetCommand = NULL;
	char *metadataCommand = NULL;
	char *truncateTriggerCreateCommand = NULL;
	Oid relationId = cacheEntry->relationId;

	List *commandList = GetTableDDLEvents(relationId);

	ownerResetCommand = TableOwnerResetCommand(relationId);
	commandList = lappend(commandList, ownerResetCommand);

	metadataCommand = DistributionCreateCommand(cacheEntry);
	commandList = lappend(commandList, metadataCommand);

	truncateTriggerCreateCommand = TruncateTriggerCreateCommand(relationId);
	commandList = lappend(commandList, truncateTriggerCreateCommand);

	return commandList;
}


/*
 * LocalGroupIdUpdateCommand creates the SQL command required to set the local group id
 * of a worker and returns the command in a string.
 */
static char *
LocalGroupIdUpdateCommand(uint32 groupId)
{
	StringInfo updateCommand = makeStringInfo();

	appendStringInfo(updateCommand, "UPDATE pg_dist_local_group SET groupid = %d",
					 groupId);

	return updateCommand->data;
}


/*
 * MarkNodeHasMetadata function sets the hasmetadata column of the specified worker in
 * pg_dist_node to true.
 */
static void
MarkNodeHasMetadata(char *nodeName, int32 nodePort)
{
	const bool indexOK = false;
	const int scanKeyCount = 2;

	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	ScanKeyData scanKey[scanKeyCount];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_node];
	bool isnull[Natts_pg_dist_node];
	bool replace[Natts_pg_dist_node];

	pgDistNode = heap_open(DistNodeRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistNode);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT8EQ, Int32GetDatum(nodePort));

	scanDescriptor = systable_beginscan(pgDistNode, InvalidOid, indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
							   nodeName, nodePort)));
	}

	memset(replace, 0, sizeof(replace));

	values[Anum_pg_dist_node_hasmetadata - 1] = BoolGetDatum(true);
	isnull[Anum_pg_dist_node_hasmetadata - 1] = false;
	replace[Anum_pg_dist_node_hasmetadata - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);
	simple_heap_update(pgDistNode, &heapTuple->t_self, heapTuple);

	CatalogUpdateIndexes(pgDistNode, heapTuple);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, NoLock);
}


/*
 * TruncateTriggerCreateCommand creates a SQL query calling worker_create_truncate_trigger
 * function, which creates the truncate trigger on the worker.
 */
static char *
TruncateTriggerCreateCommand(Oid relationId)
{
	StringInfo triggerCreateCommand = makeStringInfo();
	char *tableName = generate_qualified_relation_name(relationId);

	appendStringInfo(triggerCreateCommand,
					 "SELECT worker_create_truncate_trigger(%s)",
					 quote_literal_cstr(tableName));

	return triggerCreateCommand->data;
}
