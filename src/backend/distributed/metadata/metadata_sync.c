/*-------------------------------------------------------------------------
 *
 * metadata_sync.c
 *
 * Routines for synchronizing metadata to all workers.
 *
 * Copyright (c) Citus Data, Inc.
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
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_node.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "distributed/version_compat.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static char * LocalGroupIdUpdateCommand(int32 groupId);
static void UpdateDistNodeBoolAttr(char *nodeName, int32 nodePort, int attrNum,
								   bool value);
static List * SequenceDDLCommandsForTable(Oid relationId);
static char * TruncateTriggerCreateCommand(Oid relationId);
static char * SchemaOwnerName(Oid objectId);
static bool HasMetadataWorkers(void);
static List * DetachPartitionCommandList(void);
static bool SyncMetadataSnapshotToNode(WorkerNode *workerNode, bool raiseOnError);

PG_FUNCTION_INFO_V1(start_metadata_sync_to_node);
PG_FUNCTION_INFO_V1(stop_metadata_sync_to_node);


/*
 * start_metadata_sync_to_node function sets hasmetadata column of the given
 * node to true, and then synchronizes the metadata on the node.
 */
Datum
start_metadata_sync_to_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	char *nodeNameString = text_to_cstring(nodeName);

	StartMetadatSyncToNode(nodeNameString, nodePort);

	PG_RETURN_VOID();
}


/*
 * StartMetadatSyncToNode is the internal API for
 * start_metadata_sync_to_node().
 */
void
StartMetadatSyncToNode(char *nodeNameString, int32 nodePort)
{
	WorkerNode *workerNode = NULL;
	char *escapedNodeName = quote_literal_cstr(nodeNameString);

	/* fail if metadata synchronization doesn't succeed */
	bool raiseInterrupts = true;

	EnsureCoordinator();
	EnsureSuperUser();
	EnsureModificationsCanRun();
	CheckCitusVersion(ERROR);

	PreventInTransactionBlock(true, "start_metadata_sync_to_node");

	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	workerNode = FindWorkerNode(nodeNameString, nodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("you cannot sync metadata to a non-existent node"),
						errhint("First, add the node with SELECT master_add_node"
								"(%s,%d)", escapedNodeName, nodePort)));
	}

	if (!workerNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("you cannot sync metadata to an inactive node"),
						errhint("First, activate the node with "
								"SELECT master_activate_node(%s,%d)",
								escapedNodeName, nodePort)));
	}

	if (workerNode->groupId == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("you cannot sync metadata to the coordinator"),
						errhint("Coordinator already should have the metadata")));
	}

	MarkNodeHasMetadata(nodeNameString, nodePort, true);

	if (!WorkerNodeIsPrimary(workerNode))
	{
		/*
		 * If this is a secondary node we can't actually sync metadata to it; we assume
		 * the primary node is receiving metadata.
		 */
		return;
	}

	SyncMetadataSnapshotToNode(workerNode, raiseInterrupts);
	MarkNodeMetadataSynced(workerNode->workerName, workerNode->workerPort, true);
}


/*
 * stop_metadata_sync_to_node function sets the hasmetadata column of the specified node
 * to false in pg_dist_node table, thus indicating that the specified worker node does not
 * receive DDL changes anymore and cannot be used for issuing queries.
 */
Datum
stop_metadata_sync_to_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	WorkerNode *workerNode = NULL;

	EnsureCoordinator();
	EnsureSuperUser();
	CheckCitusVersion(ERROR);

	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	workerNode = FindWorkerNode(nodeNameString, nodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("node (%s,%d) does not exist", nodeNameString, nodePort)));
	}

	MarkNodeHasMetadata(nodeNameString, nodePort, false);
	MarkNodeMetadataSynced(nodeNameString, nodePort, false);

	PG_RETURN_VOID();
}


/*
 * ClusterHasKnownMetadataWorkers returns true if the node executing the function
 * knows at least one worker with metadata. We do it
 * (a) by checking the node that executes the function is a worker with metadata
 * (b) the coordinator knows at least one worker with metadata.
 */
bool
ClusterHasKnownMetadataWorkers()
{
	bool workerWithMetadata = false;

	if (GetLocalGroupId() != 0)
	{
		workerWithMetadata = true;
	}

	if (workerWithMetadata || HasMetadataWorkers())
	{
		return true;
	}

	return false;
}


/*
 * ShouldSyncTableMetadata checks if the metadata of a distributed table should be
 * propagated to metadata workers, i.e. the table is an MX table or reference table.
 * Tables with streaming replication model (which means RF=1) and hash distribution are
 * considered as MX tables while tables with none distribution are reference tables.
 */
bool
ShouldSyncTableMetadata(Oid relationId)
{
	DistTableCacheEntry *tableEntry = DistributedTableCacheEntry(relationId);

	bool hashDistributed = (tableEntry->partitionMethod == DISTRIBUTE_BY_HASH);
	bool streamingReplicated =
		(tableEntry->replicationModel == REPLICATION_MODEL_STREAMING);

	bool mxTable = (streamingReplicated && hashDistributed);
	bool referenceTable = (tableEntry->partitionMethod == DISTRIBUTE_BY_NONE);

	if (mxTable || referenceTable)
	{
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * SyncMetadataSnapshotToNode does the following:
 *  1. Sets the localGroupId on the worker so the worker knows which tuple in
 *     pg_dist_node represents itself.
 *  2. Recreates the distributed metadata on the given worker.
 * If raiseOnError is true, it errors out if synchronization fails.
 */
static bool
SyncMetadataSnapshotToNode(WorkerNode *workerNode, bool raiseOnError)
{
	char *extensionOwner = CitusExtensionOwnerName();

	/* generate and add the local group id's update query */
	char *localGroupIdUpdateCommand = LocalGroupIdUpdateCommand(workerNode->groupId);

	/* generate the queries which drop the metadata */
	List *dropMetadataCommandList = MetadataDropCommands();

	/* generate the queries which create the metadata from scratch */
	List *createMetadataCommandList = MetadataCreateCommands();

	List *recreateMetadataSnapshotCommandList = list_make1(localGroupIdUpdateCommand);
	recreateMetadataSnapshotCommandList = list_concat(recreateMetadataSnapshotCommandList,
													  dropMetadataCommandList);
	recreateMetadataSnapshotCommandList = list_concat(recreateMetadataSnapshotCommandList,
													  createMetadataCommandList);

	/*
	 * Send the snapshot recreation commands in a single remote transaction and
	 * if requested, error out in any kind of failure. Note that it is not
	 * required to send createMetadataSnapshotCommandList in the same transaction
	 * that we send nodeDeleteCommand and nodeInsertCommand commands below.
	 */
	if (raiseOnError)
	{
		SendCommandListToWorkerInSingleTransaction(workerNode->workerName,
												   workerNode->workerPort,
												   extensionOwner,
												   recreateMetadataSnapshotCommandList);
		return true;
	}
	else
	{
		bool success =
			SendOptionalCommandListToWorkerInTransaction(workerNode->workerName,
														 workerNode->workerPort,
														 extensionOwner,
														 recreateMetadataSnapshotCommandList);
		return success;
	}
}


/*
 * SendOptionalCommandListToWorkerInTransaction sends the given command list to
 * the given worker in a single transaction. If any of the commands fail, it
 * rollbacks the transaction, and otherwise commits.
 */
bool
SendOptionalCommandListToWorkerInTransaction(char *nodeName, int32 nodePort,
											 char *nodeUser, List *commandList)
{
	MultiConnection *workerConnection = NULL;
	ListCell *commandCell = NULL;
	int connectionFlags = FORCE_NEW_CONNECTION;
	bool failed = false;

	workerConnection = GetNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

	RemoteTransactionBegin(workerConnection);

	/* iterate over the commands and execute them in the same connection */
	foreach(commandCell, commandList)
	{
		char *commandString = lfirst(commandCell);

		if (ExecuteOptionalRemoteCommand(workerConnection, commandString, NULL) != 0)
		{
			failed = true;
			break;
		}
	}

	if (failed)
	{
		RemoteTransactionAbort(workerConnection);
	}
	else
	{
		RemoteTransactionCommit(workerConnection);
	}

	CloseConnection(workerConnection);

	return !failed;
}


/*
 * MetadataCreateCommands returns list of queries that are
 * required to create the current metadata snapshot of the node that the
 * function is called. The metadata snapshot commands includes the
 * following queries:
 *
 * (i)   Query that populates pg_dist_node table
 * (ii)  Queries that create the clustered tables (including foreign keys,
 *        partitioning hierarchy etc.)
 * (iii) Queries that populate pg_dist_partition table referenced by (ii)
 * (iv)  Queries that populate pg_dist_shard table referenced by (iii)
 * (v)   Queries that populate pg_dist_placement table referenced by (iv)
 */
List *
MetadataCreateCommands(void)
{
	List *metadataSnapshotCommandList = NIL;
	List *distributedTableList = DistributedTableList();
	List *propagatedTableList = NIL;
	bool includeNodesFromOtherClusters = true;
	List *workerNodeList = ReadDistNode(includeNodesFromOtherClusters);
	ListCell *distributedTableCell = NULL;
	char *nodeListInsertCommand = NULL;
	bool includeSequenceDefaults = true;

	/* make sure we have deterministic output for our tests */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* generate insert command for pg_dist_node table */
	nodeListInsertCommand = NodeListInsertCommand(workerNodeList);
	metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
										  nodeListInsertCommand);

	/* create the list of tables whose metadata will be created */
	foreach(distributedTableCell, distributedTableList)
	{
		DistTableCacheEntry *cacheEntry =
			(DistTableCacheEntry *) lfirst(distributedTableCell);
		if (ShouldSyncTableMetadata(cacheEntry->relationId))
		{
			propagatedTableList = lappend(propagatedTableList, cacheEntry);
		}
	}

	/* create the tables, but not the metadata */
	foreach(distributedTableCell, propagatedTableList)
	{
		DistTableCacheEntry *cacheEntry =
			(DistTableCacheEntry *) lfirst(distributedTableCell);
		Oid relationId = cacheEntry->relationId;
		ObjectAddress tableAddress = { 0 };

		List *workerSequenceDDLCommands = SequenceDDLCommandsForTable(relationId);
		List *ddlCommandList = GetTableDDLEvents(relationId, includeSequenceDefaults);
		char *tableOwnerResetCommand = TableOwnerResetCommand(relationId);

		/*
		 * Distributed tables might have dependencies on different objects, since we
		 * create shards for a distributed table via multiple sessions these objects will
		 * be created via their own connection and committed immediately so they become
		 * visible to all sessions creating shards.
		 */
		ObjectAddressSet(tableAddress, RelationRelationId, relationId);
		EnsureDependenciesExistsOnAllNodes(&tableAddress);

		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  workerSequenceDDLCommands);
		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  ddlCommandList);
		metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
											  tableOwnerResetCommand);
	}

	/* construct the foreign key constraints after all tables are created */
	foreach(distributedTableCell, propagatedTableList)
	{
		DistTableCacheEntry *cacheEntry =
			(DistTableCacheEntry *) lfirst(distributedTableCell);

		List *foreignConstraintCommands =
			GetTableForeignConstraintCommands(cacheEntry->relationId);

		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  foreignConstraintCommands);
	}

	/* construct partitioning hierarchy after all tables are created */
	foreach(distributedTableCell, propagatedTableList)
	{
		DistTableCacheEntry *cacheEntry =
			(DistTableCacheEntry *) lfirst(distributedTableCell);

		if (PartitionTable(cacheEntry->relationId))
		{
			char *alterTableAttachPartitionCommands =
				GenerateAlterTableAttachPartitionCommand(cacheEntry->relationId);

			metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
												  alterTableAttachPartitionCommands);
		}
	}

	/* after all tables are created, create the metadata */
	foreach(distributedTableCell, propagatedTableList)
	{
		DistTableCacheEntry *cacheEntry =
			(DistTableCacheEntry *) lfirst(distributedTableCell);
		List *shardIntervalList = NIL;
		List *shardCreateCommandList = NIL;
		char *metadataCommand = NULL;
		char *truncateTriggerCreateCommand = NULL;
		Oid clusteredTableId = cacheEntry->relationId;

		/* add the table metadata command first*/
		metadataCommand = DistributionCreateCommand(cacheEntry);
		metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
											  metadataCommand);

		/* add the truncate trigger command after the table became distributed */
		truncateTriggerCreateCommand =
			TruncateTriggerCreateCommand(cacheEntry->relationId);
		metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
											  truncateTriggerCreateCommand);

		/* add the pg_dist_shard{,placement} entries */
		shardIntervalList = LoadShardIntervalList(clusteredTableId);
		shardCreateCommandList = ShardListInsertCommand(shardIntervalList);

		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  shardCreateCommandList);
	}

	return metadataSnapshotCommandList;
}


/*
 * GetDistributedTableDDLEvents returns the full set of DDL commands necessary to
 * create the given distributed table on a worker. The list includes setting up any
 * sequences, setting the owner of the table, inserting table and shard metadata,
 * setting the truncate trigger and foreign key constraints.
 */
List *
GetDistributedTableDDLEvents(Oid relationId)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);

	List *shardIntervalList = NIL;
	List *commandList = NIL;
	List *foreignConstraintCommands = NIL;
	List *shardMetadataInsertCommandList = NIL;
	List *sequenceDDLCommands = NIL;
	List *tableDDLCommands = NIL;
	char *tableOwnerResetCommand = NULL;
	char *metadataCommand = NULL;
	char *truncateTriggerCreateCommand = NULL;
	bool includeSequenceDefaults = true;

	/* commands to create sequences */
	sequenceDDLCommands = SequenceDDLCommandsForTable(relationId);
	commandList = list_concat(commandList, sequenceDDLCommands);

	/* commands to create the table */
	tableDDLCommands = GetTableDDLEvents(relationId, includeSequenceDefaults);
	commandList = list_concat(commandList, tableDDLCommands);

	/* command to reset the table owner */
	tableOwnerResetCommand = TableOwnerResetCommand(relationId);
	commandList = lappend(commandList, tableOwnerResetCommand);

	/* command to insert pg_dist_partition entry */
	metadataCommand = DistributionCreateCommand(cacheEntry);
	commandList = lappend(commandList, metadataCommand);

	/* commands to create the truncate trigger of the table */
	truncateTriggerCreateCommand = TruncateTriggerCreateCommand(relationId);
	commandList = lappend(commandList, truncateTriggerCreateCommand);

	/* commands to insert pg_dist_shard & pg_dist_placement entries */
	shardIntervalList = LoadShardIntervalList(relationId);
	shardMetadataInsertCommandList = ShardListInsertCommand(shardIntervalList);
	commandList = list_concat(commandList, shardMetadataInsertCommandList);

	/* commands to create foreign key constraints */
	foreignConstraintCommands = GetTableForeignConstraintCommands(relationId);
	commandList = list_concat(commandList, foreignConstraintCommands);

	/* commands to create partitioning hierarchy */
	if (PartitionTable(relationId))
	{
		char *alterTableAttachPartitionCommands =
			GenerateAlterTableAttachPartitionCommand(relationId);
		commandList = lappend(commandList, alterTableAttachPartitionCommands);
	}

	return commandList;
}


/*
 * MetadataDropCommands returns list of queries that are required to
 * drop all the metadata of the node that are related to clustered tables.
 * The drop metadata snapshot commands includes the following queries:
 *
 * (i)   Query to disable DDL propagation (necessary for (ii)
 * (ii)  Queries that DETACH all partitions of distributed tables
 * (iii) Queries that delete all the rows from pg_dist_node table
 * (iv)  Queries that drop the clustered tables and remove its references from
 *        the pg_dist_partition. Note that distributed relation ids are gathered
 *        from the worker itself to prevent dropping any non-distributed tables
 *        with the same name.
 * (v)   Queries that delete all the rows from pg_dist_shard table referenced by (iv)
 * (vi)  Queries that delete all the rows from pg_dist_placement table
 *        referenced by (v)
 */
List *
MetadataDropCommands(void)
{
	List *dropSnapshotCommandList = NIL;
	List *detachPartitionCommandList = DetachPartitionCommandList();

	dropSnapshotCommandList = list_concat(dropSnapshotCommandList,
										  detachPartitionCommandList);

	dropSnapshotCommandList = lappend(dropSnapshotCommandList,
									  REMOVE_ALL_CLUSTERED_TABLES_COMMAND);

	dropSnapshotCommandList = lappend(dropSnapshotCommandList, DELETE_ALL_NODES);

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
	Oid primaryRole = PrimaryNodeRoleId();

	/* if there are no workers, return NULL */
	if (workerCount == 0)
	{
		return nodeListInsertCommand->data;
	}

	if (primaryRole == InvalidOid)
	{
		ereport(ERROR, (errmsg("bad metadata, noderole does not exist"),
						errdetail("you should never see this, please submit "
								  "a bug report"),
						errhint("run ALTER EXTENSION citus UPDATE and try again")));
	}

	/* generate the query without any values yet */
	appendStringInfo(nodeListInsertCommand,
					 "INSERT INTO pg_dist_node (nodeid, groupid, nodename, nodeport, "
					 "noderack, hasmetadata, metadatasynced, isactive, noderole, nodecluster) VALUES ");

	/* iterate over the worker nodes, add the values */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *hasMetadataString = workerNode->hasMetadata ? "TRUE" : "FALSE";
		char *metadataSyncedString = workerNode->metadataSynced ? "TRUE" : "FALSE";
		char *isActiveString = workerNode->isActive ? "TRUE" : "FALSE";

		Datum nodeRoleOidDatum = ObjectIdGetDatum(workerNode->nodeRole);
		Datum nodeRoleStringDatum = DirectFunctionCall1(enum_out, nodeRoleOidDatum);
		char *nodeRoleString = DatumGetCString(nodeRoleStringDatum);

		appendStringInfo(nodeListInsertCommand,
						 "(%d, %d, %s, %d, %s, %s, %s, %s, '%s'::noderole, %s)",
						 workerNode->nodeId,
						 workerNode->groupId,
						 quote_literal_cstr(workerNode->workerName),
						 workerNode->workerPort,
						 quote_literal_cstr(workerNode->workerRack),
						 hasMetadataString,
						 metadataSyncedString,
						 isActiveString,
						 nodeRoleString,
						 quote_literal_cstr(workerNode->nodeCluster));

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
	uint32 colocationId = cacheEntry->colocationId;
	char replicationModel = cacheEntry->replicationModel;
	StringInfo tablePartitionKeyString = makeStringInfo();

	if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		appendStringInfo(tablePartitionKeyString, "NULL");
	}
	else
	{
		char *partitionKeyColumnName = ColumnNameToColumn(relationId, partitionKeyString);
		appendStringInfo(tablePartitionKeyString, "column_name_to_column(%s,%s)",
						 quote_literal_cstr(qualifiedRelationName),
						 quote_literal_cstr(partitionKeyColumnName));
	}

	appendStringInfo(insertDistributionCommand,
					 "INSERT INTO pg_dist_partition "
					 "(logicalrelid, partmethod, partkey, colocationid, repmodel) "
					 "VALUES "
					 "(%s::regclass, '%c', %s, %d, '%c')",
					 quote_literal_cstr(qualifiedRelationName),
					 distributionMethod,
					 tablePartitionKeyString->data,
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
					 "SELECT worker_drop_distributed_table(%s)",
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
 * ShardListInsertCommand generates a single command that can be
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

	/* if there are no shards, return empty list */
	if (shardCount == 0)
	{
		return commandList;
	}

	/* add placements to insertPlacementCommand */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;

		List *shardPlacementList = FinalizedShardPlacementList(shardId);
		ListCell *shardPlacementCell = NULL;

		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);

			if (insertPlacementCommand->len == 0)
			{
				/* generate the shard placement query without any values yet */
				appendStringInfo(insertPlacementCommand,
								 "INSERT INTO pg_dist_placement "
								 "(shardid, shardstate, shardlength,"
								 " groupid, placementid) "
								 "VALUES ");
			}
			else
			{
				appendStringInfo(insertPlacementCommand, ",");
			}

			appendStringInfo(insertPlacementCommand,
							 "(" UINT64_FORMAT ", 1, " UINT64_FORMAT ", %d, "
							 UINT64_FORMAT ")",
							 shardId,
							 placement->shardLength,
							 placement->groupId,
							 placement->placementId);
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
		StringInfo minHashToken = makeStringInfo();
		StringInfo maxHashToken = makeStringInfo();

		if (shardInterval->minValueExists)
		{
			appendStringInfo(minHashToken, "'%d'", DatumGetInt32(
								 shardInterval->minValue));
		}
		else
		{
			appendStringInfo(minHashToken, "NULL");
		}

		if (shardInterval->maxValueExists)
		{
			appendStringInfo(maxHashToken, "'%d'", DatumGetInt32(
								 shardInterval->maxValue));
		}
		else
		{
			appendStringInfo(maxHashToken, "NULL");
		}

		appendStringInfo(insertShardCommand,
						 "(%s::regclass, " UINT64_FORMAT ", '%c', %s, %s)",
						 quote_literal_cstr(qualifiedRelationName),
						 shardId,
						 shardInterval->storageType,
						 minHashToken->data,
						 maxHashToken->data);

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
 * ShardListDeleteCommand generates a command list that can be executed to delete
 * shard and shard placement metadata for the given shard.
 */
List *
ShardDeleteCommandList(ShardInterval *shardInterval)
{
	uint64 shardId = shardInterval->shardId;
	List *commandList = NIL;
	StringInfo deletePlacementCommand = NULL;
	StringInfo deleteShardCommand = NULL;

	/* create command to delete shard placements */
	deletePlacementCommand = makeStringInfo();
	appendStringInfo(deletePlacementCommand,
					 "DELETE FROM pg_dist_placement WHERE shardid = " UINT64_FORMAT,
					 shardId);

	commandList = lappend(commandList, deletePlacementCommand->data);

	/* create command to delete shard */
	deleteShardCommand = makeStringInfo();
	appendStringInfo(deleteShardCommand,
					 "DELETE FROM pg_dist_shard WHERE shardid = " UINT64_FORMAT, shardId);

	commandList = lappend(commandList, deleteShardCommand->data);

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
 * NodeStateUpdateCommand generates a command that can be executed to update
 * isactive column of a node in pg_dist_node table.
 */
char *
NodeStateUpdateCommand(uint32 nodeId, bool isActive)
{
	StringInfo nodeStateUpdateCommand = makeStringInfo();
	char *isActiveString = isActive ? "TRUE" : "FALSE";

	appendStringInfo(nodeStateUpdateCommand,
					 "UPDATE pg_dist_node SET isactive = %s "
					 "WHERE nodeid = %u", isActiveString, nodeId);

	return nodeStateUpdateCommand->data;
}


/*
 * ShouldHaveShardsUpdateCommand generates a command that can be executed to
 * update the shouldhaveshards column of a node in pg_dist_node table.
 */
char *
ShouldHaveShardsUpdateCommand(uint32 nodeId, bool shouldHaveShards)
{
	StringInfo nodeStateUpdateCommand = makeStringInfo();
	char *shouldHaveShardsString = shouldHaveShards ? "TRUE" : "FALSE";

	appendStringInfo(nodeStateUpdateCommand,
					 "UPDATE pg_catalog.pg_dist_node SET shouldhaveshards = %s "
					 "WHERE nodeid = %u", shouldHaveShardsString, nodeId);

	return nodeStateUpdateCommand->data;
}


/*
 * ColocationIdUpdateCommand creates the SQL command to change the colocationId
 * of the table with the given name to the given colocationId in pg_dist_partition
 * table.
 */
char *
ColocationIdUpdateCommand(Oid relationId, uint32 colocationId)
{
	StringInfo command = makeStringInfo();
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	appendStringInfo(command, "UPDATE pg_dist_partition "
							  "SET colocationid = %d "
							  "WHERE logicalrelid = %s::regclass",
					 colocationId, quote_literal_cstr(qualifiedRelationName));

	return command->data;
}


/*
 * PlacementUpsertCommand creates a SQL command for upserting a pg_dist_placment
 * entry with the given properties. In the case of a conflict on placementId, the command
 * updates all properties (excluding the placementId) with the given ones.
 */
char *
PlacementUpsertCommand(uint64 shardId, uint64 placementId, int shardState,
					   uint64 shardLength, int32 groupId)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, UPSERT_PLACEMENT, shardId, shardState, shardLength,
					 groupId, placementId);

	return command->data;
}


/*
 * LocalGroupIdUpdateCommand creates the SQL command required to set the local group id
 * of a worker and returns the command in a string.
 */
static char *
LocalGroupIdUpdateCommand(int32 groupId)
{
	StringInfo updateCommand = makeStringInfo();

	appendStringInfo(updateCommand, "UPDATE pg_dist_local_group SET groupid = %d",
					 groupId);

	return updateCommand->data;
}


/*
 * MarkNodeHasMetadata function sets the hasmetadata column of the specified worker in
 * pg_dist_node to hasMetadata.
 */
void
MarkNodeHasMetadata(char *nodeName, int32 nodePort, bool hasMetadata)
{
	UpdateDistNodeBoolAttr(nodeName, nodePort,
						   Anum_pg_dist_node_hasmetadata,
						   hasMetadata);
}


/*
 * MarkNodeMetadataSynced function sets the metadatasynced column of the
 * specified worker in pg_dist_node to the given value.
 */
void
MarkNodeMetadataSynced(char *nodeName, int32 nodePort, bool synced)
{
	UpdateDistNodeBoolAttr(nodeName, nodePort,
						   Anum_pg_dist_node_metadatasynced,
						   synced);
}


/*
 * UpdateDistNodeBoolAttr updates a boolean attribute of the specified worker
 * to the given value.
 */
static void
UpdateDistNodeBoolAttr(char *nodeName, int32 nodePort, int attrNum, bool value)
{
	const bool indexOK = false;

	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	ScanKeyData scanKey[2];
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
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodePort));

	scanDescriptor = systable_beginscan(pgDistNode, InvalidOid, indexOK,
										NULL, 2, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
							   nodeName, nodePort)));
	}

	memset(replace, 0, sizeof(replace));

	values[attrNum - 1] = BoolGetDatum(value);
	isnull[attrNum - 1] = false;
	replace[attrNum - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistNode, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, NoLock);
}


/*
 * SequenceDDLCommandsForTable returns a list of commands which create sequences (and
 * their schemas) to run on workers before creating the relation. The sequence creation
 * commands are wrapped with a `worker_apply_sequence_command` call, which sets the
 * sequence space uniquely for each worker. Notice that this function is relevant only
 * during metadata propagation to workers and adds nothing to the list of sequence
 * commands if none of the workers is marked as receiving metadata changes.
 */
List *
SequenceDDLCommandsForTable(Oid relationId)
{
	List *sequenceDDLList = NIL;
	List *ownedSequences = getOwnedSequences(relationId, InvalidAttrNumber);
	ListCell *listCell;
	char *ownerName = TableOwner(relationId);

	foreach(listCell, ownedSequences)
	{
		Oid sequenceOid = (Oid) lfirst_oid(listCell);
		char *sequenceDef = pg_get_sequencedef_string(sequenceOid);
		char *escapedSequenceDef = quote_literal_cstr(sequenceDef);
		StringInfo wrappedSequenceDef = makeStringInfo();
		StringInfo sequenceGrantStmt = makeStringInfo();
		char *sequenceName = generate_qualified_relation_name(sequenceOid);
		Form_pg_sequence sequenceData = pg_get_sequencedef(sequenceOid);
		Oid sequenceTypeOid = sequenceData->seqtypid;
		char *typeName = format_type_be(sequenceTypeOid);

		/* create schema if needed */
		appendStringInfo(wrappedSequenceDef,
						 WORKER_APPLY_SEQUENCE_COMMAND,
						 escapedSequenceDef,
						 quote_literal_cstr(typeName));

		appendStringInfo(sequenceGrantStmt,
						 "ALTER SEQUENCE %s OWNER TO %s", sequenceName,
						 quote_identifier(ownerName));

		sequenceDDLList = lappend(sequenceDDLList, wrappedSequenceDef->data);
		sequenceDDLList = lappend(sequenceDDLList, sequenceGrantStmt->data);
	}

	return sequenceDDLList;
}


/*
 * CreateSchemaDDLCommand returns a "CREATE SCHEMA..." SQL string for creating the given
 * schema if not exists and with proper authorization.
 */
char *
CreateSchemaDDLCommand(Oid schemaId)
{
	char *schemaName = get_namespace_name(schemaId);
	StringInfo schemaNameDef = NULL;
	const char *ownerName = NULL;
	const char *quotedSchemaName = NULL;

	if (strncmp(schemaName, "public", NAMEDATALEN) == 0)
	{
		return NULL;
	}

	schemaNameDef = makeStringInfo();
	quotedSchemaName = quote_identifier(schemaName);
	ownerName = quote_identifier(SchemaOwnerName(schemaId));
	appendStringInfo(schemaNameDef, CREATE_SCHEMA_COMMAND, quotedSchemaName, ownerName);

	return schemaNameDef->data;
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


/*
 * SchemaOwnerName returns the name of the owner of the specified schema.
 */
static char *
SchemaOwnerName(Oid objectId)
{
	HeapTuple tuple = NULL;
	Oid ownerId = InvalidOid;
	char *ownerName = NULL;

	tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(objectId));
	if (HeapTupleIsValid(tuple))
	{
		ownerId = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;
	}
	else
	{
		ownerId = GetUserId();
	}

	ownerName = GetUserNameFromId(ownerId, false);

	ReleaseSysCache(tuple);

	return ownerName;
}


/*
 * HasMetadataWorkers returns true if any of the workers in the cluster has its
 * hasmetadata column set to true, which happens when start_metadata_sync_to_node
 * command is run.
 */
static bool
HasMetadataWorkers(void)
{
	List *workerNodeList = ActivePrimaryNodeList(NoLock);
	ListCell *workerNodeCell = NULL;

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		if (workerNode->hasMetadata)
		{
			return true;
		}
	}

	return false;
}


/*
 * CreateTableMetadataOnWorkers creates the list of commands needed to create the
 * given distributed table and sends these commands to all metadata workers i.e. workers
 * with hasmetadata=true. Before sending the commands, in order to prevent recursive
 * propagation, DDL propagation on workers are disabled with a
 * `SET citus.enable_ddl_propagation TO off;` command.
 */
void
CreateTableMetadataOnWorkers(Oid relationId)
{
	List *commandList = GetDistributedTableDDLEvents(relationId);
	ListCell *commandCell = NULL;

	/* prevent recursive propagation */
	SendCommandToWorkers(WORKERS_WITH_METADATA, DISABLE_DDL_PROPAGATION);

	/* send the commands one by one */
	foreach(commandCell, commandList)
	{
		char *command = (char *) lfirst(commandCell);

		SendCommandToWorkers(WORKERS_WITH_METADATA, command);
	}
}


/*
 * DetachPartitionCommandList returns list of DETACH commands to detach partitions
 * of all distributed tables. This function is used for detaching partitions in MX
 * workers before DROPping distributed partitioned tables in them. Thus, we are
 * disabling DDL propagation to the beginning of the commands (we are also enabling
 * DDL propagation at the end of command list to swtich back to original state). As
 * an extra step, if there are no partitions to DETACH, this function simply returns
 * empty list to not disable/enable DDL propagation for nothing.
 */
static List *
DetachPartitionCommandList(void)
{
	List *detachPartitionCommandList = NIL;
	List *distributedTableList = DistributedTableList();
	ListCell *distributedTableCell = NULL;

	/* we iterate over all distributed partitioned tables and DETACH their partitions */
	foreach(distributedTableCell, distributedTableList)
	{
		DistTableCacheEntry *cacheEntry =
			(DistTableCacheEntry *) lfirst(distributedTableCell);
		List *partitionList = NIL;
		ListCell *partitionCell = NULL;

		if (!PartitionedTable(cacheEntry->relationId))
		{
			continue;
		}

		partitionList = PartitionList(cacheEntry->relationId);
		foreach(partitionCell, partitionList)
		{
			Oid partitionRelationId = lfirst_oid(partitionCell);
			char *detachPartitionCommand =
				GenerateDetachPartitionCommand(partitionRelationId);

			detachPartitionCommandList = lappend(detachPartitionCommandList,
												 detachPartitionCommand);
		}
	}

	if (list_length(detachPartitionCommandList) == 0)
	{
		return NIL;
	}

	detachPartitionCommandList =
		lcons(DISABLE_DDL_PROPAGATION, detachPartitionCommandList);

	/*
	 * We probably do not need this but as an extra precaution, we are enabling
	 * DDL propagation to switch back to original state.
	 */
	detachPartitionCommandList = lappend(detachPartitionCommandList,
										 ENABLE_DDL_PROPAGATION);

	return detachPartitionCommandList;
}


/*
 * SyncMetadataToNodes tries recreating the metadata snapshot in the
 * metadata workers that are out of sync. Returns the result of
 * synchronization.
 */
MetadataSyncResult
SyncMetadataToNodes(void)
{
	List *workerList = NIL;
	ListCell *workerCell = NULL;
	MetadataSyncResult result = METADATA_SYNC_SUCCESS;

	if (!IsCoordinator())
	{
		return METADATA_SYNC_SUCCESS;
	}

	/*
	 * Request a RowExclusiveLock so we don't run concurrently with other
	 * functions updating pg_dist_node, but allow concurrency with functions
	 * which are just reading from pg_dist_node.
	 */
	if (!ConditionalLockRelationOid(DistNodeRelationId(), RowExclusiveLock))
	{
		return METADATA_SYNC_FAILED_LOCK;
	}

	workerList = ActivePrimaryNodeList(NoLock);

	foreach(workerCell, workerList)
	{
		WorkerNode *workerNode = lfirst(workerCell);

		if (workerNode->hasMetadata && !workerNode->metadataSynced)
		{
			bool raiseInterrupts = false;

			if (!SyncMetadataSnapshotToNode(workerNode, raiseInterrupts))
			{
				result = METADATA_SYNC_FAILED_SYNC;
			}
			else
			{
				MarkNodeMetadataSynced(workerNode->workerName,
									   workerNode->workerPort, true);
			}
		}
	}

	return result;
}
