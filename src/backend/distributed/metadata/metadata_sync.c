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

#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/async.h"
#include "distributed/argutils.h"
#include "distributed/backend_data.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "distributed/version_compat.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


/* managed via a GUC */
char *EnableManualMetadataChangesForUser = "";


static void EnsureSequentialModeMetadataOperations(void);
static List * GetDistributedTableDDLEvents(Oid relationId);
static char * LocalGroupIdUpdateCommand(int32 groupId);
static List * SequenceDependencyCommandList(Oid relationId);
static char * TruncateTriggerCreateCommand(Oid relationId);
static char * SchemaOwnerName(Oid objectId);
static bool HasMetadataWorkers(void);
static List * DetachPartitionCommandList(void);
static bool ShouldSyncTableMetadataInternal(bool hashDistributed,
											bool citusTableWithNoDistKey);
static bool SyncMetadataSnapshotToNode(WorkerNode *workerNode, bool raiseOnError);
static void DropMetadataSnapshotOnNode(WorkerNode *workerNode);
static char * CreateSequenceDependencyCommand(Oid relationId, Oid sequenceId,
											  char *columnName);
static List * GenerateGrantOnSchemaQueriesFromAclItem(Oid schemaOid,
													  AclItem *aclItem);
static GrantStmt * GenerateGrantOnSchemaStmtForRights(Oid roleOid,
													  Oid schemaOid,
													  char *permission,
													  bool withGrantOption);
static char * GenerateSetRoleQuery(Oid roleOid);
static void MetadataSyncSigTermHandler(SIGNAL_ARGS);
static void MetadataSyncSigAlrmHandler(SIGNAL_ARGS);


static bool ShouldSkipMetadataChecks(void);
static void EnsurePartitionMetadataIsSane(Oid relationId, char distributionMethod,
										  int colocationId, char replicationModel,
										  Var *distributionKey);
static void EnsureCoordinatorInitiatedOperation(void);
static void EnsureShardMetadataIsSane(Oid relationId, int64 shardId, char storageType,
									  text *shardMinValue,
									  text *shardMaxValue);
static void EnsureShardPlacementMetadataIsSane(Oid relationId, int64 shardId,
											   int64 placementId, int32 shardState,
											   int64 shardLength, int32 groupId);

PG_FUNCTION_INFO_V1(start_metadata_sync_to_node);
PG_FUNCTION_INFO_V1(stop_metadata_sync_to_node);
PG_FUNCTION_INFO_V1(worker_record_sequence_dependency);


/*
 * Functions to modify metadata. Normally modifying metadata requires
 * superuser. However, these functions can be called with superusers
 * or regular users as long as the regular user owns the input object.
 */
PG_FUNCTION_INFO_V1(citus_internal_add_partition_metadata);
PG_FUNCTION_INFO_V1(citus_internal_add_shard_metadata);
PG_FUNCTION_INFO_V1(citus_internal_add_placement_metadata);
PG_FUNCTION_INFO_V1(citus_internal_update_placement_metadata);
PG_FUNCTION_INFO_V1(citus_internal_delete_shard_metadata);
PG_FUNCTION_INFO_V1(citus_internal_update_relation_colocation);


static bool got_SIGTERM = false;
static bool got_SIGALRM = false;

#define METADATA_SYNC_APP_NAME "Citus Metadata Sync Daemon"


/*
 * start_metadata_sync_to_node function sets hasmetadata column of the given
 * node to true, and then synchronizes the metadata on the node.
 */
Datum
start_metadata_sync_to_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	char *nodeNameString = text_to_cstring(nodeName);

	StartMetadataSyncToNode(nodeNameString, nodePort);

	PG_RETURN_VOID();
}


/*
 * StartMetadataSyncToNode is the internal API for
 * start_metadata_sync_to_node().
 */
void
StartMetadataSyncToNode(const char *nodeNameString, int32 nodePort)
{
	char *escapedNodeName = quote_literal_cstr(nodeNameString);

	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	EnsureModificationsCanRun();

	EnsureSequentialModeMetadataOperations();

	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	WorkerNode *workerNode = FindWorkerNode(nodeNameString, nodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("you cannot sync metadata to a non-existent node"),
						errhint("First, add the node with SELECT citus_add_node"
								"(%s,%d)", escapedNodeName, nodePort)));
	}

	if (!workerNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("you cannot sync metadata to an inactive node"),
						errhint("First, activate the node with "
								"SELECT citus_activate_node(%s,%d)",
								escapedNodeName, nodePort)));
	}

	if (NodeIsCoordinator(workerNode))
	{
		ereport(NOTICE, (errmsg("%s:%d is the coordinator and already contains "
								"metadata, skipping syncing the metadata",
								nodeNameString, nodePort)));
		return;
	}

	UseCoordinatedTransaction();

	/*
	 * One would normally expect to set hasmetadata first, and then metadata sync.
	 * However, at this point we do the order reverse.
	 * We first set metadatasynced, and then hasmetadata; since setting columns for
	 * nodes with metadatasynced==false could cause errors.
	 * (See ErrorIfAnyMetadataNodeOutOfSync)
	 * We can safely do that because we are in a coordinated transaction and the changes
	 * are only visible to our own transaction.
	 * If anything goes wrong, we are going to rollback all the changes.
	 */
	workerNode = SetWorkerColumn(workerNode, Anum_pg_dist_node_metadatasynced,
								 BoolGetDatum(true));
	workerNode = SetWorkerColumn(workerNode, Anum_pg_dist_node_hasmetadata, BoolGetDatum(
									 true));

	if (!NodeIsPrimary(workerNode))
	{
		/*
		 * If this is a secondary node we can't actually sync metadata to it; we assume
		 * the primary node is receiving metadata.
		 */
		return;
	}

	/* fail if metadata synchronization doesn't succeed */
	bool raiseInterrupts = true;
	SyncMetadataSnapshotToNode(workerNode, raiseInterrupts);
}


/*
 * EnsureSequentialModeMetadataOperations makes sure that the current transaction is
 * already in sequential mode, or can still safely be put in sequential mode,
 * it errors if that is not possible. The error contains information for the user to
 * retry the transaction with sequential mode set from the beginning.
 *
 * Metadata objects (e.g., distributed table on the workers) exists only 1 instance of
 * the type used by potentially multiple other shards/connections. To make sure all
 * shards/connections in the transaction can interact with the metadata needs to be
 * visible on all connections used by the transaction, meaning we can only use 1
 * connection per node.
 */
static void
EnsureSequentialModeMetadataOperations(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg(
							"cannot execute metadata syncing operation because there was a "
							"parallel operation on a distributed table in the "
							"transaction"),
						errdetail("When modifying metadata, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Metadata synced or stopped syncing. To make "
							   "sure subsequent commands see the metadata correctly "
							   "we need to make sure to use only one connection for "
							   "all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * stop_metadata_sync_to_node function sets the hasmetadata column of the specified node
 * to false in pg_dist_node table, thus indicating that the specified worker node does not
 * receive DDL changes anymore and cannot be used for issuing queries.
 */
Datum
stop_metadata_sync_to_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	bool clearMetadata = PG_GETARG_BOOL(2);
	char *nodeNameString = text_to_cstring(nodeName);

	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	WorkerNode *workerNode = FindWorkerNodeAnyCluster(nodeNameString, nodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("node (%s,%d) does not exist", nodeNameString, nodePort)));
	}

	if (NodeIsCoordinator(workerNode))
	{
		ereport(NOTICE, (errmsg("node (%s,%d) is the coordinator and should have "
								"metadata, skipping stopping the metadata sync",
								nodeNameString, nodePort)));
		PG_RETURN_VOID();
	}

	if (clearMetadata)
	{
		if (NodeIsPrimary(workerNode))
		{
			ereport(NOTICE, (errmsg("dropping metadata on the node (%s,%d)",
									nodeNameString, nodePort)));
			DropMetadataSnapshotOnNode(workerNode);
		}
		else
		{
			/*
			 * If this is a secondary node we can't actually clear metadata from it,
			 * we assume the primary node is cleared.
			 */
			ereport(NOTICE, (errmsg("(%s,%d) is a secondary node: to clear the metadata,"
									" you should clear metadata from the primary node",
									nodeNameString, nodePort)));
		}
	}

	workerNode = SetWorkerColumn(workerNode, Anum_pg_dist_node_hasmetadata, BoolGetDatum(
									 false));
	workerNode = SetWorkerColumn(workerNode, Anum_pg_dist_node_metadatasynced,
								 BoolGetDatum(false));

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

	if (!IsCoordinator())
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
 * propagated to metadata workers, i.e. the table is a hash distributed table or
 * reference/citus local table.
 */
bool
ShouldSyncTableMetadata(Oid relationId)
{
	if (!OidIsValid(relationId) || !IsCitusTable(relationId))
	{
		return false;
	}

	CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(relationId);

	bool hashDistributed = IsCitusTableTypeCacheEntry(tableEntry, HASH_DISTRIBUTED);
	bool citusTableWithNoDistKey =
		IsCitusTableTypeCacheEntry(tableEntry, CITUS_TABLE_WITH_NO_DIST_KEY);

	return ShouldSyncTableMetadataInternal(hashDistributed, citusTableWithNoDistKey);
}


/*
 * ShouldSyncTableMetadataViaCatalog checks if the metadata of a distributed table should
 * be propagated to metadata workers, i.e. the table is an MX table or reference table.
 * Tables with streaming replication model (which means RF=1) and hash distribution are
 * considered as MX tables while tables with none distribution are reference tables.
 *
 * ShouldSyncTableMetadataViaCatalog does not use the CitusTableCache and instead reads
 * from catalog tables directly.
 */
bool
ShouldSyncTableMetadataViaCatalog(Oid relationId)
{
	if (!OidIsValid(relationId) || !IsCitusTableViaCatalog(relationId))
	{
		return false;
	}

	char partitionMethod = PartitionMethodViaCatalog(relationId);
	bool hashDistributed = partitionMethod == DISTRIBUTE_BY_HASH;
	bool citusTableWithNoDistKey = partitionMethod == DISTRIBUTE_BY_NONE;

	return ShouldSyncTableMetadataInternal(hashDistributed, citusTableWithNoDistKey);
}


/*
 * ShouldSyncTableMetadataInternal decides whether we should sync the metadata for a table
 * based on whether it is a hash distributed table, or a citus table with no distribution
 * key.
 *
 * This function is here to make sure that ShouldSyncTableMetadata and
 * ShouldSyncTableMetadataViaCatalog behaves the same way.
 */
static bool
ShouldSyncTableMetadataInternal(bool hashDistributed, bool citusTableWithNoDistKey)
{
	return hashDistributed || citusTableWithNoDistKey;
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
	char *currentUser = CurrentUserName();

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
		SendMetadataCommandListToWorkerInCoordinatedTransaction(workerNode->workerName,
																workerNode->workerPort,
																currentUser,
																recreateMetadataSnapshotCommandList);
		return true;
	}
	else
	{
		bool success =
			SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction(
				workerNode->workerName, workerNode->workerPort,
				currentUser, recreateMetadataSnapshotCommandList);

		return success;
	}
}


/*
 * DropMetadataSnapshotOnNode creates the queries which drop the metadata and sends them
 * to the worker given as parameter.
 */
static void
DropMetadataSnapshotOnNode(WorkerNode *workerNode)
{
	EnsureSequentialModeMetadataOperations();

	char *userName = CurrentUserName();

	/* generate the queries which drop the metadata */
	List *dropMetadataCommandList = MetadataDropCommands();

	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  LocalGroupIdUpdateCommand(0));

	SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction(
		workerNode->workerName,
		workerNode->workerPort,
		userName,
		dropMetadataCommandList);
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
	List *distributedTableList = CitusTableList();
	List *propagatedTableList = NIL;
	bool includeNodesFromOtherClusters = true;
	List *workerNodeList = ReadDistNode(includeNodesFromOtherClusters);
	IncludeSequenceDefaults includeSequenceDefaults = WORKER_NEXTVAL_SEQUENCE_DEFAULTS;

	/* make sure we have deterministic output for our tests */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* generate insert command for pg_dist_node table */
	char *nodeListInsertCommand = NodeListInsertCommand(workerNodeList);
	metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
										  nodeListInsertCommand);

	/* create the list of tables whose metadata will be created */
	CitusTableCacheEntry *cacheEntry = NULL;
	foreach_ptr(cacheEntry, distributedTableList)
	{
		if (ShouldSyncTableMetadata(cacheEntry->relationId))
		{
			propagatedTableList = lappend(propagatedTableList, cacheEntry);
		}
	}

	/* create the tables, but not the metadata */
	foreach_ptr(cacheEntry, propagatedTableList)
	{
		Oid relationId = cacheEntry->relationId;
		ObjectAddress tableAddress = { 0 };

		if (IsTableOwnedByExtension(relationId))
		{
			/* skip table creation when the Citus table is owned by an extension */
			continue;
		}

		List *ddlCommandList = GetFullTableCreationCommands(relationId,
															includeSequenceDefaults);
		char *tableOwnerResetCommand = TableOwnerResetCommand(relationId);

		/*
		 * Tables might have dependencies on different objects, since we create shards for
		 * table via multiple sessions these objects will be created via their own connection
		 * and committed immediately so they become visible to all sessions creating shards.
		 */
		ObjectAddressSet(tableAddress, RelationRelationId, relationId);
		EnsureDependenciesExistOnAllNodes(&tableAddress);

		/*
		 * Ensure sequence dependencies and mark them as distributed
		 */
		List *attnumList = NIL;
		List *dependentSequenceList = NIL;
		GetDependentSequencesWithRelation(relationId, &attnumList,
										  &dependentSequenceList, 0);
		MarkSequenceListDistributedAndPropagateDependencies(dependentSequenceList);

		List *workerSequenceDDLCommands = SequenceDDLCommandsForTable(relationId);
		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  workerSequenceDDLCommands);

		/* ddlCommandList contains TableDDLCommand information, need to materialize */
		TableDDLCommand *tableDDLCommand = NULL;
		foreach_ptr(tableDDLCommand, ddlCommandList)
		{
			Assert(CitusIsA(tableDDLCommand, TableDDLCommand));
			metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
												  GetTableDDLCommand(tableDDLCommand));
		}

		metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
											  tableOwnerResetCommand);

		List *sequenceDependencyCommandList = SequenceDependencyCommandList(
			relationId);
		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  sequenceDependencyCommandList);
	}

	/* construct the foreign key constraints after all tables are created */
	foreach_ptr(cacheEntry, propagatedTableList)
	{
		Oid relationId = cacheEntry->relationId;

		if (IsTableOwnedByExtension(relationId))
		{
			/* skip foreign key creation when the Citus table is owned by an extension */
			continue;
		}

		List *foreignConstraintCommands =
			GetReferencingForeignConstaintCommands(relationId);

		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  foreignConstraintCommands);
	}

	/* construct partitioning hierarchy after all tables are created */
	foreach_ptr(cacheEntry, propagatedTableList)
	{
		Oid relationId = cacheEntry->relationId;

		if (IsTableOwnedByExtension(relationId))
		{
			/* skip partition creation when the Citus table is owned by an extension */
			continue;
		}

		if (PartitionTable(relationId))
		{
			char *alterTableAttachPartitionCommands =
				GenerateAlterTableAttachPartitionCommand(relationId);

			metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
												  alterTableAttachPartitionCommands);
		}
	}

	/* after all tables are created, create the metadata */
	foreach_ptr(cacheEntry, propagatedTableList)
	{
		Oid clusteredTableId = cacheEntry->relationId;

		/* add the table metadata command first*/
		char *metadataCommand = DistributionCreateCommand(cacheEntry);
		metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
											  metadataCommand);

		/* add the truncate trigger command after the table became distributed */
		char *truncateTriggerCreateCommand =
			TruncateTriggerCreateCommand(cacheEntry->relationId);
		metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
											  truncateTriggerCreateCommand);

		/* add the pg_dist_shard{,placement} entries */
		List *shardIntervalList = LoadShardIntervalList(clusteredTableId);
		List *shardCreateCommandList = ShardListInsertCommand(shardIntervalList);

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
static List *
GetDistributedTableDDLEvents(Oid relationId)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	List *commandList = NIL;
	IncludeSequenceDefaults includeSequenceDefaults = WORKER_NEXTVAL_SEQUENCE_DEFAULTS;

	/* if the table is owned by an extension we only propagate pg_dist_* records */
	bool tableOwnedByExtension = IsTableOwnedByExtension(relationId);
	if (!tableOwnedByExtension)
	{
		/* commands to create sequences */
		List *sequenceDDLCommands = SequenceDDLCommandsForTable(relationId);
		commandList = list_concat(commandList, sequenceDDLCommands);

		/*
		 * Commands to create the table, these commands are TableDDLCommands so lets
		 * materialize to the non-sharded version
		 */
		List *tableDDLCommands = GetFullTableCreationCommands(relationId,
															  includeSequenceDefaults);
		TableDDLCommand *tableDDLCommand = NULL;
		foreach_ptr(tableDDLCommand, tableDDLCommands)
		{
			Assert(CitusIsA(tableDDLCommand, TableDDLCommand));
			commandList = lappend(commandList, GetTableDDLCommand(tableDDLCommand));
		}

		/* command to associate sequences with table */
		List *sequenceDependencyCommandList = SequenceDependencyCommandList(
			relationId);
		commandList = list_concat(commandList, sequenceDependencyCommandList);
	}

	/* command to insert pg_dist_partition entry */
	char *metadataCommand = DistributionCreateCommand(cacheEntry);
	commandList = lappend(commandList, metadataCommand);

	/* commands to create the truncate trigger of the table */
	char *truncateTriggerCreateCommand = TruncateTriggerCreateCommand(relationId);
	commandList = lappend(commandList, truncateTriggerCreateCommand);

	/* commands to insert pg_dist_shard & pg_dist_placement entries */
	List *shardIntervalList = LoadShardIntervalList(relationId);
	List *shardMetadataInsertCommandList = ShardListInsertCommand(shardIntervalList);
	commandList = list_concat(commandList, shardMetadataInsertCommandList);

	if (!tableOwnedByExtension)
	{
		/* commands to create foreign key constraints */
		List *foreignConstraintCommands =
			GetReferencingForeignConstaintCommands(relationId);
		commandList = list_concat(commandList, foreignConstraintCommands);

		/* commands to create partitioning hierarchy */
		if (PartitionTable(relationId))
		{
			char *alterTableAttachPartitionCommands =
				GenerateAlterTableAttachPartitionCommand(relationId);
			commandList = lappend(commandList, alterTableAttachPartitionCommands);
		}
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
					 "noderack, hasmetadata, metadatasynced, isactive, noderole, "
					 "nodecluster, shouldhaveshards) VALUES ");

	/* iterate over the worker nodes, add the values */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		char *hasMetadataString = workerNode->hasMetadata ? "TRUE" : "FALSE";
		char *metadataSyncedString = workerNode->metadataSynced ? "TRUE" : "FALSE";
		char *isActiveString = workerNode->isActive ? "TRUE" : "FALSE";
		char *shouldHaveShards = workerNode->shouldHaveShards ? "TRUE" : "FALSE";

		Datum nodeRoleOidDatum = ObjectIdGetDatum(workerNode->nodeRole);
		Datum nodeRoleStringDatum = DirectFunctionCall1(enum_out, nodeRoleOidDatum);
		char *nodeRoleString = DatumGetCString(nodeRoleStringDatum);

		appendStringInfo(nodeListInsertCommand,
						 "(%d, %d, %s, %d, %s, %s, %s, %s, '%s'::noderole, %s, %s)",
						 workerNode->nodeId,
						 workerNode->groupId,
						 quote_literal_cstr(workerNode->workerName),
						 workerNode->workerPort,
						 quote_literal_cstr(workerNode->workerRack),
						 hasMetadataString,
						 metadataSyncedString,
						 isActiveString,
						 nodeRoleString,
						 quote_literal_cstr(workerNode->nodeCluster),
						 shouldHaveShards);

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
DistributionCreateCommand(CitusTableCacheEntry *cacheEntry)
{
	StringInfo insertDistributionCommand = makeStringInfo();
	Oid relationId = cacheEntry->relationId;
	char distributionMethod = cacheEntry->partitionMethod;
	char *partitionKeyString = cacheEntry->partitionKeyString;
	char *qualifiedRelationName =
		generate_qualified_relation_name(relationId);
	uint32 colocationId = cacheEntry->colocationId;
	char replicationModel = cacheEntry->replicationModel;
	StringInfo tablePartitionKeyNameString = makeStringInfo();

	if (IsCitusTableTypeCacheEntry(cacheEntry, CITUS_TABLE_WITH_NO_DIST_KEY))
	{
		appendStringInfo(tablePartitionKeyNameString, "NULL");
	}
	else
	{
		char *partitionKeyColumnName =
			ColumnToColumnName(relationId, partitionKeyString);
		appendStringInfo(tablePartitionKeyNameString, "%s",
						 quote_literal_cstr(partitionKeyColumnName));
	}

	appendStringInfo(insertDistributionCommand,
					 "SELECT citus_internal_add_partition_metadata "
					 "(%s::regclass, '%c', %s, %d, '%c')",
					 quote_literal_cstr(qualifiedRelationName),
					 distributionMethod,
					 tablePartitionKeyNameString->data,
					 colocationId,
					 replicationModel);

	return insertDistributionCommand->data;
}


/*
 * DistributionDeleteCommand generates a command that can be executed
 * to drop a distributed table and its metadata on a remote node.
 */
char *
DistributionDeleteCommand(const char *schemaName, const char *tableName)
{
	StringInfo deleteDistributionCommand = makeStringInfo();

	char *distributedRelationName = quote_qualified_identifier(schemaName, tableName);

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
	int shardCount = list_length(shardIntervalList);

	/* if there are no shards, return empty list */
	if (shardCount == 0)
	{
		return commandList;
	}

	/* add placements to insertPlacementCommand */
	StringInfo insertPlacementCommand = makeStringInfo();
	appendStringInfo(insertPlacementCommand,
					 "WITH placement_data(shardid, shardstate, "
					 "shardlength, groupid, placementid)  AS (VALUES ");

	ShardInterval *shardInterval = NULL;
	bool firstPlacementProcessed = false;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		List *shardPlacementList = ActiveShardPlacementList(shardId);

		ShardPlacement *placement = NULL;
		foreach_ptr(placement, shardPlacementList)
		{
			if (firstPlacementProcessed)
			{
				/*
				 * As long as this is not the first placement of the first shard,
				 * append the comma.
				 */
				appendStringInfo(insertPlacementCommand, ", ");
			}
			firstPlacementProcessed = true;

			appendStringInfo(insertPlacementCommand,
							 "(%ld, %d, %ld, %d, %ld)",
							 shardId,
							 placement->shardState,
							 placement->shardLength,
							 placement->groupId,
							 placement->placementId);
		}
	}

	appendStringInfo(insertPlacementCommand, ") ");

	appendStringInfo(insertPlacementCommand,
					 "SELECT citus_internal_add_placement_metadata("
					 "shardid, shardstate, shardlength, groupid, placementid) "
					 "FROM placement_data;");

	/* now add shards to insertShardCommand */
	StringInfo insertShardCommand = makeStringInfo();
	appendStringInfo(insertShardCommand,
					 "WITH shard_data(relationname, shardid, storagetype, "
					 "shardminvalue, shardmaxvalue)  AS (VALUES ");

	foreach_ptr(shardInterval, shardIntervalList)
	{
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
						 "(%s::regclass, %ld, '%c'::\"char\", %s, %s)",
						 quote_literal_cstr(qualifiedRelationName),
						 shardId,
						 shardInterval->storageType,
						 minHashToken->data,
						 maxHashToken->data);

		if (llast(shardIntervalList) != shardInterval)
		{
			appendStringInfo(insertShardCommand, ", ");
		}
	}

	appendStringInfo(insertShardCommand, ") ");

	appendStringInfo(insertShardCommand,
					 "SELECT citus_internal_add_shard_metadata(relationname, shardid, "
					 "storagetype, shardminvalue, shardmaxvalue) "
					 "FROM shard_data;");

	/*
	 * There are no active placements for the table, so do not create the
	 * command as it'd lead to syntax error.
	 *
	 * This is normally not an expected situation, however the current
	 * implementation of citus_disable_node allows to disable nodes with
	 * the only active placements. So, for example a single shard/placement
	 * distributed table on a disabled node might trigger zero placement
	 * case.
	 *
	 * TODO: remove this check once citus_disable_node errors out for
	 * the above scenario.
	 */
	if (firstPlacementProcessed)
	{
		/* first insert shards, than the placements */
		commandList = lappend(commandList, insertShardCommand->data);
		commandList = lappend(commandList, insertPlacementCommand->data);
	}

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
	appendStringInfo(command,
					 "SELECT citus_internal_update_relation_colocation(%s::regclass, %d)",
					 quote_literal_cstr(qualifiedRelationName), colocationId);

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

	List *attnumList = NIL;
	List *dependentSequenceList = NIL;
	GetDependentSequencesWithRelation(relationId, &attnumList, &dependentSequenceList, 0);

	char *ownerName = TableOwner(relationId);

	Oid sequenceOid = InvalidOid;
	foreach_oid(sequenceOid, dependentSequenceList)
	{
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
 * GetAttributeTypeOid returns the OID of the type of the attribute of
 * provided relationId that has the provided attnum
 */
Oid
GetAttributeTypeOid(Oid relationId, AttrNumber attnum)
{
	Oid resultOid = InvalidOid;

	ScanKeyData key[2];

	/* Grab an appropriate lock on the pg_attribute relation */
	Relation attrel = table_open(AttributeRelationId, AccessShareLock);

	/* Use the index to scan only system attributes of the target relation */
	ScanKeyInit(&key[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));
	ScanKeyInit(&key[1],
				Anum_pg_attribute_attnum,
				BTLessEqualStrategyNumber, F_INT2LE,
				Int16GetDatum(attnum));

	SysScanDesc scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, NULL, 2,
										  key);

	HeapTuple attributeTuple;
	while (HeapTupleIsValid(attributeTuple = systable_getnext(scan)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		resultOid = att->atttypid;
	}

	systable_endscan(scan);
	table_close(attrel, AccessShareLock);

	return resultOid;
}


/*
 * GetDependentSequencesWithRelation appends the attnum and id of sequences that
 * have direct (owned sequences) or indirect dependency with the given relationId,
 * to the lists passed as NIL initially.
 * For both cases, we use the intermediate AttrDefault object from pg_depend.
 * If attnum is specified, we only return the sequences related to that
 * attribute of the relationId.
 */
void
GetDependentSequencesWithRelation(Oid relationId, List **attnumList,
								  List **dependentSequenceList, AttrNumber attnum)
{
	Assert(*attnumList == NIL && *dependentSequenceList == NIL);

	List *attrdefResult = NIL;
	List *attrdefAttnumResult = NIL;
	ScanKeyData key[3];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));
	if (attnum)
	{
		ScanKeyInit(&key[2],
					Anum_pg_depend_refobjsubid,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(attnum));
	}

	SysScanDesc scan = systable_beginscan(depRel, DependReferenceIndexId, true,
										  NULL, attnum ? 3 : 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (deprec->classid == AttrDefaultRelationId &&
			deprec->objsubid == 0 &&
			deprec->refobjsubid != 0 &&
			deprec->deptype == DEPENDENCY_AUTO)
		{
			attrdefResult = lappend_oid(attrdefResult, deprec->objid);
			attrdefAttnumResult = lappend_int(attrdefAttnumResult, deprec->refobjsubid);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	ListCell *attrdefOidCell = NULL;
	ListCell *attrdefAttnumCell = NULL;
	forboth(attrdefOidCell, attrdefResult, attrdefAttnumCell, attrdefAttnumResult)
	{
		Oid attrdefOid = lfirst_oid(attrdefOidCell);
		AttrNumber attrdefAttnum = lfirst_int(attrdefAttnumCell);

		List *sequencesFromAttrDef = GetSequencesFromAttrDef(attrdefOid);

		/* to simplify and eliminate cases like "DEFAULT nextval('..') - nextval('..')" */
		if (list_length(sequencesFromAttrDef) > 1)
		{
			ereport(ERROR, (errmsg(
								"More than one sequence in a column default"
								" is not supported for distribution "
								"or for adding local tables to metadata")));
		}

		if (list_length(sequencesFromAttrDef) == 1)
		{
			*dependentSequenceList = list_concat(*dependentSequenceList,
												 sequencesFromAttrDef);
			*attnumList = lappend_int(*attnumList, attrdefAttnum);
		}
	}
}


/*
 * GetSequencesFromAttrDef returns a list of sequence OIDs that have
 * dependency with the given attrdefOid in pg_depend
 */
List *
GetSequencesFromAttrDef(Oid attrdefOid)
{
	List *sequencesResult = NIL;
	ScanKeyData key[2];
	HeapTuple tup;

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(AttrDefaultRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attrdefOid));

	SysScanDesc scan = systable_beginscan(depRel, DependDependerIndexId, true,
										  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (deprec->refclassid == RelationRelationId &&
			deprec->deptype == DEPENDENCY_NORMAL &&
			get_rel_relkind(deprec->refobjid) == RELKIND_SEQUENCE)
		{
			sequencesResult = lappend_oid(sequencesResult, deprec->refobjid);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	return sequencesResult;
}


/*
 * SequenceDependencyCommandList generates commands to record the dependency
 * of sequences on tables on the worker. This dependency does not exist by
 * default since the sequences and table are created separately, but it is
 * necessary to ensure that the sequence is dropped when the table is
 * dropped.
 */
static List *
SequenceDependencyCommandList(Oid relationId)
{
	List *sequenceCommandList = NIL;
	List *columnNameList = NIL;
	List *sequenceIdList = NIL;

	ExtractDefaultColumnsAndOwnedSequences(relationId, &columnNameList, &sequenceIdList);

	ListCell *columnNameCell = NULL;
	ListCell *sequenceIdCell = NULL;

	forboth(columnNameCell, columnNameList, sequenceIdCell, sequenceIdList)
	{
		char *columnName = lfirst(columnNameCell);
		Oid sequenceId = lfirst_oid(sequenceIdCell);

		if (!OidIsValid(sequenceId))
		{
			/*
			 * ExtractDefaultColumnsAndOwnedSequences returns entries for all columns,
			 * but with 0 sequence ID unless there is default nextval(..).
			 */
			continue;
		}

		char *sequenceDependencyCommand =
			CreateSequenceDependencyCommand(relationId, sequenceId, columnName);

		sequenceCommandList = lappend(sequenceCommandList,
									  sequenceDependencyCommand);
	}

	return sequenceCommandList;
}


/*
 * CreateSequenceDependencyCommand generates a query string for calling
 * worker_record_sequence_dependency on the worker to recreate a sequence->table
 * dependency.
 */
static char *
CreateSequenceDependencyCommand(Oid relationId, Oid sequenceId, char *columnName)
{
	char *relationName = generate_qualified_relation_name(relationId);
	char *sequenceName = generate_qualified_relation_name(sequenceId);

	StringInfo sequenceDependencyCommand = makeStringInfo();

	appendStringInfo(sequenceDependencyCommand,
					 "SELECT pg_catalog.worker_record_sequence_dependency"
					 "(%s::regclass,%s::regclass,%s)",
					 quote_literal_cstr(sequenceName),
					 quote_literal_cstr(relationName),
					 quote_literal_cstr(columnName));

	return sequenceDependencyCommand->data;
}


/*
 * worker_record_sequence_dependency records the fact that the sequence depends on
 * the table in pg_depend, such that it will be automatically dropped.
 */
Datum
worker_record_sequence_dependency(PG_FUNCTION_ARGS)
{
	Oid sequenceOid = PG_GETARG_OID(0);
	Oid relationOid = PG_GETARG_OID(1);
	Name columnName = PG_GETARG_NAME(2);
	const char *columnNameStr = NameStr(*columnName);

	/* lookup column definition */
	HeapTuple columnTuple = SearchSysCacheAttName(relationOid, columnNameStr);
	if (!HeapTupleIsValid(columnTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" does not exist",
							   columnNameStr)));
	}

	Form_pg_attribute columnForm = (Form_pg_attribute) GETSTRUCT(columnTuple);
	if (columnForm->attnum <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create dependency on system column \"%s\"",
							   columnNameStr)));
	}

	ObjectAddress sequenceAddr = {
		.classId = RelationRelationId,
		.objectId = sequenceOid,
		.objectSubId = 0
	};
	ObjectAddress relationAddr = {
		.classId = RelationRelationId,
		.objectId = relationOid,
		.objectSubId = columnForm->attnum
	};


	EnsureTableOwner(sequenceOid);
	EnsureTableOwner(relationOid);

	/* dependency from sequence to table */
	recordDependencyOn(&sequenceAddr, &relationAddr, DEPENDENCY_AUTO);

	ReleaseSysCache(columnTuple);

	PG_RETURN_VOID();
}


/*
 * CreateSchemaDDLCommand returns a "CREATE SCHEMA..." SQL string for creating the given
 * schema if not exists and with proper authorization.
 */
char *
CreateSchemaDDLCommand(Oid schemaId)
{
	char *schemaName = get_namespace_name(schemaId);

	StringInfo schemaNameDef = makeStringInfo();
	const char *quotedSchemaName = quote_identifier(schemaName);
	const char *ownerName = quote_identifier(SchemaOwnerName(schemaId));
	appendStringInfo(schemaNameDef, CREATE_SCHEMA_COMMAND, quotedSchemaName, ownerName);

	return schemaNameDef->data;
}


/*
 * GrantOnSchemaDDLCommands creates a list of ddl command for replicating the permissions
 * of roles on schemas.
 */
List *
GrantOnSchemaDDLCommands(Oid schemaOid)
{
	HeapTuple schemaTuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schemaOid));
	bool isNull = true;
	Datum aclDatum = SysCacheGetAttr(NAMESPACEOID, schemaTuple, Anum_pg_namespace_nspacl,
									 &isNull);
	if (isNull)
	{
		ReleaseSysCache(schemaTuple);
		return NIL;
	}
	Acl *acl = DatumGetAclPCopy(aclDatum);
	AclItem *aclDat = ACL_DAT(acl);
	int aclNum = ACL_NUM(acl);
	List *commands = NIL;

	ReleaseSysCache(schemaTuple);

	for (int i = 0; i < aclNum; i++)
	{
		commands = list_concat(commands,
							   GenerateGrantOnSchemaQueriesFromAclItem(
								   schemaOid,
								   &aclDat[i]));
	}

	return commands;
}


/*
 * GenerateGrantOnSchemaQueryFromACL generates a query string for replicating a users permissions
 * on a schema.
 */
List *
GenerateGrantOnSchemaQueriesFromAclItem(Oid schemaOid, AclItem *aclItem)
{
	AclMode permissions = ACLITEM_GET_PRIVS(*aclItem) & ACL_ALL_RIGHTS_SCHEMA;
	AclMode grants = ACLITEM_GET_GOPTIONS(*aclItem) & ACL_ALL_RIGHTS_SCHEMA;

	/*
	 * seems unlikely but we check if there is a grant option in the list without the actual permission
	 */
	Assert(!(grants & ACL_USAGE) || (permissions & ACL_USAGE));
	Assert(!(grants & ACL_CREATE) || (permissions & ACL_CREATE));
	Oid granteeOid = aclItem->ai_grantee;
	List *queries = NIL;

	queries = lappend(queries, GenerateSetRoleQuery(aclItem->ai_grantor));

	if (permissions & ACL_USAGE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantOnSchemaStmtForRights(
										  granteeOid, schemaOid, "USAGE", grants &
										  ACL_USAGE));
		queries = lappend(queries, query);
	}
	if (permissions & ACL_CREATE)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantOnSchemaStmtForRights(
										  granteeOid, schemaOid, "CREATE", grants &
										  ACL_CREATE));
		queries = lappend(queries, query);
	}

	queries = lappend(queries, "RESET ROLE");

	return queries;
}


GrantStmt *
GenerateGrantOnSchemaStmtForRights(Oid roleOid,
								   Oid schemaOid,
								   char *permission,
								   bool withGrantOption)
{
	AccessPriv *accessPriv = makeNode(AccessPriv);
	accessPriv->priv_name = permission;
	accessPriv->cols = NULL;

	RoleSpec *roleSpec = makeNode(RoleSpec);
	roleSpec->roletype = OidIsValid(roleOid) ? ROLESPEC_CSTRING : ROLESPEC_PUBLIC;
	roleSpec->rolename = OidIsValid(roleOid) ? GetUserNameFromId(roleOid, false) : NULL;
	roleSpec->location = -1;

	GrantStmt *stmt = makeNode(GrantStmt);
	stmt->is_grant = true;
	stmt->targtype = ACL_TARGET_OBJECT;
	stmt->objtype = OBJECT_SCHEMA;
	stmt->objects = list_make1(makeString(get_namespace_name(schemaOid)));
	stmt->privileges = list_make1(accessPriv);
	stmt->grantees = list_make1(roleSpec);
	stmt->grant_option = withGrantOption;
	return stmt;
}


static char *
GenerateSetRoleQuery(Oid roleOid)
{
	StringInfo buf = makeStringInfo();
	appendStringInfo(buf, "SET ROLE %s", quote_identifier(GetUserNameFromId(roleOid,
																			false)));
	return buf->data;
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
	Oid ownerId = InvalidOid;

	HeapTuple tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(objectId));
	if (HeapTupleIsValid(tuple))
	{
		ownerId = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;
	}
	else
	{
		ownerId = GetUserId();
	}

	char *ownerName = GetUserNameFromId(ownerId, false);

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
	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(NoLock);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
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

	/* prevent recursive propagation */
	SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

	/* send the commands one by one */
	const char *command = NULL;
	foreach_ptr(command, commandList)
	{
		SendCommandToWorkersWithMetadata(command);
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
	List *distributedTableList = CitusTableList();

	/* we iterate over all distributed partitioned tables and DETACH their partitions */
	CitusTableCacheEntry *cacheEntry = NULL;
	foreach_ptr(cacheEntry, distributedTableList)
	{
		if (!PartitionedTable(cacheEntry->relationId))
		{
			continue;
		}

		List *partitionList = PartitionList(cacheEntry->relationId);
		List *detachCommands =
			GenerateDetachPartitionCommandRelationIdList(partitionList);
		detachPartitionCommandList = list_concat(detachPartitionCommandList,
												 detachCommands);
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
static MetadataSyncResult
SyncMetadataToNodes(void)
{
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

	List *syncedWorkerList = NIL;
	List *workerList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerList)
	{
		if (workerNode->hasMetadata && !workerNode->metadataSynced)
		{
			bool raiseInterrupts = false;
			if (!SyncMetadataSnapshotToNode(workerNode, raiseInterrupts))
			{
				ereport(WARNING, (errmsg("failed to sync metadata to %s:%d",
										 workerNode->workerName,
										 workerNode->workerPort)));
				result = METADATA_SYNC_FAILED_SYNC;
			}
			else
			{
				/* we add successfully synced nodes to set metadatasynced column later */
				syncedWorkerList = lappend(syncedWorkerList, workerNode);
			}
		}
	}

	foreach_ptr(workerNode, syncedWorkerList)
	{
		SetWorkerColumnOptional(workerNode, Anum_pg_dist_node_metadatasynced,
								BoolGetDatum(true));

		/* we fetch the same node again to check if it's synced or not */
		WorkerNode *nodeUpdated = FindWorkerNode(workerNode->workerName,
												 workerNode->workerPort);
		if (!nodeUpdated->metadataSynced)
		{
			/* set the result to FAILED to trigger the sync again */
			result = METADATA_SYNC_FAILED_SYNC;
		}
	}

	return result;
}


/*
 * SyncMetadataToNodesMain is the main function for syncing metadata to
 * MX nodes. It retries until success and then exits.
 */
void
SyncMetadataToNodesMain(Datum main_arg)
{
	Oid databaseOid = DatumGetObjectId(main_arg);

	/* extension owner is passed via bgw_extra */
	Oid extensionOwner = InvalidOid;
	memcpy_s(&extensionOwner, sizeof(extensionOwner),
			 MyBgworkerEntry->bgw_extra, sizeof(Oid));

	pqsignal(SIGTERM, MetadataSyncSigTermHandler);
	pqsignal(SIGALRM, MetadataSyncSigAlrmHandler);
	BackgroundWorkerUnblockSignals();

	/* connect to database, after that we can actually access catalogs */
	BackgroundWorkerInitializeConnectionByOid(databaseOid, extensionOwner, 0);

	/* make worker recognizable in pg_stat_activity */
	pgstat_report_appname(METADATA_SYNC_APP_NAME);

	bool syncedAllNodes = false;

	while (!syncedAllNodes)
	{
		InvalidateMetadataSystemCache();
		StartTransactionCommand();

		/*
		 * Some functions in ruleutils.c, which we use to get the DDL for
		 * metadata propagation, require an active snapshot.
		 */
		PushActiveSnapshot(GetTransactionSnapshot());

		if (!LockCitusExtension())
		{
			ereport(DEBUG1, (errmsg("could not lock the citus extension, "
									"skipping metadata sync")));
		}
		else if (CheckCitusVersion(DEBUG1) && CitusHasBeenLoaded())
		{
			UseCoordinatedTransaction();
			MetadataSyncResult result = SyncMetadataToNodes();

			syncedAllNodes = (result == METADATA_SYNC_SUCCESS);

			/* we use LISTEN/NOTIFY to wait for metadata syncing in tests */
			if (result != METADATA_SYNC_FAILED_LOCK)
			{
				Async_Notify(METADATA_SYNC_CHANNEL, NULL);
			}
		}

		PopActiveSnapshot();
		CommitTransactionCommand();
		ProcessCompletedNotifies();

		if (syncedAllNodes)
		{
			break;
		}

		/*
		 * If backend is cancelled (e.g. bacause of distributed deadlock),
		 * CHECK_FOR_INTERRUPTS() will raise a cancellation error which will
		 * result in exit(1).
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * SIGTERM is used for when maintenance daemon tries to clean-up
		 * metadata sync daemons spawned by terminated maintenance daemons.
		 */
		if (got_SIGTERM)
		{
			exit(0);
		}

		/*
		 * SIGALRM is used for testing purposes and it simulates an error in metadata
		 * sync daemon.
		 */
		if (got_SIGALRM)
		{
			elog(ERROR, "Error in metadata sync daemon");
		}

		pg_usleep(MetadataSyncRetryInterval * 1000);
	}
}


/*
 * MetadataSyncSigTermHandler set a flag to request termination of metadata
 * sync daemon.
 */
static void
MetadataSyncSigTermHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/*
 * MetadataSyncSigAlrmHandler set a flag to request error at metadata
 * sync daemon. This is used for testing purposes.
 */
static void
MetadataSyncSigAlrmHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGALRM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/*
 * SpawnSyncMetadataToNodes starts a background worker which runs metadata
 * sync. On success it returns workers' handle. Otherwise it returns NULL.
 */
BackgroundWorkerHandle *
SpawnSyncMetadataToNodes(Oid database, Oid extensionOwner)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;

	/* Configure a worker. */
	memset(&worker, 0, sizeof(worker));
	SafeSnprintf(worker.bgw_name, BGW_MAXLEN,
				 "Citus Metadata Sync: %u/%u",
				 database, extensionOwner);
	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* don't restart, we manage restarts from maintenance daemon */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy_s(worker.bgw_library_name, sizeof(worker.bgw_library_name), "citus");
	strcpy_s(worker.bgw_function_name, sizeof(worker.bgw_library_name),
			 "SyncMetadataToNodesMain");
	worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
	memcpy_s(worker.bgw_extra, sizeof(worker.bgw_extra), &extensionOwner,
			 sizeof(Oid));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		return NULL;
	}

	pid_t pid;
	WaitForBackgroundWorkerStartup(handle, &pid);

	return handle;
}


/*
 * SignalMetadataSyncDaemon signals metadata sync daemons belonging to
 * the given database.
 */
void
SignalMetadataSyncDaemon(Oid database, int sig)
{
	int backendCount = pgstat_fetch_stat_numbackends();
	for (int backend = 1; backend <= backendCount; backend++)
	{
		LocalPgBackendStatus *localBeEntry = pgstat_fetch_stat_local_beentry(backend);
		if (!localBeEntry)
		{
			continue;
		}

		PgBackendStatus *beStatus = &localBeEntry->backendStatus;
		if (beStatus->st_databaseid == database &&
			strncmp(beStatus->st_appname, METADATA_SYNC_APP_NAME, BGW_MAXLEN) == 0)
		{
			kill(beStatus->st_procpid, sig);
		}
	}
}


/*
 * ShouldInitiateMetadataSync returns if metadata sync daemon should be initiated.
 * It sets lockFailure to true if pg_dist_node lock couldn't be acquired for the
 * check.
 */
bool
ShouldInitiateMetadataSync(bool *lockFailure)
{
	if (!IsCoordinator())
	{
		*lockFailure = false;
		return false;
	}

	Oid distNodeOid = DistNodeRelationId();
	if (!ConditionalLockRelationOid(distNodeOid, AccessShareLock))
	{
		*lockFailure = true;
		return false;
	}

	bool shouldSyncMetadata = false;

	List *workerList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerList)
	{
		if (workerNode->hasMetadata && !workerNode->metadataSynced)
		{
			shouldSyncMetadata = true;
			break;
		}
	}

	UnlockRelationOid(distNodeOid, AccessShareLock);

	*lockFailure = false;
	return shouldSyncMetadata;
}


/*
 * citus_internal_add_partition_metadata is an internal UDF to
 * add a row to pg_dist_partition.
 */
Datum
citus_internal_add_partition_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "relation");
	Oid relationId = PG_GETARG_OID(0);

	PG_ENSURE_ARGNOTNULL(1, "distribution method");
	char distributionMethod = PG_GETARG_CHAR(1);

	PG_ENSURE_ARGNOTNULL(3, "Colocation ID");
	int colocationId = PG_GETARG_INT32(3);

	PG_ENSURE_ARGNOTNULL(4, "replication model");
	char replicationModel = PG_GETARG_CHAR(4);

	text *distributionColumnText = NULL;
	char *distributionColumnString = NULL;
	Var *distributionColumnVar = NULL;

	/* this flag is only valid for citus local tables, so set it to false */
	bool autoConverted = false;

	/* only owner of the table (or superuser) is allowed to add the Citus metadata */
	EnsureTableOwner(relationId);

	/* we want to serialize all the metadata changes to this table */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	if (!PG_ARGISNULL(2))
	{
		distributionColumnText = PG_GETARG_TEXT_P(2);
		distributionColumnString = text_to_cstring(distributionColumnText);

		Relation relation = relation_open(relationId, AccessShareLock);
		distributionColumnVar =
			BuildDistributionKeyFromColumnName(relation, distributionColumnString);
		Assert(distributionColumnVar != NULL);

		relation_close(relation, NoLock);
	}

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		if (distributionMethod == DISTRIBUTE_BY_NONE && distributionColumnVar != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Reference or local tables cannot have "
								   "distribution columns")));
		}
		else if (distributionMethod != DISTRIBUTE_BY_NONE &&
				 distributionColumnVar == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Distribution column cannot be NULL for "
								   "relation \"%s\"", get_rel_name(relationId))));
		}

		/*
		 * Even if the table owner is a malicious user and the partition
		 * metadata is not sane, the user can only affect its own tables.
		 * Given that the user is owner of the table, we should allow.
		 */
		EnsurePartitionMetadataIsSane(relationId, distributionMethod, colocationId,
									  replicationModel, distributionColumnVar);
	}

	InsertIntoPgDistPartition(relationId, distributionMethod, distributionColumnVar,
							  colocationId, replicationModel, autoConverted);

	PG_RETURN_VOID();
}


/*
 * EnsurePartitionMetadataIsSane ensures that the input values are safe
 * for inserting into pg_dist_partition metadata.
 */
static void
EnsurePartitionMetadataIsSane(Oid relationId, char distributionMethod, int colocationId,
							  char replicationModel, Var *distributionColumnVar)
{
	if (!(distributionMethod == DISTRIBUTE_BY_HASH ||
		  distributionMethod == DISTRIBUTE_BY_NONE))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for hash, reference "
							   "and local tables:%c", distributionMethod)));
	}

	if (colocationId < INVALID_COLOCATION_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for valid "
							   "colocation id values.")));
	}
	else if (colocationId != INVALID_COLOCATION_ID &&
			 distributionMethod == DISTRIBUTE_BY_HASH)
	{
		int count = 1;
		List *targetColocatedTableList =
			ColocationGroupTableList(colocationId, count);

		/*
		 * If we have any colocated hash tables, ensure if they share the
		 * same distribution key properties.
		 */
		if (list_length(targetColocatedTableList) >= 1)
		{
			Oid targetRelationId = linitial_oid(targetColocatedTableList);

			EnsureColumnTypeEquality(relationId, targetRelationId, distributionColumnVar,
									 DistPartitionKeyOrError(targetRelationId));
		}
	}


	if (!(replicationModel == REPLICATION_MODEL_2PC ||
		  replicationModel == REPLICATION_MODEL_STREAMING ||
		  replicationModel == REPLICATION_MODEL_COORDINATOR))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for "
							   "known replication models.")));
	}

	if (distributionMethod == DISTRIBUTE_BY_NONE &&
		!(replicationModel == REPLICATION_MODEL_STREAMING ||
		  replicationModel == REPLICATION_MODEL_2PC))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Local or references tables can only have '%c' or '%c' "
							   "as the replication model.",
							   REPLICATION_MODEL_STREAMING, REPLICATION_MODEL_2PC)));
	}
}


/*
 * citus_internal_add_shard_metadata is an internal UDF to
 * add a row to pg_dist_shard.
 */
Datum
citus_internal_add_shard_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "relation");
	Oid relationId = PG_GETARG_OID(0);

	PG_ENSURE_ARGNOTNULL(1, "shard id");
	int64 shardId = PG_GETARG_INT64(1);

	PG_ENSURE_ARGNOTNULL(2, "storage type");
	char storageType = PG_GETARG_CHAR(2);

	text *shardMinValue = NULL;
	if (!PG_ARGISNULL(3))
	{
		shardMinValue = PG_GETARG_TEXT_P(3);
	}

	text *shardMaxValue = NULL;
	if (!PG_ARGISNULL(4))
	{
		shardMaxValue = PG_GETARG_TEXT_P(4);
	}

	/* only owner of the table (or superuser) is allowed to add the Citus metadata */
	EnsureTableOwner(relationId);

	/* we want to serialize all the metadata changes to this table */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		/*
		 * Even if the table owner is a malicious user and the shard metadata is
		 * not sane, the user can only affect its own tables. Given that the
		 * user is owner of the table, we should allow.
		 */
		EnsureShardMetadataIsSane(relationId, shardId, storageType, shardMinValue,
								  shardMaxValue);
	}

	InsertShardRow(relationId, shardId, storageType, shardMinValue, shardMaxValue);

	PG_RETURN_VOID();
}


/*
 * EnsureCoordinatorInitiatedOperation is a helper function which ensures that
 * the execution is initiated by the coordinator on a worker node.
 */
static void
EnsureCoordinatorInitiatedOperation(void)
{
	/*
	 * We are restricting the operation to only MX workers with the local group id
	 * check. The other two checks are to ensure that the operation is initiated
	 * by the coordinator.
	 */
	if (!IsCitusInitiatedRemoteBackend() || !MyBackendIsInDisributedTransaction() ||
		GetLocalGroupId() == COORDINATOR_GROUP_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("This is an internal Citus function can only be "
							   "used in a distributed transaction")));
	}
}


/*
 * EnsureShardMetadataIsSane ensures that the input values are safe
 * for inserting into pg_dist_shard metadata.
 */
static void
EnsureShardMetadataIsSane(Oid relationId, int64 shardId, char storageType,
						  text *shardMinValue, text *shardMaxValue)
{
	if (shardId <= INVALID_SHARD_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Invalid shard id: %ld", shardId)));
	}

	if (!(storageType == SHARD_STORAGE_TABLE ||
		  storageType == SHARD_STORAGE_FOREIGN))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Invalid shard storage type: %c", storageType)));
	}

	char partitionMethod = PartitionMethodViaCatalog(relationId);
	if (partitionMethod == DISTRIBUTE_BY_INVALID)
	{
		/* connection from the coordinator operating on a shard */
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("The relation \"%s\" does not have a valid "
							   "entry in pg_dist_partition.",
							   get_rel_name(relationId))));
	}
	else if (!(partitionMethod == DISTRIBUTE_BY_HASH ||
			   partitionMethod == DISTRIBUTE_BY_NONE))
	{
		/* connection from the coordinator operating on a shard */
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Metadata syncing is only allowed for hash, "
							   "reference and local tables: %c", partitionMethod)));
	}

	List *distShardTupleList = LookupDistShardTuples(relationId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		if (shardMinValue != NULL || shardMaxValue != NULL)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shards of reference or local table \"%s\" should "
								   "have NULL shard ranges", relationName)));
		}
		else if (list_length(distShardTupleList) != 0)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("relation \"%s\" has already at least one shard, "
								   "adding more is not allowed", relationName)));
		}
	}
	else if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		if (shardMinValue == NULL || shardMaxValue == NULL)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shards of has distributed table  \"%s\" "
								   "cannot have NULL shard ranges", relationName)));
		}

		char *shardMinValueString = text_to_cstring(shardMinValue);
		char *shardMaxValueString = text_to_cstring(shardMaxValue);

		/* pg_strtoint32 does the syntax and out of bound checks for us */
		int32 shardMinValueInt = pg_strtoint32(shardMinValueString);
		int32 shardMaxValueInt = pg_strtoint32(shardMaxValueString);

		if (shardMinValueInt > shardMaxValueInt)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("shardMinValue=%d is greater than "
								   "shardMaxValue=%d for table \"%s\", which is "
								   "not allowed", shardMinValueInt,
								   shardMaxValueInt, get_rel_name(relationId))));
		}

		/*
		 * We are only dealing with hash distributed tables, that's why we
		 * can hard code data type and typemod.
		 */
		const int intervalTypeId = INT4OID;
		const int intervalTypeMod = -1;

		Relation distShardRelation = table_open(DistShardRelationId(), AccessShareLock);
		TupleDesc distShardTupleDesc = RelationGetDescr(distShardRelation);

		FmgrInfo *shardIntervalCompareFunction =
			GetFunctionInfo(intervalTypeId, BTREE_AM_OID, BTORDER_PROC);

		HeapTuple shardTuple = NULL;
		foreach_ptr(shardTuple, distShardTupleList)
		{
			ShardInterval *shardInterval =
				TupleToShardInterval(shardTuple, distShardTupleDesc,
									 intervalTypeId, intervalTypeMod);

			Datum firstMin = Int32GetDatum(shardMinValueInt);
			Datum firstMax = Int32GetDatum(shardMaxValueInt);
			Datum secondMin = shardInterval->minValue;
			Datum secondMax = shardInterval->maxValue;
			Oid collationId = InvalidOid;

			/*
			 * This is an unexpected case as we are reading the metadata, which has
			 * already been verified for being not NULL. Still, lets be extra
			 * cautious to avoid any crashes.
			 */
			if (!shardInterval->minValueExists || !shardInterval->maxValueExists)
			{
				char *relationName = get_rel_name(relationId);
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("Shards of has distributed table  \"%s\" "
									   "cannot have NULL shard ranges", relationName)));
			}

			if (ShardIntervalsOverlapWithParams(firstMin, firstMax, secondMin, secondMax,
												shardIntervalCompareFunction,
												collationId))
			{
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("Shard intervals overlap for table \"%s\": "
									   "%ld and %ld", get_rel_name(relationId),
									   shardId, shardInterval->shardId)));
			}
		}

		table_close(distShardRelation, NoLock);
	}
}


/*
 * citus_internal_add_placement_metadata is an internal UDF to
 * add a row to pg_dist_placement.
 */
Datum
citus_internal_add_placement_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);
	int32 shardState = PG_GETARG_INT32(1);
	int64 shardLength = PG_GETARG_INT64(2);
	int32 groupId = PG_GETARG_INT32(3);
	int64 placementId = PG_GETARG_INT64(4);

	bool missingOk = false;
	Oid relationId = LookupShardRelationFromCatalog(shardId, missingOk);

	/* only owner of the table is allowed to modify the metadata */
	EnsureTableOwner(relationId);

	/* we want to serialize all the metadata changes to this table */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		/*
		 * Even if the table owner is a malicious user, as long as the shard placements
		 * fit into basic requirements of Citus metadata, the user can only affect its
		 * own tables. Given that the user is owner of the table, we should allow.
		 */
		EnsureShardPlacementMetadataIsSane(relationId, shardId, placementId, shardState,
										   shardLength, groupId);
	}

	InsertShardPlacementRow(shardId, placementId, shardState, shardLength, groupId);

	PG_RETURN_VOID();
}


/*
 * EnsureShardPlacementMetadataIsSane ensures if the input parameters for
 * the shard placement metadata is sane.
 */
static void
EnsureShardPlacementMetadataIsSane(Oid relationId, int64 shardId, int64 placementId,
								   int32 shardState, int64 shardLength, int32 groupId)
{
	/* we have just read the metadata, so we are sure that the shard exists */
	Assert(ShardExists(shardId));

	if (placementId <= INVALID_PLACEMENT_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Shard placement has invalid placement id "
							   "(%ld) for shard(%ld)", placementId, shardId)));
	}

	if (shardState != SHARD_STATE_ACTIVE)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Invalid shard state: %d", shardState)));
	}

	bool nodeIsInMetadata = false;
	WorkerNode *workerNode =
		PrimaryNodeForGroup(groupId, &nodeIsInMetadata);
	if (!workerNode)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Node with group id %d for shard placement "
							   "%ld does not exist", groupId, shardId)));
	}
}


/*
 * ShouldSkipMetadataChecks returns true if the current user is allowed to
 * make any
 */
static bool
ShouldSkipMetadataChecks(void)
{
	if (strcmp(EnableManualMetadataChangesForUser, "") != 0)
	{
		/*
		 * EnableManualMetadataChangesForUser is a GUC which
		 * can be changed by a super user. We use this GUC as
		 * a safety belt in case the current metadata checks are
		 * too restrictive and the operator can allow users to skip
		 * the checks.
		 */

		/*
		 * Make sure that the user exists, and print it to prevent any
		 * optimization skipping the get_role_oid call.
		 */
		bool missingOK = false;
		Oid allowedUserId = get_role_oid(EnableManualMetadataChangesForUser, missingOK);
		if (allowedUserId == GetUserId())
		{
			return true;
		}
	}

	return false;
}


/*
 * citus_internal_update_placement_metadata is an internal UDF to
 * update a row in pg_dist_placement.
 */
Datum
citus_internal_update_placement_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);
	int32 sourceGroupId = PG_GETARG_INT32(1);
	int32 targetGroupId = PG_GETARG_INT32(2);

	ShardPlacement *placement = NULL;
	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		if (!ShardExists(shardId))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shard id does not exists: %ld", shardId)));
		}

		bool missingOk = false;
		EnsureShardOwner(shardId, missingOk);

		/*
		 * This function ensures that the source group exists hence we
		 * call it from this code-block.
		 */
		placement = ActiveShardPlacementOnGroup(sourceGroupId, shardId);

		bool nodeIsInMetadata = false;
		WorkerNode *workerNode =
			PrimaryNodeForGroup(targetGroupId, &nodeIsInMetadata);
		if (!workerNode)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Node with group id %d for shard placement "
								   "%ld does not exist", targetGroupId, shardId)));
		}
	}
	else
	{
		placement = ActiveShardPlacementOnGroup(sourceGroupId, shardId);
	}

	/*
	 * Updating pg_dist_placement ensures that the node with targetGroupId
	 * exists and this is the only placement on that group.
	 */
	if (placement == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Active placement for shard %ld is not "
							   "found on group:%d", shardId, targetGroupId)));
	}

	UpdatePlacementGroupId(placement->placementId, targetGroupId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_delete_shard_metadata is an internal UDF to
 * delete a row in pg_dist_shard and corresponding placement rows
 * from pg_dist_shard_placement.
 */
Datum
citus_internal_delete_shard_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int64 shardId = PG_GETARG_INT64(0);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		if (!ShardExists(shardId))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Shard id does not exists: %ld", shardId)));
		}

		bool missingOk = false;
		EnsureShardOwner(shardId, missingOk);
	}

	List *shardPlacementList = ShardPlacementListIncludingOrphanedPlacements(shardId);
	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, shardPlacementList)
	{
		DeleteShardPlacementRow(shardPlacement->placementId);
	}

	DeleteShardRow(shardId);

	PG_RETURN_VOID();
}


/*
 * citus_internal_update_relation_colocation is an internal UDF to
 * delete a row in pg_dist_shard and corresponding placement rows
 * from pg_dist_shard_placement.
 */
Datum
citus_internal_update_relation_colocation(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	uint32 tagetColocationId = PG_GETARG_UINT32(1);

	EnsureTableOwner(relationId);

	if (!ShouldSkipMetadataChecks())
	{
		/* this UDF is not allowed allowed for executing as a separate command */
		EnsureCoordinatorInitiatedOperation();

		/* ensure that the table is in pg_dist_partition */
		char partitionMethod = PartitionMethodViaCatalog(relationId);
		if (partitionMethod == DISTRIBUTE_BY_INVALID)
		{
			/* connection from the coordinator operating on a shard */
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("The relation \"%s\" does not have a valid "
								   "entry in pg_dist_partition.",
								   get_rel_name(relationId))));
		}
		else if (partitionMethod != DISTRIBUTE_BY_HASH)
		{
			/* connection from the coordinator operating on a shard */
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("Updating colocation ids are only allowed for hash "
								   "distributed tables: %c", partitionMethod)));
		}

		int count = 1;
		List *targetColocatedTableList =
			ColocationGroupTableList(tagetColocationId, count);

		if (list_length(targetColocatedTableList) == 0)
		{
			/* the table is colocated with none, so nothing to check */
		}
		else
		{
			Oid targetRelationId = linitial_oid(targetColocatedTableList);

			ErrorIfShardPlacementsNotColocated(relationId, targetRelationId);
			CheckReplicationModel(relationId, targetRelationId);
			CheckDistributionColumnType(relationId, targetRelationId);
		}
	}

	bool localOnly = true;
	UpdateRelationColocationGroup(relationId, tagetColocationId, localOnly);

	PG_RETURN_VOID();
}
