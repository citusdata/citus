/*-------------------------------------------------------------------------
 *
 * master_create_shards.c
 *
 * This file contains functions to distribute a table by creating shards for it
 * across a set of worker nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_d.h"
#include "commands/tablecmds.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/transaction_management.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_create_worker_shards);

/* utility functions for lazy shard creation of citus local tables */
static void RenameRelationToShardRelation(Oid relationOid, uint64 shardId);
static void RenameShardRelationConstraints(Oid relationOid, uint64 shardId);
static List * GetRelationConstraintNames(Oid relationOid);
static void RenameForeignConstraintsReferencingToShard(Oid relationOid, uint64 shardId);
static List * GetForeignConstraintsReferencingToShard(Oid relationOid);
static void RenameShardRelationIndexes(Oid relationOid, uint64 shardId);
static List * GetShardRelationIndexNames(Oid relationOid);

/*
 * master_create_worker_shards is a user facing function to create worker shards
 * for the given relation in round robin order.
 */
Datum
master_create_worker_shards(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	int32 shardCount = PG_GETARG_INT32(1);
	int32 replicationFactor = PG_GETARG_INT32(2);
	ObjectAddress tableAddress = { 0 };

	Oid distributedTableId = ResolveRelationId(tableNameText, false);

	/* do not add any data */
	bool useExclusiveConnections = false;

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	/*
	 * distributed tables might have dependencies on different objects, since we create
	 * shards for a distributed table via multiple sessions these objects will be created
	 * via their own connection and committed immediately so they become visible to all
	 * sessions creating shards.
	 */
	ObjectAddressSet(tableAddress, RelationRelationId, distributedTableId);
	EnsureDependenciesExistOnAllNodes(&tableAddress);

	EnsureReferenceTablesExistOnAllNodes();

	CreateShardsWithRoundRobinPolicy(distributedTableId, shardCount, replicationFactor,
									 useExclusiveConnections);

	PG_RETURN_VOID();
}


/*
 * CreateShardsWithRoundRobinPolicy creates empty shards for the given table
 * based on the specified number of initial shards. The function first updates
 * metadata on the coordinator node to make this shard (and its placements)
 * visible. Note that the function assumes the table is hash partitioned and
 * calculates the min/max hash token ranges for each shard, giving them an equal
 * split of the hash space. Finally, function creates empty shard placements on
 * worker nodes.
 */
void
CreateShardsWithRoundRobinPolicy(Oid distributedTableId, int32 shardCount,
								 int32 replicationFactor, bool useExclusiveConnections)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);
	bool colocatedShard = false;
	List *insertedShardPlacements = NIL;

	/* make sure table is hash partitioned */
	CheckHashPartitionedTable(distributedTableId);

	/*
	 * In contrast to append/range partitioned tables it makes more sense to
	 * require ownership privileges - shards for hash-partitioned tables are
	 * only created once, not continually during ingest as for the other
	 * partitioning types.
	 */
	EnsureTableOwner(distributedTableId);

	/* we plan to add shards: get an exclusive lock on relation oid */
	LockRelationOid(distributedTableId, ExclusiveLock);

	/* validate that shards haven't already been created for this table */
	List *existingShardList = LoadShardList(distributedTableId);
	if (existingShardList != NIL)
	{
		char *tableName = get_rel_name(distributedTableId);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   tableName)));
	}

	/* make sure that at least one shard is specified */
	if (shardCount <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shard_count must be positive")));
	}

	/* make sure that at least one replica is specified */
	if (replicationFactor <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor must be positive")));
	}

	/* make sure that RF=1 if the table is streaming replicated */
	if (cacheEntry->replicationModel == REPLICATION_MODEL_STREAMING &&
		replicationFactor > 1)
	{
		char *relationName = get_rel_name(cacheEntry->relationId);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("using replication factor %d with the streaming "
							   "replication model is not supported",
							   replicationFactor),
						errdetail("The table %s is marked as streaming replicated and "
								  "the shard replication factor of streaming replicated "
								  "tables must be 1.", relationName),
						errhint("Use replication factor 1.")));
	}

	/* calculate the split of the hash space */
	uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;

	/* don't allow concurrent node list changes that require an exclusive lock */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/* load and sort the worker node list for deterministic placement */
	List *workerNodeList = DistributedTablePlacementNodeList(NoLock);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	int32 workerNodeCount = list_length(workerNodeList);
	if (replicationFactor > workerNodeCount)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor (%d) exceeds number of worker nodes "
							   "(%d)", replicationFactor, workerNodeCount),
						errhint("Add more worker nodes or try again with a lower "
								"replication factor.")));
	}

	/* if we have enough nodes, add an extra placement attempt for backup */
	uint32 placementAttemptCount = (uint32) replicationFactor;
	if (workerNodeCount > replicationFactor)
	{
		placementAttemptCount++;
	}

	/* set shard storage type according to relation type */
	char shardStorageType = ShardStorageType(distributedTableId);

	for (int64 shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		uint32 roundRobinNodeIndex = shardIndex % workerNodeCount;

		/* initialize the hash token space for this shard */
		int32 shardMinHashToken = INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);
		uint64 shardId = GetNextShardId();

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT32_MAX;
		}

		/* insert the shard metadata row along with its min/max values */
		text *minHashTokenText = IntegerToText(shardMinHashToken);
		text *maxHashTokenText = IntegerToText(shardMaxHashToken);

		InsertShardRow(distributedTableId, shardId, shardStorageType,
					   minHashTokenText, maxHashTokenText);

		List *currentInsertedShardPlacements = InsertShardPlacementRows(
			distributedTableId,
			shardId,
			workerNodeList,
			roundRobinNodeIndex,
			replicationFactor);
		insertedShardPlacements = list_concat(insertedShardPlacements,
											  currentInsertedShardPlacements);
	}

	CreateShardsOnWorkers(distributedTableId, insertedShardPlacements,
						  useExclusiveConnections, colocatedShard);
}


/*
 * CreateColocatedShards creates shards for the target relation colocated with
 * the source relation.
 */
void
CreateColocatedShards(Oid targetRelationId, Oid sourceRelationId, bool
					  useExclusiveConnections)
{
	bool colocatedShard = true;
	List *insertedShardPlacements = NIL;

	/* make sure that tables are hash partitioned */
	CheckHashPartitionedTable(targetRelationId);
	CheckHashPartitionedTable(sourceRelationId);

	/*
	 * In contrast to append/range partitioned tables it makes more sense to
	 * require ownership privileges - shards for hash-partitioned tables are
	 * only created once, not continually during ingest as for the other
	 * partitioning types.
	 */
	EnsureTableOwner(targetRelationId);

	/* we plan to add shards: get an exclusive lock on target relation oid */
	LockRelationOid(targetRelationId, ExclusiveLock);

	/* we don't want source table to get dropped before we colocate with it */
	LockRelationOid(sourceRelationId, AccessShareLock);

	/* prevent placement changes of the source relation until we colocate with them */
	List *sourceShardIntervalList = LoadShardIntervalList(sourceRelationId);
	LockShardListMetadata(sourceShardIntervalList, ShareLock);

	/* validate that shards haven't already been created for this table */
	List *existingShardList = LoadShardList(targetRelationId);
	if (existingShardList != NIL)
	{
		char *targetRelationName = get_rel_name(targetRelationId);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   targetRelationName)));
	}

	char targetShardStorageType = ShardStorageType(targetRelationId);

	ShardInterval *sourceShardInterval = NULL;
	foreach_ptr(sourceShardInterval, sourceShardIntervalList)
	{
		uint64 sourceShardId = sourceShardInterval->shardId;
		uint64 newShardId = GetNextShardId();

		int32 shardMinValue = DatumGetInt32(sourceShardInterval->minValue);
		int32 shardMaxValue = DatumGetInt32(sourceShardInterval->maxValue);
		text *shardMinValueText = IntegerToText(shardMinValue);
		text *shardMaxValueText = IntegerToText(shardMaxValue);
		List *sourceShardPlacementList = ShardPlacementList(sourceShardId);

		InsertShardRow(targetRelationId, newShardId, targetShardStorageType,
					   shardMinValueText, shardMaxValueText);

		ShardPlacement *sourcePlacement = NULL;
		foreach_ptr(sourcePlacement, sourceShardPlacementList)
		{
			if (sourcePlacement->shardState == SHARD_STATE_TO_DELETE)
			{
				continue;
			}

			int32 groupId = sourcePlacement->groupId;
			const ShardState shardState = SHARD_STATE_ACTIVE;
			const uint64 shardSize = 0;

			/*
			 * Optimistically add shard placement row the pg_dist_shard_placement, in case
			 * of any error it will be roll-backed.
			 */
			uint64 shardPlacementId = InsertShardPlacementRow(newShardId,
															  INVALID_PLACEMENT_ID,
															  shardState, shardSize,
															  groupId);

			ShardPlacement *shardPlacement = LoadShardPlacement(newShardId,
																shardPlacementId);
			insertedShardPlacements = lappend(insertedShardPlacements, shardPlacement);
		}
	}

	CreateShardsOnWorkers(targetRelationId, insertedShardPlacements,
						  useExclusiveConnections, colocatedShard);
}


/*
 * CreateReferenceTableShard creates a single shard for the given
 * distributedTableId. The created shard does not have min/max values.
 * Also, the shard is replicated to the all active nodes in the cluster.
 */
void
CreateReferenceTableShard(Oid distributedTableId)
{
	int workerStartIndex = 0;
	text *shardMinValue = NULL;
	text *shardMaxValue = NULL;
	bool useExclusiveConnection = false;
	bool colocatedShard = false;

	/*
	 * In contrast to append/range partitioned tables it makes more sense to
	 * require ownership privileges - shards for reference tables are
	 * only created once, not continually during ingest as for the other
	 * partitioning types such as append and range.
	 */
	EnsureTableOwner(distributedTableId);

	/* we plan to add shards: get an exclusive lock on relation oid */
	LockRelationOid(distributedTableId, ExclusiveLock);

	/* set shard storage type according to relation type */
	char shardStorageType = ShardStorageType(distributedTableId);

	/* validate that shards haven't already been created for this table */
	List *existingShardList = LoadShardList(distributedTableId);
	if (existingShardList != NIL)
	{
		char *tableName = get_rel_name(distributedTableId);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   tableName)));
	}

	/*
	 * load and sort the worker node list for deterministic placements
	 * create_reference_table has already acquired pg_dist_node lock
	 */
	List *nodeList = ReferenceTablePlacementNodeList(ShareLock);
	nodeList = SortList(nodeList, CompareWorkerNodes);

	int replicationFactor = ReferenceTableReplicationFactor();

	/* get the next shard id */
	uint64 shardId = GetNextShardId();

	InsertShardRow(distributedTableId, shardId, shardStorageType, shardMinValue,
				   shardMaxValue);

	List *insertedShardPlacements = InsertShardPlacementRows(distributedTableId, shardId,
															 nodeList, workerStartIndex,
															 replicationFactor);

	CreateShardsOnWorkers(distributedTableId, insertedShardPlacements,
						  useExclusiveConnection, colocatedShard);
}


/*
 * CreateCitusLocalTableShard creates the one and only shard of the citus
 * local table lazily. That means, this function suffixes shardId to:
 *  - relation name,
 *  - all the objects "defined on" the relation and
 *  - the foreign keys referencing to the relation.
 */
void
CreateCitusLocalTableShard(Oid relationOid, uint64 shardId)
{
	RenameRelationToShardRelation(relationOid, shardId);
	RenameShardRelationConstraints(relationOid, shardId);
	RenameForeignConstraintsReferencingToShard(relationOid, shardId);
	RenameShardRelationIndexes(relationOid, shardId);
}


/*
 * RenameRelationToShardRelation appends given shardId to the end of the name
 * of relation with relationOid.
 */
static void
RenameRelationToShardRelation(Oid relationOid, uint64 shardId)
{
	Oid schemaOid = get_rel_namespace(relationOid);
	char *schemaName = get_namespace_name(schemaOid);
	char *relationName = get_rel_name(relationOid);
	char *qualifiedRelationName = quote_qualified_identifier(schemaName, relationName);

	char *shardRelationName = pstrdup(relationName);
	AppendShardIdToName(&shardRelationName, shardId);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER TABLE %s RENAME TO %s;",
					 qualifiedRelationName, shardRelationName);

	const char *commandString = renameCommand->data;

	Node *parseTree = ParseTreeNode(commandString);

	CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
						NULL, None_Receiver, NULL);
}


/*
 * RenameShardRelationConstraints appends given shardId to the end of the name
 * of constraints "defined on" the relation with relationOid. This function
 * utilizes GetRelationConstraintNames to pick the constraints to be renamed,
 * see more details on that function's comment.
 */
static void
RenameShardRelationConstraints(Oid relationOid, uint64 shardId)
{
	Oid schemaOid = get_rel_namespace(relationOid);
	char *schemaName = get_namespace_name(schemaOid);
	char *relationName = get_rel_name(relationOid);
	char *qualifiedRelationName = quote_qualified_identifier(schemaName, relationName);

	List *constraintNameList = GetRelationConstraintNames(relationOid);

	char *constraintName = NULL;
	foreach_ptr(constraintName, constraintNameList)
	{
		char *shardConstraintName = pstrdup(constraintName);
		AppendShardIdToName(&shardConstraintName, shardId);

		StringInfo renameCommand = makeStringInfo();
		appendStringInfo(renameCommand, "ALTER TABLE %s RENAME CONSTRAINT %s TO %s;",
						 qualifiedRelationName, constraintName, shardConstraintName);

		const char *commandString = renameCommand->data;

		Node *parseTree = ParseTreeNode(commandString);

		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL, NULL,
							None_Receiver, NULL);
	}
}


/*
 * GetRelationConstraintNames returns a list constraint names "defined on"
 * the relation with relationOid. Those constraints can be:
 *  - "check" constraints or,
 *  - "primary key" constraints or,
 *  - "unique" constraints or,
 *  - "trigger" constraints or,
 *  - "exclusion" constraints or,
 *  - "foreign key" constraints in which the relation is the "referencing"
 *     relation (including the self-referencing foreign keys).
 */
static List *
GetRelationConstraintNames(Oid relationOid)
{
	List *constraintNames = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOk = true;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
				F_OIDEQ, relationOid);
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													ConstraintRelidTypidNameIndexId,
													indexOk, NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		constraintNames = lappend(constraintNames, pstrdup(constraintForm->conname.data));

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return constraintNames;
}


/*
 * RenameForeignConstraintsReferencingToShard appends given shardId to the
 * end of the name of foreign key constraints in which the relation with
 * relationOid is the "referenced one" except the self-referencing foreign
 * keys. This is because, we already renamed self-referencing foreign keys
 * via RenameShardRelationConstraints function.
 */
static void
RenameForeignConstraintsReferencingToShard(Oid relationOid, uint64 shardId)
{
	List *foreignConstraintForms = GetForeignConstraintsReferencingToShard(relationOid);

	Form_pg_constraint foreignConstraintForm = NULL;
	foreach_ptr(foreignConstraintForm, foreignConstraintForms)
	{
		Oid referencedTableOid = foreignConstraintForm->conrelid;
		Oid referencedTableSchemaOid = get_rel_namespace(referencedTableOid);
		char *referencedTableSchemaName = get_namespace_name(referencedTableSchemaOid);
		char *referencedRelationName = get_rel_name(referencedTableOid);
		char *referencedRelationQualifiedName = quote_qualified_identifier(
			referencedTableSchemaName, referencedRelationName);

		char *constraintName = foreignConstraintForm->conname.data;

		char *shardConstraintName = pstrdup(constraintName);
		AppendShardIdToName(&shardConstraintName, shardId);

		StringInfo renameCommand = makeStringInfo();
		appendStringInfo(renameCommand, "ALTER TABLE %s RENAME CONSTRAINT %s TO %s;",
						 referencedRelationQualifiedName, constraintName,
						 shardConstraintName);

		const char *commandString = renameCommand->data;

		Node *parseTree = ParseTreeNode(commandString);

		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * GetForeignConstraintsReferencingToShard returns a list constraint form
 * objects for the relation with relationOid representing the foreign keys
 * in which the relation is the "referenced" relation except the
 * "self-referencing foreign keys".
 */
static List *
GetForeignConstraintsReferencingToShard(Oid relationOid)
{
	List *constraintForms = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool indexOk = false;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_confrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationOid);
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, scanIndexId,
													indexOk, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);

	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype != CONSTRAINT_FOREIGN)
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		if (constraintForm->conrelid == constraintForm->confrelid)
		{
			/* skip self referencing foreign keys */
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		constraintForms = lappend(constraintForms, constraintForm);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return constraintForms;
}


/*
 * RenameShardRelationIndexes appends given shardId to the end of the names
 * of shard relation indexes except the ones that are already renamed via
 * RenameShardRelationConstraints. This function utilizes
 * GetShardRelationIndexNames to pick the indexes to be renamed, see more
 * details on that function's comment.
 */
static void
RenameShardRelationIndexes(Oid relationOid, uint64 shardId)
{
	List *indexNameList = GetShardRelationIndexNames(relationOid);

	char *indexName = NULL;
	foreach_ptr(indexName, indexNameList)
	{
		char *shardIndexName = pstrdup(indexName);
		AppendShardIdToName(&shardIndexName, shardId);

		StringInfo renameCommand = makeStringInfo();
		appendStringInfo(renameCommand, "ALTER INDEX %s RENAME TO %s;", indexName,
						 shardIndexName);

		const char *commandString = renameCommand->data;

		Node *parseTree = ParseTreeNode(commandString);

		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * GetShardRelationIndexNames returns a list of index names "defined on" the
 * relation with relationOid explicitly by the CREATE INDEX command. That
 * means, all the constraints defined on the relation except:
 *  - primary indexes,
 *  - unique indexes and
 *  - exclusion indexes
 * that are applied by the related constraints.
 */
static List *
GetShardRelationIndexNames(Oid relationOid)
{
	List *indexNames = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOk = true;

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/* open system catalog and scan all indexes that belong to this table */
	Relation pgIndex = heap_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationOid);

	SysScanDesc scanDescriptor = systable_beginscan(pgIndex, IndexIndrelidIndexId,
													indexOk, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(heapTuple);

		if (indexForm->indisprimary || indexForm->indisunique ||
			indexForm->indisexclusion)
		{
			/*
			 * Skip the indexes that are not implied by explicitly executing
			 * a CREATE INDEX command.
			 */
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		Oid indexId = indexForm->indexrelid;

		char *indexName = get_rel_name(indexId);

		indexNames = lappend(indexNames, pstrdup(indexName));

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgIndex, AccessShareLock);

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return indexNames;
}


/*
 * CheckHashPartitionedTable looks up the partition information for the given
 * tableId and checks if the table is hash partitioned. If not, the function
 * throws an error.
 */
void
CheckHashPartitionedTable(Oid distributedTableId)
{
	char partitionType = PartitionMethod(distributedTableId);
	if (partitionType != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported table partition type: %c", partitionType)));
	}
}


/* Helper function to convert an integer value to a text type */
text *
IntegerToText(int32 value)
{
	StringInfo valueString = makeStringInfo();
	appendStringInfo(valueString, "%d", value);

	text *valueText = cstring_to_text(valueString->data);

	return valueText;
}
