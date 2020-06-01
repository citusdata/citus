/*-------------------------------------------------------------------------
 *
 * create_citus_local_table.c
 *
 * This file contains functions to create citus local tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/pg_constraint.h"
#include "distributed/citus_local_table_utils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static void ErrorIfUnsupportedCreateCitusLocalTable(Oid relationId);
static void CreateCitusLocalTableShard(Oid relationId, uint64 shardId);
static void RenameRelationToShardRelation(Oid shellRelationId, uint64 shardId);
static void RenameShardRelationConstraints(Oid shardRelationId, uint64 shardId);
static List * GetConstraintNameList(Oid relationId);
static void RenameForeignConstraintsReferencingToShard(Oid shardRelationId,
													   uint64 shardId);
static void RenameShardRelationIndexes(Oid shardRelationId, uint64 shardId);
static List * GetExplicitIndexNameList(Oid relationId);
static void CreateCitusLocalTable(Oid shellRelationId);
static void CreateShellTableForCitusLocalTable(List *shellTableDDLEvents);
static void InsertMetadataForCitusLocalTable(Oid citusLocalTableId, uint64 shardId);

PG_FUNCTION_INFO_V1(create_citus_local_table);

/*
 * create_citus_local_table creates a citus table from the table with relationId.
 * The created table would have the following properties:
 *  - it will have only one shard,
 *  - its distribution method will be DISTRIBUTE_BY_NONE,
 *  - its replication model will be ReplicationModel,
 *  - its replication factor will be set to 1.
 * Similar to reference tables, it has only 1 placement. In addition to that, that
 * single placement is only allowed to be on the coordinator.
 */
Datum
create_citus_local_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	Relation relation = try_relation_open(relationId, ExclusiveLock);

	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("could not create citus local table: "
							   "relation does not exist")));
	}

	ErrorIfUnsupportedCreateCitusLocalTable(relationId);

	ObjectAddress tableAddress = { 0 };
	ObjectAddressSet(tableAddress, RelationRelationId, relationId);

	/*
	 * Ensure dependencies first as we will create shell table on the other nodes
	 * in the MX case.
	 */
	EnsureDependenciesExistOnAllNodes(&tableAddress);

	CreateCitusLocalTable(relationId);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * ErrorIfUnsupportedCreateCitusLocalTable errors out if we cannot create the
 * citus local table from the relation relationId.
 */
static void
ErrorIfUnsupportedCreateCitusLocalTable(Oid relationId)
{
	/* citus local tables can only be created from coordinator for now */
	EnsureCoordinator();

	EnsureTableOwner(relationId);

	/* we allow creating citus local tables only from relations with RELKIND_RELATION */
	EnsureRelationKindSupported(relationId);

	if (PartitionTable(relationId) || PartitionedTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("citus local tables can not be involved in a "
							   "partition relationship")));
	}

	EnsureTableNotDistributed(relationId);

	if (!CoordinatorAddedAsWorkerNode())
	{
		const char *relationName = get_rel_name(relationId);

		Assert(relationName != NULL);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"cannot create citus local table \"%s\", citus local "
							"tables can only be created from coordinator node if "
							"it is added to pg_dist_node", relationName)));
	}
}


/*
 * CreateCitusLocalTable is the internal method to create citus local table's
 * shard and metadata.
 * Note that this function does not perform any validation on the table as it
 * is already done in create_citus_local_table.
 */
static void
CreateCitusLocalTable(Oid shellRelationId)
{
	/*
	 * Make sure that existing reference tables have been replicated to all
	 * the nodes such that we can create foreign keys and joins work
	 * immediately after creation.
	 */
	EnsureReferenceTablesExistOnAllNodes();

	/*
	 * Get an exclusive lock on relation with shellRelationId as the operations
	 * done in this function should be automic.
	 */
	LockRelationOid(shellRelationId, ExclusiveLock);

	/*
	 * Get necessary commands to recreate the shell table before renaming the
	 * given relation to the shard relation.
	 */
	List *foreignConstraintCommands =
		GetForeignConstraintCommandsTableInvolved(shellRelationId);

	/*
	 * Include DEFAULT clauses for columns getting their default values from
	 * a sequence.
	 */
	bool includeSequenceDefaults = true;

	List *shellTableDDLEvents = GetTableDDLEvents(shellRelationId,
												  includeSequenceDefaults);
	shellTableDDLEvents = list_concat(shellTableDDLEvents, foreignConstraintCommands);

	uint64 shardId = GetNextShardId();

	char *shellRelationName = get_rel_name(shellRelationId);
	Oid shellRelationSchemaId = get_rel_namespace(shellRelationId);

	/* below we convert relation with shellRelationId to the shard relation */
	CreateCitusLocalTableShard(shellRelationId, shardId);

	/*
	 * As we retrieved the DDL commands necessary to create the shell table
	 * from scratch, below we simply recreate the shell table executing them
	 * via process utility.
	 */
	CreateShellTableForCitusLocalTable(shellTableDDLEvents);

	/* update shellRelationId so it points to the shell table that we just created */
	shellRelationId = get_relname_relid(shellRelationName, shellRelationSchemaId);

	/* assert that we created the shell table properly in the same schema */
	Assert(OidIsValid(shellRelationId));

	InsertMetadataForCitusLocalTable(shellRelationId, shardId);

	/* foreign tables does not support TRUNCATE trigger */
	if (RegularTable(shellRelationId))
	{
		CreateTruncateTrigger(shellRelationId);
	}

	if (ShouldSyncTableMetadata(shellRelationId))
	{
		CreateTableMetadataOnWorkers(shellRelationId);
	}

	/*
	 * We've a custom way of foreign key graph invalidation,
	 * see InvalidateForeignKeyGraph().
	 */
	if (TableReferenced(shellRelationId) || TableReferencing(shellRelationId))
	{
		InvalidateForeignKeyGraph();
	}
}


/*
 * CreateCitusLocalTableShard creates the one and only shard of the citus
 * local table lazily. That means, this function suffixes shardId to:
 *  - relation name,
 *  - all the objects "defined on" the relation and
 *  - the foreign keys referencing to the relation.
 */
static void
CreateCitusLocalTableShard(Oid relationId, uint64 shardId)
{
	RenameRelationToShardRelation(relationId, shardId);
	RenameShardRelationConstraints(relationId, shardId);
	RenameForeignConstraintsReferencingToShard(relationId, shardId);
	RenameShardRelationIndexes(relationId, shardId);
}


/*
 * RenameRelationToShardRelation appends given shardId to the end of the name
 * of relation with shellRelationId.
 */
static void
RenameRelationToShardRelation(Oid shellRelationId, uint64 shardId)
{
	Oid shellRelationSchemaId = get_rel_namespace(shellRelationId);
	char *shellRelationSchemaName = get_namespace_name(shellRelationSchemaId);
	char *shellRelationName = get_rel_name(shellRelationId);
	char *qualifiedShellRelationName = quote_qualified_identifier(shellRelationSchemaName,
																  shellRelationName);

	char *shardRelationName = pstrdup(shellRelationName);
	AppendShardIdToName(&shardRelationName, shardId);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER TABLE %s RENAME TO %s;",
					 qualifiedShellRelationName, shardRelationName);

	const char *commandString = renameCommand->data;

	Node *parseTree = ParseTreeNode(commandString);

	CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
						NULL, None_Receiver, NULL);
}


/*
 * RenameShardRelationConstraints appends given shardId to the end of the name
 * of constraints "defined on" the relation with shardRelationId. This function
 * utilizes GetConstraintNameList to pick the constraints to be renamed,
 * see more details in function's comment.
 */
static void
RenameShardRelationConstraints(Oid shardRelationId, uint64 shardId)
{
	Oid shardRelationSchemaId = get_rel_namespace(shardRelationId);
	char *shardRelationSchemaName = get_namespace_name(shardRelationSchemaId);
	char *shardRelationName = get_rel_name(shardRelationId);
	char *qualifiedShardRelationName =
		quote_qualified_identifier(shardRelationSchemaName, shardRelationName);

	List *constraintNameList = GetConstraintNameList(shardRelationId);

	char *constraintName = NULL;
	foreach_ptr(constraintName, constraintNameList)
	{
		char *shardConstraintName = pstrdup(constraintName);
		AppendShardIdToName(&shardConstraintName, shardId);

		StringInfo renameCommand = makeStringInfo();
		appendStringInfo(renameCommand, "ALTER TABLE %s RENAME CONSTRAINT %s TO %s;",
						 qualifiedShardRelationName, constraintName, shardConstraintName);

		const char *commandString = renameCommand->data;

		Node *parseTree = ParseTreeNode(commandString);

		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * GetConstraintNameList returns a list constraint names "defined on" the
 * relation with relationId. Those constraints can be:
 *  - "check" constraints or,
 *  - "primary key" constraints or,
 *  - "unique" constraints or,
 *  - "trigger" constraints or,
 *  - "exclusion" constraints or,
 *  - "foreign key" constraints in which the relation is the "referencing"
 *     relation (including the self-referencing foreign keys).
 */
static List *
GetConstraintNameList(Oid relationId)
{
	List *constraintNameList = NIL;

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													ConstraintRelidTypidNameIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		char *constraintName = NameStr(constraintForm->conname);
		constraintNameList = lappend(constraintNameList, pstrdup(constraintName));

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return constraintNameList;
}


/*
 * RenameForeignConstraintsReferencingToShard appends given shardId to the end
 * of the name of foreign key constraints in which the relation with
 * shardRelationId is the "referenced one" except the self-referencing foreign
 * keys. This is because, we already renamed self-referencing foreign keys via
 * RenameShardRelationConstraints function.
 */
static void
RenameForeignConstraintsReferencingToShard(Oid shardRelationId, uint64 shardId)
{
	int flags = (INCLUDE_REFERENCED_CONSTRAINTS | EXCLUDE_SELF_REFERENCES);
	List *foreignKeyIdsTableReferenced = GetForeignKeyOids(shardRelationId, flags);

	Oid foreignKeyId = InvalidOid;
	foreach_oid(foreignKeyId, foreignKeyIdsTableReferenced)
	{
		HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyId));

		Assert(HeapTupleIsValid(heapTuple));

		Form_pg_constraint foreignConstraintForm =
			(Form_pg_constraint) GETSTRUCT(heapTuple);

		Oid referencedTableId = foreignConstraintForm->conrelid;
		Oid referencedTableSchemaId = get_rel_namespace(referencedTableId);
		char *referencedTableSchemaName = get_namespace_name(referencedTableSchemaId);
		char *referencedRelationName = get_rel_name(referencedTableId);
		char *referencedRelationQualifiedName =
			quote_qualified_identifier(referencedTableSchemaName, referencedRelationName);

		char *constraintName = NameStr(foreignConstraintForm->conname);

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

		ReleaseSysCache(heapTuple);
	}
}


/*
 * RenameShardRelationIndexes appends given shardId to the end of the names
 * of shard relation indexes except the ones that are already renamed via
 * RenameShardRelationConstraints. This function utilizes
 * GetExplicitIndexNameList to pick the indexes to be renamed, see more
 * details in function's comment.
 */
static void
RenameShardRelationIndexes(Oid shardRelationId, uint64 shardId)
{
	List *indexNameList = GetExplicitIndexNameList(shardRelationId);

	char *indexName = NULL;
	foreach_ptr(indexName, indexNameList)
	{
		char *shardIndexName = pstrdup(indexName);
		AppendShardIdToName(&shardIndexName, shardId);

		StringInfo renameCommand = makeStringInfo();
		appendStringInfo(renameCommand, "ALTER INDEX %s RENAME TO %s;",
						 indexName, shardIndexName);

		const char *commandString = renameCommand->data;

		Node *parseTree = ParseTreeNode(commandString);

		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * GetExplicitIndexNameList returns a list of index names defined "explicitly"
 * on the relation with relationId by the "CREATE INDEX" commands. That means,
 * all the constraints defined on the relation except:
 *  - primary indexes,
 *  - unique indexes and
 *  - exclusion indexes
 * that are actually applied by the related constraints.
 */
static List *
GetExplicitIndexNameList(Oid relationId)
{
	List *indexNameList = NIL;

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	Relation pgIndex = heap_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgIndex, IndexIndrelidIndexId,
													useIndex, NULL, scanKeyCount,
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

		indexNameList = lappend(indexNameList, pstrdup(indexName));

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgIndex, AccessShareLock);

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return indexNameList;
}


/*
 * CreateShellTableForCitusLocalTable creates the shell table for the citus
 * local tables by executing the given commands necessary to recreate the
 * table.
 */
static void
CreateShellTableForCitusLocalTable(List *shellTableDDLEvents)
{
	Assert(list_length(shellTableDDLEvents) > 0);

	char *ddlCommand = NULL;
	foreach_ptr(ddlCommand, shellTableDDLEvents)
	{
		StringInfo semicolonEndedCommand = makeStringInfo();
		appendStringInfo(semicolonEndedCommand, "%s;", ddlCommand);

		const char *commandString = semicolonEndedCommand->data;

		Node *parseTree = ParseTreeNode(commandString);

		/*
		 * If the command defines a constraint, initially do not validate it
		 * as shell table has no data in it. Note that this is only needed
		 * before inserting metadata for citus local table. This is because,
		 * after that, we already won't process constraints on the shell table.
		 */
		if (IsA(parseTree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = castNode(AlterTableStmt, parseTree);

			/* we create ALTER TABLE commands so they only have one subcommand */
			Assert(list_length(alterTableStmt->cmds) == 1);
			AlterTableCmd *alterTableCmd =
				castNode(AlterTableCmd, linitial(alterTableStmt->cmds));

			if (alterTableCmd->subtype == AT_AddConstraint)
			{
				Constraint *constraint = (Constraint *) alterTableCmd->def;
				constraint->skip_validation = true;
			}
		}

		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * InsertMetadataForCitusLocalTable inserts necessary metadata for the citus
 * local table to the following metadata tables:
 * pg_dist_partition, pg_dist_shard & pg_dist_placement.
 */
static void
InsertMetadataForCitusLocalTable(Oid citusLocalTableId, uint64 shardId)
{
	Assert(OidIsValid(citusLocalTableId));
	Assert(shardId != INVALID_SHARD_ID);

	char distributionMethod = DISTRIBUTE_BY_NONE;
	char replicationModel = ReplicationModel;

	Assert(replicationModel != REPLICATION_MODEL_2PC);

	uint32 colocationId = INVALID_COLOCATION_ID;
	Var *distributionColumn = NULL;
	InsertIntoPgDistPartition(citusLocalTableId, distributionMethod,
							  distributionColumn, colocationId,
							  replicationModel);

	/* set shard storage type according to relation type */
	char shardStorageType = ShardStorageType(citusLocalTableId);

	text *shardMinValue = NULL;
	text *shardMaxValue = NULL;
	InsertShardRow(citusLocalTableId, shardId, shardStorageType,
				   shardMinValue, shardMaxValue);

	List *nodeList = list_make1(CoordinatorNode());

	int replicationFactor = 1;
	int workerStartIndex = 0;
	InsertShardPlacementRows(citusLocalTableId, shardId, nodeList,
							 workerStartIndex, replicationFactor);
}


/*
 * IscitusLocalTable returns whether the given relationId identifies a citus
 * local table.
 */
bool
IsCitusLocalTable(Oid relationId)
{
	CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(relationId);

	if (!tableEntry->isCitusTable)
	{
		return false;
	}

	if (tableEntry->partitionMethod != DISTRIBUTE_BY_NONE)
	{
		return false;
	}

	if (tableEntry->replicationModel == REPLICATION_MODEL_2PC)
	{
		return false;
	}

	return true;
}
