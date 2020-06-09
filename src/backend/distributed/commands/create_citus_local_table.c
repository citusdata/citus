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
#include "distributed/create_citus_local_table.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/namespace_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static void ErrorIfUnsupportedCreateCitusLocalTable(Oid relationId);
static void ErrorIfUnsupportedCitusLocalTableKind(Oid relationId);
static void ErrorIfRelationIsAKnownShard(Oid relationId);
static void ErrorIfTableHasExternalForeignKeys(Oid relationId);
static void ErrorIfCoordinatorNotAddedAsWorkerNode(Oid relationId);
static uint64 ConvertLocalTableToShard(Oid relationId);
static void RenameRelationToShardRelation(Oid shellRelationId, uint64 shardId);
static void RenameShardRelationConstraints(Oid shardRelationId, uint64 shardId);
static List * GetConstraintNameList(Oid relationId);
static void RenameForeignConstraintsReferencingToShard(Oid shardRelationId,
													   uint64 shardId);
static char * GetRenameShardConstraintCommand(Oid relationId, char *constraintName,
											  uint64 shardId);
static void RenameShardRelationIndexes(Oid shardRelationId, uint64 shardId);
static char * GetRenameShardIndexCommand(char *indexName, uint64 shardId);
static List * GetExplicitIndexNameList(Oid relationId);
static void CreateCitusLocalTable(Oid relationId);
static void FinalizeCitusLocalTableCreation(Oid relationId);
static List * GetShellTableDDLEventsForCitusLocalTable(Oid relationId);
static void CreateShellTableForCitusLocalTable(List *shellTableDDLEvents);
static void DropAndMoveDefaultSequenceOwnerships(Oid sourceRelationId,
												 Oid targetRelationId);
static void ExtractColumnsOwningSequences(Oid relationId, List **columnNameList,
										  List **ownedSequenceIdList);
static void DropDefaultColumnDefinition(Oid relationId, char *columnName);
static void TransferSequenceOwnership(Oid ownedSequenceId, Oid targetRelationId,
									  char *columnName);
static void InsertMetadataForCitusLocalTable(Oid citusLocalTableId, uint64 shardId);


PG_FUNCTION_INFO_V1(create_citus_local_table);


/*
 * create_citus_local_table creates a citus table from the table with relationId.
 * by executing the internal method CreateCitusLocalTable.
 * (See CreateCitusLocalTable function's comment.)
 */
Datum
create_citus_local_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);

	CreateCitusLocalTable(relationId);

	PG_RETURN_VOID();
}


/*
 * ErrorIfUnsupportedCreateCitusLocalTable errors out if we cannot create the
 * citus local table from the relation with relationId.
 */
static void
ErrorIfUnsupportedCreateCitusLocalTable(Oid relationId)
{
	ErrorIfCoordinatorNotAddedAsWorkerNode(relationId);
	ErrorIfUnsupportedCitusLocalTableKind(relationId);
	EnsureTableNotDistributed(relationId);

	/*
	 * When creating other citus table types, we don't need to check that case as
	 * EnsureTableNotDistributed already errors out if the given relation implies
	 * a citus table. However, as we don't mark the relation as citus table, i.e we
	 * do not use the relation with relationId as the shell relation, parallel
	 * create_citus_local_table executions would not error out for that relation.
	 * Hence we need to error out for shard relations too.
	 */
	ErrorIfRelationIsAKnownShard(relationId);

	/*
	 * We do not allow creating citus local table if the table is involved in a
	 * foreign key relationship with "any other table". Note that we allow self
	 * references.
	 */
	ErrorIfTableHasExternalForeignKeys(relationId);
}


/*
 * ErrorIfUnsupportedCitusLocalTableKind errors out if the relation kind of
 * relation with relationId is not supported for citus local table creation.
 */
static void
ErrorIfUnsupportedCitusLocalTableKind(Oid relationId)
{
	const char *relationName = get_rel_name(relationId);

	if (IsChildTable(relationId) || IsParentTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create citus local table \"%s\", citus local "
							   "tables cannot be involved in inheritance relationships",
							   relationName)));
	}

	if (PartitionTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create citus local table \"%s\", citus local "
							   "tables cannot be partition of other tables",
							   relationName)));
	}

	char relationKind = get_rel_relkind(relationId);
	if (!(relationKind == RELKIND_RELATION || relationKind == RELKIND_FOREIGN_TABLE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create citus local table \"%s\", only regular "
							   "tables and foreign tables are supported for citus local "
							   "table creation", relationName)));
	}
}


/*
 * ErrorIfRelationIsAKnownShard errors out if the relation with relationId is
 * a shard relation relation.
 */
static void
ErrorIfRelationIsAKnownShard(Oid relationId)
{
	/* search the relation in all schemas */
	bool onlySearchPath = false;
	if (!RelationIsAKnownShard(relationId, onlySearchPath))
	{
		return;
	}

	const char *relationName = get_rel_name(relationId);

	ereport(ERROR, (errmsg("cannot create citus local table as \"%s\" is a shard "
						   "relation ", relationName)));
}


/*
 * ErrorIfTableHasExternalForeignKeys errors out if the relation with relationId
 * is involved in a foreign key relationship other than the self-referencing ones.
 */
static void
ErrorIfTableHasExternalForeignKeys(Oid relationId)
{
	int flags = (INCLUDE_REFERENCING_CONSTRAINTS | EXCLUDE_SELF_REFERENCES);
	List *foreignKeyIdsTableReferencing = GetForeignKeyOids(relationId, flags);

	flags = (INCLUDE_REFERENCED_CONSTRAINTS | EXCLUDE_SELF_REFERENCES);
	List *foreignKeyIdsTableReferenced = GetForeignKeyOids(relationId, flags);

	List *foreignKeysWithOtherTables = list_concat(foreignKeyIdsTableReferencing,
												   foreignKeyIdsTableReferenced);

	if (list_length(foreignKeysWithOtherTables) == 0)
	{
		return;
	}

	const char *relationName = get_rel_name(relationId);

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot create citus local table \"%s\", citus local "
						   "tables cannot be involved in foreign key relationships "
						   "with other tables initially", relationName),
					errhint("Drop foreign keys with other tables and re-define them "
							"with ALTER TABLE commands after creating the table.")));
}


/*
 * ErrorIfCoordinatorNotAddedAsWorkerNode error out if coordinator is not added
 * to metadata.
 */
static void
ErrorIfCoordinatorNotAddedAsWorkerNode(Oid relationId)
{
	if (CoordinatorAddedAsWorkerNode())
	{
		return;
	}

	const char *relationName = get_rel_name(relationId);

	ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("cannot create citus local table \"%s\", citus local "
						   "tables can only be created from coordinator node if "
						   "it is added as a worker node", relationName),
					errhint("First, add the coordinator with master_add_node "
							"command")));
}


/*
 * CreateCitusLocalTable is the internal method that creates a citus table
 * from the table with relationId. The created table would have the following
 * properties:
 *  - it will have only one shard,
 *  - its distribution method will be DISTRIBUTE_BY_NONE,
 *  - its replication model will be ReplicationModel,
 *  - its replication factor will be set to 1.
 * Similar to reference tables, it has only 1 placement. In addition to that, that
 * single placement is only allowed to be on the coordinator.
 */
static void
CreateCitusLocalTable(Oid relationId)
{
	/* these checks should be done before acquiring the lock on the table */
	EnsureCoordinator();
	EnsureTableOwner(relationId);

	/*
	 * Lock target relation with an AccessExclusiveLock as we don't want
	 * multiple backends manipulating this relation. We could actually simply
	 * lock the relation without opening it. However, we want postgres to
	 * natively error out if the relation does not exist or dropped by another
	 * backend.
	 */
	Relation relation = relation_open(relationId, AccessExclusiveLock);

	/*
	 * We immediately close relation with NoLock right after opening it. This is
	 * because, in this function, we may execute ALTER TABLE commands modifying
	 * relation's column definitions and postgres does not allow us to do so when
	 * the table is still open. (See the postgres function CheckTableNotInUse for
	 * more information)
	 */
	relation_close(relation, NoLock);

	ErrorIfUnsupportedCreateCitusLocalTable(relationId);

	ObjectAddress tableAddress = { 0 };
	ObjectAddressSet(tableAddress, RelationRelationId, relationId);

	/*
	 * Ensure dependencies first as we will create shell table on the other nodes
	 * in the MX case.
	 */
	EnsureDependenciesExistOnAllNodes(&tableAddress);

	/*
	 * Make sure that existing reference tables have been replicated to all
	 * the nodes such that we can create foreign keys and joins work
	 * immediately after creation.
	 */
	EnsureReferenceTablesExistOnAllNodes();

	List *shellTableDDLEvents = GetShellTableDDLEventsForCitusLocalTable(relationId);

	char *relationName = get_rel_name(relationId);
	Oid relationSchemaId = get_rel_namespace(relationId);

	/* below we convert relation with relationId to the shard relation */
	uint64 shardId = ConvertLocalTableToShard(relationId);

	/*
	 * As we retrieved the DDL commands necessary to create the shell table
	 * from scratch, below we simply recreate the shell table executing them
	 * via process utility.
	 */
	CreateShellTableForCitusLocalTable(shellTableDDLEvents);

	/*
	 * Set shellRelationId as the relation with relationId now points
	 * to the shard relation.
	 */
	Oid shellRelationId = get_relname_relid(relationName, relationSchemaId);

	/* assert that we created the shell table properly in the same schema */
	Assert(OidIsValid(shellRelationId));

	Oid shardRelationId = relationId;
	DropAndMoveDefaultSequenceOwnerships(shardRelationId, shellRelationId);

	InsertMetadataForCitusLocalTable(shellRelationId, shardId);

	FinalizeCitusLocalTableCreation(shellRelationId);
}


/*
 * FinalizeCitusLocalTableCreation performs completes creation of the citus
 * local table with relationId by performing operations that should be done
 * after creating the shard and inserting the metadata.
 */
static void
FinalizeCitusLocalTableCreation(Oid relationId)
{
	/*
	 * If it is a foreign table, then skip creating citus truncate trigger
	 * as foreign tables do not support truncate triggers.
	 */
	if (RegularTable(relationId))
	{
		CreateTruncateTrigger(relationId);
	}

	if (ShouldSyncTableMetadata(relationId))
	{
		CreateTableMetadataOnWorkers(relationId);
	}

	/*
	 * We've a custom way of foreign key graph invalidation,
	 * see InvalidateForeignKeyGraph().
	 */
	if (TableReferenced(relationId) || TableReferencing(relationId))
	{
		InvalidateForeignKeyGraph();
	}
}


/*
 * GetShellTableDDLEventsForCitusLocalTable returns a list of DDL commands
 * to create the shell table from scratch.
 */
static List *
GetShellTableDDLEventsForCitusLocalTable(Oid relationId)
{
	/*
	 * As we don't allow foreign keys with other tables initially, below we
	 * only pick self-referencing foreign keys.
	 */
	List *foreignConstraintCommands =
		GetReferencingForeignConstaintCommands(relationId);

	/*
	 * Include DEFAULT clauses for columns getting their default values from
	 * a sequence.
	 */
	bool includeSequenceDefaults = true;

	List *shellTableDDLEvents = GetTableDDLEvents(relationId,
												  includeSequenceDefaults);
	shellTableDDLEvents = list_concat(shellTableDDLEvents, foreignConstraintCommands);

	return shellTableDDLEvents;
}


/*
 * ConvertLocalTableToShard first acquires a shardId and then converts the
 * given relation with relationId to the shard relation with shardId. That
 * means, this function suffixes shardId to:
 *  - relation name,
 *  - all the objects "defined on" the relation and
 *  - the foreign keys referencing to the relation.
 * After converting the given relation, returns the acquired shardId.
 */
static uint64
ConvertLocalTableToShard(Oid relationId)
{
	uint64 shardId = GetNextShardId();

	RenameRelationToShardRelation(relationId, shardId);
	RenameShardRelationConstraints(relationId, shardId);
	RenameForeignConstraintsReferencingToShard(relationId, shardId);
	RenameShardRelationIndexes(relationId, shardId);

	return shardId;
}


/*
 * RenameRelationToShardRelation appends given shardId to the end of the name
 * of relation with shellRelationId.
 */
static void
RenameRelationToShardRelation(Oid shellRelationId, uint64 shardId)
{
	char *qualifiedShellRelationName = generate_qualified_relation_name(shellRelationId);

	char *shellRelationName = get_rel_name(shellRelationId);
	char *shardRelationName = pstrdup(shellRelationName);
	AppendShardIdToName(&shardRelationName, shardId);
	const char *quotedShardRelationName = quote_identifier(shardRelationName);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER TABLE %s RENAME TO %s;",
					 qualifiedShellRelationName, quotedShardRelationName);

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
	List *constraintNameList = GetConstraintNameList(shardRelationId);

	char *constraintName = NULL;
	foreach_ptr(constraintName, constraintNameList)
	{
		const char *commandString =
			GetRenameShardConstraintCommand(shardRelationId, constraintName, shardId);

		Node *parseTree = ParseTreeNode(commandString);
		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * GetConstraintNameList returns a list of constraint names "defined on" the
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

		Oid referencingTableId = foreignConstraintForm->conrelid;
		char *constraintName = NameStr(foreignConstraintForm->conname);

		const char *commandString =
			GetRenameShardConstraintCommand(referencingTableId, constraintName, shardId);

		Node *parseTree = ParseTreeNode(commandString);
		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);

		ReleaseSysCache(heapTuple);
	}
}


/*
 * GetRenameShardConstraintCommand returns DDL command to append given
 * shardId to the constrant with constraintName on the relation with
 * relationId.
 */
static char *
GetRenameShardConstraintCommand(Oid relationId, char *constraintName, uint64 shardId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	char *shardConstraintName = pstrdup(constraintName);
	AppendShardIdToName(&shardConstraintName, shardId);
	const char *quotedShardConstraintName = quote_identifier(shardConstraintName);

	const char *quotedConstraintName = quote_identifier(constraintName);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER TABLE %s RENAME CONSTRAINT %s TO %s;",
					 qualifiedRelationName, quotedConstraintName,
					 quotedShardConstraintName);

	return renameCommand->data;
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
		const char *commandString = GetRenameShardIndexCommand(indexName, shardId);

		Node *parseTree = ParseTreeNode(commandString);
		CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * GetRenameShardIndexCommand returns DDL command to append given shardId to
 * the index with indexName.
 */
static char *
GetRenameShardIndexCommand(char *indexName, uint64 shardId)
{
	char *shardIndexName = pstrdup(indexName);
	AppendShardIdToName(&shardIndexName, shardId);
	const char *quotedShardIndexName = quote_identifier(shardIndexName);

	const char *quotedIndexName = quote_identifier(indexName);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER INDEX %s RENAME TO %s;",
					 quotedIndexName, quotedShardIndexName);

	return renameCommand->data;
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
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	PushOverrideEmptySearchPath(CurrentMemoryContext);

	Relation pgIndex = heap_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgIndex, IndexIndrelidIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	List *indexNameList = NIL;

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(heapTuple);

		Oid indexId = indexForm->indexrelid;

		bool indexImpliedByConstraint = IndexImpliedByAConstraint(indexForm);

		/*
		 * Skip the indexes that are not implied by explicitly executing
		 * a CREATE INDEX command.
		 */
		if (!indexImpliedByConstraint)
		{
			char *indexName = get_rel_name(indexId);

			indexNameList = lappend(indexNameList, pstrdup(indexName));
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgIndex, NoLock);

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
		Node *parseTree = ParseTreeNode(ddlCommand);

		CitusProcessUtility(parseTree, ddlCommand, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}
}


/*
 * DropAndMoveDefaultSequenceOwnerships finds each column of relation with
 * sourceRelationId defaulting to an owned sequence. Then, drops default
 * definitions for those columns and grants ownership of each of the owned
 * sequences to the same named column of the relation with targetRelationId.
 */
static void
DropAndMoveDefaultSequenceOwnerships(Oid sourceRelationId, Oid targetRelationId)
{
	List *columnNameList = NIL;
	List *ownedSequenceIdList = NIL;
	ExtractColumnsOwningSequences(sourceRelationId, &columnNameList,
								  &ownedSequenceIdList);

	Assert(list_length(columnNameList) == list_length(ownedSequenceIdList));

	ListCell *columnNameCell = NULL;
	ListCell *ownedSequenceIdCell = NULL;
	forboth(columnNameCell, columnNameList, ownedSequenceIdCell, ownedSequenceIdList)
	{
		char *columnName = (char *) lfirst(columnNameCell);
		Oid ownedSequenceId = lfirst_oid(ownedSequenceIdCell);

		DropDefaultColumnDefinition(sourceRelationId, columnName);
		TransferSequenceOwnership(ownedSequenceId, targetRelationId, columnName);
	}
}


/*
 * ExtractColumnsOwningSequences finds each column of relation with relationId
 * defaulting to an owned sequence. Then, appends the column name and id of the
 * owned sequence -that the column defaults- to the lists passed as NIL initially.
 */
static void
ExtractColumnsOwningSequences(Oid relationId, List **columnNameList,
							  List **ownedSequenceIdList)
{
	Assert(*columnNameList == NIL && *ownedSequenceIdList == NIL);

	Relation relation = relation_open(relationId, AccessExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	/* iterate on columns that are not dropped and have DEFAULT definitions */
	AttrNumber defaultValueIndex = 0;

	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);
		if (attributeForm->attisdropped || !attributeForm->atthasdef)
		{
			/*
			 * If this column has already been dropped or it has no DEFAULT
			 * definition, skip it.
			 */
			continue;
		}

		List *columnOwnedSequences = getOwnedSequences(relationId, defaultValueIndex++);
		if (list_length(columnOwnedSequences) == 0)
		{
			/*
			 * Skip if the column does not own any sequences. In that case, column
			 * either does not take the default value from a sequence or it does
			 * not own the sequence, i.e, sequence is created and attached to the
			 * column explicitly by the user.
			 */
			continue;
		}

		/* TODO: this assumption should be validated */
		Assert(list_length(columnOwnedSequences) == 1);

		Oid ownedSequenceId = linitial_oid(columnOwnedSequences);
		*ownedSequenceIdList = lappend_oid(*ownedSequenceIdList, ownedSequenceId);

		char *columnName = NameStr(attributeForm->attname);
		*columnNameList = lappend(*columnNameList, columnName);
	}

	relation_close(relation, NoLock);
}


/*
 * DropDefaultColumnDefinition drops the DEFAULT definiton of the column with
 * columnName of the relation with relationId via process utility.
 */
static void
DropDefaultColumnDefinition(Oid relationId, char *columnName)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	const char *quotedColumnName = quote_identifier(columnName);

	StringInfo sequenceDropCommand = makeStringInfo();
	appendStringInfo(sequenceDropCommand,
					 "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
					 qualifiedRelationName, quotedColumnName);

	const char *commandString = sequenceDropCommand->data;

	Node *parseTree = ParseTreeNode(commandString);
	CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
						NULL, None_Receiver, NULL);
}


/*
 * TransferSequenceOwnership grants ownership of the sequence with sequenceId
 * to the column with targetColumnName of relation with targetRelationId via
 * process utility. Note that this function assumes that the target relation
 * has a column with targetColumnName which can default to the given sequence.
 */
static void
TransferSequenceOwnership(Oid sequenceId, Oid targetRelationId, char *targetColumnName)
{
	char *qualifiedSequenceName = generate_qualified_relation_name(sequenceId);
	char *qualifiedTargetRelationName =
		generate_qualified_relation_name(targetRelationId);
	const char *quotedTargetColumnName = quote_identifier(targetColumnName);

	StringInfo sequenceOwnershipCommand = makeStringInfo();
	appendStringInfo(sequenceOwnershipCommand, "ALTER SEQUENCE %s OWNED BY %s.%s",
					 qualifiedSequenceName, qualifiedTargetRelationName,
					 quotedTargetColumnName);

	const char *commandString = sequenceOwnershipCommand->data;

	Node *parseTree = ParseTreeNode(commandString);
	CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
						NULL, None_Receiver, NULL);
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
 * IsCitusLocalTable returns whether the given relationId identifies a citus
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
