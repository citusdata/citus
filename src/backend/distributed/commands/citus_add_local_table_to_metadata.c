/*-------------------------------------------------------------------------
 *
 * citus_add_local_table_to_metadata.c
 *
 * This file contains functions to add local table to citus metadata.
 *
 * A local table added to metadata is composed of a shell relation to wrap the
 * the regular postgres relation as its coordinator local shard.
 *
 * As we want to hide "citus local table" concept from users, we renamed
 * udf and re-branded that concept as "local tables added to metadata".
 * Note that we might still call this local table concept as "citus local" in
 * many places of the code base.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_trigger.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/sequence.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/dependency.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/namespace_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"


/*
 * Global variable for the GUC citus.use_citus_managed_tables.
 * This is used after every CREATE TABLE statement in utility_hook.c
 * If this variable is set to true, we add all created tables to metadata.
 */
bool AddAllLocalTablesToMetadata = true;

static void citus_add_local_table_to_metadata_internal(Oid relationId,
													   bool cascadeViaForeignKeys);
static void ErrorIfAddingPartitionTableToMetadata(Oid relationId);
static void ErrorIfUnsupportedCreateCitusLocalTable(Relation relation);
static void ErrorIfUnsupportedCitusLocalTableKind(Oid relationId);
static void ErrorIfUnsupportedCitusLocalColumnDefinition(Relation relation);
static void NoticeIfAutoConvertingLocalTables(bool autoConverted, Oid relationId);
static CascadeOperationType GetCascadeTypeForCitusLocalTables(bool autoConverted);
static List * GetShellTableDDLEventsForCitusLocalTable(Oid relationId);
static uint64 ConvertLocalTableToShard(Oid relationId);
static void RenameRelationToShardRelation(Oid shellRelationId, uint64 shardId);
static void RenameShardRelationConstraints(Oid shardRelationId, uint64 shardId);
static List * GetConstraintNameList(Oid relationId);
static char * GetRenameShardConstraintCommand(Oid relationId, char *constraintName,
											  uint64 shardId);
static void RenameShardRelationIndexes(Oid shardRelationId, uint64 shardId);
static void RenameShardRelationStatistics(Oid shardRelationId, uint64 shardId);
static char * GetDropTriggerCommand(Oid relationId, char *triggerName);
static char * GetRenameShardIndexCommand(Oid indexOid, uint64 shardId);
static char * GetRenameShardStatsCommand(char *statSchema, char *statsName,
										 char *statsNameWithShardId);
static void RenameShardRelationNonTruncateTriggers(Oid shardRelationId, uint64 shardId);
static char * GetRenameShardTriggerCommand(Oid shardRelationId, char *triggerName,
										   uint64 shardId);
static void DropRelationTruncateTriggers(Oid relationId);
static char * GetDropTriggerCommand(Oid relationId, char *triggerName);
static void DropViewsOnTable(Oid relationId);
static List * GetRenameStatsCommandList(List *statsOidList, uint64 shardId);
static List * ReversedOidList(List *oidList);
static void AppendExplicitIndexIdsToList(Form_pg_index indexForm,
										 List **explicitIndexIdList,
										 int flags);
static void DropDefaultExpressionsAndMoveOwnedSequenceOwnerships(Oid sourceRelationId,
																 Oid targetRelationId);
static void DropDefaultColumnDefinition(Oid relationId, char *columnName);
static void TransferSequenceOwnership(Oid ownedSequenceId, Oid targetRelationId,
									  char *columnName);
static void InsertMetadataForCitusLocalTable(Oid citusLocalTableId, uint64 shardId,
											 bool autoConverted);
static void FinalizeCitusLocalTableCreation(Oid relationId);


PG_FUNCTION_INFO_V1(citus_add_local_table_to_metadata);
PG_FUNCTION_INFO_V1(create_citus_local_table);
PG_FUNCTION_INFO_V1(remove_local_tables_from_metadata);


/*
 * citus_add_local_table_to_metadata creates a citus local table from the table with
 * relationId by executing the internal method CreateCitusLocalTable.
 * (See CreateCitusLocalTable function's comment.)
 */
Datum
citus_add_local_table_to_metadata(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	bool cascadeViaForeignKeys = PG_GETARG_BOOL(1);

	citus_add_local_table_to_metadata_internal(relationId, cascadeViaForeignKeys);

	PG_RETURN_VOID();
}


/*
 * citus_add_local_table_to_metadata_internal is the internal method for
 * citus_add_local_table_to_metadata udf.
 */
static void
citus_add_local_table_to_metadata_internal(Oid relationId, bool cascadeViaForeignKeys)
{
	CheckCitusVersion(ERROR);

	bool autoConverted = false;
	CreateCitusLocalTable(relationId, cascadeViaForeignKeys, autoConverted);
}


/*
 * create_citus_local_table is a wrapper function for old name of
 * of citus_add_local_table_to_metadata.
 *
 * The only reason for having this udf in citus binary is to make
 * multi_extension test happy as it uses this udf when testing the
 * downgrade scenario from 9.5 to 9.4.
 */
Datum
create_citus_local_table(PG_FUNCTION_ARGS)
{
	ereport(NOTICE, (errmsg("create_citus_local_table is deprecated in favour of "
							"citus_add_local_table_to_metadata")));

	Oid relationId = PG_GETARG_OID(0);

	/*
	 * create_citus_local_table doesn't have cascadeViaForeignKeys option,
	 * so we can't directly call citus_add_local_table_to_metadata udf itself
	 * since create_citus_local_table doesn't specify cascadeViaForeignKeys.
	 */
	bool cascadeViaForeignKeys = false;
	citus_add_local_table_to_metadata_internal(relationId, cascadeViaForeignKeys);

	PG_RETURN_VOID();
}


/*
 * remove_local_tables_from_metadata undistributes citus local
 * tables that are not chained with any reference tables via foreign keys.
 */
Datum
remove_local_tables_from_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	UndistributeDisconnectedCitusLocalTables();

	PG_RETURN_VOID();
}


/*
 * CreateCitusLocalTable is the internal method that creates a citus table
 * from the table with relationId. The created table would have the following
 * properties:
 *  - it will have only one shard,
 *  - its distribution method will be DISTRIBUTE_BY_NONE,
 *  - its replication model will be REPLICATION_MODEL_STREAMING,
 *  - its replication factor will be set to 1.
 * Similar to reference tables, it has only 1 placement. In addition to that, that
 * single placement is only allowed to be on the coordinator.
 */
void
CreateCitusLocalTable(Oid relationId, bool cascadeViaForeignKeys, bool autoConverted)
{
	/*
	 * These checks should be done before acquiring any locks on relation.
	 * This is because we don't allow creating citus local tables in worker
	 * nodes and we don't want to acquire any locks on a table if we are not
	 * the owner of it.
	 */
	EnsureCoordinator();
	EnsureTableOwner(relationId);

	/* enable citus_add_local_table_to_metadata on an empty node */
	InsertCoordinatorIfClusterEmpty();

	/*
	 * Creating Citus local tables relies on functions that accesses
	 * shards locally (e.g., ExecuteAndLogUtilityCommand()). As long as
	 * we don't teach those functions to access shards remotely, we
	 * cannot relax this check.
	 */
	SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);

	if (!autoConverted && IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		/*
		 * We allow users to mark local tables already added to metadata
		 * as "autoConverted = false".
		 * If the user called citus_add_local_table_to_metadata for a table that is
		 * already added to metadata, we should mark this one and connected relations
		 * as auto-converted = false.
		 */
		UpdateAutoConvertedForConnectedRelations(list_make1_oid(relationId),
												 autoConverted);

		return;
	}

	/*
	 * Lock target relation with an AccessExclusiveLock as we don't want
	 * multiple backends manipulating this relation. We could actually simply
	 * lock the relation without opening it. However, we also want to check
	 * if the relation does not exist or dropped by another backend. Also,
	 * we open the relation with try_relation_open instead of relation_open
	 * to give a nice error in case the table is dropped by another backend.
	 */
	LOCKMODE lockMode = AccessExclusiveLock;
	Relation relation = try_relation_open(relationId, lockMode);

	ErrorIfUnsupportedCreateCitusLocalTable(relation);

	ErrorIfAddingPartitionTableToMetadata(relationId);

	/*
	 * We immediately close relation with NoLock right after opening it. This is
	 * because, in this function, we may execute ALTER TABLE commands modifying
	 * relation's column definitions and postgres does not allow us to do so when
	 * the table is still open. (See the postgres function CheckTableNotInUse for
	 * more information)
	 */
	relation_close(relation, NoLock);

	NoticeIfAutoConvertingLocalTables(autoConverted, relationId);

	if (TableHasExternalForeignKeys(relationId))
	{
		if (!cascadeViaForeignKeys)
		{
			/*
			 * We do not allow creating citus local table if the table is involved in a
			 * foreign key relationship with "any other table", unless the option
			 * cascadeViaForeignKeys is given true.
			 * Note that we allow self references.
			 */
			char *qualifiedRelationName = generate_qualified_relation_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("relation %s is involved in a foreign key "
								   "relationship with another table",
								   qualifiedRelationName),
							errhint("Use cascade_via_foreign_keys option to add "
									"all the relations involved in a foreign key "
									"relationship with %s to citus metadata by "
									"executing SELECT citus_add_local_table_to_metadata($$%s$$, "
									"cascade_via_foreign_keys=>true)",
									qualifiedRelationName, qualifiedRelationName)));
		}

		CascadeOperationType cascadeType =
			GetCascadeTypeForCitusLocalTables(autoConverted);

		/*
		 * By acquiring AccessExclusiveLock, make sure that no modifications happen
		 * on the relations.
		 */
		CascadeOperationForFkeyConnectedRelations(relationId, lockMode, cascadeType);

		/*
		 * We converted every foreign key connected table in our subgraph
		 * including itself to a citus local table, so return here.
		 */
		return;
	}

	if (PartitionedTable(relationId))
	{
		List *relationList = PartitionList(relationId);
		if (list_length(relationList) > 0)
		{
			relationList = lappend_oid(relationList, relationId);

			CascadeOperationType cascadeType =
				GetCascadeTypeForCitusLocalTables(autoConverted);

			CascadeOperationForRelationIdList(relationList, AccessExclusiveLock,
											  cascadeType);
			return;
		}
	}

	ObjectAddress tableAddress = { 0 };
	ObjectAddressSet(tableAddress, RelationRelationId, relationId);

	/*
	 * Ensure that the sequences used in column defaults of the table
	 * have proper types
	 */
	EnsureRelationHasCompatibleSequenceTypes(relationId);

	/*
	 * Ensure dependencies exist as we will create shell table on the other nodes
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
	List *tableViewCreationCommands = GetViewCreationCommandsOfTable(relationId);

	char *relationName = get_rel_name(relationId);
	Oid relationSchemaId = get_rel_namespace(relationId);

	/* below we convert relation with relationId to the shard relation */
	uint64 shardId = ConvertLocalTableToShard(relationId);

	/*
	 * As we retrieved the DDL commands necessary to create the shell table
	 * from scratch, below we simply recreate the shell table executing them
	 * via process utility.
	 */
	ExecuteAndLogUtilityCommandList(shellTableDDLEvents);

	/*
	 * Execute the view creation commands with the shell table.
	 * Views will be distributed via FinalizeCitusLocalTableCreation below.
	 */
	ExecuteAndLogUtilityCommandListInTableTypeConversion(tableViewCreationCommands);

	/*
	 * Set shellRelationId as the relation with relationId now points
	 * to the shard relation.
	 */
	Oid shardRelationId = relationId;
	Oid shellRelationId = get_relname_relid(relationName, relationSchemaId);

	/* assert that we created the shell table properly in the same schema */
	Assert(OidIsValid(shellRelationId));

	/*
	 * Move sequence ownerships from shard table to shell table and also drop
	 * DEFAULT expressions from shard relation as we should evaluate such columns
	 * in shell table when needed.
	 */
	DropDefaultExpressionsAndMoveOwnedSequenceOwnerships(shardRelationId,
														 shellRelationId);

	InsertMetadataForCitusLocalTable(shellRelationId, shardId, autoConverted);

	FinalizeCitusLocalTableCreation(shellRelationId);
}


/*
 * CreateCitusLocalTablePartitionOf generates and executes the necessary commands
 * to create a table as partition of a partitioned Citus Local Table.
 * The conversion is done by CreateCitusLocalTable.
 */
void
CreateCitusLocalTablePartitionOf(CreateStmt *createStatement, Oid relationId,
								 Oid parentRelationId)
{
	if (createStatement->partspec)
	{
		/*
		 * Since partspec represents "PARTITION BY" clause, being different than
		 * NULL means that given CreateStmt attempts to create a parent table
		 * at the same time. That means multi-level partitioning within this
		 * function's context. We don't support this currently.
		 */
		char *parentRelationName = get_rel_name(parentRelationId);
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("distributing multi-level partitioned tables "
							   "is not supported"),
						errdetail("Relation \"%s\" is partitioned table itself "
								  "and it is also partition of relation \"%s\".",
								  relationName, parentRelationName)));
	}

	/*
	 * Since the shell table for the partition is not created yet on MX workers,
	 * we should disable DDL propagation before the DETACH command, to avoid
	 * getting an error on the worker.
	 */
	List *detachCommands = list_make3(DISABLE_DDL_PROPAGATION,
									  GenerateDetachPartitionCommand(relationId),
									  ENABLE_DDL_PROPAGATION);
	char *attachCommand = GenerateAlterTableAttachPartitionCommand(relationId);
	ExecuteAndLogUtilityCommandList(detachCommands);
	int fKeyFlags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;

	/*
	 * When cascadeViaForeignKeys is false, CreateCitusLocalTable doesn't expect
	 * any foreign keys on given relation. Note that we don't want to pass
	 * cascadeViaForeignKeys to be true here since we don't already allow non-inherited
	 * foreign keys on child relations, and for the inherited ones, we should have already
	 * cascaded to the other relations when creating a citus local table from parent.
	 *
	 * For this reason, we drop inherited foreign keys here, they'll anyway get created
	 * again with the attach command
	 */
	DropRelationForeignKeys(relationId, fKeyFlags);

	/* get the autoconverted field from the parent */
	CitusTableCacheEntry *entry = GetCitusTableCacheEntry(parentRelationId);

	bool cascade = false;
	bool autoConverted = entry->autoConverted;
	CreateCitusLocalTable(relationId, cascade, autoConverted);
	ExecuteAndLogUtilityCommand(attachCommand);
}


/*
 * ErrorIfAddingPartitionTableToMetadata errors out if we try to create the
 * citus local table from a partition table.
 */
static void
ErrorIfAddingPartitionTableToMetadata(Oid relationId)
{
	if (PartitionTable(relationId))
	{
		/*
		 * We do not allow converting only partitions into Citus Local Tables.
		 * Users should call the UDF citus_add_local_table_to_metadata with the
		 * parent table; then the whole partitioned table will be converted.
		 */
		char *relationName = get_rel_name(relationId);
		Oid parentRelationId = PartitionParentOid(relationId);
		char *parentRelationName = get_rel_name(parentRelationId);
		ereport(ERROR, (errmsg("cannot add local table %s to metadata since "
							   "it is a partition of %s. Instead, add the parent "
							   "table %s to metadata.",
							   relationName, parentRelationName,
							   parentRelationName)));
	}
}


/*
 * ErrorIfUnsupportedCreateCitusLocalTable errors out if we cannot create the
 * citus local table from the relation.
 */
static void
ErrorIfUnsupportedCreateCitusLocalTable(Relation relation)
{
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("cannot add local table to metadata, relation does "
							   "not exist")));
	}

	ErrorIfTableIsACatalogTable(relation);

	Oid relationId = relation->rd_id;

	ErrorIfCoordinatorNotAddedAsWorkerNode();
	ErrorIfUnsupportedCitusLocalTableKind(relationId);
	EnsureTableNotDistributed(relationId);
	ErrorIfUnsupportedCitusLocalColumnDefinition(relation);

	/*
	 * When creating other citus table types, we don't need to check that case as
	 * EnsureTableNotDistributed already errors out if the given relation implies
	 * a citus table. However, as we don't mark the relation as citus table, i.e we
	 * do not use the relation with relationId as the shell relation, parallel
	 * citus_add_local_table_to_metadata executions would not error out for that relation.
	 * Hence we need to error out for shard relations too.
	 */
	ErrorIfRelationIsAKnownShard(relationId);

	/* we do not support policies in citus community */
	ErrorIfUnsupportedPolicy(relation);
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
						errmsg("cannot add local table \"%s\" to metadata, local tables "
							   "added to metadata cannot be involved in inheritance "
							   "relationships", relationName)));
	}

	char relationKind = get_rel_relkind(relationId);
	if (!(relationKind == RELKIND_RELATION || relationKind == RELKIND_FOREIGN_TABLE ||
		  relationKind == RELKIND_PARTITIONED_TABLE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot add local table \"%s\" to metadata, only regular "
							   "tables, partitioned tables and foreign tables"
							   " can be added to citus metadata ",
							   relationName)));
	}

	if (get_rel_persistence(relationId) == RELPERSISTENCE_TEMP)
	{
		/*
		 * Currently, we use citus local tables only to support foreign keys
		 * between local tables and reference tables. Citus already doesn't
		 * support creating reference tables from temp tables.
		 * So now we are creating a citus local table from a temp table that
		 * has a foreign key from/to a reference table with persistent storage.
		 * In that case, we want to give the same error as postgres would do.
		 */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("constraints on temporary tables may reference only "
							   "temporary tables")));
	}
}


/*
 * ErrorIfUnsupportedCitusLocalColumnDefinition errors out if given relation
 * has unsupported column definition for citus local table creation.
 */
static void
ErrorIfUnsupportedCitusLocalColumnDefinition(Relation relation)
{
	TupleDesc relationDesc = RelationGetDescr(relation);
	if (RelationUsesIdentityColumns(relationDesc))
	{
		/*
		 * pg_get_tableschemadef_string doesn't know how to deparse identity
		 * columns so we cannot reflect those columns when creating shell
		 * relation. For this reason, error out here.
		 */
		Oid relationId = relation->rd_id;
		ereport(ERROR, (errmsg("cannot add %s to citus metadata since table "
							   "has identity column",
							   generate_qualified_relation_name(relationId)),
						errhint("Drop the identity columns and re-try the command")));
	}
}


/*
 * NoticeIfAutoConvertingLocalTables logs a NOTICE message to inform the user that we are
 * automatically adding local tables to metadata. The user should know that this table
 * will be undistributed automatically, if it gets disconnected from reference table(s).
 */
static void
NoticeIfAutoConvertingLocalTables(bool autoConverted, Oid relationId)
{
	if (autoConverted && ShouldEnableLocalReferenceForeignKeys())
	{
		char *qualifiedRelationName = generate_qualified_relation_name(relationId);

		/*
		 * When foreign keys between reference tables and postgres tables are
		 * enabled, we automatically undistribute citus local tables that are
		 * not chained with any reference tables back to postgres tables.
		 * So give a warning to user for that.
		 */
		ereport(NOTICE, (errmsg("local tables that are added to metadata automatically "
								"by citus, but not chained with reference tables via "
								"foreign keys might be automatically converted back to "
								"postgres tables"),
						 errhint("Executing citus_add_local_table_to_metadata($$%s$$) "
								 "prevents this for the given relation, and all of the "
								 "connected relations", qualifiedRelationName)));
	}
}


/*
 * GetCascadeTypeForCitusLocalTables returns CASCADE_AUTO_ADD_LOCAL_TABLE_TO_METADATA
 * if autoConverted is true. Returns CASCADE_USER_ADD_LOCAL_TABLE_TO_METADATA otherwise.
 */
static CascadeOperationType
GetCascadeTypeForCitusLocalTables(bool autoConverted)
{
	if (autoConverted)
	{
		return CASCADE_AUTO_ADD_LOCAL_TABLE_TO_METADATA;
	}

	return CASCADE_USER_ADD_LOCAL_TABLE_TO_METADATA;
}


/*
 * UpdateAutoConvertedForConnectedRelations updates the autoConverted field on
 * pg_dist_partition for the foreign key connected relations of the given relations.
 * Sets it to given autoConverted value for all of the connected relations.
 * We don't need to update partition relations separately, since the foreign key
 * graph already includes them, as they have the same (inherited) fkeys as their parents.
 */
void
UpdateAutoConvertedForConnectedRelations(List *relationIds, bool autoConverted)
{
	InvalidateForeignKeyGraph();

	List *relationIdList = NIL;
	Oid relid = InvalidOid;
	foreach_oid(relid, relationIds)
	{
		List *connectedRelations = GetForeignKeyConnectedRelationIdList(relid);
		relationIdList = list_concat_unique_oid(relationIdList, connectedRelations);
	}

	relationIdList = SortList(relationIdList, CompareOids);

	foreach_oid(relid, relationIdList)
	{
		UpdatePgDistPartitionAutoConverted(relid, false);
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
	IncludeSequenceDefaults includeSequenceDefaults = NEXTVAL_SEQUENCE_DEFAULTS;

	bool creatingShellTableOnRemoteNode = false;
	List *tableDDLCommands = GetFullTableCreationCommands(relationId,
														  includeSequenceDefaults,
														  creatingShellTableOnRemoteNode);

	List *shellTableDDLEvents = NIL;
	TableDDLCommand *tableDDLCommand = NULL;
	foreach_ptr(tableDDLCommand, tableDDLCommands)
	{
		Assert(CitusIsA(tableDDLCommand, TableDDLCommand));
		shellTableDDLEvents = lappend(shellTableDDLEvents,
									  GetTableDDLCommand(tableDDLCommand));
	}
	shellTableDDLEvents = list_concat(shellTableDDLEvents, foreignConstraintCommands);

	return shellTableDDLEvents;
}


/*
 * ConvertLocalTableToShard first acquires a shardId and then converts the
 * given relation with relationId to the shard relation with shardId. That
 * means, this function suffixes shardId to:
 *  - relation name,
 *  - all the objects "defined on" the relation.
 * After converting the given relation, returns the acquired shardId.
 */
static uint64
ConvertLocalTableToShard(Oid relationId)
{
	uint64 shardId = GetNextShardId();

	RenameRelationToShardRelation(relationId, shardId);
	RenameShardRelationConstraints(relationId, shardId);
	RenameShardRelationIndexes(relationId, shardId);
	RenameShardRelationStatistics(relationId, shardId);

	/*
	 * We do not create truncate triggers on shard relation. This is
	 * because truncate triggers are fired by utility hook and we would
	 * need to disable them to prevent executing them twice if we don't
	 * drop the trigger on shard relation.
	 */
	DropRelationTruncateTriggers(relationId);

	/* drop views that depend on the shard table */
	DropViewsOnTable(relationId);

	/*
	 * We create INSERT|DELETE|UPDATE triggers on shard relation too.
	 * This is because citus prevents postgres executor to fire those
	 * triggers. So, here we suffix such triggers on shard relation
	 * with shardId.
	 */
	RenameShardRelationNonTruncateTriggers(relationId, shardId);

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

	ExecuteAndLogUtilityCommand(renameCommand->data);
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
		ExecuteAndLogUtilityCommand(commandString);
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

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);

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
	table_close(pgConstraint, NoLock);

	return constraintNameList;
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
 * GetExplicitIndexOidList to pick the indexes to be renamed, see more
 * details in function's comment.
 */
static void
RenameShardRelationIndexes(Oid shardRelationId, uint64 shardId)
{
	List *indexOidList = GetExplicitIndexOidList(shardRelationId);

	Oid indexOid = InvalidOid;
	foreach_oid(indexOid, indexOidList)
	{
		const char *commandString = GetRenameShardIndexCommand(indexOid, shardId);
		ExecuteAndLogUtilityCommand(commandString);
	}
}


/*
 * GetRenameShardIndexCommand returns DDL command to append given shardId to
 * the index with indexName.
 */
static char *
GetRenameShardIndexCommand(Oid indexOid, uint64 shardId)
{
	char *indexName = get_rel_name(indexOid);
	char *shardIndexName = pstrdup(indexName);
	AppendShardIdToName(&shardIndexName, shardId);
	const char *quotedShardIndexName = quote_identifier(shardIndexName);

	const char *quotedIndexName = generate_qualified_relation_name(indexOid);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER INDEX %s RENAME TO %s;",
					 quotedIndexName, quotedShardIndexName);

	return renameCommand->data;
}


/*
 * RenameShardRelationStatistics appends given shardId to the end of the names
 * of shard relation statistics. This function utilizes GetExplicitStatsNameList
 * to pick the statistics to be renamed, see more details in function's comment.
 */
static void
RenameShardRelationStatistics(Oid shardRelationId, uint64 shardId)
{
	Relation shardRelation = RelationIdGetRelation(shardRelationId);
	List *statsOidList = RelationGetStatExtList(shardRelation);
	RelationClose(shardRelation);

	List *statsCommandList = GetRenameStatsCommandList(statsOidList, shardId);

	char *command = NULL;
	foreach_ptr(command, statsCommandList)
	{
		ExecuteAndLogUtilityCommand(command);
	}
}


/*
 * GetRenameShardStatsCommand returns DDL command to append given shardId to
 * the statistics with statName.
 */
static char *
GetRenameShardStatsCommand(char *statSchema, char *statsName, char *statsNameWithShardId)
{
	const char *quotedStatsNameWithShardId = quote_identifier(statsNameWithShardId);
	char *qualifiedStatsName = quote_qualified_identifier(statSchema, statsName);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER STATISTICS %s RENAME TO %s;",
					 qualifiedStatsName, quotedStatsNameWithShardId);

	return renameCommand->data;
}


/*
 * RenameShardRelationNonTruncateTriggers appends given shardId to the end of
 * the names of shard relation INSERT/DELETE/UPDATE triggers that are explicitly
 * created.
 */
static void
RenameShardRelationNonTruncateTriggers(Oid shardRelationId, uint64 shardId)
{
	List *triggerIdList = GetExplicitTriggerIdList(shardRelationId);

	Oid triggerId = InvalidOid;
	foreach_oid(triggerId, triggerIdList)
	{
		bool missingOk = false;
		HeapTuple triggerTuple = GetTriggerTupleById(triggerId, missingOk);
		Form_pg_trigger triggerForm = (Form_pg_trigger) GETSTRUCT(triggerTuple);

		if (!TRIGGER_FOR_TRUNCATE(triggerForm->tgtype))
		{
			char *triggerName = NameStr(triggerForm->tgname);
			char *commandString =
				GetRenameShardTriggerCommand(shardRelationId, triggerName, shardId);
			ExecuteAndLogUtilityCommand(commandString);
		}

		heap_freetuple(triggerTuple);
	}
}


/*
 * GetRenameShardTriggerCommand returns DDL command to append given shardId to
 * the trigger with triggerName.
 */
static char *
GetRenameShardTriggerCommand(Oid shardRelationId, char *triggerName, uint64 shardId)
{
	char *qualifiedShardRelationName = generate_qualified_relation_name(shardRelationId);

	char *shardTriggerName = pstrdup(triggerName);
	AppendShardIdToName(&shardTriggerName, shardId);
	const char *quotedShardTriggerName = quote_identifier(shardTriggerName);

	const char *quotedTriggerName = quote_identifier(triggerName);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER TRIGGER %s ON %s RENAME TO %s;",
					 quotedTriggerName, qualifiedShardRelationName,
					 quotedShardTriggerName);

	return renameCommand->data;
}


/*
 * DropRelationTruncateTriggers drops TRUNCATE triggers that are explicitly
 * created on relation with relationId.
 */
static void
DropRelationTruncateTriggers(Oid relationId)
{
	List *triggerIdList = GetExplicitTriggerIdList(relationId);

	Oid triggerId = InvalidOid;
	foreach_oid(triggerId, triggerIdList)
	{
		bool missingOk = false;
		HeapTuple triggerTuple = GetTriggerTupleById(triggerId, missingOk);
		Form_pg_trigger triggerForm = (Form_pg_trigger) GETSTRUCT(triggerTuple);

		if (TRIGGER_FOR_TRUNCATE(triggerForm->tgtype))
		{
			char *triggerName = NameStr(triggerForm->tgname);
			char *commandString = GetDropTriggerCommand(relationId, triggerName);
			ExecuteAndLogUtilityCommand(commandString);
		}

		heap_freetuple(triggerTuple);
	}
}


/*
 * GetDropTriggerCommand returns DDL command to drop the trigger with triggerName
 * on relationId.
 */
static char *
GetDropTriggerCommand(Oid relationId, char *triggerName)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	const char *quotedTriggerName = quote_identifier(triggerName);

	/*
	 * In postgres, the only possible object type that may depend on a trigger
	 * is the "constraint" object implied by the trigger itself if it is a
	 * constraint trigger, and it would be an internal dependency so it could
	 * be dropped without using CASCADE. Other than this, it is also possible
	 * to define dependencies on trigger via recordDependencyOn api by other
	 * extensions. We don't handle those kind of dependencies, we just drop
	 * them with CASCADE.
	 */
	StringInfo dropCommand = makeStringInfo();
	appendStringInfo(dropCommand, "DROP TRIGGER %s ON %s CASCADE;",
					 quotedTriggerName, qualifiedRelationName);

	return dropCommand->data;
}


/*
 * DropViewsOnTable drops the views that depend on the given relation.
 */
static void
DropViewsOnTable(Oid relationId)
{
	List *views = GetDependingViews(relationId);

	/*
	 * GetDependingViews returns views in the dependency order. We should drop views
	 * in the reversed order since dropping views can cascade to other views below.
	 */
	List *reverseOrderedViews = ReversedOidList(views);

	Oid viewId = InvalidOid;
	foreach_oid(viewId, reverseOrderedViews)
	{
		char *viewName = get_rel_name(viewId);
		char *schemaName = get_namespace_name(get_rel_namespace(viewId));
		char *qualifiedViewName = quote_qualified_identifier(schemaName, viewName);

		StringInfo dropCommand = makeStringInfo();
		appendStringInfo(dropCommand, "DROP %sVIEW IF EXISTS %s",
						 get_rel_relkind(viewId) == RELKIND_MATVIEW ? "MATERIALIZED " :
						 "",
						 qualifiedViewName);

		ExecuteAndLogUtilityCommand(dropCommand->data);
	}
}


/*
 * ReversedOidList takes a list of oids and returns the reverse ordered version of it.
 */
static List *
ReversedOidList(List *oidList)
{
	List *reversed = NIL;
	Oid oid = InvalidOid;
	foreach_oid(oid, oidList)
	{
		reversed = lcons_oid(oid, reversed);
	}

	return reversed;
}


/*
 * GetExplicitIndexOidList returns a list of index oids defined "explicitly"
 * on the relation with relationId by the "CREATE INDEX" commands. That means,
 * all the constraints defined on the relation except:
 *  - primary indexes,
 *  - unique indexes and
 *  - exclusion indexes
 * that are actually applied by the related constraints.
 */
List *
GetExplicitIndexOidList(Oid relationId)
{
	/* flags are not applicable for AppendExplicitIndexIdsToList */
	int flags = 0;
	return ExecuteFunctionOnEachTableIndex(relationId, AppendExplicitIndexIdsToList,
										   flags);
}


/*
 * AppendExplicitIndexIdsToList adds the given index oid if it is
 * explicitly created on its relation.
 */
static void
AppendExplicitIndexIdsToList(Form_pg_index indexForm, List **explicitIndexIdList,
							 int flags)
{
	if (!IndexImpliedByAConstraint(indexForm))
	{
		*explicitIndexIdList = lappend_oid(*explicitIndexIdList, indexForm->indexrelid);
	}
}


/*
 * GetRenameStatsCommandList returns a list of "ALTER STATISTICS ...
 * RENAME TO ..._shardId" commands for given statistics oid list.
 */
static List *
GetRenameStatsCommandList(List *statsOidList, uint64 shardId)
{
	List *statsCommandList = NIL;
	Oid statsOid;
	foreach_oid(statsOid, statsOidList)
	{
		HeapTuple tup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statsOid));

		if (!HeapTupleIsValid(tup))
		{
			ereport(WARNING, (errmsg("No stats object found with id: %u", statsOid)));
			continue;
		}

		Form_pg_statistic_ext statisticsForm = (Form_pg_statistic_ext) GETSTRUCT(tup);

		char *statsName = statisticsForm->stxname.data;
		Oid statsSchemaOid = statisticsForm->stxnamespace;
		char *statsSchema = get_namespace_name(statsSchemaOid);

		char *statsNameWithShardId = pstrdup(statsName);
		AppendShardIdToName(&statsNameWithShardId, shardId);

		char *renameShardStatsCommand = GetRenameShardStatsCommand(statsSchema, statsName,
																   statsNameWithShardId);
		statsCommandList = lappend(statsCommandList, renameShardStatsCommand);
		ReleaseSysCache(tup);
	}

	return statsCommandList;
}


/*
 * DropDefaultExpressionsAndMoveOwnedSequenceOwnerships drops default column
 * definitions for relation with sourceRelationId. Also, for each column that
 * defaults to an owned sequence, it grants ownership to the same named column
 * of the relation with targetRelationId.
 */
static void
DropDefaultExpressionsAndMoveOwnedSequenceOwnerships(Oid sourceRelationId,
													 Oid targetRelationId)
{
	List *columnNameList = NIL;
	List *ownedSequenceIdList = NIL;
	ExtractDefaultColumnsAndOwnedSequences(sourceRelationId, &columnNameList,
										   &ownedSequenceIdList);

	char *columnName = NULL;
	Oid ownedSequenceId = InvalidOid;
	forboth_ptr_oid(columnName, columnNameList, ownedSequenceId, ownedSequenceIdList)
	{
		DropDefaultColumnDefinition(sourceRelationId, columnName);

		/* column might not own a sequence */
		if (OidIsValid(ownedSequenceId))
		{
			TransferSequenceOwnership(ownedSequenceId, targetRelationId, columnName);
		}
	}
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

	/*
	 * We need to disable/enable ddl propagation for this command, to prevent
	 * sending unnecessary ALTER COLUMN commands for partitions, to MX workers.
	 */
	ExecuteAndLogUtilityCommandList(list_make3(DISABLE_DDL_PROPAGATION,
											   sequenceDropCommand->data,
											   ENABLE_DDL_PROPAGATION));
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

	/*
	 * We need to disable/enable ddl propagation for this command, to prevent
	 * sending unnecessary ALTER SEQUENCE commands for partitions, to MX workers.
	 * Especially for partitioned tables, where the same sequence is used for
	 * all partitions, this might cause errors.
	 */
	ExecuteAndLogUtilityCommandList(list_make3(DISABLE_DDL_PROPAGATION,
											   sequenceOwnershipCommand->data,
											   ENABLE_DDL_PROPAGATION));
}


/*
 * InsertMetadataForCitusLocalTable inserts necessary metadata for the citus
 * local table to the following metadata tables:
 * pg_dist_partition, pg_dist_shard & pg_dist_placement.
 */
static void
InsertMetadataForCitusLocalTable(Oid citusLocalTableId, uint64 shardId,
								 bool autoConverted)
{
	Assert(OidIsValid(citusLocalTableId));
	Assert(shardId != INVALID_SHARD_ID);

	char distributionMethod = DISTRIBUTE_BY_NONE;
	char replicationModel = REPLICATION_MODEL_STREAMING;

	uint32 colocationId = INVALID_COLOCATION_ID;
	Var *distributionColumn = NULL;
	InsertIntoPgDistPartition(citusLocalTableId, distributionMethod,
							  distributionColumn, colocationId,
							  replicationModel, autoConverted);

	/* set shard storage type according to relation type */
	char shardStorageType = ShardStorageType(citusLocalTableId);

	text *shardMinValue = NULL;
	text *shardMaxValue = NULL;
	InsertShardRow(citusLocalTableId, shardId, shardStorageType,
				   shardMinValue, shardMaxValue);

	List *nodeList = list_make1(CoordinatorNodeIfAddedAsWorkerOrError());

	int replicationFactor = 1;
	int workerStartIndex = 0;
	InsertShardPlacementRows(citusLocalTableId, shardId, nodeList,
							 workerStartIndex, replicationFactor);
}


/*
 * FinalizeCitusLocalTableCreation completes creation of the citus local table
 * with relationId by performing operations that should be done after creating
 * the shard and inserting the metadata.
 * If the cluster has metadata workers, we ensure proper propagation of the
 * sequences dependent with the table.
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
		SyncCitusTableMetadata(relationId);
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
