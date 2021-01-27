/*-------------------------------------------------------------------------
 *
 * cascade_table_operation_for_connected_relations.c
 *   Routines to execute citus table functions (e.g undistribute_table,
 *   create_citus_local_table) by cascading to foreign key connected
 *   relations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#if (PG_VERSION_NUM < PG_VERSION_12)
#include "access/htup_details.h"
#endif
#include "access/xact.h"
#include "catalog/pg_constraint.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_protocol.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static void EnsureSequentialModeForCitusTableCascadeFunction(List *relationIdList);
static void LockRelationsWithLockMode(List *relationIdList, LOCKMODE lockMode);
static List * RemovePartitionRelationIds(List *relationIdList);
static List * GetFKeyCreationCommandsForRelationIdList(List *relationIdList);
static void DropRelationIdListForeignKeys(List *relationIdList, int fKeyFlags);
static List * GetRelationDropFkeyCommands(Oid relationId, int fKeyFlags);
static char * GetDropFkeyCascadeCommand(Oid foreignKeyId);
static void ExecuteCascadeOperationForRelationIdList(List *relationIdList,
													 CascadeOperationType
													 cascadeOperationType);
static void ExecuteForeignKeyCreateCommand(const char *commandString,
										   bool skip_validation);

/*
 * CascadeOperationForConnectedRelations executes citus table function specified
 * by CascadeOperationType argument on each relation that relation
 * with relationId is connected via it's foreign key graph, which includes
 * input relation itself.
 * Also see CascadeOperationType enum definition for supported
 * citus table functions.
 */
void
CascadeOperationForConnectedRelations(Oid relationId, LOCKMODE lockMode,
									  CascadeOperationType
									  cascadeOperationType)
{
	/*
	 * As we will operate on foreign key connected relations, here we
	 * invalidate foreign key graph to be on the safe side.
	 */
	InvalidateForeignKeyGraph();

	List *fKeyConnectedRelationIdList = GetForeignKeyConnectedRelationIdList(relationId);
	LockRelationsWithLockMode(fKeyConnectedRelationIdList, lockMode);

	/*
	 * Before removing any partition relations, we should error out here if any
	 * of connected relations is a partition table involved in a foreign key that
	 * is not inherited from its parent table.
	 * We should handle this case here as we remove partition relations in this
	 * function	before ExecuteCascadeOperationForRelationIdList.
	 */
	ErrorIfAnyPartitionRelationInvolvedInNonInheritedFKey(fKeyConnectedRelationIdList);

	/*
	 * We shouldn't cascade through foreign keys on partition tables as citus
	 * table functions already have their own logics to handle partition relations.
	 */
	List *nonPartitionRelationIdList =
		RemovePartitionRelationIds(fKeyConnectedRelationIdList);

	/*
	 * Our foreign key subgraph can have distributed tables which might already
	 * be modified in current transaction. So switch to sequential execution
	 * before executing any ddl's to prevent erroring out later in this function.
	 */
	EnsureSequentialModeForCitusTableCascadeFunction(nonPartitionRelationIdList);

	/* store foreign key creation commands before dropping them */
	List *fKeyCreationCommands =
		GetFKeyCreationCommandsForRelationIdList(nonPartitionRelationIdList);

	/*
	 * Note that here we only drop referencing foreign keys for each relation.
	 * This is because referenced foreign keys are already captured as other
	 * relations' referencing foreign keys.
	 */
	int fKeyFlags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	DropRelationIdListForeignKeys(nonPartitionRelationIdList, fKeyFlags);
	ExecuteCascadeOperationForRelationIdList(nonPartitionRelationIdList,
											 cascadeOperationType);

	/* now recreate foreign keys on tables */
	bool skip_validation = true;
	ExecuteForeignKeyCreateCommandList(fKeyCreationCommands, skip_validation);
}


/*
 * LockRelationsWithLockMode sorts given relationIdList and then acquires
 * specified lockMode on those relations.
 */
static void
LockRelationsWithLockMode(List *relationIdList, LOCKMODE lockMode)
{
	Oid relationId;
	relationIdList = SortList(relationIdList, CompareOids);
	foreach_oid(relationId, relationIdList)
	{
		LockRelationOid(relationId, lockMode);
	}
}


/*
 * ErrorIfAnyPartitionRelationInvolvedInNonInheritedFKey searches given
 * relationIdList for a partition relation involved in a foreign key relationship
 * that is not inherited from its parent and errors out if such a partition
 * relation exists.
 */
void
ErrorIfAnyPartitionRelationInvolvedInNonInheritedFKey(List *relationIdList)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (!PartitionTable(relationId))
		{
			continue;
		}

		if (!RelationInvolvedInAnyNonInheritedForeignKeys(relationId))
		{
			continue;
		}

		char *partitionRelationQualifiedName =
			generate_qualified_relation_name(relationId);
		ereport(ERROR, (errmsg("cannot cascade operation via foreign keys as "
							   "partition table %s involved in a foreign key "
							   "relationship that is not inherited from it's "
							   "parent table", partitionRelationQualifiedName),
						errhint("Remove non-inherited foreign keys from %s and "
								"try operation again", partitionRelationQualifiedName)));
	}
}


/*
 * RemovePartitionRelationIds returns a list of relation id's by removing
 * partition relation id's from given relationIdList.
 */
static List *
RemovePartitionRelationIds(List *relationIdList)
{
	List *nonPartitionRelationIdList = NIL;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (PartitionTable(relationId))
		{
			continue;
		}

		nonPartitionRelationIdList = lappend_oid(nonPartitionRelationIdList, relationId);
	}

	return nonPartitionRelationIdList;
}


/*
 * EnsureSequentialModeForCitusTableCascadeFunction switches to sequential
 * execution mode if needed. If it's not possible, then errors out.
 */
static void
EnsureSequentialModeForCitusTableCascadeFunction(List *relationIdList)
{
	if (!RelationIdListHasReferenceTable(relationIdList))
	{
		/*
		 * We don't need to switch to sequential execution if there is no
		 * reference table in our foreign key subgraph.
		 */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot execute command because there was a parallel "
							   "operation on a distributed table in transaction"),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode because the "
							"operation cascades into distributed tables with foreign "
							"keys to reference tables")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * RelationIdListHasReferenceTable returns true if relationIdList has a relation
 * id that belongs to a reference table.
 */
bool
RelationIdListHasReferenceTable(List *relationIdList)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			return true;
		}
	}

	return false;
}


/*
 * GetFKeyCreationCommandsForRelationIdList returns a list of DDL commands to
 * create foreign keys for each relation in relationIdList.
 */
static List *
GetFKeyCreationCommandsForRelationIdList(List *relationIdList)
{
	List *fKeyCreationCommands = NIL;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		List *relationFKeyCreationCommands =
			GetReferencingForeignConstaintCommands(relationId);
		fKeyCreationCommands = list_concat(fKeyCreationCommands,
										   relationFKeyCreationCommands);
	}

	return fKeyCreationCommands;
}


/*
 * DropRelationIdListForeignKeys drops foreign keys for each relation in given
 * relation id list.
 */
static void
DropRelationIdListForeignKeys(List *relationIdList, int fKeyFlags)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		DropRelationForeignKeys(relationId, fKeyFlags);
	}
}


/*
 * DropRelationForeignKeys drops foreign keys where the relation with
 * relationId is the referencing relation.
 */
void
DropRelationForeignKeys(Oid relationId, int fKeyFlags)
{
	/*
	 * We undistribute citus local tables that are not chained with any reference
	 * tables via foreign keys at the end of the utility hook.
	 * Here we temporarily set the related GUC to off to disable the logic for
	 * internally executed DDL's that might invoke this mechanism unnecessarily.
	 */
	bool oldEnableLocalReferenceForeignKeys = EnableLocalReferenceForeignKeys;
	SetLocalEnableLocalReferenceForeignKeys(false);

	List *dropFkeyCascadeCommandList = GetRelationDropFkeyCommands(relationId, fKeyFlags);
	ExecuteAndLogUtilityCommandList(dropFkeyCascadeCommandList);

	SetLocalEnableLocalReferenceForeignKeys(oldEnableLocalReferenceForeignKeys);
}


/*
 * SetLocalEnableLocalReferenceForeignKeys is simply a C interface for setting
 * the following:
 *      SET LOCAL citus.enable_local_reference_table_foreign_keys = 'on'|'off';
 */
void
SetLocalEnableLocalReferenceForeignKeys(bool state)
{
	char *stateStr = state ? "on" : "off";
	set_config_option("citus.enable_local_reference_table_foreign_keys", stateStr,
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}


/*
 * GetRelationDropFkeyCommands returns a list of DDL commands to drop foreign
 * keys where the relation with relationId is the referencing relation.
 */
static List *
GetRelationDropFkeyCommands(Oid relationId, int fKeyFlags)
{
	List *dropFkeyCascadeCommandList = NIL;

	List *relationFKeyIdList = GetForeignKeyOids(relationId, fKeyFlags);

	Oid foreignKeyId;
	foreach_oid(foreignKeyId, relationFKeyIdList)
	{
		char *dropFkeyCascadeCommand = GetDropFkeyCascadeCommand(foreignKeyId);
		dropFkeyCascadeCommandList = lappend(dropFkeyCascadeCommandList,
											 dropFkeyCascadeCommand);
	}

	return dropFkeyCascadeCommandList;
}


/*
 * GetDropFkeyCascadeCommand returns DDL command to drop foreign key with
 * foreignKeyId.
 */
static char *
GetDropFkeyCascadeCommand(Oid foreignKeyId)
{
	/*
	 * As we need to execute ALTER TABLE DROP CONSTRAINT command on
	 * referencing relation, resolve it here.
	 */
	HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyId));
	Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
	Oid relationId = constraintForm->conrelid;
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	ReleaseSysCache(heapTuple);

	char *constraintName = get_constraint_name(foreignKeyId);
	const char *quotedConstraintName = quote_identifier(constraintName);

	StringInfo dropFkeyCascadeCommand = makeStringInfo();
	appendStringInfo(dropFkeyCascadeCommand, "ALTER TABLE %s DROP CONSTRAINT %s CASCADE;",
					 qualifiedRelationName, quotedConstraintName);

	return dropFkeyCascadeCommand->data;
}


/*
 * ExecuteCascadeOperationForRelationIdList executes citus table function
 * specified by CascadeOperationType argument for given relation id
 * list.
 */
static void
ExecuteCascadeOperationForRelationIdList(List *relationIdList,
										 CascadeOperationType
										 cascadeOperationType)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		/*
		 * The reason behind skipping certain table types in below loop is
		 * that we support some sort of foreign keys between postgres tables
		 * and citus tables when enable_local_reference_table_foreign_keys is
		 * false or when coordinator is not added to metadata.
		 *
		 * Also, as caller already passed the relations that we should operate
		 * on, we don't cascade via foreign keys here.
		 */
		bool cascadeViaForeignKeys = false;
		switch (cascadeOperationType)
		{
			case CASCADE_FKEY_UNDISTRIBUTE_TABLE:
			{
				if (IsCitusTable(relationId))
				{
					TableConversionParameters params = {
						.relationId = relationId,
						.cascadeViaForeignKeys = cascadeViaForeignKeys
					};
					UndistributeTable(&params);
				}

				break;
			}

			case CASCADE_FKEY_ADD_LOCAL_TABLE_TO_METADATA:
			{
				if (!IsCitusTable(relationId))
				{
					CreateCitusLocalTable(relationId, cascadeViaForeignKeys);
				}

				break;
			}

			default:
			{
				/*
				 * This is not expected as other create table functions don't have
				 * cascade option yet. To be on the safe side, error out here.
				 */
				ereport(ERROR, (errmsg("citus table function could not be found")));
			}
		}
	}
}


/*
 * ExecuteAndLogUtilityCommandList takes a list of utility commands and calls
 * ExecuteAndLogUtilityCommand function for each of them.
 */
void
ExecuteAndLogUtilityCommandList(List *utilityCommandList)
{
	char *utilityCommand = NULL;
	foreach_ptr(utilityCommand, utilityCommandList)
	{
		ExecuteAndLogUtilityCommand(utilityCommand);
	}
}


/*
 * ExecuteAndLogUtilityCommand takes a utility command and logs it in DEBUG4 log level.
 * Then, parses and executes it via CitusProcessUtility.
 */
void
ExecuteAndLogUtilityCommand(const char *commandString)
{
	ereport(DEBUG4, (errmsg("executing \"%s\"", commandString)));

	ExecuteUtilityCommand(commandString);
}


/*
 * ExecuteForeignKeyCreateCommandList takes a list of foreign key creation ddl commands
 * and calls ExecuteAndLogForeignKeyCreateCommand function for each of them.
 */
void
ExecuteForeignKeyCreateCommandList(List *ddlCommandList, bool skip_validation)
{
	char *ddlCommand = NULL;
	foreach_ptr(ddlCommand, ddlCommandList)
	{
		ExecuteForeignKeyCreateCommand(ddlCommand, skip_validation);
	}
}


/*
 * ExecuteForeignKeyCreateCommand takes a foreign key creation command
 * and logs it in DEBUG4 log level.
 *
 * Then, parses, sets skip_validation flag to considering the input and
 * executes the command via CitusProcessUtility.
 */
static void
ExecuteForeignKeyCreateCommand(const char *commandString, bool skip_validation)
{
	ereport(DEBUG4, (errmsg("executing foreign key create command \"%s\"",
							commandString)));

	Node *parseTree = ParseTreeNode(commandString);

	/*
	 * We might have thrown an error if IsA(parseTree, AlterTableStmt),
	 * but that doesn't seem to provide any benefits, so assertion is
	 * fine for this case.
	 */
	Assert(IsA(parseTree, AlterTableStmt));

	if (skip_validation && IsA(parseTree, AlterTableStmt))
	{
		parseTree =
			SkipForeignKeyValidationIfConstraintIsFkey((AlterTableStmt *) parseTree,
													   true);

		ereport(DEBUG4, (errmsg("skipping validation for foreign key create "
								"command \"%s\"", commandString)));
	}

	ProcessUtilityParseTree(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
}
