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

#include "access/xact.h"
#include "catalog/pg_constraint.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


typedef void (*CascadeOperationFunction)(Oid, bool);


static void EnsureSequentialModeForCitusTableCascadeFunction(List *relationIdList);
static bool RelationIdListHasReferenceTable(List *relationIdList);
static void LockRelationsWithLockMode(List *relationIdList, LOCKMODE lockMode);
static List * RemovePartitionRelationIds(List *relationIdList);
static List * GetFKeyCreationCommandsForRelationIdList(List *relationIdList);
static void DropRelationIdListForeignKeys(List *relationIdList);
static void DropRelationForeignKeys(Oid relationId);
static List * GetRelationDropFkeyCommands(Oid relationId);
static char * GetDropFkeyCascadeCommand(Oid relationId, Oid foreignKeyId);
static void ExecuteCascadeOperationForRelationIdList(List *relationIdList,
													 CascadeOperationType
													 cascadeOperationType);
static CascadeOperationFunction GetCascadeOperationFunction(CascadeOperationType
															cascadeOperationType);


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
	DropRelationIdListForeignKeys(nonPartitionRelationIdList);
	ExecuteCascadeOperationForRelationIdList(nonPartitionRelationIdList,
											 cascadeOperationType);

	/* now recreate foreign keys on tables */
	ExecuteAndLogDDLCommandList(fKeyCreationCommands);
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
static bool
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
DropRelationIdListForeignKeys(List *relationIdList)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		DropRelationForeignKeys(relationId);
	}
}


/*
 * DropRelationForeignKeys drops foreign keys where the relation with
 * relationId is the referencing relation.
 */
static void
DropRelationForeignKeys(Oid relationId)
{
	List *dropFkeyCascadeCommandList = GetRelationDropFkeyCommands(relationId);
	ExecuteAndLogDDLCommandList(dropFkeyCascadeCommandList);
}


/*
 * GetRelationDropFkeyCommands returns a list of DDL commands to drop foreign
 * keys where the relation with relationId is the referencing relation.
 */
static List *
GetRelationDropFkeyCommands(Oid relationId)
{
	List *dropFkeyCascadeCommandList = NIL;

	int flag = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	List *relationFKeyIdList = GetForeignKeyOids(relationId, flag);

	Oid foreignKeyId;
	foreach_oid(foreignKeyId, relationFKeyIdList)
	{
		char *dropFkeyCascadeCommand = GetDropFkeyCascadeCommand(relationId,
																 foreignKeyId);
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
GetDropFkeyCascadeCommand(Oid relationId, Oid foreignKeyId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

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
		CascadeOperationFunction cascadeOperationFunction =
			GetCascadeOperationFunction(cascadeOperationType);

		/*
		 * Caller already passed the relations that we should operate on,
		 * so we should not cascade here.
		 */
		bool cascadeViaForeignKeys = false;
		cascadeOperationFunction(relationId, cascadeViaForeignKeys);
	}
}


/*
 * GetCascadeOperationFunction returns c api for citus table operation according
 * to given CascadeOperationType.
 */
static CascadeOperationFunction
GetCascadeOperationFunction(CascadeOperationType cascadeOperationType)
{
	switch (cascadeOperationType)
	{
		case UNDISTRIBUTE_TABLE:
		{
			return UndistributeTable;
		}

		case CREATE_CITUS_LOCAL_TABLE:
		{
			return CreateCitusLocalTable;
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


/*
 * ExecuteAndLogDDLCommandList takes a list of ddl commands and calls
 * ExecuteAndLogDDLCommand function for each of them.
 */
void
ExecuteAndLogDDLCommandList(List *ddlCommandList)
{
	char *ddlCommand = NULL;
	foreach_ptr(ddlCommand, ddlCommandList)
	{
		ExecuteAndLogDDLCommand(ddlCommand);
	}
}


/*
 * ExecuteAndLogDDLCommand takes a ddl command and logs it in DEBUG4 log level.
 * Then, parses and executes it via CitusProcessUtility.
 */
void
ExecuteAndLogDDLCommand(const char *commandString)
{
	ereport(DEBUG4, (errmsg("executing \"%s\"", commandString)));

	Node *parseTree = ParseTreeNode(commandString);
	CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
						NULL, None_Receiver, NULL);
}
