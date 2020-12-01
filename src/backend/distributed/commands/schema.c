/*-------------------------------------------------------------------------
 *
 * schema.c
 *    Commands for creating and altering schemas for distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "distributed/commands.h"
#include <distributed/connection_management.h>
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include <distributed/metadata_sync.h>
#include "distributed/multi_executor.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include <distributed/remote_commands.h>
#include <distributed/remote_commands.h>
#include "distributed/version_compat.h"
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"


static List * FilterDistributedSchemas(List *schemas);
static void EnsureSequentialModeForSchemaDDL(void);


/*
 * PreprocessDropSchemaStmt invalidates the foreign key cache if any table created
 * under dropped schema involved in any foreign key relationship.
 */
List *
PreprocessDropSchemaStmt(Node *node, const char *queryString)
{
	DropStmt *dropStatement = castNode(DropStmt, node);
	Relation pgClass = NULL;
	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;

	if (dropStatement->behavior != DROP_CASCADE)
	{
		return NIL;
	}

	Value *schemaValue = NULL;
	foreach_ptr(schemaValue, dropStatement->objects)
	{
		const char *schemaString = strVal(schemaValue);
		Oid namespaceOid = get_namespace_oid(schemaString, true);

		if (namespaceOid == InvalidOid)
		{
			continue;
		}

		pgClass = table_open(RelationRelationId, AccessShareLock);

		ScanKeyInit(&scanKey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
					F_OIDEQ, namespaceOid);
		scanDescriptor = systable_beginscan(pgClass, scanIndexId, useIndex, NULL,
											scanKeyCount, scanKey);

		heapTuple = systable_getnext(scanDescriptor);
		while (HeapTupleIsValid(heapTuple))
		{
			Form_pg_class relationForm = (Form_pg_class) GETSTRUCT(heapTuple);
			char *relationName = NameStr(relationForm->relname);
			Oid relationId = get_relname_relid(relationName, namespaceOid);

			/* we're not interested in non-valid, non-distributed relations */
			if (relationId == InvalidOid || !IsCitusTable(relationId))
			{
				heapTuple = systable_getnext(scanDescriptor);
				continue;
			}

			if (IsCitusTableType(relationId, REFERENCE_TABLE))
			{
				/* prevent concurrent EnsureReferenceTablesExistOnAllNodes */
				int colocationId = CreateReferenceTableColocationId();
				LockColocationId(colocationId, ExclusiveLock);
			}

			/* invalidate foreign key cache if the table involved in any foreign key */
			if (TableReferenced(relationId) || TableReferencing(relationId))
			{
				MarkInvalidateForeignKeyGraph();

				systable_endscan(scanDescriptor);
				table_close(pgClass, NoLock);
				return NIL;
			}

			heapTuple = systable_getnext(scanDescriptor);
		}

		systable_endscan(scanDescriptor);
		table_close(pgClass, NoLock);
	}

	return NIL;
}


/*
 * PreprocessGrantOnSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on schemas. Only grant statements for distributed schema are propagated.
 */
List *
PreprocessGrantOnSchemaStmt(Node *node, const char *queryString)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SCHEMA);

	List *distributedSchemas = FilterDistributedSchemas(stmt->objects);

	if (list_length(distributedSchemas) == 0)
	{
		return NIL;
	}

	List *originalObjects = stmt->objects;

	stmt->objects = distributedSchemas;

	char *sql = DeparseTreeNode((Node *) stmt);

	stmt->objects = originalObjects;

	return NodeDDLTaskList(NON_COORDINATOR_NODES, list_make1(sql));
}


/*
 * PreprocessAlterSchemaRenameStmt is called when the user is renaming a schema.
 * The invocation happens before the statement is applied locally.
 *
 * As the schema already exists we have access to the ObjectAddress for the schema, this
 * is used to check if the schmea is distributed. If the schema is distributed the rename
 * is executed on all the workers to keep the schemas in sync across the cluster.
 */
List *
PreprocessAlterSchemaRenameStmt(Node *node, const char *queryString)
{
	ObjectAddress schemaAddress = GetObjectAddressFromParseTree(node, false);
	if (!ShouldPropagateObject(&schemaAddress))
	{
		return NIL;
	}

	/* fully qualify */
	QualifyTreeNode(node);

	/* deparse sql*/
	const char *renameStmtSql = DeparseTreeNode(node);

	EnsureSequentialModeForSchemaDDL();

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) renameStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * AlterSchemaRenameStmtObjectAddress returns the ObjectAddress of the schema that is
 * the object of the RenameStmt. Errors if missing_ok is false.
 */
ObjectAddress
AlterSchemaRenameStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_SCHEMA);

	const char *schemaName = stmt->subname;
	Oid schemaOid = get_namespace_oid(schemaName, missing_ok);

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, NamespaceRelationId, schemaOid);

	return address;
}


/*
 * FilterDistributedSchemas filters the schema list and returns the distributed ones
 * as a list
 */
static List *
FilterDistributedSchemas(List *schemas)
{
	List *distributedSchemas = NIL;

	Value *schemaValue = NULL;
	foreach_ptr(schemaValue, schemas)
	{
		const char *schemaName = strVal(schemaValue);
		Oid schemaOid = get_namespace_oid(schemaName, true);

		if (!OidIsValid(schemaOid))
		{
			continue;
		}

		ObjectAddress address = { 0 };
		ObjectAddressSet(address, NamespaceRelationId, schemaOid);

		if (!IsObjectDistributed(&address))
		{
			continue;
		}

		distributedSchemas = lappend(distributedSchemas, schemaValue);
	}

	return distributedSchemas;
}


/*
 * EnsureSequentialModeForSchemaDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the begining.
 *
 * Copy-pasted from type.c
 */
static void
EnsureSequentialModeForSchemaDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify schema because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating or altering a schema, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Schema is created or altered. To make sure subsequent "
							   "commands see the schema correctly we need to make sure to "
							   "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}
