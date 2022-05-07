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


static ObjectAddress GetObjectAddressBySchemaName(char *schemaName, bool missing_ok);
static List * FilterDistributedSchemas(List *schemas);
static bool SchemaHasDistributedTableWithFKey(char *schemaName);
static bool ShouldPropagateCreateSchemaStmt(void);
static List * GetGrantCommandsFromCreateSchemaStmt(Node *node);


/*
 * PreprocessCreateSchemaStmt is called during the planning phase for
 * CREATE SCHEMA ..
 */
List *
PreprocessCreateSchemaStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagateCreateSchemaStmt())
	{
		return NIL;
	}

	EnsureCoordinator();

	EnsureSequentialMode(OBJECT_SCHEMA);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make1(DISABLE_DDL_PROPAGATION);

	/* deparse sql*/
	const char *sql = DeparseTreeNode(node);

	commands = lappend(commands, (void *) sql);

	commands = list_concat(commands, GetGrantCommandsFromCreateSchemaStmt(node));

	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessDropSchemaStmt invalidates the foreign key cache if any table created
 * under dropped schema involved in any foreign key relationship.
 */
List *
PreprocessDropSchemaStmt(Node *node, const char *queryString,
						 ProcessUtilityContext processUtilityContext)
{
	DropStmt *dropStatement = castNode(DropStmt, node);
	Assert(dropStatement->removeType == OBJECT_SCHEMA);

	List *distributedSchemas = FilterDistributedSchemas(dropStatement->objects);

	if (list_length(distributedSchemas) < 1)
	{
		return NIL;
	}

	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	EnsureSequentialMode(OBJECT_SCHEMA);

	String *schemaVal = NULL;
	foreach_ptr(schemaVal, distributedSchemas)
	{
		if (SchemaHasDistributedTableWithFKey(strVal(schemaVal)))
		{
			MarkInvalidateForeignKeyGraph();
			break;
		}
	}

	/*
	 * We swap around the schema's in the statement to only contain the distributed
	 * schemas before deparsing. We need to restore the original list as postgres
	 * will execute on this statement locally, which requires all original schemas
	 * from the user to be present.
	 */
	List *originalObjects = dropStatement->objects;

	dropStatement->objects = distributedSchemas;

	const char *sql = DeparseTreeNode(node);

	dropStatement->objects = originalObjects;

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessGrantOnSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on schemas. Only grant statements for distributed schema are propagated.
 */
List *
PreprocessGrantOnSchemaStmt(Node *node, const char *queryString,
							ProcessUtilityContext processUtilityContext)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SCHEMA);

	List *distributedSchemas = FilterDistributedSchemas(stmt->objects);

	if (list_length(distributedSchemas) == 0)
	{
		return NIL;
	}

	/*
	 * Since access control needs to be handled manually on community, we need to support
	 * such queries by handling them locally on worker nodes.
	 */
	if (!IsCoordinator())
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
PreprocessAlterSchemaRenameStmt(Node *node, const char *queryString,
								ProcessUtilityContext processUtilityContext)
{
	ObjectAddress schemaAddress = GetObjectAddressFromParseTree(node, false);
	if (!ShouldPropagateObject(&schemaAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	/* fully qualify */
	QualifyTreeNode(node);

	/* deparse sql*/
	const char *renameStmtSql = DeparseTreeNode(node);

	EnsureSequentialMode(OBJECT_SCHEMA);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) renameStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * CreateSchemaStmtObjectAddress returns the ObjectAddress of the schema that is
 * the object of the CreateSchemaStmt. Errors if missing_ok is false.
 */
ObjectAddress
CreateSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	CreateSchemaStmt *stmt = castNode(CreateSchemaStmt, node);

	StringInfoData schemaName = { 0 };
	initStringInfo(&schemaName);

	if (stmt->schemaname == NULL)
	{
		/*
		 * If the schema name is not provided, the schema will be created
		 * with the name of the authorizated user.
		 */
		Assert(stmt->authrole != NULL);
		appendStringInfoString(&schemaName, RoleSpecString(stmt->authrole, true));
	}
	else
	{
		appendStringInfoString(&schemaName, stmt->schemaname);
	}

	return GetObjectAddressBySchemaName(schemaName.data, missing_ok);
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

	return GetObjectAddressBySchemaName(stmt->subname, missing_ok);
}


/*
 * GetObjectAddressBySchemaName returns the ObjectAddress of the schema with the
 * given name. Errors out if schema is not found and missing_ok is false.
 */
ObjectAddress
GetObjectAddressBySchemaName(char *schemaName, bool missing_ok)
{
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

	String *schemaValue = NULL;
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
 * SchemaHasDistributedTableWithFKey takes a schema name and scans the relations within
 * that schema. If any one of the relations has a foreign key relationship, it returns
 * true. Returns false otherwise.
 */
static bool
SchemaHasDistributedTableWithFKey(char *schemaName)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;

	Oid namespaceOid = get_namespace_oid(schemaName, true);

	if (namespaceOid == InvalidOid)
	{
		return false;
	}

	Relation pgClass = table_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
				F_OIDEQ, namespaceOid);
	SysScanDesc scanDescriptor = systable_beginscan(pgClass, scanIndexId, useIndex, NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
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

		/* invalidate foreign key cache if the table involved in any foreign key */
		if (TableReferenced(relationId) || TableReferencing(relationId))
		{
			systable_endscan(scanDescriptor);
			table_close(pgClass, NoLock);
			return true;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgClass, NoLock);

	return false;
}


/*
 * ShouldPropagateCreateSchemaStmt gets called only for CreateSchemaStmt's.
 * This function wraps the ShouldPropagate function which is commonly used
 * for all object types; additionally it checks whether there's a multi-statement
 * transaction ongoing or not. For transaction blocks, we require sequential mode
 * with this function, for CREATE SCHEMA statements. If Citus has not already
 * switched to sequential mode, we don't propagate.
 */
static bool
ShouldPropagateCreateSchemaStmt()
{
	if (!ShouldPropagate())
	{
		return false;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return false;
	}

	return true;
}


/*
 * GetGrantCommandsFromCreateSchemaStmt takes a CreateSchemaStmt and returns the
 * list of deparsed queries of the inner GRANT commands of the given statement.
 * Ignores commands other than GRANT statements.
 */
static List *
GetGrantCommandsFromCreateSchemaStmt(Node *node)
{
	List *commands = NIL;
	CreateSchemaStmt *stmt = castNode(CreateSchemaStmt, node);

	Node *element = NULL;
	foreach_ptr(element, stmt->schemaElts)
	{
		if (!IsA(element, GrantStmt))
		{
			continue;
		}

		GrantStmt *grantStmt = castNode(GrantStmt, element);

		switch (grantStmt->objtype)
		{
			/* we only propagate GRANT ON SCHEMA in community */
			case OBJECT_SCHEMA:
			{
				commands = lappend(commands, DeparseGrantOnSchemaStmt(element));
				break;
			}

			default:
			{
				break;
			}
		}
	}

	return commands;
}
