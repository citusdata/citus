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
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"

#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/version_compat.h"


static List * GetObjectAddressBySchemaName(char *schemaName, bool missing_ok);
static List * FilterDistributedSchemas(List *schemas);
static bool SchemaHasDistributedTableWithFKey(char *schemaName);
static bool ShouldPropagateCreateSchemaStmt(void);
static List * GetGrantCommandsFromCreateSchemaStmt(Node *node);
static bool CreateSchemaStmtCreatesTable(CreateSchemaStmt *stmt);


/*
 * PostprocessCreateSchemaStmt is called during the planning phase for
 * CREATE SCHEMA ..
 */
List *
PostprocessCreateSchemaStmt(Node *node, const char *queryString)
{
	CreateSchemaStmt *createSchemaStmt = castNode(CreateSchemaStmt, node);

	if (!ShouldPropagateCreateSchemaStmt())
	{
		return NIL;
	}

	EnsureCoordinator();

	EnsureSequentialMode(OBJECT_SCHEMA);

	bool missingOk = createSchemaStmt->if_not_exists;
	List *schemaAdressList = CreateSchemaStmtObjectAddress(node, missingOk, true);
	Assert(list_length(schemaAdressList) == 1);
	ObjectAddress *schemaAdress = linitial(schemaAdressList);
	Oid schemaId = schemaAdress->objectId;
	if (!OidIsValid(schemaId))
	{
		return NIL;
	}

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make1(DISABLE_DDL_PROPAGATION);

	/* deparse sql*/
	const char *sql = DeparseTreeNode(node);

	commands = lappend(commands, (void *) sql);

	commands = list_concat(commands, GetGrantCommandsFromCreateSchemaStmt(node));

	char *schemaName = get_namespace_name(schemaId);
	if (ShouldUseSchemaBasedSharding(schemaName))
	{
		/* for now, we don't allow creating tenant tables when creating the schema itself */
		if (CreateSchemaStmtCreatesTable(createSchemaStmt))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create distributed schema and table in a "
								   "single statement"),
							errhint("SET citus.enable_schema_based_sharding TO off, "
									"or create the schema and table in separate "
									"commands.")));
		}

		/*
		 * Skip if the schema is already inserted into pg_dist_schema.
		 * This could occur when trying to create an already existing schema,
		 * with IF NOT EXISTS clause.
		 */
		if (!IsTenantSchema(schemaId))
		{
			/*
			 * Register the tenant schema on the coordinator and save the command
			 * to register it on the workers.
			 */
			int shardCount = 1;
			int replicationFactor = 1;
			Oid distributionColumnType = InvalidOid;
			Oid distributionColumnCollation = InvalidOid;
			uint32 colocationId = CreateColocationGroup(
				shardCount, replicationFactor, distributionColumnType,
				distributionColumnCollation);

			InsertTenantSchemaLocally(schemaId, colocationId);

			commands = lappend(commands, TenantSchemaInsertCommand(schemaId,
																   colocationId));
		}
	}

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
	foreach_declared_ptr(schemaVal, distributedSchemas)
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
	if (!ShouldPropagate())
	{
		return NIL;
	}

	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SCHEMA);

	List *distributedSchemas = FilterDistributedSchemas(stmt->objects);

	if (list_length(distributedSchemas) == 0)
	{
		return NIL;
	}

	EnsureCoordinator();

	List *originalObjects = stmt->objects;

	stmt->objects = distributedSchemas;

	char *sql = DeparseTreeNode((Node *) stmt);

	stmt->objects = originalObjects;

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * CreateSchemaStmtObjectAddress returns the ObjectAddress of the schema that is
 * the object of the CreateSchemaStmt. Errors if missing_ok is false.
 */
List *
CreateSchemaStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
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
 * AlterSchemaOwnerStmtObjectAddress returns the ObjectAddress of the schema that is
 * the object of the AlterOwnerStmt. Errors if missing_ok is false.
 */
List *
AlterSchemaOwnerStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_SCHEMA);

	return GetObjectAddressBySchemaName(strVal(stmt->object), missing_ok);
}


/*
 * AlterSchemaRenameStmtObjectAddress returns the ObjectAddress of the schema that is
 * the object of the RenameStmt. Errors if missing_ok is false.
 */
List *
AlterSchemaRenameStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_SCHEMA);

	return GetObjectAddressBySchemaName(stmt->subname, missing_ok);
}


/*
 * GetObjectAddressBySchemaName returns the ObjectAddress of the schema with the
 * given name. Errors out if schema is not found and missing_ok is false.
 */
List *
GetObjectAddressBySchemaName(char *schemaName, bool missing_ok)
{
	Oid schemaOid = get_namespace_oid(schemaName, missing_ok);

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, NamespaceRelationId, schemaOid);

	return list_make1(address);
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
	foreach_declared_ptr(schemaValue, schemas)
	{
		const char *schemaName = strVal(schemaValue);
		Oid schemaOid = get_namespace_oid(schemaName, true);

		if (!OidIsValid(schemaOid))
		{
			continue;
		}

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*address, NamespaceRelationId, schemaOid);
		if (!IsAnyObjectDistributed(list_make1(address)))
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
				F_OIDEQ, ObjectIdGetDatum(namespaceOid));
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
	foreach_declared_ptr(element, stmt->schemaElts)
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


/*
 * CreateSchemaStmtCreatesTable returns true if given CreateSchemaStmt
 * creates a table using "schema_element" list.
 */
static bool
CreateSchemaStmtCreatesTable(CreateSchemaStmt *stmt)
{
	Node *element = NULL;
	foreach_declared_ptr(element, stmt->schemaElts)
	{
		/*
		 * CREATE TABLE AS and CREATE FOREIGN TABLE commands cannot be
		 * used as schema_elements anyway, so we don't need to check them.
		 */
		if (IsA(element, CreateStmt))
		{
			return true;
		}
	}

	return false;
}
