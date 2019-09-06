/*-------------------------------------------------------------------------
 *
 * type.c
 *    Commands for TYPE statements.
 *    The following types are supported in citus
 *     - Composite Types
 *     - Enum Types
 *     - Array Types
 *
 *    Types that are currently not supporter:
 *     - Range Types
 *     - Base Types
 *
 *    Range types have a dependency on functions. We can only support Range
 *    types after we have function distribution sorted.
 *
 *    Base types are more complex and often involve c code from extensions.
 *    These types should be created by creating the extension on all the
 *    workers as well. Therefore types created during the creation of an
 *    extension are not propagated to the worker nodes.
 *
 *    Types will be created on the workers during the following situations:
 *     - on type creation (except if called in a transaction)
 *       By not distributing types directly when in a transaction allows
 *       the type to be used in a newly created table that will be
 *       distributed in the same transaction. In that case the type will be
 *       created just-in-time to allow citus' parallelism to work.
 *     - just-in-time
 *       When the type is not already distributed but used in an object
 *       that will distribute now. This allows distributed tables to use
 *       types that have not yet been propagated, either due to the
 *       transaction case abvove, or due to a type predating the citus
 *       extension.
 *     - node activation
 *       Together with all objects that are marked as distributed in citus
 *       types will be created during the activation of a new node to allow
 *       reference tables to use this type.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/namespace.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define AlterEnumIsRename(stmt) (stmt->oldVal != NULL)
#define AlterEnumIsAddValue(stmt) (stmt->oldVal == NULL)


#define ALTER_TYPE_OWNER_COMMAND "ALTER TYPE %s OWNER TO %s;"
#define CREATE_OR_REPLACE_COMMAND "SELECT worker_create_or_replace(%s);"


/* forward declaration for helper functions*/
static List * FilterNameListForDistributedTypes(List *objects, bool missing_ok);
static List * TypeNameListToObjectAddresses(List *objects);
static TypeName * makeTypeNameFromRangeVar(const RangeVar *relation);
static void EnsureSequentialModeForTypeDDL(void);
static Oid get_typowner(Oid typid);
static const char * wrap_in_sql(const char *fmt, const char *sql);

/* recreate functions */
static CompositeTypeStmt * RecreateCompositeTypeStmt(Oid typeOid);
static List * composite_type_coldeflist(Oid typeOid);
static CreateEnumStmt * RecreateEnumStmt(Oid typeOid);
static List * enum_vals_list(Oid typeOid);

static bool ShouldPropagateTypeCreate(void);


List *
PlanCompositeTypeStmt(CompositeTypeStmt *stmt, const char *queryString)
{
	const char *compositeTypeStmtSql = NULL;
	List *commands = NIL;

	if (!ShouldPropagateTypeCreate())
	{
		return NIL;
	}

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will have the object, because they will
	 * either get it now, or get it in master_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/* fully qualify before lookup and later deparsing */
	QualifyTreeNode((Node *) stmt);

	/*
	 * reconstruct creation statement in a portable fashion. The create_or_replace helper
	 * function will be used to create the type in an idempotent manner on the workers.
	 *
	 * Types could exist on the worker prior to being created on the coordinator when the
	 * type previously has been attempted to be created in a transaction which did not
	 * commit on the coordinator.
	 */
	compositeTypeStmtSql = deparse_composite_type_stmt(stmt);
	compositeTypeStmtSql = wrap_in_sql(CREATE_OR_REPLACE_COMMAND, compositeTypeStmtSql);

	/*
	 * when we allow propagation within a transaction block we should make sure to only
	 * allow this in sequential mode
	 */
	EnsureSequentialModeForTypeDDL();

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) compositeTypeStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * ProcessCompositeTypeStmt is executed after the type has been created locally and before
 * we create it on the remote servers. Here we have access to the ObjectAddress of the new
 * type which we use to make sure the type's dependencies are on all nodes.
 */
void
ProcessCompositeTypeStmt(CompositeTypeStmt *stmt, const char *queryString)
{
	const ObjectAddress *typeAddress = NULL;

	/* same check we perform during planning of the statement */
	if (!ShouldPropagateTypeCreate())
	{
		return;
	}

	/*
	 * find object address of the just created object, because the type has been created
	 * locally it can't be missing
	 */
	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	EnsureDependenciesExistsOnAllNodes(typeAddress);

	MarkObjectDistributed(typeAddress);
}


/*
 * PlanAlterTypeStmt is invoked for alter type statements for composite types.
 *
 * Normally we would have a process step as well to re-ensure dependencies exists, however
 * this is already implemented by the post processing for adding columns to tables.
 */
List *
PlanAlterTypeStmt(AlterTableStmt *stmt, const char *queryString)
{
	const char *alterTypeStmtSql = NULL;
	const ObjectAddress *typeAddress = NULL;
	List *commands = NIL;

	Assert(stmt->relkind == OBJECT_TYPE);

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated.
		 */
		return NIL;
	}

	/*
	 * Only distributed types should be propagated
	 */
	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	/* reconstruct alter statement in a portable fashion */
	QualifyTreeNode((Node *) stmt);
	alterTypeStmtSql = DeparseTreeNode((Node *) stmt);

	/*
	 * all types that are distributed will need their alter statements propagated
	 * regardless if in a transaction or not. If we would not propagate the alter
	 * statement the types would be different on worker and coordinator.
	 */
	EnsureSequentialModeForTypeDDL();

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) alterTypeStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


List *
PlanCreateEnumStmt(CreateEnumStmt *stmt, const char *queryString)
{
	const char *createEnumStmtSql = NULL;
	List *commands = NIL;

	if (!ShouldPropagateTypeCreate())
	{
		return NIL;
	}

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	/* enforce fully qualified typeName for correct deparsing and lookup */
	QualifyTreeNode((Node *) stmt);

	/* reconstruct creation statement in a portable fashion */
	createEnumStmtSql = deparse_create_enum_stmt(stmt);
	createEnumStmtSql = wrap_in_sql(CREATE_OR_REPLACE_COMMAND, createEnumStmtSql);

	/*
	 * when we allow propagation within a transaction block we should make sure to only
	 * allow this in sequential mode
	 */
	EnsureSequentialModeForTypeDDL();

	/* to prevent recursion with mx we disable ddl propagation */
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) createEnumStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


void
ProcessCreateEnumStmt(CreateEnumStmt *stmt, const char *queryString)
{
	const ObjectAddress *typeAddress = NULL;

	if (!ShouldPropagateTypeCreate())
	{
		return;
	}

	/* lookup type address of just created type */
	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	EnsureDependenciesExistsOnAllNodes(typeAddress);

	/*
	 * now that the object has been created and distributed to the workers we mark them as
	 * distributed so we know to keep them up to date and recreate on a new node in the
	 * future
	 */
	MarkObjectDistributed(typeAddress);
}


/*
 * PlanAlterEnumStmt handles ALTER TYPE ... ADD VALUE for enum based types.
 */
List *
PlanAlterEnumStmt(AlterEnumStmt *stmt, const char *queryString)
{
	const char *alterEnumStmtSql = NULL;
	const ObjectAddress *typeAddress = NULL;
	List *commands = NIL;

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated here.
		 */
		return NIL;
	}

	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(typeAddress))
	{
		return NIL;
	}

	/*
	 * alter enum will run for all distributed enums, regardless if in a transaction or
	 * not since the enum will be different on the coordinator and workers if we didn't.
	 * (adding values to an enum can not run in a transaction anyway and would error by
	 * postgres already).
	 */
	EnsureSequentialModeForTypeDDL();

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	alterEnumStmtSql = DeparseTreeNode((Node *) stmt);

	/* TODO this is not needed anymore for pg12, alter enum can actually run in a xact */
	if (AlterEnumIsAddValue(stmt))
	{
		int result = 0;

		/*
		 * ADD VALUE can't be executed in a transaction, we will execute optimistically
		 * and on an error we will advise to fix the issue with the worker and rerun the
		 * query with the IF NOT EXTISTS modifier. The modifier is needed as the value
		 * might already be added to some nodes, but not all.
		 */

		commands = list_make2(DISABLE_DDL_PROPAGATION, (void *) alterEnumStmtSql);

		/* TODO function name is unwieldly long, and runs serially which is not nice */
		result = SendBareOptionalCommandListToWorkersAsUser(ALL_WORKERS, commands, NULL);

		if (result != RESPONSE_OKAY)
		{
			const char *alterEnumStmtIfNotExistsSql = NULL;
			bool oldSkipIfNewValueExists = stmt->skipIfNewValExists;

			/* deparse the query with IF NOT EXISTS */
			stmt->skipIfNewValExists = true;
			alterEnumStmtIfNotExistsSql = deparse_alter_enum_stmt(stmt);
			stmt->skipIfNewValExists = oldSkipIfNewValueExists;

			ereport(WARNING, (errmsg("not all workers applied change to enum"),
							  errdetail("retry with: %s", alterEnumStmtIfNotExistsSql),
							  errhint("make sure the coordinators can communicate with "
									  "all workers")));
		}

		return NIL;
	}

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) alterEnumStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PlanDropTypeStmt is called for all DROP TYPE statements. For all types in the list that
 * citus has distributed to the workers it will drop the type on the workers as well. If
 * no types in the drop list are distributed no calls will be made to the workers.
 */
List *
PlanDropTypeStmt(DropStmt *stmt, const char *queryString)
{
	/*
	 * We swap the list of objects to remove during deparse so we need a reference back to
	 * the old list to put back
	 */
	List *oldTypes = stmt->objects;
	List *distributedTypes = NIL;
	const char *dropStmtSql = NULL;
	ListCell *addressCell = NULL;
	List *distributedTypeAddresses = NIL;
	List *commands = NIL;

	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated here.
		 */
		return NIL;
	}

	distributedTypes = FilterNameListForDistributedTypes(oldTypes, stmt->missing_ok);
	if (list_length(distributedTypes) <= 0)
	{
		/* no distributed types to drop */
		return NIL;
	}

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here. MX workers don't have a notion of distributed
	 * types, so we block the call.
	 */
	EnsureCoordinator();

	/*
	 * remove the entries for the distributed objects on dropping
	 */
	distributedTypeAddresses = TypeNameListToObjectAddresses(distributedTypes);
	foreach(addressCell, distributedTypeAddresses)
	{
		ObjectAddress *address = (ObjectAddress *) lfirst(addressCell);
		UnmarkObjectDistributed(address);
	}

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedTypes;
	dropStmtSql = deparse_drop_type_stmt(stmt);
	stmt->objects = oldTypes;

	/* to prevent recursion with mx we disable ddl propagation */
	EnsureSequentialModeForTypeDDL();

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) dropStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


List *
PlanRenameTypeStmt(RenameStmt *stmt, const char *queryString)
{
	const char *renameStmtSql = NULL;
	const ObjectAddress *typeAddress = NULL;
	List *commands = NIL;

	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(typeAddress))
	{
		return NIL;
	}

	/*
	 * we should not get to a point where an alter happens on a distributed type during an
	 * extension statement, but better safe then sorry.
	 */
	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated here.
		 */
		return NIL;
	}


	/* fully qualify */
	QualifyTreeNode((Node *) stmt);

	/* deparse sql*/
	renameStmtSql = DeparseTreeNode((Node *) stmt);

	/* to prevent recursion with mx we disable ddl propagation */
	EnsureSequentialModeForTypeDDL();

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) renameStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


List *
PlanRenameTypeAttributeStmt(RenameStmt *stmt, const char *queryString)
{
	const char *sql = NULL;
	const ObjectAddress *address = NULL;
	List *commands = NIL;

	Assert(stmt->renameType == OBJECT_ATTRIBUTE);
	Assert(stmt->relationType == OBJECT_TYPE);

	address = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(address))
	{
		return NIL;
	}

	/*
	 * we should not get to a point where an alter happens on a distributed type during an
	 * extension statement, but better safe then sorry.
	 */
	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated here.
		 */
		return NIL;
	}

	QualifyTreeNode((Node *) stmt);

	sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForTypeDDL();
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) sql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PlanAlterTypeSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers.
 */
List *
PlanAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt, const char *queryString)
{
	const char *sql = NULL;
	const ObjectAddress *typeAddress = NULL;
	List *commands = NIL;

	Assert(stmt->objectType == OBJECT_TYPE);

	if (creating_extension)
	{
		/* types from extensions are managed by extensions, skipping */
		return NIL;
	}

	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(typeAddress))
	{
		/* not distributed to the workers, nothing to do */
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForTypeDDL();

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) sql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * ProcessAlterTypeSchemaStmt is executed after the change has been applied locally, we
 * can now use the new dependencies of the type to ensure all its dependencies exist on
 * the workers before we apply the commands remotely.
 */
void
ProcessAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt, const char *queryString)
{
	const ObjectAddress *typeAddress = NULL;

	Assert(stmt->objectType == OBJECT_TYPE);

	if (creating_extension)
	{
		/* types from extensions are managed by extensions, skipping */
		return;
	}

	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(typeAddress))
	{
		/* not distributed to the workers, nothing to do */
		return;
	}

	/* dependencies have changed (schema) lets ensure they exist */
	EnsureDependenciesExistsOnAllNodes(typeAddress);
}


List *
PlanAlterTypeOwnerStmt(AlterOwnerStmt *stmt, const char *queryString)
{
	const ObjectAddress *typeAddress = NULL;
	const char *sql = NULL;
	List *commands = NULL;

	Assert(stmt->objectType == OBJECT_TYPE);

	if (creating_extension)
	{
		/* types from extensions are managed by extensions, skipping */
		return NIL;
	}

	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!IsObjectDistributed(typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForTypeDDL();
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) sql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * CreateTypeStmtByObjectAddress returns a parsetree for the CREATE TYPE statement to
 * recreate the type by its object address.
 */
Node *
CreateTypeStmtByObjectAddress(const ObjectAddress *address)
{
	Assert(address->classId == TypeRelationId);

	switch (get_typtype(address->objectId))
	{
		case TYPTYPE_ENUM:
		{
			return (Node *) RecreateEnumStmt(address->objectId);
		}

		case TYPTYPE_COMPOSITE:
		{
			return (Node *) RecreateCompositeTypeStmt(address->objectId);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported type to generate create statement for"),
							errdetail("only enum and composite types can be recreated")));
		}
	}
}


/*
 * RecreateCompositeTypeStmt is called for composite types to create its parsetree for the
 * CREATE TYPE statement that would recreate the composite type.
 */
static CompositeTypeStmt *
RecreateCompositeTypeStmt(Oid typeOid)
{
	CompositeTypeStmt *stmt = NULL;
	List *names = NIL;

	Assert(get_typtype(typeOid) == TYPTYPE_COMPOSITE);

	stmt = makeNode(CompositeTypeStmt);
	names = stringToQualifiedNameList(format_type_be_qualified(typeOid));
	stmt->typevar = makeRangeVarFromNameList(names);
	stmt->coldeflist = composite_type_coldeflist(typeOid);

	return stmt;
}


/*
 * attributeFormToColumnDef returns a ColumnDef * describing the field and its property
 * for a pg_attribute entry.
 *
 * Note: Current implementation is only covering the features supported by composite types
 */
static ColumnDef *
attributeFormToColumnDef(Form_pg_attribute attributeForm)
{
	return makeColumnDef(NameStr(attributeForm->attname),
						 attributeForm->atttypid,
						 -1,
						 attributeForm->attcollation);
}


/*
 * composite_type_coldeflist returns a list of ColumnDef *'s that make up all the fields
 * of the composite type.
 */
static List *
composite_type_coldeflist(Oid typeOid)
{
	Relation relation = NULL;
	Oid relationId = InvalidOid;
	TupleDesc tupleDescriptor = NULL;
	int attributeIndex = 0;
	List *columnDefs = NIL;

	relationId = typeidTypeRelid(typeOid);
	relation = relation_open(relationId, AccessShareLock);

	tupleDescriptor = RelationGetDescr(relation);
	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);

		if (attributeForm->attisdropped)
		{
			/* skip logically hidden attributes */
			continue;
		}

		columnDefs = lappend(columnDefs, attributeFormToColumnDef(attributeForm));
	}

	relation_close(relation, AccessShareLock);

	return columnDefs;
}


/*
 * RecreateEnumStmt returns a parsetree for a CREATE TYPE ... AS ENUM statement that would
 * recreate the given enum type.
 */
static CreateEnumStmt *
RecreateEnumStmt(Oid typeOid)
{
	CreateEnumStmt *stmt = NULL;

	Assert(get_typtype(typeOid) == TYPTYPE_ENUM);

	stmt = makeNode(CreateEnumStmt);
	stmt->typeName = stringToQualifiedNameList(format_type_be_qualified(typeOid));
	stmt->vals = enum_vals_list(typeOid);

	return stmt;
}


/*
 * enum_vals_list returns a list of String values containing the enum values for the given
 * enum type.
 */
static List *
enum_vals_list(Oid typeOid)
{
	Relation enum_rel = NULL;
	SysScanDesc enum_scan = NULL;
	HeapTuple enum_tuple = NULL;
	ScanKeyData skey = { 0 };

	List *vals = NIL;

	/* Scan pg_enum for the members of the target enum type. */
	ScanKeyInit(&skey,
				Anum_pg_enum_enumtypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typeOid));

	enum_rel = heap_open(EnumRelationId, AccessShareLock);
	enum_scan = systable_beginscan(enum_rel,
								   EnumTypIdLabelIndexId,
								   true, NULL,
								   1, &skey);

	/* collect all value names in CREATE TYPE ... AS ENUM stmt */
	while (HeapTupleIsValid(enum_tuple = systable_getnext(enum_scan)))
	{
		Form_pg_enum en = (Form_pg_enum) GETSTRUCT(enum_tuple);
		vals = lappend(vals, makeString(pstrdup(NameStr(en->enumlabel))));
	}

	systable_endscan(enum_scan);
	heap_close(enum_rel, AccessShareLock);
	return vals;
}


/*
 * CompositeTypeStmtObjectAddress finds the ObjectAddress for the composite type described
 * by the  CompositeTypeStmt. If missing_ok is false this function throws an error if the
 * type does not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
const ObjectAddress *
CompositeTypeStmtObjectAddress(CompositeTypeStmt *stmt, bool missing_ok)
{
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	ObjectAddress *address = NULL;

	typeName = makeTypeNameFromRangeVar(stmt->typevar);
	typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


/*
 * CreateEnumStmtObjectAddress finds the ObjectAddress for the enum type described by the
 * CreateEnumStmt. If missing_ok is false this function throws an error if the  type does
 * not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
const ObjectAddress *
CreateEnumStmtObjectAddress(CreateEnumStmt *stmt, bool missing_ok)
{
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	ObjectAddress *address = NULL;

	typeName = makeTypeNameFromNameList(stmt->typeName);
	typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


/*
 * AlterTypeStmtObjectAddress finds the ObjectAddress for the type described by the ALTER
 * TYPE statement. If missing_ok is false this function throws an error if the  type does
 * not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
const ObjectAddress *
AlterTypeStmtObjectAddress(AlterTableStmt *stmt, bool missing_ok)
{
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	ObjectAddress *address = NULL;

	Assert(stmt->relkind == OBJECT_TYPE);

	typeName = makeTypeNameFromRangeVar(stmt->relation);
	typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


const ObjectAddress *
AlterEnumStmtObjectAddress(AlterEnumStmt *stmt, bool missing_ok)
{
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	ObjectAddress *address = NULL;

	typeName = makeTypeNameFromNameList(stmt->typeName);
	typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


const ObjectAddress *
RenameTypeStmtObjectAddress(RenameStmt *stmt, bool missing_ok)
{
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	ObjectAddress *address = NULL;

	Assert(stmt->renameType == OBJECT_TYPE);

	typeName = makeTypeNameFromNameList((List *) stmt->object);
	typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


const ObjectAddress *
AlterTypeSchemaStmtObjectAddress(AlterObjectSchemaStmt *stmt, bool missing_ok)
{
	ObjectAddress *address = NULL;
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	List *names = NIL;

	Assert(stmt->objectType == OBJECT_TYPE);

	names = (List *) stmt->object;

	/*
	 * we hardcode missing_ok here during LookupTypeNameOid because if we can't find it it
	 * might have already been moved in this transaction.
	 */
	typeName = makeTypeNameFromNameList(names);
	typeOid = LookupTypeNameOid(NULL, typeName, true);

	if (typeOid == InvalidOid)
	{
		/*
		 * couldn't find the type, might have already been moved to the new schema, we
		 * construct a new typename that uses the new schema to search in.
		 */

		/* typename is the last in the list of names */
		Value *typeNameStr = lfirst(list_tail(names));

		/*
		 * we don't error here either, as the error would be not a good user facing
		 * error if the type didn't exist in the first place.
		 */
		names = list_make2(makeString(stmt->newschema), typeNameStr);
		typeName = makeTypeNameFromNameList(names);
		typeOid = LookupTypeNameOid(NULL, typeName, true);

		/*
		 * if the type is still invalid we couldn't find the type, error with the same
		 * message postgres would error with it missing_ok is false (not ok to miss)
		 */
		if (!missing_ok && typeOid == InvalidOid)
		{
			names = (List *) stmt->object;
			typeName = makeTypeNameFromNameList(names);

			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("type \"%s\" does not exist",
								   TypeNameToString(typeName))));
		}
	}

	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


const ObjectAddress *
RenameTypeAttributeStmtObjectAddress(RenameStmt *stmt, bool missing_ok)
{
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	ObjectAddress *address = NULL;

	Assert(stmt->renameType == OBJECT_ATTRIBUTE);
	Assert(stmt->relationType == OBJECT_TYPE);

	typeName = makeTypeNameFromRangeVar(stmt->relation);
	typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


const ObjectAddress *
AlterTypeOwnerObjectAddress(AlterOwnerStmt *stmt, bool missing_ok)
{
	TypeName *typeName = NULL;
	Oid typeOid = InvalidOid;
	ObjectAddress *address = NULL;

	Assert(stmt->objectType == OBJECT_TYPE);

	typeName = makeTypeNameFromNameList((List *) stmt->object);
	typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, TypeRelationId, typeOid);

	return address;
}


/*
 * CompositeTypeStmtToDrop returns, given a CREATE TYPE statement, a corresponding
 * statement to drop the type that is to be created. The type does not need to exists in
 * this postgres for this function to succeed.
 */
DropStmt *
CompositeTypeStmtToDrop(CompositeTypeStmt *stmt)
{
	List *names = MakeNameListFromRangeVar(stmt->typevar);
	TypeName *typeName = makeTypeNameFromNameList(names);

	DropStmt *dropStmt = makeNode(DropStmt);
	dropStmt->removeType = OBJECT_TYPE;
	dropStmt->objects = list_make1(typeName);
	return dropStmt;
}


/*
 * CreateEnumStmtToDrop returns, given a CREATE TYPE ... AS ENUM statement, a
 * corresponding statement to drop the type that is to be created. The type does not need
 * to exists in this postgres for this function to succeed.
 */
DropStmt *
CreateEnumStmtToDrop(CreateEnumStmt *stmt)
{
	TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);

	DropStmt *dropStmt = makeNode(DropStmt);
	dropStmt->removeType = OBJECT_TYPE;
	dropStmt->objects = list_make1(typeName);
	return dropStmt;
}


/*
 * CreateTypeDDLCommandsIdempotent returns a list of DDL statements (const char *) to be
 * executed on a node to recreate the type addressed by the typeAddress.
 */
List *
CreateTypeDDLCommandsIdempotent(const ObjectAddress *typeAddress)
{
	List *ddlCommands = NIL;
	const char *ddlCommand = NULL;
	Node *stmt = NULL;
	StringInfoData buf = { 0 };
	const char *username = NULL;

	Assert(typeAddress->classId == TypeRelationId);

	if (type_is_array(typeAddress->objectId))
	{
		/*
		 * array types cannot be created on their own, but could be a direct dependency of
		 * a table. In that case they are on the dependency graph and tried to be created.
		 *
		 * By returning an empty list we will not send any commands to create this type.
		 */
		return NIL;
	}

	stmt = CreateTypeStmtByObjectAddress(typeAddress);

	/* capture ddl command for recreation and wrap in create if not exists construct */
	ddlCommand = DeparseTreeNode(stmt);
	ddlCommand = wrap_in_sql(CREATE_OR_REPLACE_COMMAND, ddlCommand);
	ddlCommands = lappend(ddlCommands, (void *) ddlCommand);

	/* add owner ship change so the creation command can be run as a different user */
	username = GetUserNameFromId(get_typowner(typeAddress->objectId), false);
	initStringInfo(&buf);
	appendStringInfo(&buf, ALTER_TYPE_OWNER_COMMAND, getObjectIdentity(typeAddress),
					 quote_identifier(username));
	ddlCommands = lappend(ddlCommands, buf.data);

	return ddlCommands;
}


/********************************************************************************
 * Section with helper functions
 *********************************************************************************/
const char *
wrap_in_sql(const char *fmt, const char *sql)
{
	StringInfoData buf = { 0 };
	initStringInfo(&buf);
	appendStringInfo(&buf, fmt, quote_literal_cstr(sql));
	return buf.data;
}


/*
 * FilterNameListForDistributedTypes takes a list of objects to delete, for Types this
 * will be a list of TypeName. This list is filtered against the types that are
 * distributed.
 *
 * The original list will not be touched, a new list will be created with only the objects
 * in there.
 */
static List *
FilterNameListForDistributedTypes(List *objects, bool missing_ok)
{
	ListCell *objectCell = NULL;
	List *result = NIL;
	foreach(objectCell, objects)
	{
		TypeName *typeName = castNode(TypeName, lfirst(objectCell));
		Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
		ObjectAddress typeAddress = { 0 };

		if (!OidIsValid(typeOid))
		{
			continue;
		}

		ObjectAddressSet(typeAddress, TypeRelationId, typeOid);
		if (IsObjectDistributed(&typeAddress))
		{
			result = lappend(result, typeName);
		}
	}
	return result;
}


/*
 * TypeNameListToObjectAddresses transforms a List * of TypeName *'s into a List * of
 * ObjectAddress *'s. For this to succeed all Types identiefied by the TypeName *'s should
 * exist on this postgres, an error will be thrown otherwise.
 */
static List *
TypeNameListToObjectAddresses(List *objects)
{
	ListCell *objectCell = NULL;
	List *result = NIL;
	foreach(objectCell, objects)
	{
		TypeName *typeName = castNode(TypeName, lfirst(objectCell));
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		ObjectAddress *typeAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*typeAddress, TypeRelationId, typeOid);
		result = lappend(result, typeAddress);
	}
	return result;
}


/*
 * get_typowner
 *
 *		Given the type OID, find its owner
 */
static Oid
get_typowner(Oid typid)
{
	Oid result = InvalidOid;
	HeapTuple tp = NULL;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);

		result = typtup->typowner;
		ReleaseSysCache(tp);
	}

	return result;
}


/*
 * makeTypeNameFromRangeVar creates a TypeName based on a RangeVar.
 */
static TypeName *
makeTypeNameFromRangeVar(const RangeVar *relation)
{
	List *names = NIL;
	if (relation->schemaname)
	{
		names = lappend(names, makeString(relation->schemaname));
	}
	names = lappend(names, makeString(relation->relname));

	return makeTypeNameFromNameList(names);
}


/*
 * EnsureSequentialModeForTypeDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginnig.
 *
 * As types are node scoped objects there exists only 1 instance of the type used by
 * potentially multiple shards. To make sure all shards in the transaction can interact
 * with the type the type needs to be visible on all connections used by the transaction,
 * meaning we can only use 1 connection per node.
 */
static void
EnsureSequentialModeForTypeDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify type because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating or altering a type, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Type is created or altered. To make sure subsequent "
							   "commands see the type correctly we need to make sure to "
							   "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * ShouldPropagateTypeCreate returns if we should propagate the creation of a type.
 *
 * There are two moments we decide to not directly propagate the creation of a type.
 *  - During the creation of an Extension; we assume the type will be created by creating
 *    the extension on the worker
 *  - During a transaction block; if types are used in a distributed table in the same
 *    block we can only provide parallelism on the table if we do not change to sequentia
 *    mode. Types will be propagated outside of this transaction to the workers so that
 *    the transaction can use 1 connection per shard and fully utilize citus' parallelism
 */
static bool
ShouldPropagateTypeCreate()
{
	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefor not be propagated here.
		 */
		return false;
	}

	/*
	 * by not propagating in a transaction block we allow for parallelism to be used when
	 * this type will be used as a column in a table that will be created and distributed
	 * in this same transaction.
	 */
	if (IsTransactionBlock())
	{
		return false;
	}

	return true;
}
