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
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/version_compat.h"
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


/* guc to turn of the automatic type distribution */
bool EnableCreateTypePropagation = true;

/* forward declaration for helper functions*/
static TypeName * MakeTypeNameFromRangeVar(const RangeVar *relation);
static Oid GetTypeOwner(Oid typeOid);
static Oid LookupNonAssociatedArrayTypeNameOid(ParseState *pstate,
											   const TypeName *typeName,
											   bool missing_ok);

/* recreate functions */
static CompositeTypeStmt * RecreateCompositeTypeStmt(Oid typeOid);
static List * CompositeTypeColumnDefList(Oid typeOid);
static CreateEnumStmt * RecreateEnumStmt(Oid typeOid);
static List * EnumValsList(Oid typeOid);

/*
 * PreprocessRenameTypeAttributeStmt is called for changes of attribute names for composite
 * types. Planning is called before the statement is applied locally.
 *
 * For distributed types we apply the attribute renames directly on all the workers to
 * keep the type in sync across the cluster.
 */
List *
PreprocessRenameTypeAttributeStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);
	Assert(stmt->relationType == OBJECT_TYPE);

	ObjectAddress typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);

	const char *sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialMode(OBJECT_TYPE);
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
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

		case TYPTYPE_DOMAIN:
		{
			return (Node *) RecreateDomainStmt(address->objectId);
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
	Assert(get_typtype(typeOid) == TYPTYPE_COMPOSITE);

	CompositeTypeStmt *stmt = makeNode(CompositeTypeStmt);
	List *names = stringToQualifiedNameList(format_type_be_qualified(typeOid));
	stmt->typevar = makeRangeVarFromNameList(names);
	stmt->coldeflist = CompositeTypeColumnDefList(typeOid);

	return stmt;
}


/*
 * attributeFormToColumnDef returns a ColumnDef * describing the field and its property
 * for a pg_attribute entry.
 */
static ColumnDef *
attributeFormToColumnDef(Form_pg_attribute attributeForm)
{
	return makeColumnDef(NameStr(attributeForm->attname),
						 attributeForm->atttypid,
						 attributeForm->atttypmod,
						 attributeForm->attcollation);
}


/*
 * CompositeTypeColumnDefList returns a list of ColumnDef *'s that make up all the fields
 * of the composite type.
 */
static List *
CompositeTypeColumnDefList(Oid typeOid)
{
	List *columnDefs = NIL;

	Oid relationId = typeidTypeRelid(typeOid);
	Relation relation = relation_open(relationId, AccessShareLock);

	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
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
	Assert(get_typtype(typeOid) == TYPTYPE_ENUM);

	CreateEnumStmt *stmt = makeNode(CreateEnumStmt);
	stmt->typeName = stringToQualifiedNameList(format_type_be_qualified(typeOid));
	stmt->vals = EnumValsList(typeOid);

	return stmt;
}


/*
 * EnumValsList returns a list of String values containing the enum values for the given
 * enum type.
 */
static List *
EnumValsList(Oid typeOid)
{
	HeapTuple enum_tuple = NULL;
	ScanKeyData skey = { 0 };

	List *vals = NIL;

	/* Scan pg_enum for the members of the target enum type. */
	ScanKeyInit(&skey,
				Anum_pg_enum_enumtypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typeOid));

	Relation enum_rel = table_open(EnumRelationId, AccessShareLock);
	SysScanDesc enum_scan = systable_beginscan(enum_rel,
											   EnumTypIdSortOrderIndexId,
											   true, NULL,
											   1, &skey);

	/* collect all value names in CREATE TYPE ... AS ENUM stmt */
	while (HeapTupleIsValid(enum_tuple = systable_getnext(enum_scan)))
	{
		Form_pg_enum en = (Form_pg_enum) GETSTRUCT(enum_tuple);
		vals = lappend(vals, makeString(pstrdup(NameStr(en->enumlabel))));
	}

	systable_endscan(enum_scan);
	table_close(enum_rel, AccessShareLock);
	return vals;
}


/*
 * CompositeTypeStmtObjectAddress finds the ObjectAddress for the composite type described
 * by the CompositeTypeStmt. If missing_ok is false this function throws an error if the
 * type does not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
ObjectAddress
CompositeTypeStmtObjectAddress(Node *node, bool missing_ok)
{
	CompositeTypeStmt *stmt = castNode(CompositeTypeStmt, node);
	TypeName *typeName = MakeTypeNameFromRangeVar(stmt->typevar);
	Oid typeOid = LookupNonAssociatedArrayTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * CreateEnumStmtObjectAddress finds the ObjectAddress for the enum type described by the
 * CreateEnumStmt. If missing_ok is false this function throws an error if the type does
 * not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
ObjectAddress
CreateEnumStmtObjectAddress(Node *node, bool missing_ok)
{
	CreateEnumStmt *stmt = castNode(CreateEnumStmt, node);
	TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);
	Oid typeOid = LookupNonAssociatedArrayTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * AlterTypeStmtObjectAddress finds the ObjectAddress for the type described by the ALTER
 * TYPE statement. If missing_ok is false this function throws an error if the type does
 * not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
ObjectAddress
AlterTypeStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_TYPE);

	TypeName *typeName = MakeTypeNameFromRangeVar(stmt->relation);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * AlterEnumStmtObjectAddress return the ObjectAddress of the enum type that is the
 * object of the AlterEnumStmt. Errors is missing_ok is false.
 */
ObjectAddress
AlterEnumStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterEnumStmt *stmt = castNode(AlterEnumStmt, node);
	TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * RenameTypeStmtObjectAddress returns the ObjectAddress of the type that is the object
 * of the RenameStmt. Errors if missing_ok is false.
 */
ObjectAddress
RenameTypeStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_TYPE);

	TypeName *typeName = makeTypeNameFromNameList((List *) stmt->object);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * AlterTypeSchemaStmtObjectAddress returns the ObjectAddress of the type that is the
 * object of the AlterObjectSchemaStmt. Errors if missing_ok is false.
 *
 * This could be called both before or after it has been applied locally. It will look in
 * the old schema first, if the type cannot be found in that schema it will look in the
 * new schema. Errors if missing_ok is false and the type cannot be found in either of the
 * schemas.
 */
ObjectAddress
AlterTypeSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TYPE || stmt->objectType == OBJECT_DOMAIN);

	List *names = (List *) stmt->object;

	/*
	 * we hardcode missing_ok here during LookupTypeNameOid because if we can't find it it
	 * might have already been moved in this transaction.
	 */
	TypeName *typeName = makeTypeNameFromNameList(names);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, true);

	if (typeOid == InvalidOid)
	{
		/*
		 * couldn't find the type, might have already been moved to the new schema, we
		 * construct a new typename that uses the new schema to search in.
		 */

		/* typename is the last in the list of names */
		String *typeNameStr = lfirst(list_tail(names));

		/*
		 * we don't error here either, as the error would be not a good user facing
		 * error if the type didn't exist in the first place.
		 */
		List *newNames = list_make2(makeString(stmt->newschema), typeNameStr);
		TypeName *newTypeName = makeTypeNameFromNameList(newNames);
		typeOid = LookupTypeNameOid(NULL, newTypeName, true);

		/*
		 * if the type is still invalid we couldn't find the type, error with the same
		 * message postgres would error with it missing_ok is false (not ok to miss)
		 */
		if (!missing_ok && typeOid == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("type \"%s\" does not exist",
								   TypeNameToString(typeName))));
		}
	}

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * RenameTypeAttributeStmtObjectAddress returns the ObjectAddress of the type that is the
 * object of the RenameStmt. Errors if missing_ok is false.
 *
 * The ObjectAddress is that of the type, not that of the attributed for which the name is
 * changed as Attributes are not distributed on their own but as a side effect of the
 * whole type distribution.
 */
ObjectAddress
RenameTypeAttributeStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);
	Assert(stmt->relationType == OBJECT_TYPE);

	TypeName *typeName = MakeTypeNameFromRangeVar(stmt->relation);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * AlterTypeOwnerObjectAddress returns the ObjectAddress of the type that is the object
 * of the AlterOwnerStmt. Errors if missing_ok is false.
 */
ObjectAddress
AlterTypeOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_TYPE);

	TypeName *typeName = makeTypeNameFromNameList((List *) stmt->object);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * CreateTypeDDLCommandsIdempotent returns a list of DDL statements (const char *) to be
 * executed on a node to recreate the type addressed by the typeAddress.
 */
List *
CreateTypeDDLCommandsIdempotent(const ObjectAddress *typeAddress)
{
	List *ddlCommands = NIL;
	StringInfoData buf = { 0 };

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

	HeapTuple tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeAddress->objectId));
	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "cache lookup failed for type %u", typeAddress->objectId);
	}

	/* Don't send any command if the type is a table's row type */
	Form_pg_type typTup = (Form_pg_type) GETSTRUCT(tup);
	if (typTup->typtype == TYPTYPE_COMPOSITE &&
		get_rel_relkind(typTup->typrelid) != RELKIND_COMPOSITE_TYPE)
	{
		return NIL;
	}

	Node *stmt = CreateTypeStmtByObjectAddress(typeAddress);

	/* capture ddl command for recreation and wrap in create if not exists construct */
	const char *ddlCommand = DeparseTreeNode(stmt);
	ddlCommand = WrapCreateOrReplace(ddlCommand);
	ddlCommands = lappend(ddlCommands, (void *) ddlCommand);

	/* add owner ship change so the creation command can be run as a different user */
	const char *username = GetUserNameFromId(GetTypeOwner(typeAddress->objectId), false);
	initStringInfo(&buf);
	appendStringInfo(&buf, ALTER_TYPE_OWNER_COMMAND,
					 getObjectIdentity_compat(typeAddress, false),
					 quote_identifier(username));
	ddlCommands = lappend(ddlCommands, buf.data);

	return ddlCommands;
}


/*
 * GenerateBackupNameForTypeCollision generates a new type name for an existing type. The
 * name is generated in such a way that the new name doesn't overlap with an existing type
 * by adding a suffix with incrementing number after the new name.
 */
char *
GenerateBackupNameForTypeCollision(const ObjectAddress *address)
{
	List *names = stringToQualifiedNameList(format_type_be_qualified(address->objectId));
	RangeVar *rel = makeRangeVarFromNameList(names);

	char *newName = palloc0(NAMEDATALEN);
	char suffix[NAMEDATALEN] = { 0 };
	char *baseName = rel->relname;
	int baseLength = strlen(baseName);
	int count = 0;

	while (true)
	{
		int suffixLength = SafeSnprintf(suffix, NAMEDATALEN - 1, "(citus_backup_%d)",
										count);

		/* trim the base name at the end to leave space for the suffix and trailing \0 */
		baseLength = Min(baseLength, NAMEDATALEN - suffixLength - 1);

		/* clear newName before copying the potentially trimmed baseName and suffix */
		memset(newName, 0, NAMEDATALEN);
		strncpy_s(newName, NAMEDATALEN, baseName, baseLength);
		strncpy_s(newName + baseLength, NAMEDATALEN - baseLength, suffix,
				  suffixLength);

		rel->relname = newName;
		TypeName *newTypeName = makeTypeNameFromNameList(MakeNameListFromRangeVar(rel));

		Oid typeOid = LookupTypeNameOid(NULL, newTypeName, true);
		if (typeOid == InvalidOid)
		{
			return newName;
		}

		count++;
	}
}


/*
 * GetTypeOwner
 *
 *		Given the type OID, find its owner
 */
static Oid
GetTypeOwner(Oid typeOid)
{
	Oid result = InvalidOid;

	HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);

		result = typtup->typowner;
		ReleaseSysCache(tp);
	}

	return result;
}


/*
 * MakeTypeNameFromRangeVar creates a TypeName based on a RangeVar.
 */
static TypeName *
MakeTypeNameFromRangeVar(const RangeVar *relation)
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
 * LookupNonAssociatedArrayTypeNameOid returns the oid of the type with the given type name
 * that is not an array type that is associated to another user defined type.
 */
static Oid
LookupNonAssociatedArrayTypeNameOid(ParseState *pstate, const TypeName *typeName,
									bool missing_ok)
{
	Type tup = LookupTypeName(NULL, typeName, NULL, missing_ok);
	Oid typeOid = InvalidOid;
	if (tup != NULL)
	{
		if (((Form_pg_type) GETSTRUCT(tup))->typelem == 0)
		{
			typeOid = ((Form_pg_type) GETSTRUCT(tup))->oid;
		}
		ReleaseSysCache(tup);
	}

	if (!missing_ok && typeOid == InvalidOid)
	{
		elog(ERROR, "type \"%s\" that is not an array type associated with "
					"another type does not exist", TypeNameToString(typeName));
	}

	return typeOid;
}
