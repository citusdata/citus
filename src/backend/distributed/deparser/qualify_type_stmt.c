/*-------------------------------------------------------------------------
 *
 * qualify_type_stmt.c
 *	  Functins specialized in fully qualifying all type statements
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "distributed/deparser.h"
#include "distributed/metadata/namespace.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"

static Oid
type_get_namespace_oid(Oid typeOid)
{
	Form_pg_type typeData = NULL;
	Relation catalog = heap_open(TypeRelationId, AccessShareLock);
	HeapTuple typeTuple = get_catalog_object_by_oid(catalog, typeOid);
	heap_close(catalog, AccessShareLock);

	typeData = (Form_pg_type) GETSTRUCT(typeTuple);

	return typeData->typnamespace;
}


void
qualify_rename_type_stmt(RenameStmt *stmt)
{
	List *names = (List *) stmt->object;

	Assert(stmt->renameType == OBJECT_TYPE);

	if (list_length(names) == 1)
	{
		/* not qualified, lookup name and add namespace name to names */
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = type_get_namespace_oid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname),
						   linitial(names));

		stmt->object = (Node *) names;
	}
}


void
qualify_alter_enum_stmt(AlterEnumStmt *stmt)
{
	List *names = stmt->typeName;

	if (list_length(names) == 1)
	{
		/* not qualified, lookup name and add namespace name to names */
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = type_get_namespace_oid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname),
						   linitial(names));

		stmt->typeName = names;
	}
}


void
qualify_alter_type_stmt(AlterTableStmt *stmt)
{
	Assert(stmt->relkind == OBJECT_TYPE);

	if (stmt->relation->schemaname == NULL)
	{
		List *names = MakeNameListFromRangeVar(stmt->relation);
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = type_get_namespace_oid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);
		stmt->relation->schemaname = nspname;
	}
}


void
qualify_composite_type_stmt(CompositeTypeStmt *stmt)
{
	if (stmt->typevar->schemaname == NULL)
	{
		Oid creationSchema = RangeVarGetCreationNamespace(stmt->typevar);
		stmt->typevar->schemaname = get_namespace_name(creationSchema);
	}
}


void
qualify_create_enum_stmt(CreateEnumStmt *stmt)
{
	if (list_length(stmt->typeName) == 1)
	{
		char *objname = NULL;
		Oid creationSchema = QualifiedNameGetCreationNamespace(stmt->typeName, &objname);
		stmt->typeName = list_make2(makeString(get_namespace_name(creationSchema)),
									linitial(stmt->typeName));
	}
}


void
qualify_alter_type_schema_stmt(AlterObjectSchemaStmt *stmt)
{
	List *names = NIL;

	Assert(stmt->objectType == OBJECT_TYPE);

	names = (List *) stmt->object;
	if (list_length(names) == 1)
	{
		/* not qualified with schema, lookup type and its schema s*/
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = type_get_namespace_oid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname), linitial(names));
		stmt->object = (Node *) names;
	}
}
