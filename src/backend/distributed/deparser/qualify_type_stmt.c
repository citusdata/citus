/*-------------------------------------------------------------------------
 *
 * qualify_type_stmt.c
 *	  Functions specialized in fully qualifying all type statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying type statements consists of adding the schema name
 *	  to the subject of the types as well as any other branch of the
 *	  parsetree.
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
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

/*
 * TypeOidGetNamespaceOid resolves the namespace oid for a type identified by its type oid
 */
static Oid
TypeOidGetNamespaceOid(Oid typeOid)
{
	Form_pg_type typeData = NULL;
	Relation catalog = heap_open(TypeRelationId, AccessShareLock);
#if PG_VERSION_NUM >= 120000
	HeapTuple typeTuple = get_catalog_object_by_oid(catalog, Anum_pg_type_oid, typeOid);
#else
	HeapTuple typeTuple = get_catalog_object_by_oid(catalog, typeOid);
#endif
	heap_close(catalog, AccessShareLock);

	typeData = (Form_pg_type) GETSTRUCT(typeTuple);

	return typeData->typnamespace;
}


void
QualifyRenameTypeStmt(RenameStmt *stmt)
{
	List *names = (List *) stmt->object;

	Assert(stmt->renameType == OBJECT_TYPE);

	if (list_length(names) == 1)
	{
		/* not qualified, lookup name and add namespace name to names */
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname),
						   linitial(names));

		stmt->object = (Node *) names;
	}
}


void
QualifyRenameTypeAttributeStmt(RenameStmt *stmt)
{
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);
	Assert(stmt->relationType == OBJECT_TYPE);

	if (stmt->relation->schemaname == NULL)
	{
		List *names = list_make1(makeString(stmt->relation->relname));
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);
		stmt->relation->schemaname = nspname;
	}
}


void
QualifyAlterEnumStmt(AlterEnumStmt *stmt)
{
	List *names = stmt->typeName;

	if (list_length(names) == 1)
	{
		/* not qualified, lookup name and add namespace name to names */
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname),
						   linitial(names));

		stmt->typeName = names;
	}
}


void
QualifyAlterTypeStmt(AlterTableStmt *stmt)
{
	Assert(stmt->relkind == OBJECT_TYPE);

	if (stmt->relation->schemaname == NULL)
	{
		List *names = MakeNameListFromRangeVar(stmt->relation);
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);
		stmt->relation->schemaname = nspname;
	}
}


void
QualifyCompositeTypeStmt(CompositeTypeStmt *stmt)
{
	if (stmt->typevar->schemaname == NULL)
	{
		Oid creationSchema = RangeVarGetCreationNamespace(stmt->typevar);
		stmt->typevar->schemaname = get_namespace_name(creationSchema);
	}
}


void
QualifyCreateEnumStmt(CreateEnumStmt *stmt)
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
QualifyAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt)
{
	List *names = NIL;

	Assert(stmt->objectType == OBJECT_TYPE);

	names = (List *) stmt->object;
	if (list_length(names) == 1)
	{
		/* not qualified with schema, lookup type and its schema s*/
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname), linitial(names));
		stmt->object = (Node *) names;
	}
}


void
QualifyAlterTypeOwnerStmt(AlterOwnerStmt *stmt)
{
	List *names = NIL;

	Assert(stmt->objectType == OBJECT_TYPE);

	names = (List *) stmt->object;
	if (list_length(names) == 1)
	{
		/* not qualified with schema, lookup type and its schema s*/
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname), linitial(names));
		stmt->object = (Node *) names;
	}
}
