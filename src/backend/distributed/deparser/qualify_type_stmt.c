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
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

static char * GetTypeNamespaceNameByNameList(List *names);
static Oid TypeOidGetNamespaceOid(Oid typeOid);

/*
 * GetTypeNamespaceNameByNameList resolved the schema name of a type by its namelist.
 */
static char *
GetTypeNamespaceNameByNameList(List *names)
{
	TypeName *typeName = makeTypeNameFromNameList(names);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
	char *nspname = get_namespace_name_or_temp(namespaceOid);
	return nspname;
}


/*
 * TypeOidGetNamespaceOid resolves the namespace oid for a type identified by its type oid
 */
static Oid
TypeOidGetNamespaceOid(Oid typeOid)
{
	HeapTuple typeTuple = SearchSysCache1(TYPEOID, typeOid);

	if (!HeapTupleIsValid(typeTuple))
	{
		elog(ERROR, "citus cache lookup failed");
		return InvalidOid;
	}
	Form_pg_type typeData = (Form_pg_type) GETSTRUCT(typeTuple);
	Oid typnamespace = typeData->typnamespace;

	ReleaseSysCache(typeTuple);

	return typnamespace;
}


void
QualifyRenameTypeStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	List *names = (List *) stmt->object;

	Assert(stmt->renameType == OBJECT_TYPE);

	if (list_length(names) == 1)
	{
		/* not qualified, lookup name and add namespace name to names */
		char *nspname = GetTypeNamespaceNameByNameList(names);
		names = list_make2(makeString(nspname), linitial(names));

		stmt->object = (Node *) names;
	}
}


void
QualifyRenameTypeAttributeStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);
	Assert(stmt->relationType == OBJECT_TYPE);

	if (stmt->relation->schemaname == NULL)
	{
		List *names = list_make1(makeString(stmt->relation->relname));
		char *nspname = GetTypeNamespaceNameByNameList(names);
		stmt->relation->schemaname = nspname;
	}
}


void
QualifyAlterEnumStmt(Node *node)
{
	AlterEnumStmt *stmt = castNode(AlterEnumStmt, node);
	List *names = stmt->typeName;

	if (list_length(names) == 1)
	{
		/* not qualified, lookup name and add namespace name to names */
		char *nspname = GetTypeNamespaceNameByNameList(names);
		names = list_make2(makeString(nspname), linitial(names));

		stmt->typeName = names;
	}
}


void
QualifyAlterTypeStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(stmt->relkind == OBJECT_TYPE);

	if (stmt->relation->schemaname == NULL)
	{
		List *names = MakeNameListFromRangeVar(stmt->relation);
		char *nspname = GetTypeNamespaceNameByNameList(names);
		stmt->relation->schemaname = nspname;
	}
}


void
QualifyCompositeTypeStmt(Node *node)
{
	CompositeTypeStmt *stmt = castNode(CompositeTypeStmt, node);

	if (stmt->typevar->schemaname == NULL)
	{
		Oid creationSchema = RangeVarGetCreationNamespace(stmt->typevar);
		stmt->typevar->schemaname = get_namespace_name(creationSchema);
	}
}


void
QualifyCreateEnumStmt(Node *node)
{
	CreateEnumStmt *stmt = castNode(CreateEnumStmt, node);

	if (list_length(stmt->typeName) == 1)
	{
		char *objname = NULL;
		Oid creationSchema = QualifiedNameGetCreationNamespace(stmt->typeName, &objname);
		stmt->typeName = list_make2(makeString(get_namespace_name(creationSchema)),
									linitial(stmt->typeName));
	}
}


void
QualifyAlterTypeSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TYPE);

	List *names = (List *) stmt->object;
	if (list_length(names) == 1)
	{
		/* not qualified with schema, lookup type and its schema s*/
		char *nspname = GetTypeNamespaceNameByNameList(names);
		names = list_make2(makeString(nspname), linitial(names));
		stmt->object = (Node *) names;
	}
}


void
QualifyAlterTypeOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_TYPE);

	List *names = (List *) stmt->object;
	if (list_length(names) == 1)
	{
		/* not qualified with schema, lookup type and its schema s*/
		char *nspname = GetTypeNamespaceNameByNameList(names);
		names = list_make2(makeString(nspname), linitial(names));
		stmt->object = (Node *) names;
	}
}
