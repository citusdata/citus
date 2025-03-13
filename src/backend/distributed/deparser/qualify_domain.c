/*-------------------------------------------------------------------------
 *
 * qualify_domain.c
 *    Functions to fully qualify, make the statements independent of
 *    search_path settings, for all domain related statements. This
 *    mostly consists of adding the schema name to all the domain
 *    names referencing domains.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"


static void QualifyTypeName(TypeName *typeName, bool missing_ok);
static void QualifyCollate(CollateClause *collClause, bool missing_ok);


/*
 * QualifyCreateDomainStmt modifies the CreateDomainStmt passed to become search_path
 * independent.
 */
void
QualifyCreateDomainStmt(Node *node)
{
	CreateDomainStmt *stmt = castNode(CreateDomainStmt, node);

	char *schemaName = NULL;
	char *domainName = NULL;

	/* fully qualify domain name */
	DeconstructQualifiedName(stmt->domainname, &schemaName, &domainName);
	if (!schemaName)
	{
		RangeVar *var = makeRangeVarFromNameList(stmt->domainname);
		Oid creationSchema = RangeVarGetCreationNamespace(var);
		schemaName = get_namespace_name(creationSchema);

		stmt->domainname = list_make2(makeString(schemaName), makeString(domainName));
	}

	/* referenced types should be fully qualified */
	QualifyTypeName(stmt->typeName, false);
	QualifyCollate(stmt->collClause, false);
}


/*
 * QualifyDropDomainStmt modifies the DropStmt for DOMAIN's to be search_path independent.
 */
void
QualifyDropDomainStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);

	TypeName *domainName = NULL;
	foreach_declared_ptr(domainName, stmt->objects)
	{
		QualifyTypeName(domainName, stmt->missing_ok);
	}
}


/*
 * QualifyAlterDomainStmt modifies the AlterDomainStmt to be search_path independent.
 */
void
QualifyAlterDomainStmt(Node *node)
{
	AlterDomainStmt *stmt = castNode(AlterDomainStmt, node);

	if (list_length(stmt->typeName) == 1)
	{
		TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);
		QualifyTypeName(typeName, false);
		stmt->typeName = typeName->names;
	}
}


/*
 * QualifyDomainRenameConstraintStmt modifies the RenameStmt for domain constraints to be
 * search_path independent.
 */
void
QualifyDomainRenameConstraintStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_DOMCONSTRAINT);

	List *domainName = castNode(List, stmt->object);
	if (list_length(domainName) == 1)
	{
		TypeName *typeName = makeTypeNameFromNameList(domainName);
		QualifyTypeName(typeName, false);
		stmt->object = (Node *) typeName->names;
	}
}


/*
 * QualifyAlterDomainOwnerStmt modifies the AlterOwnerStmt for DOMAIN's to be search_oath
 * independent.
 */
void
QualifyAlterDomainOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DOMAIN);

	List *domainName = castNode(List, stmt->object);
	if (list_length(domainName) == 1)
	{
		TypeName *typeName = makeTypeNameFromNameList(domainName);
		QualifyTypeName(typeName, false);
		stmt->object = (Node *) typeName->names;
	}
}


/*
 * QualifyRenameDomainStmt modifies the RenameStmt for the Domain to be search_path
 * independent.
 */
void
QualifyRenameDomainStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_DOMAIN);

	List *domainName = castNode(List, stmt->object);
	if (list_length(domainName) == 1)
	{
		TypeName *typeName = makeTypeNameFromNameList(domainName);
		QualifyTypeName(typeName, false);
		stmt->object = (Node *) typeName->names;
	}
}


/*
 * QualifyAlterDomainSchemaStmt modifies the AlterObjectSchemaStmt to be search_path
 * independent.
 */
void
QualifyAlterDomainSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_DOMAIN);

	List *domainName = castNode(List, stmt->object);
	if (list_length(domainName) == 1)
	{
		TypeName *typeName = makeTypeNameFromNameList(domainName);
		QualifyTypeName(typeName, false);
		stmt->object = (Node *) typeName->names;
	}
}


/*
 * QualifyTypeName qualifies a TypeName object in place. When missing_ok is false it might
 * throw an error if the type can't be found based on its name. When an oid is provided
 * missing_ok is ignored and treated as false. Meaning, even if missing_ok is true the
 * function might raise an error for non-existing types if the oid can't be found.
 */
static void
QualifyTypeName(TypeName *typeName, bool missing_ok)
{
	if (OidIsValid(typeName->typeOid))
	{
		/*
		 * When the typeName is provided as oid, fill in the names.
		 * missing_ok is ignored for oid's
		 */
		Type typeTup = typeidType(typeName->typeOid);

		char *name = typeTypeName(typeTup);

		Oid namespaceOid = TypeOidGetNamespaceOid(typeName->typeOid);
		char *schemaName = get_namespace_name(namespaceOid);

		typeName->names = list_make2(makeString(schemaName), makeString(name));

		ReleaseSysCache(typeTup);
	}
	else
	{
		char *name = NULL;
		char *schemaName = NULL;
		DeconstructQualifiedName(typeName->names, &schemaName, &name);
		if (!schemaName)
		{
			Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
			if (OidIsValid(typeOid))
			{
				Oid namespaceOid = TypeOidGetNamespaceOid(typeOid);
				schemaName = get_namespace_name(namespaceOid);

				typeName->names = list_make2(makeString(schemaName), makeString(name));
			}
		}
	}
}


/*
 * QualifyCollate qualifies any given CollateClause by adding any missing schema name to
 * the collation being identified.
 *
 * If collClause is a NULL pointer this function is a no-nop.
 */
static void
QualifyCollate(CollateClause *collClause, bool missing_ok)
{
	if (collClause == NULL)
	{
		/* no collate clause, nothing to qualify*/
		return;
	}

	if (list_length(collClause->collname) != 1)
	{
		/* already qualified */
		return;
	}

	Oid collOid = get_collation_oid(collClause->collname, missing_ok);
	ObjectAddress collationAddress = { 0 };
	ObjectAddressSet(collationAddress, CollationRelationId, collOid);

	List *objName = NIL;
	List *objArgs = NIL;

	getObjectIdentityParts(&collationAddress, &objName, &objArgs, false);

	collClause->collname = NIL;
	char *name = NULL;
	foreach_declared_ptr(name, objName)
	{
		collClause->collname = lappend(collClause->collname, makeString(name));
	}
}
