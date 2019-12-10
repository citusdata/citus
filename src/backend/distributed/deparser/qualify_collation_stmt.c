/*-------------------------------------------------------------------------
 *
 * qualify_collation_stmt.c
 *	  Functions specialized in fully qualifying all collation statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

static Node * QualifyCollationName(List *func);


/*
 * QualifyRenameCollationStmt transforms a
 * ALTER COLLATION .. RENAME TO ..
 * statement in place and makes the collation name fully qualified.
 */
void
QualifyRenameCollationStmt(RenameStmt *stmt)
{
	Assert(stmt->renameType == OBJECT_COLLATION);

	stmt->object = QualifyCollationName(castNode(List, stmt->object));
}


/*
 * QualifyAlterCollationSchemaStmt transforms a
 * ALTER COLLATION .. SET SCHEMA ..
 * statement in place and makes the collation name fully qualified.
 */
void
QualifyAlterCollationSchemaStmt(AlterObjectSchemaStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_COLLATION);

	stmt->object = QualifyCollationName(castNode(List, stmt->object));
}


/*
 * QualifyAlterCollationOwnerStmt transforms a
 * ALTER COLLATION .. OWNER TO ..
 * statement in place and makes the collation name fully qualified.
 */
void
QualifyAlterCollationOwnerStmt(AlterOwnerStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_COLLATION);

	stmt->object = QualifyCollationName(castNode(List, stmt->object));
}


/*
 * QualifyDropCollationStmt transforms a
 * DROP COLLATION ..
 * statement in place and makes the collation name fully qualified.
 */
void
QualifyDropCollationStmt(DropStmt *stmt)
{
	List *names = NIL;
	List *name = NIL;

	foreach_ptr(name, stmt->objects)
	{
		names = lappend(names, QualifyCollationName(name));
	}

	stmt->objects = names;
}


/*
 * QualifyCollation transforms a collation in place and makes its name fully qualified.
 */
Node *
QualifyCollationName(List *name)
{
	char *collationName = NULL;
	char *schemaName = NULL;

	/* check if the collation name is already qualified */
	DeconstructQualifiedName(name, &schemaName, &collationName);

	/* do a lookup for the schema name if the statement does not include one */
	if (schemaName == NULL)
	{
		Oid collid = get_collation_oid(name, true);

		if (collid == InvalidOid)
		{
			return (Node *) name;
		}

		HeapTuple colltup = SearchSysCache1(COLLOID, collid);

		if (!HeapTupleIsValid(colltup))
		{
			return (Node *) name;
		}
		Form_pg_collation collationForm =
			(Form_pg_collation) GETSTRUCT(colltup);

		schemaName = get_namespace_name(collationForm->collnamespace);
		collationName = NameStr(collationForm->collname);
		name = list_make2(makeString(schemaName), makeString(collationName));
		ReleaseSysCache(colltup);
	}

	return (Node *) name;
}
