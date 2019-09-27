/*-------------------------------------------------------------------------
 *
 * qualify_function_stmt.c
 *	  Functions specialized in fully qualifying all function statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying function statements consists of adding the schema name
 *	  to the subject of the function and types as well as any other branch of
 *    the parsetree.
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
#include "catalog/pg_proc.h"
#include "distributed/deparser.h"
#include "distributed/version_compat.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* forward declaration for qualify functions */
static void QualifyFunction(ObjectWithArgs *func, ObjectType type);
static void QualifyFunctionSchemaName(ObjectWithArgs *func, ObjectType type);


/* AssertObjectTypeIsFunctionType asserts we aren't receiving something we shouldn't */
void
AssertObjectTypeIsFunctional(ObjectType type)
{
	Assert(type == OBJECT_AGGREGATE || type == OBJECT_FUNCTION || type ==
		   OBJECT_PROCEDURE);
}


/*
 * QualifyAlterFunctionStmt transforms a
 * ALTER {AGGREGATE|FUNCTION|PROCEDURE} ..
 * statement in place and makes all (supported) statements fully qualified.
 *
 * Note that not all queries of this form are valid AlterFunctionStmt
 * (e.g. ALTER FUNCTION .. RENAME .. queries are RenameStmt )
 */
void
QualifyAlterFunctionStmt(AlterFunctionStmt *stmt)
{
	AssertObjectTypeIsFunctional(stmt->objtype);

	QualifyFunction(stmt->func, stmt->objtype);
}


/*
 * QualifyRenameFunctionStmt transforms a
 * ALTER {AGGREGATE|FUNCTION|PROCEDURE} .. RENAME TO ..
 * statement in place and makes the function name fully qualified.
 */
void
QualifyRenameFunctionStmt(RenameStmt *stmt)
{
	AssertObjectTypeIsFunctional(stmt->renameType);

	QualifyFunction(castNode(ObjectWithArgs, stmt->object), stmt->renameType);
}


/*
 * QualifyAlterFunctionSchemaStmt transforms a
 * ALTER {AGGREGATE|FUNCTION|PROCEDURE} .. SET SCHEMA ..
 * statement in place and makes the function name fully qualified.
 */
void
QualifyAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt)
{
	AssertObjectTypeIsFunctional(stmt->objectType);

	QualifyFunction(castNode(ObjectWithArgs, stmt->object), stmt->objectType);
}


/*
 * QualifyAlterFunctionOwnerStmt transforms a
 * ALTER {AGGREGATE|FUNCTION|PROCEDURE} .. OWNER TO ..
 * statement in place and makes the function name fully qualified.
 */
void
QualifyAlterFunctionOwnerStmt(AlterOwnerStmt *stmt)
{
	AssertObjectTypeIsFunctional(stmt->objectType);

	QualifyFunction(castNode(ObjectWithArgs, stmt->object), stmt->objectType);
}


/*
 * QualifyAlterFunctionDependsStmt transforms a
 * ALTER {FUNCTION|PROCEDURE} .. DEPENDS ON EXTENSION ..
 * statement in place and makes the function name fully qualified.
 */
void
QualifyAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt)
{
	AssertObjectTypeIsFunctional(stmt->objectType);

	QualifyFunction(castNode(ObjectWithArgs, stmt->object), stmt->objectType);
}


/*
 * QualifyFunction transforms a function in place and makes it's name fully qualified.
 */
void
QualifyFunction(ObjectWithArgs *func, ObjectType type)
{
	char *functionName = NULL;
	char *schemaName = NULL;

	/* check if the function name is already qualified */
	DeconstructQualifiedName(func->objname, &schemaName, &functionName);

	/* do a lookup for the schema name if the statement does not include one */
	if (schemaName == NULL)
	{
		QualifyFunctionSchemaName(func, type);
	}
}


/*
 * QualifyFunction transforms a function in place using a catalog lookup for its schema name to make it fully qualified.
 */
void
QualifyFunctionSchemaName(ObjectWithArgs *func, ObjectType type)
{
	char *schemaName = NULL;
	char *functionName = NULL;
	Oid funcid = InvalidOid;
	HeapTuple proctup;

	funcid = LookupFuncWithArgs(type, func, true);
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

	/*
	 * We can not qualify the function if the catalogs do not have any records.
	 *
	 * e.g. DROP FUNC IF EXISTS non_existent_func() do not result in a valid heap tuple
	 */
	if (HeapTupleIsValid(proctup))
	{
		Form_pg_proc procform;

		procform = (Form_pg_proc) GETSTRUCT(proctup);
		schemaName = get_namespace_name(procform->pronamespace);
		functionName = NameStr(procform->proname);
		functionName = pstrdup(functionName); /* we release the tuple before used */

		ReleaseSysCache(proctup);

		/* update the function using the schema name */
		func->objname = list_make2(makeString(schemaName), makeString(functionName));
	}
}
