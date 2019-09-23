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
void QualifyFunction(ObjectWithArgs *func);
void QualifyFunctionSchemaName(ObjectWithArgs *func);


void
QualifyAlterFunctionStmt(AlterFunctionStmt *stmt)
{
	QualifyFunction(stmt->func);
}


void
QualifyRenameFunctionStmt(RenameStmt *stmt)
{
#if (PG_VERSION_NUM < 110000)
	Assert(stmt->renameType == OBJECT_FUNCTION);
#else
	Assert(stmt->renameType == OBJECT_FUNCTION || stmt->renameType == OBJECT_PROCEDURE);
#endif

	QualifyFunction(castNode(ObjectWithArgs, stmt->object));
}


void
QualifyAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt)
{
#if (PG_VERSION_NUM < 110000)
	Assert(stmt->objectType == OBJECT_FUNCTION);
#else
	Assert(stmt->objectType == OBJECT_FUNCTION || stmt->objectType == OBJECT_PROCEDURE);
#endif

	QualifyFunction(castNode(ObjectWithArgs, stmt->object));
}


void
QualifyAlterFunctionOwnerStmt(AlterOwnerStmt *stmt)
{
#if (PG_VERSION_NUM < 110000)
	Assert(stmt->objectType == OBJECT_FUNCTION);
#else
	Assert(stmt->objectType == OBJECT_FUNCTION || stmt->objectType == OBJECT_PROCEDURE);
#endif

	QualifyFunction(castNode(ObjectWithArgs, stmt->object));
}


void
QualifyAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt)
{
#if (PG_VERSION_NUM < 110000)
	Assert(stmt->objectType == OBJECT_FUNCTION);
#else
	Assert(stmt->objectType == OBJECT_FUNCTION || stmt->objectType == OBJECT_PROCEDURE);
#endif

	QualifyFunction(castNode(ObjectWithArgs, stmt->object));
}


void
QualifyFunction(ObjectWithArgs *func)
{
	char *functionName = NULL;
	char *schemaName = NULL;

	/* check if the function name is already qualified */
	DeconstructQualifiedName(func->objname, &schemaName, &functionName);

	/* do a lookup for the schema name if the statement does not include one */
	if (schemaName == NULL)
	{
		QualifyFunctionSchemaName(func);
	}
}


void
QualifyFunctionSchemaName(ObjectWithArgs *func)
{
	char *schemaName = NULL;
	char *functionName = NULL;

	Oid funcid = InvalidOid;
	HeapTuple proctup;

	funcid = LookupFuncWithArgsCompat(OBJECT_FUNCTION, func, true);
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

		ReleaseSysCache(proctup);

		/* update the function using the schema name */
		func->objname = list_make2(makeString(schemaName), makeString(functionName));
	}
}
