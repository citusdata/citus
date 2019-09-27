/*-------------------------------------------------------------------------
 *
 * deparse_function_stmts.c
 *
 *	  All routines to deparse function and procedure statements.
 *	  This file contains all entry points specific for function and procedure statement
 *    deparsing
 *
 *	  Functions that could move later are AppendDefElem, AppendDefElemStrict, etc. These
 *	  should be reused across multiple statements and should live in their own deparse
 *	  file.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/value.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


/* forward declaration for deparse functions */
static void AppendAlterFunctionStmt(StringInfo buf, AlterFunctionStmt *stmt);
static void AppendDropFunctionStmt(StringInfo buf, DropStmt *stmt);
static void AppendFunctionName(StringInfo buf, ObjectWithArgs *func, ObjectType objtype);
static void AppendFunctionNameList(StringInfo buf, List *objects, ObjectType objtype);

static void AppendDefElem(StringInfo buf, DefElem *def);
static void AppendDefElemStrict(StringInfo buf, DefElem *def);
static void AppendDefElemVolatility(StringInfo buf, DefElem *def);
static void AppendDefElemLeakproof(StringInfo buf, DefElem *def);
static void AppendDefElemSecurity(StringInfo buf, DefElem *def);
static void AppendDefElemParallel(StringInfo buf, DefElem *def);
static void AppendDefElemCost(StringInfo buf, DefElem *def);
static void AppendDefElemRows(StringInfo buf, DefElem *def);
static void AppendDefElemSet(StringInfo buf, DefElem *def);

static void AppendRenameFunctionStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterFunctionSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterFunctionOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendAlterFunctionDependsStmt(StringInfo buf, AlterObjectDependsStmt *stmt);

static char * CopyAndConvertToUpperCase(const char *str);

/*
 * DeparseAlterFunctionStmt builds and returns a string representing the AlterFunctionStmt
 */
const char *
DeparseAlterFunctionStmt(AlterFunctionStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterFunctionStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterFunctionStmt appends a string representing the AlterFunctionStmt to a buffer
 */
static void
AppendAlterFunctionStmt(StringInfo buf, AlterFunctionStmt *stmt)
{
	ListCell *actionCell = NULL;

	if (stmt->objtype == OBJECT_FUNCTION)
	{
		appendStringInfo(buf, "ALTER FUNCTION ");
	}
	else if (stmt->objtype == OBJECT_PROCEDURE)
	{
		appendStringInfo(buf, "ALTER PROCEDURE ");
	}
	else
	{
		appendStringInfo(buf, "ALTER AGGREGATE ");
	}

	AppendFunctionName(buf, stmt->func, stmt->objtype);

	foreach(actionCell, stmt->actions)
	{
		DefElem *def = castNode(DefElem, lfirst(actionCell));
		AppendDefElem(buf, def);
	}

	appendStringInfoString(buf, ";");
}


/*
 * AppendDefElem appends a string representing the DefElem to a buffer
 */
static void
AppendDefElem(StringInfo buf, DefElem *def)
{
	if (strcmp(def->defname, "strict") == 0)
	{
		AppendDefElemStrict(buf, def);
	}
	else if (strcmp(def->defname, "volatility") == 0)
	{
		AppendDefElemVolatility(buf, def);
	}
	else if (strcmp(def->defname, "leakproof") == 0)
	{
		AppendDefElemLeakproof(buf, def);
	}
	else if (strcmp(def->defname, "security") == 0)
	{
		AppendDefElemSecurity(buf, def);
	}
	else if (strcmp(def->defname, "parallel") == 0)
	{
		AppendDefElemParallel(buf, def);
	}
	else if (strcmp(def->defname, "cost") == 0)
	{
		AppendDefElemCost(buf, def);
	}
	else if (strcmp(def->defname, "rows") == 0)
	{
		AppendDefElemRows(buf, def);
	}
	else if (strcmp(def->defname, "set") == 0)
	{
		AppendDefElemSet(buf, def);
	}
}


/*
 * AppendDefElemStrict appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemStrict(StringInfo buf, DefElem *def)
{
	if (intVal(def->arg) == 1)
	{
		appendStringInfo(buf, " STRICT");
	}
	else
	{
		appendStringInfo(buf, " CALLED ON NULL INPUT");
	}
}


/*
 * AppendDefElemVolatility appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemVolatility(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " %s", CopyAndConvertToUpperCase(strVal(def->arg)));
}


/*
 * AppendDefElemLeakproof appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemLeakproof(StringInfo buf, DefElem *def)
{
	if (intVal(def->arg) == 0)
	{
		appendStringInfo(buf, " NOT");
	}
	appendStringInfo(buf, " LEAKPROOF");
}


/*
 * AppendDefElemSecurity appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemSecurity(StringInfo buf, DefElem *def)
{
	if (intVal(def->arg) == 0)
	{
		appendStringInfo(buf, " SECURITY INVOKER");
	}
	else
	{
		appendStringInfo(buf, " SECURITY DEFINER");
	}
}


/*
 * AppendDefElemParallel appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemParallel(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " PARALLEL %s", CopyAndConvertToUpperCase(strVal(def->arg)));
}


/*
 * AppendDefElemCost appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemCost(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " COST %lf", defGetNumeric(def));
}


/*
 * AppendDefElemRows appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemRows(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " ROWS  %lf", defGetNumeric(def));
}


/*
 * AppendDefElemSet appends a string representing the DefElem to a buffer
 */
static void
AppendDefElemSet(StringInfo buf, DefElem *def)
{
	VariableSetStmt *setStmt = castNode(VariableSetStmt, def->arg);
	char *setVariableArgs = ExtractSetVariableArgs(setStmt);

	switch (setStmt->kind)
	{
		case VAR_SET_VALUE:
		{
			appendStringInfo(buf, " SET %s = %s", setStmt->name, setVariableArgs);
			break;
		}

		case VAR_SET_CURRENT:
		{
			appendStringInfo(buf, " SET %s FROM CURRENT", setStmt->name);
			break;
		}

		case VAR_SET_DEFAULT:
		{
			appendStringInfo(buf, " SET %s TO DEFAULT", setStmt->name);
			break;
		}

		case VAR_RESET:
		{
			appendStringInfo(buf, " RESET %s", setStmt->name);
			break;
		}

		case VAR_RESET_ALL:
		{
			appendStringInfoString(buf, " RESET ALL");
			break;
		}

		/* VAR_SET_MULTI is a special case for SET TRANSACTION that should not occur here */
		case VAR_SET_MULTI:
		default:
		{
			ereport(ERROR, (errmsg("Unable to deparse SET statement")));
			break;
		}
	}
}


/*
 * DeparseRenameFunctionStmt builds and returns a string representing the RenameStmt
 */
const char *
DeparseRenameFunctionStmt(RenameStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AssertObjectTypeIsFunctional(stmt->renameType);

	AppendRenameFunctionStmt(&str, stmt);

	return str.data;
}


/*
 * AppendRenameFunctionStmt appends a string representing the RenameStmt to a buffer
 */
static void
AppendRenameFunctionStmt(StringInfo buf, RenameStmt *stmt)
{
	ObjectWithArgs *func = castNode(ObjectWithArgs, stmt->object);

	if (stmt->renameType == OBJECT_FUNCTION)
	{
		appendStringInfoString(buf, "ALTER FUNCTION ");
	}
	else if (stmt->renameType == OBJECT_PROCEDURE)
	{
		appendStringInfoString(buf, "ALTER PROCEDURE ");
	}
	else
	{
		appendStringInfoString(buf, "ALTER AGGREGATE ");
	}

	AppendFunctionName(buf, func, stmt->renameType);

	appendStringInfo(buf, " RENAME TO %s;", quote_identifier(stmt->newname));
}


/*
 * DeparseAlterFunctionSchemaStmt builds and returns a string representing the AlterObjectSchemaStmt
 */
const char *
DeparseAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AssertObjectTypeIsFunctional(stmt->objectType);

	AppendAlterFunctionSchemaStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterFunctionSchemaStmt appends a string representing the AlterObjectSchemaStmt to a buffer
 */
static void
AppendAlterFunctionSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	ObjectWithArgs *func = castNode(ObjectWithArgs, stmt->object);

	if (stmt->objectType == OBJECT_FUNCTION)
	{
		appendStringInfoString(buf, "ALTER FUNCTION ");
	}
	else if (stmt->objectType == OBJECT_PROCEDURE)
	{
		appendStringInfoString(buf, "ALTER PROCEDURE ");
	}
	else
	{
		appendStringInfoString(buf, "ALTER AGGREGATE ");
	}

	AppendFunctionName(buf, func, stmt->objectType);
	appendStringInfo(buf, " SET SCHEMA %s;", quote_identifier(stmt->newschema));
}


/*
 * DeparseAlterFunctionOwnerStmt builds and returns a string representing the AlterOwnerStmt
 */
const char *
DeparseAlterFunctionOwnerStmt(AlterOwnerStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AssertObjectTypeIsFunctional(stmt->objectType);

	AppendAlterFunctionOwnerStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterFunctionOwnerStmt appends a string representing the AlterOwnerStmt to a buffer
 */
static void
AppendAlterFunctionOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	ObjectWithArgs *func = castNode(ObjectWithArgs, stmt->object);

	if (stmt->objectType == OBJECT_FUNCTION)
	{
		appendStringInfoString(buf, "ALTER FUNCTION ");
	}
	else if (stmt->objectType == OBJECT_PROCEDURE)
	{
		appendStringInfoString(buf, "ALTER PROCEDURE ");
	}
	else
	{
		appendStringInfoString(buf, "ALTER AGGREGATE ");
	}

	AppendFunctionName(buf, func, stmt->objectType);
	appendStringInfo(buf, " OWNER TO %s;", RoleSpecString(stmt->newowner));
}


/*
 * DeparseAlterFunctionDependsStmt builds and returns a string representing the AlterObjectDependsStmt
 */
const char *
DeparseAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AssertObjectTypeIsFunctional(stmt->objectType);

	AppendAlterFunctionDependsStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterFunctionDependsStmt appends a string representing the AlterObjectDependsStmt to a buffer
 */
static void
AppendAlterFunctionDependsStmt(StringInfo buf, AlterObjectDependsStmt *stmt)
{
	ObjectWithArgs *func = castNode(ObjectWithArgs, stmt->object);

	if (stmt->objectType == OBJECT_FUNCTION)
	{
		appendStringInfoString(buf, "ALTER FUNCTION ");
	}
	else if (stmt->objectType == OBJECT_PROCEDURE)
	{
		appendStringInfoString(buf, "ALTER PROCEDURE ");
	}
	else
	{
		appendStringInfoString(buf, "ALTER AGGREGATE ");
	}

	AppendFunctionName(buf, func, stmt->objectType);
	appendStringInfo(buf, " DEPENDS ON EXTENSION %s;", strVal(stmt->extname));
}


/*
 * DeparseDropFunctionStmt builds and returns a string representing the DropStmt
 */
const char *
DeparseDropFunctionStmt(DropStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AssertObjectTypeIsFunctional(stmt->removeType);

	AppendDropFunctionStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropFunctionStmt appends a string representing the DropStmt to a buffer
 */
static void
AppendDropFunctionStmt(StringInfo buf, DropStmt *stmt)
{
	if (stmt->removeType == OBJECT_FUNCTION)
	{
		appendStringInfoString(buf, "DROP FUNCTION ");
	}
	else if (stmt->removeType == OBJECT_PROCEDURE)
	{
		appendStringInfoString(buf, "DROP PROCEDURE ");
	}
	else
	{
		appendStringInfoString(buf, "DROP AGGREGATE ");
	}

	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	AppendFunctionNameList(buf, stmt->objects, stmt->removeType);

	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}

	appendStringInfoString(buf, ";");
}


/*
 * AppendFunctionNameList appends a string representing the list of function names to a buffer
 */
static void
AppendFunctionNameList(StringInfo buf, List *objects, ObjectType objtype)
{
	ListCell *objectCell = NULL;
	foreach(objectCell, objects)
	{
		Node *object = lfirst(objectCell);
		ObjectWithArgs *func = NULL;

		if (objectCell != list_head(objects))
		{
			appendStringInfo(buf, ", ");
		}

		func = castNode(ObjectWithArgs, object);

		AppendFunctionName(buf, func, objtype);
	}
}


/*
 * AppendFunctionName appends a string representing a single function name to a buffer
 */
static void
AppendFunctionName(StringInfo buf, ObjectWithArgs *func, ObjectType objtype)
{
	Oid funcid = InvalidOid;
	HeapTuple proctup;
	char *functionName = NULL;
	char *schemaName = NULL;
	char *qualifiedFunctionName;

	funcid = LookupFuncWithArgs(objtype, func, true);
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

	if (!HeapTupleIsValid(proctup))
	{
		/*
		 * DROP FUNCTION IF EXISTS absent_function arrives here
		 *
		 * There is no namespace associated with the nonexistent function,
		 * thus we return the function name as it is provided
		 */
		DeconstructQualifiedName(func->objname, &schemaName, &functionName);
	}
	else
	{
		Form_pg_proc procform;

		procform = (Form_pg_proc) GETSTRUCT(proctup);
		functionName = NameStr(procform->proname);
		functionName = pstrdup(functionName); /* we release the tuple before used */
		schemaName = get_namespace_name(procform->pronamespace);

		ReleaseSysCache(proctup);
	}

	qualifiedFunctionName = quote_qualified_identifier(schemaName, functionName);
	appendStringInfoString(buf, qualifiedFunctionName);

	if (OidIsValid(funcid))
	{
		/*
		 * If the function exists we want to use pg_get_function_identity_arguments to
		 * serialize its canonical arguments
		 */
		OverrideSearchPath *overridePath = NULL;
		Datum sqlTextDatum = 0;
		const char *args = NULL;

		/*
		 * Set search_path to NIL so that all objects outside of pg_catalog will be
		 * schema-prefixed. pg_catalog will be added automatically when we call
		 * PushOverrideSearchPath(), since we set addCatalog to true;
		 */
		overridePath = GetOverrideSearchPath(CurrentMemoryContext);
		overridePath->schemas = NIL;
		overridePath->addCatalog = true;

		PushOverrideSearchPath(overridePath);

		sqlTextDatum = DirectFunctionCall1(pg_get_function_identity_arguments,
										   ObjectIdGetDatum(funcid));

		/* revert back to original search_path */
		PopOverrideSearchPath();

		args = TextDatumGetCString(sqlTextDatum);
		appendStringInfo(buf, "(%s)", args);
	}
	else if (!func->args_unspecified)
	{
		/*
		 * The function is not found, but there is an argument list specified, this has
		 * some known issues with the "any" type. However this is mostly a bug in
		 * postgres' TypeNameListToString. For now the best we can do until we understand
		 * the underlying cause better.
		 */
		const char *args = NULL;

		args = TypeNameListToString(func->objargs);
		appendStringInfo(buf, "(%s)", args);
	}

	/*
	 * If the type is not found, and no argument list given we don't append anything here.
	 * This will cause mostly the same sql as the original statement.
	 */
}


/*
 * CopyAndConvertToUpperCase copies a string and converts all characters to uppercase
 */
static char *
CopyAndConvertToUpperCase(const char *str)
{
	char *result, *p;

	result = pstrdup(str);

	for (p = result; *p; p++)
	{
		*p = pg_toupper((unsigned char) *p);
	}

	return result;
}
