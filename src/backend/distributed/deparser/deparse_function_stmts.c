/*-------------------------------------------------------------------------
 *
 * deparse_function_stmts.c
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
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
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* forward declaration for deparse functions */
static void AppendAlterFunctionStmt(StringInfo buf, AlterFunctionStmt *stmt);
static void AppendDropFunctionStmt(StringInfo buf, DropStmt *stmt);
static void AppendFunctionNameList(StringInfo buf, List *objects);
static void AppendFunctionName(StringInfo buf, ObjectWithArgs *func);

static void AppendDefElem(StringInfo buf, DefElem *def);
static void AppendDefElemStrict(StringInfo buf, DefElem *def);
static void AppendDefElemVolatility(StringInfo buf, DefElem *def);
static void AppendDefElemLeakproof(StringInfo buf, DefElem *def);
static void AppendDefElemSecurity(StringInfo buf, DefElem *def);
static void AppendDefElemParallel(StringInfo buf, DefElem *def);
static void AppendDefElemCost(StringInfo buf, DefElem *def);
static void AppendDefElemRows(StringInfo buf, DefElem *def);
static void AppendDefElemSet(StringInfo buf, DefElem *def);


const char *
DeparseAlterFunctionStmt(AlterFunctionStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterFunctionStmt(&str, stmt);

	return str.data;
}


const char *
DeparseDropFunctionStmt(DropStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_FUNCTION);

	AppendDropFunctionStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterFunctionStmt(StringInfo buf, AlterFunctionStmt *stmt)
{
	ListCell *actionCell = NULL;
	appendStringInfo(buf, "ALTER FUNCTION ");
	AppendFunctionName(buf, stmt->func);

	foreach(actionCell, stmt->actions)
	{
		DefElem *def = castNode(DefElem, lfirst(actionCell));
		AppendDefElem(buf, def);
	}

	/* TODO: use other attributes to deparse the query here  */
	appendStringInfoString(buf, ";");
}


static void
AppendDropFunctionStmt(StringInfo buf, DropStmt *stmt)
{
	/*
	 * TODO: Hanefi you should check that this comment is still valid.
	 *
	 * already tested at call site, but for future it might be collapsed in a
	 * deparse_function_stmt so be safe and check again
	 */
	Assert(stmt->removeType == OBJECT_FUNCTION);

	appendStringInfo(buf, "DROP FUNCTION ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}
	AppendFunctionNameList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	appendStringInfoString(buf, ";");
}


static void
AppendFunctionNameList(StringInfo buf, List *objects)
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

		Assert(IsA(object, ObjectWithArgs));
		func = castNode(ObjectWithArgs, object);

		AppendFunctionName(buf, func);
	}
}


static void
AppendQualifiedFunctionName(StringInfo buf, ObjectWithArgs *func)
{
	Oid funcid = InvalidOid;
	HeapTuple proctup;
	char *functionName = NULL;
	char *schemaName = NULL;
	char *qualifiedFunctionName;

	funcid = LookupFuncWithArgs(OBJECT_FUNCTION, func, true);
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
		schemaName = get_namespace_name(procform->pronamespace);

		ReleaseSysCache(proctup);
	}

	qualifiedFunctionName = quote_qualified_identifier(schemaName, functionName);
	appendStringInfoString(buf, qualifiedFunctionName);
}


static void
AppendFunctionName(StringInfo buf, ObjectWithArgs *func)
{
	char *args = TypeNameListToString(func->objargs);

	AppendQualifiedFunctionName(buf, func);

	/* append the optional arg list if provided */
	if (args)
	{
		appendStringInfo(buf, "(%s)", args);
	}
}


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


static void
AppendDefElemVolatility(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " %s", strVal(def->arg));
}


static void
AppendDefElemLeakproof(StringInfo buf, DefElem *def)
{
	if (intVal(def->arg) == 0)
	{
		appendStringInfo(buf, " NOT");
	}
	appendStringInfo(buf, " LEAKPROOF");
}


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


static void
AppendDefElemParallel(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " PARALLEL %s", strVal(def->arg));
}


static void
AppendDefElemCost(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " COST %lf", defGetNumeric(def));
}


static void
AppendDefElemRows(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " ROWS  %lf", defGetNumeric(def));
}


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
