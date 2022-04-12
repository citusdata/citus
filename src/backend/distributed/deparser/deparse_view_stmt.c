/*-------------------------------------------------------------------------
 *
 * deparse_view_stmts.c
 *
 *	  All routines to deparse view statements.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static void AddQualifiedViewNameToCreateViewCommand(StringInfo buf, Oid viewOid);
static void AddAliasesToCreateViewCommand(StringInfo buf, ViewStmt *stmt);
static void AddOptionsToCreateViewCommand(StringInfo buf, ViewStmt *stmt);
static void AppendDropViewStmt(StringInfo buf, DropStmt *stmt);
static void AppendViewNameList(StringInfo buf, List *objects);

/*
 * DeparseViewStmt deparses the given CREATE OR REPLACE VIEW statement
 */
char *
DeparseViewStmt(Node *node)
{
	ViewStmt *stmt = castNode(ViewStmt, node);
	StringInfo viewString = makeStringInfo();

	Oid schemaOid = RangeVarGetCreationNamespace(stmt->view);
	Oid viewOid = get_relname_relid(stmt->view->relname, schemaOid);

	if (stmt->replace)
	{
		appendStringInfoString(viewString, "CREATE OR REPLACE ");
	}
	else
	{
		appendStringInfoString(viewString, "CREATE ");
	}

	if (stmt->view->relpersistence == RELPERSISTENCE_TEMP)
	{
		appendStringInfoString(viewString, "TEMPORARY ");
	}

	appendStringInfo(viewString, "VIEW ");

	AddQualifiedViewNameToCreateViewCommand(viewString, viewOid);
	AddAliasesToCreateViewCommand(viewString, stmt);
	AddOptionsToCreateViewCommand(viewString, stmt);

	/*
	 * Note that Postgres converts CREATE RECURSIVE VIEW commands to
	 * CREATE VIEW ... WITH RECURSIVE and pg_get_viewdef return it properly.
	 * So, we don't need to RECURSIVE views separately while obtaining the
	 * view creation command
	 */
	AddViewDefinitionToCreateViewCommand(viewString, viewOid);

	return viewString->data;
}


/*
 * AddQualifiedViewNameToCreateViewCommand adds the qualified view of the given view
 * statement to the given create view command.
 */
static void
AddQualifiedViewNameToCreateViewCommand(StringInfo buf, Oid viewOid)
{
	char *viewName = get_rel_name(viewOid);
	char *schemaName = get_namespace_name(get_rel_namespace(viewOid));
	char *qualifiedViewName = quote_qualified_identifier(schemaName, viewName);

	appendStringInfo(buf, "%s ", qualifiedViewName);
}


/*
 * AddAliasesToCreateViewCommand appends aliases (if exists) of the given view statement
 * to the given create view command.
 */
static void
AddAliasesToCreateViewCommand(StringInfo buf, ViewStmt *stmt)
{
	if (stmt->aliases == NIL)
	{
		return;
	}

	bool isFirstAlias = true;
	ListCell *aliasItem;
	foreach(aliasItem, stmt->aliases)
	{
		char *columnAliasName = pstrdup(quote_identifier(strVal(lfirst(aliasItem))));

		if (isFirstAlias)
		{
			appendStringInfoString(buf, "(");
			isFirstAlias = false;
		}
		else
		{
			appendStringInfoString(buf, ",");
		}

		appendStringInfoString(buf, columnAliasName);
	}

	appendStringInfoString(buf, ") ");
}


/*
 * AddOptionsToCreateViewCommand appends options (if exists) of the given view statement
 * to the given create view command. Note that this function also handles
 * WITH [CASCADED | LOCAL] CHECK OPTION part of the CREATE VIEW command as well.
 */
static void
AddOptionsToCreateViewCommand(StringInfo buf, ViewStmt *stmt)
{
	if (list_length(stmt->options) == 0)
	{
		return;
	}

	bool isFirstOption = true;
	ListCell *optionCell;
	foreach(optionCell, stmt->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		if (isFirstOption)
		{
			appendStringInfoString(buf, "WITH (");
			isFirstOption = false;
		}
		else
		{
			appendStringInfoString(buf, ",");
		}

		appendStringInfoString(buf, option->defname);
		appendStringInfoString(buf, " = ");
		appendStringInfoString(buf, defGetString(option));
	}

	appendStringInfoString(buf, ") ");
}


/*
 * AddViewDefinitionToCreateViewCommand adds the definition of the given view to the
 * given create view command.
 */
void
AddViewDefinitionToCreateViewCommand(StringInfo buf, Oid viewOid)
{
	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/*
	 * Push the transaction snapshot to be able to get vief definition with pg_get_viewdef
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	Datum viewDefinitionDatum = DirectFunctionCall1(pg_get_viewdef,
													ObjectIdGetDatum(viewOid));
	char *viewDefinition = TextDatumGetCString(viewDefinitionDatum);

	PopActiveSnapshot();
	PopOverrideSearchPath();

	appendStringInfo(buf, "AS %s ", viewDefinition);
}


/*
 * DeparseDropViewStmt deparses the given DROP VIEW statement.
 */
char *
DeparseDropViewStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_VIEW);

	AppendDropViewStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropViewStmt appends the deparsed representation of given drop stmt
 * to the given string info buffer.
 */
static void
AppendDropViewStmt(StringInfo buf, DropStmt *stmt)
{
	/*
	 * already tested at call site, but for future it might be collapsed in a
	 * DeparseDropStmt so be safe and check again
	 */
	Assert(stmt->removeType == OBJECT_VIEW);

	appendStringInfo(buf, "DROP VIEW ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}
	AppendViewNameList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	appendStringInfoString(buf, ";");
}


/*
 * AppendViewNameList appends the qualified view names by constructing them from the given
 * objects list to the given string info buffer. Note that, objects must hold schema
 * qualified view names as its' members.
 */
static void
AppendViewNameList(StringInfo buf, List *viewNamesList)
{
	bool isFirstView = true;
	List *qualifiedViewName = NULL;
	foreach_ptr(qualifiedViewName, viewNamesList)
	{
		char *quotedQualifiedVieName = NameListToQuotedString(qualifiedViewName);
		if (!isFirstView)
		{
			appendStringInfo(buf, ", ");
		}

		appendStringInfoString(buf, quotedQualifiedVieName);
		isFirstView = false;
	}
}
