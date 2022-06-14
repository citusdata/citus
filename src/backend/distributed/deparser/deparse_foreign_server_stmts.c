/*-------------------------------------------------------------------------
 *
 * deparse_foreign_server_stmts.c
 *	  All routines to deparse foreign server statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/relay_utility.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

static void AppendCreateForeignServerStmt(StringInfo buf, CreateForeignServerStmt *stmt);
static void AppendAlterForeignServerStmt(StringInfo buf, AlterForeignServerStmt *stmt);
static void AppendAlterForeignServerOptions(StringInfo buf, AlterForeignServerStmt *stmt);
static void AppendAlterForeignServerRenameStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterForeignServerOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendDropForeignServerStmt(StringInfo buf, DropStmt *stmt);
static void AppendServerNames(StringInfo buf, DropStmt *stmt);
static void AppendBehavior(StringInfo buf, DropStmt *stmt);
static char * GetDefElemActionString(DefElemAction action);

char *
DeparseCreateForeignServerStmt(Node *node)
{
	CreateForeignServerStmt *stmt = castNode(CreateForeignServerStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendCreateForeignServerStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterForeignServerStmt(Node *node)
{
	AlterForeignServerStmt *stmt = castNode(AlterForeignServerStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterForeignServerStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterForeignServerRenameStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	Assert(stmt->renameType == OBJECT_FOREIGN_SERVER);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterForeignServerRenameStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterForeignServerOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);

	Assert(stmt->objectType == OBJECT_FOREIGN_SERVER);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterForeignServerOwnerStmt(&str, stmt);

	return str.data;
}


char *
DeparseDropForeignServerStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);

	Assert(stmt->removeType == OBJECT_FOREIGN_SERVER);

	StringInfoData str;
	initStringInfo(&str);

	AppendDropForeignServerStmt(&str, stmt);

	return str.data;
}


static void
AppendCreateForeignServerStmt(StringInfo buf, CreateForeignServerStmt *stmt)
{
	appendStringInfoString(buf, "CREATE SERVER ");

	if (stmt->if_not_exists)
	{
		appendStringInfoString(buf, "IF NOT EXISTS ");
	}

	appendStringInfo(buf, "%s ", quote_identifier(stmt->servername));

	if (stmt->servertype)
	{
		appendStringInfo(buf, "TYPE %s ", quote_literal_cstr(stmt->servertype));
	}

	if (stmt->version)
	{
		appendStringInfo(buf, "VERSION %s ", quote_literal_cstr(stmt->version));
	}

	appendStringInfo(buf, "FOREIGN DATA WRAPPER %s ", quote_identifier(stmt->fdwname));

	AppendOptionListToString(buf, stmt->options);
}


static void
AppendAlterForeignServerStmt(StringInfo buf, AlterForeignServerStmt *stmt)
{
	appendStringInfo(buf, "ALTER SERVER %s ", quote_identifier(stmt->servername));

	if (stmt->has_version)
	{
		appendStringInfo(buf, "VERSION %s ", quote_literal_cstr(stmt->version));
	}

	AppendAlterForeignServerOptions(buf, stmt);
}


static void
AppendAlterForeignServerOptions(StringInfo buf, AlterForeignServerStmt *stmt)
{
	if (list_length(stmt->options) <= 0)
	{
		return;
	}

	appendStringInfoString(buf, "OPTIONS (");

	DefElemAction action = DEFELEM_UNSPEC;
	DefElem *def = NULL;
	foreach_ptr(def, stmt->options)
	{
		if (def->defaction != DEFELEM_UNSPEC)
		{
			action = def->defaction;
			char *actionString = GetDefElemActionString(action);
			appendStringInfo(buf, "%s ", actionString);
		}

		appendStringInfo(buf, "%s", quote_identifier(def->defname));

		if (action != DEFELEM_DROP)
		{
			const char *value = quote_literal_cstr(defGetString(def));
			appendStringInfo(buf, " %s", value);
		}

		if (def != llast(stmt->options))
		{
			appendStringInfoString(buf, ", ");
		}
	}

	appendStringInfoString(buf, ")");
}


static void
AppendAlterForeignServerRenameStmt(StringInfo buf, RenameStmt *stmt)
{
	appendStringInfo(buf, "ALTER SERVER %s RENAME TO %s",
					 quote_identifier(strVal(stmt->object)),
					 quote_identifier(stmt->newname));
}


static void
AppendAlterForeignServerOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	const char *servername = quote_identifier(strVal(stmt->object));
	appendStringInfo(buf, "ALTER SERVER %s OWNER TO ", servername);

	appendStringInfo(buf, "%s", RoleSpecString(stmt->newowner, true));
}


static void
AppendDropForeignServerStmt(StringInfo buf, DropStmt *stmt)
{
	appendStringInfoString(buf, "DROP SERVER ");

	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	AppendServerNames(buf, stmt);

	AppendBehavior(buf, stmt);
}


static void
AppendServerNames(StringInfo buf, DropStmt *stmt)
{
	String *serverValue = NULL;
	foreach_ptr(serverValue, stmt->objects)
	{
		const char *serverString = quote_identifier(strVal(serverValue));
		appendStringInfo(buf, "%s", serverString);

		if (serverValue != llast(stmt->objects))
		{
			appendStringInfoString(buf, ", ");
		}
	}
}


static void
AppendBehavior(StringInfo buf, DropStmt *stmt)
{
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	else if (stmt->behavior == DROP_RESTRICT)
	{
		appendStringInfoString(buf, " RESTRICT");
	}
}


static char *
GetDefElemActionString(DefElemAction action)
{
	switch (action)
	{
		case DEFELEM_ADD:
		{
			return "ADD";
		}

		case DEFELEM_SET:
		{
			return "SET";
		}

		case DEFELEM_DROP:
		{
			return "DROP";
		}

		default:
			return "";
	}
}
