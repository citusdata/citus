/*-------------------------------------------------------------------------
 *
 * deparse_schema_stmts.c
 *	  All routines to deparse schema statements.
 *	  This file contains all entry points specific for schema statement deparsing
 *	  as well as functions that are currently only used for deparsing of the
 *	  schema statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

static void AppendGrantOnSchemaStmt(StringInfo buf, GrantStmt *stmt);
static void AppendGrantOnSchemaPrivileges(StringInfo buf, GrantStmt *stmt);
static void AppendGrantOnSchemaSchemas(StringInfo buf, GrantStmt *stmt);
static void AppendGrantOnSchemaGrantees(StringInfo buf, GrantStmt *stmt);
static void AppendAlterSchemaRenameStmt(StringInfo buf, RenameStmt *stmt);

char *
DeparseGrantOnSchemaStmt(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SCHEMA);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendGrantOnSchemaStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterSchemaRenameStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterSchemaRenameStmt(&str, stmt);

	return str.data;
}


static void
AppendGrantOnSchemaStmt(StringInfo buf, GrantStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_SCHEMA);

	appendStringInfo(buf, "%s ", stmt->is_grant ? "GRANT" : "REVOKE");

	if (!stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, "GRANT OPTION FOR ");
	}

	AppendGrantOnSchemaPrivileges(buf, stmt);

	AppendGrantOnSchemaSchemas(buf, stmt);

	AppendGrantOnSchemaGrantees(buf, stmt);

	if (stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, " WITH GRANT OPTION");
	}
	if (!stmt->is_grant)
	{
		if (stmt->behavior == DROP_RESTRICT)
		{
			appendStringInfo(buf, " RESTRICT");
		}
		else if (stmt->behavior == DROP_CASCADE)
		{
			appendStringInfo(buf, " CASCADE");
		}
	}
	appendStringInfo(buf, ";");
}


static void
AppendGrantOnSchemaPrivileges(StringInfo buf, GrantStmt *stmt)
{
	if (list_length(stmt->privileges) == 0)
	{
		appendStringInfo(buf, "ALL PRIVILEGES");
	}
	else
	{
		ListCell *cell = NULL;
		foreach(cell, stmt->privileges)
		{
			AccessPriv *privilege = (AccessPriv *) lfirst(cell);
			appendStringInfoString(buf, privilege->priv_name);
			if (cell != list_tail(stmt->privileges))
			{
				appendStringInfo(buf, ", ");
			}
		}
	}
}


static void
AppendGrantOnSchemaSchemas(StringInfo buf, GrantStmt *stmt)
{
	ListCell *cell = NULL;
	appendStringInfo(buf, " ON SCHEMA ");

	foreach(cell, stmt->objects)
	{
		char *schema = strVal(lfirst(cell));
		appendStringInfoString(buf, quote_identifier(schema));
		if (cell != list_tail(stmt->objects))
		{
			appendStringInfo(buf, ", ");
		}
	}
}


static void
AppendGrantOnSchemaGrantees(StringInfo buf, GrantStmt *stmt)
{
	ListCell *cell = NULL;
	appendStringInfo(buf, " %s ", stmt->is_grant ? "TO" : "FROM");

	foreach(cell, stmt->grantees)
	{
		RoleSpec *grantee = (RoleSpec *) lfirst(cell);
		appendStringInfoString(buf, RoleSpecString(grantee, true));
		if (cell != list_tail(stmt->grantees))
		{
			appendStringInfo(buf, ", ");
		}
	}
}


static void
AppendAlterSchemaRenameStmt(StringInfo buf, RenameStmt *stmt)
{
	Assert(stmt->renameType == OBJECT_SCHEMA);

	appendStringInfo(buf, "ALTER SCHEMA %s RENAME TO %s;",
					 quote_identifier(stmt->subname), quote_identifier(stmt->newname));
}
