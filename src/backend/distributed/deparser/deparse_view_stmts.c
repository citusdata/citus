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

static void AppendDropViewStmt(StringInfo buf, DropStmt *stmt);
static void AppendViewNameList(StringInfo buf, List *objects);
static void AppendAlterViewStmt(StringInfo buf, AlterTableStmt *stmt);
static void AppendAlterViewCmd(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendAlterViewOwnerStmt(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendAlterViewSetOptionsStmt(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendAlterViewResetOptionsStmt(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendRenameViewStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterViewSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);

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


/*
 * DeparseAlterViewStmt deparses the given ALTER VIEW statement.
 */
char *
DeparseAlterViewStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterViewStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterViewStmt(StringInfo buf, AlterTableStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->relation->schemaname,
														stmt->relation->relname);

	appendStringInfo(buf, "ALTER VIEW %s ", identifier);

	AlterTableCmd *alterTableCmd = castNode(AlterTableCmd, lfirst(list_head(stmt->cmds)));
	AppendAlterViewCmd(buf, alterTableCmd);

	appendStringInfoString(buf, ";");
}


static void
AppendAlterViewCmd(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	switch (alterTableCmd->subtype)
	{
		case AT_ChangeOwner:
		{
			AppendAlterViewOwnerStmt(buf, alterTableCmd);
			break;
		}

		case AT_SetRelOptions:
		{
			AppendAlterViewSetOptionsStmt(buf, alterTableCmd);
			break;
		}

		case AT_ResetRelOptions:
		{
			AppendAlterViewResetOptionsStmt(buf, alterTableCmd);
			break;
		}

		case AT_ColumnDefault:
		{
			elog(ERROR, "Citus doesn't support setting or resetting default values for a "
						"column of view");
			break;
		}

		default:
		{
			/*
			 * ALTER VIEW command only supports for the cases checked above but an
			 * ALTER TABLE commands targeting views may have different cases. To let
			 * PG throw the right error locally, we don't throw any error here
			 */
			break;
		}
	}
}


static void
AppendAlterViewOwnerStmt(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	appendStringInfo(buf, "OWNER TO %s", RoleSpecString(alterTableCmd->newowner, true));
}


static void
AppendAlterViewSetOptionsStmt(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	ListCell *lc = NULL;
	bool initialOption = true;
	foreach(lc, (List *) alterTableCmd->def)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (initialOption)
		{
			appendStringInfo(buf, "SET (");
			initialOption = false;
		}
		else
		{
			appendStringInfo(buf, ",");
		}

		appendStringInfo(buf, "%s", def->defname);
		if (def->arg != NULL)
		{
			appendStringInfo(buf, "=");
			appendStringInfo(buf, "%s", defGetString(def));
		}
	}

	appendStringInfo(buf, ")");
}


static void
AppendAlterViewResetOptionsStmt(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	ListCell *lc = NULL;
	bool initialOption = true;
	foreach(lc, (List *) alterTableCmd->def)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (initialOption)
		{
			appendStringInfo(buf, "RESET (");
			initialOption = false;
		}
		else
		{
			appendStringInfo(buf, ",");
		}

		appendStringInfo(buf, "%s", def->defname);
	}

	appendStringInfo(buf, ")");
}


char *
DeparseRenameViewStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendRenameViewStmt(&str, stmt);

	return str.data;
}


static void
AppendRenameViewStmt(StringInfo buf, RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_COLUMN:
		{
			const char *identifier =
				quote_qualified_identifier(stmt->relation->schemaname,
										   stmt->relation->relname);
			appendStringInfo(buf, "ALTER VIEW %s RENAME COLUMN %s TO %s;", identifier,
							 quote_identifier(stmt->subname), quote_identifier(
								 stmt->newname));
			break;
		}

		case OBJECT_VIEW:
		{
			const char *identifier =
				quote_qualified_identifier(stmt->relation->schemaname,
										   stmt->relation->relname);
			appendStringInfo(buf, "ALTER VIEW %s RENAME TO %s;", identifier,
							 quote_identifier(stmt->newname));
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported subtype for alter view rename command"),
							errdetail("sub command type: %d", stmt->renameType)));
		}
	}
}


char *
DeparseAlterViewSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterViewSchemaStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterViewSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->relation->schemaname,
														stmt->relation->relname);
	appendStringInfo(buf, "ALTER VIEW %s SET SCHEMA %s;", identifier, quote_identifier(
						 stmt->newschema));
}
