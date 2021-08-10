/*-------------------------------------------------------------------------
 *
 * deparse_table_stmts.c
 *	  All routines to deparse table statements.
 *	  This file contains all entry points specific for table statement deparsing as well as
 *	  functions that are currently only used for deparsing of the table statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/deparser.h"
#include "distributed/version_compat.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

static void AppendAlterTableSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterTableStmt(StringInfo buf, AlterTableStmt *stmt);
static void AppendAlterTableCmd(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendAlterTableCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd);

char *
DeparseAlterTableSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_TABLE);

	AppendAlterTableSchemaStmt(&str, stmt);
	return str.data;
}


static void
AppendAlterTableSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_TABLE);
	appendStringInfo(buf, "ALTER TABLE ");
	if (stmt->missing_ok)
	{
		appendStringInfo(buf, "IF EXISTS ");
	}
	char *tableName = quote_qualified_identifier(stmt->relation->schemaname,
												 stmt->relation->relname);
	const char *newSchemaName = quote_identifier(stmt->newschema);
	appendStringInfo(buf, "%s SET SCHEMA %s;", tableName, newSchemaName);
}


/*
 * DeparseAlterTableStmt builds and returns a string representing the
 * AlterTableStmt where the object acted upon is of kind OBJECT_TABLE
 */
char *
DeparseAlterTableStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(AlterTableStmtObjType(stmt) == OBJECT_TABLE);

	AppendAlterTableStmt(&str, stmt);
	return str.data;
}


/*
 * AppendAlterTableStmt builds and returns an SQL command representing an
 * ALTER TABLE statement from given AlterTableStmt object where the object
 * acted upon is of kind OBJECT_TABLE
 */
static void
AppendAlterTableStmt(StringInfo buf, AlterTableStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->relation->schemaname,
														stmt->relation->relname);
	ListCell *cmdCell = NULL;

	Assert(AlterTableStmtObjType(stmt) == OBJECT_TABLE);

	appendStringInfo(buf, "ALTER TABLE %s", identifier);
	foreach(cmdCell, stmt->cmds)
	{
		if (cmdCell != list_head(stmt->cmds))
		{
			appendStringInfoString(buf, ", ");
		}

		AlterTableCmd *alterTableCmd = castNode(AlterTableCmd, lfirst(cmdCell));
		AppendAlterTableCmd(buf, alterTableCmd);
	}

	appendStringInfoString(buf, ";");
}


/*
 * AppendAlterTableCmd builds and appends to the given buffer a command
 * from given AlterTableCmd object. Currently supported commands are of type
 * AT_AddColumn and AT_SetNotNull
 */
static void
AppendAlterTableCmd(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	switch (alterTableCmd->subtype)
	{
		case AT_AddColumn:
		{
			AppendAlterTableCmdAddColumn(buf, alterTableCmd);
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported subtype for alter table command"),
							errdetail("sub command type: %d", alterTableCmd->subtype)));
		}
	}
}


/*
 * AppendAlterTableCmd builds and appends to the given buffer an AT_AddColumn command
 * from given AlterTableCmd object in the form ADD COLUMN ...
 */
static void
AppendAlterTableCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AddColumn);

	appendStringInfoString(buf, " ADD COLUMN ");

	ColumnDef *columnDefinition = (ColumnDef *) alterTableCmd->def;

	/*
	 * the way we use the deparser now, constraints are always NULL
	 * adding this check for ColumnDef consistency
	 */
	if (columnDefinition->constraints != NULL)
	{
		ereport(ERROR, (errmsg("Constraints are not supported for AT_AddColumn")));
	}

	if (columnDefinition->colname)
	{
		appendStringInfo(buf, "%s ", quote_identifier(columnDefinition->colname));
	}

	int32 typmod = 0;
	Oid typeOid = InvalidOid;
	bits16 formatFlags = FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY;
	typenameTypeIdAndMod(NULL, columnDefinition->typeName, &typeOid, &typmod);
	appendStringInfo(buf, "%s", format_type_extended(typeOid, typmod,
													 formatFlags));
	if (columnDefinition->is_not_null)
	{
		appendStringInfoString(buf, " NOT NULL");
	}

	/*
	 * the way we use the deparser now, collation is never used
	 * since the data type of columns that use sequences for default
	 * are only int,smallint and bigint (never text, varchar, char)
	 * Adding this part only for ColumnDef consistency
	 */
	Oid collationOid = GetColumnDefCollation(NULL, columnDefinition, typeOid);
	if (OidIsValid(collationOid))
	{
		const char *identifier = FormatCollateBEQualified(collationOid);
		appendStringInfo(buf, " COLLATE %s", identifier);
	}
}
