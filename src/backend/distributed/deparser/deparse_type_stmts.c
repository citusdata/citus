/*-------------------------------------------------------------------------
 *
 * deparse_type_stmts.c
 *	  All routines to deparse type statements.
 *	  This file contains all entry points specific for type statement deparsing as well as
 *	  functions that are currently only used for deparsing of the type statements.
 *
 *	  Functions that could move later are AppendColumnDef, AppendColumnDefList, etc. These
 *	  should be reused across multiple statements and should live in their own deparse
 *	  file.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"

#define AlterEnumIsRename(stmt) (stmt->oldVal != NULL)
#define AlterEnumIsAddValue(stmt) (stmt->oldVal == NULL)

/* forward declaration for deparse functions */
static void AppendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt);
static void AppendColumnDef(StringInfo str, ColumnDef *columnDef);
static void AppendColumnDefList(StringInfo str, List *columnDefs);

static void AppendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt);
static void AppendStringList(StringInfo str, List *strings);

static void AppendDropTypeStmt(StringInfo buf, DropStmt *stmt);
static void AppendTypeNameList(StringInfo buf, List *objects);

static void AppendAlterEnumStmt(StringInfo buf, AlterEnumStmt *stmt);

static void AppendAlterTypeStmt(StringInfo buf, AlterTableStmt *stmt);
static void AppendAlterTypeCmd(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendAlterTypeCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendAlterTypeCmdDropColumn(StringInfo buf, AlterTableCmd *alterTableCmd);
static void AppendAlterTypeCmdAlterColumnType(StringInfo buf,
											  AlterTableCmd *alterTableCmd);

static void AppendRenameTypeStmt(StringInfo buf, RenameStmt *stmt);
static void AppendRenameTypeAttributeStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterTypeSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterTypeOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);


/*
 * DeparseCompositeTypeStmt builds and returns a string representing the
 * CompositeTypeStmt for application on a remote server.
 */
char *
DeparseCompositeTypeStmt(Node *node)
{
	CompositeTypeStmt *stmt = castNode(CompositeTypeStmt, node);
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendCompositeTypeStmt(&sql, stmt);

	return sql.data;
}


char *
DeparseCreateEnumStmt(Node *node)
{
	CreateEnumStmt *stmt = castNode(CreateEnumStmt, node);
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendCreateEnumStmt(&sql, stmt);

	return sql.data;
}


char *
DeparseAlterEnumStmt(Node *node)
{
	AlterEnumStmt *stmt = castNode(AlterEnumStmt, node);
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendAlterEnumStmt(&sql, stmt);

	return sql.data;
}


char *
DeparseDropTypeStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_TYPE);

	AppendDropTypeStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterTypeStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->relkind == OBJECT_TYPE);

	AppendAlterTypeStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterTypeStmt(StringInfo buf, AlterTableStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->relation->schemaname,
														stmt->relation->relname);
	ListCell *cmdCell = NULL;

	Assert(stmt->relkind == OBJECT_TYPE);

	appendStringInfo(buf, "ALTER TYPE %s", identifier);
	foreach(cmdCell, stmt->cmds)
	{
		if (cmdCell != list_head(stmt->cmds))
		{
			appendStringInfoString(buf, ", ");
		}

		AlterTableCmd *alterTableCmd = castNode(AlterTableCmd, lfirst(cmdCell));
		AppendAlterTypeCmd(buf, alterTableCmd);
	}

	appendStringInfoString(buf, ";");
}


static void
AppendAlterTypeCmd(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	switch (alterTableCmd->subtype)
	{
		case AT_AddColumn:
		{
			AppendAlterTypeCmdAddColumn(buf, alterTableCmd);
			break;
		}

		case AT_DropColumn:
		{
			AppendAlterTypeCmdDropColumn(buf, alterTableCmd);
			break;
		}

		case AT_AlterColumnType:
		{
			AppendAlterTypeCmdAlterColumnType(buf, alterTableCmd);
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported subtype for alter table command"),
							errdetail("sub command type: %d", alterTableCmd->subtype)));
		}
	}
}


static void
AppendAlterTypeCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AddColumn);

	appendStringInfoString(buf, " ADD ATTRIBUTE ");
	AppendColumnDef(buf, castNode(ColumnDef, alterTableCmd->def));
}


static void
AppendAlterTypeCmdDropColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_DropColumn);
	appendStringInfo(buf, " DROP ATTRIBUTE %s", quote_identifier(alterTableCmd->name));

	if (alterTableCmd->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


static void
AppendAlterTypeCmdAlterColumnType(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AlterColumnType);
	appendStringInfo(buf, " ALTER ATTRIBUTE %s SET DATA TYPE ", quote_identifier(
						 alterTableCmd->name));
	AppendColumnDef(buf, castNode(ColumnDef, alterTableCmd->def));

	if (alterTableCmd->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


static void
AppendAlterEnumStmt(StringInfo buf, AlterEnumStmt *stmt)
{
	appendStringInfo(buf, "ALTER TYPE %s", NameListToQuotedString(stmt->typeName));

	if (AlterEnumIsRename(stmt))
	{
		/* Rename an existing label */
		appendStringInfo(buf, " RENAME VALUE %s TO %s;",
						 quote_literal_cstr(stmt->oldVal),
						 quote_literal_cstr(stmt->newVal));
	}
	else if (AlterEnumIsAddValue(stmt))
	{
		/* Add a new label */
		appendStringInfoString(buf, " ADD VALUE ");
		if (stmt->skipIfNewValExists)
		{
			appendStringInfoString(buf, "IF NOT EXISTS ");
		}
		appendStringInfoString(buf, quote_literal_cstr(stmt->newVal));

		if (stmt->newValNeighbor)
		{
			appendStringInfo(buf, " %s %s",
							 stmt->newValIsAfter ? "AFTER" : "BEFORE",
							 quote_literal_cstr(stmt->newValNeighbor));
		}

		appendStringInfoString(buf, ";");
	}
}


static void
AppendDropTypeStmt(StringInfo buf, DropStmt *stmt)
{
	/*
	 * already tested at call site, but for future it might be collapsed in a
	 * DeparseDropStmt so be safe and check again
	 */
	Assert(stmt->removeType == OBJECT_TYPE);

	appendStringInfo(buf, "DROP TYPE ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}
	AppendTypeNameList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	appendStringInfoString(buf, ";");
}


static void
AppendTypeNameList(StringInfo buf, List *objects)
{
	ListCell *objectCell = NULL;
	foreach(objectCell, objects)
	{
		TypeName *typeName = castNode(TypeName, lfirst(objectCell));
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		const char *identifier = format_type_be_qualified(typeOid);

		if (objectCell != list_head(objects))
		{
			appendStringInfo(buf, ", ");
		}

		appendStringInfoString(buf, identifier);
	}
}


/*
 * AppendCompositeTypeStmt appends the sql string to recreate a CompositeTypeStmt to the
 * provided buffer, ending in a ; for concatination of multiple statements.
 */
static void
AppendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->typevar->schemaname,
														stmt->typevar->relname);
	appendStringInfo(str, "CREATE TYPE %s AS (", identifier);
	AppendColumnDefList(str, stmt->coldeflist);
	appendStringInfo(str, ");");
}


static void
AppendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt)
{
	RangeVar *typevar = makeRangeVarFromNameList(stmt->typeName);

	/* create the identifier from the fully qualified rangevar */
	const char *identifier = quote_qualified_identifier(typevar->schemaname,
														typevar->relname);

	appendStringInfo(str, "CREATE TYPE %s AS ENUM (", identifier);
	AppendStringList(str, stmt->vals);
	appendStringInfo(str, ");");
}


static void
AppendStringList(StringInfo str, List *strings)
{
	ListCell *stringCell = NULL;
	foreach(stringCell, strings)
	{
		const char *string = strVal(lfirst(stringCell));
		if (stringCell != list_head(strings))
		{
			appendStringInfoString(str, ", ");
		}

		string = quote_literal_cstr(string);
		appendStringInfoString(str, string);
	}
}


/*
 * AppendColumnDefList appends the definition of a list of ColumnDef items to the provided
 * buffer, adding separators as necessary.
 */
static void
AppendColumnDefList(StringInfo str, List *columnDefs)
{
	ListCell *columnDefCell = NULL;
	foreach(columnDefCell, columnDefs)
	{
		if (columnDefCell != list_head(columnDefs))
		{
			appendStringInfoString(str, ", ");
		}
		AppendColumnDef(str, castNode(ColumnDef, lfirst(columnDefCell)));
	}
}


/*
 * AppendColumnDef appends the definition of one ColumnDef completely qualified to the
 * provided buffer.
 *
 * If the colname is not set that part is ommitted. This is the case in alter column type
 * statements.
 */
static void
AppendColumnDef(StringInfo str, ColumnDef *columnDef)
{
	Oid typeOid = LookupTypeNameOid(NULL, columnDef->typeName, false);
	Oid collationOid = GetColumnDefCollation(NULL, columnDef, typeOid);

	Assert(!columnDef->is_not_null); /* not null is not supported on composite types */

	if (columnDef->colname)
	{
		appendStringInfo(str, "%s ", quote_identifier(columnDef->colname));
	}

	appendStringInfo(str, "%s", format_type_be_qualified(typeOid));

	if (OidIsValid(collationOid))
	{
		const char *identifier = FormatCollateBEQualified(collationOid);
		appendStringInfo(str, " COLLATE %s", identifier);
	}
}


char *
DeparseRenameTypeStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->renameType == OBJECT_TYPE);

	AppendRenameTypeStmt(&str, stmt);

	return str.data;
}


static void
AppendRenameTypeStmt(StringInfo buf, RenameStmt *stmt)
{
	List *names = (List *) stmt->object;

	appendStringInfo(buf, "ALTER TYPE %s RENAME TO %s;", NameListToQuotedString(names),
					 quote_identifier(stmt->newname));
}


char *
DeparseRenameTypeAttributeStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->renameType == OBJECT_ATTRIBUTE);
	Assert(stmt->relationType == OBJECT_TYPE);

	AppendRenameTypeAttributeStmt(&str, stmt);

	return str.data;
}


static void
AppendRenameTypeAttributeStmt(StringInfo buf, RenameStmt *stmt)
{
	appendStringInfo(buf, "ALTER TYPE %s RENAME ATTRIBUTE %s TO %s",
					 quote_qualified_identifier(stmt->relation->schemaname,
												stmt->relation->relname),
					 quote_identifier(stmt->subname),
					 quote_identifier(stmt->newname));

	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}

	appendStringInfoString(buf, ";");
}


char *
DeparseAlterTypeSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_TYPE);

	AppendAlterTypeSchemaStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterTypeSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_TYPE);

	List *names = (List *) stmt->object;
	appendStringInfo(buf, "ALTER TYPE %s SET SCHEMA %s;", NameListToQuotedString(names),
					 quote_identifier(stmt->newschema));
}


char *
DeparseAlterTypeOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_TYPE);

	AppendAlterTypeOwnerStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterTypeOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_TYPE);

	List *names = (List *) stmt->object;
	appendStringInfo(buf, "ALTER TYPE %s OWNER TO %s;", NameListToQuotedString(names),
					 RoleSpecString(stmt->newowner, true));
}
