/*-------------------------------------------------------------------------
 *
 * deparse_type_stmts.c
 *	  All routines to deparse type statements.
 *	  This file contains all entry points specific for type statement deparsing as well as
 *	  functions that are currently only used for deparsing of the type statements.
 *
 *	  Functions that could move later are appendColumnDef, appendColumnDefList, etc. These
 *	  should be reused across multiple statements and should live in their own deparse
 *	  file.
 *
 * Copyright (c) 2019, Citus Data, Inc.
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

#include "distributed/metadata/namespace.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"

#define AlterEnumIsRename(stmt) (stmt->oldVal != NULL)
#define AlterEnumIsAddValue(stmt) (stmt->oldVal == NULL)

/* forward declaration for deparse functions */
static void appendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt);
static void appendColumnDef(StringInfo str, ColumnDef *columnDef);
static void appendColumnDefList(StringInfo str, List *columnDefs);

static void appendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt);
static void appendStringList(StringInfo str, List *strings);

static void appendDropTypeStmt(StringInfo buf, DropStmt *stmt);
static void appendTypeNameList(StringInfo buf, List *objects);

static void appendAlterEnumStmt(StringInfo buf, AlterEnumStmt *stmt);

static void appendAlterTypeStmt(StringInfo buf, AlterTableStmt *stmt);
static void appendAlterTypeCmd(StringInfo buf, AlterTableCmd *alterTableCmd);
static void appendAlterTypeCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd);
static void appendAlterTypeCmdDropColumn(StringInfo buf, AlterTableCmd *alterTableCmd);
static void appendAlterTypeCmdAlterColumnType(StringInfo buf,
											  AlterTableCmd *alterTableCmd);

static void appendRenameTypeStmt(StringInfo buf, RenameStmt *stmt);


/*
 * deparse_composite_type_stmt builds and returns a string representing the
 * CompositeTypeStmt for application on a remote server.
 */
const char *
deparse_composite_type_stmt(CompositeTypeStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendCompositeTypeStmt(&sql, stmt);

	return sql.data;
}


const char *
deparse_create_enum_stmt(CreateEnumStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendCreateEnumStmt(&sql, stmt);

	return sql.data;
}


const char *
deparse_alter_enum_stmt(AlterEnumStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendAlterEnumStmt(&sql, stmt);

	return sql.data;
}


const char *
deparse_drop_type_stmt(DropStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_TYPE);

	appendDropTypeStmt(&str, stmt);

	return str.data;
}


const char *
deparse_alter_type_stmt(AlterTableStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->relkind == OBJECT_TYPE);

	appendAlterTypeStmt(&str, stmt);

	return str.data;
}


static void
appendAlterTypeStmt(StringInfo buf, AlterTableStmt *stmt)
{
	List *names = MakeNameListFromRangeVar(stmt->relation);
	TypeName *typeName = makeTypeNameFromNameList(names);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	const char *identifier = format_type_be_qualified(typeOid);
	ListCell *cmdCell = NULL;

	Assert(stmt->relkind = OBJECT_TYPE);

	appendStringInfo(buf, "ALTER TYPE %s", identifier);
	foreach(cmdCell, stmt->cmds)
	{
		AlterTableCmd *alterTableCmd = NULL;

		if (cmdCell != list_head(stmt->cmds))
		{
			appendStringInfoString(buf, ", ");
		}

		alterTableCmd = castNode(AlterTableCmd, lfirst(cmdCell));
		appendAlterTypeCmd(buf, alterTableCmd);
	}

	appendStringInfoString(buf, ";");
}


static void
appendAlterTypeCmd(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	switch (alterTableCmd->subtype)
	{
		case AT_AddColumn:
		{
			appendAlterTypeCmdAddColumn(buf, alterTableCmd);
			break;
		}

		case AT_DropColumn:
		{
			appendAlterTypeCmdDropColumn(buf, alterTableCmd);
			break;
		}

		case AT_AlterColumnType:
		{
			appendAlterTypeCmdAlterColumnType(buf, alterTableCmd);
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
appendAlterTypeCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AddColumn);

	appendStringInfoString(buf, " ADD ATTRIBUTE ");
	appendColumnDef(buf, castNode(ColumnDef, alterTableCmd->def));
}


static void
appendAlterTypeCmdDropColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_DropColumn);
	appendStringInfo(buf, " DROP ATTRIBUTE %s", quote_identifier(alterTableCmd->name));

	if (alterTableCmd->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


static void
appendAlterTypeCmdAlterColumnType(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AlterColumnType);
	appendStringInfo(buf, " ALTER ATTRIBUTE %s SET DATA TYPE ", quote_identifier(
						 alterTableCmd->name));
	appendColumnDef(buf, castNode(ColumnDef, alterTableCmd->def));

	if (alterTableCmd->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


static void
appendAlterEnumStmt(StringInfo buf, AlterEnumStmt *stmt)
{
	TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	const char *identifier = format_type_be_qualified(typeOid);

	appendStringInfo(buf, "ALTER TYPE %s", identifier);

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
appendDropTypeStmt(StringInfo buf, DropStmt *stmt)
{
	/*
	 * already tested at call site, but for future it might be collapsed in a
	 * deparse_drop_stmt so be safe and check again
	 */
	Assert(stmt->removeType == OBJECT_TYPE);

	appendStringInfo(buf, "DROP TYPE ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}
	appendTypeNameList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	appendStringInfoString(buf, ";");
}


static void
appendTypeNameList(StringInfo buf, List *objects)
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
 * appendCompositeTypeStmt appends the sql string to recreate a CompositeTypeStmt to the
 * provided buffer, ending in a ; for concatination of multiple statements.
 */
static void
appendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->typevar->schemaname,
														stmt->typevar->relname);
	appendStringInfo(str, "CREATE TYPE %s AS (", identifier);
	appendColumnDefList(str, stmt->coldeflist);
	appendStringInfo(str, ");");
}


static void
appendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt)
{
	RangeVar *typevar = NULL;
	const char *identifier = NULL;

	typevar = makeRangeVarFromNameList(stmt->typeName);

	/* create the identifier from the fully qualified rangevar */
	identifier = quote_qualified_identifier(typevar->schemaname, typevar->relname);

	appendStringInfo(str, "CREATE TYPE %s AS ENUM (", identifier);
	appendStringList(str, stmt->vals);
	appendStringInfo(str, ");");
}


static void
appendStringList(StringInfo str, List *strings)
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
 * appendColumnDefList appends the definition of a list of ColumnDef items to the provided
 * buffer, adding separators as necessary.
 */
static void
appendColumnDefList(StringInfo str, List *columnDefs)
{
	ListCell *columnDefCell = NULL;
	foreach(columnDefCell, columnDefs)
	{
		if (columnDefCell != list_head(columnDefs))
		{
			appendStringInfoString(str, ", ");
		}
		appendColumnDef(str, castNode(ColumnDef, lfirst(columnDefCell)));
	}
}


/*
 * appendColumnDef appends the definition of one ColumnDef completely qualified to the
 * provided buffer.
 *
 * If the colname is not set that part is ommitted. This is the case in alter column type
 * statements.
 */
static void
appendColumnDef(StringInfo str, ColumnDef *columnDef)
{
	Oid typeOid = LookupTypeNameOid(NULL, columnDef->typeName, false);
	Oid collationOid = GetColumnDefCollation(NULL, columnDef, typeOid);

	Assert(!columnDef->is_not_null); /* not null is not supported on composite types */

	if (columnDef->colname)
	{
		appendStringInfo(str, "%s ", columnDef->colname);
	}

	appendStringInfo(str, "%s", format_type_be_qualified(typeOid));

	if (OidIsValid(collationOid))
	{
		const char *identifier = format_collate_be_qualified(collationOid);
		appendStringInfo(str, " COLLATE %s", identifier);
	}
}


const char *
deparse_rename_type_stmt(RenameStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->renameType == OBJECT_TYPE);

	appendRenameTypeStmt(&str, stmt);

	return str.data;
}


static void
appendRenameTypeStmt(StringInfo buf, RenameStmt *stmt)
{
	List *names = (List *) stmt->object;
	TypeName *typeName = makeTypeNameFromNameList(names);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	const char *identifier = format_type_be_qualified(typeOid);

	appendStringInfo(buf, "ALTER TYPE %s RENAME TO %s", identifier, quote_identifier(
						 stmt->newname));
}
