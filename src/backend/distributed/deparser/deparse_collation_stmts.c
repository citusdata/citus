/*-------------------------------------------------------------------------
 *
 * deparse_collation_stmts.c
 *	  All routines to deparse collation statements.
 *	  This file contains all entry points specific for type statement deparsing as well as
 *	  functions that are currently only used for deparsing of the type statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "lib/stringinfo.h"
#include "nodes/value.h"
#include "utils/builtins.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"

static void AppendDropCollationStmt(StringInfo buf, DropStmt *stmt);
static void AppendRenameCollationStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterCollationSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterCollationOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendNameList(StringInfo buf, List *objects);


/*
 * DeparseDropCollationStmt builds and returns a string representing the DropStmt
 */
char *
DeparseDropCollationStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_COLLATION);

	AppendDropCollationStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropCollationStmt appends a string representing the DropStmt to a buffer
 */
static void
AppendDropCollationStmt(StringInfo buf, DropStmt *stmt)
{
	appendStringInfoString(buf, "DROP COLLATION ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}
	AppendNameList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


/*
 * DeparseRenameCollationStmt builds and returns a string representing the RenameStmt
 */
char *
DeparseRenameCollationStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->renameType == OBJECT_COLLATION);

	AppendRenameCollationStmt(&str, stmt);

	return str.data;
}


/*
 * AppendRenameCollationStmt appends a string representing the RenameStmt to a buffer
 */
static void
AppendRenameCollationStmt(StringInfo buf, RenameStmt *stmt)
{
	List *names = (List *) stmt->object;

	appendStringInfo(buf, "ALTER COLLATION %s RENAME TO %s;",
					 NameListToQuotedString(names),
					 quote_identifier(stmt->newname));
}


/*
 * DeparseAlterCollationSchemaStmt builds and returns a string representing the AlterObjectSchemaStmt
 */
char *
DeparseAlterCollationSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_COLLATION);

	AppendAlterCollationSchemaStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterCollationSchemaStmt appends a string representing the AlterObjectSchemaStmt to a buffer
 */
static void
AppendAlterCollationSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_COLLATION);

	List *names = (List *) stmt->object;
	appendStringInfo(buf, "ALTER COLLATION %s SET SCHEMA %s;", NameListToQuotedString(
						 names),
					 quote_identifier(stmt->newschema));
}


/*
 * DeparseAlterCollationOwnerStmt builds and returns a string representing the AlterOwnerStmt
 */
char *
DeparseAlterCollationOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_COLLATION);

	AppendAlterCollationOwnerStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterCollationOwnerStmt appends a string representing the AlterOwnerStmt to a buffer
 */
static void
AppendAlterCollationOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_COLLATION);

	List *names = (List *) stmt->object;
	appendStringInfo(buf, "ALTER COLLATION %s OWNER TO %s;", NameListToQuotedString(
						 names),
					 RoleSpecString(stmt->newowner, true));
}


static void
AppendNameList(StringInfo buf, List *objects)
{
	ListCell *objectCell = NULL;

	foreach(objectCell, objects)
	{
		List *name = castNode(List, lfirst(objectCell));

		if (objectCell != list_head(objects))
		{
			appendStringInfo(buf, ", ");
		}

		appendStringInfoString(buf, NameListToQuotedString(name));
	}
}
