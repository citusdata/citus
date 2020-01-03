/*-------------------------------------------------------------------------
 *
 * deparse_extension_stmts.c
 *	  All routines to deparse extension statements.
 *	  This file contains deparse functions for extension statement deparsing
 *    as well as related helper functions.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/deparser.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

/* Local functions forward declarations for helper functions */
static void AppendCreateExtensionStmt(StringInfo buf, CreateExtensionStmt *stmt);
static void AppendDropExtensionStmt(StringInfo buf, DropStmt *stmt);
static void AppendExtensionNameList(StringInfo buf, List *objects);
static void AppendAlterExtensionSchemaStmt(StringInfo buf,
										   AlterObjectSchemaStmt *alterExtensionSchemaStmt);
static void AppendAlterExtensionStmt(StringInfo buf,
									 AlterExtensionStmt *alterExtensionStmt);


/*
 * GetExtensionOption returns Value* of DefElem node with "defname" from "options" list
 */
Value *
GetExtensionOption(List *extensionOptions, const char *defname)
{
	Value *targetValue = NULL;

	ListCell *defElemCell = NULL;

	foreach(defElemCell, extensionOptions)
	{
		DefElem *defElement = (DefElem *) lfirst(defElemCell);

		if (IsA(defElement, DefElem) && strncmp(defElement->defname, defname,
												NAMEDATALEN) == 0)
		{
			targetValue = (Value *) defElement->arg;
			break;
		}
	}

	/* return target string safely */
	if (targetValue)
	{
		return targetValue;
	}
	else
	{
		return NULL;
	}
}


/*
 * DeparseCreateExtensionStmt builds and returns a string representing the
 * CreateExtensionStmt to be sent to worker nodes.
 */
char *
DeparseCreateExtensionStmt(Node *node)
{
	CreateExtensionStmt *stmt = castNode(CreateExtensionStmt, node);
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendCreateExtensionStmt(&sql, stmt);

	return sql.data;
}


/*
 * AppendCreateExtensionStmt appends a string representing the CreateExtensionStmt to a buffer
 */
static void
AppendCreateExtensionStmt(StringInfo buf, CreateExtensionStmt *createExtensionStmt)
{
	List *optionsList = createExtensionStmt->options;

	const char *extensionName = createExtensionStmt->extname;
	extensionName = quote_identifier(extensionName);

	/*
	 * We fetch "new_version", "schema" and "cascade" options from
	 * optionList as we will append "IF NOT EXISTS" clause regardless of
	 * statement's content before propagating it to worker nodes.
	 * We also do not care old_version for now.
	 */
	Value *schemaNameValue = GetExtensionOption(optionsList, "schema");

	/* these can be NULL hence check before fetching the stored value */
	Value *newVersionValue = GetExtensionOption(optionsList, "new_version");
	Value *cascadeValue = GetExtensionOption(optionsList, "cascade");

	/*
	 * We do not check for if schemaName is NULL as we append it in deparse
	 * logic if it is not specified.
	 */
	const char *schemaName = strVal(schemaNameValue);
	schemaName = quote_identifier(schemaName);

	appendStringInfo(buf, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s",
					 extensionName, schemaName);

	/* "new_version" may not be specified in CreateExtensionStmt */
	if (newVersionValue)
	{
		const char *newVersion = strVal(newVersionValue);
		newVersion = quote_identifier(newVersion);

		appendStringInfo(buf, " VERSION %s", newVersion);
	}

	/* "cascade" may not be specified in CreateExtensionStmt */
	if (cascadeValue)
	{
		bool cascade = intVal(cascadeValue);

		if (cascade)
		{
			appendStringInfoString(buf, " CASCADE");
		}
	}

	appendStringInfoString(buf, " ;");
}


/*
 * DeparseAlterExtensionStmt builds and returns a string representing the
 * AlterExtensionStmt to be sent to worker nodes.
 */
char *
DeparseAlterExtensionStmt(Node *node)
{
	AlterExtensionStmt *stmt = castNode(AlterExtensionStmt, node);
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendAlterExtensionStmt(&sql, stmt);

	return sql.data;
}


/*
 * AppendAlterExtensionStmt appends a string representing the AlterExtensionStmt to a buffer
 */
static void
AppendAlterExtensionStmt(StringInfo buf, AlterExtensionStmt *alterExtensionStmt)
{
	List *optionsList = alterExtensionStmt->options;

	const char *extensionName = alterExtensionStmt->extname;
	extensionName = quote_identifier(extensionName);

	Value *newVersionValue = GetExtensionOption(optionsList, "new_version");

	appendStringInfo(buf, "ALTER EXTENSION %s UPDATE ", extensionName);

	/* "new_version" may not be specified in AlterExtensionStmt */
	if (newVersionValue)
	{
		const char *newVersion = strVal(newVersionValue);
		newVersion = quote_identifier(newVersion);

		appendStringInfo(buf, " TO %s", newVersion);
	}

	appendStringInfoString(buf, ";");
}


/*
 * DeparseDropExtensionStmt builds and returns a string representing the DropStmt
 */
char *
DeparseDropExtensionStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendDropExtensionStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropExtensionStmt appends a string representing the DropStmt for
 * an extension to a buffer.
 */
static void
AppendDropExtensionStmt(StringInfo str, DropStmt *dropStmt)
{
	/* we append "IF NOT EXISTS" clause regardless of the content of the statement. */
	appendStringInfoString(str, "DROP EXTENSION IF EXISTS ");

	/*
	 * Pick the distributed ones from the  "objects" list that is storing
	 * the object names to be deleted.
	 */
	AppendExtensionNameList(str, dropStmt->objects);

	/* depending on behaviour field of DropStmt, we should append CASCADE or RESTRICT */
	if (dropStmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(str, " CASCADE;");
	}
	else
	{
		appendStringInfoString(str, " RESTRICT;");
	}
}


/*
 * AppendExtensionNameList appends a string representing the list of
 * extension names to a buffer.
 */
static void
AppendExtensionNameList(StringInfo str, List *objects)
{
	ListCell *objectCell = NULL;

	foreach(objectCell, objects)
	{
		const char *extensionName = strVal(lfirst(objectCell));
		extensionName = quote_identifier(extensionName);

		if (objectCell != list_head(objects))
		{
			appendStringInfo(str, ", ");
		}

		appendStringInfoString(str, extensionName);
	}
}


/*
 * DeparseAlterExtensionSchemaStmt builds and returns a string representing the
 * AlterObjectSchemaStmt (ALTER EXTENSION SET SCHEMA).
 */
char *
DeparseAlterExtensionSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_EXTENSION);

	AppendAlterExtensionSchemaStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterExtensionSchemaStmt appends a string representing the AlterObjectSchemaStmt
 * for an extension to a buffer.
 */
static void
AppendAlterExtensionSchemaStmt(StringInfo buf,
							   AlterObjectSchemaStmt *alterExtensionSchemaStmt)
{
	Assert(alterExtensionSchemaStmt->objectType == OBJECT_EXTENSION);

	const char *extensionName = strVal(alterExtensionSchemaStmt->object);
	const char *newSchemaName = alterExtensionSchemaStmt->newschema;

	extensionName = quote_identifier(extensionName);
	newSchemaName = quote_identifier(newSchemaName);

	appendStringInfo(buf, "ALTER EXTENSION %s SET SCHEMA %s;", extensionName,
					 newSchemaName);
}
