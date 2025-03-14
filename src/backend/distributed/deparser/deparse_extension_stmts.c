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
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

/* Local functions forward declarations for helper functions */
static void AppendCreateExtensionStmt(StringInfo buf, CreateExtensionStmt *stmt);
static void AppendCreateExtensionStmtOptions(StringInfo buf, List *options);
static void AppendDropExtensionStmt(StringInfo buf, DropStmt *stmt);
static void AppendExtensionNameList(StringInfo buf, List *objects);
static void AppendAlterExtensionSchemaStmt(StringInfo buf,
										   AlterObjectSchemaStmt *alterExtensionSchemaStmt);
static void AppendAlterExtensionStmt(StringInfo buf,
									 AlterExtensionStmt *alterExtensionStmt);


/*
 * GetExtensionOption returns DefElem * node with "defname" from "options" list
 */
DefElem *
GetExtensionOption(List *extensionOptions, const char *defname)
{
	DefElem *defElement = NULL;
	foreach_declared_ptr(defElement, extensionOptions)
	{
		if (IsA(defElement, DefElem) &&
			strncmp(defElement->defname, defname, NAMEDATALEN) == 0)
		{
			return defElement;
		}
	}

	return NULL;
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
	appendStringInfoString(buf, "CREATE EXTENSION ");

	if (createExtensionStmt->if_not_exists)
	{
		appendStringInfoString(buf, "IF NOT EXISTS ");
	}

	/*
	 * Up until here we have been ending the statement in a space, which makes it possible
	 * to just append the quoted extname. From here onwards we will not have the string
	 * ending in a space so appends should begin with a whitespace.
	 */
	appendStringInfoString(buf, quote_identifier(createExtensionStmt->extname));
	AppendCreateExtensionStmtOptions(buf, createExtensionStmt->options);
	appendStringInfoString(buf, ";");
}


/*
 * AppendCreateExtensionStmtOptions takes the option list of a CreateExtensionStmt and
 * loops over the options to add them to the statement we are building.
 *
 * An error will be thrown if we run into an unsupported option, comparable to how
 * postgres gives an error when parsing this list.
 */
static void
AppendCreateExtensionStmtOptions(StringInfo buf, List *options)
{
	if (list_length(options) > 0)
	{
		/* only append WITH if we actual will add options to the statement */
		appendStringInfoString(buf, " WITH");
	}

	/* Add the options to the statement */
	DefElem *defElem = NULL;
	foreach_declared_ptr(defElem, options)
	{
		if (strcmp(defElem->defname, "schema") == 0)
		{
			const char *schemaName = defGetString(defElem);
			appendStringInfo(buf, " SCHEMA  %s", quote_identifier(schemaName));
		}
		else if (strcmp(defElem->defname, "new_version") == 0)
		{
			const char *newVersion = defGetString(defElem);
			appendStringInfo(buf, " VERSION %s", quote_identifier(newVersion));
		}
		else if (strcmp(defElem->defname, "old_version") == 0)
		{
			const char *oldVersion = defGetString(defElem);
			appendStringInfo(buf, " FROM %s", quote_identifier(oldVersion));
		}
		else if (strcmp(defElem->defname, "cascade") == 0)
		{
			bool cascade = defGetBoolean(defElem);
			if (cascade)
			{
				appendStringInfoString(buf, " CASCADE");
			}
		}
		else
		{
			elog(ERROR, "unrecognized option: %s", defElem->defname);
		}
	}
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

	appendStringInfo(buf, "ALTER EXTENSION %s UPDATE", extensionName);

	/*
	 * Append the options for ALTER EXTENSION ... UPDATE
	 * Currently there is only 1 option, but this structure follows how postgres parses
	 * the options.
	 */
	DefElem *option = NULL;
	foreach_declared_ptr(option, optionsList)
	{
		if (strcmp(option->defname, "new_version") == 0)
		{
			const char *newVersion = defGetString(option);
			appendStringInfo(buf, " TO %s", quote_identifier(newVersion));
		}
		else
		{
			elog(ERROR, "unrecognized option: %s", option->defname);
		}
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
