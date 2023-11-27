/*-------------------------------------------------------------------------
 *
 * deparse_database_stmts.c
 *
 *	  All routines to deparse database statements.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_version_compat.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#include "commands/defrem.h"
#include "distributed/commands.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"


static void AppendAlterDatabaseOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendAlterDatabaseSetStmt(StringInfo buf, AlterDatabaseSetStmt *stmt);
static void AppendAlterDatabaseStmt(StringInfo buf, AlterDatabaseStmt *stmt);
static void AppendCreateDatabaseStmt(StringInfo buf, CreatedbStmt *stmt);
static void AppendDropDatabaseStmt(StringInfo buf, DropdbStmt *stmt);
static void AppendGrantOnDatabaseStmt(StringInfo buf, GrantStmt *stmt);
static void AppendBasicAlterDatabaseOptions(StringInfo buf, DefElem *def, bool *
											prefixAppendedForBasicOptions, char *dbname);
static void AppendGrantDatabases(StringInfo buf, GrantStmt *stmt);
static void AppendAlterDatabaseSetTablespace(StringInfo buf, DefElem *def, char *dbname);

const DefElemOptionFormat createDatabaseOptionFormats[] = {
	{ "owner", " OWNER %s", OPTION_FORMAT_STRING },
	{ "template", " TEMPLATE %s", OPTION_FORMAT_STRING },
	{ "encoding", " ENCODING %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "strategy", " STRATEGY %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "locale", " LOCALE %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "lc_collate", " LC_COLLATE %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "lc_ctype", " LC_CTYPE %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "icu_locale", " ICU_LOCALE %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "icu_rules", " ICU_RULES %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "locale_provider", " LOCALE_PROVIDER %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "collation_version", " COLLATION_VERSION %s", OPTION_FORMAT_LITERAL_CSTR },
	{ "tablespace", " TABLESPACE %s", OPTION_FORMAT_STRING },
	{ "allow_connections", " ALLOW_CONNECTIONS %s", OPTION_FORMAT_BOOLEAN },
	{ "connection_limit", " CONNECTION LIMIT %d", OPTION_FORMAT_INTEGER },
	{ "is_template", " IS_TEMPLATE %s", OPTION_FORMAT_BOOLEAN }
};


const DefElemOptionFormat alterDatabaseOptionFormats[] = {
	{ "is_template", " IS_TEMPLATE %s", OPTION_FORMAT_BOOLEAN },
	{ "allow_connections", " ALLOW_CONNECTIONS %s", OPTION_FORMAT_BOOLEAN },
	{ "connection_limit", " CONNECTION LIMIT %d", OPTION_FORMAT_INTEGER },
};


/*
 * DeparseAlterDatabaseOwnerStmt
 *      Deparse an AlterDatabaseOwnerStmt node
 *
 * This function is responsible for producing a string representation of an
 * AlterDatabaseOwnerStmt node, which represents an ALTER DATABASE statement
 * that changes the owner of a database. The output string includes the ALTER
 * DATABASE keyword, the name of the database being altered, and the new owner
 * of the database.
 *
 * Parameters:
 *  - node: a pointer to the AlterDatabaseOwnerStmt node to be deparsed
 *
 * Returns:
 *  - a string representation of the ALTER DATABASE statement
 */
char *
DeparseAlterDatabaseOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_DATABASE);

	AppendAlterDatabaseOwnerStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterDatabaseOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_DATABASE);

	appendStringInfo(buf,
					 "ALTER DATABASE %s OWNER TO %s;",
					 quote_identifier(strVal((String *) stmt->object)),
					 RoleSpecString(stmt->newowner, true));
}


char *
DeparseGrantOnDatabaseStmt(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_DATABASE);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendGrantOnDatabaseStmt(&str, stmt);

	return str.data;
}


static void
AppendGrantOnDatabaseStmt(StringInfo buf, GrantStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_DATABASE);

	AppendGrantSharedPrefix(buf, stmt);

	AppendGrantDatabases(buf, stmt);

	AppendGrantSharedSuffix(buf, stmt);
}


static void
AppendGrantDatabases(StringInfo buf, GrantStmt *stmt)
{
	ListCell *cell = NULL;
	appendStringInfo(buf, " ON DATABASE ");

	foreach(cell, stmt->objects)
	{
		char *database = strVal(lfirst(cell));
		appendStringInfoString(buf, quote_identifier(database));
		if (cell != list_tail(stmt->objects))
		{
			appendStringInfo(buf, ", ");
		}
	}
}


char *
DeparseAlterDatabaseStmt(Node *node)
{
	AlterDatabaseStmt *stmt = castNode(AlterDatabaseStmt, node);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterDatabaseStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterDatabaseStmt(StringInfo buf, AlterDatabaseStmt *stmt)
{
	if (stmt->options)
	{
		ListCell *cell = NULL;
		bool prefixAppendedForBasicOptions = false;
		foreach(cell, stmt->options)
		{
			DefElem *def = castNode(DefElem, lfirst(cell));
			if (strcmp(def->defname, "tablespace") == 0)
			{
				AppendAlterDatabaseSetTablespace(buf, def, stmt->dbname);
				break;
			}
			else
			{
				AppendBasicAlterDatabaseOptions(buf, def, &prefixAppendedForBasicOptions,
												stmt->dbname);
			}
		}
	}

	appendStringInfo(buf, ";");
}


static void
AppendAlterDatabaseSetTablespace(StringInfo buf, DefElem *def, char *dbname)
{
	appendStringInfo(buf,
					 "ALTER DATABASE %s SET TABLESPACE %s",
					 quote_identifier(dbname), quote_identifier(defGetString(def)));
}


/*
 * Appends basic ALTER DATABASE options to a string buffer.
 * Basic options are those that can be appended to the ALTER DATABASE statement
 * after the "WITH" keyword.(i.e. ALLOW_CONNECTIONS, CONNECTION LIMIT, IS_TEMPLATE)
 * The tablespace option is not a basic option since it is defined with SET option.
 *
 * This function takes a string buffer, a DefElem representing a database option,
 * a boolean indicating whether the prefix "ALTER DATABASE <dbname> WITH" has
 * already been appended, and a database name. It appends the SQL representation
 * of the database option to the string buffer.
 *
 * Returns:
 *   A boolean indicating whether the prefix "ALTER DATABASE <dbname> WITH" has
 *   been appended to the buffer. This is the same as the input
 *   prefixAppendedForBasicOptions if the prefix was already appended, or true
 *   if this function appended the prefix.
 */
static void
AppendBasicAlterDatabaseOptions(StringInfo buf, DefElem *def, bool *
								prefixAppendedForBasicOptions, char *dbname)
{
	if (!(*prefixAppendedForBasicOptions))
	{
		appendStringInfo(buf, "ALTER DATABASE %s WITH", quote_identifier(dbname));
		*prefixAppendedForBasicOptions = true;
	}
	DefElemOptionToStatement(buf, def, alterDatabaseOptionFormats, lengthof(
								 alterDatabaseOptionFormats));
}


#if PG_VERSION_NUM >= PG_VERSION_15
char *
DeparseAlterDatabaseRefreshCollStmt(Node *node)
{
	AlterDatabaseRefreshCollStmt *stmt = (AlterDatabaseRefreshCollStmt *) node;

	StringInfoData str;
	initStringInfo(&str);

	appendStringInfo(&str, "ALTER DATABASE %s REFRESH COLLATION VERSION;",
					 quote_identifier(
						 stmt->dbname));

	return str.data;
}


#endif

/*
 * Deparses an ALTER DATABASE RENAME statement.
 *
 * This function takes a Node pointer representing an ALTER DATABASE RENAME
 * statement, and returns a string that is the SQL representation of that
 * statement.
 *
 * Parameters:
 *   node: A pointer to a Node representing an ALTER DATABASE RENAME statement.
 *
 * Returns:
 *   A string representing the SQL command to rename a database. The string is
 *   in the format "ALTER DATABASE <oldname> RENAME TO <newname>", where
 *   <oldname> and <newname> are the old and new database names, respectively.
 */
char *
DeparseAlterDatabaseRenameStmt(Node *node)
{
	RenameStmt *stmt = (RenameStmt *) node;

	StringInfoData str;
	initStringInfo(&str);

	appendStringInfo(&str, "ALTER DATABASE %s RENAME TO %s",
					 quote_identifier(stmt->subname),
					 quote_identifier(stmt->newname));

	return str.data;
}


char *
DeparseAlterDatabaseSetStmt(Node *node)
{
	AlterDatabaseSetStmt *stmt = castNode(AlterDatabaseSetStmt, node);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterDatabaseSetStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterDatabaseSetStmt(StringInfo buf, AlterDatabaseSetStmt *stmt)
{
	appendStringInfo(buf, "ALTER DATABASE %s", quote_identifier(stmt->dbname));

	VariableSetStmt *varSetStmt = castNode(VariableSetStmt, stmt->setstmt);

	AppendVariableSet(buf, varSetStmt);
}


char *
DeparseCreateDatabaseStmt(Node *node)
{
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendCreateDatabaseStmt(&str, stmt);

	return str.data;
}


static void
AppendCreateDatabaseStmt(StringInfo buf, CreatedbStmt *stmt)
{
	appendStringInfo(buf,
					 "CREATE DATABASE %s",
					 quote_identifier(stmt->dbname));

	DefElem *option = NULL;
	foreach_ptr(option, stmt->options)
	{
		DefElemOptionToStatement(buf, option, createDatabaseOptionFormats,
								 lengthof(createDatabaseOptionFormats));
	}
}


char *
DeparseDropDatabaseStmt(Node *node)
{
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendDropDatabaseStmt(&str, stmt);

	return str.data;
}


static void
AppendDropDatabaseStmt(StringInfo buf, DropdbStmt *stmt)
{
	char *ifExistsStatement = stmt->missing_ok ? "IF EXISTS" : "";
	appendStringInfo(buf,
					 "DROP DATABASE %s %s",
					 ifExistsStatement,
					 quote_identifier(stmt->dbname));

	if (list_length(stmt->options) > 1)
	{
		/* FORCE is the only option that can be provided for this command */
		elog(ERROR, "got unexpected number of options for DROP DATABASE");
	}
	else if (list_length(stmt->options) == 1)
	{
		DefElem *option = linitial(stmt->options);
		appendStringInfo(buf, " WITH ( ");

		if (strcmp(option->defname, "force") == 0)
		{
			appendStringInfo(buf, "FORCE");
		}
		else
		{
			/* FORCE is the only option that can be provided for this command */
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("unrecognized DROP DATABASE option \"%s\"",
								   option->defname)));
		}

		appendStringInfo(buf, " )");
	}
}
