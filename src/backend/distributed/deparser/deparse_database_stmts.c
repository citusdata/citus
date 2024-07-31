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

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"


static void AppendAlterDatabaseOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendAlterDatabaseSetStmt(StringInfo buf, AlterDatabaseSetStmt *stmt);
static void AppendAlterDatabaseStmt(StringInfo buf, AlterDatabaseStmt *stmt);
static void AppendCreateDatabaseStmt(StringInfo buf, CreatedbStmt *stmt);
static void AppendDropDatabaseStmt(StringInfo buf, DropdbStmt *stmt);
static void AppendGrantOnDatabaseStmt(StringInfo buf, GrantStmt *stmt);
static void AppendBasicAlterDatabaseOptions(StringInfo buf, AlterDatabaseStmt *stmt);
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


static void
AppendGrantOnDatabaseStmt(StringInfo buf, GrantStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_DATABASE);

	AppendGrantSharedPrefix(buf, stmt);

	AppendGrantDatabases(buf, stmt);

	AppendGrantSharedSuffix(buf, stmt);
}


static void
AppendAlterDatabaseStmt(StringInfo buf, AlterDatabaseStmt *stmt)
{
	if (list_length(stmt->options) == 0)
	{
		elog(ERROR, "got unexpected number of options for ALTER DATABASE");
	}

	if (stmt->options)
	{
		DefElem *firstOption = linitial(stmt->options);
		if (strcmp(firstOption->defname, "tablespace") == 0)
		{
			AppendAlterDatabaseSetTablespace(buf, firstOption, stmt->dbname);

			/* SET tablespace cannot be combined with other options */
			return;
		}


		appendStringInfo(buf, "ALTER DATABASE %s WITH",
						 quote_identifier(stmt->dbname));

		AppendBasicAlterDatabaseOptions(buf, stmt);
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
 * AppendBasicAlterDatabaseOptions appends basic ALTER DATABASE options to a string buffer.
 * Basic options are those that can be appended to the ALTER DATABASE statement
 * after the "WITH" keyword.(i.e. ALLOW_CONNECTIONS, CONNECTION LIMIT, IS_TEMPLATE)
 * For example, the tablespace option is not a basic option since it is defined via SET keyword.
 *
 * This function takes a string buffer and an AlterDatabaseStmt as input.
 * It appends the basic options to the string buffer.
 *
 */
static void
AppendBasicAlterDatabaseOptions(StringInfo buf, AlterDatabaseStmt *stmt)
{
	DefElem *def = NULL;
	foreach_declared_ptr(def, stmt->options)
	{
		DefElemOptionToStatement(buf, def, alterDatabaseOptionFormats, lengthof(
									 alterDatabaseOptionFormats));
	}
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


char *
DeparseAlterDatabaseStmt(Node *node)
{
	AlterDatabaseStmt *stmt = castNode(AlterDatabaseStmt, node);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendAlterDatabaseStmt(&str, stmt);

	return str.data;
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

static void
AppendAlterDatabaseSetStmt(StringInfo buf, AlterDatabaseSetStmt *stmt)
{
	appendStringInfo(buf, "ALTER DATABASE %s", quote_identifier(stmt->dbname));

	VariableSetStmt *varSetStmt = castNode(VariableSetStmt, stmt->setstmt);

	AppendVariableSet(buf, varSetStmt);
}


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
AppendCreateDatabaseStmt(StringInfo buf, CreatedbStmt *stmt)
{
	/*
	 * Make sure that we don't try to deparse something that this
	 * function doesn't expect.
	 *
	 * This is also useful to throw an error for unsupported CREATE
	 * DATABASE options when the command is issued from non-main dbs
	 * because we use the same function to deparse CREATE DATABASE
	 * commands there too.
	 */
	EnsureSupportedCreateDatabaseCommand(stmt);

	appendStringInfo(buf,
					 "CREATE DATABASE %s",
					 quote_identifier(stmt->dbname));

	DefElem *option = NULL;
	foreach_declared_ptr(option, stmt->options)
	{
		DefElemOptionToStatement(buf, option, createDatabaseOptionFormats,
								 lengthof(createDatabaseOptionFormats));
	}
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


char *
DeparseDropDatabaseStmt(Node *node)
{
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendDropDatabaseStmt(&str, stmt);

	return str.data;
}
