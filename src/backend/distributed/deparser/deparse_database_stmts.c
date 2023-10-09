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
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

#include "distributed/deparser.h"
#include "distributed/citus_ruleutils.h"
#include "commands/defrem.h"
#include "distributed/deparser.h"
#include "distributed/log_utils.h"
#include "parser/parse_type.h"
#include "distributed/listutils.h"

static void AppendAlterDatabaseOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendAlterDatabaseStmt(StringInfo buf, AlterDatabaseStmt *stmt);
static void AppendDefElemConnLimit(StringInfo buf, DefElem *def);

const struct option_format create_database_option_formats[] = {
	{ "template", " TEMPLATE %s", "string" },
	{ "owner", " OWNER %s", "string" },
	{ "tablespace", " TABLESPACE %s", "string" },
	{ "connection_limit", " CONNECTION LIMIT %d", "integer" },
	{ "encoding", " ENCODING %s", "literal_cstr" },
	{ "locale", " LOCALE %s", "literal_cstr" },
	{ "lc_collate", " LC_COLLATE %s", "literal_cstr" },
	{ "lc_ctype", " LC_CTYPE %s", "literal_cstr" },
	{ "icu_locale", " ICU_LOCALE %s", "literal_cstr" },
	{ "icu_rules", " ICU_RULES %s", "literal_cstr" },
	{ "locale_provider", " LOCALE_PROVIDER %s", "literal_cstr" },
	{ "is_template", " IS_TEMPLATE %s", "boolean" },
	{ "allow_connections", " ALLOW_CONNECTIONS %s", "boolean" },
	{ "collation_version", " COLLATION_VERSION %s", "literal_cstr" },
	{ "strategy", " STRATEGY %s", "literal_cstr" },
	{ "oid", " OID %d", "object_id" },
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
AppendDefElemConnLimit(StringInfo buf, DefElem *def)
{
	appendStringInfo(buf, " CONNECTION LIMIT %ld", (long int) defGetNumeric(def));
}


static void
AppendAlterDatabaseStmt(StringInfo buf, AlterDatabaseStmt *stmt)
{
	appendStringInfo(buf, "ALTER DATABASE %s ", quote_identifier(stmt->dbname));

	if (stmt->options)
	{
		ListCell *cell = NULL;
		appendStringInfo(buf, "WITH ");
		foreach(cell, stmt->options)
		{
			DefElem *def = castNode(DefElem, lfirst(cell));
			if (strcmp(def->defname, "is_template") == 0)
			{
				appendStringInfo(buf, "IS_TEMPLATE %s",
								 quote_literal_cstr(strVal(def->arg)));
			}
			else if (strcmp(def->defname, "connection_limit") == 0)
			{
				AppendDefElemConnLimit(buf, def);
			}
			else if (strcmp(def->defname, "allow_connections") == 0)
			{
				ereport(ERROR,
						errmsg("ALLOW_CONNECTIONS is not supported"));
			}
			else
			{
				ereport(ERROR,
						errmsg("unrecognized ALTER DATABASE option: %s",
							   def->defname));
			}
		}
	}

	appendStringInfo(buf, ";");
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
	appendStringInfo(buf,
					 "CREATE DATABASE %s",
					 quote_identifier(stmt->dbname));

	DefElem *option = NULL;

	foreach_ptr(option, stmt->options)
	{
		handleOption(buf, option, create_database_option_formats, lengthof(
						 create_database_option_formats));
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
	appendStringInfo(buf,
					 "DROP DATABASE %s",
					 quote_identifier(stmt->dbname));

	DefElem *option = NULL;

	foreach_ptr(option, stmt->options)
	{
		if (strcmp(option->defname, "force") == 0)
		{
			appendStringInfo(buf, " FORCE");
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("unrecognized DROP DATABASE option \"%s\"",
								   option->defname)));
		}
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
