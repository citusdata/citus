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
#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"


static void AppendCreatedbStmt(StringInfo buf, CreatedbStmt *stmt);
static void AppendAlterDatabaseOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendDropdbStmt(StringInfo buf, DropdbStmt *stmt);


char *
DeparseCreatedbStmt(Node *node)
{
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendCreatedbStmt(&str, stmt);

	return str.data;
}


static void
AppendCreatedbStmt(StringInfo buf, CreatedbStmt *stmt)
{
	appendStringInfo(buf,
					 "CREATE DATABASE %s",
					 quote_identifier(stmt->dbname));

	DefElem *option = NULL;

	foreach_ptr(option, stmt->options)
	{
		if (strcmp(option->defname, "tablespace") == 0)
		{
			char *tablespaceName = defGetString(option);

			appendStringInfo(buf, " TABLESPACE %s",
							 quote_identifier(tablespaceName));
		}
		else if (strcmp(option->defname, "owner") == 0)
		{
			char *owner = defGetString(option);

			appendStringInfo(buf, " OWNER %s",
							 quote_identifier(owner));
		}
		else if (strcmp(option->defname, "template") == 0)
		{
			char *template = defGetString(option);

			appendStringInfo(buf, " TEMPLATE %s",
							 quote_identifier(template));
		}
		else if (strcmp(option->defname, "encoding") == 0)
		{
			char *encoding = defGetString(option);

			appendStringInfo(buf, " ENCODING %s",
							 quote_literal_cstr(encoding));
		}
		else if (strcmp(option->defname, "locale") == 0)
		{
			char *locale = defGetString(option);

			appendStringInfo(buf, " LOCALE %s",
							 quote_literal_cstr(locale));
		}
		else if (strcmp(option->defname, "lc_collate") == 0)
		{
			char *lc_collate = defGetString(option);

			appendStringInfo(buf, " LC_COLLATE %s",
							 quote_literal_cstr(lc_collate));
		}
		else if (strcmp(option->defname, "lc_ctype") == 0)
		{
			char *lc_ctype = defGetString(option);

			appendStringInfo(buf, " LC_CTYPE %s",
							 quote_literal_cstr(lc_ctype));
		}
		else if (strcmp(option->defname, "icuLocale") == 0)
		{
			char *icuLocale = defGetString(option);

			appendStringInfo(buf, " ICU_LOCALE %s",
							 quote_literal_cstr(icuLocale));
		}
		else if (strcmp(option->defname, "localeProvider") == 0)
		{
			char *localeProvider = defGetString(option);

			appendStringInfo(buf, " LOCALE_PROVIDER %s",
							 quote_literal_cstr(localeProvider));
		}
		else if (strcmp(option->defname, "is_template") == 0)
		{
			bool isTemplate = defGetBoolean(option);

			appendStringInfo(buf, " IS_TEMPLATE %s",
							 isTemplate ? "true" : "false");
		}
		else if (strcmp(option->defname, "allow_connections") == 0)
		{
			bool allowConnections = defGetBoolean(option);

			appendStringInfo(buf, " ALLOW_CONNECTIONS %s",
							 allowConnections ? "true" : "false");
		}
		else if (strcmp(option->defname, "connection_limit") == 0)
		{
			int connectionLimit = defGetInt32(option);

			appendStringInfo(buf, " CONNECTION_LIMIT %d", connectionLimit);
		}
#if PG_VERSION_NUM >= PG_VERSION_15
		else if (strcmp(option->defname, "collation_version") == 0)
		{
			char *collationVersion = defGetString(option);

			appendStringInfo(buf, " COLLATION_VERSION %s",
							 quote_literal_cstr(collationVersion));
		}
		else if (strcmp(option->defname, "oid") == 0)
		{
			Oid objectId = defGetObjectId(option);

			appendStringInfo(buf, " OID %d", objectId);
		}
		else if (strcmp(option->defname, "strategy") == 0)
		{
			char *strategy = defGetString(option);

			appendStringInfo(buf, " STRATEGY %s",
							 quote_literal_cstr(strategy));
		}
#endif
		else if (strcmp(option->defname, "location") == 0)
		{
			/* deprecated option */
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
DeparseDropdbStmt(Node *node)
{
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendDropdbStmt(&str, stmt);

	return str.data;
}


static void
AppendDropdbStmt(StringInfo buf, DropdbStmt *stmt)
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
