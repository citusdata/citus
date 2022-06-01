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

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"

static void AppendAlterDatabaseOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);


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
