/*-------------------------------------------------------------------------
 *
 * deparse_foreign_table_stmts.c
 *	  All routines to deparse foreign table statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/relay_utility.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

static void AppendAlterForeignTableSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);


char *
DeparseAlterForeignTableSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

    Assert(stmt->objectType == OBJECT_FOREIGN_TABLE);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterForeignTableSchemaStmt(&str, stmt);

	return str.data;
}


static void
AppendAlterForeignTableSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	char *tableName = quote_qualified_identifier(stmt->relation->schemaname,
												 stmt->relation->relname);

	appendStringInfo(buf, "ALTER FOREIGN TABLE %s%s SET SCHEMA %s",
                     stmt->missing_ok ? "IF EXISTS ":"",
					 tableName, quote_identifier(stmt->newschema));
}
