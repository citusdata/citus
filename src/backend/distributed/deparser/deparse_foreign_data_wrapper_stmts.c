/*-------------------------------------------------------------------------
 *
 * deparse_foreign_data_wrapper_stmts.c
 *	  All routines to deparse foreign data wrapper statements.
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

static void AppendGrantOnFDWStmt(StringInfo buf, GrantStmt *stmt);
static void AppendGrantOnFDWNames(StringInfo buf, GrantStmt *stmt);


char *
DeparseGrantOnFDWStmt(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_FDW);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendGrantOnFDWStmt(&str, stmt);

	return str.data;
}


static void
AppendGrantOnFDWStmt(StringInfo buf, GrantStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_FDW);

	appendStringInfo(buf, "%s ", stmt->is_grant ? "GRANT" : "REVOKE");

	if (!stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, "GRANT OPTION FOR ");
	}

	AppendGrantPrivileges(buf, stmt);

	AppendGrantOnFDWNames(buf, stmt);

	AppendGrantGrantees(buf, stmt);

	if (stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, " WITH GRANT OPTION");
	}
	if (!stmt->is_grant)
	{
		if (stmt->behavior == DROP_RESTRICT)
		{
			appendStringInfo(buf, " RESTRICT");
		}
		else if (stmt->behavior == DROP_CASCADE)
		{
			appendStringInfo(buf, " CASCADE");
		}
	}
	appendStringInfo(buf, ";");
}


static void
AppendGrantOnFDWNames(StringInfo buf, GrantStmt *stmt)
{
	ListCell *cell = NULL;
	appendStringInfo(buf, " ON FOREIGN DATA WRAPPER ");

	foreach(cell, stmt->objects)
	{
		char *fdwname = strVal(lfirst(cell));

		appendStringInfoString(buf, quote_identifier(fdwname));
		if (cell != list_tail(stmt->objects))
		{
			appendStringInfo(buf, ", ");
		}
	}
}
