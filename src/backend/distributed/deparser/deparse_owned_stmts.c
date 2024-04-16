/*-------------------------------------------------------------------------
 *
 * deparse_owned_stmts.c
 *	  Functions to turn all Statement structures related to owned back
 *    into sql.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"

static void AppendDropOwnedStmt(StringInfo buf, DropOwnedStmt *stmt);
static void AppendRoleList(StringInfo buf, List *roleList);

/*
 * DeparseDropOwnedStmt builds and returns a string representing of the
 * DropOwnedStmt for application on a remote server.
 */
char *
DeparseDropOwnedStmt(Node *node)
{
	DropOwnedStmt *stmt = castNode(DropOwnedStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	AppendDropOwnedStmt(&buf, stmt);

	return buf.data;
}


/*
 * AppendDropOwnedStmt generates the string representation of the
 * DropOwnedStmt and appends it to the buffer.
 */
static void
AppendDropOwnedStmt(StringInfo buf, DropOwnedStmt *stmt)
{
	appendStringInfo(buf, "DROP OWNED BY ");

	AppendRoleList(buf, stmt->roles);

	if (stmt->behavior == DROP_RESTRICT)
	{
		appendStringInfo(buf, " RESTRICT");
	}
	else if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfo(buf, " CASCADE");
	}
}


static void
AppendRoleList(StringInfo buf, List *roleList)
{
	ListCell *cell = NULL;
	foreach(cell, roleList)
	{
		Node *roleNode = (Node *) lfirst(cell);
		Assert(IsA(roleNode, RoleSpec) || IsA(roleNode, AccessPriv));
		char const *rolename = NULL;
		if (IsA(roleNode, RoleSpec))
		{
			rolename = RoleSpecString((RoleSpec *) roleNode, true);
		}
		appendStringInfoString(buf, rolename);
		if (cell != list_tail(roleList))
		{
			appendStringInfo(buf, ", ");
		}
	}
}
