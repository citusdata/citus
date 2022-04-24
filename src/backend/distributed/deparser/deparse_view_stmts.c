/*-------------------------------------------------------------------------
 *
 * deparse_view_stmts.c
 *
 *	  All routines to deparse view statements.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static void AppendDropViewStmt(StringInfo buf, DropStmt *stmt);
static void AppendViewNameList(StringInfo buf, List *objects);

/*
 * DeparseDropViewStmt deparses the given DROP VIEW statement.
 */
char *
DeparseDropViewStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_VIEW);

	AppendDropViewStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropViewStmt appends the deparsed representation of given drop stmt
 * to the given string info buffer.
 */
static void
AppendDropViewStmt(StringInfo buf, DropStmt *stmt)
{
	/*
	 * already tested at call site, but for future it might be collapsed in a
	 * DeparseDropStmt so be safe and check again
	 */
	Assert(stmt->removeType == OBJECT_VIEW);

	appendStringInfo(buf, "DROP VIEW ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}
	AppendViewNameList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	appendStringInfoString(buf, ";");
}


/*
 * AppendViewNameList appends the qualified view names by constructing them from the given
 * objects list to the given string info buffer. Note that, objects must hold schema
 * qualified view names as its' members.
 */
static void
AppendViewNameList(StringInfo buf, List *viewNamesList)
{
	bool isFirstView = true;
	List *qualifiedViewName = NULL;
	foreach_ptr(qualifiedViewName, viewNamesList)
	{
		char *quotedQualifiedVieName = NameListToQuotedString(qualifiedViewName);
		if (!isFirstView)
		{
			appendStringInfo(buf, ", ");
		}

		appendStringInfoString(buf, quotedQualifiedVieName);
		isFirstView = false;
	}
}
