/*-------------------------------------------------------------------------
 *
 * deparse_seclabel_stmts.c
 *	  All routines to deparse SECURITY LABEL statements.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

#include "distributed/deparser.h"

static void
BeginSecLabel(StringInfo buf, SecLabelStmt *stmt)
{
	initStringInfo(buf);
	appendStringInfoString(buf, "SECURITY LABEL ");

	if (stmt->provider != NULL)
	{
		appendStringInfo(buf, "FOR %s ", quote_identifier(stmt->provider));
	}

	appendStringInfoString(buf, "ON ");
}


static void
EndSecLabel(StringInfo buf, SecLabelStmt *stmt)
{
	appendStringInfo(buf, "IS %s", (stmt->label != NULL) ?
					 quote_literal_cstr(stmt->label) : "NULL");
}


/*
 * DeparseRoleSecLabelStmt builds and returns a string representation of the
 * SecLabelStmt for application on a remote server. The SecLabelStmt is for
 * a role object.
 */
char *
DeparseRoleSecLabelStmt(Node *node)
{
	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);
	char *role_name = strVal(secLabelStmt->object);
	StringInfoData buf = { 0 };

	BeginSecLabel(&buf, secLabelStmt);
	appendStringInfo(&buf, "ROLE %s ", quote_identifier(role_name));
	EndSecLabel(&buf, secLabelStmt);

	return buf.data;
}


/*
 * DeparseTableSecLabelStmt builds and returns a string representation of the
 * SecLabelStmt for application on a remote server. The SecLabelStmt is for	a
 * table.
 */
char *
DeparseTableSecLabelStmt(Node *node)
{
	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);
	List *names = (List *) secLabelStmt->object;
	StringInfoData buf = { 0 };

	BeginSecLabel(&buf, secLabelStmt);
	appendStringInfo(&buf, "TABLE %s", quote_identifier(strVal(linitial(names))));
	if (list_length(names) > 1)
	{
		appendStringInfo(&buf, ".%s", quote_identifier(strVal(lsecond(names))));
	}
	appendStringInfoString(&buf, " ");
	EndSecLabel(&buf, secLabelStmt);

	return buf.data;
}


/*
 * DeparseColumnSecLabelStmt builds and returns a string representation of the
 * SecLabelStmt for application on a remote server. The SecLabelStmt is for	a
 * column of a distributed table.
 */
char *
DeparseColumnSecLabelStmt(Node *node)
{
	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);
	List *names = (List *) secLabelStmt->object;
	StringInfoData buf = { 0 };

	BeginSecLabel(&buf, secLabelStmt);
	appendStringInfo(&buf, "COLUMN %s.%s",
					 quote_identifier(strVal(linitial(names))),
					 quote_identifier(strVal(lsecond(names))));
	if (list_length(names) > 2)
	{
		appendStringInfo(&buf, ".%s", quote_identifier(strVal(lthird(names))));
	}
	appendStringInfoString(&buf, " ");
	EndSecLabel(&buf, secLabelStmt);

	return buf.data;
}
