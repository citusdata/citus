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

static void AppendSecLabelStmt(StringInfo buf, SecLabelStmt *stmt);

/*
 * DeparseSecLabelStmt builds and returns a string representing of the
 * SecLabelStmt for application on a remote server.
 */
char *
DeparseSecLabelStmt(Node *node)
{
	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	AppendSecLabelStmt(&buf, secLabelStmt);

	return buf.data;
}


/*
 * AppendSecLabelStmt generates the string representation of the
 * SecLabelStmt and appends it to the buffer.
 */
static void
AppendSecLabelStmt(StringInfo buf, SecLabelStmt *stmt)
{
	appendStringInfoString(buf, "SECURITY LABEL ");

	if (stmt->provider != NULL)
	{
		appendStringInfo(buf, "FOR %s ", quote_identifier(stmt->provider));
	}

	appendStringInfoString(buf, "ON ");

	switch (stmt->objtype)
	{
		case OBJECT_ROLE:
		{
			char *role_name = strVal(stmt->object);
			appendStringInfo(buf, "ROLE %s ", quote_identifier(role_name));
			break;
		}

		case OBJECT_TABLE:
		{
			List *names = (List *) stmt->object;
			appendStringInfo(buf, "TABLE %s", quote_identifier(strVal(linitial(names))));
			if (list_length(names) > 1)
			{
				appendStringInfo(buf, ".%s", quote_identifier(strVal(lsecond(names))));
			}
			appendStringInfoString(buf, " ");
			break;
		}

		case OBJECT_COLUMN:
		{
			List *names = (List *) stmt->object;
			Assert(list_length(names) >= 2);
			appendStringInfo(buf, "COLUMN %s.%s",
							 quote_identifier(strVal(linitial(names))),
							 quote_identifier(strVal(lsecond(names))));
			if (list_length(names) > 2)
			{
				appendStringInfo(buf, ".%s", quote_identifier(strVal(lthird(names))));
			}
			appendStringInfoString(buf, " ");
			break;
		}

		/* normally, we shouldn't reach this */
		default:
		{
			ereport(ERROR, (errmsg("unsupported security label statement for"
								   " deparsing")));
		}
	}

	appendStringInfoString(buf, "IS ");

	if (stmt->label != NULL)
	{
		appendStringInfo(buf, "%s", quote_literal_cstr(stmt->label));
	}
	else
	{
		appendStringInfoString(buf, "NULL");
	}
}
