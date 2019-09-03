/*-------------------------------------------------------------------------
 *
 * qualify.c
 *	  The deparser relies on fully qualified names on all statements to
 *	  work transparently on a remote worker node. Here we have helpers to
 *	  fully qualify parsetrees
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"
static void qualify_rename_stmt(RenameStmt *stmt);


/*
 * QualifyTreeNode transforms the statement in place and makes all (supported) statements
 * fully qualified. Fully qualified statements allow for application on a remote postgres
 * server irregardless of their search_path.
 */
void
QualifyTreeNode(Node *stmt)
{
	switch (nodeTag(stmt))
	{
		case T_RenameStmt:
		{
			qualify_rename_stmt(castNode(RenameStmt, stmt));
			return;
		}

		default:
		{
			/* skip unsupported statements */
			break;
		}
	}
}


static void
qualify_rename_stmt(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_TYPE:
		{
			qualify_rename_type_stmt(stmt);
			return;
		}

		default:
		{
			/* skip unsupported statements */
			break;
		}
	}
}
