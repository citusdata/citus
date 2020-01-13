/*-------------------------------------------------------------------------
 *
 * deparse_attribute_stmts.c
 *	  All routines to deparse attribute statements.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"

char *
DeparseRenameAttributeStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return DeparseRenameTypeAttributeStmt(node);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported rename attribute statement for"
								   " deparsing")));
		}
	}
}
