/*-------------------------------------------------------------------------
 *
 * deparse.c
 *    Entrypoint for deparsing parsetrees.
 *
 *    The goal of deparsing parsetrees is to reconstruct sql statements
 *    from any parsed sql statement by ParseTreeNode. Deparsed statements
 *    can be used to reapply them on remote postgres nodes like the citus
 *    workers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"

/*
 * DeparseTreeNode aims to be the inverse of postgres' ParseTreeNode. Currently with
 * limited support. Check support before using, and add support for new statements as
 * required.
 */
char *
DeparseTreeNode(Node *stmt)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);

	if (!ops->deparse)
	{
		ereport(ERROR, (errmsg("unsupported statement for deparsing")));
	}

	return ops->deparse(stmt);
}


/*
 * DeparseTreeNodes deparses all stmts in the list from the statement datastructure into
 * sql statements.
 */
List *
DeparseTreeNodes(List *stmts)
{
	List *sqls = NIL;
	Node *stmt = NULL;
	foreach_declared_ptr(stmt, stmts)
	{
		sqls = lappend(sqls, DeparseTreeNode(stmt));
	}
	return sqls;
}
