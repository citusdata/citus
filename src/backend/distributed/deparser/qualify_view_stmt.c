/*-------------------------------------------------------------------------
 *
 * qualify_view_stmt.c
 *	  Functions specialized in fully qualifying all view statements. These
 *	  functions are dispatched from qualify.c
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "nodes/nodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

/*
 * QualifyDropViewStmt quailifies the view names of the DROP VIEW statement.
 */
void
QualifyDropViewStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	List *qualifiedViewNames = NIL;

	List *viewName = NULL;
	foreach_ptr(viewName, stmt->objects)
	{
		/*
		 * If the view name is not qualified, qualify it. Else use it directly
		 */
		if (list_length(viewName) == 1)
		{
			char *objname = NULL;
			Oid schemaOid = QualifiedNameGetCreationNamespace(viewName, &objname);
			char *schemaName = get_namespace_name(schemaOid);
			List *qualifiedViewName = list_make2(makeString(schemaName),
												 linitial(viewName));
			qualifiedViewNames = lappend(qualifiedViewNames, qualifiedViewName);
		}
		else
		{
			qualifiedViewNames = lappend(qualifiedViewNames, viewName);
		}
	}

	stmt->objects = qualifiedViewNames;
}
