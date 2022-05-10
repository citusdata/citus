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

	List *possiblyQualifiedViewName = NULL;
	foreach_ptr(possiblyQualifiedViewName, stmt->objects)
	{
		char *viewName = NULL;
		char *schemaName = NULL;
		DeconstructQualifiedName(possiblyQualifiedViewName, &schemaName, &viewName);

		if (schemaName == NULL)
		{
			char *objname = NULL;
			Oid schemaOid = QualifiedNameGetCreationNamespace(possiblyQualifiedViewName,
															  &objname);
			schemaName = get_namespace_name(schemaOid);
			List *qualifiedViewName = list_make2(makeString(schemaName),
												 makeString(viewName));
			qualifiedViewNames = lappend(qualifiedViewNames, qualifiedViewName);
		}
		else
		{
			qualifiedViewNames = lappend(qualifiedViewNames, possiblyQualifiedViewName);
		}
	}

	stmt->objects = qualifiedViewNames;
}
