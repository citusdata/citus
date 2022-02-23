/*-------------------------------------------------------------------------
 *
 * qualify_aggregate_stmts.c
 *	  Functions specialized in fully qualifying all aggregate statements.
 *    These functions are dispatched from qualify.c
 *
 *	  Fully qualifying aggregate statements consists of adding the schema name
 *	  to the subject of the types as well as any other branch of the parsetree.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/deparser.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

void
QualifyDefineAggregateStmt(Node *node)
{
	DefineStmt *stmt = castNode(DefineStmt, node);

	if (list_length(stmt->defnames) == 1)
	{
		char *objname = NULL;
		Oid creationSchema = QualifiedNameGetCreationNamespace(stmt->defnames, &objname);
		stmt->defnames = list_make2(makeString(get_namespace_name(creationSchema)),
									linitial(stmt->defnames));
	}
}
