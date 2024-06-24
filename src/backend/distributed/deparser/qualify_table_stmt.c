/*-------------------------------------------------------------------------
 *
 * qualify_table_stmt.c
 *	  Functions specialized in fully qualifying all table statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying table statements consists of adding the schema name
 *	  to the subject of the table as well as any other branch of the
 *	  parsetree.
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "distributed/deparser.h"

void
QualifyAlterTableSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

	if (stmt->relation->schemaname == NULL)
	{
		Oid tableOid = RelnameGetRelid(stmt->relation->relname);
		Oid schemaOid = get_rel_namespace(tableOid);
		stmt->relation->schemaname = get_namespace_name(schemaOid);
	}
}
