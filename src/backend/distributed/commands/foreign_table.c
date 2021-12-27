/*-------------------------------------------------------------------------
 *
 * foreign_table.c
 *    Commands for FOREIGN TABLE statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_foreign_server.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_transaction.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"


/*
 * PreprocessAlterForeignTableSchemaStmt is called during the planning phase for
 * ALTER FOREIGN TABLE .. SET SCHEMA ..
 */
List *
PreprocessAlterForeignTableSchemaStmt(Node *node, const char *queryString,
									  ProcessUtilityContext processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_FOREIGN_TABLE);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	Oid relationId = RangeVarGetRelid(stmt->relation,
									  AccessExclusiveLock,
									  stmt->missing_ok);

	/*
	 * If the table does not exist, don't do anything here to allow PostgreSQL
	 * to throw the appropriate error or notice message later.
	 */
	if (!OidIsValid(relationId))
	{
		return NIL;
	}

	/* skip if it is a regular Postgres table */
	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);

	char *sql = DeparseTreeNode(node);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}
