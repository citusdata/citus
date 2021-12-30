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

	if (!ShouldPropagate() || stmt->relation == NULL)
	{
		return NIL;
	}

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt,
														  stmt->missing_ok);
	Oid relationId = address.objectId;

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

	char *sql = DeparseTreeNode((Node *) stmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = relationId;
	ddlJob->metadataSyncCommand = sql;
	ddlJob->taskList = DDLTaskList(relationId, ddlJob->metadataSyncCommand);

	return list_make1(ddlJob);
}
