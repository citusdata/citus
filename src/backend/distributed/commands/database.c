/*-------------------------------------------------------------------------
 *
 * database.c
 *    Commands to interact with the database object in a distributed
 *    environment.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "nodes/parsenodes.h"
#include "server/access/xact.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_transaction.h"

static void EnsureSequentialModeForDatabaseDDL(void);


List *
PreprocessAlterDatabaseOwnerStmt(Node *node, const char *queryString,
								 ProcessUtilityContext processUtilityContext)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DATABASE);

	ObjectAddress typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForDatabaseDDL();
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


List *
PostprocessAlterDatabaseOwnerStmt(Node *node, const char *queryString)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DATABASE);

	ObjectAddress typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&typeAddress);
	return NIL;
}


/*
 * AlterTypeOwnerObjectAddress returns the ObjectAddress of the type that is the object
 * of the AlterOwnerStmt. Errors if missing_ok is false.
 */
ObjectAddress
AlterDatabaseOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DATABASE);

	Oid databaseOid = get_database_oid(strVal((Value *) stmt->object), missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, DatabaseRelationId, databaseOid);

	return address;
}


/*
 * EnsureSequentialModeForDatabaseDDL makes sure that the current transaction is already
 * in sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginning.
 */
static void
EnsureSequentialModeForDatabaseDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify database because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating or altering a database, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Database is created or altered. To make sure subsequent "
							   "commands see the type correctly we need to make sure to "
							   "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}
