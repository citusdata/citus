/*-------------------------------------------------------------------------
 *
 * function.c
 *    Commands for FUNCTION statements.
 *
 *    We currently support replicating function definitions on the
 *    coordinator in all the worker nodes in the form of
 *
 *    CREATE OR REPLACE FUNCTION ... queries.
 *
 *    ALTER or DROP operations are not yet propagated.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_proc.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_transaction.h"
#include "server/access/xact.h"
#include "server/catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"

/* forward declaration for helper functions*/
static const char * GetFunctionDDLCommand(Oid funcOid);
static void EnsureSequentialModeForFunctionDDL(void);

PG_FUNCTION_INFO_V1(create_distributed_function);

/*
 * create_distributed_function gets a function or procedure name with their list of
 * argument types in parantheses, then it creates a new distributed function.
 */
Datum
create_distributed_function(PG_FUNCTION_ARGS)
{
	RegProcedure funcOid = PG_GETARG_OID(0);
	const char *ddlCommand = NULL;
	ObjectAddress functionAddress = { 0 };

	/* if called on NULL input, error out */
	if (funcOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("create_distributed_function() requires a single "
							   "parameter that is a valid function or procedure name "
							   "followed by a list of parameters in parantheses"),
						errhint("skip the parameters with OUT argtype as they are not "
								"part of the signature in PostgreSQL")));
	}

	ObjectAddressSet(functionAddress, ProcedureRelationId, funcOid);

	/*
	 * when we allow propagation within a transaction block we should make sure to only
	 * allow this in sequential mode
	 */
	EnsureSequentialModeForFunctionDDL();

	EnsureDependenciesExistsOnAllNodes(&functionAddress);

	ddlCommand = GetFunctionDDLCommand(funcOid);
	SendCommandToWorkers(ALL_WORKERS, ddlCommand);

	MarkObjectDistributed(&functionAddress);

	PG_RETURN_VOID();
}


/*
 * GetFunctionDDLCommand returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
 * the specified function.
 */
static const char *
GetFunctionDDLCommand(RegProcedure funcOid)
{
	OverrideSearchPath *overridePath = NULL;
	Datum sqlTextDatum = 0;

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	sqlTextDatum = DirectFunctionCall1(pg_get_functiondef,
									   ObjectIdGetDatum(funcOid));

	/* revert back to original search_path */
	PopOverrideSearchPath();

	const char *sql = TextDatumGetCString(sqlTextDatum);
	return sql;
}


/*
 * EnsureSequentialModeForFunctionDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginnig.
 *
 * As functions are node scoped objects there exists only 1 instance of the function used by
 * potentially multiple shards. To make sure all shards in the transaction can interact
 * with the function the function needs to be visible on all connections used by the transaction,
 * meaning we can only use 1 connection per node.
 */
static void
EnsureSequentialModeForFunctionDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create function because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating a distributed function, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail(
						 "A distributed function is created. To make sure subsequent "
						 "commands see the type correctly we need to make sure to "
						 "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}
