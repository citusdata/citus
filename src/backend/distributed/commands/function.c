/*-------------------------------------------------------------------------
 *
 * function.c
 *    Commands for FUNCTION statements.
 *    Currently the following will be supported in Citus:
 * 	  -
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_proc.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/worker_transaction.h"
#include "utils/fmgrprotos.h"
#include "utils/builtins.h"

/* forward declaration for helper functions*/
static const char * GetFunctionDDLCommand(Oid funcOid);

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
	ObjectAddressSet(functionAddress, ProcedureRelationId, funcOid);

	EnsureDependenciesExistsOnAllNodes(&functionAddress);

	ddlCommand = GetFunctionDDLCommand(funcOid);
	SendCommandToWorkersAsUser(ALL_WORKERS, ddlCommand, NULL);

	MarkObjectDistributed(&functionAddress);

	PG_RETURN_VOID();
}


static const char *
GetFunctionDDLCommand(Oid funcOid)
{
	Datum sqlTextDatum = DirectFunctionCall1(pg_get_functiondef,
											 ObjectIdGetDatum(funcOid));
	const char *sql = TextDatumGetCString(sqlTextDatum);
	return sql;
}
