/*-------------------------------------------------------------------------
 *
 * test/src/intermediate_results.c
 *
 * This file contains functions to test functions related to
 * src/backend/distributed/executor/intermediate_results.c.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"

PG_FUNCTION_INFO_V1(store_intermediate_result_on_node);


/*
 * store_intermediate_result_on_node executes a query and streams the results
 * into a file on the given node.
 */
Datum
store_intermediate_result_on_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeNameText = PG_GETARG_TEXT_P(0);
	char *nodeNameString = text_to_cstring(nodeNameText);
	int nodePort = PG_GETARG_INT32(1);
	text *resultIdText = PG_GETARG_TEXT_P(2);
	char *resultIdString = text_to_cstring(resultIdText);
	text *queryText = PG_GETARG_TEXT_P(3);
	char *queryString = text_to_cstring(queryText);
	bool writeLocalFile = false;
	ParamListInfo paramListInfo = NULL;

	WorkerNode *workerNode = FindWorkerNodeOrError(nodeNameString, nodePort);

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	EState *estate = CreateExecutorState();
	DestReceiver *resultDest = CreateRemoteFileDestReceiver(resultIdString, estate,
															list_make1(workerNode),
															writeLocalFile);

	ExecuteQueryStringIntoDestReceiver(queryString, paramListInfo,
									   (DestReceiver *) resultDest);

	FreeExecutorState(estate);

	PG_RETURN_VOID();
}
