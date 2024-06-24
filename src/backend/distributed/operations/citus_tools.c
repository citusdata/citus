/*-------------------------------------------------------------------------
 *
 * citus_tools.c
 *	  UDF to run multi shard/worker queries
 *
 * This file contains functions to run commands on other worker/shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_server_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/utils/array_type.h"
#include "distributed/utils/function.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"


PG_FUNCTION_INFO_V1(master_run_on_worker);

static int ParseCommandParameters(FunctionCallInfo fcinfo, StringInfo **nodeNameArray,
								  int **nodePortsArray, StringInfo **commandStringArray,
								  bool *parallel);
static void ExecuteCommandsInParallelAndStoreResults(StringInfo *nodeNameArray,
													 int *nodePortArray,
													 StringInfo *commandStringArray,
													 bool *statusArray,
													 StringInfo *resultStringArray,
													 int commandCount);
static bool GetConnectionStatusAndResult(MultiConnection *connection, bool *resultStatus,
										 StringInfo queryResultString);
static void ExecuteCommandsAndStoreResults(StringInfo *nodeNameArray,
										   int *nodePortArray,
										   StringInfo *commandStringArray,
										   bool *statusArray,
										   StringInfo *resultStringArray,
										   int commandCount);
static bool ExecuteOptionalSingleResultCommand(MultiConnection *connection,
											   char *queryString, StringInfo
											   queryResultString);
static Tuplestorestate * CreateTupleStore(TupleDesc tupleDescriptor,
										  StringInfo *nodeNameArray, int *nodePortArray,
										  bool *statusArray,
										  StringInfo *resultArray, int commandCount);


/*
 * master_run_on_worker executes queries/commands to run on specified worker and
 * returns success status and query/command result. Expected input is 3 arrays
 * containing node names, node ports, and query strings, and boolean flag to specify
 * parallel execution. The function then returns node_name, node_port, success,
 * result tuples upon completion of the query. The same user credentials are used
 * to connect to remote nodes.
 */
Datum
master_run_on_worker(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	bool parallelExecution = false;
	StringInfo *nodeNameArray = NULL;
	int *nodePortArray = NULL;
	StringInfo *commandStringArray = NULL;

	/* check to see if caller supports us returning a tuplestore */
	if (!rsinfo || !(rsinfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));
	}

	int commandCount = ParseCommandParameters(fcinfo, &nodeNameArray, &nodePortArray,
											  &commandStringArray, &parallelExecution);

	MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	MemoryContext oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* get the requested return tuple description */
	TupleDesc tupleDescriptor = CreateTupleDescCopy(rsinfo->expectedDesc);

	/*
	 * Check to make sure we have correct tuple descriptor
	 */
	if (tupleDescriptor->natts != 4 ||
		TupleDescAttr(tupleDescriptor, 0)->atttypid != TEXTOID ||
		TupleDescAttr(tupleDescriptor, 1)->atttypid != INT4OID ||
		TupleDescAttr(tupleDescriptor, 2)->atttypid != BOOLOID ||
		TupleDescAttr(tupleDescriptor, 3)->atttypid != TEXTOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
				 errmsg("query-specified return tuple and "
						"function return type are not compatible")));
	}

	/*
	 * prepare storage for status and result values.
	 * commandCount is based on user input however, it is the length of list
	 * instead of a user given integer, hence this should be safe here in terms
	 * of memory allocation.
	 */
	bool *statusArray = palloc0(commandCount * sizeof(bool));
	StringInfo *resultArray = palloc0(commandCount * sizeof(StringInfo));
	for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		resultArray[commandIndex] = makeStringInfo();
	}

	if (parallelExecution)
	{
		ExecuteCommandsInParallelAndStoreResults(nodeNameArray, nodePortArray,
												 commandStringArray,
												 statusArray, resultArray, commandCount);
	}
	else
	{
		ExecuteCommandsAndStoreResults(nodeNameArray, nodePortArray, commandStringArray,
									   statusArray, resultArray, commandCount);
	}

	/* let the caller know we're sending back a tuplestore */
	rsinfo->returnMode = SFRM_Materialize;
	Tuplestorestate *tupleStore = CreateTupleStore(tupleDescriptor,
												   nodeNameArray, nodePortArray,
												   statusArray,
												   resultArray, commandCount);
	rsinfo->setResult = tupleStore;
	rsinfo->setDesc = tupleDescriptor;

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_VOID();
}


/* ParseCommandParameters reads call parameters and fills in data structures */
static int
ParseCommandParameters(FunctionCallInfo fcinfo, StringInfo **nodeNameArray,
					   int **nodePortsArray, StringInfo **commandStringArray,
					   bool *parallel)
{
	ArrayType *nodeNameArrayObject = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *nodePortArrayObject = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType *commandStringArrayObject = PG_GETARG_ARRAYTYPE_P(2);
	bool parallelExecution = PG_GETARG_BOOL(3);
	int nodeNameCount = ArrayObjectCount(nodeNameArrayObject);
	int nodePortCount = ArrayObjectCount(nodePortArrayObject);
	int commandStringCount = ArrayObjectCount(commandStringArrayObject);
	Datum *nodeNameDatumArray = DeconstructArrayObject(nodeNameArrayObject);
	Datum *nodePortDatumArray = DeconstructArrayObject(nodePortArrayObject);
	Datum *commandStringDatumArray = DeconstructArrayObject(commandStringArrayObject);

	if (nodeNameCount != nodePortCount || nodeNameCount != commandStringCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("expected same number of node name, port, and query string")));
	}

	StringInfo *nodeNames = palloc0(nodeNameCount * sizeof(StringInfo));
	int *nodePorts = palloc0(nodeNameCount * sizeof(int));
	StringInfo *commandStrings = palloc0(nodeNameCount * sizeof(StringInfo));

	for (int index = 0; index < nodeNameCount; index++)
	{
		text *nodeNameText = DatumGetTextP(nodeNameDatumArray[index]);
		char *nodeName = text_to_cstring(nodeNameText);
		int32 nodePort = DatumGetInt32(nodePortDatumArray[index]);
		text *commandText = DatumGetTextP(commandStringDatumArray[index]);
		char *commandString = text_to_cstring(commandText);

		nodeNames[index] = makeStringInfo();
		commandStrings[index] = makeStringInfo();

		appendStringInfo(nodeNames[index], "%s", nodeName);
		nodePorts[index] = nodePort;
		appendStringInfo(commandStrings[index], "%s", commandString);
	}

	*nodeNameArray = nodeNames;
	*nodePortsArray = nodePorts;
	*commandStringArray = commandStrings;
	*parallel = parallelExecution;

	return nodeNameCount;
}


/*
 * ExecuteCommandsInParallelAndStoreResults connects to each node specified in
 * nodeNameArray and nodePortArray, and executes command in commandStringArray
 * in parallel fashion. Execution success status and result is reported for
 * each command in statusArray and resultStringArray. Each array contains
 * commandCount items.
 */
static void
ExecuteCommandsInParallelAndStoreResults(StringInfo *nodeNameArray, int *nodePortArray,
										 StringInfo *commandStringArray,
										 bool *statusArray, StringInfo *resultStringArray,
										 int commandCount)
{
	MultiConnection **connectionArray =
		palloc0(commandCount * sizeof(MultiConnection *));
	int finishedCount = 0;

	/* start connections asynchronously */
	for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		char *nodeName = nodeNameArray[commandIndex]->data;
		int nodePort = nodePortArray[commandIndex];
		int connectionFlags = FORCE_NEW_CONNECTION;
		connectionArray[commandIndex] =
			StartNodeConnection(connectionFlags, nodeName, nodePort);
	}

	/* establish connections */
	for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		MultiConnection *connection = connectionArray[commandIndex];
		StringInfo queryResultString = resultStringArray[commandIndex];
		char *nodeName = nodeNameArray[commandIndex]->data;
		int nodePort = nodePortArray[commandIndex];

		FinishConnectionEstablishment(connection);

		/* check whether connection attempt was successful */
		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			appendStringInfo(queryResultString, "failed to connect to %s:%d", nodeName,
							 nodePort);
			statusArray[commandIndex] = false;
			CloseConnection(connection);
			connectionArray[commandIndex] = NULL;
			finishedCount++;
			continue;
		}

		/* set the application_name to avoid nested execution checks */
		int querySent = SendRemoteCommand(connection, psprintf(
											  "SET application_name TO '%s%ld'",
											  CITUS_RUN_COMMAND_APPLICATION_NAME_PREFIX,
											  GetGlobalPID()));
		if (querySent == 0)
		{
			StoreErrorMessage(connection, queryResultString);
			statusArray[commandIndex] = false;
			CloseConnection(connection);
			connectionArray[commandIndex] = NULL;
			finishedCount++;
			continue;
		}

		statusArray[commandIndex] = true;
	}

	/* send queries at once */
	for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		MultiConnection *connection = connectionArray[commandIndex];
		if (connection == NULL)
		{
			continue;
		}

		bool raiseInterrupts = true;
		PGresult *queryResult = GetRemoteCommandResult(connection, raiseInterrupts);

		/* write the result value or error message to queryResultString */
		StringInfo queryResultString = resultStringArray[commandIndex];
		bool success = EvaluateSingleQueryResult(connection, queryResult,
												 queryResultString);
		if (!success)
		{
			statusArray[commandIndex] = false;
			CloseConnection(connection);
			connectionArray[commandIndex] = NULL;
			finishedCount++;
			continue;
		}

		/* clear results for the next command */
		PQclear(queryResult);

		bool raiseErrors = false;
		ClearResults(connection, raiseErrors);

		/* we only care about the SET application_name result on failure */
		resetStringInfo(queryResultString);
	}

	/* send queries at once */
	for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		MultiConnection *connection = connectionArray[commandIndex];
		char *queryString = commandStringArray[commandIndex]->data;
		StringInfo queryResultString = resultStringArray[commandIndex];

		/*
		 * If we don't have a connection, nothing to send, error string should already
		 * been filled.
		 */
		if (connection == NULL)
		{
			continue;
		}

		int querySent = SendRemoteCommand(connection, queryString);
		if (querySent == 0)
		{
			StoreErrorMessage(connection, queryResultString);
			statusArray[commandIndex] = false;
			CloseConnection(connection);
			connectionArray[commandIndex] = NULL;
			finishedCount++;
		}
	}

	/* check for query results */
	while (finishedCount < commandCount)
	{
		for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
		{
			MultiConnection *connection = connectionArray[commandIndex];
			StringInfo queryResultString = resultStringArray[commandIndex];
			bool success = false;

			if (connection == NULL)
			{
				continue;
			}

			bool queryFinished = GetConnectionStatusAndResult(connection, &success,
															  queryResultString);

			if (queryFinished)
			{
				finishedCount++;
				statusArray[commandIndex] = success;
				connectionArray[commandIndex] = NULL;
				CloseConnection(connection);
			}
		}

		CHECK_FOR_INTERRUPTS();

		if (finishedCount < commandCount)
		{
			long sleepIntervalPerCycle = RemoteTaskCheckInterval * 1000L;
			pg_usleep(sleepIntervalPerCycle);
		}
	}

	pfree(connectionArray);
}


/*
 * GetConnectionStatusAndResult checks the active connection and returns true if
 * query execution is finished (either success or fail).
 * Query success/fail in resultStatus, and query result in queryResultString are
 * reported upon completion of the query.
 */
static bool
GetConnectionStatusAndResult(MultiConnection *connection, bool *resultStatus,
							 StringInfo queryResultString)
{
	bool finished = true;
	ConnStatusType connectionStatus = PQstatus(connection->pgConn);

	*resultStatus = false;
	resetStringInfo(queryResultString);

	if (connectionStatus == CONNECTION_BAD)
	{
		appendStringInfo(queryResultString, "connection lost");
		return finished;
	}

	int consumeInput = PQconsumeInput(connection->pgConn);
	if (consumeInput == 0)
	{
		appendStringInfo(queryResultString, "query result unavailable");
		return finished;
	}

	/* check later if busy */
	if (PQisBusy(connection->pgConn) != 0)
	{
		finished = false;
		return finished;
	}

	/* query result is available at this point */
	PGresult *queryResult = PQgetResult(connection->pgConn);
	bool success = EvaluateSingleQueryResult(connection, queryResult, queryResultString);
	PQclear(queryResult);

	*resultStatus = success;
	finished = true;
	return true;
}


/*
 * ExecuteCommandsAndStoreResults connects to each node specified in
 * nodeNameArray and nodePortArray, and executes command in commandStringArray
 * in sequential order. Execution success status and result is reported for
 * each command in statusArray and resultStringArray. Each array contains
 * commandCount items.
 */
static void
ExecuteCommandsAndStoreResults(StringInfo *nodeNameArray, int *nodePortArray,
							   StringInfo *commandStringArray, bool *statusArray,
							   StringInfo *resultStringArray, int commandCount)
{
	for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		CHECK_FOR_INTERRUPTS();

		char *nodeName = nodeNameArray[commandIndex]->data;
		int32 nodePort = nodePortArray[commandIndex];
		char *queryString = commandStringArray[commandIndex]->data;
		StringInfo queryResultString = resultStringArray[commandIndex];

		int connectionFlags = FORCE_NEW_CONNECTION;
		MultiConnection *connection =
			GetNodeConnection(connectionFlags, nodeName, nodePort);

		/* set the application_name to avoid nested execution checks */
		bool success = ExecuteOptionalSingleResultCommand(
			connection,
			psprintf(
				"SET application_name TO '%s%ld'",
				CITUS_RUN_COMMAND_APPLICATION_NAME_PREFIX,
				GetGlobalPID()),
			queryResultString
			);
		if (!success)
		{
			statusArray[commandIndex] = false;
			CloseConnection(connection);
			continue;
		}

		/* we only care about the SET application_name result on failure */
		resetStringInfo(queryResultString);

		/* send the actual query string */
		success = ExecuteOptionalSingleResultCommand(connection, queryString,
													 queryResultString);

		statusArray[commandIndex] = success;
		CloseConnection(connection);
	}
}


/*
 * ExecuteOptionalSingleResultCommand executes a query at specified remote node using
 * the calling user's credentials. The function returns the query status
 * (success/failure), and query result. The query is expected to return a single
 * target containing zero or one rows.
 */
static bool
ExecuteOptionalSingleResultCommand(MultiConnection *connection, char *queryString,
								   StringInfo queryResultString)
{
	if (PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		appendStringInfo(queryResultString, "failed to connect to %s:%d",
						 connection->hostname, connection->port);
		return false;
	}

	if (!SendRemoteCommand(connection, queryString))
	{
		appendStringInfo(queryResultString, "failed to send query to %s:%d",
						 connection->hostname, connection->port);
		return false;
	}

	bool raiseInterrupts = true;
	PGresult *queryResult = GetRemoteCommandResult(connection, raiseInterrupts);

	/* write the result value or error message to queryResultString */
	bool success = EvaluateSingleQueryResult(connection, queryResult, queryResultString);

	/* clear result and close the connection */
	PQclear(queryResult);

	bool raiseErrors = false;
	ClearResults(connection, raiseErrors);

	return success;
}


/* CreateTupleStore prepares result tuples from individual query results */
static Tuplestorestate *
CreateTupleStore(TupleDesc tupleDescriptor,
				 StringInfo *nodeNameArray, int *nodePortArray, bool *statusArray,
				 StringInfo *resultArray, int commandCount)
{
	Tuplestorestate *tupleStore = tuplestore_begin_heap(true, false, work_mem);
	bool nulls[4] = { false, false, false, false };

	for (int commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		Datum values[4];
		StringInfo nodeNameString = nodeNameArray[commandIndex];
		StringInfo resultString = resultArray[commandIndex];
		text *nodeNameText = cstring_to_text_with_len(nodeNameString->data,
													  nodeNameString->len);
		text *resultText = cstring_to_text_with_len(resultString->data,
													resultString->len);

		values[0] = PointerGetDatum(nodeNameText);
		values[1] = Int32GetDatum(nodePortArray[commandIndex]);
		values[2] = BoolGetDatum(statusArray[commandIndex]);
		values[3] = PointerGetDatum(resultText);

		HeapTuple tuple = heap_form_tuple(tupleDescriptor, values, nulls);
		tuplestore_puttuple(tupleStore, tuple);

		heap_freetuple(tuple);
		pfree(nodeNameText);
		pfree(resultText);
	}
	return tupleStore;
}
