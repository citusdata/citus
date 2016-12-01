/*-------------------------------------------------------------------------
 *
 * master_citus_tools.c
 *	  UDF to run multi shard/worker queries
 *
 * This file contains functions to run commands on other worker/shards.
 *
 * Copyright (c) 2016-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"


#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "distributed/connection_cache.h"
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_server_executor.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "distributed/multi_client_executor.h"


PG_FUNCTION_INFO_V1(master_run_on_worker);

static int ParseCommandParameters(FunctionCallInfo fcinfo, StringInfo **nodeNameArray,
								  int **nodePortsArray, StringInfo **commandStringArray,
								  bool *parallel);
static void ExecuteCommandsInParallelAndStoreResults(StringInfo *nodeNameArray,
													 int *nodePortArray,
													 StringInfo *commandStringArray,
													 bool *statusArray,
													 StringInfo *resultStringArray,
													 int commmandCount);
static bool GetConnectionStatusAndResult(PGconn *connection, bool *resultStatus,
										 StringInfo queryResultString);
static bool EvaluateQueryResult(PGconn *connection, PGresult *queryResult, StringInfo
								queryResultString);
static void StoreErrorMessage(PGconn *connection, StringInfo queryResultString);
static void ExecuteCommandsAndStoreResults(StringInfo *nodeNameArray,
										   int *nodePortArray,
										   StringInfo *commandStringArray,
										   bool *statusArray,
										   StringInfo *resultStringArray,
										   int commmandCount);
static bool ExecuteRemoteQueryOrCommand(char *nodeName, uint32 nodePort,
										char *queryString, StringInfo queryResult);
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
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx = NULL;
	MemoryContext oldcontext = NULL;
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = NULL;
	bool parallelExecution = false;
	StringInfo *nodeNameArray = NULL;
	int *nodePortArray = NULL;
	StringInfo *commandStringArray = NULL;
	bool *statusArray = NULL;
	StringInfo *resultArray = NULL;
	int commandIndex = 0;
	int commandCount = 0;

	/* check to see if caller supports us returning a tuplestore */
	if (!rsinfo || !(rsinfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));
	}

	commandCount = ParseCommandParameters(fcinfo, &nodeNameArray, &nodePortArray,
										  &commandStringArray, &parallelExecution);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* get the requested return tuple description */
	tupleDescriptor = CreateTupleDescCopy(rsinfo->expectedDesc);

	/*
	 * Check to make sure we have correct tuple descriptor
	 */
	if (tupleDescriptor->natts != 4 ||
		tupleDescriptor->attrs[0]->atttypid != TEXTOID ||
		tupleDescriptor->attrs[1]->atttypid != INT4OID ||
		tupleDescriptor->attrs[2]->atttypid != BOOLOID ||
		tupleDescriptor->attrs[3]->atttypid != TEXTOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
				 errmsg("query-specified return tuple and "
						"function return type are not compatible")));
	}

	/* prepare storage for status and result values */
	statusArray = palloc0(commandCount * sizeof(bool));
	resultArray = palloc0(commandCount * sizeof(StringInfo));
	for (commandIndex = 0; commandIndex < commandCount; commandIndex++)
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
	tupleStore = CreateTupleStore(tupleDescriptor,
								  nodeNameArray, nodePortArray, statusArray,
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
	int index = 0;
	StringInfo *nodeNames = NULL;
	int *nodePorts = NULL;
	StringInfo *commandStrings = NULL;

	if (nodeNameCount != nodePortCount || nodeNameCount != commandStringCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("expected same number of node name, port, and query string")));
	}

	nodeNames = palloc0(nodeNameCount * sizeof(StringInfo));
	nodePorts = palloc0(nodeNameCount * sizeof(int));
	commandStrings = palloc0(nodeNameCount * sizeof(StringInfo));

	for (index = 0; index < nodeNameCount; index++)
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
 * ExecuteCommandsInParellelAndStoreResults connects to each node specified in
 * nodeNameArray and nodePortArray, and executes command in commandStringArray
 * in parallel fashion. Execution success status and result is reported for
 * each command in statusArray and resultStringArray. Each array contains
 * commandCount items.
 */
static void
ExecuteCommandsInParallelAndStoreResults(StringInfo *nodeNameArray, int *nodePortArray,
										 StringInfo *commandStringArray,
										 bool *statusArray, StringInfo *resultStringArray,
										 int commmandCount)
{
	int commandIndex = 0;
	char *nodeUser = CurrentUserName();
	PGconn **connectionArray = palloc0(commmandCount * sizeof(PGconn *));
	int finishedCount = 0;

	/* establish connections */
	for (commandIndex = 0; commandIndex < commmandCount; commandIndex++)
	{
		char *nodeName = nodeNameArray[commandIndex]->data;
		int nodePort = nodePortArray[commandIndex];
		PGconn *connection = ConnectToNode(nodeName, nodePort, nodeUser);
		StringInfo queryResultString = resultStringArray[commandIndex];

		statusArray[commandIndex] = true;

		connectionArray[commandIndex] = connection;

		if (connection == NULL)
		{
			appendStringInfo(queryResultString, "failed to connect to %s:%d", nodeName,
							 (int) nodePort);
			statusArray[commandIndex] = false;
			finishedCount++;
		}
	}

	/* send queries at once */
	for (commandIndex = 0; commandIndex < commmandCount; commandIndex++)
	{
		int querySent = 0;
		PGconn *connection = connectionArray[commandIndex];
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

		querySent = PQsendQuery(connection, queryString);

		if (querySent == 0)
		{
			StoreErrorMessage(connection, queryResultString);
			statusArray[commandIndex] = false;
			CloseConnectionByPGconn(connection);
			connectionArray[commandIndex] = NULL;
			finishedCount++;
		}
	}

	/* check for query results */
	while (finishedCount < commmandCount)
	{
		for (commandIndex = 0; commandIndex < commmandCount; commandIndex++)
		{
			PGconn *connection = connectionArray[commandIndex];
			StringInfo queryResultString = resultStringArray[commandIndex];
			bool success = false;
			bool queryFinished = false;

			if (connection == NULL)
			{
				continue;
			}

			queryFinished = GetConnectionStatusAndResult(connection, &success,
														 queryResultString);

			if (queryFinished)
			{
				finishedCount++;
				statusArray[commandIndex] = success;
				connectionArray[commandIndex] = NULL;
				CloseConnectionByPGconn(connection);
			}
		}

		CHECK_FOR_INTERRUPTS();

		if (finishedCount < commmandCount)
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
GetConnectionStatusAndResult(PGconn *connection, bool *resultStatus,
							 StringInfo queryResultString)
{
	bool finished = true;
	ConnStatusType connectionStatus = PQstatus(connection);
	int consumeInput = 0;
	PGresult *queryResult = NULL;
	bool success = false;

	*resultStatus = false;
	resetStringInfo(queryResultString);

	if (connectionStatus == CONNECTION_BAD)
	{
		appendStringInfo(queryResultString, "connection lost");
		return finished;
	}

	consumeInput = PQconsumeInput(connection);
	if (consumeInput == 0)
	{
		appendStringInfo(queryResultString, "query result unavailable");
		return finished;
	}

	/* check later if busy */
	if (PQisBusy(connection) != 0)
	{
		finished = false;
		return finished;
	}

	/* query result is available at this point */
	queryResult = PQgetResult(connection);
	success = EvaluateQueryResult(connection, queryResult, queryResultString);
	PQclear(queryResult);

	*resultStatus = success;
	finished = true;
	return true;
}


/*
 * EvaluateQueryResult gets the query result from connection and returns
 * true if the query is executed successfully, false otherwise. A query result
 * or an error message is returned in queryResultString. The function requires
 * that the query returns a single column/single row result. It returns an
 * error otherwise.
 */
static bool
EvaluateQueryResult(PGconn *connection, PGresult *queryResult,
					StringInfo queryResultString)
{
	bool success = false;

	ExecStatusType resultStatus = PQresultStatus(queryResult);
	if (resultStatus == PGRES_COMMAND_OK)
	{
		char *commandStatus = PQcmdStatus(queryResult);
		appendStringInfo(queryResultString, "%s", commandStatus);
		success = true;
	}
	else if (resultStatus == PGRES_TUPLES_OK)
	{
		int ntuples = PQntuples(queryResult);
		int nfields = PQnfields(queryResult);

		/* error if query returns more than 1 rows, or more than 1 fields */
		if (nfields != 1)
		{
			appendStringInfo(queryResultString,
							 "expected a single column in query target");
		}
		else if (ntuples > 1)
		{
			appendStringInfo(queryResultString,
							 "expected a single row in query result");
		}
		else
		{
			int row = 0;
			int column = 0;
			if (!PQgetisnull(queryResult, row, column))
			{
				char *queryResultValue = PQgetvalue(queryResult, row, column);
				appendStringInfo(queryResultString, "%s", queryResultValue);
			}
			success = true;
		}
	}
	else
	{
		StoreErrorMessage(connection, queryResultString);
	}

	return success;
}


/*
 * StoreErrorMessage gets the error message from connection and stores it
 * in queryResultString. It should be called only when error is present
 * otherwise it would return a default error message.
 */
static void
StoreErrorMessage(PGconn *connection, StringInfo queryResultString)
{
	char *errorMessage = PQerrorMessage(connection);
	char *firstNewlineIndex = strchr(errorMessage, '\n');

	/* trim the error message at the line break */
	if (firstNewlineIndex != NULL)
	{
		*firstNewlineIndex = '\0';
	}

	/* put a default error message if no error message is reported */
	if (errorMessage == NULL)
	{
		errorMessage = "An error occurred while running the query";
	}

	appendStringInfo(queryResultString, "%s", errorMessage);
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
							   StringInfo *resultStringArray, int commmandCount)
{
	int commandIndex = 0;
	for (commandIndex = 0; commandIndex < commmandCount; commandIndex++)
	{
		char *nodeName = nodeNameArray[commandIndex]->data;
		int32 nodePort = nodePortArray[commandIndex];
		bool success = false;
		char *queryString = commandStringArray[commandIndex]->data;
		StringInfo queryResultString = resultStringArray[commandIndex];

		success = ExecuteRemoteQueryOrCommand(nodeName, nodePort, queryString,
											  queryResultString);

		statusArray[commandIndex] = success;

		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * ExecuteRemoteQueryOrCommand executes a query at specified remote node using
 * the calling user's credentials. The function returns the query status
 * (success/failure), and query result. The query is expected to return a single
 * target containing zero or one rows.
 */
static bool
ExecuteRemoteQueryOrCommand(char *nodeName, uint32 nodePort, char *queryString,
							StringInfo queryResultString)
{
	char *nodeUser = CurrentUserName();
	PGconn *nodeConnection = ConnectToNode(nodeName, nodePort, nodeUser);
	bool success = false;

	if (nodeConnection == NULL)
	{
		appendStringInfo(queryResultString, "failed to connect to %s:%d", nodeName,
						 (int) nodePort);
		return false;
	}

	PG_TRY();
	{
		PGresult *queryResult = PQexec(nodeConnection, queryString);
		success = EvaluateQueryResult(nodeConnection, queryResult, queryResultString);

		PQclear(queryResult);

		/* close the connection */
		CloseConnectionByPGconn(nodeConnection);
		nodeConnection = NULL;
	}
	PG_CATCH();
	{
		StoreErrorMessage(nodeConnection, queryResultString);

		/* close the connection */
		CloseConnectionByPGconn(nodeConnection);
		nodeConnection = NULL;
	}
	PG_END_TRY();

	return success;
}


/* CreateTupleStore prepares result tuples from individual query results */
static Tuplestorestate *
CreateTupleStore(TupleDesc tupleDescriptor,
				 StringInfo *nodeNameArray, int *nodePortArray, bool *statusArray,
				 StringInfo *resultArray, int commandCount)
{
	Tuplestorestate *tupleStore = tuplestore_begin_heap(true, false, work_mem);
	int commandIndex = 0;
	bool nulls[4] = { false, false, false, false };

	for (commandIndex = 0; commandIndex < commandCount; commandIndex++)
	{
		Datum values[4];
		HeapTuple tuple = NULL;
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

		tuple = heap_form_tuple(tupleDescriptor, values, nulls);
		tuplestore_puttuple(tupleStore, tuple);

		heap_freetuple(tuple);
		pfree(nodeNameText);
		pfree(resultText);
	}

	tuplestore_donestoring(tupleStore);

	return tupleStore;
}
