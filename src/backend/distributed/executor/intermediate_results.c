/*-------------------------------------------------------------------------
 *
 * intermediate_results.c
 *   Functions for writing and reading intermediate results.
 *
 * Copyright (c) 2017, Citus Data, Inc.
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

#include "catalog/pg_enum.h"
#include "commands/copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/transmit.h"
#include "distributed/transaction_identifier.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static bool CreatedResultsDirectory = false;


/* CopyDestReceiver can be used to stream results into a distributed table */
typedef struct RemoteFileDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	char *resultId;

	/* descriptor of the tuples that are sent to the worker */
	TupleDesc tupleDescriptor;

	/* EState for per-tuple memory allocation */
	EState *executorState;

	/* MemoryContext for DestReceiver session */
	MemoryContext memoryContext;

	/* worker nodes to send data to */
	List *initialNodeList;
	List *connectionList;

	/* whether to write to a local file */
	bool writeLocalFile;
	File fileDesc;

	/* state on how to copy out data types */
	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;

	/* number of tuples sent */
	uint64 tuplesSent;
} RemoteFileDestReceiver;


static void RemoteFileDestReceiverStartup(DestReceiver *dest, int operation,
										  TupleDesc inputTupleDescriptor);
static StringInfo ConstructCopyResultStatement(const char *resultId);
static void WriteToLocalFile(StringInfo copyData, File fileDesc);
static bool RemoteFileDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest);
static void BroadcastCopyData(StringInfo dataBuffer, List *connectionList);
static void SendCopyDataOverConnection(StringInfo dataBuffer,
									   MultiConnection *connection);
static void RemoteFileDestReceiverShutdown(DestReceiver *destReceiver);
static void RemoteFileDestReceiverDestroy(DestReceiver *destReceiver);

static char * CreateIntermediateResultsDirectory(void);
static char * IntermediateResultsDirectory(void);
static char * QueryResultFileName(const char *resultId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(read_intermediate_result);
PG_FUNCTION_INFO_V1(broadcast_intermediate_result);
PG_FUNCTION_INFO_V1(create_intermediate_result);


/*
 * broadcast_intermediate_result executes a query and streams the results
 * into a file on all workers.
 */
Datum
broadcast_intermediate_result(PG_FUNCTION_ARGS)
{
	text *resultIdText = PG_GETARG_TEXT_P(0);
	char *resultIdString = text_to_cstring(resultIdText);
	text *queryText = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(queryText);
	EState *estate = NULL;
	List *nodeList = NIL;
	bool writeLocalFile = false;
	RemoteFileDestReceiver *resultDest = NULL;
	ParamListInfo paramListInfo = NULL;

	CheckCitusVersion(ERROR);

	nodeList = ActivePrimaryNodeList();
	estate = CreateExecutorState();
	resultDest = (RemoteFileDestReceiver *) CreateRemoteFileDestReceiver(resultIdString,
																		 estate, nodeList,
																		 writeLocalFile);

	ExecuteQueryStringIntoDestReceiver(queryString, paramListInfo,
									   (DestReceiver *) resultDest);

	FreeExecutorState(estate);

	PG_RETURN_INT64(resultDest->tuplesSent);
}


/*
 * create_intermediate_result executes a query and writes the results
 * into a local file.
 */
Datum
create_intermediate_result(PG_FUNCTION_ARGS)
{
	text *resultIdText = PG_GETARG_TEXT_P(0);
	char *resultIdString = text_to_cstring(resultIdText);
	text *queryText = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(queryText);
	EState *estate = NULL;
	List *nodeList = NIL;
	bool writeLocalFile = true;
	RemoteFileDestReceiver *resultDest = NULL;
	ParamListInfo paramListInfo = NULL;

	CheckCitusVersion(ERROR);

	estate = CreateExecutorState();
	resultDest = (RemoteFileDestReceiver *) CreateRemoteFileDestReceiver(resultIdString,
																		 estate, nodeList,
																		 writeLocalFile);

	ExecuteQueryStringIntoDestReceiver(queryString, paramListInfo,
									   (DestReceiver *) resultDest);

	FreeExecutorState(estate);

	PG_RETURN_INT64(resultDest->tuplesSent);
}


/*
 * CreateRemoteFileDestReceiver creates a DestReceiver that streams results
 * to a set of worker nodes.
 */
DestReceiver *
CreateRemoteFileDestReceiver(char *resultId, EState *executorState,
							 List *initialNodeList, bool writeLocalFile)
{
	RemoteFileDestReceiver *resultDest = NULL;

	resultDest = (RemoteFileDestReceiver *) palloc0(sizeof(RemoteFileDestReceiver));

	/* set up the DestReceiver function pointers */
	resultDest->pub.receiveSlot = RemoteFileDestReceiverReceive;
	resultDest->pub.rStartup = RemoteFileDestReceiverStartup;
	resultDest->pub.rShutdown = RemoteFileDestReceiverShutdown;
	resultDest->pub.rDestroy = RemoteFileDestReceiverDestroy;
	resultDest->pub.mydest = DestCopyOut;

	/* set up output parameters */
	resultDest->resultId = resultId;
	resultDest->executorState = executorState;
	resultDest->initialNodeList = initialNodeList;
	resultDest->memoryContext = CurrentMemoryContext;
	resultDest->writeLocalFile = writeLocalFile;

	return (DestReceiver *) resultDest;
}


/*
 * RemoteFileDestReceiverStartup implements the rStartup interface of
 * RemoteFileDestReceiver. It opens the relation
 */
static void
RemoteFileDestReceiverStartup(DestReceiver *dest, int operation,
							  TupleDesc inputTupleDescriptor)
{
	RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) dest;

	const char *resultId = resultDest->resultId;

	CopyOutState copyOutState = NULL;
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	List *initialNodeList = resultDest->initialNodeList;
	ListCell *initialNodeCell = NULL;
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;

	resultDest->tupleDescriptor = inputTupleDescriptor;

	/* define how tuples will be serialised */
	copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = CanUseBinaryCopyFormat(inputTupleDescriptor);
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = GetPerTupleMemoryContext(resultDest->executorState);
	resultDest->copyOutState = copyOutState;

	resultDest->columnOutputFunctions = ColumnOutputFunctions(inputTupleDescriptor,
															  copyOutState->binary);

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived from
	 * the distributed transaction ID across all workers and on the coordinator
	 * itself. Even if we only store results locally, we still want to assign
	 * a transaction ID in case we later store results on workers.
	 *
	 * When we start using broadcast_intermediate_result from workers, we
	 * need to make sure that we don't override the transaction ID here.
	 */
	BeginOrContinueCoordinatedTransaction();

	if (resultDest->writeLocalFile)
	{
		const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
		const int fileMode = (S_IRUSR | S_IWUSR);
		const char *fileName = NULL;

		/* make sure the directory exists */
		CreateIntermediateResultsDirectory();

		fileName = QueryResultFileName(resultId);

		elog(DEBUG1, "writing to local file \"%s\"", fileName);

		resultDest->fileDesc = FileOpenForTransmit(fileName, fileFlags, fileMode);
	}

	foreach(initialNodeCell, initialNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(initialNodeCell);
		int connectionFlags = 0;
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;

		connection = StartNodeConnection(connectionFlags, nodeName, nodePort);
		ClaimConnectionExclusively(connection);
		MarkRemoteTransactionCritical(connection);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* must open transaction blocks to use intermediate results */
	RemoteTransactionsBeginIfNecessary(connectionList);

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		StringInfo copyCommand = NULL;
		bool querySent = false;

		copyCommand = ConstructCopyResultStatement(resultId);

		querySent = SendRemoteCommand(connection, copyCommand->data);
		if (!querySent)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		bool raiseInterrupts = true;

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (PQresultStatus(result) != PGRES_COPY_IN)
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);
	}

	if (copyOutState->binary)
	{
		/* send headers when using binary encoding */
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyBinaryHeaders(copyOutState);
		BroadcastCopyData(copyOutState->fe_msgbuf, connectionList);

		if (resultDest->writeLocalFile)
		{
			WriteToLocalFile(copyOutState->fe_msgbuf, resultDest->fileDesc);
		}
	}

	resultDest->connectionList = connectionList;
}


/*
 * ConstructCopyResultStatement constructs the text of a COPY statement
 * for copying into a result file.
 */
static StringInfo
ConstructCopyResultStatement(const char *resultId)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, "COPY \"%s\" FROM STDIN WITH (format result)",
					 resultId);

	return command;
}


/*
 * RemoteFileDestReceiverReceive implements the receiveSlot function of
 * RemoteFileDestReceiver. It takes a TupleTableSlot and sends the contents to
 * all worker nodes.
 */
static bool
RemoteFileDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) dest;

	TupleDesc tupleDescriptor = resultDest->tupleDescriptor;

	List *connectionList = resultDest->connectionList;
	CopyOutState copyOutState = resultDest->copyOutState;
	FmgrInfo *columnOutputFunctions = resultDest->columnOutputFunctions;

	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	StringInfo copyData = copyOutState->fe_msgbuf;

	EState *executorState = resultDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	slot_getallattrs(slot);

	columnValues = slot->tts_values;
	columnNulls = slot->tts_isnull;

	resetStringInfo(copyData);

	/* construct row in COPY format */
	AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
					  copyOutState, columnOutputFunctions, NULL);

	/* send row to nodes */
	BroadcastCopyData(copyData, connectionList);

	/* write to local file (if applicable) */
	if (resultDest->writeLocalFile)
	{
		WriteToLocalFile(copyOutState->fe_msgbuf, resultDest->fileDesc);
	}

	MemoryContextSwitchTo(oldContext);

	resultDest->tuplesSent++;

	ResetPerTupleExprContext(executorState);

	return true;
}


/*
 * WriteToLocalResultsFile writes the bytes in a StringInfo to a local file.
 */
static void
WriteToLocalFile(StringInfo copyData, File fileDesc)
{
#if (PG_VERSION_NUM >= 100000)
	int bytesWritten = FileWrite(fileDesc, copyData->data, copyData->len, PG_WAIT_IO);
#else
	int bytesWritten = FileWrite(fileDesc, copyData->data, copyData->len);
#endif
	if (bytesWritten < 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not append to file: %m")));
	}
}


/*
 * RemoteFileDestReceiverShutdown implements the rShutdown interface of
 * RemoteFileDestReceiver. It ends the COPY on all the open connections and closes
 * the relation.
 */
static void
RemoteFileDestReceiverShutdown(DestReceiver *destReceiver)
{
	RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) destReceiver;

	List *connectionList = resultDest->connectionList;
	CopyOutState copyOutState = resultDest->copyOutState;

	if (copyOutState->binary)
	{
		/* send footers when using binary encoding */
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyBinaryFooters(copyOutState);
		BroadcastCopyData(copyOutState->fe_msgbuf, connectionList);

		if (resultDest->writeLocalFile)
		{
			WriteToLocalFile(copyOutState->fe_msgbuf, resultDest->fileDesc);
		}
	}

	/* close the COPY input */
	EndRemoteCopy(0, connectionList, true);

	if (resultDest->writeLocalFile)
	{
		FileClose(resultDest->fileDesc);
	}
}


/*
 * BroadcastCopyData sends copy data to all connections in a list.
 */
static void
BroadcastCopyData(StringInfo dataBuffer, List *connectionList)
{
	ListCell *connectionCell = NULL;
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		SendCopyDataOverConnection(dataBuffer, connection);
	}
}


/*
 * SendCopyDataOverConnection sends serialized COPY data over the given
 * connection.
 */
static void
SendCopyDataOverConnection(StringInfo dataBuffer, MultiConnection *connection)
{
	if (!PutRemoteCopyData(connection, dataBuffer->data, dataBuffer->len))
	{
		ReportConnectionError(connection, ERROR);
	}
}


/*
 * RemoteFileDestReceiverDestroy frees memory allocated as part of the
 * RemoteFileDestReceiver and closes file descriptors.
 */
static void
RemoteFileDestReceiverDestroy(DestReceiver *destReceiver)
{
	RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) destReceiver;

	if (resultDest->copyOutState)
	{
		pfree(resultDest->copyOutState);
	}

	if (resultDest->columnOutputFunctions)
	{
		pfree(resultDest->columnOutputFunctions);
	}

	pfree(resultDest);
}


/*
 * ReceiveQueryResultViaCopy is called when a COPY "resultid" FROM
 * STDIN WITH (format result) command is received from the client.
 * The command is followed by the raw copy data stream, which is
 * redirected to a file.
 *
 * File names are automatically prefixed with the user OID. Users
 * are only allowed to read query results from their own directory.
 */
void
ReceiveQueryResultViaCopy(const char *resultId)
{
	const char *resultFileName = NULL;

	CreateIntermediateResultsDirectory();

	resultFileName = QueryResultFileName(resultId);

	RedirectCopyDataToRegularFile(resultFileName);
}


/*
 * CreateIntermediateResultsDirectory creates the intermediate result
 * directory for the current transaction if it does not exist and ensures
 * that the directory is removed at the end of the transaction.
 */
static char *
CreateIntermediateResultsDirectory(void)
{
	char *resultDirectory = IntermediateResultsDirectory();
	int makeOK = 0;

	if (!CreatedResultsDirectory)
	{
		makeOK = mkdir(resultDirectory, S_IRWXU);
		if (makeOK != 0 && errno != EEXIST)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not create intermediate results directory "
								   "\"%s\": %m",
								   resultDirectory)));
		}

		CreatedResultsDirectory = true;
	}

	return resultDirectory;
}


/*
 * QueryResultFileName returns the file name in which to store
 * an intermediate result with the given key in the per transaction
 * result directory.
 */
static char *
QueryResultFileName(const char *resultId)
{
	StringInfo resultFileName = makeStringInfo();
	const char *resultDirectory = IntermediateResultsDirectory();
	char *checkChar = (char *) resultId;

	for (; *checkChar; checkChar++)
	{
		if (!((*checkChar >= 'a' && *checkChar <= 'z') ||
			  (*checkChar >= 'A' && *checkChar <= 'Z') ||
			  (*checkChar >= '0' && *checkChar <= '9') ||
			  (*checkChar == '_') || (*checkChar == '-')))
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
							errmsg("result key \"%s\" contains invalid character",
								   resultId),
							errhint("Result keys may only contain letters, numbers, "
									"underscores and hyphens.")));
		}
	}

	appendStringInfo(resultFileName, "%s/%s.data",
					 resultDirectory, resultId);

	return resultFileName->data;
}


/*
 * IntermediateResultsDirectory returns the directory to use for a query result
 * file with a particular key. The filename includes the user OID, such
 * that users can never read each other's files.
 *
 * In a distributed transaction, the directory has the form:
 * base/pgsql_job_cache/<user id>_<coordinator node id>_<transaction number>/
 *
 * In a non-distributed transaction, the directory has the form:
 * base/pgsql_job_cache/<user id>_<process id>/
 *
 * The latter form can be used for testing COPY ... WITH (format result) without
 * assigning a distributed transaction ID.
 *
 * The pgsql_job_cache directory is emptied on restart in case of failure.
 */
static char *
IntermediateResultsDirectory(void)
{
	StringInfo resultFileName = makeStringInfo();
	Oid userId = GetUserId();
	DistributedTransactionId *transactionId = GetCurrentDistributedTransactionId();
	int initiatorNodeIdentifier = transactionId->initiatorNodeIdentifier;
	uint64 transactionNumber = transactionId->transactionNumber;

	if (transactionNumber > 0)
	{
		appendStringInfo(resultFileName, "base/" PG_JOB_CACHE_DIR "/%u_%u_%lu",
						 userId, initiatorNodeIdentifier, transactionNumber);
	}
	else
	{
		appendStringInfo(resultFileName, "base/" PG_JOB_CACHE_DIR "/%u_%u",
						 userId, MyProcPid);
	}

	return resultFileName->data;
}


/*
 * RemoveIntermediateResultsDirectory removes the intermediate result directory
 * for the current distributed transaction, if any was created.
 */
void
RemoveIntermediateResultsDirectory(void)
{
	if (CreatedResultsDirectory)
	{
		StringInfo resultsDirectory = makeStringInfo();
		appendStringInfoString(resultsDirectory, IntermediateResultsDirectory());

		CitusRemoveDirectory(resultsDirectory);

		CreatedResultsDirectory = false;
	}
}


/*
 * read_intermediate_result is a UDF that returns a COPY-formatted intermediate
 * result file as a set of records. The file is parsed according to the columns
 * definition list specified by the user, e.g.:
 *
 * SELECT * FROM read_intermediate_result('foo', 'csv') AS (a int, b int)
 *
 * The file is read from the directory returned by IntermediateResultsDirectory,
 * which includes the user ID.
 *
 * read_intermediate_result is a volatile function because it cannot be
 * evaluated until execution time, but for distributed planning purposes we can
 * treat it in the same way as immutable functions and reference tables, since
 * we know it will return the same result on all nodes.
 */
Datum
read_intermediate_result(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	text *resultIdText = PG_GETARG_TEXT_P(0);
	char *resultIdString = text_to_cstring(resultIdText);
	Datum copyFormatOidDatum = PG_GETARG_DATUM(1);
	Datum copyFormatLabelDatum = DirectFunctionCall1(enum_out, copyFormatOidDatum);
	char *copyFormatLabel = DatumGetCString(copyFormatLabelDatum);

	char *resultFileName = NULL;
	struct stat fileStat;
	int statOK = 0;

	Tuplestorestate *tupstore = NULL;
	TupleDesc tupleDescriptor = NULL;
	MemoryContext oldcontext = NULL;

	CheckCitusVersion(ERROR);

	resultFileName = QueryResultFileName(resultIdString);
	statOK = stat(resultFileName, &fileStat);
	if (statOK != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("result \"%s\" does not exist", resultIdString)));
	}

	/* check to see if query supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}

	if (!(rsinfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "materialize mode required, but it is not allowed in this context")));
	}

	/* get a tuple descriptor for our result type */
	switch (get_call_result_type(fcinfo, NULL, &tupleDescriptor))
	{
		case TYPEFUNC_COMPOSITE:
		{
			/* success */
			break;
		}

		case TYPEFUNC_RECORD:
		{
			/* failed to determine actual type of RECORD */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
			break;
		}

		default:
		{
			/* result type isn't composite */
			elog(ERROR, "return type must be a row type");
			break;
		}
	}

	tupleDescriptor = CreateTupleDescCopy(tupleDescriptor);

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupleDescriptor;
	MemoryContextSwitchTo(oldcontext);

	ReadFileIntoTupleStore(resultFileName, copyFormatLabel, tupleDescriptor, tupstore);

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
