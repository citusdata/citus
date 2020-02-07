/*-------------------------------------------------------------------------
 *
 * intermediate_results.c
 *   Functions for writing and reading intermediate results.
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

#include "catalog/pg_enum.h"
#include "commands/copy.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/transmit.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"
#include "distributed/version_compat.h"
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
	FileCompat fileCompat;

	/* state on how to copy out data types */
	StringInfo buffer;

	/* number of tuples sent */
	uint64 tuplesSent;
} RemoteFileDestReceiver;


static void RemoteFileDestReceiverStartup(DestReceiver *dest, int operation,
										  TupleDesc inputTupleDescriptor);
static StringInfo ConstructCopyResultStatement(const char *resultId);
static void WriteToLocalFile(StringInfo copyData, FileCompat *fileCompat);
static bool RemoteFileDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest);
static void BroadcastCopyData(StringInfo dataBuffer, List *connectionList);
static void SendCopyDataOverConnection(StringInfo dataBuffer,
									   MultiConnection *connection);
static void RemoteFileDestReceiverShutdown(DestReceiver *destReceiver);
static void RemoteFileDestReceiverDestroy(DestReceiver *destReceiver);

static char * IntermediateResultsDirectory(void);
static void ReadIntermediateResultsIntoFuncOutput(FunctionCallInfo fcinfo,
												  char *copyFormat,
												  Datum *resultIdArray,
												  int resultCount);
static uint64 FetchRemoteIntermediateResult(MultiConnection *connection, char *resultId);
static CopyStatus CopyDataFromConnection(MultiConnection *connection,
										 FileCompat *fileCompat,
										 uint64 *bytesReceived);
static void SerializeSingleDatum(StringInfo datumBuffer, Datum datum,
								 bool datumTypeByValue, int datumTypeLength,
								 char datumTypeAlign);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(read_intermediate_result);
PG_FUNCTION_INFO_V1(read_intermediate_result_array);
PG_FUNCTION_INFO_V1(broadcast_intermediate_result);
PG_FUNCTION_INFO_V1(create_intermediate_result);
PG_FUNCTION_INFO_V1(fetch_intermediate_results);


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
	bool writeLocalFile = false;
	ParamListInfo paramListInfo = NULL;

	CheckCitusVersion(ERROR);

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	List *nodeList = ActivePrimaryWorkerNodeList(NoLock);
	EState *estate = CreateExecutorState();
	RemoteFileDestReceiver *resultDest =
		(RemoteFileDestReceiver *) CreateRemoteFileDestReceiver(resultIdString,
																estate,
																nodeList,
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
	List *nodeList = NIL;
	bool writeLocalFile = true;
	ParamListInfo paramListInfo = NULL;

	CheckCitusVersion(ERROR);

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	EState *estate = CreateExecutorState();
	RemoteFileDestReceiver *resultDest =
		(RemoteFileDestReceiver *) CreateRemoteFileDestReceiver(resultIdString,
																estate,
																nodeList,
																writeLocalFile);

	ExecuteQueryStringIntoDestReceiver(queryString, paramListInfo,
									   (DestReceiver *) resultDest);

	FreeExecutorState(estate);

	PG_RETURN_INT64(resultDest->tuplesSent);
}


/*
 * CreateRemoteFileDestReceiver creates a DestReceiver that streams results
 * to a set of worker nodes. If the scope of the intermediate result is a
 * distributed transaction, then it's up to the caller to ensure that a
 * coordinated transaction is started prior to using the DestReceiver.
 */
DestReceiver *
CreateRemoteFileDestReceiver(char *resultId, EState *executorState,
							 List *initialNodeList, bool writeLocalFile)
{
	RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) palloc0(
		sizeof(RemoteFileDestReceiver));

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
 * RemoteFileDestReceiver. It opens connections to the nodes in initialNodeList,
 * and sends the COPY command on all connections.
 */
static void
RemoteFileDestReceiverStartup(DestReceiver *dest, int operation,
							  TupleDesc inputTupleDescriptor)
{
	RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) dest;

	const char *resultId = resultDest->resultId;

	List *initialNodeList = resultDest->initialNodeList;
	ListCell *initialNodeCell = NULL;
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;

	resultDest->tupleDescriptor = inputTupleDescriptor;
	resultDest->buffer = makeStringInfo();

	if (resultDest->writeLocalFile)
	{
		const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
		const int fileMode = (S_IRUSR | S_IWUSR);

		/* make sure the directory exists */
		CreateIntermediateResultsDirectory();

		const char *fileName = QueryResultFileName(resultId);

		resultDest->fileCompat = FileCompatFromFileStart(FileOpenForTransmit(fileName,
																			 fileFlags,
																			 fileMode));
	}

	foreach(initialNodeCell, initialNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(initialNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;

		/*
		 * We prefer to use a connection that is not associcated with
		 * any placements. The reason is that we claim this connection
		 * exclusively and that would prevent the consecutive DML/DDL
		 * use the same connection.
		 */
		int flags = REQUIRE_SIDECHANNEL;

		MultiConnection *connection = StartNodeConnection(flags, nodeName, nodePort);
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

		StringInfo copyCommand = ConstructCopyResultStatement(resultId);

		bool querySent = SendRemoteCommand(connection, copyCommand->data);
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

	StringInfo buffer = resultDest->buffer;

	EState *executorState = resultDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	slot_getallattrs(slot);

	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	/* place holder for tuple size so we fill it later */
	int sizePos = buffer->len;
	int size = 0;
	appendBinaryStringInfo(buffer, (char *) &size, sizeof(size));

	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts;)
	{
		unsigned char bitarray = 0;
		for (int bitIndex = 0; bitIndex < 8 && columnIndex < tupleDescriptor->natts;
			 bitIndex++, columnIndex++)
		{
			if (columnNulls[columnIndex])
			{
				bitarray |= (1 << bitIndex);
			}
		}

		appendBinaryStringInfo(buffer, (char *) &bitarray, 1);
	}

	/* serialize tuple ... */
	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		if (columnNulls[columnIndex])
		{
			continue;
		}

		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, columnIndex);
		SerializeSingleDatum(buffer, columnValues[columnIndex],
							 attributeForm->attbyval, attributeForm->attlen,
							 attributeForm->attalign);
	}

	/* fill in the correct size */
	size = buffer->len - sizePos - sizeof(size);
	memcpy(buffer->data + sizePos, &size, sizeof(size));

	if (buffer->len > 8192)
	{
		/* send row to nodes */
		BroadcastCopyData(buffer, connectionList);

		/* write to local file (if applicable) */
		if (resultDest->writeLocalFile)
		{
			WriteToLocalFile(buffer, &resultDest->fileCompat);
		}

		resetStringInfo(buffer);
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
WriteToLocalFile(StringInfo copyData, FileCompat *fileCompat)
{
	int bytesWritten = FileWriteCompat(fileCompat, copyData->data,
									   copyData->len,
									   PG_WAIT_IO);
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
	StringInfo buffer = resultDest->buffer;

	if (buffer->len > 0)
	{
		/* send row to nodes */
		BroadcastCopyData(buffer, connectionList);

		/* write to local file (if applicable) */
		if (resultDest->writeLocalFile)
		{
			WriteToLocalFile(buffer, &resultDest->fileCompat);
		}

		resetStringInfo(buffer);
	}

	/* close the COPY input */
	EndRemoteCopy(0, connectionList);

	if (resultDest->writeLocalFile)
	{
		FileClose(resultDest->fileCompat.fd);
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

	pfree(resultDest);
}


/*
 * SendQueryResultViaCopy is called when a COPY "resultid" TO STDOUT
 * WITH (format result) command is received from the client. The
 * contents of the file are sent directly to the client.
 */
void
SendQueryResultViaCopy(const char *resultId)
{
	const char *resultFileName = QueryResultFileName(resultId);

	SendRegularFile(resultFileName);
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
	CreateIntermediateResultsDirectory();

	const char *resultFileName = QueryResultFileName(resultId);

	RedirectCopyDataToRegularFile(resultFileName);
}


/*
 * CreateIntermediateResultsDirectory creates the intermediate result
 * directory for the current transaction if it does not exist and ensures
 * that the directory is removed at the end of the transaction.
 */
char *
CreateIntermediateResultsDirectory(void)
{
	char *resultDirectory = IntermediateResultsDirectory();
	int makeOK = 0;

	if (!CreatedResultsDirectory)
	{
		makeOK = mkdir(resultDirectory, S_IRWXU);
		if (makeOK != 0)
		{
			if (errno == EEXIST)
			{
				/* someone else beat us to it, that's ok */
				return resultDirectory;
			}

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
char *
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
		CitusRemoveDirectory(IntermediateResultsDirectory());

		CreatedResultsDirectory = false;
	}
}


/*
 * IntermediateResultSize returns the file size of the intermediate result
 * or -1 if the file does not exist.
 */
int64
IntermediateResultSize(char *resultId)
{
	struct stat fileStat;

	char *resultFileName = QueryResultFileName(resultId);
	int statOK = stat(resultFileName, &fileStat);
	if (statOK < 0)
	{
		return -1;
	}

	return (int64) fileStat.st_size;
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
	Datum resultId = PG_GETARG_DATUM(0);
	Datum copyFormatOidDatum = PG_GETARG_DATUM(1);
	Datum copyFormatLabelDatum = DirectFunctionCall1(enum_out, copyFormatOidDatum);
	char *copyFormatLabel = DatumGetCString(copyFormatLabelDatum);

	CheckCitusVersion(ERROR);

	ReadIntermediateResultsIntoFuncOutput(fcinfo, copyFormatLabel, &resultId, 1);

	PG_RETURN_DATUM(0);
}


/*
 * read_intermediate_result_array returns the set of records in a set of given
 * COPY-formatted intermediate result files.
 *
 * The usage and semantics of this is same as read_intermediate_result(), except
 * that its first argument is an array of result ids.
 */
Datum
read_intermediate_result_array(PG_FUNCTION_ARGS)
{
	ArrayType *resultIdObject = PG_GETARG_ARRAYTYPE_P(0);
	Datum copyFormatOidDatum = PG_GETARG_DATUM(1);

	Datum copyFormatLabelDatum = DirectFunctionCall1(enum_out, copyFormatOidDatum);
	char *copyFormatLabel = DatumGetCString(copyFormatLabelDatum);

	CheckCitusVersion(ERROR);

	int32 resultCount = ArrayGetNItems(ARR_NDIM(resultIdObject), ARR_DIMS(
										   resultIdObject));
	Datum *resultIdArray = DeconstructArrayObject(resultIdObject);

	ReadIntermediateResultsIntoFuncOutput(fcinfo, copyFormatLabel,
										  resultIdArray, resultCount);

	PG_RETURN_DATUM(0);
}


/*
 * ReadIntermediateResultsIntoFuncOutput reads the given result files and stores
 * them at the function's output tuple store. Errors out if any of the result files
 * don't exist.
 */
static void
ReadIntermediateResultsIntoFuncOutput(FunctionCallInfo fcinfo, char *copyFormat,
									  Datum *resultIdArray, int resultCount)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	for (int resultIndex = 0; resultIndex < resultCount; resultIndex++)
	{
		char *resultId = TextDatumGetCString(resultIdArray[resultIndex]);
		char *resultFileName = QueryResultFileName(resultId);
		struct stat fileStat;

		int statOK = stat(resultFileName, &fileStat);
		if (statOK != 0)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("result \"%s\" does not exist", resultId)));
		}

		ReadFileIntoTupleStore(resultFileName, copyFormat, tupleDescriptor, tupleStore);
	}

	tuplestore_donestoring(tupleStore);
}


/*
 * fetch_intermediate_results fetches a set of intermediate results defined in an
 * array of result IDs from a remote node and writes them to a local intermediate
 * result with the same ID.
 */
Datum
fetch_intermediate_results(PG_FUNCTION_ARGS)
{
	ArrayType *resultIdObject = PG_GETARG_ARRAYTYPE_P(0);
	Datum *resultIdArray = DeconstructArrayObject(resultIdObject);
	int32 resultCount = ArrayObjectCount(resultIdObject);
	text *remoteHostText = PG_GETARG_TEXT_P(1);
	char *remoteHost = text_to_cstring(remoteHostText);
	int remotePort = PG_GETARG_INT32(2);

	int connectionFlags = FORCE_NEW_CONNECTION;
	int resultIndex = 0;
	int64 totalBytesWritten = 0L;

	CheckCitusVersion(ERROR);

	if (resultCount == 0)
	{
		PG_RETURN_INT64(0);
	}

	if (!IsMultiStatementTransaction())
	{
		ereport(ERROR, (errmsg("fetch_intermediate_results can only be used in a "
							   "distributed transaction")));
	}

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	EnsureDistributedTransactionId();

	MultiConnection *connection = GetNodeConnection(connectionFlags, remoteHost,
													remotePort);

	if (PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to fetch intermediate results",
							   remoteHost, remotePort)));
	}

	StringInfo beginAndSetXactId = BeginAndSetDistributedTransactionIdCommand();
	ExecuteCriticalRemoteCommand(connection, beginAndSetXactId->data);

	for (resultIndex = 0; resultIndex < resultCount; resultIndex++)
	{
		char *resultId = TextDatumGetCString(resultIdArray[resultIndex]);

		totalBytesWritten += FetchRemoteIntermediateResult(connection, resultId);
	}

	ExecuteCriticalRemoteCommand(connection, "END");

	CloseConnection(connection);

	PG_RETURN_INT64(totalBytesWritten);
}


/*
 * FetchRemoteIntermediateResult fetches a remote intermediate result over
 * the given connection.
 */
static uint64
FetchRemoteIntermediateResult(MultiConnection *connection, char *resultId)
{
	uint64 totalBytesWritten = 0;

	StringInfo copyCommand = makeStringInfo();
	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	PGconn *pgConn = connection->pgConn;
	int socket = PQsocket(pgConn);
	bool raiseErrors = true;

	CreateIntermediateResultsDirectory();

	appendStringInfo(copyCommand, "COPY \"%s\" TO STDOUT WITH (format result)",
					 resultId);

	if (!SendRemoteCommand(connection, copyCommand->data))
	{
		ReportConnectionError(connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(connection, raiseErrors);
	if (PQresultStatus(result) != PGRES_COPY_OUT)
	{
		ReportResultError(connection, result, ERROR);
	}

	PQclear(result);

	char *localPath = QueryResultFileName(resultId);
	File fileDesc = FileOpenForTransmit(localPath, fileFlags, fileMode);
	FileCompat fileCompat = FileCompatFromFileStart(fileDesc);

	while (true)
	{
		int waitFlags = WL_SOCKET_READABLE | WL_POSTMASTER_DEATH;

		CopyStatus copyStatus = CopyDataFromConnection(connection, &fileCompat,
													   &totalBytesWritten);
		if (copyStatus == CLIENT_COPY_FAILED)
		{
			ereport(ERROR, (errmsg("failed to read result \"%s\" from node %s:%d",
								   resultId, connection->hostname, connection->port)));
		}
		else if (copyStatus == CLIENT_COPY_DONE)
		{
			break;
		}

		Assert(copyStatus == CLIENT_COPY_MORE);

		int rc = WaitLatchOrSocket(MyLatch, waitFlags, socket, 0, PG_WAIT_EXTENSION);
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
	}

	FileClose(fileDesc);

	ClearResults(connection, raiseErrors);

	return totalBytesWritten;
}


/*
 * CopyDataFromConnection reads a row of copy data from connection and writes it
 * to the given file.
 */
static CopyStatus
CopyDataFromConnection(MultiConnection *connection, FileCompat *fileCompat,
					   uint64 *bytesReceived)
{
	/*
	 * Consume input to handle the case where previous copy operation might have
	 * received zero bytes.
	 */
	int consumed = PQconsumeInput(connection->pgConn);
	if (consumed == 0)
	{
		return CLIENT_COPY_FAILED;
	}

	/* receive copy data message in an asynchronous manner */
	char *receiveBuffer = NULL;
	bool asynchronous = true;
	int receiveLength = PQgetCopyData(connection->pgConn, &receiveBuffer, asynchronous);
	while (receiveLength > 0)
	{
		/* received copy data; append these data to file */
		errno = 0;

		int bytesWritten = FileWriteCompat(fileCompat, receiveBuffer,
										   receiveLength, PG_WAIT_IO);
		if (bytesWritten != receiveLength)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not append to file: %m")));
		}

		*bytesReceived += receiveLength;
		PQfreemem(receiveBuffer);
		receiveLength = PQgetCopyData(connection->pgConn, &receiveBuffer, asynchronous);
	}

	if (receiveLength == 0)
	{
		/* we cannot read more data without blocking */
		return CLIENT_COPY_MORE;
	}
	else if (receiveLength == -1)
	{
		/* received copy done message */
		bool raiseInterrupts = true;
		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		ExecStatusType resultStatus = PQresultStatus(result);
		CopyStatus copyStatus = 0;

		if (resultStatus == PGRES_COMMAND_OK)
		{
			copyStatus = CLIENT_COPY_DONE;
		}
		else
		{
			copyStatus = CLIENT_COPY_FAILED;

			ReportResultError(connection, result, WARNING);
		}

		PQclear(result);
		ForgetResults(connection);

		return copyStatus;
	}
	else
	{
		Assert(receiveLength == -2);
		ReportConnectionError(connection, WARNING);

		return CLIENT_COPY_FAILED;
	}
}


/*
 * SerializeSingleDatum serializes the given datum value and appends it to the
 * provided string info buffer.
 *
 * (taken from cstore_fdw)
 */
static void
SerializeSingleDatum(StringInfo datumBuffer, Datum datum, bool datumTypeByValue,
					 int datumTypeLength, char datumTypeAlign)
{
	uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
	uint32 datumLengthAligned = att_align_nominal(datumLength, datumTypeAlign);

	enlargeStringInfo(datumBuffer, datumBuffer->len + datumLengthAligned + 1);

	char *currentDatumDataPointer = datumBuffer->data + datumBuffer->len;

	if (datumTypeLength > 0)
	{
		if (datumTypeByValue)
		{
			store_att_byval(currentDatumDataPointer, datum, datumTypeLength);
		}
		else
		{
			memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumTypeLength);
		}
	}
	else
	{
		Assert(!datumTypeByValue);
		memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumLength);
	}

	datumBuffer->len += datumLengthAligned;
}
