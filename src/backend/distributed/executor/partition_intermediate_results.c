/*-------------------------------------------------------------------------
 *
 * partition_intermediate_results.c
 *   Functions for writing partitioned intermediate results.
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
#include "port.h"

#include "access/hash.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_enum.h"
#include "commands/copy.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/transmit.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/syscache.h"


struct PartitionedResultDestReceiver;
typedef uint32 (*PartitionIdFunc)(Datum, void *);


/* CopyDestReceiver can be used to stream results into a distributed table */
typedef struct PartitionedResultDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	char *resultIdPrefix;

	/* descriptor of the tuples that are sent to the worker */
	TupleDesc tupleDescriptor;

	/* EState for per-tuple memory allocation */
	EState *executorState;

	/* MemoryContext for DestReceiver session */
	MemoryContext memoryContext;

	/* partitioning logic */
	int partitionColumnIndex;
	PartitionIdFunc determinePartitionId;
	void *partitionContext;

	/* output files */
	int partitionCount;
	FileOutputStream *partitionFiles;

	/* state on how to copy out data types */
	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;

	/* number of tuples sent */
	uint64 tuplesSent;
} PartitionedResultDestReceiver;


static DestReceiver * CreatePartitionedResultDestReceiver(char *resultId,
														  EState *executorState,
														  int partitionColumnIndex,
														  int partitionCount,
														  PartitionIdFunc partitionIdFunc,
														  void *partitionContext);
static HashPartitionContext * CreateHashPartitionContext(Datum *hashRangeArray,
														 int partitionCount,
														 Oid partitionColumnType);
static uint32 HashPartitionId(Datum partitionValue, void *context);
static void PartitionedResultDestReceiverStartup(DestReceiver *dest, int operation,
												 TupleDesc inputTupleDescriptor);
static bool PartitionedResultDestReceiverReceive(TupleTableSlot *slot,
												 DestReceiver *dest);
static void FileOutputStreamOpen(FileOutputStream *file);
static void FileOutputStreamWrite(FileOutputStream *file, StringInfo dataToWrite);
static void FileOutputStreamFlush(FileOutputStream *file);
static void PartitionedResultDestReceiverShutdown(DestReceiver *destReceiver);
static void PartitionedResultDestReceiverDestroy(DestReceiver *destReceiver);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_predistribute_query_result);


/*
 * worker_predistribute_query_result executes a query and writes the results
 * into a set of local files according to the partition scheme and the partition
 * column.
 */
Datum
worker_predistribute_query_result(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	text *resultIdPrefixText = PG_GETARG_TEXT_P(0);
	char *resultIdPrefixString = text_to_cstring(resultIdPrefixText);
	text *queryText = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(queryText);

	int partitionColumnIndex = PG_GETARG_INT32(2);
	ArrayType *hashRangeObject = PG_GETARG_ARRAYTYPE_P(3);

	Query *query = NULL;
	PlannedStmt *queryPlan = NULL;
	Portal portal = NULL;
	TupleDesc tupleDescriptor;
	Oid partitionColumnType = InvalidOid;

	HashPartitionContext *partitionContext = NULL;
	Datum *hashRangeArray = DeconstructArrayObject(hashRangeObject);
	int32 partitionCount = ArrayObjectCount(hashRangeObject);

	EState *estate = NULL;
	DestReceiver *dest = NULL;
	PartitionedResultDestReceiver *resultDest = NULL;
	ParamListInfo paramListInfo = NULL;

	TupleDesc returnTupleDesc = NULL;
	Tuplestorestate *tupleStore = NULL;
	MemoryContext perQueryContext = resultInfo->econtext->ecxt_per_query_memory;
	MemoryContext oldContext = NULL;
	int partitionIndex = 0;

	int cursorOptions = 0;
	int eflags = 0;
	long count = FETCH_ALL;

	CheckCitusVersion(ERROR);

	tupleStore = SetupTuplestore(fcinfo, &returnTupleDesc);

	if (partitionCount == 0)
	{
		ereport(ERROR, (errmsg("number of partitions cannot be 0")));
	}

	if (partitionColumnIndex < 0)
	{
		ereport(ERROR, (errmsg("partition column index cannot be negative")));
	}

	/* parse the query */
	query = ParseQueryString(queryString, NULL, 0);

	/* plan the query */
	cursorOptions = CURSOR_OPT_PARALLEL_OK;
	queryPlan = pg_plan_query(query, cursorOptions, paramListInfo);

	/* create a new portal for executing the query */
	portal = CreateNewPortal();

	/* don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;

	/* start execution early in order to extract the tuple descriptor  */
	PortalDefineQuery(portal, NULL, queryString, "SELECT", list_make1(queryPlan), NULL);
	PortalStart(portal, paramListInfo, eflags, GetActiveSnapshot());

	/* extract the partition column */
	tupleDescriptor = portal->tupDesc;

	if (tupleDescriptor == NULL)
	{
		elog(ERROR, "no tuple descriptor");
	}

	if (partitionColumnIndex >= tupleDescriptor->natts)
	{
		ereport(ERROR, (errmsg("partition column index cannot be greater than %d",
							   tupleDescriptor->natts - 1)));
	}

	/* determine the partition column type */
	partitionColumnType = TupleDescAttr(tupleDescriptor, partitionColumnIndex)->atttypid;

	/* build the partition context */
	partitionContext = CreateHashPartitionContext(hashRangeArray, partitionCount,
												  partitionColumnType);

	/* prepare the output destination */
	estate = CreateExecutorState();
	dest = CreatePartitionedResultDestReceiver(resultIdPrefixString, estate,
											   partitionColumnIndex,
											   partitionCount, HashPartitionId,
											   partitionContext);

	resultDest = (PartitionedResultDestReceiver *) dest;

	/* execute the query */
	PortalRun(portal, count, false, true, dest, dest, NULL);

	oldContext = MemoryContextSwitchTo(perQueryContext);

	tupleStore = tuplestore_begin_heap(true, false, work_mem);
	resultInfo->returnMode = SFRM_Materialize;
	resultInfo->setResult = tupleStore;
	resultInfo->setDesc = returnTupleDesc;

	for (partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		FileOutputStream *partitionFile = &resultDest->partitionFiles[partitionIndex];
		int64 bytesWritten = partitionFile->bytesWritten;
		int64 recordsWritten = partitionFile->recordsWritten;
		Datum values[3];
		bool nulls[3];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(partitionIndex);
		values[1] = UInt64GetDatum(recordsWritten);
		values[2] = UInt64GetDatum(bytesWritten);

		tuplestore_putvalues(tupleStore, returnTupleDesc, values, nulls);
	}

	tuplestore_donestoring(tupleStore);

	MemoryContextSwitchTo(oldContext);

	PortalDrop(portal, false);
	FreeExecutorState(estate);

	PG_RETURN_INT64(resultDest->tuplesSent);
}


/*
 * CreateHashPartitionContext creates a new partition context for dividing
 * values of type partitionColumnType into hash buckets according to the
 * ranges whose minimum hash value is in hashRangeArray.
 */
static HashPartitionContext *
CreateHashPartitionContext(Datum *hashRangeArray, int partitionCount,
						   Oid partitionColumnType)
{
	HashPartitionContext *partitionContext = NULL;
	FmgrInfo *hashFunction = NULL;

	partitionContext = palloc0(sizeof(HashPartitionContext));
	partitionContext->syntheticShardIntervalArray =
		SyntheticShardIntervalArrayForShardMinValues(hashRangeArray, partitionCount);
	partitionContext->hasUniformHashDistribution =
		HasUniformHashDistribution(partitionContext->syntheticShardIntervalArray,
								   partitionCount);

	/* use column's type information to get the hashing function */
	hashFunction = GetFunctionInfo(partitionColumnType, HASH_AM_OID, HASHSTANDARD_PROC);

	partitionContext->hashFunction = hashFunction;
	partitionContext->partitionCount = partitionCount;

	/* we'll use binary search, we need the comparison function */
	if (!partitionContext->hasUniformHashDistribution)
	{
		partitionContext->comparisonFunction =
			GetFunctionInfo(partitionColumnType, BTREE_AM_OID, BTORDER_PROC);
	}

	return partitionContext;
}


/*
 * HashPartitionId determines the partition number for the given data value
 * using hash partitioning. More specifically, the function returns zero if the
 * given data value is null. If not, the function follows the exact same approach
 * as Citus distributed planner uses.
 */
static uint32
HashPartitionId(Datum partitionValue, void *context)
{
	HashPartitionContext *hashPartitionContext = (HashPartitionContext *) context;
	FmgrInfo *hashFunction = hashPartitionContext->hashFunction;
	uint32 partitionCount = hashPartitionContext->partitionCount;
	ShardInterval **syntheticShardIntervalArray =
		hashPartitionContext->syntheticShardIntervalArray;
	FmgrInfo *comparisonFunction = hashPartitionContext->comparisonFunction;
	Datum hashDatum = FunctionCall1(hashFunction, partitionValue);
	int32 hashResult = 0;
	uint32 hashPartitionId = 0;

	if (hashDatum == 0)
	{
		return hashPartitionId;
	}

	if (hashPartitionContext->hasUniformHashDistribution)
	{
		uint64 hashTokenIncrement = HASH_TOKEN_COUNT / partitionCount;

		hashResult = DatumGetInt32(hashDatum);
		hashPartitionId = (uint32) (hashResult - INT32_MIN) / hashTokenIncrement;
	}
	else
	{
		hashPartitionId =
			SearchCachedShardInterval(hashDatum, syntheticShardIntervalArray,
									  partitionCount, comparisonFunction);
	}

	return hashPartitionId;
}


/*
 * CreatePartitionedResultDestReceiver creates a DestReceiver that streams results
 * to a set of worker nodes. If the scope of the intermediate result is a
 * distributed transaction, then it's up to the caller to ensure that a
 * coordinated transaction is started prior to using the DestReceiver.
 */
static DestReceiver *
CreatePartitionedResultDestReceiver(char *resultIdPrefix, EState *executorState,
									int partitionColumnIndex,
									int partitionCount, PartitionIdFunc partitionIdFunc,
									void *partitionContext)
{
	PartitionedResultDestReceiver *resultDest = NULL;

	resultDest = (PartitionedResultDestReceiver *) palloc0(
		sizeof(PartitionedResultDestReceiver));

	/* set up the DestReceiver function pointers */
	resultDest->pub.receiveSlot = PartitionedResultDestReceiverReceive;
	resultDest->pub.rStartup = PartitionedResultDestReceiverStartup;
	resultDest->pub.rShutdown = PartitionedResultDestReceiverShutdown;
	resultDest->pub.rDestroy = PartitionedResultDestReceiverDestroy;
	resultDest->pub.mydest = DestCopyOut;

	/* set up output parameters */
	resultDest->resultIdPrefix = resultIdPrefix;
	resultDest->executorState = executorState;
	resultDest->memoryContext = CurrentMemoryContext;
	resultDest->partitionColumnIndex = partitionColumnIndex;
	resultDest->partitionCount = partitionCount;
	resultDest->determinePartitionId = partitionIdFunc;
	resultDest->partitionContext = partitionContext;
	resultDest->partitionFiles =
		(FileOutputStream *) palloc0(partitionCount * sizeof(FileOutputStream));

	return (DestReceiver *) resultDest;
}


/*
 * PartitionedResultDestReceiverStartup implements the rStartup interface of
 * PartitionedResultDestReceiver. It opens the relation
 */
static void
PartitionedResultDestReceiverStartup(DestReceiver *dest, int operation,
									 TupleDesc inputTupleDescriptor)
{
	PartitionedResultDestReceiver *resultDest = (PartitionedResultDestReceiver *) dest;

	const char *resultIdPrefix = resultDest->resultIdPrefix;

	CopyOutState copyOutState = NULL;
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	int partitionCount = resultDest->partitionCount;
	int partitionIndex = 0;
	FileOutputStream *partitionFileArray = resultDest->partitionFiles;
	uint32 fileBufferSize = 0;

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

	/* make sure the directory exists */
	CreateIntermediateResultsDirectory();

	fileBufferSize = (uint32) Max(16384 * 1024 / partitionCount, 1024);

	for (partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		char *filePath = NULL;
		StringInfo resultId = makeStringInfo();
		StringInfo filePathStringInfo = makeStringInfo();

		appendStringInfo(resultId, "%s.%d", resultIdPrefix, partitionIndex);

		filePath = QueryResultFileName(resultId->data);
		appendStringInfoString(filePathStringInfo, filePath);

		/* initialize the entry but do not create the file yet */
		partitionFileArray[partitionIndex].fileDescriptor = 0;
		partitionFileArray[partitionIndex].fileBuffer = makeStringInfo();
		partitionFileArray[partitionIndex].filePath = filePathStringInfo;
		partitionFileArray[partitionIndex].bufferSize = fileBufferSize;
		partitionFileArray[partitionIndex].bytesWritten = 0L;
		partitionFileArray[partitionIndex].recordsWritten = 0L;
	}
}


/*
 * PartitionedResultDestReceiverReceive implements the receiveSlot function of
 * PartitionedResultDestReceiver. It takes a TupleTableSlot and sends the contents to
 * all worker nodes.
 */
static bool
PartitionedResultDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	PartitionedResultDestReceiver *resultDest = (PartitionedResultDestReceiver *) dest;

	TupleDesc tupleDescriptor = resultDest->tupleDescriptor;

	CopyOutState copyOutState = resultDest->copyOutState;
	FmgrInfo *columnOutputFunctions = resultDest->columnOutputFunctions;
	int partitionColumnIndex = resultDest->partitionColumnIndex;

	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	StringInfo copyData = copyOutState->fe_msgbuf;

	Datum partitionColumnValue = 0;
	void *partitionContext = resultDest->partitionContext;
	int partitionId = 0;
	FileOutputStream *partitionFile = NULL;

	EState *executorState = resultDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	/* deform the tuple */
	slot_getallattrs(slot);

	columnValues = slot->tts_values;
	columnNulls = slot->tts_isnull;

	/* find the partition column value */
	partitionColumnValue = columnValues[partitionColumnIndex];

	/* determine the right partition */
	partitionId = resultDest->determinePartitionId(partitionColumnValue,
												   partitionContext);

	/* find the right partition file */
	partitionFile = &resultDest->partitionFiles[partitionId];
	if (partitionFile->fileDescriptor == 0)
	{
		/* create the file */
		FileOutputStreamOpen(partitionFile);

		if (copyOutState->binary)
		{
			/* send headers when using binary encoding */
			resetStringInfo(copyOutState->fe_msgbuf);
			AppendCopyBinaryHeaders(copyOutState);

			FileOutputStreamWrite(partitionFile, copyOutState->fe_msgbuf);
		}
	}

	/* construct row in COPY format */
	resetStringInfo(copyData);
	AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
					  copyOutState, columnOutputFunctions, NULL);

	/* append row to the file */
	FileOutputStreamWrite(partitionFile, copyOutState->fe_msgbuf);

	partitionFile->recordsWritten++;
	resultDest->tuplesSent++;

	MemoryContextSwitchTo(oldContext);
	ResetPerTupleExprContext(executorState);

	return true;
}


/*
 * FileOutputStreamOpen opens the file and adds headers if necessary.
 */
static void
FileOutputStreamOpen(FileOutputStream *file)
{
	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);
	char *filePath = file->filePath->data;
	File fileDescriptor = 0;

	fileDescriptor = PathNameOpenFilePerm(filePath, fileFlags, fileMode);
	if (fileDescriptor < 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open file \"%s\": %m", filePath)));
	}

	file->fileDescriptor = fileDescriptor;
}


/*
 * FileOutputStreamWrite appends given data to file stream's internal buffers.
 * The function then checks if buffered data exceeds preconfigured buffer size;
 * if so, the function flushes the buffer to the underlying file.
 */
static void
FileOutputStreamWrite(FileOutputStream *file, StringInfo dataToWrite)
{
	StringInfo fileBuffer = file->fileBuffer;
	uint32 newBufferSize = fileBuffer->len + dataToWrite->len;

	appendBinaryStringInfo(fileBuffer, dataToWrite->data, dataToWrite->len);

	file->bytesWritten += dataToWrite->len;

	if (newBufferSize > file->bufferSize)
	{
		FileOutputStreamFlush(file);

		resetStringInfo(fileBuffer);
	}
}


/* Flushes data buffered in the file stream object to the underlying file. */
static void
FileOutputStreamFlush(FileOutputStream *file)
{
	StringInfo fileBuffer = file->fileBuffer;
	int written = 0;

	errno = 0;
	written = FileWrite(file->fileDescriptor, fileBuffer->data, fileBuffer->len,
						PG_WAIT_IO);
	if (written != fileBuffer->len)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write %d bytes to partition file \"%s\"",
							   fileBuffer->len, file->filePath->data)));
	}
}


/*
 * PartitionedResultDestReceiverShutdown implements the rShutdown interface of
 * PartitionedResultDestReceiver. It ends the COPY on all the open connections and closes
 * the relation.
 */
static void
PartitionedResultDestReceiverShutdown(DestReceiver *destReceiver)
{
	PartitionedResultDestReceiver *resultDest =
		(PartitionedResultDestReceiver *) destReceiver;
	int partitionCount = resultDest->partitionCount;
	int partitionIndex = 0;

	CopyOutState copyOutState = resultDest->copyOutState;

	for (partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		FileOutputStream *partitionFile = &resultDest->partitionFiles[partitionIndex];

		if (partitionFile->fileDescriptor != 0)
		{
			if (copyOutState->binary)
			{
				/* send footers when using binary encoding */
				resetStringInfo(copyOutState->fe_msgbuf);
				AppendCopyBinaryFooters(copyOutState);

				FileOutputStreamWrite(partitionFile, copyOutState->fe_msgbuf);
			}

			FileOutputStreamFlush(partitionFile);
			FileClose(partitionFile->fileDescriptor);
		}

		FreeStringInfo(partitionFile->fileBuffer);
		FreeStringInfo(partitionFile->filePath);
	}
}


/*
 * PartitionedResultDestReceiverDestroy frees memory allocated as part of the
 * PartitionedResultDestReceiver and closes file descriptors.
 */
static void
PartitionedResultDestReceiverDestroy(DestReceiver *destReceiver)
{
	PartitionedResultDestReceiver *resultDest =
		(PartitionedResultDestReceiver *) destReceiver;

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
