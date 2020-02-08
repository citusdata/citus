/*-------------------------------------------------------------------------
 *
 * worker_sql_task_protocol.c
 *
 * Routines for executing SQL tasks during task-tracker execution.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "pgstat.h"

#include "distributed/commands/multi_copy.h"
#include "distributed/multi_executor.h"
#include "distributed/transmit.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/* necessary to get S_IRUSR, S_IWUSR definitions on illumos */
#include <sys/stat.h>

#define COPY_BUFFER_SIZE (4 * 1024 * 1024)

/* TaskFileDestReceiver can be used to stream results into a file */
typedef struct TaskFileDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* descriptor of the tuples that are sent to the worker */
	TupleDesc tupleDescriptor;

	/* context for per-tuple memory allocation */
	MemoryContext tupleContext;

	/* MemoryContext for DestReceiver session */
	MemoryContext memoryContext;

	/* output file */
	char *filePath;
	FileCompat fileCompat;
	IntermediateResultFormat format;

	/* state on how to copy out data types */
	IntermediateResultEncoder *encoder;

	/* statistics */
	uint64 tuplesSent;
	uint64 bytesSent;
} TaskFileDestReceiver;


static void TaskFileDestReceiverStartup(DestReceiver *dest, int operation,
										TupleDesc inputTupleDescriptor);
static bool TaskFileDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest);
static void WriteToLocalFile(StringInfo copyData, TaskFileDestReceiver *taskFileDest);
static void TaskFileDestReceiverShutdown(DestReceiver *destReceiver);
static void TaskFileDestReceiverDestroy(DestReceiver *destReceiver);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_execute_sql_task);


/*
 * worker_execute_sql_task executes a query and writes the results to
 * a file according to the usual task naming scheme.
 */
Datum
worker_execute_sql_task(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 taskId = PG_GETARG_UINT32(1);
	text *queryText = PG_GETARG_TEXT_P(2);
	char *queryString = text_to_cstring(queryText);
	bool binaryCopyFormat = PG_GETARG_BOOL(3);


	/* job directory is created prior to scheduling the task */
	StringInfo jobDirectoryName = JobDirectoryName(jobId);
	StringInfo taskFilename = UserTaskFilename(jobDirectoryName, taskId);

	Query *query = ParseQueryString(queryString, NULL, 0);
	int64 tuplesSent = WorkerExecuteSqlTask(query, taskFilename->data, binaryCopyFormat);

	PG_RETURN_INT64(tuplesSent);
}


/*
 * WorkerExecuteSqlTask executes an already-parsed query and writes the result
 * to the given task file.
 */
int64
WorkerExecuteSqlTask(Query *query, char *taskFilename, bool binaryCopyFormat)
{
	ParamListInfo paramListInfo = NULL;

	IntermediateResultFormat format =
		binaryCopyFormat ? BINARY_COPY_FORMAT : TEXT_COPY_FORMAT;

	EState *estate = CreateExecutorState();
	MemoryContext tupleContext = GetPerTupleMemoryContext(estate);
	TaskFileDestReceiver *taskFileDest =
		(TaskFileDestReceiver *) CreateFileDestReceiver(taskFilename, tupleContext,
														format);

	ExecuteQueryIntoDestReceiver(query, paramListInfo, (DestReceiver *) taskFileDest);

	int64 tuplesSent = taskFileDest->tuplesSent;

	taskFileDest->pub.rDestroy((DestReceiver *) taskFileDest);
	FreeExecutorState(estate);

	return tuplesSent;
}


/*
 * CreateFileDestReceiver creates a DestReceiver for writing query results
 * to a file.
 */
DestReceiver *
CreateFileDestReceiver(char *filePath, MemoryContext tupleContext,
					   IntermediateResultFormat format)
{
	TaskFileDestReceiver *taskFileDest = (TaskFileDestReceiver *) palloc0(
		sizeof(TaskFileDestReceiver));

	/* set up the DestReceiver function pointers */
	taskFileDest->pub.receiveSlot = TaskFileDestReceiverReceive;
	taskFileDest->pub.rStartup = TaskFileDestReceiverStartup;
	taskFileDest->pub.rShutdown = TaskFileDestReceiverShutdown;
	taskFileDest->pub.rDestroy = TaskFileDestReceiverDestroy;
	taskFileDest->pub.mydest = DestCopyOut;

	/* set up output parameters */
	taskFileDest->tupleContext = tupleContext;
	taskFileDest->memoryContext = CurrentMemoryContext;
	taskFileDest->filePath = pstrdup(filePath);
	taskFileDest->format = format;

	return (DestReceiver *) taskFileDest;
}


/*
 * TaskFileDestReceiverStartup implements the rStartup interface of
 * TaskFileDestReceiver. It opens the destination file and sets up
 * the CopyOutState.
 */
static void
TaskFileDestReceiverStartup(DestReceiver *dest, int operation,
							TupleDesc inputTupleDescriptor)
{
	TaskFileDestReceiver *taskFileDest = (TaskFileDestReceiver *) dest;

	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	/* use the memory context that was in place when the DestReceiver was created */
	MemoryContext oldContext = MemoryContextSwitchTo(taskFileDest->memoryContext);

	taskFileDest->tupleDescriptor = inputTupleDescriptor;

	/* define how tuples will be serialised */
	taskFileDest->encoder = IntermediateResultEncoderCreate(inputTupleDescriptor,
															taskFileDest->format,
															taskFileDest->tupleContext);

	taskFileDest->fileCompat = FileCompatFromFileStart(FileOpenForTransmit(
														   taskFileDest->filePath,
														   fileFlags,
														   fileMode));

	MemoryContextSwitchTo(oldContext);
}


/*
 * TaskFileDestReceiverReceive implements the receiveSlot function of
 * TaskFileDestReceiver. It takes a TupleTableSlot and writes the contents
 * to a local file.
 */
static bool
TaskFileDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	TaskFileDestReceiver *taskFileDest = (TaskFileDestReceiver *) dest;

	IntermediateResultEncoder *encoder = taskFileDest->encoder;

	MemoryContext executorTupleContext = taskFileDest->tupleContext;
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	slot_getallattrs(slot);

	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	IntermediateResultEncoderReceive(encoder, columnValues, columnNulls);

	StringInfo buffer = encoder->outputBuffer;
	if (buffer->len > COPY_BUFFER_SIZE)
	{
		WriteToLocalFile(buffer, taskFileDest);
		resetStringInfo(buffer);
	}

	MemoryContextSwitchTo(oldContext);

	taskFileDest->tuplesSent++;

	MemoryContextReset(executorTupleContext);

	return true;
}


/*
 * WriteToLocalResultsFile writes the bytes in a StringInfo to a local file.
 */
static void
WriteToLocalFile(StringInfo copyData, TaskFileDestReceiver *taskFileDest)
{
	int bytesWritten = FileWriteCompat(&taskFileDest->fileCompat, copyData->data,
									   copyData->len, PG_WAIT_IO);
	if (bytesWritten < 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not append to file: %m")));
	}

	taskFileDest->bytesSent += bytesWritten;
}


/*
 * TaskFileDestReceiverShutdown implements the rShutdown interface of
 * TaskFileDestReceiver. It writes the footer and closes the file.
 * the relation.
 */
static void
TaskFileDestReceiverShutdown(DestReceiver *destReceiver)
{
	TaskFileDestReceiver *taskFileDest = (TaskFileDestReceiver *) destReceiver;

	IntermediateResultEncoder *encoder = taskFileDest->encoder;
	IntermediateResultEncoderDone(encoder);

	StringInfo buffer = encoder->outputBuffer;
	if (buffer->len > 0)
	{
		WriteToLocalFile(buffer, taskFileDest);
		resetStringInfo(buffer);
	}

	FileClose(taskFileDest->fileCompat.fd);
}


/*
 * TaskFileDestReceiverDestroy frees memory allocated as part of the
 * TaskFileDestReceiver and closes file descriptors.
 */
static void
TaskFileDestReceiverDestroy(DestReceiver *destReceiver)
{
	TaskFileDestReceiver *taskFileDest = (TaskFileDestReceiver *) destReceiver;

	IntermediateResultEncoderDestroy(taskFileDest->encoder);

	pfree(taskFileDest->filePath);
	pfree(taskFileDest);
}


/*
 * FileDestReceiverStats returns statistics for the given file dest receiver.
 */
void
FileDestReceiverStats(DestReceiver *dest, uint64 *rowsSent, uint64 *bytesSent)
{
	TaskFileDestReceiver *fileDestReceiver = (TaskFileDestReceiver *) dest;
	*rowsSent = fileDestReceiver->tuplesSent;
	*bytesSent = fileDestReceiver->bytesSent;
}
