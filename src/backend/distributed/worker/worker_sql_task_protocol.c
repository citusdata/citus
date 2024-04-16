/*-------------------------------------------------------------------------
 *
 * worker_sql_task_protocol.c
 *
 * Routines for executing SQL tasks.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "pgstat.h"

#include "utils/builtins.h"
#include "utils/memutils.h"

#include "distributed/commands/multi_copy.h"
#include "distributed/multi_executor.h"
#include "distributed/transmit.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"

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
	bool binaryCopyFormat;

	/* state on how to copy out data types */
	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;

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
	ereport(ERROR, (errmsg("This UDF is deprecated.")));

	PG_RETURN_INT64(0);
}


/*
 * CreateFileDestReceiver creates a DestReceiver for writing query results
 * to a file.
 */
DestReceiver *
CreateFileDestReceiver(char *filePath, MemoryContext tupleContext, bool binaryCopyFormat)
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
	taskFileDest->binaryCopyFormat = binaryCopyFormat;

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

	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	/* use the memory context that was in place when the DestReceiver was created */
	MemoryContext oldContext = MemoryContextSwitchTo(taskFileDest->memoryContext);

	taskFileDest->tupleDescriptor = inputTupleDescriptor;

	/* define how tuples will be serialised */
	CopyOutState copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = taskFileDest->binaryCopyFormat;
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = taskFileDest->tupleContext;
	taskFileDest->copyOutState = copyOutState;

	taskFileDest->columnOutputFunctions = ColumnOutputFunctions(inputTupleDescriptor,
																copyOutState->binary);

	taskFileDest->fileCompat = FileCompatFromFileStart(FileOpenForTransmit(
														   taskFileDest->filePath,
														   fileFlags,
														   fileMode));

	if (copyOutState->binary)
	{
		/* write headers when using binary encoding */
		AppendCopyBinaryHeaders(copyOutState);
	}

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

	TupleDesc tupleDescriptor = taskFileDest->tupleDescriptor;

	CopyOutState copyOutState = taskFileDest->copyOutState;
	FmgrInfo *columnOutputFunctions = taskFileDest->columnOutputFunctions;

	StringInfo copyData = copyOutState->fe_msgbuf;

	MemoryContext executorTupleContext = taskFileDest->tupleContext;
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	slot_getallattrs(slot);

	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	/* construct row in COPY format */
	AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
					  copyOutState, columnOutputFunctions, NULL);

	if (copyData->len > COPY_BUFFER_SIZE)
	{
		WriteToLocalFile(copyOutState->fe_msgbuf, taskFileDest);
		resetStringInfo(copyData);
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
	CopyOutState copyOutState = taskFileDest->copyOutState;

	if (copyOutState->fe_msgbuf->len > 0)
	{
		WriteToLocalFile(copyOutState->fe_msgbuf, taskFileDest);
		resetStringInfo(copyOutState->fe_msgbuf);
	}

	if (copyOutState->binary)
	{
		/* write footers when using binary encoding */
		AppendCopyBinaryFooters(copyOutState);
		WriteToLocalFile(copyOutState->fe_msgbuf, taskFileDest);
		resetStringInfo(copyOutState->fe_msgbuf);
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

	if (taskFileDest->copyOutState)
	{
		pfree(taskFileDest->copyOutState);
		taskFileDest->copyOutState = NULL;
	}

	if (taskFileDest->columnOutputFunctions)
	{
		pfree(taskFileDest->columnOutputFunctions);
		taskFileDest->columnOutputFunctions = NULL;
	}

	if (taskFileDest->filePath)
	{
		pfree(taskFileDest->filePath);
		taskFileDest->filePath = NULL;
	}
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
