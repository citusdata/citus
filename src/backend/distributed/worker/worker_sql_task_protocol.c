/*-------------------------------------------------------------------------
 *
 * worker_sql_task_protocol.c
 *
 * Routines for executing SQL tasks during task-tracker execution.
 *
 * Copyright (c) 2012-2018, Citus Data, Inc.
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


/* TaskFileDestReceiver can be used to stream results into a file */
typedef struct TaskFileDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* descriptor of the tuples that are sent to the worker */
	TupleDesc tupleDescriptor;

	/* EState for per-tuple memory allocation */
	EState *executorState;

	/* MemoryContext for DestReceiver session */
	MemoryContext memoryContext;

	/* output file */
	char *filePath;
	FileCompat fileCompat;
	bool binaryCopyFormat;

	/* state on how to copy out data types */
	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;

	/* number of tuples sent */
	uint64 tuplesSent;
} TaskFileDestReceiver;


static DestReceiver * CreateTaskFileDestReceiver(char *filePath, EState *executorState,
												 bool binaryCopyFormat);
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

	int64 tuplesSent = 0;
	Query *query = NULL;

	/* job directory is created prior to scheduling the task */
	StringInfo jobDirectoryName = JobDirectoryName(jobId);
	StringInfo taskFilename = UserTaskFilename(jobDirectoryName, taskId);

	query = ParseQueryString(queryString, NULL, 0);
	tuplesSent = WorkerExecuteSqlTask(query, taskFilename->data, binaryCopyFormat);

	PG_RETURN_INT64(tuplesSent);
}


/*
 * WorkerExecuteSqlTask executes an already-parsed query and writes the result
 * to the given task file.
 */
int64
WorkerExecuteSqlTask(Query *query, char *taskFilename, bool binaryCopyFormat)
{
	EState *estate = NULL;
	TaskFileDestReceiver *taskFileDest = NULL;
	ParamListInfo paramListInfo = NULL;
	int64 tuplesSent = 0L;

	estate = CreateExecutorState();
	taskFileDest =
		(TaskFileDestReceiver *) CreateTaskFileDestReceiver(taskFilename, estate,
															binaryCopyFormat);

	ExecuteQueryIntoDestReceiver(query, paramListInfo, (DestReceiver *) taskFileDest);

	tuplesSent = taskFileDest->tuplesSent;

	taskFileDest->pub.rDestroy((DestReceiver *) taskFileDest);
	FreeExecutorState(estate);

	return tuplesSent;
}


/*
 * CreateTaskFileDestReceiver creates a DestReceiver for writing query results
 * to a task file.
 */
static DestReceiver *
CreateTaskFileDestReceiver(char *filePath, EState *executorState, bool binaryCopyFormat)
{
	TaskFileDestReceiver *taskFileDest = NULL;

	taskFileDest = (TaskFileDestReceiver *) palloc0(sizeof(TaskFileDestReceiver));

	/* set up the DestReceiver function pointers */
	taskFileDest->pub.receiveSlot = TaskFileDestReceiverReceive;
	taskFileDest->pub.rStartup = TaskFileDestReceiverStartup;
	taskFileDest->pub.rShutdown = TaskFileDestReceiverShutdown;
	taskFileDest->pub.rDestroy = TaskFileDestReceiverDestroy;
	taskFileDest->pub.mydest = DestCopyOut;

	/* set up output parameters */
	taskFileDest->executorState = executorState;
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

	CopyOutState copyOutState = NULL;
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	/* use the memory context that was in place when the DestReceiver was created */
	MemoryContext oldContext = MemoryContextSwitchTo(taskFileDest->memoryContext);

	taskFileDest->tupleDescriptor = inputTupleDescriptor;

	/* define how tuples will be serialised */
	copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = taskFileDest->binaryCopyFormat;
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = GetPerTupleMemoryContext(taskFileDest->executorState);
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
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyBinaryHeaders(copyOutState);

		WriteToLocalFile(copyOutState->fe_msgbuf, taskFileDest);
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

	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	StringInfo copyData = copyOutState->fe_msgbuf;

	EState *executorState = taskFileDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	slot_getallattrs(slot);

	columnValues = slot->tts_values;
	columnNulls = slot->tts_isnull;

	resetStringInfo(copyData);

	/* construct row in COPY format */
	AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
					  copyOutState, columnOutputFunctions, NULL);

	WriteToLocalFile(copyOutState->fe_msgbuf, taskFileDest);

	MemoryContextSwitchTo(oldContext);

	taskFileDest->tuplesSent++;

	ResetPerTupleExprContext(executorState);

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

	if (copyOutState->binary)
	{
		/* write footers when using binary encoding */
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyBinaryFooters(copyOutState);
		WriteToLocalFile(copyOutState->fe_msgbuf, taskFileDest);
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
	}

	if (taskFileDest->columnOutputFunctions)
	{
		pfree(taskFileDest->columnOutputFunctions);
	}

	pfree(taskFileDest->filePath);
	pfree(taskFileDest);
}
