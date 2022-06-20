/*-------------------------------------------------------------------------
 *
 * worker_shard_copy.c
 *   Functions for copying a shard to desintaion with push copy.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "libpq-fe.h"
#include "postgres.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_shard_copy.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/local_multi_copy.h"
#include "distributed/worker_manager.h"
#include "distributed/connection_management.h"
#include "distributed/relation_utils.h"
#include "distributed/version_compat.h"
#include "distributed/local_executor.h"

/*
 * LocalCopyBuffer is used in copy callback to return the copied rows.
 * The reason this is a global variable is that we cannot pass an additional
 * argument to the copy callback.
 */
static StringInfo LocalCopyBuffer;

typedef struct ShardCopyDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* Destination Relation Name */
	char* destinationShardFullyQualifiedName;

	/* descriptor of the tuples that are sent to the worker */
	TupleDesc tupleDescriptor;

	/* state on how to copy out data types */
	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;

	/* number of tuples sent */
	int64 tuplesSent;

	/* destination node id */
	uint32_t destinationNodeId;

	/* local copy if destination shard in same node */
	bool useLocalCopy;

	/* EState for per-tuple memory allocation */
	EState *executorState;

	/*
	 * Connection for destination shard (NULL if useLocalCopy is true)
	*/
	MultiConnection *connection;

} ShardCopyDestReceiver;

static bool ShardCopyDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest);
static void ShardCopyDestReceiverStartup(DestReceiver *dest, int operation,
										TupleDesc inputTupleDescriptor);
static void ShardCopyDestReceiverShutdown(DestReceiver *destReceiver);
static void ShardCopyDestReceiverDestroy(DestReceiver *destReceiver);
static bool CanUseLocalCopy(uint64 destinationNodeId);
static StringInfo ConstructCopyStatement(char* destinationShardFullyQualifiedName, bool useBinaryFormat);
static void WriteLocalTuple(TupleTableSlot *slot, ShardCopyDestReceiver *copyDest);
static bool ShouldSendCopyNow(StringInfo buffer);
static int  ReadFromLocalBufferCallback(void *outBuf, int minRead, int maxRead);
static void LocalCopyToShard(ShardCopyDestReceiver *copyDest, CopyOutState localCopyOutState);

static bool CanUseLocalCopy(uint64 destinationNodeId)
{
	/* If destination node is same as source, use local copy */
	return GetLocalNodeId() == destinationNodeId;
}

/*
 * ShouldSendCopyNow returns true if the given buffer size exceeds the
 * local copy buffer size threshold.
 */
static bool
ShouldSendCopyNow(StringInfo buffer)
{
	/* LocalCopyFlushThreshold is in bytes */
	return buffer->len > LocalCopyFlushThresholdByte;
}

static bool
ShardCopyDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	ShardCopyDestReceiver *copyDest = (ShardCopyDestReceiver *) dest;

	/*
	 * Switch to a per-tuple memory memory context. When used in
	 * context of Split Copy, this is a no-op as switch is already done.
	 */
	EState *executorState = copyDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	/* Create connection lazily */
	if(copyDest->tuplesSent == 0 && (!copyDest->useLocalCopy))
	{
		int connectionFlags = OUTSIDE_TRANSACTION;
		char *currentUser = CurrentUserName();
		WorkerNode *workerNode = FindNodeWithNodeId(copyDest->destinationNodeId, false /* missingOk */);
		copyDest->connection = GetNodeUserDatabaseConnection(connectionFlags,
															workerNode->workerName,
															workerNode->workerPort,
															currentUser,
															NULL /* database (current) */);
		ClaimConnectionExclusively(copyDest->connection);

		StringInfo copyStatement = ConstructCopyStatement(copyDest->destinationShardFullyQualifiedName,
			copyDest->copyOutState->binary);
		ExecuteCriticalRemoteCommand(copyDest->connection, copyStatement->data);
	}

	slot_getallattrs(slot);
	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	CopyOutState copyOutState = copyDest->copyOutState;
	if(copyDest->useLocalCopy)
	{
		WriteLocalTuple(slot, copyDest);
		if (ShouldSendCopyNow(copyOutState->fe_msgbuf))
		{
			LocalCopyToShard(copyDest, copyOutState);
		}
	}
	else
	{
		FmgrInfo *columnOutputFunctions = copyDest->columnOutputFunctions;

		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyRowData(columnValues, columnNulls, copyDest->tupleDescriptor,
							copyOutState, columnOutputFunctions, NULL /* columnCoercionPaths */);
		if (!PutRemoteCopyData(copyDest->connection, copyOutState->fe_msgbuf->data, copyOutState->fe_msgbuf->len))
		{
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("Failed to COPY to shard %s,",
							   copyDest->destinationShardFullyQualifiedName),
						errdetail("failed to send %d bytes %s on node %u", copyOutState->fe_msgbuf->len,
								  copyOutState->fe_msgbuf->data,
								  copyDest->destinationNodeId)));
		}
	}

	MemoryContextSwitchTo(oldContext);
	ResetPerTupleExprContext(executorState);

	copyDest->tuplesSent++;
	return true;
}

static void
ShardCopyDestReceiverStartup(DestReceiver *dest, int operation, TupleDesc inputTupleDescriptor)
{
	ShardCopyDestReceiver *copyDest = (ShardCopyDestReceiver *) dest;
	copyDest->tupleDescriptor = inputTupleDescriptor;
	copyDest->tuplesSent = 0;

	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	/* define how tuples will be serialised */
	CopyOutState copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->binary = CanUseBinaryCopyFormat(inputTupleDescriptor);
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->rowcontext = GetPerTupleMemoryContext(copyDest->executorState);
	copyDest->columnOutputFunctions = ColumnOutputFunctions(inputTupleDescriptor,
															  copyOutState->binary);
	copyDest->copyOutState = copyOutState;
}

static void
ShardCopyDestReceiverShutdown(DestReceiver *dest)
{
	ShardCopyDestReceiver *copyDest = (ShardCopyDestReceiver *) dest;

	if(copyDest->useLocalCopy)
	{
		if (copyDest->copyOutState != NULL &&
			copyDest->copyOutState->fe_msgbuf->len > 0)
		{
			LocalCopyToShard(copyDest, copyDest->copyOutState);
		}
	}
	else if(copyDest->connection != NULL)
	{
		resetStringInfo(copyDest->copyOutState->fe_msgbuf);
		if(copyDest->copyOutState->binary)
		{
			AppendCopyBinaryFooters(copyDest->copyOutState);
		}

		/* end the COPY input */
		if (!PutRemoteCopyEnd(copyDest->connection, NULL /* errormsg */))
		{
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("Failed to COPY to destination shard %s",
								   copyDest->destinationShardFullyQualifiedName)));
		}

		/* check whether there were any COPY errors */
		PGresult *result = GetRemoteCommandResult(copyDest->connection, true /* raiseInterrupts */);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReportCopyError(copyDest->connection, result);
		}

		PQclear(result);
		ForgetResults(copyDest->connection);
		CloseConnection(copyDest->connection);
	}
}

DestReceiver * CreateShardCopyDestReceiver(
	EState *executorState,
	char* destinationShardFullyQualifiedName,
	uint32_t destinationNodeId)
{
	ShardCopyDestReceiver *copyDest = (ShardCopyDestReceiver *) palloc0(
		sizeof(ShardCopyDestReceiver));

	/* set up the DestReceiver function pointers */
	copyDest->pub.receiveSlot = ShardCopyDestReceiverReceive;
	copyDest->pub.rStartup = ShardCopyDestReceiverStartup;
	copyDest->pub.rShutdown = ShardCopyDestReceiverShutdown;
	copyDest->pub.rDestroy = ShardCopyDestReceiverDestroy;
	copyDest->pub.mydest = DestCopyOut;
	copyDest->executorState = executorState;

	copyDest->destinationNodeId = destinationNodeId;
	copyDest->destinationShardFullyQualifiedName = destinationShardFullyQualifiedName;
	copyDest->tuplesSent = 0;
	copyDest->connection = NULL;
	copyDest->useLocalCopy = CanUseLocalCopy(destinationNodeId);

	return (DestReceiver *) copyDest;
}

static void
ShardCopyDestReceiverDestroy(DestReceiver *dest)
{
	ShardCopyDestReceiver *copyDest = (ShardCopyDestReceiver *) dest;

	if (copyDest->copyOutState)
	{
		pfree(copyDest->copyOutState);
	}

	if (copyDest->columnOutputFunctions)
	{
		pfree(copyDest->columnOutputFunctions);
	}

	pfree(copyDest);
}

/*
 * ConstructCopyStatement constructs the text of a COPY statement
 * for copying into a result table
 */
static StringInfo
ConstructCopyStatement(char *destinationShardFullyQualifiedName, bool useBinaryFormat)
{
	StringInfo command  = makeStringInfo();
	appendStringInfo(command, "COPY %s FROM STDIN",
		destinationShardFullyQualifiedName);

	if(useBinaryFormat)
	{
		appendStringInfo(command, "WITH (format binary);");
	}
	else
	{
		appendStringInfo(command, ";");
	}

	return command;
}

static void WriteLocalTuple(TupleTableSlot *slot, ShardCopyDestReceiver *copyDest)
{
	CopyOutState localCopyOutState = copyDest->copyOutState;

	/*
	 * Since we are doing a local copy, the following statements should
	 * use local execution to see the changes
	 */
	SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);

	bool isBinaryCopy = localCopyOutState->binary;
	bool shouldAddBinaryHeaders = (isBinaryCopy && localCopyOutState->fe_msgbuf->len == 0);
	if (shouldAddBinaryHeaders)
	{
		AppendCopyBinaryHeaders(localCopyOutState);
	}

	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;
	FmgrInfo *columnOutputFunctions = copyDest->columnOutputFunctions;

	AppendCopyRowData(columnValues, columnNulls, copyDest->tupleDescriptor,
					  localCopyOutState, columnOutputFunctions,
					  NULL /* columnCoercionPaths */);
}

/*
 * LocalCopyToShard finishes local copy for the given destination shard.
 */
static void
LocalCopyToShard(ShardCopyDestReceiver *copyDest, CopyOutState localCopyOutState)
{
	bool isBinaryCopy = localCopyOutState->binary;
	if (isBinaryCopy)
	{
		AppendCopyBinaryFooters(localCopyOutState);
	}

	/*
	 * Set the buffer as a global variable to allow ReadFromLocalBufferCallback
	 * to read from it. We cannot pass additional arguments to
	 * ReadFromLocalBufferCallback.
	 */
	LocalCopyBuffer = localCopyOutState->fe_msgbuf;

	char *destinationShardSchemaName = NULL;
	char *destinationShardRelationName = NULL;
	DeconstructQualifiedName(list_make1(copyDest->destinationShardFullyQualifiedName), &destinationShardSchemaName, &destinationShardRelationName);

	Oid destinationSchemaOid = get_namespace_oid(destinationShardSchemaName, false /* missing_ok */);
	Oid destinationShardOid = get_relname_relid(destinationShardRelationName, destinationSchemaOid);

	DefElem *binaryFormatOption = NULL;
	if (isBinaryCopy)
	{
		binaryFormatOption = makeDefElem("format", (Node *) makeString("binary"), -1);
	}

	Relation shard = table_open(destinationShardOid, RowExclusiveLock);
	ParseState *pState = make_parsestate(NULL /* parentParseState */);
	(void) addRangeTableEntryForRelation(pState, shard, AccessShareLock,
										 NULL /* alias */, false /* inh */, false /* inFromCl */);
	CopyFromState cstate = BeginCopyFrom_compat(pState, shard,
												NULL /* whereClause */,
												NULL /* fileName */,
												false /* is_program */,
												ReadFromLocalBufferCallback,
												NULL /* attlist (NULL is all columns) */,
												list_make1(binaryFormatOption));
	resetStringInfo(localCopyOutState->fe_msgbuf);

	CopyFrom(cstate);
	EndCopyFrom(cstate);

	table_close(shard, NoLock);
	free_parsestate(pState);
}

/*
 * ReadFromLocalBufferCallback is the copy callback.
 * It always tries to copy maxRead bytes.
 */
static int
ReadFromLocalBufferCallback(void *outBuf, int minRead, int maxRead)
{
	int bytesRead = 0;
	int avail = LocalCopyBuffer->len - LocalCopyBuffer->cursor;
	int bytesToRead = Min(avail, maxRead);
	if (bytesToRead > 0)
	{
		memcpy_s(outBuf, bytesToRead,
				 &LocalCopyBuffer->data[LocalCopyBuffer->cursor], bytesToRead);
	}
	bytesRead += bytesToRead;
	LocalCopyBuffer->cursor += bytesToRead;

	return bytesRead;
}

