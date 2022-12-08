/*-------------------------------------------------------------------------
 *
 * worker_shard_copy.c
 *   Functions for copying a shard to destination.
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
#include "replication/origin.h"

/*
 * LocalCopyBuffer is used in copy callback to return the copied rows.
 * The reason this is a global variable is that we cannot pass an additional
 * argument to the copy callback.
 */
static StringInfo LocalCopyBuffer;
#define CDC_REPLICATION_ORIGIN_CREATE_IF_NOT_EXISTS_CMD \
	"SELECT  pg_catalog.pg_replication_origin_create('citus_internal_%d') \
	where (select pg_catalog.pg_replication_origin_oid('citus_internal_%d')) IS NULL;"

#define CDC_REPLICATION_ORIGIN_SESION_SETUP_CMD \
	"SELECT pg_catalog.pg_replication_origin_session_setup('citus_internal_%d') \
	where pg_catalog.pg_replication_origin_session_is_setup()='f';"

#define CDC_REPLICATION_ORIGIN_SESION_RESET_CMD \
	"SELECT pg_catalog.pg_replication_origin_session_reset() \
	where pg_catalog.pg_replication_origin_session_is_setup()='t';"

typedef struct ShardCopyDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* Destination Relation Name */
	List *destinationShardFullyQualifiedName;

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

	/* Replication Origin Id for local copy*/
	RepOriginId originId;

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
static bool CanUseLocalCopy(uint32_t destinationNodeId);
static StringInfo ConstructShardCopyStatement(List *destinationShardFullyQualifiedName,
											  bool
											  useBinaryFormat);
static void WriteLocalTuple(TupleTableSlot *slot, ShardCopyDestReceiver *copyDest);
static int ReadFromLocalBufferCallback(void *outBuf, int minRead, int maxRead);
static void LocalCopyToShard(ShardCopyDestReceiver *copyDest, CopyOutState
							 localCopyOutState);
static void ConnectToRemoteAndStartCopy(ShardCopyDestReceiver *copyDest);

static void ReplicationOriginSessionCreate(ShardCopyDestReceiver *dest);
static void ReplicationOriginSessionSetup(ShardCopyDestReceiver *dest);
static void ReplicationOriginSessionReset(ShardCopyDestReceiver *dest);

static bool
CanUseLocalCopy(uint32_t destinationNodeId)
{
	/* If destination node is same as source, use local copy */
	return GetLocalNodeId() == (int32) destinationNodeId;
}


#define REPLICATION_ORIGIN_CMD_BUFFER_SIZE 1024

/* Connect to node with source shard and trigger copy start.  */
static void
ConnectToRemoteAndStartCopy(ShardCopyDestReceiver *copyDest)
{
	int connectionFlags = OUTSIDE_TRANSACTION;
	char *currentUser = CurrentUserName();
	WorkerNode *workerNode = FindNodeWithNodeId(copyDest->destinationNodeId,
												false /* missingOk */);
	copyDest->connection = GetNodeUserDatabaseConnection(connectionFlags,
														 workerNode->workerName,
														 workerNode->workerPort,
														 currentUser,
														 NULL /* database (current) */);
	ClaimConnectionExclusively(copyDest->connection);

	ReplicationOriginSessionSetup(copyDest);

	StringInfo copyStatement = ConstructShardCopyStatement(
		copyDest->destinationShardFullyQualifiedName,
		copyDest->copyOutState->binary);
	if (!SendRemoteCommand(copyDest->connection, copyStatement->data))
	{
		ReportConnectionError(copyDest->connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(copyDest->connection,
											  true /* raiseInterrupts */);
	if (PQresultStatus(result) != PGRES_COPY_IN)
	{
		ReportResultError(copyDest->connection, result, ERROR);
	}

	PQclear(result);
}


/*
 * CreateShardCopyDestReceiver creates a DestReceiver that copies into
 * a destinationShardFullyQualifiedName on destinationNodeId.
 */
DestReceiver *
CreateShardCopyDestReceiver(EState *executorState,
							List *destinationShardFullyQualifiedName,
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


#define InvalidRepOriginId 0

/*
 * ShardCopyDestReceiverReceive implements the receiveSlot function of
 * ShardCopyDestReceiver. It takes a TupleTableSlot and sends the contents to
 * the appropriate destination node.
 */
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

	/* If remote copy, connect lazily and initiate copy */
	if (copyDest->tuplesSent == 0 && (!copyDest->useLocalCopy))
	{
		ConnectToRemoteAndStartCopy(copyDest);
	}

	slot_getallattrs(slot);
	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	CopyOutState copyOutState = copyDest->copyOutState;
	if (copyDest->useLocalCopy)
	{
		/* Setup replication origin session for local copy*/
		ReplicationOriginSessionSetup(copyDest);

		WriteLocalTuple(slot, copyDest);
		if (copyOutState->fe_msgbuf->len > LocalCopyFlushThresholdByte)
		{
			LocalCopyToShard(copyDest, copyOutState);
		}
	}
	else
	{
		resetStringInfo(copyOutState->fe_msgbuf);
		if (copyDest->copyOutState->binary && copyDest->tuplesSent == 0)
		{
			AppendCopyBinaryHeaders(copyDest->copyOutState);
		}

		AppendCopyRowData(columnValues,
						  columnNulls,
						  copyDest->tupleDescriptor,
						  copyOutState,
						  copyDest->columnOutputFunctions,
						  NULL /* columnCoercionPaths */);
		if (!PutRemoteCopyData(copyDest->connection, copyOutState->fe_msgbuf->data,
							   copyOutState->fe_msgbuf->len))
		{
			char *destinationShardSchemaName = linitial(
				copyDest->destinationShardFullyQualifiedName);
			char *destinationShardRelationName = lsecond(
				copyDest->destinationShardFullyQualifiedName);

			char *errorMessage = PQerrorMessage(copyDest->connection->pgConn);
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("Failed to COPY to shard %s.%s : %s,",
								   destinationShardSchemaName,
								   destinationShardRelationName,
								   errorMessage),
							errdetail("failed to send %d bytes %s on node %u",
									  copyOutState->fe_msgbuf->len,
									  copyOutState->fe_msgbuf->data,
									  copyDest->destinationNodeId)));
		}
	}

	MemoryContextSwitchTo(oldContext);
	ResetPerTupleExprContext(executorState);

	copyDest->tuplesSent++;
	return true;
}


/*
 * ShardCopyDestReceiverStartup implements the rStartup interface of ShardCopyDestReceiver.
 */
static void
ShardCopyDestReceiverStartup(DestReceiver *dest, int operation, TupleDesc
							 inputTupleDescriptor)
{
	ShardCopyDestReceiver *copyDest = (ShardCopyDestReceiver *) dest;
	copyDest->tupleDescriptor = inputTupleDescriptor;
	copyDest->tuplesSent = 0;

	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	/* define how tuples will be serialised */
	CopyOutState copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->binary = EnableBinaryProtocol && CanUseBinaryCopyFormat(
		inputTupleDescriptor);
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->rowcontext = GetPerTupleMemoryContext(copyDest->executorState);
	copyDest->columnOutputFunctions = ColumnOutputFunctions(inputTupleDescriptor,
															copyOutState->binary);
	copyDest->copyOutState = copyOutState;
	ReplicationOriginSessionCreate(copyDest);
}


/* ReplicationOriginSessionCreate creates a new replication origin if it does
 * not already exist already. To make the replication origin name unique
 * for different nodes, origin node's id is appended to the prefix citus_internal_.*/
static void
ReplicationOriginSessionCreate(ShardCopyDestReceiver *dest)
{
	int localid = GetLocalNodeId();
	if (dest->useLocalCopy)
	{
		char originName[64];
		snprintf(originName, sizeof(originName), "citus_internal_%d", localid);
		RepOriginId originId = replorigin_by_name(originName, true);
		if (originId == InvalidRepOriginId)
		{
			originId = replorigin_create(originName);
		}
		dest->originId = originId;
	}
	else
	{
		int connectionFlags = OUTSIDE_TRANSACTION;
		char *currentUser = CurrentUserName();
		WorkerNode *workerNode = FindNodeWithNodeId(dest->destinationNodeId,
													false /* missingOk */);
		MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																	workerNode->workerName,
																	workerNode->workerPort,
																	currentUser,
																	NULL /* database (current) */);

		char replicationOrginCreateCommand[REPLICATION_ORIGIN_CMD_BUFFER_SIZE];
		snprintf(replicationOrginCreateCommand, REPLICATION_ORIGIN_CMD_BUFFER_SIZE,
				 CDC_REPLICATION_ORIGIN_CREATE_IF_NOT_EXISTS_CMD, localid, localid);

		ExecuteCriticalRemoteCommand(connection, replicationOrginCreateCommand);
		CloseConnection(connection);
		dest->originId = InvalidRepOriginId;
	}
}


/* ReplicationOriginSessionSetup sets up a new replication origin session in a
 * local or remote session depending on the useLocalCopy flag. If useLocalCopy
 * is set, a local replication origin session is setup, otherwise a remote
 * replication origin session is setup to the destination node.
 */
void
ReplicationOriginSessionSetup(ShardCopyDestReceiver *dest)
{
	if (dest->useLocalCopy)
	{
		/*Setup Replication Origin in local session */
		if (replorigin_session_origin == InvalidRepOriginId)
		{
			replorigin_session_setup(dest->originId);
			replorigin_session_origin = dest->originId;
		}
	}
	else
	{
		/*Setup Replication Origin in remote session */
		char replicationOrginSetupCommand[REPLICATION_ORIGIN_CMD_BUFFER_SIZE];
		int localId = GetLocalNodeId();
		snprintf(replicationOrginSetupCommand, REPLICATION_ORIGIN_CMD_BUFFER_SIZE,
				 CDC_REPLICATION_ORIGIN_SESION_SETUP_CMD, localId);
		ExecuteCriticalRemoteCommand(dest->connection, replicationOrginSetupCommand);
	}
}


/* ReplicationOriginSessionReset resets the replication origin session in a
 * local or remote session depending on the useLocalCopy flag.
 */
void
ReplicationOriginSessionReset(ShardCopyDestReceiver *dest)
{
	if (dest->useLocalCopy)
	{
		/*Reset Replication Origin in local session */
		if (replorigin_session_origin != InvalidRepOriginId)
		{
			replorigin_session_reset();
			replorigin_session_origin = InvalidRepOriginId;
		}
		dest->originId = InvalidRepOriginId;
	}
	else
	{
		/*Reset Replication Origin in remote session */
		ExecuteCriticalRemoteCommand(dest->connection,
									 CDC_REPLICATION_ORIGIN_SESION_RESET_CMD);
	}
}


/*
 * ShardCopyDestReceiverShutdown implements the rShutdown interface of
 * ShardCopyDestReceiver. It ends all open COPY operations, copying any pending
 * data in buffer.
 */
static void
ShardCopyDestReceiverShutdown(DestReceiver *dest)
{
	ShardCopyDestReceiver *copyDest = (ShardCopyDestReceiver *) dest;

	if (copyDest->useLocalCopy)
	{
		if (copyDest->copyOutState != NULL &&
			copyDest->copyOutState->fe_msgbuf->len > 0)
		{
			/* end the COPY input */
			LocalCopyToShard(copyDest, copyDest->copyOutState);
		}
		ReplicationOriginSessionReset(copyDest);
	}
	else if (copyDest->connection != NULL)
	{
		resetStringInfo(copyDest->copyOutState->fe_msgbuf);
		if (copyDest->copyOutState->binary)
		{
			AppendCopyBinaryFooters(copyDest->copyOutState);
		}

		/* end the COPY input */
		if (!PutRemoteCopyEnd(copyDest->connection, NULL /* errormsg */))
		{
			char *destinationShardSchemaName = linitial(
				copyDest->destinationShardFullyQualifiedName);
			char *destinationShardRelationName = lsecond(
				copyDest->destinationShardFullyQualifiedName);

			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("Failed to COPY to destination shard %s.%s",
								   destinationShardSchemaName,
								   destinationShardRelationName),
							errdetail("failed to send %d bytes %s on node %u",
									  copyDest->copyOutState->fe_msgbuf->len,
									  copyDest->copyOutState->fe_msgbuf->data,
									  copyDest->destinationNodeId)));
		}

		/* check whether there were any COPY errors */
		PGresult *result = GetRemoteCommandResult(copyDest->connection,
												  true /* raiseInterrupts */);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReportCopyError(copyDest->connection, result);
		}

		PQclear(result);
		ForgetResults(copyDest->connection);
		ReplicationOriginSessionReset(copyDest);

		CloseConnection(copyDest->connection);
	}
}


/*
 * ShardCopyDestReceiverDestroy frees the DestReceiver.
 */
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
 * ConstructShardCopyStatement constructs the text of a COPY statement
 * for copying into a result table
 */
static StringInfo
ConstructShardCopyStatement(List *destinationShardFullyQualifiedName, bool
							useBinaryFormat)
{
	char *destinationShardSchemaName = linitial(destinationShardFullyQualifiedName);
	char *destinationShardRelationName = lsecond(destinationShardFullyQualifiedName);

	StringInfo command = makeStringInfo();
	appendStringInfo(command, "COPY %s.%s FROM STDIN",
					 quote_identifier(destinationShardSchemaName), quote_identifier(
						 destinationShardRelationName));

	if (useBinaryFormat)
	{
		appendStringInfo(command, " WITH (format binary);");
	}
	else
	{
		appendStringInfo(command, ";");
	}

	return command;
}


/* Write Tuple to Local Shard. */
static void
WriteLocalTuple(TupleTableSlot *slot, ShardCopyDestReceiver *copyDest)
{
	CopyOutState localCopyOutState = copyDest->copyOutState;

	/*
	 * Since we are doing a local copy, the following statements should
	 * use local execution to see the changes
	 */
	SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);

	bool isBinaryCopy = localCopyOutState->binary;
	bool shouldAddBinaryHeaders = (isBinaryCopy && localCopyOutState->fe_msgbuf->len ==
								   0);
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
 * LocalCopyToShard performs local copy for the given destination shard.
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

	char *destinationShardSchemaName = linitial(
		copyDest->destinationShardFullyQualifiedName);
	char *destinationShardRelationName = lsecond(
		copyDest->destinationShardFullyQualifiedName);

	Oid destinationSchemaOid = get_namespace_oid(destinationShardSchemaName,
												 false /* missing_ok */);
	Oid destinationShardOid = get_relname_relid(destinationShardRelationName,
												destinationSchemaOid);

	DefElem *binaryFormatOption = NULL;
	if (isBinaryCopy)
	{
		binaryFormatOption = makeDefElem("format", (Node *) makeString("binary"), -1);
	}

	Relation shard = table_open(destinationShardOid, RowExclusiveLock);
	ParseState *pState = make_parsestate(NULL /* parentParseState */);
	(void) addRangeTableEntryForRelation(pState, shard, AccessShareLock,
										 NULL /* alias */, false /* inh */,
										 false /* inFromCl */);

	List *options = (isBinaryCopy) ? list_make1(binaryFormatOption) : NULL;
	CopyFromState cstate = BeginCopyFrom_compat(pState, shard,
												NULL /* whereClause */,
												NULL /* fileName */,
												false /* is_program */,
												ReadFromLocalBufferCallback,
												NULL /* attlist (NULL is all columns) */,
												options);
	CopyFrom(cstate);
	EndCopyFrom(cstate);
	resetStringInfo(localCopyOutState->fe_msgbuf);

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
