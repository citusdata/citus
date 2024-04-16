/*-------------------------------------------------------------------------
 *
 * local_multi_copy.c
 *    Commands for running a copy locally
 *
 * For each local placement, we have a buffer. When we receive a slot
 * from a copy, the slot will be put to the corresponding buffer based
 * on the shard id. When the buffer size exceeds the threshold a local
 * copy will be done. Also If we reach to the end of copy, we will send
 * the current buffer for local copy.
 *
 * The existing logic from multi_copy.c and format are used, therefore
 * even if user did not do a copy with binary format, it is possible that
 * we are going to be using binary format internally.
 *
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <netinet/in.h> /* for htons */

#include "postgres.h"

#include "safe_lib.h"

#include "catalog/namespace.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "utils/lsyscache.h"

#include "distributed/commands/multi_copy.h"
#include "distributed/intermediate_results.h"
#include "distributed/local_executor.h"
#include "distributed/local_multi_copy.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/replication_origin_session_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/transmit.h"
#include "distributed/version_compat.h"

/* managed via GUC, default is 512 kB */
int LocalCopyFlushThresholdByte = 512 * 1024;


static void AddSlotToBuffer(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest,
							CopyOutState localCopyOutState);
static bool ShouldAddBinaryHeaders(StringInfo buffer, bool isBinary);
static bool ShouldSendCopyNow(StringInfo buffer);
static void DoLocalCopy(StringInfo buffer, Oid relationId, int64 shardId,
						CopyStmt *copyStatement, bool isEndOfCopy, bool isPublishable);
static int ReadFromLocalBufferCallback(void *outBuf, int minRead, int maxRead);


/*
 * LocalCopyBuffer is used in copy callback to return the copied rows.
 * The reason this is a global variable is that we cannot pass an additional
 * argument to the copy callback.
 */
static StringInfo LocalCopyBuffer;


/*
 * WriteTupleToLocalShard adds the given slot and does a local copy if
 * this is the end of copy, or the buffer size exceeds the threshold.
 */
void
WriteTupleToLocalShard(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest, int64
					   shardId,
					   CopyOutState localCopyOutState)
{
	/*
	 * Since we are doing a local copy, the following statements should
	 * use local execution to see the changes
	 */
	SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);

	bool isBinaryCopy = localCopyOutState->binary;
	if (ShouldAddBinaryHeaders(localCopyOutState->fe_msgbuf, isBinaryCopy))
	{
		AppendCopyBinaryHeaders(localCopyOutState);
	}

	AddSlotToBuffer(slot, copyDest, localCopyOutState);

	if (ShouldSendCopyNow(localCopyOutState->fe_msgbuf))
	{
		if (isBinaryCopy)
		{
			/*
			 * We're going to flush the buffer to disk by effectively doing a full
			 * COPY command. Hence we also need to add footers to the current buffer.
			 */
			AppendCopyBinaryFooters(localCopyOutState);
		}
		bool isEndOfCopy = false;
		DoLocalCopy(localCopyOutState->fe_msgbuf, copyDest->distributedRelationId,
					shardId,
					copyDest->copyStatement, isEndOfCopy, copyDest->isPublishable);
		resetStringInfo(localCopyOutState->fe_msgbuf);
	}
}


/*
 * WriteTupleToLocalFile adds the given slot and does a local copy to the
 * file if the buffer size exceeds the threshold.
 */
void
WriteTupleToLocalFile(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest,
					  int64 shardId, CopyOutState localFileCopyOutState,
					  FileCompat *fileCompat)
{
	AddSlotToBuffer(slot, copyDest, localFileCopyOutState);

	if (ShouldSendCopyNow(localFileCopyOutState->fe_msgbuf))
	{
		WriteToLocalFile(localFileCopyOutState->fe_msgbuf, fileCompat);
		resetStringInfo(localFileCopyOutState->fe_msgbuf);
	}
}


/*
 * FinishLocalCopyToShard finishes local copy for the given shard with the shard id.
 */
void
FinishLocalCopyToShard(CitusCopyDestReceiver *copyDest, int64 shardId,
					   CopyOutState localCopyOutState)
{
	bool isBinaryCopy = localCopyOutState->binary;
	if (isBinaryCopy)
	{
		AppendCopyBinaryFooters(localCopyOutState);
	}
	bool isEndOfCopy = true;
	DoLocalCopy(localCopyOutState->fe_msgbuf, copyDest->distributedRelationId, shardId,
				copyDest->copyStatement, isEndOfCopy, copyDest->isPublishable);
}


/*
 * FinishLocalCopyToFile finishes local copy for the given file.
 */
void
FinishLocalCopyToFile(CopyOutState localFileCopyOutState, FileCompat *fileCompat)
{
	StringInfo data = localFileCopyOutState->fe_msgbuf;

	bool isBinaryCopy = localFileCopyOutState->binary;
	if (isBinaryCopy)
	{
		AppendCopyBinaryFooters(localFileCopyOutState);
	}
	WriteToLocalFile(data, fileCompat);
	resetStringInfo(localFileCopyOutState->fe_msgbuf);

	FileClose(fileCompat->fd);
}


/*
 * AddSlotToBuffer serializes the given slot and adds it to
 * the buffer in localCopyOutState.
 */
static void
AddSlotToBuffer(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest, CopyOutState
				localCopyOutState)
{
	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;
	FmgrInfo *columnOutputFunctions = copyDest->columnOutputFunctions;
	CopyCoercionData *columnCoercionPaths = copyDest->columnCoercionPaths;

	AppendCopyRowData(columnValues, columnNulls, copyDest->tupleDescriptor,
					  localCopyOutState, columnOutputFunctions,
					  columnCoercionPaths);
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


/*
 * DoLocalCopy finds the shard table from the distributed relation id, and copies the given
 * buffer into the shard.
 * CopyFrom calls ReadFromLocalBufferCallback to read bytes from the buffer
 * as though it was reading from stdin. It then parses the tuples and
 * writes them to the shardOid table.
 */
static void
DoLocalCopy(StringInfo buffer, Oid relationId, int64 shardId, CopyStmt *copyStatement,
			bool isEndOfCopy, bool isPublishable)
{
	/*
	 * Set the buffer as a global variable to allow ReadFromLocalBufferCallback
	 * to read from it. We cannot pass additional arguments to
	 * ReadFromLocalBufferCallback.
	 */
	LocalCopyBuffer = buffer;
	if (!isPublishable)
	{
		SetupReplicationOriginLocalSession();
	}

	Oid shardOid = GetTableLocalShardOid(relationId, shardId);
	Relation shard = table_open(shardOid, RowExclusiveLock);
	ParseState *pState = make_parsestate(NULL);
	(void) addRangeTableEntryForRelation(pState, shard, AccessShareLock,
										 NULL, false, false);
	CopyFromState cstate = BeginCopyFrom(pState, shard, NULL, NULL, false,
										 ReadFromLocalBufferCallback,
										 copyStatement->attlist,
										 copyStatement->options);
	CopyFrom(cstate);
	EndCopyFrom(cstate);

	table_close(shard, NoLock);
	if (!isPublishable)
	{
		ResetReplicationOriginLocalSession();
	}
	free_parsestate(pState);
}


/*
 * ShouldAddBinaryHeaders returns true if the given buffer
 * is empty and the format is binary.
 */
static bool
ShouldAddBinaryHeaders(StringInfo buffer, bool isBinary)
{
	if (!isBinary)
	{
		return false;
	}
	return buffer->len == 0;
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
