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

#include "postgres.h"
#include "commands/copy.h"
#include "catalog/namespace.h"
#include "parser/parse_relation.h"
#include "utils/lsyscache.h"
#include "nodes/makefuncs.h"
#include "safe_lib.h"
#include <netinet/in.h> /* for htons */

#include "distributed/transmit.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/local_executor.h"
#include "distributed/local_multi_copy.h"
#include "distributed/shard_utils.h"

/*
 * LOCAL_COPY_BUFFER_SIZE is buffer size for local copy.
 * There will be one buffer for each local placement, therefore
 * the maximum amount of memory that might be alocated is
 * LOCAL_COPY_BUFFER_SIZE * #local_placement
 */
#define LOCAL_COPY_BUFFER_SIZE (1 * 512 * 1024)


static int ReadFromLocalBufferCallback(void *outbuf, int minread, int maxread);
static Relation CreateCopiedShard(RangeVar *distributedRel, Relation shard);
static void AddSlotToBuffer(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest,
							bool isBinary);

static bool ShouldSendCopyNow(StringInfo buffer);
static void DoLocalCopy(StringInfo buffer, Oid relationId, int64 shardId,
						CopyStmt *copyStatement, bool isEndOfCopy);
static bool ShouldAddBinaryHeaders(StringInfo buffer, bool isBinary);

/*
 * localCopyBuffer is used in copy callback to return the copied rows.
 * The reason this is a global variable is that we cannot pass an additional
 * argument to the copy callback.
 */
StringInfo localCopyBuffer;

/*
 * ProcessLocalCopy adds the given slot and does a local copy if
 * this is the end of copy, or the buffer size exceeds the threshold.
 */
void
ProcessLocalCopy(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest, int64 shardId,
				 StringInfo buffer, bool isEndOfCopy)
{
	/*
	 * Here we save the previous buffer, and put the local shard's buffer
	 * into copyOutState. The motivation is to use the existing logic to
	 * serialize a row slot into buffer.
	 */
	StringInfo previousBuffer = copyDest->copyOutState->fe_msgbuf;
	copyDest->copyOutState->fe_msgbuf = buffer;

	/* since we are doing a local copy, the following statements should use local execution to see the changes */
	TransactionAccessedLocalPlacement = true;

	bool isBinaryCopy = copyDest->copyOutState->binary;
	AddSlotToBuffer(slot, copyDest, isBinaryCopy);

	if (isEndOfCopy || ShouldSendCopyNow(buffer))
	{
		if (isBinaryCopy)
		{
			AppendCopyBinaryFooters(copyDest->copyOutState);
		}

		DoLocalCopy(buffer, copyDest->distributedRelationId, shardId,
					copyDest->copyStatement, isEndOfCopy);
	}
	copyDest->copyOutState->fe_msgbuf = previousBuffer;
}


/*
 * AddSlotToBuffer serializes the given slot and adds it to the buffer in copyDest.
 * If the copy format is binary, it adds binary headers as well.
 */
static void
AddSlotToBuffer(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest, bool isBinary)
{
	if (ShouldAddBinaryHeaders(copyDest->copyOutState->fe_msgbuf, isBinary))
	{
		AppendCopyBinaryHeaders(copyDest->copyOutState);
	}

	if (slot != NULL)
	{
		Datum *columnValues = slot->tts_values;
		bool *columnNulls = slot->tts_isnull;
		FmgrInfo *columnOutputFunctions = copyDest->columnOutputFunctions;
		CopyCoercionData *columnCoercionPaths = copyDest->columnCoercionPaths;

		AppendCopyRowData(columnValues, columnNulls, copyDest->tupleDescriptor,
						  copyDest->copyOutState, columnOutputFunctions,
						  columnCoercionPaths);
	}
}


/*
 * ShouldSendCopyNow returns true if the given buffer size exceeds the
 * local copy buffer size threshold.
 */
static bool
ShouldSendCopyNow(StringInfo buffer)
{
	return buffer->len > LOCAL_COPY_BUFFER_SIZE;
}


/*
 * DoLocalCopy finds the shard table from the distributed relation id, and copies the given
 * buffer into the shard.
 */
static void
DoLocalCopy(StringInfo buffer, Oid relationId, int64 shardId, CopyStmt *copyStatement,
			bool isEndOfCopy)
{
	localCopyBuffer = buffer;

	Oid shardOid = GetShardLocalTableOid(relationId, shardId);
	Relation shard = heap_open(shardOid, RowExclusiveLock);
	Relation copiedShard = CreateCopiedShard(copyStatement->relation, shard);
	ParseState *pState = make_parsestate(NULL);

	/* p_rtable of pState is set so that we can check constraints. */
	pState->p_rtable = CreateRangeTable(copiedShard, ACL_INSERT);

	CopyState cstate = BeginCopyFrom(pState, copiedShard, NULL, false,
									 ReadFromLocalBufferCallback,
									 copyStatement->attlist, copyStatement->options);
	CopyFrom(cstate);
	EndCopyFrom(cstate);

	heap_close(shard, NoLock);
	free_parsestate(pState);
	FreeStringInfo(buffer);
	if (!isEndOfCopy)
	{
		buffer = makeStringInfo();
	}
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
 * CreateCopiedShard clones deep copies the necessary fields of the given
 * relation.
 */
Relation
CreateCopiedShard(RangeVar *distributedRel, Relation shard)
{
	TupleDesc tupleDescriptor = RelationGetDescr(shard);

	Relation copiedDistributedRelation = (Relation) palloc(sizeof(RelationData));
	Form_pg_class copiedDistributedRelationTuple = (Form_pg_class) palloc(
		CLASS_TUPLE_SIZE);

	*copiedDistributedRelation = *shard;
	*copiedDistributedRelationTuple = *shard->rd_rel;

	copiedDistributedRelation->rd_rel = copiedDistributedRelationTuple;
	copiedDistributedRelation->rd_att = CreateTupleDescCopyConstr(tupleDescriptor);

	Oid tableId = RangeVarGetRelid(distributedRel, NoLock, false);

	/*
	 * BeginCopyFrom opens all partitions of given partitioned table with relation_open
	 * and it expects its caller to close those relations. We do not have direct access
	 * to opened relations, thus we are changing relkind of partitioned tables so that
	 * Postgres will treat those tables as regular relations and will not open its
	 * partitions.
	 */
	if (PartitionedTable(tableId))
	{
		copiedDistributedRelationTuple->relkind = RELKIND_RELATION;
	}
	return copiedDistributedRelation;
}


/*
 * ReadFromLocalBufferCallback is the copy callback.
 * It always tries to copy maxread bytes.
 */
static int
ReadFromLocalBufferCallback(void *outbuf, int minread, int maxread)
{
	int bytesread = 0;
	int avail = localCopyBuffer->len - localCopyBuffer->cursor;
	int bytesToRead = Min(avail, maxread);
	if (bytesToRead > 0)
	{
		memcpy_s(outbuf, bytesToRead + strlen((char *) outbuf),
				 &localCopyBuffer->data[localCopyBuffer->cursor], bytesToRead);
	}
	bytesread += bytesToRead;
	localCopyBuffer->cursor += bytesToRead;

	return bytesread;
}
