/*-------------------------------------------------------------------------
 *
 * cstore_writer.c
 *
 * This file contains function definitions for writing cstore files. This
 * includes the logic for writing file level metadata, writing row stripes,
 * and calculating chunk skip nodes.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"

#include "columnar/cstore.h"
#include "columnar/cstore_version_compat.h"

static StripeBuffers * CreateEmptyStripeBuffers(uint32 stripeMaxRowCount,
												uint32 chunkRowCount,
												uint32 columnCount);
static StripeSkipList * CreateEmptyStripeSkipList(uint32 stripeMaxRowCount,
												  uint32 chunkRowCount,
												  uint32 columnCount);
static void FlushStripe(TableWriteState *writeState);
static StringInfo SerializeBoolArray(bool *boolArray, uint32 boolArrayLength);
static void SerializeSingleDatum(StringInfo datumBuffer, Datum datum,
								 bool datumTypeByValue, int datumTypeLength,
								 char datumTypeAlign);
static void SerializeChunkData(TableWriteState *writeState, uint32 chunkIndex,
							   uint32 rowCount);
static void UpdateChunkSkipNodeMinMax(ColumnChunkSkipNode *chunkSkipNode,
									  Datum columnValue, bool columnTypeByValue,
									  int columnTypeLength, Oid columnCollation,
									  FmgrInfo *comparisonFunction);
static Datum DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength);
static StringInfo CopyStringInfo(StringInfo sourceString);

/*
 * CStoreBeginWrite initializes a cstore data load operation and returns a table
 * handle. This handle should be used for adding the row values and finishing the
 * data load operation. If the cstore footer file already exists, we read the
 * footer and then seek to right after the last stripe  where the new stripes
 * will be added.
 */
TableWriteState *
CStoreBeginWrite(RelFileNode relfilenode,
				 ColumnarOptions options,
				 TupleDesc tupleDescriptor)
{
	/* get comparison function pointers for each of the columns */
	uint32 columnCount = tupleDescriptor->natts;
	FmgrInfo **comparisonFunctionArray = palloc0(columnCount * sizeof(FmgrInfo *));
	for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *comparisonFunction = NULL;
		FormData_pg_attribute *attributeForm = TupleDescAttr(tupleDescriptor,
															 columnIndex);

		if (!attributeForm->attisdropped)
		{
			Oid typeId = attributeForm->atttypid;

			comparisonFunction = GetFunctionInfoOrNull(typeId, BTREE_AM_OID,
													   BTORDER_PROC);
		}

		comparisonFunctionArray[columnIndex] = comparisonFunction;
	}

	/*
	 * We allocate all stripe specific data in the stripeWriteContext, and
	 * reset this memory context once we have flushed the stripe to the file.
	 * This is to avoid memory leaks.
	 */
	MemoryContext stripeWriteContext = AllocSetContextCreate(CurrentMemoryContext,
															 "Stripe Write Memory Context",
															 ALLOCSET_DEFAULT_SIZES);

	bool *columnMaskArray = palloc(columnCount * sizeof(bool));
	memset(columnMaskArray, true, columnCount);

	ChunkData *chunkData = CreateEmptyChunkData(columnCount, columnMaskArray,
												options.chunkRowCount);

	TableWriteState *writeState = palloc0(sizeof(TableWriteState));
	writeState->relfilenode = relfilenode;
	writeState->options = options;
	writeState->tupleDescriptor = CreateTupleDescCopy(tupleDescriptor);
	writeState->comparisonFunctionArray = comparisonFunctionArray;
	writeState->stripeBuffers = NULL;
	writeState->stripeSkipList = NULL;
	writeState->stripeWriteContext = stripeWriteContext;
	writeState->chunkData = chunkData;
	writeState->compressionBuffer = NULL;
	writeState->perTupleContext = AllocSetContextCreate(CurrentMemoryContext,
														"CStore per tuple context",
														ALLOCSET_DEFAULT_SIZES);

	return writeState;
}


/*
 * CStoreWriteRow adds a row to the cstore file. If the stripe is not initialized,
 * we create structures to hold stripe data and skip list. Then, we serialize and
 * append data to serialized value buffer for each of the columns and update
 * corresponding skip nodes. Then, whole chunk data is compressed at every
 * rowChunkCount insertion. Then, if row count exceeds stripeMaxRowCount, we flush
 * the stripe, and add its metadata to the table footer.
 */
void
CStoreWriteRow(TableWriteState *writeState, Datum *columnValues, bool *columnNulls)
{
	uint32 columnIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	uint32 columnCount = writeState->tupleDescriptor->natts;
	ColumnarOptions *options = &writeState->options;
	const uint32 chunkRowCount = options->chunkRowCount;
	ChunkData *chunkData = writeState->chunkData;
	MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

	if (stripeBuffers == NULL)
	{
		stripeBuffers = CreateEmptyStripeBuffers(options->stripeRowCount,
												 chunkRowCount, columnCount);
		stripeSkipList = CreateEmptyStripeSkipList(options->stripeRowCount,
												   chunkRowCount, columnCount);
		writeState->stripeBuffers = stripeBuffers;
		writeState->stripeSkipList = stripeSkipList;
		writeState->compressionBuffer = makeStringInfo();

		/*
		 * serializedValueBuffer lives in stripe write memory context so it needs to be
		 * initialized when the stripe is created.
		 */
		for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{
			chunkData->valueBufferArray[columnIndex] = makeStringInfo();
		}
	}

	uint32 chunkIndex = stripeBuffers->rowCount / chunkRowCount;
	uint32 chunkRowIndex = stripeBuffers->rowCount % chunkRowCount;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnChunkSkipNode **chunkSkipNodeArray = stripeSkipList->chunkSkipNodeArray;
		ColumnChunkSkipNode *chunkSkipNode =
			&chunkSkipNodeArray[columnIndex][chunkIndex];

		if (columnNulls[columnIndex])
		{
			chunkData->existsArray[columnIndex][chunkRowIndex] = false;
		}
		else
		{
			FmgrInfo *comparisonFunction =
				writeState->comparisonFunctionArray[columnIndex];
			Form_pg_attribute attributeForm =
				TupleDescAttr(writeState->tupleDescriptor, columnIndex);
			bool columnTypeByValue = attributeForm->attbyval;
			int columnTypeLength = attributeForm->attlen;
			Oid columnCollation = attributeForm->attcollation;
			char columnTypeAlign = attributeForm->attalign;

			chunkData->existsArray[columnIndex][chunkRowIndex] = true;

			SerializeSingleDatum(chunkData->valueBufferArray[columnIndex],
								 columnValues[columnIndex], columnTypeByValue,
								 columnTypeLength, columnTypeAlign);

			UpdateChunkSkipNodeMinMax(chunkSkipNode, columnValues[columnIndex],
									  columnTypeByValue, columnTypeLength,
									  columnCollation, comparisonFunction);
		}

		chunkSkipNode->rowCount++;
	}

	stripeSkipList->chunkCount = chunkIndex + 1;

	/* last row of the chunk is inserted serialize the chunk */
	if (chunkRowIndex == chunkRowCount - 1)
	{
		SerializeChunkData(writeState, chunkIndex, chunkRowCount);
	}

	stripeBuffers->rowCount++;
	if (stripeBuffers->rowCount >= options->stripeRowCount)
	{
		CStoreFlushPendingWrites(writeState);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CStoreEndWrite finishes a cstore data load operation. If we have an unflushed
 * stripe, we flush it. Then, we sync and close the cstore data file. Last, we
 * flush the footer to a temporary file, and atomically rename this temporary
 * file to the original footer file.
 */
void
CStoreEndWrite(TableWriteState *writeState)
{
	CStoreFlushPendingWrites(writeState);

	MemoryContextDelete(writeState->stripeWriteContext);
	pfree(writeState->comparisonFunctionArray);
	FreeChunkData(writeState->chunkData);
	pfree(writeState);
}


void
CStoreFlushPendingWrites(TableWriteState *writeState)
{
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	if (stripeBuffers != NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

		FlushStripe(writeState);
		MemoryContextReset(writeState->stripeWriteContext);

		/* set stripe data and skip list to NULL so they are recreated next time */
		writeState->stripeBuffers = NULL;
		writeState->stripeSkipList = NULL;

		MemoryContextSwitchTo(oldContext);
	}
}


/*
 * CreateEmptyStripeBuffers allocates an empty StripeBuffers structure with the given
 * column count.
 */
static StripeBuffers *
CreateEmptyStripeBuffers(uint32 stripeMaxRowCount, uint32 chunkRowCount,
						 uint32 columnCount)
{
	uint32 columnIndex = 0;
	uint32 maxChunkCount = (stripeMaxRowCount / chunkRowCount) + 1;
	ColumnBuffers **columnBuffersArray = palloc0(columnCount * sizeof(ColumnBuffers *));

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 chunkIndex = 0;
		ColumnChunkBuffers **chunkBuffersArray =
			palloc0(maxChunkCount * sizeof(ColumnChunkBuffers *));

		for (chunkIndex = 0; chunkIndex < maxChunkCount; chunkIndex++)
		{
			chunkBuffersArray[chunkIndex] = palloc0(sizeof(ColumnChunkBuffers));
			chunkBuffersArray[chunkIndex]->existsBuffer = NULL;
			chunkBuffersArray[chunkIndex]->valueBuffer = NULL;
			chunkBuffersArray[chunkIndex]->valueCompressionType = COMPRESSION_NONE;
		}

		columnBuffersArray[columnIndex] = palloc0(sizeof(ColumnBuffers));
		columnBuffersArray[columnIndex]->chunkBuffersArray = chunkBuffersArray;
	}

	StripeBuffers *stripeBuffers = palloc0(sizeof(StripeBuffers));
	stripeBuffers->columnBuffersArray = columnBuffersArray;
	stripeBuffers->columnCount = columnCount;
	stripeBuffers->rowCount = 0;

	return stripeBuffers;
}


/*
 * CreateEmptyStripeSkipList allocates an empty StripeSkipList structure with
 * the given column count. This structure has enough chunks to hold statistics
 * for stripeMaxRowCount rows.
 */
static StripeSkipList *
CreateEmptyStripeSkipList(uint32 stripeMaxRowCount, uint32 chunkRowCount,
						  uint32 columnCount)
{
	uint32 columnIndex = 0;
	uint32 maxChunkCount = (stripeMaxRowCount / chunkRowCount) + 1;

	ColumnChunkSkipNode **chunkSkipNodeArray =
		palloc0(columnCount * sizeof(ColumnChunkSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		chunkSkipNodeArray[columnIndex] =
			palloc0(maxChunkCount * sizeof(ColumnChunkSkipNode));
	}

	StripeSkipList *stripeSkipList = palloc0(sizeof(StripeSkipList));
	stripeSkipList->columnCount = columnCount;
	stripeSkipList->chunkCount = 0;
	stripeSkipList->chunkSkipNodeArray = chunkSkipNodeArray;

	return stripeSkipList;
}


void
WriteToSmgr(Relation rel, uint64 logicalOffset, char *data, uint32 dataLength)
{
	uint64 remaining = dataLength;
	Buffer buffer;

	while (remaining > 0)
	{
		SmgrAddr addr = logical_to_smgr(logicalOffset);

		RelationOpenSmgr(rel);
		BlockNumber nblocks PG_USED_FOR_ASSERTS_ONLY =
			smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
		Assert(addr.blockno < nblocks);
		RelationCloseSmgr(rel);

		buffer = ReadBuffer(rel, addr.blockno);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

		Page page = BufferGetPage(buffer);
		PageHeader phdr = (PageHeader) page;
		if (PageIsNew(page))
		{
			PageInit(page, BLCKSZ, 0);
		}

		/*
		 * After a transaction has been rolled-back, we might be
		 * over-writing the rolledback write, so phdr->pd_lower can be
		 * different from addr.offset.
		 *
		 * We reset pd_lower to reset the rolledback write.
		 */
		if (phdr->pd_lower > addr.offset)
		{
			ereport(DEBUG1, (errmsg("over-writing page %u", addr.blockno),
							 errdetail("This can happen after a roll-back.")));
			phdr->pd_lower = addr.offset;
		}
		Assert(phdr->pd_lower == addr.offset);

		START_CRIT_SECTION();

		uint64 to_write = Min(phdr->pd_upper - phdr->pd_lower, remaining);
		memcpy_s(page + phdr->pd_lower, phdr->pd_upper - phdr->pd_lower, data, to_write);
		phdr->pd_lower += to_write;

		MarkBufferDirty(buffer);

		if (RelationNeedsWAL(rel))
		{
			XLogBeginInsert();

			/*
			 * Since cstore will mostly write whole pages we force the transmission of the
			 * whole image in the buffer
			 */
			XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE);

			XLogRecPtr recptr = XLogInsert(RM_GENERIC_ID, 0);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buffer);

		data += to_write;
		remaining -= to_write;
		logicalOffset += to_write;
	}
}


/*
 * FlushStripe flushes current stripe data into the file. The function first ensures
 * the last data chunk for each column is properly serialized and compressed. Then,
 * the function creates the skip list and footer buffers. Finally, the function
 * flushes the skip list, data, and footer buffers to the file.
 */
static void
FlushStripe(TableWriteState *writeState)
{
	StripeMetadata stripeMetadata = { 0 };
	uint32 columnIndex = 0;
	uint32 chunkIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	ColumnChunkSkipNode **columnSkipNodeArray = stripeSkipList->chunkSkipNodeArray;
	TupleDesc tupleDescriptor = writeState->tupleDescriptor;
	uint32 columnCount = tupleDescriptor->natts;
	uint32 chunkCount = stripeSkipList->chunkCount;
	uint32 chunkRowCount = writeState->options.chunkRowCount;
	uint32 lastChunkIndex = stripeBuffers->rowCount / chunkRowCount;
	uint32 lastChunkRowCount = stripeBuffers->rowCount % chunkRowCount;
	uint64 stripeSize = 0;
	uint64 stripeRowCount = 0;

	elog(DEBUG1, "Flushing Stripe of size %d", stripeBuffers->rowCount);

	Oid relationId = RelidByRelfilenode(writeState->relfilenode.spcNode,
										writeState->relfilenode.relNode);
	Relation relation = relation_open(relationId, NoLock);

	/*
	 * check if the last chunk needs serialization , the last chunk was not serialized
	 * if it was not full yet, e.g.  (rowCount > 0)
	 */
	if (lastChunkRowCount > 0)
	{
		SerializeChunkData(writeState, lastChunkIndex, lastChunkRowCount);
	}

	/* update buffer sizes in stripe skip list */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnChunkSkipNode *chunkSkipNodeArray = columnSkipNodeArray[columnIndex];
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];

		for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			uint64 existsBufferSize = chunkBuffers->existsBuffer->len;
			ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkIndex];

			chunkSkipNode->existsChunkOffset = stripeSize;
			chunkSkipNode->existsLength = existsBufferSize;
			stripeSize += existsBufferSize;
		}

		for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			uint64 valueBufferSize = chunkBuffers->valueBuffer->len;
			CompressionType valueCompressionType = chunkBuffers->valueCompressionType;
			ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkIndex];

			chunkSkipNode->valueChunkOffset = stripeSize;
			chunkSkipNode->valueLength = valueBufferSize;
			chunkSkipNode->valueCompressionType = valueCompressionType;
			chunkSkipNode->valueCompressionLevel = writeState->options.compressionLevel;
			chunkSkipNode->decompressedValueSize = chunkBuffers->decompressedValueSize;

			stripeSize += valueBufferSize;
		}
	}

	for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
	{
		stripeRowCount +=
			stripeSkipList->chunkSkipNodeArray[0][chunkIndex].rowCount;
	}

	stripeMetadata = ReserveStripe(relation, stripeSize,
								   stripeRowCount, columnCount, chunkCount,
								   chunkRowCount);

	uint64 currentFileOffset = stripeMetadata.fileOffset;

	/*
	 * Each stripe has only one section:
	 * Data section, in which we store data for each column continuously.
	 * We store data for each for each column in chunks. For each chunk, we
	 * store two buffers: "exists" buffer, and "value" buffer. "exists" buffer
	 * tells which values are not NULL. "value" buffer contains values for
	 * present values. For each column, we first store all "exists" buffers,
	 * and then all "value" buffers.
	 */

	/* flush the data buffers */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];

		for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			StringInfo existsBuffer = chunkBuffers->existsBuffer;

			WriteToSmgr(relation, currentFileOffset,
						existsBuffer->data, existsBuffer->len);
			currentFileOffset += existsBuffer->len;
		}

		for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			StringInfo valueBuffer = chunkBuffers->valueBuffer;

			WriteToSmgr(relation, currentFileOffset,
						valueBuffer->data, valueBuffer->len);
			currentFileOffset += valueBuffer->len;
		}
	}

	/* create skip list and footer buffers */
	SaveStripeSkipList(writeState->relfilenode,
					   stripeMetadata.id,
					   stripeSkipList, tupleDescriptor);

	relation_close(relation, NoLock);
}


/*
 * SerializeBoolArray serializes the given boolean array and returns the result
 * as a StringInfo. This function packs every 8 boolean values into one byte.
 */
static StringInfo
SerializeBoolArray(bool *boolArray, uint32 boolArrayLength)
{
	uint32 boolArrayIndex = 0;
	uint32 byteCount = (boolArrayLength + 7) / 8;

	StringInfo boolArrayBuffer = makeStringInfo();
	enlargeStringInfo(boolArrayBuffer, byteCount);
	boolArrayBuffer->len = byteCount;
	memset(boolArrayBuffer->data, 0, byteCount);

	for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++)
	{
		if (boolArray[boolArrayIndex])
		{
			uint32 byteIndex = boolArrayIndex / 8;
			uint32 bitIndex = boolArrayIndex % 8;
			boolArrayBuffer->data[byteIndex] |= (1 << bitIndex);
		}
	}

	return boolArrayBuffer;
}


/*
 * SerializeSingleDatum serializes the given datum value and appends it to the
 * provided string info buffer.
 */
static void
SerializeSingleDatum(StringInfo datumBuffer, Datum datum, bool datumTypeByValue,
					 int datumTypeLength, char datumTypeAlign)
{
	uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
	uint32 datumLengthAligned = att_align_nominal(datumLength, datumTypeAlign);

	enlargeStringInfo(datumBuffer, datumLengthAligned);

	char *currentDatumDataPointer = datumBuffer->data + datumBuffer->len;
	memset(currentDatumDataPointer, 0, datumLengthAligned);

	if (datumTypeLength > 0)
	{
		if (datumTypeByValue)
		{
			store_att_byval(currentDatumDataPointer, datum, datumTypeLength);
		}
		else
		{
			memcpy_s(currentDatumDataPointer, datumBuffer->maxlen - datumBuffer->len,
					 DatumGetPointer(datum), datumTypeLength);
		}
	}
	else
	{
		Assert(!datumTypeByValue);
		memcpy_s(currentDatumDataPointer, datumBuffer->maxlen - datumBuffer->len,
				 DatumGetPointer(datum), datumLength);
	}

	datumBuffer->len += datumLengthAligned;
}


/*
 * SerializeChunkData serializes and compresses chunk data at given chunk index with given
 * compression type for every column.
 */
static void
SerializeChunkData(TableWriteState *writeState, uint32 chunkIndex, uint32 rowCount)
{
	uint32 columnIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	ChunkData *chunkData = writeState->chunkData;
	CompressionType requestedCompressionType = writeState->options.compressionType;
	int compressionLevel = writeState->options.compressionLevel;
	const uint32 columnCount = stripeBuffers->columnCount;
	StringInfo compressionBuffer = writeState->compressionBuffer;

	/* serialize exist values, data values are already serialized */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		ColumnChunkBuffers *chunkBuffers = columnBuffers->chunkBuffersArray[chunkIndex];

		chunkBuffers->existsBuffer =
			SerializeBoolArray(chunkData->existsArray[columnIndex], rowCount);
	}

	/*
	 * check and compress value buffers, if a value buffer is not compressable
	 * then keep it as uncompressed, store compression information.
	 */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		ColumnChunkBuffers *chunkBuffers = columnBuffers->chunkBuffersArray[chunkIndex];
		CompressionType actualCompressionType = COMPRESSION_NONE;

		StringInfo serializedValueBuffer = chunkData->valueBufferArray[columnIndex];

		Assert(requestedCompressionType >= 0 &&
			   requestedCompressionType < COMPRESSION_COUNT);

		chunkBuffers->decompressedValueSize =
			chunkData->valueBufferArray[columnIndex]->len;

		/*
		 * if serializedValueBuffer is be compressed, update serializedValueBuffer
		 * with compressed data and store compression type.
		 */
		bool compressed = CompressBuffer(serializedValueBuffer, compressionBuffer,
										 requestedCompressionType,
										 compressionLevel);
		if (compressed)
		{
			serializedValueBuffer = compressionBuffer;
			actualCompressionType = requestedCompressionType;
		}

		/* store (compressed) value buffer */
		chunkBuffers->valueCompressionType = actualCompressionType;
		chunkBuffers->valueBuffer = CopyStringInfo(serializedValueBuffer);

		/* valueBuffer needs to be reset for next chunk's data */
		resetStringInfo(chunkData->valueBufferArray[columnIndex]);
	}
}


/*
 * UpdateChunkSkipNodeMinMax takes the given column value, and checks if this
 * value falls outside the range of minimum/maximum values of the given column
 * chunk skip node. If it does, the function updates the column chunk skip node
 * accordingly.
 */
static void
UpdateChunkSkipNodeMinMax(ColumnChunkSkipNode *chunkSkipNode, Datum columnValue,
						  bool columnTypeByValue, int columnTypeLength,
						  Oid columnCollation, FmgrInfo *comparisonFunction)
{
	bool hasMinMax = chunkSkipNode->hasMinMax;
	Datum previousMinimum = chunkSkipNode->minimumValue;
	Datum previousMaximum = chunkSkipNode->maximumValue;
	Datum currentMinimum = 0;
	Datum currentMaximum = 0;

	/* if type doesn't have a comparison function, skip min/max values */
	if (comparisonFunction == NULL)
	{
		return;
	}

	if (!hasMinMax)
	{
		currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
	}
	else
	{
		Datum minimumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMinimum);
		Datum maximumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMaximum);
		int minimumComparison = DatumGetInt32(minimumComparisonDatum);
		int maximumComparison = DatumGetInt32(maximumComparisonDatum);

		if (minimumComparison < 0)
		{
			currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMinimum = previousMinimum;
		}

		if (maximumComparison > 0)
		{
			currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMaximum = previousMaximum;
		}
	}

	chunkSkipNode->hasMinMax = true;
	chunkSkipNode->minimumValue = currentMinimum;
	chunkSkipNode->maximumValue = currentMaximum;
}


/* Creates a copy of the given datum. */
static Datum
DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength)
{
	Datum datumCopy = 0;

	if (datumTypeByValue)
	{
		datumCopy = datum;
	}
	else
	{
		uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
		char *datumData = palloc0(datumLength);
		memcpy_s(datumData, datumLength, DatumGetPointer(datum), datumLength);

		datumCopy = PointerGetDatum(datumData);
	}

	return datumCopy;
}


/*
 * CopyStringInfo creates a deep copy of given source string allocating only needed
 * amount of memory.
 */
static StringInfo
CopyStringInfo(StringInfo sourceString)
{
	StringInfo targetString = palloc0(sizeof(StringInfoData));

	if (sourceString->len > 0)
	{
		targetString->data = palloc0(sourceString->len);
		targetString->len = sourceString->len;
		targetString->maxlen = sourceString->len;
		memcpy_s(targetString->data, sourceString->len,
				 sourceString->data, sourceString->len);
	}

	return targetString;
}


bool
ContainsPendingWrites(TableWriteState *state)
{
	return state->stripeBuffers != NULL && state->stripeBuffers->rowCount != 0;
}
