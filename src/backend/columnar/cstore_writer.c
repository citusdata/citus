/*-------------------------------------------------------------------------
 *
 * cstore_writer.c
 *
 * This file contains function definitions for writing cstore files. This
 * includes the logic for writing file level metadata, writing row stripes,
 * and calculating block skip nodes.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "columnar/cstore.h"
#include "columnar/cstore_version_compat.h"

static StripeBuffers * CreateEmptyStripeBuffers(uint32 stripeMaxRowCount,
												uint32 blockRowCount,
												uint32 columnCount);
static StripeSkipList * CreateEmptyStripeSkipList(uint32 stripeMaxRowCount,
												  uint32 blockRowCount,
												  uint32 columnCount);
static void FlushStripe(TableWriteState *writeState);
static StringInfo SerializeBoolArray(bool *boolArray, uint32 boolArrayLength);
static void SerializeSingleDatum(StringInfo datumBuffer, Datum datum,
								 bool datumTypeByValue, int datumTypeLength,
								 char datumTypeAlign);
static void SerializeBlockData(TableWriteState *writeState, uint32 blockIndex,
							   uint32 rowCount);
static void UpdateBlockSkipNodeMinMax(ColumnBlockSkipNode *blockSkipNode,
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
CStoreBeginWrite(Relation relation,
				 CompressionType compressionType,
				 uint64 stripeMaxRowCount, uint32 blockRowCount,
				 TupleDesc tupleDescriptor)
{
	TableWriteState *writeState = NULL;
	FmgrInfo **comparisonFunctionArray = NULL;
	MemoryContext stripeWriteContext = NULL;
	uint32 columnCount = 0;
	uint32 columnIndex = 0;
	bool *columnMaskArray = NULL;
	BlockData *blockData = NULL;

	/* get comparison function pointers for each of the columns */
	columnCount = tupleDescriptor->natts;
	comparisonFunctionArray = palloc0(columnCount * sizeof(FmgrInfo *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
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
	stripeWriteContext = AllocSetContextCreate(CurrentMemoryContext,
											   "Stripe Write Memory Context",
											   ALLOCSET_DEFAULT_SIZES);

	columnMaskArray = palloc(columnCount * sizeof(bool));
	memset(columnMaskArray, true, columnCount);

	blockData = CreateEmptyBlockData(columnCount, columnMaskArray, blockRowCount);

	writeState = palloc0(sizeof(TableWriteState));
	writeState->relation = relation;
	writeState->compressionType = compressionType;
	writeState->stripeMaxRowCount = stripeMaxRowCount;
	writeState->blockRowCount = blockRowCount;
	writeState->tupleDescriptor = tupleDescriptor;
	writeState->comparisonFunctionArray = comparisonFunctionArray;
	writeState->stripeBuffers = NULL;
	writeState->stripeSkipList = NULL;
	writeState->stripeWriteContext = stripeWriteContext;
	writeState->blockData = blockData;
	writeState->compressionBuffer = NULL;

	return writeState;
}


/*
 * CStoreWriteRow adds a row to the cstore file. If the stripe is not initialized,
 * we create structures to hold stripe data and skip list. Then, we serialize and
 * append data to serialized value buffer for each of the columns and update
 * corresponding skip nodes. Then, whole block data is compressed at every
 * rowBlockCount insertion. Then, if row count exceeds stripeMaxRowCount, we flush
 * the stripe, and add its metadata to the table footer.
 */
void
CStoreWriteRow(TableWriteState *writeState, Datum *columnValues, bool *columnNulls)
{
	uint32 columnIndex = 0;
	uint32 blockIndex = 0;
	uint32 blockRowIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	uint32 columnCount = writeState->tupleDescriptor->natts;
	const uint32 blockRowCount = writeState->blockRowCount;
	BlockData *blockData = writeState->blockData;
	MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

	if (stripeBuffers == NULL)
	{
		stripeBuffers = CreateEmptyStripeBuffers(writeState->stripeMaxRowCount,
												 blockRowCount, columnCount);
		stripeSkipList = CreateEmptyStripeSkipList(writeState->stripeMaxRowCount,
												   blockRowCount, columnCount);
		writeState->stripeBuffers = stripeBuffers;
		writeState->stripeSkipList = stripeSkipList;
		writeState->compressionBuffer = makeStringInfo();

		/*
		 * serializedValueBuffer lives in stripe write memory context so it needs to be
		 * initialized when the stripe is created.
		 */
		for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{
			blockData->valueBufferArray[columnIndex] = makeStringInfo();
		}
	}

	blockIndex = stripeBuffers->rowCount / blockRowCount;
	blockRowIndex = stripeBuffers->rowCount % blockRowCount;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockSkipNode **blockSkipNodeArray = stripeSkipList->blockSkipNodeArray;
		ColumnBlockSkipNode *blockSkipNode =
			&blockSkipNodeArray[columnIndex][blockIndex];

		if (columnNulls[columnIndex])
		{
			blockData->existsArray[columnIndex][blockRowIndex] = false;
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

			blockData->existsArray[columnIndex][blockRowIndex] = true;

			SerializeSingleDatum(blockData->valueBufferArray[columnIndex],
								 columnValues[columnIndex], columnTypeByValue,
								 columnTypeLength, columnTypeAlign);

			UpdateBlockSkipNodeMinMax(blockSkipNode, columnValues[columnIndex],
									  columnTypeByValue, columnTypeLength,
									  columnCollation, comparisonFunction);
		}

		blockSkipNode->rowCount++;
	}

	stripeSkipList->blockCount = blockIndex + 1;

	/* last row of the block is inserted serialize the block */
	if (blockRowIndex == blockRowCount - 1)
	{
		SerializeBlockData(writeState, blockIndex, blockRowCount);
	}

	stripeBuffers->rowCount++;
	if (stripeBuffers->rowCount >= writeState->stripeMaxRowCount)
	{
		FlushStripe(writeState);

		/* set stripe data and skip list to NULL so they are recreated next time */
		writeState->stripeBuffers = NULL;
		writeState->stripeSkipList = NULL;
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
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;

	if (stripeBuffers != NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

		FlushStripe(writeState);
		MemoryContextReset(writeState->stripeWriteContext);

		MemoryContextSwitchTo(oldContext);
	}

	MemoryContextDelete(writeState->stripeWriteContext);
	pfree(writeState->comparisonFunctionArray);
	FreeBlockData(writeState->blockData);
	pfree(writeState);
}


/*
 * CreateEmptyStripeBuffers allocates an empty StripeBuffers structure with the given
 * column count.
 */
static StripeBuffers *
CreateEmptyStripeBuffers(uint32 stripeMaxRowCount, uint32 blockRowCount,
						 uint32 columnCount)
{
	StripeBuffers *stripeBuffers = NULL;
	uint32 columnIndex = 0;
	uint32 maxBlockCount = (stripeMaxRowCount / blockRowCount) + 1;
	ColumnBuffers **columnBuffersArray = palloc0(columnCount * sizeof(ColumnBuffers *));

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 blockIndex = 0;
		ColumnBlockBuffers **blockBuffersArray =
			palloc0(maxBlockCount * sizeof(ColumnBlockBuffers *));

		for (blockIndex = 0; blockIndex < maxBlockCount; blockIndex++)
		{
			blockBuffersArray[blockIndex] = palloc0(sizeof(ColumnBlockBuffers));
			blockBuffersArray[blockIndex]->existsBuffer = NULL;
			blockBuffersArray[blockIndex]->valueBuffer = NULL;
			blockBuffersArray[blockIndex]->valueCompressionType = COMPRESSION_NONE;
		}

		columnBuffersArray[columnIndex] = palloc0(sizeof(ColumnBuffers));
		columnBuffersArray[columnIndex]->blockBuffersArray = blockBuffersArray;
	}

	stripeBuffers = palloc0(sizeof(StripeBuffers));
	stripeBuffers->columnBuffersArray = columnBuffersArray;
	stripeBuffers->columnCount = columnCount;
	stripeBuffers->rowCount = 0;

	return stripeBuffers;
}


/*
 * CreateEmptyStripeSkipList allocates an empty StripeSkipList structure with
 * the given column count. This structure has enough blocks to hold statistics
 * for stripeMaxRowCount rows.
 */
static StripeSkipList *
CreateEmptyStripeSkipList(uint32 stripeMaxRowCount, uint32 blockRowCount,
						  uint32 columnCount)
{
	StripeSkipList *stripeSkipList = NULL;
	uint32 columnIndex = 0;
	uint32 maxBlockCount = (stripeMaxRowCount / blockRowCount) + 1;

	ColumnBlockSkipNode **blockSkipNodeArray =
		palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		blockSkipNodeArray[columnIndex] =
			palloc0(maxBlockCount * sizeof(ColumnBlockSkipNode));
	}

	stripeSkipList = palloc0(sizeof(StripeSkipList));
	stripeSkipList->columnCount = columnCount;
	stripeSkipList->blockCount = 0;
	stripeSkipList->blockSkipNodeArray = blockSkipNodeArray;

	return stripeSkipList;
}


static void
WriteToSmgr(Relation rel, uint64 logicalOffset, char *data, uint32 dataLength)
{
	uint64 remaining = dataLength;
	Buffer buffer;

	while (remaining > 0)
	{
		SmgrAddr addr = logical_to_smgr(logicalOffset);
		BlockNumber nblocks;
		Page page;
		PageHeader phdr;
		uint64 to_write;

		RelationOpenSmgr(rel);
		nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
		Assert(addr.blockno < nblocks);
		(void) nblocks; /* keep compiler quiet */
		RelationCloseSmgr(rel);

		buffer = ReadBuffer(rel, addr.blockno);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

		page = BufferGetPage(buffer);
		phdr = (PageHeader) page;
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

		to_write = Min(phdr->pd_upper - phdr->pd_lower, remaining);
		memcpy(page + phdr->pd_lower, data, to_write);
		phdr->pd_lower += to_write;

		MarkBufferDirty(buffer);

		if (RelationNeedsWAL(rel))
		{
			XLogRecPtr recptr = 0;

			XLogBeginInsert();

			/*
			 * Since cstore will mostly write whole pages we force the transmission of the
			 * whole image in the buffer
			 */
			XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE);

			recptr = XLogInsert(RM_GENERIC_ID, 0);
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
 * the last data block for each column is properly serialized and compressed. Then,
 * the function creates the skip list and footer buffers. Finally, the function
 * flushes the skip list, data, and footer buffers to the file.
 */
static void
FlushStripe(TableWriteState *writeState)
{
	StripeMetadata stripeMetadata = { 0 };
	uint32 columnIndex = 0;
	uint32 blockIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	ColumnBlockSkipNode **columnSkipNodeArray = stripeSkipList->blockSkipNodeArray;
	TupleDesc tupleDescriptor = writeState->tupleDescriptor;
	uint32 columnCount = tupleDescriptor->natts;
	uint32 blockCount = stripeSkipList->blockCount;
	uint32 blockRowCount = writeState->blockRowCount;
	uint32 lastBlockIndex = stripeBuffers->rowCount / blockRowCount;
	uint32 lastBlockRowCount = stripeBuffers->rowCount % blockRowCount;
	uint64 currentFileOffset = 0;
	uint64 stripeSize = 0;
	uint64 stripeRowCount = 0;

	/*
	 * check if the last block needs serialization , the last block was not serialized
	 * if it was not full yet, e.g.  (rowCount > 0)
	 */
	if (lastBlockRowCount > 0)
	{
		SerializeBlockData(writeState, lastBlockIndex, lastBlockRowCount);
	}

	/* update buffer sizes in stripe skip list */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockSkipNode *blockSkipNodeArray = columnSkipNodeArray[columnIndex];
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];

		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			ColumnBlockBuffers *blockBuffers =
				columnBuffers->blockBuffersArray[blockIndex];
			uint64 existsBufferSize = blockBuffers->existsBuffer->len;
			ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];

			blockSkipNode->existsBlockOffset = stripeSize;
			blockSkipNode->existsLength = existsBufferSize;
			stripeSize += existsBufferSize;
		}

		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			ColumnBlockBuffers *blockBuffers =
				columnBuffers->blockBuffersArray[blockIndex];
			uint64 valueBufferSize = blockBuffers->valueBuffer->len;
			CompressionType valueCompressionType = blockBuffers->valueCompressionType;
			ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];

			blockSkipNode->valueBlockOffset = stripeSize;
			blockSkipNode->valueLength = valueBufferSize;
			blockSkipNode->valueCompressionType = valueCompressionType;

			stripeSize += valueBufferSize;
		}
	}

	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		stripeRowCount +=
			stripeSkipList->blockSkipNodeArray[0][blockIndex].rowCount;
	}

	stripeMetadata = ReserveStripe(writeState->relation, stripeSize,
								   stripeRowCount, columnCount, blockCount,
								   blockRowCount);

	currentFileOffset = stripeMetadata.fileOffset;

	/*
	 * Each stripe has only one section:
	 * Data section, in which we store data for each column continuously.
	 * We store data for each for each column in blocks. For each block, we
	 * store two buffers: "exists" buffer, and "value" buffer. "exists" buffer
	 * tells which values are not NULL. "value" buffer contains values for
	 * present values. For each column, we first store all "exists" buffers,
	 * and then all "value" buffers.
	 */

	/* flush the data buffers */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			ColumnBlockBuffers *blockBuffers =
				columnBuffers->blockBuffersArray[blockIndex];
			StringInfo existsBuffer = blockBuffers->existsBuffer;

			WriteToSmgr(writeState->relation, currentFileOffset,
						existsBuffer->data, existsBuffer->len);
			currentFileOffset += existsBuffer->len;
		}

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			ColumnBlockBuffers *blockBuffers =
				columnBuffers->blockBuffersArray[blockIndex];
			StringInfo valueBuffer = blockBuffers->valueBuffer;

			WriteToSmgr(writeState->relation, currentFileOffset,
						valueBuffer->data, valueBuffer->len);
			currentFileOffset += valueBuffer->len;
		}
	}

	/* create skip list and footer buffers */
	SaveStripeSkipList(writeState->relation->rd_node.relNode,
					   stripeMetadata.id,
					   stripeSkipList, tupleDescriptor);
}


/*
 * SerializeBoolArray serializes the given boolean array and returns the result
 * as a StringInfo. This function packs every 8 boolean values into one byte.
 */
static StringInfo
SerializeBoolArray(bool *boolArray, uint32 boolArrayLength)
{
	StringInfo boolArrayBuffer = NULL;
	uint32 boolArrayIndex = 0;
	uint32 byteCount = (boolArrayLength + 7) / 8;

	boolArrayBuffer = makeStringInfo();
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
	char *currentDatumDataPointer = NULL;

	enlargeStringInfo(datumBuffer, datumLengthAligned);

	currentDatumDataPointer = datumBuffer->data + datumBuffer->len;
	memset(currentDatumDataPointer, 0, datumLengthAligned);

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


/*
 * SerializeBlockData serializes and compresses block data at given block index with given
 * compression type for every column.
 */
static void
SerializeBlockData(TableWriteState *writeState, uint32 blockIndex, uint32 rowCount)
{
	uint32 columnIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	BlockData *blockData = writeState->blockData;
	CompressionType requestedCompressionType = writeState->compressionType;
	const uint32 columnCount = stripeBuffers->columnCount;
	StringInfo compressionBuffer = writeState->compressionBuffer;

	/* serialize exist values, data values are already serialized */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		ColumnBlockBuffers *blockBuffers = columnBuffers->blockBuffersArray[blockIndex];

		blockBuffers->existsBuffer =
			SerializeBoolArray(blockData->existsArray[columnIndex], rowCount);
	}

	/*
	 * check and compress value buffers, if a value buffer is not compressable
	 * then keep it as uncompressed, store compression information.
	 */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		ColumnBlockBuffers *blockBuffers = columnBuffers->blockBuffersArray[blockIndex];
		StringInfo serializedValueBuffer = NULL;
		CompressionType actualCompressionType = COMPRESSION_NONE;
		bool compressed = false;

		serializedValueBuffer = blockData->valueBufferArray[columnIndex];

		/* the only other supported compression type is pg_lz for now */
		Assert(requestedCompressionType == COMPRESSION_NONE ||
			   requestedCompressionType == COMPRESSION_PG_LZ);

		/*
		 * if serializedValueBuffer is be compressed, update serializedValueBuffer
		 * with compressed data and store compression type.
		 */
		compressed = CompressBuffer(serializedValueBuffer, compressionBuffer,
									requestedCompressionType);
		if (compressed)
		{
			serializedValueBuffer = compressionBuffer;
			actualCompressionType = COMPRESSION_PG_LZ;
		}

		/* store (compressed) value buffer */
		blockBuffers->valueCompressionType = actualCompressionType;
		blockBuffers->valueBuffer = CopyStringInfo(serializedValueBuffer);

		/* valueBuffer needs to be reset for next block's data */
		resetStringInfo(blockData->valueBufferArray[columnIndex]);
	}
}


/*
 * UpdateBlockSkipNodeMinMax takes the given column value, and checks if this
 * value falls outside the range of minimum/maximum values of the given column
 * block skip node. If it does, the function updates the column block skip node
 * accordingly.
 */
static void
UpdateBlockSkipNodeMinMax(ColumnBlockSkipNode *blockSkipNode, Datum columnValue,
						  bool columnTypeByValue, int columnTypeLength,
						  Oid columnCollation, FmgrInfo *comparisonFunction)
{
	bool hasMinMax = blockSkipNode->hasMinMax;
	Datum previousMinimum = blockSkipNode->minimumValue;
	Datum previousMaximum = blockSkipNode->maximumValue;
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

	blockSkipNode->hasMinMax = true;
	blockSkipNode->minimumValue = currentMinimum;
	blockSkipNode->maximumValue = currentMaximum;
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
		memcpy(datumData, DatumGetPointer(datum), datumLength);

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
		memcpy(targetString->data, sourceString->data, sourceString->len);
	}

	return targetString;
}
