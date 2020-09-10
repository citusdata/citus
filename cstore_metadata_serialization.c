/*-------------------------------------------------------------------------
 *
 * cstore_metadata_serialization.c
 *
 * This file contains function definitions for serializing/deserializing cstore
 * metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/tupmacs.h"

#include "cstore.h"
#include "cstore_metadata_serialization.h"
#include "cstore.pb-c.h"

/* local functions forward declarations */
static ProtobufCBinaryData DatumToProtobufBinary(Datum datum, bool typeByValue,
												 int typeLength);
static Datum ProtobufBinaryToDatum(ProtobufCBinaryData protobufBinary,
								   bool typeByValue, int typeLength);


/*
 * SerializeColumnSkipList serializes a column skip list, where the colum skip
 * list includes all block skip nodes for that column. The function then returns
 * the result as a string info.
 */
StringInfo
SerializeColumnSkipList(ColumnBlockSkipNode *blockSkipNodeArray, uint32 blockCount,
						bool typeByValue, int typeLength)
{
	StringInfo blockSkipListBuffer = NULL;
	Protobuf__ColumnBlockSkipList protobufBlockSkipList =
		PROTOBUF__COLUMN_BLOCK_SKIP_LIST__INIT;
	Protobuf__ColumnBlockSkipNode **protobufBlockSkipNodeArray = NULL;
	uint32 blockIndex = 0;
	uint8 *blockSkipListData = NULL;
	uint32 blockSkipListSize = 0;

	protobufBlockSkipNodeArray = palloc0(blockCount *
										 sizeof(Protobuf__ColumnBlockSkipNode *));
	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockSkipNode blockSkipNode = blockSkipNodeArray[blockIndex];
		Protobuf__ColumnBlockSkipNode *protobufBlockSkipNode = NULL;
		ProtobufCBinaryData binaryMinimumValue = { 0, 0 };
		ProtobufCBinaryData binaryMaximumValue = { 0, 0 };

		if (blockSkipNode.hasMinMax)
		{
			binaryMinimumValue = DatumToProtobufBinary(blockSkipNode.minimumValue,
													   typeByValue, typeLength);
			binaryMaximumValue = DatumToProtobufBinary(blockSkipNode.maximumValue,
													   typeByValue, typeLength);
		}

		protobufBlockSkipNode = palloc0(sizeof(Protobuf__ColumnBlockSkipNode));
		protobuf__column_block_skip_node__init(protobufBlockSkipNode);
		protobufBlockSkipNode->has_rowcount = true;
		protobufBlockSkipNode->rowcount = blockSkipNode.rowCount;
		protobufBlockSkipNode->has_minimumvalue = blockSkipNode.hasMinMax;
		protobufBlockSkipNode->minimumvalue = binaryMinimumValue;
		protobufBlockSkipNode->has_maximumvalue = blockSkipNode.hasMinMax;
		protobufBlockSkipNode->maximumvalue = binaryMaximumValue;
		protobufBlockSkipNode->has_valueblockoffset = true;
		protobufBlockSkipNode->valueblockoffset = blockSkipNode.valueBlockOffset;
		protobufBlockSkipNode->has_valuelength = true;
		protobufBlockSkipNode->valuelength = blockSkipNode.valueLength;
		protobufBlockSkipNode->has_existsblockoffset = true;
		protobufBlockSkipNode->existsblockoffset = blockSkipNode.existsBlockOffset;
		protobufBlockSkipNode->has_existslength = true;
		protobufBlockSkipNode->existslength = blockSkipNode.existsLength;
		protobufBlockSkipNode->has_valuecompressiontype = true;
		protobufBlockSkipNode->valuecompressiontype =
			(Protobuf__CompressionType) blockSkipNode.valueCompressionType;

		protobufBlockSkipNodeArray[blockIndex] = protobufBlockSkipNode;
	}

	protobufBlockSkipList.n_blockskipnodearray = blockCount;
	protobufBlockSkipList.blockskipnodearray = protobufBlockSkipNodeArray;

	blockSkipListSize =
		protobuf__column_block_skip_list__get_packed_size(&protobufBlockSkipList);
	blockSkipListData = palloc0(blockSkipListSize);
	protobuf__column_block_skip_list__pack(&protobufBlockSkipList, blockSkipListData);

	blockSkipListBuffer = palloc0(sizeof(StringInfoData));
	blockSkipListBuffer->len = blockSkipListSize;
	blockSkipListBuffer->maxlen = blockSkipListSize;
	blockSkipListBuffer->data = (char *) blockSkipListData;

	return blockSkipListBuffer;
}


/*
 * DeserializeBlockCount deserializes the given column skip list buffer and
 * returns the number of blocks in column skip list.
 */
uint32
DeserializeBlockCount(StringInfo buffer)
{
	uint32 blockCount = 0;
	Protobuf__ColumnBlockSkipList *protobufBlockSkipList = NULL;

	protobufBlockSkipList =
		protobuf__column_block_skip_list__unpack(NULL, buffer->len,
												 (uint8 *) buffer->data);
	if (protobufBlockSkipList == NULL)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid skip list buffer")));
	}

	blockCount = protobufBlockSkipList->n_blockskipnodearray;

	protobuf__column_block_skip_list__free_unpacked(protobufBlockSkipList, NULL);

	return blockCount;
}


/*
 * DeserializeRowCount deserializes the given column skip list buffer and
 * returns the total number of rows in block skip list.
 */
uint32
DeserializeRowCount(StringInfo buffer)
{
	uint32 rowCount = 0;
	Protobuf__ColumnBlockSkipList *protobufBlockSkipList = NULL;
	uint32 blockIndex = 0;
	uint32 blockCount = 0;

	protobufBlockSkipList =
		protobuf__column_block_skip_list__unpack(NULL, buffer->len,
												 (uint8 *) buffer->data);
	if (protobufBlockSkipList == NULL)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid skip list buffer")));
	}

	blockCount = (uint32) protobufBlockSkipList->n_blockskipnodearray;
	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		Protobuf__ColumnBlockSkipNode *protobufBlockSkipNode =
			protobufBlockSkipList->blockskipnodearray[blockIndex];
		rowCount += protobufBlockSkipNode->rowcount;
	}

	protobuf__column_block_skip_list__free_unpacked(protobufBlockSkipList, NULL);

	return rowCount;
}


/*
 * DeserializeColumnSkipList deserializes the given buffer and returns the result as
 * a ColumnBlockSkipNode array. If the number of unpacked block skip nodes are not
 * equal to the given block count function errors out.
 */
ColumnBlockSkipNode *
DeserializeColumnSkipList(StringInfo buffer, bool typeByValue, int typeLength,
						  uint32 blockCount)
{
	ColumnBlockSkipNode *blockSkipNodeArray = NULL;
	uint32 blockIndex = 0;
	Protobuf__ColumnBlockSkipList *protobufBlockSkipList = NULL;

	protobufBlockSkipList =
		protobuf__column_block_skip_list__unpack(NULL, buffer->len,
												 (uint8 *) buffer->data);
	if (protobufBlockSkipList == NULL)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid skip list buffer")));
	}

	if (protobufBlockSkipList->n_blockskipnodearray != blockCount)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("block skip node count and block count don't match")));
	}

	blockSkipNodeArray = palloc0(blockCount * sizeof(ColumnBlockSkipNode));

	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		Protobuf__ColumnBlockSkipNode *protobufBlockSkipNode = NULL;
		ColumnBlockSkipNode *blockSkipNode = NULL;
		bool hasMinMax = false;
		Datum minimumValue = 0;
		Datum maximumValue = 0;

		protobufBlockSkipNode = protobufBlockSkipList->blockskipnodearray[blockIndex];
		if (!protobufBlockSkipNode->has_rowcount ||
			!protobufBlockSkipNode->has_existsblockoffset ||
			!protobufBlockSkipNode->has_valueblockoffset ||
			!protobufBlockSkipNode->has_existslength ||
			!protobufBlockSkipNode->has_valuelength ||
			!protobufBlockSkipNode->has_valuecompressiontype)
		{
			ereport(ERROR, (errmsg("could not unpack column store"),
							errdetail("missing required block skip node metadata")));
		}

		if (protobufBlockSkipNode->has_minimumvalue !=
			protobufBlockSkipNode->has_maximumvalue)
		{
			ereport(ERROR, (errmsg("could not unpack column store"),
							errdetail("has minimum and has maximum fields "
									  "don't match")));
		}

		hasMinMax = protobufBlockSkipNode->has_minimumvalue;
		if (hasMinMax)
		{
			minimumValue = ProtobufBinaryToDatum(protobufBlockSkipNode->minimumvalue,
												 typeByValue, typeLength);
			maximumValue = ProtobufBinaryToDatum(protobufBlockSkipNode->maximumvalue,
												 typeByValue, typeLength);
		}

		blockSkipNode = &blockSkipNodeArray[blockIndex];
		blockSkipNode->rowCount = protobufBlockSkipNode->rowcount;
		blockSkipNode->hasMinMax = hasMinMax;
		blockSkipNode->minimumValue = minimumValue;
		blockSkipNode->maximumValue = maximumValue;
		blockSkipNode->existsBlockOffset = protobufBlockSkipNode->existsblockoffset;
		blockSkipNode->valueBlockOffset = protobufBlockSkipNode->valueblockoffset;
		blockSkipNode->existsLength = protobufBlockSkipNode->existslength;
		blockSkipNode->valueLength = protobufBlockSkipNode->valuelength;
		blockSkipNode->valueCompressionType =
			(CompressionType) protobufBlockSkipNode->valuecompressiontype;
	}

	protobuf__column_block_skip_list__free_unpacked(protobufBlockSkipList, NULL);

	return blockSkipNodeArray;
}


/* Converts a datum to a ProtobufCBinaryData. */
static ProtobufCBinaryData
DatumToProtobufBinary(Datum datum, bool datumTypeByValue, int datumTypeLength)
{
	ProtobufCBinaryData protobufBinary = { 0, 0 };

	int datumLength = att_addlength_datum(0, datumTypeLength, datum);
	char *datumBuffer = palloc0(datumLength);

	if (datumTypeLength > 0)
	{
		if (datumTypeByValue)
		{
			store_att_byval(datumBuffer, datum, datumTypeLength);
		}
		else
		{
			memcpy(datumBuffer, DatumGetPointer(datum), datumTypeLength);
		}
	}
	else
	{
		memcpy(datumBuffer, DatumGetPointer(datum), datumLength);
	}

	protobufBinary.data = (uint8 *) datumBuffer;
	protobufBinary.len = datumLength;

	return protobufBinary;
}


/* Converts the given ProtobufCBinaryData to a Datum. */
static Datum
ProtobufBinaryToDatum(ProtobufCBinaryData protobufBinary, bool datumTypeByValue,
					  int datumTypeLength)
{
	Datum datum = 0;

	/*
	 * We copy the protobuf data so the result of this function lives even
	 * after the unpacked protobuf struct is freed.
	 */
	char *binaryDataCopy = palloc0(protobufBinary.len);
	memcpy(binaryDataCopy, protobufBinary.data, protobufBinary.len);

	datum = fetch_att(binaryDataCopy, datumTypeByValue, datumTypeLength);

	return datum;
}
