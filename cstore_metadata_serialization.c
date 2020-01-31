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
#include "cstore_fdw.h"
#include "cstore_metadata_serialization.h"
#include "cstore.pb-c.h"
#include "access/tupmacs.h"


/* local functions forward declarations */
static ProtobufCBinaryData DatumToProtobufBinary(Datum datum, bool typeByValue,
												 int typeLength);
static Datum ProtobufBinaryToDatum(ProtobufCBinaryData protobufBinary,
								   bool typeByValue, int typeLength);


/*
 * SerializePostScript serializes the given postscript and returns the result as
 * a StringInfo.
 */
StringInfo
SerializePostScript(uint64 tableFooterLength)
{
	StringInfo postscriptBuffer = NULL;
	Protobuf__PostScript protobufPostScript = PROTOBUF__POST_SCRIPT__INIT;
	uint8 *postscriptData = NULL;
	uint32 postscriptSize = 0;

	protobufPostScript.has_tablefooterlength = true;
	protobufPostScript.tablefooterlength = tableFooterLength;
	protobufPostScript.has_versionmajor = true;
	protobufPostScript.versionmajor = CSTORE_VERSION_MAJOR;
	protobufPostScript.has_versionminor = true;
	protobufPostScript.versionminor = CSTORE_VERSION_MINOR;
	protobufPostScript.magicnumber = pstrdup(CSTORE_MAGIC_NUMBER);

	postscriptSize = protobuf__post_script__get_packed_size(&protobufPostScript);
	postscriptData = palloc0(postscriptSize);
	protobuf__post_script__pack(&protobufPostScript, postscriptData);

	postscriptBuffer = palloc0(sizeof(StringInfoData));
	postscriptBuffer->len = postscriptSize;
	postscriptBuffer->maxlen = postscriptSize;
	postscriptBuffer->data = (char *) postscriptData;

	return postscriptBuffer;
}


/*
 * SerializeTableFooter serializes the given table footer and returns the result
 * as a StringInfo.
 */
StringInfo
SerializeTableFooter(TableFooter *tableFooter)
{
	StringInfo tableFooterBuffer = NULL;
	Protobuf__TableFooter protobufTableFooter = PROTOBUF__TABLE_FOOTER__INIT;
	Protobuf__StripeMetadata **stripeMetadataArray = NULL;
	ListCell *stripeMetadataCell = NULL;
	uint8 *tableFooterData = NULL;
	uint32 tableFooterSize = 0;
	uint32 stripeIndex = 0;

	List *stripeMetadataList = tableFooter->stripeMetadataList;
	uint32 stripeCount = list_length(stripeMetadataList);
	stripeMetadataArray = palloc0(stripeCount * sizeof(Protobuf__StripeMetadata *));

	foreach(stripeMetadataCell, stripeMetadataList)
	{
		StripeMetadata *stripeMetadata = lfirst(stripeMetadataCell);

		Protobuf__StripeMetadata *protobufStripeMetadata = NULL;
		protobufStripeMetadata = palloc0(sizeof(Protobuf__StripeMetadata));
		protobuf__stripe_metadata__init(protobufStripeMetadata);
		protobufStripeMetadata->has_fileoffset = true;
		protobufStripeMetadata->fileoffset = stripeMetadata->fileOffset;
		protobufStripeMetadata->has_skiplistlength = true;
		protobufStripeMetadata->skiplistlength = stripeMetadata->skipListLength;
		protobufStripeMetadata->has_datalength = true;
		protobufStripeMetadata->datalength = stripeMetadata->dataLength;
		protobufStripeMetadata->has_footerlength = true;
		protobufStripeMetadata->footerlength = stripeMetadata->footerLength;

		stripeMetadataArray[stripeIndex] = protobufStripeMetadata;
		stripeIndex++;
	}

	protobufTableFooter.n_stripemetadataarray = stripeCount;
	protobufTableFooter.stripemetadataarray = stripeMetadataArray;
	protobufTableFooter.has_blockrowcount = true;
	protobufTableFooter.blockrowcount = tableFooter->blockRowCount;

	tableFooterSize = protobuf__table_footer__get_packed_size(&protobufTableFooter);
	tableFooterData = palloc0(tableFooterSize);
	protobuf__table_footer__pack(&protobufTableFooter, tableFooterData);

	tableFooterBuffer = palloc0(sizeof(StringInfoData));
	tableFooterBuffer->len = tableFooterSize;
	tableFooterBuffer->maxlen = tableFooterSize;
	tableFooterBuffer->data = (char *) tableFooterData;

	return tableFooterBuffer;
}


/*
 * SerializeStripeFooter serializes given stripe footer and returns the result
 * as a StringInfo.
 */
StringInfo
SerializeStripeFooter(StripeFooter *stripeFooter)
{
	StringInfo stripeFooterBuffer = NULL;
	Protobuf__StripeFooter protobufStripeFooter = PROTOBUF__STRIPE_FOOTER__INIT;
	uint8 *stripeFooterData = NULL;
	uint32 stripeFooterSize = 0;

	protobufStripeFooter.n_skiplistsizearray = stripeFooter->columnCount;
	protobufStripeFooter.skiplistsizearray = (uint64_t *) stripeFooter->skipListSizeArray;
	protobufStripeFooter.n_existssizearray = stripeFooter->columnCount;
	protobufStripeFooter.existssizearray = (uint64_t *) stripeFooter->existsSizeArray;
	protobufStripeFooter.n_valuesizearray = stripeFooter->columnCount;
	protobufStripeFooter.valuesizearray = (uint64_t *) stripeFooter->valueSizeArray;

	stripeFooterSize = protobuf__stripe_footer__get_packed_size(&protobufStripeFooter);
	stripeFooterData = palloc0(stripeFooterSize);
	protobuf__stripe_footer__pack(&protobufStripeFooter, stripeFooterData);

	stripeFooterBuffer = palloc0(sizeof(StringInfoData));
	stripeFooterBuffer->len = stripeFooterSize;
	stripeFooterBuffer->maxlen = stripeFooterSize;
	stripeFooterBuffer->data = (char *) stripeFooterData;

	return stripeFooterBuffer;
}


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
		ProtobufCBinaryData binaryMinimumValue = {0, 0};
		ProtobufCBinaryData binaryMaximumValue = {0, 0};

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
 * DeserializePostScript deserializes the given postscript buffer and returns
 * the size of table footer in tableFooterLength pointer.
 */
void
DeserializePostScript(StringInfo buffer, uint64 *tableFooterLength)
{
	Protobuf__PostScript *protobufPostScript = NULL;
	protobufPostScript = protobuf__post_script__unpack(NULL, buffer->len,
													   (uint8 *) buffer->data);
	if (protobufPostScript == NULL)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid postscript buffer")));
	}

	if (protobufPostScript->versionmajor != CSTORE_VERSION_MAJOR ||
		protobufPostScript->versionminor > CSTORE_VERSION_MINOR)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid column store version number")));
	}
	else if (strncmp(protobufPostScript->magicnumber, CSTORE_MAGIC_NUMBER,
					 NAMEDATALEN) != 0)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid magic number")));
	}

	(*tableFooterLength) = protobufPostScript->tablefooterlength;

	protobuf__post_script__free_unpacked(protobufPostScript, NULL);
}


/*
 * DeserializeTableFooter deserializes the given buffer and returns the result as
 * a TableFooter struct.
 */
TableFooter *
DeserializeTableFooter(StringInfo buffer)
{
	TableFooter *tableFooter = NULL;
	Protobuf__TableFooter *protobufTableFooter = NULL;
	List *stripeMetadataList = NIL;
	uint64 blockRowCount = 0;
	uint32 stripeCount = 0;
	uint32 stripeIndex = 0;

	protobufTableFooter = protobuf__table_footer__unpack(NULL, buffer->len,
														 (uint8 *) buffer->data);
	if (protobufTableFooter == NULL)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid table footer buffer")));
	}

	if (!protobufTableFooter->has_blockrowcount)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("missing required table footer metadata fields")));
	}
	else if (protobufTableFooter->blockrowcount < BLOCK_ROW_COUNT_MINIMUM ||
			 protobufTableFooter->blockrowcount > BLOCK_ROW_COUNT_MAXIMUM)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid block row count")));
	}
	blockRowCount = protobufTableFooter->blockrowcount;

	stripeCount = protobufTableFooter->n_stripemetadataarray;
	for (stripeIndex = 0; stripeIndex < stripeCount; stripeIndex++)
	{
		StripeMetadata *stripeMetadata = NULL;
		Protobuf__StripeMetadata *protobufStripeMetadata = NULL;

		protobufStripeMetadata = protobufTableFooter->stripemetadataarray[stripeIndex];
		if (!protobufStripeMetadata->has_fileoffset ||
			!protobufStripeMetadata->has_skiplistlength ||
			!protobufStripeMetadata->has_datalength ||
			!protobufStripeMetadata->has_footerlength)
		{
			ereport(ERROR, (errmsg("could not unpack column store"),
							errdetail("missing required stripe metadata fields")));
		}

		stripeMetadata = palloc0(sizeof(StripeMetadata));
		stripeMetadata->fileOffset = protobufStripeMetadata->fileoffset;
		stripeMetadata->skipListLength = protobufStripeMetadata->skiplistlength;
		stripeMetadata->dataLength = protobufStripeMetadata->datalength;
		stripeMetadata->footerLength = protobufStripeMetadata->footerlength;

		stripeMetadataList = lappend(stripeMetadataList, stripeMetadata);
	}

	protobuf__table_footer__free_unpacked(protobufTableFooter, NULL);

	tableFooter = palloc0(sizeof(TableFooter));
	tableFooter->stripeMetadataList = stripeMetadataList;
	tableFooter->blockRowCount = blockRowCount;

	return tableFooter;
}


/*
 * DeserializeStripeFooter deserializes the given buffer and returns the result
 * as a StripeFooter struct.
 */
StripeFooter *
DeserializeStripeFooter(StringInfo buffer)
{
	StripeFooter *stripeFooter = NULL;
	Protobuf__StripeFooter *protobufStripeFooter = NULL;
	uint64 *skipListSizeArray = NULL;
	uint64 *existsSizeArray = NULL;
	uint64 *valueSizeArray = NULL;
	uint64 sizeArrayLength = 0;
	uint32 columnCount = 0;

	protobufStripeFooter = protobuf__stripe_footer__unpack(NULL, buffer->len,
														   (uint8 *) buffer->data);
	if (protobufStripeFooter == NULL)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("invalid stripe footer buffer")));
	}

	columnCount = protobufStripeFooter->n_skiplistsizearray;
	if (protobufStripeFooter->n_existssizearray != columnCount ||
		protobufStripeFooter->n_valuesizearray != columnCount)
	{
		ereport(ERROR, (errmsg("could not unpack column store"),
						errdetail("stripe size array lengths don't match")));
	}

	sizeArrayLength = columnCount * sizeof(uint64);

	skipListSizeArray = palloc0(sizeArrayLength);
	existsSizeArray = palloc0(sizeArrayLength);
	valueSizeArray = palloc0(sizeArrayLength);

	memcpy(skipListSizeArray, protobufStripeFooter->skiplistsizearray, sizeArrayLength);
	memcpy(existsSizeArray, protobufStripeFooter->existssizearray, sizeArrayLength);
	memcpy(valueSizeArray, protobufStripeFooter->valuesizearray, sizeArrayLength);

	protobuf__stripe_footer__free_unpacked(protobufStripeFooter, NULL);

	stripeFooter = palloc0(sizeof(StripeFooter));
	stripeFooter->skipListSizeArray = skipListSizeArray;
	stripeFooter->existsSizeArray = existsSizeArray;
	stripeFooter->valueSizeArray = valueSizeArray;
	stripeFooter->columnCount = columnCount;

	return stripeFooter;
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
	ProtobufCBinaryData protobufBinary = {0, 0};

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
