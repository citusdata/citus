/*-------------------------------------------------------------------------
 *
 * columnar_metadata.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 * Manages metadata for columnar relations in separate, shared metadata tables
 * in the "columnar" schema.
 *
 *   * holds basic stripe information including data size and row counts
 *   * holds basic chunk and chunk group information like data offsets and
 *     min/max values (used for Chunk Group Filtering)
 *   * useful for fast VACUUM operations (e.g. reporting with VACUUM VERBOSE)
 *   * useful for stats/costing
 *   * maps logical row numbers to stripe IDs
 *   * TODO: visibility information
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "citus_version.h"
#include "columnar/columnar.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_version_compat.h"
#include "distributed/listutils.h"

#include <sys/stat.h>
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "distributed/metadata_cache.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "lib/stringinfo.h"
#include "port.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"


typedef struct
{
	Relation rel;
	EState *estate;
} ModifyState;

static void InsertStripeMetadataRow(uint64 storageId, StripeMetadata *stripe);
static void GetHighestUsedAddressAndId(uint64 storageId,
									   uint64 *highestUsedAddress,
									   uint64 *highestUsedId);
static List * ReadDataFileStripeList(uint64 storageId, Snapshot snapshot);
static StripeMetadata * BuildStripeMetadata(Datum *datumArray);
static uint32 * ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe, uint32
										chunkGroupCount);
static Oid ColumnarStorageIdSequenceRelationId(void);
static Oid ColumnarStripeRelationId(void);
static Oid ColumnarStripePKeyIndexRelationId(void);
static Oid ColumnarStripeFirstRowNumberIndexRelationId(void);
static Oid ColumnarOptionsRelationId(void);
static Oid ColumnarOptionsIndexRegclass(void);
static Oid ColumnarChunkRelationId(void);
static Oid ColumnarChunkGroupRelationId(void);
static Oid ColumnarChunkIndexRelationId(void);
static Oid ColumnarChunkGroupIndexRelationId(void);
static Oid ColumnarNamespaceId(void);
static uint64 LookupStorageId(RelFileNode relfilenode);
static uint64 GetHighestUsedFirstRowNumber(uint64 storageId);
static void DeleteStorageFromColumnarMetadataTable(Oid metadataTableId,
												   AttrNumber storageIdAtrrNumber,
												   Oid storageIdIndexId,
												   uint64 storageId);
static ModifyState * StartModifyRelation(Relation rel);
static void InsertTupleAndEnforceConstraints(ModifyState *state, Datum *values,
											 bool *nulls);
static void DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple);
static void FinishModifyRelation(ModifyState *state);
static EState * create_estate_for_relation(Relation rel);
static bytea * DatumToBytea(Datum value, Form_pg_attribute attrForm);
static Datum ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm);
static bool WriteColumnarOptions(Oid regclass, ColumnarOptions *options, bool overwrite);

PG_FUNCTION_INFO_V1(columnar_relation_storageid);

/* constants for columnar.options */
#define Natts_columnar_options 5
#define Anum_columnar_options_regclass 1
#define Anum_columnar_options_chunk_group_row_limit 2
#define Anum_columnar_options_stripe_row_limit 3
#define Anum_columnar_options_compression_level 4
#define Anum_columnar_options_compression 5

/* ----------------
 *		columnar.options definition.
 * ----------------
 */
typedef struct FormData_columnar_options
{
	Oid regclass;
	int32 chunk_group_row_limit;
	int32 stripe_row_limit;
	int32 compressionLevel;
	NameData compression;

#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_columnar_options;
typedef FormData_columnar_options *Form_columnar_options;


/* constants for columnar.stripe */
#define Natts_columnar_stripe 9
#define Anum_columnar_stripe_storageid 1
#define Anum_columnar_stripe_stripe 2
#define Anum_columnar_stripe_file_offset 3
#define Anum_columnar_stripe_data_length 4
#define Anum_columnar_stripe_column_count 5
#define Anum_columnar_stripe_chunk_row_count 6
#define Anum_columnar_stripe_row_count 7
#define Anum_columnar_stripe_chunk_count 8
#define Anum_columnar_stripe_first_row_number 9

/* constants for columnar.chunk_group */
#define Natts_columnar_chunkgroup 4
#define Anum_columnar_chunkgroup_storageid 1
#define Anum_columnar_chunkgroup_stripe 2
#define Anum_columnar_chunkgroup_chunk 3
#define Anum_columnar_chunkgroup_row_count 4

/* constants for columnar.chunk */
#define Natts_columnar_chunk 14
#define Anum_columnar_chunk_storageid 1
#define Anum_columnar_chunk_stripe 2
#define Anum_columnar_chunk_attr 3
#define Anum_columnar_chunk_chunk 4
#define Anum_columnar_chunk_minimum_value 5
#define Anum_columnar_chunk_maximum_value 6
#define Anum_columnar_chunk_value_stream_offset 7
#define Anum_columnar_chunk_value_stream_length 8
#define Anum_columnar_chunk_exists_stream_offset 9
#define Anum_columnar_chunk_exists_stream_length 10
#define Anum_columnar_chunk_value_compression_type 11
#define Anum_columnar_chunk_value_compression_level 12
#define Anum_columnar_chunk_value_decompressed_size 13
#define Anum_columnar_chunk_value_count 14


/*
 * InitColumnarOptions initialized the columnar table options. Meaning it writes the
 * default options to the options table if not already existing.
 */
void
InitColumnarOptions(Oid regclass)
{
	/*
	 * When upgrading we retain options for all columnar tables by upgrading
	 * "columnar.options" catalog table, so we shouldn't do anything here.
	 */
	if (IsBinaryUpgrade)
	{
		return;
	}

	ColumnarOptions defaultOptions = {
		.chunkRowCount = columnar_chunk_group_row_limit,
		.stripeRowCount = columnar_stripe_row_limit,
		.compressionType = columnar_compression,
		.compressionLevel = columnar_compression_level
	};

	WriteColumnarOptions(regclass, &defaultOptions, false);
}


/*
 * SetColumnarOptions writes the passed table options as the authoritive options to the
 * table irregardless of the optiones already existing or not. This can be used to put a
 * table in a certain state.
 */
void
SetColumnarOptions(Oid regclass, ColumnarOptions *options)
{
	WriteColumnarOptions(regclass, options, true);
}


/*
 * WriteColumnarOptions writes the options to the catalog table for a given regclass.
 *  - If overwrite is false it will only write the values if there is not already a record
 *    found.
 *  - If overwrite is true it will always write the settings
 *
 * The return value indicates if the record has been written.
 */
static bool
WriteColumnarOptions(Oid regclass, ColumnarOptions *options, bool overwrite)
{
	/*
	 * When upgrading we should retain the options from the previous
	 * cluster and don't write new options.
	 */
	Assert(!IsBinaryUpgrade);

	bool written = false;

	bool nulls[Natts_columnar_options] = { 0 };
	Datum values[Natts_columnar_options] = {
		ObjectIdGetDatum(regclass),
		Int32GetDatum(options->chunkRowCount),
		Int32GetDatum(options->stripeRowCount),
		Int32GetDatum(options->compressionLevel),
		0, /* to be filled below */
	};

	NameData compressionName = { 0 };
	namestrcpy(&compressionName, CompressionTypeStr(options->compressionType));
	values[Anum_columnar_options_compression - 1] = NameGetDatum(&compressionName);

	/* create heap tuple and insert into catalog table */
	Relation columnarOptions = relation_open(ColumnarOptionsRelationId(),
											 RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(columnarOptions);

	/* find existing item to perform update if exist */
	ScanKeyData scanKey[1] = { 0 };
	ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(regclass));

	Relation index = index_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		if (overwrite)
		{
			/* TODO check if the options are actually different, skip if not changed */
			/* update existing record */
			bool update[Natts_columnar_options] = { 0 };
			update[Anum_columnar_options_chunk_group_row_limit - 1] = true;
			update[Anum_columnar_options_stripe_row_limit - 1] = true;
			update[Anum_columnar_options_compression_level - 1] = true;
			update[Anum_columnar_options_compression - 1] = true;

			HeapTuple tuple = heap_modify_tuple(heapTuple, tupleDescriptor,
												values, nulls, update);
			CatalogTupleUpdate(columnarOptions, &tuple->t_self, tuple);
			written = true;
		}
	}
	else
	{
		/* inserting new record */
		HeapTuple newTuple = heap_form_tuple(tupleDescriptor, values, nulls);
		CatalogTupleInsert(columnarOptions, newTuple);
		written = true;
	}

	if (written)
	{
		CommandCounterIncrement();
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	relation_close(columnarOptions, RowExclusiveLock);

	return written;
}


/*
 * DeleteColumnarTableOptions removes the columnar table options for a regclass. When
 * missingOk is false it will throw an error when no table options can be found.
 *
 * Returns whether a record has been removed.
 */
bool
DeleteColumnarTableOptions(Oid regclass, bool missingOk)
{
	bool result = false;

	/*
	 * When upgrading we shouldn't delete or modify table options and
	 * retain options from the previous cluster.
	 */
	Assert(!IsBinaryUpgrade);

	Relation columnarOptions = relation_open(ColumnarOptionsRelationId(),
											 RowExclusiveLock);

	/* find existing item to remove */
	ScanKeyData scanKey[1] = { 0 };
	ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(regclass));

	Relation index = index_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		CatalogTupleDelete(columnarOptions, &heapTuple->t_self);
		CommandCounterIncrement();

		result = true;
	}
	else if (!missingOk)
	{
		ereport(ERROR, (errmsg("missing options for regclass: %d", regclass)));
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	relation_close(columnarOptions, RowExclusiveLock);

	return result;
}


bool
ReadColumnarOptions(Oid regclass, ColumnarOptions *options)
{
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(regclass));

	Oid columnarOptionsOid = ColumnarOptionsRelationId();
	Relation columnarOptions = try_relation_open(columnarOptionsOid, AccessShareLock);
	if (columnarOptions == NULL)
	{
		/*
		 * Extension has been dropped. This can be called while
		 * dropping extension or database via ObjectAccess().
		 */
		return false;
	}

	Relation index = try_relation_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
	if (index == NULL)
	{
		table_close(columnarOptions, AccessShareLock);

		/* extension has been dropped */
		return false;
	}

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		Form_columnar_options tupOptions = (Form_columnar_options) GETSTRUCT(heapTuple);

		options->chunkRowCount = tupOptions->chunk_group_row_limit;
		options->stripeRowCount = tupOptions->stripe_row_limit;
		options->compressionLevel = tupOptions->compressionLevel;
		options->compressionType = ParseCompressionType(NameStr(tupOptions->compression));
	}
	else
	{
		/* populate options with system defaults */
		options->compressionType = columnar_compression;
		options->stripeRowCount = columnar_stripe_row_limit;
		options->chunkRowCount = columnar_chunk_group_row_limit;
		options->compressionLevel = columnar_compression_level;
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	relation_close(columnarOptions, AccessShareLock);

	return true;
}


/*
 * SaveStripeSkipList saves chunkList for a given stripe as rows
 * of columnar.chunk.
 */
void
SaveStripeSkipList(RelFileNode relfilenode, uint64 stripe, StripeSkipList *chunkList,
				   TupleDesc tupleDescriptor)
{
	uint32 columnIndex = 0;
	uint32 chunkIndex = 0;
	uint32 columnCount = chunkList->columnCount;

	uint64 storageId = LookupStorageId(relfilenode);
	Oid columnarChunkOid = ColumnarChunkRelationId();
	Relation columnarChunk = table_open(columnarChunkOid, RowExclusiveLock);
	ModifyState *modifyState = StartModifyRelation(columnarChunk);

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		for (chunkIndex = 0; chunkIndex < chunkList->chunkCount; chunkIndex++)
		{
			ColumnChunkSkipNode *chunk =
				&chunkList->chunkSkipNodeArray[columnIndex][chunkIndex];

			Datum values[Natts_columnar_chunk] = {
				UInt64GetDatum(storageId),
				Int64GetDatum(stripe),
				Int32GetDatum(columnIndex + 1),
				Int32GetDatum(chunkIndex),
				0, /* to be filled below */
				0, /* to be filled below */
				Int64GetDatum(chunk->valueChunkOffset),
				Int64GetDatum(chunk->valueLength),
				Int64GetDatum(chunk->existsChunkOffset),
				Int64GetDatum(chunk->existsLength),
				Int32GetDatum(chunk->valueCompressionType),
				Int32GetDatum(chunk->valueCompressionLevel),
				Int64GetDatum(chunk->decompressedValueSize),
				Int64GetDatum(chunk->rowCount)
			};

			bool nulls[Natts_columnar_chunk] = { false };

			if (chunk->hasMinMax)
			{
				values[Anum_columnar_chunk_minimum_value - 1] =
					PointerGetDatum(DatumToBytea(chunk->minimumValue,
												 &tupleDescriptor->attrs[columnIndex]));
				values[Anum_columnar_chunk_maximum_value - 1] =
					PointerGetDatum(DatumToBytea(chunk->maximumValue,
												 &tupleDescriptor->attrs[columnIndex]));
			}
			else
			{
				nulls[Anum_columnar_chunk_minimum_value - 1] = true;
				nulls[Anum_columnar_chunk_maximum_value - 1] = true;
			}

			InsertTupleAndEnforceConstraints(modifyState, values, nulls);
		}
	}

	FinishModifyRelation(modifyState);
	table_close(columnarChunk, RowExclusiveLock);

	CommandCounterIncrement();
}


/*
 * SaveChunkGroups saves the metadata for given chunk groups in columnar.chunk_group.
 */
void
SaveChunkGroups(RelFileNode relfilenode, uint64 stripe,
				List *chunkGroupRowCounts)
{
	uint64 storageId = LookupStorageId(relfilenode);
	Oid columnarChunkGroupOid = ColumnarChunkGroupRelationId();
	Relation columnarChunkGroup = table_open(columnarChunkGroupOid, RowExclusiveLock);
	ModifyState *modifyState = StartModifyRelation(columnarChunkGroup);

	ListCell *lc = NULL;
	int chunkId = 0;

	foreach(lc, chunkGroupRowCounts)
	{
		int64 rowCount = lfirst_int(lc);
		Datum values[Natts_columnar_chunkgroup] = {
			UInt64GetDatum(storageId),
			Int64GetDatum(stripe),
			Int32GetDatum(chunkId),
			Int64GetDatum(rowCount)
		};

		bool nulls[Natts_columnar_chunkgroup] = { false };

		InsertTupleAndEnforceConstraints(modifyState, values, nulls);
		chunkId++;
	}

	FinishModifyRelation(modifyState);
	table_close(columnarChunkGroup, NoLock);

	CommandCounterIncrement();
}


/*
 * ReadStripeSkipList fetches chunk metadata for a given stripe.
 */
StripeSkipList *
ReadStripeSkipList(RelFileNode relfilenode, uint64 stripe, TupleDesc tupleDescriptor,
				   uint32 chunkCount)
{
	int32 columnIndex = 0;
	HeapTuple heapTuple = NULL;
	uint32 columnCount = tupleDescriptor->natts;
	ScanKeyData scanKey[2];

	uint64 storageId = LookupStorageId(relfilenode);

	Oid columnarChunkOid = ColumnarChunkRelationId();
	Relation columnarChunk = table_open(columnarChunkOid, AccessShareLock);
	Relation index = index_open(ColumnarChunkIndexRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_columnar_chunk_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunk_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(stripe));

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarChunk, index, NULL,
															2, scanKey);

	StripeSkipList *chunkList = palloc0(sizeof(StripeSkipList));
	chunkList->chunkCount = chunkCount;
	chunkList->columnCount = columnCount;
	chunkList->chunkSkipNodeArray = palloc0(columnCount * sizeof(ColumnChunkSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		chunkList->chunkSkipNodeArray[columnIndex] =
			palloc0(chunkCount * sizeof(ColumnChunkSkipNode));
	}

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_columnar_chunk];
		bool isNullArray[Natts_columnar_chunk];

		heap_deform_tuple(heapTuple, RelationGetDescr(columnarChunk), datumArray,
						  isNullArray);

		int32 attr = DatumGetInt32(datumArray[Anum_columnar_chunk_attr - 1]);
		int32 chunkIndex = DatumGetInt32(datumArray[Anum_columnar_chunk_chunk - 1]);

		if (attr <= 0 || attr > columnCount)
		{
			ereport(ERROR, (errmsg("invalid columnar chunk entry"),
							errdetail("Attribute number out of range: %d", attr)));
		}

		if (chunkIndex < 0 || chunkIndex >= chunkCount)
		{
			ereport(ERROR, (errmsg("invalid columnar chunk entry"),
							errdetail("Chunk number out of range: %d", chunkIndex)));
		}

		columnIndex = attr - 1;

		ColumnChunkSkipNode *chunk =
			&chunkList->chunkSkipNodeArray[columnIndex][chunkIndex];
		chunk->rowCount = DatumGetInt64(datumArray[Anum_columnar_chunk_value_count -
												   1]);
		chunk->valueChunkOffset =
			DatumGetInt64(datumArray[Anum_columnar_chunk_value_stream_offset - 1]);
		chunk->valueLength =
			DatumGetInt64(datumArray[Anum_columnar_chunk_value_stream_length - 1]);
		chunk->existsChunkOffset =
			DatumGetInt64(datumArray[Anum_columnar_chunk_exists_stream_offset - 1]);
		chunk->existsLength =
			DatumGetInt64(datumArray[Anum_columnar_chunk_exists_stream_length - 1]);
		chunk->valueCompressionType =
			DatumGetInt32(datumArray[Anum_columnar_chunk_value_compression_type - 1]);
		chunk->valueCompressionLevel =
			DatumGetInt32(datumArray[Anum_columnar_chunk_value_compression_level - 1]);
		chunk->decompressedValueSize =
			DatumGetInt64(datumArray[Anum_columnar_chunk_value_decompressed_size - 1]);

		if (isNullArray[Anum_columnar_chunk_minimum_value - 1] ||
			isNullArray[Anum_columnar_chunk_maximum_value - 1])
		{
			chunk->hasMinMax = false;
		}
		else
		{
			bytea *minValue = DatumGetByteaP(
				datumArray[Anum_columnar_chunk_minimum_value - 1]);
			bytea *maxValue = DatumGetByteaP(
				datumArray[Anum_columnar_chunk_maximum_value - 1]);

			chunk->minimumValue =
				ByteaToDatum(minValue, &tupleDescriptor->attrs[columnIndex]);
			chunk->maximumValue =
				ByteaToDatum(maxValue, &tupleDescriptor->attrs[columnIndex]);

			chunk->hasMinMax = true;
		}
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarChunk, AccessShareLock);

	chunkList->chunkGroupRowCounts =
		ReadChunkGroupRowCounts(storageId, stripe, chunkCount);

	return chunkList;
}


/*
 * FindStripeByRowNumber returns StripeMetadata for the stripe that has the
 * row with rowNumber by doing backward index scan on
 * stripe_first_row_number_idx. If no such row exists, then returns NULL.
 */
StripeMetadata *
FindStripeByRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot)
{
	StripeMetadata *foundStripeMetadata = NULL;

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_stripe_first_row_number,
				BTLessEqualStrategyNumber, F_INT8LE, UInt64GetDatum(rowNumber));

	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);
	Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
								AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															snapshot, 2,
															scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, BackwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(columnarStripes);
		Datum datumArray[Natts_columnar_stripe];
		bool isNullArray[Natts_columnar_stripe];
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		StripeMetadata *stripeMetadata = BuildStripeMetadata(datumArray);
		if (rowNumber < stripeMetadata->firstRowNumber + stripeMetadata->rowCount)
		{
			foundStripeMetadata = stripeMetadata;
		}
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	return foundStripeMetadata;
}


/*
 * FindStripeWithHighestRowNumber returns StripeMetadata for the stripe that
 * has the row with highest rowNumber by doing backward index scan on
 * stripe_first_row_number_idx. If given relation is empty, then returns NULL.
 */
StripeMetadata *
FindStripeWithHighestRowNumber(Relation relation, Snapshot snapshot)
{
	StripeMetadata *stripeWithHighestRowNumber = NULL;

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(storageId));

	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);
	Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
								AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															snapshot, 1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, BackwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(columnarStripes);
		Datum datumArray[Natts_columnar_stripe];
		bool isNullArray[Natts_columnar_stripe];
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		stripeWithHighestRowNumber = BuildStripeMetadata(datumArray);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	return stripeWithHighestRowNumber;
}


/*
 * ReadChunkGroupRowCounts returns an array of row counts of chunk groups for the
 * given stripe.
 */
static uint32 *
ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe, uint32 chunkGroupCount)
{
	Oid columnarChunkGroupOid = ColumnarChunkGroupRelationId();
	Relation columnarChunkGroup = table_open(columnarChunkGroupOid, AccessShareLock);
	Relation index = index_open(ColumnarChunkGroupIndexRelationId(), AccessShareLock);

	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_chunkgroup_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunkgroup_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(stripe));

	SysScanDesc scanDescriptor =
		systable_beginscan_ordered(columnarChunkGroup, index, NULL, 2, scanKey);

	uint32 chunkGroupIndex = 0;
	HeapTuple heapTuple = NULL;
	uint32 *chunkGroupRowCounts = palloc0(chunkGroupCount * sizeof(uint32));

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_columnar_chunkgroup];
		bool isNullArray[Natts_columnar_chunkgroup];

		heap_deform_tuple(heapTuple,
						  RelationGetDescr(columnarChunkGroup),
						  datumArray, isNullArray);

		uint32 tupleChunkGroupIndex =
			DatumGetUInt32(datumArray[Anum_columnar_chunkgroup_chunk - 1]);
		if (chunkGroupIndex >= chunkGroupCount ||
			tupleChunkGroupIndex != chunkGroupIndex)
		{
			elog(ERROR, "unexpected chunk group");
		}

		chunkGroupRowCounts[chunkGroupIndex] =
			(uint32) DatumGetUInt64(datumArray[Anum_columnar_chunkgroup_row_count - 1]);
		chunkGroupIndex++;
	}

	if (chunkGroupIndex != chunkGroupCount)
	{
		elog(ERROR, "unexpected chunk group count");
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarChunkGroup, AccessShareLock);

	return chunkGroupRowCounts;
}


/*
 * InsertStripeMetadataRow adds a row to columnar.stripe.
 */
static void
InsertStripeMetadataRow(uint64 storageId, StripeMetadata *stripe)
{
	bool nulls[Natts_columnar_stripe] = { 0 };
	Datum values[Natts_columnar_stripe] = {
		UInt64GetDatum(storageId),
		Int64GetDatum(stripe->id),
		Int64GetDatum(stripe->fileOffset),
		Int64GetDatum(stripe->dataLength),
		Int32GetDatum(stripe->columnCount),
		Int32GetDatum(stripe->chunkGroupRowCount),
		Int64GetDatum(stripe->rowCount),
		Int32GetDatum(stripe->chunkCount),
		UInt64GetDatum(stripe->firstRowNumber)
	};

	Oid columnarStripesOid = ColumnarStripeRelationId();
	Relation columnarStripes = table_open(columnarStripesOid, RowExclusiveLock);

	ModifyState *modifyState = StartModifyRelation(columnarStripes);

	InsertTupleAndEnforceConstraints(modifyState, values, nulls);

	FinishModifyRelation(modifyState);

	CommandCounterIncrement();

	table_close(columnarStripes, RowExclusiveLock);
}


/*
 * StripesForRelfilenode returns a list of StripeMetadata for stripes
 * of the given relfilenode.
 */
List *
StripesForRelfilenode(RelFileNode relfilenode)
{
	uint64 storageId = LookupStorageId(relfilenode);

	return ReadDataFileStripeList(storageId, GetTransactionSnapshot());
}


/*
 * GetHighestUsedAddress returns the highest used address for the given
 * relfilenode across all active and inactive transactions.
 *
 * This is used by truncate stage of VACUUM, and VACUUM can be called
 * for empty tables. So this doesn't throw errors for empty tables and
 * returns 0.
 */
uint64
GetHighestUsedAddress(RelFileNode relfilenode)
{
	uint64 storageId = LookupStorageId(relfilenode);

	uint64 highestUsedAddress = 0;
	uint64 highestUsedId = 0;
	GetHighestUsedAddressAndId(storageId, &highestUsedAddress, &highestUsedId);

	return highestUsedAddress;
}


/*
 * GetHighestUsedAddressAndId returns the highest used address and id for
 * the given relfilenode across all active and inactive transactions.
 */
static void
GetHighestUsedAddressAndId(uint64 storageId,
						   uint64 *highestUsedAddress,
						   uint64 *highestUsedId)
{
	ListCell *stripeMetadataCell = NULL;

	SnapshotData SnapshotDirty;
	InitDirtySnapshot(SnapshotDirty);

	List *stripeMetadataList = ReadDataFileStripeList(storageId, &SnapshotDirty);

	*highestUsedId = 0;

	/* file starts with metapage */
	*highestUsedAddress = COLUMNAR_BYTES_PER_PAGE;

	foreach(stripeMetadataCell, stripeMetadataList)
	{
		StripeMetadata *stripe = lfirst(stripeMetadataCell);
		uint64 lastByte = stripe->fileOffset + stripe->dataLength - 1;
		*highestUsedAddress = Max(*highestUsedAddress, lastByte);
		*highestUsedId = Max(*highestUsedId, stripe->id);
	}
}


/*
 * ReserveStripe reserves and stripe of given size for the given relation,
 * and inserts it into columnar.stripe. It is guaranteed that concurrent
 * writes won't overwrite the returned stripe.
 */
StripeMetadata
ReserveStripe(Relation rel, uint64 sizeBytes,
			  uint64 rowCount, uint64 columnCount,
			  uint64 chunkCount, uint64 chunkGroupRowCount,
			  uint64 stripeFirstRowNumber)
{
	StripeMetadata stripe = { 0 };

	uint64 storageId = ColumnarStorageGetStorageId(rel, false);

	uint64 stripeId = ColumnarStorageReserveStripe(rel);
	uint64 resLogicalStart = ColumnarStorageReserveData(rel, sizeBytes);

	stripe.fileOffset = resLogicalStart;
	stripe.dataLength = sizeBytes;
	stripe.chunkCount = chunkCount;
	stripe.chunkGroupRowCount = chunkGroupRowCount;
	stripe.columnCount = columnCount;
	stripe.rowCount = rowCount;
	stripe.id = stripeId;
	stripe.firstRowNumber = stripeFirstRowNumber;

	InsertStripeMetadataRow(storageId, &stripe);

	return stripe;
}


/*
 * ReadDataFileStripeList reads the stripe list for a given storageId
 * in the given snapshot.
 */
static List *
ReadDataFileStripeList(uint64 storageId, Snapshot snapshot)
{
	List *stripeMetadataList = NIL;
	ScanKeyData scanKey[1];
	HeapTuple heapTuple;

	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(storageId));

	Oid columnarStripesOid = ColumnarStripeRelationId();

	Relation columnarStripes = table_open(columnarStripesOid, AccessShareLock);
	Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
								AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(columnarStripes);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															snapshot, 1,
															scanKey);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_columnar_stripe];
		bool isNullArray[Natts_columnar_stripe];

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		StripeMetadata *stripeMetadata = BuildStripeMetadata(datumArray);
		stripeMetadataList = lappend(stripeMetadataList, stripeMetadata);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	return stripeMetadataList;
}


/*
 * BuildStripeMetadata builds a StripeMetadata object from given datumArray.
 */
static StripeMetadata *
BuildStripeMetadata(Datum *datumArray)
{
	StripeMetadata *stripeMetadata = palloc0(sizeof(StripeMetadata));
	stripeMetadata->id = DatumGetInt64(datumArray[Anum_columnar_stripe_stripe - 1]);
	stripeMetadata->fileOffset = DatumGetInt64(
		datumArray[Anum_columnar_stripe_file_offset - 1]);
	stripeMetadata->dataLength = DatumGetInt64(
		datumArray[Anum_columnar_stripe_data_length - 1]);
	stripeMetadata->columnCount = DatumGetInt32(
		datumArray[Anum_columnar_stripe_column_count - 1]);
	stripeMetadata->chunkCount = DatumGetInt32(
		datumArray[Anum_columnar_stripe_chunk_count - 1]);
	stripeMetadata->chunkGroupRowCount = DatumGetInt32(
		datumArray[Anum_columnar_stripe_chunk_row_count - 1]);
	stripeMetadata->rowCount = DatumGetInt64(
		datumArray[Anum_columnar_stripe_row_count - 1]);
	stripeMetadata->firstRowNumber = DatumGetUInt64(
		datumArray[Anum_columnar_stripe_first_row_number - 1]);
	return stripeMetadata;
}


/*
 * DeleteMetadataRows removes the rows with given relfilenode from columnar
 * metadata tables.
 */
void
DeleteMetadataRows(RelFileNode relfilenode)
{
	/*
	 * During a restore for binary upgrade, metadata tables and indexes may or
	 * may not exist.
	 */
	if (IsBinaryUpgrade)
	{
		return;
	}

	uint64 storageId = LookupStorageId(relfilenode);

	DeleteStorageFromColumnarMetadataTable(ColumnarStripeRelationId(),
										   Anum_columnar_stripe_storageid,
										   ColumnarStripePKeyIndexRelationId(),
										   storageId);
	DeleteStorageFromColumnarMetadataTable(ColumnarChunkGroupRelationId(),
										   Anum_columnar_chunkgroup_storageid,
										   ColumnarChunkGroupIndexRelationId(),
										   storageId);
	DeleteStorageFromColumnarMetadataTable(ColumnarChunkRelationId(),
										   Anum_columnar_chunk_storageid,
										   ColumnarChunkIndexRelationId(),
										   storageId);
}


/*
 * DeleteStorageFromColumnarMetadataTable removes the rows with given
 * storageId from given columnar metadata table.
 */
static void
DeleteStorageFromColumnarMetadataTable(Oid metadataTableId,
									   AttrNumber storageIdAtrrNumber,
									   Oid storageIdIndexId, uint64 storageId)
{
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], storageIdAtrrNumber, BTEqualStrategyNumber,
				F_INT8EQ, UInt64GetDatum(storageId));

	Relation metadataTable = try_relation_open(metadataTableId, AccessShareLock);
	if (metadataTable == NULL)
	{
		/* extension has been dropped */
		return;
	}

	Relation index = index_open(storageIdIndexId, AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(metadataTable, index, NULL,
															1, scanKey);

	ModifyState *modifyState = StartModifyRelation(metadataTable);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		DeleteTupleAndEnforceConstraints(modifyState, heapTuple);
		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan_ordered(scanDescriptor);

	FinishModifyRelation(modifyState);

	index_close(index, AccessShareLock);
	table_close(metadataTable, AccessShareLock);
}


/*
 * StartModifyRelation allocates resources for modifications.
 */
static ModifyState *
StartModifyRelation(Relation rel)
{
	EState *estate = create_estate_for_relation(rel);

	/* ExecSimpleRelationInsert, ... require caller to open indexes */
	ExecOpenIndices(estate->es_result_relation_info, false);

	ModifyState *modifyState = palloc(sizeof(ModifyState));
	modifyState->rel = rel;
	modifyState->estate = estate;

	return modifyState;
}


/*
 * InsertTupleAndEnforceConstraints inserts a tuple into a relation and makes
 * sure constraints are enforced and indexes are updated.
 */
static void
InsertTupleAndEnforceConstraints(ModifyState *state, Datum *values, bool *nulls)
{
	TupleDesc tupleDescriptor = RelationGetDescr(state->rel);
	HeapTuple tuple = heap_form_tuple(tupleDescriptor, values, nulls);

	TupleTableSlot *slot = ExecInitExtraTupleSlot(state->estate, tupleDescriptor,
												  &TTSOpsHeapTuple);

	ExecStoreHeapTuple(tuple, slot, false);

	/* use ExecSimpleRelationInsert to enforce constraints */
	ExecSimpleRelationInsert(state->estate, slot);
}


/*
 * DeleteTupleAndEnforceConstraints deletes a tuple from a relation and
 * makes sure constraints (e.g. FK constraints) are enforced.
 */
static void
DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple)
{
	EState *estate = state->estate;
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;

	ItemPointer tid = &(heapTuple->t_self);
	simple_heap_delete(state->rel, tid);

	/* execute AFTER ROW DELETE Triggers to enforce constraints */
	ExecARDeleteTriggers(estate, resultRelInfo, tid, NULL, NULL);
}


/*
 * FinishModifyRelation cleans up resources after modifications are done.
 */
static void
FinishModifyRelation(ModifyState *state)
{
	ExecCloseIndices(state->estate->es_result_relation_info);

	AfterTriggerEndQuery(state->estate);
	ExecCleanUpTriggerState(state->estate);
	ExecResetTupleTable(state->estate->es_tupleTable, false);
	FreeExecutorState(state->estate);
}


/*
 * Based on a similar function from
 * postgres/src/backend/replication/logical/worker.c.
 *
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
static EState *
create_estate_for_relation(Relation rel)
{
	EState *estate = CreateExecutorState();

	RangeTblEntry *rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;
	ExecInitRangeTable(estate, list_make1(rte));

	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	estate->es_output_cid = GetCurrentCommandId(true);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}


/*
 * DatumToBytea serializes a datum into a bytea value.
 */
static bytea *
DatumToBytea(Datum value, Form_pg_attribute attrForm)
{
	int datumLength = att_addlength_datum(0, attrForm->attlen, value);
	bytea *result = palloc0(datumLength + VARHDRSZ);

	SET_VARSIZE(result, datumLength + VARHDRSZ);

	if (attrForm->attlen > 0)
	{
		if (attrForm->attbyval)
		{
			Datum tmp;
			store_att_byval(&tmp, value, attrForm->attlen);

			memcpy_s(VARDATA(result), datumLength + VARHDRSZ,
					 &tmp, attrForm->attlen);
		}
		else
		{
			memcpy_s(VARDATA(result), datumLength + VARHDRSZ,
					 DatumGetPointer(value), attrForm->attlen);
		}
	}
	else
	{
		memcpy_s(VARDATA(result), datumLength + VARHDRSZ,
				 DatumGetPointer(value), datumLength);
	}

	return result;
}


/*
 * ByteaToDatum deserializes a value which was previously serialized using
 * DatumToBytea.
 */
static Datum
ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm)
{
	/*
	 * We copy the data so the result of this function lives even
	 * after the byteaDatum is freed.
	 */
	char *binaryDataCopy = palloc0(VARSIZE_ANY_EXHDR(bytes));
	memcpy_s(binaryDataCopy, VARSIZE_ANY_EXHDR(bytes),
			 VARDATA_ANY(bytes), VARSIZE_ANY_EXHDR(bytes));

	return fetch_att(binaryDataCopy, attrForm->attbyval, attrForm->attlen);
}


/*
 * ColumnarStorageIdSequenceRelationId returns relation id of columnar.stripe.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStorageIdSequenceRelationId(void)
{
	return get_relname_relid("storageid_seq", ColumnarNamespaceId());
}


/*
 * ColumnarStripeRelationId returns relation id of columnar.stripe.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripeRelationId(void)
{
	return get_relname_relid("stripe", ColumnarNamespaceId());
}


/*
 * ColumnarStripePKeyIndexRelationId returns relation id of columnar.stripe_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripePKeyIndexRelationId(void)
{
	return get_relname_relid("stripe_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarStripeFirstRowNumberIndexRelationId returns relation id of
 * columnar.stripe_first_row_number_idx.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripeFirstRowNumberIndexRelationId(void)
{
	return get_relname_relid("stripe_first_row_number_idx", ColumnarNamespaceId());
}


/*
 * ColumnarOptionsRelationId returns relation id of columnar.options.
 */
static Oid
ColumnarOptionsRelationId(void)
{
	return get_relname_relid("options", ColumnarNamespaceId());
}


/*
 * ColumnarOptionsIndexRegclass returns relation id of columnar.options_pkey.
 */
static Oid
ColumnarOptionsIndexRegclass(void)
{
	return get_relname_relid("options_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarChunkRelationId returns relation id of columnar.chunk.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkRelationId(void)
{
	return get_relname_relid("chunk", ColumnarNamespaceId());
}


/*
 * ColumnarChunkGroupRelationId returns relation id of columnar.chunk_group.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkGroupRelationId(void)
{
	return get_relname_relid("chunk_group", ColumnarNamespaceId());
}


/*
 * ColumnarChunkIndexRelationId returns relation id of columnar.chunk_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkIndexRelationId(void)
{
	return get_relname_relid("chunk_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarChunkGroupIndexRelationId returns relation id of columnar.chunk_group_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkGroupIndexRelationId(void)
{
	return get_relname_relid("chunk_group_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarNamespaceId returns namespace id of the schema we store columnar
 * related tables.
 */
static Oid
ColumnarNamespaceId(void)
{
	return get_namespace_oid("columnar", false);
}


/*
 * LookupStorageId reads storage metapage to find the storage ID for the given relfilenode. It returns
 * false if the relation doesn't have a meta page yet.
 */
static uint64
LookupStorageId(RelFileNode relfilenode)
{
	Oid relationId = RelidByRelfilenode(relfilenode.spcNode,
										relfilenode.relNode);

	Relation relation = relation_open(relationId, AccessShareLock);
	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	table_close(relation, AccessShareLock);

	return storageId;
}


/*
 * ColumnarMetadataNewStorageId - create a new, unique storage id and return
 * it.
 */
uint64
ColumnarMetadataNewStorageId()
{
	return nextval_internal(ColumnarStorageIdSequenceRelationId(), false);
}


/*
 * columnar_relation_storageid returns storage id associated with the
 * given relation id, or -1 if there is no associated storage id yet.
 */
Datum
columnar_relation_storageid(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	Relation relation = relation_open(relationId, AccessShareLock);
	if (!IsColumnarTableAmTable(relationId))
	{
		elog(ERROR, "relation \"%s\" is not a columnar table",
			 RelationGetRelationName(relation));
	}

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);

	relation_close(relation, AccessShareLock);

	PG_RETURN_INT64(storageId);
}


/*
 * ColumnarStorageUpdateIfNeeded - upgrade columnar storage to the current version by
 * using information from the metadata tables.
 */
void
ColumnarStorageUpdateIfNeeded(Relation rel, bool isUpgrade)
{
	if (ColumnarStorageIsCurrent(rel))
	{
		return;
	}

	RelationOpenSmgr(rel);
	BlockNumber nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	if (nblocks < 2)
	{
		ColumnarStorageInit(rel->rd_smgr, ColumnarMetadataNewStorageId());
		return;
	}

	uint64 storageId = ColumnarStorageGetStorageId(rel, true);

	uint64 highestId;
	uint64 highestOffset;
	GetHighestUsedAddressAndId(storageId, &highestOffset, &highestId);

	uint64 reservedStripeId = highestId + 1;
	uint64 reservedOffset = highestOffset + 1;
	uint64 reservedRowNumber = GetHighestUsedFirstRowNumber(storageId) + 1;
	ColumnarStorageUpdateCurrent(rel, isUpgrade, reservedStripeId,
								 reservedRowNumber, reservedOffset);
}


/*
 * GetHighestUsedFirstRowNumber returns the highest used first_row_number
 * for given storageId. Returns COLUMNAR_INVALID_ROW_NUMBER if storage with
 * storageId has no stripes.
 * Note that normally we would use ColumnarStorageGetReservedRowNumber
 * to decide that. However, this function is designed to be used when
 * building the metapage itself during upgrades.
 */
static uint64
GetHighestUsedFirstRowNumber(uint64 storageId)
{
	List *stripeMetadataList = ReadDataFileStripeList(storageId,
													  GetTransactionSnapshot());
	if (list_length(stripeMetadataList) == 0)
	{
		return COLUMNAR_INVALID_ROW_NUMBER;
	}

	/* XXX: Better to have an invalid value for StripeMetadata.rowCount too */
	uint64 stripeRowCount = -1;
	uint64 highestFirstRowNumber = COLUMNAR_INVALID_ROW_NUMBER;

	StripeMetadata *stripeMetadata = NULL;
	foreach_ptr(stripeMetadata, stripeMetadataList)
	{
		highestFirstRowNumber = Max(highestFirstRowNumber,
									stripeMetadata->firstRowNumber);
		stripeRowCount = stripeMetadata->rowCount;
	}

	return highestFirstRowNumber + stripeRowCount - 1;
}
