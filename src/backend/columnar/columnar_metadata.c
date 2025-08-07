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


#include <sys/stat.h>

#include "postgres.h"

#include "miscadmin.h"
#include "port.h"
#include "safe_lib.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "citus_version.h"
#include "pg_version_constants.h"

#include "columnar/columnar.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_version_compat.h"

#include "distributed/listutils.h"

#if PG_VERSION_NUM >= PG_VERSION_16
#include "parser/parse_relation.h"
#include "storage/relfilelocator.h"
#include "utils/relfilenumbermap.h"
#else
#include "utils/relfilenodemap.h"
#endif

#define COLUMNAR_RELOPTION_NAMESPACE "columnar"
#define SLOW_METADATA_ACCESS_WARNING \
	"Metadata index %s is not available, this might mean slower read/writes " \
	"on columnar tables. This is expected during Postgres upgrades and not " \
	"expected otherwise."

typedef struct
{
	Relation rel;
	EState *estate;
	ResultRelInfo *resultRelInfo;
} ModifyState;

/* RowNumberLookupMode to be used in StripeMetadataLookupRowNumber */
typedef enum RowNumberLookupMode
{
	/*
	 * Find the stripe whose firstRowNumber is less than or equal to given
	 * input rowNumber.
	 */
	FIND_LESS_OR_EQUAL,

	/*
	 * Find the stripe whose firstRowNumber is greater than input rowNumber.
	 */
	FIND_GREATER
} RowNumberLookupMode;

static void ParseColumnarRelOptions(List *reloptions, ColumnarOptions *options);
static void InsertEmptyStripeMetadataRow(uint64 storageId, uint64 stripeId,
										 uint32 columnCount, uint32 chunkGroupRowCount,
										 uint64 firstRowNumber);
static void GetHighestUsedAddressAndId(uint64 storageId,
									   uint64 *highestUsedAddress,
									   uint64 *highestUsedId);
static StripeMetadata * UpdateStripeMetadataRow(uint64 storageId, uint64 stripeId,
												bool *update, Datum *newValues);
static List * ReadDataFileStripeList(uint64 storageId, Snapshot snapshot);
static StripeMetadata * BuildStripeMetadata(Relation columnarStripes,
											HeapTuple heapTuple);
static uint32 * ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe, uint32
										chunkGroupCount, Snapshot snapshot);
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
static uint64 LookupStorageId(RelFileLocator relfilelocator);
static uint64 GetHighestUsedRowNumber(uint64 storageId);
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
static StripeMetadata * StripeMetadataLookupRowNumber(Relation relation, uint64 rowNumber,
													  Snapshot snapshot,
													  RowNumberLookupMode lookupMode);
static void CheckStripeMetadataConsistency(StripeMetadata *stripeMetadata);

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
 * ParseColumnarRelOptions - update the given 'options' using the given list
 * of DefElem.
 */
static void
ParseColumnarRelOptions(List *reloptions, ColumnarOptions *options)
{
	ListCell *lc = NULL;

	foreach(lc, reloptions)
	{
		DefElem *elem = castNode(DefElem, lfirst(lc));

		if (elem->defnamespace == NULL ||
			strcmp(elem->defnamespace, COLUMNAR_RELOPTION_NAMESPACE) != 0)
		{
			ereport(ERROR, (errmsg("columnar options must have the prefix \"%s\"",
								   COLUMNAR_RELOPTION_NAMESPACE)));
		}

		if (strcmp(elem->defname, "chunk_group_row_limit") == 0)
		{
			options->chunkRowCount = (elem->arg == NULL) ?
									 columnar_chunk_group_row_limit : defGetInt64(elem);
			if (options->chunkRowCount < CHUNK_ROW_COUNT_MINIMUM ||
				options->chunkRowCount > CHUNK_ROW_COUNT_MAXIMUM)
			{
				ereport(ERROR, (errmsg("chunk group row count limit out of range"),
								errhint("chunk group row count limit must be between "
										UINT64_FORMAT " and " UINT64_FORMAT,
										(uint64) CHUNK_ROW_COUNT_MINIMUM,
										(uint64) CHUNK_ROW_COUNT_MAXIMUM)));
			}
		}
		else if (strcmp(elem->defname, "stripe_row_limit") == 0)
		{
			options->stripeRowCount = (elem->arg == NULL) ?
									  columnar_stripe_row_limit : defGetInt64(elem);

			if (options->stripeRowCount < STRIPE_ROW_COUNT_MINIMUM ||
				options->stripeRowCount > STRIPE_ROW_COUNT_MAXIMUM)
			{
				ereport(ERROR, (errmsg("stripe row count limit out of range"),
								errhint("stripe row count limit must be between "
										UINT64_FORMAT " and " UINT64_FORMAT,
										(uint64) STRIPE_ROW_COUNT_MINIMUM,
										(uint64) STRIPE_ROW_COUNT_MAXIMUM)));
			}
		}
		else if (strcmp(elem->defname, "compression") == 0)
		{
			options->compressionType = (elem->arg == NULL) ?
									   columnar_compression : ParseCompressionType(
				defGetString(elem));

			if (options->compressionType == COMPRESSION_TYPE_INVALID)
			{
				ereport(ERROR, (errmsg("unknown compression type for columnar table: %s",
									   quote_identifier(defGetString(elem)))));
			}
		}
		else if (strcmp(elem->defname, "compression_level") == 0)
		{
			options->compressionLevel = (elem->arg == NULL) ?
										columnar_compression_level : defGetInt64(elem);

			if (options->compressionLevel < COMPRESSION_LEVEL_MIN ||
				options->compressionLevel > COMPRESSION_LEVEL_MAX)
			{
				ereport(ERROR, (errmsg("compression level out of range"),
								errhint("compression level must be between %d and %d",
										COMPRESSION_LEVEL_MIN,
										COMPRESSION_LEVEL_MAX)));
			}
		}
		else
		{
			ereport(ERROR, (errmsg("unrecognized columnar storage parameter \"%s\"",
								   elem->defname)));
		}
	}
}


/*
 * ExtractColumnarOptions - extract columnar options from inOptions, appending
 * to inoutColumnarOptions. Return the remaining (non-columnar) options.
 */
List *
ExtractColumnarRelOptions(List *inOptions, List **inoutColumnarOptions)
{
	List *otherOptions = NIL;

	ListCell *lc = NULL;
	foreach(lc, inOptions)
	{
		DefElem *elem = castNode(DefElem, lfirst(lc));

		if (elem->defnamespace != NULL &&
			strcmp(elem->defnamespace, COLUMNAR_RELOPTION_NAMESPACE) == 0)
		{
			*inoutColumnarOptions = lappend(*inoutColumnarOptions, elem);
		}
		else
		{
			otherOptions = lappend(otherOptions, elem);
		}
	}

	/* validate options */
	ColumnarOptions dummy = { 0 };
	ParseColumnarRelOptions(*inoutColumnarOptions, &dummy);

	return otherOptions;
}


/*
 * SetColumnarRelOptions - apply the list of DefElem options to the
 * relation. If there are duplicates, the last one in the list takes effect.
 */
void
SetColumnarRelOptions(RangeVar *rv, List *reloptions)
{
	ColumnarOptions options = { 0 };

	if (reloptions == NIL)
	{
		return;
	}

	Relation rel = relation_openrv(rv, AccessShareLock);
	Oid relid = RelationGetRelid(rel);
	relation_close(rel, NoLock);

	/* get existing or default options */
	if (!ReadColumnarOptions(relid, &options))
	{
		/* if extension doesn't exist, just return */
		return;
	}

	ParseColumnarRelOptions(reloptions, &options);

	SetColumnarOptions(relid, &options);
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

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
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

	Relation columnarOptions = try_relation_open(ColumnarOptionsRelationId(),
												 RowExclusiveLock);
	if (columnarOptions == NULL)
	{
		/* extension has been dropped */
		return false;
	}

	/* find existing item to remove */
	ScanKeyData scanKey[1] = { 0 };
	ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(regclass));

	Relation index = index_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
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

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
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
SaveStripeSkipList(RelFileLocator relfilelocator, uint64 stripe,
				   StripeSkipList *chunkList,
				   TupleDesc tupleDescriptor)
{
	uint32 columnIndex = 0;
	uint32 chunkIndex = 0;
	uint32 columnCount = chunkList->columnCount;

	uint64 storageId = LookupStorageId(relfilelocator);
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
												 Attr(tupleDescriptor, columnIndex)));
				values[Anum_columnar_chunk_maximum_value - 1] =
					PointerGetDatum(DatumToBytea(chunk->maximumValue,
												 Attr(tupleDescriptor, columnIndex)));
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
}


/*
 * SaveChunkGroups saves the metadata for given chunk groups in columnar.chunk_group.
 */
void
SaveChunkGroups(RelFileLocator relfilelocator, uint64 stripe,
				List *chunkGroupRowCounts)
{
	uint64 storageId = LookupStorageId(relfilelocator);
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
}


/*
 * ReadStripeSkipList fetches chunk metadata for a given stripe.
 */
StripeSkipList *
ReadStripeSkipList(RelFileLocator relfilelocator, uint64 stripe,
				   TupleDesc tupleDescriptor,
				   uint32 chunkCount, Snapshot snapshot)
{
	int32 columnIndex = 0;
	HeapTuple heapTuple = NULL;
	uint32 columnCount = tupleDescriptor->natts;
	ScanKeyData scanKey[2];

	uint64 storageId = LookupStorageId(relfilelocator);

	Oid columnarChunkOid = ColumnarChunkRelationId();
	Relation columnarChunk = table_open(columnarChunkOid, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_columnar_chunk_storageid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunk_stripe,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(stripe));

	Oid indexId = ColumnarChunkIndexRelationId();
	bool indexOk = OidIsValid(indexId);
	SysScanDesc scanDescriptor = systable_beginscan(columnarChunk, indexId,
													indexOk, snapshot, 2, scanKey);

	static bool loggedSlowMetadataAccessWarning = false;
	if (!indexOk && !loggedSlowMetadataAccessWarning)
	{
		ereport(WARNING, (errmsg(SLOW_METADATA_ACCESS_WARNING, "chunk_pkey")));
		loggedSlowMetadataAccessWarning = true;
	}

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
				ByteaToDatum(minValue, Attr(tupleDescriptor, columnIndex));
			chunk->maximumValue =
				ByteaToDatum(maxValue, Attr(tupleDescriptor, columnIndex));

			chunk->hasMinMax = true;
		}
	}

	systable_endscan(scanDescriptor);
	table_close(columnarChunk, AccessShareLock);

	chunkList->chunkGroupRowCounts =
		ReadChunkGroupRowCounts(storageId, stripe, chunkCount, snapshot);

	return chunkList;
}


/*
 * FindStripeByRowNumber returns StripeMetadata for the stripe that has the
 * smallest firstRowNumber among the stripes whose firstRowNumber is grater
 * than given rowNumber. If no such stripe exists, then returns NULL.
 */
StripeMetadata *
FindNextStripeByRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot)
{
	return StripeMetadataLookupRowNumber(relation, rowNumber, snapshot, FIND_GREATER);
}


/*
 * FindStripeByRowNumber returns StripeMetadata for the stripe that contains
 * the row with rowNumber. If no such stripe exists, then returns NULL.
 */
StripeMetadata *
FindStripeByRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot)
{
	StripeMetadata *stripeMetadata =
		FindStripeWithMatchingFirstRowNumber(relation, rowNumber, snapshot);
	if (!stripeMetadata)
	{
		return NULL;
	}

	if (rowNumber > StripeGetHighestRowNumber(stripeMetadata))
	{
		return NULL;
	}

	return stripeMetadata;
}


/*
 * FindStripeWithMatchingFirstRowNumber returns a StripeMetadata object for
 * the stripe that has the greatest firstRowNumber among the stripes whose
 * firstRowNumber is smaller than or equal to given rowNumber. If no such
 * stripe exists, then returns NULL.
 *
 * Note that this doesn't mean that found stripe certainly contains the tuple
 * with given rowNumber. This is because, it also needs to be verified if
 * highest row number that found stripe contains is greater than or equal to
 * given rowNumber. For this reason, unless that additional check is done,
 * this function is mostly useful for checking against "possible" constraint
 * violations due to concurrent writes that are not flushed by other backends
 * yet.
 */
StripeMetadata *
FindStripeWithMatchingFirstRowNumber(Relation relation, uint64 rowNumber,
									 Snapshot snapshot)
{
	return StripeMetadataLookupRowNumber(relation, rowNumber, snapshot,
										 FIND_LESS_OR_EQUAL);
}


/*
 * StripeWriteState returns write state of given stripe.
 */
StripeWriteStateEnum
StripeWriteState(StripeMetadata *stripeMetadata)
{
	if (stripeMetadata->aborted)
	{
		return STRIPE_WRITE_ABORTED;
	}
	else if (stripeMetadata->rowCount > 0)
	{
		return STRIPE_WRITE_FLUSHED;
	}
	else
	{
		return STRIPE_WRITE_IN_PROGRESS;
	}
}


/*
 * StripeGetHighestRowNumber returns rowNumber of the row with highest
 * rowNumber in given stripe.
 */
uint64
StripeGetHighestRowNumber(StripeMetadata *stripeMetadata)
{
	return stripeMetadata->firstRowNumber + stripeMetadata->rowCount - 1;
}


/*
 * StripeMetadataLookupRowNumber returns StripeMetadata for the stripe whose
 * firstRowNumber is less than or equal to (FIND_LESS_OR_EQUAL), or is
 * greater than (FIND_GREATER) given rowNumber.
 * If no such stripe exists, then returns NULL.
 */
static StripeMetadata *
StripeMetadataLookupRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot,
							  RowNumberLookupMode lookupMode)
{
	Assert(lookupMode == FIND_LESS_OR_EQUAL || lookupMode == FIND_GREATER);

	StripeMetadata *foundStripeMetadata = NULL;

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(storageId));

	StrategyNumber strategyNumber = InvalidStrategy;
	RegProcedure procedure = InvalidOid;
	if (lookupMode == FIND_LESS_OR_EQUAL)
	{
		strategyNumber = BTLessEqualStrategyNumber;
		procedure = F_INT8LE;
	}
	else if (lookupMode == FIND_GREATER)
	{
		strategyNumber = BTGreaterStrategyNumber;
		procedure = F_INT8GT;
	}
	ScanKeyInit(&scanKey[1], Anum_columnar_stripe_first_row_number,
				strategyNumber, procedure, Int64GetDatum(rowNumber));

	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);

	Oid indexId = ColumnarStripeFirstRowNumberIndexRelationId();
	bool indexOk = OidIsValid(indexId);
	SysScanDesc scanDescriptor = systable_beginscan(columnarStripes, indexId, indexOk,
													snapshot, 2, scanKey);

	static bool loggedSlowMetadataAccessWarning = false;
	if (!indexOk && !loggedSlowMetadataAccessWarning)
	{
		ereport(WARNING, (errmsg(SLOW_METADATA_ACCESS_WARNING,
								 "stripe_first_row_number_idx")));
		loggedSlowMetadataAccessWarning = true;
	}

	if (indexOk)
	{
		ScanDirection scanDirection = NoMovementScanDirection;
		if (lookupMode == FIND_LESS_OR_EQUAL)
		{
			scanDirection = BackwardScanDirection;
		}
		else if (lookupMode == FIND_GREATER)
		{
			scanDirection = ForwardScanDirection;
		}
		HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, scanDirection);
		if (HeapTupleIsValid(heapTuple))
		{
			foundStripeMetadata = BuildStripeMetadata(columnarStripes, heapTuple);
		}
	}
	else
	{
		HeapTuple heapTuple = NULL;
		while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
		{
			StripeMetadata *stripe = BuildStripeMetadata(columnarStripes, heapTuple);
			if (!foundStripeMetadata)
			{
				/* first match */
				foundStripeMetadata = stripe;
			}
			else if (lookupMode == FIND_LESS_OR_EQUAL &&
					 stripe->firstRowNumber > foundStripeMetadata->firstRowNumber)
			{
				/*
				 * Among the stripes with firstRowNumber less-than-or-equal-to given,
				 * we're looking for the one with the greatest firstRowNumber.
				 */
				foundStripeMetadata = stripe;
			}
			else if (lookupMode == FIND_GREATER &&
					 stripe->firstRowNumber < foundStripeMetadata->firstRowNumber)
			{
				/*
				 * Among the stripes with firstRowNumber greater-than given,
				 * we're looking for the one with the smallest firstRowNumber.
				 */
				foundStripeMetadata = stripe;
			}
		}
	}

	systable_endscan(scanDescriptor);
	table_close(columnarStripes, AccessShareLock);

	return foundStripeMetadata;
}


/*
 * CheckStripeMetadataConsistency first decides if stripe write operation for
 * given stripe is "flushed", "aborted" or "in-progress", then errors out if
 * its metadata entry contradicts with this fact.
 *
 * Checks performed here are just to catch bugs, so it is encouraged to call
 * this function whenever a StripeMetadata object is built from an heap tuple
 * of columnar.stripe. Currently, BuildStripeMetadata is the only function
 * that does this.
 */
static void
CheckStripeMetadataConsistency(StripeMetadata *stripeMetadata)
{
	bool stripeLooksInProgress =
		stripeMetadata->rowCount == 0 && stripeMetadata->chunkCount == 0 &&
		stripeMetadata->fileOffset == ColumnarInvalidLogicalOffset &&
		stripeMetadata->dataLength == 0;

	/*
	 * Even if stripe is flushed, fileOffset and dataLength might be equal
	 * to 0 for zero column tables, but those two should still be consistent
	 * with respect to each other.
	 */
	bool stripeLooksFlushed =
		stripeMetadata->rowCount > 0 && stripeMetadata->chunkCount > 0 &&
		((stripeMetadata->fileOffset != ColumnarInvalidLogicalOffset &&
		  stripeMetadata->dataLength > 0) ||
		 (stripeMetadata->fileOffset == ColumnarInvalidLogicalOffset &&
		  stripeMetadata->dataLength == 0));

	StripeWriteStateEnum stripeWriteState = StripeWriteState(stripeMetadata);
	if (stripeWriteState == STRIPE_WRITE_FLUSHED && stripeLooksFlushed)
	{
		/*
		 * If stripe was flushed to disk, then we expect stripe to store
		 * at least one tuple.
		 */
		return;
	}
	else if (stripeWriteState == STRIPE_WRITE_IN_PROGRESS && stripeLooksInProgress)
	{
		/*
		 * If stripe was not flushed to disk, then values of given four
		 * fields should match the columns inserted by
		 * InsertEmptyStripeMetadataRow.
		 */
		return;
	}
	else if (stripeWriteState == STRIPE_WRITE_ABORTED && (stripeLooksInProgress ||
														  stripeLooksFlushed))
	{
		/*
		 * Stripe metadata entry for an aborted write can be complete or
		 * incomplete. We might have aborted the transaction before or after
		 * inserting into stripe metadata.
		 */
		return;
	}

	ereport(ERROR, (errmsg("unexpected stripe state, stripe metadata "
						   "entry for stripe with id=" UINT64_FORMAT
						   " is not consistent", stripeMetadata->id)));
}


/*
 * FindStripeWithHighestRowNumber returns StripeMetadata for the stripe that
 * has the row with highest rowNumber. If given relation is empty, then returns
 * NULL.
 */
StripeMetadata *
FindStripeWithHighestRowNumber(Relation relation, Snapshot snapshot)
{
	StripeMetadata *stripeWithHighestRowNumber = NULL;

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(storageId));

	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);

	Oid indexId = ColumnarStripeFirstRowNumberIndexRelationId();
	bool indexOk = OidIsValid(indexId);
	SysScanDesc scanDescriptor = systable_beginscan(columnarStripes, indexId, indexOk,
													snapshot, 1, scanKey);

	static bool loggedSlowMetadataAccessWarning = false;
	if (!indexOk && !loggedSlowMetadataAccessWarning)
	{
		ereport(WARNING, (errmsg(SLOW_METADATA_ACCESS_WARNING,
								 "stripe_first_row_number_idx")));
		loggedSlowMetadataAccessWarning = true;
	}

	if (indexOk)
	{
		/* do one-time fetch using the index */
		HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor,
													   BackwardScanDirection);
		if (HeapTupleIsValid(heapTuple))
		{
			stripeWithHighestRowNumber = BuildStripeMetadata(columnarStripes, heapTuple);
		}
	}
	else
	{
		HeapTuple heapTuple = NULL;
		while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
		{
			StripeMetadata *stripe = BuildStripeMetadata(columnarStripes, heapTuple);
			if (!stripeWithHighestRowNumber ||
				stripe->firstRowNumber > stripeWithHighestRowNumber->firstRowNumber)
			{
				/* first or a greater match */
				stripeWithHighestRowNumber = stripe;
			}
		}
	}

	systable_endscan(scanDescriptor);
	table_close(columnarStripes, AccessShareLock);

	return stripeWithHighestRowNumber;
}


/*
 * ReadChunkGroupRowCounts returns an array of row counts of chunk groups for the
 * given stripe.
 */
static uint32 *
ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe, uint32 chunkGroupCount,
						Snapshot snapshot)
{
	Oid columnarChunkGroupOid = ColumnarChunkGroupRelationId();
	Relation columnarChunkGroup = table_open(columnarChunkGroupOid, AccessShareLock);

	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_chunkgroup_storageid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunkgroup_stripe,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(stripe));

	Oid indexId = ColumnarChunkGroupIndexRelationId();
	bool indexOk = OidIsValid(indexId);
	SysScanDesc scanDescriptor =
		systable_beginscan(columnarChunkGroup, indexId, indexOk, snapshot, 2, scanKey);

	static bool loggedSlowMetadataAccessWarning = false;
	if (!indexOk && !loggedSlowMetadataAccessWarning)
	{
		ereport(WARNING, (errmsg(SLOW_METADATA_ACCESS_WARNING, "chunk_group_pkey")));
		loggedSlowMetadataAccessWarning = true;
	}

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
		if (tupleChunkGroupIndex >= chunkGroupCount)
		{
			elog(ERROR, "unexpected chunk group");
		}

		chunkGroupRowCounts[tupleChunkGroupIndex] =
			(uint32) DatumGetUInt64(datumArray[Anum_columnar_chunkgroup_row_count - 1]);
	}

	systable_endscan(scanDescriptor);
	table_close(columnarChunkGroup, AccessShareLock);

	return chunkGroupRowCounts;
}


/*
 * InsertEmptyStripeMetadataRow adds a row to columnar.stripe for the empty
 * stripe reservation made for stripeId.
 */
static void
InsertEmptyStripeMetadataRow(uint64 storageId, uint64 stripeId, uint32 columnCount,
							 uint32 chunkGroupRowCount, uint64 firstRowNumber)
{
	bool nulls[Natts_columnar_stripe] = { false };

	Datum values[Natts_columnar_stripe] = { 0 };
	values[Anum_columnar_stripe_storageid - 1] =
		UInt64GetDatum(storageId);
	values[Anum_columnar_stripe_stripe - 1] =
		UInt64GetDatum(stripeId);
	values[Anum_columnar_stripe_column_count - 1] =
		UInt32GetDatum(columnCount);
	values[Anum_columnar_stripe_chunk_row_count - 1] =
		UInt32GetDatum(chunkGroupRowCount);
	values[Anum_columnar_stripe_first_row_number - 1] =
		UInt64GetDatum(firstRowNumber);

	/* stripe has no rows yet, so initialize rest of the columns accordingly */
	values[Anum_columnar_stripe_row_count - 1] =
		UInt64GetDatum(0);
	values[Anum_columnar_stripe_file_offset - 1] =
		UInt64GetDatum(ColumnarInvalidLogicalOffset);
	values[Anum_columnar_stripe_data_length - 1] =
		UInt64GetDatum(0);
	values[Anum_columnar_stripe_chunk_count - 1] =
		UInt32GetDatum(0);

	Oid columnarStripesOid = ColumnarStripeRelationId();
	Relation columnarStripes = table_open(columnarStripesOid, RowExclusiveLock);

	ModifyState *modifyState = StartModifyRelation(columnarStripes);

	InsertTupleAndEnforceConstraints(modifyState, values, nulls);

	FinishModifyRelation(modifyState);

	table_close(columnarStripes, RowExclusiveLock);
}


/*
 * StripesForRelfilelocator returns a list of StripeMetadata for stripes
 * of the given relfilenode.
 */
List *
StripesForRelfilelocator(RelFileLocator relfilelocator)
{
	uint64 storageId = LookupStorageId(relfilelocator);

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
GetHighestUsedAddress(RelFileLocator relfilelocator)
{
	uint64 storageId = LookupStorageId(relfilelocator);

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
 * ReserveEmptyStripe reserves an empty stripe for given relation
 * and inserts it into columnar.stripe. It is guaranteed that concurrent
 * writes won't overwrite the returned stripe.
 */
EmptyStripeReservation *
ReserveEmptyStripe(Relation rel, uint64 columnCount, uint64 chunkGroupRowCount,
				   uint64 stripeRowCount)
{
	EmptyStripeReservation *stripeReservation = palloc0(sizeof(EmptyStripeReservation));

	uint64 storageId = ColumnarStorageGetStorageId(rel, false);

	stripeReservation->stripeId = ColumnarStorageReserveStripeId(rel);
	stripeReservation->stripeFirstRowNumber =
		ColumnarStorageReserveRowNumber(rel, stripeRowCount);

	/*
	 * XXX: Instead of inserting a dummy entry to columnar.stripe and
	 * updating it when flushing the stripe, we could have a hash table
	 * in shared memory for the bookkeeping of ongoing writes.
	 */
	InsertEmptyStripeMetadataRow(storageId, stripeReservation->stripeId,
								 columnCount, chunkGroupRowCount,
								 stripeReservation->stripeFirstRowNumber);

	return stripeReservation;
}


/*
 * CompleteStripeReservation completes reservation of the stripe with
 * stripeId for given size and in-place updates related stripe metadata tuple
 * to complete reservation.
 */
StripeMetadata *
CompleteStripeReservation(Relation rel, uint64 stripeId, uint64 sizeBytes,
						  uint64 rowCount, uint64 chunkCount)
{
	uint64 resLogicalStart = ColumnarStorageReserveData(rel, sizeBytes);
	uint64 storageId = ColumnarStorageGetStorageId(rel, false);

	bool update[Natts_columnar_stripe] = { false };
	update[Anum_columnar_stripe_file_offset - 1] = true;
	update[Anum_columnar_stripe_data_length - 1] = true;
	update[Anum_columnar_stripe_row_count - 1] = true;
	update[Anum_columnar_stripe_chunk_count - 1] = true;

	Datum newValues[Natts_columnar_stripe] = { 0 };
	newValues[Anum_columnar_stripe_file_offset - 1] = Int64GetDatum(resLogicalStart);
	newValues[Anum_columnar_stripe_data_length - 1] = Int64GetDatum(sizeBytes);
	newValues[Anum_columnar_stripe_row_count - 1] = UInt64GetDatum(rowCount);
	newValues[Anum_columnar_stripe_chunk_count - 1] = Int32GetDatum(chunkCount);

	return UpdateStripeMetadataRow(storageId, stripeId, update, newValues);
}


/*
 * UpdateStripeMetadataRow updates stripe metadata tuple for the stripe with
 * stripeId according to given newValues and update arrays.
 * Note that this function shouldn't be used for the cases where any indexes
 * of stripe metadata should be updated according to modifications done.
 */
static StripeMetadata *
UpdateStripeMetadataRow(uint64 storageId, uint64 stripeId, bool *update,
						Datum *newValues)
{
	SnapshotData dirtySnapshot;
	InitDirtySnapshot(dirtySnapshot);

	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_stripe_stripe,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(stripeId));

	Oid columnarStripesOid = ColumnarStripeRelationId();

#if PG_VERSION_NUM >= 180000

	/* CatalogTupleUpdate performs a normal heap UPDATE → RowExclusiveLock */
	const LOCKMODE openLockMode = RowExclusiveLock;
#else

	/* In‑place update never changed tuple length → AccessShareLock was enough */
	const LOCKMODE openLockMode = AccessShareLock;
#endif

	Relation columnarStripes = table_open(columnarStripesOid, openLockMode);

	Oid indexId = ColumnarStripePKeyIndexRelationId();
	bool indexOk = OidIsValid(indexId);
	SysScanDesc scanDescriptor = systable_beginscan(columnarStripes, indexId, indexOk,
													&dirtySnapshot, 2, scanKey);

	static bool loggedSlowMetadataAccessWarning = false;
	if (!indexOk && !loggedSlowMetadataAccessWarning)
	{
		ereport(WARNING, (errmsg(SLOW_METADATA_ACCESS_WARNING, "stripe_pkey")));
		loggedSlowMetadataAccessWarning = true;
	}

	HeapTuple oldTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(oldTuple))
	{
		ereport(ERROR, (errmsg("attempted to modify an unexpected stripe, "
							   "columnar storage with id=" UINT64_FORMAT
							   " does not have stripe with id=" UINT64_FORMAT,
							   storageId, stripeId)));
	}

	bool newNulls[Natts_columnar_stripe] = { false };
	TupleDesc tupleDescriptor = RelationGetDescr(columnarStripes);
	HeapTuple modifiedTuple = heap_modify_tuple(oldTuple,
												tupleDescriptor,
												newValues,
												newNulls,
												update);

#if PG_VERSION_NUM < PG_VERSION_18

	/*
	 * heap_inplace_update already doesn't allow changing size of the original
	 * tuple, so we don't allow setting any Datum's to NULL values.
	 */
	heap_inplace_update(columnarStripes, modifiedTuple);

	/*
	 * Existing tuple now contains modifications, because we used
	 * heap_inplace_update().
	 */
	HeapTuple newTuple = oldTuple;
#else

	/* Regular catalog UPDATE keeps indexes in sync */
	CatalogTupleUpdate(columnarStripes, &oldTuple->t_self, modifiedTuple);
	HeapTuple newTuple = modifiedTuple;
#endif

	CommandCounterIncrement();

	/*
	 * Must not pass modifiedTuple, because BuildStripeMetadata expects a real
	 * heap tuple with MVCC fields.
	 */
	StripeMetadata *modifiedStripeMetadata =
		BuildStripeMetadata(columnarStripes, newTuple);

	systable_endscan(scanDescriptor);
	table_close(columnarStripes, openLockMode);

	/* return StripeMetadata object built from modified tuple */
	return modifiedStripeMetadata;
}


/*
 * ReadDataFileStripeList reads the stripe list for a given storageId
 * in the given snapshot.
 *
 * Doesn't sort the stripes by their ids before returning if
 * stripe_first_row_number_idx is not available --normally can only happen
 * during pg upgrades.
 */
static List *
ReadDataFileStripeList(uint64 storageId, Snapshot snapshot)
{
	List *stripeMetadataList = NIL;
	ScanKeyData scanKey[1];
	HeapTuple heapTuple;

	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(storageId));

	Oid columnarStripesOid = ColumnarStripeRelationId();

	Relation columnarStripes = table_open(columnarStripesOid, AccessShareLock);

	Oid indexId = ColumnarStripeFirstRowNumberIndexRelationId();
	bool indexOk = OidIsValid(indexId);
	SysScanDesc scanDescriptor = systable_beginscan(columnarStripes, indexId,
													indexOk, snapshot, 1, scanKey);

	static bool loggedSlowMetadataAccessWarning = false;
	if (!indexOk && !loggedSlowMetadataAccessWarning)
	{
		ereport(WARNING, (errmsg(SLOW_METADATA_ACCESS_WARNING,
								 "stripe_first_row_number_idx")));
		loggedSlowMetadataAccessWarning = true;
	}

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		StripeMetadata *stripeMetadata = BuildStripeMetadata(columnarStripes, heapTuple);
		stripeMetadataList = lappend(stripeMetadataList, stripeMetadata);
	}

	systable_endscan(scanDescriptor);
	table_close(columnarStripes, AccessShareLock);

	return stripeMetadataList;
}


/*
 * BuildStripeMetadata builds a StripeMetadata object from given heap tuple.
 *
 * NB: heapTuple must be a proper heap tuple with MVCC fields.
 */
static StripeMetadata *
BuildStripeMetadata(Relation columnarStripes, HeapTuple heapTuple)
{
	Assert(RelationGetRelid(columnarStripes) == ColumnarStripeRelationId());

	Datum datumArray[Natts_columnar_stripe];
	bool isNullArray[Natts_columnar_stripe];
	heap_deform_tuple(heapTuple, RelationGetDescr(columnarStripes),
					  datumArray, isNullArray);

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

	/*
	 * If there is unflushed data in a parent transaction, then we would
	 * have already thrown an error before starting to scan the table.. If
	 * the data is from an earlier subxact that committed, then it would
	 * have been flushed already. For this reason, we don't care about
	 * subtransaction id here.
	 */
	TransactionId entryXmin = HeapTupleHeaderGetXmin(heapTuple->t_data);
	stripeMetadata->aborted = !TransactionIdIsInProgress(entryXmin) &&
							  TransactionIdDidAbort(entryXmin);
	stripeMetadata->insertedByCurrentXact =
		TransactionIdIsCurrentTransactionId(entryXmin);

	CheckStripeMetadataConsistency(stripeMetadata);

	return stripeMetadata;
}


/*
 * DeleteMetadataRows removes the rows with given relfilenode from columnar
 * metadata tables.
 */
void
DeleteMetadataRows(RelFileLocator relfilelocator)
{
	/*
	 * During a restore for binary upgrade, metadata tables and indexes may or
	 * may not exist.
	 */
	if (IsBinaryUpgrade)
	{
		return;
	}

	uint64 storageId = LookupStorageId(relfilelocator);

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
				F_INT8EQ, Int64GetDatum(storageId));

	Relation metadataTable = try_relation_open(metadataTableId, AccessShareLock);
	if (metadataTable == NULL)
	{
		/* extension has been dropped */
		return;
	}

	bool indexOk = OidIsValid(storageIdIndexId);
	SysScanDesc scanDescriptor = systable_beginscan(metadataTable, storageIdIndexId,
													indexOk, NULL, 1, scanKey);

	static bool loggedSlowMetadataAccessWarning = false;
	if (!indexOk && !loggedSlowMetadataAccessWarning)
	{
		ereport(WARNING, (errmsg(SLOW_METADATA_ACCESS_WARNING,
								 "on a columnar metadata table")));
		loggedSlowMetadataAccessWarning = true;
	}

	ModifyState *modifyState = StartModifyRelation(metadataTable);

	HeapTuple heapTuple;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		DeleteTupleAndEnforceConstraints(modifyState, heapTuple);
	}

	systable_endscan(scanDescriptor);

	FinishModifyRelation(modifyState);

	table_close(metadataTable, AccessShareLock);
}


/*
 * StartModifyRelation allocates resources for modifications.
 */
static ModifyState *
StartModifyRelation(Relation rel)
{
	EState *estate = create_estate_for_relation(rel);

	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

	/* ExecSimpleRelationInsert, ... require caller to open indexes */
	ExecOpenIndices(resultRelInfo, false);

	ModifyState *modifyState = palloc(sizeof(ModifyState));
	modifyState->rel = rel;
	modifyState->estate = estate;
	modifyState->resultRelInfo = resultRelInfo;

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
	ExecSimpleRelationInsert(state->resultRelInfo, state->estate, slot);
}


/*
 * DeleteTupleAndEnforceConstraints deletes a tuple from a relation and
 * makes sure constraints (e.g. FK constraints) are enforced.
 */
static void
DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple)
{
	EState *estate = state->estate;
	ResultRelInfo *resultRelInfo = state->resultRelInfo;

	ItemPointer tid = &(heapTuple->t_self);
	simple_heap_delete(state->rel, tid);

	/* execute AFTER ROW DELETE Triggers to enforce constraints */
	ExecARDeleteTriggers(estate, resultRelInfo, tid, NULL, NULL, false);
}


/*
 * FinishModifyRelation cleans up resources after modifications are done.
 */
static void
FinishModifyRelation(ModifyState *state)
{
	ExecCloseIndices(state->resultRelInfo);

	AfterTriggerEndQuery(state->estate);
	ExecCloseResultRelations(state->estate);
	ExecCloseRangeTableRelations(state->estate);
	ExecResetTupleTable(state->estate->es_tupleTable, false);
	FreeExecutorState(state->estate);

	CommandCounterIncrement();
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

/* Prepare permission info on PG 16+ */
#if PG_VERSION_NUM >= PG_VERSION_16
	List *perminfos = NIL;
	addRTEPermissionInfo(&perminfos, rte);
#endif

/* Initialize the range table, with the right signature for each PG version */
#if PG_VERSION_NUM >= PG_VERSION_18

	/* PG 18+ needs four arguments (unpruned_relids) */
	ExecInitRangeTable(
		estate,
		list_make1(rte),
		perminfos,
		NULL  /* unpruned_relids: not used by columnar */
		);
#elif PG_VERSION_NUM >= PG_VERSION_16

	/* PG 16–17: three-arg signature (permInfos) */
	ExecInitRangeTable(
		estate,
		list_make1(rte),
		perminfos
		);
#else

	/* PG 15: two-arg signature */
	ExecInitRangeTable(
		estate,
		list_make1(rte)
		);
#endif

	estate->es_output_cid = GetCurrentCommandId(true);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}


/*
 * DatumToBytea serializes a datum into a bytea value.
 *
 * Since we don't want to limit datum size to RSIZE_MAX unnecessarily,
 * we use memcpy instead of memcpy_s several places in this function.
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

			memcpy(VARDATA(result), &tmp, attrForm->attlen); /* IGNORE-BANNED */
		}
		else
		{
			memcpy(VARDATA(result), DatumGetPointer(value), attrForm->attlen); /* IGNORE-BANNED */
		}
	}
	else
	{
		memcpy(VARDATA(result), DatumGetPointer(value), datumLength); /* IGNORE-BANNED */
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

	/*
	 * We use IGNORE-BANNED here since we don't want to limit datum size to
	 * RSIZE_MAX unnecessarily.
	 */
	memcpy(binaryDataCopy, VARDATA_ANY(bytes), VARSIZE_ANY_EXHDR(bytes)); /* IGNORE-BANNED */

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
	Oid namespace = get_namespace_oid("columnar_internal", true);

	/* if schema is earlier than 11.1-1 */
	if (!OidIsValid(namespace))
	{
		namespace = get_namespace_oid("columnar", false);
	}

	return namespace;
}


/*
 * LookupStorageId reads storage metapage to find the storage ID for the given relfilenode. It returns
 * false if the relation doesn't have a meta page yet.
 */
static uint64
LookupStorageId(RelFileLocator relfilelocator)
{
	Oid relationId = RelidByRelfilenumber(RelationTablespace_compat(relfilelocator),
										  RelationPhysicalIdentifierNumber_compat(
											  relfilelocator));

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

	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}

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

	BlockNumber nblocks = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);
	if (nblocks < 2)
	{
		ColumnarStorageInit(RelationGetSmgr(rel), ColumnarMetadataNewStorageId());
		return;
	}

	uint64 storageId = ColumnarStorageGetStorageId(rel, true);

	uint64 highestId;
	uint64 highestOffset;
	GetHighestUsedAddressAndId(storageId, &highestOffset, &highestId);

	uint64 reservedStripeId = highestId + 1;
	uint64 reservedOffset = highestOffset + 1;
	uint64 reservedRowNumber = GetHighestUsedRowNumber(storageId) + 1;
	ColumnarStorageUpdateCurrent(rel, isUpgrade, reservedStripeId,
								 reservedRowNumber, reservedOffset);
}


/*
 * GetHighestUsedRowNumber returns the highest used rowNumber for given
 * storageId. Returns COLUMNAR_INVALID_ROW_NUMBER if storage with
 * storageId has no stripes.
 * Note that normally we would use ColumnarStorageGetReservedRowNumber
 * to decide that. However, this function is designed to be used when
 * building the metapage itself during upgrades.
 */
static uint64
GetHighestUsedRowNumber(uint64 storageId)
{
	uint64 highestRowNumber = COLUMNAR_INVALID_ROW_NUMBER;

	List *stripeMetadataList = ReadDataFileStripeList(storageId,
													  GetTransactionSnapshot());
	StripeMetadata *stripeMetadata = NULL;
	foreach_declared_ptr(stripeMetadata, stripeMetadataList)
	{
		highestRowNumber = Max(highestRowNumber,
							   StripeGetHighestRowNumber(stripeMetadata));
	}

	return highestRowNumber;
}
