/*-------------------------------------------------------------------------
 *
 * cstore_metadata_tables.c
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "citus_version.h"
#include "columnar/cstore.h"
#include "columnar/cstore_version_compat.h"

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


/*
 * Content of the first page in main fork, which stores metadata at file
 * level.
 */
typedef struct ColumnarMetapage
{
	/*
	 * Store version of file format used, so we can detect files from
	 * previous versions if we change file format.
	 */
	int versionMajor;
	int versionMinor;

	/*
	 * Each of the metadata table rows are identified by a storageId.
	 * We store it also in the main fork so we can link metadata rows
	 * with data files.
	 */
	uint64 storageId;
} ColumnarMetapage;


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
static Oid CStoreStripesRelationId(void);
static Oid CStoreStripesIndexRelationId(void);
static Oid ColumnarOptionsRelationId(void);
static Oid ColumnarOptionsIndexRegclass(void);
static Oid CStoreSkipNodesRelationId(void);
static Oid CStoreSkipNodesIndexRelationId(void);
static Oid CStoreNamespaceId(void);
static ModifyState * StartModifyRelation(Relation rel);
static void InsertTupleAndEnforceConstraints(ModifyState *state, Datum *values,
											 bool *nulls);
static void DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple);
static void FinishModifyRelation(ModifyState *state);
static EState * create_estate_for_relation(Relation rel);
static bytea * DatumToBytea(Datum value, Form_pg_attribute attrForm);
static Datum ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm);
static ColumnarMetapage * InitMetapage(Relation relation);
static ColumnarMetapage * ReadMetapage(RelFileNode relfilenode, bool missingOk);
static uint64 GetNextStorageId(void);
static bool WriteColumnarOptions(Oid regclass, ColumnarOptions *options, bool overwrite);

PG_FUNCTION_INFO_V1(columnar_relation_storageid);

/* constants for columnar.options */
#define Natts_cstore_options 5
#define Anum_cstore_options_regclass 1
#define Anum_cstore_options_chunk_row_count 2
#define Anum_cstore_options_stripe_row_count 3
#define Anum_cstore_options_compression_level 4
#define Anum_cstore_options_compression 5

/* ----------------
 *		columnar.options definition.
 * ----------------
 */
typedef struct FormData_cstore_options
{
	Oid regclass;
	int32 chunk_row_count;
	int32 stripe_row_count;
	int32 compressionLevel;
	NameData compression;

#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_cstore_options;
typedef FormData_cstore_options *Form_cstore_options;


/* constants for cstore_stripe */
#define Natts_cstore_stripes 8
#define Anum_cstore_stripes_storageid 1
#define Anum_cstore_stripes_stripe 2
#define Anum_cstore_stripes_file_offset 3
#define Anum_cstore_stripes_data_length 4
#define Anum_cstore_stripes_column_count 5
#define Anum_cstore_stripes_chunk_count 6
#define Anum_cstore_stripes_chunk_row_count 7
#define Anum_cstore_stripes_row_count 8

/* constants for cstore_skipnodes */
#define Natts_cstore_skipnodes 14
#define Anum_cstore_skipnodes_storageid 1
#define Anum_cstore_skipnodes_stripe 2
#define Anum_cstore_skipnodes_attr 3
#define Anum_cstore_skipnodes_chunk 4
#define Anum_cstore_skipnodes_row_count 5
#define Anum_cstore_skipnodes_minimum_value 6
#define Anum_cstore_skipnodes_maximum_value 7
#define Anum_cstore_skipnodes_value_stream_offset 8
#define Anum_cstore_skipnodes_value_stream_length 9
#define Anum_cstore_skipnodes_exists_stream_offset 10
#define Anum_cstore_skipnodes_exists_stream_length 11
#define Anum_cstore_skipnodes_value_compression_type 12
#define Anum_cstore_skipnodes_value_compression_level 13
#define Anum_cstore_skipnodes_value_decompressed_size 14


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
		.chunkRowCount = cstore_chunk_row_count,
		.stripeRowCount = cstore_stripe_row_count,
		.compressionType = cstore_compression,
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

	bool nulls[Natts_cstore_options] = { 0 };
	Datum values[Natts_cstore_options] = {
		ObjectIdGetDatum(regclass),
		Int32GetDatum(options->chunkRowCount),
		Int32GetDatum(options->stripeRowCount),
		Int32GetDatum(options->compressionLevel),
		0, /* to be filled below */
	};

	NameData compressionName = { 0 };
	namestrcpy(&compressionName, CompressionTypeStr(options->compressionType));
	values[Anum_cstore_options_compression - 1] = NameGetDatum(&compressionName);

	/* create heap tuple and insert into catalog table */
	Relation columnarOptions = relation_open(ColumnarOptionsRelationId(),
											 RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(columnarOptions);

	/* find existing item to perform update if exist */
	ScanKeyData scanKey[1] = { 0 };
	ScanKeyInit(&scanKey[0], Anum_cstore_options_regclass, BTEqualStrategyNumber, F_OIDEQ,
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
			bool update[Natts_cstore_options] = { 0 };
			update[Anum_cstore_options_chunk_row_count - 1] = true;
			update[Anum_cstore_options_stripe_row_count - 1] = true;
			update[Anum_cstore_options_compression_level - 1] = true;
			update[Anum_cstore_options_compression - 1] = true;

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
	index_close(index, NoLock);
	relation_close(columnarOptions, NoLock);

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
	ScanKeyInit(&scanKey[0], Anum_cstore_options_regclass, BTEqualStrategyNumber, F_OIDEQ,
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
	index_close(index, NoLock);
	relation_close(columnarOptions, NoLock);

	return result;
}


bool
ReadColumnarOptions(Oid regclass, ColumnarOptions *options)
{
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_cstore_options_regclass, BTEqualStrategyNumber, F_OIDEQ,
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
		heap_close(columnarOptions, NoLock);

		/* extension has been dropped */
		return false;
	}

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		Form_cstore_options tupOptions = (Form_cstore_options) GETSTRUCT(heapTuple);

		options->chunkRowCount = tupOptions->chunk_row_count;
		options->stripeRowCount = tupOptions->stripe_row_count;
		options->compressionLevel = tupOptions->compressionLevel;
		options->compressionType = ParseCompressionType(NameStr(tupOptions->compression));
	}
	else
	{
		/* populate options with system defaults */
		options->compressionType = cstore_compression;
		options->stripeRowCount = cstore_stripe_row_count;
		options->chunkRowCount = cstore_chunk_row_count;
		options->compressionLevel = columnar_compression_level;
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	relation_close(columnarOptions, NoLock);

	return true;
}


/*
 * SaveStripeSkipList saves StripeSkipList for a given stripe as rows
 * of cstore_skipnodes.
 */
void
SaveStripeSkipList(RelFileNode relfilenode, uint64 stripe, StripeSkipList *stripeSkipList,
				   TupleDesc tupleDescriptor)
{
	uint32 columnIndex = 0;
	uint32 chunkIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

	ColumnarMetapage *metapage = ReadMetapage(relfilenode, false);
	Oid cstoreSkipNodesOid = CStoreSkipNodesRelationId();
	Relation cstoreSkipNodes = heap_open(cstoreSkipNodesOid, RowExclusiveLock);
	ModifyState *modifyState = StartModifyRelation(cstoreSkipNodes);

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
		{
			ColumnChunkSkipNode *skipNode =
				&stripeSkipList->chunkSkipNodeArray[columnIndex][chunkIndex];

			Datum values[Natts_cstore_skipnodes] = {
				UInt64GetDatum(metapage->storageId),
				Int64GetDatum(stripe),
				Int32GetDatum(columnIndex + 1),
				Int32GetDatum(chunkIndex),
				Int64GetDatum(skipNode->rowCount),
				0, /* to be filled below */
				0, /* to be filled below */
				Int64GetDatum(skipNode->valueChunkOffset),
				Int64GetDatum(skipNode->valueLength),
				Int64GetDatum(skipNode->existsChunkOffset),
				Int64GetDatum(skipNode->existsLength),
				Int32GetDatum(skipNode->valueCompressionType),
				Int32GetDatum(skipNode->valueCompressionLevel),
				Int64GetDatum(skipNode->decompressedValueSize)
			};

			bool nulls[Natts_cstore_skipnodes] = { false };

			if (skipNode->hasMinMax)
			{
				values[Anum_cstore_skipnodes_minimum_value - 1] =
					PointerGetDatum(DatumToBytea(skipNode->minimumValue,
												 &tupleDescriptor->attrs[columnIndex]));
				values[Anum_cstore_skipnodes_maximum_value - 1] =
					PointerGetDatum(DatumToBytea(skipNode->maximumValue,
												 &tupleDescriptor->attrs[columnIndex]));
			}
			else
			{
				nulls[Anum_cstore_skipnodes_minimum_value - 1] = true;
				nulls[Anum_cstore_skipnodes_maximum_value - 1] = true;
			}

			InsertTupleAndEnforceConstraints(modifyState, values, nulls);
		}
	}

	FinishModifyRelation(modifyState);
	heap_close(cstoreSkipNodes, NoLock);

	CommandCounterIncrement();
}


/*
 * ReadStripeSkipList fetches StripeSkipList for a given stripe.
 */
StripeSkipList *
ReadStripeSkipList(RelFileNode relfilenode, uint64 stripe, TupleDesc tupleDescriptor,
				   uint32 chunkCount)
{
	int32 columnIndex = 0;
	HeapTuple heapTuple = NULL;
	uint32 columnCount = tupleDescriptor->natts;
	ScanKeyData scanKey[2];

	ColumnarMetapage *metapage = ReadMetapage(relfilenode, false);

	Oid cstoreSkipNodesOid = CStoreSkipNodesRelationId();
	Relation cstoreSkipNodes = heap_open(cstoreSkipNodesOid, AccessShareLock);
	Relation index = index_open(CStoreSkipNodesIndexRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_cstore_skipnodes_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(metapage->storageId));
	ScanKeyInit(&scanKey[1], Anum_cstore_skipnodes_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(stripe));

	SysScanDesc scanDescriptor = systable_beginscan_ordered(cstoreSkipNodes, index, NULL,
															2, scanKey);

	StripeSkipList *skipList = palloc0(sizeof(StripeSkipList));
	skipList->chunkCount = chunkCount;
	skipList->columnCount = columnCount;
	skipList->chunkSkipNodeArray = palloc0(columnCount * sizeof(ColumnChunkSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		skipList->chunkSkipNodeArray[columnIndex] =
			palloc0(chunkCount * sizeof(ColumnChunkSkipNode));
	}

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_cstore_skipnodes];
		bool isNullArray[Natts_cstore_skipnodes];

		heap_deform_tuple(heapTuple, RelationGetDescr(cstoreSkipNodes), datumArray,
						  isNullArray);

		int32 attr = DatumGetInt32(datumArray[Anum_cstore_skipnodes_attr - 1]);
		int32 chunkIndex = DatumGetInt32(datumArray[Anum_cstore_skipnodes_chunk - 1]);

		if (attr <= 0 || attr > columnCount)
		{
			ereport(ERROR, (errmsg("invalid stripe skipnode entry"),
							errdetail("Attribute number out of range: %d", attr)));
		}

		if (chunkIndex < 0 || chunkIndex >= chunkCount)
		{
			ereport(ERROR, (errmsg("invalid stripe skipnode entry"),
							errdetail("Chunk number out of range: %d", chunkIndex)));
		}

		columnIndex = attr - 1;

		ColumnChunkSkipNode *skipNode =
			&skipList->chunkSkipNodeArray[columnIndex][chunkIndex];
		skipNode->rowCount = DatumGetInt64(datumArray[Anum_cstore_skipnodes_row_count -
													  1]);
		skipNode->valueChunkOffset =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_value_stream_offset - 1]);
		skipNode->valueLength =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_value_stream_length - 1]);
		skipNode->existsChunkOffset =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_exists_stream_offset - 1]);
		skipNode->existsLength =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_exists_stream_length - 1]);
		skipNode->valueCompressionType =
			DatumGetInt32(datumArray[Anum_cstore_skipnodes_value_compression_type - 1]);
		skipNode->valueCompressionLevel =
			DatumGetInt32(datumArray[Anum_cstore_skipnodes_value_compression_level - 1]);
		skipNode->decompressedValueSize =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_value_decompressed_size - 1]);

		if (isNullArray[Anum_cstore_skipnodes_minimum_value - 1] ||
			isNullArray[Anum_cstore_skipnodes_maximum_value - 1])
		{
			skipNode->hasMinMax = false;
		}
		else
		{
			bytea *minValue = DatumGetByteaP(
				datumArray[Anum_cstore_skipnodes_minimum_value - 1]);
			bytea *maxValue = DatumGetByteaP(
				datumArray[Anum_cstore_skipnodes_maximum_value - 1]);

			skipNode->minimumValue =
				ByteaToDatum(minValue, &tupleDescriptor->attrs[columnIndex]);
			skipNode->maximumValue =
				ByteaToDatum(maxValue, &tupleDescriptor->attrs[columnIndex]);

			skipNode->hasMinMax = true;
		}
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreSkipNodes, NoLock);

	return skipList;
}


/*
 * InsertStripeMetadataRow adds a row to cstore_stripes.
 */
static void
InsertStripeMetadataRow(uint64 storageId, StripeMetadata *stripe)
{
	bool nulls[Natts_cstore_stripes] = { 0 };
	Datum values[Natts_cstore_stripes] = {
		UInt64GetDatum(storageId),
		Int64GetDatum(stripe->id),
		Int64GetDatum(stripe->fileOffset),
		Int64GetDatum(stripe->dataLength),
		Int32GetDatum(stripe->columnCount),
		Int32GetDatum(stripe->chunkCount),
		Int32GetDatum(stripe->chunkRowCount),
		Int64GetDatum(stripe->rowCount)
	};

	Oid cstoreStripesOid = CStoreStripesRelationId();
	Relation cstoreStripes = heap_open(cstoreStripesOid, RowExclusiveLock);

	ModifyState *modifyState = StartModifyRelation(cstoreStripes);

	InsertTupleAndEnforceConstraints(modifyState, values, nulls);

	FinishModifyRelation(modifyState);

	CommandCounterIncrement();

	heap_close(cstoreStripes, NoLock);
}


/*
 * StripesForRelfilenode returns a list of StripeMetadata for stripes
 * of the given relfilenode.
 */
List *
StripesForRelfilenode(RelFileNode relfilenode)
{
	ColumnarMetapage *metapage = ReadMetapage(relfilenode, true);
	if (metapage == NULL)
	{
		/* empty relation */
		return NIL;
	}


	return ReadDataFileStripeList(metapage->storageId, GetTransactionSnapshot());
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
	uint64 highestUsedAddress = 0;
	uint64 highestUsedId = 0;
	ColumnarMetapage *metapage = ReadMetapage(relfilenode, true);

	/* empty data file? */
	if (metapage == NULL)
	{
		return 0;
	}

	GetHighestUsedAddressAndId(metapage->storageId, &highestUsedAddress, &highestUsedId);

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
	*highestUsedAddress = CSTORE_BYTES_PER_PAGE;

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
 * and inserts it into cstore_stripes. It is guaranteed that concurrent
 * writes won't overwrite the returned stripe.
 */
StripeMetadata
ReserveStripe(Relation rel, uint64 sizeBytes,
			  uint64 rowCount, uint64 columnCount,
			  uint64 chunkCount, uint64 chunkRowCount)
{
	StripeMetadata stripe = { 0 };
	uint64 currLogicalHigh = 0;
	uint64 highestId = 0;

	/*
	 * We take ShareUpdateExclusiveLock here, so two space
	 * reservations conflict, space reservation <-> vacuum
	 * conflict, but space reservation doesn't conflict with
	 * reads & writes.
	 */
	LockRelation(rel, ShareUpdateExclusiveLock);

	RelFileNode relfilenode = rel->rd_node;


	/*
	 * If this is the first stripe for this relation, initialize the
	 * metapage, otherwise use the previously initialized metapage.
	 */
	ColumnarMetapage *metapage = ReadMetapage(relfilenode, true);
	if (metapage == NULL)
	{
		metapage = InitMetapage(rel);
	}

	GetHighestUsedAddressAndId(metapage->storageId, &currLogicalHigh, &highestId);
	SmgrAddr currSmgrHigh = logical_to_smgr(currLogicalHigh);

	SmgrAddr resSmgrStart = next_block_start(currSmgrHigh);
	uint64 resLogicalStart = smgr_to_logical(resSmgrStart);

	uint64 resLogicalEnd = resLogicalStart + sizeBytes - 1;
	SmgrAddr resSmgrEnd = logical_to_smgr(resLogicalEnd);

	RelationOpenSmgr(rel);
	uint64 nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);

	while (resSmgrEnd.blockno >= nblocks)
	{
		Buffer newBuffer = ReadBuffer(rel, P_NEW);
		ReleaseBuffer(newBuffer);
		nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	}

	RelationCloseSmgr(rel);

	stripe.fileOffset = resLogicalStart;
	stripe.dataLength = sizeBytes;
	stripe.chunkCount = chunkCount;
	stripe.chunkRowCount = chunkRowCount;
	stripe.columnCount = columnCount;
	stripe.rowCount = rowCount;
	stripe.id = highestId + 1;

	InsertStripeMetadataRow(metapage->storageId, &stripe);

	UnlockRelation(rel, ShareUpdateExclusiveLock);

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

	ScanKeyInit(&scanKey[0], Anum_cstore_stripes_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(storageId));

	Oid cstoreStripesOid = CStoreStripesRelationId();

	Relation cstoreStripes = heap_open(cstoreStripesOid, AccessShareLock);
	Relation index = index_open(CStoreStripesIndexRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(cstoreStripes);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(cstoreStripes, index,
															snapshot, 1,
															scanKey);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_cstore_stripes];
		bool isNullArray[Natts_cstore_stripes];

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		StripeMetadata *stripeMetadata = palloc0(sizeof(StripeMetadata));
		stripeMetadata->id = DatumGetInt64(datumArray[Anum_cstore_stripes_stripe - 1]);
		stripeMetadata->fileOffset = DatumGetInt64(
			datumArray[Anum_cstore_stripes_file_offset - 1]);
		stripeMetadata->dataLength = DatumGetInt64(
			datumArray[Anum_cstore_stripes_data_length - 1]);
		stripeMetadata->columnCount = DatumGetInt32(
			datumArray[Anum_cstore_stripes_column_count - 1]);
		stripeMetadata->chunkCount = DatumGetInt32(
			datumArray[Anum_cstore_stripes_chunk_count - 1]);
		stripeMetadata->chunkRowCount = DatumGetInt32(
			datumArray[Anum_cstore_stripes_chunk_row_count - 1]);
		stripeMetadata->rowCount = DatumGetInt64(
			datumArray[Anum_cstore_stripes_row_count - 1]);

		stripeMetadataList = lappend(stripeMetadataList, stripeMetadata);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreStripes, NoLock);

	return stripeMetadataList;
}


/*
 * DeleteMetadataRows removes the rows with given relfilenode from cstore_stripes.
 */
void
DeleteMetadataRows(RelFileNode relfilenode)
{
	ScanKeyData scanKey[1];

	/*
	 * During a restore for binary upgrade, metadata tables and indexes may or
	 * may not exist.
	 */
	if (IsBinaryUpgrade)
	{
		return;
	}

	ColumnarMetapage *metapage = ReadMetapage(relfilenode, true);
	if (metapage == NULL)
	{
		/*
		 * No data has been written to this storage yet, so there is no
		 * associated metadata yet.
		 */
		return;
	}

	ScanKeyInit(&scanKey[0], Anum_cstore_stripes_storageid,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(metapage->storageId));

	Oid cstoreStripesOid = CStoreStripesRelationId();
	Relation cstoreStripes = try_relation_open(cstoreStripesOid, AccessShareLock);
	if (cstoreStripes == NULL)
	{
		/* extension has been dropped */
		return;
	}

	Relation index = index_open(CStoreStripesIndexRelationId(), AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(cstoreStripes, index, NULL,
															1, scanKey);

	ModifyState *modifyState = StartModifyRelation(cstoreStripes);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		DeleteTupleAndEnforceConstraints(modifyState, heapTuple);
		heapTuple = systable_getnext(scanDescriptor);
	}

	FinishModifyRelation(modifyState);

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreStripes, NoLock);
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

#if PG_VERSION_NUM >= 120000
	TupleTableSlot *slot = ExecInitExtraTupleSlot(state->estate, tupleDescriptor,
												  &TTSOpsHeapTuple);

	ExecStoreHeapTuple(tuple, slot, false);
#else
	TupleTableSlot *slot = ExecInitExtraTupleSlot(state->estate, tupleDescriptor);
	ExecStoreTuple(tuple, slot, InvalidBuffer, false);
#endif

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
	ResultRelInfo *resultRelInfo;

	EState *estate = CreateExecutorState();

	RangeTblEntry *rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
#if PG_VERSION_NUM >= 120000
	rte->rellockmode = AccessShareLock;
	ExecInitRangeTable(estate, list_make1(rte));
#endif

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	estate->es_output_cid = GetCurrentCommandId(true);

#if PG_VERSION_NUM < 120000

	/* Triggers might need a slot */
	if (resultRelInfo->ri_TrigDesc)
	{
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, NULL);
	}
#endif

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
			store_att_byval(VARDATA(result), value, attrForm->attlen);
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
 * CStoreStripesRelationId returns relation id of cstore_stripes.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreStripesRelationId(void)
{
	return get_relname_relid("columnar_stripes", CStoreNamespaceId());
}


/*
 * CStoreStripesIndexRelationId returns relation id of cstore_stripes_idx.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreStripesIndexRelationId(void)
{
	return get_relname_relid("columnar_stripes_pkey", CStoreNamespaceId());
}


/*
 * ColumnarOptionsRelationId returns relation id of columnar.options.
 */
static Oid
ColumnarOptionsRelationId(void)
{
	return get_relname_relid("options", CStoreNamespaceId());
}


/*
 * ColumnarOptionsIndexRegclass returns relation id of columnar.options_pkey.
 */
static Oid
ColumnarOptionsIndexRegclass(void)
{
	return get_relname_relid("options_pkey", CStoreNamespaceId());
}


/*
 * CStoreSkipNodesRelationId returns relation id of cstore_skipnodes.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreSkipNodesRelationId(void)
{
	return get_relname_relid("columnar_skipnodes", CStoreNamespaceId());
}


/*
 * CStoreSkipNodesIndexRelationId returns relation id of cstore_skipnodes_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreSkipNodesIndexRelationId(void)
{
	return get_relname_relid("columnar_skipnodes_pkey", CStoreNamespaceId());
}


/*
 * CStoreNamespaceId returns namespace id of the schema we store cstore
 * related tables.
 */
static Oid
CStoreNamespaceId(void)
{
	return get_namespace_oid("columnar", false);
}


/*
 * ReadMetapage reads metapage for the given relfilenode. It returns
 * false if the relation doesn't have a meta page yet.
 */
static ColumnarMetapage *
ReadMetapage(RelFileNode relfilenode, bool missingOk)
{
	StringInfo metapageBuffer = NULL;
	Oid relationId = RelidByRelfilenode(relfilenode.spcNode,
										relfilenode.relNode);
	if (OidIsValid(relationId))
	{
		Relation relation = relation_open(relationId, NoLock);

		RelationOpenSmgr(relation);
		int nblocks = smgrnblocks(relation->rd_smgr, MAIN_FORKNUM);
		RelationCloseSmgr(relation);

		if (nblocks != 0)
		{
			metapageBuffer = ReadFromSmgr(relation, 0, sizeof(ColumnarMetapage));
		}

		relation_close(relation, NoLock);
	}

	if (metapageBuffer == NULL)
	{
		if (!missingOk)
		{
			elog(ERROR, "columnar metapage was not found");
		}

		return NULL;
	}

	ColumnarMetapage *metapage = palloc0(sizeof(ColumnarMetapage));
	memcpy_s((void *) metapage, sizeof(ColumnarMetapage),
			 metapageBuffer->data, sizeof(ColumnarMetapage));

	return metapage;
}


/*
 * InitMetapage initializes metapage for the given relation.
 */
static ColumnarMetapage *
InitMetapage(Relation relation)
{
	/*
	 * If we init metapage during upgrade, we might override the
	 * pre-upgrade storage id which will render pre-upgrade data
	 * invisible.
	 */
	Assert(!IsBinaryUpgrade);

	ColumnarMetapage *metapage = palloc0(sizeof(ColumnarMetapage));
	metapage->storageId = GetNextStorageId();
	metapage->versionMajor = CSTORE_VERSION_MAJOR;
	metapage->versionMinor = CSTORE_VERSION_MINOR;

	/* create the first block */
	Buffer newBuffer = ReadBuffer(relation, P_NEW);
	ReleaseBuffer(newBuffer);

	Assert(sizeof(ColumnarMetapage) <= BLCKSZ - SizeOfPageHeaderData);
	WriteToSmgr(relation, 0, (char *) metapage, sizeof(ColumnarMetapage));

	return metapage;
}


/*
 * GetNextStorageId returns the next value from the storage id sequence.
 */
static uint64
GetNextStorageId(void)
{
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Oid sequenceId = get_relname_relid("storageid_seq", CStoreNamespaceId());
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	/*
	 * Not all users have update access to the sequence, so switch
	 * security context.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Generate new and unique storage id from sequence.
	 */
	Datum storageIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	uint64 storageId = DatumGetInt64(storageIdDatum);

	return storageId;
}


/*
 * columnar_relation_storageid returns storage id associated with the
 * given relation id, or -1 if there is no associated storage id yet.
 */
Datum
columnar_relation_storageid(PG_FUNCTION_ARGS)
{
	uint64 storageId = -1;

#if HAS_TABLEAM
	Oid relationId = PG_GETARG_OID(0);
	Relation relation = relation_open(relationId, AccessShareLock);
	if (IsCStoreTableAmTable(relationId))
	{
		ColumnarMetapage *metadata = ReadMetapage(relation->rd_node, true);
		if (metadata != NULL)
		{
			storageId = metadata->storageId;
		}
	}

	relation_close(relation, AccessShareLock);
#endif

	PG_RETURN_INT64(storageId);
}
