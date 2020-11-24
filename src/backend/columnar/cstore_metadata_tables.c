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
static Oid CStoreDataFilesRelationId(void);
static Oid CStoreDataFilesIndexRelationId(void);
static Oid CStoreSkipNodesRelationId(void);
static Oid CStoreSkipNodesIndexRelationId(void);
static Oid CStoreNamespaceId(void);
static bool ReadCStoreDataFiles(uint64 storageId, DataFileMetadata *metadata);
static ModifyState * StartModifyRelation(Relation rel);
static void InsertTupleAndEnforceConstraints(ModifyState *state, Datum *values,
											 bool *nulls);
static void DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple);
static void FinishModifyRelation(ModifyState *state);
static EState * create_estate_for_relation(Relation rel);
static bytea * DatumToBytea(Datum value, Form_pg_attribute attrForm);
static Datum ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm);
static ColumnarMetapage InitMetapage(Relation relation);
static bool ReadMetapage(RelFileNode relfilenode, ColumnarMetapage *metapage);
static uint64 GetNextStorageId(void);

PG_FUNCTION_INFO_V1(columnar_relation_storageid);


/* constants for cstore_table */
#define Natts_cstore_data_files 6
#define Anum_cstore_data_files_storageid 1
#define Anum_cstore_data_files_block_row_count 2
#define Anum_cstore_data_files_stripe_row_count 3
#define Anum_cstore_data_files_compression 4
#define Anum_cstore_data_files_version_major 5
#define Anum_cstore_data_files_version_minor 6

/* ----------------
 *		cstore.cstore_data_files definition.
 * ----------------
 */
typedef struct FormData_cstore_data_files
{
	uint64 storageid;
	int32 block_row_count;
	int32 stripe_row_count;
	NameData compression;
	int64 version_major;
	int64 version_minor;

#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_cstore_data_files;
typedef FormData_cstore_data_files *Form_cstore_data_files;

/* constants for cstore_stripe */
#define Natts_cstore_stripes 8
#define Anum_cstore_stripes_storageid 1
#define Anum_cstore_stripes_stripe 2
#define Anum_cstore_stripes_file_offset 3
#define Anum_cstore_stripes_data_length 4
#define Anum_cstore_stripes_column_count 5
#define Anum_cstore_stripes_block_count 6
#define Anum_cstore_stripes_block_row_count 7
#define Anum_cstore_stripes_row_count 8

/* constants for cstore_skipnodes */
#define Natts_cstore_skipnodes 12
#define Anum_cstore_skipnodes_storageid 1
#define Anum_cstore_skipnodes_stripe 2
#define Anum_cstore_skipnodes_attr 3
#define Anum_cstore_skipnodes_block 4
#define Anum_cstore_skipnodes_row_count 5
#define Anum_cstore_skipnodes_minimum_value 6
#define Anum_cstore_skipnodes_maximum_value 7
#define Anum_cstore_skipnodes_value_stream_offset 8
#define Anum_cstore_skipnodes_value_stream_length 9
#define Anum_cstore_skipnodes_exists_stream_offset 10
#define Anum_cstore_skipnodes_exists_stream_length 11
#define Anum_cstore_skipnodes_value_compression_type 12


/*
 * InitCStoreDataFileMetadata adds a record for the given relation
 * in cstore_data_files.
 */
void
InitCStoreDataFileMetadata(Relation relation, int blockRowCount,
						   int stripeRowCount,
						   CompressionType compression)
{
	ColumnarMetapage metapage = InitMetapage(relation);

	bool nulls[Natts_cstore_data_files] = { 0 };
	Datum values[Natts_cstore_data_files] = {
		UInt64GetDatum(metapage.storageId),
		Int32GetDatum(blockRowCount),
		Int32GetDatum(stripeRowCount),
		0, /* to be filled below */
		Int32GetDatum(CSTORE_VERSION_MAJOR),
		Int32GetDatum(CSTORE_VERSION_MINOR)
	};

	NameData compressionName = { 0 };
	namestrcpy(&compressionName, CompressionTypeStr(compression));
	values[Anum_cstore_data_files_compression - 1] = NameGetDatum(&compressionName);

	DeleteDataFileMetadataRowIfExists(relation->rd_node);

	Oid cstoreDataFilesOid = CStoreDataFilesRelationId();
	Relation cstoreDataFiles = heap_open(cstoreDataFilesOid, RowExclusiveLock);

	ModifyState *modifyState = StartModifyRelation(cstoreDataFiles);
	InsertTupleAndEnforceConstraints(modifyState, values, nulls);
	FinishModifyRelation(modifyState);

	CommandCounterIncrement();

	heap_close(cstoreDataFiles, NoLock);
}


void
UpdateCStoreDataFileMetadata(RelFileNode relfilenode, int blockRowCount, int
							 stripeRowCount,
							 CompressionType compression)
{
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;
	Datum values[Natts_cstore_data_files] = { 0 };
	bool isnull[Natts_cstore_data_files] = { 0 };
	bool replace[Natts_cstore_data_files] = { 0 };
	bool changed = false;

	ColumnarMetapage metapage;
	if (!ReadMetapage(relfilenode, &metapage))
	{
		elog(ERROR, "metapage was not found");
	}

	Relation cstoreDataFiles = heap_open(CStoreDataFilesRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(cstoreDataFiles);

	ScanKeyInit(&scanKey[0], Anum_cstore_data_files_storageid, BTEqualStrategyNumber,
				F_INT8EQ, UInt64GetDatum(metapage.storageId));

	SysScanDesc scanDescriptor = systable_beginscan(cstoreDataFiles,
													CStoreDataFilesIndexRelationId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (heapTuple == NULL)
	{
		ereport(ERROR, (errmsg("relfilenode %d doesn't belong to a cstore table",
							   relfilenode.relNode)));
	}

	Form_cstore_data_files metadata = (Form_cstore_data_files) GETSTRUCT(heapTuple);

	if (metadata->block_row_count != blockRowCount)
	{
		values[Anum_cstore_data_files_block_row_count - 1] = Int32GetDatum(blockRowCount);
		isnull[Anum_cstore_data_files_block_row_count - 1] = false;
		replace[Anum_cstore_data_files_block_row_count - 1] = true;
		changed = true;
	}

	if (metadata->stripe_row_count != stripeRowCount)
	{
		values[Anum_cstore_data_files_stripe_row_count - 1] = Int32GetDatum(
			stripeRowCount);
		isnull[Anum_cstore_data_files_stripe_row_count - 1] = false;
		replace[Anum_cstore_data_files_stripe_row_count - 1] = true;
		changed = true;
	}

	if (ParseCompressionType(NameStr(metadata->compression)) != compression)
	{
		Name compressionName = palloc0(sizeof(NameData));
		namestrcpy(compressionName, CompressionTypeStr(compression));
		values[Anum_cstore_data_files_compression - 1] = NameGetDatum(compressionName);
		isnull[Anum_cstore_data_files_compression - 1] = false;
		replace[Anum_cstore_data_files_compression - 1] = true;
		changed = true;
	}

	if (changed)
	{
		heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
									  replace);

		CatalogTupleUpdate(cstoreDataFiles, &heapTuple->t_self, heapTuple);

		CommandCounterIncrement();
	}

	systable_endscan(scanDescriptor);

	heap_close(cstoreDataFiles, NoLock);
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
	uint32 blockIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

	ColumnarMetapage metapage;
	if (!ReadMetapage(relfilenode, &metapage))
	{
		elog(WARNING, "metapage was not found");
	}

	Oid cstoreSkipNodesOid = CStoreSkipNodesRelationId();
	Relation cstoreSkipNodes = heap_open(cstoreSkipNodesOid, RowExclusiveLock);
	ModifyState *modifyState = StartModifyRelation(cstoreSkipNodes);

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			ColumnBlockSkipNode *skipNode =
				&stripeSkipList->blockSkipNodeArray[columnIndex][blockIndex];

			Datum values[Natts_cstore_skipnodes] = {
				UInt64GetDatum(metapage.storageId),
				Int64GetDatum(stripe),
				Int32GetDatum(columnIndex + 1),
				Int32GetDatum(blockIndex),
				Int64GetDatum(skipNode->rowCount),
				0, /* to be filled below */
				0, /* to be filled below */
				Int64GetDatum(skipNode->valueBlockOffset),
				Int64GetDatum(skipNode->valueLength),
				Int64GetDatum(skipNode->existsBlockOffset),
				Int64GetDatum(skipNode->existsLength),
				Int32GetDatum(skipNode->valueCompressionType)
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
				   uint32 blockCount)
{
	int32 columnIndex = 0;
	HeapTuple heapTuple = NULL;
	uint32 columnCount = tupleDescriptor->natts;
	ScanKeyData scanKey[2];

	ColumnarMetapage metapage;
	if (!ReadMetapage(relfilenode, &metapage))
	{
		elog(WARNING, "metapage was not found");
	}

	Oid cstoreSkipNodesOid = CStoreSkipNodesRelationId();
	Relation cstoreSkipNodes = heap_open(cstoreSkipNodesOid, AccessShareLock);
	Relation index = index_open(CStoreSkipNodesIndexRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_cstore_skipnodes_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(metapage.storageId));
	ScanKeyInit(&scanKey[1], Anum_cstore_skipnodes_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(stripe));

	SysScanDesc scanDescriptor = systable_beginscan_ordered(cstoreSkipNodes, index, NULL,
															2, scanKey);

	StripeSkipList *skipList = palloc0(sizeof(StripeSkipList));
	skipList->blockCount = blockCount;
	skipList->columnCount = columnCount;
	skipList->blockSkipNodeArray = palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		skipList->blockSkipNodeArray[columnIndex] =
			palloc0(blockCount * sizeof(ColumnBlockSkipNode));
	}

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_cstore_skipnodes];
		bool isNullArray[Natts_cstore_skipnodes];

		heap_deform_tuple(heapTuple, RelationGetDescr(cstoreSkipNodes), datumArray,
						  isNullArray);

		int32 attr = DatumGetInt32(datumArray[Anum_cstore_skipnodes_attr - 1]);
		int32 blockIndex = DatumGetInt32(datumArray[Anum_cstore_skipnodes_block - 1]);

		if (attr <= 0 || attr > columnCount)
		{
			ereport(ERROR, (errmsg("invalid stripe skipnode entry"),
							errdetail("Attribute number out of range: %d", attr)));
		}

		if (blockIndex < 0 || blockIndex >= blockCount)
		{
			ereport(ERROR, (errmsg("invalid stripe skipnode entry"),
							errdetail("Block number out of range: %d", blockIndex)));
		}

		columnIndex = attr - 1;

		ColumnBlockSkipNode *skipNode =
			&skipList->blockSkipNodeArray[columnIndex][blockIndex];
		skipNode->rowCount = DatumGetInt64(datumArray[Anum_cstore_skipnodes_row_count -
													  1]);
		skipNode->valueBlockOffset =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_value_stream_offset - 1]);
		skipNode->valueLength =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_value_stream_length - 1]);
		skipNode->existsBlockOffset =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_exists_stream_offset - 1]);
		skipNode->existsLength =
			DatumGetInt64(datumArray[Anum_cstore_skipnodes_exists_stream_length - 1]);
		skipNode->valueCompressionType =
			DatumGetInt32(datumArray[Anum_cstore_skipnodes_value_compression_type - 1]);

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
		Int32GetDatum(stripe->blockCount),
		Int32GetDatum(stripe->blockRowCount),
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
 * ReadDataFileMetadata constructs DataFileMetadata for a given relfilenode by reading
 * from cstore_data_files and cstore_stripes.
 */
DataFileMetadata *
ReadDataFileMetadata(RelFileNode relfilenode, bool missingOk)
{
	ColumnarMetapage metapage;
	DataFileMetadata *datafileMetadata = palloc0(sizeof(DataFileMetadata));

	bool found = ReadMetapage(relfilenode, &metapage);
	if (found)
	{
		found = ReadCStoreDataFiles(metapage.storageId, datafileMetadata);
	}

	if (!found)
	{
		if (!missingOk)
		{
			ereport(ERROR, (errmsg("Relfilenode %d doesn't belong to a cstore table.",
								   relfilenode.relNode)));
		}
		else
		{
			return NULL;
		}
	}

	datafileMetadata->stripeMetadataList =
		ReadDataFileStripeList(metapage.storageId, GetTransactionSnapshot());

	return datafileMetadata;
}


/*
 * GetHighestUsedAddress returns the highest used address for the given
 * relfilenode across all active and inactive transactions.
 */
uint64
GetHighestUsedAddress(RelFileNode relfilenode)
{
	uint64 highestUsedAddress = 0;
	uint64 highestUsedId = 0;
	ColumnarMetapage metapage;
	if (!ReadMetapage(relfilenode, &metapage))
	{
		elog(ERROR, "metapage was not found");
	}

	GetHighestUsedAddressAndId(metapage.storageId, &highestUsedAddress, &highestUsedId);

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
	*highestUsedAddress = sizeof(ColumnarMetapage);

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
			  uint64 blockCount, uint64 blockRowCount)
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
	ColumnarMetapage metapage;
	if (!ReadMetapage(relfilenode, &metapage))
	{
		elog(WARNING, "metapage was not found");
	}

	GetHighestUsedAddressAndId(metapage.storageId, &currLogicalHigh, &highestId);
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
	stripe.blockCount = blockCount;
	stripe.blockRowCount = blockRowCount;
	stripe.columnCount = columnCount;
	stripe.rowCount = rowCount;
	stripe.id = highestId + 1;

	InsertStripeMetadataRow(metapage.storageId, &stripe);

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
		stripeMetadata->blockCount = DatumGetInt32(
			datumArray[Anum_cstore_stripes_block_count - 1]);
		stripeMetadata->blockRowCount = DatumGetInt32(
			datumArray[Anum_cstore_stripes_block_row_count - 1]);
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
 * ReadCStoreDataFiles reads corresponding record from cstore_data_files. Returns
 * false if table was not found in cstore_data_files.
 */
static bool
ReadCStoreDataFiles(uint64 storageid, DataFileMetadata *metadata)
{
	bool found = false;
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_cstore_data_files_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageid));

	Oid cstoreDataFilesOid = CStoreDataFilesRelationId();
	Relation cstoreDataFiles = try_relation_open(cstoreDataFilesOid, AccessShareLock);
	if (cstoreDataFiles == NULL)
	{
		/*
		 * Extension has been dropped. This can be called while
		 * dropping extension or database via ObjectAccess().
		 */
		return false;
	}

	Relation index = try_relation_open(CStoreDataFilesIndexRelationId(), AccessShareLock);
	if (index == NULL)
	{
		heap_close(cstoreDataFiles, NoLock);

		/* extension has been dropped */
		return false;
	}

	TupleDesc tupleDescriptor = RelationGetDescr(cstoreDataFiles);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(cstoreDataFiles, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		Datum datumArray[Natts_cstore_data_files];
		bool isNullArray[Natts_cstore_data_files];
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		if (metadata)
		{
			metadata->blockRowCount = DatumGetInt32(
				datumArray[Anum_cstore_data_files_block_row_count - 1]);
			metadata->stripeRowCount = DatumGetInt32(
				datumArray[Anum_cstore_data_files_stripe_row_count - 1]);
			Name compressionName = DatumGetName(
				datumArray[Anum_cstore_data_files_compression - 1]);
			metadata->compression = ParseCompressionType(NameStr(*compressionName));
		}

		found = true;
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreDataFiles, NoLock);

	return found;
}


/*
 * DeleteDataFileMetadataRowIfExists removes the row with given relfilenode from cstore_stripes.
 */
void
DeleteDataFileMetadataRowIfExists(RelFileNode relfilenode)
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

	ColumnarMetapage metapage;
	if (!ReadMetapage(relfilenode, &metapage))
	{
		return;
	}

	ScanKeyInit(&scanKey[0], Anum_cstore_data_files_storageid,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(metapage.storageId));

	Oid cstoreDataFilesOid = CStoreDataFilesRelationId();
	Relation cstoreDataFiles = try_relation_open(cstoreDataFilesOid, AccessShareLock);
	if (cstoreDataFiles == NULL)
	{
		/* extension has been dropped */
		return;
	}

	Relation index = index_open(CStoreDataFilesIndexRelationId(), AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(cstoreDataFiles, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		ModifyState *modifyState = StartModifyRelation(cstoreDataFiles);
		DeleteTupleAndEnforceConstraints(modifyState, heapTuple);
		FinishModifyRelation(modifyState);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreDataFiles, NoLock);
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
	return get_relname_relid("cstore_stripes", CStoreNamespaceId());
}


/*
 * CStoreStripesIndexRelationId returns relation id of cstore_stripes_idx.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreStripesIndexRelationId(void)
{
	return get_relname_relid("cstore_stripes_pkey", CStoreNamespaceId());
}


/*
 * CStoreDataFilesRelationId returns relation id of cstore_data_files.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreDataFilesRelationId(void)
{
	return get_relname_relid("cstore_data_files", CStoreNamespaceId());
}


/*
 * CStoreDataFilesIndexRelationId returns relation id of cstore_data_files_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreDataFilesIndexRelationId(void)
{
	return get_relname_relid("cstore_data_files_pkey", CStoreNamespaceId());
}


/*
 * CStoreSkipNodesRelationId returns relation id of cstore_skipnodes.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreSkipNodesRelationId(void)
{
	return get_relname_relid("cstore_skipnodes", CStoreNamespaceId());
}


/*
 * CStoreSkipNodesIndexRelationId returns relation id of cstore_skipnodes_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreSkipNodesIndexRelationId(void)
{
	return get_relname_relid("cstore_skipnodes_pkey", CStoreNamespaceId());
}


/*
 * CStoreNamespaceId returns namespace id of the schema we store cstore
 * related tables.
 */
static Oid
CStoreNamespaceId(void)
{
	return get_namespace_oid("cstore", false);
}


static bool
ReadMetapage(RelFileNode relfilenode, ColumnarMetapage *metapage)
{
	Oid relationId = RelidByRelfilenode(relfilenode.spcNode,
										relfilenode.relNode);
	if (!OidIsValid(relationId))
	{
		return false;
	}

	Relation relation = relation_open(relationId, NoLock);

	RelationOpenSmgr(relation);
	int nblocks = smgrnblocks(relation->rd_smgr, MAIN_FORKNUM);
	RelationCloseSmgr(relation);

	if (nblocks == 0)
	{
		relation_close(relation, NoLock);
		return false;
	}

	StringInfo metapageBuffer = ReadFromSmgr(relation, 0, sizeof(ColumnarMetapage));
	relation_close(relation, NoLock);

	memcpy((void *) metapage, metapageBuffer->data, sizeof(ColumnarMetapage));

	return true;
}


static ColumnarMetapage
InitMetapage(Relation relation)
{
	ColumnarMetapage metapage;
	metapage.storageId = GetNextStorageId();

	/* create the first block */
	Buffer newBuffer = ReadBuffer(relation, P_NEW);
	ReleaseBuffer(newBuffer);

	Assert(sizeof(ColumnarMetapage) <= BLCKSZ - SizeOfPageHeaderData);
	WriteToSmgr(relation, 0, (char *) &metapage, sizeof(ColumnarMetapage));

	return metapage;
}


static uint64
GetNextStorageId(void)
{
	Oid sequenceId = get_relname_relid("cstore_storageid_seq", CStoreNamespaceId());
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	/*
	 * Generate new and unique storage id from sequence.
	 * TODO: should we restrict access to the sequence, which might require
	 * switching security context here?
	 */
	Datum storageIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	uint64 storageId = DatumGetInt64(storageIdDatum);

	return storageId;
}


Datum
columnar_relation_storageid(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	uint64 storageId = -1;

	Relation relation = relation_open(relationId, AccessShareLock);
	ColumnarMetapage metadata;
	if (IsCStoreTableAmTable(relationId) &&
		ReadMetapage(relation->rd_node, &metadata))
	{
		storageId = metadata.storageId;
	}

	relation_close(relation, AccessShareLock);

	PG_RETURN_INT64(storageId);
}
