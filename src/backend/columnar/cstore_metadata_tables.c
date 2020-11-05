/*-------------------------------------------------------------------------
 *
 * cstore_metadata_tables.c
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

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

typedef struct
{
	Relation rel;
	EState *estate;
} ModifyState;

static void InsertStripeMetadataRow(Oid relfilenode, StripeMetadata *stripe);
static void GetHighestUsedAddressAndId(Oid relfilenode,
									   uint64 *highestUsedAddress,
									   uint64 *highestUsedId);
static List * ReadDataFileStripeList(Oid relfilenode, Snapshot snapshot);
static Oid CStoreStripesRelationId(void);
static Oid CStoreStripesIndexRelationId(void);
static Oid CStoreDataFilesRelationId(void);
static Oid CStoreDataFilesIndexRelationId(void);
static Oid CStoreSkipNodesRelationId(void);
static Oid CStoreSkipNodesIndexRelationId(void);
static Oid CStoreNamespaceId(void);
static bool ReadCStoreDataFiles(Oid relfilenode, DataFileMetadata *metadata);
static ModifyState * StartModifyRelation(Relation rel);
static void InsertTupleAndEnforceConstraints(ModifyState *state, Datum *values,
											 bool *nulls);
static void DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple);
static void FinishModifyRelation(ModifyState *state);
static EState * create_estate_for_relation(Relation rel);
static bytea * DatumToBytea(Datum value, Form_pg_attribute attrForm);
static Datum ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm);

/* constants for cstore_table */
#define Natts_cstore_data_files 6
#define Anum_cstore_data_files_relfilenode 1
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
	Oid relfilenode;
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
#define Anum_cstore_stripes_relfilenode 1
#define Anum_cstore_stripes_stripe 2
#define Anum_cstore_stripes_file_offset 3
#define Anum_cstore_stripes_data_length 4
#define Anum_cstore_stripes_column_count 5
#define Anum_cstore_stripes_block_count 6
#define Anum_cstore_stripes_block_row_count 7
#define Anum_cstore_stripes_row_count 8

/* constants for cstore_skipnodes */
#define Natts_cstore_skipnodes 12
#define Anum_cstore_skipnodes_relfilenode 1
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
 * InitCStoreDataFileMetadata adds a record for the given relfilenode
 * in cstore_data_files.
 */
void
InitCStoreDataFileMetadata(Oid relfilenode, int blockRowCount, int stripeRowCount,
						   CompressionType compression)
{
	NameData compressionName = { 0 };

	namestrcpy(&compressionName, CompressionTypeStr(compression));

	bool nulls[Natts_cstore_data_files] = { 0 };
	Datum values[Natts_cstore_data_files] = {
		ObjectIdGetDatum(relfilenode),
		Int32GetDatum(blockRowCount),
		Int32GetDatum(stripeRowCount),
		NameGetDatum(&compressionName),
		Int32GetDatum(CSTORE_VERSION_MAJOR),
		Int32GetDatum(CSTORE_VERSION_MINOR)
	};

	DeleteDataFileMetadataRowIfExists(relfilenode);

	Oid cstoreDataFilesOid = CStoreDataFilesRelationId();
	Relation cstoreDataFiles = heap_open(cstoreDataFilesOid, RowExclusiveLock);

	ModifyState *modifyState = StartModifyRelation(cstoreDataFiles);
	InsertTupleAndEnforceConstraints(modifyState, values, nulls);
	FinishModifyRelation(modifyState);

	CommandCounterIncrement();

	heap_close(cstoreDataFiles, NoLock);
}


void
UpdateCStoreDataFileMetadata(Oid relfilenode, int blockRowCount, int stripeRowCount,
							 CompressionType compression)
{
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;
	Datum values[Natts_cstore_data_files] = { 0 };
	bool isnull[Natts_cstore_data_files] = { 0 };
	bool replace[Natts_cstore_data_files] = { 0 };

	Relation cstoreDataFiles = heap_open(CStoreDataFilesRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(cstoreDataFiles);

	ScanKeyInit(&scanKey[0], Anum_cstore_data_files_relfilenode, BTEqualStrategyNumber,
				F_INT8EQ, ObjectIdGetDatum(relfilenode));

	SysScanDesc scanDescriptor = systable_beginscan(cstoreDataFiles,
													CStoreDataFilesIndexRelationId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (heapTuple == NULL)
	{
		ereport(ERROR, (errmsg("relfilenode %d doesn't belong to a cstore table",
							   relfilenode)));
	}

	Form_cstore_data_files metadata = (Form_cstore_data_files) GETSTRUCT(heapTuple);

	bool changed = false;
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
SaveStripeSkipList(Oid relfilenode, uint64 stripe, StripeSkipList *stripeSkipList,
				   TupleDesc tupleDescriptor)
{
	uint32 columnIndex = 0;
	uint32 blockIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

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
				ObjectIdGetDatum(relfilenode),
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
ReadStripeSkipList(Oid relfilenode, uint64 stripe, TupleDesc tupleDescriptor,
				   uint32 blockCount)
{
	int32 columnIndex = 0;
	HeapTuple heapTuple = NULL;
	uint32 columnCount = tupleDescriptor->natts;
	ScanKeyData scanKey[2];

	Oid cstoreSkipNodesOid = CStoreSkipNodesRelationId();
	Relation cstoreSkipNodes = heap_open(cstoreSkipNodesOid, AccessShareLock);
	Relation index = index_open(CStoreSkipNodesIndexRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_cstore_skipnodes_relfilenode,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relfilenode));
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
InsertStripeMetadataRow(Oid relfilenode, StripeMetadata *stripe)
{
	bool nulls[Natts_cstore_stripes] = { 0 };
	Datum values[Natts_cstore_stripes] = {
		ObjectIdGetDatum(relfilenode),
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
ReadDataFileMetadata(Oid relfilenode, bool missingOk)
{
	DataFileMetadata *datafileMetadata = palloc0(sizeof(DataFileMetadata));
	bool found = ReadCStoreDataFiles(relfilenode, datafileMetadata);
	if (!found)
	{
		if (!missingOk)
		{
			ereport(ERROR, (errmsg("Relfilenode %d doesn't belong to a cstore table.",
								   relfilenode)));
		}
		else
		{
			return NULL;
		}
	}

	datafileMetadata->stripeMetadataList =
		ReadDataFileStripeList(relfilenode, GetTransactionSnapshot());

	return datafileMetadata;
}


/*
 * GetHighestUsedAddress returns the highest used address for the given
 * relfilenode across all active and inactive transactions.
 */
uint64
GetHighestUsedAddress(Oid relfilenode)
{
	uint64 highestUsedAddress = 0;
	uint64 highestUsedId = 0;

	GetHighestUsedAddressAndId(relfilenode, &highestUsedAddress, &highestUsedId);

	return highestUsedAddress;
}


/*
 * GetHighestUsedAddressAndId returns the highest used address and id for
 * the given relfilenode across all active and inactive transactions.
 */
static void
GetHighestUsedAddressAndId(Oid relfilenode,
						   uint64 *highestUsedAddress,
						   uint64 *highestUsedId)
{
	ListCell *stripeMetadataCell = NULL;

	SnapshotData SnapshotDirty;
	InitDirtySnapshot(SnapshotDirty);

	List *stripeMetadataList = ReadDataFileStripeList(relfilenode, &SnapshotDirty);

	*highestUsedId = 0;
	*highestUsedAddress = 0;

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

	Oid relfilenode = rel->rd_node.relNode;
	GetHighestUsedAddressAndId(relfilenode, &currLogicalHigh, &highestId);
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

	InsertStripeMetadataRow(relfilenode, &stripe);

	UnlockRelation(rel, ShareUpdateExclusiveLock);

	return stripe;
}


/*
 * ReadDataFileStripeList reads the stripe list for a given relfilenode
 * in the given snapshot.
 */
static List *
ReadDataFileStripeList(Oid relfilenode, Snapshot snapshot)
{
	List *stripeMetadataList = NIL;
	ScanKeyData scanKey[1];
	HeapTuple heapTuple;

	ScanKeyInit(&scanKey[0], Anum_cstore_stripes_relfilenode,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relfilenode));

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
ReadCStoreDataFiles(Oid relfilenode, DataFileMetadata *metadata)
{
	bool found = false;
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_cstore_data_files_relfilenode,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relfilenode));

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
DeleteDataFileMetadataRowIfExists(Oid relfilenode)
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

	ScanKeyInit(&scanKey[0], Anum_cstore_data_files_relfilenode,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relfilenode));

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
			memcpy(VARDATA(result), DatumGetPointer(value), attrForm->attlen);
		}
	}
	else
	{
		memcpy(VARDATA(result), DatumGetPointer(value), datumLength);
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
	memcpy(binaryDataCopy, VARDATA_ANY(bytes), VARSIZE_ANY_EXHDR(bytes));

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
