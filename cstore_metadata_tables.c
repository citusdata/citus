/*-------------------------------------------------------------------------
 *
 * cstore_metadata_tables.c
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "cstore.h"
#include "cstore_version_compat.h"

#include <sys/stat.h>
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/tableam.h"
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
#include "optimizer/optimizer.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "cstore_metadata_serialization.h"

static Oid CStoreStripeAttrRelationId(void);
static Oid CStoreStripeAttrIndexRelationId(void);
static Oid CStoreStripesRelationId(void);
static Oid CStoreStripesIndexRelationId(void);
static Oid CStoreTablesRelationId(void);
static Oid CStoreTablesIndexRelationId(void);
static Oid CStoreNamespaceId(void);
static void InsertStripeAttrRow(Oid relid, uint64 stripe, AttrNumber attr,
								uint64 existsSize, uint64 valuesSize,
								uint64 skiplistSize);
static int TableBlockRowCount(Oid relid);
static void DeleteTableMetadataRowIfExists(Oid relid);
static void InsertTupleAndEnforceConstraints(Relation rel, HeapTuple heapTuple);
static void DeleteTupleAndEnforceConstraints(Relation rel, HeapTuple heapTuple);
static EState * create_estate_for_relation(Relation rel);

/* constants for cstore_stripe_attr */
#define Natts_cstore_stripe_attr 6
#define Anum_cstore_stripe_attr_relid 1
#define Anum_cstore_stripe_attr_stripe 2
#define Anum_cstore_stripe_attr_attr 3
#define Anum_cstore_stripe_attr_exists_size 4
#define Anum_cstore_stripe_attr_value_size 5
#define Anum_cstore_stripe_attr_skiplist_size 6

/* constants for cstore_table */
#define Natts_cstore_tables 4
#define Anum_cstore_tables_relid 1
#define Anum_cstore_tables_block_row_count 2
#define Anum_cstore_tables_version_major 3
#define Anum_cstore_tables_version_minor 4

/* constants for cstore_stripe */
#define Natts_cstore_stripes 5
#define Anum_cstore_stripes_relid 1
#define Anum_cstore_stripes_stripe 2
#define Anum_cstore_stripes_file_offset 3
#define Anum_cstore_stripes_skiplist_length 4
#define Anum_cstore_stripes_data_length 5

/*
 * InitCStoreTableMetadata adds a record for the given relation in cstore_table.
 */
void
InitCStoreTableMetadata(Oid relid, int blockRowCount)
{
	Oid cstoreTableOid = InvalidOid;
	Relation cstoreTable = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple tuple = NULL;

	bool nulls[Natts_cstore_tables] = { 0 };
	Datum values[Natts_cstore_tables] = {
		ObjectIdGetDatum(relid),
		Int32GetDatum(blockRowCount),
		Int32GetDatum(CSTORE_VERSION_MAJOR),
		Int32GetDatum(CSTORE_VERSION_MINOR)
	};

	DeleteTableMetadataRowIfExists(relid);

	cstoreTableOid = CStoreTablesRelationId();
	cstoreTable = heap_open(cstoreTableOid, RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(cstoreTable);

	tuple = heap_form_tuple(tupleDescriptor, values, nulls);

	InsertTupleAndEnforceConstraints(cstoreTable, tuple);

	CommandCounterIncrement();

	heap_close(cstoreTable, NoLock);
}


/*
 * InsertStripeMetadataRow adds a row to cstore_stripes.
 */
void
InsertStripeMetadataRow(Oid relid, StripeMetadata *stripe)
{
	bool nulls[Natts_cstore_stripes] = { 0 };
	Datum values[Natts_cstore_stripes] = {
		ObjectIdGetDatum(relid),
		Int64GetDatum(stripe->id),
		Int64GetDatum(stripe->fileOffset),
		Int64GetDatum(stripe->skipListLength),
		Int64GetDatum(stripe->dataLength)
	};

	Oid cstoreStripesOid = CStoreStripesRelationId();
	Relation cstoreStripes = heap_open(cstoreStripesOid, RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(cstoreStripes);

	HeapTuple tuple = heap_form_tuple(tupleDescriptor, values, nulls);

	InsertTupleAndEnforceConstraints(cstoreStripes, tuple);

	CommandCounterIncrement();

	heap_close(cstoreStripes, NoLock);
}


/*
 * ReadTableMetadata constructs TableMetadata for a given relid by reading
 * from cstore_tables and cstore_stripes.
 */
TableMetadata *
ReadTableMetadata(Oid relid)
{
	Oid cstoreStripesOid = InvalidOid;
	Relation cstoreStripes = NULL;
	Relation index = NULL;
	TupleDesc tupleDescriptor = NULL;
	ScanKeyData scanKey[1];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple;

	TableMetadata *tableMetadata = palloc0(sizeof(TableMetadata));
	tableMetadata->blockRowCount = TableBlockRowCount(relid);

	ScanKeyInit(&scanKey[0], Anum_cstore_stripes_relid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relid));

	cstoreStripesOid = CStoreStripesRelationId();
	cstoreStripes = heap_open(cstoreStripesOid, AccessShareLock);
	index = index_open(CStoreStripesIndexRelationId(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(cstoreStripes);

	scanDescriptor = systable_beginscan_ordered(cstoreStripes, index, NULL, 1, scanKey);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		StripeMetadata *stripeMetadata = NULL;
		Datum datumArray[Natts_cstore_stripes];
		bool isNullArray[Natts_cstore_stripes];

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		stripeMetadata = palloc0(sizeof(StripeMetadata));
		stripeMetadata->id = DatumGetInt64(datumArray[Anum_cstore_stripes_stripe - 1]);
		stripeMetadata->fileOffset = DatumGetInt64(
			datumArray[Anum_cstore_stripes_file_offset - 1]);
		stripeMetadata->dataLength = DatumGetInt64(
			datumArray[Anum_cstore_stripes_data_length - 1]);
		stripeMetadata->skipListLength = DatumGetInt64(
			datumArray[Anum_cstore_stripes_skiplist_length - 1]);

		tableMetadata->stripeMetadataList = lappend(tableMetadata->stripeMetadataList,
													stripeMetadata);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreStripes, NoLock);

	return tableMetadata;
}


/*
 * TableBlockRowCount returns block_row_count column from cstore_tables for a given relid.
 */
static int
TableBlockRowCount(Oid relid)
{
	int blockRowCount = 0;
	Oid cstoreTablesOid = InvalidOid;
	Relation cstoreTables = NULL;
	Relation index = NULL;
	TupleDesc tupleDescriptor = NULL;
	ScanKeyData scanKey[1];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;

	ScanKeyInit(&scanKey[0], Anum_cstore_tables_relid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relid));

	cstoreTablesOid = CStoreTablesRelationId();
	cstoreTables = heap_open(cstoreTablesOid, AccessShareLock);
	index = index_open(CStoreTablesIndexRelationId(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(cstoreTables);

	scanDescriptor = systable_beginscan_ordered(cstoreTables, index, NULL, 1, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		Datum datumArray[Natts_cstore_tables];
		bool isNullArray[Natts_cstore_tables];
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		blockRowCount = DatumGetInt32(datumArray[Anum_cstore_tables_block_row_count - 1]);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreTables, NoLock);

	return blockRowCount;
}


/*
 * DeleteTableMetadataRowIfExists removes the row with given relid from cstore_stripes.
 */
static void
DeleteTableMetadataRowIfExists(Oid relid)
{
	Oid cstoreTablesOid = InvalidOid;
	Relation cstoreTables = NULL;
	Relation index = NULL;
	ScanKeyData scanKey[1];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;

	ScanKeyInit(&scanKey[0], Anum_cstore_tables_relid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relid));

	cstoreTablesOid = CStoreTablesRelationId();
	cstoreTables = table_open(cstoreTablesOid, AccessShareLock);
	index = index_open(CStoreTablesIndexRelationId(), AccessShareLock);

	scanDescriptor = systable_beginscan_ordered(cstoreTables, index, NULL, 1, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		DeleteTupleAndEnforceConstraints(cstoreTables, heapTuple);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	table_close(cstoreTables, NoLock);
}


/*
 * InsertTupleAndEnforceConstraints inserts a tuple into a relation and
 * makes sure constraints (e.g. FK constraints, NOT NULL, ...) are enforced.
 */
static void
InsertTupleAndEnforceConstraints(Relation rel, HeapTuple heapTuple)
{
	EState *estate = NULL;
	TupleTableSlot *slot = NULL;

	estate = create_estate_for_relation(rel);
	slot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(heapTuple, slot, false);

	ExecOpenIndices(estate->es_result_relation_info, false);

	/* ExecSimpleRelationInsert executes any constraints */
	ExecSimpleRelationInsert(estate, slot);
	
	ExecCloseIndices(estate->es_result_relation_info);

	AfterTriggerEndQuery(estate);
	ExecCleanUpTriggerState(estate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);
}



/*
 * DeleteTupleAndEnforceConstraints deletes a tuple from a relation and
 * makes sure constraints (e.g. FK constraints) are enforced.
 */
static void
DeleteTupleAndEnforceConstraints(Relation rel, HeapTuple heapTuple)
{
	EState *estate = create_estate_for_relation(rel);
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;

	ItemPointer tid = &(heapTuple->t_self);
	simple_table_tuple_delete(rel, tid, estate->es_snapshot);

	/* execute AFTER ROW DELETE Triggers to enforce constraints */
	ExecARDeleteTriggers(estate, resultRelInfo,
							tid, NULL, NULL);

	AfterTriggerEndQuery(estate);
	ExecCleanUpTriggerState(estate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);
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
	EState *estate;
	ResultRelInfo *resultRelInfo;
	RangeTblEntry *rte;

	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;
	ExecInitRangeTable(estate, list_make1(rte));

	resultRelInfo = makeNode(ResultRelInfo);
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
 * SaveStripeFooter stores give StripeFooter as cstore_stripe_attr records.
 */
void
SaveStripeFooter(Oid relid, uint64 stripe, StripeFooter *footer)
{
	for (AttrNumber attr = 1; attr <= footer->columnCount; attr++)
	{
		InsertStripeAttrRow(relid, stripe, attr,
							footer->existsSizeArray[attr - 1],
							footer->valueSizeArray[attr - 1],
							footer->skipListSizeArray[attr - 1]);
	}
}


/*
 * InsertStripeAttrRow adds a row to cstore_stripe_attr.
 */
static void
InsertStripeAttrRow(Oid relid, uint64 stripe, AttrNumber attr,
					uint64 existsSize, uint64 valuesSize,
					uint64 skiplistSize)
{
	bool nulls[Natts_cstore_stripe_attr] = { 0 };
	Datum values[Natts_cstore_stripe_attr] = {
		ObjectIdGetDatum(relid),
		Int64GetDatum(stripe),
		Int16GetDatum(attr),
		Int64GetDatum(existsSize),
		Int64GetDatum(valuesSize),
		Int64GetDatum(skiplistSize)
	};

	Oid cstoreStripeAttrOid = CStoreStripeAttrRelationId();
	Relation cstoreStripeAttrs = heap_open(cstoreStripeAttrOid, RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(cstoreStripeAttrs);

	HeapTuple tuple = heap_form_tuple(tupleDescriptor, values, nulls);

	InsertTupleAndEnforceConstraints(cstoreStripeAttrs, tuple);

	CommandCounterIncrement();

	heap_close(cstoreStripeAttrs, NoLock);
}


/*
 * ReadStripeFooter returns a StripeFooter by reading relevant records from
 * cstore_stripe_attr.
 */
StripeFooter *
ReadStripeFooter(Oid relid, uint64 stripe, int relationColumnCount)
{
	StripeFooter *footer = NULL;
	HeapTuple heapTuple;

	Oid cstoreStripeAttrOid = CStoreStripeAttrRelationId();
	Relation cstoreStripeAttrs = heap_open(cstoreStripeAttrOid, AccessShareLock);
	Relation index = index_open(CStoreStripeAttrIndexRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(cstoreStripeAttrs);

	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_cstore_stripe_attr_relid,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(relid));
	ScanKeyInit(&scanKey[1], Anum_cstore_stripe_attr_stripe,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(stripe));

	scanDescriptor = systable_beginscan_ordered(cstoreStripeAttrs, index, NULL, 2,
												scanKey);

	footer = palloc0(sizeof(StripeFooter));
	footer->existsSizeArray = palloc0(relationColumnCount * sizeof(int64));
	footer->valueSizeArray = palloc0(relationColumnCount * sizeof(int64));
	footer->skipListSizeArray = palloc0(relationColumnCount * sizeof(int64));

	/*
	 * Stripe can have less columns than the relation if ALTER TABLE happens
	 * after stripe is formed. So we calculate column count of a stripe as
	 * maximum attribute number for that stripe.
	 */
	footer->columnCount = 0;

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_cstore_stripe_attr];
		bool isNullArray[Natts_cstore_stripe_attr];
		AttrNumber attr = 0;

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		attr = DatumGetInt16(datumArray[2]);

		footer->columnCount = Max(footer->columnCount, attr);

		while (attr > relationColumnCount)
		{
			ereport(ERROR, (errmsg("unexpected attribute %d for a relation with %d attrs",
								   attr, relationColumnCount)));
		}

		footer->existsSizeArray[attr - 1] =
			DatumGetInt64(datumArray[Anum_cstore_stripe_attr_exists_size - 1]);
		footer->valueSizeArray[attr - 1] =
			DatumGetInt64(datumArray[Anum_cstore_stripe_attr_value_size - 1]);
		footer->skipListSizeArray[attr - 1] =
			DatumGetInt64(datumArray[Anum_cstore_stripe_attr_skiplist_size - 1]);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, NoLock);
	heap_close(cstoreStripeAttrs, NoLock);

	return footer;
}


/*
 * CStoreStripeAttrRelationId returns relation id of cstore_stripe_attr.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreStripeAttrRelationId(void)
{
	return get_relname_relid("cstore_stripe_attr", CStoreNamespaceId());
}


/*
 * CStoreStripeAttrRelationId returns relation id of cstore_stripe_attr_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreStripeAttrIndexRelationId(void)
{
	return get_relname_relid("cstore_stripe_attr_pkey", CStoreNamespaceId());
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
 * CStoreTablesRelationId returns relation id of cstore_tables.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreTablesRelationId(void)
{
	return get_relname_relid("cstore_tables", CStoreNamespaceId());
}


/*
 * CStoreTablesIndexRelationId returns relation id of cstore_tables_idx.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreTablesIndexRelationId(void)
{
	return get_relname_relid("cstore_tables_pkey", CStoreNamespaceId());
}


static Oid
CStoreNamespaceId(void)
{
	return get_namespace_oid("cstore", false);
}
