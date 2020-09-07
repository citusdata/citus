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
#include "access/nbtree.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
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
static void InsertStripeAttrRow(Oid relid, uint64 stripe, AttrNumber attr,
								uint64 existsSize, uint64 valuesSize,
								uint64 skiplistSize);

/* constants for cstore_stripe_attr */
#define Natts_cstore_stripe_attr 6
#define Anum_cstore_stripe_attr_relid 1
#define Anum_cstore_stripe_attr_stripe 2
#define Anum_cstore_stripe_attr_attr 3
#define Anum_cstore_stripe_attr_exists_size 4
#define Anum_cstore_stripe_attr_value_size 5
#define Anum_cstore_stripe_attr_skiplist_size 6

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

	CatalogTupleInsert(cstoreStripeAttrs, tuple);

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
	return get_relname_relid("cstore_stripe_attr", PG_CATALOG_NAMESPACE);
}


/*
 * CStoreStripeAttrRelationId returns relation id of cstore_stripe_attr_idx.
 * TODO: should we cache this similar to citus?
 */
static Oid
CStoreStripeAttrIndexRelationId(void)
{
	return get_relname_relid("cstore_stripe_attr_idx", PG_CATALOG_NAMESPACE);
}
