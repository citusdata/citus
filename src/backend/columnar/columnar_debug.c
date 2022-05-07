/*-------------------------------------------------------------------------
 *
 * columnar_debug.c
 *
 * Helper functions to debug column store.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "funcapi.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "distributed/pg_version_constants.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

#include "pg_version_compat.h"
#include "columnar/columnar.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_version_compat.h"

static void MemoryContextTotals(MemoryContext context, MemoryContextCounters *counters);

PG_FUNCTION_INFO_V1(columnar_store_memory_stats);
PG_FUNCTION_INFO_V1(columnar_storage_info);


/*
 * columnar_store_memory_stats returns a record of 3 values: size of
 * TopMemoryContext, TopTransactionContext, and Write State context.
 */
Datum
columnar_store_memory_stats(PG_FUNCTION_ARGS)
{
	const int resultColumnCount = 3;

	TupleDesc tupleDescriptor = CreateTemplateTupleDesc(resultColumnCount);

	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 1, "TopMemoryContext",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 2, "TopTransactionContext",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 3, "WriteStateContext",
					   INT8OID, -1, 0);

	tupleDescriptor = BlessTupleDesc(tupleDescriptor);

	MemoryContextCounters transactionCounters = { 0 };
	MemoryContextCounters topCounters = { 0 };
	MemoryContextCounters writeStateCounters = { 0 };
	MemoryContextTotals(TopTransactionContext, &transactionCounters);
	MemoryContextTotals(TopMemoryContext, &topCounters);
	MemoryContextTotals(GetWriteContextForDebug(), &writeStateCounters);

	bool nulls[3] = { false };
	Datum values[3] = {
		Int64GetDatum(topCounters.totalspace),
		Int64GetDatum(transactionCounters.totalspace),
		Int64GetDatum(writeStateCounters.totalspace)
	};

	HeapTuple tuple = heap_form_tuple(tupleDescriptor, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}


/*
 * columnar_storage_info - UDF to return internal storage info for a columnar relation.
 *
 * DDL:
 *  CREATE OR REPLACE FUNCTION columnar_storage_info(
 *      rel regclass,
 *      version_major OUT int4,
 *      version_minor OUT int4,
 *      storage_id OUT int8,
 *      reserved_stripe_id OUT int8,
 *      reserved_row_number OUT int8,
 *      reserved_offset OUT int8)
 *    STRICT
 *    LANGUAGE c AS 'MODULE_PATHNAME', 'columnar_storage_info';
 */
Datum
columnar_storage_info(PG_FUNCTION_ARGS)
{
#define STORAGE_INFO_NATTS 6
	Oid relid = PG_GETARG_OID(0);
	TupleDesc tupdesc;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	if (tupdesc->natts != STORAGE_INFO_NATTS)
	{
		elog(ERROR, "return type must have %d columns", STORAGE_INFO_NATTS);
	}

	Relation rel = table_open(relid, AccessShareLock);
	if (!IsColumnarTableAmTable(relid))
	{
		ereport(ERROR, (errmsg("table \"%s\" is not a columnar table",
							   RelationGetRelationName(rel))));
	}

	Datum values[STORAGE_INFO_NATTS] = { 0 };
	bool nulls[STORAGE_INFO_NATTS] = { 0 };

	/*
	 * Pass force = true so that we can inspect metapages that are not the
	 * current version.
	 *
	 * NB: ensure the order and number of attributes correspond to DDL
	 * declaration.
	 */
	values[0] = Int32GetDatum(ColumnarStorageGetVersionMajor(rel, true));
	values[1] = Int32GetDatum(ColumnarStorageGetVersionMinor(rel, true));
	values[2] = Int64GetDatum(ColumnarStorageGetStorageId(rel, true));
	values[3] = Int64GetDatum(ColumnarStorageGetReservedStripeId(rel, true));
	values[4] = Int64GetDatum(ColumnarStorageGetReservedRowNumber(rel, true));
	values[5] = Int64GetDatum(ColumnarStorageGetReservedOffset(rel, true));

	/* release lock */
	table_close(rel, AccessShareLock);

	HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}


/*
 * MemoryContextTotals adds stats of the given memory context and its
 * subtree to the given counters.
 */
static void
MemoryContextTotals(MemoryContext context, MemoryContextCounters *counters)
{
	if (context == NULL)
	{
		return;
	}

	MemoryContext child;
	for (child = context->firstchild; child != NULL; child = child->nextchild)
	{
		MemoryContextTotals(child, counters);
	}

	context->methods->stats_compat(context, NULL, NULL, counters, true);
}
