/*-------------------------------------------------------------------------
 *
 * cstore_debug.c
 *
 * Helper functions to debug column store.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "pg_config.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "distributed/pg_version_constants.h"
#include "distributed/tuplestore.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

#include "columnar/cstore.h"
#include "columnar/cstore_version_compat.h"

static void MemoryContextTotals(MemoryContext context, MemoryContextCounters *counters);

PG_FUNCTION_INFO_V1(column_store_memory_stats);


/*
 * column_store_memory_stats returns a record of 3 values: size of
 * TopMemoryContext, TopTransactionContext, and Write State context.
 */
Datum
column_store_memory_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	const int resultColumnCount = 3;

#if PG_VERSION_NUM >= PG_VERSION_12
	tupleDescriptor = CreateTemplateTupleDesc(resultColumnCount);
#else
	tupleDescriptor = CreateTemplateTupleDesc(resultColumnCount, false);
#endif

	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 1, "TopMemoryContext",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 2, "TopTransactionContext",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 3, "WriteStateContext",
					   INT8OID, -1, 0);

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

	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	tuplestore_putvalues(tupleStore, tupleDescriptor, values, nulls);
	tuplestore_donestoring(tupleStore);

	PG_RETURN_DATUM(0);
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

	context->methods->stats(context, NULL, NULL, counters);
}
