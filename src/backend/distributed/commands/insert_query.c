/*-------------------------------------------------------------------------
 *
 * insert_query.c
 *
 * Routines for inserting query results into a distributed table.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "distributed/multi_copy.h"
#include "distributed/worker_protocol.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/* struct type to use a local query executed using SPI as a tuple source */
typedef struct LocalQueryContext
{
	const char *query;
	Portal queryPortal;
	int indexInBatch;
	MemoryContext spiContext;
} LocalQueryContext;


/* Local functions forward declarations */
static uint64 InsertQueryResult(Oid relationId, const char *query);
static CopyTupleSource * CreateLocalQueryTupleSource(const char *query);
static void LocalQueryOpen(void *context, Relation relation,
						   ErrorContextCallback *errorCallback);
static void VerifyTupleDescriptorsMatch(TupleDesc tableDescriptor,
										TupleDesc resultDescriptor);
static bool LocalQueryNextTuple(void *context, Datum *columnValues,
								bool *columnNulls);
static void LocalQueryClose(void *context);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_insert_query_result);


/*
 * master_isnert_query_result runs a query using SPI and inserts the results
 * into a distributed table.
 */
Datum
master_insert_query_result(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	char *query = text_to_cstring(PG_GETARG_TEXT_P(1));

	uint64 processedRowCount = 0;

	processedRowCount = InsertQueryResult(relationId, query);

	PG_RETURN_INT64(processedRowCount);
}


/*
 * InsertQueryResult runs a query using SPI and inserts the results into
 * a distributed table.
 */
static uint64
InsertQueryResult(Oid relationId, const char *query)
{
	CopyTupleSource *tupleSource = CreateLocalQueryTupleSource(query);

	/*
	 * Unfortunately, COPY requires a RangeVar * instead of an Oid to deal
	 * with non-existent tables (COPY from worker). Translate relationId
	 * into RangeVar *.
	 */
	char *relationName = get_rel_name(relationId);
	Oid relationSchemaId = get_rel_namespace(relationId);
	char *relationSchemaName = get_namespace_name(relationSchemaId);
	RangeVar *relation = makeRangeVar(relationSchemaName, relationName, -1);

	uint64 processedRowCount = CopyTupleSourceToShards(tupleSource, relation);

	return processedRowCount;
}


/*
 * CreateLocalQueryTupleSource creates and returns a tuple source for a local
 * query executed using SPI.
 */
static CopyTupleSource *
CreateLocalQueryTupleSource(const char *query)
{
	LocalQueryContext *localQueryContext = palloc0(sizeof(LocalQueryContext));
	CopyTupleSource *tupleSource = palloc0(sizeof(CopyTupleSource));

	localQueryContext->query = query;
	localQueryContext->queryPortal = NULL;
	localQueryContext->indexInBatch = 0;
	localQueryContext->spiContext = NULL;

	tupleSource->context = localQueryContext;
	tupleSource->rowContext = AllocSetContextCreate(CurrentMemoryContext,
													"InsertQueryRowContext",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	tupleSource->Open = LocalQueryOpen;
	tupleSource->NextTuple = LocalQueryNextTuple;
	tupleSource->Close = LocalQueryClose;

	return tupleSource;
}


/*
 * LocalQueryOpen starts query execution through SPI.
 */
static void
LocalQueryOpen(void *context, Relation relation, ErrorContextCallback *errorCallback)
{
	LocalQueryContext *localQueryContext = (LocalQueryContext *) context;

	const char *query = localQueryContext->query;
	TupleDesc tableDescriptor = RelationGetDescr(relation);
	Portal queryPortal = NULL;
	int connected = 0;

	const char *noPortalName = NULL;
	const bool readOnly = false;
	const bool fetchForward = true;
	const int noCursorOptions = 0;
	const int prefetchCount = ROW_PREFETCH_COUNT;

	MemoryContext oldContext = CurrentMemoryContext;

	/* for now, don't provide any special context */
	errorCallback->callback = NULL;
	errorCallback->arg = NULL;
	errorCallback->previous = error_context_stack;

	/* initialize SPI */
	connected = SPI_connect();
	if (connected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	queryPortal = SPI_cursor_open_with_args(noPortalName, query,
											0, NULL, NULL, NULL, /* no arguments */
											readOnly, noCursorOptions);
	if (queryPortal == NULL)
	{
		ereport(ERROR, (errmsg("could not open cursor for query \"%s\"", query)));
	}

	localQueryContext->queryPortal = queryPortal;

	/* fetch the first batch */
	SPI_cursor_fetch(queryPortal, fetchForward, prefetchCount);
	if (SPI_processed > 0)
	{
		TupleDesc resultDescriptor = SPI_tuptable->tupdesc;

		VerifyTupleDescriptorsMatch(tableDescriptor, resultDescriptor);
	}

	localQueryContext->spiContext = MemoryContextSwitchTo(oldContext);
}


/*
 * VerifyTupleDescriptorsMatch throws an error if the tuple descriptor does not
 * match that of the table.
 */
static void
VerifyTupleDescriptorsMatch(TupleDesc tableDescriptor, TupleDesc resultDescriptor)
{
	int tableColumnCount = tableDescriptor->natts;
	int tupleColumnCount = resultDescriptor->natts;
	int tableColumnIndex = 0;
	int tupleColumnIndex = 0;

	for (tableColumnIndex = 0; tableColumnIndex < tableColumnCount; tableColumnIndex++)
	{
		Form_pg_attribute tableColumn = NULL;
		Form_pg_attribute tupleColumn = NULL;

		tableColumn = tableDescriptor->attrs[tableColumnIndex];
		if (tableColumn->attisdropped)
		{
			continue;
		}

		if (tupleColumnIndex >= tupleColumnCount)
		{
			ereport(ERROR, (errmsg("query result has fewer columns than table")));
		}

		tupleColumn = resultDescriptor->attrs[tupleColumnIndex];

		if (tableColumn->atttypid != tupleColumn->atttypid)
		{
			char *columnName = NameStr(tableColumn->attname);
			ereport(ERROR, (errmsg("query result does not match the type of "
								   "column \"%s\"", columnName)));
		}

		tupleColumnIndex++;
	}

	if (tupleColumnIndex < tupleColumnCount)
	{
		ereport(ERROR, (errmsg("query result has more columns than table")));
	}
}


/*
 * LocalQueryNextTuple reads a tuple from SPI and evaluates any missing
 * default values.
 */
static bool
LocalQueryNextTuple(void *context, Datum *columnValues, bool *columnNulls)
{
	LocalQueryContext *localQueryContext = (LocalQueryContext *) context;

	Portal queryPortal = localQueryContext->queryPortal;
	HeapTuple tuple = NULL;
	TupleDesc resultDescriptor = NULL;
	const bool fetchForward = true;
	const int prefetchCount = ROW_PREFETCH_COUNT;

	/*
	 * Check if we reached the end of our current batch. It would look slightly nicer
	 * if we did this after reading a tuple, but we still need to use the tuple after
	 * this function completes.
	 */
	if (SPI_processed > 0 && localQueryContext->indexInBatch >= SPI_processed)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(localQueryContext->spiContext);

		/* release the current batch */
		SPI_freetuptable(SPI_tuptable);

		/* fetch a new batch */
		SPI_cursor_fetch(queryPortal, fetchForward, prefetchCount);

		MemoryContextSwitchTo(oldContext);

		localQueryContext->indexInBatch = 0;
	}

	if (SPI_processed == 0)
	{
		return false;
	}

	/* "read" the tuple */
	tuple = SPI_tuptable->vals[localQueryContext->indexInBatch];
	localQueryContext->indexInBatch++;

	/* extract the column values and nulls */
	resultDescriptor = SPI_tuptable->tupdesc;
	heap_deform_tuple(tuple, resultDescriptor, columnValues, columnNulls);

	return true;
}


/*
 * LocalQueryClose closes the SPI cursor.
 */
static void
LocalQueryClose(void *context)
{
	LocalQueryContext *localQueryContext = (LocalQueryContext *) context;

	Portal queryPortal = localQueryContext->queryPortal;
	int finished = 0;

	MemoryContext oldContext = MemoryContextSwitchTo(localQueryContext->spiContext);

	SPI_freetuptable(SPI_tuptable);
	SPI_cursor_close(queryPortal);

	/* will restore memory context to what it was when SPI_connect was called */
	finished = SPI_finish();
	if (finished != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not disconnect from SPI manager")));
	}

	MemoryContextSwitchTo(oldContext);
}
