/*-------------------------------------------------------------------------
 *
 * xact_stats.c
 *
 * This file contains functions to provide helper UDFs for testing transaction
 * statistics.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

static Size MemoryContextTotalSpace(MemoryContext context);

PG_FUNCTION_INFO_V1(top_transaction_context_size);

/*
 * top_transaction_context_size returns current size of TopTransactionContext.
 */
Datum
top_transaction_context_size(PG_FUNCTION_ARGS)
{
	Size totalSpace = MemoryContextTotalSpace(TopTransactionContext);
	PG_RETURN_INT64(totalSpace);
}


/*
 * MemoryContextTotalSpace returns total space allocated in context and its children.
 */
static Size
MemoryContextTotalSpace(MemoryContext context)
{
	Size totalSpace = 0;

	MemoryContextCounters totals = { 0 };
	TopTransactionContext->methods->stats(TopTransactionContext, NULL, NULL, &totals);
	totalSpace += totals.totalspace;

	for (MemoryContext child = context->firstchild;
		 child != NULL;
		 child = child->nextchild)
	{
		totalSpace += MemoryContextTotalSpace(child);
	}

	return totalSpace;
}
