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

#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"


static Size MemoryContextTotalSpace(MemoryContext context);

PG_FUNCTION_INFO_V1(top_transaction_context_size);
PG_FUNCTION_INFO_V1(coordinated_transaction_should_use_2PC);


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
	TopTransactionContext->methods->stats(TopTransactionContext, NULL, NULL,
										  &totals, true);
	totalSpace += totals.totalspace;

	for (MemoryContext child = context->firstchild;
		 child != NULL;
		 child = child->nextchild)
	{
		totalSpace += MemoryContextTotalSpace(child);
	}

	return totalSpace;
}


/*
 * coordinated_transaction_should_use_2PC returns true if the transaction is in a
 * coordinated transaction and uses 2PC. If the transaction is nott in a
 * coordinated transaction, the function throws an error.
 */
Datum
coordinated_transaction_should_use_2PC(PG_FUNCTION_ARGS)
{
	if (!InCoordinatedTransaction())
	{
		ereport(ERROR, (errmsg("The transaction is not a coordinated transaction")));
	}

	PG_RETURN_BOOL(GetCoordinatedTransactionShouldUse2PC());
}
