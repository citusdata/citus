/*-------------------------------------------------------------------------
 *
 * test/src/distributed_deadlock_detection.c
 *
 * This file contains functions to exercise distributed deadlock detection
 * related lower level functionality.
 *
 * Copyright (c) 20167, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "distributed/backend_data.h"
#include "distributed/distributed_deadlock_detection.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/transaction_identifier.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


PG_FUNCTION_INFO_V1(get_adjacency_list_wait_graph);


/*
 * get_adjacency_list_wait_graph returns the wait graph in adjacency list format. For the
 * details see BuildAdjacencyListForWaitGraph().
 *
 * This function is mostly useful for testing and debugging purposes.
 */
Datum
get_adjacency_list_wait_graph(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *returnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = NULL;
	MemoryContext perQueryContext = NULL;
	MemoryContext oldContext = NULL;

	WaitGraph *waitGraph = NULL;
	HTAB *adjacencyList = NULL;
	HASH_SEQ_STATUS status;
	TransactionNode *transactionNode = NULL;

	const int attributeCount = 2;
	Datum values[attributeCount];
	bool isNulls[attributeCount];

	CheckCitusVersion(ERROR);

	/* check to see if caller supports us returning a tuplestore */
	if (returnSetInfo == NULL || !IsA(returnSetInfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context " \
						"that cannot accept a set")));
	}

	if (!(returnSetInfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}

	/* build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDescriptor) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	perQueryContext = returnSetInfo->econtext->ecxt_per_query_memory;

	oldContext = MemoryContextSwitchTo(perQueryContext);

	tupleStore = tuplestore_begin_heap(true, false, work_mem);
	returnSetInfo->returnMode = SFRM_Materialize;
	returnSetInfo->setResult = tupleStore;
	returnSetInfo->setDesc = tupleDescriptor;

	MemoryContextSwitchTo(oldContext);

	waitGraph = BuildGlobalWaitGraph();
	adjacencyList = BuildAdjacencyListsForWaitGraph(waitGraph);

	/* iterate on all nodes */
	hash_seq_init(&status, adjacencyList);

	while ((transactionNode = (TransactionNode *) hash_seq_search(&status)) != 0)
	{
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		values[0] = UInt64GetDatum(transactionNode->transactionId.transactionNumber);
		values[1] = CStringGetDatum(WaitsForToString(transactionNode->waitsFor));

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}
