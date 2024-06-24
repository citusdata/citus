/*-------------------------------------------------------------------------
 *
 * test/src/distributed_deadlock_detection.c
 *
 * This file contains functions to exercise distributed deadlock detection
 * related lower level functionality.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "distributed/backend_data.h"
#include "distributed/distributed_deadlock_detection.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"


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
	CheckCitusVersion(ERROR);

	TupleDesc tupleDescriptor = NULL;

	HASH_SEQ_STATUS status;
	TransactionNode *transactionNode = NULL;

	Datum values[2];
	bool isNulls[2];

	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	/* distributed deadlock detection only considers distributed txs */
	bool onlyDistributedTx = true;
	WaitGraph *waitGraph = BuildGlobalWaitGraph(onlyDistributedTx);
	HTAB *adjacencyList = BuildAdjacencyListsForWaitGraph(waitGraph);

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

	PG_RETURN_VOID();
}
