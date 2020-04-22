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
#include "distributed/transaction/backend_data.h"
#include "distributed/transaction/distributed_deadlock_detection.h"
#include "distributed/utils/hash_helpers.h"
#include "distributed/utils/listutils.h"
#include "distributed/transaction/lock_graph.h"
#include "distributed/metadata/metadata_cache.h"
#include "distributed/transaction_identifier.h"
#include "distributed/utils/tuplestore.h"
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
	TupleDesc tupleDescriptor = NULL;

	HASH_SEQ_STATUS status;
	TransactionNode *transactionNode = NULL;

	Datum values[2];
	bool isNulls[2];

	CheckCitusVersion(ERROR);

	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	WaitGraph *waitGraph = BuildGlobalWaitGraph();
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

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}
