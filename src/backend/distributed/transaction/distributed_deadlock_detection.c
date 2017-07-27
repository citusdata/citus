/*-------------------------------------------------------------------------
 *
 * distributed_deadlock_detection.c
 *
 *  Functions for performing distributed deadlock detection.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
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


static TransactionNode * GetOrCreateTransactionNode(HTAB *adjacencyList,
													DistributedTransactionId *
													transactionId);
static uint32 DistributedTransactionIdHash(const void *key, Size keysize);
static int DistributedTransactionIdCompare(const void *a, const void *b, Size keysize);


/*
 * BuildAdjacencyListsForWaitGraph converts the input wait graph to
 * an adjacency list for further processing.
 *
 * The input wait graph consists of set of wait edges between all
 * backends in the Citus cluster.
 *
 * We represent the adjacency list with an HTAB structure. Each node is
 * represented with a DistributedTransactionId and each edge is represented with
 * a TransactionNode structure.
 *
 * While iterating over the input wait edges, we follow the algorithm
 * below:
 *    for each edge in waitGraph:
 *      - find the corresponding nodes for waiting and
 *        blocking transactions in the adjacency list
 *          - if not found, add new node(s) to the list
 *      - Add blocking transaction to the waiting transaction's waitFor
 *        list
 *
 *  The format of the adjacency list becomes the following:
 *      [transactionId] = [transactionNode->waitsFor {list of waiting transaction nodes}]
 */
extern HTAB *
BuildAdjacencyListsForWaitGraph(WaitGraph *waitGraph)
{
	HASHCTL info;
	uint32 hashFlags = 0;
	HTAB *adjacencyList = NULL;
	int edgeIndex = 0;
	int edgeCount = waitGraph->edgeCount;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(DistributedTransactionId);
	info.entrysize = sizeof(TransactionNode);
	info.hash = DistributedTransactionIdHash;
	info.match = DistributedTransactionIdCompare;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	adjacencyList = hash_create("distributed deadlock detection", 64, &info, hashFlags);

	for (edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++)
	{
		WaitEdge *edge = &waitGraph->edges[edgeIndex];
		TransactionNode *waitingTransaction = NULL;
		TransactionNode *blockingTransaction = NULL;

		DistributedTransactionId waitingId = {
			edge->waitingNodeId,
			edge->waitingTransactionNum,
			edge->waitingTransactionStamp
		};

		DistributedTransactionId blockingId = {
			edge->blockingNodeId,
			edge->blockingTransactionNum,
			edge->blockingTransactionStamp
		};

		waitingTransaction =
			GetOrCreateTransactionNode(adjacencyList, &waitingId);
		blockingTransaction =
			GetOrCreateTransactionNode(adjacencyList, &blockingId);

		waitingTransaction->waitsFor = lappend(waitingTransaction->waitsFor,
											   blockingTransaction);
	}

	return adjacencyList;
}


/*
 * GetOrCreateTransactionNode searches distributedTransactionHash for the given
 * given transactionId. If the transaction is not found, a new transaction node
 * with the given transaction identifier is added.
 */
static TransactionNode *
GetOrCreateTransactionNode(HTAB *adjacencyList, DistributedTransactionId *transactionId)
{
	TransactionNode *transactionNode = NULL;
	bool found = false;

	transactionNode = (TransactionNode *) hash_search(adjacencyList, transactionId,
													  HASH_ENTER, &found);
	if (!found)
	{
		transactionNode->waitsFor = NIL;
	}

	return transactionNode;
}


/*
 * DistributedTransactionIdHash returns hashed value for a given distributed
 * transaction id.
 */
static uint32
DistributedTransactionIdHash(const void *key, Size keysize)
{
	DistributedTransactionId *entry = (DistributedTransactionId *) key;
	uint32 hash = 0;

	hash = hash_uint32(entry->initiatorNodeIdentifier);
	hash = hash_combine(hash, hash_any((unsigned char *) &entry->transactionNumber,
									   sizeof(int64)));
	hash = hash_combine(hash, hash_any((unsigned char *) &entry->timestamp,
									   sizeof(TimestampTz)));

	return hash;
}


/*
 * DistributedTransactionIdCompare compares DistributedTransactionId's a and b
 * and returns -1 if a < b, 1 if a > b, 0 if they are equal.
 *
 * DistributedTransactionId are first compared by their timestamp, then transaction
 * number, then node identifier.
 */
static int
DistributedTransactionIdCompare(const void *a, const void *b, Size keysize)
{
	DistributedTransactionId *xactIdA = (DistributedTransactionId *) a;
	DistributedTransactionId *xactIdB = (DistributedTransactionId *) b;

	if (!TimestampDifferenceExceeds(xactIdB->timestamp, xactIdA->timestamp, 0))
	{
		/* ! (B <= A) = A < B */
		return -1;
	}
	else if (!TimestampDifferenceExceeds(xactIdA->timestamp, xactIdB->timestamp, 0))
	{
		/* ! (A <= B) = A > B */
		return 1;
	}
	else if (xactIdA->transactionNumber < xactIdB->transactionNumber)
	{
		return -1;
	}
	else if (xactIdA->transactionNumber > xactIdB->transactionNumber)
	{
		return 1;
	}
	else if (xactIdA->initiatorNodeIdentifier < xactIdB->initiatorNodeIdentifier)
	{
		return -1;
	}
	else if (xactIdA->initiatorNodeIdentifier > xactIdB->initiatorNodeIdentifier)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
