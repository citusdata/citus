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
#include "pgstat.h"

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


/* used only for finding the deadlock cycle path */
typedef struct QueuedTransactionNode
{
	TransactionNode *transactionNode;

	int currentStackDepth;
} QueuedTransactionNode;


/* GUC, determining whether debug messages for deadlock detection sent to LOG */
bool LogDistributedDeadlockDetection = false;


static bool CheckDeadlockForTransactionNode(TransactionNode *startingTransactionNode,
											TransactionNode **transactionNodeStack,
											List **deadlockPath);
static void PrependOutgoingNodesToQueue(TransactionNode *queuedTransactionNode,
										int currentStackDepth,
										List **toBeVisitedNodes);
static void BuildDeadlockPathList(QueuedTransactionNode *cycledTransactionNode,
								  TransactionNode **transactionNodeStack,
								  List **deadlockPath);
static void ResetVisitedFields(HTAB *adjacencyList);
static void AssociateDistributedTransactionWithBackendProc(TransactionNode *
														   transactionNode);
static TransactionNode * GetOrCreateTransactionNode(HTAB *adjacencyList,
													DistributedTransactionId *
													transactionId);
static uint32 DistributedTransactionIdHash(const void *key, Size keysize);
static int DistributedTransactionIdCompare(const void *a, const void *b, Size keysize);
static void LogCancellingBackend(TransactionNode *transactionNode);
static void LogTransactionNode(TransactionNode *transactionNode);
static void LogDistributedDeadlockDebugMessage(const char *errorMessage);

PG_FUNCTION_INFO_V1(check_distributed_deadlocks);


/*
 * check_distributed_deadlocks is the external API for manually
 * checking for distributed deadlocks. For the details, see
 * CheckForDistributedDeadlocks().
 */
Datum
check_distributed_deadlocks(PG_FUNCTION_ARGS)
{
	bool deadlockFound = CheckForDistributedDeadlocks();

	return BoolGetDatum(deadlockFound);
}


/*
 * CheckForDistributedDeadlocks is the entry point for detecing
 * distributed deadlocks.
 *
 * In plain words, the function first builds a wait graph by
 * adding the wait edges from the local node and then adding the
 * remote wait edges to form a global wait graph. Later, the wait
 * graph is converted into another graph representation (adjacency
 * lists) for more efficient searches. Finally, a DFS is done on
 * the adjacency lists. Finding a cycle in the graph unveils a
 * distributed deadlock. Upon finding a deadlock, the youngest
 * participant backend is cancelled.
 *
 * The complexity of the algorithm is O(N) for each distributed
 * transaction that's checked for deadlocks. Note that there exists
 *  0 to MaxBackends number of transactions.
 *
 * The function returns true if a deadlock is found. Otherwise, returns
 * false.
 */
bool
CheckForDistributedDeadlocks(void)
{
	WaitGraph *waitGraph = NULL;
	HTAB *adjacencyLists = NULL;
	HASH_SEQ_STATUS status;
	TransactionNode *transactionNode = NULL;
	int edgeCount = 0;
	int localGroupId = GetLocalGroupId();
	List *workerNodeList = ActiveReadableNodeList();

	/*
	 * We don't need to do any distributed deadlock checking if there
	 * are no worker nodes. This might even be problematic for a non-mx
	 * worker node which has the same group id with its master (i.e., 0),
	 * which may erroneously decide to kill the deadlocks happening on it.
	 */
	if (list_length(workerNodeList) == 0)
	{
		return false;
	}

	waitGraph = BuildGlobalWaitGraph();
	adjacencyLists = BuildAdjacencyListsForWaitGraph(waitGraph);

	edgeCount = waitGraph->edgeCount;

	/*
	 * We iterate on transaction nodes and search for deadlocks where the
	 * starting node is the given transaction node.
	 */
	hash_seq_init(&status, adjacencyLists);
	while ((transactionNode = (TransactionNode *) hash_seq_search(&status)) != 0)
	{
		bool deadlockFound = false;
		List *deadlockPath = NIL;
		TransactionNode *transactionNodeStack[edgeCount];

		/* we're only interested in finding deadlocks originating from this node */
		if (transactionNode->transactionId.initiatorNodeIdentifier != localGroupId)
		{
			continue;
		}

		ResetVisitedFields(adjacencyLists);

		deadlockFound = CheckDeadlockForTransactionNode(transactionNode,
														transactionNodeStack,
														&deadlockPath);
		if (deadlockFound)
		{
			TransactionNode *youngestTransaction = transactionNode;
			ListCell *participantTransactionCell = NULL;

			/* there should be at least two transactions to get into a deadlock */
			Assert(list_length(deadlockPath) > 1);

			LogDistributedDeadlockDebugMessage("Distributed deadlock found among the "
											   "following distributed transactions:");

			/*
			 * We search for the youngest participant for two reasons
			 * (i) predictable results (ii) cancel the youngest transaction
			 * (i.e., if a DDL continues for 1 hour and deadlocks with a
			 * SELECT continues for 10 msec, we prefer to cancel the SELECT).
			 *
			 * We're also searching for the youngest transactions initiated by
			 * this node.
			 */
			foreach(participantTransactionCell, deadlockPath)
			{
				TransactionNode *currentNode =
					(TransactionNode *) lfirst(participantTransactionCell);

				TimestampTz youngestTimestamp =
					youngestTransaction->transactionId.timestamp;
				TimestampTz currentTimestamp = currentNode->transactionId.timestamp;

				AssociateDistributedTransactionWithBackendProc(currentNode);

				LogTransactionNode(currentNode);

				if (currentNode->transactionId.initiatorNodeIdentifier ==
					GetLocalGroupId() &&
					timestamptz_cmp_internal(currentTimestamp, youngestTimestamp) == 1)
				{
					youngestTransaction = currentNode;
				}
			}

			/* we should find the backend */
			Assert(youngestTransaction->initiatorProc != NULL);

			CancelTransactionDueToDeadlock(youngestTransaction->initiatorProc);
			LogCancellingBackend(youngestTransaction);

			hash_seq_term(&status);

			return true;
		}
	}

	return false;
}


/*
 * CheckDeadlockForDistributedTransaction does a DFS starting with the given
 * transaction node and checks for a cycle (i.e., the node can be reached again
 * while traversing the graph).
 *
 * Finding a cycle  indicates a distributed deadlock and the function returns
 * true on that case. Also, the deadlockPath is filled with the transaction
 * nodes that form the cycle.
 */
static bool
CheckDeadlockForTransactionNode(TransactionNode *startingTransactionNode,
								TransactionNode **transactionNodeStack,
								List **deadlockPath)
{
	List *toBeVisitedNodes = NIL;
	int currentStackDepth = 0;

	/*
	 * We keep transactionNodeStack to keep track of the deadlock paths. At this point,
	 * adjust the depth of the starting node and set the stack's first element with
	 * the starting node.
	 */
	transactionNodeStack[currentStackDepth] = startingTransactionNode;

	PrependOutgoingNodesToQueue(startingTransactionNode, currentStackDepth,
								&toBeVisitedNodes);

	/* traverse the graph and search for the deadlocks */
	while (toBeVisitedNodes != NIL)
	{
		QueuedTransactionNode *queuedTransactionNode =
			(QueuedTransactionNode *) linitial(toBeVisitedNodes);
		TransactionNode *currentTransactionNode = queuedTransactionNode->transactionNode;

		toBeVisitedNodes = list_delete_first(toBeVisitedNodes);

		/* cycle found, let the caller know about the cycle */
		if (currentTransactionNode == startingTransactionNode)
		{
			BuildDeadlockPathList(queuedTransactionNode, transactionNodeStack,
								  deadlockPath);

			return true;
		}

		/* don't need to revisit the node again */
		if (currentTransactionNode->transactionVisited)
		{
			continue;
		}

		currentTransactionNode->transactionVisited = true;

		/* set the stack's corresponding element with the current node */
		currentStackDepth = queuedTransactionNode->currentStackDepth;
		transactionNodeStack[currentStackDepth] = currentTransactionNode;

		PrependOutgoingNodesToQueue(currentTransactionNode, currentStackDepth,
									&toBeVisitedNodes);
	}

	return false;
}


/*
 * PrependOutgoingNodesToQueue prepends the waiters of the input transaction nodes to the
 * toBeVisitedNodes.
 */
static void
PrependOutgoingNodesToQueue(TransactionNode *transactionNode, int currentStackDepth,
							List **toBeVisitedNodes)
{
	ListCell *currentWaitForCell = NULL;

	/* as we traverse outgoing edges, increment the depth */
	currentStackDepth++;

	/* prepend to the list to continue depth-first search */
	foreach(currentWaitForCell, transactionNode->waitsFor)
	{
		TransactionNode *waitForTransaction =
			(TransactionNode *) lfirst(currentWaitForCell);
		QueuedTransactionNode *queuedNode = palloc0(sizeof(QueuedTransactionNode));

		queuedNode->transactionNode = waitForTransaction;
		queuedNode->currentStackDepth = currentStackDepth;

		*toBeVisitedNodes = lappend(*toBeVisitedNodes, queuedNode);
	}
}


/*
 * BuildDeadlockPathList fills deadlockPath with a list of transactions involved
 * in a distributed deadlock (i.e. a cycle in the graph).
 */
static void
BuildDeadlockPathList(QueuedTransactionNode *cycledTransactionNode,
					  TransactionNode **transactionNodeStack,
					  List **deadlockPath)
{
	int deadlockStackDepth = cycledTransactionNode->currentStackDepth;
	int stackIndex = 0;

	*deadlockPath = NIL;

	for (stackIndex = 0; stackIndex < deadlockStackDepth; stackIndex++)
	{
		*deadlockPath = lappend(*deadlockPath, transactionNodeStack[stackIndex]);
	}
}


/*
 * ResetVisitedFields goes over all the elements of the input adjacency list
 * and sets transactionVisited to false.
 */
static void
ResetVisitedFields(HTAB *adjacencyList)
{
	HASH_SEQ_STATUS status;
	TransactionNode *resetNode = NULL;

	/* reset all visited fields */
	hash_seq_init(&status, adjacencyList);

	while ((resetNode = (TransactionNode *) hash_seq_search(&status)) != 0)
	{
		resetNode->transactionVisited = false;
	}
}


/*
 * AssociateDistributedTransactionWithBackendProc gets a transaction node
 * and searches the corresponding backend. Once found, transactionNodes'
 * initiatorProc is set to it.
 *
 * The function goes over all the backends, checks for the backend with
 * the same transaction number as the given transaction node.
 */
static void
AssociateDistributedTransactionWithBackendProc(TransactionNode *transactionNode)
{
	int backendIndex = 0;

	for (backendIndex = 0; backendIndex < MaxBackends; ++backendIndex)
	{
		PGPROC *currentProc = &ProcGlobal->allProcs[backendIndex];
		BackendData currentBackendData;
		DistributedTransactionId *currentTransactionId = NULL;

		/* we're not interested in processes that are not active or waiting on a lock */
		if (currentProc->pid <= 0)
		{
			continue;
		}

		GetBackendDataForProc(currentProc, &currentBackendData);

		/* we're only interested in distribtued transactions */
		if (!IsInDistributedTransaction(&currentBackendData))
		{
			continue;
		}

		currentTransactionId = &currentBackendData.transactionId;

		if (currentTransactionId->transactionNumber !=
			transactionNode->transactionId.transactionNumber)
		{
			continue;
		}

		/* we're only interested in transactions started on this node */
		if (!currentTransactionId->transactionOriginator)
		{
			continue;
		}

		/* at the point we should only have transactions initiated by this node */
		Assert(currentTransactionId->initiatorNodeIdentifier == GetLocalGroupId());

		transactionNode->initiatorProc = currentProc;

		break;
	}
}


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
		bool transactionOriginator = false;

		DistributedTransactionId waitingId = {
			edge->waitingNodeId,
			transactionOriginator,
			edge->waitingTransactionNum,
			edge->waitingTransactionStamp
		};

		DistributedTransactionId blockingId = {
			edge->blockingNodeId,
			transactionOriginator,
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
		transactionNode->initiatorProc = NULL;
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


/*
 * LogCancellingBackend should only be called when a distributed transaction's
 * backend is cancelled due to distributed deadlocks. It sends which transaction
 * is cancelled and its corresponding pid to the log.
 */
static void
LogCancellingBackend(TransactionNode *transactionNode)
{
	StringInfo logMessage = NULL;

	if (!LogDistributedDeadlockDetection)
	{
		return;
	}

	logMessage = makeStringInfo();

	appendStringInfo(logMessage, "Cancelling the following backend "
								 "to resolve distributed deadlock "
								 "(transaction numner = %ld, pid = %d)",
					 transactionNode->transactionId.transactionNumber,
					 transactionNode->initiatorProc->pid);

	LogDistributedDeadlockDebugMessage(logMessage->data);
}


/*
 * LogTransactionNode converts the transaction node to a human readable form
 * and sends to the logs via LogDistributedDeadlockDebugMessage().
 */
static void
LogTransactionNode(TransactionNode *transactionNode)
{
	StringInfo logMessage = NULL;
	DistributedTransactionId *transactionId = NULL;

	if (!LogDistributedDeadlockDetection)
	{
		return;
	}

	logMessage = makeStringInfo();
	transactionId = &(transactionNode->transactionId);

	appendStringInfo(logMessage, "[DistributedTransactionId: (%d, %ld, %s)] = ",
					 transactionId->initiatorNodeIdentifier,
					 transactionId->transactionNumber,
					 timestamptz_to_str(transactionId->timestamp));

	appendStringInfo(logMessage, "[WaitsFor transaction numbers: %s]",
					 WaitsForToString(transactionNode->waitsFor));

	/* log the backend query if the proc is associated with the transaction */
	if (transactionNode->initiatorProc != NULL)
	{
		const char *backendQuery =
			pgstat_get_backend_current_activity(transactionNode->initiatorProc->pid,
												false);

		appendStringInfo(logMessage, "[Backend Query: %s]", backendQuery);
	}

	LogDistributedDeadlockDebugMessage(logMessage->data);
}


/*
 * LogDistributedDeadlockDebugMessage checks EnableDistributedDeadlockDebugging flag. If
 * it is true, the input message is sent to the logs with LOG level. Also, current timestamp
 * is prepanded to the message.
 */
static void
LogDistributedDeadlockDebugMessage(const char *errorMessage)
{
	if (!LogDistributedDeadlockDetection)
	{
		return;
	}

	ereport(LOG, (errmsg("[%s] %s", timestamptz_to_str(GetCurrentTimestamp()),
						 errorMessage)));
}


/*
 * WaitsForToString is only intended for testing and debugging. It gets a
 * waitsForList and returns the list of transaction nodes' transactionNumber
 * in a string.
 */
char *
WaitsForToString(List *waitsFor)
{
	StringInfo transactionIdStr = makeStringInfo();
	ListCell *waitsForCell = NULL;

	foreach(waitsForCell, waitsFor)
	{
		TransactionNode *waitingNode = (TransactionNode *) lfirst(waitsForCell);

		if (transactionIdStr->len != 0)
		{
			appendStringInfoString(transactionIdStr, ",");
		}

		appendStringInfo(transactionIdStr, "%ld",
						 waitingNode->transactionId.transactionNumber);
	}

	return transactionIdStr->data;
}
