/*
 * lock_graph.h
 *
 * Data structures and functions for gathering lock graphs between
 * distributed transactions.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCK_GRAPH_H
#define LOCK_GRAPH_H


#include "postgres.h"
#include "datatype/timestamp.h"


/*
 * Describes an edge in a waiting-for graph of locks.  This isn't used for
 * deadlock-checking directly, but to gather the information necessary to
 * do so.
 *
 * The datatypes here are a bit looser than strictly necessary, because
 * they're transported as the return type from an SQL function.
 */
typedef struct WaitEdge
{
	int waitingPid;
	int waitingNodeId;
	int64 waitingTransactionNum;
	TimestampTz waitingTransactionStamp;

	int blockingPid;
	int blockingNodeId;
	int64 blockingTransactionNum;
	TimestampTz blockingTransactionStamp;

	/* blocking transaction is also waiting on a lock */
	bool isBlockingXactWaiting;
} WaitEdge;


/*
 * WaitGraph represent a graph of wait edges as an adjacency list.
 */
typedef struct WaitGraph
{
	int localNodeId;
	int allocatedSize;
	int edgeCount;
	WaitEdge *edges;
} WaitGraph;


extern WaitGraph * BuildGlobalWaitGraph(void);
extern bool IsProcessWaitingForLock(PGPROC *proc);
extern bool IsInDistributedTransaction(BackendData *backendData);


#endif /* LOCK_GRAPH_H */
