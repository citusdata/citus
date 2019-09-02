/*
 * lock_graph.h
 *
 * Data structures and functions for gathering lock graphs between
 * distributed transactions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCK_GRAPH_H
#define LOCK_GRAPH_H


#include "postgres.h"
#include "libpq-fe.h"

#include "datatype/timestamp.h"
#include "distributed/backend_data.h"
#include "storage/lock.h"


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
extern TimestampTz ParseTimestampTzField(PGresult *result, int rowIndex, int colIndex);
extern int64 ParseIntField(PGresult *result, int rowIndex, int colIndex);

/* some utility function to parse results */
extern int64 ParseIntField(PGresult *result, int rowIndex, int colIndex);
extern bool ParseBoolField(PGresult *result, int rowIndex, int colIndex);
extern TimestampTz ParseTimestampTzField(PGresult *result, int rowIndex, int colIndex);


#endif /* LOCK_GRAPH_H */
