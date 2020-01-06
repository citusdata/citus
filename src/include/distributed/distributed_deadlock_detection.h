/*-------------------------------------------------------------------------
 *
 * distributed_deadlock_detection.h
 *	  Type and function declarations used for performing distributed deadlock
 *	  detection.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_DEADLOCK_DETECTION_H
#define DISTRIBUTED_DEADLOCK_DETECTION_H

#include "postgres.h"

#include "access/hash.h"
#include "distributed/backend_data.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/transaction_identifier.h"
#include "nodes/pg_list.h"

typedef struct TransactionNode
{
	DistributedTransactionId transactionId;

	/* list of TransactionNode that this distributed transaction is waiting for */
	List *waitsFor;

	/* backend that is on the initiator node */
	PGPROC *initiatorProc;

	bool transactionVisited;
} TransactionNode;


/* GUC, determining whether debug messages for deadlock detection sent to LOG */
extern bool LogDistributedDeadlockDetection;


extern bool CheckForDistributedDeadlocks(void);
extern HTAB * BuildAdjacencyListsForWaitGraph(WaitGraph *waitGraph);
extern char * WaitsForToString(List *waitsFor);


#endif /* DISTRIBUTED_DEADLOCK_DETECTION_H */
