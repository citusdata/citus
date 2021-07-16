/*
 * backend_data.h
 *
 * Data structure definition for managing backend data and related function
 * declarations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef BACKEND_DATA_H
#define BACKEND_DATA_H


#include "access/twophase.h"
#include "datatype/timestamp.h"
#include "distributed/transaction_identifier.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/s_lock.h"


/*
 * CitusInitiatedBackend keeps some information about the backends that are
 * initiated by Citus.
 */
typedef struct CitusInitiatedBackend
{
	int initiatorNodeIdentifier;
	bool transactionOriginator;
} CitusInitiatedBackend;


/*
 * Each backend's active distributed transaction information is tracked via
 * BackendData in shared memory.
 *
 * DistributedTransactionId already has the same fields that CitusInitiatedBackend
 * has. However, we prefer to keep them seperate since CitusInitiatedBackend is a
 * broader concept which covers the backends that are not initiated via a distributed
 * transaction as well. In other words, we could have backends that
 * CitusInitiatedBackend is set but DistributedTransactionId is not set such as an
 * "INSERT" query which is not inside a transaction block.
 */
typedef struct BackendData
{
	Oid databaseId;
	Oid userId;
	slock_t mutex;
	bool cancelledDueToDeadlock;
	CitusInitiatedBackend citusBackend;
	DistributedTransactionId transactionId;
} BackendData;


extern void InitializeBackendManagement(void);
extern int TotalProcCount(void);
extern void InitializeBackendData(void);
extern void LockBackendSharedMemory(LWLockMode lockMode);
extern void UnlockBackendSharedMemory(void);
extern void UnSetDistributedTransactionId(void);
extern void AssignDistributedTransactionId(void);
extern void MarkCitusInitiatedCoordinatorBackend(void);
extern void GetBackendDataForProc(PGPROC *proc, BackendData *result);
extern void CancelTransactionDueToDeadlock(PGPROC *proc);
extern bool MyBackendGotCancelledDueToDeadlock(bool clearState);
extern bool MyBackendIsInDisributedTransaction(void);
extern List * ActiveDistributedTransactionNumbers(void);
extern LocalTransactionId GetMyProcLocalTransactionId(void);
extern int GetAllActiveClientBackendCount(void);
extern void IncrementClientBackendCounter(void);
extern void DecrementClientBackendCounter(void);

#endif /* BACKEND_DATA_H */
