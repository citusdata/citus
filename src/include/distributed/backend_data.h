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
	uint64 globalPID;
	bool distributedCommandOriginator;
	DistributedTransactionId transactionId;
} BackendData;


extern void InitializeBackendManagement(void);
extern int TotalProcCount(void);
extern void InitializeBackendData(void);
extern void LockBackendSharedMemory(LWLockMode lockMode);
extern void UnlockBackendSharedMemory(void);
extern void UnSetDistributedTransactionId(void);
extern void UnSetGlobalPID(void);
extern void AssignDistributedTransactionId(void);
extern void AssignGlobalPID(void);
extern uint64 GetGlobalPID(void);
extern void SetBackendDataDistributedCommandOriginator(bool
													   distributedCommandOriginator);
extern uint64 ExtractGlobalPID(char *applicationName);
extern int ExtractNodeIdFromGlobalPID(uint64 globalPID, bool missingOk);
extern int ExtractProcessIdFromGlobalPID(uint64 globalPID);
extern void GetBackendDataForProc(PGPROC *proc, BackendData *result);
extern void CancelTransactionDueToDeadlock(PGPROC *proc);
extern bool MyBackendGotCancelledDueToDeadlock(bool clearState);
extern bool MyBackendIsInDisributedTransaction(void);
extern List * ActiveDistributedTransactionNumbers(void);
extern LocalTransactionId GetMyProcLocalTransactionId(void);
extern int GetExternalClientBackendCount(void);
extern uint32 IncrementExternalClientBackendCounter(void);
extern void DecrementExternalClientBackendCounter(void);

#define INVALID_CITUS_INTERNAL_BACKEND_GPID 0
#define GLOBAL_PID_NODE_ID_FOR_NODES_NOT_IN_METADATA 99999999

#endif /* BACKEND_DATA_H */
