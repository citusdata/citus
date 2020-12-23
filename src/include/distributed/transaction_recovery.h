/*-------------------------------------------------------------------------
 *
 * transaction_recovery.h
 *	  Type and function declarations used in recovering 2PC transactions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSACTION_RECOVERY_H
#define TRANSACTION_RECOVERY_H


/* GUC to configure interval for 2PC auto-recovery */
extern int Recover2PCInterval;


/* Functions declarations for worker transactions */
extern void LogTransactionRecord(int32 groupId, char *transactionName);
extern int RecoverTwoPhaseCommits(void);
extern void DeleteWorkerTransactions(WorkerNode *workerNode);

#endif /* TRANSACTION_RECOVERY_H */
