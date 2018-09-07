/*-------------------------------------------------------------------------
 *
 * worker_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  workers.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_TRANSACTION_H
#define WORKER_TRANSACTION_H

#include "distributed/worker_manager.h"


/*
 * TargetWorkerSet is used for determining the type of workers that a command
 * is targeted to.
 */
typedef enum TargetWorkerSet
{
	WORKERS_WITH_METADATA,
	OTHER_WORKERS,
	ALL_WORKERS
} TargetWorkerSet;


/* Functions declarations for worker transactions */
extern List * GetWorkerTransactions(void);
extern void SendCommandToWorker(char *nodeName, int32 nodePort, char *command);
extern void SendCommandToFirstWorker(char *command);
extern void SendCommandToWorkers(TargetWorkerSet targetWorkerSet, char *command);
extern void SendBareCommandListToWorkers(TargetWorkerSet targetWorkerSet,
										 List *commandList);
extern void SendCommandToWorkersParams(TargetWorkerSet targetWorkerSet, char *command,
									   int parameterCount, const Oid *parameterTypes,
									   const char *const *parameterValues);
extern void SendCommandListToWorkerInSingleTransaction(char *nodeName, int32 nodePort,
													   char *nodeUser, List *commandList);
extern void RemoveWorkerTransaction(char *nodeName, int32 nodePort);

/* helper functions for worker transactions */
extern bool IsWorkerTransactionActive(void);

#endif /* WORKER_TRANSACTION_H */
