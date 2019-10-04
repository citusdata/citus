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
#include "storage/lockdefs.h"


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
extern List * TargetWorkerSetNodeList(TargetWorkerSet targetWorkerSet, LOCKMODE lockMode);
extern void SendCommandToWorker(char *nodeName, int32 nodePort, const char *command);
extern void SendCommandToWorkersAsUser(TargetWorkerSet targetWorkerSet, const
									   char *nodeUser,
									   const char *command);
extern void SendCommandToWorkerAsUser(char *nodeName, int32 nodePort,
									  const char *nodeUser, const char *command);
extern void SendCommandToWorkers(TargetWorkerSet targetWorkerSet, const char *command);
extern void SendBareCommandListToWorkers(TargetWorkerSet targetWorkerSet,
										 List *commandList);
extern int SendBareOptionalCommandListToWorkersAsUser(TargetWorkerSet targetWorkerSet,
													  List *commandList,
													  const char *user);
extern void SendCommandToWorkersParams(TargetWorkerSet targetWorkerSet,
									   const char *command, const char *user,
									   int parameterCount, const Oid *parameterTypes,
									   const char *const *parameterValues);
extern void EnsureNoModificationsHaveBeenDone(void);
extern void SendCommandListToWorkerInSingleTransaction(const char *nodeName,
													   int32 nodePort,
													   const char *nodeUser,
													   List *commandList);
extern void RemoveWorkerTransaction(char *nodeName, int32 nodePort);

/* helper functions for worker transactions */
extern bool IsWorkerTransactionActive(void);

#endif /* WORKER_TRANSACTION_H */
