/*-------------------------------------------------------------------------
 *
 * worker_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  workers.
 *
 * Copyright (c) Citus Data, Inc.
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
	NON_COORDINATOR_METADATA_NODES,
	NON_COORDINATOR_NODES,
	ALL_SHARD_NODES
} TargetWorkerSet;


/* Functions declarations for worker transactions */
extern List * GetWorkerTransactions(void);
extern List * TargetWorkerSetNodeList(TargetWorkerSet targetWorkerSet, LOCKMODE lockMode);
extern void SendCommandToWorker(const char *nodeName, int32 nodePort, const
								char *command);
extern void SendCommandToWorkersAsUser(TargetWorkerSet targetWorkerSet,
									   const char *nodeUser, const char *command);
extern void SendCommandToWorkerAsUser(const char *nodeName, int32 nodePort,
									  const char *nodeUser, const char *command);
extern bool SendOptionalCommandListToWorkerOutsideTransaction(const char *nodeName,
															  int32 nodePort,
															  const char *nodeUser,
															  List *commandList);
extern bool SendOptionalCommandListToWorkerInCoordinatedTransaction(const char *nodeName,
																	int32 nodePort,
																	const char *nodeUser,
																	List *commandList);
extern void SendCommandToWorkersWithMetadata(const char *command);
extern void SendBareCommandListToMetadataWorkers(List *commandList);
extern void EnsureNoModificationsHaveBeenDone(void);
extern void SendCommandListToWorkerOutsideTransaction(const char *nodeName,
													  int32 nodePort,
													  const char *nodeUser,
													  List *commandList);
extern void SendCommandListToWorkerInCoordinatedTransaction(const char *nodeName,
															int32 nodePort,
															const char *nodeUser,
															List *commandList);
extern void SendCommandToWorkersOptionalInParallel(TargetWorkerSet targetWorkerSet,
												   const char *command,
												   const char *user);
void SendCommandToWorkersInParallel(TargetWorkerSet targetWorkerSet,
									const char *command, const char *user);
extern void RemoveWorkerTransaction(const char *nodeName, int32 nodePort);

/* helper functions for worker transactions */
extern bool IsWorkerTransactionActive(void);

#endif /* WORKER_TRANSACTION_H */
