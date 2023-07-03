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

#include "distributed/connection_management.h"
#include "distributed/worker_manager.h"
#include "storage/lockdefs.h"


/*
 * TargetWorkerSet is used for determining the type of workers that a command
 * is targeted to.
 */
typedef enum TargetWorkerSet
{
	/*
	 * All the active primary nodes in the metadata which have metadata
	 * except the coordinator
	 */
	NON_COORDINATOR_METADATA_NODES,

	/*
	 * All the active primary nodes in the metadata except the coordinator
	 */
	NON_COORDINATOR_NODES,

	/*
	 * All active primary nodes in the metadata
	 */
	ALL_SHARD_NODES,

	/*
	 * All the active primary nodes in the metadata which have metadata
	 * (includes the coodinator if it is added)
	 */
	METADATA_NODES
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
extern bool SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
	MultiConnection *workerConnection,
	List *
	commandList);
extern bool SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction(const
																			char *nodeName,
																			int32 nodePort,
																			const char *
																			nodeUser,
																			List *
																			commandList);
extern void SendCommandToWorkersWithMetadata(const char *command);
extern void SendCommandToWorkersWithMetadataViaSuperUser(const char *command);
extern void SendCommandListToWorkersWithMetadataViaSuperUser(List *commands);
extern void SendBareCommandListToMetadataWorkers(List *commandList);
extern void EnsureNoModificationsHaveBeenDone(void);
extern void SendCommandListToWorkerOutsideTransaction(const char *nodeName,
													  int32 nodePort,
													  const char *nodeUser,
													  List *commandList);
extern void SendCommandListToWorkerOutsideTransactionWithConnection(
	MultiConnection *workerConnection,
	List *commandList);
extern void SendCommandListToWorkerListWithBareConnections(List *workerConnections,
														   List *commandList);
extern void SendMetadataCommandListToWorkerListInCoordinatedTransaction(
	List *workerNodeList,
	const char *
	nodeUser,
	List *commandList);
extern void RemoveWorkerTransaction(const char *nodeName, int32 nodePort);

/* helper functions for worker transactions */
extern bool IsWorkerTransactionActive(void);

extern bool IsWorkerTheCurrentNode(WorkerNode *workerNode);

#endif /* WORKER_TRANSACTION_H */
