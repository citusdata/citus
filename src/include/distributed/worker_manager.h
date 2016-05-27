/*-------------------------------------------------------------------------
 *
 * worker_manager.h
 *	  Header and type declarations for managing worker nodes and for placing
 *	  shards on worker nodes in an intelligent manner.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_MANAGER_H
#define WORKER_MANAGER_H

#include "nodes/pg_list.h"


/* Worker node name's maximum length */
#define WORKER_LENGTH 256

/* Maximum length of worker port number (represented as string) */
#define MAX_PORT_LENGTH 10

/* default filename for citus.worker_list_file */
#define WORKER_LIST_FILENAME "pg_worker_list.conf"

/* Implementation specific definitions used in finding worker nodes */
#define WORKER_RACK_TRIES 5
#define WORKER_DEFAULT_RACK "default"


/*
 * WorkerNode keeps shared memory state for active and temporarily failed worker
 * nodes. Permanently failed or departed nodes on the other hand are eventually
 * purged from the shared hash. In the current implementation, the distinction
 * between active, temporarily failed, and permanently departed nodes is made
 * based on the node's presence in the membership file; only nodes in this file
 * appear in the shared hash. In the future, worker nodes will report their
 * health status to the master via heartbeats, and these heartbeats along with
 * membership information will be used to determine a worker node's liveliness.
 */
typedef struct WorkerNode
{
	uint32 workerPort;              /* node's port; part of hash table key */
	char workerName[WORKER_LENGTH]; /* node's name; part of hash table key */
	char workerRack[WORKER_LENGTH]; /* node's network location */

	bool inWorkerFile;              /* is node in current membership file? */
} WorkerNode;


/* Config variables managed via guc.c */
extern int MaxWorkerNodesTracked;
extern char *WorkerListFileName;


/* Function declarations for finding worker nodes to place shards on */
extern WorkerNode * WorkerGetRandomCandidateNode(List *currentNodeList);
extern WorkerNode * WorkerGetRoundRobinCandidateNode(List *workerNodeList,
													 uint64 shardId,
													 uint32 placementIndex);
extern WorkerNode * WorkerGetLocalFirstCandidateNode(List *currentNodeList);
extern WorkerNode * WorkerGetNodeWithName(const char *hostname);
extern uint32 WorkerGetLiveNodeCount(void);
extern List * WorkerNodeList(void);
extern bool WorkerNodeActive(const char *nodeName, uint32 nodePort);
extern List * ResponsiveWorkerNodeList(void);

/* Function declarations for loading into shared hash tables */
extern void WorkerNodeRegister(void);
extern void LoadWorkerNodeList(const char *workerFilename);

/* Function declarations for worker node utilities */
extern int CompareWorkerNodes(const void *leftElement, const void *rightElement);

#endif   /* WORKER_MANAGER_H */
