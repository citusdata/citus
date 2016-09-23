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


/*
 * In memory representation of pg_dist_node table elements.
 *
 * Caution: In some places we use memcpy to copy WorkerNodes. Those sites will have to be
 * changed if you add any pointers to this structure.
 */
typedef struct WorkerNode
{
	uint32 nodeId;                      /* node's unique id */
	uint32 workerPort;                  /* node's port */
	char workerName[WORKER_LENGTH];     /* node's name */
	uint32 groupId;                     /* node's groupId; same for the nodes that are in the same group */
} WorkerNode;


/* Config variables managed via guc.c */
extern int MaxWorkerNodesTracked;


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

/* Function declarations for worker node utilities */
extern int CompareWorkerNodes(const void *leftElement, const void *rightElement);

#endif   /* WORKER_MANAGER_H */
