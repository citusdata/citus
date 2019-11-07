/*-------------------------------------------------------------------------
 *
 * worker_manager.h
 *	  Header and type declarations for managing worker nodes and for placing
 *	  shards on worker nodes in an intelligent manner.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_MANAGER_H
#define WORKER_MANAGER_H

#include "postgres.h"

#include "storage/lmgr.h"
#include "storage/lockdefs.h"
#include "nodes/pg_list.h"


/* Worker nodeName's, nodePort's, and nodeCluster's maximum length */
#define WORKER_LENGTH 256

/* Maximum length of worker port number (represented as string) */
#define MAX_PORT_LENGTH 10

/* Implementation specific definitions used in finding worker nodes */
#define WORKER_RACK_TRIES 5
#define WORKER_DEFAULT_RACK "default"

#define WORKER_DEFAULT_CLUSTER "default"

/*
 * In memory representation of pg_dist_node table elements. The elements are hold in
 * WorkerNodeHash table.
 * IMPORTANT: The order of the fields in this definition should match the
 * column order of pg_dist_node
 */
typedef struct WorkerNode
{
	uint32 nodeId;                      /* node's unique id, key of the hash table */
	uint32 workerPort;                  /* node's port */
	char workerName[WORKER_LENGTH];     /* node's name */
	int32 groupId;                      /* node's groupId; same for the nodes that are in the same group */
	char workerRack[WORKER_LENGTH];     /* node's network location */
	bool hasMetadata;                   /* node gets metadata changes */
	bool isActive;                      /* node's state */
	Oid nodeRole;                       /* the node's role in its group */
	char nodeCluster[NAMEDATALEN];      /* the cluster the node is a part of */
	bool metadataSynced;                /* node has the most recent metadata */
	bool shouldHaveShards;              /* if the node should have distributed table shards on it or not */
} WorkerNode;


/* Config variables managed via guc.c */
extern int MaxWorkerNodesTracked;
extern char *WorkerListFileName;
extern char *CurrentCluster;


/* Function declarations for finding worker nodes to place shards on */
extern WorkerNode * WorkerGetRandomCandidateNode(List *currentNodeList);
extern WorkerNode * WorkerGetRoundRobinCandidateNode(List *workerNodeList,
													 uint64 shardId,
													 uint32 placementIndex);
extern WorkerNode * WorkerGetLocalFirstCandidateNode(List *currentNodeList);
extern uint32 ActivePrimaryNodeCount(void);
extern List * ActivePrimaryNodeList(LOCKMODE lockMode);
extern List * ActivePrimaryShouldHaveShardsNodeList(LOCKMODE lockMode);
extern uint32 ActiveReadableNodeCount(void);
extern List * ActiveReadableNodeList(void);
extern WorkerNode * GetWorkerNodeByNodeId(int nodeId);
extern WorkerNode * FindWorkerNode(char *nodeName, int32 nodePort);
extern WorkerNode * FindWorkerNodeAnyCluster(const char *nodeName, int32 nodePort);
extern List * ReadWorkerNodes(bool includeNodesFromOtherClusters);
extern void EnsureCoordinator(void);
extern uint32 GroupForNode(char *nodeName, int32 nodePorT);
extern WorkerNode * PrimaryNodeForGroup(int32 groupId, bool *groupContainsNodes);
extern bool WorkerNodeIsPrimary(WorkerNode *worker);
extern bool WorkerNodeIsSecondary(WorkerNode *worker);
extern bool WorkerNodeIsPrimaryShouldHaveShardsNode(WorkerNode *worker);
extern bool WorkerNodeIsReadable(WorkerNode *worker);
extern uint32 CountPrimariesWithMetadata(void);
extern WorkerNode * GetFirstPrimaryWorkerNode(void);

/* Function declarations for worker node utilities */
extern int CompareWorkerNodes(const void *leftElement, const void *rightElement);
extern int WorkerNodeCompare(const void *lhsKey, const void *rhsKey, Size keySize);

#endif   /* WORKER_MANAGER_H */
