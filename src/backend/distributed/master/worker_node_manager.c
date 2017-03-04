/*-------------------------------------------------------------------------
 *
 * worker_node_manager.c
 *	  Routines for reading worker nodes from membership file, and allocating
 *	  candidate nodes for shard placement.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "distributed/worker_manager.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/pg_dist_node.h"
#include "libpq/hba.h"
#include "libpq/ip.h"
#include "libpq/libpq-be.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/* Config variables managed via guc.c */
char *WorkerListFileName;
int MaxWorkerNodesTracked = 2048;    /* determines worker node hash table size */


/* Local functions forward declarations */
static char * ClientHostAddress(StringInfo remoteHostStringInfo);
static WorkerNode * FindRandomNodeFromList(List *workerNodeList);
static WorkerNode * WorkerGetNodeWithName(const char *hostname);
static bool OddNumber(uint32 number);
static bool ListMember(List *currentList, WorkerNode *workerNode);

static List * PrimaryNodesNotInList(List *currentList);


/* ------------------------------------------------------------
 * Worker node selection functions follow
 * ------------------------------------------------------------
 */

/*
 * WorkerGetRandomCandidateNode takes in a list of worker nodes, and then allocates
 * a new worker node. The allocation is performed by randomly picking a primary worker
 * node which is not in currentNodeList.
 *
 * Note that the function returns null if the worker membership list does not
 * contain enough nodes to allocate a new worker node.
 */
WorkerNode *
WorkerGetRandomCandidateNode(List *currentNodeList)
{
	WorkerNode *workerNode = NULL;
	bool wantSameRack = false;
	uint32 tryCount = WORKER_RACK_TRIES;
	uint32 tryIndex = 0;

	uint32 currentNodeCount = list_length(currentNodeList);
	List *candidateWorkerNodeList = PrimaryNodesNotInList(currentNodeList);

	/*
	 * We check if the shard has already been placed on all nodes known to us.
	 * This check is rather defensive, and has the drawback of performing a full
	 * scan over the worker node hash.
	 */
	if (list_length(candidateWorkerNodeList) == 0)
	{
		return NULL;
	}

	/* if current node list is empty, randomly pick one node and return */
	if (currentNodeCount == 0)
	{
		workerNode = FindRandomNodeFromList(candidateWorkerNodeList);
		return workerNode;
	}

	/*
	 * If the current list has an odd number of nodes (1, 3, 5, etc), we want to
	 * place the shard on a different rack than the first node's rack.
	 * Otherwise, we want to place the shard on the same rack as the first node.
	 */
	if (OddNumber(currentNodeCount))
	{
		wantSameRack = false;
	}
	else
	{
		wantSameRack = true;
	}

	/*
	 * We try to find a worker node that fits our rack-aware placement strategy.
	 * If after a predefined number of tries, we still cannot find such a node,
	 * we simply give up and return the last worker node we found.
	 */
	for (tryIndex = 0; tryIndex < tryCount; tryIndex++)
	{
		WorkerNode *firstNode = (WorkerNode *) linitial(currentNodeList);
		char *firstRack = firstNode->workerRack;
		char *workerRack = NULL;
		bool sameRack = false;

		workerNode = FindRandomNodeFromList(candidateWorkerNodeList);
		workerRack = workerNode->workerRack;

		sameRack = (strncmp(workerRack, firstRack, WORKER_LENGTH) == 0);
		if ((sameRack && wantSameRack) || (!sameRack && !wantSameRack))
		{
			break;
		}
	}

	return workerNode;
}


/*
 * WorkerGetRoundRobinCandidateNode takes in a list of worker nodes and returns
 * a candidate worker node from that list. To select this node, this function
 * uses the round-robin policy. An ideal round-robin implementation requires
 * keeping shared state for shard placements; and we instead approximate our
 * implementation by relying on the ever-increasing shardId. So, the first
 * worker node selected will be the node at the (shardId MOD worker node count)
 * index and the remaining candidate nodes will be the next nodes in the list.
 *
 * Note that the function returns null if the worker membership list does not
 * contain enough nodes to place all replicas.
 */
WorkerNode *
WorkerGetRoundRobinCandidateNode(List *workerNodeList, uint64 shardId,
								 uint32 placementIndex)
{
	uint32 workerNodeCount = list_length(workerNodeList);
	WorkerNode *candidateNode = NULL;

	if (placementIndex < workerNodeCount)
	{
		uint32 candidateNodeIndex = (shardId + placementIndex) % workerNodeCount;
		candidateNode = (WorkerNode *) list_nth(workerNodeList, candidateNodeIndex);
	}

	return candidateNode;
}


/*
 * WorkerGetLocalFirstCandidateNode takes in a list of worker nodes, and then
 * allocates a new primary worker node. The allocation is performed according to the
 * following policy: if the list is empty, the node where the caller is connecting
 * from is allocated; if the list is not empty, a node is allocated according
 * to random policy.
 */
WorkerNode *
WorkerGetLocalFirstCandidateNode(List *currentNodeList)
{
	WorkerNode *candidateNode = NULL;
	uint32 currentNodeCount = list_length(currentNodeList);

	/* choose first candidate node to be the client's host */
	if (currentNodeCount == 0)
	{
		StringInfo clientHostStringInfo = makeStringInfo();
		char *clientHost = NULL;
		char *errorMessage = ClientHostAddress(clientHostStringInfo);

		if (errorMessage != NULL)
		{
			ereport(ERROR, (errmsg("%s", errorMessage),
							errdetail("Could not find the first worker "
									  "node for local-node-first policy."),
							errhint("Make sure that you are not on the "
									"master node.")));
		}

		/* if hostname is localhost.localdomain, change it to localhost */
		clientHost = clientHostStringInfo->data;
		if (strncmp(clientHost, "localhost.localdomain", WORKER_LENGTH) == 0)
		{
			clientHost = pstrdup("localhost");
		}

		candidateNode = WorkerGetNodeWithName(clientHost);
		if (candidateNode == NULL)
		{
			ereport(ERROR, (errmsg("could not find worker node for "
								   "host: %s", clientHost)));
		}
	}
	else
	{
		/* find a candidate node different from those already selected */
		candidateNode = WorkerGetRandomCandidateNode(currentNodeList);
	}

	return candidateNode;
}


/*
 * ClientHostAddress appends the connecting client's fully qualified hostname
 * to the given StringInfo. If there is no such connection or the connection is
 * over Unix domain socket, the function fills the error message and returns it.
 * On success, it just returns NULL.
 */
static char *
ClientHostAddress(StringInfo clientHostStringInfo)
{
	Port *port = MyProcPort;
	char *clientHost = NULL;
	char *errorMessage = NULL;
	int clientHostLength = NI_MAXHOST;
	int flags = NI_NAMEREQD;    /* require fully qualified hostname */
	int nameFound = 0;

	if (port == NULL)
	{
		errorMessage = "cannot find tcp/ip connection to client";
		return errorMessage;
	}

	switch (port->raddr.addr.ss_family)
	{
		case AF_INET:
#ifdef HAVE_IPV6
		case AF_INET6:
#endif
			{
				break;
			}

		default:
		{
			errorMessage = "invalid address family in connection";
			return errorMessage;
		}
	}

	clientHost = palloc0(clientHostLength);

	nameFound = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
								   clientHost, clientHostLength, NULL, 0, flags);
	if (nameFound == 0)
	{
		appendStringInfo(clientHostStringInfo, "%s", clientHost);
	}
	else
	{
		StringInfo errorMessageStringInfo = makeStringInfo();
		appendStringInfo(errorMessageStringInfo, "could not resolve client host: %s",
						 gai_strerror(nameFound));

		errorMessage = errorMessageStringInfo->data;
		return errorMessage;
	}

	return errorMessage;
}


/*
 * WorkerGetNodeWithName finds and returns a primary node from the membership list that
 * has the given hostname. The function returns null if no such node exists.
 */
static WorkerNode *
WorkerGetNodeWithName(const char *hostname)
{
	WorkerNode *workerNode = NULL;
	HASH_SEQ_STATUS status;
	HTAB *workerNodeHash = GetWorkerNodeHash();

	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		int nameCompare = strncmp(workerNode->workerName, hostname, WORKER_LENGTH);
		if (nameCompare == 0 && workerNode->nodeRole == NODE_ROLE_PRIMARY)
		{
			/* we need to terminate the scan since we break */
			hash_seq_term(&status);
			break;
		}
	}

	return workerNode;
}


/*
 * WorkerGetLiveGroupCount returns the number of groups which have a primary capable of
 * accepting writes.
 */
uint32
WorkerGetLiveGroupCount(void)
{
	HTAB *workerNodeHash = GetWorkerNodeHash();
	uint32 liveWorkerCount = 0;
	HASH_SEQ_STATUS status;
	WorkerNode *workerNode = NULL;

	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		if (workerNode->nodeRole == NODE_ROLE_PRIMARY)
		{
			liveWorkerCount++;
		}
	}

	return liveWorkerCount;
}


/*
 * WorkerNodeList iterates over the hash table that includes the worker nodes, and adds
 * them to a list which is returned.
 *
 * It only returns nodes which are primaries.
 */
List *
WorkerNodeList(void)
{
	List *workerNodeList = NIL;
	WorkerNode *workerNode = NULL;
	HTAB *workerNodeHash = GetWorkerNodeHash();
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		WorkerNode *workerNodeCopy;

		if (workerNode->nodeRole != NODE_ROLE_PRIMARY)
		{
			continue;
		}

		workerNodeCopy = palloc0(sizeof(WorkerNode));
		memcpy(workerNodeCopy, workerNode, sizeof(WorkerNode));
		workerNodeList = lappend(workerNodeList, workerNodeCopy);
	}

	return workerNodeList;
}


/*
 * WorkerNodesNotInList scans through the worker node hash and returns a list
 * of all primary nodes which are not in currentList.
 */
static List *
PrimaryNodesNotInList(List *currentList)
{
	List *workerNodeList = NIL;
	HTAB *workerNodeHash = GetWorkerNodeHash();
	WorkerNode *workerNode = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, workerNodeHash);

	/* this is O(n*m) but there usually aren't many nodes in currentList */
	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		WorkerNode *workerNodeCopy;

		if ((workerNode->nodeRole != NODE_ROLE_PRIMARY) ||
			ListMember(currentList, workerNode))
		{
			continue;
		}

		workerNodeCopy = palloc0(sizeof(WorkerNode));
		memcpy(workerNodeCopy, workerNode, sizeof(WorkerNode));
		workerNodeList = lappend(workerNodeList, workerNodeCopy);
	}

	return workerNodeList;
}


/* FindRandomNodeFromList picks a random node from the list provided to it */
static WorkerNode *
FindRandomNodeFromList(List *candidateWorkerNodeList)
{
	uint32 candidateNodeCount = list_length(candidateWorkerNodeList);

	/* nb, the random seed has already been set by the postmaster when starting up. */
	uint32 workerPosition = (random() % candidateNodeCount);

	WorkerNode *workerNode = (WorkerNode *) list_nth(candidateWorkerNodeList,
													 workerPosition);

	return workerNode;
}


/*
 * OddNumber function returns true if given number is odd; returns false otherwise.
 */
static bool
OddNumber(uint32 number)
{
	bool oddNumber = ((number % 2) == 1);
	return oddNumber;
}


/* Checks if given worker node is a member of the current list. */
static bool
ListMember(List *currentList, WorkerNode *workerNode)
{
	bool listMember = false;
	Size keySize = WORKER_LENGTH + sizeof(uint32);

	ListCell *currentCell = NULL;
	foreach(currentCell, currentList)
	{
		WorkerNode *currentNode = (WorkerNode *) lfirst(currentCell);
		if (WorkerNodeCompare(workerNode, currentNode, keySize) == 0)
		{
			listMember = true;
		}
	}

	return listMember;
}


/*
 * CompareWorkerNodes compares two pointers to worker nodes using the exact
 * same logic employed by WorkerNodeCompare.
 */
int
CompareWorkerNodes(const void *leftElement, const void *rightElement)
{
	const void *leftWorker = *((const void **) leftElement);
	const void *rightWorker = *((const void **) rightElement);
	int compare = 0;
	Size ignoredKeySize = 0;

	compare = WorkerNodeCompare(leftWorker, rightWorker, ignoredKeySize);

	return compare;
}


/*
 * WorkerNodeCompare compares two worker nodes by their host name and port
 * number. Two nodes that only differ by their rack locations are considered to
 * be equal to each other.
 */
int
WorkerNodeCompare(const void *lhsKey, const void *rhsKey, Size keySize)
{
	const WorkerNode *workerLhs = (const WorkerNode *) lhsKey;
	const WorkerNode *workerRhs = (const WorkerNode *) rhsKey;

	int nameCompare = 0;
	int portCompare = 0;

	nameCompare = strncmp(workerLhs->workerName, workerRhs->workerName, WORKER_LENGTH);
	if (nameCompare != 0)
	{
		return nameCompare;
	}

	portCompare = workerLhs->workerPort - workerRhs->workerPort;
	return portCompare;
}
