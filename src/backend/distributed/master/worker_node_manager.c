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
int MaxWorkerNodesTracked = 2048;    /* determines worker node hash table size */


/* Local functions forward declarations */
static char * ClientHostAddress(StringInfo remoteHostStringInfo);
static WorkerNode * FindRandomNodeNotInList(List *workerNodesList,
											List *currentNodeList);
static bool ListMember(List *currentList, WorkerNode *workerNode);
static int WorkerNodeCompare(const void *lhsKey, const void *rhsKey, Size keySize);
static bool WorkerNodeResponsive(const char *workerName, uint32 workerPort);


/* ------------------------------------------------------------
 * Worker node selection functions follow
 * ------------------------------------------------------------
 */

/*
 * WorkerGetRandomCandidateNode takes in a list of worker nodes, and then allocates
 * a new worker node. The allocation is performed a by randomly picking a worker node
 * which is not in currentNodeList.
 *
 * Note that the function returns null if the worker membership list does not
 * contain enough nodes to allocate a new worker node.
 */
WorkerNode *
WorkerGetRandomCandidateNode(List *currentNodeList)
{
	WorkerNode *workerNode = NULL;
	WorkerNode *resultWorkerNode = NULL;
	List *workerNodeList = GetWorkerNodeList();

	/*
	 * We check if the shard has already been placed on all nodes known to us.
	 */
	uint32 currentNodeCount = list_length(currentNodeList);
	uint32 liveNodeCount = list_length(workerNodeList);
	if (currentNodeCount >= liveNodeCount)
	{
		return NULL;
	}

	workerNode = FindRandomNodeNotInList(workerNodeList, currentNodeList);

	resultWorkerNode = palloc(sizeof(WorkerNode));
	memcpy(resultWorkerNode, workerNode, sizeof(WorkerNode));
	list_free_deep(workerNodeList);

	return resultWorkerNode;
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
 * allocates a new worker node. The allocation is performed according to the
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
 * WorkerGetNodeWithName finds and returns a node from the membership list that
 * has the given hostname. The function returns null if no such node exists.
 */
WorkerNode *
WorkerGetNodeWithName(const char *hostname)
{
	WorkerNode *resultWorkerNode = NULL;
	ListCell *workerNodeCell = NULL;
	List *workerNodeList = GetWorkerNodeList();

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		int nameCompare = strncmp(workerNode->workerName, hostname, WORKER_LENGTH);
		if (nameCompare == 0)
		{
			resultWorkerNode = palloc(sizeof(WorkerNode));
			memcpy(resultWorkerNode, workerNode, sizeof(WorkerNode));
			break;
		}
	}

	list_free_deep(workerNodeList);
	return resultWorkerNode;
}


/* Returns the number of live nodes in the cluster. */
uint32
WorkerGetLiveNodeCount(void)
{
	uint32 liveWorkerCount = 0;
	List *workerNodeList = GetWorkerNodeList();

	/* todo: this is such a waste of resources. we create a whole new list just to count
	 * it and immediately free it */

	liveWorkerCount = list_length(workerNodeList);

	list_free_deep(workerNodeList);
	return liveWorkerCount;
}


/*
 * WorkerNodeList returns a list of worker nodes, don't forget to free it after you've
 * used it!
 *
 * todo: add free to all call sites?
 */
List *
WorkerNodeList(void)
{
	List *workerNodeList = GetWorkerNodeList();
	return workerNodeList;
}


/*
 * WorkerNodeActive looks up a worker node with the given name and port number
 * in the current membership list. If such a worker node exists, the function
 * returns true.
 */
bool
WorkerNodeActive(const char *nodeName, uint32 nodePort)
{
	bool nodeActive = false;
	ListCell *workerNodeCell = NULL;
	List *workerNodeList = GetWorkerNodeList();

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		if (strncmp(workerNode->workerName, nodeName, WORKER_LENGTH) == 0 &&
			workerNode->workerPort == nodePort)
		{
			nodeActive = true;
			break;
		}
	}

	list_free_deep(workerNodeList);
	return nodeActive;
}


/*
 * FindRandomNodeNotInList finds a random node from the shared hash that is not
 * a member of the current node list. The caller is responsible for making the
 * necessary node count checks to ensure that such a node exists.
 *
 * This function is O(workerNodesList * currentNodeList). This is unfortunate but was
 * easy to implement.
 */
static WorkerNode *
FindRandomNodeNotInList(List *workerNodesList, List *currentNodeList)
{
	ListCell *workerNodeCell;

	uint32 workerPosition = 0;
	uint32 workerIndex = 0;

	uint32 workerNodeCount = list_length(workerNodesList);
	uint32 currentNodeCount = list_length(currentNodeList);

	Assert(workerNodeCount > currentNodeCount);

	/*
	 * Please note that the random seed has already been set by the postmaster when
	 * starting up.
	 */
	workerPosition = random() % (workerNodeCount - currentNodeCount);

	foreach(workerNodeCell, workerNodesList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		bool listMember = ListMember(currentNodeList, workerNode);

		if (listMember)
		{
			continue;
		}

		if (workerIndex == workerPosition)
		{
			return workerNode;
		}

		workerIndex++;
	}

	return NULL;
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
static int
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


/* ResponsiveWorkerNodeList returns a list of all responsive worker nodes */
List *
ResponsiveWorkerNodeList(void)
{
	List *responsiveWorkerNodeList = NULL;
	ListCell *workerNodeCell = NULL;
	List *workerNodeList = WorkerNodeList();

	foreach(workerNodeCell, workerNodeList)
	{
		bool workerNodeResponsive = false;
		WorkerNode *workerNode = lfirst(workerNodeCell);

		workerNodeResponsive = WorkerNodeResponsive(workerNode->workerName,
													workerNode->workerPort);
		if (workerNodeResponsive)
		{
			responsiveWorkerNodeList = lappend(responsiveWorkerNodeList, workerNode);
		}
	}

	return responsiveWorkerNodeList;
}


/*
 * WorkerNodeResponsive returns true if the given worker node is reponsive.
 * Otherwise, it returns false.
 *
 * This function is based on worker_node_responsive function present in the
 * shard rebalancer.
 */
static bool
WorkerNodeResponsive(const char *workerName, uint32 workerPort)
{
	bool workerNodeResponsive = false;

	int connectionId = MultiClientConnect(workerName, workerPort, NULL, NULL);
	if (connectionId != INVALID_CONNECTION_ID)
	{
		MultiClientDisconnect(connectionId);

		workerNodeResponsive = true;
	}

	return workerNodeResponsive;
}
