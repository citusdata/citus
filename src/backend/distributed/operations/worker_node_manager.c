/*-------------------------------------------------------------------------
 *
 * worker_node_manager.c
 *	  Routines for reading worker nodes from membership file, and allocating
 *	  candidate nodes for shard placement.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "common/ip.h"
#include "libpq/hba.h"
#include "libpq/libpq-be.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "distributed/coordinator_protocol.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/worker_manager.h"


/* Config variables managed via guc.c */
char *WorkerListFileName;
int MaxWorkerNodesTracked = 2048;    /* determines worker node hash table size */


/* Local functions forward declarations */
static bool NodeIsPrimaryWorker(WorkerNode *node);
static bool NodeIsReadableWorker(WorkerNode *node);


/* ------------------------------------------------------------
 * Worker node selection functions follow
 * ------------------------------------------------------------
 */


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
 * ActivePrimaryNonCoordinatorNodeCount returns the number of groups with a primary in the cluster.
 * This method excludes coordinator even if it is added as a worker to cluster.
 */
uint32
ActivePrimaryNonCoordinatorNodeCount(void)
{
	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	uint32 liveWorkerCount = list_length(workerNodeList);

	return liveWorkerCount;
}


/*
 * ActiveReadableNodeCount returns the number of nodes in the cluster.
 */
uint32
ActiveReadableNodeCount(void)
{
	List *nodeList = ActiveReadableNodeList();
	return list_length(nodeList);
}


/*
 * NodeIsCoordinator returns true if the given node represents the coordinator.
 */
bool
NodeIsCoordinator(WorkerNode *node)
{
	return node->groupId == COORDINATOR_GROUP_ID;
}


/*
 * ActiveNodeListFilterFunc returns a list of all active nodes that checkFunction
 * returns true for.
 * lockMode specifies which lock to use on pg_dist_node, this is necessary when
 * the caller wouldn't want nodes to be added concurrent to their use of this list
 */
static List *
FilterActiveNodeListFunc(LOCKMODE lockMode, bool (*checkFunction)(WorkerNode *))
{
	List *workerNodeList = NIL;
	WorkerNode *workerNode = NULL;
	HASH_SEQ_STATUS status;

	Assert(checkFunction != NULL);

	if (lockMode != NoLock)
	{
		LockRelationOid(DistNodeRelationId(), lockMode);
	}

	HTAB *workerNodeHash = GetWorkerNodeHash();
	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		if (workerNode->isActive && checkFunction(workerNode))
		{
			WorkerNode *workerNodeCopy = palloc0(sizeof(WorkerNode));
			*workerNodeCopy = *workerNode;
			workerNodeList = lappend(workerNodeList, workerNodeCopy);
		}
	}

	return workerNodeList;
}


/*
 * ActivePrimaryNonCoordinatorNodeList returns a list of all active primary worker nodes
 * in workerNodeHash. lockMode specifies which lock to use on pg_dist_node,
 * this is necessary when the caller wouldn't want nodes to be added concurrent
 * to their use of this list.
 * This method excludes coordinator even if it is added as a worker to cluster.
 */
List *
ActivePrimaryNonCoordinatorNodeList(LOCKMODE lockMode)
{
	EnsureModificationsCanRun();
	return FilterActiveNodeListFunc(lockMode, NodeIsPrimaryWorker);
}


/*
 * ActivePrimaryNodeList returns a list of all active primary nodes in
 * workerNodeHash.
 */
List *
ActivePrimaryNodeList(LOCKMODE lockMode)
{
	EnsureModificationsCanRun();
	return FilterActiveNodeListFunc(lockMode, NodeIsPrimary);
}


/*
 * ActivePrimaryRemoteNodeList returns a list of all active primary nodes in
 * workerNodeHash.
 */
List *
ActivePrimaryRemoteNodeList(LOCKMODE lockMode)
{
	EnsureModificationsCanRun();
	return FilterActiveNodeListFunc(lockMode, NodeIsPrimaryAndRemote);
}


/*
 * NodeIsPrimaryWorker returns true if the node is a primary worker node.
 */
static bool
NodeIsPrimaryWorker(WorkerNode *node)
{
	return !NodeIsCoordinator(node) && NodeIsPrimary(node);
}


/*
 * CoordinatorAddedAsWorkerNode returns true if coordinator is added to the
 * pg_dist_node.
 */
bool
CoordinatorAddedAsWorkerNode()
{
	bool groupContainsNodes = false;

	PrimaryNodeForGroup(COORDINATOR_GROUP_ID, &groupContainsNodes);

	return groupContainsNodes;
}


/*
 * ReferenceTablePlacementNodeList returns the set of nodes that should have
 * reference table placements. This includes all primaries, including the
 * coordinator if known.
 */
List *
ReferenceTablePlacementNodeList(LOCKMODE lockMode)
{
	EnsureModificationsCanRun();
	return FilterActiveNodeListFunc(lockMode, NodeIsPrimary);
}


/*
 * CoordinatorNodeIfAddedAsWorkerOrError returns the WorkerNode object for
 * coordinator node if it is added to pg_dist_node, otherwise errors out.
 * Also, as CoordinatorAddedAsWorkerNode acquires AccessShareLock on pg_dist_node
 * and doesn't release it, callers can safely assume coordinator won't be
 * removed from metadata until the end of transaction when this function
 * returns coordinator node.
 */
WorkerNode *
CoordinatorNodeIfAddedAsWorkerOrError()
{
	ErrorIfCoordinatorNotAddedAsWorkerNode();

	WorkerNode *coordinatorNode = LookupNodeForGroup(COORDINATOR_GROUP_ID);

	WorkerNode *coordinatorNodeCopy = palloc0(sizeof(WorkerNode));
	*coordinatorNodeCopy = *coordinatorNode;

	return coordinatorNodeCopy;
}


/*
 * ErrorIfCoordinatorNotAddedAsWorkerNode errors out if coordinator is not added
 * to metadata.
 */
void
ErrorIfCoordinatorNotAddedAsWorkerNode()
{
	if (CoordinatorAddedAsWorkerNode())
	{
		return;
	}

	ereport(ERROR, (errmsg("operation is not allowed when coordinator "
						   "is not added into metadata"),
					errhint("Use \"SELECT citus_set_coordinator_host('"
							"<hostname>', '<port>')\" to configure the "
							"coordinator hostname and port")));
}


/*
 * DistributedTablePlacementNodeList returns a list of all active, primary
 * worker nodes that can store new data, i.e shouldstoreshards is 'true'
 */
List *
DistributedTablePlacementNodeList(LOCKMODE lockMode)
{
	EnsureModificationsCanRun();
	return FilterActiveNodeListFunc(lockMode, NodeCanHaveDistTablePlacements);
}


/*
 * NodeCanHaveDistTablePlacements returns true if the given node can have
 * shards of a distributed table.
 */
bool
NodeCanHaveDistTablePlacements(WorkerNode *node)
{
	if (!NodeIsPrimary(node))
	{
		return false;
	}

	return node->shouldHaveShards;
}


/*
 * ActiveReadableNonCoordinatorNodeList returns a list of all nodes in workerNodeHash
 * that are readable nodes This method excludes coordinator.
 */
List *
ActiveReadableNonCoordinatorNodeList(void)
{
	return FilterActiveNodeListFunc(NoLock, NodeIsReadableWorker);
}


/*
 * ActiveReadableNodeList returns a list of all nodes in workerNodeHash
 * that are readable workers.
 * This method includes coordinator if it is added as a worker to the cluster.
 */
List *
ActiveReadableNodeList(void)
{
	return FilterActiveNodeListFunc(NoLock, NodeIsReadable);
}


/*
 * NodeIsReadableWorker returns true if the given node is a readable worker node.
 */
static bool
NodeIsReadableWorker(WorkerNode *node)
{
	return !NodeIsCoordinator(node) && NodeIsReadable(node);
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
	Size ignoredKeySize = 0;

	int compare = WorkerNodeCompare(leftWorker, rightWorker, ignoredKeySize);

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

	return NodeNamePortCompare(workerLhs->workerName, workerRhs->workerName,
							   workerLhs->workerPort, workerRhs->workerPort);
}


/*
 * WorkerNodeHashCode computes the hash code for a worker node from the node's
 * host name and port number. Nodes that only differ by their rack locations
 * hash to the same value.
 */
uint32
WorkerNodeHashCode(const void *key, Size keySize)
{
	const WorkerNode *worker = (const WorkerNode *) key;
	const char *workerName = worker->workerName;
	const uint32 *workerPort = &(worker->workerPort);

	/* standard hash function outlined in Effective Java, Item 8 */
	uint32 result = 17;
	result = 37 * result + string_hash(workerName, WORKER_LENGTH);
	result = 37 * result + tag_hash(workerPort, sizeof(uint32));
	return result;
}


/*
 * NodeNamePortCompare implements the common logic for comparing two nodes
 * with their given nodeNames and ports.
 *
 * This function is useful for ensuring consistency of sort operations between
 * different representations of nodes in the cluster such as WorkerNode and
 * WorkerPool.
 */
int
NodeNamePortCompare(const char *workerLhsName, const char *workerRhsName,
					int workerLhsPort, int workerRhsPort)
{
	int nameCompare = strncmp(workerLhsName, workerRhsName, WORKER_LENGTH);
	if (nameCompare != 0)
	{
		return nameCompare;
	}

	int portCompare = workerLhsPort - workerRhsPort;
	return portCompare;
}


/*
 * GetFirstPrimaryWorkerNode returns the primary worker node with the
 * lowest rank based on CompareWorkerNodes.
 *
 * The ranking is arbitrary, but needs to be kept consistent with IsFirstWorkerNode.
 */
WorkerNode *
GetFirstPrimaryWorkerNode(void)
{
	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(RowShareLock);
	WorkerNode *firstWorkerNode = NULL;
	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		if (firstWorkerNode == NULL ||
			CompareWorkerNodes(&workerNode, &firstWorkerNode) < 0)
		{
			firstWorkerNode = workerNode;
		}
	}

	return firstWorkerNode;
}
