/*
 * node_metadata.c
 *	  Functions that operate on pg_dist_node
 *
 * Copyright (c) Citus Data, Inc.
 */
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"


#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/skey.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/sequence.h"
#include "distributed/citus_acquire_lock.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/maintenanced.h"
#include "distributed/master_protocol.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_node.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"


/* default group size */
int GroupSize = 1;

/* config variable managed via guc.c */
char *CurrentCluster = "default";

typedef struct NodeMetadata
{
	int32 groupId;
	char *nodeRack;
	bool hasMetadata;
	bool metadataSynced;
	bool isActive;
	Oid nodeRole;
	bool shouldHaveShards;
	char *nodeCluster;
} NodeMetadata;

/* local function forward declarations */
static int ActivateNode(char *nodeName, int nodePort);
static void RemoveNodeFromCluster(char *nodeName, int32 nodePort);
static int AddNodeMetadata(char *nodeName, int32 nodePort, NodeMetadata
						   *nodeMetadata, bool *nodeAlreadyExists);
static WorkerNode * SetNodeState(char *nodeName, int32 nodePort, bool isActive);
static HeapTuple GetNodeTuple(const char *nodeName, int32 nodePort);
static int32 GetNextGroupId(void);
static int GetNextNodeId(void);
static void InsertNodeRow(int nodeid, char *nodename, int32 nodeport, NodeMetadata
						  *nodeMetadata);
static void DeleteNodeRow(char *nodename, int32 nodeport);
static void SetUpDistributedTableDependencies(WorkerNode *workerNode);
static WorkerNode * TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple);
static WorkerNode * ModifiableWorkerNode(const char *nodeName, int32 nodePort);
static void UpdateNodeLocation(int32 nodeId, char *newNodeName, int32 newNodePort);
static bool UnsetMetadataSyncedForAll(void);
static WorkerNode * SetShouldHaveShards(WorkerNode *workerNode, bool shouldHaveShards);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_add_node);
PG_FUNCTION_INFO_V1(master_add_inactive_node);
PG_FUNCTION_INFO_V1(master_add_secondary_node);
PG_FUNCTION_INFO_V1(master_set_node_property);
PG_FUNCTION_INFO_V1(master_remove_node);
PG_FUNCTION_INFO_V1(master_disable_node);
PG_FUNCTION_INFO_V1(master_activate_node);
PG_FUNCTION_INFO_V1(master_update_node);
PG_FUNCTION_INFO_V1(get_shard_id_for_distribution_column);


/*
 * DefaultNodeMetadata creates a NodeMetadata struct with the fields set to
 * sane defaults, e.g. nodeRack = WORKER_DEFAULT_RACK.
 */
static NodeMetadata
DefaultNodeMetadata()
{
	NodeMetadata nodeMetadata = {
		.nodeRack = WORKER_DEFAULT_RACK,
		.shouldHaveShards = true,
	};
	return nodeMetadata;
}


/*
 * master_add_node function adds a new node to the cluster and returns its id. It also
 * replicates all reference tables to the new node.
 */
Datum
master_add_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	int nodeId = 0;

	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	bool nodeAlreadyExists = false;
	nodeMetadata.groupId = PG_GETARG_INT32(2);

	CheckCitusVersion(ERROR);

	/*
	 * During tests this function is called before nodeRole and nodeCluster have been
	 * created.
	 */
	if (PG_NARGS() == 3)
	{
		nodeMetadata.nodeRole = InvalidOid;
		nodeMetadata.nodeCluster = "default";
	}
	else
	{
		Name nodeClusterName = PG_GETARG_NAME(4);
		nodeMetadata.nodeCluster = NameStr(*nodeClusterName);

		nodeMetadata.nodeRole = PG_GETARG_OID(3);
	}

	nodeId = AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
							 &nodeAlreadyExists);

	/*
	 * After adding new node, if the node did not already exist, we will activate
	 * the node. This means we will replicate all reference tables to the new
	 * node.
	 */
	if (!nodeAlreadyExists)
	{
		ActivateNode(nodeNameString, nodePort);
	}

	PG_RETURN_INT32(nodeId);
}


/*
 * master_add_inactive_node function adds a new node to the cluster as inactive node
 * and returns id of the newly added node. It does not replicate reference
 * tables to the new node, it only adds new node to the pg_dist_node table.
 */
Datum
master_add_inactive_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	Name nodeClusterName = PG_GETARG_NAME(4);

	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	bool nodeAlreadyExists = false;
	int nodeId = 0;
	nodeMetadata.groupId = PG_GETARG_INT32(2);
	nodeMetadata.nodeRole = PG_GETARG_OID(3);
	nodeMetadata.nodeCluster = NameStr(*nodeClusterName);

	CheckCitusVersion(ERROR);

	nodeId = AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
							 &nodeAlreadyExists);

	PG_RETURN_INT32(nodeId);
}


/*
 * master_add_secondary_node adds a new secondary node to the cluster. It accepts as
 * arguments the primary node it should share a group with.
 */
Datum
master_add_secondary_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);

	text *primaryName = PG_GETARG_TEXT_P(2);
	int32 primaryPort = PG_GETARG_INT32(3);
	char *primaryNameString = text_to_cstring(primaryName);

	Name nodeClusterName = PG_GETARG_NAME(4);
	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	bool nodeAlreadyExists = false;
	int nodeId = 0;

	nodeMetadata.groupId = GroupForNode(primaryNameString, primaryPort);
	nodeMetadata.nodeCluster = NameStr(*nodeClusterName);
	nodeMetadata.nodeRole = SecondaryNodeRoleId();
	nodeMetadata.isActive = true;

	CheckCitusVersion(ERROR);

	nodeId = AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
							 &nodeAlreadyExists);

	PG_RETURN_INT32(nodeId);
}


/*
 * master_remove_node function removes the provided node from the pg_dist_node table of
 * the master node and all nodes with metadata.
 * The call to the master_remove_node should be done by the super user and the specified
 * node should not have any active placements.
 * This function also deletes all reference table placements belong to the given node from
 * pg_dist_placement, but it does not drop actual placement at the node. In the case of
 * re-adding the node, master_add_node first drops and re-creates the reference tables.
 */
Datum
master_remove_node(PG_FUNCTION_ARGS)
{
	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	CheckCitusVersion(ERROR);

	RemoveNodeFromCluster(text_to_cstring(nodeNameText), nodePort);

	PG_RETURN_VOID();
}


/*
 * master_disable_node function sets isactive value of the provided node as inactive at
 * master node and all nodes with metadata regardless of the node having an active shard
 * placement.
 *
 * The call to the master_disable_node must be done by the super user.
 *
 * This function also deletes all reference table placements belong to the given node
 * from pg_dist_placement, but it does not drop actual placement at the node. In the case
 * of re-activating the node, master_add_node first drops and re-creates the reference
 * tables.
 */
Datum
master_disable_node(PG_FUNCTION_ARGS)
{
	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeName = text_to_cstring(nodeNameText);
	WorkerNode *workerNode = ModifiableWorkerNode(nodeName, nodePort);
	bool isActive = false;
	bool onlyConsiderActivePlacements = false;

	if (WorkerNodeIsPrimary(workerNode))
	{
		/*
		 * Delete reference table placements so they are not taken into account
		 * for the check if there are placements after this
		 */
		DeleteAllReferenceTablePlacementsFromNodeGroup(workerNode->groupId);

		if (NodeGroupHasShardPlacements(workerNode->groupId,
										onlyConsiderActivePlacements))
		{
			ereport(NOTICE, (errmsg(
								 "Node %s:%d has active shard placements. Some queries "
								 "may fail after this operation. Use "
								 "SELECT master_activate_node('%s', %d) to activate this "
								 "node back.",
								 workerNode->workerName, nodePort, workerNode->workerName,
								 nodePort)));
		}
	}

	SetNodeState(nodeName, nodePort, isActive);

	PG_RETURN_VOID();
}


/*
 * master_set_node_property sets a property of the node
 */
Datum
master_set_node_property(PG_FUNCTION_ARGS)
{
	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	text *propertyText = PG_GETARG_TEXT_P(2);
	bool value = PG_GETARG_BOOL(3);

	WorkerNode *workerNode = ModifiableWorkerNode(text_to_cstring(nodeNameText),
												  nodePort);

	if (strcmp(text_to_cstring(propertyText), "shouldhaveshards") == 0)
	{
		SetShouldHaveShards(workerNode, value);
	}
	else
	{
		ereport(ERROR, (errmsg(
							"only the 'shouldhaveshards' property can be set using this function"
							)));
	}


	PG_RETURN_VOID();
}


/*
 * SetUpDistributedTableDependencies sets up up the following on a node if it's
 * a primary node that currently stores data:
 * - All dependencies (e.g., types, schemas)
 * - Reference tables, because they are needed to handle queries efficiently.
 * - Distributed functions
 */
static void
SetUpDistributedTableDependencies(WorkerNode *newWorkerNode)
{
	if (WorkerNodeIsPrimary(newWorkerNode))
	{
		EnsureNoModificationsHaveBeenDone();
		ReplicateAllDependenciesToNode(newWorkerNode->workerName,
									   newWorkerNode->workerPort);
		ReplicateAllReferenceTablesToNode(newWorkerNode->workerName,
										  newWorkerNode->workerPort);

		/*
		 * Let the maintanince deamon do the hard work of syncing the metadata.
		 * We prefer this because otherwise node activation might fail within
		 * transaction blocks.
		 */
		if (ClusterHasDistributedFunctionWithDistArgument())
		{
			MarkNodeHasMetadata(newWorkerNode->workerName, newWorkerNode->workerPort,
								true);
			TriggerMetadataSync(MyDatabaseId);
		}
	}
}


/*
 * ModifiableWorkerNode gets the requested WorkerNode and also gets locks
 * required for modifying it. This fails if the node does not exist.
 */
static WorkerNode *
ModifiableWorkerNode(const char *nodeName, int32 nodePort)
{
	WorkerNode *workerNode = NULL;

	CheckCitusVersion(ERROR);

	EnsureCoordinator();

	/* take an exclusive lock on pg_dist_node to serialize pg_dist_node changes */
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errmsg("node at \"%s:%u\" does not exist", nodeName, nodePort)));
	}

	return workerNode;
}


/*
 * master_activate_node UDF activates the given node. It sets the node's isactive
 * value to active and replicates all reference tables to that node.
 */
Datum
master_activate_node(PG_FUNCTION_ARGS)
{
	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	WorkerNode *workerNode = ModifiableWorkerNode(text_to_cstring(nodeNameText),
												  nodePort);
	ActivateNode(workerNode->workerName, workerNode->workerPort);

	PG_RETURN_INT32(workerNode->nodeId);
}


/*
 * GroupForNode returns the group which a given node belongs to.
 *
 * It only works if the requested node is a part of CurrentCluster.
 */
uint32
GroupForNode(char *nodeName, int nodePort)
{
	WorkerNode *workerNode = FindWorkerNode(nodeName, nodePort);

	if (workerNode == NULL)
	{
		ereport(ERROR, (errmsg("node at \"%s:%u\" does not exist", nodeName, nodePort)));
	}

	return workerNode->groupId;
}


/*
 * WorkerNodeIsPrimary returns whether the argument represents a primary node.
 */
bool
WorkerNodeIsPrimary(WorkerNode *worker)
{
	Oid primaryRole = PrimaryNodeRoleId();

	/* if nodeRole does not yet exist, all nodes are primary nodes */
	if (primaryRole == InvalidOid)
	{
		return true;
	}

	return worker->nodeRole == primaryRole;
}


/*
 * WorkerNodeIsSecondary returns whether the argument represents a secondary node.
 */
bool
WorkerNodeIsSecondary(WorkerNode *worker)
{
	Oid secondaryRole = SecondaryNodeRoleId();

	/* if nodeRole does not yet exist, all nodes are primary nodes */
	if (secondaryRole == InvalidOid)
	{
		return false;
	}

	return worker->nodeRole == secondaryRole;
}


/*
 * WorkerNodeIsPrimaryShouldHaveShardsNode returns whether the argument represents a
 * primary node that is a eligible for new data.
 */
bool
WorkerNodeIsPrimaryShouldHaveShardsNode(WorkerNode *worker)
{
	if (!WorkerNodeIsPrimary(worker))
	{
		return false;
	}

	return worker->shouldHaveShards;
}


/*
 * WorkerNodeIsReadable returns whether we're allowed to send SELECT queries to this
 * node.
 */
bool
WorkerNodeIsReadable(WorkerNode *workerNode)
{
	if (ReadFromSecondaries == USE_SECONDARY_NODES_NEVER &&
		WorkerNodeIsPrimary(workerNode))
	{
		return true;
	}

	if (ReadFromSecondaries == USE_SECONDARY_NODES_ALWAYS &&
		WorkerNodeIsSecondary(workerNode))
	{
		return true;
	}

	return false;
}


/*
 * PrimaryNodeForGroup returns the (unique) primary in the specified group.
 *
 * If there are any nodes in the requested group and groupContainsNodes is not NULL
 * it will set the bool groupContainsNodes references to true.
 */
WorkerNode *
PrimaryNodeForGroup(int32 groupId, bool *groupContainsNodes)
{
	WorkerNode *workerNode = NULL;
	HASH_SEQ_STATUS status;
	HTAB *workerNodeHash = GetWorkerNodeHash();

	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		int32 workerNodeGroupId = workerNode->groupId;
		if (workerNodeGroupId != groupId)
		{
			continue;
		}

		if (groupContainsNodes != NULL)
		{
			*groupContainsNodes = true;
		}

		if (WorkerNodeIsPrimary(workerNode))
		{
			hash_seq_term(&status);
			return workerNode;
		}
	}

	return NULL;
}


/*
 * ActivateNode activates the node with nodeName and nodePort. Currently, activation
 * includes only replicating the reference tables and setting isactive column of the
 * given node.
 */
static int
ActivateNode(char *nodeName, int nodePort)
{
	WorkerNode *newWorkerNode = NULL;
	bool isActive = true;

	/* take an exclusive lock on pg_dist_node to serialize pg_dist_node changes */
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	newWorkerNode = SetNodeState(nodeName, nodePort, isActive);

	SetUpDistributedTableDependencies(newWorkerNode);
	return newWorkerNode->nodeId;
}


/*
 * master_update_node moves the requested node to a different nodename and nodeport. It
 * locks to ensure no queries are running concurrently; and is intended for customers who
 * are running their own failover solution.
 */
Datum
master_update_node(PG_FUNCTION_ARGS)
{
	int32 nodeId = PG_GETARG_INT32(0);

	text *newNodeName = PG_GETARG_TEXT_P(1);
	int32 newNodePort = PG_GETARG_INT32(2);

	/*
	 * force is used when an update needs to happen regardless of conflicting locks. This
	 * feature is important to force the update during a failover due to failure, eg. by
	 * a high-availability system such as pg_auto_failover. The strategy is to start a
	 * background worker that actively cancels backends holding conflicting locks with
	 * this backend.
	 *
	 * Defaults to false
	 */
	bool force = PG_GETARG_BOOL(3);
	int32 lock_cooldown = PG_GETARG_INT32(4);

	char *newNodeNameString = text_to_cstring(newNodeName);
	WorkerNode *workerNode = NULL;
	WorkerNode *workerNodeWithSameAddress = NULL;
	List *placementList = NIL;
	BackgroundWorkerHandle *handle = NULL;

	CheckCitusVersion(ERROR);

	workerNodeWithSameAddress = FindWorkerNodeAnyCluster(newNodeNameString, newNodePort);
	if (workerNodeWithSameAddress != NULL)
	{
		/* a node with the given hostname and port already exists in the metadata */

		if (workerNodeWithSameAddress->nodeId == nodeId)
		{
			/* it's the node itself, meaning this is a noop update */
			PG_RETURN_VOID();
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("there is already another node with the specified "
								   "hostname and port")));
		}
	}

	workerNode = LookupNodeByNodeId(nodeId);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
						errmsg("node %u not found", nodeId)));
	}

	/*
	 * If the node is a primary node we block reads and writes.
	 *
	 * This lock has two purposes:
	 *
	 * - Ensure buggy code in Citus doesn't cause failures when the
	 *   nodename/nodeport of a node changes mid-query
	 *
	 * - Provide fencing during failover, after this function returns all
	 *   connections will use the new node location.
	 *
	 * Drawback:
	 *
	 * - This function blocks until all previous queries have finished. This
	 *   means that long-running queries will prevent failover.
	 *
	 *   In case of node failure said long-running queries will fail in the end
	 *   anyway as they will be unable to commit successfully on the failed
	 *   machine. To cause quick failure of these queries use force => true
	 *   during the invocation of master_update_node to terminate conflicting
	 *   backends proactively.
	 *
	 * It might be worth blocking reads to a secondary for the same reasons,
	 * though we currently only query secondaries on follower clusters
	 * where these locks will have no effect.
	 */
	if (WorkerNodeIsPrimary(workerNode))
	{
		/*
		 * before acquiring the locks check if we want a background worker to help us to
		 * aggressively obtain the locks.
		 */
		if (force)
		{
			handle = StartLockAcquireHelperBackgroundWorker(MyProcPid, lock_cooldown);
		}

		placementList = AllShardPlacementsOnNodeGroup(workerNode->groupId);
		LockShardsInPlacementListMetadata(placementList, AccessExclusiveLock);
	}

	UpdateNodeLocation(nodeId, newNodeNameString, newNodePort);

	strlcpy(workerNode->workerName, newNodeNameString, WORKER_LENGTH);
	workerNode->workerPort = newNodePort;

	/*
	 * Propagate the updated pg_dist_node entry to all metadata workers.
	 * citus-ha uses master_update_node() in a prepared transaction, and
	 * we don't support coordinated prepared transactions, so we cannot
	 * propagate the changes to the worker nodes here. Instead we mark
	 * all metadata nodes as not-synced and ask maintenanced to do the
	 * propagation.
	 *
	 * It is possible that maintenance daemon does the first resync too
	 * early, but that's fine, since this will start a retry loop with
	 * 5 second intervals until sync is complete.
	 */
	if (UnsetMetadataSyncedForAll())
	{
		TriggerMetadataSync(MyDatabaseId);
	}

	if (handle != NULL)
	{
		/*
		 * this will be called on memory context cleanup as well, if the worker has been
		 * terminated already this will be a noop
		 */
		TerminateBackgroundWorker(handle);
	}

	PG_RETURN_VOID();
}


static void
UpdateNodeLocation(int32 nodeId, char *newNodeName, int32 newNodePort)
{
	const bool indexOK = true;

	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	ScanKeyData scanKey[1];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_node];
	bool isnull[Natts_pg_dist_node];
	bool replace[Natts_pg_dist_node];

	pgDistNode = heap_open(DistNodeRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistNode);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodeid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodeId));

	scanDescriptor = systable_beginscan(pgDistNode, DistNodeNodeIdIndexId(), indexOK,
										NULL, 1, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
							   newNodeName, newNodePort)));
	}

	memset(replace, 0, sizeof(replace));

	values[Anum_pg_dist_node_nodeport - 1] = Int32GetDatum(newNodePort);
	isnull[Anum_pg_dist_node_nodeport - 1] = false;
	replace[Anum_pg_dist_node_nodeport - 1] = true;

	values[Anum_pg_dist_node_nodename - 1] = CStringGetTextDatum(newNodeName);
	isnull[Anum_pg_dist_node_nodename - 1] = false;
	replace[Anum_pg_dist_node_nodename - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistNode, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, NoLock);
}


/*
 * get_shard_id_for_distribution_column function takes a distributed table name and a
 * distribution value then returns shard id of the shard which belongs to given table and
 * contains given value. This function only works for hash distributed tables.
 */
Datum
get_shard_id_for_distribution_column(PG_FUNCTION_ARGS)
{
	ShardInterval *shardInterval = NULL;
	char distributionMethod = 0;
	Oid relationId = InvalidOid;

	CheckCitusVersion(ERROR);

	/*
	 * To have optional parameter as NULL, we defined this UDF as not strict, therefore
	 * we need to check all parameters for NULL values.
	 */
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("relation cannot be NULL")));
	}

	relationId = PG_GETARG_OID(0);
	EnsureTablePermissions(relationId, ACL_SELECT);

	if (!IsDistributedTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("relation is not distributed")));
	}

	distributionMethod = PartitionMethod(relationId);
	if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		List *shardIntervalList = LoadShardIntervalList(relationId);
		if (shardIntervalList == NIL)
		{
			PG_RETURN_INT64(0);
		}

		shardInterval = (ShardInterval *) linitial(shardIntervalList);
	}
	else if (distributionMethod == DISTRIBUTE_BY_HASH ||
			 distributionMethod == DISTRIBUTE_BY_RANGE)
	{
		Var *distributionColumn = NULL;
		Oid distributionDataType = InvalidOid;
		Oid inputDataType = InvalidOid;
		char *distributionValueString = NULL;
		Datum inputDatum = 0;
		Datum distributionValueDatum = 0;
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);

		/* if given table is not reference table, distributionValue cannot be NULL */
		if (PG_ARGISNULL(1))
		{
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("distribution value cannot be NULL for tables other "
								   "than reference tables.")));
		}

		inputDatum = PG_GETARG_DATUM(1);
		inputDataType = get_fn_expr_argtype(fcinfo->flinfo, 1);
		distributionValueString = DatumToString(inputDatum, inputDataType);

		distributionColumn = DistPartitionKey(relationId);
		distributionDataType = distributionColumn->vartype;

		distributionValueDatum = StringToDatum(distributionValueString,
											   distributionDataType);

		shardInterval = FindShardInterval(distributionValueDatum, cacheEntry);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("finding shard id of given distribution value is only "
							   "supported for hash partitioned tables, range partitioned "
							   "tables and reference tables.")));
	}

	if (shardInterval != NULL)
	{
		PG_RETURN_INT64(shardInterval->shardId);
	}

	PG_RETURN_INT64(0);
}


/*
 * FindWorkerNode searches over the worker nodes and returns the workerNode
 * if it already exists. Else, the function returns NULL.
 */
WorkerNode *
FindWorkerNode(char *nodeName, int32 nodePort)
{
	WorkerNode *cachedWorkerNode = NULL;
	HTAB *workerNodeHash = GetWorkerNodeHash();
	bool handleFound = false;
	void *hashKey = NULL;

	WorkerNode *searchedNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	strlcpy(searchedNode->workerName, nodeName, WORKER_LENGTH);
	searchedNode->workerPort = nodePort;

	hashKey = (void *) searchedNode;
	cachedWorkerNode = (WorkerNode *) hash_search(workerNodeHash, hashKey, HASH_FIND,
												  &handleFound);
	if (handleFound)
	{
		WorkerNode *workerNode = (WorkerNode *) palloc(sizeof(WorkerNode));
		memcpy(workerNode, cachedWorkerNode, sizeof(WorkerNode));
		return workerNode;
	}

	return NULL;
}


/*
 * FindWorkerNodeAnyCluster returns the workerNode no matter which cluster it is a part
 * of. FindWorkerNodes, like almost every other function, acts as if nodes in other
 * clusters do not exist.
 */
WorkerNode *
FindWorkerNodeAnyCluster(const char *nodeName, int32 nodePort)
{
	WorkerNode *workerNode = NULL;

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);

	HeapTuple heapTuple = GetNodeTuple(nodeName, nodePort);
	if (heapTuple != NULL)
	{
		workerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);
	}

	heap_close(pgDistNode, NoLock);
	return workerNode;
}


/*
 * ReadWorkerNodes iterates over pg_dist_node table, converts each row
 * into it's memory representation (i.e., WorkerNode) and adds them into
 * a list. Lastly, the list is returned to the caller.
 *
 * It skips nodes which are not in the current clusters unless requested to do otherwise
 * by includeNodesFromOtherClusters.
 */
List *
ReadWorkerNodes(bool includeNodesFromOtherClusters)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	List *workerNodeList = NIL;
	TupleDesc tupleDescriptor = NULL;

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);

	scanDescriptor = systable_beginscan(pgDistNode,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(pgDistNode);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		WorkerNode *workerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);

		if (includeNodesFromOtherClusters ||
			strncmp(workerNode->nodeCluster, CurrentCluster, WORKER_LENGTH) == 0)
		{
			/* the coordinator acts as if it never sees nodes not in it's cluster */
			workerNodeList = lappend(workerNodeList, workerNode);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, NoLock);

	return workerNodeList;
}


/*
 * RemoveNodeFromCluster removes the provided node from the pg_dist_node table of
 * the master node and all nodes with metadata.
 * The call to the master_remove_node should be done by the super user. If there are
 * active shard placements on the node; the function errors out.
 * This function also deletes all reference table placements belong to the given node from
 * pg_dist_placement, but it does not drop actual placement at the node. It also
 * modifies replication factor of the colocation group of reference tables, so that
 * replication factor will be equal to worker count.
 */
static void
RemoveNodeFromCluster(char *nodeName, int32 nodePort)
{
	char *nodeDeleteCommand = NULL;
	WorkerNode *workerNode = ModifiableWorkerNode(nodeName, nodePort);

	if (WorkerNodeIsPrimary(workerNode))
	{
		bool onlyConsiderActivePlacements = false;

		/*
		 * Delete reference table placements so they are not taken into account
		 * for the check if there are placements after this
		 */
		DeleteAllReferenceTablePlacementsFromNodeGroup(workerNode->groupId);

		if (NodeGroupHasShardPlacements(workerNode->groupId,
										onlyConsiderActivePlacements))
		{
			ereport(ERROR, (errmsg("you cannot remove the primary node of a node group "
								   "which has shard placements")));
		}
	}

	DeleteNodeRow(workerNode->workerName, nodePort);

	nodeDeleteCommand = NodeDeleteCommand(workerNode->nodeId);

	/* make sure we don't have any lingering session lifespan connections */
	CloseNodeConnectionsAfterTransaction(workerNode->workerName, nodePort);

	SendCommandToWorkers(WORKERS_WITH_METADATA, nodeDeleteCommand);
}


/* CountPrimariesWithMetadata returns the number of primary nodes which have metadata. */
uint32
CountPrimariesWithMetadata(void)
{
	uint32 primariesWithMetadata = 0;
	WorkerNode *workerNode = NULL;

	HASH_SEQ_STATUS status;
	HTAB *workerNodeHash = GetWorkerNodeHash();

	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		if (workerNode->hasMetadata && WorkerNodeIsPrimary(workerNode))
		{
			primariesWithMetadata++;
		}
	}

	return primariesWithMetadata;
}


/*
 * AddNodeMetadata checks the given node information and adds the specified node to the
 * pg_dist_node table of the master and workers with metadata.
 * If the node already exists, the function returns the id of the node.
 * If not, the following procedure is followed while adding a node: If the groupId is not
 * explicitly given by the user, the function picks the group that the new node should
 * be in with respect to GroupSize. Then, the new node is inserted into the local
 * pg_dist_node as well as the nodes with hasmetadata=true.
 */
static int
AddNodeMetadata(char *nodeName, int32 nodePort,
				NodeMetadata *nodeMetadata,
				bool *nodeAlreadyExists)
{
	int nextNodeIdInt = 0;
	WorkerNode *workerNode = NULL;
	char *nodeDeleteCommand = NULL;
	uint32 primariesWithMetadata = 0;

	EnsureCoordinator();

	*nodeAlreadyExists = false;

	/*
	 * Take an exclusive lock on pg_dist_node to serialize node changes.
	 * We may want to relax or have more fine-grained locking in the future
	 * to allow users to add multiple nodes concurrently.
	 */
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);
	if (workerNode != NULL)
	{
		/* fill return data and return */
		*nodeAlreadyExists = true;

		return workerNode->nodeId;
	}

	/* user lets Citus to decide on the group that the newly added node should be in */
	if (nodeMetadata->groupId == 0)
	{
		nodeMetadata->groupId = GetNextGroupId();
	}

	/* if nodeRole hasn't been added yet there's a constraint for one-node-per-group */
	if (nodeMetadata->nodeRole != InvalidOid && nodeMetadata->nodeRole ==
		PrimaryNodeRoleId())
	{
		WorkerNode *existingPrimaryNode = PrimaryNodeForGroup(nodeMetadata->groupId,
															  NULL);

		if (existingPrimaryNode != NULL)
		{
			ereport(ERROR, (errmsg("group %d already has a primary node",
								   nodeMetadata->groupId)));
		}
	}

	if (nodeMetadata->nodeRole == PrimaryNodeRoleId())
	{
		if (strncmp(nodeMetadata->nodeCluster,
					WORKER_DEFAULT_CLUSTER,
					WORKER_LENGTH) != 0)
		{
			ereport(ERROR, (errmsg("primaries must be added to the default cluster")));
		}
	}

	/* generate the new node id from the sequence */
	nextNodeIdInt = GetNextNodeId();

	InsertNodeRow(nextNodeIdInt, nodeName, nodePort, nodeMetadata);

	workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);

	/* send the delete command to all primary nodes with metadata */
	nodeDeleteCommand = NodeDeleteCommand(workerNode->nodeId);
	SendCommandToWorkers(WORKERS_WITH_METADATA, nodeDeleteCommand);

	/* finally prepare the insert command and send it to all primary nodes */
	primariesWithMetadata = CountPrimariesWithMetadata();
	if (primariesWithMetadata != 0)
	{
		List *workerNodeList = list_make1(workerNode);
		char *nodeInsertCommand = NodeListInsertCommand(workerNodeList);

		SendCommandToWorkers(WORKERS_WITH_METADATA, nodeInsertCommand);
	}

	return workerNode->nodeId;
}


/*
 * SetWorkerColumn function sets the column with the specified index
 * (see pg_dist_node.h) on the worker in pg_dist_node.
 * It returns the new worker node after the modification.
 */
static WorkerNode *
SetWorkerColumn(WorkerNode *workerNode, int columnIndex, Datum value)
{
	Relation pgDistNode = heap_open(DistNodeRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);
	HeapTuple heapTuple = GetNodeTuple(workerNode->workerName, workerNode->workerPort);
	WorkerNode *newWorkerNode = NULL;

	Datum values[Natts_pg_dist_node];
	bool isnull[Natts_pg_dist_node];
	bool replace[Natts_pg_dist_node];
	char *metadataSyncCommand = NULL;


	switch (columnIndex)
	{
		case Anum_pg_dist_node_isactive:
		{
			metadataSyncCommand = ShouldHaveShardsUpdateCommand(workerNode->nodeId,
																DatumGetBool(value));
			break;
		}

		case Anum_pg_dist_node_shouldhaveshards:
		{
			metadataSyncCommand = ShouldHaveShardsUpdateCommand(workerNode->nodeId,
																DatumGetBool(value));
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
								   workerNode->workerName, workerNode->workerPort)));
		}
	}

	if (heapTuple == NULL)
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
							   workerNode->workerName, workerNode->workerPort)));
	}

	memset(replace, 0, sizeof(replace));
	values[columnIndex - 1] = value;
	isnull[columnIndex - 1] = false;
	replace[columnIndex - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistNode, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());
	CommandCounterIncrement();

	newWorkerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);

	heap_close(pgDistNode, NoLock);

	/* we also update the column at worker nodes */
	SendCommandToWorkers(WORKERS_WITH_METADATA, metadataSyncCommand);
	return newWorkerNode;
}


/*
 * SetShouldHaveShards function sets the shouldhaveshards column of the
 * specified worker in pg_dist_node.
 * It returns the new worker node after the modification.
 */
static WorkerNode *
SetShouldHaveShards(WorkerNode *workerNode, bool shouldHaveShards)
{
	return SetWorkerColumn(workerNode, Anum_pg_dist_node_shouldhaveshards,
						   BoolGetDatum(shouldHaveShards));
}


/*
 * SetNodeState function sets the isactive column of the specified worker in
 * pg_dist_node to isActive.
 * It returns the new worker node after the modification.
 */
static WorkerNode *
SetNodeState(char *nodeName, int nodePort, bool isActive)
{
	WorkerNode *workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);
	return SetWorkerColumn(workerNode, Anum_pg_dist_node_isactive,
						   BoolGetDatum(isActive));
}


/*
 * GetNodeTuple function returns the heap tuple of given nodeName and nodePort. If the
 * node is not found this function returns NULL.
 *
 * This function may return worker nodes from other clusters.
 */
static HeapTuple
GetNodeTuple(const char *nodeName, int32 nodePort)
{
	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);
	const int scanKeyCount = 2;
	const bool indexOK = false;

	ScanKeyData scanKey[2];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	HeapTuple nodeTuple = NULL;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodePort));
	scanDescriptor = systable_beginscan(pgDistNode, InvalidOid, indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		nodeTuple = heap_copytuple(heapTuple);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, NoLock);

	return nodeTuple;
}


/*
 * GetNextGroupId allocates and returns a unique groupId for the group
 * to be created. This allocation occurs both in shared memory and in write
 * ahead logs; writing to logs avoids the risk of having groupId collisions.
 *
 * Please note that the caller is still responsible for finalizing node data
 * and the groupId with the master node. Further note that this function relies
 * on an internal sequence created in initdb to generate unique identifiers.
 */
int32
GetNextGroupId()
{
	text *sequenceName = cstring_to_text(GROUPID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum groupIdDatum = 0;
	int32 groupId = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	groupIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	groupId = DatumGetInt32(groupIdDatum);

	return groupId;
}


/*
 * GetNextNodeId allocates and returns a unique nodeId for the node
 * to be added. This allocation occurs both in shared memory and in write
 * ahead logs; writing to logs avoids the risk of having nodeId collisions.
 *
 * Please note that the caller is still responsible for finalizing node data
 * and the nodeId with the master node. Further note that this function relies
 * on an internal sequence created in initdb to generate unique identifiers.
 */
int
GetNextNodeId()
{
	text *sequenceName = cstring_to_text(NODEID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum nextNodeIdDatum;
	int nextNodeId = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	nextNodeIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	nextNodeId = DatumGetUInt32(nextNodeIdDatum);

	return nextNodeId;
}


/*
 * EnsureCoordinator checks if the current node is the coordinator. If it does not,
 * the function errors out.
 */
void
EnsureCoordinator(void)
{
	int localGroupId = GetLocalGroupId();

	if (localGroupId != 0)
	{
		ereport(ERROR, (errmsg("operation is not allowed on this node"),
						errhint("Connect to the coordinator and run it again.")));
	}
}


/*
 * InsertNodeRow opens the node system catalog, and inserts a new row with the
 * given values into that system catalog.
 *
 * NOTE: If you call this function you probably need to have taken a
 * ShareRowExclusiveLock then checked that you're not adding a second primary to
 * an existing group. If you don't it's possible for the metadata to become inconsistent.
 */
static void
InsertNodeRow(int nodeid, char *nodeName, int32 nodePort, NodeMetadata *nodeMetadata)
{
	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_node];
	bool isNulls[Natts_pg_dist_node];

	Datum nodeClusterStringDatum = CStringGetDatum(nodeMetadata->nodeCluster);
	Datum nodeClusterNameDatum = DirectFunctionCall1(namein, nodeClusterStringDatum);

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_node_nodeid - 1] = UInt32GetDatum(nodeid);
	values[Anum_pg_dist_node_groupid - 1] = Int32GetDatum(nodeMetadata->groupId);
	values[Anum_pg_dist_node_nodename - 1] = CStringGetTextDatum(nodeName);
	values[Anum_pg_dist_node_nodeport - 1] = UInt32GetDatum(nodePort);
	values[Anum_pg_dist_node_noderack - 1] = CStringGetTextDatum(nodeMetadata->nodeRack);
	values[Anum_pg_dist_node_hasmetadata - 1] = BoolGetDatum(nodeMetadata->hasMetadata);
	values[Anum_pg_dist_node_metadatasynced - 1] = BoolGetDatum(
		nodeMetadata->metadataSynced);
	values[Anum_pg_dist_node_isactive - 1] = BoolGetDatum(nodeMetadata->isActive);
	values[Anum_pg_dist_node_noderole - 1] = ObjectIdGetDatum(nodeMetadata->nodeRole);
	values[Anum_pg_dist_node_nodecluster - 1] = nodeClusterNameDatum;
	values[Anum_pg_dist_node_shouldhaveshards - 1] = BoolGetDatum(
		nodeMetadata->shouldHaveShards);

	pgDistNode = heap_open(DistNodeRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistNode);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistNode, heapTuple);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();

	/* close relation */
	heap_close(pgDistNode, NoLock);
}


/*
 * DeleteNodeRow removes the requested row from pg_dist_node table if it exists.
 */
static void
DeleteNodeRow(char *nodeName, int32 nodePort)
{
	const int scanKeyCount = 2;
	bool indexOK = false;

	HeapTuple heapTuple = NULL;
	SysScanDesc heapScan = NULL;
	ScanKeyData scanKey[2];
	Relation pgDistNode = heap_open(DistNodeRelationId(), RowExclusiveLock);

	/*
	 * simple_heap_delete() expects that the caller has at least an
	 * AccessShareLock on replica identity index.
	 */
	Relation replicaIndex = index_open(RelationGetReplicaIndex(pgDistNode),
									   AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodePort));

	heapScan = systable_beginscan(pgDistNode, InvalidOid, indexOK,
								  NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(heapScan);

	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
							   nodeName, nodePort)));
	}

	simple_heap_delete(pgDistNode, &(heapTuple->t_self));

	systable_endscan(heapScan);

	/* ensure future commands don't use the node we just removed */
	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command won't see the row */
	CommandCounterIncrement();

	heap_close(replicaIndex, AccessShareLock);
	heap_close(pgDistNode, NoLock);
}


/*
 * TupleToWorkerNode takes in a heap tuple from pg_dist_node, and
 * converts this tuple to an equivalent struct in memory. The function assumes
 * the caller already has locks on the tuple, and doesn't perform any locking.
 */
static WorkerNode *
TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	WorkerNode *workerNode = NULL;
	Datum datumArray[Natts_pg_dist_node];
	bool isNullArray[Natts_pg_dist_node];
	char *nodeName = NULL;
	char *nodeRack = NULL;

	Assert(!HeapTupleHasNulls(heapTuple));

	/*
	 * This function can be called before "ALTER TABLE ... ADD COLUMN nodecluster ...",
	 * therefore heap_deform_tuple() won't set the isNullArray for this column. We
	 * initialize it true to be safe in that case.
	 */
	memset(isNullArray, true, sizeof(isNullArray));

	/*
	 * We use heap_deform_tuple() instead of heap_getattr() to expand tuple
	 * to contain missing values when ALTER TABLE ADD COLUMN happens.
	 */
	heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

	nodeName = DatumGetCString(datumArray[Anum_pg_dist_node_nodename - 1]);
	nodeRack = DatumGetCString(datumArray[Anum_pg_dist_node_noderack - 1]);

	workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	workerNode->nodeId = DatumGetUInt32(datumArray[Anum_pg_dist_node_nodeid - 1]);
	workerNode->workerPort = DatumGetUInt32(datumArray[Anum_pg_dist_node_nodeport - 1]);
	workerNode->groupId = DatumGetInt32(datumArray[Anum_pg_dist_node_groupid - 1]);
	strlcpy(workerNode->workerName, TextDatumGetCString(nodeName), WORKER_LENGTH);
	strlcpy(workerNode->workerRack, TextDatumGetCString(nodeRack), WORKER_LENGTH);
	workerNode->hasMetadata = DatumGetBool(datumArray[Anum_pg_dist_node_hasmetadata - 1]);
	workerNode->metadataSynced =
		DatumGetBool(datumArray[Anum_pg_dist_node_metadatasynced - 1]);
	workerNode->isActive = DatumGetBool(datumArray[Anum_pg_dist_node_isactive - 1]);
	workerNode->nodeRole = DatumGetObjectId(datumArray[Anum_pg_dist_node_noderole - 1]);
	workerNode->shouldHaveShards = DatumGetBool(
		datumArray[Anum_pg_dist_node_shouldhaveshards -
				   1]);

	/*
	 * nodecluster column can be missing. In the case of extension creation/upgrade,
	 * master_initialize_node_metadata function is called before the nodecluster
	 * column is added to pg_dist_node table.
	 */
	if (!isNullArray[Anum_pg_dist_node_nodecluster - 1])
	{
		Name nodeClusterName =
			DatumGetName(datumArray[Anum_pg_dist_node_nodecluster - 1]);
		char *nodeClusterString = NameStr(*nodeClusterName);
		strlcpy(workerNode->nodeCluster, nodeClusterString, NAMEDATALEN);
	}

	return workerNode;
}


/*
 * StringToDatum transforms a string representation into a Datum.
 */
Datum
StringToDatum(char *inputString, Oid dataType)
{
	Oid typIoFunc = InvalidOid;
	Oid typIoParam = InvalidOid;
	int32 typeModifier = -1;
	Datum datum = 0;

	getTypeInputInfo(dataType, &typIoFunc, &typIoParam);
	getBaseTypeAndTypmod(dataType, &typeModifier);

	datum = OidInputFunctionCall(typIoFunc, inputString, typIoParam, typeModifier);

	return datum;
}


/*
 * DatumToString returns the string representation of the given datum.
 */
char *
DatumToString(Datum datum, Oid dataType)
{
	char *outputString = NULL;
	Oid typIoFunc = InvalidOid;
	bool typIsVarlena = false;

	getTypeOutputInfo(dataType, &typIoFunc, &typIsVarlena);
	outputString = OidOutputFunctionCall(typIoFunc, datum);

	return outputString;
}


/*
 * UnsetMetadataSyncedForAll sets the metadatasynced column of all metadata
 * nodes to false. It returns true if it updated at least a node.
 */
static bool
UnsetMetadataSyncedForAll(void)
{
	bool updatedAtLeastOne = false;
	Relation relation = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[2];
	int scanKeyCount = 2;
	bool indexOK = false;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	CatalogIndexState indstate;

	/*
	 * Concurrent master_update_node() calls might iterate and try to update
	 * pg_dist_node in different orders. To protect against deadlock, we
	 * get an exclusive lock here.
	 */
	relation = heap_open(DistNodeRelationId(), ExclusiveLock);
	tupleDescriptor = RelationGetDescr(relation);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_hasmetadata,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_metadatasynced,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));

	indstate = CatalogOpenIndexes(relation);

	scanDescriptor = systable_beginscan(relation,
										InvalidOid, indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		updatedAtLeastOne = true;
	}

	while (HeapTupleIsValid(heapTuple))
	{
		HeapTuple newHeapTuple = NULL;
		Datum values[Natts_pg_dist_node];
		bool isnull[Natts_pg_dist_node];
		bool replace[Natts_pg_dist_node];

		memset(replace, false, sizeof(replace));
		memset(isnull, false, sizeof(isnull));
		memset(values, 0, sizeof(values));

		values[Anum_pg_dist_node_metadatasynced - 1] = BoolGetDatum(false);
		replace[Anum_pg_dist_node_metadatasynced - 1] = true;

		newHeapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
										 replace);

		CatalogTupleUpdateWithInfo(relation, &newHeapTuple->t_self, newHeapTuple,
								   indstate);

		CommandCounterIncrement();

		heap_freetuple(newHeapTuple);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	CatalogCloseIndexes(indstate);
	heap_close(relation, NoLock);

	return updatedAtLeastOne;
}
