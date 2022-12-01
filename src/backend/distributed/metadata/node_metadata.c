/*
 * node_metadata.c
 *	  Functions that operate on pg_dist_node
 *
 * Copyright (c) Citus Data, Inc.
 */
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "utils/plancache.h"


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
#include "distributed/citus_safe_lib.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/maintenanced.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_utility.h"
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
#include "distributed/multi_partitioning_utils.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/string_utils.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/transaction_recovery.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "postmaster/postmaster.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#define INVALID_GROUP_ID -1

/* default group size */
int GroupSize = 1;

/* config variable managed via guc.c */
char *CurrentCluster = "default";

/* did current transaction modify pg_dist_node? */
bool TransactionModifiedNodeMetadata = false;

bool EnableMetadataSync = true;

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
static void RemoveNodeFromCluster(char *nodeName, int32 nodePort);
static void ErrorIfNodeContainsNonRemovablePlacements(WorkerNode *workerNode);
static bool PlacementHasActivePlacementOnAnotherGroup(GroupShardPlacement
													  *sourcePlacement);
static int AddNodeMetadata(char *nodeName, int32 nodePort, NodeMetadata
						   *nodeMetadata, bool *nodeAlreadyExists);
static WorkerNode * SetNodeState(char *nodeName, int32 nodePort, bool isActive);
static HeapTuple GetNodeTuple(const char *nodeName, int32 nodePort);
static int32 GetNextGroupId(void);
static int GetNextNodeId(void);
static void InsertPlaceholderCoordinatorRecord(void);
static void InsertNodeRow(int nodeid, char *nodename, int32 nodeport, NodeMetadata
						  *nodeMetadata);
static void DeleteNodeRow(char *nodename, int32 nodeport);
static void SyncDistributedObjectsToNodeList(List *workerNodeList);
static void UpdateLocalGroupIdOnNode(WorkerNode *workerNode);
static void SyncPgDistTableMetadataToNodeList(List *nodeList);
static List * InterTableRelationshipCommandList();
static void BlockDistributedQueriesOnMetadataNodes(void);
static WorkerNode * TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple);
static List * PropagateNodeWideObjectsCommandList();
static WorkerNode * ModifiableWorkerNode(const char *nodeName, int32 nodePort);
static bool NodeIsLocal(WorkerNode *worker);
static void DropExistingMetadataInOutsideTransaction(List *nodeList);
static void SetLockTimeoutLocally(int32 lock_cooldown);
static void UpdateNodeLocation(int32 nodeId, char *newNodeName, int32 newNodePort);
static bool UnsetMetadataSyncedForAllWorkers(void);
static char * GetMetadataSyncCommandToSetNodeColumn(WorkerNode *workerNode,
													int columnIndex,
													Datum value);
static char * NodeHasmetadataUpdateCommand(uint32 nodeId, bool hasMetadata);
static char * NodeMetadataSyncedUpdateCommand(uint32 nodeId, bool metadataSynced);
static void ErrorIfCoordinatorMetadataSetFalse(WorkerNode *workerNode, Datum value,
											   char *field);
static WorkerNode * SetShouldHaveShards(WorkerNode *workerNode, bool shouldHaveShards);
static void RemoveOldShardPlacementForNodeGroup(int groupId);
static int FindCoordinatorNodeId(void);
static WorkerNode * FindNodeAnyClusterByNodeId(uint32 nodeId);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_set_coordinator_host);
PG_FUNCTION_INFO_V1(citus_add_node);
PG_FUNCTION_INFO_V1(master_add_node);
PG_FUNCTION_INFO_V1(citus_add_inactive_node);
PG_FUNCTION_INFO_V1(master_add_inactive_node);
PG_FUNCTION_INFO_V1(citus_add_secondary_node);
PG_FUNCTION_INFO_V1(master_add_secondary_node);
PG_FUNCTION_INFO_V1(citus_set_node_property);
PG_FUNCTION_INFO_V1(master_set_node_property);
PG_FUNCTION_INFO_V1(citus_remove_node);
PG_FUNCTION_INFO_V1(master_remove_node);
PG_FUNCTION_INFO_V1(citus_disable_node);
PG_FUNCTION_INFO_V1(master_disable_node);
PG_FUNCTION_INFO_V1(citus_activate_node);
PG_FUNCTION_INFO_V1(master_activate_node);
PG_FUNCTION_INFO_V1(citus_update_node);
PG_FUNCTION_INFO_V1(master_update_node);
PG_FUNCTION_INFO_V1(get_shard_id_for_distribution_column);
PG_FUNCTION_INFO_V1(citus_nodename_for_nodeid);
PG_FUNCTION_INFO_V1(citus_nodeport_for_nodeid);
PG_FUNCTION_INFO_V1(citus_coordinator_nodeid);
PG_FUNCTION_INFO_V1(citus_is_coordinator);


/*
 * DefaultNodeMetadata creates a NodeMetadata struct with the fields set to
 * sane defaults, e.g. nodeRack = WORKER_DEFAULT_RACK.
 */
static NodeMetadata
DefaultNodeMetadata()
{
	NodeMetadata nodeMetadata;

	/* ensure uninitialized padding doesn't escape the function */
	memset_struct_0(nodeMetadata);
	nodeMetadata.nodeRack = WORKER_DEFAULT_RACK;
	nodeMetadata.shouldHaveShards = true;
	nodeMetadata.groupId = INVALID_GROUP_ID;

	return nodeMetadata;
}


/*
 * citus_set_coordinator_host configures the hostname and port through which worker
 * nodes can connect to the coordinator.
 */
Datum
citus_set_coordinator_host(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);

	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	nodeMetadata.groupId = 0;
	nodeMetadata.shouldHaveShards = false;
	nodeMetadata.nodeRole = PG_GETARG_OID(2);

	Name nodeClusterName = PG_GETARG_NAME(3);
	nodeMetadata.nodeCluster = NameStr(*nodeClusterName);

	bool isCoordinatorInMetadata = false;
	WorkerNode *coordinatorNode = PrimaryNodeForGroup(COORDINATOR_GROUP_ID,
													  &isCoordinatorInMetadata);
	if (!isCoordinatorInMetadata)
	{
		bool nodeAlreadyExists = false;

		/* add the coordinator to pg_dist_node if it was not already added */
		AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
						&nodeAlreadyExists);

		/* we just checked */
		Assert(!nodeAlreadyExists);
	}
	else
	{
		/*
		 * since AddNodeMetadata takes an exclusive lock on pg_dist_node, we
		 * do not need to worry about concurrent changes (e.g. deletion) and
		 * can proceed to update immediately.
		 */

		UpdateNodeLocation(coordinatorNode->nodeId, nodeNameString, nodePort);

		/* clear cached plans that have the old host/port */
		ResetPlanCache();
	}

	TransactionModifiedNodeMetadata = true;

	PG_RETURN_VOID();
}


/*
 * citus_add_node function adds a new node to the cluster and returns its id. It also
 * replicates all reference tables to the new node.
 */
Datum
citus_add_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);

	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	bool nodeAlreadyExists = false;
	nodeMetadata.groupId = PG_GETARG_INT32(2);

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

	if (nodeMetadata.groupId == COORDINATOR_GROUP_ID)
	{
		/* by default, we add the coordinator without shards */
		nodeMetadata.shouldHaveShards = false;
	}

	int nodeId = AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
								 &nodeAlreadyExists);
	TransactionModifiedNodeMetadata = true;

	/*
	 * After adding new node, if the node did not already exist, we will activate
	 * the node. This means we will replicate all reference tables to the new
	 * node.
	 */
	if (!nodeAlreadyExists)
	{
		WorkerNode *workerNode = FindWorkerNodeAnyCluster(nodeNameString, nodePort);

		/*
		 * If the worker is not marked as a coordinator, check that
		 * the node is not trying to add itself
		 */
		if (workerNode != NULL &&
			workerNode->groupId != COORDINATOR_GROUP_ID &&
			workerNode->nodeRole != SecondaryNodeRoleId() &&
			IsWorkerTheCurrentNode(workerNode))
		{
			ereport(ERROR, (errmsg("Node cannot add itself as a worker."),
							errhint(
								"Add the node as a coordinator by using: "
								"SELECT citus_set_coordinator_host('%s', %d);",
								nodeNameString, nodePort)));
		}

		ActivateNode(nodeNameString, nodePort);
	}

	PG_RETURN_INT32(nodeId);
}


/*
 * master_add_node is a wrapper function for old UDF name.
 */
Datum
master_add_node(PG_FUNCTION_ARGS)
{
	return citus_add_node(fcinfo);
}


/*
 * citus_add_inactive_node function adds a new node to the cluster as inactive node
 * and returns id of the newly added node. It does not replicate reference
 * tables to the new node, it only adds new node to the pg_dist_node table.
 */
Datum
citus_add_inactive_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	Name nodeClusterName = PG_GETARG_NAME(4);

	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	bool nodeAlreadyExists = false;
	nodeMetadata.groupId = PG_GETARG_INT32(2);
	nodeMetadata.nodeRole = PG_GETARG_OID(3);
	nodeMetadata.nodeCluster = NameStr(*nodeClusterName);

	if (nodeMetadata.groupId == COORDINATOR_GROUP_ID)
	{
		ereport(ERROR, (errmsg("coordinator node cannot be added as inactive node")));
	}

	int nodeId = AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
								 &nodeAlreadyExists);
	TransactionModifiedNodeMetadata = true;

	PG_RETURN_INT32(nodeId);
}


/*
 * master_add_inactive_node is a wrapper function for old UDF name.
 */
Datum
master_add_inactive_node(PG_FUNCTION_ARGS)
{
	return citus_add_inactive_node(fcinfo);
}


/*
 * citus_add_secondary_node adds a new secondary node to the cluster. It accepts as
 * arguments the primary node it should share a group with.
 */
Datum
citus_add_secondary_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);

	text *primaryName = PG_GETARG_TEXT_P(2);
	int32 primaryPort = PG_GETARG_INT32(3);
	char *primaryNameString = text_to_cstring(primaryName);

	Name nodeClusterName = PG_GETARG_NAME(4);
	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	bool nodeAlreadyExists = false;

	nodeMetadata.groupId = GroupForNode(primaryNameString, primaryPort);
	nodeMetadata.nodeCluster = NameStr(*nodeClusterName);
	nodeMetadata.nodeRole = SecondaryNodeRoleId();
	nodeMetadata.isActive = true;

	int nodeId = AddNodeMetadata(nodeNameString, nodePort, &nodeMetadata,
								 &nodeAlreadyExists);
	TransactionModifiedNodeMetadata = true;

	PG_RETURN_INT32(nodeId);
}


/*
 * master_add_secondary_node is a wrapper function for old UDF name.
 */
Datum
master_add_secondary_node(PG_FUNCTION_ARGS)
{
	return citus_add_secondary_node(fcinfo);
}


/*
 * citus_remove_node function removes the provided node from the pg_dist_node table of
 * the master node and all nodes with metadata.
 * The call to the citus_remove_node should be done by the super user and the specified
 * node should not have any active placements.
 * This function also deletes all reference table placements belong to the given node from
 * pg_dist_placement, but it does not drop actual placement at the node. In the case of
 * re-adding the node, citus_add_node first drops and re-creates the reference tables.
 */
Datum
citus_remove_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	RemoveNodeFromCluster(text_to_cstring(nodeNameText), nodePort);
	TransactionModifiedNodeMetadata = true;

	PG_RETURN_VOID();
}


/*
 * master_remove_node is a wrapper function for old UDF name.
 */
Datum
master_remove_node(PG_FUNCTION_ARGS)
{
	return citus_remove_node(fcinfo);
}


/*
 * citus_disable_node function sets isactive value of the provided node as inactive at
 * coordinator and all nodes with metadata regardless of the node having an active shard
 * placement.
 *
 * The call to the citus_disable_node must be done by the super user.
 *
 * This function also deletes all reference table placements belong to the given node
 * from pg_dist_placement, but it does not drop actual placement at the node. In the case
 * of re-activating the node, citus_add_node first drops and re-creates the reference
 * tables.
 */
Datum
citus_disable_node(PG_FUNCTION_ARGS)
{
	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	bool synchronousDisableNode = PG_GETARG_BOOL(2);

	char *nodeName = text_to_cstring(nodeNameText);
	WorkerNode *workerNode = ModifiableWorkerNode(nodeName, nodePort);

	/* there is no concept of invalid coordinator */
	bool isActive = false;
	ErrorIfCoordinatorMetadataSetFalse(workerNode, BoolGetDatum(isActive),
									   "isactive");

	WorkerNode *firstWorkerNode = GetFirstPrimaryWorkerNode();
	bool disablingFirstNode =
		(firstWorkerNode && firstWorkerNode->nodeId == workerNode->nodeId);

	if (disablingFirstNode && !synchronousDisableNode)
	{
		/*
		 * We sync metadata async and optionally in the background worker,
		 * it would mean that some nodes might get the updates while other
		 * not. And, if the node metadata that is changing is the first
		 * worker node, the problem gets nasty. We serialize modifications
		 * to replicated tables by acquiring locks on the first worker node.
		 *
		 * If some nodes get the metadata changes and some do not, they'd be
		 * acquiring the locks on different nodes. Hence, having the
		 * possibility of diverged shard placements for the same shard.
		 *
		 * To prevent that, we currently do not allow disabling the first
		 * worker node unless it is explicitly opted synchronous.
		 */
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("disabling the first worker node in the "
							   "metadata is not allowed"),
						errhint("You can force disabling node, SELECT "
								"citus_disable_node('%s', %d, "
								"synchronous:=true);", workerNode->workerName,
								nodePort),
						errdetail("Citus uses the first worker node in the "
								  "metadata for certain internal operations when "
								  "replicated tables are modified. Synchronous mode "
								  "ensures that all nodes have the same view of the "
								  "first worker node, which is used for certain "
								  "locking operations.")));
	}

	/*
	 * First, locally mark the node as inactive. We'll later trigger background
	 * worker to sync the metadata changes to the relevant nodes.
	 */
	workerNode =
		SetWorkerColumnLocalOnly(workerNode,
								 Anum_pg_dist_node_isactive,
								 BoolGetDatum(isActive));
	if (NodeIsPrimary(workerNode))
	{
		/*
		 * We do not allow disabling nodes if it contains any
		 * primary placement that is the "only" active placement
		 * for any given shard.
		 */
		ErrorIfNodeContainsNonRemovablePlacements(workerNode);
	}

	TransactionModifiedNodeMetadata = true;

	if (synchronousDisableNode)
	{
		/*
		 * The user might pick between sync vs async options.
		 *      - Pros for the sync option:
		 *          (a) the changes become visible on the cluster immediately
		 *          (b) even if the first worker node is disabled, there is no
		 *              risk of divergence of the placements of replicated shards
		 *      - Cons for the sync options:
		 *          (a) Does not work within 2PC transaction (e.g., BEGIN;
		 *              citus_disable_node(); PREPARE TRANSACTION ...);
		 *          (b) If there are multiple node failures (e.g., one another node
		 *              than the current node being disabled), the sync option would
		 *              fail because it'd try to sync the metadata changes to a node
		 *              that is not up and running.
		 */
		if (firstWorkerNode && firstWorkerNode->nodeId == workerNode->nodeId)
		{
			/*
			 * We cannot let any modification query on a replicated table to run
			 * concurrently with citus_disable_node() on the first worker node. If
			 * we let that, some worker nodes might calculate FirstWorkerNode()
			 * different than others. See LockShardListResourcesOnFirstWorker()
			 * for the details.
			 */
			BlockDistributedQueriesOnMetadataNodes();
		}

		SyncNodeMetadataToNodes();
	}
	else if (UnsetMetadataSyncedForAllWorkers())
	{
		/*
		 * We have not propagated the node metadata changes yet, make sure that all the
		 * active nodes get the metadata updates. We defer this operation to the
		 * background worker to make it possible disabling nodes when multiple nodes
		 * are down.
		 *
		 * Note that the active placements reside on the active nodes. Hence, when
		 * Citus finds active placements, it filters out the placements that are on
		 * the disabled nodes. That's why, we don't have to change/sync placement
		 * metadata at this point. Instead, we defer that to citus_activate_node()
		 * where we expect all nodes up and running.
		 */

		TriggerNodeMetadataSyncOnCommit();
	}

	PG_RETURN_VOID();
}


/*
 * BlockDistributedQueriesOnMetadataNodes blocks all the modification queries on
 * all nodes. Hence, should be used with caution.
 */
static void
BlockDistributedQueriesOnMetadataNodes(void)
{
	/* first, block on the coordinator */
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	/*
	 * Note that we might re-design this lock to be more granular than
	 * pg_dist_node, scoping only for modifications on the replicated
	 * tables. However, we currently do not have any such mechanism and
	 * given that citus_disable_node() runs instantly, it seems acceptable
	 * to block reads (or modifications on non-replicated tables) for
	 * a while.
	 */

	/* only superuser can disable node */
	Assert(superuser());

	SendCommandToWorkersWithMetadata(
		"LOCK TABLE pg_catalog.pg_dist_node IN EXCLUSIVE MODE;");
}


/*
 * master_disable_node is a wrapper function for old UDF name.
 */
Datum
master_disable_node(PG_FUNCTION_ARGS)
{
	return citus_disable_node(fcinfo);
}


/*
 * citus_set_node_property sets a property of the node
 */
Datum
citus_set_node_property(PG_FUNCTION_ARGS)
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

	TransactionModifiedNodeMetadata = true;

	PG_RETURN_VOID();
}


/*
 * master_set_node_property is a wrapper function for old UDF name.
 */
Datum
master_set_node_property(PG_FUNCTION_ARGS)
{
	return citus_set_node_property(fcinfo);
}


/*
 * InterTableRelationshipCommandList returns the command list to
 * set up the multiple integrations including
 *
 * (i) Foreign keys
 * (ii) Partionining hierarchy
 *
 * for each citus table.
 */
static List *
InterTableRelationshipCommandList()
{
	List *distributedTableList = CitusTableList();
	List *propagatedTableList = NIL;
	List *multipleTableIntegrationCommandList = NIL;

	CitusTableCacheEntry *cacheEntry = NULL;
	foreach_ptr(cacheEntry, distributedTableList)
	{
		/*
		 * Skip foreign key and partition creation when we shouldn't need to sync
		 * tablem metadata or the Citus table is owned by an extension.
		 */
		if (ShouldSyncTableMetadata(cacheEntry->relationId) &&
			!IsTableOwnedByExtension(cacheEntry->relationId))
		{
			propagatedTableList = lappend(propagatedTableList, cacheEntry);
		}
	}

	foreach_ptr(cacheEntry, propagatedTableList)
	{
		Oid relationId = cacheEntry->relationId;

		List *commandListForRelation =
			InterTableRelationshipOfRelationCommandList(relationId);

		multipleTableIntegrationCommandList = list_concat(
			multipleTableIntegrationCommandList,
			commandListForRelation);
	}

	multipleTableIntegrationCommandList = lcons(DISABLE_DDL_PROPAGATION,
												multipleTableIntegrationCommandList);
	multipleTableIntegrationCommandList = lappend(multipleTableIntegrationCommandList,
												  ENABLE_DDL_PROPAGATION);

	return multipleTableIntegrationCommandList;
}


/*
 * PgDistTableMetadataSyncCommandList returns the command list to sync the pg_dist_*
 * (except pg_dist_node) metadata. We call them as table metadata.
 */
List *
PgDistTableMetadataSyncCommandList(void)
{
	List *distributedTableList = CitusTableList();
	List *propagatedTableList = NIL;
	List *metadataSnapshotCommandList = NIL;

	/* create the list of tables whose metadata will be created */
	CitusTableCacheEntry *cacheEntry = NULL;
	foreach_ptr(cacheEntry, distributedTableList)
	{
		if (ShouldSyncTableMetadata(cacheEntry->relationId))
		{
			propagatedTableList = lappend(propagatedTableList, cacheEntry);
		}
	}

	/* create pg_dist_partition, pg_dist_shard and pg_dist_placement entries */
	foreach_ptr(cacheEntry, propagatedTableList)
	{
		List *tableMetadataCreateCommandList =
			CitusTableMetadataCreateCommandList(cacheEntry->relationId);

		metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
												  tableMetadataCreateCommandList);
	}

	/* commands to insert pg_dist_colocation entries */
	List *colocationGroupSyncCommandList = ColocationGroupCreateCommandList();
	metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
											  colocationGroupSyncCommandList);

	List *distributedObjectSyncCommandList = DistributedObjectMetadataSyncCommandList();
	metadataSnapshotCommandList = list_concat(metadataSnapshotCommandList,
											  distributedObjectSyncCommandList);

	metadataSnapshotCommandList = lcons(DISABLE_DDL_PROPAGATION,
										metadataSnapshotCommandList);
	metadataSnapshotCommandList = lappend(metadataSnapshotCommandList,
										  ENABLE_DDL_PROPAGATION);

	return metadataSnapshotCommandList;
}


/*
 * PropagateNodeWideObjectsCommandList is called during node activation to
 * propagate any object that should be propagated for every node. These are
 * generally not linked to any distributed object but change system wide behaviour.
 */
static List *
PropagateNodeWideObjectsCommandList()
{
	/* collect all commands */
	List *ddlCommands = NIL;

	if (EnableAlterRoleSetPropagation)
	{
		/*
		 * Get commands for database and postgres wide settings. Since these settings are not
		 * linked to any role that can be distributed we need to distribute them seperately
		 */
		List *alterRoleSetCommands = GenerateAlterRoleSetCommandForRole(InvalidOid);
		ddlCommands = list_concat(ddlCommands, alterRoleSetCommands);
	}

	if (list_length(ddlCommands) > 0)
	{
		/* if there are command wrap them in enable_ddl_propagation off */
		ddlCommands = lcons(DISABLE_DDL_PROPAGATION, ddlCommands);
		ddlCommands = lappend(ddlCommands, ENABLE_DDL_PROPAGATION);
	}

	return ddlCommands;
}


/*
 * SyncDistributedObjectsCommandList returns commands to sync object dependencies
 * to the given worker node. To be idempotent, it first drops the ones required to be
 * dropped.
 *
 * Object dependencies include:
 *
 * - All dependencies (e.g., types, schemas, sequences)
 * - All shell distributed tables
 * - Inter relation between those shell tables
 * - Node wide objects
 *
 * We also update the local group id here, as handling sequence dependencies
 * requires it.
 */
List *
SyncDistributedObjectsCommandList(WorkerNode *workerNode)
{
	List *commandList = NIL;

	/*
	 * Propagate node wide objects. It includes only roles for now.
	 */
	commandList = list_concat(commandList, PropagateNodeWideObjectsCommandList());

	/*
	 * Replicate all objects of the pg_dist_object to the remote node.
	 */
	commandList = list_concat(commandList, ReplicateAllObjectsToNodeCommandList(
								  workerNode->workerName, workerNode->workerPort));

	/*
	 * After creating each table, handle the inter table relationship between
	 * those tables.
	 */
	commandList = list_concat(commandList, InterTableRelationshipCommandList());

	return commandList;
}


/*
 * SyncDistributedObjectsToNodeList sync the distributed objects to the node. It includes
 * - All dependencies (e.g., types, schemas, sequences)
 * - All shell distributed table
 * - Inter relation between those shell tables
 *
 * Note that we do not create the distributed dependencies on the coordinator
 * since all the dependencies should be present in the coordinator already.
 */
static void
SyncDistributedObjectsToNodeList(List *workerNodeList)
{
	List *workerNodesToSync = NIL;
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		if (NodeIsCoordinator(workerNode))
		{
			/* coordinator has all the objects */
			continue;
		}

		if (!NodeIsPrimary(workerNode))
		{
			/* secondary nodes gets the objects from their primaries via replication */
			continue;
		}

		workerNodesToSync = lappend(workerNodesToSync, workerNode);
	}

	if (workerNodesToSync == NIL)
	{
		return;
	}

	EnsureSequentialModeMetadataOperations();

	Assert(ShouldPropagate());

	List *commandList = SyncDistributedObjectsCommandList(workerNode);

	/* send commands to new workers, the current user should be a superuser */
	Assert(superuser());
	SendMetadataCommandListToWorkerListInCoordinatedTransaction(
		workerNodesToSync,
		CurrentUserName(),
		commandList);
}


/*
 * UpdateLocalGroupIdOnNode updates local group id on node.
 */
static void
UpdateLocalGroupIdOnNode(WorkerNode *workerNode)
{
	if (NodeIsPrimary(workerNode) && !NodeIsCoordinator(workerNode))
	{
		List *commandList = list_make1(LocalGroupIdUpdateCommand(workerNode->groupId));

		/* send commands to new workers, the current user should be a superuser */
		Assert(superuser());
		SendMetadataCommandListToWorkerListInCoordinatedTransaction(
			list_make1(workerNode),
			CurrentUserName(),
			commandList);
	}
}


/*
 * SyncPgDistTableMetadataToNodeList syncs the pg_dist_partition, pg_dist_shard
 * pg_dist_placement and pg_dist_object metadata entries.
 *
 */
static void
SyncPgDistTableMetadataToNodeList(List *nodeList)
{
	/* send commands to new workers, the current user should be a superuser */
	Assert(superuser());

	List *nodesWithMetadata = NIL;
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, nodeList)
	{
		if (NodeIsPrimary(workerNode) && !NodeIsCoordinator(workerNode))
		{
			nodesWithMetadata = lappend(nodesWithMetadata, workerNode);
		}
	}

	if (nodesWithMetadata == NIL)
	{
		return;
	}

	List *syncPgDistMetadataCommandList = PgDistTableMetadataSyncCommandList();
	SendMetadataCommandListToWorkerListInCoordinatedTransaction(
		nodesWithMetadata,
		CurrentUserName(),
		syncPgDistMetadataCommandList);
}


/*
 * ModifiableWorkerNode gets the requested WorkerNode and also gets locks
 * required for modifying it. This fails if the node does not exist.
 */
static WorkerNode *
ModifiableWorkerNode(const char *nodeName, int32 nodePort)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	/* take an exclusive lock on pg_dist_node to serialize pg_dist_node changes */
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	WorkerNode *workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);
	if (workerNode == NULL)
	{
		ereport(ERROR, (errmsg("node at \"%s:%u\" does not exist", nodeName, nodePort)));
	}

	return workerNode;
}


/*
 * citus_activate_node UDF activates the given node. It sets the node's isactive
 * value to active and replicates all reference tables to that node.
 */
Datum
citus_activate_node(PG_FUNCTION_ARGS)
{
	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	WorkerNode *workerNode = ModifiableWorkerNode(text_to_cstring(nodeNameText),
												  nodePort);
	ActivateNode(workerNode->workerName, workerNode->workerPort);

	TransactionModifiedNodeMetadata = true;

	PG_RETURN_INT32(workerNode->nodeId);
}


/*
 * master_activate_node is a wrapper function for old UDF name.
 */
Datum
master_activate_node(PG_FUNCTION_ARGS)
{
	return citus_activate_node(fcinfo);
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
 * NodeIsPrimaryAndRemote returns whether the argument represents the remote
 * primary node.
 */
bool
NodeIsPrimaryAndRemote(WorkerNode *worker)
{
	return NodeIsPrimary(worker) && !NodeIsLocal(worker);
}


/*
 * NodeIsPrimary returns whether the argument represents a primary node.
 */
bool
NodeIsPrimary(WorkerNode *worker)
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
 * NodeIsLocal returns whether the argument represents the local node.
 */
static bool
NodeIsLocal(WorkerNode *worker)
{
	return worker->groupId == GetLocalGroupId();
}


/*
 * NodeIsSecondary returns whether the argument represents a secondary node.
 */
bool
NodeIsSecondary(WorkerNode *worker)
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
 * NodeIsReadable returns whether we're allowed to send SELECT queries to this
 * node.
 */
bool
NodeIsReadable(WorkerNode *workerNode)
{
	if (ReadFromSecondaries == USE_SECONDARY_NODES_NEVER &&
		NodeIsPrimary(workerNode))
	{
		return true;
	}

	if (ReadFromSecondaries == USE_SECONDARY_NODES_ALWAYS &&
		NodeIsSecondary(workerNode))
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

		if (NodeIsPrimary(workerNode))
		{
			hash_seq_term(&status);
			return workerNode;
		}
	}

	return NULL;
}


/*
 * ActivateNodeList iterates over the nodeList and activates the nodes.
 * Some part of the node activation is done parallel across the nodes,
 * such as syncing the metadata. However, reference table replication is
 * done one by one across nodes.
 */
void
ActivateNodeList(List *nodeList)
{
	/*
	 * We currently require the object propagation to happen via superuser,
	 * see #5139. While activating a node, we sync both metadata and object
	 * propagation.
	 *
	 * In order to have a fully transactional semantics with add/activate
	 * node operations, we require superuser. Note that for creating
	 * non-owned objects, we already require a superuser connection.
	 * By ensuring the current user to be a superuser, we can guarantee
	 * to send all commands within the same remote transaction.
	 */
	EnsureSuperUser();

	/* take an exclusive lock on pg_dist_node to serialize pg_dist_node changes */
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);


	List *nodeToSyncMetadata = NIL;
	WorkerNode *node = NULL;
	foreach_ptr(node, nodeList)
	{
		/*
		 * First, locally mark the node is active, if everything goes well,
		 * we are going to sync this information to all the metadata nodes.
		 */
		WorkerNode *workerNode =
			FindWorkerNodeAnyCluster(node->workerName, node->workerPort);
		if (workerNode == NULL)
		{
			ereport(ERROR, (errmsg("node at \"%s:%u\" does not exist", node->workerName,
								   node->workerPort)));
		}

		/* both nodes should be the same */
		Assert(workerNode->nodeId == node->nodeId);

		/*
		 * Delete existing reference and replicated table placements on the
		 * given groupId if the group has been disabled earlier (e.g., isActive
		 * set to false).
		 *
		 * Sync the metadata changes to all existing metadata nodes irrespective
		 * of the current nodes' metadata sync state. We expect all nodes up
		 * and running when another node is activated.
		 */
		if (!workerNode->isActive && NodeIsPrimary(workerNode))
		{
			bool localOnly = false;
			DeleteAllReplicatedTablePlacementsFromNodeGroup(workerNode->groupId,
															localOnly);
		}

		workerNode =
			SetWorkerColumnLocalOnly(workerNode, Anum_pg_dist_node_isactive,
									 BoolGetDatum(true));

		if (NodeIsCoordinator(workerNode) && NodeIsPrimary(workerNode))
		{
			ereport(NOTICE, (errmsg("%s:%d is the coordinator and already contains "
									"metadata, skipping syncing the metadata",
									workerNode->workerName, workerNode->workerPort)));
			continue;
		}

		/* TODO: Once all tests will be enabled for MX, we can remove sync by default check */
		bool syncMetadata = EnableMetadataSync && NodeIsPrimary(workerNode);
		if (syncMetadata)
		{
			/*
			 * We are going to sync the metadata anyway in this transaction, so do
			 * not fail just because the current metadata is not synced.
			 */
			SetWorkerColumn(workerNode, Anum_pg_dist_node_metadatasynced,
							BoolGetDatum(true));

			/*
			 * Update local group id first, as object dependency logic requires to have
			 * updated local group id.
			 */
			UpdateLocalGroupIdOnNode(workerNode);

			nodeToSyncMetadata = lappend(nodeToSyncMetadata, workerNode);
		}
	}

	/*
	 * Ideally, we'd want to drop and sync the metadata inside a
	 * single transaction. However, for some users, the metadata might
	 * be excessively large. In that cases, Postgres fails to handle
	 * processing of all the commands inside a single transaction.
	 *
	 * As of PG 15, we get error messages like the following at
	 * commit time that are caused by relcache/catcache invalidations:
	 *    ERROR:  invalid memory alloc request size 1073741824
	 *
	 * That's why, we try to split as much of the work as possible.
	 * It incurs some risk of failures during these steps causing
	 * issues if any of the steps is not idempotent.
	 */
	DropExistingMetadataInOutsideTransaction(nodeToSyncMetadata);


	/*
	 * Sync distributed objects first. We must sync distributed objects before
	 * replicating reference tables to the remote node, as reference tables may
	 * need such objects.
	 */
	SyncDistributedObjectsToNodeList(nodeToSyncMetadata);

	/*
	 * Sync node metadata. We must sync node metadata before syncing table
	 * related pg_dist_xxx metadata. Since table related metadata requires
	 * to have right pg_dist_node entries.
	 */
	foreach_ptr(node, nodeToSyncMetadata)
	{
		SyncNodeMetadataToNode(node->workerName, node->workerPort);
	}

	/*
	 * As the last step, sync the table related metadata to the remote node.
	 * We must handle it as the last step because of limitations shared with
	 * above comments.
	 */
	SyncPgDistTableMetadataToNodeList(nodeToSyncMetadata);

	foreach_ptr(node, nodeList)
	{
		bool isActive = true;

		/* finally, let all other active metadata nodes to learn about this change */
		SetNodeState(node->workerName, node->workerPort, isActive);
	}
}


/*
 * DropExistingMetadataInOutsideTransaction gets a nodeList and sends the
 * necessary commands to drop the metadata (excluding node metadata).
 *
 * The function establishes one connection per node, and closes at the end.
 */
static void
DropExistingMetadataInOutsideTransaction(List *nodeList)
{
	List *connectionList = NIL;

	/* first, establish new connections */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, nodeList)
	{
		int connectionFlags = FORCE_NEW_CONNECTION;

		Assert(superuser());
		MultiConnection *connection =
			GetNodeUserDatabaseConnection(connectionFlags, workerNode->workerName,
										  workerNode->workerPort, NULL, NULL);

		connectionList = lappend(connectionList, connection);
	}

	char *command = NULL;
	List *commandList = DropExistingMetadataCommandList();
	foreach_ptr(command, commandList)
	{
		ExecuteRemoteCommandInConnectionList(connectionList, command);
	}

	/* finally, close the connections as we don't need them anymore */
	MultiConnection *connection;
	foreach_ptr(connection, connectionList)
	{
		CloseConnection(connection);
	}
}


/*
 * DropExistingMetadataCommandList returns a list of commands that can
 * be used to drop the metadata (excluding node metadata).
 */
List *
DropExistingMetadataCommandList()
{
	List *dropMetadataCommandList = NIL;

	/* remove all dist table and object related metadata first */
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  BREAK_CITUS_TABLE_SEQUENCE_DEPENDENCY_COMMAND);
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  REMOVE_PARTITIONED_SHELL_TABLES_COMMAND);
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  REMOVE_ALL_SHELL_TABLES_COMMAND);


	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  DELETE_ALL_PARTITIONS);
	dropMetadataCommandList = lappend(dropMetadataCommandList, DELETE_ALL_SHARDS);
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  DELETE_ALL_PLACEMENTS);
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  DELETE_ALL_DISTRIBUTED_OBJECTS);
	dropMetadataCommandList = lappend(dropMetadataCommandList,
									  DELETE_ALL_COLOCATION);

	return dropMetadataCommandList;
}


/*
 * ActivateNode activates the node with nodeName and nodePort. Currently, activation
 * includes only replicating the reference tables and setting isactive column of the
 * given node.
 */
int
ActivateNode(char *nodeName, int nodePort)
{
	bool isActive = true;

	WorkerNode *workerNode = ModifiableWorkerNode(nodeName, nodePort);
	ActivateNodeList(list_make1(workerNode));

	/* finally, let all other active metadata nodes to learn about this change */
	WorkerNode *newWorkerNode = SetNodeState(nodeName, nodePort, isActive);
	Assert(newWorkerNode->nodeId == workerNode->nodeId);

	TransactionModifiedNodeMetadata = true;

	return newWorkerNode->nodeId;
}


/*
 * citus_update_node moves the requested node to a different nodename and nodeport. It
 * locks to ensure no queries are running concurrently; and is intended for customers who
 * are running their own failover solution.
 */
Datum
citus_update_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

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
	List *placementList = NIL;
	BackgroundWorkerHandle *handle = NULL;

	WorkerNode *workerNodeWithSameAddress = FindWorkerNodeAnyCluster(newNodeNameString,
																	 newNodePort);
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

	WorkerNode *workerNode = FindNodeAnyClusterByNodeId(nodeId);
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
	 *   during the invocation of citus_update_node to terminate conflicting
	 *   backends proactively.
	 *
	 * It might be worth blocking reads to a secondary for the same reasons,
	 * though we currently only query secondaries on follower clusters
	 * where these locks will have no effect.
	 */
	if (NodeIsPrimary(workerNode))
	{
		/*
		 * before acquiring the locks check if we want a background worker to help us to
		 * aggressively obtain the locks.
		 */
		if (force)
		{
			handle = StartLockAcquireHelperBackgroundWorker(MyProcPid, lock_cooldown);
			if (!handle)
			{
				/*
				 * We failed to start a background worker, which probably means that we exceeded
				 * max_worker_processes, and this is unlikely to be resolved by retrying. We do not want
				 * to repeatedly throw an error because if citus_update_node is called to complete a
				 * failover then finishing is the only way to bring the cluster back up. Therefore we
				 * give up on killing other backends and simply wait for the lock. We do set
				 * lock_timeout to lock_cooldown, because we don't want to wait forever to get a lock.
				 */
				SetLockTimeoutLocally(lock_cooldown);
				ereport(WARNING, (errmsg(
									  "could not start background worker to kill backends with conflicting"
									  " locks to force the update. Degrading to acquiring locks "
									  "with a lock time out."),
								  errhint(
									  "Increasing max_worker_processes might help.")));
			}
		}

		placementList = AllShardPlacementsOnNodeGroup(workerNode->groupId);
		LockShardsInPlacementListMetadata(placementList, AccessExclusiveLock);
	}

	/*
	 * if we have planned statements such as prepared statements, we should clear the cache so that
	 * the planned cache doesn't return the old nodename/nodepost.
	 */
	ResetPlanCache();

	UpdateNodeLocation(nodeId, newNodeNameString, newNodePort);

	/* we should be able to find the new node from the metadata */
	workerNode = FindWorkerNodeAnyCluster(newNodeNameString, newNodePort);
	Assert(workerNode->nodeId == nodeId);

	/*
	 * Propagate the updated pg_dist_node entry to all metadata workers.
	 * citus-ha uses citus_update_node() in a prepared transaction, and
	 * we don't support coordinated prepared transactions, so we cannot
	 * propagate the changes to the worker nodes here. Instead we mark
	 * all metadata nodes as not-synced and ask maintenanced to do the
	 * propagation.
	 *
	 * It is possible that maintenance daemon does the first resync too
	 * early, but that's fine, since this will start a retry loop with
	 * 5 second intervals until sync is complete.
	 */
	if (UnsetMetadataSyncedForAllWorkers())
	{
		TriggerNodeMetadataSyncOnCommit();
	}

	if (handle != NULL)
	{
		/*
		 * this will be called on memory context cleanup as well, if the worker has been
		 * terminated already this will be a noop
		 */
		TerminateBackgroundWorker(handle);
	}

	TransactionModifiedNodeMetadata = true;

	PG_RETURN_VOID();
}


/*
 * master_update_node is a wrapper function for old UDF name.
 */
Datum
master_update_node(PG_FUNCTION_ARGS)
{
	return citus_update_node(fcinfo);
}


/*
 * SetLockTimeoutLocally sets the lock_timeout to the given value.
 * This setting is local.
 */
static void
SetLockTimeoutLocally(int32 lockCooldown)
{
	set_config_option("lock_timeout", ConvertIntToString(lockCooldown),
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}


static void
UpdateNodeLocation(int32 nodeId, char *newNodeName, int32 newNodePort)
{
	const bool indexOK = true;

	ScanKeyData scanKey[1];
	Datum values[Natts_pg_dist_node];
	bool isnull[Natts_pg_dist_node];
	bool replace[Natts_pg_dist_node];

	Relation pgDistNode = table_open(DistNodeRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodeid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodeId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistNode, DistNodeNodeIdIndexId(),
													indexOK,
													NULL, 1, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
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
	table_close(pgDistNode, NoLock);
}


/*
 * get_shard_id_for_distribution_column function takes a distributed table name and a
 * distribution value then returns shard id of the shard which belongs to given table and
 * contains given value. This function only works for hash distributed tables.
 */
Datum
get_shard_id_for_distribution_column(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	ShardInterval *shardInterval = NULL;

	/*
	 * To have optional parameter as NULL, we defined this UDF as not strict, therefore
	 * we need to check all parameters for NULL values.
	 */
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("relation cannot be NULL")));
	}

	Oid relationId = PG_GETARG_OID(0);
	EnsureTablePermissions(relationId, ACL_SELECT);

	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("relation is not distributed")));
	}

	if (IsCitusTableType(relationId, CITUS_TABLE_WITH_NO_DIST_KEY))
	{
		List *shardIntervalList = LoadShardIntervalList(relationId);
		if (shardIntervalList == NIL)
		{
			PG_RETURN_INT64(0);
		}

		shardInterval = (ShardInterval *) linitial(shardIntervalList);
	}
	else if (IsCitusTableType(relationId, HASH_DISTRIBUTED) ||
			 IsCitusTableType(relationId, RANGE_DISTRIBUTED))
	{
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

		/* if given table is not reference table, distributionValue cannot be NULL */
		if (PG_ARGISNULL(1))
		{
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("distribution value cannot be NULL for tables other "
								   "than reference tables.")));
		}

		Datum inputDatum = PG_GETARG_DATUM(1);
		Oid inputDataType = get_fn_expr_argtype(fcinfo->flinfo, 1);
		char *distributionValueString = DatumToString(inputDatum, inputDataType);

		Var *distributionColumn = DistPartitionKeyOrError(relationId);
		Oid distributionDataType = distributionColumn->vartype;

		Datum distributionValueDatum = StringToDatum(distributionValueString,
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
 * citus_nodename_for_nodeid returns the node name for the node with given node id
 */
Datum
citus_nodename_for_nodeid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int nodeId = PG_GETARG_INT32(0);

	WorkerNode *node = FindNodeAnyClusterByNodeId(nodeId);

	if (node == NULL)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(cstring_to_text(node->workerName));
}


/*
 * citus_nodeport_for_nodeid returns the node port for the node with given node id
 */
Datum
citus_nodeport_for_nodeid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int nodeId = PG_GETARG_INT32(0);

	WorkerNode *node = FindNodeAnyClusterByNodeId(nodeId);

	if (node == NULL)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_INT32(node->workerPort);
}


/*
 * citus_coordinator_nodeid returns the node id of the coordinator node
 */
Datum
citus_coordinator_nodeid(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int coordinatorNodeId = FindCoordinatorNodeId();

	if (coordinatorNodeId == -1)
	{
		PG_RETURN_INT32(0);
	}

	PG_RETURN_INT32(coordinatorNodeId);
}


/*
 * citus_is_coordinator returns whether the current node is a coordinator.
 * We consider the node a coordinator if its group ID is 0 and it has
 * pg_dist_node entries (only group ID 0 could indicate a worker without
 * metadata).
 */
Datum
citus_is_coordinator(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	bool isCoordinator = false;

	if (GetLocalGroupId() == COORDINATOR_GROUP_ID &&
		ActiveReadableNodeCount() > 0)
	{
		isCoordinator = true;
	}

	PG_RETURN_BOOL(isCoordinator);
}


/*
 * FindWorkerNode searches over the worker nodes and returns the workerNode
 * if it already exists. Else, the function returns NULL.
 */
WorkerNode *
FindWorkerNode(const char *nodeName, int32 nodePort)
{
	HTAB *workerNodeHash = GetWorkerNodeHash();
	bool handleFound = false;

	WorkerNode *searchedNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	strlcpy(searchedNode->workerName, nodeName, WORKER_LENGTH);
	searchedNode->workerPort = nodePort;

	void *hashKey = (void *) searchedNode;
	WorkerNode *cachedWorkerNode = (WorkerNode *) hash_search(workerNodeHash, hashKey,
															  HASH_FIND,
															  &handleFound);
	if (handleFound)
	{
		WorkerNode *workerNode = (WorkerNode *) palloc(sizeof(WorkerNode));
		*workerNode = *cachedWorkerNode;
		return workerNode;
	}

	return NULL;
}


/*
 * FindWorkerNode searches over the worker nodes and returns the workerNode
 * if it exists otherwise it errors out.
 */
WorkerNode *
FindWorkerNodeOrError(const char *nodeName, int32 nodePort)
{
	WorkerNode *node = FindWorkerNode(nodeName, nodePort);
	if (node == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
						errmsg("node %s:%d not found", nodeName, nodePort)));
	}
	return node;
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

	Relation pgDistNode = table_open(DistNodeRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);

	HeapTuple heapTuple = GetNodeTuple(nodeName, nodePort);
	if (heapTuple != NULL)
	{
		workerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);
	}

	table_close(pgDistNode, NoLock);
	return workerNode;
}


/*
 * FindNodeAnyClusterByNodeId searches pg_dist_node and returns the node with
 * the nodeId. If the node can't be found returns NULL.
 */
static WorkerNode *
FindNodeAnyClusterByNodeId(uint32 nodeId)
{
	bool includeNodesFromOtherClusters = true;
	List *nodeList = ReadDistNode(includeNodesFromOtherClusters);
	WorkerNode *node = NULL;

	foreach_ptr(node, nodeList)
	{
		if (node->nodeId == nodeId)
		{
			return node;
		}
	}

	return NULL;
}


/*
 * FindNodeWithNodeId searches pg_dist_node and returns the node with the nodeId.
 * If the node cannot be found this functions errors.
 */
WorkerNode *
FindNodeWithNodeId(int nodeId, bool missingOk)
{
	List *nodeList = ActiveReadableNodeList();
	WorkerNode *node = NULL;

	foreach_ptr(node, nodeList)
	{
		if (node->nodeId == nodeId)
		{
			return node;
		}
	}

	/* there isn't any node with nodeId in pg_dist_node */
	if (!missingOk)
	{
		elog(ERROR, "node with node id %d could not be found", nodeId);
	}

	return NULL;
}


/*
 * FindCoordinatorNodeId returns the node id of the coordinator node
 */
static int
FindCoordinatorNodeId()
{
	bool includeNodesFromOtherClusters = false;
	List *nodeList = ReadDistNode(includeNodesFromOtherClusters);
	WorkerNode *node = NULL;

	foreach_ptr(node, nodeList)
	{
		if (NodeIsCoordinator(node))
		{
			return node->nodeId;
		}
	}

	return -1;
}


/*
 * ReadDistNode iterates over pg_dist_node table, converts each row
 * into its memory representation (i.e., WorkerNode) and adds them into
 * a list. Lastly, the list is returned to the caller.
 *
 * It skips nodes which are not in the current clusters unless requested to do otherwise
 * by includeNodesFromOtherClusters.
 */
List *
ReadDistNode(bool includeNodesFromOtherClusters)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	List *workerNodeList = NIL;

	Relation pgDistNode = table_open(DistNodeRelationId(), AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan(pgDistNode,
													InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		WorkerNode *workerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);

		if (includeNodesFromOtherClusters ||
			strncmp(workerNode->nodeCluster, CurrentCluster, WORKER_LENGTH) == 0)
		{
			/* the coordinator acts as if it never sees nodes not in its cluster */
			workerNodeList = lappend(workerNodeList, workerNode);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistNode, NoLock);

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
	WorkerNode *workerNode = ModifiableWorkerNode(nodeName, nodePort);
	if (NodeIsPrimary(workerNode))
	{
		ErrorIfNodeContainsNonRemovablePlacements(workerNode);

		/*
		 * Delete reference table placements so they are not taken into account
		 * for the check if there are placements after this.
		 */
		bool localOnly = false;
		DeleteAllReplicatedTablePlacementsFromNodeGroup(workerNode->groupId,
														localOnly);

		/*
		 * Secondary nodes are read-only, never 2PC is used.
		 * Hence, no items can be inserted to pg_dist_transaction
		 * for secondary nodes.
		 */
		DeleteWorkerTransactions(workerNode);
	}

	DeleteNodeRow(workerNode->workerName, nodePort);

	RemoveOldShardPlacementForNodeGroup(workerNode->groupId);

	/* make sure we don't have any lingering session lifespan connections */
	CloseNodeConnectionsAfterTransaction(workerNode->workerName, nodePort);

	if (EnableMetadataSync)
	{
		char *nodeDeleteCommand = NodeDeleteCommand(workerNode->nodeId);

		SendCommandToWorkersWithMetadata(nodeDeleteCommand);
	}
}


/*
 * ErrorIfNodeContainsNonRemovablePlacements throws an error if the input node
 * contains at least one placement on the node that is the last active
 * placement.
 */
static void
ErrorIfNodeContainsNonRemovablePlacements(WorkerNode *workerNode)
{
	int32 groupId = workerNode->groupId;
	List *shardPlacements = AllShardPlacementsOnNodeGroup(groupId);
	GroupShardPlacement *placement = NULL;
	foreach_ptr(placement, shardPlacements)
	{
		if (!PlacementHasActivePlacementOnAnotherGroup(placement))
		{
			Oid relationId = RelationIdForShard(placement->shardId);
			char *qualifiedRelationName = generate_qualified_relation_name(relationId);

			ereport(ERROR, (errmsg("cannot remove or disable the node "
								   "%s:%d because because it contains "
								   "the only shard placement for "
								   "shard " UINT64_FORMAT, workerNode->workerName,
								   workerNode->workerPort, placement->shardId),
							errdetail("One of the table(s) that prevents the operation "
									  "complete successfully is %s",
									  qualifiedRelationName),
							errhint("To proceed, either drop the tables or use "
									"undistribute_table() function to convert "
									"them to local tables")));
		}
	}
}


/*
 * PlacementHasActivePlacementOnAnotherGroup returns true if there is at least
 * one more active placement of the input sourcePlacement on another group.
 */
static bool
PlacementHasActivePlacementOnAnotherGroup(GroupShardPlacement *sourcePlacement)
{
	uint64 shardId = sourcePlacement->shardId;
	List *activePlacementList = ActiveShardPlacementList(shardId);

	bool foundActivePlacementOnAnotherGroup = false;
	ShardPlacement *activePlacement = NULL;
	foreach_ptr(activePlacement, activePlacementList)
	{
		if (activePlacement->groupId != sourcePlacement->groupId)
		{
			foundActivePlacementOnAnotherGroup = true;
			break;
		}
	}

	return foundActivePlacementOnAnotherGroup;
}


/*
 * RemoveOldShardPlacementForNodeGroup removes all old shard placements
 * for the given node group from pg_dist_placement.
 */
static void
RemoveOldShardPlacementForNodeGroup(int groupId)
{
	/*
	 * Prevent concurrent deferred drop
	 */
	LockPlacementCleanup();
	List *shardPlacementsOnNode = AllShardPlacementsOnNodeGroup(groupId);
	GroupShardPlacement *placement = NULL;
	foreach_ptr(placement, shardPlacementsOnNode)
	{
		if (placement->shardState == SHARD_STATE_TO_DELETE)
		{
			DeleteShardPlacementRow(placement->placementId);
		}
	}
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
		if (workerNode->hasMetadata && NodeIsPrimary(workerNode))
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
	EnsureCoordinator();

	*nodeAlreadyExists = false;

	WorkerNode *workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);
	if (workerNode != NULL)
	{
		/* return early without holding locks when the node already exists */
		*nodeAlreadyExists = true;

		return workerNode->nodeId;
	}

	/*
	 * We are going to change pg_dist_node, prevent any concurrent reads that
	 * are not tolerant to concurrent node addition by taking an exclusive
	 * lock (conflicts with all but AccessShareLock).
	 *
	 * We may want to relax or have more fine-grained locking in the future
	 * to allow users to add multiple nodes concurrently.
	 */
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);

	/* recheck in case 2 node additions pass the first check concurrently */
	workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);
	if (workerNode != NULL)
	{
		*nodeAlreadyExists = true;

		return workerNode->nodeId;
	}

	if (nodeMetadata->groupId != COORDINATOR_GROUP_ID &&
		strcmp(nodeName, "localhost") != 0)
	{
		/*
		 * User tries to add a worker with a non-localhost address. If the coordinator
		 * is added with "localhost" as well, the worker won't be able to connect.
		 */

		bool isCoordinatorInMetadata = false;
		WorkerNode *coordinatorNode = PrimaryNodeForGroup(COORDINATOR_GROUP_ID,
														  &isCoordinatorInMetadata);
		if (isCoordinatorInMetadata &&
			strcmp(coordinatorNode->workerName, "localhost") == 0)
		{
			ereport(ERROR, (errmsg("cannot add a worker node when the coordinator "
								   "hostname is set to localhost"),
							errdetail("Worker nodes need to be able to connect to the "
									  "coordinator to transfer data."),
							errhint("Use SELECT citus_set_coordinator_host('<hostname>') "
									"to configure the coordinator hostname")));
		}
	}

	/*
	 * When adding the first worker when the coordinator has shard placements,
	 * print a notice on how to drain the coordinator.
	 */
	if (nodeMetadata->groupId != COORDINATOR_GROUP_ID && CoordinatorAddedAsWorkerNode() &&
		ActivePrimaryNonCoordinatorNodeCount() == 0 &&
		NodeGroupHasShardPlacements(COORDINATOR_GROUP_ID, true))
	{
		WorkerNode *coordinator = CoordinatorNodeIfAddedAsWorkerOrError();

		ereport(NOTICE, (errmsg("shards are still on the coordinator after adding the "
								"new node"),
						 errhint("Use SELECT rebalance_table_shards(); to balance "
								 "shards data between workers and coordinator or "
								 "SELECT citus_drain_node(%s,%d); to permanently "
								 "move shards away from the coordinator.",
								 quote_literal_cstr(coordinator->workerName),
								 coordinator->workerPort)));
	}

	/* user lets Citus to decide on the group that the newly added node should be in */
	if (nodeMetadata->groupId == INVALID_GROUP_ID)
	{
		nodeMetadata->groupId = GetNextGroupId();
	}

	if (nodeMetadata->groupId == COORDINATOR_GROUP_ID)
	{
		/*
		 * Coordinator has always the authoritative metadata, reflect this
		 * fact in the pg_dist_node.
		 */
		nodeMetadata->hasMetadata = true;
		nodeMetadata->metadataSynced = true;

		/*
		 * There is no concept of "inactive" coordinator, so hard code it.
		 */
		nodeMetadata->isActive = true;
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
	int nextNodeIdInt = GetNextNodeId();

	InsertNodeRow(nextNodeIdInt, nodeName, nodePort, nodeMetadata);

	workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);

	if (EnableMetadataSync)
	{
		/* send the delete command to all primary nodes with metadata */
		char *nodeDeleteCommand = NodeDeleteCommand(workerNode->nodeId);
		SendCommandToWorkersWithMetadata(nodeDeleteCommand);

		/* finally prepare the insert command and send it to all primary nodes */
		uint32 primariesWithMetadata = CountPrimariesWithMetadata();
		if (primariesWithMetadata != 0)
		{
			List *workerNodeList = list_make1(workerNode);
			char *nodeInsertCommand = NodeListInsertCommand(workerNodeList);

			SendCommandToWorkersWithMetadata(nodeInsertCommand);
		}
	}

	return workerNode->nodeId;
}


/*
 * SetWorkerColumn function sets the column with the specified index
 * on the worker in pg_dist_node, by calling SetWorkerColumnLocalOnly.
 * It also sends the same command for node update to other metadata nodes.
 * If anything fails during the transaction, we rollback it.
 * Returns the new worker node after the modification.
 */
WorkerNode *
SetWorkerColumn(WorkerNode *workerNode, int columnIndex, Datum value)
{
	workerNode = SetWorkerColumnLocalOnly(workerNode, columnIndex, value);

	if (EnableMetadataSync)
	{
		char *metadataSyncCommand =
			GetMetadataSyncCommandToSetNodeColumn(workerNode, columnIndex, value);

		SendCommandToWorkersWithMetadata(metadataSyncCommand);
	}

	return workerNode;
}


/*
 * SetWorkerColumnOptional function sets the column with the specified index
 * on the worker in pg_dist_node, by calling SetWorkerColumnLocalOnly.
 * It also sends the same command optionally for node update to other metadata nodes,
 * meaning that failures are ignored. Returns the new worker node after the modification.
 */
WorkerNode *
SetWorkerColumnOptional(WorkerNode *workerNode, int columnIndex, Datum value)
{
	char *metadataSyncCommand = GetMetadataSyncCommandToSetNodeColumn(workerNode,
																	  columnIndex,
																	  value);

	List *workerNodeList = TargetWorkerSetNodeList(NON_COORDINATOR_METADATA_NODES,
												   ShareLock);

	/* open connections in parallel */
	WorkerNode *worker = NULL;
	foreach_ptr(worker, workerNodeList)
	{
		bool success = SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction(
			worker->workerName, worker->workerPort,
			CurrentUserName(),
			list_make1(metadataSyncCommand));

		if (!success)
		{
			/* metadata out of sync, mark the worker as not synced */
			ereport(WARNING, (errmsg("Updating the metadata of the node %s:%d "
									 "is failed on node %s:%d. "
									 "Metadata on %s:%d is marked as out of sync.",
									 workerNode->workerName, workerNode->workerPort,
									 worker->workerName, worker->workerPort,
									 worker->workerName, worker->workerPort)));

			SetWorkerColumnLocalOnly(worker, Anum_pg_dist_node_metadatasynced,
									 BoolGetDatum(false));
		}
		else if (workerNode->nodeId == worker->nodeId)
		{
			/*
			 * If this is the node we want to update and it is updated succesfully,
			 * then we can safely update the flag on the coordinator as well.
			 */
			SetWorkerColumnLocalOnly(workerNode, columnIndex, value);
		}
	}

	return FindWorkerNode(workerNode->workerName, workerNode->workerPort);
}


/*
 * SetWorkerColumnLocalOnly function sets the column with the specified index
 * (see pg_dist_node.h) on the worker in pg_dist_node.
 * It returns the new worker node after the modification.
 */
WorkerNode *
SetWorkerColumnLocalOnly(WorkerNode *workerNode, int columnIndex, Datum value)
{
	Relation pgDistNode = table_open(DistNodeRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);
	HeapTuple heapTuple = GetNodeTuple(workerNode->workerName, workerNode->workerPort);

	Datum values[Natts_pg_dist_node];
	bool isnull[Natts_pg_dist_node];
	bool replace[Natts_pg_dist_node];

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

	WorkerNode *newWorkerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);

	table_close(pgDistNode, NoLock);

	return newWorkerNode;
}


/*
 * GetMetadataSyncCommandToSetNodeColumn checks if the given workerNode and value is
 * valid or not. Then it returns the necessary metadata sync command as a string.
 */
static char *
GetMetadataSyncCommandToSetNodeColumn(WorkerNode *workerNode, int columnIndex, Datum
									  value)
{
	char *metadataSyncCommand = NULL;

	switch (columnIndex)
	{
		case Anum_pg_dist_node_hasmetadata:
		{
			ErrorIfCoordinatorMetadataSetFalse(workerNode, value, "hasmetadata");
			metadataSyncCommand = NodeHasmetadataUpdateCommand(workerNode->nodeId,
															   DatumGetBool(value));
			break;
		}

		case Anum_pg_dist_node_isactive:
		{
			ErrorIfCoordinatorMetadataSetFalse(workerNode, value, "isactive");

			metadataSyncCommand = NodeStateUpdateCommand(workerNode->nodeId,
														 DatumGetBool(value));
			break;
		}

		case Anum_pg_dist_node_shouldhaveshards:
		{
			metadataSyncCommand = ShouldHaveShardsUpdateCommand(workerNode->nodeId,
																DatumGetBool(value));
			break;
		}

		case Anum_pg_dist_node_metadatasynced:
		{
			ErrorIfCoordinatorMetadataSetFalse(workerNode, value, "metadatasynced");
			metadataSyncCommand = NodeMetadataSyncedUpdateCommand(workerNode->nodeId,
																  DatumGetBool(value));
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
								   workerNode->workerName, workerNode->workerPort)));
		}
	}

	return metadataSyncCommand;
}


/*
 * NodeHasmetadataUpdateCommand generates and returns a SQL UPDATE command
 * that updates the hasmetada column of pg_dist_node, for the given nodeid.
 */
static char *
NodeHasmetadataUpdateCommand(uint32 nodeId, bool hasMetadata)
{
	StringInfo updateCommand = makeStringInfo();
	char *hasMetadataString = hasMetadata ? "TRUE" : "FALSE";
	appendStringInfo(updateCommand,
					 "UPDATE pg_dist_node SET hasmetadata = %s "
					 "WHERE nodeid = %u",
					 hasMetadataString, nodeId);
	return updateCommand->data;
}


/*
 * NodeMetadataSyncedUpdateCommand generates and returns a SQL UPDATE command
 * that updates the metadataSynced column of pg_dist_node, for the given nodeid.
 */
static char *
NodeMetadataSyncedUpdateCommand(uint32 nodeId, bool metadataSynced)
{
	StringInfo updateCommand = makeStringInfo();
	char *hasMetadataString = metadataSynced ? "TRUE" : "FALSE";
	appendStringInfo(updateCommand,
					 "UPDATE pg_dist_node SET metadatasynced = %s "
					 "WHERE nodeid = %u",
					 hasMetadataString, nodeId);
	return updateCommand->data;
}


/*
 * ErrorIfCoordinatorMetadataSetFalse throws an error if the input node
 * is the coordinator and the value is false.
 */
static void
ErrorIfCoordinatorMetadataSetFalse(WorkerNode *workerNode, Datum value, char *field)
{
	bool valueBool = DatumGetBool(value);
	if (!valueBool && workerNode->groupId == COORDINATOR_GROUP_ID)
	{
		ereport(ERROR, (errmsg("cannot change \"%s\" field of the "
							   "coordinator node", field)));
	}
}


/*
 * SetShouldHaveShards function sets the shouldhaveshards column of the
 * specified worker in pg_dist_node. also propagates this to other metadata nodes.
 * It returns the new worker node after the modification.
 */
static WorkerNode *
SetShouldHaveShards(WorkerNode *workerNode, bool shouldHaveShards)
{
	return SetWorkerColumn(workerNode, Anum_pg_dist_node_shouldhaveshards, BoolGetDatum(
							   shouldHaveShards));
}


/*
 * SetNodeState function sets the isactive column of the specified worker in
 * pg_dist_node to isActive. Also propagates this to other metadata nodes.
 * It returns the new worker node after the modification.
 */
static WorkerNode *
SetNodeState(char *nodeName, int nodePort, bool isActive)
{
	WorkerNode *workerNode = FindWorkerNodeAnyCluster(nodeName, nodePort);
	return SetWorkerColumn(workerNode, Anum_pg_dist_node_isactive, BoolGetDatum(
							   isActive));
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
	Relation pgDistNode = table_open(DistNodeRelationId(), AccessShareLock);
	const int scanKeyCount = 2;
	const bool indexOK = false;

	ScanKeyData scanKey[2];
	HeapTuple nodeTuple = NULL;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodePort));
	SysScanDesc scanDescriptor = systable_beginscan(pgDistNode, InvalidOid, indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		nodeTuple = heap_copytuple(heapTuple);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistNode, NoLock);

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

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	Datum groupIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	int32 groupId = DatumGetInt32(groupIdDatum);

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

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	Datum nextNodeIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	int nextNodeId = DatumGetUInt32(nextNodeIdDatum);

	return nextNodeId;
}


/*
 * EnsureCoordinator checks if the current node is the coordinator. If it does not,
 * the function errors out.
 */
void
EnsureCoordinator(void)
{
	int32 localGroupId = GetLocalGroupId();

	if (localGroupId != 0)
	{
		ereport(ERROR, (errmsg("operation is not allowed on this node"),
						errhint("Connect to the coordinator and run it again.")));
	}
}


/*
 * EnsureCoordinatorIsInMetadata checks whether the coordinator is added to the
 * metadata, which is required for many operations.
 */
void
EnsureCoordinatorIsInMetadata(void)
{
	bool isCoordinatorInMetadata = false;
	PrimaryNodeForGroup(COORDINATOR_GROUP_ID, &isCoordinatorInMetadata);
	if (!isCoordinatorInMetadata)
	{
		ereport(ERROR, (errmsg("coordinator is not added to the metadata"),
						errhint("Use SELECT citus_set_coordinator_host('<hostname>') "
								"to configure the coordinator hostname")));
	}
}


/*
 * InsertCoordinatorIfClusterEmpty can be used to ensure Citus tables can be
 * created even on a node that has just performed CREATE EXTENSION citus;
 */
void
InsertCoordinatorIfClusterEmpty(void)
{
	/* prevent concurrent node additions */
	Relation pgDistNode = table_open(DistNodeRelationId(), RowShareLock);

	if (!HasAnyNodes())
	{
		/*
		 * create_distributed_table being called for the first time and there are
		 * no pg_dist_node records. Add a record for the coordinator.
		 */
		InsertPlaceholderCoordinatorRecord();
	}

	/*
	 * We release the lock, if InsertPlaceholderCoordinatorRecord was called
	 * we already have a strong (RowExclusive) lock.
	 */
	table_close(pgDistNode, RowShareLock);
}


/*
 * InsertPlaceholderCoordinatorRecord inserts a placeholder record for the coordinator
 * to be able to create distributed tables on a single node.
 */
static void
InsertPlaceholderCoordinatorRecord(void)
{
	NodeMetadata nodeMetadata = DefaultNodeMetadata();
	nodeMetadata.groupId = 0;
	nodeMetadata.shouldHaveShards = true;
	nodeMetadata.nodeRole = PrimaryNodeRoleId();
	nodeMetadata.nodeCluster = "default";

	bool nodeAlreadyExists = false;

	/* as long as there is a single node, localhost should be ok */
	AddNodeMetadata(LocalHostName, PostPortNumber, &nodeMetadata, &nodeAlreadyExists);
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

	Relation pgDistNode = table_open(DistNodeRelationId(), RowExclusiveLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistNode, heapTuple);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();

	/* close relation */
	table_close(pgDistNode, NoLock);
}


/*
 * DeleteNodeRow removes the requested row from pg_dist_node table if it exists.
 */
static void
DeleteNodeRow(char *nodeName, int32 nodePort)
{
	const int scanKeyCount = 2;
	bool indexOK = false;

	ScanKeyData scanKey[2];
	Relation pgDistNode = table_open(DistNodeRelationId(), RowExclusiveLock);

	/*
	 * simple_heap_delete() expects that the caller has at least an
	 * AccessShareLock on primary key index.
	 *
	 * XXX: This does not seem required, do we really need to acquire this lock?
	 * Postgres doesn't acquire such locks on indexes before deleting catalog tuples.
	 * Linking here the reasons we added this lock acquirement:
	 * https://github.com/citusdata/citus/pull/2851#discussion_r306569462
	 * https://github.com/citusdata/citus/pull/2855#discussion_r313628554
	 * https://github.com/citusdata/citus/issues/1890
	 */
	Relation replicaIndex = index_open(RelationGetPrimaryKeyIndex(pgDistNode),
									   AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodePort));

	SysScanDesc heapScan = systable_beginscan(pgDistNode, InvalidOid, indexOK,
											  NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(heapScan);

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

	table_close(replicaIndex, AccessShareLock);
	table_close(pgDistNode, NoLock);
}


/*
 * TupleToWorkerNode takes in a heap tuple from pg_dist_node, and
 * converts this tuple to an equivalent struct in memory. The function assumes
 * the caller already has locks on the tuple, and doesn't perform any locking.
 */
static WorkerNode *
TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	Datum datumArray[Natts_pg_dist_node];
	bool isNullArray[Natts_pg_dist_node];

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

	char *nodeName = DatumGetCString(datumArray[Anum_pg_dist_node_nodename - 1]);
	char *nodeRack = DatumGetCString(datumArray[Anum_pg_dist_node_noderack - 1]);

	WorkerNode *workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
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

	getTypeInputInfo(dataType, &typIoFunc, &typIoParam);
	getBaseTypeAndTypmod(dataType, &typeModifier);

	Datum datum = OidInputFunctionCall(typIoFunc, inputString, typIoParam, typeModifier);

	return datum;
}


/*
 * DatumToString returns the string representation of the given datum.
 */
char *
DatumToString(Datum datum, Oid dataType)
{
	Oid typIoFunc = InvalidOid;
	bool typIsVarlena = false;

	getTypeOutputInfo(dataType, &typIoFunc, &typIsVarlena);
	char *outputString = OidOutputFunctionCall(typIoFunc, datum);

	return outputString;
}


/*
 * UnsetMetadataSyncedForAllWorkers sets the metadatasynced column of all metadata
 * worker nodes to false. It returns true if it updated at least a node.
 */
static bool
UnsetMetadataSyncedForAllWorkers(void)
{
	bool updatedAtLeastOne = false;
	ScanKeyData scanKey[3];
	int scanKeyCount = 3;
	bool indexOK = false;

	/*
	 * Concurrent citus_update_node() calls might iterate and try to update
	 * pg_dist_node in different orders. To protect against deadlock, we
	 * get an exclusive lock here.
	 */
	Relation relation = table_open(DistNodeRelationId(), ExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_hasmetadata,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_metadatasynced,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));

	/* coordinator always has the up to date metadata */
	ScanKeyInit(&scanKey[2], Anum_pg_dist_node_groupid,
				BTGreaterStrategyNumber, F_INT4GT,
				Int32GetDatum(COORDINATOR_GROUP_ID));

	CatalogIndexState indstate = CatalogOpenIndexes(relation);

	SysScanDesc scanDescriptor = systable_beginscan(relation,
													InvalidOid, indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		updatedAtLeastOne = true;
	}

	while (HeapTupleIsValid(heapTuple))
	{
		Datum values[Natts_pg_dist_node];
		bool isnull[Natts_pg_dist_node];
		bool replace[Natts_pg_dist_node];

		memset(replace, false, sizeof(replace));
		memset(isnull, false, sizeof(isnull));
		memset(values, 0, sizeof(values));

		values[Anum_pg_dist_node_metadatasynced - 1] = BoolGetDatum(false);
		replace[Anum_pg_dist_node_metadatasynced - 1] = true;

		HeapTuple newHeapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values,
												   isnull,
												   replace);

		CatalogTupleUpdateWithInfo(relation, &newHeapTuple->t_self, newHeapTuple,
								   indstate);

		CommandCounterIncrement();

		heap_freetuple(newHeapTuple);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	CatalogCloseIndexes(indstate);
	table_close(relation, NoLock);

	return updatedAtLeastOne;
}
