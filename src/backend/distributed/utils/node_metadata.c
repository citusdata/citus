/*
 * node_metadata.c
 *	  Functions that operate on pg_dist_node
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 */
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"


#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#if (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600)
#include "access/stratnum.h"
#else
#include "access/skey.h"
#endif
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "commands/sequence.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/master_protocol.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_node.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"
#include "storage/lock.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"


/* default group size */
int GroupSize = 1;


/* local function forward declarations */
static Datum ActivateNode(char *nodeName, int nodePort);
static void RemoveNodeFromCluster(char *nodeName, int32 nodePort);
static Datum AddNodeMetadata(char *nodeName, int32 nodePort, int32 groupId,
							 char *nodeRack, bool hasMetadata, bool isActive,
							 bool *nodeAlreadyExists);
static void SetNodeState(char *nodeName, int32 nodePort, bool isActive);
static HeapTuple GetNodeTuple(char *nodeName, int32 nodePort);
static Datum GenerateNodeTuple(WorkerNode *workerNode);
static int32 GetNextGroupId(void);
static uint32 GetMaxGroupId(void);
static int GetNextNodeId(void);
static void InsertNodeRow(int nodeid, char *nodename, int32 nodeport, uint32 groupId,
						  char *nodeRack, bool hasMetadata, bool isActive);
static void DeleteNodeRow(char *nodename, int32 nodeport);
static List * ParseWorkerNodeFileAndRename(void);
static WorkerNode * TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_add_node);
PG_FUNCTION_INFO_V1(master_add_inactive_node);
PG_FUNCTION_INFO_V1(master_remove_node);
PG_FUNCTION_INFO_V1(master_disable_node);
PG_FUNCTION_INFO_V1(master_activate_node);
PG_FUNCTION_INFO_V1(master_initialize_node_metadata);
PG_FUNCTION_INFO_V1(get_shard_id_for_distribution_column);


/*
 * master_add_node function adds a new node to the cluster and returns its data. It also
 * replicates all reference tables to the new node.
 */
Datum
master_add_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	int32 groupId = 0;
	char *nodeRack = WORKER_DEFAULT_RACK;
	bool hasMetadata = false;
	bool isActive = false;
	bool nodeAlreadyExists = false;
	Datum nodeRecord;

	CheckCitusVersion(ERROR);

	nodeRecord = AddNodeMetadata(nodeNameString, nodePort, groupId, nodeRack,
								 hasMetadata, isActive, &nodeAlreadyExists);

	/*
	 * After adding new node, if the node did not already exist, we will activate
	 * the node. This means we will replicate all reference tables to the new
	 * node.
	 */
	if (!nodeAlreadyExists)
	{
		nodeRecord = ActivateNode(nodeNameString, nodePort);
	}

	PG_RETURN_CSTRING(nodeRecord);
}


/*
 * master_add_inactive_node function adds a new node to the cluster as inactive node
 * and returns information about newly added node. It does not replicate reference
 * tables to the new node, it only adds new node to the pg_dist_node table.
 */
Datum
master_add_inactive_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	int32 groupId = 0;
	char *nodeRack = WORKER_DEFAULT_RACK;
	bool hasMetadata = false;
	bool isActive = false;
	bool nodeAlreadyExists = false;
	Datum nodeRecord;

	CheckCitusVersion(ERROR);

	nodeRecord = AddNodeMetadata(nodeNameString, nodePort, groupId, nodeRack,
								 hasMetadata, isActive, &nodeAlreadyExists);

	PG_RETURN_CSTRING(nodeRecord);
}


/*
 * master_remove_node function removes the provided node from the pg_dist_node table of
 * the master node and all nodes with metadata.
 * The call to the master_remove_node should be done by the super user and the specified
 * node should not have any active placements.
 * This function also deletes all reference table placements belong to the given node from
 * pg_dist_shard_placement, but it does not drop actual placement at the node. In the case
 * of re-adding the node, master_add_node first drops and re-creates the reference tables.
 */
Datum
master_remove_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);

	CheckCitusVersion(ERROR);

	RemoveNodeFromCluster(nodeNameString, nodePort);

	PG_RETURN_VOID();
}


/*
 * master_disable_node function sets isactive value of the provided node as inactive
 * at master node and all nodes with metadata regardless of the node having an
 * active shard placement.
 * The call to the master_disable_node must be done by the super user.
 * This function also deletes all reference table placements belong to the given
 * node from pg_dist_shard_placement, but it does not drop actual placement at
 * the node. In the case of re-activating the node, master_add_node first drops
 * and re-creates the reference tables.
 */
Datum
master_disable_node(PG_FUNCTION_ARGS)
{
	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	char *nodeName = text_to_cstring(nodeNameText);
	bool hasShardPlacements = false;
	bool isActive = false;

	CheckCitusVersion(ERROR);

	DeleteAllReferenceTablePlacementsFromNode(nodeName, nodePort);

	hasShardPlacements = NodeHasActiveShardPlacements(nodeName, nodePort);
	if (hasShardPlacements)
	{
		ereport(NOTICE, (errmsg("Node %s:%d has active shard placements. Some queries "
								"may fail after this operation. Use "
								"SELECT master_activate_node('%s', %d) to activate this "
								"node back.",
								nodeName, nodePort, nodeName, nodePort)));
	}

	SetNodeState(nodeName, nodePort, isActive);

	PG_RETURN_VOID();
}


/*
 * master_activate_node UDF activates the given node. It sets the node's isactive
 * value to active and replicates all reference tables to that node.
 */
Datum
master_activate_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	char *nodeNameString = text_to_cstring(nodeName);
	Datum nodeRecord = 0;

	CheckCitusVersion(ERROR);

	nodeRecord = ActivateNode(nodeNameString, nodePort);

	PG_RETURN_CSTRING(nodeRecord);
}


/*
 * ActivateNode activates the node with nodeName and nodePort. Currently, activation
 * includes only replicating the reference tables and setting isactive column of the
 * given node.
 */
static Datum
ActivateNode(char *nodeName, int nodePort)
{
	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);
	HeapTuple heapTuple = GetNodeTuple(nodeName, nodePort);
	CommandId commandId = GetCurrentCommandId(true);
	LockTupleMode lockTupleMode = LockTupleExclusive;
	LockWaitPolicy lockWaitPolicy = LockWaitError;
	bool followUpdates = false;
	Buffer buffer = 0;
	HeapUpdateFailureData heapUpdateFailureData;

	WorkerNode *workerNode = NULL;
	bool isActive = true;
	Datum nodeRecord = 0;

	heap_lock_tuple(pgDistNode, heapTuple, commandId, lockTupleMode, lockWaitPolicy,
					followUpdates, &buffer, &heapUpdateFailureData);
	ReleaseBuffer(buffer);

	SetNodeState(nodeName, nodePort, isActive);

	ReplicateAllReferenceTablesToNode(nodeName, nodePort);

	workerNode = FindWorkerNode(nodeName, nodePort);
	nodeRecord = GenerateNodeTuple(workerNode);

	heap_close(pgDistNode, AccessShareLock);

	return nodeRecord;
}


/*
 * master_initialize_node_metadata is run once, when upgrading citus. It ingests the
 * existing pg_worker_list.conf into pg_dist_node, then adds a header to the file stating
 * that it's no longer used.
 */
Datum
master_initialize_node_metadata(PG_FUNCTION_ARGS)
{
	ListCell *workerNodeCell = NULL;
	List *workerNodes = NULL;
	bool nodeAlreadyExists = false;

	CheckCitusVersion(ERROR);

	workerNodes = ParseWorkerNodeFileAndRename();
	foreach(workerNodeCell, workerNodes)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		AddNodeMetadata(workerNode->workerName, workerNode->workerPort, 0,
						workerNode->workerRack, false, workerNode->isActive,
						&nodeAlreadyExists);
	}

	PG_RETURN_BOOL(true);
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
			PG_RETURN_INT64(NULL);
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

		distributionColumn = PartitionKey(relationId);
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

	PG_RETURN_INT64(NULL);
}


/*
 * FindWorkerNode searches over the worker nodes and returns the workerNode
 * if it already exists. Else, the function returns NULL.
 */
WorkerNode *
FindWorkerNode(char *nodeName, int32 nodePort)
{
	WorkerNode *workerNode = NULL;
	HTAB *workerNodeHash = GetWorkerNodeHash();
	bool handleFound = false;
	void *hashKey = NULL;

	WorkerNode *searchedNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	strlcpy(searchedNode->workerName, nodeName, WORKER_LENGTH);
	searchedNode->workerPort = nodePort;

	hashKey = (void *) searchedNode;
	workerNode = (WorkerNode *) hash_search(workerNodeHash, hashKey,
											HASH_FIND, &handleFound);

	return workerNode;
}


/*
 * ReadWorkerNodes iterates over pg_dist_node table, converts each row
 * into it's memory representation (i.e., WorkerNode) and adds them into
 * a list. Lastly, the list is returned to the caller.
 */
List *
ReadWorkerNodes()
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	List *workerNodeList = NIL;
	TupleDesc tupleDescriptor = NULL;

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessExclusiveLock);

	scanDescriptor = systable_beginscan(pgDistNode,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(pgDistNode);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		WorkerNode *workerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);
		workerNodeList = lappend(workerNodeList, workerNode);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, AccessExclusiveLock);

	return workerNodeList;
}


/*
 * RemoveNodeFromCluster removes the provided node from the pg_dist_node table of
 * the master node and all nodes with metadata.
 * The call to the master_remove_node should be done by the super user. If there are
 * active shard placements on the node; the function errors out.
 * This function also deletes all reference table placements belong to the given node from
 * pg_dist_shard_placement, but it does not drop actual placement at the node. It also
 * modifies replication factor of the colocation group of reference tables, so that
 * replication factor will be equal to worker count.
 */
static void
RemoveNodeFromCluster(char *nodeName, int32 nodePort)
{
	char *nodeDeleteCommand = NULL;
	bool hasShardPlacements = false;
	WorkerNode *workerNode = NULL;
	List *referenceTableList = NIL;
	uint32 deletedNodeId = INVALID_PLACEMENT_ID;

	EnsureCoordinator();
	EnsureSuperUser();

	workerNode = FindWorkerNode(nodeName, nodePort);

	if (workerNode != NULL)
	{
		deletedNodeId = workerNode->nodeId;
	}

	DeleteNodeRow(nodeName, nodePort);

	DeleteAllReferenceTablePlacementsFromNode(nodeName, nodePort);

	/*
	 * After deleting reference tables placements, we will update replication factor
	 * column for colocation group of reference tables so that replication factor will
	 * be equal to worker count.
	 */
	referenceTableList = ReferenceTableOidList();
	if (list_length(referenceTableList) != 0)
	{
		Oid firstReferenceTableId = linitial_oid(referenceTableList);
		uint32 referenceTableColocationId = TableColocationId(firstReferenceTableId);

		List *workerNodeList = ActiveWorkerNodeList();
		int workerCount = list_length(workerNodeList);

		UpdateColocationGroupReplicationFactor(referenceTableColocationId, workerCount);
	}

	hasShardPlacements = NodeHasActiveShardPlacements(nodeName, nodePort);
	if (hasShardPlacements)
	{
		ereport(ERROR, (errmsg("you cannot remove a node which has active "
							   "shard placements")));
	}

	nodeDeleteCommand = NodeDeleteCommand(deletedNodeId);

	/* make sure we don't have any lingering session lifespan connections */
	CloseNodeConnectionsAfterTransaction(nodeName, nodePort);

	SendCommandToWorkers(WORKERS_WITH_METADATA, nodeDeleteCommand);
}


/*
 * AddNodeMetadata checks the given node information and adds the specified node to the
 * pg_dist_node table of the master and workers with metadata.
 * If the node already exists, the function returns the information about the node.
 * If not, the following prodecure is followed while adding a node: If the groupId is not
 * explicitly given by the user, the function picks the group that the new node should
 * be in with respect to GroupSize. Then, the new node is inserted into the local
 * pg_dist_node as well as the nodes with hasmetadata=true.
 */
static Datum
AddNodeMetadata(char *nodeName, int32 nodePort, int32 groupId, char *nodeRack,
				bool hasMetadata, bool isActive, bool *nodeAlreadyExists)
{
	Relation pgDistNode = NULL;
	int nextNodeIdInt = 0;
	Datum returnData = 0;
	WorkerNode *workerNode = NULL;
	char *nodeDeleteCommand = NULL;
	char *nodeInsertCommand = NULL;
	List *workerNodeList = NIL;

	EnsureCoordinator();
	EnsureSuperUser();

	*nodeAlreadyExists = false;

	/* acquire a lock so that no one can do this concurrently */
	pgDistNode = heap_open(DistNodeRelationId(), AccessExclusiveLock);

	/* check if the node already exists in the cluster */
	workerNode = FindWorkerNode(nodeName, nodePort);
	if (workerNode != NULL)
	{
		/* fill return data and return */
		returnData = GenerateNodeTuple(workerNode);

		/* close the heap */
		heap_close(pgDistNode, AccessExclusiveLock);

		*nodeAlreadyExists = true;

		PG_RETURN_DATUM(returnData);
	}

	/* user lets Citus to decide on the group that the newly added node should be in */
	if (groupId == 0)
	{
		groupId = GetNextGroupId();
	}
	else
	{
		uint maxGroupId = GetMaxGroupId();

		if (groupId > maxGroupId)
		{
			ereport(ERROR, (errmsg("you cannot add a node to a non-existing group")));
		}
	}

	/* generate the new node id from the sequence */
	nextNodeIdInt = GetNextNodeId();

	InsertNodeRow(nextNodeIdInt, nodeName, nodePort, groupId, nodeRack, hasMetadata,
				  isActive);

	workerNode = FindWorkerNode(nodeName, nodePort);

	/* send the delete command all nodes with metadata */
	nodeDeleteCommand = NodeDeleteCommand(workerNode->nodeId);
	SendCommandToWorkers(WORKERS_WITH_METADATA, nodeDeleteCommand);

	/* finally prepare the insert command and send it to all primary nodes */
	workerNodeList = list_make1(workerNode);
	nodeInsertCommand = NodeListInsertCommand(workerNodeList);
	SendCommandToWorkers(WORKERS_WITH_METADATA, nodeInsertCommand);

	heap_close(pgDistNode, AccessExclusiveLock);

	/* fetch the worker node, and generate the output */
	workerNode = FindWorkerNode(nodeName, nodePort);
	returnData = GenerateNodeTuple(workerNode);

	return returnData;
}


/*
 * SetNodeState function sets the isactive column of the specified worker in
 * pg_dist_node to true.
 */
static void
SetNodeState(char *nodeName, int32 nodePort, bool isActive)
{
	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNode);
	HeapTuple heapTuple = GetNodeTuple(nodeName, nodePort);

	Datum values[Natts_pg_dist_node];
	bool isnull[Natts_pg_dist_node];
	bool replace[Natts_pg_dist_node];

	char *nodeStateUpdateCommand = NULL;
	WorkerNode *workerNode = NULL;

	memset(replace, 0, sizeof(replace));
	values[Anum_pg_dist_node_isactive - 1] = BoolGetDatum(isActive);
	isnull[Anum_pg_dist_node_isactive - 1] = false;
	replace[Anum_pg_dist_node_isactive - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);
	simple_heap_update(pgDistNode, &heapTuple->t_self, heapTuple);

	CatalogUpdateIndexes(pgDistNode, heapTuple);
	CitusInvalidateRelcacheByRelid(DistNodeRelationId());
	CommandCounterIncrement();

	heap_close(pgDistNode, AccessShareLock);

	/* we also update isactive column at worker nodes */
	workerNode = FindWorkerNode(nodeName, nodePort);
	nodeStateUpdateCommand = NodeStateUpdateCommand(workerNode->nodeId, isActive);
	SendCommandToWorkers(WORKERS_WITH_METADATA, nodeStateUpdateCommand);
}


/*
 * GetNodeTuple function returns heap tuple of given nodeName and nodePort. If
 * there are no node tuple with specified nodeName and nodePort, this function
 * errors out.
 */
static HeapTuple
GetNodeTuple(char *nodeName, int32 nodePort)
{
	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);
	const int scanKeyCount = 2;
	const bool indexOK = false;

	ScanKeyData scanKey[scanKeyCount];
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	HeapTuple nodeTuple = NULL;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT8EQ, Int32GetDatum(nodePort));
	scanDescriptor = systable_beginscan(pgDistNode, InvalidOid, indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
							   nodeName, nodePort)));
	}

	nodeTuple = heap_copytuple(heapTuple);

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, AccessShareLock);

	return nodeTuple;
}


/*
 * GenerateNodeTuple gets a worker node and return a heap tuple of
 * given worker node.
 */
static Datum
GenerateNodeTuple(WorkerNode *workerNode)
{
	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum nodeDatum = 0;
	Datum values[Natts_pg_dist_node];
	bool isNulls[Natts_pg_dist_node];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_node_nodeid - 1] = UInt32GetDatum(workerNode->nodeId);
	values[Anum_pg_dist_node_groupid - 1] = UInt32GetDatum(workerNode->groupId);
	values[Anum_pg_dist_node_nodename - 1] = CStringGetTextDatum(workerNode->workerName);
	values[Anum_pg_dist_node_nodeport - 1] = UInt32GetDatum(workerNode->workerPort);
	values[Anum_pg_dist_node_noderack - 1] = CStringGetTextDatum(workerNode->workerRack);
	values[Anum_pg_dist_node_hasmetadata - 1] = BoolGetDatum(workerNode->hasMetadata);
	values[Anum_pg_dist_node_isactive - 1] = BoolGetDatum(workerNode->isActive);

	/* open shard relation and insert new tuple */
	pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);

	/* generate the tuple */
	tupleDescriptor = RelationGetDescr(pgDistNode);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);
	nodeDatum = HeapTupleGetDatum(heapTuple);

	/* close the relation */
	heap_close(pgDistNode, AccessShareLock);

	return nodeDatum;
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
	Oid sequenceId = ResolveRelationId(sequenceName);
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

	groupId = DatumGetUInt32(groupIdDatum);

	return groupId;
}


/*
 * GetMaxGroupId iterates over the worker node hash, and returns the maximum
 * group id from the table.
 */
static uint32
GetMaxGroupId()
{
	uint32 maxGroupId = 0;
	WorkerNode *workerNode = NULL;
	HTAB *workerNodeHash = GetWorkerNodeHash();
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		uint32 workerNodeGroupId = workerNode->groupId;

		if (workerNodeGroupId > maxGroupId)
		{
			maxGroupId = workerNodeGroupId;
		}
	}

	return maxGroupId;
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
	Oid sequenceId = ResolveRelationId(sequenceName);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum nextNodedIdDatum = 0;
	int nextNodeId = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	nextNodedIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	PG_RETURN_DATUM(nextNodedIdDatum);

	nextNodeId = DatumGetUInt32(nextNodeId);

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
 * InsertNodedRow opens the node system catalog, and inserts a new row with the
 * given values into that system catalog.
 */
static void
InsertNodeRow(int nodeid, char *nodeName, int32 nodePort, uint32 groupId, char *nodeRack,
			  bool hasMetadata, bool isActive)
{
	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_node];
	bool isNulls[Natts_pg_dist_node];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_node_nodeid - 1] = UInt32GetDatum(nodeid);
	values[Anum_pg_dist_node_groupid - 1] = UInt32GetDatum(groupId);
	values[Anum_pg_dist_node_nodename - 1] = CStringGetTextDatum(nodeName);
	values[Anum_pg_dist_node_nodeport - 1] = UInt32GetDatum(nodePort);
	values[Anum_pg_dist_node_noderack - 1] = CStringGetTextDatum(nodeRack);
	values[Anum_pg_dist_node_hasmetadata - 1] = BoolGetDatum(hasMetadata);
	values[Anum_pg_dist_node_isactive - 1] = BoolGetDatum(isActive);

	/* open shard relation and insert new tuple */
	pgDistNode = heap_open(DistNodeRelationId(), AccessExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistNode);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistNode, heapTuple);
	CatalogUpdateIndexes(pgDistNode, heapTuple);

	/* close relation and invalidate previous cache entry */
	heap_close(pgDistNode, AccessExclusiveLock);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();
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
	ScanKeyData scanKey[scanKeyCount];

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT8EQ, Int32GetDatum(nodePort));

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
	heap_close(pgDistNode, AccessExclusiveLock);

	/* ensure future commands don't use the node we just removed */
	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command won't see the row */
	CommandCounterIncrement();
}


/*
 * ParseWorkerNodeFileAndRename opens and parses the node name and node port from the
 * specified configuration file and after that, renames it marking it is not used anymore.
 * Note that this function is deprecated. Do not use this function for any new
 * features.
 */
static List *
ParseWorkerNodeFileAndRename()
{
	FILE *workerFileStream = NULL;
	List *workerNodeList = NIL;
	char workerNodeLine[MAXPGPATH];
	char *workerFilePath = make_absolute_path(WorkerListFileName);
	StringInfo renamedWorkerFilePath = makeStringInfo();
	char *workerPatternTemplate = "%%%u[^# \t]%%*[ \t]%%%u[^# \t]%%*[ \t]%%%u[^# \t]";
	char workerLinePattern[1024];
	const int workerNameIndex = 0;
	const int workerPortIndex = 1;

	memset(workerLinePattern, '\0', sizeof(workerLinePattern));

	workerFileStream = AllocateFile(workerFilePath, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		if (errno == ENOENT)
		{
			ereport(DEBUG1, (errmsg("worker list file located at \"%s\" is not present",
									workerFilePath)));
		}
		else
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open worker list file \"%s\": %m",
								   workerFilePath)));
		}
		return NIL;
	}

	/* build pattern to contain node name length limit */
	snprintf(workerLinePattern, sizeof(workerLinePattern), workerPatternTemplate,
			 WORKER_LENGTH, MAX_PORT_LENGTH, WORKER_LENGTH);

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream) != NULL)
	{
		const int workerLineLength = strnlen(workerNodeLine, MAXPGPATH);
		WorkerNode *workerNode = NULL;
		char *linePointer = NULL;
		int32 nodePort = 5432; /* default port number */
		int fieldCount = 0;
		bool lineIsInvalid = false;
		char nodeName[WORKER_LENGTH + 1];
		char nodeRack[WORKER_LENGTH + 1];
		char nodePortString[MAX_PORT_LENGTH + 1];

		memset(nodeName, '\0', sizeof(nodeName));
		strlcpy(nodeRack, WORKER_DEFAULT_RACK, sizeof(nodeRack));
		memset(nodePortString, '\0', sizeof(nodePortString));

		if (workerLineLength == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("worker node list file line exceeds the maximum "
								   "length of %d", MAXPGPATH)));
		}

		/* trim trailing newlines preserved by fgets, if any */
		linePointer = workerNodeLine + workerLineLength - 1;
		while (linePointer >= workerNodeLine &&
			   (*linePointer == '\n' || *linePointer == '\r'))
		{
			*linePointer-- = '\0';
		}

		/* skip leading whitespace */
		for (linePointer = workerNodeLine; *linePointer; linePointer++)
		{
			if (!isspace((unsigned char) *linePointer))
			{
				break;
			}
		}

		/* if the entire line is whitespace or a comment, skip it */
		if (*linePointer == '\0' || *linePointer == '#')
		{
			continue;
		}

		/* parse line; node name is required, but port and rack are optional */
		fieldCount = sscanf(linePointer, workerLinePattern,
							nodeName, nodePortString, nodeRack);

		/* adjust field count for zero based indexes */
		fieldCount--;

		/* raise error if no fields were assigned */
		if (fieldCount < workerNameIndex)
		{
			lineIsInvalid = true;
		}

		/* no special treatment for nodeName: already parsed by sscanf */

		/* if a second token was specified, convert to integer port */
		if (fieldCount >= workerPortIndex)
		{
			char *nodePortEnd = NULL;

			errno = 0;
			nodePort = strtol(nodePortString, &nodePortEnd, 10);

			if (errno != 0 || (*nodePortEnd) != '\0' || nodePort <= 0)
			{
				lineIsInvalid = true;
			}
		}

		if (lineIsInvalid)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("could not parse worker node line: %s",
								   workerNodeLine),
							errhint("Lines in the worker node file must contain a valid "
									"node name and, optionally, a positive port number. "
									"Comments begin with a '#' character and extend to "
									"the end of their line.")));
		}

		/* allocate worker node structure and set fields */
		workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));

		strlcpy(workerNode->workerName, nodeName, WORKER_LENGTH);
		strlcpy(workerNode->workerRack, nodeRack, WORKER_LENGTH);
		workerNode->workerPort = nodePort;
		workerNode->hasMetadata = false;
		workerNode->isActive = true;

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	/* rename the file, marking that it is not used anymore */
	appendStringInfo(renamedWorkerFilePath, "%s", workerFilePath);
	appendStringInfo(renamedWorkerFilePath, ".obsolete");
	rename(workerFilePath, renamedWorkerFilePath->data);

	FreeFile(workerFileStream);
	free(workerFilePath);

	return workerNodeList;
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
	bool isNull = false;

	Datum nodeId = heap_getattr(heapTuple, Anum_pg_dist_node_nodeid,
								tupleDescriptor, &isNull);
	Datum groupId = heap_getattr(heapTuple, Anum_pg_dist_node_groupid,
								 tupleDescriptor, &isNull);
	Datum nodeName = heap_getattr(heapTuple, Anum_pg_dist_node_nodename,
								  tupleDescriptor, &isNull);
	Datum nodePort = heap_getattr(heapTuple, Anum_pg_dist_node_nodeport,
								  tupleDescriptor, &isNull);
	Datum nodeRack = heap_getattr(heapTuple, Anum_pg_dist_node_noderack,
								  tupleDescriptor, &isNull);
	Datum hasMetadata = heap_getattr(heapTuple, Anum_pg_dist_node_hasmetadata,
									 tupleDescriptor, &isNull);
	Datum isActive = heap_getattr(heapTuple, Anum_pg_dist_node_isactive,
								  tupleDescriptor, &isNull);

	Assert(!HeapTupleHasNulls(heapTuple));

	workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	workerNode->nodeId = DatumGetUInt32(nodeId);
	workerNode->workerPort = DatumGetUInt32(nodePort);
	workerNode->groupId = DatumGetUInt32(groupId);
	strlcpy(workerNode->workerName, TextDatumGetCString(nodeName), WORKER_LENGTH);
	strlcpy(workerNode->workerRack, TextDatumGetCString(nodeRack), WORKER_LENGTH);
	workerNode->hasMetadata = DatumGetBool(hasMetadata);
	workerNode->isActive = DatumGetBool(isActive);

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
