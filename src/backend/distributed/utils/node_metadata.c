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
#include "distributed/connection_management.h"
#include "distributed/master_protocol.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_node.h"
#include "distributed/reference_table_utils.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "storage/lock.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relcache.h"


/* default group size */
int GroupSize = 1;


/* local function forward declarations */
static void RemoveNodeFromCluster(char *nodeName, int32 nodePort, bool forceRemove);
static Datum AddNodeMetadata(char *nodeName, int32 nodePort, int32 groupId,
							 char *nodeRack, bool hasMetadata, bool *nodeAlreadyExists);
static Datum GenerateNodeTuple(WorkerNode *workerNode);
static int32 GetNextGroupId(void);
static uint32 GetMaxGroupId(void);
static int GetNextNodeId(void);
static void InsertNodeRow(int nodeid, char *nodename, int32 nodeport, uint32 groupId,
						  char *nodeRack, bool hasMetadata);
static void DeleteNodeRow(char *nodename, int32 nodeport);
static List * ParseWorkerNodeFileAndRename(void);
static WorkerNode * TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_add_node);
PG_FUNCTION_INFO_V1(master_remove_node);
PG_FUNCTION_INFO_V1(master_disable_node);
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
	bool nodeAlreadyExists = false;

	Datum returnData = AddNodeMetadata(nodeNameString, nodePort, groupId, nodeRack,
									   hasMetadata, &nodeAlreadyExists);

	/*
	 * After adding new node, if the node is not already exist, we  replicate all existing
	 * reference tables to the new node. ReplicateAllReferenceTablesToAllNodes replicates
	 * reference tables to all nodes however, it skips nodes which already has healthy
	 * placement of particular reference table.
	 */
	if (!nodeAlreadyExists)
	{
		ReplicateAllReferenceTablesToAllNodes();
	}

	PG_RETURN_CSTRING(returnData);
}


/*
 * master_remove_node function removes the provided node from the pg_dist_node table of
 * the master node and all nodes with metadata.
 * The call to the master_remove_node should be done by the super user and the specified
 * node should not have any active placements.
 */
Datum
master_remove_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	bool forceRemove = false;
	RemoveNodeFromCluster(nodeNameString, nodePort, forceRemove);

	PG_RETURN_VOID();
}


/*
 * master_disable_node function removes the provided node from the pg_dist_node table of
 * the master node and all nodes with metadata regardless of the node having an active
 * shard placement.
 * The call to the master_remove_node should be done by the super user.
 */
Datum
master_disable_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	char *nodeNameString = text_to_cstring(nodeName);
	bool forceRemove = true;
	RemoveNodeFromCluster(nodeNameString, nodePort, forceRemove);

	PG_RETURN_VOID();
}


/*
 * master_initialize_node_metadata is run once, when upgrading citus. It injests the
 * existing pg_worker_list.conf into pg_dist_node, then adds a header to the file stating
 * that it's no longer used.
 */
Datum
master_initialize_node_metadata(PG_FUNCTION_ARGS)
{
	ListCell *workerNodeCell = NULL;
	List *workerNodes = ParseWorkerNodeFileAndRename();
	bool nodeAlreadyExists = false;

	foreach(workerNodeCell, workerNodes)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		AddNodeMetadata(workerNode->workerName, workerNode->workerPort, 0,
						workerNode->workerRack, false, &nodeAlreadyExists);
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
	Oid relationId = InvalidOid;
	Datum distributionValue = 0;

	Var *distributionColumn = NULL;
	char distributionMethod = 0;
	Oid expectedElementType = InvalidOid;
	Oid inputElementType = InvalidOid;
	DistTableCacheEntry *cacheEntry = NULL;
	int shardCount = 0;
	ShardInterval **shardIntervalArray = NULL;
	FmgrInfo *hashFunction = NULL;
	FmgrInfo *compareFunction = NULL;
	bool useBinarySearch = true;
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
		/* if given table is not reference table, distributionValue cannot be NULL */
		if (PG_ARGISNULL(1))
		{
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("distribution value cannot be NULL for tables other "
								   "than reference tables.")));
		}

		distributionValue = PG_GETARG_DATUM(1);

		distributionColumn = PartitionKey(relationId);
		expectedElementType = distributionColumn->vartype;
		inputElementType = get_fn_expr_argtype(fcinfo->flinfo, 1);
		if (expectedElementType != inputElementType)
		{
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("invalid distribution value type"),
							errdetail("Type of the value does not match the type of the "
									  "distribution column. Expected type id: %d, given "
									  "type id: %d", expectedElementType,
									  inputElementType)));
		}

		cacheEntry = DistributedTableCacheEntry(relationId);

		if (distributionMethod == DISTRIBUTE_BY_HASH &&
			cacheEntry->hasUniformHashDistribution)
		{
			useBinarySearch = false;
		}

		shardCount = cacheEntry->shardIntervalArrayLength;
		shardIntervalArray = cacheEntry->sortedShardIntervalArray;
		hashFunction = cacheEntry->hashFunction;
		compareFunction = cacheEntry->shardIntervalCompareFunction;
		shardInterval = FindShardInterval(distributionValue, shardIntervalArray,
										  shardCount, distributionMethod, compareFunction,
										  hashFunction, useBinarySearch);
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
 * active shard placements on the node; the function removes the node when forceRemove
 * flag is set, it errors out otherwise.
 */
static void
RemoveNodeFromCluster(char *nodeName, int32 nodePort, bool forceRemove)
{
	char *nodeDeleteCommand = NULL;
	bool hasShardPlacements = false;
	WorkerNode *workerNode = NULL;

	EnsureSchemaNode();
	EnsureSuperUser();

	hasShardPlacements = NodeHasActiveShardPlacements(nodeName, nodePort);
	if (hasShardPlacements)
	{
		if (forceRemove)
		{
			ereport(NOTICE, (errmsg("Node %s:%d has active shard placements. Some "
									"queries may fail after this operation. Use "
									"select master_add_node('%s', %d) to add this "
									"node back.",
									nodeName, nodePort, nodeName, nodePort)));
		}
		else
		{
			ereport(ERROR, (errmsg("you cannot remove a node which has active "
								   "shard placements"),
							errhint("Consider using master_disable_node.")));
		}
	}

	workerNode = FindWorkerNode(nodeName, nodePort);

	DeleteNodeRow(nodeName, nodePort);

	nodeDeleteCommand = NodeDeleteCommand(workerNode->nodeId);

	/* make sure we don't have any open connections */
	CloseNodeConnections(nodeName, nodePort);

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
				bool hasMetadata, bool *nodeAlreadyExists)
{
	Relation pgDistNode = NULL;
	int nextNodeIdInt = 0;
	Datum returnData = 0;
	WorkerNode *workerNode = NULL;
	char *nodeDeleteCommand = NULL;
	char *nodeInsertCommand = NULL;
	List *workerNodeList = NIL;

	EnsureSchemaNode();
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

	InsertNodeRow(nextNodeIdInt, nodeName, nodePort, groupId, nodeRack, hasMetadata);

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
 * EnsureSchemaNode checks if the current node is the schema node. If it does not,
 * the function errors out.
 */
void
EnsureSchemaNode(void)
{
	int localGroupId = GetLocalGroupId();

	if (localGroupId != 0)
	{
		ereport(ERROR, (errmsg("operation is not allowed on this node"),
						errhint("Connect to the schema node and run it again.")));
	}
}


/*
 * InsertNodedRow opens the node system catalog, and inserts a new row with the
 * given values into that system catalog.
 */
static void
InsertNodeRow(int nodeid, char *nodeName, int32 nodePort, uint32 groupId, char *nodeRack,
			  bool hasMetadata)
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

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	FreeFile(workerFileStream);
	free(workerFilePath);

	/* rename the file, marking that it is not used anymore */
	appendStringInfo(renamedWorkerFilePath, "%s", workerFilePath);
	appendStringInfo(renamedWorkerFilePath, ".obsolete");
	rename(workerFilePath, renamedWorkerFilePath->data);

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

	Assert(!HeapTupleHasNulls(heapTuple));

	workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	workerNode->nodeId = DatumGetUInt32(nodeId);
	workerNode->workerPort = DatumGetUInt32(nodePort);
	workerNode->groupId = DatumGetUInt32(groupId);
	strlcpy(workerNode->workerName, TextDatumGetCString(nodeName), WORKER_LENGTH);
	strlcpy(workerNode->workerRack, TextDatumGetCString(nodeRack), WORKER_LENGTH);
	workerNode->hasMetadata = DatumGetBool(hasMetadata);

	return workerNode;
}
