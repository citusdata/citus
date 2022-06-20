/*-------------------------------------------------------------------------
 *
 * split_shard_replication_setup.c
 *    This file contains functions to setup information about list of shards
 *    that are being split.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "common/hashfn.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(worker_split_shard_replication_setup);

static HTAB *ShardInfoHashMap = NULL;

/* key for NodeShardMappingEntry */
typedef struct NodeShardMappingKey
{
	uint32_t nodeId;
	Oid tableOwnerId;
} NodeShardMappingKey;

/* Entry for hash map */
typedef struct NodeShardMappingEntry
{
	NodeShardMappingKey key;
	List *shardSplitInfoList;
} NodeShardMappingEntry;

/* Function declarations */
static void ParseShardSplitInfoFromDatum(Datum shardSplitInfoDatum,
										 uint64 *sourceShardId,
										 uint64 *childShardId,
										 int32 *minValue,
										 int32 *maxValue,
										 int32 *nodeId);

static ShardSplitInfo * CreateShardSplitInfo(uint64 sourceShardIdToSplit,
											 uint64 desSplitChildShardId,
											 int32 minValue,
											 int32 maxValue,
											 int32 nodeId);
static void AddShardSplitInfoEntryForNodeInMap(ShardSplitInfo *shardSplitInfo);
static void PopulateShardSplitInfoInSM(ShardSplitInfoSMHeader *shardSplitInfoSMHeader,
									   HTAB *shardInfoHashMap);

static void SetupHashMapForShardInfo(void);
static uint32 NodeShardMappingHash(const void *key, Size keysize);
static int NodeShardMappingHashCompare(const void *left, const void *right, Size keysize);


/*
 * worker_split_shard_replication_setup UDF creates in-memory data structures
 * to store the meta information about the shard undergoing split and new split
 * children along with their placements required during the catch up phase
 * of logical replication.
 * This meta information is stored in a shared memory segment and accessed
 * by logical decoding plugin.
 *
 * Split information is given by user as an Array of source shards undergoing splits
 * in the below format.
 * Array[Array[sourceShardId, childShardId, minValue, maxValue, Destination NodeId]]
 *
 * sourceShardId - id of the shard that is undergoing a split
 * childShardId  - id of shard that stores a specific range of values
 *                 belonging to sourceShardId(parent)
 * minValue      - lower bound(inclusive) of hash value which childShard stores
 *
 * maxValue      - upper bound(inclusive) of hash value which childShard stores
 *
 * NodeId        - Node where the childShardId is located
 *
 * The function parses the data and builds routing map per destination node id.
 * Multiple shards can be placed on the same destiation node. Source and
 * destinations nodes can be same too.
 *
 * Usage Semantics:
 * This UDF returns a shared memory handle where the information is stored. This shared memory
 * handle is used by caller to encode replication slot name as "citus_split_nodeId_sharedMemoryHandle_tableOnwerId"
 * for every distinct  table owner. The same encoded slot name is stored in one of the fields of the
 * in-memory data structure(ShardSplitInfo).
 *
 * There is a 1-1 mapping between a table owner id and a replication slot. One replication
 * slot takes care of replicating changes for all shards belonging to the same owner on a particular node.
 *
 * During the replication phase, 'decoding_plugin_for_shard_split' called for a change on a particular
 * replication slot, will decode the shared memory handle from its slot name and will attach to the
 * shared memory. The plugin consumes the information from shared memory. It routes the tuple
 * from the source shard to the appropriate destination shard for which the respective slot is
 * responsible.
 */
Datum
worker_split_shard_replication_setup(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("targets can't be null")));
	}

	ArrayType *shardInfoArrayObject = PG_GETARG_ARRAYTYPE_P(0);
	if (array_contains_nulls(shardInfoArrayObject))
	{
		ereport(ERROR, (errmsg("Unexpectedly shard info array contains a null value")));
	}

	/* SetupMap */
	SetupHashMapForShardInfo();

	int shardSplitInfoCount = 0;

	ArrayIterator shardInfo_iterator = array_create_iterator(shardInfoArrayObject, 0,
															 NULL);
	Datum shardInfoDatum = 0;
	bool isnull = false;
	while (array_iterate(shardInfo_iterator, &shardInfoDatum, &isnull))
	{
		uint64 sourceShardId = 0;
		uint64 childShardId = 0;
		int32 minValue = 0;
		int32 maxValue = 0;
		int32 nodeId = 0;

		ParseShardSplitInfoFromDatum(shardInfoDatum, &sourceShardId, &childShardId,
									 &minValue, &maxValue, &nodeId);

		ShardSplitInfo *shardSplitInfo = CreateShardSplitInfo(
			sourceShardId,
			childShardId,
			minValue,
			maxValue,
			nodeId);

		AddShardSplitInfoEntryForNodeInMap(shardSplitInfo);
		shardSplitInfoCount++;
	}

	dsm_handle dsmHandle;
	ShardSplitInfoSMHeader *splitShardInfoSMHeader =
		CreateSharedMemoryForShardSplitInfo(shardSplitInfoCount, &dsmHandle);

	PopulateShardSplitInfoInSM(splitShardInfoSMHeader,
							   ShardInfoHashMap);

	/* store handle in statically allocated shared memory*/
	StoreSharedMemoryHandle(dsmHandle);

	PG_RETURN_VOID();
}


/*
 * SetupHashMapForShardInfo initializes a hash map to store shard split
 * information by grouping them node id wise. The key of the hash table
 * is 'nodeId' and value is a list of ShardSplitInfo that are placed on
 * this particular node.
 */
static void
SetupHashMapForShardInfo()
{
	HASHCTL info;
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(NodeShardMappingKey);
	info.entrysize = sizeof(NodeShardMappingEntry);
	info.hash = NodeShardMappingHash;
	info.match = NodeShardMappingHashCompare;
	info.hcxt = CurrentMemoryContext;

	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

	ShardInfoHashMap = hash_create("ShardInfoMap", 128, &info, hashFlags);
}


/*
 * CreateShardSplitInfo function constructs ShardSplitInfo data structure
 * with appropriate OIs' for source and destination relation.
 *
 * sourceShardIdToSplit - Existing shardId which has a valid entry in cache and catalogue
 * desSplitChildShardId - New split child shard which doesn't have an entry in metacache yet.
 * minValue				- Minimum hash value for desSplitChildShardId
 * maxValue				- Maximum hash value for desSplitChildShardId
 * nodeId				- NodeId where
 * However we can use shard ID and construct qualified shardName.
 */
ShardSplitInfo *
CreateShardSplitInfo(uint64 sourceShardIdToSplit,
					 uint64 desSplitChildShardId,
					 int32 minValue,
					 int32 maxValue,
					 int32 nodeId)
{
	ShardInterval *shardIntervalToSplit = LoadShardInterval(sourceShardIdToSplit);

	/* If metadata is not synced, we cannot proceed further as split work flow assumes
	 * metadata to be synced on worker node hosting source shard to split.
	 */
	if (shardIntervalToSplit == NULL)
	{
		ereport(ERROR,
				errmsg(
					"Could not find metadata corresponding to source shard id: %ld. "
					"Split workflow assumes metadata to be synced across "
					"worker nodes hosting source shards.", sourceShardIdToSplit));
	}

	CitusTableCacheEntry *cachedTableEntry = GetCitusTableCacheEntry(
		shardIntervalToSplit->relationId);

	if (!IsCitusTableTypeCacheEntry(cachedTableEntry, HASH_DISTRIBUTED))
	{
		Relation distributedRel = RelationIdGetRelation(cachedTableEntry->relationId);
		ereport(ERROR, (errmsg(
							"Citus does only support Hash distributed tables to be split."),
						errdetail("Table '%s' is not Hash distributed",
								  RelationGetRelationName(distributedRel))
						));
		RelationClose(distributedRel);
	}

	Assert(shardIntervalToSplit->minValueExists);
	Assert(shardIntervalToSplit->maxValueExists);

	/* Oid of distributed table */
	Oid citusTableOid = shardIntervalToSplit->relationId;
	Oid sourceShardToSplitOid = GetTableLocalShardOid(citusTableOid,
													  sourceShardIdToSplit);

	/* Oid of dummy table at the source */
	Oid destSplitChildShardOid = GetTableLocalShardOid(citusTableOid,
													   desSplitChildShardId);

	if (citusTableOid == InvalidOid ||
		sourceShardToSplitOid == InvalidOid ||
		destSplitChildShardOid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("Invalid citusTableOid:%u, "
							   "sourceShardToSplitOid:%u, "
							   "destSplitChildShardOid:%u ",
							   citusTableOid,
							   sourceShardToSplitOid,
							   destSplitChildShardOid)));
	}

	/* determine the partition column in the tuple descriptor */
	Var *partitionColumn = cachedTableEntry->partitionColumn;
	if (partitionColumn == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("Invalid Partition Column")));
	}
	int partitionColumnIndex = partitionColumn->varattno - 1;

	ShardSplitInfo *shardSplitInfo = palloc0(sizeof(ShardSplitInfo));
	shardSplitInfo->distributedTableOid = citusTableOid;
	shardSplitInfo->partitionColumnIndex = partitionColumnIndex;
	shardSplitInfo->sourceShardOid = sourceShardToSplitOid;
	shardSplitInfo->splitChildShardOid = destSplitChildShardOid;
	shardSplitInfo->shardMinValue = minValue;
	shardSplitInfo->shardMaxValue = maxValue;
	shardSplitInfo->nodeId = nodeId;

	return shardSplitInfo;
}


/*
 * AddShardSplitInfoEntryForNodeInMap function add's ShardSplitInfo entry
 * to the hash map. The key is nodeId on which the new shard is to be placed.
 */
static void
AddShardSplitInfoEntryForNodeInMap(ShardSplitInfo *shardSplitInfo)
{
	NodeShardMappingKey key;
	key.nodeId = shardSplitInfo->nodeId;
	key.tableOwnerId = TableOwnerOid(shardSplitInfo->distributedTableOid);

	bool found = false;
	NodeShardMappingEntry *nodeMappingEntry =
		(NodeShardMappingEntry *) hash_search(ShardInfoHashMap, &key, HASH_ENTER,
											  &found);
	if (!found)
	{
		nodeMappingEntry->shardSplitInfoList = NIL;
	}

	nodeMappingEntry->shardSplitInfoList =
		lappend(nodeMappingEntry->shardSplitInfoList, (ShardSplitInfo *) shardSplitInfo);
}


/*
 * PopulateShardSplitInfoInSM function copies information from the hash map
 * into shared memory segment. This information is consumed by the WAL sender
 * process during logical replication.
 *
 * shardSplitInfoSMHeader - Shared memory header
 *
 * shardInfoHashMap    - Hashmap containing parsed split information
 *                       per nodeId wise
 */
static void
PopulateShardSplitInfoInSM(ShardSplitInfoSMHeader *shardSplitInfoSMHeader,
						   HTAB *shardInfoHashMap)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardInfoHashMap);

	NodeShardMappingEntry *entry = NULL;
	int index = 0;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32_t nodeId = entry->key.nodeId;
		uint32_t tableOwnerId = entry->key.tableOwnerId;
		char *derivedSlotName =
			encode_replication_slot(nodeId, tableOwnerId);

		List *shardSplitInfoList = entry->shardSplitInfoList;
		ShardSplitInfo *splitShardInfo = NULL;
		foreach_ptr(splitShardInfo, shardSplitInfoList)
		{
			shardSplitInfoSMHeader->splitInfoArray[index] = *splitShardInfo;
			strcpy_s(shardSplitInfoSMHeader->splitInfoArray[index].slotName, NAMEDATALEN,
					 derivedSlotName);
			index++;
		}
	}
}


/*
 * NodeShardMappingHash returns hash value by combining hash of node id
 * and tableowner Id.
 */
static uint32
NodeShardMappingHash(const void *key, Size keysize)
{
	NodeShardMappingKey *entry = (NodeShardMappingKey *) key;
	uint32 hash = hash_uint32(entry->nodeId);
	hash = hash_combine(hash, hash_uint32(entry->tableOwnerId));
	return hash;
}


/*
 * Comparator function for hash keys
 */
static int
NodeShardMappingHashCompare(const void *left, const void *right, Size keysize)
{
	NodeShardMappingKey *leftKey = (NodeShardMappingKey *) left;
	NodeShardMappingKey *rightKey = (NodeShardMappingKey *) right;

	if (leftKey->nodeId != rightKey->nodeId ||
		leftKey->tableOwnerId != rightKey->tableOwnerId)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}


static void
ParseShardSplitInfoFromDatum(Datum shardSplitInfoDatum,
							 uint64 *sourceShardId,
							 uint64 *childShardId,
							 int32 *minValue,
							 int32 *maxValue,
							 int32 *nodeId)
{
	HeapTupleHeader dataTuple = DatumGetHeapTupleHeader(shardSplitInfoDatum);
	bool isnull = false;

	Datum sourceShardIdDatum = GetAttributeByName(dataTuple, "source_shard_id", &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg("source_shard_id for split_shard_info can't be null")));
	}
	*sourceShardId = DatumGetUInt64(sourceShardIdDatum);

	Datum childShardIdDatum = GetAttributeByName(dataTuple, "child_shard_id", &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg("child_shard_id for split_shard_info can't be null")));
	}
	*childShardId = DatumGetUInt64(childShardIdDatum);

	Datum minValueDatum = GetAttributeByName(dataTuple, "shard_min_value", &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg("shard_min_value for split_shard_info can't be null")));
	}
	char *shardMinValueString = text_to_cstring(DatumGetTextP(minValueDatum));
	*minValue = SafeStringToInt32(shardMinValueString);

	Datum maxValueDatum = GetAttributeByName(dataTuple, "shard_max_value", &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg("shard_max_value for split_shard_info can't be null")));
	}
	char *shardMaxValueString = text_to_cstring(DatumGetTextP(maxValueDatum));
	*maxValue = SafeStringToInt32(shardMaxValueString);

	Datum nodeIdDatum = GetAttributeByName(dataTuple, "node_id", &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg("node_id for split_shard_info can't be null")));
	}

	*nodeId = DatumGetInt32(nodeIdDatum);
}
