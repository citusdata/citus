/*-------------------------------------------------------------------------
 *
 * shard_split_replication.c
 *    This file contains functions to setup information about list of shards
 *    that are being split.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "distributed/utils/array_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_split.h"
#include "distributed/shard_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "common/hashfn.h"
#include "safe_str_lib.h"
#include "distributed/citus_safe_lib.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(split_shard_replication_setup);

static HTAB *ShardInfoHashMap = NULL;

/* Entry for hash map */
typedef struct NodeShardMappingEntry
{
	uint64_t key;
	List *shardSplitInfoList;
} NodeShardMappingEntry;

/* Function declarations */
static void ParseShardSplitInfo(ArrayType *shardInfoArrayObject,
								int shardSplitInfoIndex,
								uint64 *sourceShardId,
								uint64 *desShardId,
								int32 *minValue,
								int32 *maxValue,
								int32 *nodeId);
static ShardSplitInfo * CreateShardSplitInfo(uint64 sourceShardIdToSplit,
											 uint64 desSplitChildShardId,
											 int32 minValue,
											 int32 maxValue,
											 int32 nodeId);
static void AddShardSplitInfoEntryForNodeInMap(ShardSplitInfo *shardSplitInfo);
static void * PopulateShardSplitInfoInSM(ShardSplitInfo *shardSplitInfoArray,
										 HTAB *shardInfoHashMap,
										 dsm_handle dsmHandle,
										 int shardSplitInfoCount);
static void SetupHashMapForShardInfo();

/*
 * split_shard_replication_setup UDF creates in-memory data structures
 * to store the meta information about the shard undergoing split and new split
 * children along with their placements required during the catch up phase
 * of logical replication.
 * This meta information is stored in a shared memory segment and accessed
 * by logical decoding plugin.
 *
 * Split information is given by user as an Array in the below format
 * [{sourceShardId, childShardId, minValue, maxValue, Destination NodeId}]
 *
 * sourceShardId - id of the shard that is undergoing a split
 * childShardId  - id of shard that stores a specific range of values
 *                 belonging to sourceShardId(parent)
 * minValue      - lower bound of hash value which childShard stores
 *
 * maxValue      - upper bound of hash value which childShard stores
 *
 * NodeId        - Node where the childShardId is located
 *
 * The function parses the data and builds routing map per destination node id.
 * Multiple shards can be placed on the same destiation node. Source and
 * destinations nodes can be same too.
 *
 * There is a 1-1 mapping between a target node and a replication slot as one replication
 * slot takes care of replicating changes for one node.
 * The 'logical_decoding_plugin' consumes this information and routes the tuple
 * from the source shard to the appropriate destination shard that falls in the
 * respective range.
 */
Datum
split_shard_replication_setup(PG_FUNCTION_ARGS)
{
	ArrayType *shardInfoArrayObject = PG_GETARG_ARRAYTYPE_P(0);
	int shardInfoArrayLength = ARR_DIMS(shardInfoArrayObject)[0];

	/* SetupMap */
	SetupHashMapForShardInfo();

	int shardSplitInfoCount = 0;
	for (int index = 0; index < shardInfoArrayLength; index++)
	{
		uint64 sourceShardId = 0;
		uint64 desShardId = 0;
		int32 minValue = 0;
		int32 maxValue = 0;
		int32 nodeId = 0;

		ParseShardSplitInfo(
			shardInfoArrayObject,
			index,
			&sourceShardId,
			&desShardId,
			&minValue,
			&maxValue,
			&nodeId);

		ShardSplitInfo *shardSplitInfo = CreateShardSplitInfo(
			sourceShardId,
			desShardId,
			minValue,
			maxValue,
			nodeId);

		AddShardSplitInfoEntryForNodeInMap(shardSplitInfo);
		shardSplitInfoCount++;
	}

	dsm_handle dsmHandle;
	ShardSplitInfo *splitShardInfoSMArray =
		CreateSharedMemoryForShardSplitInfo(shardSplitInfoCount, &dsmHandle);

	PopulateShardSplitInfoInSM(splitShardInfoSMArray,
							   ShardInfoHashMap,
							   dsmHandle,
							   shardSplitInfoCount);

	return dsmHandle;
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
	info.keysize = sizeof(uint64_t);
	info.entrysize = sizeof(NodeShardMappingEntry);
	info.hash = uint32_hash;
	info.hcxt = CurrentMemoryContext;
	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	ShardInfoHashMap = hash_create("ShardInfoMap", 128, &info, hashFlags);
}


static void
ParseShardSplitInfo(ArrayType *shardInfoArrayObject,
					int shardSplitInfoIndex,
					uint64 *sourceShardId,
					uint64 *desShardId,
					int32 *minValue,
					int32 *maxValue,
					int32 *nodeId)
{
	Oid elemtypeId = ARR_ELEMTYPE(shardInfoArrayObject);
	int elemtypeLength = 0;
	bool elemtypeByValue = false;
	char elemtypeAlignment = 0;
	get_typlenbyvalalign(elemtypeId, &elemtypeLength, &elemtypeByValue,
						 &elemtypeAlignment);

	int elementIndex = 0;
	int indexes[] = { shardSplitInfoIndex + 1, elementIndex + 1 };
	bool isNull = false;

	/* Get source shard Id */
	Datum sourceShardIdDat = array_ref(
		shardInfoArrayObject,
		2,
		indexes,
		-1, /* (> 0 is for fixed-length arrays -- these are assumed to be 1-d, 0-based) */
		elemtypeLength,
		elemtypeByValue,
		elemtypeAlignment,
		&isNull);

	if (isNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("null entry found for source shardId")));
	}

	*sourceShardId = DatumGetUInt64(sourceShardIdDat);

	/* Get destination shard Id */
	elementIndex++;
	isNull = false;
	indexes[0] = shardSplitInfoIndex + 1;
	indexes[1] = elementIndex + 1;
	Datum destinationShardIdDat = array_ref(
		shardInfoArrayObject,
		2,
		indexes,
		-1, /* (> 0 is for fixed-length arrays -- these are assumed to be 1-d, 0-based) */
		elemtypeLength,
		elemtypeByValue,
		elemtypeAlignment,
		&isNull);

	if (isNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("null entry found for destination shardId")));
	}

	*desShardId = DatumGetUInt64(destinationShardIdDat);

	/* Get minValue for destination shard */
	elementIndex++;
	isNull = false;
	indexes[0] = shardSplitInfoIndex + 1;
	indexes[1] = elementIndex + 1;
	Datum minValueDat = array_ref(
		shardInfoArrayObject,
		2,
		indexes,
		-1, /* (> 0 is for fixed-length arrays -- these are assumed to be 1-d, 0-based) */
		elemtypeLength,
		elemtypeByValue,
		elemtypeAlignment,
		&isNull);

	if (isNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("null entry found for min value")));
	}

	*minValue = DatumGetInt32(minValueDat);

	/* Get maxValue for destination shard */
	elementIndex++;
	isNull = false;
	indexes[0] = shardSplitInfoIndex + 1;
	indexes[1] = elementIndex + 1;
	Datum maxValueDat = array_ref(
		shardInfoArrayObject,
		2,
		indexes,
		-1, /* (> 0 is for fixed-length arrays -- these are assumed to be 1-d, 0-based) */
		elemtypeLength,
		elemtypeByValue,
		elemtypeAlignment,
		&isNull);

	if (isNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("null entry found for max value")));
	}

	*maxValue = DatumGetInt32(maxValueDat);

	/* Get nodeId for shard placement*/
	elementIndex++;
	isNull = false;
	indexes[0] = shardSplitInfoIndex + 1;
	indexes[1] = elementIndex + 1;
	Datum nodeIdDat = array_ref(
		shardInfoArrayObject,
		2,
		indexes,
		-1, /* (> 0 is for fixed-length arrays -- these are assumed to be 1-d, 0-based) */
		elemtypeLength,
		elemtypeByValue,
		elemtypeAlignment,
		&isNull);

	if (isNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("null entry found for max value")));
	}

	*nodeId = DatumGetInt32(nodeIdDat);

	PG_RETURN_VOID();
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
	CitusTableCacheEntry *cachedTableEntry = GetCitusTableCacheEntry(
		shardIntervalToSplit->relationId);

	/*Todo(sameer): Also check if non-distributed table */
	if (!IsCitusTableTypeCacheEntry(cachedTableEntry, HASH_DISTRIBUTED))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Cannot Support the feature")));
	}

	Assert(shardIntervalToSplit->minValueExists);
	Assert(shardIntervalToSplit->maxValueExists);

	/* Oid of distributed table */
	Oid citusTableOid = InvalidOid;
	citusTableOid = shardIntervalToSplit->relationId;
	Oid sourceShardToSplitOid = InvalidOid;
	sourceShardToSplitOid = GetTableLocalShardOid(citusTableOid,
												  sourceShardIdToSplit);

	/* Oid of dummy table at the source */
	Oid desSplitChildShardOid = InvalidOid;
	desSplitChildShardOid = GetTableLocalShardOid(citusTableOid,
												  desSplitChildShardId);

	if (citusTableOid == InvalidOid ||
		sourceShardToSplitOid == InvalidOid ||
		desSplitChildShardOid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("Invalid citusTableOid:%u "
							   "sourceShardToSplitOid: %u,"
							   "desSplitChildShardOid :%u ",
							   citusTableOid,
							   sourceShardToSplitOid,
							   desSplitChildShardOid)));
	}

	/* Get PartitionColumnIndex for citusTableOid */
	int partitionColumnIndex = -1;

	/* determine the partition column in the tuple descriptor */
	Var *partitionColumn = cachedTableEntry->partitionColumn;
	if (partitionColumn == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("Invalid Partition Column")));
	}
	partitionColumnIndex = partitionColumn->varattno - 1;

	ShardSplitInfo *shardSplitInfo = palloc0(sizeof(ShardSplitInfo));
	shardSplitInfo->distributedTableOid = citusTableOid;
	shardSplitInfo->partitionColumnIndex = partitionColumnIndex;
	shardSplitInfo->sourceShardOid = sourceShardToSplitOid;
	shardSplitInfo->splitChildShardOid = desSplitChildShardOid;
	shardSplitInfo->shardMinValue = minValue;
	shardSplitInfo->shardMaxValue = maxValue;
	shardSplitInfo->nodeId = nodeId;

	return shardSplitInfo;
}


/*
 * AddShardSplitInfoEntryForNodeInMap function add's ShardSplitInfo entry
 * to the hash map. The key is nodeId on which the new shard is to be placed.
 */
void
AddShardSplitInfoEntryForNodeInMap(ShardSplitInfo *shardSplitInfo)
{
	uint64_t keyNodeId = shardSplitInfo->nodeId;
	bool found = false;
	NodeShardMappingEntry *nodeMappingEntry =
		(NodeShardMappingEntry *) hash_search(ShardInfoHashMap, &keyNodeId, HASH_ENTER,
											  &found);

	if (!found)
	{
		nodeMappingEntry->shardSplitInfoList = NULL;
		nodeMappingEntry->key = keyNodeId;
	}

	nodeMappingEntry->shardSplitInfoList =
		lappend(nodeMappingEntry->shardSplitInfoList, (ShardSplitInfo *) shardSplitInfo);

	PG_RETURN_VOID();
}


/*
 * PopulateShardSplitInfoInSM function copies information from the hash map
 * into shared memory segment. This information is consumed by the WAL sender
 * process during logical replication.
 *
 * shardSplitInfoArray - Shared memory pointer where information has to
 *                       be copied
 *
 * shardInfoHashMap    - Hashmap containing parsed split information
 *                       per nodeId wise
 *
 * dsmHandle           - Shared memory segment handle
 */
void *
PopulateShardSplitInfoInSM(ShardSplitInfo *shardSplitInfoArray,
						   HTAB *shardInfoHashMap,
						   dsm_handle dsmHandle,
						   int shardSplitInfoCount)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardInfoHashMap);

	NodeShardMappingEntry *entry = NULL;
	int index = 0;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint64_t nodeId = entry->key;
		char *derivedSlotName =
			encode_replication_slot(nodeId, dsmHandle);

		List *shardSplitInfoList = entry->shardSplitInfoList;
		ListCell *listCell = NULL;
		foreach(listCell, shardSplitInfoList)
		{
			ShardSplitInfo *splitShardInfo = (ShardSplitInfo *) lfirst(listCell);
			ShardSplitInfo *shardInfoInSM = &shardSplitInfoArray[index];

			shardInfoInSM->distributedTableOid = splitShardInfo->distributedTableOid;
			shardInfoInSM->partitionColumnIndex = splitShardInfo->partitionColumnIndex;
			shardInfoInSM->sourceShardOid = splitShardInfo->sourceShardOid;
			shardInfoInSM->splitChildShardOid = splitShardInfo->splitChildShardOid;
			shardInfoInSM->shardMinValue = splitShardInfo->shardMinValue;
			shardInfoInSM->shardMaxValue = splitShardInfo->shardMaxValue;
			shardInfoInSM->nodeId = splitShardInfo->nodeId;
			strcpy_s(shardInfoInSM->slotName, NAMEDATALEN, derivedSlotName);
			index++;
		}
	}

	PG_RETURN_VOID();
}
