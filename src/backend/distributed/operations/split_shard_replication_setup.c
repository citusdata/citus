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
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "common/hashfn.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/connection_management.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"
#include "distributed/remote_commands.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "commands/dbcommands.h"

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


static void CreatePublishersForSplitChildren(HTAB *shardInfoHashMap);
StringInfo GetSoureAndDestinationShardNames(List* shardSplitInfoList);
char *  ConstructFullyQualifiedSplitChildShardName(ShardSplitInfo* shardSplitInfo);

/*
 * worker_split_shard_replication_setup UDF creates in-memory data structures
 * to store the meta information about the shard undergoing split and new split
 * children along with their placements. This info is required during the catch up
 * phase of logical replication.
 * This meta information is stored in a shared memory segment and accessed
 * by logical decoding plugin.
 *
 * Split information is given by user as an Array of custom data type 'citus.split_shard_info'.
 * (worker_split_shard_replication_setup(citus.split_shard_info[]))
 *
 * Fields of custom data type 'citus.split_shard_info':
 * source_shard_id - id of the shard that is undergoing a split
 *
 * child_shard_id  - id of shard that stores a specific range of values
 *                   belonging to sourceShardId(parent)
 *
 * shard_min_value - lower bound(inclusive) of hash value which childShard stores.
 *
 * shard_max_value - upper bound(inclusive) of hash value which childShard stores
 *
 * node_id         - Node where the childShardId is located
 *
 * The function parses the data and builds routing map with key for each distinct
 * <nodeId, tableOwner> pair. Multiple shards can be placed on the same destination node.
 * Source and destination nodes can be same too.
 *
 * There is a 1-1 mapping between a table owner and a replication slot. One replication
 * slot takes care of replicating changes for all shards belonging to the same owner on a particular node.
 *
 * During the replication phase, WAL senders will attach to the shared memory
 * populated by current UDF. It routes the tuple from the source shard to the appropriate destination
 * shard for which the respective slot is responsible.
 */
Datum
worker_split_shard_replication_setup(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("split_shard_info array cannot be NULL")));
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
	StoreShardSplitSharedMemoryHandle(dsmHandle);

	CreatePublishersForSplitChildren(ShardInfoHashMap);

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
	shardSplitInfo->sourceShardId = sourceShardIdToSplit;
	shardSplitInfo->splitChildShardId = desSplitChildShardId;

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
	int splitInfoIndex = 0;
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
			shardSplitInfoSMHeader->splitInfoArray[splitInfoIndex] = *splitShardInfo;
			strcpy_s(shardSplitInfoSMHeader->splitInfoArray[splitInfoIndex].slotName,
					 NAMEDATALEN,
					 derivedSlotName);
			splitInfoIndex++;
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

static void CreatePublishersForSplitChildren(HTAB *shardInfoHashMap)
{
		
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardInfoHashMap);

	NodeShardMappingEntry *entry = NULL;
	int splitInfoIndex = 0;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32_t nodeId = entry->key.nodeId;
		uint32_t tableOwnerId = entry->key.tableOwnerId;

		int connectionFlags = FORCE_NEW_CONNECTION;
		printf("Sameer getting new connection \n");
		MultiConnection *sourceConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	 "localhost",
																	 PostPortNumber,
																	  CitusExtensionOwnerName(),
																	  get_database_name(
																		  MyDatabaseId));
		StringInfo shardNamesForPublication = GetSoureAndDestinationShardNames(entry->shardSplitInfoList);

		StringInfo command = makeStringInfo();
		appendStringInfo(command, "CREATE PUBLICATION sameerpub_%u_%u FOR TABLE %s", nodeId, tableOwnerId,shardNamesForPublication->data);
		ExecuteCriticalRemoteCommand(sourceConnection, command->data);
		printf("Sameer UserName: %s \n", GetUserNameFromId(tableOwnerId, false));
	}
}

StringInfo GetSoureAndDestinationShardNames(List* shardSplitInfoList)
{
	HASHCTL info;
	int flags = HASH_ELEM | HASH_CONTEXT;

	/* initialise the hash table */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(uint64);
	info.hcxt = CurrentMemoryContext;

	HTAB *sourceShardIdSet = hash_create("Source ShardId Set", 128, &info, flags);
	
	/* Get child shard names */
	StringInfo allShardNames = makeStringInfo();
	bool addComma = false;

	ShardSplitInfo *shardSplitInfo = NULL;
	foreach_ptr(shardSplitInfo, shardSplitInfoList)
	{
		/* add source shard id to the hash table to get list of unique source shard ids */
		bool found = false;
		uint64 sourceShardId = shardSplitInfo->sourceShardId;
		hash_search(sourceShardIdSet, &sourceShardId, HASH_ENTER, &found);

		if(addComma)
		{
			appendStringInfo(allShardNames, ",");
		}

		/* Append fully qualified split child shard name */
		char *childShardName = ConstructFullyQualifiedSplitChildShardName(shardSplitInfo);
		appendStringInfo(allShardNames, childShardName);
		addComma = true;
	}


	HASH_SEQ_STATUS status;
	hash_seq_init(&status, sourceShardIdSet);
	uint64 *sourceShardIdEntry = NULL;
	while ((sourceShardIdEntry = hash_seq_search(&status)) != NULL)
	{
		ShardInterval *sourceShardInterval = LoadShardInterval(*sourceShardIdEntry);
		char* sourceShardName = ConstructQualifiedShardName(sourceShardInterval);

		if(addComma)
		{
			appendStringInfo(allShardNames, ",");
		}

		appendStringInfo(allShardNames, sourceShardName);
		addComma = true;
	}

	return allShardNames;
}

char *
ConstructFullyQualifiedSplitChildShardName(ShardSplitInfo* shardSplitInfo)
{
	Oid schemaId = get_rel_namespace(shardSplitInfo->distributedTableOid);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(shardSplitInfo->distributedTableOid);

	char *shardName = pstrdup(tableName);
	AppendShardIdToName(&shardName, shardSplitInfo->splitChildShardId);
	shardName = quote_qualified_identifier(schemaName, shardName);

	return shardName;
}