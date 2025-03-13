/*-------------------------------------------------------------------------
 *
 * worker_split_shard_replication_setup_udf.c
 *    This file contains functions to setup information about list of shards
 *    that are being split.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/connection_management.h"
#include "distributed/distribution_column.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_utils.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/tuplestore.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(worker_split_shard_replication_setup);

static HTAB *ShardInfoHashMap = NULL;

/* Function declarations */
static void ParseShardSplitInfoFromDatum(Datum shardSplitInfoDatum,
										 uint64 *sourceShardId,
										 char **partitionColumnName,
										 uint64 *childShardId,
										 int32 *minValue,
										 int32 *maxValue,
										 int32 *nodeId);

static ShardSplitInfo * CreateShardSplitInfo(uint64 sourceShardIdToSplit,
											 char *partitionColumnName,
											 uint64 desSplitChildShardId,
											 int32 minValue,
											 int32 maxValue,
											 int32 nodeId);
static void AddShardSplitInfoEntryForNodeInMap(ShardSplitInfo *shardSplitInfo);
static void PopulateShardSplitInfoInSM(ShardSplitInfoSMHeader *shardSplitInfoSMHeader,
									   OperationId operationId);

static void ReturnReplicationSlotInfo(Tuplestorestate *tupleStore,
									  TupleDesc tupleDescriptor,
									  OperationId operationId);

/*
 * worker_split_shard_replication_setup UDF creates in-memory data structures
 * to store the meta information about the shard undergoing split and new split
 * children along with their placements. This info is required during the catch up
 * phase of logical replication.
 * This meta information is stored in a shared memory segment and accessed
 * by logical decoding plugin.
 *
 * Split information is given by user as an Array of custom data type 'pg_catalog.split_shard_info'.
 * (worker_split_shard_replication_setup(pg_catalog.split_shard_info[]))
 *
 * Fields of custom data type 'pg_catalog.split_shard_info':
 * source_shard_id - id of the shard that is undergoing a split
 *
 * distribution_column - Distribution column name
 *
 * child_shard_id  - id of shard that stores a specific range of values
 *                   belonging to sourceShardId(parent)
 *
 * shard_min_value - Lower bound(inclusive) of hash value which childShard stores
 *
 * shard_max_value - Upper bound(inclusive) of hash value which childShard stores
 *
 * node_id         - Node where the childShardId is located
 *
 * The function parses the data and builds routing map with key for each distinct
 * <nodeId, tableOwner> pair. Multiple shards can be placed on the same destination node.
 * Source and destination nodes can be same too.
 *
 * There is a 1-1 mapping between a (table owner, node) and replication slot. One replication
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

	OperationId operationId = DatumGetUInt64(PG_GETARG_DATUM(1));

	/* SetupMap */
	ShardInfoHashMap = CreateSimpleHash(NodeAndOwner, GroupedShardSplitInfos);

	int shardSplitInfoCount = 0;

	ArrayIterator shardInfo_iterator = array_create_iterator(shardInfoArrayObject, 0,
															 NULL);
	Datum shardInfoDatum = 0;
	bool isnull = false;
	while (array_iterate(shardInfo_iterator, &shardInfoDatum, &isnull))
	{
		uint64 sourceShardId = 0;
		char *partitionColumnName = NULL;
		uint64 childShardId = 0;
		int32 minValue = 0;
		int32 maxValue = 0;
		int32 nodeId = 0;

		ParseShardSplitInfoFromDatum(shardInfoDatum, &sourceShardId,
									 &partitionColumnName, &childShardId,
									 &minValue, &maxValue, &nodeId);

		ShardSplitInfo *shardSplitInfo = CreateShardSplitInfo(
			sourceShardId,
			partitionColumnName,
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

	PopulateShardSplitInfoInSM(splitShardInfoSMHeader, operationId);

	/* store handle in statically allocated shared memory*/
	StoreShardSplitSharedMemoryHandle(dsmHandle);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	ReturnReplicationSlotInfo(tupleStore, tupleDescriptor, operationId);

	PG_RETURN_VOID();
}


/*
 * CreateShardSplitInfo function constructs ShardSplitInfo data structure
 * with appropriate OIs' for source and destination relation.
 *
 * sourceShardIdToSplit - Existing shardId which has a valid entry in cache and catalogue
 * partitionColumnName  - Name of column to use for partitioning
 * desSplitChildShardId - New split child shard which doesn't have an entry in metacache yet
 * minValue				- Minimum hash value for desSplitChildShardId
 * maxValue				- Maximum hash value for desSplitChildShardId
 * nodeId				- NodeId where
 * However we can use shard ID and construct qualified shardName.
 */
ShardSplitInfo *
CreateShardSplitInfo(uint64 sourceShardIdToSplit,
					 char *partitionColumnName,
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
	Var *partitionColumn = BuildDistributionKeyFromColumnName(sourceShardToSplitOid,
															  partitionColumnName,
															  AccessShareLock);
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
 * AddShardSplitInfoEntryForNodeInMap function adds ShardSplitInfo entry
 * to the hash map. The key is nodeId on which the new shard is to be placed.
 */
static void
AddShardSplitInfoEntryForNodeInMap(ShardSplitInfo *shardSplitInfo)
{
	NodeAndOwner key;
	key.nodeId = shardSplitInfo->nodeId;
	key.tableOwnerId = TableOwnerOid(shardSplitInfo->distributedTableOid);

	bool found = false;
	GroupedShardSplitInfos *groupedInfos =
		(GroupedShardSplitInfos *) hash_search(ShardInfoHashMap, &key, HASH_ENTER,
											   &found);
	if (!found)
	{
		groupedInfos->shardSplitInfoList = NIL;
	}

	groupedInfos->shardSplitInfoList =
		lappend(groupedInfos->shardSplitInfoList, (ShardSplitInfo *) shardSplitInfo);
}


/*
 * PopulateShardSplitInfoInSM function copies information from the hash map
 * into shared memory segment. This information is consumed by the WAL sender
 * process during logical replication.
 *
 * shardSplitInfoSMHeader - Shared memory header
 */
static void
PopulateShardSplitInfoInSM(ShardSplitInfoSMHeader *shardSplitInfoSMHeader,
						   OperationId operationId)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, ShardInfoHashMap);

	GroupedShardSplitInfos *entry = NULL;
	int splitInfoIndex = 0;
	while ((entry = (GroupedShardSplitInfos *) hash_seq_search(&status)) != NULL)
	{
		uint32_t nodeId = entry->key.nodeId;
		uint32_t tableOwnerId = entry->key.tableOwnerId;
		char *derivedSlotName =
			ReplicationSlotNameForNodeAndOwnerForOperation(SHARD_SPLIT,
														   nodeId,
														   tableOwnerId,
														   operationId);

		List *shardSplitInfoList = entry->shardSplitInfoList;
		ShardSplitInfo *splitShardInfo = NULL;
		foreach_declared_ptr(splitShardInfo, shardSplitInfoList)
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
 * ParseShardSplitInfoFromDatum deserializes individual fields of 'pg_catalog.split_shard_info'
 * datatype.
 */
static void
ParseShardSplitInfoFromDatum(Datum shardSplitInfoDatum,
							 uint64 *sourceShardId,
							 char **partitionColumnName,
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

	Datum partitionColumnDatum = GetAttributeByName(dataTuple, "distribution_column",
													&isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"distribution_column for split_shard_info can't be null")));
	}
	*partitionColumnName = TextDatumGetCString(partitionColumnDatum);

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


/*
 * ReturnReplicationSlotInfo writes 'pg_catalog.replication_slot_info'
 * records to tuplestore.
 * This information is used by the coordinator to create replication slots as a
 * part of non-blocking split workflow.
 */
static void
ReturnReplicationSlotInfo(Tuplestorestate *tupleStore,
						  TupleDesc tupleDescriptor,
						  OperationId operationId)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, ShardInfoHashMap);

	GroupedShardSplitInfos *entry = NULL;
	while ((entry = (GroupedShardSplitInfos *) hash_seq_search(&status)) != NULL)
	{
		Datum values[3];
		bool nulls[3];

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));

		values[0] = Int32GetDatum(entry->key.nodeId);

		char *tableOwnerName = GetUserNameFromId(entry->key.tableOwnerId, false);
		values[1] = CStringGetTextDatum(tableOwnerName);

		char *slotName =
			ReplicationSlotNameForNodeAndOwnerForOperation(SHARD_SPLIT,
														   entry->key.nodeId,
														   entry->key.tableOwnerId,
														   operationId);
		values[2] = CStringGetTextDatum(slotName);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, nulls);
	}
}
