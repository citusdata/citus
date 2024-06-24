/*-------------------------------------------------------------------------
 *
 * split_shards.c
 *
 * This file contains functions to split a shard according to a given
 * distribution column value.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"

#include "catalog/pg_class.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "distributed/colocation_utils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_split.h"
#include "distributed/utils/distribution_column_map.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(isolate_tenant_to_new_shard);
PG_FUNCTION_INFO_V1(worker_hash);


/*
 * isolate_tenant_to_new_shard isolates a tenant to its own shard by spliting
 * the current matching shard.
 */
Datum
isolate_tenant_to_new_shard(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid relationId = PG_GETARG_OID(0);
	Datum inputDatum = PG_GETARG_DATUM(1);
	text *cascadeOptionText = PG_GETARG_TEXT_P(2);
	Oid shardTransferModeOid = PG_GETARG_OID(3);

	EnsureTableOwner(relationId);

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	char partitionMethod = cacheEntry->partitionMethod;
	if (partitionMethod != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot isolate tenant because tenant isolation "
							   "is only support for hash distributed tables")));
	}

	List *colocatedTableList = ColocatedTableList(relationId);
	int colocatedTableCount = list_length(colocatedTableList);

	Oid inputDataType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	char *tenantIdString = DatumToString(inputDatum, inputDataType);

	char *cascadeOptionString = text_to_cstring(cascadeOptionText);
	if (pg_strncasecmp(cascadeOptionString, "CASCADE", NAMEDATALEN) != 0 &&
		colocatedTableCount > 1)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot isolate tenant because \"%s\" has colocated "
							   "tables", relationName),
						errhint("Use CASCADE option to isolate tenants for the "
								"colocated tables too. Example usage: "
								"isolate_tenant_to_new_shard('%s', '%s', 'CASCADE')",
								relationName, tenantIdString)));
	}

	EnsureReferenceTablesExistOnAllNodes();

	Var *distributionColumn = DistPartitionKey(relationId);

	/* earlier we checked that the table was hash partitioned, so there should be a distribution column */
	Assert(distributionColumn != NULL);

	Oid distributionColumnType = distributionColumn->vartype;

	Datum tenantIdDatum = StringToDatum(tenantIdString, distributionColumnType);
	ShardInterval *sourceShard = FindShardInterval(tenantIdDatum, cacheEntry);
	if (sourceShard == NULL)
	{
		ereport(ERROR, (errmsg("tenant does not have a shard")));
	}

	int shardMinValue = DatumGetInt32(sourceShard->minValue);
	int shardMaxValue = DatumGetInt32(sourceShard->maxValue);
	if (shardMinValue == shardMaxValue)
	{
		char *tableName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						(errmsg("table %s has already been isolated for the given value",
								quote_identifier(tableName)))));
	}

	List *sourcePlacementList = ActiveShardPlacementList(sourceShard->shardId);
	if (list_length(sourcePlacementList) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot isolate tenants when using shard replication")));
	}

	ShardPlacement *sourceShardPlacement = linitial(sourcePlacementList);

	/* get hash function name */
	FmgrInfo *hashFunction = cacheEntry->hashFunction;

	/* get hashed value of the distribution value */
	Datum hashedValueDatum = FunctionCall1Coll(hashFunction,
											   cacheEntry->partitionColumn->varcollid,
											   tenantIdDatum);
	int hashedValue = DatumGetInt32(hashedValueDatum);

	List *shardSplitPointsList = NIL;

	/*
	 * If the hash value lies at one of the boundaries, we only have a single
	 * split point.
	 */
	if (hashedValue == shardMinValue)
	{
		shardSplitPointsList = list_make1_int(hashedValue);
	}
	else if (hashedValue == shardMaxValue)
	{
		shardSplitPointsList = list_make1_int(hashedValue - 1);
	}
	else
	{
		shardSplitPointsList = list_make2_int(hashedValue - 1, hashedValue);
	}

	/* we currently place the isolated hash value into the same node */
	int sourceNodeId = sourceShardPlacement->nodeId;
	List *nodeIdsForPlacementList = list_make2_int(sourceNodeId, sourceNodeId);

	if (list_length(shardSplitPointsList) > 1)
	{
		nodeIdsForPlacementList = lappend_int(nodeIdsForPlacementList, sourceNodeId);
	}

	DistributionColumnMap *distributionColumnOverrides = NULL;
	List *sourceColocatedShardIntervalList = NIL;
	SplitMode splitMode = LookupSplitMode(shardTransferModeOid);
	SplitShard(splitMode,
			   ISOLATE_TENANT_TO_NEW_SHARD,
			   sourceShard->shardId,
			   shardSplitPointsList,
			   nodeIdsForPlacementList,
			   distributionColumnOverrides,
			   sourceColocatedShardIntervalList,
			   INVALID_COLOCATION_ID);

	cacheEntry = GetCitusTableCacheEntry(relationId);
	ShardInterval *newShard = FindShardInterval(tenantIdDatum, cacheEntry);

	PG_RETURN_INT64(newShard->shardId);
}


/*
 * worker_hash returns the hashed value of the given value.
 */
Datum
worker_hash(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Datum valueDatum = PG_GETARG_DATUM(0);

	/* figure out hash function from the data type */
	Oid valueDataType = get_fn_expr_argtype(fcinfo->flinfo, 0);
	TypeCacheEntry *typeEntry = lookup_type_cache(valueDataType,
												  TYPECACHE_HASH_PROC_FINFO);

	if (typeEntry->hash_proc_finfo.fn_oid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot find a hash function for the input type"),
						errhint("Cast input to a data type with a hash function.")));
	}

	FmgrInfo *hashFunction = palloc0(sizeof(FmgrInfo));
	fmgr_info_copy(hashFunction, &(typeEntry->hash_proc_finfo), CurrentMemoryContext);

	/* calculate hash value */
	Datum hashedValueDatum =
		FunctionCall1Coll(hashFunction, PG_GET_COLLATION(), valueDatum);

	PG_RETURN_INT32(hashedValueDatum);
}
