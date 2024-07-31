/*-------------------------------------------------------------------------
 *
 * test/src/colocations_utils.c
 *
 * This file contains functions to test co-location functionality
 * within Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "catalog/pg_type.h"

#include "distributed/colocation_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/utils/array_type.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(get_table_colocation_id);
PG_FUNCTION_INFO_V1(tables_colocated);
PG_FUNCTION_INFO_V1(shards_colocated);
PG_FUNCTION_INFO_V1(get_colocated_table_array);
PG_FUNCTION_INFO_V1(find_shard_interval_index);


/*
 * get_table_colocation_id returns colocation id of given distributed table.
 */
Datum
get_table_colocation_id(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	uint32 colocationId = TableColocationId(distributedTableId);

	PG_RETURN_INT32(colocationId);
}


/*
 * tables_colocated checks if given two tables are co-located or not. If they are
 * co-located, this function returns true.
 */
Datum
tables_colocated(PG_FUNCTION_ARGS)
{
	Oid leftDistributedTableId = PG_GETARG_OID(0);
	Oid rightDistributedTableId = PG_GETARG_OID(1);
	bool tablesColocated = TablesColocated(leftDistributedTableId,
										   rightDistributedTableId);

	PG_RETURN_BOOL(tablesColocated);
}


/*
 * shards_colocated checks if given two shards are co-located or not. If they are
 * co-located, this function returns true.
 */
Datum
shards_colocated(PG_FUNCTION_ARGS)
{
	uint32 leftShardId = PG_GETARG_UINT32(0);
	uint32 rightShardId = PG_GETARG_UINT32(1);
	ShardInterval *leftShard = LoadShardInterval(leftShardId);
	ShardInterval *rightShard = LoadShardInterval(rightShardId);

	bool shardsColocated = ShardsColocated(leftShard, rightShard);

	PG_RETURN_BOOL(shardsColocated);
}


/*
 * get_colocated_tables_array returns array of table oids which are co-located with given
 * distributed table.
 */
Datum
get_colocated_table_array(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);

	List *colocatedTableList = ColocatedTableList(distributedTableId);
	int colocatedTableCount = list_length(colocatedTableList);
	Datum *colocatedTablesDatumArray = palloc0(colocatedTableCount * sizeof(Datum));
	Oid arrayTypeId = OIDOID;
	int colocatedTableIndex = 0;

	Oid colocatedTableId = InvalidOid;
	foreach_declared_oid(colocatedTableId, colocatedTableList)
	{
		Datum colocatedTableDatum = ObjectIdGetDatum(colocatedTableId);

		colocatedTablesDatumArray[colocatedTableIndex] = colocatedTableDatum;
		colocatedTableIndex++;
	}

	ArrayType *colocatedTablesArrayType = DatumArrayToArrayType(colocatedTablesDatumArray,
																colocatedTableCount,
																arrayTypeId);

	PG_RETURN_ARRAYTYPE_P(colocatedTablesArrayType);
}


/*
 * find_shard_interval_index finds index of given shard in sorted shard interval list.
 */
Datum
find_shard_interval_index(PG_FUNCTION_ARGS)
{
	uint32 shardId = PG_GETARG_UINT32(0);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	uint32 shardIndex = ShardIndex(shardInterval);

	PG_RETURN_INT32(shardIndex);
}
