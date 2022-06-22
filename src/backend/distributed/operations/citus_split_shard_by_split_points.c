/*-------------------------------------------------------------------------
 *
 * citus_split_shard_by_split_points.c
 *
 * This file contains functions to split a shard.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
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

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_split_shard_by_split_points);

static SplitMode LookupSplitMode(Oid shardSplitModeOid);

/*
 * citus_split_shard_by_split_points(shard_id bigint, split_points integer[], node_ids integer[])
 * Split source shard into multiple shards using the given split points.
 * 'shard_id' is the id of source shard to split.
 * 'split_points' is an array that represents the split points.
 * 'node_ids' is an array that represents the placement node ids of the new shards.
 * 'split_mode citus.split_mode' is the mode of split.
 */
Datum
citus_split_shard_by_split_points(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	uint64 shardIdToSplit = DatumGetUInt64(PG_GETARG_DATUM(0));

	ArrayType *splitPointsArrayObject = PG_GETARG_ARRAYTYPE_P(1);
	List *shardSplitPointsList = TextArrayTypeToIntegerList(splitPointsArrayObject,
															INT4OID);

	ArrayType *nodeIdsArrayObject = PG_GETARG_ARRAYTYPE_P(2);
	List *nodeIdsForPlacementList = IntegerArrayTypeToList(nodeIdsArrayObject);

	Oid shardSplitModeOid = PG_GETARG_OID(3);
	SplitMode shardSplitMode = LookupSplitMode(shardSplitModeOid);

	SplitShard(
		shardSplitMode,
		SHARD_SPLIT_API,
		shardIdToSplit,
		shardSplitPointsList,
		nodeIdsForPlacementList);

	PG_RETURN_VOID();
}


/*
 * LookupSplitMode maps the oids of citus.shard_split_mode enum
 * values to an enum.
 */
SplitMode
LookupSplitMode(Oid shardSplitModeOid)
{
	SplitMode shardSplitMode = BLOCKING_SPLIT;

	Datum enumLabelDatum = DirectFunctionCall1(enum_out, shardSplitModeOid);
	char *enumLabel = DatumGetCString(enumLabelDatum);

	if (strncmp(enumLabel, "blocking", NAMEDATALEN) == 0)
	{
		shardSplitMode = BLOCKING_SPLIT;
	}

	/* Extend with other modes as we support them */
	else
	{
		ereport(ERROR, (errmsg("Invalid label for enum: %s", enumLabel)));
	}

	return shardSplitMode;
}
