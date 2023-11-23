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
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_split.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/utils/array_type.h"
#include "distributed/utils/distribution_column_map.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_split_shard_by_split_points);

/*
 * citus_split_shard_by_split_points(shard_id bigint, split_points text[], node_ids integer[], shard_transfer_mode citus.shard_transfer_mode)
 * Split source shard into multiple shards using the given split points.
 * 'shard_id' is the id of source shard to split.
 * 'split_points' is an array that represents the split points.
 * 'node_ids' is an array that represents the placement node ids of the new shards.
 * 'shard_transfer_mode citus.shard_transfer_mode' is the transfer mode for split.
 */
Datum
citus_split_shard_by_split_points(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	uint64 shardIdToSplit = DatumGetUInt64(PG_GETARG_DATUM(0));

	ArrayType *splitPointsArrayObject = PG_GETARG_ARRAYTYPE_P(1);
	List *shardSplitPointsList = TextArrayTypeToIntegerList(splitPointsArrayObject);

	ArrayType *nodeIdsArrayObject = PG_GETARG_ARRAYTYPE_P(2);
	List *nodeIdsForPlacementList = IntegerArrayTypeToList(nodeIdsArrayObject);

	Oid shardTransferModeOid = PG_GETARG_OID(3);
	SplitMode shardSplitMode = LookupSplitMode(shardTransferModeOid);

	DistributionColumnMap *distributionColumnOverrides = NULL;
	List *sourceColocatedShardIntervalList = NIL;
	SplitShard(
		shardSplitMode,
		SHARD_SPLIT_API,
		shardIdToSplit,
		shardSplitPointsList,
		nodeIdsForPlacementList,
		distributionColumnOverrides,
		sourceColocatedShardIntervalList,
		INVALID_COLOCATION_ID);

	PG_RETURN_VOID();
}


/*
 * LookupSplitMode maps the oids of citus.shard_transfer_mode to SplitMode enum.
 */
SplitMode
LookupSplitMode(Oid shardTransferModeOid)
{
	SplitMode shardSplitMode = BLOCKING_SPLIT;

	Datum enumLabelDatum = DirectFunctionCall1(enum_out, shardTransferModeOid);
	char *enumLabel = DatumGetCString(enumLabelDatum);

	/* Extend with other modes as we support them */
	if (strncmp(enumLabel, "block_writes", NAMEDATALEN) == 0)
	{
		shardSplitMode = BLOCKING_SPLIT;
	}
	else if (strncmp(enumLabel, "force_logical", NAMEDATALEN) == 0)
	{
		shardSplitMode = NON_BLOCKING_SPLIT;
	}
	else if (strncmp(enumLabel, "auto", NAMEDATALEN) == 0)
	{
		shardSplitMode = AUTO_SPLIT;
	}
	else
	{
		/* We will not get here as postgres will validate the enum value. */
		ereport(ERROR, (errmsg(
							"Invalid shard tranfer mode: '%s'. Expected split mode is 'block_writes/auto/force_logical'.",
							enumLabel)));
	}

	return shardSplitMode;
}
