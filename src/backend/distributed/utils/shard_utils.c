/*-------------------------------------------------------------------------
 *
 * shard_utils.c
 *
 * This file contains functions to perform useful operations on shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"

#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/relay_utility.h"
#include "distributed/shard_utils.h"

static int GetLargestShardId(void);

/*
 * GetTableLocalShardOid returns the oid of the shard from the given distributed
 * relation with the shardId.
 */
Oid
GetTableLocalShardOid(Oid citusTableOid, uint64 shardId)
{
	const char *citusTableName = get_rel_name(citusTableOid);

	Assert(citusTableName != NULL);

	/* construct shard relation name */
	char *shardRelationName = pstrdup(citusTableName);
	AppendShardIdToName(&shardRelationName, shardId);

	Oid citusTableSchemaOid = get_rel_namespace(citusTableOid);

	Oid shardRelationOid = get_relname_relid(shardRelationName, citusTableSchemaOid);

	return shardRelationOid;
}


/*
 * GetLongestShardName is a utility function that returns the name of the shard of a
 * table that has the longest name in terms of number of characters.
 *
 * Both the Oid and name of the table are required so we can create longest shard names
 * after a RENAME.
 */
char *
GetLongestShardName(Oid citusTableOid, char *finalRelationName)
{
	char *longestShardName = pstrdup(finalRelationName);
	ShardInterval *shardInterval = LoadShardIntervalWithLongestShardName(citusTableOid);
	AppendShardIdToName(&longestShardName, shardInterval->shardId);

	return longestShardName;
}


/*
 * GetLongestShardNameForLocalPartition is a utility function that creates a hypothetical shard
 * name for a partition table that is not distributed yet.
 */
char *
GetLongestShardNameForLocalPartition(Oid parentTableOid, char *partitionRelationName)
{
	char *longestShardName = pstrdup(partitionRelationName);
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(parentTableOid);
	int shardIntervalCount = cacheEntry->shardIntervalArrayLength;
	int newShardId = GetLargestShardId() + shardIntervalCount;
	AppendShardIdToName(&longestShardName, newShardId);

	return longestShardName;
}


/*
 * GetLargestShardId returns the biggest shard id, and returns a 10^6 in case of failure
 * to get the last value from the sequence.
 */
int
GetLargestShardId()
{
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	text *sequenceName = cstring_to_text(SHARDID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	volatile int64 largestShardId = 0;

	/*
	 * pg_sequence_last_value() returns NULL if the sequence value is not yet used.
	 * DirectFunctionCall1() gives an ERROR message on NULL return values, and that's why we
	 * need a PG_TRY block.
	 */
	PG_TRY();
	{
		Datum lastShardIdDatum = DirectFunctionCall1(pg_sequence_last_value,
													 sequenceIdDatum);
		largestShardId = DatumGetInt64(lastShardIdDatum);
	}
	PG_CATCH();
	{
		/* assume that we have a shardId with 7 digits */
		largestShardId = 1000000;
	}
	PG_END_TRY();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return largestShardId;
}
