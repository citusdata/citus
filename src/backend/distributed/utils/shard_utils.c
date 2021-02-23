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

#include "utils/lsyscache.h"
#include "distributed/metadata_utility.h"
#include "distributed/relay_utility.h"
#include "distributed/shard_utils.h"

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
