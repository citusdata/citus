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
#include "distributed/metadata_cache.h"
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
 * GetLocalShardOidOfTableWithoutDistributionKey returns OID of the local shard
 * of the given citus table without distribution keys.
 *
 * Callers of this function must ensure that noDistKeyTableOid is owned either
 * by a reference table or a citus local table. Note that this function returns
 * InvalidOid if the given table does not have a local shard.
 */
Oid
GetLocalShardOidOfTableWithoutDistributionKey(Oid noDistKeyTableOid)
{
	const CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(noDistKeyTableOid);

	/* given OID should belong to a valid reference table */
	Assert(cacheEntry != NULL && CitusTableWithoutDistributionKey(
			   cacheEntry->partitionMethod));

	const ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[0];
	uint64 referenceTableShardId = shardInterval->shardId;

	return GetTableLocalShardOid(noDistKeyTableOid, referenceTableShardId);
}
