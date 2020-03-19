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

#include "distributed/relay_utility.h"
#include "distributed/shard_utils.h"

/*
 * GetShardOid returns the oid of the shard from the given distributed relation
 * with the shardid.
 */
Oid
GetShardOid(Oid distRelId, uint64 shardId)
{
	char *relationName = get_rel_name(distRelId);
	AppendShardIdToName(&relationName, shardId);

	Oid schemaId = get_rel_namespace(distRelId);

	return get_relname_relid(relationName, schemaId);
}
