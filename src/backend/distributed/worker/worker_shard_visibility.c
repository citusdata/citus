/*
 * worker_shard_visibility.c
 *
 * TODO: Write some meaningful comment
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/metadata_cache.h"
#include "distributed/worker_protocol.h"
#include "utils/lsyscache.h"


PG_FUNCTION_INFO_V1(relation_is_a_known_shard);


static bool RelationIsAKnownShard(Oid shardRelationId);


Datum
relation_is_a_known_shard(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	PG_RETURN_BOOL(RelationIsAKnownShard(relationId));
}

/*
 * Given a relationId, check whether it's a shard of any distributed table.
 * We can only do that in MX since we've metadata there. We can actually
 * implement for non-mx as well, but, there is currently no need for that.
 *
 * TODO: improve the comment
 * TODO: Make sure that we're not missing any edge cases with our
 * 		 implementation
 */
bool
RelationIsAKnownShard(Oid shardRelationId)
{
	int localGroupId = -1;
	char *shardRelationName = NULL;
	char *relationName = NULL;
	bool missingOk = true;
	uint64 shardId = INVALID_SHARD_ID;
	ShardInterval *shardInterval = NULL;
	Oid relationId = InvalidOid;
	char *shardIdString = NULL;
	int relationNameLength = 0;


	/*
	 * TODO: version check
	 */

	if (!OidIsValid(shardRelationId))
	{
		/* we cannot continue without a valid Oid */
		return false;
	}

	localGroupId = GetLocalGroupId();
	if (localGroupId == 0)
	{
		/*
		 * We're not interested in shards in the coordinator
		 * or non-mx worker nodes.
		*/
		return false;
	}

	shardRelationName = get_rel_name(shardRelationId);

	/* find the last underscore and increment for shardId string */
	shardIdString = strrchr(shardRelationName, SHARD_NAME_SEPARATOR);
	if (shardIdString == NULL)
	{
		return false;
	}

	relationNameLength = shardIdString - shardRelationName;
	relationName = strndup(shardRelationName, relationNameLength);

	relationId = RelnameGetRelid(relationName);
	if (!OidIsValid(relationId))
	{
		/* there is no such relation */
		return false;
	}

	if (!IsDistributedTable(relationId))
	{
		/* we're  obviously only interested in distributed tables */
		return false;
	}

	shardId = ExtractShardId(shardRelationName, missingOk);
	if (shardId == INVALID_SHARD_ID)
	{
		/*
		 * The format of the table name does not align with
		 * our shard name definition.
		 */
		return false;
	}

	/*
	 * At this point we're sure that this is a shard of a
	 * distributed table.
	 */
	shardInterval = LoadShardInterval(shardId);
	if (shardInterval->relationId == relationId)
	{
		return true;
	}

	return false;
}
