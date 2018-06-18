/*-------------------------------------------------------------------------
 *
 * relation_access_tracking.c
 *
 *   Transaction access tracking for Citus. The functions in this file
 *   are intended to track the relation accesses within a transaction. The
 *   logic here is mostly useful when a reference table is referred by
 *   a distributed table via a foreign key. Whenever such a pair of tables
 *   are acccesed inside a transaction, Citus should detect and act
 *   accordingly.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/hash_helpers.h"
#include "distributed/multi_join_order.h"
#include "distributed/metadata_cache.h"
#include "distributed/relation_access_tracking.h"
#include "utils/hsearch.h"


#define PARALLEL_MODE_FLAG_OFFSET 3

/*
 * Hash table mapping relations to the
 *      (relationId) = (relationAccessType and relationAccessMode)
 *
 * RelationAccessHash is used to keep track of relation accesses types (e.g., select,
 * dml or ddl) along with access modes (e.g., no access, sequential access or
 * parallel access).
 *
 * We keep an integer per relation and use some of the bits to identify the access types
 * and access modes.
 *
 * We store the access types in the first 3 bits:
 *  - 0th bit is set for SELECT accesses to a relation
 *  - 1st bit is set for DML accesses to a relation
 *  - 2nd bit is set for DDL accesses to a relation
 *
 * and, access modes in the next 3 bits:
 *  - 3rd bit is set for PARALLEL SELECT accesses to a relation
 *  - 4th bit is set for PARALLEL DML accesses to a relation
 *  - 5th bit is set for PARALLEL DDL accesses to a relation
 *
 */
typedef struct RelationAccessHashKey
{
	Oid relationId;
} RelationAccessHashKey;

typedef struct RelationAccessHashEntry
{
	RelationAccessHashKey key;

	int relationAccessMode;
} RelationAccessHashEntry;

static HTAB *RelationAccessHash;

static RelationAccessMode GetRelationAccessMode(Oid relationId,
												ShardPlacementAccessType accessType);
static void RecordParallelRelationAccess(Oid relationId, ShardPlacementAccessType
										 placementAccess);


/*
 * Empty RelationAccessHash, without destroying the hash table itself.
 */
void
ResetRelationAccessHash()
{
	hash_delete_all(RelationAccessHash);
}


/*
 * Allocate RelationAccessHash.
 */
void
AllocateRelationAccessHash()
{
	HASHCTL info;
	uint32 hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(RelationAccessHashKey);
	info.entrysize = sizeof(RelationAccessHashEntry);
	info.hash = tag_hash;
	info.hcxt = ConnectionContext;
	hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	RelationAccessHash = hash_create("citus connection cache (relationid)",
									 8, &info, hashFlags);
}


/*
 * AssociatePlacementAccessWithRelation associates the placement access to the
 * distributed relation that the placement belongs to.
 */
void
AssociatePlacementAccessWithRelation(ShardPlacement *placement,
									 ShardPlacementAccessType accessType)
{
	uint64 shardId = placement->shardId;
	Oid relationId = RelationIdForShard(shardId);
	RelationAccessHashKey hashKey;
	RelationAccessHashEntry *hashEntry;
	bool found = false;

	hashKey.relationId = relationId;

	hashEntry = hash_search(RelationAccessHash, &hashKey, HASH_ENTER, &found);
	if (!found)
	{
		hashEntry->relationAccessMode = 0;
	}

	/* set the bit representing the access type */
	hashEntry->relationAccessMode |= (1 << (accessType));
}


/*
 * RecordRelationMultiShardSelectAccessForTask goes over all the relations
 * in the relationShardList and records the select access per each table.
 */
void
RecordRelationMultiShardSelectAccessForTask(Task *task)
{
	List *relationShardList = NIL;
	ListCell *relationShardCell = NULL;
	Oid lastRelationId = InvalidOid;

	/* no point in recoding accesses in non-transaction blocks, skip the loop */
	if (!IsTransactionBlock())
	{
		return;
	}

	relationShardList = task->relationShardList;

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);
		Oid currentRelationId = relationShard->relationId;

		/*
		 * An optimization, skip going to hash table if we've already
		 * recorded the relation.
		 */
		if (currentRelationId == lastRelationId)
		{
			continue;
		}

		RecordParallelSelectAccess(currentRelationId);

		lastRelationId = currentRelationId;
	}
}


/*
 * RecordRelationMultiShardModifyAccessForTask gets a task and records
 * the accesses. Note that the target relation is recorded with modify access
 * where as the subqueries inside the modify query is recorded with select
 * access.
 */
void
RecordRelationMultiShardModifyAccessForTask(Task *task)
{
	List *relationShardList = NULL;
	ListCell *relationShardCell = NULL;
	Oid lastRelationId = InvalidOid;

	/* no point in recoding accesses in non-transaction blocks, skip the loop */
	if (!IsTransactionBlock())
	{
		return;
	}

	/* anchor shard is always associated with modify access */
	RecordParallelModifyAccess(RelationIdForShard(task->anchorShardId));

	if (task->modifyWithSubquery)
	{
		relationShardList = task->relationShardList;
		foreach(relationShardCell, relationShardList)
		{
			RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);
			Oid currentRelationId = relationShard->relationId;

			/*
			 * An optimization, skip going to hash table if we've already
			 * recorded the relation.
			 */
			if (currentRelationId == lastRelationId)
			{
				continue;
			}

			RecordParallelSelectAccess(currentRelationId);

			lastRelationId = currentRelationId;
		}
	}
}


/*
 * RecordRelationMultiShardDDLAccessForTask marks all the relationShards
 * with parallel DDL access if exists. That case is valid for inter-shard
 * DDL commands such as foreign key creation. The function also records
 * the relation that anchorShardId belongs to.
 */
void
RecordRelationMultiShardDDLAccessForTask(Task *task)
{
	List *relationShardList = task->relationShardList;
	ListCell *relationShardCell = NULL;

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);

		RecordParallelDDLAccess(relationShard->relationId);
	}

	RecordParallelDDLAccess(RelationIdForShard(task->anchorShardId));
}


/*
 * RecordParallelSelectAccess is a wrapper around RecordParallelRelationAccess()
 */
void
RecordParallelSelectAccess(Oid relationId)
{
	RecordParallelRelationAccess(relationId, PLACEMENT_ACCESS_SELECT);
}


/*
 * RecordParallelModifyAccess is a wrapper around RecordParallelRelationAccess()
 */
void
RecordParallelModifyAccess(Oid relationId)
{
	RecordParallelRelationAccess(relationId, PLACEMENT_ACCESS_DML);
}


/*
 * RecordParallelDDLAccess is a wrapper around RecordParallelRelationAccess()
 */
void
RecordParallelDDLAccess(Oid relationId)
{
	RecordParallelRelationAccess(relationId, PLACEMENT_ACCESS_DDL);
}


/*
 * RecordParallelRelationAccess records the relation access mode as parallel
 * for the given access type (e.g., select, dml or ddl) in the RelationAccessHash.
 *
 * The function becomes no-op for non-transaction blocks
 */
static void
RecordParallelRelationAccess(Oid relationId, ShardPlacementAccessType placementAccess)
{
	RelationAccessHashKey hashKey;
	RelationAccessHashEntry *hashEntry;
	bool found = false;
	int multiShardAccessBit = 0;

	/* no point in recoding accesses in non-transaction blocks */
	if (!IsTransactionBlock())
	{
		return;
	}

	hashKey.relationId = relationId;

	hashEntry = hash_search(RelationAccessHash, &hashKey, HASH_ENTER, &found);
	if (!found)
	{
		hashEntry->relationAccessMode = 0;
	}

	/* set the bit representing the access type */
	hashEntry->relationAccessMode |= (1 << (placementAccess));

	/* set the bit representing access mode */
	multiShardAccessBit = placementAccess + PARALLEL_MODE_FLAG_OFFSET;
	hashEntry->relationAccessMode |= (1 << multiShardAccessBit);
}


/*
 * GetRelationSelectAccessMode is a wrapper around GetRelationAccessMode.
 */
RelationAccessMode
GetRelationSelectAccessMode(Oid relationId)
{
	return GetRelationAccessMode(relationId, PLACEMENT_ACCESS_SELECT);
}


/*
 * GetRelationDMLAccessMode is a wrapper around GetRelationAccessMode.
 */
RelationAccessMode
GetRelationDMLAccessMode(Oid relationId)
{
	return GetRelationAccessMode(relationId, PLACEMENT_ACCESS_DML);
}


/*
 * GetRelationDDLAccessMode is a wrapper around GetRelationAccessMode.
 */
RelationAccessMode
GetRelationDDLAccessMode(Oid relationId)
{
	return GetRelationAccessMode(relationId, PLACEMENT_ACCESS_DDL);
}


/*
 * GetRelationAccessMode returns the relation access mode (e.g., none, sequential
 * or parallel) for the given access type (e.g., select, dml or ddl).
 */
static RelationAccessMode
GetRelationAccessMode(Oid relationId, ShardPlacementAccessType accessType)
{
	RelationAccessHashKey hashKey;
	RelationAccessHashEntry *hashEntry;
	int relationAcessMode = 0;
	bool found = false;
	int multiShardAccessBit = accessType + PARALLEL_MODE_FLAG_OFFSET;

	/* no point in getting the mode when not inside a transaction block */
	if (!IsTransactionBlock())
	{
		return RELATION_NOT_ACCESSED;
	}

	hashKey.relationId = relationId;

	hashEntry = hash_search(RelationAccessHash, &hashKey, HASH_FIND, &found);
	if (!found)
	{
		/* relation not accessed at all */
		return RELATION_NOT_ACCESSED;
	}


	relationAcessMode = hashEntry->relationAccessMode;
	if (!(relationAcessMode & (1 << accessType)))
	{
		/* relation not accessed with the given access type */
		return RELATION_NOT_ACCESSED;
	}

	if (relationAcessMode & (1 << multiShardAccessBit))
	{
		return RELATION_PARALLEL_ACCESSED;
	}
	else
	{
		return RELATION_SEQUENTIAL_ACCESSED;
	}
}
