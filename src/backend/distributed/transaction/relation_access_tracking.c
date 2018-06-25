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
#include "distributed/colocation_utils.h"
#include "distributed/hash_helpers.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
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


static void RecordRelationAccess(Oid relationId, ShardPlacementAccessType accessType);
static void RecordPlacementAccessToCache(Oid relationId,
										 ShardPlacementAccessType accessType);
static RelationAccessMode GetRelationAccessMode(Oid relationId,
												ShardPlacementAccessType accessType);
static void RecordParallelRelationAccess(Oid relationId, ShardPlacementAccessType
										 placementAccess);
static void RecordParallelRelationAccessToCache(Oid relationId,
												ShardPlacementAccessType placementAccess);


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
AllocateRelationAccessHash(void)
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
	Oid relationId = InvalidOid;
	uint64 shardId = INVALID_SHARD_ID;

	if (!ShouldRecordRelationAccess())
	{
		return;
	}

	shardId = placement->shardId;
	relationId = RelationIdForShard(shardId);

	RecordRelationAccess(relationId, accessType);
}


/*
 * RecordRelationAccess associates the access to the distributed relation. The
 * function takes partitioned relations into account as well.
 *
 * We implemented this function to prevent accessing placement metadata during
 * recursive calls of the function itself (e.g., avoid
 * AssociatePlacementAccessWithRelation()).
 */
static void
RecordRelationAccess(Oid relationId, ShardPlacementAccessType accessType)
{
	/*
	 * If a relation is partitioned, record accesses to all of its partitions as well.
	 * We prefer to use PartitionedTableNoLock() because at this point the necessary
	 * locks on the relation has already been acquired.
	 */
	if (PartitionedTableNoLock(relationId))
	{
		List *partitionList = PartitionList(relationId);
		ListCell *partitionCell = NULL;

		foreach(partitionCell, partitionList)
		{
			Oid partitionOid = lfirst_oid(partitionCell);

			/*
			 * During create_distributed_table, the partitions may not
			 * have been created yet and so there are no placements yet.
			 * We're already going to register them when we distribute
			 * the partitions.
			 */
			if (!IsDistributedTable(partitionOid))
			{
				continue;
			}

			/* recursively call the function to cover multi-level partitioned tables */
			RecordRelationAccess(partitionOid, accessType);
		}
	}
	else if (PartitionTableNoLock(relationId))
	{
		Oid parentOid = PartitionParentOid(relationId);

		/* record the parent */
		RecordPlacementAccessToCache(parentOid, accessType);
	}

	/* always record the relation that is being considered */
	RecordPlacementAccessToCache(relationId, accessType);
}


/*
 * RecordPlacementAccessToCache is a utility function which saves the given
 * relation id's access to the RelationAccessHash.
 */
static void
RecordPlacementAccessToCache(Oid relationId, ShardPlacementAccessType accessType)
{
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
 * RecordRelationParallelSelectAccessForTask goes over all the relations
 * in the relationShardList and records the select access per each table.
 */
void
RecordRelationParallelSelectAccessForTask(Task *task)
{
	List *relationShardList = NIL;
	ListCell *relationShardCell = NULL;
	Oid lastRelationId = InvalidOid;

	/* no point in recoding accesses in non-transaction blocks, skip the loop */
	if (!ShouldRecordRelationAccess())
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
 * RecordRelationParallelModifyAccessForTask gets a task and records
 * the accesses. Note that the target relation is recorded with modify access
 * where as the subqueries inside the modify query is recorded with select
 * access.
 */
void
RecordRelationParallelModifyAccessForTask(Task *task)
{
	List *relationShardList = NULL;
	ListCell *relationShardCell = NULL;
	Oid lastRelationId = InvalidOid;

	/* no point in recoding accesses in non-transaction blocks, skip the loop */
	if (!ShouldRecordRelationAccess())
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
 * RecordRelationParallelDDLAccessForTask marks all the relationShards
 * with parallel DDL access if exists. That case is valid for inter-shard
 * DDL commands such as foreign key creation. The function also records
 * the relation that anchorShardId belongs to.
 */
void
RecordRelationParallelDDLAccessForTask(Task *task)
{
	List *relationShardList = task->relationShardList;
	ListCell *relationShardCell = NULL;
	Oid lastRelationId = InvalidOid;

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

		RecordParallelDDLAccess(currentRelationId);
		lastRelationId = currentRelationId;
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
 * The function also takes partitions and partitioned tables into account.
 */
static void
RecordParallelRelationAccess(Oid relationId, ShardPlacementAccessType placementAccess)
{
	if (!ShouldRecordRelationAccess())
	{
		return;
	}

	/*
	 * If a relation is partitioned, record accesses to all of its partitions as well.
	 * We prefer to use PartitionedTableNoLock() because at this point the necessary
	 * locks on the relation has already been acquired.
	 */
	if (PartitionedTableNoLock(relationId))
	{
		List *partitionList = PartitionList(relationId);
		ListCell *partitionCell = NULL;

		foreach(partitionCell, partitionList)
		{
			Oid partitionOid = lfirst_oid(partitionCell);

			/* recursively record all relation accesses of its partitions */
			RecordParallelRelationAccess(partitionOid, placementAccess);
		}
	}
	else if (PartitionTableNoLock(relationId))
	{
		Oid parentOid = PartitionParentOid(relationId);

		/* only record the parent */
		RecordParallelRelationAccessToCache(parentOid, placementAccess);
	}

	RecordParallelRelationAccessToCache(relationId, placementAccess);
}


/*
 * RecordParallelRelationAccessToCache is a utility function which saves the given
 * relation id's access to the RelationAccessHash.
 */
static void
RecordParallelRelationAccessToCache(Oid relationId,
									ShardPlacementAccessType placementAccess)
{
	RelationAccessHashKey hashKey;
	RelationAccessHashEntry *hashEntry;
	bool found = false;
	int parallelRelationAccessBit = 0;

	hashKey.relationId = relationId;

	hashEntry = hash_search(RelationAccessHash, &hashKey, HASH_ENTER, &found);
	if (!found)
	{
		hashEntry->relationAccessMode = 0;
	}

	/* set the bit representing the access type */
	hashEntry->relationAccessMode |= (1 << (placementAccess));

	/* set the bit representing access mode */
	parallelRelationAccessBit = placementAccess + PARALLEL_MODE_FLAG_OFFSET;
	hashEntry->relationAccessMode |= (1 << parallelRelationAccessBit);
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
	int parallelRelationAccessBit = accessType + PARALLEL_MODE_FLAG_OFFSET;

	/* no point in getting the mode when not inside a transaction block */
	if (!ShouldRecordRelationAccess())
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

	if (relationAcessMode & (1 << parallelRelationAccessBit))
	{
		return RELATION_PARALLEL_ACCESSED;
	}
	else
	{
		return RELATION_SEQUENTIAL_ACCESSED;
	}
}


/*
 * ShouldRecordRelationAccess returns true when we should keep track
 * of the relation accesses.
 *
 * In many cases, we'd only need IsTransactionBlock(), however, for some cases such as
 * CTEs, where Citus uses the same connections accross multiple queries, we should
 * still record the relation accesses even not inside an explicit transaction block.
 * Thus, keeping track of the relation accesses inside coordinated transactions is
 * also required.
 */
bool
ShouldRecordRelationAccess()
{
	if (IsTransactionBlock() || InCoordinatedTransaction())
	{
		return true;
	}

	return false;
}
