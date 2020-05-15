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
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/colocation_utils.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/relation_access_tracking.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"


/* Config variables managed via guc.c */
bool EnforceForeignKeyRestrictions = true;

#define PARALLEL_MODE_FLAG_OFFSET 3

/* simply set parallel bits as defined below for select, dml and ddl */
#define PARALLEL_ACCESS_MASK (int) (0 | \
									(1 << (PLACEMENT_ACCESS_SELECT + \
										   PARALLEL_MODE_FLAG_OFFSET)) | \
									(1 << (PLACEMENT_ACCESS_DML + \
										   PARALLEL_MODE_FLAG_OFFSET)) | \
									(1 << (PLACEMENT_ACCESS_DDL + \
										   PARALLEL_MODE_FLAG_OFFSET)))


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


/* functions related to access recording */
static void RecordRelationAccessBase(Oid relationId, ShardPlacementAccessType accessType);
static void RecordPlacementAccessToCache(Oid relationId,
										 ShardPlacementAccessType accessType);
static void RecordRelationParallelSelectAccessForTask(Task *task);
static void RecordRelationParallelModifyAccessForTask(Task *task);
static void RecordRelationParallelDDLAccessForTask(Task *task);
static RelationAccessMode GetRelationAccessMode(Oid relationId,
												ShardPlacementAccessType accessType);
static void RecordParallelRelationAccess(Oid relationId, ShardPlacementAccessType
										 placementAccess);
static void RecordParallelRelationAccessToCache(Oid relationId,
												ShardPlacementAccessType placementAccess);

/* functions related to access conflict checks */
static char * PlacementAccessTypeToText(ShardPlacementAccessType accessType);
static void CheckConflictingRelationAccesses(Oid relationId,
											 ShardPlacementAccessType accessType);
static bool HoldsConflictingLockWithReferencingRelations(Oid relationId,
														 ShardPlacementAccessType
														 placementAccess,
														 Oid *conflictingRelationId,
														 ShardPlacementAccessType *
														 conflictingAccessMode);
static void CheckConflictingParallelRelationAccesses(Oid relationId,
													 ShardPlacementAccessType
													 accessType);
static bool HoldsConflictingLockWithReferencedRelations(Oid relationId,
														ShardPlacementAccessType
														placementAccess,
														Oid *conflictingRelationId,
														ShardPlacementAccessType *
														conflictingAccessMode);


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

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(RelationAccessHashKey);
	info.entrysize = sizeof(RelationAccessHashEntry);
	info.hash = tag_hash;
	info.hcxt = ConnectionContext;
	uint32 hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	RelationAccessHash = hash_create("citus connection cache (relationid)",
									 8, &info, hashFlags);
}


/*
 * RecordRelationAccessIfReferenceTable marks the relation accessed if it is a
 * reference relation.
 *
 * The function is a wrapper around RecordRelationAccessBase().
 */
void
RecordRelationAccessIfReferenceTable(Oid relationId, ShardPlacementAccessType accessType)
{
	if (!ShouldRecordRelationAccess())
	{
		return;
	}

	/*
	 * We keep track of relation accesses for the purposes of foreign keys to
	 * reference tables. So, other distributed tables are not relevant for now.
	 * Additionally, partitioned tables with lots of partitions might require
	 * recursively calling RecordRelationAccessBase(), so becareful about
	 * removing this check.
	 */
	if (PartitionMethod(relationId) != DISTRIBUTE_BY_NONE)
	{
		return;
	}

	RecordRelationAccessBase(relationId, accessType);
}


/*
 * PlacementAccessTypeToText converts ShardPlacementAccessType to
 * text representation.
 */
static char *
PlacementAccessTypeToText(ShardPlacementAccessType accessType)
{
	switch (accessType)
	{
		case PLACEMENT_ACCESS_SELECT:
		{
			return "SELECT";
			break;
		}

		case PLACEMENT_ACCESS_DML:
		{
			return "DML";
		}

		case PLACEMENT_ACCESS_DDL:
		{
			return "DDL";
		}

		default:
		{
			return "None";
			break;
		}
	}
}


/*
 * RecordRelationAccessBase associates the access to the distributed relation. The
 * function takes partitioned relations into account as well.
 *
 * We implemented this function to prevent accessing placement metadata during
 * recursive calls of the function itself (e.g., avoid
 * RecordRelationAccessBase()).
 */
static void
RecordRelationAccessBase(Oid relationId, ShardPlacementAccessType accessType)
{
	/*
	 * We call this only for reference tables, and we don't support partitioned
	 * reference tables.
	 */
	Assert(!PartitionedTable(relationId) && !PartitionTable(relationId));

	/* make sure that this is not a conflicting access */
	CheckConflictingRelationAccesses(relationId, accessType);

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
	bool found = false;

	hashKey.relationId = relationId;

	RelationAccessHashEntry *hashEntry = hash_search(RelationAccessHash, &hashKey,
													 HASH_ENTER, &found);
	if (!found)
	{
		hashEntry->relationAccessMode = 0;
	}

	/* set the bit representing the access type */
	hashEntry->relationAccessMode |= (1 << (accessType));
}


/*
 * RecordParallelRelationAccessForTaskList gets a task list and records
 * the necessary parallel relation accesses for the task list.
 *
 * This function is used to enforce foreign keys from distributed
 * tables to reference tables.
 */
void
RecordParallelRelationAccessForTaskList(List *taskList)
{
	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
	{
		/* sequential mode prevents parallel access */
		return;
	}

	if (list_length(taskList) < 2)
	{
		/* single shard task doesn't mean parallel access in our definition */
		return;
	}

	/*
	 * Since all the tasks in a task list is expected to operate on the same
	 * distributed table(s), we only need to process the first task.
	 */
	Task *firstTask = linitial(taskList);

	if (firstTask->taskType == READ_TASK)
	{
		RecordRelationParallelSelectAccessForTask(firstTask);
	}
	else if (firstTask->taskType == MODIFY_TASK)
	{
		if (firstTask->rowValuesLists != NIL)
		{
			/*
			 * We always run multi-row INSERTs in a sequential
			 * mode (hard-coded). Thus, we do not mark as parallel
			 * access even if the prerequisites hold.
			 */
		}
		else
		{
			/*
			 * We prefer to mark with all remaining multi-shard modifications
			 * with both modify and select accesses.
			 */
			RecordRelationParallelModifyAccessForTask(firstTask);
			RecordRelationParallelSelectAccessForTask(firstTask);
		}
	}
	else
	{
		RecordRelationParallelDDLAccessForTask(firstTask);
	}
}


/*
 * RecordRelationParallelSelectAccessForTask goes over all the relations
 * in the relationShardList and records the select access per each table.
 */
static void
RecordRelationParallelSelectAccessForTask(Task *task)
{
	Oid lastRelationId = InvalidOid;

	/* no point in recoding accesses in non-transaction blocks, skip the loop */
	if (!ShouldRecordRelationAccess())
	{
		return;
	}

	List *relationShardList = task->relationShardList;

	RelationShard *relationShard = NULL;
	foreach_ptr(relationShard, relationShardList)
	{
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
static void
RecordRelationParallelModifyAccessForTask(Task *task)
{
	List *relationShardList = NULL;
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
		RelationShard *relationShard = NULL;
		foreach_ptr(relationShard, relationShardList)
		{
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
static void
RecordRelationParallelDDLAccessForTask(Task *task)
{
	List *relationShardList = task->relationShardList;
	Oid lastRelationId = InvalidOid;

	RelationShard *relationShard = NULL;
	foreach_ptr(relationShard, relationShardList)
	{
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

	if (task->anchorShardId != INVALID_SHARD_ID)
	{
		RecordParallelDDLAccess(RelationIdForShard(task->anchorShardId));
	}
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

	/* act accordingly if it's a conflicting access */
	CheckConflictingParallelRelationAccesses(relationId, placementAccess);

	/* If a relation is partitioned, record accesses to all of its partitions as well. */
	if (PartitionedTable(relationId))
	{
		List *partitionList = PartitionList(relationId);

		Oid partitionOid = InvalidOid;
		foreach_oid(partitionOid, partitionList)
		{
			/* recursively record all relation accesses of its partitions */
			RecordParallelRelationAccess(partitionOid, placementAccess);
		}
	}
	else if (PartitionTable(relationId))
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
	bool found = false;

	hashKey.relationId = relationId;

	RelationAccessHashEntry *hashEntry = hash_search(RelationAccessHash, &hashKey,
													 HASH_ENTER, &found);
	if (!found)
	{
		hashEntry->relationAccessMode = 0;
	}

	/* set the bit representing the access type */
	hashEntry->relationAccessMode |= (1 << (placementAccess));

	/* set the bit representing access mode */
	int parallelRelationAccessBit = placementAccess + PARALLEL_MODE_FLAG_OFFSET;
	hashEntry->relationAccessMode |= (1 << parallelRelationAccessBit);
}


/*
 * ParallelQueryExecutedInTransaction returns true if any parallel query
 * is executed in the current transaction.
 */
bool
ParallelQueryExecutedInTransaction(void)
{
	HASH_SEQ_STATUS status;

	if (!ShouldRecordRelationAccess() || RelationAccessHash == NULL)
	{
		return false;
	}

	hash_seq_init(&status, RelationAccessHash);

	RelationAccessHashEntry *hashEntry = (RelationAccessHashEntry *) hash_seq_search(
		&status);
	while (hashEntry != NULL)
	{
		int relationAccessMode = hashEntry->relationAccessMode;
		if ((relationAccessMode & PARALLEL_ACCESS_MASK))
		{
			hash_seq_term(&status);
			return true;
		}

		hashEntry = (RelationAccessHashEntry *) hash_seq_search(&status);
	}

	return false;
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
	bool found = false;
	int parallelRelationAccessBit = accessType + PARALLEL_MODE_FLAG_OFFSET;

	/* no point in getting the mode when not inside a transaction block */
	if (!ShouldRecordRelationAccess())
	{
		return RELATION_NOT_ACCESSED;
	}

	hashKey.relationId = relationId;

	RelationAccessHashEntry *hashEntry = hash_search(RelationAccessHash, &hashKey,
													 HASH_FIND, &found);
	if (!found)
	{
		/* relation not accessed at all */
		return RELATION_NOT_ACCESSED;
	}


	int relationAcessMode = hashEntry->relationAccessMode;
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
		return RELATION_REFERENCE_ACCESSED;
	}
}


/*
 * ShouldRecordRelationAccess returns true when we should keep track
 * of the relation accesses.
 *
 * In many cases, we'd only need IsMultiStatementTransaction(), however, for some
 * cases such as CTEs, where Citus uses the same connections accross multiple queries,
 * we should still record the relation accesses even not inside an explicit transaction
 * block. Thus, keeping track of the relation accesses inside coordinated transactions
 * is also required.
 */
bool
ShouldRecordRelationAccess()
{
	if (EnforceForeignKeyRestrictions &&
		(IsMultiStatementTransaction() || InCoordinatedTransaction()))
	{
		return true;
	}

	return false;
}


/*
 * CheckConflictingRelationAccesses is mostly a wrapper around
 * HoldsConflictingLockWithReferencingRelations(). We're only interested in accesses
 * to reference tables that are referenced via a foreign constraint by a
 * hash distributed tables.
 */
static void
CheckConflictingRelationAccesses(Oid relationId, ShardPlacementAccessType accessType)
{
	Oid conflictingReferencingRelationId = InvalidOid;
	ShardPlacementAccessType conflictingAccessType = PLACEMENT_ACCESS_SELECT;

	if (!EnforceForeignKeyRestrictions || !IsCitusTable(relationId))
	{
		return;
	}

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	if (!(cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE &&
		  cacheEntry->referencingRelationsViaForeignKey != NIL))
	{
		return;
	}

	if (HoldsConflictingLockWithReferencingRelations(relationId, accessType,
													 &conflictingReferencingRelationId,
													 &conflictingAccessType))
	{
		char *relationName = get_rel_name(relationId);
		char *conflictingRelationName = get_rel_name(conflictingReferencingRelationId);

		char *accessTypeText = PlacementAccessTypeToText(accessType);
		char *conflictingAccessTypeText =
			PlacementAccessTypeToText(conflictingAccessType);

		/*
		 * Relation could already be dropped if the accessType is DDL and the
		 * command that we were executing were a DROP command. In that case,
		 * as this function is executed via DROP trigger, standard_ProcessUtility
		 * had already dropped the table from PostgreSQL's perspective. Hence, it
		 * returns NULL pointer for the name of the relation.
		 */
		if (relationName == NULL)
		{
			ereport(ERROR, (errmsg("cannot execute %s on reference table because "
								   "there was a parallel %s access to distributed table "
								   "\"%s\" in the same transaction",
								   accessTypeText, conflictingAccessTypeText,
								   conflictingRelationName),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else
		{
			ereport(ERROR, (errmsg(
								"cannot execute %s on reference table \"%s\" because "
								"there was a parallel %s access to distributed table "
								"\"%s\" in the same transaction",
								accessTypeText, relationName,
								conflictingAccessTypeText,
								conflictingRelationName),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
	}
	else if (cacheEntry->referencingRelationsViaForeignKey != NIL &&
			 accessType > PLACEMENT_ACCESS_SELECT)
	{
		char *relationName = get_rel_name(relationId);

		if (ParallelQueryExecutedInTransaction())
		{
			/*
			 * If there has already been a parallel query executed, the sequential mode
			 * would still use the already opened parallel connections to the workers,
			 * thus contradicting our purpose of using sequential mode.
			 */
			ereport(ERROR, (errmsg("cannot modify reference table \"%s\" because there "
								   "was a parallel operation on a distributed table",
								   relationName),
							errdetail("When there is a foreign key to a reference "
									  "table, Citus needs to perform all operations "
									  "over a single connection per node to ensure "
									  "consistency."),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else if (MultiShardConnectionType == PARALLEL_CONNECTION)
		{
			/*
			 * We can still continue with multi-shard queries in sequential mode, so
			 * set it.
			 */
			ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
							 errdetail(
								 "Reference table \"%s\" is modified, which might lead "
								 "to data inconsistencies or distributed deadlocks via "
								 "parallel accesses to hash distributed tables due to "
								 "foreign keys. Any parallel modification to "
								 "those hash distributed tables in the same "
								 "transaction can only be executed in sequential query "
								 "execution mode", relationName)));

			/*
			 * Switching to sequential mode is admittedly confusing and, could be useless
			 * and less performant in some cases. However, if we do not switch to
			 * sequential mode at this point, we'd loose the opportunity to do so
			 * later when a parallel query is executed on the hash distributed relations
			 * that are referencing this reference table.
			 */
			SetLocalMultiShardModifyModeToSequential();
		}
	}
}


/*
 * CheckConflictingParallelRelationAccesses is mostly a wrapper around
 * HoldsConflictingLockWithReferencedRelations().  We're only interested in parallel
 * accesses to distributed tables that refers reference tables via foreign constraint.
 *
 */
static void
CheckConflictingParallelRelationAccesses(Oid relationId, ShardPlacementAccessType
										 accessType)
{
	Oid conflictingReferencingRelationId = InvalidOid;
	ShardPlacementAccessType conflictingAccessType = PLACEMENT_ACCESS_SELECT;

	if (!EnforceForeignKeyRestrictions || !IsCitusTable(relationId))
	{
		return;
	}

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	if (!(cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH &&
		  cacheEntry->referencedRelationsViaForeignKey != NIL))
	{
		return;
	}

	if (MultiShardConnectionType == PARALLEL_CONNECTION &&
		HoldsConflictingLockWithReferencedRelations(relationId, accessType,
													&conflictingReferencingRelationId,
													&conflictingAccessType))
	{
		char *relationName = get_rel_name(relationId);
		char *conflictingRelationName = get_rel_name(conflictingReferencingRelationId);

		char *accessTypeText = PlacementAccessTypeToText(accessType);
		char *conflictingAccessTypeText =
			PlacementAccessTypeToText(conflictingAccessType);

		if (ParallelQueryExecutedInTransaction())
		{
			/*
			 * If there has already been a parallel query executed, the sequential mode
			 * would still use the already opened parallel connections to the workers,
			 * thus contradicting our purpose of using sequential mode.
			 */
			ereport(ERROR, (errmsg("cannot execute parallel %s on table \"%s\" "
								   "after %s command on reference table "
								   "\"%s\" because there is a foreign key between "
								   "them and \"%s\" has been accessed in this transaction",
								   accessTypeText, relationName,
								   conflictingAccessTypeText, conflictingRelationName,
								   conflictingRelationName),
							errdetail("When there is a foreign key to a reference "
									  "table, Citus needs to perform all operations "
									  "over a single connection per node to ensure "
									  "consistency."),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else
		{
			ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
							 errdetail("cannot execute parallel %s on table \"%s\" "
									   "after %s command on reference table "
									   "\"%s\" because there is a foreign key between "
									   "them and \"%s\" has been accessed in this transaction",
									   accessTypeText, relationName,
									   conflictingAccessTypeText, conflictingRelationName,
									   conflictingRelationName)));

			SetLocalMultiShardModifyModeToSequential();
		}
	}
}


/*
 * HoldsConflictingLockWithReferencedRelations returns true if the input relationId is a
 * hash distributed table and it holds any conflicting locks with the reference tables that
 * the distributed table has a foreign key to the reference table.
 */
static bool
HoldsConflictingLockWithReferencedRelations(Oid relationId, ShardPlacementAccessType
											placementAccess,
											Oid *conflictingRelationId,

											ShardPlacementAccessType *
											conflictingAccessMode)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	Oid referencedRelation = InvalidOid;
	foreach_oid(referencedRelation, cacheEntry->referencedRelationsViaForeignKey)
	{
		/* we're only interested in foreign keys to reference tables */
		if (PartitionMethod(referencedRelation) != DISTRIBUTE_BY_NONE)
		{
			continue;
		}

		/*
		 * A select on a reference table could conflict with a DDL
		 * on a distributed table.
		 */
		RelationAccessMode selectMode = GetRelationSelectAccessMode(referencedRelation);
		if (placementAccess == PLACEMENT_ACCESS_DDL &&
			selectMode != RELATION_NOT_ACCESSED)
		{
			*conflictingRelationId = referencedRelation;
			*conflictingAccessMode = PLACEMENT_ACCESS_SELECT;

			return true;
		}

		/*
		 * Both DML and DDL operations on a reference table conflicts with
		 * any parallel operation on distributed tables.
		 */
		RelationAccessMode dmlMode = GetRelationDMLAccessMode(referencedRelation);
		if (dmlMode != RELATION_NOT_ACCESSED)
		{
			*conflictingRelationId = referencedRelation;
			*conflictingAccessMode = PLACEMENT_ACCESS_DML;

			return true;
		}

		RelationAccessMode ddlMode = GetRelationDDLAccessMode(referencedRelation);
		if (ddlMode != RELATION_NOT_ACCESSED)
		{
			*conflictingRelationId = referencedRelation;
			*conflictingAccessMode = PLACEMENT_ACCESS_DDL;

			return true;
		}
	}

	return false;
}


/*
 * HoldsConflictingLockWithReferencingRelations returns true when the input relationId is a
 * reference table and it holds any conflicting locks with the distributed tables where
 * the distributed table has a foreign key to the reference table.
 *
 * If returns true, the referencing relation and conflictingAccessMode are also set.
 */
static bool
HoldsConflictingLockWithReferencingRelations(Oid relationId, ShardPlacementAccessType
											 placementAccess, Oid *conflictingRelationId,
											 ShardPlacementAccessType *
											 conflictingAccessMode)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	bool holdsConflictingLocks = false;

	Assert(PartitionMethod(relationId) == DISTRIBUTE_BY_NONE);

	Oid referencingRelation = InvalidOid;
	foreach_oid(referencingRelation, cacheEntry->referencingRelationsViaForeignKey)
	{
		/*
		 * We're only interested in foreign keys to reference tables from
		 * hash distributed tables.
		 */
		if (!IsCitusTable(referencingRelation) ||
			PartitionMethod(referencingRelation) != DISTRIBUTE_BY_HASH)
		{
			continue;
		}

		/*
		 * Rules that we apply:
		 *      - SELECT on a reference might table conflict with
		 *        a previous parallel DDL on a distributed table
		 *      - DML on a reference table might conflict with
		 *        a previous parallel DML or DDL on a distributed
		 *        table
		 *      - DDL on a reference table might conflict with
		 *        a parellel SELECT, DML or DDL on a distributed
		 *        table
		 */
		if (placementAccess == PLACEMENT_ACCESS_SELECT)
		{
			RelationAccessMode ddlMode = GetRelationDDLAccessMode(referencingRelation);

			if (ddlMode == RELATION_PARALLEL_ACCESSED)
			{
				/* SELECT on a distributed table conflicts with DDL / TRUNCATE */
				holdsConflictingLocks = true;
				*conflictingAccessMode = PLACEMENT_ACCESS_DDL;
			}
		}
		else if (placementAccess == PLACEMENT_ACCESS_DML)
		{
			RelationAccessMode dmlMode = GetRelationDMLAccessMode(referencingRelation);

			if (dmlMode == RELATION_PARALLEL_ACCESSED)
			{
				holdsConflictingLocks = true;
				*conflictingAccessMode = PLACEMENT_ACCESS_DML;
			}

			RelationAccessMode ddlMode = GetRelationDDLAccessMode(referencingRelation);
			if (ddlMode == RELATION_PARALLEL_ACCESSED)
			{
				/* SELECT on a distributed table conflicts with DDL / TRUNCATE */
				holdsConflictingLocks = true;
				*conflictingAccessMode = PLACEMENT_ACCESS_DDL;
			}
		}
		else if (placementAccess == PLACEMENT_ACCESS_DDL)
		{
			RelationAccessMode selectMode = GetRelationSelectAccessMode(
				referencingRelation);
			if (selectMode == RELATION_PARALLEL_ACCESSED)
			{
				holdsConflictingLocks = true;
				*conflictingAccessMode = PLACEMENT_ACCESS_SELECT;
			}

			RelationAccessMode dmlMode = GetRelationDMLAccessMode(referencingRelation);
			if (dmlMode == RELATION_PARALLEL_ACCESSED)
			{
				holdsConflictingLocks = true;
				*conflictingAccessMode = PLACEMENT_ACCESS_DML;
			}

			RelationAccessMode ddlMode = GetRelationDDLAccessMode(referencingRelation);
			if (ddlMode == RELATION_PARALLEL_ACCESSED)
			{
				holdsConflictingLocks = true;
				*conflictingAccessMode = PLACEMENT_ACCESS_DDL;
			}
		}

		if (holdsConflictingLocks)
		{
			*conflictingRelationId = referencingRelation;

			return true;
		}
	}

	return false;
}
