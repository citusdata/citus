/*-------------------------------------------------------------------------
 *
 * resource_lock.c
 *	  Locking Infrastructure for Citus.
 *
 * To avoid introducing a new type of locktag - that then could not be
 * displayed by core functionality - we reuse advisory locks. If we'd just
 * reused them directly we'd run into danger conflicting with user-defined
 * advisory locks, but luckily advisory locks only two values for 'field4' in
 * the locktag.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/tablecmds.h"
#include "distributed/colocation_utils.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/distributed_planner.h"
#include "distributed/relay_utility.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"


/* static definition and declarations */
struct LockModeToStringType
{
	LOCKMODE lockMode;
	const char *name;
};

/*
 * list of lock mode mappings, number of items need to be kept in sync
 * with lock_mode_to_string_map_count.
 */
static const struct LockModeToStringType lockmode_to_string_map[] = {
	{ NoLock, "NoLock" },
	{ AccessShareLock, "ACCESS SHARE" },
	{ RowShareLock, "ROW SHARE" },
	{ RowExclusiveLock, "ROW EXCLUSIVE" },
	{ ShareUpdateExclusiveLock, "SHARE UPDATE EXCLUSIVE" },
	{ ShareLock, "SHARE" },
	{ ShareRowExclusiveLock, "SHARE ROW EXCLUSIVE" },
	{ ExclusiveLock, "EXCLUSIVE" },
	{ AccessExclusiveLock, "ACCESS EXCLUSIVE" }
};
static const int lock_mode_to_string_map_count = sizeof(lockmode_to_string_map) /
												 sizeof(lockmode_to_string_map[0]);


/* local function forward declarations */
static LOCKMODE IntToLockMode(int mode);
static void LockReferencedReferenceShardResources(uint64 shardId, LOCKMODE lockMode);
static void LockShardListResources(List *shardIntervalList, LOCKMODE lockMode);
static void LockShardListResourcesOnFirstWorker(LOCKMODE lockmode,
												List *shardIntervalList);
static bool IsFirstWorkerNode();
static void CitusRangeVarCallbackForLockTable(const RangeVar *rangeVar, Oid relationId,
											  Oid oldRelationId, void *arg);
static AclResult CitusLockTableAclCheck(Oid relationId, LOCKMODE lockmode, Oid userId);
static void SetLocktagForShardDistributionMetadata(int64 shardId, LOCKTAG *tag);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(lock_shard_metadata);
PG_FUNCTION_INFO_V1(lock_shard_resources);
PG_FUNCTION_INFO_V1(lock_relation_if_exists);


/*
 * lock_shard_metadata allows the shard distribution metadata to be locked
 * remotely to block concurrent writes from workers in MX tables.
 *
 * This function does not sort the array to avoid deadlock, callers
 * must ensure a consistent order.
 */
Datum
lock_shard_metadata(PG_FUNCTION_ARGS)
{
	LOCKMODE lockMode = IntToLockMode(PG_GETARG_INT32(0));
	ArrayType *shardIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);

	CheckCitusVersion(ERROR);

	if (ARR_NDIM(shardIdArrayObject) == 0)
	{
		ereport(ERROR, (errmsg("no locks specified")));
	}

	/* we don't want random users to block writes */
	EnsureSuperUser();

	int shardIdCount = ArrayObjectCount(shardIdArrayObject);
	Datum *shardIdArrayDatum = DeconstructArrayObject(shardIdArrayObject);

	for (int shardIdIndex = 0; shardIdIndex < shardIdCount; shardIdIndex++)
	{
		int64 shardId = DatumGetInt64(shardIdArrayDatum[shardIdIndex]);

		LockShardDistributionMetadata(shardId, lockMode);
	}

	PG_RETURN_VOID();
}


/*
 * lock_shard_resources allows shard resources to be locked
 * remotely to serialise non-commutative writes on shards.
 *
 * This function does not sort the array to avoid deadlock, callers
 * must ensure a consistent order.
 */
Datum
lock_shard_resources(PG_FUNCTION_ARGS)
{
	LOCKMODE lockMode = IntToLockMode(PG_GETARG_INT32(0));
	ArrayType *shardIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);

	CheckCitusVersion(ERROR);

	if (ARR_NDIM(shardIdArrayObject) == 0)
	{
		ereport(ERROR, (errmsg("no locks specified")));
	}

	/* we don't want random users to block writes */
	EnsureSuperUser();

	int shardIdCount = ArrayObjectCount(shardIdArrayObject);
	Datum *shardIdArrayDatum = DeconstructArrayObject(shardIdArrayObject);

	for (int shardIdIndex = 0; shardIdIndex < shardIdCount; shardIdIndex++)
	{
		int64 shardId = DatumGetInt64(shardIdArrayDatum[shardIdIndex]);

		LockShardResource(shardId, lockMode);
	}

	PG_RETURN_VOID();
}


/*
 * LockShardListResourcesOnFirstWorker acquires the resource locks for the specified
 * shards on the first worker. Acquiring a lock with or without metadata does not
 * matter for us. So, worker does not have to be an MX node, acquiring the lock
 * on any worker node is enough. Note that the function does not sort the shard list,
 * therefore the caller should sort the shard list in order to avoid deadlocks.
 */
static void
LockShardListResourcesOnFirstWorker(LOCKMODE lockmode, List *shardIntervalList)
{
	StringInfo lockCommand = makeStringInfo();
	int processedShardIntervalCount = 0;
	int totalShardIntervalCount = list_length(shardIntervalList);
	WorkerNode *firstWorkerNode = GetFirstPrimaryWorkerNode();
	int connectionFlags = 0;
	const char *superuser = CitusExtensionOwnerName();

	appendStringInfo(lockCommand, "SELECT lock_shard_resources(%d, ARRAY[", lockmode);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		int64 shardId = shardInterval->shardId;

		appendStringInfo(lockCommand, "%lu", shardId);

		processedShardIntervalCount++;
		if (processedShardIntervalCount != totalShardIntervalCount)
		{
			appendStringInfo(lockCommand, ", ");
		}
	}

	appendStringInfo(lockCommand, "])");

	/* need to hold the lock until commit */
	UseCoordinatedTransaction();

	/*
	 * Use the superuser connection to make sure we are allowed to lock.
	 * This also helps ensure we only use one connection.
	 */
	MultiConnection *firstWorkerConnection = GetNodeUserDatabaseConnection(
		connectionFlags,
		firstWorkerNode
		->workerName,
		firstWorkerNode
		->workerPort,
		superuser,
		NULL);

	/* the SELECT .. FOR UPDATE breaks if we lose the connection */
	MarkRemoteTransactionCritical(firstWorkerConnection);

	/* make sure we are in a tranasaction block to hold the lock until commit */
	RemoteTransactionBeginIfNecessary(firstWorkerConnection);

	/* grab the lock on the first worker node */
	ExecuteCriticalRemoteCommand(firstWorkerConnection, lockCommand->data);
}


/*
 * IsFirstWorkerNode checks whether the node is the first worker node sorted
 * according to the host name and port number.
 */
static bool
IsFirstWorkerNode()
{
	List *workerNodeList = ActivePrimaryWorkerNodeList(NoLock);

	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	if (list_length(workerNodeList) == 0)
	{
		return false;
	}

	WorkerNode *firstWorkerNode = (WorkerNode *) linitial(workerNodeList);

	if (firstWorkerNode->groupId == GetLocalGroupId())
	{
		return true;
	}

	return false;
}


/*
 * LockShardListMetadataOnWorkers acquires the matadata locks for the specified shards on
 * metadata workers. Note that the function does not sort the shard list, therefore the
 * caller should sort the shard list in order to avoid deadlocks.
 */
void
LockShardListMetadataOnWorkers(LOCKMODE lockmode, List *shardIntervalList)
{
	StringInfo lockCommand = makeStringInfo();
	int processedShardIntervalCount = 0;
	int totalShardIntervalCount = list_length(shardIntervalList);

	if (list_length(shardIntervalList) == 0)
	{
		return;
	}

	appendStringInfo(lockCommand, "SELECT lock_shard_metadata(%d, ARRAY[", lockmode);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		int64 shardId = shardInterval->shardId;

		appendStringInfo(lockCommand, "%lu", shardId);

		processedShardIntervalCount++;
		if (processedShardIntervalCount != totalShardIntervalCount)
		{
			appendStringInfo(lockCommand, ", ");
		}
	}

	appendStringInfo(lockCommand, "])");

	SendCommandToWorkersWithMetadata(lockCommand->data);
}


/*
 * IntToLockMode verifies whether the specified integer is an accepted lock mode
 * and returns it as a LOCKMODE enum.
 */
static LOCKMODE
IntToLockMode(int mode)
{
	if (mode == ExclusiveLock)
	{
		return ExclusiveLock;
	}
	else if (mode == ShareLock)
	{
		return ShareLock;
	}
	else if (mode == AccessShareLock)
	{
		return AccessShareLock;
	}
	else if (mode == RowExclusiveLock)
	{
		return RowExclusiveLock;
	}
	else
	{
		elog(ERROR, "unsupported lockmode %d", mode);
	}
}


/*
 * LockColocationId returns after acquiring a co-location ID lock, typically used
 * for rebalancing and replication.
 */
void
LockColocationId(int colocationId, LOCKMODE lockMode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	SET_LOCKTAG_REBALANCE_COLOCATION(tag, (int64) colocationId);
	(void) LockAcquire(&tag, lockMode, sessionLock, dontWait);
}


/*
 * UnlockColocationId releases a co-location ID lock.
 */
void
UnlockColocationId(int colocationId, LOCKMODE lockMode)
{
	LOCKTAG tag;
	const bool sessionLock = false;

	SET_LOCKTAG_REBALANCE_COLOCATION(tag, (int64) colocationId);
	LockRelease(&tag, lockMode, sessionLock);
}


/*
 * LockShardDistributionMetadata returns after grabbing a lock for distribution
 * metadata related to the specified shard, blocking if required. Any locks
 * acquired using this method are released at transaction end.
 */
void
LockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	SetLocktagForShardDistributionMetadata(shardId, &tag);
	(void) LockAcquire(&tag, lockMode, sessionLock, dontWait);
}


/*
 * TryLockShardDistributionMetadata tries to grab a lock for distribution
 * metadata related to the specified shard, returning false if the lock
 * is currently taken. Any locks acquired using this method are released
 * at transaction end.
 */
bool
TryLockShardDistributionMetadata(int64 shardId, LOCKMODE lockMode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;

	SetLocktagForShardDistributionMetadata(shardId, &tag);
	bool lockAcquired = LockAcquire(&tag, lockMode, sessionLock, dontWait);

	return lockAcquired;
}


static void
SetLocktagForShardDistributionMetadata(int64 shardId, LOCKTAG *tag)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid citusTableId = shardInterval->relationId;
	CitusTableCacheEntryRef *citusTableRef = GetCitusTableCacheEntry(citusTableId);
	uint32 colocationId = citusTableRef->cacheEntry->colocationId;

	if (colocationId == INVALID_COLOCATION_ID ||
		citusTableRef->cacheEntry->partitionMethod != DISTRIBUTE_BY_HASH)
	{
		SET_LOCKTAG_SHARD_METADATA_RESOURCE(*tag, MyDatabaseId, shardId);
	}
	else
	{
		SET_LOCKTAG_COLOCATED_SHARDS_METADATA_RESOURCE(*tag, MyDatabaseId, colocationId,
													   shardInterval->shardIndex);
	}

	ReleaseTableCacheEntry(citusTableRef);
}


/*
 * LockReferencedReferenceShardDistributionMetadata acquires shard distribution
 * metadata locks with the given lock mode on the reference tables which has a
 * foreign key from the given relation.
 *
 * It also gets metadata locks on worker nodes to prevent concurrent write
 * operations on reference tables from metadata nodes.
 */
void
LockReferencedReferenceShardDistributionMetadata(uint64 shardId, LOCKMODE lockMode)
{
	Oid relationId = RelationIdForShard(shardId);

	CitusTableCacheEntryRef *cacheRef = GetCitusTableCacheEntry(relationId);
	List *referencedRelationList = cacheRef->cacheEntry->referencedRelationsViaForeignKey;
	List *shardIntervalList = GetSortedReferenceShardIntervals(referencedRelationList);

	if (list_length(shardIntervalList) > 0 && ClusterHasKnownMetadataWorkers())
	{
		LockShardListMetadataOnWorkers(lockMode, shardIntervalList);
	}

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		LockShardDistributionMetadata(shardInterval->shardId, lockMode);
	}

	ReleaseTableCacheEntry(cacheRef);
}


/*
 * LockReferencedReferenceShardResources acquires resource locks with the
 * given lock mode on the reference tables which has a foreign key from
 * the given relation.
 *
 * It also gets resource locks on worker nodes to prevent concurrent write
 * operations on reference tables from metadata nodes.
 */
static void
LockReferencedReferenceShardResources(uint64 shardId, LOCKMODE lockMode)
{
	Oid relationId = RelationIdForShard(shardId);

	CitusTableCacheEntryRef *cacheRef = GetCitusTableCacheEntry(relationId);

	/*
	 * Note that referencedRelationsViaForeignKey contains transitively referenced
	 * relations too.
	 */
	List *referencedRelationList = cacheRef->cacheEntry->referencedRelationsViaForeignKey;
	List *referencedShardIntervalList =
		GetSortedReferenceShardIntervals(referencedRelationList);

	if (list_length(referencedShardIntervalList) > 0 &&
		ClusterHasKnownMetadataWorkers() &&
		!IsFirstWorkerNode())
	{
		/*
		 * When there is metadata, all nodes can write to the reference table,
		 * but the writes need to be serialised. To achieve that, all nodes will
		 * take the shard resource lock on the first worker node via RPC, except
		 * for the first worker node which will just take it the regular way.
		 */
		LockShardListResourcesOnFirstWorker(lockMode, referencedShardIntervalList);
	}

	ShardInterval *referencedShardInterval = NULL;
	foreach_ptr(referencedShardInterval, referencedShardIntervalList)
	{
		LockShardResource(referencedShardInterval->shardId, lockMode);
	}

	ReleaseTableCacheEntry(cacheRef);
}


/*
 * GetSortedReferenceShardIntervals iterates through the given relation list,
 * lists the shards of reference tables, and returns the list after sorting.
 */
List *
GetSortedReferenceShardIntervals(List *relationList)
{
	List *shardIntervalList = NIL;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationList)
	{
		if (PartitionMethod(relationId) != DISTRIBUTE_BY_NONE)
		{
			continue;
		}

		List *currentShardIntervalList = LoadShardIntervalList(relationId);
		shardIntervalList = lappend(shardIntervalList, linitial(
										currentShardIntervalList));
	}

	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);

	return shardIntervalList;
}


/*
 * LockShardResource acquires a lock needed to modify data on a remote shard.
 * This task may be assigned to multiple backends at the same time, so the lock
 * manages any concurrency issues associated with shard file fetching and DML
 * command execution.
 */
void
LockShardResource(uint64 shardId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	AssertArg(shardId != INVALID_SHARD_ID);

	SET_LOCKTAG_SHARD_RESOURCE(tag, MyDatabaseId, shardId);

	(void) LockAcquire(&tag, lockmode, sessionLock, dontWait);
}


/* Releases the lock associated with the relay file fetching/DML task. */
void
UnlockShardResource(uint64 shardId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;

	SET_LOCKTAG_SHARD_RESOURCE(tag, MyDatabaseId, shardId);

	LockRelease(&tag, lockmode, sessionLock);
}


/*
 * LockJobResource acquires a lock for creating resources associated with the
 * given jobId. This resource is typically a job schema (namespace), and less
 * commonly a partition task directory.
 */
void
LockJobResource(uint64 jobId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	SET_LOCKTAG_JOB_RESOURCE(tag, MyDatabaseId, jobId);

	(void) LockAcquire(&tag, lockmode, sessionLock, dontWait);
}


/* Releases the lock for resources associated with the given job id. */
void
UnlockJobResource(uint64 jobId, LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;

	SET_LOCKTAG_JOB_RESOURCE(tag, MyDatabaseId, jobId);

	LockRelease(&tag, lockmode, sessionLock);
}


/*
 * LockShardListMetadata takes shared locks on the metadata of all shards in
 * shardIntervalList to prevents concurrent placement changes.
 */
void
LockShardListMetadata(List *shardIntervalList, LOCKMODE lockMode)
{
	/* lock shards in order of shard id to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		int64 shardId = shardInterval->shardId;

		LockShardDistributionMetadata(shardId, lockMode);
	}
}


/*
 * LockPlacementListMetadata takes locks on the metadata of all shards in
 * shardPlacementList to prevent concurrent placement changes.
 */
void
LockShardsInPlacementListMetadata(List *shardPlacementList, LOCKMODE lockMode)
{
	/* lock shards in order of shard id to prevent deadlock */
	shardPlacementList =
		SortList(shardPlacementList, CompareShardPlacementsByShardId);

	GroupShardPlacement *placement = NULL;
	foreach_ptr(placement, shardPlacementList)
	{
		int64 shardId = placement->shardId;

		LockShardDistributionMetadata(shardId, lockMode);
	}
}


/*
 * SerializeNonCommutativeWrites acquires the required locks to prevent concurrent
 * writes on the given shards.
 *
 * If the modified shard is a reference table's shard and the cluster is an MX
 * cluster we need to get shard resource lock on the first worker node to
 * prevent divergence possibility between placements of the reference table.
 *
 * In other workers, by acquiring a lock on the first worker, we're serializing
 * non-commutative modifications to a reference table. If the node executing the
 * command is the first worker, defined via IsFirstWorker(), we skip acquiring
 * the lock remotely to avoid an extra round-trip and/or self-deadlocks.
 *
 * Finally, if we're not dealing with reference tables on MX cluster, we'll
 * always acquire the lock with LockShardListResources() call.
 */
void
SerializeNonCommutativeWrites(List *shardIntervalList, LOCKMODE lockMode)
{
	ShardInterval *firstShardInterval = (ShardInterval *) linitial(shardIntervalList);
	int64 firstShardId = firstShardInterval->shardId;

	if (ReferenceTableShardId(firstShardId))
	{
		if (ClusterHasKnownMetadataWorkers() && !IsFirstWorkerNode())
		{
			LockShardListResourcesOnFirstWorker(lockMode, shardIntervalList);
		}

		/*
		 * Referenced tables can cascade their changes to this table, and we
		 * want to serialize changes to keep different replicas consistent.
		 */
		LockReferencedReferenceShardResources(firstShardId, lockMode);
	}


	LockShardListResources(shardIntervalList, lockMode);
}


/*
 * LockShardListResources takes locks on all shards in shardIntervalList to
 * prevent concurrent DML statements on those shards.
 */
static void
LockShardListResources(List *shardIntervalList, LOCKMODE lockMode)
{
	/* lock shards in order of shard id to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		int64 shardId = shardInterval->shardId;

		LockShardResource(shardId, lockMode);
	}
}


/*
 * LockRelationShardResources takes locks on all shards in a list of RelationShards
 * to prevent concurrent DML statements on those shards.
 */
void
LockRelationShardResources(List *relationShardList, LOCKMODE lockMode)
{
	/* lock shards in a consistent order to prevent deadlock */
	relationShardList = SortList(relationShardList, CompareRelationShards);

	RelationShard *relationShard = NULL;
	foreach_ptr(relationShard, relationShardList)
	{
		uint64 shardId = relationShard->shardId;

		if (shardId != INVALID_SHARD_ID)
		{
			LockShardResource(shardId, lockMode);
		}
	}
}


/*
 * LockParentShardResourceIfPartition checks whether the given shard belongs
 * to a partition. If it does, LockParentShardResourceIfPartition acquires a
 * shard resource lock on the colocated shard of the parent table.
 */
void
LockParentShardResourceIfPartition(uint64 shardId, LOCKMODE lockMode)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid relationId = shardInterval->relationId;

	if (PartitionTable(relationId))
	{
		int shardIndex = ShardIndex(shardInterval);
		Oid parentRelationId = PartitionParentOid(relationId);
		uint64 parentShardId = ColocatedShardIdInRelation(parentRelationId, shardIndex);

		LockShardResource(parentShardId, lockMode);
	}
}


/*
 * LockModeTextToLockMode gets a lockMode name and returns its corresponding LOCKMODE.
 * The function errors out if the input lock mode isn't defined in the PostgreSQL's
 * explicit locking table.
 */
LOCKMODE
LockModeTextToLockMode(const char *lockModeName)
{
	LOCKMODE lockMode = -1;

	for (int lockIndex = 0; lockIndex < lock_mode_to_string_map_count; lockIndex++)
	{
		const struct LockModeToStringType *lockMap = lockmode_to_string_map + lockIndex;
		if (pg_strncasecmp(lockMap->name, lockModeName, NAMEDATALEN) == 0)
		{
			lockMode = lockMap->lockMode;
			break;
		}
	}

	/* we could not find the lock mode we are looking for */
	if (lockMode == -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("unknown lock mode: %s", lockModeName)));
	}

	return lockMode;
}


/*
 * LockModeToLockModeText gets a lockMode enum and returns its corresponding text
 * representation.
 * The function errors out if the input lock mode isn't defined in the PostgreSQL's
 * explicit locking table.
 */
const char *
LockModeToLockModeText(LOCKMODE lockMode)
{
	const char *lockModeText = NULL;

	for (int lockIndex = 0; lockIndex < lock_mode_to_string_map_count; lockIndex++)
	{
		const struct LockModeToStringType *lockMap = lockmode_to_string_map + lockIndex;
		if (lockMode == lockMap->lockMode)
		{
			lockModeText = lockMap->name;
			break;
		}
	}

	/* we could not find the lock mode we are looking for */
	if (lockModeText == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("unknown lock mode enum: %d", (int) lockMode)));
	}

	return lockModeText;
}


/*
 * lock_relation_if_exists gets a relation name and lock mode
 * and returns true if the relation exists and can be locked with
 * the given lock mode. If the relation doesn't exists, the function
 * return false.
 *
 * The relation name should be qualified with the schema name.
 *
 * The function errors out if the lockmode isn't defined in the PostgreSQL's
 * explicit locking table.
 */
Datum
lock_relation_if_exists(PG_FUNCTION_ARGS)
{
	text *relationName = PG_GETARG_TEXT_P(0);
	text *lockModeText = PG_GETARG_TEXT_P(1);
	char *lockModeCString = text_to_cstring(lockModeText);

	/* ensure that we're in a transaction block */
	RequireTransactionBlock(true, "lock_relation_if_exists");

	/* get the lock mode */
	LOCKMODE lockMode = LockModeTextToLockMode(lockModeCString);

	/* resolve relationId from passed in schema and relation name */
	List *relationNameList = textToQualifiedNameList(relationName);
	RangeVar *relation = makeRangeVarFromNameList(relationNameList);

	/* lock the relation with the lock mode */
	Oid relationId = RangeVarGetRelidExtended(relation, lockMode, RVR_MISSING_OK,
											  CitusRangeVarCallbackForLockTable,
											  (void *) &lockMode);
	bool relationExists = OidIsValid(relationId);

	PG_RETURN_BOOL(relationExists);
}


/*
 * CitusRangeVarCallbackForLockTable is a callback for RangeVarGetRelidExtended used
 * to check whether the user has permission to lock a table in a particular mode.
 *
 * This function is a copy of RangeVarCallbackForLockTable in lockcmds.c adapted to
 * Citus code style.
 */
static void
CitusRangeVarCallbackForLockTable(const RangeVar *rangeVar, Oid relationId,
								  Oid oldRelationId, void *arg)
{
	LOCKMODE lockmode = *(LOCKMODE *) arg;

	if (!OidIsValid(relationId))
	{
		/* table doesn't exist, so no permissions check */
		return;
	}

	/* we only allow tables and views to be locked */
	if (!RegularTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a table", rangeVar->relname)));
	}

	/* check permissions */
	AclResult aclResult = CitusLockTableAclCheck(relationId, lockmode, GetUserId());
	if (aclResult != ACLCHECK_OK)
	{
		aclcheck_error(aclResult, get_relkind_objtype(get_rel_relkind(relationId)),
					   rangeVar->relname);
	}
}


/*
 * CitusLockTableAclCheck checks whether a user has permission to lock a relation
 * in the given lock mode.
 *
 * This function is a copy of LockTableAclCheck in lockcmds.c adapted to Citus
 * code style.
 */
static AclResult
CitusLockTableAclCheck(Oid relationId, LOCKMODE lockmode, Oid userId)
{
	AclMode aclMask;

	/* verify adequate privilege */
	if (lockmode == AccessShareLock)
	{
		aclMask = ACL_SELECT;
	}
	else if (lockmode == RowExclusiveLock)
	{
		aclMask = ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE;
	}
	else
	{
		aclMask = ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE;
	}

	AclResult aclResult = pg_class_aclcheck(relationId, userId, aclMask);

	return aclResult;
}
