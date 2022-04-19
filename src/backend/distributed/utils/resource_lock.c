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
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/distributed_planner.h"
#include "distributed/relay_utility.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"
#include "distributed/utils/array_type.h"
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
static bool AnyTableReplicated(List *shardIntervalList,
							   List **replicatedShardIntervalList);
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
	CheckCitusVersion(ERROR);

	LOCKMODE lockMode = IntToLockMode(PG_GETARG_INT32(0));
	ArrayType *shardIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);

	if (ARR_NDIM(shardIdArrayObject) == 0)
	{
		ereport(ERROR, (errmsg("no locks specified")));
	}

	int shardIdCount = ArrayObjectCount(shardIdArrayObject);
	Datum *shardIdArrayDatum = DeconstructArrayObject(shardIdArrayObject);

	for (int shardIdIndex = 0; shardIdIndex < shardIdCount; shardIdIndex++)
	{
		int64 shardId = DatumGetInt64(shardIdArrayDatum[shardIdIndex]);

		/*
		 * We don't want random users to block writes. The callers of this
		 * function either operates on all the colocated placements, such
		 * as shard moves, or requires superuser such as adding node.
		 * In other words, the coordinator initiated operations has already
		 * ensured table owner, we are preventing any malicious attempt to
		 * use this function.
		 */
		bool missingOk = true;
		EnsureShardOwner(shardId, missingOk);

		LockShardDistributionMetadata(shardId, lockMode);
	}

	PG_RETURN_VOID();
}


/*
 * EnsureShardOwner gets the shardId and reads pg_dist_partition to find
 * the corresponding relationId. If the relation does not exist, the function
 * returns. If the relation exists, the function ensures if the current
 * user is the owner of the table.
 *
 */
void
EnsureShardOwner(uint64 shardId, bool missingOk)
{
	Oid relationId = LookupShardRelationFromCatalog(shardId, missingOk);

	if (!OidIsValid(relationId) && missingOk)
	{
		/*
		 * This could happen in two ways. First, a malicious user is trying
		 * to acquire locks on non-existing shards. Second, the metadata has
		 * not been synced (or not yet visible) to this node. In the second
		 * case, there is no point in locking the shards because no other
		 * transaction can be accessing the table.
		 */
		return;
	}

	EnsureTableOwner(relationId);
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
	CheckCitusVersion(ERROR);

	LOCKMODE lockMode = IntToLockMode(PG_GETARG_INT32(0));
	ArrayType *shardIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);

	if (ARR_NDIM(shardIdArrayObject) == 0)
	{
		ereport(ERROR, (errmsg("no locks specified")));
	}

	int shardIdCount = ArrayObjectCount(shardIdArrayObject);
	Datum *shardIdArrayDatum = DeconstructArrayObject(shardIdArrayObject);

	/*
	 * The executor calls this UDF for modification queries. So, any user
	 * who has the the rights to modify this table are actually able
	 * to call the UDF.
	 *
	 * So, at this point, we make sure that any malicious user who doesn't
	 * have modification privileges to call this UDF.
	 *
	 * Update/Delete/Truncate commands already acquires ExclusiveLock
	 * on the executor. However, for INSERTs, the user might have only
	 * INSERTs granted, so add a special case for it.
	 */
	AclMode aclMask = ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE;
	if (lockMode == RowExclusiveLock)
	{
		aclMask |= ACL_INSERT;
	}

	for (int shardIdIndex = 0; shardIdIndex < shardIdCount; shardIdIndex++)
	{
		int64 shardId = DatumGetInt64(shardIdArrayDatum[shardIdIndex]);

		/*
		 * We don't want random users to block writes. If the current user
		 * has privileges to modify the shard, then the user can already
		 * acquire the lock. So, we allow.
		 */
		bool missingOk = true;
		Oid relationId = LookupShardRelationFromCatalog(shardId, missingOk);

		if (!OidIsValid(relationId) && missingOk)
		{
			/*
			 * This could happen in two ways. First, a malicious user is trying
			 * to acquire locks on non-existing shards. Second, the metadata has
			 * not been synced (or not yet visible) to this node. In the second
			 * case, there is no point in locking the shards because no other
			 * transaction can be accessing the table.
			 */
			continue;
		}

		EnsureTablePermissions(relationId, aclMask);

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
	if (!AllowModificationsFromWorkersToReplicatedTables)
	{
		/*
		 * Allowing modifications from worker nodes for replicated tables requires
		 * to serialize modifications, see AcquireExecutorShardLocksForExecution()
		 * for the details.
		 *
		 * If the user opted for disabling modifications from the workers, we do not
		 * need to acquire these remote locks. Returning early saves us from an additional
		 * network round-trip.
		 */
		Assert(AnyTableReplicated(shardIntervalList, NULL));
		return;
	}

	StringInfo lockCommand = makeStringInfo();
	int processedShardIntervalCount = 0;
	int totalShardIntervalCount = list_length(shardIntervalList);
	WorkerNode *firstWorkerNode = GetFirstPrimaryWorkerNode();
	int connectionFlags = 0;
	const char *currentUser = CurrentUserName();

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
		currentUser,
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
	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(NoLock);

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


static void
SetLocktagForShardDistributionMetadata(int64 shardId, LOCKTAG *tag)
{
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid citusTableId = shardInterval->relationId;
	CitusTableCacheEntry *citusTable = GetCitusTableCacheEntry(citusTableId);
	uint32 colocationId = citusTable->colocationId;

	if (colocationId == INVALID_COLOCATION_ID ||
		!IsCitusTableTypeCacheEntry(citusTable, HASH_DISTRIBUTED))
	{
		SET_LOCKTAG_SHARD_METADATA_RESOURCE(*tag, MyDatabaseId, shardId);
	}
	else
	{
		SET_LOCKTAG_COLOCATED_SHARDS_METADATA_RESOURCE(*tag, MyDatabaseId, colocationId,
													   shardInterval->shardIndex);
	}
}


/*
 * LockPlacementCleanup takes an exclusive lock to ensure that only one process
 * can cleanup placements at the same time.
 */
void
LockPlacementCleanup(void)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;
	SET_LOCKTAG_PLACEMENT_CLEANUP(tag);
	(void) LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
}


/*
 * TryLockPlacementCleanup takes an exclusive lock to ensure that only one
 * process can cleanup placements at the same time.
 */
bool
TryLockPlacementCleanup(void)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;
	SET_LOCKTAG_PLACEMENT_CLEANUP(tag);
	bool lockAcquired = LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
	return lockAcquired;
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

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	List *referencedRelationList = cacheEntry->referencedRelationsViaForeignKey;
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

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	/*
	 * Note that referencedRelationsViaForeignKey contains transitively referenced
	 * relations too.
	 */
	List *referencedRelationList = cacheEntry->referencedRelationsViaForeignKey;
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
		if (!IsCitusTableType(relationId, REFERENCE_TABLE))
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

	SET_LOCKTAG_SHARD_RESOURCE(tag, MyDatabaseId, shardId);

	(void) LockAcquire(&tag, lockmode, sessionLock, dontWait);
}


/* LockTransactionRecovery acquires a lock for transaction recovery */
void
LockTransactionRecovery(LOCKMODE lockmode)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	SET_LOCKTAG_CITUS_OPERATION(tag, CITUS_TRANSACTION_RECOVERY);

	(void) LockAcquire(&tag, lockmode, sessionLock, dontWait);
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
 * LockShardsInPlacementListMetadata takes locks on the metadata of all shards in
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
	if (shardIntervalList == NIL)
	{
		return;
	}

	List *replicatedShardList = NIL;
	if (AnyTableReplicated(shardIntervalList, &replicatedShardList))
	{
		if (ClusterHasKnownMetadataWorkers() && !IsFirstWorkerNode())
		{
			LockShardListResourcesOnFirstWorker(lockMode, replicatedShardList);
		}

		ShardInterval *firstShardInterval =
			(ShardInterval *) linitial(replicatedShardList);
		if (ReferenceTableShardId(firstShardInterval->shardId))
		{
			/*
			 * Referenced tables can cascade their changes to this table, and we
			 * want to serialize changes to keep different replicas consistent.
			 *
			 * We currently only support foreign keys to reference tables, which are
			 * single shard. So, getting the first shard should be sufficient here.
			 */
			LockReferencedReferenceShardResources(firstShardInterval->shardId, lockMode);
		}
	}

	LockShardListResources(shardIntervalList, lockMode);
}


/*
 * AnyTableReplicated iterates on the shard list and returns true
 * if any of the shard is a replicated table. We qualify replicated
 * tables as any reference table or any distributed table with
 * replication factor > 1.
 *
 * If the optional replicatedShardIntervalList is passed, the function
 * fills it with the replicated shard intervals.
 */
static bool
AnyTableReplicated(List *shardIntervalList, List **replicatedShardIntervalList)
{
	if (replicatedShardIntervalList == NULL)
	{
		/* the caller is not interested in the replicatedShardIntervalList */
		List *localList = NIL;
		replicatedShardIntervalList = &localList;
	}

	*replicatedShardIntervalList = NIL;

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		int64 shardId = shardInterval->shardId;

		Oid relationId = RelationIdForShard(shardId);
		if (ReferenceTableShardId(shardId))
		{
			*replicatedShardIntervalList =
				lappend(*replicatedShardIntervalList, LoadShardInterval(shardId));
		}
		else if (!SingleReplicatedTable(relationId))
		{
			*replicatedShardIntervalList =
				lappend(*replicatedShardIntervalList, LoadShardInterval(shardId));
		}
	}

	return list_length(*replicatedShardIntervalList) > 0;
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
	if (relationShardList == NIL)
	{
		return;
	}

	List *shardIntervalList = NIL;
	RelationShard *relationShard = NULL;
	foreach_ptr(relationShard, relationShardList)
	{
		uint64 shardId = relationShard->shardId;

		ShardInterval *shardInterval = LoadShardInterval(shardId);

		shardIntervalList = lappend(shardIntervalList, shardInterval);
	}

	/* lock shards in a consistent order to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);
	SerializeNonCommutativeWrites(shardIntervalList, lockMode);
}


/*
 * LockParentShardResourceIfPartition checks whether the given shard belongs
 * to a partition. If it does, LockParentShardResourceIfPartition acquires a
 * shard resource lock on the colocated shard of the parent table.
 */
void
LockParentShardResourceIfPartition(List *shardIntervalList, LOCKMODE lockMode)
{
	List *parentShardIntervalList = NIL;

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		Oid relationId = shardInterval->relationId;

		if (PartitionTable(relationId))
		{
			int shardIndex = ShardIndex(shardInterval);
			Oid parentRelationId = PartitionParentOid(relationId);
			uint64 parentShardId = ColocatedShardIdInRelation(parentRelationId,
															  shardIndex);

			ShardInterval *parentShardInterval = LoadShardInterval(parentShardId);
			parentShardIntervalList = lappend(parentShardIntervalList,
											  parentShardInterval);
		}
	}

	LockShardListResources(parentShardIntervalList, lockMode);
}


/*
 * LockModeCStringToLockMode gets a lockMode name and returns its corresponding LOCKMODE.
 * The function errors out if the input lock mode isn't defined in the PostgreSQL's
 * explicit locking table.
 */
LOCKMODE
LockModeCStringToLockMode(const char *lockModeName)
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
 * LockModeToLockModeCString gets a lockMode enum and returns its corresponding text
 * representation.
 * The function errors out if the input lock mode isn't defined in the PostgreSQL's
 * explicit locking table.
 */
const char *
LockModeToLockModeCString(LOCKMODE lockMode)
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
 * LockRelationIfExists gets a relation name, lock mode, a nowait flag
 * and returns true if the relation exists and can be locked with
 * the given lock mode. If the relation doesn't exists, the function
 * return false.
 *
 * If nowait is true, and the lock is already acquired an error will be
 * thrown.
 *
 * The relation name should be qualified with the schema name.
 *
 * The function errors out if the lockmode isn't defined in the PostgreSQL's
 * explicit locking table.
 */
static bool
LockRelationIfExists(text *relationName, const char *lockModeCString, bool nowait)
{
	/* get the lock mode */
	LOCKMODE lockMode = LockModeCStringToLockMode(lockModeCString);

	/* resolve relationId from passed in schema and relation name */
	List *relationNameList = textToQualifiedNameList(relationName);
	RangeVar *relation = makeRangeVarFromNameList(relationNameList);

	uint32 nowaitFlag = nowait ? RVR_NOWAIT : 0;

	/* lock the relation with the lock mode */
	Oid relationId = RangeVarGetRelidExtended(relation, lockMode, RVR_MISSING_OK |
											  nowaitFlag,
											  CitusRangeVarCallbackForLockTable,
											  (void *) &lockMode);
	bool relationExists = OidIsValid(relationId);
	return relationExists;
}


/*
 * lock_relation_if_exists is a wrapper for LockRelationIfExists.
 * Because lock_relation_if_exists is a udf and exposed to the user,
 * this function also checks that it must be executed in a transaction block.
 */
Datum
lock_relation_if_exists(PG_FUNCTION_ARGS)
{
	text *relationName = PG_GETARG_TEXT_P(0);
	text *lockModeText = PG_GETARG_TEXT_P(1);
	char *lockModeCString = text_to_cstring(lockModeText);

	bool nowait = false;
	if (PG_NARGS() == 3)
	{
		nowait = PG_GETARG_BOOL(2);
	}

	/* ensure that we're in a transaction block */
	RequireTransactionBlock(true, "lock_relation_if_exists");

	bool relationExists = LockRelationIfExists(relationName, lockModeCString, nowait);
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

	/* we only allow tables, views and foreign tables to be locked */
	if (!RegularTable(relationId) && !IsForeignTable(relationId))
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


/*
 * AcquireDistributedLockOnRelations_Internal acquire a distributed lock on worker nodes
 * for given list of relations ids. Worker node list is sorted so that the lock
 * is acquired in the same order regardless of which node it was run on.
 * Notice that no lock is acquired on coordinator node if the coordinator is not
 * added to the metadata.
 *
 * Notice that no validation is done on the relationIds, that is the responsibility of
 * AcquireDistributedLockOnRelations.
 *
 * A nowait flag is used to require the locks to be available immediately
 * and if that is not the case, an error will be thrown
 */
static void
AcquireDistributedLockOnRelations_Internal(List *relationIdList, LOCKMODE lockMode, bool
										   nowait)
{
	Oid relationId = InvalidOid;
	List *workerNodeList = ActivePrimaryNodeList(NoLock);
	Oid userId = GetUserId();

	const char *lockModeCString = LockModeToLockModeCString(lockMode);

	/*
	 * We want to acquire locks in the same order across the nodes.
	 */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	UseCoordinatedTransaction();

	int32 localGroupId = GetLocalGroupId();

	foreach_oid(relationId, relationIdList)
	{
		char *qualifiedRelationName = generate_qualified_relation_name(relationId);
		StringInfo lockRelationCommand = makeStringInfo();

		char *lockCommand = nowait ? LOCK_RELATION_IF_EXISTS_NOWAIT :
							LOCK_RELATION_IF_EXISTS;
		appendStringInfo(lockRelationCommand, lockCommand,
						 quote_literal_cstr(qualifiedRelationName),
						 lockModeCString);

		/* preemptive permission check so a worker connection is not */
		/* established if the user is not allowed to acquire the lock */
		CitusLockTableAclCheck(relationId, lockMode, userId);

		WorkerNode *workerNode = NULL;
		foreach_ptr(workerNode, workerNodeList)
		{
			const char *nodeName = workerNode->workerName;
			int nodePort = workerNode->workerPort;

			/* if local node is one of the targets, acquire the lock locally */
			if (workerNode->groupId == localGroupId)
			{
				LockRelationIfExists(
					cstring_to_text(qualifiedRelationName),
					lockModeCString,
					nowait);

				continue;
			}

			SendCommandToWorker(nodeName, nodePort, lockRelationCommand->data);
		}
	}
}


/*
 * AcquireDistributedLockOnRelations filters relations before passing them to
 * AcquireDistributedLockOnRelations_Internal to acquire the locks.
 *
 * Only tables, views, and foreign tables can be locked with this function. Other relations
 * will cause an error.
 *
 * Gracefully locks relations so no errors are thrown for things like invalid id-s
 * or missing relations on worker nodes.
 *
 * Considers different types of relations based on a 'configs' parameter:
 * - DIST_LOCK_DEFAULT: locks citus tables
 * - DIST_LOCK_VIEWS_RECUR: locks citus tables that locked views are dependent on recursively
 * - DIST_LOCK_REFERENCING_TABLES: locks tables that refer to locked citus tables with a foreign key
 * - DIST_LOCK_NOWAIT: throws an error if the lock is not immediately available
 */
void
AcquireDistributedLockOnRelations(List *relationList, LOCKMODE lockMode, uint32 configs)
{
	/* nothing to do if there is no metadata at worker nodes */
	if (!ClusterHasKnownMetadataWorkers())
	{
		return;
	}

	List *distributedRelationList = NIL;

	RangeVar *rangeVar = NULL;
	List *relations = list_copy(relationList);
	foreach_ptr(rangeVar, relations)
	{
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, false);

		if ((configs & DIST_LOCK_VIEWS_RECUR) > 0 && get_rel_relkind(relationId) ==
			RELKIND_VIEW)
		{
			ObjectAddress viewAddress = { 0 };
			Oid schemaOid = RangeVarGetCreationNamespace(rangeVar);
			ObjectAddressSet(viewAddress, schemaOid, relationId);

			List *distDependencies = GetDistributableDependenciesForObject(&viewAddress);

			ObjectAddress *address = NULL;
			foreach_ptr(address, distDependencies)
			{
				distributedRelationList = list_append_unique_oid(distributedRelationList,
																 address->objectId);
			}

			continue;
		}

		if (!IsCitusTable(relationId))
		{
			continue;
		}

		if (list_member_oid(distributedRelationList, relationId))
		{
			continue;
		}

		distributedRelationList = lappend_oid(distributedRelationList, relationId);

		if ((configs & DIST_LOCK_REFERENCING_TABLES) > 0)
		{
			Oid referencingRelationId = InvalidOid;
			CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
			Assert(cacheEntry != NULL);

			List *referencingTableList = cacheEntry->referencingRelationsViaForeignKey;
			foreach_oid(referencingRelationId, referencingTableList)
			{
				distributedRelationList = list_append_unique_oid(distributedRelationList,
																 referencingRelationId);
			}
		}
	}

	if (distributedRelationList != NIL)
	{
		bool nowait = (configs & DIST_LOCK_NOWAIT) > 0;
		AcquireDistributedLockOnRelations_Internal(distributedRelationList, lockMode,
												   nowait);
	}
}


/*
 * ErrorIfUnsupportedLockStmt errors out if:
 * - The lock statement is not in a transaction block
 * - The relation id-s being locked do not exist
 * - Locking shard, but citus.enable_manual_changes_to_shards is false
 */
void
ErrorIfUnsupportedLockStmt(LockStmt *stmt, bool isTopLevel)
{
	RequireTransactionBlock(isTopLevel, "LOCK TABLE");

	RangeVar *rangeVar = NULL;
	foreach_ptr(rangeVar, stmt->relations)
	{
		bool missingOk = false;
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, missingOk);

		/* Note that allowing the user to lock shards could lead to */
		/* distributed deadlocks due to shards not being locked when */
		/* a distributed table is locked. */
		/* However, because citus.enable_manual_changes_to_shards */
		/* is a guc which is not visible by default, whoever is using this */
		/* guc will hopefully know what they're doing and avoid such scenarios. */
		ErrorIfIllegallyChangingKnownShard(relationId);
	}
}
