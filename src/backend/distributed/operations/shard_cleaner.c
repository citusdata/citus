/*-------------------------------------------------------------------------
 *
 * shard_cleaner.c
 *	  This implements the background process that cleans shards that are
 *	  left around. Shards that are left around are marked as state 4
 *	  (SHARD_STATE_TO_DELETE) in pg_dist_placement.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/sequence.h"
#include "postmaster/postmaster.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_transaction.h"


/*
 * CleanupRecord represents a record from pg_dist_cleanup_record.
 */
typedef struct CleanupRecord
{
	/* unique identifier of the record (for deletes) */
	CleanupRecordId recordId;

	/* identifier of the operation that generated the record (must not be in progress) */
	OperationId operationId;

	/* type of the object (e.g. shard placement) */
	CleanupObjectType objectType;

	/* fully qualified name of the object */
	char *objectName;

	/* node grou ID on which the object is located */
	int nodeGroupId;

	/* whether the record indicates cleanup after successful completion */
	bool isSuccess;
} CleanupRecord;


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_cleanup_orphaned_shards);
PG_FUNCTION_INFO_V1(isolation_cleanup_orphaned_shards);

/* cleanup functions */
static bool TryDropShard(char *qualifiedTableName, char *nodeName, int nodePort);
static bool TryLockRelationAndPlacementCleanup(Oid relationId, LOCKMODE lockmode);
static List * ListCleanupRecords(void);
static uint64 GetNextCleanupRecordId(void);
static CleanupRecord * TupleToCleanupRecord(HeapTuple heapTuple, TupleDesc
											tupleDescriptor);
static bool CleanupRecordExists(CleanupRecordId recordId);
static void DeleteCleanupRecordByRecordId(CleanupRecordId recordId);

/* cleanup operation ID functions */
static OperationId GetNextOperationId(void);
static void LockOperationId(OperationId operationId);
static bool IsOperationInProgress(OperationId operationId);

/* operation ID set by StartCleanupOperation */
OperationId CurrentOperationId = INVALID_OPERATION_ID;


/*
 * citus_cleanup_orphaned_shards implements a user-facing UDF to delete
 * orphaned shards that are still haning around in the system. These shards are
 * orphaned by previous actions that were not directly able to delete the
 * placements eg. shard moving or dropping of a distributed table while one of
 * the data nodes was not online.
 *
 * This function iterates through placements where shardstate is
 * SHARD_STATE_TO_DELETE (shardstate = 4), drops the corresponding tables from
 * the node and removes the placement information from the catalog.
 *
 * The function takes no arguments and runs cluster wide. It cannot be run in a
 * transaction, because holding the locks it takes for a long time is not good.
 * While the locks are held, it is impossible for the background daemon to
 * cleanup orphaned shards.
 */
Datum
citus_cleanup_orphaned_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	PreventInTransactionBlock(true, "citus_cleanup_orphaned_shards");

	bool waitForLocks = true;
	int droppedShardCount = DropOrphanedShards(waitForLocks);
	if (droppedShardCount > 0)
	{
		ereport(NOTICE, (errmsg("cleaned up %d orphaned shards", droppedShardCount)));
	}

	PG_RETURN_VOID();
}


/*
 * isolation_cleanup_orphaned_shards implements a test UDF that's the same as
 * citus_cleanup_orphaned_shards. The only difference is that this command can
 * be run in transactions, this is to test
 */
Datum
isolation_cleanup_orphaned_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	bool waitForLocks = true;
	int droppedShardCount = DropOrphanedShards(waitForLocks);
	if (droppedShardCount > 0)
	{
		ereport(NOTICE, (errmsg("cleaned up %d orphaned shards", droppedShardCount)));
	}

	PG_RETURN_VOID();
}


/*
 * DropOrphanedShardsInSeparateTransaction cleans up orphaned shards by
 * connecting to localhost. This is done, so that the locks that
 * DropOrphanedShards takes are only held for a short time.
 */
void
DropOrphanedShardsInSeparateTransaction(void)
{
	ExecuteRebalancerCommandInSeparateTransaction("CALL citus_cleanup_orphaned_shards()");
}


/*
 * TryDropOrphanedShards is a wrapper around DropOrphanedShards that catches
 * any errors to make it safe to use in the maintenance daemon.
 *
 * If dropping any of the shards failed this function returns -1, otherwise it
 * returns the number of dropped shards.
 */
int
TryDropOrphanedShards(bool waitForLocks)
{
	int droppedShardCount = 0;
	MemoryContext savedContext = CurrentMemoryContext;
	PG_TRY();
	{
		droppedShardCount = DropOrphanedShards(waitForLocks);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		/* rethrow as WARNING */
		edata->elevel = WARNING;
		ThrowErrorData(edata);
	}
	PG_END_TRY();

	return droppedShardCount;
}


/*
 * DropOrphanedShards removes shards that were marked SHARD_STATE_TO_DELETE before.
 *
 * It does so by trying to take an exclusive lock on the shard and its
 * colocated placements before removing. If the lock cannot be obtained it
 * skips the group and continues with others. The group that has been skipped
 * will be removed at a later time when there are no locks held anymore on
 * those placements.
 *
 * If waitForLocks is false, then if we cannot take a lock on pg_dist_placement
 * we continue without waiting.
 *
 * Before doing any of this it will take an exclusive PlacementCleanup lock.
 * This is to ensure that this function is not being run concurrently.
 * Otherwise really bad race conditions are possible, such as removing all
 * placements of a shard. waitForLocks indicates if this function should
 * wait for this lock or not.
 *
 */
int
DropOrphanedShards(bool waitForLocks)
{
	int removedShardCount = 0;

	/*
	 * We should try to take the highest lock that we take
	 * later in this function for pg_dist_placement. We take RowExclusiveLock
	 * in DeleteShardPlacementRow.
	 */
	LOCKMODE lockmode = RowExclusiveLock;

	if (!IsCoordinator())
	{
		return 0;
	}

	if (waitForLocks)
	{
		LockPlacementCleanup();
	}
	else
	{
		Oid distPlacementId = DistPlacementRelationId();
		if (!TryLockRelationAndPlacementCleanup(distPlacementId, lockmode))
		{
			return 0;
		}
	}

	int failedShardDropCount = 0;


	/*
	 * First handle to-be-deleted placements which are generated in case
	 * of shard moves and deferred drop.
	 */
	List *shardPlacementList = AllShardPlacementsWithShardPlacementState(
		SHARD_STATE_TO_DELETE);

	GroupShardPlacement *placement = NULL;
	foreach_ptr(placement, shardPlacementList)
	{
		if (!PrimaryNodeForGroup(placement->groupId, NULL) ||
			!ShardExists(placement->shardId))
		{
			continue;
		}

		ShardPlacement *shardPlacement = LoadShardPlacement(placement->shardId,
															placement->placementId);
		ShardInterval *shardInterval = LoadShardInterval(placement->shardId);
		char *qualifiedTableName = ConstructQualifiedShardName(shardInterval);

		if (TryDropShard(qualifiedTableName, shardPlacement->nodeName,
						 shardPlacement->nodePort))
		{
			/* delete the to-be-deleted placement metadata */
			DeleteShardPlacementRow(placement->placementId);

			removedShardCount++;
		}
		else
		{
			failedShardDropCount++;
		}
	}

	/*
	 * Cleanup objects listed in pg_dist_cleanup_record.
	 */
	List *recordList = ListCleanupRecords();

	CleanupRecord *record = NULL;
	foreach_ptr(record, recordList)
	{
		if (record->objectType != CLEANUP_SHARD_PLACEMENT)
		{
			/* we currently only clean shard placements */
			continue;
		}

		if (IsOperationInProgress(record->operationId))
		{
			/* operation that generated the record is still running */
			continue;
		}

		char *qualifiedTableName = record->objectName;
		WorkerNode *workerNode = LookupNodeForGroup(record->nodeGroupId);

		if (!CleanupRecordExists(record->recordId))
		{
			/*
			 * The operation completed successfully just after we called
			 * ListCleanupRecords in which case the record is now gone.
			 */
			continue;
		}

		if (TryDropShard(qualifiedTableName, workerNode->workerName,
						 workerNode->workerPort))
		{
			/* delete the cleanup record */
			DeleteCleanupRecordByRecordId(record->recordId);

			removedShardCount++;
		}
		else
		{
			failedShardDropCount++;
		}
	}

	if (failedShardDropCount > 0)
	{
		ereport(WARNING, (errmsg("Failed to drop %d orphaned shards out of %d",
								 failedShardDropCount, list_length(shardPlacementList))));
	}

	return removedShardCount;
}


/*
 * TryLockRelationAndPlacementCleanup tries to lock the given relation
 * and the placement cleanup. If it cannot, it returns false.
 *
 */
static bool
TryLockRelationAndPlacementCleanup(Oid relationId, LOCKMODE lockmode)
{
	if (!ConditionalLockRelationOid(relationId, lockmode))
	{
		ereport(DEBUG1, (errmsg(
							 "could not acquire shard lock to cleanup placements")));
		return false;
	}

	if (!TryLockPlacementCleanup())
	{
		ereport(DEBUG1, (errmsg("could not acquire lock to cleanup placements")));
		return false;
	}
	return true;
}


/*
 * TryDropShard tries to drop the given shard placement and returns
 * true on success.
 */
static bool
TryDropShard(char *qualifiedTableName, char *nodeName, int nodePort)
{
	ereport(LOG, (errmsg("dropping shard placement %s "
						 "on %s:%d after it was moved away",
						 qualifiedTableName, nodeName, nodePort)));

	/* prepare sql query to execute to drop the shard */
	StringInfo dropQuery = makeStringInfo();
	appendStringInfo(dropQuery, DROP_REGULAR_TABLE_COMMAND, qualifiedTableName);

	/*
	 * We set a lock_timeout here so that if there are running queries on the
	 * shards we won't get blocked more than 1s and fail.
	 *
	 * The lock timeout also avoids getting stuck in a distributed deadlock, which
	 * can occur because we might be holding pg_dist_placement locks while also
	 * taking locks on the shard placements, and this code interrupts the
	 * distributed deadlock detector.
	 */
	List *dropCommandList = list_make2("SET LOCAL lock_timeout TO '1s'",
									   dropQuery->data);

	/* remove the shard from the node */
	bool success = true;
	SendOptionalCommandListToWorkerOutsideTransaction(nodeName,
													  nodePort,
													  NULL, dropCommandList);

	return success;
}


/*
 * InsertCleanupRecordInCurrentTransaction inserts a new pg_dist_cleanup_record entry
 * as part of the current transaction. This is primarily useful for deferred drop scenarios,
 * since these records would roll back in case of failure.
 *
 * For failure scenarios, use a subtransaction (direct insert via localhost).
 */
void
InsertCleanupRecordInCurrentTransaction(CleanupObjectType objectType, char *objectName,
										int nodeGroupId)
{
	/* StartOperationRequiringCleanup must have been called at this point */
	Assert(CurrentOperationId != INVALID_OPERATION_ID);

	Datum values[Natts_pg_dist_cleanup_record];
	bool isNulls[Natts_pg_dist_cleanup_record];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	CleanupRecordId recordId = GetNextCleanupRecordId();
	OperationId operationId = CurrentOperationId;

	values[Anum_pg_dist_cleanup_record_record_id - 1] = UInt64GetDatum(recordId);
	values[Anum_pg_dist_cleanup_record_operation_id - 1] = UInt64GetDatum(operationId);
	values[Anum_pg_dist_cleanup_record_object_type - 1] = Int32GetDatum(objectType);
	values[Anum_pg_dist_cleanup_record_object_name - 1] = CStringGetTextDatum(objectName);
	values[Anum_pg_dist_cleanup_record_node_group_id - 1] = Int32GetDatum(nodeGroupId);
	values[Anum_pg_dist_cleanup_record_is_success - 1] = BoolGetDatum(true);

	/* open shard relation and insert new tuple */
	Oid relationId = DistCleanupRecordRelationId();
	Relation pgDistCleanupRecord = table_open(relationId, RowExclusiveLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistCleanupRecord);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistCleanupRecord, heapTuple);

	CommandCounterIncrement();
	table_close(pgDistCleanupRecord, NoLock);
}


/*
 * InsertCleanupRecordInSeparateTransaction inserts a new pg_dist_cleanup_record entry
 * in a separate transaction to ensure the record persists after rollback. We should
 * delete these records if the operation completes successfully.
 *
 * For failure scenarios, use a subtransaction (direct insert via localhost).
 */
void
InsertCleanupRecordInSubtransaction(CleanupObjectType objectType, char *objectName,
									int nodeGroupId)
{
	/* StartOperationRequiringCleanup must have been called at this point */
	Assert(CurrentOperationId != INVALID_OPERATION_ID);

	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "INSERT INTO pg_catalog.pg_dist_cleanup_record "
					 " (operation_id, object_type, object_name, node_group_id)"
					 " VALUES (" UINT64_FORMAT ", %d, %s, %d)",
					 CurrentOperationId,
					 objectType,
					 quote_literal_cstr(objectName),
					 nodeGroupId);

	ExecuteRebalancerCommandInSeparateTransaction(command->data);
}


/*
 * GetNextCleanupRecordId generates a new record ID using the sequence.
 */
static CleanupRecordId
GetNextCleanupRecordId(void)
{
	RangeVar *sequenceName = makeRangeVar("pg_catalog",
										  "pg_dist_cleanup_record_record_id_seq", -1);

	bool missingOK = false;
	Oid sequenceId = RangeVarGetRelid(sequenceName, NoLock, missingOK);

	bool checkPermissions = false;
	return nextval_internal(sequenceId, checkPermissions);
}


/*
 * ListCleanupRecords lists all the current cleanup records.
 */
static List *
ListCleanupRecords(void)
{
	Relation pgDistCleanupRecord = table_open(DistCleanupRecordRelationId(),
											  AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistCleanupRecord);

	List *recordList = NIL;
	int scanKeyCount = 0;
	bool indexOK = false;

	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanupRecord, InvalidOid,
													indexOK, NULL, scanKeyCount, NULL);

	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		CleanupRecord *record = TupleToCleanupRecord(heapTuple, tupleDescriptor);
		recordList = lappend(recordList, record);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistCleanupRecord, NoLock);

	return recordList;
}


/*
 * TupleToCleanupRecord converts a pg_dist_cleanup_record tuple into a CleanupRecord struct.
 */
static CleanupRecord *
TupleToCleanupRecord(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	Datum datumArray[Natts_pg_dist_cleanup_record];
	bool isNullArray[Natts_pg_dist_cleanup_record];
	heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

	CleanupRecord *record = palloc0(sizeof(CleanupRecord));

	record->recordId =
		DatumGetUInt64(datumArray[Anum_pg_dist_cleanup_record_record_id - 1]);

	record->objectType =
		DatumGetInt32(datumArray[Anum_pg_dist_cleanup_record_object_type - 1]);

	record->objectName =
		TextDatumGetCString(datumArray[Anum_pg_dist_cleanup_record_object_name - 1]);

	record->nodeGroupId =
		DatumGetInt32(datumArray[Anum_pg_dist_cleanup_record_node_group_id - 1]);

	record->isSuccess =
		DatumGetBool(datumArray[Anum_pg_dist_cleanup_record_is_success - 1]);

	return record;
}


/*
 * CleanupRecordExists returns whether a cleanup record with the given
 * record ID exists in pg_dist_cleanup_record.
 */
static bool
CleanupRecordExists(CleanupRecordId recordId)
{
	Relation pgDistCleanupRecord = table_open(DistCleanupRecordRelationId(),
											  AccessShareLock);

	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_cleanup_record_record_id,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(recordId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanupRecord,
													DistCleanupRecordPrimaryKeyIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	bool recordExists = HeapTupleIsValid(heapTuple);

	systable_endscan(scanDescriptor);

	CommandCounterIncrement();
	table_close(pgDistCleanupRecord, NoLock);

	return recordExists;
}


/*
 * DeleteCleanupRecordByRecordId deletes a single pg_dist_cleanup_record entry.
 */
static void
DeleteCleanupRecordByRecordId(CleanupRecordId recordId)
{
	Relation pgDistCleanupRecord = table_open(DistCleanupRecordRelationId(),
											  RowExclusiveLock);

	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_cleanup_record_record_id,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(recordId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanupRecord,
													DistCleanupRecordPrimaryKeyIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (heapTuple == NULL)
	{
		ereport(ERROR, (errmsg("could not find cleanup record " UINT64_FORMAT,
							   recordId)));
	}

	simple_heap_delete(pgDistCleanupRecord, &heapTuple->t_self);

	systable_endscan(scanDescriptor);

	CommandCounterIncrement();
	table_close(pgDistCleanupRecord, NoLock);
}


/*
 * DeleteCurrentCleanupRecords deletes all failure cleanup records belonging to the
 * current operation. This is generally used to signal that those objects
 * have been dropped already by successful completion of the transaction.
 */
void
DeleteMyCleanupOnFailureRecords(void)
{
	Relation pgDistCleanupRecord = table_open(DistCleanupRecordRelationId(),
											  RowExclusiveLock);

	const int scanKeyCount = 2;
	ScanKeyData scanKey[2];
	bool indexOK = false;

	/* find failure records belonging to the current operation */
	OperationId operationId = CurrentOperationId;
	bool isSuccess = false;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_cleanup_record_operation_id,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(operationId));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_cleanup_record_is_success,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(isSuccess));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanupRecord,
													InvalidOid, indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		simple_heap_delete(pgDistCleanupRecord, &heapTuple->t_self);
	}

	systable_endscan(scanDescriptor);

	CommandCounterIncrement();
	table_close(pgDistCleanupRecord, NoLock);
}


/*
 * StartOperationRequiringCleanup should be called by an operation that wishes to generate
 * cleanup records.
 */
OperationId
StartOperationRequiringCleanup(void)
{
	CurrentOperationId = GetNextOperationId();

	LockOperationId(CurrentOperationId);

	return CurrentOperationId;
}


/*
 * GetNextOperationId generates a new operation ID using the sequence.
 */
static OperationId
GetNextOperationId(void)
{
	RangeVar *sequenceName = makeRangeVar("pg_catalog", "pg_dist_operation_id_seq", -1);

	bool missingOK = false;
	Oid sequenceId = RangeVarGetRelid(sequenceName, NoLock, missingOK);

	bool checkPermissions = false;
	return nextval_internal(sequenceId, checkPermissions);
}


/*
 * LockOperationId takes an exclusive lock on the operation ID to let other
 * backends know that the operation is active.
 */
static void
LockOperationId(OperationId operationId)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;
	SET_LOCKTAG_CLEANUP_OPERATION_ID(tag, operationId);
	(void) LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
}


/*
 * IsOperationInProgress checks whether an operation is in progress by
 * acquiring a share lock on the operation ID, which conflicts with any
 * transaction that has called LockOperationId.
 */
static bool
IsOperationInProgress(OperationId operationId)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;
	SET_LOCKTAG_CLEANUP_OPERATION_ID(tag, operationId);
	bool lockAcquired = LockAcquire(&tag, ShareLock, sessionLock, dontWait);
	return !lockAcquired;
}
