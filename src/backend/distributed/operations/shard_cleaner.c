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
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "postmaster/postmaster.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_transaction.h"
#include "distributed/pg_dist_cleanup.h"

/* GUC configuration for shard cleaner */
int NextOperationId = 0;
int NextCleanupRecordId = 0;

/* Data structure for cleanup operation */

/*
 * CleanupRecord represents a record from pg_dist_cleanup.
 */
typedef struct CleanupRecord
{
	/* unique identifier of the record */
	uint64 recordId;

	/* identifier of the operation that generated the record */
	OperationId operationId;

	/* type of the object (e.g. shard) */
	CleanupObject objectType;

	/* fully qualified name of the object */
	char *objectName;

	/* node group ID on which the object is located */
	int nodeGroupId;

	/* cleanup policy that determines when object is cleaned */
	CleanupPolicy policy;
} CleanupRecord;

/* operation ID set by RegisterOperationNeedingCleanup */
OperationId CurrentOperationId = INVALID_OPERATION_ID;

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(citus_cleanup_orphaned_shards);
PG_FUNCTION_INFO_V1(isolation_cleanup_orphaned_shards);
PG_FUNCTION_INFO_V1(citus_cleanup_orphaned_resources);

static int DropOrphanedShardsForMove(bool waitForLocks);
static bool TryDropResourceByCleanupRecordOutsideTransaction(CleanupRecord *record,
															 char *nodeName,
															 int nodePort);
static bool TryDropShardOutsideTransaction(char *qualifiedTableName,
										   char *nodeName,
										   int nodePort);
static bool TryLockRelationAndPlacementCleanup(Oid relationId, LOCKMODE lockmode);

/* Functions for cleanup infrastructure */
static CleanupRecord * TupleToCleanupRecord(HeapTuple heapTuple,
											TupleDesc
											tupleDescriptor);
static OperationId GetNextOperationId(void);
static uint64 GetNextCleanupRecordId(void);
static void LockOperationId(OperationId operationId);
static bool TryLockOperationId(OperationId operationId);
static void DeleteCleanupRecordByRecordId(uint64 recordId);
static void DeleteCleanupRecordByRecordIdOutsideTransaction(uint64 recordId);
static bool CleanupRecordExists(uint64 recordId);
static List * ListCleanupRecords(void);
static List * ListCleanupRecordsForCurrentOperation(void);
static int DropOrphanedResourcesForCleanup(void);
static int CompareCleanupRecordsByObjectType(const void *leftElement,
											 const void *rightElement);

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
	int droppedShardCount = DropOrphanedShardsForMove(waitForLocks);
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
	int droppedShardCount = DropOrphanedShardsForMove(waitForLocks);
	if (droppedShardCount > 0)
	{
		ereport(NOTICE, (errmsg("cleaned up %d orphaned shards", droppedShardCount)));
	}

	PG_RETURN_VOID();
}


/*
 * citus_cleanup_orphaned_resources implements a user-facing UDF to delete
 * orphaned resources that are present in the system. These resources are
 * orphaned by previous actions that either failed or marked the resources
 * for deferred cleanup.
 *
 * The function takes no arguments and runs on co-ordinator. It cannot be run in a
 * transaction, because holding the locks it takes for a long time is not good.
 * While the locks are held, it is impossible for the background daemon to
 * perform concurrent cleanup.
 */
Datum
citus_cleanup_orphaned_resources(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	PreventInTransactionBlock(true, "citus_cleanup_orphaned_resources");

	int droppedCount = DropOrphanedResourcesForCleanup();
	if (droppedCount > 0)
	{
		ereport(NOTICE, (errmsg("cleaned up %d orphaned resources", droppedCount)));
	}

	PG_RETURN_VOID();
}


/*
 * DropOrphanedShardsInSeparateTransaction cleans up orphaned shards by
 * connecting to localhost. This is done, so that the locks that
 * DropOrphanedShardsForMove takes are only held for a short time.
 */
void
DropOrphanedShardsInSeparateTransaction(void)
{
	int connectionFlag = FORCE_NEW_CONNECTION;
	MultiConnection *connection = GetNodeConnection(connectionFlag, LocalHostName,
													PostPortNumber);
	ExecuteCriticalRemoteCommand(connection, "CALL citus_cleanup_orphaned_shards()");
	CloseConnection(connection);
}


/*
 * TryDropOrphanedResources is a wrapper around DropOrphanedShardsForMove and
 * DropOrphanedResourcesForCleanup that catches any errors to make it safe to
 * use in the maintenance daemon.
 *
 * If dropping any of the shards failed this function returns -1, otherwise it
 * returns the number of dropped shards.
 */
int
TryDropOrphanedResources(bool waitForLocks)
{
	int droppedShardCount = 0;
	MemoryContext savedContext = CurrentMemoryContext;

	/*
	 * Start a subtransaction so we can rollback database's state to it in case
	 * of error.
	 */
	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		droppedShardCount = DropOrphanedShardsForMove(waitForLocks);
		droppedShardCount += DropOrphanedResourcesForCleanup();

		/*
		 * Releasing a subtransaction doesn't free its memory context, since the
		 * data it contains will be needed at upper commit. See the comments for
		 * AtSubCommit_Memory() at postgres/src/backend/access/transam/xact.c.
		 */
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();

		/* rethrow as WARNING */
		edata->elevel = WARNING;
		ThrowErrorData(edata);
	}
	PG_END_TRY();

	return droppedShardCount;
}


/*
 * DropOrphanedResourcesForCleanup removes resources that were marked for cleanup by operation.
 * It does so by trying to take an exclusive lock on the resources. If the lock cannot be
 * obtained it skips the resource and continues with others.
 * The resource that has been skipped will be removed at a later iteration when there are no
 * locks held anymore.
 */
static int
DropOrphanedResourcesForCleanup()
{
	/* Only runs on Coordinator */
	if (!IsCoordinator())
	{
		return 0;
	}

	List *cleanupRecordList = ListCleanupRecords();

	/*
	 * We sort the records before cleaning up by their types, because of dependencies.
	 * For example, a subscription might depend on a publication.
	 */
	cleanupRecordList = SortList(cleanupRecordList,
								 CompareCleanupRecordsByObjectType);

	int removedResourceCountForCleanup = 0;
	int failedResourceCountForCleanup = 0;
	CleanupRecord *record = NULL;

	foreach_ptr(record, cleanupRecordList)
	{
		if (!PrimaryNodeForGroup(record->nodeGroupId, NULL))
		{
			continue;
		}

		/* Advisory locks are reentrant */
		if (!TryLockOperationId(record->operationId))
		{
			/* operation that the cleanup record is part of is still running */
			continue;
		}

		char *resourceName = record->objectName;
		WorkerNode *workerNode = LookupNodeForGroup(record->nodeGroupId);

		/*
		 * Now that we have the lock, check if record exists.
		 * The operation could have completed successfully just after we called
		 * ListCleanupRecords in which case the record will be now gone.
		 */
		if (!CleanupRecordExists(record->recordId))
		{
			continue;
		}

		if (TryDropResourceByCleanupRecordOutsideTransaction(record,
															 workerNode->workerName,
															 workerNode->workerPort))
		{
			if (record->policy == CLEANUP_DEFERRED_ON_SUCCESS)
			{
				ereport(LOG, (errmsg("deferred drop of orphaned resource %s on %s:%d "
									 "completed",
									 resourceName,
									 workerNode->workerName, workerNode->workerPort)));
			}
			else
			{
				ereport(LOG, (errmsg("cleaned up orphaned resource %s on %s:%d which "
									 "was left behind after a failed operation",
									 resourceName,
									 workerNode->workerName, workerNode->workerPort)));
			}

			/* delete the cleanup record */
			DeleteCleanupRecordByRecordId(record->recordId);
			removedResourceCountForCleanup++;
		}
		else
		{
			/*
			 * We log failures at the end, since they occur repeatedly
			 * for a large number of objects.
			 */
			failedResourceCountForCleanup++;
		}
	}

	if (failedResourceCountForCleanup > 0)
	{
		ereport(WARNING, (errmsg("failed to clean up %d orphaned resources out of %d",
								 failedResourceCountForCleanup,
								 list_length(cleanupRecordList))));
	}

	return removedResourceCountForCleanup;
}


/*
 * DropOrphanedShardsForMove removes shards that were marked SHARD_STATE_TO_DELETE before.
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
static int
DropOrphanedShardsForMove(bool waitForLocks)
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

		if (TryDropShardOutsideTransaction(qualifiedTableName,
										   shardPlacement->nodeName,
										   shardPlacement->nodePort))
		{
			ereport(LOG, (errmsg("deferred drop of orphaned shard %s on %s:%d "
								 "after a move completed",
								 qualifiedTableName,
								 shardPlacement->nodeName,
								 shardPlacement->nodePort)));

			/* delete the actual placement */
			DeleteShardPlacementRow(placement->placementId);
			removedShardCount++;
		}
		else
		{
			/*
			 * We log failures at the end, since they occur repeatedly
			 * for a large number of objects.
			 */
			failedShardDropCount++;
		}
	}

	if (failedShardDropCount > 0)
	{
		ereport(WARNING, (errmsg("failed to clean up %d orphaned shards out of %d",
								 failedShardDropCount, list_length(shardPlacementList))));
	}

	return removedShardCount;
}


/*
 * RegisterOperationNeedingCleanup is be called by an operation to register
 * for cleanup.
 */
OperationId
RegisterOperationNeedingCleanup(void)
{
	CurrentOperationId = GetNextOperationId();

	LockOperationId(CurrentOperationId);

	return CurrentOperationId;
}


/*
 * FinalizeOperationNeedingCleanupOnFailure is be called by an operation to signal
 * completion with failure. This will trigger cleanup of appropriate resources.
 */
void
FinalizeOperationNeedingCleanupOnFailure(const char *operationName)
{
	/* We must have a valid OperationId. Any operation requring cleanup
	 * will call RegisterOperationNeedingCleanup.
	 */
	Assert(CurrentOperationId != INVALID_OPERATION_ID);

	List *currentOperationRecordList = ListCleanupRecordsForCurrentOperation();

	/*
	 * We sort the records before cleaning up by their types, because of dependencies.
	 * For example, a subscription might depend on a publication.
	 */
	currentOperationRecordList = SortList(currentOperationRecordList,
										  CompareCleanupRecordsByObjectType);

	int failedShardCountOnComplete = 0;

	CleanupRecord *record = NULL;
	foreach_ptr(record, currentOperationRecordList)
	{
		if (record->policy == CLEANUP_ALWAYS || record->policy == CLEANUP_ON_FAILURE)
		{
			WorkerNode *workerNode = LookupNodeForGroup(record->nodeGroupId);

			/*
			 * For all resources of CurrentOperationId that are marked as 'CLEANUP_ALWAYS' or
			 * 'CLEANUP_ON_FAILURE', drop resource and cleanup records.
			 */
			if (TryDropResourceByCleanupRecordOutsideTransaction(record,
																 workerNode->workerName,
																 workerNode->workerPort))
			{
				/*
				 * Given the operation is failing and we will abort its transaction, we cannot delete
				 * records in the current transaction. Delete these records outside of the
				 * current transaction via a localhost connection.
				 */
				DeleteCleanupRecordByRecordIdOutsideTransaction(record->recordId);
			}
			else if (record->objectType == CLEANUP_OBJECT_SHARD_PLACEMENT)
			{
				/*
				 * We log failures at the end, since they occur repeatedly
				 * for a large number of objects.
				 */
				failedShardCountOnComplete++;
			}
		}
	}

	if (failedShardCountOnComplete > 0)
	{
		ereport(WARNING, (errmsg("failed to clean up %d orphaned shards out of %d after "
								 "a %s operation failed",
								 failedShardCountOnComplete,
								 list_length(currentOperationRecordList),
								 operationName)));
	}
}


/*
 * FinalizeOperationNeedingCleanupOnSuccess is be called by an operation to signal
 * completion with success. This will trigger cleanup of appropriate resources.
 */
void
FinalizeOperationNeedingCleanupOnSuccess(const char *operationName)
{
	/* We must have a valid OperationId. Any operation requring cleanup
	 * will call RegisterOperationNeedingCleanup.
	 */
	Assert(CurrentOperationId != INVALID_OPERATION_ID);

	List *currentOperationRecordList = ListCleanupRecordsForCurrentOperation();

	/*
	 * We sort the records before cleaning up by their types, because of dependencies.
	 * For example, a subscription might depend on a publication.
	 */
	currentOperationRecordList = SortList(currentOperationRecordList,
										  CompareCleanupRecordsByObjectType);

	int failedShardCountOnComplete = 0;

	CleanupRecord *record = NULL;
	foreach_ptr(record, currentOperationRecordList)
	{
		if (record->policy == CLEANUP_ALWAYS)
		{
			WorkerNode *workerNode = LookupNodeForGroup(record->nodeGroupId);

			/*
			 * For all resources of CurrentOperationId that are marked as 'CLEANUP_ALWAYS'
			 * drop resource and cleanup records.
			 */
			if (TryDropResourceByCleanupRecordOutsideTransaction(record,
																 workerNode->workerName,
																 workerNode->workerPort))
			{
				/*
				 * Delete cleanup records outside transaction as:
				 * The resources are marked as 'CLEANUP_ALWAYS' and should be cleaned no matter
				 * the operation succeeded or failed.
				 */
				DeleteCleanupRecordByRecordIdOutsideTransaction(record->recordId);
			}
			else if (record->objectType == CLEANUP_OBJECT_SHARD_PLACEMENT)
			{
				/*
				 * We log failures at the end, since they occur repeatedly
				 * for a large number of objects.
				 */
				failedShardCountOnComplete++;
			}
		}
		else if (record->policy == CLEANUP_ON_FAILURE)
		{
			/* Delete cleanup records (and not the actual resource) in same transaction as:
			 * The resources are marked as 'CLEANUP_ON_FAILURE' and we are approaching a successful
			 * completion of the operation. However, we cannot guarentee that operation will succeed
			 * so we tie the Delete with parent transaction.
			 */
			DeleteCleanupRecordByRecordId(record->recordId);
		}
	}

	if (failedShardCountOnComplete > 0)
	{
		ereport(WARNING, (errmsg(
							  "failed to clean up %d orphaned shards out of %d after "
							  "a %s operation completed",
							  failedShardCountOnComplete,
							  list_length(currentOperationRecordList),
							  operationName)));
	}
}


/*
 * CompareRecordsByObjectType is a comparison function for sort
 * cleanup records by their object type.
 */
static int
CompareCleanupRecordsByObjectType(const void *leftElement, const void *rightElement)
{
	CleanupRecord *leftRecord = *((CleanupRecord **) leftElement);
	CleanupRecord *rightRecord = *((CleanupRecord **) rightElement);

	/* we compare 64-bit integers, instead of casting their difference to int */
	if (leftRecord->objectType > rightRecord->objectType)
	{
		return 1;
	}
	else if (leftRecord->objectType < rightRecord->objectType)
	{
		return -1;
	}

	return 0;
}


/*
 * InsertCleanupRecordsForShardIntervalList inserts a record into pg_dist_cleanup,
 * with the given object type, name and policy, for all shard placements in the
 * given shardInterval list.
 */
void
InsertCleanupRecordsForShardIntervalList(List *shardIntervalList,
										 CleanupObject objectType,
										 char *objectName,
										 CleanupPolicy policy)
{
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		List *shardPlacementList = ActiveShardPlacementList(shardInterval->shardId);

		ShardPlacement *placement = NULL;
		foreach_ptr(placement, shardPlacementList)
		{
			/* log shard in pg_dist_cleanup */
			InsertCleanupRecordInSubtransaction(objectType,
												objectName,
												placement->groupId,
												policy);
		}
	}
}


/*
 * InsertCleanupRecordInCurrentTransaction inserts a new pg_dist_cleanup_record entry
 * as part of the current transaction. This is primarily useful for deferred drop scenarios,
 * since these records would roll back in case of operation failure.
 */
void
InsertCleanupRecordInCurrentTransaction(CleanupObject objectType,
										char *objectName,
										int nodeGroupId,
										CleanupPolicy policy)
{
	/* We must have a valid OperationId. Any operation requring cleanup
	 * will call RegisterOperationNeedingCleanup.
	 */
	Assert(CurrentOperationId != INVALID_OPERATION_ID);

	Datum values[Natts_pg_dist_cleanup];
	bool isNulls[Natts_pg_dist_cleanup];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	uint64 recordId = GetNextCleanupRecordId();
	OperationId operationId = CurrentOperationId;

	values[Anum_pg_dist_cleanup_record_id - 1] = UInt64GetDatum(recordId);
	values[Anum_pg_dist_cleanup_operation_id - 1] = UInt64GetDatum(operationId);
	values[Anum_pg_dist_cleanup_object_type - 1] = Int32GetDatum(objectType);
	values[Anum_pg_dist_cleanup_object_name - 1] = CStringGetTextDatum(objectName);
	values[Anum_pg_dist_cleanup_node_group_id - 1] = Int32GetDatum(nodeGroupId);
	values[Anum_pg_dist_cleanup_policy_type - 1] = Int32GetDatum(policy);

	/* open cleanup relation and insert new tuple */
	Oid relationId = DistCleanupRelationId();
	Relation pgDistCleanup = table_open(relationId, RowExclusiveLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistCleanup);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistCleanup, heapTuple);

	CommandCounterIncrement();
	table_close(pgDistCleanup, NoLock);
}


/*
 * InsertCleanupRecordInSubtransaction inserts a new pg_dist_cleanup_record entry
 * in a separate transaction to ensure the record persists after rollback. We should
 * delete these records if the operation completes successfully.
 *
 * For failure scenarios, use a subtransaction (direct insert via localhost).
 */
void
InsertCleanupRecordInSubtransaction(CleanupObject objectType,
									char *objectName,
									int nodeGroupId,
									CleanupPolicy policy)
{
	/* We must have a valid OperationId. Any operation requring cleanup
	 * will call RegisterOperationNeedingCleanup.
	 */
	Assert(CurrentOperationId != INVALID_OPERATION_ID);

	StringInfo sequenceName = makeStringInfo();
	appendStringInfo(sequenceName, "%s.%s",
					 PG_CATALOG,
					 CLEANUPRECORDID_SEQUENCE_NAME);

	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "INSERT INTO %s.%s "
					 " (record_id, operation_id, object_type, object_name, node_group_id, policy_type) "
					 " VALUES ( nextval('%s'), " UINT64_FORMAT ", %d, %s, %d, %d)",
					 PG_CATALOG,
					 PG_DIST_CLEANUP,
					 sequenceName->data,
					 CurrentOperationId,
					 objectType,
					 quote_literal_cstr(objectName),
					 nodeGroupId,
					 policy);

	MultiConnection *connection =
		GetConnectionForLocalQueriesOutsideTransaction(CitusExtensionOwnerName());
	SendCommandListToWorkerOutsideTransactionWithConnection(connection,
															list_make1(command->data));
}


/*
 * DeleteCleanupRecordByRecordId deletes a cleanup record by record id.
 */
static void
DeleteCleanupRecordByRecordIdOutsideTransaction(uint64 recordId)
{
	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "DELETE FROM %s.%s "
					 "WHERE record_id = %lu",
					 PG_CATALOG,
					 PG_DIST_CLEANUP,
					 recordId);

	MultiConnection *connection = GetConnectionForLocalQueriesOutsideTransaction(
		CitusExtensionOwnerName());
	SendCommandListToWorkerOutsideTransactionWithConnection(connection,
															list_make1(command->data));
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
 * TryDropResourceByCleanupRecordOutsideTransaction tries to drop the given resource
 * and returns true on success.
 */
static bool
TryDropResourceByCleanupRecordOutsideTransaction(CleanupRecord *record,
												 char *nodeName,
												 int nodePort)
{
	List *dropCommandList = NIL;

	switch (record->objectType)
	{
		case CLEANUP_OBJECT_SHARD_PLACEMENT:
		{
			return TryDropShardOutsideTransaction(record->objectName,
												  nodeName, nodePort);
		}

		case CLEANUP_OBJECT_SUBSCRIPTION:
		{
			StringInfo disableQuery = makeStringInfo();
			appendStringInfo(disableQuery,
							 "ALTER SUBSCRIPTION %s DISABLE",
							 quote_identifier(record->objectName));

			StringInfo alterQuery = makeStringInfo();
			appendStringInfo(alterQuery,
							 "ALTER SUBSCRIPTION %s SET (slot_name = NONE)",
							 quote_identifier(record->objectName));

			StringInfo dropQuery = makeStringInfo();
			appendStringInfo(dropQuery,
							 "DROP SUBSCRIPTION IF EXISTS %s",
							 quote_identifier(record->objectName));

			dropCommandList = list_make3(disableQuery->data,
										 alterQuery->data,
										 dropQuery->data);
			break;
		}

		case CLEANUP_OBJECT_PUBLICATION:
		{
			StringInfo dropQuery = makeStringInfo();
			appendStringInfo(dropQuery,
							 "DROP PUBLICATION IF EXISTS %s",
							 quote_identifier(record->objectName));

			dropCommandList = list_make1(dropQuery->data);
			break;
		}

		case CLEANUP_OBJECT_REPLICATION_SLOT:
		{
			StringInfo dropQuery = makeStringInfo();
			appendStringInfo(dropQuery,
							 "select pg_drop_replication_slot(slot_name) "
							 "from pg_replication_slots where slot_name = %s",
							 quote_literal_cstr(record->objectName));

			dropCommandList = list_make1(dropQuery->data);
			break;
		}

		case CLEANUP_OBJECT_USER:
		{
			StringInfo dropQuery = makeStringInfo();
			appendStringInfo(dropQuery,
							 "DROP USER IF EXISTS %s",
							 record->objectName);

			dropCommandList = list_make2("SET LOCAL citus.enable_ddl_propagation TO OFF",
										 dropQuery->data);
			break;
		}

		default:
		{
			ereport(WARNING, (errmsg(
								  "Invalid object type %d on failed operation cleanup",
								  record->objectType)));
			return false;
		}
	}

	int connectionFlags = OUTSIDE_TRANSACTION;
	MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  CitusExtensionOwnerName(),
																	  NULL);
	bool success = SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
		workerConnection,
		dropCommandList);

	return success;
}


/*
 * TryDropShardOutsideTransaction tries to drop the given shard placement and returns
 * true on success.
 */
static bool
TryDropShardOutsideTransaction(char *qualifiedTableName,
							   char *nodeName,
							   int nodePort)
{
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
	int connectionFlags = OUTSIDE_TRANSACTION;
	MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  CurrentUserName(),
																	  NULL);
	bool success = SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
		workerConnection,
		dropCommandList);

	return success;
}


/*
 * GetNextOperationId allocates and returns a unique operationId for an operation
 * requiring potential cleanup. This allocation occurs both in shared memory and
 * in write ahead logs; writing to logs avoids the risk of having operationId collisions.
 */
static OperationId
GetNextOperationId()
{
	OperationId operationdId = INVALID_OPERATION_ID;

	/*
	 * In regression tests, we would like to generate operation IDs consistently
	 * even if the tests run in parallel. Instead of the sequence, we can use
	 * the next_operation_id GUC to specify which operation ID the current session should
	 * generate next. The GUC is automatically increased by 1 every time a new
	 * operation ID is generated.
	 */
	if (NextOperationId > 0)
	{
		operationdId = NextOperationId;
		NextOperationId += 1;

		return operationdId;
	}

	/* Generate sequence using a subtransaction. else we can hold replication slot creation for operations */
	StringInfo sequenceName = makeStringInfo();
	appendStringInfo(sequenceName, "%s.%s",
					 PG_CATALOG,
					 OPERATIONID_SEQUENCE_NAME);

	StringInfo nextValueCommand = makeStringInfo();
	appendStringInfo(nextValueCommand, "SELECT nextval(%s);",
					 quote_literal_cstr(sequenceName->data));

	MultiConnection *connection = GetConnectionForLocalQueriesOutsideTransaction(
		CitusExtensionOwnerName());

	PGresult *result = NULL;
	int queryResult = ExecuteOptionalRemoteCommand(connection, nextValueCommand->data,
												   &result);
	if (queryResult != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1 ||
		PQnfields(result) != 1)
	{
		ReportResultError(connection, result, ERROR);
	}

	operationdId = SafeStringToUint64(PQgetvalue(result, 0, 0 /* nodeId column*/));

	PQclear(result);
	ForgetResults(connection);

	return operationdId;
}


/*
 * ListCleanupRecords lists all the current cleanup records.
 */
static List *
ListCleanupRecords(void)
{
	Relation pgDistCleanup = table_open(DistCleanupRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistCleanup);

	List *recordList = NIL;
	int scanKeyCount = 0;
	bool indexOK = false;

	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanup, InvalidOid,
													indexOK, NULL, scanKeyCount, NULL);

	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		CleanupRecord *record = TupleToCleanupRecord(heapTuple, tupleDescriptor);
		recordList = lappend(recordList, record);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistCleanup, NoLock);

	return recordList;
}


/*
 * ListCleanupRecordsForCurrentOperation lists all the cleanup records for
 * current operation.
 */
static List *
ListCleanupRecordsForCurrentOperation(void)
{
	/* We must have a valid OperationId. Any operation requring cleanup
	 * will call RegisterOperationNeedingCleanup.
	 */
	Assert(CurrentOperationId != INVALID_OPERATION_ID);

	Relation pgDistCleanup = table_open(DistCleanupRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistCleanup);

	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_pg_dist_cleanup_operation_id, BTEqualStrategyNumber,
				F_INT8EQ, UInt64GetDatum(CurrentOperationId));

	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;
	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanup, scanIndexId, useIndex,
													NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = NULL;
	List *recordList = NIL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		CleanupRecord *record = TupleToCleanupRecord(heapTuple, tupleDescriptor);
		recordList = lappend(recordList, record);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistCleanup, NoLock);

	return recordList;
}


/*
 * TupleToCleanupRecord converts a pg_dist_cleanup record tuple into a CleanupRecord struct.
 */
static CleanupRecord *
TupleToCleanupRecord(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	Datum datumArray[Natts_pg_dist_cleanup];
	bool isNullArray[Natts_pg_dist_cleanup];
	heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

	CleanupRecord *record = palloc0(sizeof(CleanupRecord));

	record->recordId =
		DatumGetUInt64(datumArray[Anum_pg_dist_cleanup_record_id - 1]);

	record->operationId =
		DatumGetUInt64(datumArray[Anum_pg_dist_cleanup_operation_id - 1]);

	record->objectType =
		DatumGetInt32(datumArray[Anum_pg_dist_cleanup_object_type - 1]);

	record->objectName =
		TextDatumGetCString(datumArray[Anum_pg_dist_cleanup_object_name - 1]);

	record->nodeGroupId =
		DatumGetInt32(datumArray[Anum_pg_dist_cleanup_node_group_id - 1]);

	record->policy =
		DatumGetInt32(datumArray[Anum_pg_dist_cleanup_policy_type - 1]);

	return record;
}


/*
 * CleanupRecordExists returns whether a cleanup record with the given
 * record ID exists in pg_dist_cleanup_record.
 */
static bool
CleanupRecordExists(uint64 recordId)
{
	Relation pgDistCleanup = table_open(DistCleanupRelationId(),
										AccessShareLock);

	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_cleanup_record_id,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(recordId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanup,
													DistCleanupPrimaryKeyIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	bool recordExists = HeapTupleIsValid(heapTuple);

	systable_endscan(scanDescriptor);

	CommandCounterIncrement();
	table_close(pgDistCleanup, NoLock);

	return recordExists;
}


/*
 * DeleteCleanupRecordByRecordId deletes a single pg_dist_cleanup_record entry.
 */
static void
DeleteCleanupRecordByRecordId(uint64 recordId)
{
	Relation pgDistCleanup = table_open(DistCleanupRelationId(),
										RowExclusiveLock);

	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_cleanup_record_id,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(recordId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanup,
													DistCleanupPrimaryKeyIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (heapTuple == NULL)
	{
		ereport(ERROR, (errmsg("could not find cleanup record " UINT64_FORMAT,
							   recordId)));
	}

	simple_heap_delete(pgDistCleanup, &heapTuple->t_self);

	systable_endscan(scanDescriptor);

	CommandCounterIncrement();
	table_close(pgDistCleanup, NoLock);
}


/*
 * GetNextCleanupRecordId allocates and returns a unique recordid for a cleanup entry.
 * This allocation occurs both in shared memory and
 * in write ahead logs; writing to logs avoids the risk of having operationId collisions.
 */
static uint64
GetNextCleanupRecordId(void)
{
	uint64 recordId = INVALID_CLEANUP_RECORD_ID;

	/*
	 * In regression tests, we would like to generate record IDs consistently
	 * even if the tests run in parallel. Instead of the sequence, we can use
	 * the next_record_id GUC to specify which recordid ID the current session should
	 * generate next. The GUC is automatically increased by 1 every time a new
	 * record ID is generated.
	 */
	if (NextCleanupRecordId > 0)
	{
		recordId = NextCleanupRecordId;
		NextCleanupRecordId += 1;

		return recordId;
	}

	RangeVar *sequenceName = makeRangeVar(PG_CATALOG,
										  CLEANUPRECORDID_SEQUENCE_NAME,
										  -1);

	bool missingOK = false;
	Oid sequenceId = RangeVarGetRelid(sequenceName, NoLock, missingOK);
	bool checkPermissions = false;
	return nextval_internal(sequenceId, checkPermissions);
}


/*
 * LockOperationId takes an exclusive lock to ensure that only one process
 * can cleanup operationId resources at the same time.
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
 * TryLockOperationId takes an exclusive lock (with dontWait = true) to ensure that
 * only one process can cleanup operationId resources at the same time.
 */
static bool
TryLockOperationId(OperationId operationId)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;
	SET_LOCKTAG_CLEANUP_OPERATION_ID(tag, operationId);
	LockAcquireResult lockResult = LockAcquire(&tag, ExclusiveLock, sessionLock,
											   dontWait);
	return (lockResult != LOCKACQUIRE_NOT_AVAIL);
}
