/*-------------------------------------------------------------------------
 *
 * shard_cleaner.c
 *	  This implements the background process that cleans shards and resources
 *	  that are left around.
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
#include "nodes/makefuncs.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_cleanup.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/worker_transaction.h"

#define REPLICATION_SLOT_CATALOG_TABLE_NAME "pg_replication_slots"
#define STR_ERRCODE_OBJECT_IN_USE "55006"
#define STR_ERRCODE_UNDEFINED_OBJECT "42704"

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
PG_FUNCTION_INFO_V1(citus_cleanup_orphaned_resources);
PG_FUNCTION_INFO_V1(isolation_cleanup_orphaned_resources);

static bool TryDropResourceByCleanupRecordOutsideTransaction(CleanupRecord *record,
															 char *nodeName,
															 int nodePort);
static bool TryDropShardOutsideTransaction(char *qualifiedTableName,
										   char *nodeName,
										   int nodePort);
static bool TryDropSubscriptionOutsideTransaction(char *subscriptionName,
												  char *nodeName,
												  int nodePort);
static bool TryDropPublicationOutsideTransaction(char *publicationName,
												 char *nodeName,
												 int nodePort);
static bool TryDropReplicationSlotOutsideTransaction(char *replicationSlotName,
													 char *nodeName,
													 int nodePort);
static bool TryDropUserOutsideTransaction(char *username, char *nodeName, int nodePort);

static CleanupRecord * GetCleanupRecordByNameAndType(char *objectName,
													 CleanupObject type);

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
 * citus_cleanup_orphaned_shards is noop.
 * Use citus_cleanup_orphaned_resources instead.
 */
Datum
citus_cleanup_orphaned_shards(PG_FUNCTION_ARGS)
{
	ereport(WARNING, (errmsg("citus_cleanup_orphaned_shards is deprecated. "
							 "Use citus_cleanup_orphaned_resources instead")));
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
 * isolation_cleanup_orphaned_resources implements a test UDF that's the same as
 * citus_cleanup_orphaned_resources. The only difference is that this command can
 * be run in transactions, this is needed to test this function in isolation tests
 * since commands are automatically run in transactions there.
 */
Datum
isolation_cleanup_orphaned_resources(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	DropOrphanedResourcesForCleanup();

	PG_RETURN_VOID();
}


/*
 * DropOrphanedResourcesInSeparateTransaction cleans up orphaned resources by
 * connecting to localhost.
 */
void
DropOrphanedResourcesInSeparateTransaction(void)
{
	int connectionFlag = FORCE_NEW_CONNECTION;
	MultiConnection *connection = GetNodeConnection(connectionFlag, LocalHostName,
													PostPortNumber);
	ExecuteCriticalRemoteCommand(connection, "CALL citus_cleanup_orphaned_resources()");
	CloseConnection(connection);
}


/*
 * TryDropOrphanedResources is a wrapper around DropOrphanedResourcesForCleanup
 * that catches any errors to make it safe to use in the maintenance daemon.
 *
 * If dropping any of the resources failed this function returns -1, otherwise it
 * returns the number of dropped resources.
 */
int
TryDropOrphanedResources()
{
	int droppedResourceCount = 0;
	MemoryContext savedContext = CurrentMemoryContext;

	/*
	 * Start a subtransaction so we can rollback database's state to it in case
	 * of error.
	 */
	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		droppedResourceCount = DropOrphanedResourcesForCleanup();

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

	return droppedResourceCount;
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

	foreach_declared_ptr(record, cleanupRecordList)
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
	foreach_declared_ptr(record, currentOperationRecordList)
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
 * InsertCleanupRecordInCurrentTransaction inserts a new pg_dist_cleanup entry
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
 * InsertCleanupRecordInSubtransaction inserts a new pg_dist_cleanup entry in a
 * separate transaction to ensure the record persists after rollback. We should
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
 * DeleteCleanupRecordByRecordIdOutsideTransaction deletes a cleanup record by record id.
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
 * TryDropResourceByCleanupRecordOutsideTransaction tries to drop the given resource
 * and returns true on success.
 */
static bool
TryDropResourceByCleanupRecordOutsideTransaction(CleanupRecord *record,
												 char *nodeName,
												 int nodePort)
{
	switch (record->objectType)
	{
		case CLEANUP_OBJECT_SHARD_PLACEMENT:
		{
			return TryDropShardOutsideTransaction(record->objectName,
												  nodeName, nodePort);
		}

		case CLEANUP_OBJECT_SUBSCRIPTION:
		{
			return TryDropSubscriptionOutsideTransaction(record->objectName,
														 nodeName, nodePort);
		}

		case CLEANUP_OBJECT_PUBLICATION:
		{
			return TryDropPublicationOutsideTransaction(record->objectName,
														nodeName, nodePort);
		}

		case CLEANUP_OBJECT_REPLICATION_SLOT:
		{
			return TryDropReplicationSlotOutsideTransaction(record->objectName,
															nodeName, nodePort);
		}

		case CLEANUP_OBJECT_USER:
		{
			return TryDropUserOutsideTransaction(record->objectName, nodeName, nodePort);
		}

		default:
		{
			ereport(WARNING, (errmsg(
								  "Invalid object type %d on failed operation cleanup",
								  record->objectType)));
			return false;
		}
	}

	return false;
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
 * TryDropSubscriptionOutsideTransaction drops subscription with the given name on the
 * subscriber node if it exists. Note that this doesn't drop the replication slot on the
 * publisher node. The reason is that sometimes this is not possible. To known
 * cases where this is not possible are:
 * 1. Due to the node with the replication slot being down.
 * 2. Due to a deadlock when the replication is on the same node as the
 *    subscription, which is the case for shard splits to the local node.
 *
 * So instead of directly dropping the subscription, including the attached
 * replication slot, the subscription is first disconnected from the
 * replication slot before dropping it. The replication slot itself should be
 * dropped using DropReplicationSlot on the source connection.
 */
static bool
TryDropSubscriptionOutsideTransaction(char *subscriptionName,
									  char *nodeName,
									  int nodePort)
{
	int connectionFlags = OUTSIDE_TRANSACTION;
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																nodeName, nodePort,
																CitusExtensionOwnerName(),
																NULL);

	RemoteTransactionBegin(connection);

	if (ExecuteOptionalRemoteCommand(connection,
									 "SET LOCAL lock_timeout TO '1s'", NULL) != 0)
	{
		RemoteTransactionAbort(connection);
		ResetRemoteTransaction(connection);
		return false;
	}

	int querySent = SendRemoteCommand(
		connection,
		psprintf("ALTER SUBSCRIPTION %s DISABLE", quote_identifier(subscriptionName)));
	if (querySent == 0)
	{
		ReportConnectionError(connection, WARNING);
		RemoteTransactionAbort(connection);
		ResetRemoteTransaction(connection);
		return false;
	}

	bool raiseInterrupts = true;
	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);

	if (!IsResponseOK(result))
	{
		char *errorcode = PQresultErrorField(result, PG_DIAG_SQLSTATE);
		if (errorcode != NULL && strcmp(errorcode, STR_ERRCODE_UNDEFINED_OBJECT) == 0)
		{
			/*
			 * The subscription doesn't exist, so we can return right away.
			 * This DropSubscription call is effectively a no-op.
			 */
			PQclear(result);
			ForgetResults(connection);
			RemoteTransactionAbort(connection);
			ResetRemoteTransaction(connection);
			return true;
		}
		else
		{
			ReportResultError(connection, result, WARNING);
			PQclear(result);
			ForgetResults(connection);
			RemoteTransactionAbort(connection);
			ResetRemoteTransaction(connection);
			return false;
		}
	}

	PQclear(result);
	ForgetResults(connection);
	RemoteTransactionCommit(connection);
	ResetRemoteTransaction(connection);

	StringInfo alterQuery = makeStringInfo();
	appendStringInfo(alterQuery,
					 "ALTER SUBSCRIPTION %s SET (slot_name = NONE)",
					 quote_identifier(subscriptionName));

	StringInfo dropQuery = makeStringInfo();
	appendStringInfo(dropQuery,
					 "DROP SUBSCRIPTION %s",
					 quote_identifier(subscriptionName));

	List *dropCommandList = list_make3("SET LOCAL lock_timeout TO '1s'",
									   alterQuery->data, dropQuery->data);
	bool success = SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
		connection,
		dropCommandList);

	return success;
}


/*
 * TryDropPublicationOutsideTransaction drops the publication with the given name if it
 * exists.
 */
static bool
TryDropPublicationOutsideTransaction(char *publicationName,
									 char *nodeName,
									 int nodePort)
{
	int connectionFlags = OUTSIDE_TRANSACTION;
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																nodeName, nodePort,
																CitusExtensionOwnerName(),
																NULL);
	StringInfo dropQuery = makeStringInfo();
	appendStringInfo(dropQuery,
					 "DROP PUBLICATION IF EXISTS %s",
					 quote_identifier(publicationName));

	List *dropCommandList = list_make2("SET LOCAL lock_timeout TO '1s'",
									   dropQuery->data);
	bool success = SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
		connection,
		dropCommandList);

	return success;
}


/*
 * TryDropReplicationSlotOutsideTransaction drops the replication slot with the given
 * name if it exists.
 */
static bool
TryDropReplicationSlotOutsideTransaction(char *replicationSlotName,
										 char *nodeName,
										 int nodePort)
{
	int connectionFlags = OUTSIDE_TRANSACTION;
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																nodeName, nodePort,
																CitusExtensionOwnerName(),
																NULL);

	RemoteTransactionBegin(connection);

	if (ExecuteOptionalRemoteCommand(connection,
									 "SET LOCAL lock_timeout TO '1s'", NULL) != 0)
	{
		RemoteTransactionAbort(connection);
		ResetRemoteTransaction(connection);
		return false;
	}

	int querySent = SendRemoteCommand(
		connection,
		psprintf(
			"select pg_drop_replication_slot(slot_name) from "
			REPLICATION_SLOT_CATALOG_TABLE_NAME
			" where slot_name = %s",
			quote_literal_cstr(replicationSlotName))
		);

	if (querySent == 0)
	{
		ReportConnectionError(connection, WARNING);
		RemoteTransactionAbort(connection);
		ResetRemoteTransaction(connection);
		return false;
	}

	bool raiseInterrupts = true;
	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);

	if (IsResponseOK(result))
	{
		PQclear(result);
		ForgetResults(connection);
		RemoteTransactionCommit(connection);
		ResetRemoteTransaction(connection);
		return true;
	}

	char *errorcode = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	if (errorcode != NULL && strcmp(errorcode, STR_ERRCODE_OBJECT_IN_USE) != 0)
	{
		/* throw a warning unless object is in use */
		ReportResultError(connection, result, WARNING);
	}

	PQclear(result);
	ForgetResults(connection);
	RemoteTransactionAbort(connection);
	ResetRemoteTransaction(connection);

	return false;
}


/*
 * TryDropUserOutsideTransaction drops the user with the given name if it exists.
 */
static bool
TryDropUserOutsideTransaction(char *username,
							  char *nodeName, int nodePort)
{
	int connectionFlags = OUTSIDE_TRANSACTION;
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																nodeName, nodePort,
																CitusExtensionOwnerName(),
																NULL);

	/*
	 * The DROP USER command should not propagate, so we temporarily disable
	 * DDL propagation.
	 */
	bool success = SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
		connection,
		list_make3(
			"SET LOCAL lock_timeout TO '1s'",
			"SET LOCAL citus.enable_ddl_propagation TO OFF;",
			psprintf("DROP USER IF EXISTS %s;",
					 quote_identifier(username))));

	return success;
}


/*
 * ErrorIfCleanupRecordForShardExists errors out if a cleanup record for the given
 * shard name exists.
 */
void
ErrorIfCleanupRecordForShardExists(char *shardName)
{
	CleanupRecord *record =
		GetCleanupRecordByNameAndType(shardName, CLEANUP_OBJECT_SHARD_PLACEMENT);

	if (record == NULL)
	{
		return;
	}

	ereport(ERROR, (errmsg("shard move failed as the orphaned shard %s leftover "
						   "from the previous move could not be cleaned up",
						   record->objectName)));
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
				F_INT8EQ, Int64GetDatum(CurrentOperationId));

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
 * GetCleanupRecordByNameAndType returns the cleanup record with given name and type,
 * if any, returns NULL otherwise.
 */
static CleanupRecord *
GetCleanupRecordByNameAndType(char *objectName, CleanupObject type)
{
	CleanupRecord *objectFound = NULL;

	Relation pgDistCleanup = table_open(DistCleanupRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistCleanup);

	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_pg_dist_cleanup_object_type, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(type));

	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;
	SysScanDesc scanDescriptor = systable_beginscan(pgDistCleanup, scanIndexId, useIndex,
													NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		CleanupRecord *record = TupleToCleanupRecord(heapTuple, tupleDescriptor);
		if (strcmp(record->objectName, objectName) == 0)
		{
			objectFound = record;
			break;
		}
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistCleanup, NoLock);

	return objectFound;
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
 * record ID exists in pg_dist_cleanup.
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
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(recordId));

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
 * DeleteCleanupRecordByRecordId deletes a single pg_dist_cleanup entry.
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
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(recordId));

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
