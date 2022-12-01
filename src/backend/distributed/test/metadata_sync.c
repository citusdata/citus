/*-------------------------------------------------------------------------
 *
 * test/src/metadata_sync.c
 *
 * This file contains functions to exercise the metadata snapshoy
 * generation functionality within Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "catalog/pg_type.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/listutils.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/utils/array_type.h"
#include "distributed/worker_manager.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/array.h"
#include "utils/builtins.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(activate_node_snapshot);
PG_FUNCTION_INFO_V1(wait_until_metadata_sync);
PG_FUNCTION_INFO_V1(trigger_metadata_sync);
PG_FUNCTION_INFO_V1(raise_error_in_metadata_sync);


/*
 * activate_node_snapshot prints all the queries that are required
 * to activate a node.
 */
Datum
activate_node_snapshot(PG_FUNCTION_ARGS)
{
	/*
	 * Activate node commands are created using the given worker node,
	 * so we are using first primary worker node just for test purposes.
	 */
	WorkerNode *dummyWorkerNode = GetFirstPrimaryWorkerNode();

	List *updateLocalGroupCommand =
		list_make1(LocalGroupIdUpdateCommand(dummyWorkerNode->groupId));
	List *dropMetadataCommandList = DropExistingMetadataCommandList();
	List *syncDistObjCommands = SyncDistributedObjectsCommandList(dummyWorkerNode);
	List *dropNodeSnapshotCommands = NodeMetadataDropCommands();
	List *createSnapshotCommands = NodeMetadataCreateCommands();
	List *pgDistTableMetadataSyncCommands = PgDistTableMetadataSyncCommandList();

	List *activateNodeCommandList = NIL;
	int activateNodeCommandIndex = 0;
	Oid ddlCommandTypeId = TEXTOID;

	activateNodeCommandList =
		list_concat(activateNodeCommandList, updateLocalGroupCommand);
	activateNodeCommandList =
		list_concat(activateNodeCommandList, dropMetadataCommandList);
	activateNodeCommandList = list_concat(activateNodeCommandList, syncDistObjCommands);
	activateNodeCommandList =
		list_concat(activateNodeCommandList, dropNodeSnapshotCommands);
	activateNodeCommandList = list_concat(activateNodeCommandList,
										  createSnapshotCommands);
	activateNodeCommandList = list_concat(activateNodeCommandList,
										  pgDistTableMetadataSyncCommands);

	int activateNodeCommandCount = list_length(activateNodeCommandList);
	Datum *activateNodeCommandDatumArray = palloc0(activateNodeCommandCount *
												   sizeof(Datum));

	const char *activateNodeSnapshotCommand = NULL;
	foreach_ptr(activateNodeSnapshotCommand, activateNodeCommandList)
	{
		Datum activateNodeSnapshotCommandDatum = CStringGetTextDatum(
			activateNodeSnapshotCommand);

		activateNodeCommandDatumArray[activateNodeCommandIndex] =
			activateNodeSnapshotCommandDatum;
		activateNodeCommandIndex++;
	}

	ArrayType *activateNodeCommandArrayType = DatumArrayToArrayType(
		activateNodeCommandDatumArray,
		activateNodeCommandCount,
		ddlCommandTypeId);

	PG_RETURN_ARRAYTYPE_P(activateNodeCommandArrayType);
}


/*
 * wait_until_metadata_sync waits until the maintenance daemon does a metadata
 * sync, or times out.
 */
Datum
wait_until_metadata_sync(PG_FUNCTION_ARGS)
{
	uint32 timeout = PG_GETARG_UINT32(0);

	List *workerList = ActivePrimaryNonCoordinatorNodeList(NoLock);
	bool waitNotifications = false;

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerList)
	{
		/* if already has metadata, no need to do it again */
		if (workerNode->hasMetadata && !workerNode->metadataSynced)
		{
			waitNotifications = true;
			break;
		}
	}

	/*
	 * If all the metadata nodes have already been synced, we should not wait.
	 * That's primarily because the maintenance deamon might have already sent
	 * the notification and we'd wait unnecessarily here. Worse, the test outputs
	 * might be inconsistent across executions due to the warning.
	 */
	if (!waitNotifications)
	{
		PG_RETURN_VOID();
	}

	MultiConnection *connection = GetNodeConnection(FORCE_NEW_CONNECTION,
													LOCAL_HOST_NAME, PostPortNumber);
	ExecuteCriticalRemoteCommand(connection, "LISTEN " METADATA_SYNC_CHANNEL);

	int waitFlags = WL_SOCKET_READABLE | WL_TIMEOUT | WL_POSTMASTER_DEATH;
	int waitResult = WaitLatchOrSocket(NULL, waitFlags, PQsocket(connection->pgConn),
									   timeout, 0);
	if (waitResult & WL_POSTMASTER_DEATH)
	{
		ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
	}
	else if (waitResult & WL_SOCKET_MASK)
	{
		ClearResults(connection, true);
	}
	else if (waitResult & WL_TIMEOUT)
	{
		elog(WARNING, "waiting for metadata sync timed out");
	}

	CloseConnection(connection);

	PG_RETURN_VOID();
}


/*
 * trigger_metadata_sync triggers metadata sync for testing.
 */
Datum
trigger_metadata_sync(PG_FUNCTION_ARGS)
{
	TriggerNodeMetadataSyncOnCommit();
	PG_RETURN_VOID();
}


/*
 * raise_error_in_metadata_sync causes metadata sync to raise an error.
 */
Datum
raise_error_in_metadata_sync(PG_FUNCTION_ARGS)
{
	/* metadata sync uses SIGALRM to test errors */
	SignalMetadataSyncDaemon(MyDatabaseId, SIGALRM);
	PG_RETURN_VOID();
}
