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
#include "distributed/listutils.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/array.h"
#include "utils/builtins.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_metadata_snapshot);
PG_FUNCTION_INFO_V1(wait_until_metadata_sync);
PG_FUNCTION_INFO_V1(trigger_metadata_sync);
PG_FUNCTION_INFO_V1(raise_error_in_metadata_sync);


/*
 * master_metadata_snapshot prints all the queries that are required
 * to generate a metadata snapshot.
 */
Datum
master_metadata_snapshot(PG_FUNCTION_ARGS)
{
	List *dropSnapshotCommands = MetadataDropCommands();
	List *createSnapshotCommands = MetadataCreateCommands();
	List *snapshotCommandList = NIL;
	int snapshotCommandIndex = 0;
	Oid ddlCommandTypeId = TEXTOID;

	snapshotCommandList = list_concat(snapshotCommandList, dropSnapshotCommands);
	snapshotCommandList = list_concat(snapshotCommandList, createSnapshotCommands);

	int snapshotCommandCount = list_length(snapshotCommandList);
	Datum *snapshotCommandDatumArray = palloc0(snapshotCommandCount * sizeof(Datum));

	const char *metadataSnapshotCommand = NULL;
	foreach_ptr(metadataSnapshotCommand, snapshotCommandList)
	{
		Datum metadataSnapshotCommandDatum = CStringGetTextDatum(metadataSnapshotCommand);

		snapshotCommandDatumArray[snapshotCommandIndex] = metadataSnapshotCommandDatum;
		snapshotCommandIndex++;
	}

	ArrayType *snapshotCommandArrayType = DatumArrayToArrayType(snapshotCommandDatumArray,
																snapshotCommandCount,
																ddlCommandTypeId);

	PG_RETURN_ARRAYTYPE_P(snapshotCommandArrayType);
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
													"localhost", PostPortNumber);
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
	TriggerMetadataSync(MyDatabaseId);
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
