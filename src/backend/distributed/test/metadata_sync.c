/*-------------------------------------------------------------------------
 *
 * test/src/metadata_sync.c
 *
 * This file contains functions to exercise the metadata snapshoy
 * generation functionality within Citus.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "catalog/pg_type.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
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
	ListCell *snapshotCommandCell = NULL;
	int snapshotCommandCount = 0;
	Datum *snapshotCommandDatumArray = NULL;
	ArrayType *snapshotCommandArrayType = NULL;
	int snapshotCommandIndex = 0;
	Oid ddlCommandTypeId = TEXTOID;

	snapshotCommandList = list_concat(snapshotCommandList, dropSnapshotCommands);
	snapshotCommandList = list_concat(snapshotCommandList, createSnapshotCommands);

	snapshotCommandCount = list_length(snapshotCommandList);
	snapshotCommandDatumArray = palloc0(snapshotCommandCount * sizeof(Datum));

	foreach(snapshotCommandCell, snapshotCommandList)
	{
		char *metadataSnapshotCommand = (char *) lfirst(snapshotCommandCell);
		Datum metadataSnapshotCommandDatum = CStringGetTextDatum(metadataSnapshotCommand);

		snapshotCommandDatumArray[snapshotCommandIndex] = metadataSnapshotCommandDatum;
		snapshotCommandIndex++;
	}

	snapshotCommandArrayType = DatumArrayToArrayType(snapshotCommandDatumArray,
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
	int waitResult = 0;

	MultiConnection *connection = GetNodeConnection(FORCE_NEW_CONNECTION,
													"localhost", PostPortNumber);
	ExecuteCriticalRemoteCommand(connection, "LISTEN " METADATA_SYNC_CHANNEL, NULL);

	waitResult = WaitLatchOrSocket(NULL, WL_SOCKET_READABLE | WL_TIMEOUT,
								   PQsocket(connection->pgConn), timeout, 0);
	if (waitResult & WL_SOCKET_MASK)
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
