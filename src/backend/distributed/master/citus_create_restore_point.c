/*-------------------------------------------------------------------------
 *
 * citus_create_restore_point.c
 *
 * UDF for creating a consistent restore point across all nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_type.h"
#include "distributed/connection_management.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "nodes/pg_list.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"


#define CREATE_RESTORE_POINT_COMMAND "SELECT pg_catalog.pg_create_restore_point($1::text)"


/* local functions forward declarations */
static List * OpenConnectionsToAllWorkerNodes(LOCKMODE lockMode);
static void BlockDistributedTransactions(void);
static void CreateRemoteRestorePoints(char *restoreName, List *connectionList);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_create_restore_point);


/*
 * citus_create_restore_point blocks writes to distributed tables and then
 * runs pg_create_restore_point on all nodes. This creates a consistent
 * restore point under the assumption that there are no other writers
 * than the coordinator.
 */
Datum
citus_create_restore_point(PG_FUNCTION_ARGS)
{
	text *restoreNameText = PG_GETARG_TEXT_P(0);
	char *restoreNameString = NULL;
	XLogRecPtr localRestorePoint = InvalidXLogRecPtr;
	List *connectionList = NIL;

	CheckCitusVersion(ERROR);
	EnsureSuperUser();
	EnsureCoordinator();

	if (RecoveryInProgress())
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("recovery is in progress"),
				  errhint("WAL control functions cannot be executed during recovery."))));
	}

	if (!XLogIsNeeded())
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL level not sufficient for creating a restore point"),
				 errhint("wal_level must be set to \"replica\" or \"logical\" at server "
						 "start.")));
	}

	restoreNameString = text_to_cstring(restoreNameText);
	if (strlen(restoreNameString) >= MAXFNAMELEN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("value too long for restore point (maximum %d characters)",
						MAXFNAMELEN - 1)));
	}

	/*
	 * establish connections to all nodes before taking any locks
	 * ShareLock prevents new nodes being added, rendering connectionList incomplete
	 */
	connectionList = OpenConnectionsToAllWorkerNodes(ShareLock);

	/*
	 * Send a BEGIN to bust through pgbouncer. We won't actually commit since
	 * that takes time. Instead we just close the connections and roll back,
	 * which doesn't undo pg_create_restore_point.
	 */
	RemoteTransactionListBegin(connectionList);

	/* DANGER: finish as quickly as possible after this */
	BlockDistributedTransactions();

	/* do local restore point first to bail out early if something goes wrong */
	localRestorePoint = XLogRestorePoint(restoreNameString);

	/* run pg_create_restore_point on all nodes */
	CreateRemoteRestorePoints(restoreNameString, connectionList);

	PG_RETURN_LSN(localRestorePoint);
}


/*
 * OpenConnectionsToAllNodes opens connections to all nodes and returns the list
 * of connections.
 */
static List *
OpenConnectionsToAllWorkerNodes(LOCKMODE lockMode)
{
	List *connectionList = NIL;
	List *workerNodeList = NIL;
	ListCell *workerNodeCell = NULL;
	int connectionFlags = FORCE_NEW_CONNECTION;

	workerNodeList = ActivePrimaryWorkerNodeList(lockMode);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		MultiConnection *connection = NULL;

		connection = StartNodeConnection(connectionFlags, workerNode->workerName,
										 workerNode->workerPort);
		MarkRemoteTransactionCritical(connection);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	return connectionList;
}


/*
 * BlockDistributedTransactions blocks distributed transactions that use 2PC
 * and changes to pg_dist_node (e.g. node addition) and pg_dist_partition
 * (table creation).
 */
static void
BlockDistributedTransactions(void)
{
	LockRelationOid(DistNodeRelationId(), ExclusiveLock);
	LockRelationOid(DistPartitionRelationId(), ExclusiveLock);
	LockRelationOid(DistTransactionRelationId(), ExclusiveLock);
}


/*
 * CreateRemoteRestorePoints creates a restore point via each of the
 * connections in the list in parallel.
 */
static void
CreateRemoteRestorePoints(char *restoreName, List *connectionList)
{
	ListCell *connectionCell = NULL;
	int parameterCount = 1;
	Oid parameterTypes[1] = { TEXTOID };
	const char *parameterValues[1] = { restoreName };

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		int querySent = SendRemoteCommandParams(connection, CREATE_RESTORE_POINT_COMMAND,
												parameterCount, parameterTypes,
												parameterValues);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);

		ForgetResults(connection);
		CloseConnection(connection);
	}
}
