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
#include "nodes/pg_list.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/remote_commands.h"


#define CREATE_RESTORE_POINT_COMMAND "SELECT pg_catalog.pg_create_restore_point($1::text)"

/*
 * BLOCK_DISTRIBUTED_WRITES_COMMAND acquires ExclusiveLock on:
 * 1. pg_dist_transaction - blocks 2PC commit decisions
 * 2. pg_dist_partition - blocks DDL operations on distributed tables
 *
 * This ensures both DML (via 2PC) and DDL are blocked on metadata nodes.
 */
#define BLOCK_DISTRIBUTED_WRITES_COMMAND \
		"LOCK TABLE pg_catalog.pg_dist_transaction IN EXCLUSIVE MODE; " \
		"LOCK TABLE pg_catalog.pg_dist_partition IN EXCLUSIVE MODE"

/* local functions forward declarations */
static List * OpenConnectionsToAllWorkerNodes(LOCKMODE lockMode);
static void BlockDistributedTransactions(void);
static void CreateRemoteRestorePoints(char *restoreName, List *connectionList);
static void BlockDistributedTransactionsOnAllMetadataNodes(List *connectionList);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_create_restore_point);


/*
 * citus_create_restore_point creates a cluster-consistent restore point
 * across all nodes in the Citus cluster.
 *
 * In coordinator-only mode, this function blocks new distributed writes
 * at the coordinator and creates restore points on all worker nodes.
 *
 * In MX mode (multi-writer), this function blocks both DML and DDL
 * operations on all metadata nodes by acquiring ExclusiveLock on:
 *   - pg_dist_transaction: blocks 2PC commit decisions (DML)
 *   - pg_dist_partition: blocks DDL on distributed tables
 *
 * This prevents new distributed transactions from recording commit decisions
 * and blocks schema changes, ensuring all restore points represent the same
 * consistent cluster state.
 *
 * The function returns the LSN of the restore point on the coordinator,
 * maintaining backward compatibility with the original implementation.
 *
 * Key insight: We do NOT need to drain in-flight transactions. The commit
 * decision in Citus 2PC happens when LogTransactionRecord() writes to
 * pg_dist_transaction, which occurs BEFORE the writer's local commit.
 * By blocking writes to pg_dist_transaction, we prevent commit decisions
 * from being made. Transactions that have already recorded their commit
 * decision will complete normally, while those that haven't will
 * be blocked. This creates a clean cut point for consistency.
 */
Datum
citus_create_restore_point(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();
	EnsureCoordinator();

	text *restoreNameText = PG_GETARG_TEXT_P(0);

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

	char *restoreNameString = text_to_cstring(restoreNameText);
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
	List *connectionList = OpenConnectionsToAllWorkerNodes(ShareLock);
	XLogRecPtr localRestorePoint = InvalidXLogRecPtr;

	PG_TRY();
	{
		/*
		 * Send a BEGIN to bust through pgbouncer. We won't actually commit since
		 * that takes time. Instead we just close the connections and roll back,
		 * which doesn't undo pg_create_restore_point.
		 */
		RemoteTransactionListBegin(connectionList);

		/* DANGER: finish as quickly as possible after this */
		BlockDistributedTransactions();

		BlockDistributedTransactionsOnAllMetadataNodes(connectionList);

		/* do local restore point first to bail out early if something goes wrong */
		localRestorePoint = XLogRestorePoint(restoreNameString);

		/* run pg_create_restore_point on all nodes */
		CreateRemoteRestorePoints(restoreNameString, connectionList);

		/* close connections to all nodes and
		 * all locks gets released as part of the transaction rollback
		 */
		MultiConnection *conn = NULL;
		foreach_declared_ptr(conn, connectionList)
		{
			ForgetResults(conn);
			CloseConnection(conn);
		}
		connectionList = NIL;
	}
	PG_CATCH();
	{
		/*
		 * On error, ensure we clean up connections and release locks.
		 * Rolling back the metadata node transactions releases the
		 * ExclusiveLocks on pg_dist_transaction cluster-wide.
		 */
		MultiConnection *conn = NULL;
		foreach_declared_ptr(conn, connectionList)
		{
			ForgetResults(conn);
			CloseConnection(conn);
		}
		connectionList = NIL;
		PG_RE_THROW();
	}
	PG_END_TRY();

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
	int connectionFlags = FORCE_NEW_CONNECTION;

	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(lockMode);

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		MultiConnection *connection = StartNodeConnection(connectionFlags,
														  workerNode->workerName,
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
 * BlockDistributedTransactionsOnAllMetadataNodes blocks distributed transactions
 * on all metadata nodes by executing pg_lock_table remotely.
 *
 * This is the MX-mode equivalent of BlockDistributedTransactions(), extended
 * to all nodes capable of initiating distributed transactions. We must hold
 * these locks across the cluster to prevent commit decisions from being made
 * on any node.
 *
 * The function expects that connections are already in a transaction block
 * (BEGIN has been sent). The locks will be held until the transaction is
 * rolled back or committed.
 */
static void
BlockDistributedTransactionsOnAllMetadataNodes(List *connectionList)
{
	/*
	 * Send LOCK TABLE commands to all metadata nodes in parallel. We use
	 * standard SQL LOCK TABLE syntax to acquire ExclusiveLock on catalog
	 * tables, mirroring what BlockDistributedTransactions() does on the
	 * coordinator via LockRelationOid().
	 *
	 * The BLOCK_DISTRIBUTED_WRITES_COMMAND acquires:
	 * 1. ExclusiveLock on pg_dist_transaction (blocks 2PC commit decisions)
	 * 2. ExclusiveLock on pg_dist_partition (blocks DDL on distributed tables)
	 *
	 * Note: Unlike the local coordinator lock which also locks pg_dist_node,
	 * we don't lock pg_dist_node on remote nodes because node management
	 * operations (adding/removing nodes) are still coordinator-only.
	 *
	 * These locks naturally serialize concurrent restore point operations
	 * cluster-wide, so no additional advisory lock is needed.
	 */

	/* Build list of remote metadata node connections */
	List *metadataConnectionList = NIL;
	MultiConnection *connection = NULL;
	foreach_declared_ptr(connection, connectionList)
	{
		WorkerNode *workerNode = FindWorkerNode(connection->hostname, connection->port);
		bool isRemoteMetadataNode = workerNode != NULL &&
									NodeIsPrimaryAndRemote(workerNode);

		if (isRemoteMetadataNode)
		{
			metadataConnectionList = lappend(metadataConnectionList, connection);
		}
	}

	/* Send lock commands in parallel to all remote metadata nodes */
	foreach_declared_ptr(connection, metadataConnectionList)
	{
		/*
		 * We could use ExecuteCriticalRemoteCommand instead, but it would
		 * not allow us to execute the commands in parallel. So for sake of
		 * performance, we use SendRemoteCommand and send lock commands in parallel
		 * to all metadata nodes, and later wait for all lock acquisitions to complete.
		 */
		int querySent = SendRemoteCommand(connection, BLOCK_DISTRIBUTED_WRITES_COMMAND);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/*
	 * Wait for all lock acquisitions to complete. If any node fails to
	 * acquire locks (e.g., due to a conflicting lock), this will error out.
	 */
	foreach_declared_ptr(connection, metadataConnectionList)
	{
		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);
		ForgetResults(connection);
	}
}


/*
 * CreateRemoteRestorePoints creates a restore point via each of the
 * connections in the list in parallel.
 */
static void
CreateRemoteRestorePoints(char *restoreName, List *connectionList)
{
	int parameterCount = 1;
	Oid parameterTypes[1] = { TEXTOID };
	const char *parameterValues[1] = { restoreName };

	MultiConnection *connection = NULL;
	foreach_declared_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommandParams(connection, CREATE_RESTORE_POINT_COMMAND,
												parameterCount, parameterTypes,
												parameterValues, false);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	foreach_declared_ptr(connection, connectionList)
	{
		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);

		ForgetResults(connection);
	}
}
