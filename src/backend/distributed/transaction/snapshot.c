/*-------------------------------------------------------------------------
 *
 * snapshot.c
 *   Basic distributed snapshot isolation implementation
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction/snapshot.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "storage/lockdefs.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


/* locking in share mode conflicts with 2PC commit, but not with other snapshots */
#define BLOCK_2PC_COMMAND "LOCK pg_catalog.pg_dist_transaction IN SHARE MODE"
#define EXPORT_SNAPSHOT_COMMAND "SELECT pg_catalog.pg_export_snapshot()"

/*
 * ExportedSnapshotEntry represents an entry in the ExportSnapshotsHash hash.
 */
typedef struct ExportedSnapshotEntry
{
	/*
	 * We reuse the full connection key instead of node ID primarily for
	 * ease-of-integration with connection and transaction management APIs.
	 */
	ConnectionHashKey key;

	/* name of the exported snapshot (file) */
	char snapshotName[MAXPGPATH];
} ExportedSnapshotEntry;


PG_FUNCTION_INFO_V1(citus_use_snapshot);

static void UseDistributedSnapshot(void);
static HTAB * CreateExportSnapshotsHash(void);


/*
 * ExportSnapshotsHash contains the node ID to snapshot name mapping
 * for the current transaction.
 */
static HTAB *ExportSnapshotsHash = NULL;

/*
 * citus_use_snapshot creates a distributed snapshot and uses it for the
 * remainder of the transaction.
 */
Datum
citus_use_snapshot(PG_FUNCTION_ARGS)
{
	UseDistributedSnapshot();
	PG_RETURN_VOID();
}


/*
 * UseDistributedSnapshot briefly locks 2PCs across the cluster, thenn exports
 * snapshots on all nodes, and releases the locks. The exports snapshots are
 * subsequently used in all worker node transactions for the remainder of the
 * transaction.
 *
 * The net effect is that ongoing 2PCs are either visible in all snapshots
 * or not yet visible.
 */
static void
UseDistributedSnapshot(void)
{
	if (ExportSnapshotsHash != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("citus_use_snapshot can only be called once per "
							   "transaction")));
	}

	if (!IsolationUsesXactSnapshot())
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("citus_use_snapshot can only be used in a transaction "
							   "with isolation level SERIALIZABLE or REPEATABLE READ")));
	}

	if (GetTopTransactionIdIfAny() != InvalidTransactionId || IsSubTransaction() ||
		!dlist_is_empty(&InProgressTransactions))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("citus_use_snapshot must be called before any query")));
	}

	HTAB *snapshotsHash = CreateExportSnapshotsHash();

	/* we definitely want a distributed transaction when asking for a snapshot */
	UseCoordinatedTransaction();

	/*
	 * Prepare a set of regular connections for exporting the snapshot. These can
	 * be reused for other purposes by the current user.
	 */
	int connectionFlags = 0;
	List *snapshotConnections = GetConnectionsToTargetNodeSet(connectionFlags,
															  OTHER_PRIMARY_NODES,
															  NoLock,
															  CurrentUserName());

	/*
	 * Prepare a set of new connections to lock pg_dist_transaction as superuser.
	 *
	 * We open connections to other nodes using superuser to have lock privileges
	 * on pg_dist_transaction. We immediately close these connections to release
	 * the lock and free up the resources.
	 *
	 * We do not take specific locks on pg_dist_node to avoid deadlock opportunities.
	 * If a new node gets added concurrently, it seems unlikely that shards will arrive
	 * there before we get our snapshot.
	 */
	List *blockingConnections = GetConnectionsToTargetNodeSet(FORCE_NEW_CONNECTION,
															  OTHER_PRIMARY_NODES,
															  NoLock,
															  CitusExtensionOwnerName());


	/* lock pg_dist_transaction locally to prevent concurrent 2PC commits */
	LockRelationOid(DistTransactionRelationId(), ShareLock);

	/* lock pg_dist_transaction on all other nodes to prevent concurrent 2PC commits */
	RemoteTransactionListBegin(blockingConnections);
	ExecuteRemoteCommandInConnectionList(blockingConnections, BLOCK_2PC_COMMAND);

	/* export a snapshot on the current node */
	char *localSnapshotName = ExportSnapshot(GetActiveSnapshot());

	/* create the hash key for the local node */
	WorkerNode *localNode = LookupNodeByNodeIdOrError(GetLocalNodeId());

	ConnectionHashKey localNodeKey;
	memset_struct_0(localNodeKey);

	strlcpy(localNodeKey.hostname, localNode->workerName, MAX_NODE_LENGTH);
	localNodeKey.port = localNode->workerPort;
	strlcpy(localNodeKey.user, CurrentUserName(), NAMEDATALEN);
	strlcpy(localNodeKey.database, CurrentDatabaseName(), NAMEDATALEN);
	localNodeKey.replicationConnParam = false;

	/* add the snapshot name of the current node to the hash */
	bool isFound = false;
	ExportedSnapshotEntry *localSnapshotEntry =
		hash_search(snapshotsHash, &localNodeKey, HASH_ENTER, &isFound);
	strlcpy(localSnapshotEntry->snapshotName, localSnapshotName, MAXPGPATH);

	/*
	 * Now export a snapshot on other nodes. The current isolation level
	 * is at least REPATABLE READ (enforced above), which will be propagated
	 * to other nodes as part of the BEGIN.
	 */
	RemoteTransactionListBegin(snapshotConnections);
	SendRemoteCommandToConnectionList(snapshotConnections, EXPORT_SNAPSHOT_COMMAND);

	MultiConnection *connection = NULL;
	foreach_ptr(connection, snapshotConnections)
	{
		/* create the hash key for the remote node */
		ConnectionHashKey remoteNodeKey;
		memset_struct_0(remoteNodeKey);

		strlcpy(remoteNodeKey.hostname, connection->hostname, MAX_NODE_LENGTH);
		remoteNodeKey.port = connection->port;
		strlcpy(remoteNodeKey.user, connection->user, NAMEDATALEN);
		strlcpy(remoteNodeKey.database, connection->database, NAMEDATALEN);
		remoteNodeKey.replicationConnParam = false;

		/*
		 * Create the hash entry before GetRemoteCommandResult to avoid allocations
		 * (OOM possibilities) before PQclear.
		 */
		ExportedSnapshotEntry *remoteSnapshotEntry =
			hash_search(snapshotsHash, &remoteNodeKey, HASH_ENTER, &isFound);
		Assert(!isFound);

		bool raiseErrors = true;
		PGresult *queryResult = GetRemoteCommandResult(connection, raiseErrors);
		if (!IsResponseOK(queryResult))
		{
			ReportResultError(connection, queryResult, ERROR);
		}

		if (PQntuples(queryResult) != 1 || PQnfields(queryResult) != 1)
		{
			PQclear(queryResult);
			ereport(ERROR, (errmsg("unexpected result for: %s",
								   EXPORT_SNAPSHOT_COMMAND)));
		}

		char *exportedSnapshotName = PQgetvalue(queryResult, 0, 0);

		/* first copy into stack (avoid allocations / OOM risk) */
		strlcpy(remoteSnapshotEntry->snapshotName, exportedSnapshotName, MAXPGPATH);

		PQclear(queryResult);
		ClearResults(connection, raiseErrors);
	}

	/* unblock 2PCs by closing the connections holding locks */
	CloseConnectionList(blockingConnections);

	/* start using the snapshot */
	ExportSnapshotsHash = snapshotsHash;
}


/*
 * CreateExportSnapshotsHash creates the node ID to exported snapshot name
 * mapping.
 */
static HTAB *
CreateExportSnapshotsHash(void)
{
	HASHCTL info;
	memset_struct_0(info);

	info.keysize = sizeof(ConnectionHashKey);
	info.entrysize = sizeof(ExportedSnapshotEntry);

	/*
	 * We may use the snapshot throughout the lifetime of the transaction,
	 * hence we use TopTransactionContext.
	 *
	 * (CreateSimpleHash et al. use CurrentMemoryContext)
	 */
	info.hcxt = TopTransactionContext;

	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	HTAB *exportedSnapshotsHash = hash_create("exported snapshot names", 32, &info,
											  hashFlags);

	return exportedSnapshotsHash;
}


/*
 * GetSnapshotNameForNode returns the snapshot name for the given node or NULL
 * if no snapshot was exported.
 */
char *
GetSnapshotNameForNode(char *hostname, int port, char *userName, char *databaseName)
{
	if (ExportSnapshotsHash == NULL)
	{
		return NULL;
	}

	ConnectionHashKey nodeKey;
	memset_struct_0(nodeKey);
	strlcpy(nodeKey.hostname, hostname, MAX_NODE_LENGTH);
	nodeKey.port = port;
	strlcpy(nodeKey.user, userName, NAMEDATALEN);
	strlcpy(nodeKey.database, databaseName, NAMEDATALEN);
	nodeKey.replicationConnParam = false;

	bool isFound = false;
	ExportedSnapshotEntry *remoteSnapshotEntry =
		hash_search(ExportSnapshotsHash, &nodeKey, HASH_FIND, &isFound);
	if (!isFound)
	{
		return NULL;
	}

	/* safe to return since it is allocated until the end of the transaction */
	return remoteSnapshotEntry->snapshotName;
}


/*
 * ResetExportedSnapshots resets the hash of exported snapshots.
 *
 * We do not bother freeing memory, since it is allocated in
 * TopTransactionContext and will get reset at the end of the
 * transaction.
 */
void
ResetExportedSnapshots(void)
{
	ExportSnapshotsHash = NULL;
}
