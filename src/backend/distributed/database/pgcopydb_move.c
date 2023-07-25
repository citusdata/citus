#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"
#include "safe_mem_lib.h"
#include "pg_version_compat.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlogdefs.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_subscription.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/subscriptioncmds.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/database/database_sharding.h"
#include "distributed/database/pgcopydb.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/pooler/pgbouncer_manager.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#if PG_VERSION_NUM >= PG_VERSION_15
#include "utils/wait_event.h"
#endif


/*
 * DatabaseMoveState represents the current state of a migration.
 */
typedef enum DatabaseMoveState
{
	DATABASE_MIGRATION_INITIAL,
	DATABASE_MIGRATION_WAIT_FOR_DATA,
	DATABASE_MIGRATION_WAIT_FOR_CATCHUP,
	DATABASE_MIGRATION_FINISHED,
	DATABASE_MIGRATION_FAILED,
	DATABASE_MIGRATION_FINISHED_FAILED
} DatabaseMoveState;

/*
 * DatabaseMoveMode represents different ways of doing a database
 * migration:
 * - schema only (pg_dump without data)
 * - dump only (pg_dump with data)
 * - full (pg_dump for schema, logical replication for data & changes)
 */
typedef enum DatabaseMoveMode
{
	MIGRATION_MODE_SCHEMA_ONLY,
	MIGRATION_MODE_DUMP_ONLY,
	MIGRATION_MODE_LOGICAL_REPLICATION
} DatabaseMoveMode;

typedef struct DatabaseMove
{
	/* name of the database to migrate */
	char *databaseName;

	/* node group ID of the target node */
	int targetNodeGroupId;

	/* type of migration we want to do */
	DatabaseMoveMode mode;

	/* connection info for the source node */
	char *sourceConnectionInfo;

	/* connection info for the target node */
	char *targetConnectionInfo;

	/* current state of the migration */
	DatabaseMoveState state;
} DatabaseMove;


void _PG_init(void);

static void MoveDatabase(DatabaseMove *moveDesc);
static void ExecuteDatabaseCommand(char *command, MultiConnection *conn, bool isLocal);


PG_FUNCTION_INFO_V1(pgcopydb_database_move);
PG_FUNCTION_INFO_V1(pgcopydb_clone);


/*
 * pgcopydb_database_move moves a database shard from one node
 * to another.
 */
Datum
pgcopydb_database_move(PG_FUNCTION_ARGS)
{
	text *databaseNameText = PG_GETARG_TEXT_P(0);
	char *databaseName = text_to_cstring(databaseNameText);
	int targetNodeGroupId = PG_GETARG_INT32(1);

	DatabaseMove moveDesc = {
		.databaseName = databaseName,
		.targetNodeGroupId = targetNodeGroupId,
		.mode = MIGRATION_MODE_LOGICAL_REPLICATION
	};

	MoveDatabase(&moveDesc);

	PG_RETURN_VOID();
}


/*
 * pgcopydb_clone does a pgcopydb clone via a UDF.
 */
Datum
pgcopydb_clone(PG_FUNCTION_ARGS)
{
	text *sourceText = PG_GETARG_TEXT_P(0);
	char *sourceURL = text_to_cstring(sourceText);
	text *targetText = PG_GETARG_TEXT_P(1);
	char *targetURL = text_to_cstring(targetText);
	text *migrationNameText = PG_GETARG_TEXT_P(2);
	char *migrationName = text_to_cstring(migrationNameText);

	char *output = RunPgcopydbClone(sourceURL, targetURL, migrationName);

	elog(NOTICE, "%s", output);

	PG_RETURN_VOID();
}


/*
 * MoveDatabase orchestrates the end-to-end move of a database using pgcopydb.
 */
static void
MoveDatabase(DatabaseMove *moveDesc)
{
	/* verify that pgcopydb is on PATH before doing any work */
	char *pgcopydbPath = GetPgcopydbPath();
	if (pgcopydbPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pgcopydb on PATH")));
	}

	char *databaseName = moveDesc->databaseName;
	int targetNodeGroupId = moveDesc->targetNodeGroupId;

	bool missingOk = false;
	Oid databaseOid = get_database_oid(databaseName, missingOk);
	DatabaseShard *dbShard = GetDatabaseShardByOid(databaseOid);
	if (dbShard == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("database %s is not a shard",
							   quote_identifier(databaseName))));
	}

	if (targetNodeGroupId == dbShard->nodeGroupId)
	{
		ereport(NOTICE, (errmsg("database is already on node group %d",
								targetNodeGroupId)));
		return;
	}

	RegisterOperationNeedingCleanup();
	UseCoordinatedTransaction();

	char *migrationName = psprintf("pgcopydb_" UINT64_FORMAT, CurrentOperationId);
	char *incomingDatabaseName = psprintf("_incoming_" UINT64_FORMAT, CurrentOperationId);
	char *replaceDatabaseName = psprintf("_replace_" UINT64_FORMAT, CurrentOperationId);
	char *oldDatabaseName = psprintf("_old_" UINT64_FORMAT, CurrentOperationId);

	WorkerNode *source = LookupNodeForGroup(dbShard->nodeGroupId);
	WorkerNode *target = LookupNodeForGroup(targetNodeGroupId);

	bool sourceIsLocal = source->groupId == GetLocalGroupId();
	bool targetIsLocal = target->groupId == GetLocalGroupId();

	/* admin connection for the source server */
	int connectionFlags = 0;
	MultiConnection *sourceConn = GetNodeUserDatabaseConnection(connectionFlags,
																source->workerName,
																source->workerPort,
																CurrentUserName(),
																CurrentDatabaseName());
	if (PQstatus(sourceConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(sourceConn, ERROR);
	}

	/* open a connection to run pgcopydb on the target node */
	connectionFlags = OUTSIDE_TRANSACTION;
	MultiConnection *targetConn = GetNodeUserDatabaseConnection(connectionFlags,
																target->workerName,
																target->workerPort,
																CurrentUserName(),
																CurrentDatabaseName());
	if (PQstatus(targetConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(targetConn, ERROR);
	}

	ForceConnectionCloseAtTransactionEnd(sourceConn);
	ForceConnectionCloseAtTransactionEnd(targetConn);

	/* create the database with the desired properties, but a temporary name */
	CreatedbStmt *createdbStmt = RecreateCreatedbStmt(databaseOid);

	/* we do not want to propagate the CREATE/ALTER/DROP DATABASE */
	char *setCommand = "SET citus.enable_ddl_propagation TO off";
	ExecuteCriticalRemoteCommand(sourceConn, setCommand);
	ExecuteCriticalRemoteCommand(targetConn, setCommand);

	setCommand = "SET citus.enable_create_database_propagation TO off";
	ExecuteCriticalRemoteCommand(sourceConn, setCommand);
	ExecuteCriticalRemoteCommand(targetConn, setCommand);

	/* create a replacement database on the source */
	InsertCleanupRecordInSubtransaction(CLEANUP_OBJECT_DATABASE,
										replaceDatabaseName,
										source->groupId,
										CLEANUP_ON_FAILURE);

	CreatedbStmt *createReplaceDbStmt = copyObject(createdbStmt);
	createReplaceDbStmt->dbname = replaceDatabaseName;
	char *createReplaceDatabaseCommand = DeparseTreeNode((Node *) createReplaceDbStmt);
	ExecuteCriticalRemoteCommand(sourceConn, createReplaceDatabaseCommand);

	/* create the temporary target database */
	InsertCleanupRecordInSubtransaction(CLEANUP_OBJECT_DATABASE,
										incomingDatabaseName,
										target->groupId,
										CLEANUP_ON_FAILURE);

	CreatedbStmt *createIncomingDbStmt = copyObject(createdbStmt);
	createIncomingDbStmt->dbname = incomingDatabaseName;
	char *createIncomingDatabaseCommand = DeparseTreeNode((Node *) createIncomingDbStmt);
	ExecuteCriticalRemoteCommand(targetConn, createIncomingDatabaseCommand);

	/* TODO: don't open a connection just to get the URL */
	connectionFlags = 0;
	MultiConnection *sourceURLConn = GetNodeUserDatabaseConnection(connectionFlags,
																   source->workerName,
																   source->workerPort,
																   CurrentUserName(),
																   databaseName);
	if (PQstatus(sourceURLConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(sourceURLConn, ERROR);
	}

	moveDesc->sourceConnectionInfo = GetConnectionString(sourceURLConn);
	CloseConnection(sourceURLConn);

	connectionFlags = 0;
	MultiConnection *targetURLConn = GetNodeUserDatabaseConnection(connectionFlags,
																   target->workerName,
																   target->workerPort,
																   CurrentUserName(),
																   incomingDatabaseName);
	if (PQstatus(targetURLConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(targetURLConn, ERROR);
	}

	moveDesc->targetConnectionInfo = GetConnectionString(targetURLConn);
	CloseConnection(targetURLConn);

	/* if something fails, clean the slot created by pgcopydb */
	InsertCleanupRecordInSubtransaction(CLEANUP_OBJECT_REPLICATION_SLOT,
										migrationName,
										source->groupId,
										CLEANUP_ALWAYS);

	/* TODO: cleanup record for replication origin */

	/* kick off the pgcopydb clone */
	StringInfo remoteCloneCommand = makeStringInfo();
	appendStringInfo(remoteCloneCommand,
					 "SELECT pg_catalog.pgcopydb_clone(%s,%s,%s)",
					 quote_literal_cstr(moveDesc->sourceConnectionInfo),
					 quote_literal_cstr(moveDesc->targetConnectionInfo),
					 quote_literal_cstr(migrationName));

	int querySent = SendRemoteCommand(targetConn, remoteCloneCommand->data);
	if (querySent == 0)
	{
		ReportConnectionError(targetConn, ERROR);
	}

	int targetConnSocket = PQsocket(targetConn->pgConn);

	/*
	 * we handle I/O for the pgcopydb_clone() query inline to also
	 * be able to do other things. This code is derived from
	 * FinishConnectionIO.
	 */
	while (true)
	{
		int waitFlags = WL_POSTMASTER_DEATH | WL_LATCH_SET;

		/* try to send all pending data */
		int sendStatus = PQflush(targetConn->pgConn);

		/* if sending failed, there's nothing more we can do */
		if (sendStatus == -1)
		{
			ereport(ERROR, (errmsg("failed to send command to worker node")));
		}
		else if (sendStatus == 1)
		{
			waitFlags |= WL_SOCKET_WRITEABLE;
		}

		if (PQconsumeInput(targetConn->pgConn) == 0)
		{
			ereport(ERROR, (errmsg("lost connection to node %s:%d",
								   target->workerName, target->workerPort)));
		}

		if (PQisBusy(targetConn->pgConn))
		{
			waitFlags |= WL_SOCKET_READABLE;
		}

		if ((waitFlags & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) == 0)
		{
			break;
		}

		long timeout = 1000; /* ms */
		int rc = WaitLatchOrSocket(MyLatch, waitFlags, targetConnSocket, timeout,
								   PG_WAIT_EXTENSION);
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		elog(NOTICE, "waiting for database move to finish");

		/* TODO: monitor the overall move */
	}

	if (PQstatus(targetConn->pgConn) == CONNECTION_BAD)
	{
		ereport(ERROR, (errmsg("lost connection to node %s:%d",
							   target->workerName, target->workerPort)));
	}

	PGresult *result = PQgetResult(targetConn->pgConn);
	if (!IsResponseOK(result))
	{
		ReportResultError(targetConn, result, ERROR);
	}

	PQclear(result);

	bool raiseErrors = true;
	ClearResults(targetConn, raiseErrors);

	CHECK_FOR_INTERRUPTS();

	/* TODO: unpause on failure */

	/* stop all database connections */
	PauseDatabaseOnInboundPgBouncers(databaseName);

	RemoteTransactionBegin(sourceConn);
	RemoteTransactionBegin(targetConn);

	/* rename the old database on the source */
	char *renameCommand = psprintf("ALTER DATABASE %s RENAME TO %s",
								   quote_identifier(databaseName),
								   quote_identifier(oldDatabaseName));
	ExecuteDatabaseCommand(renameCommand, sourceConn, sourceIsLocal);

	/* clean up the old database on success (data drop!) */
	InsertCleanupRecordInCurrentTransaction(CLEANUP_OBJECT_DATABASE,
											oldDatabaseName,
											source->groupId,
											CLEANUP_DEFERRED_ON_SUCCESS);

	/* replace with a newly created database */
	renameCommand = psprintf("ALTER DATABASE %s RENAME TO %s",
							 quote_identifier(replaceDatabaseName),
							 quote_identifier(databaseName));
	ExecuteDatabaseCommand(renameCommand, sourceConn, sourceIsLocal);

	/* rename the old (empty) database on the target */
	renameCommand = psprintf("ALTER DATABASE %s RENAME TO %s",
							 quote_identifier(databaseName),
							 quote_identifier(oldDatabaseName));
	ExecuteDatabaseCommand(renameCommand, targetConn, targetIsLocal);

	/* clean up the old database on success (drop empty) */
	InsertCleanupRecordInCurrentTransaction(CLEANUP_OBJECT_DATABASE,
											oldDatabaseName,
											target->groupId,
											CLEANUP_DEFERRED_ON_SUCCESS);

	/* rename the incoming database to be the new database */
	renameCommand = psprintf("ALTER DATABASE %s RENAME TO %s",
							 quote_identifier(incomingDatabaseName),
							 quote_identifier(databaseName));
	ExecuteDatabaseCommand(renameCommand, targetConn, targetIsLocal);

	/* database OID might have changed if the current node was involved */
	missingOk = false;
	databaseOid = get_database_oid(databaseName, missingOk);

	/* remark the database as distributed */
	ObjectAddress dbAddress = { 0 };
	ObjectAddressSet(dbAddress, DatabaseRelationId, databaseOid);
	MarkObjectDistributed(&dbAddress);

	/* do all operations on the database over a connection (even if local) */
	UpdateDatabaseShard(databaseOid, targetNodeGroupId);

	ResumeDatabaseOnInboundPgBouncers(databaseName);

	FinalizeOperationNeedingCleanupOnSuccess("database move");
}


/*
 * ExecuteDatabaseCommand executes a command on a database either over
 * a connection or locally via SPI.
 */
static void
ExecuteDatabaseCommand(char *command, MultiConnection *conn, bool isLocal)
{
	if (isLocal)
	{
		ExecuteQueryViaSPI(command, SPI_OK_UTILITY);
	}
	else
	{
		ExecuteCriticalRemoteCommand(conn, command);
	}
}
