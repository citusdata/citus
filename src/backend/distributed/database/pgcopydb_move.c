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
#include "distributed/jsonbutils.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/pooler/pgbouncer_manager.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "libpq/libpq.h"
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
#include "utils/jsonb.h"
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
 * - source is in read-only
 * - use logical replication during the move
 */
typedef enum DatabaseMoveMode
{
	MIGRATION_MODE_READ_ONLY,
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
static void DoDatabaseClone(DatabaseMove *moveDesc,
							MultiConnection *sourceDbConn,
							MultiConnection *targetCloneConn,
							MultiConnection *targetProgressConn,
							char *migrationName);
static void LockDatabaseConnections(char *databaseName, MultiConnection *connection,
									bool sourceIsLocal);
static void ExecuteDatabaseCommand(char *command, MultiConnection *conn, bool isLocal);
static bool CheckDataCopyProgress(MultiConnection *targetProgressConn,
								  char *sourceConnInfo, char *migrationName);
static bool GetRemoteJsonb(MultiConnection *connection, char *command, Datum *jsonDatum);
static void SetEndPosOnSourceDatabase(MultiConnection *targetConn,
									  char *sourceConnInfo, char *migrationName);


PG_FUNCTION_INFO_V1(pgcopydb_database_move);
PG_FUNCTION_INFO_V1(pgcopydb_clone);
PG_FUNCTION_INFO_V1(pgcopydb_list_progress);
PG_FUNCTION_INFO_V1(pgcopydb_end_follow);


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
	bool useFollow = PG_GETARG_BOOL(2);
	text *migrationNameText = PG_GETARG_TEXT_P(3);
	char *migrationName = text_to_cstring(migrationNameText);

	char *output = RunPgcopydbClone(sourceURL, targetURL, migrationName,
									useFollow);

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
	int connectionFlags = REQUIRE_METADATA_CONNECTION;
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
	connectionFlags = REQUIRE_METADATA_CONNECTION;
	MultiConnection *targetConn = GetNodeUserDatabaseConnection(connectionFlags,
																target->workerName,
																target->workerPort,
																CurrentUserName(),
																CurrentDatabaseName());
	if (PQstatus(targetConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(targetConn, ERROR);
	}

	/* open a connection to check progress on the target node */
	connectionFlags = FORCE_NEW_CONNECTION;
	MultiConnection *targetProgressConn =
		GetNodeUserDatabaseConnection(connectionFlags,
									  target->workerName,
									  target->workerPort,
									  CurrentUserName(),
									  CurrentDatabaseName());
	if (PQstatus(targetProgressConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(targetProgressConn, ERROR);
	}

	/* avoid reusing this connection for other operations */
	ClaimConnectionExclusively(targetProgressConn);

	/* make sure we do not leave behind session state */
	ForceConnectionCloseAtTransactionEnd(sourceConn);
	ForceConnectionCloseAtTransactionEnd(targetConn);
	ForceConnectionCloseAtTransactionEnd(targetProgressConn);

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
	MultiConnection *sourceDbConn = GetNodeUserDatabaseConnection(connectionFlags,
																  source->workerName,
																  source->workerPort,
																  CurrentUserName(),
																  databaseName);
	if (PQstatus(sourceDbConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(sourceDbConn, ERROR);
	}

	moveDesc->sourceConnectionInfo = GetConnectionString(sourceDbConn);

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

	/* if something fails, clean the replication origin created by pgcopydb */
	InsertCleanupRecordInSubtransaction(CLEANUP_OBJECT_REPLICATION_ORIGIN,
										migrationName,
										target->groupId,
										CLEANUP_ALWAYS);

	DoDatabaseClone(moveDesc, sourceDbConn, targetConn, targetProgressConn,
					migrationName);

	/*
	 * In read-only mode, we do not pause during the pgcopydb, so do it
	 * now.
	 */
	if (moveDesc->mode == MIGRATION_MODE_READ_ONLY)
	{
		/* TODO: unpause on failure! */
		PauseDatabaseOnInboundPgBouncers(databaseName);
	}

	/* close our database connection, since it would prevent rename */
	CloseConnection(sourceDbConn);

	RemoteTransactionBegin(sourceConn);
	RemoteTransactionBegin(targetConn);

	/* make sure we disallow new connections */
	LockDatabaseConnections(databaseName, sourceConn, sourceIsLocal);

	/*
	 * We (idempotently) pause again, mainly to make sure that the pgbouncer
	 * did not crash in between the first pause and us locking the database.
	 * In that case pausing again would fail.
	 */
	PauseDatabaseOnInboundPgBouncers(databaseName);

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

	/* TODO: resume after commit */
	ResumeDatabaseOnInboundPgBouncers(databaseName);

	FinalizeOperationNeedingCleanupOnSuccess("database move");

	UnclaimConnection(targetProgressConn);
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


/*
 * DoDatabaseClone performs a pgcopydb clone between two remote databases.
 */
static void
DoDatabaseClone(DatabaseMove *moveDesc,
				MultiConnection *sourceDbConn,
				MultiConnection *targetCloneConn,
				MultiConnection *targetProgressConn,
				char *migrationName)
{
	bool useFollow = moveDesc->mode == MIGRATION_MODE_LOGICAL_REPLICATION;

	/* kick off the pgcopydb clone */
	StringInfo remoteCloneCommand = makeStringInfo();
	appendStringInfo(remoteCloneCommand,
					 "SELECT pg_catalog.pgcopydb_clone(%s,%s,%s,%s)",
					 quote_literal_cstr(moveDesc->sourceConnectionInfo),
					 quote_literal_cstr(moveDesc->targetConnectionInfo),
					 useFollow ? "true" : "false",
					 quote_literal_cstr(migrationName));

	int querySent = SendRemoteCommand(targetCloneConn, remoteCloneCommand->data);
	if (querySent == 0)
	{
		ReportConnectionError(targetCloneConn, ERROR);
	}

	TimestampTz lastProgressCheckTime = GetCurrentTimestamp();

	int targetConnSocket = PQsocket(targetCloneConn->pgConn);
	bool isDataDone = false;

	/*
	 * we handle I/O for the pgcopydb_clone() query inline to also
	 * be able to do other things. This code is derived from
	 * FinishConnectionIO.
	 */
	while (true)
	{
		CHECK_FOR_INTERRUPTS();

		int waitFlags = WL_POSTMASTER_DEATH | WL_LATCH_SET;

		/* try to send all pending data */
		int sendStatus = PQflush(targetCloneConn->pgConn);

		/* if sending failed, there's nothing more we can do */
		if (sendStatus == -1)
		{
			ereport(ERROR, (errmsg("failed to send command to worker node")));
		}
		else if (sendStatus == 1)
		{
			waitFlags |= WL_SOCKET_WRITEABLE;
		}

		if (PQconsumeInput(targetCloneConn->pgConn) == 0)
		{
			ereport(ERROR, (errmsg("lost connection to node %s:%d",
								   targetCloneConn->hostname, targetCloneConn->port)));
		}

		if (PQisBusy(targetCloneConn->pgConn))
		{
			waitFlags |= WL_SOCKET_READABLE;
		}

		if ((waitFlags & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) == 0)
		{
			break;
		}

		waitFlags |= WL_TIMEOUT;
		long timeout = 2000; /* ms */

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

		TimestampTz currentTime = GetCurrentTimestamp();

		if (!isDataDone &&
			TimestampDifferenceExceeds(lastProgressCheckTime, currentTime, 2000))
		{
			/* TODO: reset memory context */
			isDataDone = CheckDataCopyProgress(targetProgressConn,
											   moveDesc->sourceConnectionInfo,
											   migrationName);
			if (isDataDone && useFollow)
			{
				/*
				 * Data copy is completed, now pause traffic.
				 *
				 * TODO: unpause on failure!
				 * TODO: wait for catch up
				 */
				PauseDatabaseOnInboundPgBouncers(moveDesc->databaseName);

				/*
				 * Set the endpos to the current WAL LSN, once pgcopydb
				 * reaches this LSN we will exit this loop.
				 */
				SetEndPosOnSourceDatabase(targetProgressConn,
										  moveDesc->sourceConnectionInfo,
										  migrationName);
			}

			lastProgressCheckTime = currentTime;
		}
	}

	if (PQstatus(targetCloneConn->pgConn) == CONNECTION_BAD)
	{
		ereport(ERROR, (errmsg("lost connection to node %s:%d",
							   targetCloneConn->hostname, targetCloneConn->port)));
	}

	PGresult *result = PQgetResult(targetCloneConn->pgConn);
	if (!IsResponseOK(result))
	{
		ReportResultError(targetCloneConn, result, ERROR);
	}

	PQclear(result);

	bool raiseErrors = true;
	ClearResults(targetCloneConn, raiseErrors);
}


/*
 * LockDatabaseConnections locks the given database.
 */
static void
LockDatabaseConnections(char *databaseName, MultiConnection *connection,
						bool sourceIsLocal)
{
	char *lockCommand =
		psprintf("SELECT pg_catalog.citus_database_lock(%s)",
				 quote_literal_cstr(databaseName));

	if (sourceIsLocal)
	{
		ExecuteQueryViaSPI(lockCommand, SPI_OK_SELECT);
	}
	else
	{
		ExecuteCriticalRemoteCommand(connection, lockCommand);
	}

}


/*
 * pgcopydb_list_progress does a pgcopydb list progress via a UDF.
 */
Datum
pgcopydb_list_progress(PG_FUNCTION_ARGS)
{
	text *sourceText = PG_GETARG_TEXT_P(0);
	char *sourceURL = text_to_cstring(sourceText);
	text *migrationNameText = PG_GETARG_TEXT_P(1);
	char *migrationName = text_to_cstring(migrationNameText);

	char *output = RunPgcopydbListProgress(sourceURL, migrationName);
	if (output == NULL)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(cstring_to_text(output));
}


/*
 * CheckDataCopyProgress checks the progress of an ongoing pgcopydb clone
 * over the given connection.
 */
static bool
CheckDataCopyProgress(MultiConnection *targetProgressConn,
					  char *sourceConnInfo, char *migrationName)
{
	bool isDataDone = false;

	StringInfo remoteProgressCommand = makeStringInfo();
	appendStringInfo(remoteProgressCommand,
					 "SELECT pg_catalog.pgcopydb_list_progress(%s,%s)",
					 quote_literal_cstr(sourceConnInfo),
					 quote_literal_cstr(migrationName));

	Datum progressJson = 0;
	if (GetRemoteJsonb(targetProgressConn, remoteProgressCommand->data,
					   &progressJson))
	{
		Datum tablesJson = 0;
		Datum indexesJson = 0;

		if (ExtractFieldJsonbDatum(progressJson, "tables", &tablesJson) &&
			ExtractFieldJsonbDatum(progressJson, "indexes", &indexesJson))
		{
			int tablesTotal = ExtractFieldInt32(tablesJson, "total", -1);
			int tablesDone = ExtractFieldInt32(tablesJson, "done", -1);
			int indexesTotal = ExtractFieldInt32(indexesJson, "total", -1);
			int indexesDone = ExtractFieldInt32(indexesJson, "done", -1);

			if (tablesDone == tablesTotal && indexesDone == indexesTotal)
			{
				ereport(NOTICE, (errmsg("data copy and index rebuild completed")));
				isDataDone = true;
			}
			else
			{
				ereport(NOTICE, (errmsg("%d table%s and %d index%s remaining",
										tablesTotal - tablesDone,
										tablesTotal - tablesDone != 1 ? "s":"",
										indexesTotal - indexesDone,
										indexesTotal - indexesDone != 1 ? "es":"")));
			}
		}
	}

	return isDataDone;
}


/*
 * GetRemoteJsonb executes a command that returns a single JSON over the given connection.
 */
static bool
GetRemoteJsonb(MultiConnection *connection, char *command, Datum *jsonDatum)
{
	bool raiseInterrupts = true;

	int querySent = SendRemoteCommand(connection, command);
	if (querySent == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	int rowCount = PQntuples(result);
	if (rowCount != 1)
	{
		PQclear(result);
		ForgetResults(connection);
		return InvalidXLogRecPtr;
	}

	int colCount = PQnfields(result);
	if (colCount != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of columns returned by: %s",
							   command)));
	}

	bool isNull = PQgetisnull(result, 0, 0);
	if (!isNull)
	{
		char *resultString = PQgetvalue(result, 0, 0);
		*jsonDatum = DirectFunctionCall1Coll(jsonb_in, InvalidOid,
											 CStringGetDatum(resultString));
	}

	PQclear(result);
	ClearResults(connection, raiseInterrupts);

	return !isNull;
}


/*
 * pgcopydb_end_follow does a pgcopydb stream sentinel set endpos via a UDF
 * to finalize a pgcopydb clone --follow.
 */
Datum
pgcopydb_end_follow(PG_FUNCTION_ARGS)
{
	text *sourceText = PG_GETARG_TEXT_P(0);
	char *sourceURL = text_to_cstring(sourceText);
	text *migrationNameText = PG_GETARG_TEXT_P(1);
	char *migrationName = text_to_cstring(migrationNameText);

	char *output = RunPgcopydbStreamSentinelSetEndpos(sourceURL, migrationName);
	if (output == NULL)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(cstring_to_text(output));
}



/*
 * SetEndPosOnSourceDatabase updates the endpos in the pgcopydb.sentinel
 * table to finalize a pgcopydb clone --follow.
 */
static void
SetEndPosOnSourceDatabase(MultiConnection *targetConn,
						  char *sourceConnInfo, char *migrationName)
{
	StringInfo setEndposCommand = makeStringInfo();

	appendStringInfo(setEndposCommand,
					 "SELECT pg_catalog.pgcopydb_end_follow(%s, %s)",
					 quote_literal_cstr(sourceConnInfo),
					 quote_literal_cstr(migrationName));

	ExecuteCriticalRemoteCommand(targetConn, setEndposCommand->data);
}
