#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"
#include "safe_mem_lib.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlogdefs.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_subscription.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/subscriptioncmds.h"
#include "commands/trigger.h"
#include "lib/stringinfo.h"
#include "distributed/connection_management.h"
#include "distributed/database/database_sharding.h"
#include "distributed/database/ddl_replication.h"
#include "distributed/database/migrator.h"
#include "distributed/database/pg_dump.h"
#include "distributed/database/remote_publication.h"
#include "distributed/database/remote_subscription.h"
#include "distributed/database/source_database_info.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
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
#include "utils/wait_event.h"


#define MAX_MIGRATION_NAME_LENGTH 50

/*
 * DatabaseMigrationState represents the current state of a migration.
 */
typedef enum DatabaseMigrationState
{
	DATABASE_MIGRATION_INITIAL,
	DATABASE_MIGRATION_WAIT_FOR_DATA,
	DATABASE_MIGRATION_WAIT_FOR_CATCHUP,
	DATABASE_MIGRATION_FINISHED,
	DATABASE_MIGRATION_FAILED,
	DATABASE_MIGRATION_FINISHED_FAILED
} DatabaseMigrationState;

/*
 * DatabaseMigrationMode represents different ways of doing a database
 * migration:
 * - schema only (pg_dump without data)
 * - dump only (pg_dump with data)
 * - full (pg_dump for schema, logical replication for data & changes)
 */
typedef enum DatabaseMigrationMode
{
	MIGRATION_MODE_SCHEMA_ONLY,
	MIGRATION_MODE_DUMP_ONLY,
	MIGRATION_MODE_LOGICAL_REPLICATION
} DatabaseMigrationMode;

typedef struct DatabaseMigration
{
	/* name of the migration */
	char *migrationName;

	/* name of the database to migrate */
	char *databaseName;

	/* node group ID of the target node */
	int targetNodeGroupId;

	/* type of migration we want to do */
	DatabaseMigrationMode mode;

	/* connection info for the source node */
	char *sourceConnectionInfo;

	/* whether a publication was created on the source */
	bool createdPublication;

	/* whether a replication slot was created on the source */
	bool createdReplicationSlot;

	/* whether a subscription was created on the target */
	bool createdSubscription;

	/* current state of the migration */
	DatabaseMigrationState state;
} DatabaseMigration;


void _PG_init(void);

static bool DatabaseMigrationStart(DatabaseMigration *migration);
static char * ReplicationSlotNameForMigration(char *migrationName);
static char * PublicationNameForMigration(char *migrationName);
static char * SubscriptionNameForMigration(char *migrationName);
static char * GetConnectionString(MultiConnection *connection);
static void MigrateSchema(char *sourceConnectionString,
						  MultiConnection *destConn,
						  char *snapshotName,
						  bool includeData,
						  bool dropIfExists);
static List * PreProcessPgDumpParseTrees(List *pgDumpParseTrees);
static bool IsPgCronPolicy(CreatePolicyStmt *statement);
static bool AllTablesHaveReplicaIdentity(List *tableList);
static bool DatabaseMigrationFinish(DatabaseMigration *migration);
static void WaitForCatchUp(MultiConnection *sourceConn, MultiConnection *destConn,
						   char *subscriptionName);
static void AdjustSequences(MultiConnection *conn, List *sequenceList);
static void SetSequenceValue(MultiConnection *conn, char *schemaName, char *sequenceName,
							 int64 value);


PG_FUNCTION_INFO_V1(database_shard_move);
PG_FUNCTION_INFO_V1(database_shard_move_start);
PG_FUNCTION_INFO_V1(database_shard_move_finish);


/*
 * database_shard_move does a full migration of a database.
 */
Datum
database_shard_move(PG_FUNCTION_ARGS)
{
	text *databaseNameText = PG_GETARG_TEXT_P(0);
	char *databaseName = text_to_cstring(databaseNameText);
	int targetNodeGroupId = PG_GETARG_INT32(1);

	DatabaseMigration migration = {
		.migrationName = "0",
		.databaseName = databaseName,
		.targetNodeGroupId = targetNodeGroupId,
		.mode = MIGRATION_MODE_LOGICAL_REPLICATION
	};

	if (DatabaseMigrationStart(&migration))
	{
		DatabaseMigrationFinish(&migration);
	}

	PG_RETURN_VOID();
}


/*
 * database_shard_move_start starts a migration from the remote database.
 */
Datum
database_shard_move_start(PG_FUNCTION_ARGS)
{
	/* use one global name for migrations for now */
	text *databaseNameText = PG_GETARG_TEXT_P(0);
	char *databaseName = text_to_cstring(databaseNameText);
	int targetNodeGroupId = PG_GETARG_INT32(1);

	DatabaseMigration migration = {
		.migrationName = "0",
		.databaseName = databaseName,
		.targetNodeGroupId = targetNodeGroupId,
		.mode = MIGRATION_MODE_LOGICAL_REPLICATION
	};

	DatabaseMigrationStart(&migration);
	PG_RETURN_VOID();
}


/*
 * DatabaseMigrationStart does the initial steps to initiate a shard
 * move.
 */
static bool
DatabaseMigrationStart(DatabaseMigration *migration)
{
	/* verify that pg_dump is on PATH before doing any work */
	char *pgDumpPath = GetPgDumpPath();
	if (pgDumpPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pg_dump on PATH")));
	}

	if (strlen(migration->migrationName) > MAX_MIGRATION_NAME_LENGTH)
	{
		ereport(ERROR, (errmsg("migration name cannot be longer than %d characters",
							   MAX_MIGRATION_NAME_LENGTH)));
	}

	char *migrationName = migration->migrationName;
	char *databaseName = migration->databaseName;
	int targetNodeGroupId = migration->targetNodeGroupId;

	char *slotName = ReplicationSlotNameForMigration(migrationName);
	char *publicationName = PublicationNameForMigration(migrationName);
	char *subscriptionName = SubscriptionNameForMigration(migrationName);

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
		return false;
	}

	WorkerNode *source = LookupNodeForGroup(dbShard->nodeGroupId);
	WorkerNode *destination = LookupNodeForGroup(targetNodeGroupId);

	/* open a regular connection to inspect schema and set up publication */
	int connectionFlags = 0;
	MultiConnection *sourceConn = GetNodeUserDatabaseConnection(connectionFlags,
																source->workerName,
																source->workerPort,
																CurrentUserName(),
																databaseName);
	if (PQstatus(sourceConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(sourceConn, ERROR);
	}

	/* open a replication connection to create a replication slot */
	MultiConnection *replicationConn = NULL;
	MultiConnection *destCitusConn = NULL;

	if (migration->mode == MIGRATION_MODE_LOGICAL_REPLICATION)
	{
		/* replication connection to the source database */
		connectionFlags = REQUIRE_REPLICATION_CONNECTION_PARAM;
		replicationConn = GetNodeUserDatabaseConnection(connectionFlags,
														source->workerName,
														source->workerPort,
														CurrentUserName(),
														databaseName);
		if (PQstatus(replicationConn->pgConn) != CONNECTION_OK)
		{
			ReportConnectionError(replicationConn, ERROR);
		}

		/* connection to Citus database on target node */
		connectionFlags = 0;
		destCitusConn = GetNodeUserDatabaseConnection(connectionFlags,
													  destination->workerName,
													  destination->workerPort,
													  CurrentUserName(),
													  NULL);
		if (PQstatus(replicationConn->pgConn) != CONNECTION_OK)
		{
			ReportConnectionError(replicationConn, ERROR);
		}
	}

	/* regular connection to the target database */
	connectionFlags = 0;
	MultiConnection *destConn = GetNodeUserDatabaseConnection(connectionFlags,
															  destination->workerName,
															  destination->workerPort,
															  CurrentUserName(),
															  databaseName);
	if (PQstatus(destConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(destConn, ERROR);
	}

	if (migration->mode == MIGRATION_MODE_LOGICAL_REPLICATION &&
		RemoteSubscriptionExists(destConn, subscriptionName))
	{
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
						errmsg("migration \"%s\" already exists",
							   migration->migrationName)));
	}

	migration->sourceConnectionInfo = GetConnectionString(sourceConn);

	PG_TRY();
	{
		char *snapshotName = NULL;

		if (migration->mode == MIGRATION_MODE_LOGICAL_REPLICATION)
		{
			if (EnableDDLReplicationInDatabaseShardMove)
			{
				/* create a DDL replication table on both nodes */
				CreateDDLReplicationTable(sourceConn);
				CreateDDLReplicationTable(destConn);
				CreateDDLReplicationTrigger(destConn);
			}

			XLogRecPtr lsn = InvalidXLogRecPtr;
			bool exportSnapshot = true;
			snapshotName = CreateRemoteReplicationSlot(replicationConn, slotName,
													   exportSnapshot, &lsn);
		}

		migration->createdReplicationSlot = true;

		List *tableList = ListSourceDatabaseTables(sourceConn);

		if (!AllTablesHaveReplicaIdentity(tableList))
		{
			ereport(WARNING, (errmsg("not all tables have replica identities"),
							  errdetail("some UPDATE/DELETE commands may fail "
										"on the source during migration")));
		}

		ereport(NOTICE, (errmsg("migrating schema")));

		/* DUMP_ONLY mode copies data using pg_dump (for small, immediate migrations) */
		bool includeData = migration->mode == MIGRATION_MODE_DUMP_ONLY;

		/* TODO: drop and recreate database */
		bool dropIfExists = true;

		MigrateSchema(migration->sourceConnectionInfo, destConn,
					  snapshotName, includeData, dropIfExists);

		if (migration->mode == MIGRATION_MODE_LOGICAL_REPLICATION)
		{
			ereport(NOTICE, (errmsg("starting replication")));

			CreateRemotePublication(sourceConn, publicationName);
			migration->createdPublication = true;

			CreateRemoteSubscription(destConn, migration->sourceConnectionInfo,
									 subscriptionName,
									 publicationName,
									 slotName);
			migration->createdSubscription = true;

			StartRemoteMigrationMonitor(destCitusConn, databaseName, subscriptionName);

			CloseConnection(replicationConn);
			CloseConnection(destCitusConn);
		}

		CloseConnection(sourceConn);
		CloseConnection(destConn);
	}
	PG_CATCH();
	{
		/* close connections, which might be in a broken state */
		CloseConnection(sourceConn);
		CloseConnection(destConn);

		if (migration->mode == MIGRATION_MODE_LOGICAL_REPLICATION)
		{
			CloseConnection(replicationConn);
			CloseConnection(destCitusConn);
		}

		/* try to clean up in case of failure */
		MultiConnection *cleanupConn;

		/* clean up subscription first */
		if (migration->createdSubscription)
		{
			cleanupConn = GetNodeUserDatabaseConnection(connectionFlags,
														destination->workerName,
														destination->workerPort,
														CurrentUserName(),
														databaseName);

			if (PQstatus(cleanupConn->pgConn) == CONNECTION_OK)
			{
				DropRemoteSubscription(cleanupConn, subscriptionName);
			}

			CloseConnection(cleanupConn);
		}

		if (migration->createdPublication || migration->createdReplicationSlot)
		{
			cleanupConn = GetNodeUserDatabaseConnection(connectionFlags,
														source->workerName,
														source->workerPort,
														CurrentUserName(),
														databaseName);

			if (PQstatus(cleanupConn->pgConn) == CONNECTION_OK)
			{
				if (migration->createdPublication)
				{
					DropRemotePublication(cleanupConn, publicationName);
				}

				if (migration->createdReplicationSlot)
				{
					DropRemoteReplicationSlot(cleanupConn, slotName);
				}
			}

			CloseConnection(cleanupConn);
		}

		PG_RE_THROW();
	}
	PG_END_TRY();

	return true;
}


/*
 * ReplicationSlotNameForMigration returns the name of the logical replication
 * slot in the source database for a given migration.
 */
static char *
ReplicationSlotNameForMigration(char *migrationName)
{
	StringInfo migrationSlot = makeStringInfo();
	appendStringInfo(migrationSlot, "database_shard_move_%s_slot", migrationName);
	return migrationSlot->data;
}


/*
 * PublicationNameForMigration returns the name of the publication for a
 * given migration.
 */
static char *
PublicationNameForMigration(char *migrationName)
{
	StringInfo migrationPub = makeStringInfo();
	appendStringInfo(migrationPub, "database_shard_move_%s_pub", migrationName);
	return migrationPub->data;
}


/*
 * SubscriptionNameForMigration returns the name of the subscription for a
 * given migration.
 */
static char *
SubscriptionNameForMigration(char *migrationName)
{
	StringInfo migrationSub = makeStringInfo();
	appendStringInfo(migrationSub, "database_shard_move_%s_sub", migrationName);
	return migrationSub->data;
}


/*
 * GetConnectionString gets the connection string for a given connection.
 */
static char *
GetConnectionString(MultiConnection *conn)
{
	StringInfo connectionString = makeStringInfo();
	PQconninfoOption *connOptions = PQconninfo(conn->pgConn);

	for (int optionIndex = 0; connOptions[optionIndex].keyword != NULL; optionIndex++)
	{
		if (connOptions[optionIndex].val != NULL)
		{
			appendStringInfo(connectionString,
							 "%s=%s ",
							 connOptions[optionIndex].keyword,
							 quote_literal_cstr(connOptions[optionIndex].val));
		}
	}

	return connectionString->data;
}


/*
 * AllTablesHaveReplicaIdentity returns whether all tables in the list have
 * a replica identity.
 */
static bool
AllTablesHaveReplicaIdentity(List *tableList)
{
	ListCell *tableCell = NULL;

	foreach(tableCell, tableList)
	{
		SourceDatabaseTable *userTable = (SourceDatabaseTable *) lfirst(tableCell);

		if (!userTable->hasReplicaIdentity)
		{
			return false;
		}
	}

	return true;
}


/*
 * MigrateSchema runs pg_dump on the remote database and runs all the commands
 * against the local database.
 */
static void
MigrateSchema(char *sourceConnectionString, MultiConnection *destConn,
			  char *snapshotName, bool includeData, bool dropIfExists)
{
	List *schemaList = NIL;
	List *excludeTableList = NIL;

	/* obtain the full pg_dump output */
	char *databaseDump = RunPgDump(sourceConnectionString, snapshotName, schemaList,
								   excludeTableList, includeData, dropIfExists);

	/* run the pg_dump output through the parser */
	List *pgDumpParseTrees = pg_parse_query(databaseDump);

	/* apply checks and transformations before executing pg_dump statements */
	pgDumpParseTrees = PreProcessPgDumpParseTrees(pgDumpParseTrees);

	StringInfo filteredDump = makeStringInfo();

	RawStmt *parseTree = NULL;
	foreach_ptr(parseTree, pgDumpParseTrees)
	{
		Assert(parseTree->stmt_location >= 0);
		Assert(parseTree->stmt_len >= 0);

		char *command = palloc(parseTree->stmt_len + 2);
		memcpy_s(command, parseTree->stmt_len + 2,
				 databaseDump + parseTree->stmt_location, parseTree->stmt_len);
		command[parseTree->stmt_len] = ';';
		command[parseTree->stmt_len + 1] = '\0';

		appendStringInfoString(filteredDump, command);
	}

	ExecuteCriticalRemoteCommand(destConn, filteredDump->data);
}


/*
 * PreProcessPgDumpParseTrees pre-processes statements from pg_dump,
 * which are not always directly replayable.
 */
static List *
PreProcessPgDumpParseTrees(List *pgDumpParseTrees)
{
	List *finalParseTrees = NIL;

	RawStmt *parseTree = NULL;
	foreach_ptr(parseTree, pgDumpParseTrees)
	{
		Assert(parseTree->stmt_location >= 0);
		Assert(parseTree->stmt_len >= 0);

		Node *statement = parseTree->stmt;

		if (IsA(statement, CreatePolicyStmt))
		{
			CreatePolicyStmt *createPolicyStmt = castNode(CreatePolicyStmt, statement);

			if (IsPgCronPolicy(createPolicyStmt))
			{
				/* pg_dump erroneously shows CREATE POLICY for extension-owned tables */
				continue;
			}
		}

		finalParseTrees = lappend(finalParseTrees, parseTree);
	}

	return finalParseTrees;
}


/*
 * IsPgCronPolicy returns whether a given CREATE POLICY statement belongs
 * to pg_cron.
 */
static bool
IsPgCronPolicy(CreatePolicyStmt *statement)
{
	if (strcmp(statement->table->schemaname, "cron") != 0)
	{
		/* policy is not on a table in the cron schema */
		return false;
	}

	if (strcmp(statement->policy_name, "cron_job_policy") != 0 &&
		strcmp(statement->policy_name, "cron_job_run_details_policy") != 0)
	{
		/* policy is not one of the ones created by pg_cron */
		return false;
	}

	return true;
}


/*
 * database_shard_move_finish_move finishes a named migration.
 */
Datum
database_shard_move_finish(PG_FUNCTION_ARGS)
{
	text *databaseNameText = PG_GETARG_TEXT_P(0);
	char *databaseName = text_to_cstring(databaseNameText);

	/* TODO: this should be stored */
	int targetNodeGroupId = PG_GETARG_INT32(1);

	DatabaseMigration migration = {
		.migrationName = "0",
		.databaseName = databaseName,
		.targetNodeGroupId = targetNodeGroupId,
		.mode = MIGRATION_MODE_LOGICAL_REPLICATION
	};

	DatabaseMigrationFinish(&migration);

	PG_RETURN_VOID();
}


/*
 * DatabaseMigrationFinish finishes a migration by shutting down logical replication,
 * copying over sequence values, and updating the metadata.
 */
static bool
DatabaseMigrationFinish(DatabaseMigration *migration)
{
	char *migrationName = migration->migrationName;
	char *databaseName = migration->databaseName;
	int targetNodeGroupId = migration->targetNodeGroupId;

	char *subscriptionName = SubscriptionNameForMigration(migrationName);

	bool missingOk = false;
	Oid databaseOid = get_database_oid(databaseName, missingOk);
	DatabaseShard *dbShard = GetDatabaseShardByOid(databaseOid);
	if (dbShard == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("database %s is not a shard",
							   quote_identifier(databaseName))));
	}

	WorkerNode *source = LookupNodeForGroup(dbShard->nodeGroupId);
	WorkerNode *destination = LookupNodeForGroup(targetNodeGroupId);

	int connectionFlags = 0;
	MultiConnection *sourceConn = GetNodeUserDatabaseConnection(connectionFlags,
																source->workerName,
																source->workerPort,
																CurrentUserName(),
																databaseName);
	if (PQstatus(sourceConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(sourceConn, ERROR);
	}

	connectionFlags = 0;
	MultiConnection *destConn = GetNodeUserDatabaseConnection(connectionFlags,
															  destination->workerName,
															  destination->workerPort,
															  CurrentUserName(),
															  databaseName);
	if (PQstatus(destConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(destConn, ERROR);
	}

	if (migration->mode == MIGRATION_MODE_LOGICAL_REPLICATION &&
		!RemoteSubscriptionExists(destConn, subscriptionName))
	{
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
						errmsg("subscription \"%s\" does not exist on target node",
							   subscriptionName)));
	}

	WaitForCatchUp(sourceConn, destConn, subscriptionName);

	PauseDatabaseOnInboundPgBouncers(databaseName);

	/* TODO: wait for final changes */

	DropRemoteSubscription(destConn, subscriptionName);

	char *publicationName = PublicationNameForMigration(migrationName);
	DropRemotePublication(sourceConn, publicationName);

	char *slotName = ReplicationSlotNameForMigration(migration->migrationName);
	DropRemoteReplicationSlot(sourceConn, slotName);

	List *sequenceList = ListSourceDatabaseSequences(sourceConn);
	AdjustSequences(destConn, sequenceList);

	UpdateDatabaseShard(databaseOid, targetNodeGroupId);

	ResumeDatabaseOnInboundPgBouncers(databaseName);

	return true;
}


/*
 * WaitForCatchUp waits for the logical replication to catch up between
 * the two connections.
 */
static void
WaitForCatchUp(MultiConnection *sourceConn,
			   MultiConnection *destConn,
			   char *subscriptionName)
{
	XLogRecPtr sourcePosition = GetRemoteLogPosition(sourceConn);

	char *subscriptionLSNCommand = psprintf("SELECT pg_catalog.min(latest_end_lsn) "
											"FROM pg_catalog.pg_stat_subscription "
											"WHERE subname = %s",
											quote_literal_cstr(subscriptionName));

	MemoryContext loopContext = AllocSetContextCreateInternal(CurrentMemoryContext,
															  "WaitForCatchUp",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext oldContext = MemoryContextSwitchTo(loopContext);


	while (true)
	{
		XLogRecPtr targetPosition = GetRemoteLSN(destConn, subscriptionLSNCommand);
		if (targetPosition >= sourcePosition)
		{
			ereport(NOTICE, (errmsg("the subscription on node %s:%d has caught up with "
									"the source LSN %X/%X",
									destConn->hostname, destConn->port,
									LSN_FORMAT_ARGS(sourcePosition))));
			break;
		}

		/* TODO: timeouts */

		WaitForMiliseconds(2000);

		MemoryContextReset(loopContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * AdjustSequences updates the next value in local sequences based on the value
 * of sequences in the source database.
 */
static void
AdjustSequences(MultiConnection *conn, List *sequenceList)
{
	ListCell *sequenceCell = NULL;

	foreach(sequenceCell, sequenceList)
	{
		SourceDatabaseSequence *sequence =
			(SourceDatabaseSequence *) lfirst(sequenceCell);

		Oid schemaId = get_namespace_oid(sequence->schemaName, true);
		if (!OidIsValid(schemaId))
		{
			/* schema has been dropped on destination */
			continue;
		}

		Oid relationId = get_relname_relid(sequence->sequenceName, schemaId);
		if (!OidIsValid(relationId))
		{
			/* sequence has been dropped on destination */
			continue;
		}

		SetSequenceValue(conn, sequence->schemaName, sequence->sequenceName,
						 sequence->lastValue);
	}
}


/*
 * SetSequenceValue sets a sequence value over the given connection.
 */
static void
SetSequenceValue(MultiConnection *conn, char *schemaName, char *sequenceName,
				 int64 value)
{
	char *qualifiedSequenceName = quote_qualified_identifier(schemaName, sequenceName);

	StringInfo command = makeStringInfo();

	appendStringInfo(command,
					 "SELECT pg_catalog.setval(%s," INT64_FORMAT ")",
					 quote_literal_cstr(qualifiedSequenceName),
					 value);

	ExecuteCriticalRemoteCommand(conn, command->data);
}
