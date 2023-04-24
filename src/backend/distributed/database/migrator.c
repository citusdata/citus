#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

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
#include "distributed/database/migrator.h"
#include "distributed/database/pg_dump.h"
#include "distributed/database/remote_publication.h"
#include "distributed/database/remote_subscription.h"
#include "distributed/database/source_database_info.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "postmaster/postmaster.h"
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


#define MAX_MIGRATION_NAME_LENGTH 50


void _PG_init(void);

static char * ReplicationSlotNameForMigration(char *migrationName);
static char * PublicationNameForMigration(char *migrationName);
static char * SubscriptionNameForMigration(char *migrationName);
static char * GetConnectionString(MultiConnection *connection);
static void MigrateSchema(char *sourceConnectionString,
						  MultiConnection *destConn,
						  char *snapshotName,
						  bool dropIfExists);
static void CheckReplicaIdentities(List *tableList);
static void SwitchSourceToReadOnly(MultiConnection *conn);
static void AdjustSequences(MultiConnection *conn, List *sequenceList);
static void SetSequenceValue(MultiConnection *conn, char *schemaName, char *sequenceName,
							 int64 value);


PG_FUNCTION_INFO_V1(database_shard_move_start);
PG_FUNCTION_INFO_V1(database_shard_move_finish);


/*
 * migrator_start_migration_from starts a migration from the remote database.
 */
Datum
database_shard_move_start(PG_FUNCTION_ARGS)
{
	/* use one global name for migrations for now */
	char *migrationNameString = "0";
	text *databaseNameText = PG_GETARG_TEXT_P(0);
	char *databaseName = text_to_cstring(databaseNameText);
	int targetNodeGroupId = PG_GETARG_INT32(1);
	bool dropIfExists = PG_GETARG_BOOL(2);
	bool requireReplicaIdentity = PG_GETARG_BOOL(3);

	/* verify that pg_dump is on PATH before doing any work */
	char *pgDumpPath = GetPgDumpPath();
	if (pgDumpPath == NULL)
	{
		ereport(ERROR, (errmsg("could not locate pg_dump on PATH")));
	}

	if (strlen(migrationNameString) > MAX_MIGRATION_NAME_LENGTH)
	{
		ereport(ERROR, (errmsg("migration name cannot be longer than %d characters",
							   MAX_MIGRATION_NAME_LENGTH)));
	}

	char *slotName = ReplicationSlotNameForMigration(migrationNameString);
	char *publicationName = PublicationNameForMigration(migrationNameString);
	char *subscriptionName = SubscriptionNameForMigration(migrationNameString);

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
		PG_RETURN_VOID();
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
	connectionFlags = REQUIRE_REPLICATION_CONNECTION_PARAM;
	MultiConnection *replicationConn = GetNodeUserDatabaseConnection(connectionFlags,
																	 source->workerName,
																	 source->workerPort,
																	 CurrentUserName(),
																	 databaseName);
	if (PQstatus(replicationConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(replicationConn, ERROR);
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

	if (RemoteSubscriptionExists(destConn, subscriptionName))
	{
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
						errmsg("migration \"%s\" already exists",
							   migrationNameString)));
	}

	bool hasPgMigratorSource =
		SourceDatabaseHasPgMigratorSourceExtension(sourceConn);
	if (!hasPgMigratorSource)
	{
		ereport(DEBUG2, (errmsg("source database does not have the "
								"pg_migrator_source extension, DDL will not be "
								"replicated")));
	}

	char *sourceConnectionString = GetConnectionString(sourceConn);

	volatile bool createdReplicationSlot = false;
	volatile bool createdPublication = false;

	PG_TRY();
	{
		XLogRecPtr lsn = InvalidXLogRecPtr;
		bool exportSnapshot = true;
		char *snapshotName = CreateRemoteReplicationSlot(replicationConn, slotName,
														 exportSnapshot, &lsn);

		createdReplicationSlot = true;

		List *tableList = ListSourceDatabaseTables(sourceConn);

		if (requireReplicaIdentity)
		{
			CheckReplicaIdentities(tableList);
		}

		if (hasPgMigratorSource)
		{
			/*
			 * Hack: delete ddl_propagation records before starting replication.
			 * to avoid replaying past records. This is not concurrency safe.
			 */
			DeleteDDLPropagationRecordsOnSource(sourceConn);
		}

		ereport(NOTICE, (errmsg("migrating schema")));

		MigrateSchema(sourceConnectionString, destConn,
					  snapshotName, dropIfExists);

		ereport(NOTICE, (errmsg("starting replication")));

		CreateRemotePublication(sourceConn, publicationName);
		createdPublication = true;

		CreateRemoteSubscription(destConn, sourceConnectionString, subscriptionName,
								 publicationName, slotName);

		CloseConnection(sourceConn);
		CloseConnection(replicationConn);
		CloseConnection(destConn);
	}
	PG_CATCH();
	{
		CloseConnection(sourceConn);
		CloseConnection(replicationConn);
		CloseConnection(destConn);

		MultiConnection *cleanupConn = GetNodeUserDatabaseConnection(connectionFlags,
																	 source->workerName,
																	 source->workerPort,
																	 CurrentUserName(),
																	 databaseName);
		if (PQstatus(cleanupConn->pgConn) == CONNECTION_OK)
		{
			/* try to clean up in case of failure */
			if (createdPublication)
			{
				DropRemotePublication(cleanupConn, publicationName);
			}

			if (createdReplicationSlot)
			{
				DropRemoteReplicationSlot(cleanupConn, slotName);
			}
		}

		CloseConnection(cleanupConn);

		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_TEXT_P(cstring_to_text(subscriptionName));
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
 * CheckReplicaIdentities throws an error if one of the tables does not have
 * a replica identity.
 */
static void
CheckReplicaIdentities(List *tableList)
{
	ListCell *tableCell = NULL;

	foreach(tableCell, tableList)
	{
		SourceDatabaseTable *userTable = (SourceDatabaseTable *) lfirst(tableCell);

		if (!userTable->hasReplicaIdentity)
		{
			char *qualifiedTableName = quote_qualified_identifier(userTable->schemaName,
																  userTable->tableName);

			ereport(ERROR, (errmsg("table %s does not have a primary key or replica "
								   "identity", qualifiedTableName),
							errhint("Create a primary key or re-run with "
									"require_replica_identities := false ")));
		}
	}
}


/*
 * MigrateSchema runs pg_dump on the remote database and runs all the commands
 * against the local database.
 */
static void
MigrateSchema(char *sourceConnectionString, MultiConnection *destConn,
			  char *snapshotName, bool dropIfExists)
{
	List *schemaList = NIL;
	List *excludeTableList = NIL;

	/* obtain the full pg_dump output */
	char *databaseDump = RunPgDump(sourceConnectionString, snapshotName, schemaList,
								   excludeTableList, dropIfExists);

	/* run the pg_dump output through the parser */
	/*List *pgDumpParseTrees = pg_parse_query(databaseDump); */

	ExecuteCriticalRemoteCommand(destConn, databaseDump);
}


/*
 * database_shard_move_finish_move finishes a named migration.
 */
Datum
database_shard_move_finish(PG_FUNCTION_ARGS)
{
	/* use one global name for migrations for now */
	char *migrationNameString = "0";

	text *databaseNameText = PG_GETARG_TEXT_P(0);
	char *databaseName = text_to_cstring(databaseNameText);

	/* TODO: this should be stored in shared memory */
	int targetNodeGroupId = PG_GETARG_INT32(1);
	bool switchSourceToReadOnly = PG_GETARG_BOOL(2);

	char *subscriptionName = SubscriptionNameForMigration(migrationNameString);

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


	if (switchSourceToReadOnly)
	{
		SwitchSourceToReadOnly(sourceConn);
	}

	/* TODO: wait for final changes */

	DropRemoteSubscription(destConn, subscriptionName);

	char *publicationName = PublicationNameForMigration(migrationNameString);
	DropRemotePublication(sourceConn, publicationName);

	char *slotName = ReplicationSlotNameForMigration(migrationNameString);
	DropRemoteReplicationSlot(sourceConn, slotName);

	List *sequenceList = ListSourceDatabaseSequences(sourceConn);
	AdjustSequences(destConn, sequenceList);

	UpdateDatabaseShard(databaseOid, targetNodeGroupId);

	PG_RETURN_TEXT_P(cstring_to_text("finished"));
}


/*
 * SwitchSourceToReadOnly switches the source database to read only
 * to prevent a split brain scenario.
 */
static void
SwitchSourceToReadOnly(MultiConnection *conn)
{
	StringInfo command = makeStringInfo();
	char *databaseName = PQdb(conn->pgConn);

	appendStringInfo(command,
					 "ALTER DATABASE %s SET default_transaction_read_only TO on",
					 quote_identifier(databaseName));

	ExecuteCriticalRemoteCommand(conn, command->data);

	char *reloadCommand = "SELECT pg_reload_conf()";

	ExecuteCriticalRemoteCommand(conn, reloadCommand);

	/* keep the current session writable */
	char *setReadOnlyCommand = "SET default_transaction_read_only TO off";

	ExecuteCriticalRemoteCommand(conn, setReadOnlyCommand);
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
