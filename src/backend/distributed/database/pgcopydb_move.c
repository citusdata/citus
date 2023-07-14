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
#include "distributed/database/pgcopydb.h"
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



PG_FUNCTION_INFO_V1(pgcopydb_database_move);


/*
 * pgcopydb_database_move does a full migration of a database using
 * pgcopydb.
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

	/* open a regular connection to the target database */
	connectionFlags = 0;
	MultiConnection *targetConn = GetNodeUserDatabaseConnection(connectionFlags,
																destination->workerName,
																destination->workerPort,
																CurrentUserName(),
																databaseName);
	if (PQstatus(targetConn->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(targetConn, ERROR);
	}

	moveDesc->sourceConnectionInfo = GetConnectionString(sourceConn);
	moveDesc->targetConnectionInfo = GetConnectionString(targetConn);

	/* TODO: read from stdout, and monitor the overall move */
	char *output = RunPgcopydb(moveDesc->sourceConnectionInfo,
							   moveDesc->targetConnectionInfo);

	elog(NOTICE, "%s", output);

	CHECK_FOR_INTERRUPTS();

	PauseDatabaseOnInboundPgBouncers(databaseName);

	UpdateDatabaseShard(databaseOid, targetNodeGroupId);

	ResumeDatabaseOnInboundPgBouncers(databaseName);
}
