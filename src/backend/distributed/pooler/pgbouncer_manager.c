#include <unistd.h>
#include <sys/wait.h>

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "common/file_perm.h"
#include "common/hashfn.h"
#include "common/ip.h"
#include "common/string.h"
#include "distributed/connection_management.h"
#include "distributed/database/database_sharding.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/pooler/pgbouncer_manager.h"
#include "distributed/remote_commands.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/shared_library_init.h"
#include "distributed/worker_transaction.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "postmaster/bgworker.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/varlena.h"


#define PGBOUNCER_USERS_FILE "citus-pgbouncer-users.txt"
#define PGBOUNCER_DATABASES_FILE "citus-pgbouncer-databases.txt"


/*
 * PgBouncerProcess represents the state of a single, running pgbouncer process.
 */
typedef struct PgBouncerProcess
{
	/* whether we believe the pgbouncer to be running */
	bool isActive;

	/* PID of the pgbouncer process */
	pid_t pgbouncerPid;

	/* path to the unix domain socket */
	char unixDomainSocketDir[MAXPGPATH];
} PgBouncerProcess;


/*
 * PgBouncerManagerSharedData encapsulates the lock on the shared memory
 * state.
 */
typedef struct PgBouncerManagerSharedData
{
	/* lock tranche */
	int pgbouncerTrancheId;
	char *pgbouncerTrancheName;
	LWLock inboundPgBouncerLock;

	/* all running pgbouncer processes */
	PgBouncerProcess inboundPgBouncers[PGBOUNCER_INBOUND_PROCS_MAX];

	/* number of running pgbouncer processes */
	int activePgBouncerCount;

	/* latch of the pgbouncer manager latch */
	Latch *pgBouncerManagerLatch;
} PgBouncerManagerSharedData;


static void SharedPgBouncerManagerShmemInit(void);
static void StartPgBouncerManager(void);
static void PgBouncerManagerSigChldHandler(SIGNAL_ARGS);
static void PgBouncerManagerSigTermHandler(SIGNAL_ARGS);
static void PgBouncerManagerSigHupHandler(SIGNAL_ARGS);
static void PgBouncerManagerShmemExit(int code, Datum arg);
static void GenerateInboundPgBouncerConfigs(void);
static void GenerateUsersFile(void);
static void GenerateDatabaseShardsFile(void);
static void GenerateInboundPgBouncerConfig(int pgBouncerId);
static int CalculatePeerIdForNodeGroup(int nodeGroupId, int pgBouncerId);
static int GetInboundPgBouncerDefaultPoolSize(void);
static char * GetInboundPgBouncerUnixDomainSocketDir(int pgBouncerId);
static char * GetFirstUnixSocketDirectory(void);
static char * GetCommaSeparatedSuperusers(void);
static void SafeWriteToFile(char *content, int contentLength, char *path);
static void EnsurePgBouncersRunning(void);
static void EnsurePgBouncerRunning(int pgBouncerId);
static void SignalInboundPgBouncers(int signo);
static bool ExecuteCommandOnAllInboundPgBouncers(char *command);
static void LockInboundPgBouncerState(LWLockMode lockMode);
static void UnlockInboundPgBouncerState(void);


/* GUC variable that sets the number of inbound pgbouncer procs */
int PgBouncerInboundProcs = PGBOUNCER_INBOUND_PROCS_DEFAULT;

/* GUC variable that sets the inbound pgbouncer port */
int PgBouncerInboundPort = 6432;

/* GUC variable that sets the pgbouncer config file to include */
char *PgBouncerIncludeConfig = "";

/* GUC variable that sets the path to pgbouncer executable */
char *PgBouncerPath = "pgbouncer";

/* global variable to trigger reconfigure post-commit */
bool ReconfigurePgBouncersOnCommit = false;

/* set when a SIGHUP was received */
static volatile sig_atomic_t got_SIGHUP = false;

/* set when a SIGTERM was received */
static volatile sig_atomic_t got_SIGTERM = false;

/* locks on shared memory state */
static PgBouncerManagerSharedData *PgBouncerState = NULL;

/* used to reliably distinguish between parent and child processes */
static bool IsParent = true;

/* next link in the shared memory startup hook chain */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


/*
 * InitializeSharedPgBouncerManager requests the necessary shared memory
 * from Postgres and sets up the shared memory startup hook.
 */
void
InitializeSharedPgBouncerManager(void)
{
#if PG_VERSION_NUM < PG_VERSION_15
	if (!IsUnderPostmaster)
	{
		RequestAddinShmemSpace(SharedPgBouncerManagerShmemSize());
	}
#endif

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = SharedPgBouncerManagerShmemInit;

	StartPgBouncerManager();
}


/*
 * SharedPgBouncerManagerShmemSize returns the size that should be allocated
 * on the shared memory for inbound pgbouncer.
 */
size_t
SharedPgBouncerManagerShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(PgBouncerManagerSharedData));

	return size;
}


/*
 * SharedPgBouncerManagerShmemInit initializes the shared memory used
 * for keeping track of inbound pgbouncers across backends.
 */
static void
SharedPgBouncerManagerShmemInit(void)
{
	bool alreadyInitialized = false;

	/*
	 * Currently the lock isn't required because allocation only happens at
	 * startup in postmaster, but it doesn't hurt, and makes things more
	 * consistent with other extensions.
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	PgBouncerState =
		(PgBouncerManagerSharedData *) ShmemInitStruct(
			"PgBouncer Manager Data",
			sizeof(PgBouncerManagerSharedData),
			&alreadyInitialized);

	if (!alreadyInitialized)
	{
		PgBouncerState->pgbouncerTrancheId = LWLockNewTrancheId();
		PgBouncerState->pgbouncerTrancheName =
			"PgBouncer Manager Tranche";
		LWLockRegisterTranche(PgBouncerState->pgbouncerTrancheId,
							  PgBouncerState->pgbouncerTrancheName);

		LWLockInitialize(&PgBouncerState->inboundPgBouncerLock,
						 PgBouncerState->pgbouncerTrancheId);
	}

	LWLockRelease(AddinShmemInitLock);

	Assert(PgBouncerState != NULL);
	Assert(PgBouncerState->pgbouncerTrancheId != 0);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * StartPgBouncerManager starts the main background worker that manages pgbouncers.
 */
static void
StartPgBouncerManager(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 1; /* seconds */
	worker.bgw_main_arg = 0;
	worker.bgw_notify_pid = 0;
	strlcpy(worker.bgw_library_name, "citus",
			sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_function_name, "PgBouncerManagerMain",
			sizeof(worker.bgw_function_name));
	strlcpy(worker.bgw_name, "Citus PgBouncer Manager",
			sizeof(worker.bgw_name));

	RegisterBackgroundWorker(&worker);
}


/*
 * PgBouncerManagerMain is the main entry point for a database-specific
 * background worker.
 */
void
PgBouncerManagerMain(Datum arg)
{
	/* set my latch */
	LockInboundPgBouncerState(LW_EXCLUSIVE);
	PgBouncerState->pgBouncerManagerLatch = MyLatch;
	UnlockInboundPgBouncerState();

	/* set up signal handlers for the background worker */
	pqsignal(SIGCHLD, PgBouncerManagerSigChldHandler);
	pqsignal(SIGHUP, PgBouncerManagerSigHupHandler);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, PgBouncerManagerSigTermHandler);

	BackgroundWorkerUnblockSignals();

	/* connect to the database as superuser */
	BackgroundWorkerInitializeConnection(CitusMainDatabase, NULL, 0);

	/* make sure we terminate the pgbouncers when this process terminates */
	before_shmem_exit(PgBouncerManagerShmemExit, (Datum) 0);

	/* GenerateInboundPgBouncerConfigs and EnsurePgBouncersRunning require a transaction */
	StartTransactionCommand();

	ereport(LOG, (errmsg("citus pgbouncer manager started for database %s",
						 CitusMainDatabase)));

	/* on start, always start pgbouncers */
	GenerateInboundPgBouncerConfigs();
	EnsurePgBouncersRunning();

	CommitTransactionCommand();

	while (!got_SIGTERM)
	{
		int latchFlags = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
		double timeout = 10000.0;
		bool doReconfigure = false;

		int rc = WaitLatch(MyLatch, latchFlags, (long) timeout, PG_WAIT_EXTENSION);
		if (rc & WL_POSTMASTER_DEATH)
		{
			proc_exit(1);
		}

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();

			doReconfigure = true;
		}

		if (got_SIGTERM)
		{
			break;
		}

		if (got_SIGHUP)
		{
			got_SIGHUP = false;

			/* reload postgresql.conf */
			ProcessConfigFile(PGC_SIGHUP);

			doReconfigure = true;
		}

		StartTransactionCommand();

		if (doReconfigure)
		{
			GenerateInboundPgBouncerConfigs();

			/* tell the pgbouncers about possibly changed configs */
			SignalInboundPgBouncers(SIGHUP);
		}

		/* in every iteration, make sure pgbouncers are running */

		EnsurePgBouncersRunning();
		CommitTransactionCommand();
	}

	elog(LOG, "citus pgbouncer manager exiting");
}


/* PgBouncerManagerSigTermHandler calls proc_exit(0) */
static void
PgBouncerManagerSigTermHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


/*
 * PgBouncerManagerSigHupHandler set a flag to re-read config file at next
 * convenient time.
 */
static void
PgBouncerManagerSigHupHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGHUP = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}


static void
PgBouncerManagerSigChldHandler(SIGNAL_ARGS)
{
	int saved_errno = errno;
	int exitStatus = 0;
	int pid = 0;

	while ((pid = waitpid((pid_t) (-1), &exitStatus, WNOHANG)) > 0)
	{
		/* TODO: remove from shared hash */
	}

	/* wake up main loop */
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = saved_errno;
}


static void
PgBouncerManagerShmemExit(int code, Datum arg)
{ }


/*
 * GenerateInboundPgBouncerConfigs generates the users and config files for inbound
 * pgbouncers.
 */
static void
GenerateInboundPgBouncerConfigs(void)
{
	GenerateUsersFile();

	if (EnableDatabaseSharding)
	{
		GenerateDatabaseShardsFile();
	}

	for (int pgBouncerId = 0; pgBouncerId < PgBouncerInboundProcs; pgBouncerId++)
	{
		GenerateInboundPgBouncerConfig(pgBouncerId);
	}
}


/*
 * GenerateUsersFile generates the auth_file with empty passwords
 * for all roles in the database, such that they can all connect
 * via the inbound pgbouncer.
 */
static void
GenerateUsersFile(void)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = false;
	HeapTuple heapTuple = NULL;

	Relation pgAuthId = table_open(AuthIdRelationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgAuthId);

	ScanKeyInit(&scanKey[0], Anum_pg_authid_rolcanlogin,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));

	SysScanDesc scanDescriptor = systable_beginscan(pgAuthId, InvalidOid, indexOK,
													NULL, scanKeyCount, scanKey);

	StringInfoData usersList;
	initStringInfo(&usersList);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		bool isNull = false;

		Datum roleNameDatum = heap_getattr(heapTuple, Anum_pg_authid_rolname,
										   tupleDescriptor, &isNull);
		Name roleName = DatumGetName(roleNameDatum);
		char *roleNameStr = NameStr(*roleName);
		const char *quotedRoleName = quote_identifier(roleNameStr);

		if (quotedRoleName[0] == '"')
		{
			/* role name is already quoted */
			appendStringInfoString(&usersList, quotedRoleName);
		}
		else
		{
			/* force quotes */
			appendStringInfo(&usersList, "\"%s\"", quotedRoleName);
		}

		Datum passwordDatum = heap_getattr(heapTuple, Anum_pg_authid_rolpassword,
										   tupleDescriptor, &isNull);
		char *password = TextDatumGetCString(passwordDatum);

		appendStringInfo(&usersList, " \"%s\"\n", password);
	}

	SafeWriteToFile(usersList.data, usersList.len, PGBOUNCER_USERS_FILE);

	systable_endscan(scanDescriptor);
	table_close(pgAuthId, AccessShareLock);
}


/*
 * GenerateInboundPgbouncerDatabaseFile generates the database section of a
 * pgbouncer configuration file for database shards.
 */
static void
GenerateDatabaseShardsFile(void)
{
	List *databaseShardList = ListDatabaseShards();
	DatabaseShard *databaseShard = NULL;

	StringInfoData databaseList;
	initStringInfo(&databaseList);

	foreach_ptr(databaseShard, databaseShardList)
	{
		WorkerNode *workerNode = LookupNodeForGroup(databaseShard->nodeGroupId);
		char *databaseName = get_database_name(databaseShard->databaseOid);
		if (databaseName == NULL)
		{
			/* database no longer exists */
			continue;
		}

		appendStringInfo(&databaseList, "%s = host=%s port=%d\n",
						 quote_identifier(databaseName),
						 workerNode->workerName,
						 workerNode->workerPort);
	}

	SafeWriteToFile(databaseList.data, databaseList.len, PGBOUNCER_DATABASES_FILE);
}


/*
 * GenerateInboundPgBouncerConfig generates a pgbouncer ini file for the given
 * peer ID.
 */
static void
GenerateInboundPgBouncerConfig(int myPgBouncerId)
{
	int myGroupId = GetLocalGroupId();

	/* allows for up to 32 pgbouncers */
	int myPeerId = CalculatePeerIdForNodeGroup(myGroupId, myPgBouncerId);
	int defaultPoolSize = GetInboundPgBouncerDefaultPoolSize();

	char *unixDomainSocketDir = GetInboundPgBouncerUnixDomainSocketDir(myPgBouncerId);
	char *superusers = GetCommaSeparatedSuperusers();

	StringInfo pgbouncerConfig = makeStringInfo();

	appendStringInfo(pgbouncerConfig,
					 "[databases]\n"
					 "%s = host=%s port=%d dbname=%s\n"
					 "%s\n",
					 CitusMainDatabase,
					 GetFirstUnixSocketDirectory(),
					 PostPortNumber,
					 CitusMainDatabase,
					 EnableDatabaseSharding ? "%include " PGBOUNCER_DATABASES_FILE : "");

	appendStringInfo(pgbouncerConfig,
					 "[pgbouncer]\n"
					 "pool_mode = transaction\n"
					 "peer_id = %d\n"
					 "unix_socket_dir = %s\n"
					 "listen_addr = %s\n"
					 "listen_port = %d\n"
					 "so_reuseport = 1\n"
					 "pidfile = citus-pgbouncer-inbound-%d.pid\n"
					 "syslog_ident = citus-pgbouncer-inbound-%d\n",
					 myPeerId,
					 unixDomainSocketDir,
					 ListenAddresses,
					 PgBouncerInboundPort,
					 myPgBouncerId,
					 myPgBouncerId);

	/* TODO: auth_hba_file */
	appendStringInfo(pgbouncerConfig,

	                 /* TODO: make configurable */
					 "auth_type = trust\n"
					 "auth_file = %s\n"
					 "admin_users = %s\n",
					 PGBOUNCER_USERS_FILE,
					 superusers);

	appendStringInfo(pgbouncerConfig,
					 "default_pool_size = %d\n"
					 "tcp_keepalive = 1\n"
					 "tcp_keepcnt = %d\n"
					 "tcp_keepidle = %d\n"
					 "tcp_keepintvl = %d\n",
					 defaultPoolSize,
					 tcp_keepalives_count,
					 tcp_keepalives_idle,
					 tcp_keepalives_interval);

	appendStringInfo(pgbouncerConfig,
					 "client_tls_sslmode = require\n"
					 "client_tls_protocols = secure\n");

	if (SSLCipherSuites != NULL && SSLCipherSuites[0] != '\0')
	{
		appendStringInfo(pgbouncerConfig,
						 "client_tls_ciphers = %s\n",
						 SSLCipherSuites);
	}

	if (ssl_key_file != NULL && ssl_key_file[0] != '\0')
	{
		appendStringInfo(pgbouncerConfig,
						 "client_tls_key_file = %s\n",
						 ssl_key_file);
	}

	if (ssl_cert_file != NULL && ssl_cert_file[0] != '\0')
	{
		appendStringInfo(pgbouncerConfig,
						 "client_tls_cert_file = %s\n",
						 ssl_cert_file);
	}

	if (ssl_ca_file != NULL && ssl_ca_file[0] != '\0')
	{
		appendStringInfo(pgbouncerConfig,
						 "client_tls_ca_file = %s\n",
						 ssl_ca_file);
	}

	if (PgBouncerIncludeConfig != NULL && PgBouncerIncludeConfig[0] != '\0')
	{
		appendStringInfo(pgbouncerConfig,
						 "%%include %s\n",
						 PgBouncerIncludeConfig);
	}

	appendStringInfoString(pgbouncerConfig,
						   "\n[peers]\n");

	/* add the peer IDs of local inbound pgbouncers using unix domain socket dirs */
	for (int pgBouncerId = 0; pgBouncerId < PgBouncerInboundProcs; pgBouncerId++)
	{
		int peerId = CalculatePeerIdForNodeGroup(myGroupId, pgBouncerId);
		char *peerUnixDomainSocket = GetInboundPgBouncerUnixDomainSocketDir(pgBouncerId);

		appendStringInfo(pgbouncerConfig, "%d = host=%s port=%d\n",
						 peerId,
						 peerUnixDomainSocket,
						 PgBouncerInboundPort);
	}

	List *workerNodeList = TargetWorkerSetNodeList(OTHER_METADATA_NODES, NoLock);
	WorkerNode *workerNode = NULL;

	/* add the peer IDs of inbound pgbouncers on other nodes */
	foreach_ptr(workerNode, workerNodeList)
	{
		for (int pgBouncerId = 0; pgBouncerId < PgBouncerInboundProcs; pgBouncerId++)
		{
			int peerId = CalculatePeerIdForNodeGroup(workerNode->groupId,
													 pgBouncerId);

			/* we assume inbound pgbouncer port is the same everywhere */
			appendStringInfo(pgbouncerConfig, "%d = host=%s port=%d\n",
							 peerId,
							 workerNode->workerName,
							 PgBouncerInboundPort);
		}
	}


	StringInfo configFile = makeStringInfo();
	appendStringInfo(configFile, "citus-pgbouncer-inbound-%d.ini", myPgBouncerId);

	SafeWriteToFile(pgbouncerConfig->data, pgbouncerConfig->len, configFile->data);
}


/*
 * CalculatePeerIdForNodeGroup returns the peer ID for a given node group ID
 * and local pgbouncer ID.
 */
static int
CalculatePeerIdForNodeGroup(int nodeGroupId, int pgBouncerId)
{
	return (nodeGroupId << PGBOUNCER_PEER_ID_LOCAL_ID_BITS) | (pgBouncerId + 1);
}


/*
 * GetInboundPgBouncerDefaultPoolSize returns a default_pool_size value
 * that can be used for an inbound pgbouncer process.
 */
static int
GetInboundPgBouncerDefaultPoolSize(void)
{
	int defaultPoolSize = 0;

	if (MaxClientConnections >= 0)
	{
		defaultPoolSize = MaxClientConnections / PgBouncerInboundProcs;
	}
	else
	{
		defaultPoolSize = MaxConnections / PgBouncerInboundProcs;
	}

	if (defaultPoolSize == 0)
	{
		defaultPoolSize = 1;
	}

	return defaultPoolSize;
}


/*
 * GetInboundPgBouncerUnixDomainSocketDir return the unix domain socket dir for
 * the given peer ID.
 */
static char *
GetInboundPgBouncerUnixDomainSocketDir(int pgBouncerId)
{
	StringInfo unixDomainSocketDir = makeStringInfo();

	appendStringInfo(unixDomainSocketDir,
					 "%s/citus-pgbouncer-inbound-%d.sock",
					 GetFirstUnixSocketDirectory(),
					 pgBouncerId);

	return unixDomainSocketDir->data;
}


/*
 * GetFirstUnixSocketDirectory returns the first unix socket directory in
 * the postgresql configuration.
 */
static char *
GetFirstUnixSocketDirectory(void)
{
	if (Unix_socket_directories == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unix_socket_directories needs to be set "
							   "to use pgbouncer")));
	}

	/* Need a modifiable copy of Unix_socket_directories */
	char *rawstring = pstrdup(Unix_socket_directories);

	List *directoryList = NIL;

	/* Parse string into list of directories */
	if (!SplitDirectoriesString(rawstring, ',', &directoryList))
	{
		/* syntax error in list */
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax in parameter \"%s\"",
						"unix_socket_directories")));
	}

	if (directoryList == NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unix_socket_directories needs to be set "
							   "to use pgbouncer")));
	}

	return (char *) linitial(directoryList);
}


/*
 * GetCommaSeparatedSuperusers returns a comma-separate list of database superusers.
 */
static char *
GetCommaSeparatedSuperusers(void)
{
	ScanKeyData scanKey[2];
	int scanKeyCount = 2;
	bool indexOK = false;
	HeapTuple heapTuple = NULL;

	Relation pgAuthId = table_open(AuthIdRelationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgAuthId);

	ScanKeyInit(&scanKey[0], Anum_pg_authid_rolcanlogin,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));

	ScanKeyInit(&scanKey[1], Anum_pg_authid_rolsuper,
				BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));

	SysScanDesc scanDescriptor = systable_beginscan(pgAuthId, InvalidOid, indexOK,
													NULL, scanKeyCount, scanKey);

	StringInfoData usersList;
	initStringInfo(&usersList);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		bool isNull = false;

		Datum roleNameDatum = heap_getattr(heapTuple, Anum_pg_authid_rolname,
										   tupleDescriptor, &isNull);
		Name roleName = DatumGetName(roleNameDatum);
		char *roleNameStr = NameStr(*roleName);
		const char *quotedRoleName = quote_identifier(roleNameStr);

		appendStringInfo(&usersList, "%s%s",
						 usersList.len > 0 ? ", " : "",
						 quotedRoleName);
	}

	systable_endscan(scanDescriptor);
	table_close(pgAuthId, AccessShareLock);

	return usersList.data;
}


/*
 * SafeWriteToFile writes the contents to a staging file (<path>.stage) and
 * then performs a durable_rename.
 */
static void
SafeWriteToFile(char *content, int contentLength, char *path)
{
	StringInfoData stagingPath;
	initStringInfo(&stagingPath);
	appendStringInfo(&stagingPath, "%s.stage", path);

	FILE *fileStream = NULL;
	if ((fileStream = fopen(stagingPath.data, "w")) == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create file \"%s\": %m",
							   stagingPath.data)));
	}

	if (fwrite(content, sizeof(char), contentLength, fileStream) != contentLength)
	{
		if (errno == 0)
		{
			/* assume out of disk */
			errno = ENOSPC;
		}

		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write to file \"%s\": %m",
							   stagingPath.data)));
	}

	fclose(fileStream);

	if (durable_rename(stagingPath.data, path, ERROR) < 0)
	{
		/* not really necessary since we use ERROR log level */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						stagingPath.data, path)));
	}
}


/*
 * EnsurePgBouncersRunning ensures pgbouncer is running for all primary nodes.
 */
void
EnsurePgBouncersRunning(void)
{
	for (int pgBouncerId = 0; pgBouncerId < PgBouncerInboundProcs; pgBouncerId++)
	{
		EnsurePgBouncerRunning(pgBouncerId);
	}
}


/*
 * EnsurePgBouncerRunning ensures pgbouncer is running for the given node.
 */
static void
EnsurePgBouncerRunning(int pgBouncerId)
{
	/*
	 * First check the hash with a light-weight lock. This is optional,
	 * but avoids frequently taking an exclusive lock.
	 */
	LockInboundPgBouncerState(LW_SHARED);

	PgBouncerProcess *proc = &(PgBouncerState->inboundPgBouncers[pgBouncerId]);

	/* find pgbouncer in shared memory hash */
	if (proc->isActive)
	{
		if (kill(proc->pgbouncerPid, 0) == 0)
		{
			/* pgbouncer is still running (common case) */
			UnlockInboundPgBouncerState();
			return;
		}
	}

	UnlockInboundPgBouncerState();

	/*
	 * Recheck everything to be robust to concurrent (re)starts.
	 */
	LockInboundPgBouncerState(LW_EXCLUSIVE);

	proc = &(PgBouncerState->inboundPgBouncers[pgBouncerId]);

	/* find pgbouncer in shared memory hash */
	if (proc->isActive)
	{
		if (kill(proc->pgbouncerPid, 0) == 0)
		{
			/* pgbouncer is running (must have just started) */
			UnlockInboundPgBouncerState();
			return;
		}
	}

	char *unixDomainSocketDir = GetInboundPgBouncerUnixDomainSocketDir(pgBouncerId);

	StringInfo pidFile = makeStringInfo();
	appendStringInfo(pidFile, "citus-pgbouncer-inbound-%d.pid", pgBouncerId);

	/* check if a pgbouncer is already running */
	FILE *pidFileStream = NULL;
	if ((pidFileStream = fopen(pidFile->data, "r")) != NULL)
	{
		int readPid = 0;

		/* parse PID from file */
		if (fscanf(pidFileStream, "%d", &readPid) == 1)
		{
			/* found a PID, check if it is running (reload configs if so) */
			if (kill(readPid, SIGHUP) == 0)
			{
				/* TODO: check if process is actually pgbouncer */
				ereport(LOG, (errmsg("pgbouncer running with PID %d", readPid)));

				proc->isActive = true;
				proc->pgbouncerPid = readPid;
				strlcpy(proc->unixDomainSocketDir, unixDomainSocketDir, MAXPGPATH);

				UnlockInboundPgBouncerState();
				fclose(pidFileStream);
				return;
			}
		}

		fclose(pidFileStream);
	}

	/* process is not running */
	proc->isActive = false;

	/* make sure the unix domain socket directory exists */
	mkdir(unixDomainSocketDir, pg_dir_create_mode);

	StringInfo configFile = makeStringInfo();
	appendStringInfo(configFile, "citus-pgbouncer-inbound-%d.ini", pgBouncerId);

	/* start a pgbouncer process */
	pid_t pgbouncerPid = fork_process();
	if (pgbouncerPid < 0)
	{
		ereport(WARNING, (errmsg("failed to start process for pgbouncer")));

		UnlockInboundPgBouncerState();
		return;
	}
	else if (pgbouncerPid == 0)
	{
		/* drop our connection to postmaster's shared memory */
		PGSharedMemoryDetach();

		/* guard callbacks against pgbouncer entering */
		IsParent = false;

		/* child process will now become pgbouncer */
		char *argv[4];
		argv[0] = "pgbouncer";

		/*argv[1] = "-q"; */
		argv[1] = configFile->data;
		argv[2] = NULL;

		/* become pgbouncer */
		execvp(argv[0], argv);

		/* oops, no pgbouncer around */
		ereport(WARNING, (errmsg("could not start %s", argv[0]),
						  errhint("Make sure %s is in the PATH", argv[0])));

		/* parent will try to restart on failure, wait 60s before that happens */
		pg_usleep(60000000L);

		exit(1);
	}

	ereport(LOG, (errmsg("started inbound pgbouncer peer %d with PID %d",
						 pgBouncerId, pgbouncerPid)));

	proc->isActive = true;
	proc->pgbouncerPid = pgbouncerPid;

	strlcpy(proc->unixDomainSocketDir, unixDomainSocketDir, MAXPGPATH);

	UnlockInboundPgBouncerState();
}


/*
 * LockInboundPgBouncerState is a utility function that should be used when
 * accessing to the PgBouncerState, which is in the shared memory.
 */
static void
LockInboundPgBouncerState(LWLockMode lockMode)
{
	LWLockAcquire(&PgBouncerState->inboundPgBouncerLock, lockMode);
}


/*
 * UnlockInboundPgBouncerState is a utility function that should be used after
 * LockInboundPgBouncerState().
 */
static void
UnlockInboundPgBouncerState(void)
{
	LWLockRelease(&PgBouncerState->inboundPgBouncerLock);
}


/*
 * SignalInboundPgBouncers sends a signal to all pgbouncers.
 */
static void
SignalInboundPgBouncers(int signo)
{
	LockInboundPgBouncerState(LW_SHARED);

	for (int pgBouncerId = 0; pgBouncerId < PgBouncerInboundProcs; pgBouncerId++)
	{
		PgBouncerProcess *proc = &(PgBouncerState->inboundPgBouncers[pgBouncerId]);

		kill(proc->pgbouncerPid, signo);
	}

	UnlockInboundPgBouncerState();
}


/*
 * PauseDatabaseOnInboundPgBouncers pauses traffic to a database on all
 * inbound pgbouncers.
 */
bool
PauseDatabaseOnInboundPgBouncers(char *databaseName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, "PAUSE %s", quote_identifier(databaseName));

	return ExecuteCommandOnAllInboundPgBouncers(command->data);
}


/*
 * ResumeDatabaseOnInboundPgBouncers resumes traffic to a database on all
 * inbound pgbouncers.
 */
bool
ResumeDatabaseOnInboundPgBouncers(char *databaseName)
{
	StringInfo command = makeStringInfo();

	appendStringInfo(command, "RESUME %s", quote_identifier(databaseName));

	return ExecuteCommandOnAllInboundPgBouncers(command->data);
}


/*
 * ExecuteCommandOnAllInboundPgBouncers executes a command on all pgbouncers.
 */
static bool
ExecuteCommandOnAllInboundPgBouncers(char *command)
{
	bool success = true;

	LockInboundPgBouncerState(LW_SHARED);

	List *connectionList = NIL;

	for (int pgBouncerId = 0; pgBouncerId < PgBouncerInboundProcs; pgBouncerId++)
	{
		PgBouncerProcess *proc = &(PgBouncerState->inboundPgBouncers[pgBouncerId]);

		/* open an admin connection */
		int connectionFlags = 0;
		char *databaseName = "pgbouncer";
		MultiConnection *conn = StartNodeUserDatabaseConnection(connectionFlags,
																proc->unixDomainSocketDir,
																PgBouncerInboundPort,
																NULL, databaseName);


		connectionList = lappend(connectionList, conn);
	}

	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, command);
		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
			success = false;
		}
	}

	/* get results */
	foreach_ptr(connection, connectionList)
	{
		bool raiseErrors = false;
		PGresult *result = GetRemoteCommandResult(connection, raiseErrors);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			success = false;
		}

		PQclear(result);
		ClearResults(connection, raiseErrors);

		CloseConnection(connection);
	}

	UnlockInboundPgBouncerState();

	return success;
}


/*
 * TriggerPgBouncerReconfigureIfNeeded sets the latch on the pgbouncer manager to
 * trigger a reconfigure.
 */
void
TriggerPgBouncerReconfigureIfNeeded(void)
{
	if (!ReconfigurePgBouncersOnCommit)
	{
		return;
	}

	if (PgBouncerState->pgBouncerManagerLatch != NULL)
	{
		SetLatch(PgBouncerState->pgBouncerManagerLatch);
	}

	ReconfigurePgBouncersOnCommit = false;
}
