/*-------------------------------------------------------------------------
 *
 * shared_library_init.c
 *	  Functionality related to the initialization of the Citus extension.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "fmgr.h"
#include "miscadmin.h"

#include "citus_version.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "distributed/backend_data.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/distributed_deadlock_detection.h"
#include "distributed/local_executor.h"
#include "distributed/maintenanced.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/run_from_same_connection.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/query_stats.h"
#include "distributed/remote_commands.h"
#include "distributed/shared_library_init.h"
#include "distributed/statistics_collection.h"
#include "distributed/subplan_execution.h"
#include "distributed/task_tracker.h"
#include "distributed/transaction_management.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"
#include "port/atomics.h"
#include "postmaster/postmaster.h"
#include "optimizer/planner.h"
#include "optimizer/paths.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/varlena.h"

/* marks shared object as one loadable by the postgres version compiled against */
PG_MODULE_MAGIC;

static char *CitusVersion = CITUS_VERSION;

void _PG_init(void);

static void ResizeStackToMaximumDepth(void);
static void multi_log_hook(ErrorData *edata);
static void CreateRequiredDirectories(void);
static void RegisterCitusConfigVariables(void);
static bool ErrorIfNotASuitableDeadlockFactor(double *newval, void **extra,
											  GucSource source);
static void NormalizeWorkerListPath(void);
static bool NodeConninfoGucCheckHook(char **newval, void **extra, GucSource source);
static void NodeConninfoGucAssignHook(const char *newval, void *extra);
static bool StatisticsCollectionGucCheckHook(bool *newval, void **extra, GucSource
											 source);

/* static variable to hold value of deprecated GUC variable */
static bool ExpireCachedShards = false;
static int LargeTableShardCount = 0;
static int CitusSSLMode = 0;

/* *INDENT-OFF* */
/* GUC enum definitions */
static const struct config_enum_entry propagate_set_commands_options[] = {
	{"none", PROPSETCMD_NONE, false},
	{"local", PROPSETCMD_LOCAL, false},
	{NULL, 0, false}
};


static const struct config_enum_entry task_assignment_policy_options[] = {
	{ "greedy", TASK_ASSIGNMENT_GREEDY, false },
	{ "first-replica", TASK_ASSIGNMENT_FIRST_REPLICA, false },
	{ "round-robin", TASK_ASSIGNMENT_ROUND_ROBIN, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry replication_model_options[] = {
	{ "statement", REPLICATION_MODEL_COORDINATOR, false },
	{ "streaming", REPLICATION_MODEL_STREAMING, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry task_executor_type_options[] = {
	{ "adaptive", MULTI_EXECUTOR_ADAPTIVE, false },
	{ "real-time", MULTI_EXECUTOR_ADAPTIVE, false }, /* ignore real-time executor, always use adaptive */
	{ "task-tracker", MULTI_EXECUTOR_TASK_TRACKER, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry shard_placement_policy_options[] = {
	{ "local-node-first", SHARD_PLACEMENT_LOCAL_NODE_FIRST, false },
	{ "round-robin", SHARD_PLACEMENT_ROUND_ROBIN, false },
	{ "random", SHARD_PLACEMENT_RANDOM, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry use_secondary_nodes_options[] = {
	{ "never", USE_SECONDARY_NODES_NEVER, false },
	{ "always", USE_SECONDARY_NODES_ALWAYS, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry shard_commit_protocol_options[] = {
	{ "1pc", COMMIT_PROTOCOL_1PC, false },
	{ "2pc", COMMIT_PROTOCOL_2PC, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry multi_task_query_log_level_options[] = {
	{ "off", MULTI_TASK_QUERY_INFO_OFF, false },
	{ "debug", DEBUG2, false },
	{ "log", LOG, false },
	{ "notice", NOTICE, false },
	{ "warning", WARNING, false },
	{ "error", ERROR, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry multi_shard_modify_connection_options[] = {
	{ "parallel", PARALLEL_CONNECTION, false },
	{ "sequential", SEQUENTIAL_CONNECTION, false },
	{ NULL, 0, false }
};

/* *INDENT-ON* */


/* shared library initialization function */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("Citus can only be loaded via shared_preload_libraries"),
						errhint("Add citus to shared_preload_libraries configuration "
								"variable in postgresql.conf in master and workers. Note "
								"that citus should be at the beginning of "
								"shared_preload_libraries.")));
	}

	/*
	 * Perform checks before registering any hooks, to avoid erroring out in a
	 * partial state.
	 *
	 * In many cases (e.g. planner and utility hook, to run inside
	 * pg_stat_statements et. al.) we have to be loaded before other hooks
	 * (thus as the innermost/last running hook) to be able to do our
	 * duties. For simplicity insist that all hooks are previously unused.
	 */
	if (planner_hook != NULL || ProcessUtility_hook != NULL ||
		ExecutorStart_hook != NULL || ExecutorRun_hook != NULL)
	{
		ereport(ERROR, (errmsg("Citus has to be loaded first"),
						errhint("Place citus at the beginning of "
								"shared_preload_libraries.")));
	}

	ResizeStackToMaximumDepth();

	/*
	 * Extend the database directory structure before continuing with
	 * initialization - one of the later steps might require them to exist.
	 * If in a sub-process (windows / EXEC_BACKEND) this already has been
	 * done.
	 */
	if (!IsUnderPostmaster)
	{
		CreateRequiredDirectories();
	}

	InitConnParams();

	/*
	 * Register Citus configuration variables. Do so before intercepting
	 * hooks or calling initialization functions, in case we want to do the
	 * latter in a configuration dependent manner.
	 */
	RegisterCitusConfigVariables();

	/* make our additional node types known */
	RegisterNodes();

	/* make our custom scan nodes known */
	RegisterCitusCustomScanMethods();

	/* intercept planner */
	planner_hook = distributed_planner;

	/* register utility hook */
	ProcessUtility_hook = multi_ProcessUtility;

	/* register for planner hook */
	set_rel_pathlist_hook = multi_relation_restriction_hook;
	set_join_pathlist_hook = multi_join_restriction_hook;
	ExecutorStart_hook = CitusExecutorStart;
	ExecutorRun_hook = CitusExecutorRun;

	/* register hook for error messages */
	emit_log_hook = multi_log_hook;

	InitializeMaintenanceDaemon();

	/* organize that task tracker is started once server is up */
	TaskTrackerRegister();

	/* initialize coordinated transaction management */
	InitializeTransactionManagement();
	InitializeBackendManagement();
	InitializeConnectionManagement();
	InitPlacementConnectionManagement();
	InitializeCitusQueryStats();

	/* enable modification of pg_catalog tables during pg_upgrade */
	if (IsBinaryUpgrade)
	{
		SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
						PGC_S_OVERRIDE);
	}
}


/*
 * Stack size increase during high memory load may cause unexpected crashes.
 * With this alloca call, we are increasing stack size explicitly, so that if
 * it is not possible to increase stack size, we will get an OOM error instead
 * of a crash.
 *
 * This function is called on backend startup. The allocated memory will
 * automatically be released at the end of the function's scope. However, we'd
 * have already expanded the stack and it wouldn't shrink back. So, in a sense,
 * per backend we're securing max_stack_depth kB's of memory on the stack upfront.
 *
 * Not all the backends require max_stack_depth kB's on the stack, so we might end
 * up with unnecessary allocations. However, the default value is 2MB, which seems
 * an acceptable trade-off. Also, allocating memory upfront may perform better
 * under some circumstances.
 */
static void
ResizeStackToMaximumDepth(void)
{
#ifndef WIN32
	volatile char *stack_resizer = NULL;
	long max_stack_depth_bytes = max_stack_depth * 1024L;

	stack_resizer = alloca(max_stack_depth_bytes);

	/*
	 * Different architectures might have different directions while
	 * growing the stack. So, touch both ends.
	 */
	stack_resizer[0] = 0;
	stack_resizer[max_stack_depth_bytes - 1] = 0;

	/*
	 * Passing the address to external function also prevents the function
	 * from being optimized away, and the debug elog can also help with
	 * diagnosis if needed.
	 */
	elog(DEBUG5, "entry stack is at %p, increased to %p, the top and bottom values of "
				 "the stack is %d and %d", &stack_resizer[0],
		 &stack_resizer[max_stack_depth_bytes - 1],
		 stack_resizer[max_stack_depth_bytes - 1], stack_resizer[0]);

#endif
}


/*
 * multi_log_hook intercepts postgres log commands. We use this to override
 * postgres error messages when they're not specific enough for the users.
 */
static void
multi_log_hook(ErrorData *edata)
{
	/*
	 * Show the user a meaningful error message when a backend is cancelled
	 * by the distributed deadlock detection.
	 */
	if (edata->elevel == ERROR && edata->sqlerrcode == ERRCODE_QUERY_CANCELED &&
		MyBackendGotCancelledDueToDeadlock())
	{
		edata->sqlerrcode = ERRCODE_T_R_DEADLOCK_DETECTED;
		edata->message = "canceling the transaction since it was "
						 "involved in a distributed deadlock";
	}
}


/*
 * StartupCitusBackend initializes per-backend infrastructure, and is called
 * the first time citus is used in a database.
 *
 * NB: All code here has to be able to cope with this routine being called
 * multiple times in the same backend.  This will e.g. happen when the
 * extension is created or upgraded.
 */
void
StartupCitusBackend(void)
{
	InitializeMaintenanceDaemonBackend();
	InitializeBackendData();
}


/*
 * CreateRequiredDirectories - Create directories required for Citus to
 * function.
 *
 * These used to be created by initdb, but that's not possible anymore.
 */
static void
CreateRequiredDirectories(void)
{
	int dirNo = 0;
	const char *subdirs[] = {
		"pg_foreign_file",
		"pg_foreign_file/cached",
		"base/" PG_JOB_CACHE_DIR
	};

	for (dirNo = 0; dirNo < lengthof(subdirs); dirNo++)
	{
		int ret = mkdir(subdirs[dirNo], S_IRWXU);

		if (ret != 0 && errno != EEXIST)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not create directory \"%s\": %m",
								   subdirs[dirNo])));
		}
	}
}


/* Register Citus configuration variables. */
static void
RegisterCitusConfigVariables(void)
{
	DefineCustomIntVariable(
		"citus.node_connection_timeout",
		gettext_noop("Sets the maximum duration to connect to worker nodes."),
		NULL,
		&NodeConnectionTimeout,
		5000, 10, 60 * 60 * 1000,
		PGC_USERSET,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	/* keeping temporarily for updates from pre-6.0 versions */
	DefineCustomStringVariable(
		"citus.worker_list_file",
		gettext_noop("Sets the server's \"worker_list\" configuration file."),
		NULL,
		&WorkerListFileName,
		NULL,
		PGC_POSTMASTER,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);
	NormalizeWorkerListPath();

	DefineCustomIntVariable(
		"citus.sslmode",
		gettext_noop("This variable has been deprecated. Use the citus.node_conninfo "
					 "GUC instead."),
		NULL,
		&CitusSSLMode,
		0, 0, 32,
		PGC_POSTMASTER,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.binary_master_copy_format",
		gettext_noop("Use the binary master copy format."),
		gettext_noop("When enabled, data is copied from workers to the master "
					 "in PostgreSQL's binary serialization format."),
		&BinaryMasterCopyFormat,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.binary_worker_copy_format",
		gettext_noop("Use the binary worker copy format."),
		gettext_noop("When enabled, data is copied from workers to workers "
					 "in PostgreSQL's binary serialization format when "
					 "joining large tables."),
		&BinaryWorkerCopyFormat,
		false,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.expire_cached_shards",
		gettext_noop("This GUC variable has been deprecated."),
		NULL,
		&ExpireCachedShards,
		false,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_local_execution",
		gettext_noop("Enables queries on shards that are local to the current node "
					 "to be planned and executed locally."),
		NULL,
		&EnableLocalExecution,
		true,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_single_hash_repartition_joins",
		gettext_noop("Enables single hash repartitioning between hash "
					 "distributed tables"),
		NULL,
		&EnableSingleHashRepartitioning,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_fast_path_router_planner",
		gettext_noop("Enables fast path router planner"),
		NULL,
		&EnableFastPathRouterPlanner,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.override_table_visibility",
		gettext_noop("Enables replacing occurencens of pg_catalog.pg_table_visible() "
					 "with pg_catalog.citus_table_visible()"),
		gettext_noop("When enabled, shards on the Citus MX worker (data) nodes would be "
					 "filtered out by many psql commands to provide better user "
					 "experience."),
		&OverrideTableVisibility,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enforce_foreign_key_restrictions",
		gettext_noop("Enforce restrictions while querying distributed/reference "
					 "tables with foreign keys"),
		gettext_noop("When enabled, cascading modifications from reference tables "
					 "to distributed tables are traced and acted accordingly "
					 "to avoid creating distributed deadlocks and ensure correctness."),
		&EnforceForeignKeyRestrictions,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);


	DefineCustomBoolVariable(
		"citus.subquery_pushdown",
		gettext_noop("Enables supported subquery pushdown to workers."),
		NULL,
		&SubqueryPushdown,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_multi_join_order",
		gettext_noop("Logs the distributed join order to the server log."),
		gettext_noop("We use this private configuration entry as a debugging aid. "
					 "If enabled, we print the distributed join order."),
		&LogMultiJoinOrder,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_remote_commands",
		gettext_noop("Log queries sent to other nodes in the server log"),
		NULL,
		&LogRemoteCommands,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_local_commands",
		gettext_noop("Log queries that are executed locally, can be overriden by "
					 "citus.log_remote_commands"),
		NULL,
		&LogLocalCommands,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_distributed_deadlock_detection",
		gettext_noop("Log distributed deadlock detection related processing in "
					 "the server log"),
		NULL,
		&LogDistributedDeadlockDetection,
		false,
		PGC_SIGHUP,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.explain_distributed_queries",
		gettext_noop("Enables Explain for distributed queries."),
		gettext_noop("When enabled, the Explain command shows remote and local "
					 "plans when used with a distributed query. It is enabled "
					 "by default, but can be disabled for regression tests."),
		&ExplainDistributedQueries,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.explain_all_tasks",
		gettext_noop("Enables showing output for all tasks in Explain."),
		gettext_noop("The Explain command for distributed queries shows "
					 "the remote plan for a single task by default. When "
					 "this configuration entry is enabled, the plan for "
					 "all tasks is shown, but the Explain takes longer."),
		&ExplainAllTasks,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.all_modifications_commutative",
		gettext_noop("Bypasses commutativity checks when enabled"),
		NULL,
		&AllModificationsCommutative,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomRealVariable(
		"citus.distributed_deadlock_detection_factor",
		gettext_noop("Sets the time to wait before checking for distributed "
					 "deadlocks. Postgres' deadlock_timeout setting is "
					 "multiplied with the value. If the value is set to"
					 "1000, distributed deadlock detection is disabled."),
		NULL,
		&DistributedDeadlockDetectionTimeoutFactor,
		2.0, -1.0, 1000.0,
		PGC_SIGHUP,
		0,
		ErrorIfNotASuitableDeadlockFactor, NULL, NULL);

	DefineCustomIntVariable(
		"citus.recover_2pc_interval",
		gettext_noop("Sets the time to wait between recovering 2PCs."),
		gettext_noop("2PC transaction recovery needs to run every so often "
					 "to clean up records in pg_dist_transaction and "
					 "potentially roll failed 2PCs forward. This setting "
					 "determines how often recovery should run, "
					 "use -1 to disable."),
		&Recover2PCInterval,
		60000, -1, 7 * 24 * 3600 * 1000,
		PGC_SIGHUP,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.metadata_sync_interval",
		gettext_noop("Sets the time to wait between metadata syncs."),
		gettext_noop("metadata sync needs to run every so often "
					 "to synchronize metadata to metadata nodes "
					 "that are out of sync."),
		&MetadataSyncInterval,
		60000, 1, 7 * 24 * 3600 * 1000,
		PGC_SIGHUP,
		GUC_UNIT_MS | GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.metadata_sync_retry_interval",
		gettext_noop("Sets the interval to retry failed metadata syncs."),
		gettext_noop("metadata sync needs to run every so often "
					 "to synchronize metadata to metadata nodes "
					 "that are out of sync."),
		&MetadataSyncRetryInterval,
		5000, 1, 7 * 24 * 3600 * 1000,
		PGC_SIGHUP,
		GUC_UNIT_MS | GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.select_opens_transaction_block",
		gettext_noop("Open transaction blocks for SELECT commands"),
		gettext_noop("When enabled, Citus will always send a BEGIN to workers when "
					 "running a distributed SELECT in a transaction block (the "
					 "default). When disabled, Citus will only send BEGIN before "
					 "the first write or other operation that requires a distributed "
					 "transaction, meaning the SELECT on the worker commits "
					 "immediately, releasing any locks and apply any changes made "
					 "through function calls even if the distributed transaction "
					 "aborts."),
		&SelectOpensTransactionBlock,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.function_opens_transaction_block",
		gettext_noop("Open transaction blocks for function calls"),
		gettext_noop("When enabled, Citus will always send a BEGIN to workers when "
					 "running distributed queres in a function. When disabled, the "
					 "queries may be committed immediately after the statemnent "
					 "completes. Disabling this flag is dangerous, it is only provided "
					 "for backwards compatibility with pre-8.2 behaviour."),
		&FunctionOpensTransactionBlock,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.force_max_query_parallelization",
		gettext_noop("Open as many connections as possible to maximize query "
					 "parallelization"),
		gettext_noop("When enabled, Citus will force the executor to use "
					 "as many connections as possible while executing a "
					 "parallel distributed query. If not enabled, the executor"
					 "might choose to use less connections to optimize overall "
					 "query execution throughput. Internally, setting this true "
					 "will end up with using one connection per task."),
		&ForceMaxQueryParallelization,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.executor_slow_start_interval",
		gettext_noop("Time to wait between opening connections to the same worker node"),
		gettext_noop("When the individual tasks of a multi-shard query take very "
					 "little time, they can often be finished over a single (often "
					 "already cached) connection. To avoid redundantly opening "
					 "additional connections, the executor waits between connection "
					 "attempts for the configured number of milliseconds. At the end "
					 "of the interval, it increases the number of connections it is "
					 "allowed to open next time."),
		&ExecutorSlowStartInterval,
		10, 0, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_MS | GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_deadlock_prevention",
		gettext_noop("Avoids deadlocks by preventing concurrent multi-shard commands"),
		gettext_noop("Multi-shard modifications such as UPDATE, DELETE, and "
					 "INSERT...SELECT are typically executed in parallel. If multiple "
					 "such commands run concurrently and affect the same rows, then "
					 "they are likely to deadlock. When enabled, this flag prevents "
					 "multi-shard modifications from running concurrently when they "
					 "affect the same shards in order to prevent deadlocks."),
		&EnableDeadlockPrevention,
		true,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_ddl_propagation",
		gettext_noop("Enables propagating DDL statements to worker shards"),
		NULL,
		&EnableDDLPropagation,
		true,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_object_propagation",
		gettext_noop("Enables propagating object creation for more complex objects, "
					 "schema's will always be created"),
		NULL,
		&EnableDependencyCreation,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_create_type_propagation",
		gettext_noop("Enables propagating of CREATE TYPE statements to workers"),
		NULL,
		&EnableCreateTypePropagation,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.propagate_set_commands",
		gettext_noop("Sets which SET commands are propagated to workers."),
		NULL,
		&PropagateSetCommands,
		PROPSETCMD_NONE,
		propagate_set_commands_options,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_router_execution",
		gettext_noop("Enables router execution"),
		NULL,
		&EnableRouterExecution,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.shard_count",
		gettext_noop("Sets the number of shards for a new hash-partitioned table"
					 "created with create_distributed_table()."),
		NULL,
		&ShardCount,
		32, 1, 64000,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.shard_replication_factor",
		gettext_noop("Sets the replication factor for shards."),
		gettext_noop("Shards are replicated across nodes according to this "
					 "replication factor. Note that shards read this "
					 "configuration value at sharded table creation time, "
					 "and later reuse the initially read value."),
		&ShardReplicationFactor,
		1, 1, 100,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.shard_max_size",
		gettext_noop("Sets the maximum size a shard will grow before it gets split."),
		gettext_noop("Shards store table and file data. When the source "
					 "file's size for one shard exceeds this configuration "
					 "value, the database ensures that either a new shard "
					 "gets created, or the current one gets split. Note that "
					 "shards read this configuration value at sharded table "
					 "creation time, and later reuse the initially read value."),
		&ShardMaxSize,
		1048576, 256, INT_MAX, /* max allowed size not set to MAX_KILOBYTES on purpose */
		PGC_USERSET,
		GUC_UNIT_KB,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.sort_returning",
		gettext_noop("Sorts the RETURNING clause to get consistent test output"),
		gettext_noop("This feature is not intended for users. It is developed "
					 "to get consistent regression test outputs. When enabled, "
					 "the RETURNING clause returns the tuples sorted. The sort "
					 "is done for all the entries, starting from the first one."
					 "Finally, the sorting is done in ASC order."),
		&SortReturning,
		false,
		PGC_SUSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_intermediate_result_size",
		gettext_noop("Sets the maximum size of the intermediate results in KB for "
					 "CTEs and complex subqueries."),
		NULL,
		&MaxIntermediateResult,
		1048576, -1, MAX_KILOBYTES,
		PGC_USERSET,
		GUC_UNIT_KB,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_adaptive_executor_pool_size",
		gettext_noop("Sets the maximum number of connections per worker node used by "
					 "the adaptive executor to execute a multi-shard command"),
		gettext_noop("The adaptive executor may open multiple connections per worker "
					 "node when running multi-shard commands to parallelize the command "
					 "across multiple cores on the worker. This setting specifies the "
					 "maximum number of connections it will open. The number of "
					 "connections is also bounded by the number of shards on the node. "
					 "This setting can be used to reduce the memory usage of a query "
					 "and allow a higher degree of concurrency when concurrent "
					 "multi-shard queries open too many connections to a worker."),
		&MaxAdaptiveExecutorPoolSize,
		16, 1, INT_MAX,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_worker_nodes_tracked",
		gettext_noop("Sets the maximum number of worker nodes that are tracked."),
		gettext_noop("Worker nodes' network locations, their membership and "
					 "health status are tracked in a shared hash table on "
					 "the master node. This configuration value limits the "
					 "size of the hash table, and consequently the maximum "
					 "number of worker nodes that can be tracked."),
		&MaxWorkerNodesTracked,
		2048, 8, INT_MAX,
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.remote_task_check_interval",
		gettext_noop("Sets the frequency at which we check job statuses."),
		gettext_noop("The master node assigns tasks to workers nodes, and "
					 "then regularly checks with them about each task's "
					 "progress. This configuration value sets the time "
					 "interval between two consequent checks."),
		&RemoteTaskCheckInterval,
		10, 1, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.task_tracker_delay",
		gettext_noop("Task tracker sleep time between task management rounds."),
		gettext_noop("The task tracker process wakes up regularly, walks over "
					 "all tasks assigned to it, and schedules and executes these "
					 "tasks. Then, the task tracker sleeps for a time period "
					 "before walking over these tasks again. This configuration "
					 "value determines the length of that sleeping period."),
		&TaskTrackerDelay,
		200, 1, 100000,
		PGC_SIGHUP,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_cached_conns_per_worker",
		gettext_noop("Sets the maximum number of connections to cache per worker."),
		gettext_noop("Each backend opens connections to the workers to query the "
					 "shards. At the end of the transaction, the configurated number "
					 "of connections is kept open to speed up subsequent commands. "
					 "Increasing this value will reduce the latency of multi-shard "
					 "queries, but increases overhead on the workers"),
		&MaxCachedConnectionsPerWorker,
		1, 0, INT_MAX,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_assign_task_batch_size",
		gettext_noop("Sets the maximum number of tasks to assign per round."),
		gettext_noop("The master node synchronously assigns tasks to workers in "
					 "batches. Bigger batches allow for faster task assignment, "
					 "but it may take longer for all workers to get tasks "
					 "if the number of workers is large. This configuration "
					 "value controls the maximum batch size."),
		&MaxAssignTaskBatchSize,
		64, 1, INT_MAX,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_tracked_tasks_per_node",
		gettext_noop("Sets the maximum number of tracked tasks per node."),
		gettext_noop("The task tracker processes keeps all assigned tasks in "
					 "a shared hash table, and schedules and executes these "
					 "tasks as appropriate. This configuration value limits "
					 "the size of the hash table, and therefore the maximum "
					 "number of tasks that can be tracked at any given time."),
		&MaxTrackedTasksPerNode,
		1024, 8, INT_MAX,
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_running_tasks_per_node",
		gettext_noop("Sets the maximum number of tasks to run concurrently per node."),
		gettext_noop("The task tracker process schedules and executes the tasks "
					 "assigned to it as appropriate. This configuration value "
					 "sets the maximum number of tasks to execute concurrently "
					 "on one node at any given time."),
		&MaxRunningTasksPerNode,
		8, 1, INT_MAX,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.partition_buffer_size",
		gettext_noop("Sets the buffer size to use for partition operations."),
		gettext_noop("Worker nodes allow for table data to be repartitioned "
					 "into multiple text files, much like Hadoop's Map "
					 "command. This configuration value sets the buffer size "
					 "to use per partition operation. After the buffer fills "
					 "up, we flush the repartitioned data into text files."),
		&PartitionBufferSize,
		8192, 0, (INT_MAX / 1024), /* result stored in int variable */
		PGC_USERSET,
		GUC_UNIT_KB,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.large_table_shard_count",
		gettext_noop("This variable has been deprecated."),
		gettext_noop("Consider reference tables instead"),
		&LargeTableShardCount,
		4, 1, 10000,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.limit_clause_row_fetch_count",
		gettext_noop("Number of rows to fetch per task for limit clause optimization."),
		gettext_noop("Select queries get partitioned and executed as smaller "
					 "tasks. In some cases, select queries with limit clauses "
					 "may need to fetch all rows from each task to generate "
					 "results. In those cases, and where an approximation would "
					 "produce meaningful results, this configuration value sets "
					 "the number of rows to fetch from each task."),
		&LimitClauseRowFetchCount,
		-1, -1, INT_MAX,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomRealVariable(
		"citus.count_distinct_error_rate",
		gettext_noop("Desired error rate when calculating count(distinct) "
					 "approximates using the postgresql-hll extension. "
					 "0.0 disables approximations for count(distinct); 1.0 "
					 "provides no guarantees about the accuracy of results."),
		NULL,
		&CountDistinctErrorRate,
		0.0, 0.0, 1.0,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.multi_shard_commit_protocol",
		gettext_noop("Sets the commit protocol for commands modifying multiple shards."),
		gettext_noop("When a failure occurs during commands that modify multiple "
					 "shards, two-phase commit is required to ensure data is never lost "
					 "and this is the default. However, changing to 1pc may give small "
					 "performance benefits."),
		&MultiShardCommitProtocol,
		COMMIT_PROTOCOL_2PC,
		shard_commit_protocol_options,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.single_shard_commit_protocol",
		gettext_noop(
			"Sets the commit protocol for commands modifying a single shards with multiple replicas."),
		gettext_noop("When a failure occurs during commands that modify multiple "
					 "replicas, two-phase commit is required to ensure data is never lost "
					 "and this is the default. However, changing to 1pc may give small "
					 "performance benefits."),
		&SingleShardCommitProtocol,
		COMMIT_PROTOCOL_2PC,
		shard_commit_protocol_options,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.task_assignment_policy",
		gettext_noop("Sets the policy to use when assigning tasks to worker nodes."),
		gettext_noop("The master node assigns tasks to worker nodes based on shard "
					 "locations. This configuration value specifies the policy to "
					 "use when making these assignments. The greedy policy aims to "
					 "evenly distribute tasks across worker nodes, first-replica just "
					 "assigns tasks in the order shard placements were created, "
					 "and the round-robin policy assigns tasks to worker nodes in "
					 "a round-robin fashion."),
		&TaskAssignmentPolicy,
		TASK_ASSIGNMENT_GREEDY,
		task_assignment_policy_options,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.replication_model",
		gettext_noop("Sets the replication model to be used for distributed tables."),
		gettext_noop("Depending upon the execution environment, statement- or streaming-"
					 "based replication modes may be employed. Though most Citus deploy-"
					 "ments will simply use statement replication, hosted and MX-style"
					 "deployments should set this parameter to 'streaming'."),
		&ReplicationModel,
		REPLICATION_MODEL_COORDINATOR,
		replication_model_options,
		PGC_SUSET,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.task_executor_type",
		gettext_noop("Sets the executor type to be used for distributed queries."),
		gettext_noop("The master node chooses between two different executor types "
					 "when executing a distributed query.The real-time executor is "
					 "optimal for simple key-value lookup queries and queries that "
					 "involve aggregations and/or co-located joins on multiple shards. "
					 "The task-tracker executor is optimal for long-running, complex "
					 "queries that touch thousands of shards and/or that involve table "
					 "repartitioning."),
		&TaskExecutorType,
		MULTI_EXECUTOR_ADAPTIVE,
		task_executor_type_options,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_repartition_joins",
		gettext_noop("Allows Citus to use task-tracker executor when necessary."),
		NULL,
		&EnableRepartitionJoins,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.shard_placement_policy",
		gettext_noop("Sets the policy to use when choosing nodes for shard placement."),
		gettext_noop("The master node chooses which worker nodes to place new shards "
					 "on. This configuration value specifies the policy to use when "
					 "selecting these nodes. The local-node-first policy places the "
					 "first replica on the client node and chooses others randomly. "
					 "The round-robin policy aims to distribute shards evenly across "
					 "the cluster by selecting nodes in a round-robin fashion."
					 "The random policy picks all workers randomly."),
		&ShardPlacementPolicy,
		SHARD_PLACEMENT_ROUND_ROBIN, shard_placement_policy_options,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.use_secondary_nodes",
		gettext_noop("Sets the policy to use when choosing nodes for SELECT queries."),
		NULL,
		&ReadFromSecondaries,
		USE_SECONDARY_NODES_NEVER, use_secondary_nodes_options,
		PGC_SU_BACKEND,
		0,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.multi_task_query_log_level",
		gettext_noop("Sets the level of multi task query execution log messages"),
		NULL,
		&MultiTaskQueryLogLevel,
		MULTI_TASK_QUERY_INFO_OFF, multi_task_query_log_level_options,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.multi_shard_modify_mode",
		gettext_noop("Sets the connection type for multi shard modify queries"),
		NULL,
		&MultiShardConnectionType,
		PARALLEL_CONNECTION, multi_shard_modify_connection_options,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.version",
		gettext_noop("Shows the Citus library version"),
		NULL,
		&CitusVersion,
		CITUS_VERSION,
		PGC_INTERNAL,
		0,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.cluster_name",
		gettext_noop("Which cluster this node is a part of"),
		NULL,
		&CurrentCluster,
		"default",
		PGC_SU_BACKEND,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.writable_standby_coordinator",
		gettext_noop("Enables simple DML via a streaming replica of the coordinator"),
		NULL,
		&WritableStandbyCoordinator,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_version_checks",
		gettext_noop("Enables version checks during CREATE/ALTER EXTENSION commands"),
		NULL,
		&EnableVersionChecks,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_unique_job_ids",
		gettext_noop("Enables unique job IDs by prepending the local process ID and "
					 "group ID. This should usually be enabled, but can be disabled "
					 "for repeatable output in regression tests."),
		NULL,
		&EnableUniqueJobIds,
		true,
		PGC_USERSET,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.next_shard_id",
		gettext_noop("Set the next shard ID to use in shard creation."),
		gettext_noop("Shard IDs are normally generated using a sequence. If "
					 "next_shard_id is set to a non-zero value, shard IDs will "
					 "instead be generated by incrementing from the value of "
					 "this GUC and this will be reflected in the GUC. This is "
					 "mainly useful to ensure consistent shard IDs when running "
					 "tests in parallel."),
		&NextShardId,
		0, 0, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.next_placement_id",
		gettext_noop("Set the next placement ID to use in placement creation."),
		gettext_noop("Placement IDs are normally generated using a sequence. If "
					 "next_placement_id is set to a non-zero value, placement IDs will "
					 "instead be generated by incrementing from the value of "
					 "this GUC and this will be reflected in the GUC. This is "
					 "mainly useful to ensure consistent placement IDs when running "
					 "tests in parallel."),
		&NextPlacementId,
		0, 0, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_task_string_size",
		gettext_noop("Sets the maximum size (in bytes) of a worker task call string."),
		gettext_noop("Active worker tasks' are tracked in a shared hash table "
					 "on the master node. This configuration value limits the "
					 "maximum size of an individual worker task, and "
					 "affects the size of pre-allocated shared memory."),
		&MaxTaskStringSize,
		12288, 8192, 65536,
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_statistics_collection",
		gettext_noop("Enables sending basic usage statistics to Citus."),
		gettext_noop("Citus uploads daily anonymous usage reports containing "
					 "rounded node count, shard size, distributed table count, "
					 "and operating system name. This configuration value controls "
					 "whether these reports are sent."),
		&EnableStatisticsCollection,
#if defined(HAVE_LIBCURL) && defined(ENABLE_CITUS_STATISTICS_COLLECTION)
		true,
#else
		false,
#endif
		PGC_SIGHUP,
		GUC_SUPERUSER_ONLY,
		&StatisticsCollectionGucCheckHook,
		NULL, NULL);

	DefineCustomStringVariable(
		"citus.node_conninfo",
		gettext_noop("Sets parameters used for outbound connections."),
		NULL,
		&NodeConninfo,
#ifdef USE_SSL
		"sslmode=require",
#else
		"sslmode=prefer",
#endif
		PGC_SIGHUP,
		GUC_SUPERUSER_ONLY,
		NodeConninfoGucCheckHook,
		NodeConninfoGucAssignHook,
		NULL);

	DefineCustomIntVariable(
		"citus.isolation_test_session_remote_process_id",
		NULL,
		NULL,
		&IsolationTestSessionRemoteProcessID,
		-1, -1, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.isolation_test_session_process_id",
		NULL,
		NULL,
		&IsolationTestSessionProcessID,
		-1, -1, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	NormalizeWorkerListPath();


	/* warn about config items in the citus namespace that are not registered above */
	EmitWarningsOnPlaceholders("citus");
}


/*
 * We don't want to allow values less than 1.0. However, we define -1 as the value to disable
 * distributed deadlock checking. Here we enforce our special constraint.
 */
static bool
ErrorIfNotASuitableDeadlockFactor(double *newval, void **extra, GucSource source)
{
	if (*newval <= 1.0 && *newval != -1.0)
	{
		ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg(
							  "citus.distributed_deadlock_detection_factor cannot be less than 1. "
							  "To disable distributed deadlock detection set the value to -1.")));

		return false;
	}

	return true;
}


/*
 * NormalizeWorkerListPath converts the path configured via
 * citus.worker_list_file into an absolute path, falling back to the default
 * value if necessary. The previous value of the config variable is
 * overwritten with the normalized value.
 *
 * NB: This has to be called before ChangeToDataDir() is called as otherwise
 * the relative paths won't make much sense to the user anymore.
 */
static void
NormalizeWorkerListPath(void)
{
	char *absoluteFileName = NULL;

	if (WorkerListFileName != NULL)
	{
		absoluteFileName = make_absolute_path(WorkerListFileName);
	}
	else if (DataDir != NULL)
	{
		absoluteFileName = malloc(strlen(DataDir) + strlen(WORKER_LIST_FILENAME) + 2);
		if (absoluteFileName == NULL)
		{
			ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("out of memory")));
		}

		sprintf(absoluteFileName, "%s/%s", DataDir, WORKER_LIST_FILENAME);
	}
	else
	{
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("%s does not know where to find the \"worker_list_file\" "
							   "configuration file.\n"
							   "This can be specified as \"citus.worker_list_file\" in "
							   "\"%s\", or by the -D invocation option, or by the PGDATA "
							   "environment variable.\n", progname, ConfigFileName)));
	}

	SetConfigOption("citus.worker_list_file", absoluteFileName, PGC_POSTMASTER,
					PGC_S_OVERRIDE);
	free(absoluteFileName);
}


/*
 * NodeConninfoGucCheckHook ensures conninfo settings are in the expected form
 * and that the keywords of all non-null settings are on a whitelist devised to
 * keep users from setting options that may result in confusion.
 */
static bool
NodeConninfoGucCheckHook(char **newval, void **extra, GucSource source)
{
	/* this array _must_ be kept in an order usable by bsearch */
	const char *whitelist[] = {
		"application_name",
		"connect_timeout",
			#if defined(ENABLE_GSS) && defined(ENABLE_SSPI)
		"gsslib",
			#endif
		"keepalives",
		"keepalives_count",
		"keepalives_idle",
		"keepalives_interval",
			#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
		"krbsrvname",
			#endif
		"sslcompression",
		"sslcrl",
		"sslmode",
		"sslrootcert"
	};
	char *errorMsg = NULL;
	bool conninfoValid = CheckConninfo(*newval, whitelist, lengthof(whitelist),
									   &errorMsg);

	if (!conninfoValid)
	{
		GUC_check_errdetail("%s", errorMsg);
	}

	return conninfoValid;
}


/*
 * NodeConninfoGucAssignHook is the assignment hook for the node_conninfo GUC
 * variable. Though this GUC is a "string", we actually parse it as a non-URI
 * PQconninfo key/value setting, storing the resultant PQconninfoOption values
 * using the public functions in connection_configuration.c.
 */
static void
NodeConninfoGucAssignHook(const char *newval, void *extra)
{
	PQconninfoOption *optionArray = NULL;
	PQconninfoOption *option = NULL;

	if (newval == NULL)
	{
		newval = "";
	}

	optionArray = PQconninfoParse(newval, NULL);
	if (optionArray == NULL)
	{
		ereport(FATAL, (errmsg("cannot parse node_conninfo value"),
						errdetail("The GUC check hook should prevent "
								  "all malformed values.")));
	}

	ResetConnParams();

	for (option = optionArray; option->keyword != NULL; option++)
	{
		if (option->val == NULL || option->val[0] == '\0')
		{
			continue;
		}

		AddConnParam(option->keyword, option->val);
	}

	PQconninfoFree(optionArray);
}


static bool
StatisticsCollectionGucCheckHook(bool *newval, void **extra, GucSource source)
{
#ifdef HAVE_LIBCURL
	return true;
#else

	/* if libcurl is not installed, only accept false */
	if (*newval)
	{
		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errdetail("Citus was compiled without libcurl support.");
		return false;
	}
	else
	{
		return true;
	}
#endif
}
