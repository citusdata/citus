/*-------------------------------------------------------------------------
 *
 * shared_library_init.c
 *	  Functionality related to the initialization of the Citus extension.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "postgres.h"

/* necessary to get alloca on illumos */
#ifdef __sun
#include <alloca.h>
#endif

#include "fmgr.h"
#include "miscadmin.h"
#include "safe_lib.h"

#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_extension.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "commands/seclabel.h"
#include "common/string.h"
#include "executor/executor.h"
#include "libpq/auth.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "port/atomics.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "citus_version.h"

#include "columnar/columnar.h"

#include "distributed/adaptive_executor.h"
#include "distributed/backend_data.h"
#include "distributed/background_jobs.h"
#include "distributed/causal_clock.h"
#include "distributed/citus_depended_object.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/combine_query_planner.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/cte_inline.h"
#include "distributed/distributed_deadlock_detection.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/local_distributed_join_planner.h"
#include "distributed/local_executor.h"
#include "distributed/local_multi_copy.h"
#include "distributed/locally_reserved_shared_connections.h"
#include "distributed/log_utils.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/placement_connection.h"
#include "distributed/priority.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/query_stats.h"
#include "distributed/recursive_planning.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/repartition_executor.h"
#include "distributed/replication_origin_session_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/run_from_same_connection.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_transfer.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/shared_library_init.h"
#include "distributed/statistics_collection.h"
#include "distributed/subplan_execution.h"
#include "distributed/time_constants.h"
#include "distributed/transaction_management.h"
#include "distributed/transaction_recovery.h"
#include "distributed/utils/citus_stat_tenants.h"
#include "distributed/utils/directory.h"
#include "distributed/utils/restore_interval.h"
#include "distributed/worker_log_messages.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"

/* marks shared object as one loadable by the postgres version compiled against */
PG_MODULE_MAGIC;

ColumnarSupportsIndexAM_type extern_ColumnarSupportsIndexAM = NULL;
CompressionTypeStr_type extern_CompressionTypeStr = NULL;
IsColumnarTableAmTable_type extern_IsColumnarTableAmTable = NULL;
ReadColumnarOptions_type extern_ReadColumnarOptions = NULL;

/*
 * Define "pass-through" functions so that a SQL function defined as one of
 * these symbols in the citus module can use the definition in the columnar
 * module.
 */
#define DEFINE_COLUMNAR_PASSTHROUGH_FUNC(funcname) \
	static PGFunction CppConcat(extern_, funcname); \
	PG_FUNCTION_INFO_V1(funcname); \
	Datum funcname(PG_FUNCTION_ARGS) \
	{ \
		return CppConcat(extern_, funcname)(fcinfo); \
	}
#define INIT_COLUMNAR_SYMBOL(typename, funcname) \
	CppConcat(extern_, funcname) = \
		(typename) (void *) lookup_external_function(handle, # funcname)

#define CDC_DECODER_DYNAMIC_LIB_PATH "$libdir/citus_decoders:$libdir"

DEFINE_COLUMNAR_PASSTHROUGH_FUNC(columnar_handler)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(alter_columnar_table_set)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(alter_columnar_table_reset)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(upgrade_columnar_storage)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(downgrade_columnar_storage)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(columnar_relation_storageid)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(columnar_storage_info)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(columnar_store_memory_stats)
DEFINE_COLUMNAR_PASSTHROUGH_FUNC(test_columnar_storage_write_new_page)

#define DUMMY_REAL_TIME_EXECUTOR_ENUM_VALUE 9999999
static char *CitusVersion = CITUS_VERSION;
static char *DeprecatedEmptyString = "";
static char *MitmfifoEmptyString = "";
static bool DeprecatedDeferShardDeleteOnMove = true;
static bool DeprecatedDeferShardDeleteOnSplit = true;
static bool DeprecatedReplicateReferenceTablesOnActivate = false;

/* deprecated GUC value that should not be used anywhere outside this file */
static int ReplicationModel = REPLICATION_MODEL_STREAMING;

/* we override the application_name assign_hook and keep a pointer to the old one */
static GucStringAssignHook OldApplicationNameAssignHook = NULL;

/*
 * Flag to indicate when ApplicationNameAssignHook becomes responsible for
 * updating the global pid.
 */
static bool FinishedStartupCitusBackend = false;

static object_access_hook_type PrevObjectAccessHook = NULL;

#if PG_VERSION_NUM >= PG_VERSION_15
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

void _PG_init(void);

#if PG_VERSION_NUM >= PG_VERSION_15
static void citus_shmem_request(void);
#endif
static void CitusObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId, int
								  subId, void *arg);
static void DoInitialCleanup(void);
static void ResizeStackToMaximumDepth(void);
static void multi_log_hook(ErrorData *edata);
static bool IsSequenceOverflowError(ErrorData *edata);
static void RegisterConnectionCleanup(void);
static void RegisterExternalClientBackendCounterDecrement(void);
static void CitusCleanupConnectionsAtExit(int code, Datum arg);
static void DecrementExternalClientBackendCounterAtExit(int code, Datum arg);
static void CreateRequiredDirectories(void);
static void RegisterCitusConfigVariables(void);
static void OverridePostgresConfigProperties(void);
static bool ErrorIfNotASuitableDeadlockFactor(double *newval, void **extra,
											  GucSource source);
static bool WarnIfDeprecatedExecutorUsed(int *newval, void **extra, GucSource source);
static bool WarnIfReplicationModelIsSet(int *newval, void **extra, GucSource source);
static bool NoticeIfSubqueryPushdownEnabled(bool *newval, void **extra, GucSource source);
static bool ShowShardsForAppNamePrefixesCheckHook(char **newval, void **extra,
												  GucSource source);
static void ShowShardsForAppNamePrefixesAssignHook(const char *newval, void *extra);
static void ApplicationNameAssignHook(const char *newval, void *extra);
static void CpuPriorityAssignHook(int newval, void *extra);
static bool NodeConninfoGucCheckHook(char **newval, void **extra, GucSource source);
static void NodeConninfoGucAssignHook(const char *newval, void *extra);
static const char * MaxSharedPoolSizeGucShowHook(void);
static const char * LocalPoolSizeGucShowHook(void);
static bool StatisticsCollectionGucCheckHook(bool *newval, void **extra, GucSource
											 source);
static void CitusAuthHook(Port *port, int status);
static bool IsSuperuser(char *userName);
static void AdjustDynamicLibraryPathForCdcDecoders(void);

static ClientAuthentication_hook_type original_client_auth_hook = NULL;
static emit_log_hook_type original_emit_log_hook = NULL;

/* *INDENT-OFF* */
/* GUC enum definitions */
static const struct config_enum_entry propagate_set_commands_options[] = {
	{"none", PROPSETCMD_NONE, false},
	{"local", PROPSETCMD_LOCAL, false},
	{NULL, 0, false}
};


static const struct config_enum_entry stat_statements_track_options[] = {
	{ "none", STAT_STATEMENTS_TRACK_NONE, false },
	{ "all", STAT_STATEMENTS_TRACK_ALL, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry stat_tenants_track_options[] = {
	{ "none", STAT_TENANTS_TRACK_NONE, false },
	{ "all", STAT_TENANTS_TRACK_ALL, false },
	{ NULL, 0, false }
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
	{ "real-time", DUMMY_REAL_TIME_EXECUTOR_ENUM_VALUE, false }, /* keep it for backward comp. */
	{ "task-tracker", MULTI_EXECUTOR_ADAPTIVE, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry use_secondary_nodes_options[] = {
	{ "never", USE_SECONDARY_NODES_NEVER, false },
	{ "always", USE_SECONDARY_NODES_ALWAYS, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry coordinator_aggregation_options[] = {
	{ "disabled", COORDINATOR_AGGREGATION_DISABLED, false },
	{ "row-gather", COORDINATOR_AGGREGATION_ROW_GATHER, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry log_level_options[] = {
	{ "off", CITUS_LOG_LEVEL_OFF, false },
	{ "debug5", DEBUG5, false},
	{ "debug4", DEBUG4, false},
	{ "debug3", DEBUG3, false},
	{ "debug2", DEBUG2, false},
	{ "debug1", DEBUG1, false},
	{ "debug", DEBUG2, true},
	{ "log", LOG, false},
	{ "info", INFO, true},
	{ "notice", NOTICE, false},
	{ "warning", WARNING, false},
	{ "error", ERROR, false},
	{ NULL, 0, false}
};


static const struct config_enum_entry local_table_join_policies[] = {
	{ "never", LOCAL_JOIN_POLICY_NEVER, false},
	{ "prefer-local", LOCAL_JOIN_POLICY_PREFER_LOCAL, false},
	{ "prefer-distributed", LOCAL_JOIN_POLICY_PREFER_DISTRIBUTED, false},
	{ "auto", LOCAL_JOIN_POLICY_AUTO, false},
	{ NULL, 0, false}
};


static const struct config_enum_entry multi_shard_modify_connection_options[] = {
	{ "parallel", PARALLEL_CONNECTION, false },
	{ "sequential", SEQUENTIAL_CONNECTION, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry explain_analyze_sort_method_options[] = {
	{ "execution-time", EXPLAIN_ANALYZE_SORT_BY_TIME, false },
	{ "taskId", EXPLAIN_ANALYZE_SORT_BY_TASK_ID, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry create_object_propagation_options[] = {
	{"deferred",  CREATE_OBJECT_PROPAGATION_DEFERRED,  false},
	{"automatic", CREATE_OBJECT_PROPAGATION_AUTOMATIC, false},
	{"immediate", CREATE_OBJECT_PROPAGATION_IMMEDIATE, false},
	{NULL,        0,                                   false}
};

/*
 * This used to choose CPU priorities for GUCs. For most other integer options
 * we use the -1 value as inherit/default/unset. For CPU priorities this isn't
 * possible, because they can actually have negative values. So we need a value
 * outside of the range that's valid for priorities. But if this is only one
 * more or less than the valid values, this can also be quite confusing for
 * people that don't know the exact range of valid values.
 *
 * So, instead we opt for using an enum that contains all valid priority values
 * as strings, as well as the "inherit" string to indicate that the priority
 * value should not be changed.
 */
static const struct config_enum_entry cpu_priority_options[] = {
	{ "inherit", CPU_PRIORITY_INHERIT, false },
	{ "-20", -20, false},
	{ "-19", -19, false},
	{ "-18", -18, false},
	{ "-17", -17, false},
	{ "-16", -16, false},
	{ "-15", -15, false},
	{ "-14", -14, false},
	{ "-13", -13, false},
	{ "-12", -12, false},
	{ "-11", -11, false},
	{ "-10", -10, false},
	{ "-9", -9, false},
	{ "-8", -8, false},
	{ "-7", -7, false},
	{ "-6", -6, false},
	{ "-5", -5, false},
	{ "-4", -4, false},
	{ "-3", -3, false},
	{ "-2", -2, false},
	{ "-1", -1, false},
	{ "0", 0, false},
	{ "1", 1, false},
	{ "2", 2, false},
	{ "3", 3, false},
	{ "4", 4, false},
	{ "5", 5, false},
	{ "6", 6, false},
	{ "7", 7, false},
	{ "8", 8, false},
	{ "9", 9, false},
	{ "10", 10, false},
	{ "11", 11, false},
	{ "12", 12, false},
	{ "13", 13, false},
	{ "14", 14, false},
	{ "15", 15, false},
	{ "16", 16, false},
	{ "17", 17, false},
	{ "18", 18, false},
	{ "19", 19, false},
	{ NULL, 0, false}
};

static const struct config_enum_entry metadata_sync_mode_options[] = {
	{ "transactional", METADATA_SYNC_TRANSACTIONAL, false },
	{ "nontransactional", METADATA_SYNC_NON_TRANSACTIONAL, false },
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
	 * Register contstraint_handler hooks of safestringlib first. This way
	 * loading the extension will error out if one of these constraints are hit
	 * during load.
	 */
	set_str_constraint_handler_s(ereport_constraint_handler);
	set_mem_constraint_handler_s(ereport_constraint_handler);

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
		ExecutorStart_hook != NULL || ExecutorRun_hook != NULL ||
		ExplainOneQuery_hook != NULL)
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

	/* register for planner hook */
	set_rel_pathlist_hook = multi_relation_restriction_hook;
	get_relation_info_hook = multi_get_relation_info_hook;
	set_join_pathlist_hook = multi_join_restriction_hook;
	ExecutorStart_hook = CitusExecutorStart;
	ExecutorRun_hook = CitusExecutorRun;
	ExplainOneQuery_hook = CitusExplainOneQuery;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = CitusAttributeToEnd;

	/* register hook for error messages */
	original_emit_log_hook = emit_log_hook;
	emit_log_hook = multi_log_hook;


	/*
	 * Register hook for counting client backends that
	 * are successfully authenticated.
	 */
	original_client_auth_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = CitusAuthHook;

#if PG_VERSION_NUM >= PG_VERSION_15
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = citus_shmem_request;
#endif

	InitializeMaintenanceDaemon();
	InitializeMaintenanceDaemonForMainDb();

	/* initialize coordinated transaction management */
	InitializeTransactionManagement();
	InitializeBackendManagement();
	InitializeConnectionManagement();
	InitPlacementConnectionManagement();
	InitRelationAccessHash();
	InitializeCitusQueryStats();
	InitializeSharedConnectionStats();
	InitializeLocallyReservedSharedConnections();
	InitializeClusterClockMem();

	/*
	 * Adjust the Dynamic Library Path to prepend citus_decodes to the dynamic
	 * library path. This is needed to make sure that the citus decoders are
	 * loaded before the default decoders for CDC.
	 */
	if (EnableChangeDataCapture)
	{
		AdjustDynamicLibraryPathForCdcDecoders();
	}


	/* initialize shard split shared memory handle management */
	InitializeShardSplitSMHandleManagement();

	InitializeMultiTenantMonitorSMHandleManagement();

	/* enable modification of pg_catalog tables during pg_upgrade */
	if (IsBinaryUpgrade)
	{
		SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
						PGC_S_OVERRIDE);
	}

	/*
	 * In postmasters execution of _PG_init, IsUnderPostmaster will be false and
	 * we want to do the cleanup at that time only, otherwise there is a chance that
	 * there will be parallel queries and we might do a cleanup for things that are
	 * already in use. This is only needed in Windows.
	 */
	if (!IsUnderPostmaster)
	{
		DoInitialCleanup();
	}

	PrevObjectAccessHook = object_access_hook;
	object_access_hook = CitusObjectAccessHook;


	/* ensure columnar module is loaded at the right time */
	load_file(COLUMNAR_MODULE_NAME, false);

	/*
	 * Register utility hook. This must be done after loading columnar, so
	 * that the citus hook is called first, followed by the columnar hook,
	 * followed by standard_ProcessUtility. That allows citus to distribute
	 * ALTER TABLE commands before columnar strips out the columnar-specific
	 * options.
	 */
	PrevProcessUtility = (ProcessUtility_hook != NULL) ?
						 ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = citus_ProcessUtility;

	/*
	 * Acquire symbols for columnar functions that citus calls.
	 */
	void *handle = NULL;

	/* use load_external_function() the first time to initialize the handle */
	extern_ColumnarSupportsIndexAM = (ColumnarSupportsIndexAM_type) (void *)
									 load_external_function(COLUMNAR_MODULE_NAME,
															"ColumnarSupportsIndexAM",
															true, &handle);

	CacheRegisterRelcacheCallback(InvalidateDistRelationCacheCallback,
								  (Datum) 0);

	INIT_COLUMNAR_SYMBOL(CompressionTypeStr_type, CompressionTypeStr);
	INIT_COLUMNAR_SYMBOL(IsColumnarTableAmTable_type, IsColumnarTableAmTable);
	INIT_COLUMNAR_SYMBOL(ReadColumnarOptions_type, ReadColumnarOptions);

	/* initialize symbols for "pass-through" functions */
	INIT_COLUMNAR_SYMBOL(PGFunction, columnar_handler);
	INIT_COLUMNAR_SYMBOL(PGFunction, alter_columnar_table_set);
	INIT_COLUMNAR_SYMBOL(PGFunction, alter_columnar_table_reset);
	INIT_COLUMNAR_SYMBOL(PGFunction, upgrade_columnar_storage);
	INIT_COLUMNAR_SYMBOL(PGFunction, downgrade_columnar_storage);
	INIT_COLUMNAR_SYMBOL(PGFunction, columnar_relation_storageid);
	INIT_COLUMNAR_SYMBOL(PGFunction, columnar_storage_info);
	INIT_COLUMNAR_SYMBOL(PGFunction, columnar_store_memory_stats);
	INIT_COLUMNAR_SYMBOL(PGFunction, test_columnar_storage_write_new_page);

	/*
	 * This part is only for SECURITY LABEL tests
	 * mimicking what an actual security label provider would do
	 */
	if (RunningUnderCitusTestSuite)
	{
		register_label_provider("citus '!tests_label_provider",
								citus_test_object_relabel);
	}
}


/*
 * PrependCitusDecodersToDynamicLibrayPath prepends the $libdir/citus_decoders
 * to the dynamic library path. This is needed to make sure that the citus
 * decoders are loaded before the default decoders for CDC.
 */
static void
AdjustDynamicLibraryPathForCdcDecoders(void)
{
	if (strcmp(Dynamic_library_path, "$libdir") == 0)
	{
		SetConfigOption("dynamic_library_path", CDC_DECODER_DYNAMIC_LIB_PATH,
						PGC_POSTMASTER, PGC_S_OVERRIDE);
	}
}


#if PG_VERSION_NUM >= PG_VERSION_15

/*
 * Requests any additional shared memory required for citus.
 */
static void
citus_shmem_request(void)
{
	if (prev_shmem_request_hook)
	{
		prev_shmem_request_hook();
	}

	RequestAddinShmemSpace(BackendManagementShmemSize());
	RequestAddinShmemSpace(SharedConnectionStatsShmemSize());
	RequestAddinShmemSpace(MaintenanceDaemonShmemSize());
	RequestAddinShmemSpace(CitusQueryStatsSharedMemSize());
	RequestAddinShmemSpace(LogicalClockShmemSize());
	RequestNamedLWLockTranche(STATS_SHARED_MEM_NAME, 1);
}


#endif


/*
 * DoInitialCleanup does cleanup at start time.
 * Currently it:
 * - Removes intermediate result directories ( in case there are any leftovers)
 */
static void
DoInitialCleanup(void)
{
	CleanupJobCacheDirectory();
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
	long max_stack_depth_bytes = max_stack_depth * 1024L;

	/*
	 * Explanation of IGNORE-BANNED:
	 * alloca is safe to use here since we limit the allocated size. We cannot
	 * use malloc as a replacement, since we actually want to grow the stack
	 * here.
	 */
	volatile char *stack_resizer = alloca(max_stack_depth_bytes); /* IGNORE-BANNED */

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
	 * by the distributed deadlock detection. Also reset the state for this,
	 * since the next cancelation of the backend might have another reason.
	 *
	 * We also want to provide a useful hint for sequence overflow errors
	 * because they're likely to be caused by the way Citus handles smallint/int
	 * based sequences on worker nodes. Note that we add the hint without checking
	 * whether we're on a worker node or the sequence was used on a distributed
	 * table because catalog might not be available at this point. And given
	 * that this hint might be shown for regular Postgres tables too, we inject
	 * the hint only when EnableUnsupportedFeatureMessages is set to true.
	 * Otherwise, vanilla tests would fail.
	 */
	bool clearState = true;
	if (edata->elevel == ERROR && edata->sqlerrcode == ERRCODE_QUERY_CANCELED &&
		MyBackendGotCancelledDueToDeadlock(clearState))
	{
		edata->sqlerrcode = ERRCODE_T_R_DEADLOCK_DETECTED;

		/*
		 * This hook is called by EmitErrorReport() when emitting the ereport
		 * either to frontend or to the server logs. And some callers of
		 * EmitErrorReport() (e.g.: errfinish()) seems to assume that string
		 * fields of given ErrorData object needs to be freed. For this reason,
		 * we copy the message into heap here.
		 */
		edata->message = pstrdup("canceling the transaction since it was "
								 "involved in a distributed deadlock");
	}
	else if (EnableUnsupportedFeatureMessages &&
			 IsSequenceOverflowError(edata))
	{
		edata->detail = pstrdup("nextval(sequence) calls in worker nodes "
								"are not supported for column defaults of "
								"type int or smallint");
		edata->hint = pstrdup("If the command was issued from a worker node, "
							  "try issuing it from the coordinator node "
							  "instead.");
	}

	if (original_emit_log_hook)
	{
		original_emit_log_hook(edata);
	}
}


/*
 * IsSequenceOverflowError returns true if the given error is a sequence
 * overflow error.
 */
static bool
IsSequenceOverflowError(ErrorData *edata)
{
	static const char *sequenceOverflowedMsgPrefix =
		"nextval: reached maximum value of sequence";
	static const int sequenceOverflowedMsgPrefixLen = 42;

	return edata->elevel == ERROR &&
		   edata->sqlerrcode == ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED &&
		   edata->message != NULL &&
		   strncmp(edata->message, sequenceOverflowedMsgPrefix,
				   sequenceOverflowedMsgPrefixLen) == 0;
}


/*
 * StartupCitusBackend initializes per-backend infrastructure, and is called
 * the first time citus is used in a database.
 *
 * NB: All code here has to be able to cope with this routine being called
 * multiple times in the same backend.  This will e.g. happen when the
 * extension is created, upgraded or dropped. Due to the way we detect the
 * extension being dropped this can also happen when autovacuum runs ANALYZE on
 * pg_dist_partition, see InvalidateDistRelationCacheCallback for details.
 */
void
StartupCitusBackend(void)
{
	InitializeMaintenanceDaemonBackend();

	/*
	 * For query backends this will be a no-op, because InitializeBackendData
	 * is already called from the CitusAuthHook. But for background workers we
	 * still need to initialize the backend data.
	 */
	InitializeBackendData(application_name);

	/*
	 * If this is an external connection or a background workers this will
	 * generate the global PID for this connection. For internal connections
	 * this is a no-op, since InitializeBackendData will already have extracted
	 * the gpid from the application_name.
	 */
	AssignGlobalPID(application_name);

	SetBackendDataDatabaseId();
	RegisterConnectionCleanup();
	FinishedStartupCitusBackend = true;
}


/*
 * GetCurrentClientMinMessageLevelName returns the name of the
 * the GUC client_min_messages for its specified value.
 */
const char *
GetClientMinMessageLevelNameForValue(int minMessageLevel)
{
	struct config_enum record = { 0 };
	record.options = log_level_options;
	const char *clientMinMessageLevelName = config_enum_lookup_by_value(&record,
																		minMessageLevel);
	return clientMinMessageLevelName;
}


/*
 * RegisterConnectionCleanup cleans up any resources left at the end of the
 * session. We prefer to cleanup before shared memory exit to make sure that
 * this session properly releases anything hold in the shared memory.
 */
static void
RegisterConnectionCleanup(void)
{
	static bool registeredCleanup = false;
	if (registeredCleanup == false)
	{
		before_shmem_exit(CitusCleanupConnectionsAtExit, 0);

		registeredCleanup = true;
	}
}


/*
 * RegisterExternalClientBackendCounterDecrement is called when the backend terminates.
 * For all client backends, we register a callback that will undo
 */
static void
RegisterExternalClientBackendCounterDecrement(void)
{
	static bool registeredCleanup = false;
	if (registeredCleanup == false)
	{
		before_shmem_exit(DecrementExternalClientBackendCounterAtExit, 0);

		registeredCleanup = true;
	}
}


/*
 * CitusCleanupConnectionsAtExit is called before_shmem_exit() of the
 * backend for the purposes of any clean-up needed.
 */
static void
CitusCleanupConnectionsAtExit(int code, Datum arg)
{
	/* properly close all the cached connections */
	ShutdownAllConnections();

	/*
	 * Make sure that we give the shared connections back to the shared
	 * pool if any. This operation is a no-op if the reserved connections
	 * are already given away.
	 */
	DeallocateReservedConnections();

	/* we don't want any monitoring view/udf to show already exited backends */
	SetActiveMyBackend(false);
	UnSetGlobalPID();
}


/*
 * DecrementExternalClientBackendCounterAtExit is called before_shmem_exit() of the
 * backend for the purposes decrementing
 */
static void
DecrementExternalClientBackendCounterAtExit(int code, Datum arg)
{
	DecrementExternalClientBackendCounter();
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
	const char *subdir = ("base/" PG_JOB_CACHE_DIR);

	if (MakePGDirectory(subdir) != 0 && errno != EEXIST)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create directory \"%s\": %m",
							   subdir)));
	}
}


/* Register Citus configuration variables. */
static void
RegisterCitusConfigVariables(void)
{
	DefineCustomBoolVariable(
		"citus.all_modifications_commutative",
		gettext_noop("Bypasses commutativity checks when enabled"),
		NULL,
		&AllModificationsCommutative,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.allow_modifications_from_workers_to_replicated_tables",
		gettext_noop("Enables modifications from workers to replicated "
					 "tables such as reference tables or hash "
					 "distributed tables with replication factor "
					 "greater than 1."),
		gettext_noop("Allowing modifications from the worker nodes "
					 "requires extra locking which might decrease "
					 "the throughput. Disabling this GUC skips the "
					 "extra locking and prevents modifications from "
					 "worker nodes."),
		&AllowModificationsFromWorkersToReplicatedTables,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.allow_nested_distributed_execution",
		gettext_noop("Enables distributed execution within a task "
					 "of another distributed execution."),
		gettext_noop("Nested distributed execution can happen when Citus "
					 "pushes down a call to a user-defined function within "
					 "a distributed query, and the function contains another "
					 "distributed query. In this scenario, Citus makes no "
					 "guarantess with regards to correctness and it is therefore "
					 "disallowed by default. This setting can be used to allow "
					 "nested distributed execution."),
		&AllowNestedDistributedExecution,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.allow_unsafe_constraints",
		gettext_noop("Enables unique constraints and exclusion constraints "
					 "that do not include a distribution column."),
		gettext_noop("To enforce global uniqueness, Citus normally requires "
					 "that unique constraints and exclusion constraints contain "
					 "the distribution column. If the tuple does not include the "
					 "distribution column, Citus cannot ensure that the same value "
					 "is not present in another shard. However, in some cases the "
					 "index creator knows that uniqueness within the shard implies "
					 "global uniqueness (e.g. when indexing an expression derived "
					 "from the distribution column) and adding the distribution column "
					 "separately may not be desirable. This setting can then be used "
					 "to disable the check."),
		&AllowUnsafeConstraints,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.allow_unsafe_locks_from_workers",
		gettext_noop("Enables acquiring a distributed lock from a worker "
					 "when the coordinator is not in the metadata"),
		gettext_noop("Set to false by default. If set to true, enables "
					 "acquiring a distributed lock from a worker "
					 "when the coordinator is not in the metadata. "
					 "This type of lock is unsafe because the worker will not be "
					 "able to lock the coordinator; the coordinator will be able to "
					 "intialize distributed operations on the resources locked "
					 "by the worker. This can lead to concurrent operations from the "
					 "coordinator and distributed deadlocks since the coordinator "
					 "and the workers would not acquire locks across the same nodes "
					 "in the same order."),
		&EnableAcquiringUnsafeLockFromWorkers,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.background_task_queue_interval",
		gettext_noop("Time to wait between checks for scheduled background tasks."),
		NULL,
		&BackgroundTaskQueueCheckInterval,
		5000, -1, 7 * 24 * 3600 * 1000,
		PGC_SIGHUP,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.check_available_space_before_move",
		gettext_noop("When enabled will check free disk space before a shard move"),
		gettext_noop(
			"Free disk space will be checked when this setting is enabled before each shard move."),
		&CheckAvailableSpaceBeforeMove,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.cluster_name",
		gettext_noop("Which cluster this node is a part of"),
		NULL,
		&CurrentCluster,
		"default",
		PGC_SU_BACKEND,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.coordinator_aggregation_strategy",
		gettext_noop("Sets the strategy for when an aggregate cannot be pushed down. "
					 "'row-gather' will pull up intermediate rows to the coordinator, "
					 "while 'disabled' will error if coordinator aggregation is necessary"),
		NULL,
		&CoordinatorAggregationStrategy,
		COORDINATOR_AGGREGATION_ROW_GATHER,
		coordinator_aggregation_options,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.copy_switchover_threshold",
		gettext_noop("Sets the threshold for copy to be switched "
					 "over per connection."),
		gettext_noop("Data size threshold to switch over the active placement for "
					 "a connection. If this is too low, overhead of starting COPY "
					 "commands will hurt the performance. If this is too high, "
					 "buffered data will use lots of memory. 4MB is a good balance "
					 "between memory usage and performance. Note that this is irrelevant "
					 "in the common case where we open one connection per placement."),
		&CopySwitchOverThresholdBytes,
		4 * 1024 * 1024, 1, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_BYTE | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_STANDARD,
		NULL, NULL, NULL);

	/*
	 * This doesn't use cpu_priority_options on purpose, because we always need
	 * to know the actual priority value so that `RESET citus.cpu_priority`
	 * actually changes the priority back.
	 */
	DefineCustomIntVariable(
		"citus.cpu_priority",
		gettext_noop("Sets the CPU priority of the current backend."),
		gettext_noop("Lower numbers cause more favorable scheduling, so the "
					 "queries that this backend runs will be able to use more "
					 "CPU resources compared to queries from other backends. "
					 "WARNING: Changing this setting can lead to a pnemomenom "
					 "called 'priority inversion', due to locks being held "
					 "between different backends. This means that processes "
					 "might be scheduled in the exact oposite way of what you "
					 "want, i.e. processes that you want scheduled a lot, are "
					 "scheduled very little. So use this setting at your own "
					 "risk."),
		&CpuPriority,
		GetOwnPriority(), -20, 19,
		PGC_SUSET,
		GUC_STANDARD,
		NULL, CpuPriorityAssignHook, NULL);

	DefineCustomEnumVariable(
		"citus.cpu_priority_for_logical_replication_senders",
		gettext_noop("Sets the CPU priority for backends that send logical "
					 "replication changes to other nodes for online shard "
					 "moves and splits."),
		gettext_noop("Lower numbers cause more favorable scheduling, so the "
					 "backends used to do the shard move will get more CPU "
					 "resources. 'inherit' is a special value and disables "
					 "overriding the CPU priority for backends that send "
					 "logical replication changes."),
		&CpuPriorityLogicalRepSender,
		CPU_PRIORITY_INHERIT, cpu_priority_options,
		PGC_SUSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.create_object_propagation",
		gettext_noop("Controls the behavior of CREATE statements in transactions for "
					 "supported objects"),
		gettext_noop("When creating new objects in transactions this setting is used to "
					 "determine the behavior for propagating. When objects are created "
					 "in a multi-statement transaction block Citus needs to switch to "
					 "sequential mode (if not already) to make sure the objects are "
					 "visible to later statements on shards. The switch to sequential is "
					 "not always desired. By changing this behavior the user can trade "
					 "off performance for full transactional consistency on the creation "
					 "of new objects."),
		&CreateObjectPropagationMode,
		CREATE_OBJECT_PROPAGATION_IMMEDIATE, create_object_propagation_options,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.defer_drop_after_shard_move",
		gettext_noop("Deprecated, Citus always defers drop after shard move"),
		NULL,
		&DeprecatedDeferShardDeleteOnMove,
		true,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.defer_drop_after_shard_split",
		gettext_noop("Deprecated, Citus always defers drop after shard split"),
		NULL,
		&DeprecatedDeferShardDeleteOnSplit,
		true,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.defer_shard_delete_interval",
		gettext_noop("Sets the time to wait between background deletion for shards."),
		gettext_noop("Shards that are marked for deferred deletion need to be deleted in "
					 "the background at a later time. This is done at a regular interval "
					 "configured here. The deletion is executed optimistically, it tries "
					 "to take a lock on a shard to clean, if the lock can't be acquired "
					 "the background worker moves on. When set to -1 this background "
					 "process is skipped."),
		&DeferShardDeleteInterval,
		15000, -1, 7 * 24 * 3600 * 1000,
		PGC_SIGHUP,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomRealVariable(
		"citus.desired_percent_disk_available_after_move",
		gettext_noop(
			"Sets how many percentage of free disk space should be after a shard move"),
		gettext_noop(
			"This setting controls how much free space should be available after a shard move. "
			"If the free disk space will be lower than this parameter, then shard move will result in "
			"an error."),
		&DesiredPercentFreeAfterMove,
		10.0, 0.0, 100.0,
		PGC_SIGHUP,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomRealVariable(
		"citus.distributed_deadlock_detection_factor",
		gettext_noop("Sets the time to wait before checking for distributed "
					 "deadlocks. Postgres' deadlock_timeout setting is "
					 "multiplied with the value. If the value is set to -1, "
					 "distributed deadlock detection is disabled."),
		NULL,
		&DistributedDeadlockDetectionTimeoutFactor,
		2.0, -1.0, 1000.0,
		PGC_SIGHUP,
		GUC_STANDARD,
		ErrorIfNotASuitableDeadlockFactor, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_alter_database_owner",
		gettext_noop("Enables propagating ALTER DATABASE ... OWNER TO ... statements to "
					 "workers"),
		NULL,
		&EnableAlterDatabaseOwner,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_alter_role_propagation",
		gettext_noop("Enables propagating ALTER ROLE statements to workers (excluding "
					 "ALTER ROLE SET)"),
		NULL,
		&EnableAlterRolePropagation,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_alter_role_set_propagation",
		gettext_noop("Enables propagating ALTER ROLE SET statements to workers"),
		NULL,
		&EnableAlterRoleSetPropagation,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_binary_protocol",
		gettext_noop(
			"Enables communication between nodes using binary protocol when possible"),
		NULL,
		&EnableBinaryProtocol,
		true,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_change_data_capture",
		gettext_noop("Enables using replication origin tracking for change data capture"),
		NULL,
		&EnableChangeDataCapture,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_cluster_clock",
		gettext_noop("When users explicitly call UDF citus_get_transaction_clock() "
					 "and the flag is true, it returns the maximum "
					 "clock among all nodes. All nodes move to the "
					 "new clock. If clocks go bad for any reason, "
					 "this serves as a safety valve."),
		NULL,
		&EnableClusterClock,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomBoolVariable(
		"citus.enable_cost_based_connection_establishment",
		gettext_noop("When enabled the connection establishment times "
					 "and task execution times into account for deciding "
					 "whether or not to establish new connections."),
		NULL,
		&EnableCostBasedConnectionEstablishment,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_create_database_propagation",
		gettext_noop("Enables propagating CREATE DATABASE "
					 "and DROP DATABASE statements to workers."),
		NULL,
		&EnableCreateDatabasePropagation,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_create_role_propagation",
		gettext_noop("Enables propagating CREATE ROLE "
					 "and DROP ROLE statements to workers"),
		NULL,
		&EnableCreateRolePropagation,
		true,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_create_type_propagation",
		gettext_noop("Enables propagating of CREATE TYPE statements to workers"),
		NULL,
		&EnableCreateTypePropagation,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_ddl_propagation",
		gettext_noop("Enables propagating DDL statements to worker shards"),
		NULL,
		&EnableDDLPropagation,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_fast_path_router_planner",
		gettext_noop("Enables fast path router planner"),
		NULL,
		&EnableFastPathRouterPlanner,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_local_execution",
		gettext_noop("Enables queries on shards that are local to the current node "
					 "to be planned and executed locally."),
		NULL,
		&EnableLocalExecution,
		true,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_local_reference_table_foreign_keys",
		gettext_noop("Enables foreign keys from/to local tables"),
		gettext_noop("When enabled, foreign keys between local tables and reference "
					 "tables supported."),
		&EnableLocalReferenceForeignKeys,
		true,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_manual_changes_to_shards",
		gettext_noop("Enables dropping and truncating known shards."),
		gettext_noop("Set to false by default. If set to true, enables "
					 "dropping and truncating shards on the coordinator "
					 "(or the workers with metadata)"),
		&EnableManualChangesToShards,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.enable_manual_metadata_changes_for_user",
		gettext_noop("Enables some helper UDFs to modify metadata "
					 "for the given user"),
		NULL,
		&EnableManualMetadataChangesForUser,
		"",
		PGC_SIGHUP,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_metadata_sync",
		gettext_noop("Enables object and metadata syncing."),
		NULL,
		&EnableMetadataSync,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_non_colocated_router_query_pushdown",
		gettext_noop("Enables router planner for the queries that reference "
					 "non-colocated distributed tables."),
		gettext_noop("Normally, router planner planner is only enabled for "
					 "the queries that reference colocated distributed tables "
					 "because it is not guaranteed to have the target shards "
					 "always on the same node, e.g., after rebalancing the "
					 "shards. For this reason, while enabling this flag allows "
					 "some degree of optimization for the queries that reference "
					 "non-colocated distributed tables, it is not guaranteed "
					 "that the same query will work after rebalancing the shards "
					 "or altering the shard count of one of those distributed "
					 "tables."),
		&EnableNonColocatedRouterQueryPushdown,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_repartition_joins",
		gettext_noop("Allows Citus to repartition data between nodes."),
		NULL,
		&EnableRepartitionJoins,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_repartitioned_insert_select",
		gettext_noop("Enables repartitioned INSERT/SELECTs"),
		NULL,
		&EnableRepartitionedInsertSelect,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_router_execution",
		gettext_noop("Enables router execution"),
		NULL,
		&EnableRouterExecution,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_schema_based_sharding",
		gettext_noop("Enables schema based sharding."),
		gettext_noop("The schemas created while this is ON will be automatically "
					 "associated with individual colocation groups such that the "
					 "tables created in those schemas will be automatically "
					 "converted to colocated distributed tables without a shard "
					 "key."),
		&EnableSchemaBasedSharding,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_single_hash_repartition_joins",
		gettext_noop("Enables single hash repartitioning between hash "
					 "distributed tables"),
		NULL,
		&EnableSingleHashRepartitioning,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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

	DefineCustomBoolVariable(
		"citus.enable_unique_job_ids",
		gettext_noop("Enables unique job IDs by prepending the local process ID and "
					 "group ID. This should usually be enabled, but can be disabled "
					 "for repeatable output in regression tests."),
		NULL,
		&EnableUniqueJobIds,
		true,
		PGC_USERSET,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_unsafe_triggers",
		gettext_noop("Enables arbitrary triggers on distributed tables which may cause "
					 "visibility and deadlock issues. Use at your own risk."),
		NULL,
		&EnableUnsafeTriggers,
		false,
		PGC_USERSET,
		GUC_STANDARD | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_unsupported_feature_messages",
		gettext_noop("Controls showing of some citus related messages. It is intended to "
					 "be used before vanilla tests to stop unwanted citus messages."),
		NULL,
		&EnableUnsupportedFeatureMessages,
		true,
		PGC_SUSET,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enable_version_checks",
		gettext_noop("Enables version checks during CREATE/ALTER EXTENSION commands"),
		NULL,
		&EnableVersionChecks,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.enforce_object_restrictions_for_local_objects",
		gettext_noop(
			"Controls some restrictions for local objects."),
		NULL,
		&EnforceLocalObjectRestrictions,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.explain_analyze_sort_method",
		gettext_noop("Sets the sorting method for EXPLAIN ANALYZE queries."),
		gettext_noop("This parameter is intended for testing. It is developed "
					 "to get consistent regression test outputs. When it is set "
					 "to 'time', EXPLAIN ANALYZE output is sorted by execution "
					 "duration on workers. When it is set to 'taskId', it is "
					 "sorted by task id. By default, it is set to 'time'; but "
					 "in regression tests, it's set to 'taskId' for consistency."),
		&ExplainAnalyzeSortMethod,
		EXPLAIN_ANALYZE_SORT_BY_TIME, explain_analyze_sort_method_options,
		PGC_USERSET,
		0,
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
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.force_max_query_parallelization",
		gettext_noop("Open as many connections as possible to maximize query "
					 "parallelization"),
		gettext_noop("When enabled, Citus will force the executor to use "
					 "as many connections as possible while executing a "
					 "parallel distributed query. If not enabled, the executor "
					 "might choose to use less connections to optimize overall "
					 "query execution throughput. Internally, setting this true "
					 "will end up with using one connection per task."),
		&ForceMaxQueryParallelization,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.grep_remote_commands",
		gettext_noop(
			"Applies \"command\" like citus.grep_remote_commands, if returns "
			"true, the command is logged."),
		NULL,
		&GrepRemoteCommands,
		"",
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.hide_citus_dependent_objects",
		gettext_noop(
			"Hides some objects, which depends on citus extension, from pg meta class queries. "
			"It is intended to be used only before postgres vanilla tests to not break them."),
		NULL,
		&HideCitusDependentObjects,
		false,
		PGC_USERSET,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	/*
	 * This was a GUC we added on Citus 11.0.1, and
	 * replaced with another name on 11.0.2 via #5920.
	 * However, as this GUC has been used in
	 * citus_shard_indexes_on_worker-11.0.1
	 * script. So, it is not easy to completely get rid
	 * of the GUC. Especially with PG 15+, Postgres verifies
	 * existence of the GUCs that are used. So, without this
	 * CREATE EXTENSION fails.
	 */
	DefineCustomStringVariable(
		"citus.hide_shards_from_app_name_prefixes",
		gettext_noop("Deprecated, use citus.show_shards_for_app_name_prefixes"),
		NULL,
		&DeprecatedEmptyString,
		"",
		PGC_SUSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.isolation_test_session_process_id",
		NULL,
		NULL,
		&IsolationTestSessionProcessID,
		-1, -1, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.isolation_test_session_remote_process_id",
		NULL,
		NULL,
		&IsolationTestSessionRemoteProcessID,
		-1, -1, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.local_copy_flush_threshold",
		gettext_noop("Sets the threshold for local copy to be flushed."),
		NULL,
		&LocalCopyFlushThresholdByte,
		512 * 1024, 1, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_BYTE | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.local_hostname",
		gettext_noop("Sets the hostname when connecting back to itself."),
		gettext_noop("For some operations nodes, mostly the coordinator, connect back to "
					 "itself. When configuring SSL certificates it sometimes is required "
					 "to use a specific hostname to match the CN of the certificate when "
					 "verify-full is used."),
		&LocalHostName,
		"localhost",
		PGC_SUSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.local_shared_pool_size",
		gettext_noop(
			"Sets the maximum number of connections allowed for the shards on the "
			"local node across all the backends from this node. Setting to -1 disables "
			"connections throttling. Setting to 0 makes it auto-adjust, meaning "
			"equal to the half of max_connections on the coordinator."),
		gettext_noop("As a rule of thumb, the value should be at most equal to the "
					 "max_connections on the local node."),
		&LocalSharedPoolSize,
		0, -1, INT_MAX,
		PGC_SIGHUP,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, LocalPoolSizeGucShowHook);

	DefineCustomEnumVariable(
		"citus.local_table_join_policy",
		gettext_noop("defines the behaviour when a distributed table "
					 "is joined with a local table"),
		gettext_noop(
			"There are 4 values available. The default, 'auto' will recursively plan "
			"distributed tables if there is a constant filter on a unique index. "
			"'prefer-local' will choose local tables if possible. "
			"'prefer-distributed' will choose distributed tables if possible. "
			"'never' will basically skip local table joins."
			),
		&LocalTableJoinPolicy,
		LOCAL_JOIN_POLICY_AUTO,
		local_table_join_policies,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_distributed_deadlock_detection",
		gettext_noop("Log distributed deadlock detection related processing in "
					 "the server log"),
		NULL,
		&LogDistributedDeadlockDetection,
		false,
		PGC_SIGHUP,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_intermediate_results",
		gettext_noop("Log intermediate results sent to other nodes"),
		NULL,
		&LogIntermediateResults,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_local_commands",
		gettext_noop("Log queries that are executed locally, can be overriden by "
					 "citus.log_remote_commands"),
		NULL,
		&LogLocalCommands,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_multi_join_order",
		gettext_noop("Logs the distributed join order to the server log."),
		gettext_noop("We use this private configuration entry as a debugging aid. "
					 "If enabled, we print the distributed join order."),
		&LogMultiJoinOrder,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.log_remote_commands",
		gettext_noop("Log queries sent to other nodes in the server log"),
		NULL,
		&LogRemoteCommands,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.logical_replication_timeout",
		gettext_noop("Sets the timeout to error out when logical replication is used"),
		gettext_noop("Citus uses logical replication when it moves/replicates shards. "
					 "This setting determines when Citus gives up waiting for progress "
					 "during logical replication and errors out."),
		&LogicalReplicationTimeout,
		2 * 60 * 60 * 1000, 0, 7 * 24 * 3600 * 1000,
		PGC_SIGHUP,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.main_db",
		gettext_noop("Which database is designated as the main_db"),
		NULL,
		&MainDb,
		"",
		PGC_POSTMASTER,
		GUC_STANDARD,
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
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_background_task_executors",
		gettext_noop(
			"Sets the maximum number of parallel task executor workers for scheduled "
			"background tasks"),
		gettext_noop(
			"Controls the maximum number of parallel task executors the task monitor "
			"can create for scheduled background tasks. Note that the value is not effective "
			"if it is set a value higher than 'max_worker_processes' postgres parameter . It is "
			"also not guaranteed to have exactly specified number of parallel task executors "
			"because total background worker count is shared by all background workers. The value "
			"represents the possible maximum number of task executors."),
		&MaxBackgroundTaskExecutors,
		4, 1, MAX_BG_TASK_EXECUTORS,
		PGC_SIGHUP,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_background_task_executors_per_node",
		gettext_noop(
			"Sets the maximum number of parallel background task executor workers "
			"for scheduled background tasks that involve a particular node"),
		NULL,
		&MaxBackgroundTaskExecutorsPerNode,
		1, 1, 128,
		PGC_SIGHUP,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_cached_connection_lifetime",
		gettext_noop("Sets the maximum lifetime of cached connections to other nodes."),
		NULL,
		&MaxCachedConnectionLifetime,
		10 * MS_PER_MINUTE, -1, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_MS | GUC_STANDARD,
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
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_client_connections",
		gettext_noop("Sets the maximum number of connections regular clients can make"),
		gettext_noop("To ensure that a Citus cluster has a sufficient number of "
					 "connection slots to serve queries internally, it can be "
					 "useful to reserve connection slots for Citus internal "
					 "connections. When max_client_connections is set to a value "
					 "below max_connections, the remaining connections are reserved "
					 "for connections between Citus nodes. This does not affect "
					 "superuser_reserved_connections. If set to -1, no connections "
					 "are reserved."),
		&MaxClientConnections,
		-1, -1, MaxConnections,
		PGC_SUSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_high_priority_background_processes",
		gettext_noop("Sets the maximum number of background processes "
					 "that can have their CPU priority increased at the same "
					 "time on a specific node."),
		gettext_noop("This setting is useful to make sure logical replication "
					 "senders don't take over the CPU of the entire machine."),
		&MaxHighPriorityBackgroundProcesess,
		2, 0, 10000,
		PGC_SUSET,
		GUC_STANDARD,
		NULL, NULL, NULL);


	DefineCustomIntVariable(
		"citus.max_intermediate_result_size",
		gettext_noop("Sets the maximum size of the intermediate results in KB for "
					 "CTEs and complex subqueries."),
		NULL,
		&MaxIntermediateResult,
		1048576, -1, MAX_KILOBYTES,
		PGC_USERSET,
		GUC_UNIT_KB | GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_matview_size_to_auto_recreate",
		gettext_noop("Sets the maximum size of materialized views in MB to "
					 "automatically distribute them."),
		NULL,
		&MaxMatViewSizeToAutoRecreate,
		1024, -1, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_MB | GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_rebalancer_logged_ignored_moves",
		gettext_noop("Sets the maximum number of ignored moves the rebalance logs"),
		NULL,
		&MaxRebalancerLoggedIgnoredMoves,
		5, -1, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.max_shared_pool_size",
		gettext_noop("Sets the maximum number of connections allowed per worker node "
					 "across all the backends from this node. Setting to -1 disables "
					 "connections throttling. Setting to 0 makes it auto-adjust, meaning "
					 "equal to max_connections on the coordinator."),
		gettext_noop("As a rule of thumb, the value should be at most equal to the "
					 "max_connections on the remote nodes."),
		&MaxSharedPoolSize,
		0, -1, INT_MAX,
		PGC_SIGHUP,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, MaxSharedPoolSizeGucShowHook);

	DefineCustomIntVariable(
		"citus.max_worker_nodes_tracked",
		gettext_noop("Sets the maximum number of worker nodes that are tracked."),
		gettext_noop("Worker nodes' network locations, their membership and "
					 "health status are tracked in a shared hash table on "
					 "the master node. This configuration value limits the "
					 "size of the hash table, and consequently the maximum "
					 "number of worker nodes that can be tracked. "
					 "Citus keeps some information about the worker nodes "
					 "in the shared memory for certain optimizations. The "
					 "optimizations are enforced up to this number of worker "
					 "nodes. Any additional worker nodes may not benefit from "
					 "the optimizations."),
		&MaxWorkerNodesTracked,
		2048, 1024, INT_MAX,
		PGC_POSTMASTER,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.metadata_sync_interval",
		gettext_noop("Sets the time to wait between metadata syncs."),
		gettext_noop("metadata sync needs to run every so often "
					 "to synchronize metadata to metadata nodes "
					 "that are out of sync."),
		&MetadataSyncInterval,
		60 * MS_PER_SECOND, 1, 7 * MS_PER_DAY,
		PGC_SIGHUP,
		GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.metadata_sync_mode",
		gettext_noop("Sets transaction mode for metadata syncs."),
		gettext_noop("metadata sync can be run inside a single coordinated "
					 "transaction or with multiple small transactions in "
					 "idempotent way. By default we sync metadata in single "
					 "coordinated transaction. When we hit memory problems "
					 "at workers, we have alternative nontransactional mode "
					 "where we send each command with separate transaction."),
		&MetadataSyncTransMode,
		METADATA_SYNC_TRANSACTIONAL, metadata_sync_mode_options,
		PGC_SUSET,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.metadata_sync_retry_interval",
		gettext_noop("Sets the interval to retry failed metadata syncs."),
		gettext_noop("metadata sync needs to run every so often "
					 "to synchronize metadata to metadata nodes "
					 "that are out of sync."),
		&MetadataSyncRetryInterval,
		5 * MS_PER_SECOND, 1, 7 * MS_PER_DAY,
		PGC_SIGHUP,
		GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	/*
	 * Previously we setting this configuration parameter
	 * in the fly for failure tests schedule.
	 * However, PG15 doesn't allow that anymore: reserved prefixes
	 * like "citus" cannot be used to set non-existing GUCs.
	 * Relevant PG commit: 88103567cb8fa5be46dc9fac3e3b8774951a2be7
	 */

	DefineCustomStringVariable(
		"citus.mitmfifo",
		gettext_noop("Sets the citus mitm fifo path for failure tests"),
		gettext_noop("This GUC is only used for testing."),
		&MitmfifoEmptyString,
		"",
		PGC_SUSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.multi_shard_modify_mode",
		gettext_noop("Sets the connection type for multi shard modify queries"),
		NULL,
		&MultiShardConnectionType,
		PARALLEL_CONNECTION, multi_shard_modify_connection_options,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.multi_task_query_log_level",
		gettext_noop("Sets the level of multi task query execution log messages"),
		NULL,
		&MultiTaskQueryLogLevel,
		CITUS_LOG_LEVEL_OFF, log_level_options,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.next_cleanup_record_id",
		gettext_noop("Set the next cleanup record ID to use in operation creation."),
		gettext_noop("Cleanup record IDs are normally generated using a sequence. If "
					 "next_cleanup_record_id is set to a non-zero value, cleanup record IDs will "
					 "instead be generated by incrementing from the value of "
					 "this GUC and this will be reflected in the GUC. This is "
					 "mainly useful to ensure consistent cleanup record IDs when running "
					 "tests in parallel."),
		&NextCleanupRecordId,
		0, 0, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.next_operation_id",
		gettext_noop("Set the next operation ID to use in operation creation."),
		gettext_noop("Operation IDs are normally generated using a sequence. If "
					 "next_operation_id is set to a non-zero value, operation IDs will "
					 "instead be generated by incrementing from the value of "
					 "this GUC and this will be reflected in the GUC. This is "
					 "mainly useful to ensure consistent operation IDs when running "
					 "tests in parallel."),
		&NextOperationId,
		0, 0, INT_MAX,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.node_connection_timeout",
		gettext_noop("Sets the maximum duration to connect to worker nodes."),
		NULL,
		&NodeConnectionTimeout,
		30 * MS_PER_SECOND, 10 * MS, MS_PER_HOUR,
		PGC_USERSET,
		GUC_UNIT_MS | GUC_STANDARD,
		NULL, NULL, NULL);

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

	DefineCustomBoolVariable(
		"citus.override_table_visibility",
		gettext_noop("Enables replacing occurrrences of pg_catalog.pg_table_visible() "
					 "with pg_catalog.citus_table_visible()"),
		gettext_noop("When enabled, shards on the Citus MX worker (data) nodes would be "
					 "filtered out by many psql commands to provide better user "
					 "experience."),
		&OverrideTableVisibility,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.prevent_incomplete_connection_establishment",
		gettext_noop("When enabled, the executor waits until all the connections "
					 "are successfully established."),
		gettext_noop("Under some load, the executor may decide to establish some "
					 "extra connections to further parallelize the execution. However, "
					 "before the connection establishment is done, the execution might "
					 "have already finished. When this GUC is set to true, the execution "
					 "waits for such connections to be established."),
		&PreventIncompleteConnectionEstablishment,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.propagate_session_settings_for_loopback_connection",
		gettext_noop(
			"When enabled, rebalancer propagates all the allowed GUC settings to new connections."),
		NULL,
		&PropagateSessionSettingsForLoopbackConnection,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.propagate_set_commands",
		gettext_noop("Sets which SET commands are propagated to workers."),
		NULL,
		&PropagateSetCommands,
		PROPSETCMD_NONE,
		propagate_set_commands_options,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.rebalancer_by_disk_size_base_cost",
		gettext_noop(
			"When using the by_disk_size rebalance strategy each shard group "
			"will get this cost in bytes added to its actual disk size. This "
			"is used to avoid creating a bad balance when there's very little "
			"data in some of the shards. The assumption is that even empty "
			"shards have some cost, because of parallelism and because empty "
			"shard groups will likely grow in the future."),
		gettext_noop(
			"The main reason this is configurable, is so it can be lowered for Citus its regression tests."),
		&RebalancerByDiskSizeBaseCost,
		100 * 1024 * 1024, 0, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_BYTE | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.recover_2pc_interval",
		gettext_noop("Sets the time to wait between recovering 2PCs."),
		gettext_noop("2PC transaction recovery needs to run every so often "
					 "to clean up records in pg_dist_transaction and "
					 "potentially roll failed 2PCs forward. This setting "
					 "determines how often recovery should run, "
					 "use -1 to disable."),
		&Recover2PCInterval,
		60 * MS_PER_SECOND, -1, 7 * MS_PER_DAY,
		PGC_SIGHUP,
		GUC_UNIT_MS | GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.remote_copy_flush_threshold",
		gettext_noop("Sets the threshold for remote copy to be flushed."),
		gettext_noop("When sending data over remote connections via the COPY protocol, "
					 "bytes are first buffered internally by libpq. If the number of "
					 "bytes buffered exceeds the threshold, Citus waits for all the "
					 "bytes to flush."),
		&RemoteCopyFlushThreshold,
		8 * 1024 * 1024, 0, INT_MAX,
		PGC_USERSET,
		GUC_UNIT_BYTE | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_UNIT_MS | GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.repartition_join_bucket_count_per_node",
		gettext_noop("Sets the bucket size for repartition joins per node"),
		gettext_noop("Repartition joins create buckets in each node and "
					 "uses those to shuffle data around nodes. "),
		&RepartitionJoinBucketCountPerNode,
		4, 1, INT_MAX,
		PGC_SIGHUP,
		GUC_STANDARD | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	/* deprecated setting */
	DefineCustomBoolVariable(
		"citus.replicate_reference_tables_on_activate",
		NULL,
		NULL,
		&DeprecatedReplicateReferenceTablesOnActivate,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.replication_model",
		gettext_noop("Deprecated. Please use citus.shard_replication_factor instead"),
		gettext_noop(
			"Shard replication model is determined by the shard replication factor. "
			"'statement' replication is used only when the replication factor is one."),
		&ReplicationModel,
		REPLICATION_MODEL_STREAMING,
		replication_model_options,
		PGC_SUSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		WarnIfReplicationModelIsSet, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.running_under_citus_test_suite",
		gettext_noop(
			"Only useful for testing purposes, when set to true, Citus does some "
			"tricks to implement useful isolation tests with rebalancing. It also "
			"registers a dummy label provider for SECURITY LABEL tests. Should "
			"never be set to true on production systems "),
		gettext_noop("for details of the tricks implemented, refer to the source code"),
		&RunningUnderCitusTestSuite,
		false,
		PGC_SUSET,
		GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.shard_count",
		gettext_noop("Sets the number of shards for a new hash-partitioned table "
					 "created with create_distributed_table()."),
		NULL,
		&ShardCount,
		32, 1, MAX_SHARD_COUNT,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.shard_replication_factor",
		gettext_noop("Sets the replication factor for shards."),
		gettext_noop("Shards are replicated across nodes according to this "
					 "replication factor. Note that shards read this "
					 "configuration value at sharded table creation time, "
					 "and later reuse the initially read value."),
		&ShardReplicationFactor,
		1, 1, MAX_SHARD_REPLICATION_FACTOR,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.show_shards_for_app_name_prefixes",
		gettext_noop("If application_name starts with one of these values, show shards"),
		gettext_noop("Citus places distributed tables and shards in the same schema. "
					 "That can cause confusion when inspecting the list of tables on "
					 "a node with shards. By default the shards are hidden from "
					 "pg_class. This GUC can be used to show the shards to certain "
					 "applications based on the application_name of the connection. "
					 "The default is empty string, which hides shards from all "
					 "applications. This behaviour can be overridden using the "
					 "citus.override_table_visibility setting"),
		&ShowShardsForAppNamePrefixes,
		"",
		PGC_USERSET,
		GUC_STANDARD,
		ShowShardsForAppNamePrefixesCheckHook,
		ShowShardsForAppNamePrefixesAssignHook,
		NULL);

	DefineCustomBoolVariable(
		"citus.skip_advisory_lock_permission_checks",
		gettext_noop("Postgres would normally enforce some "
					 "ownership checks while acquiring locks. "
					 "When this setting is 'on', Citus skips "
					 "ownership checks on internal advisory "
					 "locks."),
		NULL,
		&SkipAdvisoryLockPermissionChecks,
		false,
		GUC_SUPERUSER_ONLY,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.skip_constraint_validation",
		gettext_noop("Skip validation of constraints"),
		gettext_noop("Validating constraints is a costly operation which effects Citus' "
					 "performance negatively. With this GUC set to true, we skip "
					 "validating them. Constraint validation can be redundant for some "
					 "cases. For instance, when moving a shard, which has already "
					 "validated constraints at the source; we don't need to validate "
					 "the constraints again at the destination."),
		&SkipConstraintValidation,
		false,
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.skip_jsonb_validation_in_copy",
		gettext_noop("Skip validation of JSONB columns on the coordinator during COPY "
					 "into a distributed table"),
		gettext_noop("Parsing large JSON objects may incur significant CPU overhead, "
					 "which can lower COPY throughput. If this GUC is set (the default), "
					 "JSON parsing is skipped on the coordinator, which means you cannot "
					 "see the line number in case of malformed JSON, but throughput will "
					 "be higher. This setting does not apply if the input format is "
					 "binary."),
		&SkipJsonbValidationInCopy,
		true,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.sort_returning",
		gettext_noop("Sorts the RETURNING clause to get consistent test output"),
		gettext_noop("This feature is not intended for users. It is developed "
					 "to get consistent regression test outputs. When enabled, "
					 "the RETURNING clause returns the tuples sorted. The sort "
					 "is done for all the entries, starting from the first one. "
					 "Finally, the sorting is done in ASC order."),
		&SortReturning,
		false,
		PGC_SUSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	/*
	 * It takes about 140 bytes of shared memory to store one row, therefore
	 * this setting should be used responsibly. setting it to 10M will require
	 * 1.4GB of shared memory.
	 */
	DefineCustomIntVariable(
		"citus.stat_statements_max",
		gettext_noop("Determines maximum number of statements tracked by "
					 "citus_stat_statements."),
		NULL,
		&StatStatementsMax,
		50000, 1000, 10000000,
		PGC_POSTMASTER,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.stat_statements_purge_interval",
		gettext_noop("Determines time interval in seconds for "
					 "citus_stat_statements to purge expired entries."),
		NULL,
		&StatStatementsPurgeInterval,
		10, -1, INT_MAX,
		PGC_SIGHUP,
		GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.stat_statements_track",
		gettext_noop(
			"Enables/Disables the stats collection for citus_stat_statements."),
		gettext_noop("Enables the stats collection when set to 'all'. "
					 "Disables when set to 'none'. Disabling can be useful for "
					 "avoiding extra CPU cycles needed for the calculations."),
		&StatStatementsTrack,
		STAT_STATEMENTS_TRACK_NONE,
		stat_statements_track_options,
		PGC_SUSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.stat_tenants_limit",
		gettext_noop("Number of tenants to be shown in citus_stat_tenants."),
		NULL,
		&StatTenantsLimit,
		100, 1, 10000,
		PGC_POSTMASTER,
		GUC_STANDARD,
		NULL, NULL, NULL);
	DefineCustomEnumVariable(
		"citus.stat_tenants_log_level",
		gettext_noop("Sets the level of citus_stat_tenants log messages"),
		NULL,
		&StatTenantsLogLevel,
		CITUS_LOG_LEVEL_OFF, log_level_options,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.stat_tenants_period",
		gettext_noop("Period in seconds to be used for calculating the tenant "
					 "statistics in citus_stat_tenants."),
		NULL,
		&StatTenantsPeriod,
		60, 1, 60 * 60 * 24,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.stat_tenants_track",
		gettext_noop("Enables/Disables the stats collection for citus_stat_tenants."),
		gettext_noop("Enables the stats collection when set to 'all'. "
					 "Disables when set to 'none'. Disabling can be useful for "
					 "avoiding extra CPU cycles needed for the calculations."),
		&StatTenantsTrack,
		STAT_TENANTS_TRACK_NONE,
		stat_tenants_track_options,
		PGC_SUSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomRealVariable(
		"citus.stat_tenants_untracked_sample_rate",
		gettext_noop("Sampling rate for new tenants in citus_stat_tenants."),
		NULL,
		&StatTenantsSampleRateForNewTenants,
		1, 0, 1,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.subquery_pushdown",
		gettext_noop("Usage of this GUC is highly discouraged, please read the long "
					 "description"),
		gettext_noop("When enabled, the planner skips many correctness checks "
					 "for subqueries and pushes down the queries to shards as-is. "
					 "It means that the queries are likely to return wrong results "
					 "unless the user is absolutely sure that pushing down the "
					 "subquery is safe. This GUC is maintained only for backward "
					 "compatibility, no new users are supposed to use it. The planner "
					 "is capable of pushing down as much computation as possible to the "
					 "shards depending on the query."),
		&SubqueryPushdown,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NoticeIfSubqueryPushdownEnabled, NULL, NULL);

	DefineCustomStringVariable(
		"citus.superuser",
		gettext_noop("Name of a superuser role to be used in Citus main database "
					 "connections"),
		NULL,
		&SuperuserRole,
		"",
		PGC_SUSET,
		GUC_STANDARD,
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
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.task_executor_type",
		gettext_noop("Sets the executor type to be used for distributed queries."),
		gettext_noop("The master node chooses between two different executor types "
					 "when executing a distributed query.The adaptive executor is "
					 "optimal for simple key-value lookup queries and queries that "
					 "involve aggregations and/or co-located joins on multiple shards. "),
		&TaskExecutorType,
		MULTI_EXECUTOR_ADAPTIVE,
		task_executor_type_options,
		PGC_USERSET,
		GUC_STANDARD,
		WarnIfDeprecatedExecutorUsed, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.use_citus_managed_tables",
		gettext_noop("Allows new local tables to be accessed on workers"),
		gettext_noop("Adds all newly created tables to Citus metadata by default, "
					 "when enabled. Set to false by default."),
		&AddAllLocalTablesToMetadata,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.use_secondary_nodes",
		gettext_noop("Sets the policy to use when choosing nodes for SELECT queries."),
		NULL,
		&ReadFromSecondaries,
		USE_SECONDARY_NODES_NEVER, use_secondary_nodes_options,
		PGC_SU_BACKEND,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus.values_materialization_threshold",
		gettext_noop("Sets the maximum number of rows allowed for pushing down "
					 "VALUES clause in multi-shard queries. If the number of "
					 "rows exceeds the threshold, the VALUES is materialized "
					 "via pull-push execution. When set to -1, materialization "
					 "is disabled. When set to 0, all VALUES are materialized."),
		gettext_noop("When the VALUES is pushed down (i.e., not materialized), "
					 "the VALUES clause needs to be deparsed for every shard on "
					 "the coordinator - and parsed on the workers. As this "
					 "setting increased, the associated overhead is multiplied "
					 "by the shard count. When materialized, the VALUES is "
					 "deparsed and parsed once. The downside of materialization "
					 "is that Postgres may choose a poor plan when joining "
					 "the materialized result with tables."),
		&ValuesMaterializationThreshold,
		100, -1, INT_MAX,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.version",
		gettext_noop("Shows the Citus library version"),
		NULL,
		&CitusVersion,
		CITUS_VERSION,
		PGC_INTERNAL,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"citus.worker_min_messages",
		gettext_noop("Log messages from workers only if their log level is at or above "
					 "the configured level"),
		NULL,
		&WorkerMinMessages,
		NOTICE,
		log_level_options,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.writable_standby_coordinator",
		gettext_noop("Enables simple DML via a streaming replica of the coordinator"),
		NULL,
		&WritableStandbyCoordinator,
		false,
		PGC_USERSET,
		GUC_STANDARD,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus.restore_point_interval",
		gettext_noop("Sets the timeout for periodic recovery cluster point"),
		NULL,
		&RestorePointInterval,
		"never",
		PGC_SIGHUP,
		GUC_STANDARD,
		GucCheckInterval, NULL, NULL);

	DefineCustomStringVariable(
		"citus.restore_point_interval_name",
		gettext_noop("Sets the prefix of pointname for the periodic recovery cluster point"),
		NULL,
		&RestorePointIntervalName,
		NULL,
		PGC_SIGHUP,
		GUC_STANDARD,
		NULL, NULL, NULL);

	/* warn about config items in the citus namespace that are not registered above */
	EmitWarningsOnPlaceholders("citus");

	OverridePostgresConfigProperties();
}


/*
 * OverridePostgresConfigProperties overrides GUC properties where we want
 * custom behaviour. We should consider using Postgres function find_option
 * in this function once it is exported by Postgres in a later release.
 */
static void
OverridePostgresConfigProperties(void)
{
	int gucCount = 0;
	struct config_generic **guc_vars = get_guc_variables_compat(&gucCount);

	for (int gucIndex = 0; gucIndex < gucCount; gucIndex++)
	{
		struct config_generic *var = (struct config_generic *) guc_vars[gucIndex];

		if (strcmp(var->name, "application_name") == 0)
		{
			struct config_string *stringVar = (struct config_string *) var;

			OldApplicationNameAssignHook = stringVar->assign_hook;
			stringVar->assign_hook = ApplicationNameAssignHook;
		}

		/*
		 * Turn on GUC_REPORT for search_path. GUC_REPORT provides that an S (Parameter Status)
		 * packet is appended after the C (Command Complete) packet sent from the server
		 * for SET command. S packet contains the new value of the parameter
		 * if its value has been changed.
		 */
		if (strcmp(var->name, "search_path") == 0)
		{
			var->flags |= GUC_REPORT;
		}
	}
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
 * WarnIfDeprecatedExecutorUsed prints a warning and sets the config value to
 * adaptive executor (a.k.a., ignores real-time executor).
 */
static bool
WarnIfDeprecatedExecutorUsed(int *newval, void **extra, GucSource source)
{
	if (*newval == DUMMY_REAL_TIME_EXECUTOR_ENUM_VALUE)
	{
		ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("Ignoring the setting, real-time executor is "
								 "deprecated")));

		/* adaptive executor is superset of real-time, so switch to that */
		*newval = MULTI_EXECUTOR_ADAPTIVE;
	}

	return true;
}


/*
 * NoticeIfSubqueryPushdownEnabled prints a notice when a user sets
 * citus.subquery_pushdown to ON. It doesn't print the notice if the
 * value is already true.
 */
static bool
NoticeIfSubqueryPushdownEnabled(bool *newval, void **extra, GucSource source)
{
	/* notice only when the value changes */
	if (*newval == true && SubqueryPushdown == false)
	{
		ereport(NOTICE, (errcode(ERRCODE_WARNING_DEPRECATED_FEATURE),
						 errmsg("Setting citus.subquery_pushdown flag is "
								"discouraged becuase it forces the planner "
								"to pushdown certain queries, skipping "
								"relevant correctness checks."),
						 errdetail(
							 "When enabled, the planner skips many correctness checks "
							 "for subqueries and pushes down the queries to shards as-is. "
							 "It means that the queries are likely to return wrong results "
							 "unless the user is absolutely sure that pushing down the "
							 "subquery is safe. This GUC is maintained only for backward "
							 "compatibility, no new users are supposed to use it. The planner "
							 "is capable of pushing down as much computation as possible to the "
							 "shards depending on the query.")));
	}

	return true;
}


/*
 * WarnIfReplicationModelIsSet prints a warning when a user sets
 * citus.replication_model.
 */
static bool
WarnIfReplicationModelIsSet(int *newval, void **extra, GucSource source)
{
	/* print a warning only when user sets the guc */
	if (source == PGC_S_SESSION)
	{
		ereport(NOTICE, (errcode(ERRCODE_WARNING_DEPRECATED_FEATURE),
						 errmsg(
							 "Setting citus.replication_model has no effect. Please use "
							 "citus.shard_replication_factor instead."),
						 errdetail(
							 "Citus determines the replication model based on the "
							 "replication factor and the replication models of the colocated "
							 "shards. If a colocated table is present, the replication model "
							 "is inherited. Otherwise 'streaming' replication is preferred if "
							 "supported by the replication factor.")));
	}

	return true;
}


/*
 * ShowShardsForAppNamePrefixesCheckHook ensures that the
 * citus.show_shards_for_app_name_prefixes holds a valid list of application_name
 * values.
 */
static bool
ShowShardsForAppNamePrefixesCheckHook(char **newval, void **extra, GucSource source)
{
	List *prefixList = NIL;

	/* SplitGUCList scribbles on the input */
	char *splitCopy = pstrdup(*newval);

	/* check whether we can split into a list of identifiers */
	if (!SplitGUCList(splitCopy, ',', &prefixList))
	{
		GUC_check_errdetail("not a valid list of identifiers");
		return false;
	}

	char *appNamePrefix = NULL;
	foreach_ptr(appNamePrefix, prefixList)
	{
		int prefixLength = strlen(appNamePrefix);
		if (prefixLength >= NAMEDATALEN)
		{
			GUC_check_errdetail("prefix %s is more than %d characters", appNamePrefix,
								NAMEDATALEN);
			return false;
		}

		char *prefixAscii = pstrdup(appNamePrefix);
		pg_clean_ascii_compat(prefixAscii, 0);

		if (strcmp(prefixAscii, appNamePrefix) != 0)
		{
			GUC_check_errdetail("prefix %s in citus.show_shards_for_app_name_prefixes "
								"contains non-ascii characters", appNamePrefix);
			return false;
		}
	}

	return true;
}


/*
 * ShowShardsForAppNamePrefixesAssignHook ensures changes to
 * citus.show_shards_for_app_name_prefixes are reflected in the decision
 * whether or not to show shards.
 */
static void
ShowShardsForAppNamePrefixesAssignHook(const char *newval, void *extra)
{
	ResetHideShardsDecision();
}


/*
 * ApplicationNameAssignHook is called whenever application_name changes
 * to allow us to reset our hide shards decision.
 */
static void
ApplicationNameAssignHook(const char *newval, void *extra)
{
	ResetHideShardsDecision();
	DetermineCitusBackendType(newval);

	/*
	 * AssignGlobalPID might read from catalog tables to get the the local
	 * nodeid. But ApplicationNameAssignHook might be called before catalog
	 * access is available to the backend (such as in early stages of
	 * authentication). We use StartupCitusBackend to initialize the global pid
	 * after catalogs are available. After that happens this hook becomes
	 * responsible to update the global pid on later application_name changes.
	 * So we set the FinishedStartupCitusBackend flag in StartupCitusBackend to
	 * indicate when this responsibility handoff has happened.
	 *
	 * Another solution to the catalog table acccess problem would be to update
	 * global pid lazily, like we do for HideShards. But that's not possible
	 * for the global pid, since it is stored in shared memory instead of in a
	 * process-local global variable. So other processes might want to read it
	 * before this process has updated it. So instead we try to set it as early
	 * as reasonably possible, which is also why we extract global pids in the
	 * AuthHook already (extracting doesn't require catalog access).
	 */
	if (FinishedStartupCitusBackend)
	{
		AssignGlobalPID(newval);
	}
	OldApplicationNameAssignHook(newval, extra);
}


/*
 * NodeConninfoGucCheckHook ensures conninfo settings are in the expected form
 * and that the keywords of all non-null settings are on a allowlist devised to
 * keep users from setting options that may result in confusion.
 */
static bool
NodeConninfoGucCheckHook(char **newval, void **extra, GucSource source)
{
	/* this array _must_ be kept in an order usable by bsearch */
	const char *allowedConninfoKeywords[] = {
		"connect_timeout",
			#if defined(ENABLE_GSS) && defined(ENABLE_SSPI)
		"gsslib",
			#endif
		"host",
		"keepalives",
		"keepalives_count",
		"keepalives_idle",
		"keepalives_interval",
			#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
		"krbsrvname",
			#endif
		"sslcert",
		"sslcompression",
		"sslcrl",
		"sslkey",
		"sslmode",
		"sslrootcert",
		"tcp_user_timeout",
	};
	char *errorMsg = NULL;
	bool conninfoValid = CheckConninfo(*newval, allowedConninfoKeywords,
									   lengthof(allowedConninfoKeywords), &errorMsg);

	if (!conninfoValid)
	{
		GUC_check_errdetail("%s", errorMsg);
	}

	return conninfoValid;
}


/*
 * CpuPriorityAssignHook changes the priority of the current backend to match
 * the chosen value.
 */
static void
CpuPriorityAssignHook(int newval, void *extra)
{
	SetOwnPriority(newval);
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
	if (newval == NULL)
	{
		newval = "";
	}

	if (strcmp(newval, NodeConninfo) == 0 && checkAtBootPassed)
	{
		/* It did not change, no need to do anything */
		return;
	}

	checkAtBootPassed = true;

	PQconninfoOption *optionArray = PQconninfoParse(newval, NULL);
	if (optionArray == NULL)
	{
		ereport(FATAL, (errmsg("cannot parse node_conninfo value"),
						errdetail("The GUC check hook should prevent "
								  "all malformed values.")));
	}

	ResetConnParams();

	for (PQconninfoOption *option = optionArray; option->keyword != NULL; option++)
	{
		if (option->val == NULL || option->val[0] == '\0')
		{
			continue;
		}

		AddConnParam(option->keyword, option->val);
	}

	PQconninfoFree(optionArray);

	/*
	 * Mark all connections for shutdown, since they have been opened using old
	 * connection settings. This is mostly important when changing SSL
	 * parameters, otherwise these would not be applied and connections could
	 * be unencrypted when the user doesn't want that.
	 */
	CloseAllConnectionsAfterTransaction();
}


/*
 * MaxSharedPoolSizeGucShowHook overrides the value that is shown to the
 * user when the default value has not been set.
 */
static const char *
MaxSharedPoolSizeGucShowHook(void)
{
	StringInfo newvalue = makeStringInfo();

	if (MaxSharedPoolSize == 0)
	{
		appendStringInfo(newvalue, "%d", GetMaxSharedPoolSize());
	}
	else
	{
		appendStringInfo(newvalue, "%d", MaxSharedPoolSize);
	}

	return (const char *) newvalue->data;
}


/*
 * LocalPoolSizeGucShowHook overrides the value that is shown to the
 * user when the default value has not been set.
 */
static const char *
LocalPoolSizeGucShowHook(void)
{
	StringInfo newvalue = makeStringInfo();

	appendStringInfo(newvalue, "%d", GetLocalSharedPoolSize());

	return (const char *) newvalue->data;
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


/*
 * CitusAuthHook is a callback for client authentication that Postgres provides.
 * Citus uses this hook to count the number of active backends.
 */
static void
CitusAuthHook(Port *port, int status)
{
	/*
	 * We determine the backend type here because other calls in this hook rely
	 * on it, both IsExternalClientBackend and InitializeBackendData. These
	 * calls would normally initialize its value based on the application_name
	 * global, but this global is not set yet at this point in the connection
	 * initialization. So here we determine it based on the value from Port.
	 */
	DetermineCitusBackendType(port->application_name);

	/* external connections to not have a GPID immediately */
	if (IsExternalClientBackend())
	{
		/*
		 * We raise the shared connection counter pre-emptively. As a result, we may
		 * have scenarios in which a few simultaneous connection attempts prevent
		 * each other from succeeding, but we avoid scenarios where we oversubscribe
		 * the system.
		 *
		 * By also calling RegisterExternalClientBackendCounterDecrement here, we
		 * immediately lower the counter if we throw a FATAL error below. The client
		 * connection counter may temporarily exceed maxClientConnections in between.
		 */
		RegisterExternalClientBackendCounterDecrement();

		uint32 externalClientCount = IncrementExternalClientBackendCounter();

		/*
		 * Limit non-superuser client connections if citus.max_client_connections
		 * is set.
		 */
		if (MaxClientConnections >= 0 &&
			!IsSuperuser(port->user_name) &&
			externalClientCount > MaxClientConnections)
		{
			ereport(FATAL, (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
							errmsg("remaining connection slots are reserved for "
								   "non-replication superuser connections"),
							errdetail("the server is configured to accept up to %d "
									  "regular client connections",
									  MaxClientConnections)));
		}
	}

	/*
	 * Right after this, but before we assign global pid, this backend might
	 * get blocked by a DDL as that happens during parsing.
	 *
	 * That's why, we now initialize its backend data, with the gpid.
	 *
	 * We do this so that this backend gets the chance to show up in
	 * citus_lock_waits.
	 *
	 * We cannot assign a new global PID yet here, because that would require
	 * reading from catalogs, but that's not allowed this early in the
	 * connection startup (because no database has been assigned yet).
	 *
	 * A second reason is for backends that never call StartupCitusBackend. For
	 * those we already set the global PID in the backend data here to be able
	 * to do blocked process detection on connections that are opened over a
	 * replication connection. A replication connection backend will never call
	 * StartupCitusBackend, which normally sets up the global PID.
	 */
	InitializeBackendData(port->application_name);

	IsMainDB = (strncmp(MainDb, "", NAMEDATALEN) == 0 ||
				strncmp(MainDb, port->database_name, NAMEDATALEN) == 0);

	/* let other authentication hooks to kick in first */
	if (original_client_auth_hook)
	{
		original_client_auth_hook(port, status);
	}
}


/*
 * IsSuperuser returns whether the role with the given name is superuser. If
 * the user doesn't exist, this simply returns false instead of throwing an
 * error. This is done to not leak information about users existing or not, in
 * some cases postgres is vague about this on purpose. So, by returning false
 * we let postgres return this possibly vague error message.
 */
static bool
IsSuperuser(char *roleName)
{
	if (roleName == NULL)
	{
		return false;
	}

	HeapTuple roleTuple = SearchSysCache1(AUTHNAME, CStringGetDatum(roleName));
	if (!HeapTupleIsValid(roleTuple))
	{
		return false;
	}

	Form_pg_authid rform = (Form_pg_authid) GETSTRUCT(roleTuple);
	bool isSuperuser = rform->rolsuper;

	ReleaseSysCache(roleTuple);

	return isSuperuser;
}


/*
 * CitusObjectAccessHook is called when an object is created.
 *
 * We currently use it to track CREATE EXTENSION citus; operations to make sure we
 * clear the metadata if the transaction is rolled back.
 */
static void
CitusObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId, int subId,
					  void *arg)
{
	if (PrevObjectAccessHook)
	{
		PrevObjectAccessHook(access, classId, objectId, subId, arg);
	}

	/* Checks if the access is post_create and that it's an extension id */
	if (access == OAT_POST_CREATE && classId == ExtensionRelationId)
	{
		/* There's currently an engine bug that makes it difficult to check
		 * the provided objectId with extension oid so we will set the value
		 * regardless if it's citus being created */
		SetCreateCitusTransactionLevel(GetCurrentTransactionNestLevel());
	}
}
