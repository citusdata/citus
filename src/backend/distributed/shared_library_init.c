/*-------------------------------------------------------------------------
 *
 * shared_library_init.c
 *	  Initialize Citus extension
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "fmgr.h"
#include "miscadmin.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_transaction.h"
#include "distributed/multi_utility.h"
#include "distributed/task_tracker.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "postmaster/postmaster.h"
#include "optimizer/planner.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"

/* marks shared object as one loadable by the postgres version compiled against */
PG_MODULE_MAGIC;

void _PG_init(void);

static void CreateRequiredDirectories(void);
static void RegisterCitusConfigVariables(void);
static void NormalizeWorkerListPath(void);


/* *INDENT-OFF* */
/* GUC enum definitions */
static const struct config_enum_entry task_assignment_policy_options[] = {
	{ "greedy", TASK_ASSIGNMENT_GREEDY, false },
	{ "first-replica", TASK_ASSIGNMENT_FIRST_REPLICA, false },
	{ "round-robin", TASK_ASSIGNMENT_ROUND_ROBIN, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry task_executor_type_options[] = {
	{ "real-time", MULTI_EXECUTOR_REAL_TIME, false },
	{ "task-tracker", MULTI_EXECUTOR_TASK_TRACKER, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry shard_placement_policy_options[] = {
	{ "local-node-first", SHARD_PLACEMENT_LOCAL_NODE_FIRST, false },
	{ "round-robin", SHARD_PLACEMENT_ROUND_ROBIN, false },
	{ "random", SHARD_PLACEMENT_RANDOM, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry multi_shard_commit_protocol_options[] = {
	{ "1pc", COMMIT_PROTOCOL_1PC, false },
	{ "2pc", COMMIT_PROTOCOL_2PC, false },
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
	if (planner_hook != NULL ||
		ExplainOneQuery_hook != NULL ||
		ExecutorStart_hook != NULL ||
		ExecutorRun_hook != NULL ||
		ExecutorFinish_hook != NULL ||
		ExecutorEnd_hook != NULL ||
		ProcessUtility_hook != NULL)
	{
		ereport(ERROR, (errmsg("Citus has to be loaded first"),
						errhint("Place citus at the beginning of "
								"shared_preload_libraries.")));
	}

	/*
	 * Extend the database directory structure before continuing with
	 * initialization - one of the later steps might require them to exist.
	 */
	CreateRequiredDirectories();

	/*
	 * Register Citus configuration variables. Do so before intercepting
	 * hooks or calling initialization functions, in case we want to do the
	 * latter in a configuration dependent manner.
	 */
	RegisterCitusConfigVariables();

	/* intercept planner */
	planner_hook = multi_planner;

	/* intercept explain */
	ExplainOneQuery_hook = MultiExplainOneQuery;

	/* intercept executor */
	ExecutorStart_hook = multi_ExecutorStart;
	ExecutorRun_hook = multi_ExecutorRun;
	ExecutorFinish_hook = multi_ExecutorFinish;
	ExecutorEnd_hook = multi_ExecutorEnd;

	/* register utility hook */
	ProcessUtility_hook = multi_ProcessUtility;

	/* organize that task tracker is started once server is up */
	TaskTrackerRegister();

	/* initialize worker node manager */
	WorkerNodeRegister();
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
		"base/pgsql_job_cache"
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
	DefineCustomStringVariable(
		"citus.worker_list_file",
		gettext_noop("Sets the server's \"worker_list\" configuration file."),
		NULL,
		&WorkerListFileName,
		NULL,
		PGC_POSTMASTER,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, NULL);
	NormalizeWorkerListPath();

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
		gettext_noop("Enables shard cache expiration if a shard's size on disk has "
					 "changed."),
		gettext_noop("When appending to an existing shard, old data may still be cached "
					 "on other workers. This configuration entry activates automatic "
					 "expiration, but should not be used with manual updates to shards."),
		&ExpireCachedShards,
		false,
		PGC_SIGHUP,
		0,
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
		"citus.explain_multi_logical_plan",
		gettext_noop("Enables Explain to print out distributed logical plans."),
		gettext_noop("We use this private configuration entry as a debugging aid. "
					 "If enabled, the Explain command prints out the optimized "
					 "logical plan for distributed queries."),
		&ExplainMultiLogicalPlan,
		false,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus.explain_multi_physical_plan",
		gettext_noop("Enables Explain to print out distributed physical plans."),
		gettext_noop("We use this private configuration entry as a debugging aid. "
					 "If enabled, the Explain command prints out the physical "
					 "plan for distributed queries."),
		&ExplainMultiPhysicalPlan,
		false,
		PGC_USERSET,
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

	DefineCustomBoolVariable(
		"citus.enable_ddl_propagation",
		gettext_noop("Enables propagating DDL statements to worker shards"),
		NULL,
		&EnableDDLPropagation,
		true,
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
		2, 1, 100,
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
		10, 1, REMOTE_NODE_CONNECT_TIMEOUT,
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
		200, 10, 100000,
		PGC_SIGHUP,
		GUC_UNIT_MS,
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
		gettext_noop("The shard count threshold over which a table is considered large."),
		gettext_noop("A distributed table is considered to be large if it has "
					 "more shards than the value specified here. This largeness "
					 "criteria is then used in picking a table join order during "
					 "distributed query planning."),
		&LargeTableShardCount,
		4, 1, 10000,
		PGC_USERSET,
		0,
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
					 "shards (currently, only COPY on distributed tables modifies more "
					 "than one shard), two-phase commit is required to ensure data is "
					 "never lost. Change this setting to '2pc' from its default '1pc' to "
					 "enable 2 PC. You must also set max_prepared_transactions on the "
					 "worker nodes. Recovery from failed 2PCs is currently manual."),
		&MultiShardCommitProtocol,
		COMMIT_PROTOCOL_1PC,
		multi_shard_commit_protocol_options,
		PGC_USERSET,
		0,
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
		MULTI_EXECUTOR_REAL_TIME,
		task_executor_type_options,
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

	/* warn about config items in the citus namespace that are not registered above */
	EmitWarningsOnPlaceholders("citus");
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
