# Citus toplevel Makefile

EXTENSION = citus

MODULE_big = citus

OBJS = src/backend/distributed/shared_library_init.o \
     src/backend/distributed/commands/create_distributed_table.o \
     src/backend/distributed/commands/drop_distributed_table.o \
     src/backend/distributed/commands/multi_copy.o \
     src/backend/distributed/commands/transmit.o \
     src/backend/distributed/connection/connection_management.o \
     src/backend/distributed/connection/placement_connection.o \
     src/backend/distributed/connection/remote_commands.o \
     src/backend/distributed/executor/citus_custom_scan.o \
     src/backend/distributed/executor/insert_select_executor.o \
     src/backend/distributed/executor/intermediate_results.o \
     src/backend/distributed/executor/multi_client_executor.o \
     src/backend/distributed/executor/multi_executor.o \
     src/backend/distributed/executor/multi_real_time_executor.o \
     src/backend/distributed/executor/multi_router_executor.o \
     src/backend/distributed/executor/multi_server_executor.o \
     src/backend/distributed/executor/multi_task_tracker_executor.o \
     src/backend/distributed/executor/multi_utility.o \
     src/backend/distributed/executor/subplan_execution.o \
     src/backend/distributed/master/citus_create_restore_point.o \
     src/backend/distributed/master/master_citus_tools.o \
     src/backend/distributed/master/master_create_shards.o \
     src/backend/distributed/master/master_delete_protocol.o \
     src/backend/distributed/master/master_metadata_utility.o \
     src/backend/distributed/master/master_modify_multiple_shards.o \
     src/backend/distributed/master/master_node_protocol.o \
     src/backend/distributed/master/master_repair_shards.o \
     src/backend/distributed/master/master_split_shards.o \
     src/backend/distributed/master/master_stage_protocol.o \
     src/backend/distributed/master/master_truncate.o \
     src/backend/distributed/master/worker_node_manager.o \
     src/backend/distributed/metadata/metadata_sync.o \
     src/backend/distributed/planner/deparse_shard_query.o \
     src/backend/distributed/planner/distributed_planner.o \
     src/backend/distributed/planner/insert_select_planner.o \
     src/backend/distributed/planner/multi_explain.o \
     src/backend/distributed/planner/multi_join_order.o \
     src/backend/distributed/planner/multi_logical_optimizer.o \
     src/backend/distributed/planner/multi_logical_planner.o \
     src/backend/distributed/planner/multi_master_planner.o \
     src/backend/distributed/planner/multi_physical_planner.o \
     src/backend/distributed/planner/query_colocation_checker.o \
     src/backend/distributed/planner/multi_router_planner.o \
     src/backend/distributed/planner/postgres_planning_functions.o \
     src/backend/distributed/planner/recursive_planning.o \
     src/backend/distributed/planner/relation_restriction_equivalence.o \
     src/backend/distributed/planner/shard_pruning.o \
     src/backend/distributed/progress/multi_progress.o \
     src/backend/distributed/relay/relay_event_utility.o \
     src/backend/distributed/test/colocation_utils.o \
     src/backend/distributed/test/create_shards.o \
     src/backend/distributed/test/deparse_shard_query.o \
     src/backend/distributed/test/distributed_deadlock_detection.o \
     src/backend/distributed/test/distribution_metadata.o \
     src/backend/distributed/test/fake_fdw.o \
     src/backend/distributed/test/generate_ddl_commands.o \
     src/backend/distributed/test/metadata_sync.o \
     src/backend/distributed/test/partitioning_utils.o \
     src/backend/distributed/test/progress_utils.o \
     src/backend/distributed/test/prune_shard_list.o \
     src/backend/distributed/transaction/backend_data.o \
     src/backend/distributed/transaction/distributed_deadlock_detection.o \
     src/backend/distributed/transaction/lock_graph.o \
     src/backend/distributed/transaction/multi_shard_transaction.o \
     src/backend/distributed/transaction/remote_transaction.o \
     src/backend/distributed/transaction/transaction_management.o \
     src/backend/distributed/transaction/transaction_recovery.o \
     src/backend/distributed/transaction/worker_transaction.o \
     src/backend/distributed/utils/citus_clauses.o \
     src/backend/distributed/utils/citus_copyfuncs.o \
     src/backend/distributed/utils/citus_nodefuncs.o \
     src/backend/distributed/utils/citus_outfuncs.o \
     src/backend/distributed/utils/citus_readfuncs.o \
     src/backend/distributed/utils/citus_ruleutils.o \
     src/backend/distributed/utils/citus_version.o \
     src/backend/distributed/utils/colocation_utils.o \
     src/backend/distributed/utils/distribution_column.o \
     src/backend/distributed/utils/errormessage.o \
     src/backend/distributed/utils/hash_helpers.o \
     src/backend/distributed/utils/listutils.o \
     src/backend/distributed/utils/maintenanced.o \
     src/backend/distributed/utils/metadata_cache.o \
     src/backend/distributed/utils/multi_partitioning_utils.o \
     src/backend/distributed/utils/multi_resowner.o \
     src/backend/distributed/utils/node_metadata.o \
     src/backend/distributed/utils/reference_table_utils.o \
     src/backend/distributed/utils/resource_lock.o \
     src/backend/distributed/utils/ruleutils_10.o \
     src/backend/distributed/utils/ruleutils_96.o \
     src/backend/distributed/utils/shardinterval_utils.o \
     src/backend/distributed/utils/statistics_collection.o \
     src/backend/distributed/worker/task_tracker.o \
     src/backend/distributed/worker/task_tracker_protocol.o \
     src/backend/distributed/worker/worker_data_fetch_protocol.o \
     src/backend/distributed/worker/worker_drop_protocol.o \
     src/backend/distributed/worker/worker_file_access_protocol.o \
     src/backend/distributed/worker/worker_merge_protocol.o \
     src/backend/distributed/worker/worker_partition_protocol.o \
     src/backend/distributed/worker/worker_truncate_trigger_protocol.o \
     $(WIN32RES)

EXTENSION=
MODULE_big=
OBJS=

# the above is so that builds can work on Windows, the below is for all other platforms

citus_subdir = .
citus_top_builddir = .

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
  $(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

all: extension

# build extension
extension: $(citus_top_builddir)/src/include/citus_version.h
	$(MAKE) -C src/backend/distributed/ all
install-extension: extension
	$(MAKE) -C src/backend/distributed/ install
install-headers: extension
	$(MKDIR_P) '$(DESTDIR)$(includedir_server)/distributed/'
# generated headers are located in the build directory
	$(INSTALL_DATA) $(citus_top_builddir)/src/include/citus_version.h '$(DESTDIR)$(includedir_server)/'
# the rest in the source tree
	$(INSTALL_DATA) $(citus_abs_srcdir)/src/include/distributed/*.h '$(DESTDIR)$(includedir_server)/distributed/'
clean-extension:
	$(MAKE) -C src/backend/distributed/ clean
.PHONY: extension install-extension clean-extension
# Add to generic targets
install: install-extension install-headers
clean: clean-extension

# apply or check style
reindent:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet
check-style:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet --check
.PHONY: reindent check-style

# depend on install for now
check: all install
	$(MAKE) -C src/test/regress check-full

.PHONY: all check install clean
