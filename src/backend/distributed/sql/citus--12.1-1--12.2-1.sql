-- citus--12.1-1--12.2-1
-- bump version to 12.2-1

#include "udfs/citus_internal_database_command/12.2-1.sql"
#include "udfs/citus_add_rebalance_strategy/12.2-1.sql"

#include "udfs/start_management_transaction/12.2-1.sql"
#include "udfs/execute_command_on_remote_nodes_as_user/12.2-1.sql"
#include "udfs/mark_object_distributed/12.2-1.sql"
DROP FUNCTION pg_catalog.citus_unmark_object_distributed(oid, oid, int);
#include "udfs/citus_unmark_object_distributed/12.2-1.sql"
#include "udfs/commit_management_command_2pc/12.2-1.sql"

ALTER TABLE pg_catalog.pg_dist_transaction ADD COLUMN outer_xid xid8;

#include "udfs/citus_internal_acquire_citus_advisory_object_class_lock/12.2-1.sql"

GRANT USAGE ON SCHEMA citus_internal TO PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.commit_management_command_2pc FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.execute_command_on_remote_nodes_as_user FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.find_groupid_for_node FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.mark_object_distributed FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.pg_dist_node_trigger_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.pg_dist_rebalance_strategy_trigger_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.pg_dist_shard_placement_trigger_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.refresh_isolation_tester_prepared_statement FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.replace_isolation_tester_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.restore_isolation_tester_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.start_management_transaction FROM PUBLIC;

#include "udfs/citus_internal_add_colocation_metadata/12.2-1.sql"
#include "udfs/citus_internal_add_object_metadata/12.2-1.sql"
#include "udfs/citus_internal_add_partition_metadata/12.2-1.sql"
#include "udfs/citus_internal_add_placement_metadata/12.2-1.sql"
#include "udfs/citus_internal_add_shard_metadata/12.2-1.sql"
#include "udfs/citus_internal_add_tenant_schema/12.2-1.sql"
#include "udfs/citus_internal_adjust_local_clock_to_remote/12.2-1.sql"
#include "udfs/citus_internal_delete_colocation_metadata/12.2-1.sql"
#include "udfs/citus_internal_delete_partition_metadata/12.2-1.sql"
#include "udfs/citus_internal_delete_placement_metadata/12.2-1.sql"
#include "udfs/citus_internal_delete_shard_metadata/12.2-1.sql"
#include "udfs/citus_internal_delete_tenant_schema/12.2-1.sql"
#include "udfs/citus_internal_local_blocked_processes/12.2-1.sql"
#include "udfs/citus_internal_global_blocked_processes/12.2-1.sql"
#include "udfs/citus_blocking_pids/12.2-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/12.2-1.sql"
DROP VIEW IF EXISTS pg_catalog.citus_lock_waits;
#include "udfs/citus_lock_waits/12.2-1.sql"

#include "udfs/citus_internal_mark_node_not_synced/12.2-1.sql"
#include "udfs/citus_internal_unregister_tenant_schema_globally/12.2-1.sql"
#include "udfs/citus_drop_trigger/12.2-1.sql"
#include "udfs/citus_internal_update_none_dist_table_metadata/12.2-1.sql"
#include "udfs/citus_internal_update_placement_metadata/12.2-1.sql"
#include "udfs/citus_internal_update_relation_colocation/12.2-1.sql"
#include "udfs/repl_origin_helper/12.2-1.sql"
#include "udfs/citus_finish_pg_upgrade/12.2-1.sql"
