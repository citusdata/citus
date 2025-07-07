-- citus--13.0-1--13.1-1
-- bump version to 13.1-1

#include "udfs/citus_internal_database_command/13.1-1.sql"
#include "udfs/citus_add_rebalance_strategy/13.1-1.sql"

DROP FUNCTION pg_catalog.citus_unmark_object_distributed(oid, oid, int);
#include "udfs/citus_unmark_object_distributed/13.1-1.sql"

ALTER TABLE pg_catalog.pg_dist_transaction ADD COLUMN outer_xid xid8;

#include "udfs/citus_internal_acquire_citus_advisory_object_class_lock/13.1-1.sql"

GRANT USAGE ON SCHEMA citus_internal TO PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.find_groupid_for_node FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.pg_dist_node_trigger_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.pg_dist_rebalance_strategy_trigger_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.pg_dist_shard_placement_trigger_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.refresh_isolation_tester_prepared_statement FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.replace_isolation_tester_func FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_internal.restore_isolation_tester_func FROM PUBLIC;

#include "udfs/citus_internal_add_colocation_metadata/13.1-1.sql"
#include "udfs/citus_internal_add_object_metadata/13.1-1.sql"
#include "udfs/citus_internal_add_partition_metadata/13.1-1.sql"
#include "udfs/citus_internal_add_placement_metadata/13.1-1.sql"
#include "udfs/citus_internal_add_shard_metadata/13.1-1.sql"
#include "udfs/citus_internal_add_tenant_schema/13.1-1.sql"
#include "udfs/citus_internal_adjust_local_clock_to_remote/13.1-1.sql"
#include "udfs/citus_internal_delete_colocation_metadata/13.1-1.sql"
#include "udfs/citus_internal_delete_partition_metadata/13.1-1.sql"
#include "udfs/citus_internal_delete_placement_metadata/13.1-1.sql"
#include "udfs/citus_internal_delete_shard_metadata/13.1-1.sql"
#include "udfs/citus_internal_delete_tenant_schema/13.1-1.sql"
#include "udfs/citus_internal_local_blocked_processes/13.1-1.sql"
#include "udfs/citus_internal_global_blocked_processes/13.1-1.sql"
#include "udfs/citus_blocking_pids/13.1-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/13.1-1.sql"
DROP VIEW IF EXISTS pg_catalog.citus_lock_waits;
#include "udfs/citus_lock_waits/13.1-1.sql"

#include "udfs/citus_internal_mark_node_not_synced/13.1-1.sql"
#include "udfs/citus_internal_unregister_tenant_schema_globally/13.1-1.sql"
#include "udfs/citus_drop_trigger/13.1-1.sql"
#include "udfs/citus_internal_update_none_dist_table_metadata/13.1-1.sql"
#include "udfs/citus_internal_update_placement_metadata/13.1-1.sql"
#include "udfs/citus_internal_update_relation_colocation/13.1-1.sql"
#include "udfs/repl_origin_helper/13.1-1.sql"
#include "udfs/citus_finish_pg_upgrade/13.1-1.sql"
#include "udfs/citus_is_primary_node/13.1-1.sql"
#include "udfs/citus_stat_counters/13.1-1.sql"
#include "udfs/citus_stat_counters_reset/13.1-1.sql"
#include "udfs/citus_nodes/13.1-1.sql"
#include "udfs/citus_column_stats/13.1-1.sql"

-- Since shard_name/13.1-1.sql first drops the function and then creates it, we first
-- need to drop citus_shards view since that view depends on this function. And immediately
-- after creating the function, we recreate citus_shards view again.
DROP VIEW pg_catalog.citus_shards;
#include "udfs/shard_name/13.1-1.sql"
#include "udfs/citus_shards/12.0-1.sql"
