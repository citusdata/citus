-- citus--12.2-1--12.1-1

DROP FUNCTION citus_internal.database_command(text);
DROP FUNCTION citus_internal.acquire_citus_advisory_object_class_lock(int, cstring);

#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"

DROP FUNCTION citus_internal.start_management_transaction(
    outer_xid xid8
);

DROP FUNCTION citus_internal.execute_command_on_remote_nodes_as_user(
    query text,
    username text
);

DROP FUNCTION citus_internal.mark_object_distributed(
    classId Oid, objectName text, objectId Oid, connectionUser text
);

DROP FUNCTION pg_catalog.citus_unmark_object_distributed(oid,oid,int,boolean);
#include "../udfs/citus_unmark_object_distributed/10.0-1.sql"

DROP FUNCTION citus_internal.commit_management_command_2pc();

ALTER TABLE pg_catalog.pg_dist_transaction DROP COLUMN outer_xid;
REVOKE USAGE ON SCHEMA citus_internal FROM PUBLIC;

DROP FUNCTION citus_internal.add_colocation_metadata(int, int, int, regtype, oid);
DROP FUNCTION citus_internal.add_object_metadata(text, text[], text[], integer, integer, boolean);
DROP FUNCTION citus_internal.add_partition_metadata(regclass, "char", text, integer, "char");
DROP FUNCTION citus_internal.add_placement_metadata(bigint, bigint, integer, bigint);
DROP FUNCTION citus_internal.add_shard_metadata(regclass, bigint, "char", text, text);
DROP FUNCTION citus_internal.add_tenant_schema(oid, integer);
DROP FUNCTION citus_internal.adjust_local_clock_to_remote(pg_catalog.cluster_clock);
DROP FUNCTION citus_internal.delete_colocation_metadata(int);
DROP FUNCTION citus_internal.delete_partition_metadata(regclass);
DROP FUNCTION citus_internal.delete_placement_metadata(bigint);
DROP FUNCTION citus_internal.delete_shard_metadata(bigint);
DROP FUNCTION citus_internal.delete_tenant_schema(oid);
DROP FUNCTION citus_internal.local_blocked_processes();
#include "../udfs/citus_blocking_pids/11.0-1.sql"
#include "../udfs/citus_isolation_test_session_is_blocked/11.1-1.sql"
DROP VIEW IF EXISTS pg_catalog.citus_lock_waits;
#include "../udfs/citus_lock_waits/11.0-1.sql"
DROP FUNCTION citus_internal.global_blocked_processes();

DROP FUNCTION citus_internal.mark_node_not_synced(int, int);
DROP FUNCTION citus_internal.unregister_tenant_schema_globally(oid, text);
#include "../udfs/citus_drop_trigger/12.0-1.sql"
DROP FUNCTION citus_internal.update_none_dist_table_metadata(oid, "char", bigint, boolean);
DROP FUNCTION citus_internal.update_placement_metadata(bigint, integer, integer);
DROP FUNCTION citus_internal.update_relation_colocation(oid, int);
DROP FUNCTION citus_internal.start_replication_origin_tracking();
DROP FUNCTION citus_internal.stop_replication_origin_tracking();
DROP FUNCTION citus_internal.is_replication_origin_tracking_active();
#include "../udfs/citus_finish_pg_upgrade/12.1-1.sql"
