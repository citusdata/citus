-- citus--13.1-1--13.0-1

DROP FUNCTION citus_internal.database_command(text);
DROP FUNCTION citus_internal.acquire_citus_advisory_object_class_lock(int, cstring);

#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"

DROP FUNCTION pg_catalog.citus_unmark_object_distributed(oid,oid,int,boolean);
#include "../udfs/citus_unmark_object_distributed/10.0-1.sql"

ALTER TABLE pg_catalog.pg_dist_transaction DROP COLUMN outer_xid;
REVOKE USAGE ON SCHEMA citus_internal FROM PUBLIC;

DROP FUNCTION pg_catalog.citus_is_primary_node();
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

DROP VIEW pg_catalog.citus_stat_counters;
DROP FUNCTION pg_catalog.citus_stat_counters(oid);
DROP FUNCTION pg_catalog.citus_stat_counters_reset(oid);
DROP VIEW IF EXISTS pg_catalog.citus_nodes;
DROP FUNCTION IF EXISTS pg_catalog.citus_column_stats;

-- Definition of shard_name() prior to this release doesn't have a separate SQL file
-- because it's quite an old UDF that its prior definition(s) was(were) squashed into
-- citus--8.0-1.sql. For this reason, to downgrade it, here we directly execute its old
-- definition instead of including it from such a separate file.
--
-- And before dropping and creating the function, we also need to drop citus_shards view
-- since it depends on it. And immediately after creating the function, we recreate
-- citus_shards view again.

DROP VIEW pg_catalog.citus_shards;

DROP FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint, skip_qualify_public boolean);
CREATE FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint)
    RETURNS text
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$shard_name$$;
COMMENT ON FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint)
    IS 'returns schema-qualified, shard-extended identifier of object name';

#include "../udfs/citus_shards/12.0-1.sql"
