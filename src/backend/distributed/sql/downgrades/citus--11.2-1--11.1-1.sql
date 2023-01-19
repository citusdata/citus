-- citus--11.2-1--11.1-1
#include "../udfs/get_rebalance_progress/11.1-1.sql"
#include "../udfs/citus_isolation_test_session_is_blocked/11.1-1.sql"
DROP FUNCTION pg_catalog.citus_get_node_clock();
DROP FUNCTION pg_catalog.citus_get_transaction_clock();
DROP FUNCTION pg_catalog.citus_internal_adjust_local_clock_to_remote(cluster_clock);
DROP FUNCTION pg_catalog.citus_is_clock_after(cluster_clock, cluster_clock);
DROP FUNCTION pg_catalog.citus_job_list();
DROP FUNCTION pg_catalog.citus_job_status(bigint,boolean);
DROP FUNCTION pg_catalog.citus_rebalance_status(boolean);
DROP FUNCTION pg_catalog.cluster_clock_logical(cluster_clock);
DROP SEQUENCE pg_catalog.pg_dist_clock_logical_seq;
DROP OPERATOR CLASS pg_catalog.cluster_clock_ops USING btree CASCADE;
DROP OPERATOR FAMILY pg_catalog.cluster_clock_ops USING btree CASCADE;
DROP TYPE pg_catalog.cluster_clock CASCADE;
DROP FUNCTION pg_catalog.worker_split_shard_replication_setup(pg_catalog.split_shard_info[], bigint);
DROP TYPE pg_catalog.replication_slot_info;
DROP TYPE pg_catalog.split_shard_info;

CREATE FUNCTION pg_catalog.worker_append_table_to_shard(text, text, text, integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_append_table_to_shard$$;
COMMENT ON FUNCTION pg_catalog.worker_append_table_to_shard(text, text, text, integer)
    IS 'append a regular table''s contents to the shard';

#include "../udfs/worker_split_shard_replication_setup/11.1-1.sql"
DROP FUNCTION pg_catalog.citus_task_wait(bigint, pg_catalog.citus_task_status);
#include "../udfs/citus_prepare_pg_upgrade/11.1-1.sql"
#include "../udfs/citus_finish_pg_upgrade/11.1-1.sql"

DROP FUNCTION pg_catalog.citus_copy_shard_placement(bigint, integer, integer, citus.shard_transfer_mode);
DROP FUNCTION pg_catalog.citus_move_shard_placement(bigint, integer, integer, citus.shard_transfer_mode);
DROP FUNCTION pg_catalog.citus_internal_add_placement_metadata(bigint, bigint, integer, bigint);
#include "../udfs/citus_internal_add_placement_metadata/10.2-1.sql"
