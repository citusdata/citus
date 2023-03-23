-- citus--11.3-1--11.2-1

DROP FUNCTION pg_catalog.citus_internal_start_replication_origin_tracking();
DROP FUNCTION pg_catalog.citus_internal_stop_replication_origin_tracking();
DROP FUNCTION pg_catalog.citus_internal_is_replication_origin_tracking_active();
ALTER TABLE pg_catalog.pg_dist_authinfo REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_partition REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_placement REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_rebalance_strategy REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_shard REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_transaction REPLICA IDENTITY NOTHING;

ALTER TABLE pg_catalog.pg_dist_authinfo REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_partition REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_placement REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_rebalance_strategy REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_shard REPLICA IDENTITY NOTHING;
ALTER TABLE pg_catalog.pg_dist_transaction REPLICA IDENTITY NOTHING;

DROP PROCEDURE pg_catalog.worker_drop_all_shell_tables(bool);
DROP FUNCTION pg_catalog.citus_internal_mark_node_not_synced(int, int);

DROP VIEW pg_catalog.citus_stat_tenants_local;
DROP FUNCTION pg_catalog.citus_stat_tenants_local(boolean);

DROP VIEW pg_catalog.citus_stat_tenants;
DROP FUNCTION pg_catalog.citus_stat_tenants(boolean);

DROP FUNCTION pg_catalog.citus_stat_tenants_local_reset();
DROP FUNCTION pg_catalog.citus_stat_tenants_reset();

ALTER TABLE pg_catalog.pg_dist_background_task DROP COLUMN nodes_involved;
