-- citus--11.2-1--11.1-1
DROP FUNCTION pg_catalog.citus_get_cluster_clock();
DROP FUNCTION pg_catalog.citus_internal_adjust_local_clock_to_remote(cluster_clock);
DROP FUNCTION pg_catalog.citus_is_clock_after(cluster_clock, cluster_clock);
DROP TYPE pg_catalog.cluster_clock CASCADE;
DROP TABLE pg_catalog.pg_dist_commit_transaction;
