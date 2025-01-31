\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int > 11 OR
       (substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int = 11 AND
        substring(:'upgrade_test_old_citus_version', 'v\d+\.(\d+)\.\d+')::int >= 2)
AS upgrade_test_old_citus_version_gte_11_2;
\gset
\if :upgrade_test_old_citus_version_gte_11_2
\q
\endif

-- verify that the orphaned placement is deleted and cleanup record is created
SELECT COUNT(*) FROM pg_dist_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='table_with_orphaned_shards'::regclass);
SELECT * FROM pg_dist_cleanup;
CALL citus_cleanup_orphaned_resources();
DROP TABLE table_with_orphaned_shards;

-- Re-enable automatic shard cleanup by maintenance daemon as
-- we have disabled it in upgrade_pg_dist_cleanup_before.sql
ALTER SYSTEM RESET citus.defer_shard_delete_interval;
SELECT pg_reload_conf();
