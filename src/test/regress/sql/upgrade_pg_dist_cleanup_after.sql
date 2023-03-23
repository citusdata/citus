\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int < 10
AS upgrade_test_old_citus_version_lt_10_0;
\gset
\if :upgrade_test_old_citus_version_lt_10_0
\else
\q
\endif

-- verify that the orphaned placement is deleted and cleanup record is created
SELECT COUNT(*) FROM pg_dist_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='table_with_orphaned_shards'::regclass);
SELECT * FROM pg_dist_cleanup;
CALL citus_cleanup_orphaned_resources();
DROP TABLE table_with_orphaned_shards;
