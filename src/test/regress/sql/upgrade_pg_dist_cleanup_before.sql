\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int < 10
AS upgrade_test_old_citus_version_lt_10_0;
\gset
\if :upgrade_test_old_citus_version_lt_10_0
\else
\q
\endif

-- create a table with orphaned shards to see if orphaned shards will be dropped
-- and cleanup records will be created for them
CREATE TABLE table_with_orphaned_shards (a int);
SELECT create_distributed_table('table_with_orphaned_shards', 'a');
-- show all 32 placements are active
SELECT COUNT(*) FROM pg_dist_placement WHERE shardstate = 1 AND shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='table_with_orphaned_shards'::regclass);
-- mark one shard as orphaned
INSERT INTO pg_dist_placement(placementid, shardid, shardstate, shardlength, groupid)
    SELECT nextval('pg_dist_placement_placementid_seq'::regclass), shardid, 4, shardlength, 3-groupid
    FROM pg_dist_placement
    WHERE shardid % 32 = 1 AND shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='table_with_orphaned_shards'::regclass)
