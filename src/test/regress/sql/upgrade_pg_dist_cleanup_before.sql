\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int > 11 OR
       (substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int = 11 AND
        substring(:'upgrade_test_old_citus_version', 'v\d+\.(\d+)\.\d+')::int >= 2)
AS upgrade_test_old_citus_version_gte_11_2;
\gset
\if :upgrade_test_old_citus_version_gte_11_2
\q
\endif

-- create a table with orphaned shards to see if orphaned shards will be dropped
-- and cleanup records will be created for them
SET citus.next_shard_id TO 980000;
CREATE TABLE table_with_orphaned_shards (a int);
SELECT create_distributed_table('table_with_orphaned_shards', 'a');
-- show all 32 placements are active
SELECT COUNT(*) FROM pg_dist_placement WHERE shardstate = 1 AND shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='table_with_orphaned_shards'::regclass);
-- create an orphaned placement based on an existing one
--
-- But before doing that, first disable automatic shard cleanup
-- by maintenance daemon so that we can reliably test the cleanup
-- in upgrade_pg_dist_cleanup_after.sql.

ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();

SELECT pg_sleep(0.1);

INSERT INTO pg_dist_placement(placementid, shardid, shardstate, shardlength, groupid)
    SELECT nextval('pg_dist_placement_placementid_seq'::regclass), shardid, 4, shardlength, 3-groupid
    FROM pg_dist_placement
    WHERE shardid = 980001;
