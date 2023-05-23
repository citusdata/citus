CREATE SCHEMA null_dist_key_udfs;
SET search_path TO null_dist_key_udfs;

SET citus.next_shard_id TO 1720000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
-- test some other udf's with single shard tables
CREATE TABLE null_dist_key_table(a int);
SELECT create_distributed_table('null_dist_key_table', null, colocate_with=>'none', distribution_type=>null);

SELECT truncate_local_data_after_distributing_table('null_dist_key_table');

-- should work --
-- insert some data & create an index for table size udf's
INSERT INTO null_dist_key_table VALUES (1), (2), (3);
CREATE INDEX null_dist_key_idx ON null_dist_key_table(a);

SELECT citus_table_size('null_dist_key_table');
SELECT citus_total_relation_size('null_dist_key_table');
SELECT citus_relation_size('null_dist_key_table');
SELECT * FROM pg_catalog.citus_shard_sizes() WHERE table_name LIKE '%null_dist_key_table%';

BEGIN;
  SELECT lock_relation_if_exists('null_dist_key_table', 'ACCESS SHARE');
  SELECT count(*) FROM pg_locks where relation='null_dist_key_table'::regclass;
COMMIT;

SELECT partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid = 'null_dist_key_table'::regclass;
SELECT master_get_table_ddl_events('null_dist_key_table');

SELECT column_to_column_name(logicalrelid, partkey)
FROM pg_dist_partition WHERE logicalrelid = 'null_dist_key_table'::regclass;

SELECT column_name_to_column('null_dist_key_table', 'a');

SELECT master_update_shard_statistics(shardid)
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='null_dist_key_table'::regclass) as shardid;

SELECT truncate_local_data_after_distributing_table('null_dist_key_table');

-- should return a single element array that only includes its own shard id
SELECT shardid=unnest(get_colocated_shard_array(shardid))
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='null_dist_key_table'::regclass) as shardid;

BEGIN;
  SELECT master_remove_partition_metadata('null_dist_key_table'::regclass::oid, 'null_dist_key_udfs', 'null_dist_key_table');

  -- should print 0
  select count(*) from pg_dist_partition where logicalrelid='null_dist_key_table'::regclass;
ROLLBACK;

-- should fail --

SELECT update_distributed_table_colocation('null_dist_key_table', colocate_with => 'none');

SELECT master_create_empty_shard('null_dist_key_table');

-- return true
SELECT citus_table_is_visible('null_dist_key_table'::regclass::oid);

-- return false
SELECT relation_is_a_known_shard('null_dist_key_table');

-- return | false | true |
SELECT citus_table_is_visible(tableName::regclass::oid), relation_is_a_known_shard(tableName::regclass)
FROM (SELECT tableName FROM pg_catalog.pg_tables WHERE tablename LIKE 'null_dist_key_table%') as tableName;

-- should fail, maybe support in the future
SELECT create_reference_table('null_dist_key_table');
SELECT create_distributed_table('null_dist_key_table', 'a');
SELECT create_distributed_table_concurrently('null_dist_key_table', 'a');
SELECT citus_add_local_table_to_metadata('null_dist_key_table');

-- test altering distribution column, fails for single shard tables
SELECT alter_distributed_table('null_dist_key_table', distribution_column := 'a');

-- test altering shard count, fails for single shard tables
SELECT alter_distributed_table('null_dist_key_table', shard_count := 6);

-- test shard splitting udf, fails for single shard tables
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
SELECT citus_split_shard_by_split_points(
	1720000,
	ARRAY['-1073741826'],
	ARRAY[:worker_1_node, :worker_2_node],
    'block_writes');

SELECT colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE '%null_dist_key_table%';
-- test alter_table_set_access_method and verify it doesn't change the colocation id
SELECT alter_table_set_access_method('null_dist_key_table', 'columnar');
SELECT colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE '%null_dist_key_table%';

-- undistribute
SELECT undistribute_table('null_dist_key_table');
-- verify that the metadata is gone
SELECT COUNT(*) = 0 FROM pg_dist_partition WHERE logicalrelid::text LIKE '%null_dist_key_table%';
SELECT COUNT(*) = 0 FROM pg_dist_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid::text LIKE '%null_dist_key_table%');
SELECT COUNT(*) = 0 FROM pg_dist_shard WHERE logicalrelid::text LIKE '%null_dist_key_table%';

DROP SCHEMA null_dist_key_udfs CASCADE;
