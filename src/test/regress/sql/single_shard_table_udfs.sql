CREATE SCHEMA null_dist_key_udfs;
SET search_path TO null_dist_key_udfs;

SET citus.next_shard_id TO 1820000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 198000;
SET client_min_messages TO ERROR;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid=>0);
RESET client_min_messages;
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
	1820000,
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

-- test update_distributed_table_colocation
CREATE TABLE update_col_1 (a INT);
CREATE TABLE update_col_2 (a INT);
CREATE TABLE update_col_3 (a INT);

-- create colocated single shard distributed tables, so the shards will be
-- in the same worker node
SELECT create_distributed_table ('update_col_1', null, colocate_with:='none');
SELECT create_distributed_table ('update_col_2', null, colocate_with:='update_col_1');

-- now create a third single shard distributed table that is not colocated,
-- with the new colocation id the new table will be in the other worker node
SELECT create_distributed_table ('update_col_3', null, colocate_with:='none');

-- make sure nodes are correct
SELECT c1.nodeport = c2.nodeport AS same_node
FROM citus_shards c1, citus_shards c2, pg_dist_node p1, pg_dist_node p2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_2' AND
      p1.nodeport = c1.nodeport AND p2.nodeport = c2.nodeport AND
      p1.noderole = 'primary' AND p2.noderole = 'primary';

SELECT c1.nodeport = c2.nodeport AS same_node
FROM citus_shards c1, citus_shards c2, pg_dist_node p1, pg_dist_node p2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_3' AND
      p1.nodeport = c1.nodeport AND p2.nodeport = c2.nodeport AND
      p1.noderole = 'primary' AND p2.noderole = 'primary';

-- and the update_col_1 and update_col_2 are colocated
SELECT c1.colocation_id = c2.colocation_id AS colocated
FROM public.citus_tables c1, public.citus_tables c2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_2';

-- break the colocation
SELECT update_distributed_table_colocation('update_col_2', colocate_with:='none');

SELECT c1.colocation_id = c2.colocation_id AS colocated
FROM public.citus_tables c1, public.citus_tables c2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_2';

-- re-colocate, the shards were already in the same node
SELECT update_distributed_table_colocation('update_col_2', colocate_with:='update_col_1');

SELECT c1.colocation_id = c2.colocation_id AS colocated
FROM public.citus_tables c1, public.citus_tables c2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_2';

-- update_col_1 and update_col_3 are not colocated, because they are not in the some node
SELECT c1.colocation_id = c2.colocation_id AS colocated
FROM public.citus_tables c1, public.citus_tables c2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_3';

-- they should not be able to be colocated since the shards are in different nodes
SELECT update_distributed_table_colocation('update_col_3', colocate_with:='update_col_1');

SELECT c1.colocation_id = c2.colocation_id AS colocated
FROM public.citus_tables c1, public.citus_tables c2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_3';

-- hash distributed and single shard distributed tables cannot be colocated
CREATE TABLE update_col_4 (a INT);
SELECT create_distributed_table ('update_col_4', 'a', colocate_with:='none');

SELECT update_distributed_table_colocation('update_col_1', colocate_with:='update_col_4');
SELECT update_distributed_table_colocation('update_col_4', colocate_with:='update_col_1');

-- test columnar UDFs
CREATE TABLE columnar_tbl (a INT) USING COLUMNAR;
SELECT * FROM columnar.options WHERE relation = 'columnar_tbl'::regclass;
SELECT alter_columnar_table_set('columnar_tbl', compression_level => 2);
SELECT * FROM columnar.options WHERE relation = 'columnar_tbl'::regclass;
SELECT alter_columnar_table_reset('columnar_tbl', compression_level => true);
SELECT * FROM columnar.options WHERE relation = 'columnar_tbl'::regclass;

SELECT columnar_internal.upgrade_columnar_storage(c.oid)
FROM pg_class c, pg_am a
WHERE c.relam = a.oid AND amname = 'columnar' AND relname = 'columnar_tbl';

SELECT columnar_internal.downgrade_columnar_storage(c.oid)
FROM pg_class c, pg_am a
WHERE c.relam = a.oid AND amname = 'columnar' AND relname = 'columnar_tbl';

CREATE OR REPLACE FUNCTION columnar_storage_info(
    rel regclass,
    version_major OUT int4,
    version_minor OUT int4,
    storage_id OUT int8,
    reserved_stripe_id OUT int8,
    reserved_row_number OUT int8,
    reserved_offset OUT int8)
  STRICT
  LANGUAGE c AS 'citus', $$columnar_storage_info$$;

SELECT version_major, version_minor, reserved_stripe_id, reserved_row_number, reserved_offset FROM columnar_storage_info('columnar_tbl');

SELECT columnar.get_storage_id(oid) = storage_id FROM pg_class, columnar_storage_info('columnar_tbl') WHERE relname = 'columnar_tbl';

SET client_min_messages TO WARNING;
DROP SCHEMA null_dist_key_udfs CASCADE;
