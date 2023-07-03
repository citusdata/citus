CREATE SCHEMA null_dist_key_udfs;
SET search_path TO null_dist_key_udfs;

SET citus.next_shard_id TO 1820000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 198000;
SET client_min_messages TO ERROR;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid=>0);
RESET client_min_messages;

CREATE FUNCTION get_referencing_relation_id_list(Oid)
RETURNS SETOF Oid
LANGUAGE C STABLE STRICT
AS 'citus', $$get_referencing_relation_id_list$$;

CREATE FUNCTION get_referenced_relation_id_list(Oid)
RETURNS SETOF Oid
LANGUAGE C STABLE STRICT
AS 'citus', $$get_referenced_relation_id_list$$;

CREATE OR REPLACE FUNCTION get_foreign_key_connected_relations(IN table_name regclass)
RETURNS SETOF RECORD
LANGUAGE C STRICT
AS 'citus', $$get_foreign_key_connected_relations$$;

CREATE OR REPLACE FUNCTION citus_get_all_dependencies_for_object(classid oid, objid oid, objsubid int)
RETURNS SETOF RECORD
LANGUAGE C STRICT
AS 'citus', $$citus_get_all_dependencies_for_object$$;

CREATE OR REPLACE FUNCTION citus_get_dependencies_for_object(classid oid, objid oid, objsubid int)
RETURNS SETOF RECORD
LANGUAGE C STRICT
AS 'citus', $$citus_get_dependencies_for_object$$;

CREATE OR REPLACE FUNCTION pg_catalog.is_citus_depended_object(oid,oid)
RETURNS bool
LANGUAGE C
AS 'citus', $$is_citus_depended_object$$;

CREATE FUNCTION shards_colocated(bigint, bigint)
RETURNS bool
AS 'citus'
LANGUAGE C STRICT;

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
SELECT shard_name, shard_size FROM pg_catalog.citus_shard_sizes(), citus_shards
WHERE shardid = shard_id AND shard_name LIKE '%null_dist_key_table%' AND nodeport IN (:worker_1_port, :worker_2_port);

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

-- create 7 single shard tables, 3 of them are colocated, for testing shard moves / rebalance on them
CREATE TABLE single_shard_table_col1_1 (a INT PRIMARY KEY);
CREATE TABLE single_shard_table_col1_2 (a TEXT PRIMARY KEY);
CREATE TABLE single_shard_table_col1_3 (a TIMESTAMP PRIMARY KEY);
CREATE TABLE single_shard_table_col2_1 (a INT PRIMARY KEY);
CREATE TABLE single_shard_table_col3_1 (a INT PRIMARY KEY);
CREATE TABLE single_shard_table_col4_1 (a INT PRIMARY KEY);
CREATE TABLE single_shard_table_col5_1 (a INT PRIMARY KEY);
SELECT create_distributed_table('single_shard_table_col1_1', null, colocate_with=>'none');
SELECT create_distributed_table('single_shard_table_col1_2', null, colocate_with=>'single_shard_table_col1_1');
SELECT create_distributed_table('single_shard_table_col1_3', null, colocate_with=>'single_shard_table_col1_2');
SELECT create_distributed_table('single_shard_table_col2_1', null, colocate_with=>'none');
SELECT create_distributed_table('single_shard_table_col3_1', null, colocate_with=>'none');
SELECT create_distributed_table('single_shard_table_col4_1', null, colocate_with=>'none');
SELECT create_distributed_table('single_shard_table_col5_1', null, colocate_with=>'none');

-- initial status
SELECT shardid, nodeport FROM pg_dist_shard_placement WHERE shardid > 1820000 ORDER BY shardid;

-- errors out because streaming replicated
SELECT citus_copy_shard_placement(1820005, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
SELECT master_copy_shard_placement(1820005, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
SELECT citus_copy_shard_placement(1820005, :worker_1_node, :worker_2_node);

-- no changes because it's already balanced
SELECT rebalance_table_shards(rebalance_strategy := 'by_shard_count');

-- same placements
SELECT shardid, nodeport FROM pg_dist_shard_placement WHERE shardid > 1820000 ORDER BY shardid;

-- manually move 2 shard from 2 colocation groups to make the cluster unbalanced
SELECT citus_move_shard_placement(1820005, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
SELECT citus_move_shard_placement(1820007, :worker_1_node, :worker_2_node);

-- all placements are located on worker 2
SELECT shardid, nodeport FROM pg_dist_shard_placement WHERE shardid > 1820000 ORDER BY shardid;

-- move some of them to worker 1 to balance the cluster
SELECT rebalance_table_shards();

-- the final status, balanced
SELECT shardid, nodeport FROM pg_dist_shard_placement WHERE shardid > 1820000 ORDER BY shardid;

-- verify we didn't break any colocations
SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE '%single_shard_table_col%' ORDER BY colocationid, logicalrelid;

-- drop preexisting tables
-- we can remove the drop commands once the issue is fixed: https://github.com/citusdata/citus/issues/6948
SET client_min_messages TO ERROR;
DROP TABLE IF EXISTS public.lineitem, public.orders, public.customer_append, public.part_append, public.supplier_single_shard,
    public.events, public.users, public.lineitem_hash_part, public.lineitem_subquery, public.orders_hash_part,
    public.orders_subquery, public.unlogged_table CASCADE;
DROP SCHEMA IF EXISTS with_basics, subquery_and_ctes CASCADE;
DROP TABLE IF EXISTS public.users_table, public.events_table, public.agg_results, public.agg_results_second, public.agg_results_third, public.agg_results_fourth, public.agg_results_window CASCADE;
-- drain node
SELECT citus_drain_node('localhost', :worker_2_port, 'block_writes');
SELECT citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
RESET client_min_messages;

-- see the plan for moving 4 shards, 3 of them are in the same colocation group
SELECT * FROM get_rebalance_table_shards_plan();

-- move some of them to worker 2 to balance the cluster
SELECT 1 FROM citus_rebalance_start();

-- stop it
SELECT * FROM citus_rebalance_stop();

-- show rebalance status, see the cancelled job for two moves
SELECT state, details FROM citus_rebalance_status();

-- start again
SELECT 1 FROM citus_rebalance_start();

-- show rebalance status, scheduled a job for two moves
SELECT state, details FROM citus_rebalance_status();

-- wait for rebalance to be completed
SELECT * FROM citus_rebalance_wait();

-- the final status, balanced
SELECT shardid, nodeport FROM pg_dist_shard_placement WHERE shardid > 1820000 ORDER BY shardid;

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

-- make sure nodes are correct and test shards_colocated UDF
SELECT c1.nodeport = c2.nodeport AS same_node, shards_colocated(c1.shardid, c2.shardid)
FROM citus_shards c1, citus_shards c2, pg_dist_node p1, pg_dist_node p2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_2' AND
      p1.nodeport = c1.nodeport AND p2.nodeport = c2.nodeport AND
      p1.noderole = 'primary' AND p2.noderole = 'primary';

SELECT c1.nodeport = c2.nodeport AS same_node, shards_colocated(c1.shardid, c2.shardid)
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

-- test shards_colocated UDF with shards in same node but different colocation groups
SELECT shards_colocated(c1.shardid, c2.shardid)
FROM citus_shards c1, citus_shards c2, pg_dist_node p1, pg_dist_node p2
WHERE c1.table_name::text = 'update_col_1' AND c2.table_name::text = 'update_col_2' AND
      p1.nodeport = c1.nodeport AND p2.nodeport = c2.nodeport AND
      p1.noderole = 'primary' AND p2.noderole = 'primary';

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
SELECT create_distributed_table('columnar_tbl', NULL, colocate_with:='none');

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


-- test time series functions
CREATE TABLE part_tbl (a DATE) PARTITION BY RANGE (a);
CREATE TABLE part_tbl_1 PARTITION OF part_tbl FOR VALUES FROM ('2000-01-01') TO ('2010-01-01');
CREATE TABLE part_tbl_2 PARTITION OF part_tbl FOR VALUES FROM ('2020-01-01') TO ('2030-01-01');

SELECT create_distributed_table('part_tbl', NULL, colocate_with:='none');

SELECT * FROM time_partitions WHERE parent_table::text = 'part_tbl';

SELECT time_partition_range('part_tbl_2');

SELECT get_missing_time_partition_ranges('part_tbl', INTERVAL '10 years', '2050-01-01', '2000-01-01');

SELECT create_time_partitions('part_tbl', INTERVAL '10 years', '2050-01-01', '2000-01-01');

CALL drop_old_time_partitions('part_tbl', '2030-01-01');

SELECT * FROM time_partitions WHERE parent_table::text = 'part_tbl';

-- test locking shards
CREATE TABLE lock_tbl_1 (a INT);
SELECT create_distributed_table('lock_tbl_1', NULL, colocate_with:='none');

CREATE TABLE lock_tbl_2 (a INT);
SELECT create_distributed_table('lock_tbl_2', NULL, colocate_with:='none');

BEGIN;
SELECT lock_shard_metadata(3, array_agg(distinct(shardid)))
FROM citus_shards WHERE table_name::text = 'lock_tbl_1';

SELECT lock_shard_metadata(5, array_agg(distinct(shardid)))
FROM citus_shards WHERE table_name::text LIKE 'lock\_tbl\__';

SELECT table_name, classid, mode, granted
FROM pg_locks, public.citus_tables
WHERE
      locktype = 'advisory' AND
      table_name::text LIKE 'lock\_tbl\__' AND
      objid = colocation_id
      ORDER BY 1, 3;
END;


BEGIN;
SELECT lock_shard_resources(3, array_agg(distinct(shardid)))
FROM citus_shards WHERE table_name::text = 'lock_tbl_1';

SELECT lock_shard_resources(5, array_agg(distinct(shardid)))
FROM citus_shards WHERE table_name::text LIKE 'lock\_tbl\__';

SELECT locktype, table_name, mode, granted
FROM pg_locks, citus_shards, pg_dist_node
WHERE
      objid = shardid AND
      table_name::text LIKE 'lock\_tbl\__' AND
      citus_shards.nodeport = pg_dist_node.nodeport AND
      noderole = 'primary'
      ORDER BY 2, 3;
END;

-- test foreign key UDFs
CREATE TABLE fkey_s1 (a INT UNIQUE);
CREATE TABLE fkey_r (a INT UNIQUE);

CREATE TABLE fkey_s2 (x INT, y INT);
CREATE TABLE fkey_s3 (x INT, y INT);

SELECT create_distributed_table('fkey_s1', NULL, colocate_with:='none');
SELECT create_reference_table('fkey_r');

SELECT create_distributed_table('fkey_s2', NULL, colocate_with:='fkey_s1');
SELECT create_distributed_table('fkey_s3', NULL, colocate_with:='fkey_s1');

ALTER TABLE fkey_s2 ADD CONSTRAINT f1 FOREIGN KEY (x) REFERENCES fkey_s1 (a);
ALTER TABLE fkey_s2 ADD CONSTRAINT f2 FOREIGN KEY (y) REFERENCES fkey_r (a);

ALTER TABLE fkey_s3 ADD CONSTRAINT f3 FOREIGN KEY (x) REFERENCES fkey_s1 (a);
ALTER TABLE fkey_s3 ADD CONSTRAINT f4 FOREIGN KEY (y) REFERENCES fkey_r (a);

SELECT get_referencing_relation_id_list::regclass::text FROM get_referencing_relation_id_list('fkey_s1'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass::text FROM get_referenced_relation_id_list('fkey_s2'::regclass) ORDER BY 1;

SELECT oid::regclass::text FROM get_foreign_key_connected_relations('fkey_s1'::regclass) AS f(oid oid) ORDER BY 1;

--test dependency functions
CREATE TYPE dep_type AS (a INT);
CREATE TABLE dep_tbl(a INT, b dep_type);
SELECT create_distributed_table('dep_tbl', NULL, colocate_with:='none');
CREATE VIEW dep_view AS SELECT * FROM dep_tbl;

-- find all the dependencies of table dep_tbl
SELECT
	pg_identify_object(t.classid, t.objid, t.objsubid)
FROM
	(SELECT * FROM pg_get_object_address('table', '{dep_tbl}', '{}')) as addr
JOIN LATERAL
	citus_get_all_dependencies_for_object(addr.classid, addr.objid, addr.objsubid) as t(classid oid, objid oid, objsubid int)
ON TRUE
	ORDER BY 1;

-- find all the dependencies of view dep_view
SELECT
	pg_identify_object(t.classid, t.objid, t.objsubid)
FROM
	(SELECT * FROM pg_get_object_address('view', '{dep_view}', '{}')) as addr
JOIN LATERAL
	citus_get_all_dependencies_for_object(addr.classid, addr.objid, addr.objsubid) as t(classid oid, objid oid, objsubid int)
ON TRUE
	ORDER BY 1;

-- find non-distributed dependencies of table dep_tbl
SELECT
	pg_identify_object(t.classid, t.objid, t.objsubid)
FROM
	(SELECT * FROM pg_get_object_address('table', '{dep_tbl}', '{}')) as addr
JOIN LATERAL
	citus_get_dependencies_for_object(addr.classid, addr.objid, addr.objsubid) as t(classid oid, objid oid, objsubid int)
ON TRUE
	ORDER BY 1;

SET citus.hide_citus_dependent_objects TO true;
CREATE TABLE citus_dep_tbl (a noderole);
SELECT create_distributed_table('citus_dep_tbl', NULL, colocate_with:='none');

SELECT is_citus_depended_object('pg_class'::regclass, 'citus_dep_tbl'::regclass);
RESET citus.hide_citus_dependent_objects;

-- test replicate_reference_tables
SET client_min_messages TO WARNING;
DROP SCHEMA null_dist_key_udfs CASCADE;
RESET client_min_messages;
CREATE SCHEMA null_dist_key_udfs;
SET search_path TO null_dist_key_udfs;

SELECT citus_remove_node('localhost', :worker_2_port);

CREATE TABLE rep_ref (a INT UNIQUE);
SELECT create_reference_table('rep_ref');

CREATE TABLE rep_sing (a INT);
SELECT create_distributed_table('rep_sing', NULL, colocate_with:='none');

ALTER TABLE rep_sing ADD CONSTRAINT rep_fkey FOREIGN KEY (a) REFERENCES rep_ref(a);

SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT count(*) FROM citus_shards WHERE table_name = 'rep_ref'::regclass AND nodeport = :worker_2_port;
SELECT replicate_reference_tables('block_writes');
SELECT count(*) FROM citus_shards WHERE table_name = 'rep_ref'::regclass AND nodeport = :worker_2_port;

-- test fix_partition_shard_index_names
SET citus.next_shard_id TO 3820000;
CREATE TABLE part_tbl_sing (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
SELECT create_distributed_table('part_tbl_sing', NULL, colocate_with:='none');

-- create a partition with a long name and another with a short name
CREATE TABLE partition_table_with_very_long_name PARTITION OF part_tbl_sing FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE p PARTITION OF part_tbl_sing FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

-- create an index on parent table
-- we will see that it doesn't matter whether we name the index on parent or not
-- indexes auto-generated on partitions will not use this name
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX short ON part_tbl_sing USING btree (another_col, partition_col);

SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'null_dist_key_udfs' AND tablename SIMILAR TO 'p%' ORDER BY 1, 2;

SELECT nodeport AS part_tbl_sing_port
FROM citus_shards
WHERE table_name = 'part_tbl_sing'::regclass AND
      nodeport IN (:worker_1_port, :worker_2_port) \gset

\c - - - :part_tbl_sing_port
-- the names are generated correctly
-- shard id has been appended to all index names which didn't end in shard id
-- this goes in line with Citus's way of naming indexes of shards: always append shardid to the end
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'null_dist_key_udfs' AND tablename SIMILAR TO 'p%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO null_dist_key_udfs;

--test isolate_tenant_to_new_shard
CREATE TABLE iso_tbl (a INT);
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('iso_tbl', NULL, colocate_with:='none');
SELECT isolate_tenant_to_new_shard('iso_tbl', 5);

-- test replicate_table_shards
CREATE TABLE rep_tbl (a INT);
SELECT create_distributed_table('rep_tbl', NULL, colocate_with:='none');
SELECT replicate_table_shards('rep_tbl');

-- test debug_equality_expression
CREATE FUNCTION debug_equality_expression(regclass)
RETURNS cstring
AS 'citus'
LANGUAGE C STRICT;

CREATE TABLE debug_tbl (a INT);
SELECT create_distributed_table ('debug_tbl', NULL, colocate_with:='none');
SELECT debug_equality_expression('debug_tbl'::regclass);

-- test partition_column_id
CREATE FUNCTION partition_column_id(regclass)
RETURNS smallint
AS 'citus'
LANGUAGE C STRICT;

CREATE TABLE partcol_tbl (a INT);
SELECT create_distributed_table ('partcol_tbl', NULL, colocate_with:='none');
SELECT partition_column_id('partcol_tbl'::regclass);

-- test citus_shard_cost_by_disk_size
CREATE TABLE size_tbl_dist (a INT, b TEXT);
SELECT create_distributed_table('size_tbl_dist', 'a', shard_count:=4, colocate_with:='none');

CREATE TABLE size_tbl_single (a INT, b TEXT);
SELECT create_distributed_table('size_tbl_single', NULL, colocate_with:='none');

INSERT INTO size_tbl_dist SELECT 1, '1234567890' FROM generate_series(1, 10000);
INSERT INTO size_tbl_single SELECT 1, '1234567890' FROM generate_series(1, 10000);

SELECT citus_shard_cost_by_disk_size(c1.shardid) = citus_shard_cost_by_disk_size(c2.shardid) AS equal_cost
FROM citus_shards c1, citus_shards c2
WHERE c1.table_name::TEXT = 'size_tbl_dist' AND c2.table_name::TEXT = 'size_tbl_single'
ORDER BY c1.shard_size DESC
LIMIT 1;

-- test update statistics UDFs
CREATE TABLE update_tbl_stat (a INT, b TEXT);
SELECT create_distributed_table('update_tbl_stat', NULL, colocate_with:='none');

SELECT shardid AS update_tbl_stat_shard
FROM citus_shards
WHERE table_name::TEXT = 'update_tbl_stat'
LIMIT 1 \gset

SELECT shardlength > 0 FROM pg_dist_shard_placement WHERE shardid = :update_tbl_stat_shard LIMIT 1;

INSERT INTO update_tbl_stat SELECT 1, '1234567890' FROM generate_series(1, 10000);

SELECT shardlength > 0 FROM pg_dist_shard_placement WHERE shardid = :update_tbl_stat_shard LIMIT 1;

SELECT citus_update_table_statistics('update_tbl_stat');

SELECT shardlength > 0 FROM pg_dist_shard_placement WHERE shardid = :update_tbl_stat_shard LIMIT 1;

CREATE TABLE update_shard_stat (a INT, b TEXT);
SELECT create_distributed_table('update_shard_stat', NULL, colocate_with:='none');

SELECT shardid AS update_shard_stat_shard
FROM citus_shards
WHERE table_name::TEXT = 'update_shard_stat'
LIMIT 1 \gset

SELECT shardlength > 0 FROM pg_dist_shard_placement WHERE shardid = :update_shard_stat_shard LIMIT 1;

INSERT INTO update_shard_stat SELECT 1, '1234567890' FROM generate_series(1, 10000);

SELECT shardlength > 0 FROM pg_dist_shard_placement WHERE shardid = :update_shard_stat_shard LIMIT 1;

SELECT 1 FROM citus_update_shard_statistics(:update_shard_stat_shard);

SELECT shardlength > 0 FROM pg_dist_shard_placement WHERE shardid = :update_shard_stat_shard LIMIT 1;

-- test citus clock
SET citus.enable_cluster_clock TO ON;

CREATE TABLE clock_single(a INT);
SELECT create_distributed_table('clock_single', NULL, colocate_with:='none');

SELECT citus_get_node_clock() AS nc1 \gset
SELECT citus_get_node_clock() AS nc2 \gset
SELECT citus_get_node_clock() AS nc3 \gset

SELECT citus_is_clock_after(:'nc2', :'nc1');
SELECT citus_is_clock_after(:'nc3', :'nc2');

BEGIN;
SELECT citus_get_node_clock() AS nc4 \gset
COPY clock_single FROM STDIN;
1
2
\.
SELECT citus_get_node_clock() AS nc5 \gset
END;

SELECT citus_is_clock_after(:'nc4', :'nc3');
SELECT citus_is_clock_after(:'nc5', :'nc4');

BEGIN;
SELECT citus_get_transaction_clock();
END;

-- Transaction with single shard table access
SELECT nodeport AS clock_shard_nodeport FROM citus_shards
WHERE table_name::text = 'clock_single' AND nodeport IN (:worker_1_port, :worker_2_port) \gset

BEGIN;
COPY clock_single FROM STDIN;
1
2
\.
SELECT get_current_transaction_id() \gset tid
SET client_min_messages TO DEBUG1;
-- Capture the transaction timestamp
SELECT citus_get_transaction_clock() as txnclock \gset
COMMIT;

-- Check to see if the clock is persisted in the sequence.
SELECT result as logseq from run_command_on_workers($$SELECT last_value FROM pg_dist_clock_logical_seq$$)
WHERE nodeport = :clock_shard_nodeport \gset
SELECT cluster_clock_logical(:'txnclock') as txnlog \gset
SELECT :logseq = :txnlog;

BEGIN;
COPY clock_single FROM STDIN;
1
2
\.
SELECT get_current_transaction_id() \gset tid
SET client_min_messages TO DEBUG1;
-- Capture the transaction timestamp
SELECT citus_get_transaction_clock() as txnclock \gset
ROLLBACK;

SELECT result as logseq from run_command_on_workers($$SELECT last_value FROM pg_dist_clock_logical_seq$$)
WHERE nodeport = :clock_shard_nodeport \gset
SELECT cluster_clock_logical(:'txnclock') as txnlog \gset
SELECT :logseq = :txnlog;

-- test table with space in its name in citus_shards
CREATE TABLE "t b l" (a INT);
SELECT create_distributed_table('"t b l"', NULL, colocate_with:='none');

SELECT table_name, shard_size FROM citus_shards
WHERE table_name = '"t b l"'::regclass AND nodeport IN (:worker_1_port, :worker_2_port);

SET client_min_messages TO WARNING;
DROP SCHEMA null_dist_key_udfs CASCADE;
