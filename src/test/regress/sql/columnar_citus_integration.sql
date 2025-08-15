
SET client_min_messages TO WARNING;
CREATE EXTENSION IF NOT EXISTS citus_columnar;
RESET client_min_messages;

-- remove coordinator if it is added to pg_dist_node
SELECT COUNT(master_remove_node(nodename, nodeport)) >= 0
FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:master_port;

SELECT success, result FROM run_command_on_all_nodes($cmd$
  ALTER SYSTEM SET columnar.compression TO 'none'
$cmd$);
SELECT success, result FROM run_command_on_all_nodes($cmd$
  SELECT pg_reload_conf()
$cmd$);

CREATE SCHEMA columnar_citus_integration;
SET search_path TO columnar_citus_integration;
SET citus.next_shard_id TO 20090000;

SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;

-- table options integration testing
-- distributed table 1 placement
CREATE TABLE table_option (a int, b text) USING columnar;
SELECT create_distributed_table('table_option', 'a');

-- setting: compression
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.compression = pglz);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.compression);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: compression_level
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.compression_level = 13);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.compression_level);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: chunk_group_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.chunk_group_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.chunk_group_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: stripe_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.stripe_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.stripe_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify settings are propagated when creating a table
CREATE TABLE table_option_2 (a int, b text) USING columnar;
ALTER TABLE table_option_2 SET
  (columnar.chunk_group_row_limit = 2000,
   columnar.stripe_row_limit = 20000,
   columnar.compression = pglz,
   columnar.compression_level = 15);
SELECT create_distributed_table('table_option_2', 'a');

-- verify settings on placements
SELECT run_command_on_placements('table_option_2',$cmd$
  SELECT ROW(chunk_group_row_limit, stripe_row_limit, compression, compression_level) FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify undistribute works
SELECT undistribute_table('table_option');
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'table_option'::regclass;
SELECT compression FROM columnar.options WHERE relation = 'table_option'::regclass;

DROP TABLE table_option, table_option_2;

-- verify settings get to all placements when there are multiple replica's
SET citus.shard_replication_factor TO 2;

-- table options integration testing
CREATE TABLE table_option (a int, b text) USING columnar;
SELECT create_distributed_table('table_option', 'a');

-- setting: compression
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.compression = pglz);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.compression);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: compression_level
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.compression_level = 17);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.compression_level);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: chunk_group_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.chunk_group_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.chunk_group_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: stripe_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option SET (columnar.stripe_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option RESET (columnar.stripe_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify settings are propagated when creating a table
CREATE TABLE table_option_2 (a int, b text) USING columnar;
ALTER TABLE table_option_2 SET
  (columnar.chunk_group_row_limit = 2000,
   columnar.stripe_row_limit = 20000,
   columnar.compression = pglz,
   columnar.compression_level = 19);
SELECT create_distributed_table('table_option_2', 'a');

-- verify settings on placements
SELECT run_command_on_placements('table_option_2',$cmd$
  SELECT ROW(chunk_group_row_limit, stripe_row_limit, compression, compression_level) FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify undistribute works
SELECT undistribute_table('table_option');
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'table_option'::regclass;
SELECT compression FROM columnar.options WHERE relation = 'table_option'::regclass;

DROP TABLE table_option, table_option_2;

-- test options on a reference table
CREATE TABLE table_option_reference (a int, b text) USING columnar;
SELECT create_reference_table('table_option_reference');

-- setting: compression
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_reference SET (columnar.compression = pglz);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_reference RESET (columnar.compression);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: compression_level
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_reference SET (columnar.compression_level = 11);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_reference RESET (columnar.compression_level);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: chunk_group_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_reference SET (columnar.chunk_group_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_reference RESET (columnar.chunk_group_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: stripe_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_reference SET (columnar.stripe_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_reference RESET (columnar.stripe_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option_reference',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify settings are propagated when creating a table
CREATE TABLE table_option_reference_2 (a int, b text) USING columnar;
ALTER TABLE table_option_reference_2 SET
  (columnar.chunk_group_row_limit = 2000,
   columnar.stripe_row_limit = 20000,
   columnar.compression = pglz,
   columnar.compression_level = 9);
SELECT create_reference_table('table_option_reference_2');

-- verify settings on placements
SELECT run_command_on_placements('table_option_reference_2',$cmd$
  SELECT ROW(chunk_group_row_limit, stripe_row_limit, compression, compression_level) FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify undistribute works
SELECT undistribute_table('table_option_reference');
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'table_option_reference'::regclass;
SELECT compression FROM columnar.options WHERE relation = 'table_option_reference'::regclass;

DROP TABLE table_option_reference, table_option_reference_2;

SET citus.shard_replication_factor TO 1;

-- test options on a citus local table
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
CREATE TABLE table_option_citus_local (a int, b text) USING columnar;
SELECT citus_add_local_table_to_metadata('table_option_citus_local');

-- setting: compression
-- get baseline for setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_citus_local SET (columnar.compression = pglz);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_citus_local RESET (columnar.compression);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT compression FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: compression_level
-- get baseline for setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_citus_local SET (columnar.compression_level = 11);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_citus_local RESET (columnar.compression_level);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT compression_level FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: chunk_group_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_citus_local SET (columnar.chunk_group_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_citus_local RESET (columnar.chunk_group_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT chunk_group_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- setting: stripe_row_limit
-- get baseline for setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- change setting
ALTER TABLE table_option_citus_local SET (columnar.stripe_row_limit = 2000);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);
-- reset setting
ALTER TABLE table_option_citus_local RESET (columnar.stripe_row_limit);
-- verify setting
SELECT run_command_on_placements('table_option_citus_local',$cmd$
  SELECT stripe_row_limit FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify settings are propagated when creating a table
CREATE TABLE table_option_citus_local_2 (a int, b text) USING columnar;
ALTER TABLE table_option_citus_local_2 SET
  (columnar.chunk_group_row_limit = 2000,
   columnar.stripe_row_limit = 20000,
   columnar.compression = pglz,
   columnar.compression_level = 9);
SELECT citus_add_local_table_to_metadata('table_option_citus_local_2');

-- verify settings on placements
SELECT run_command_on_placements('table_option_citus_local_2',$cmd$
  SELECT ROW(chunk_group_row_limit, stripe_row_limit, compression, compression_level) FROM columnar.options WHERE relation = '%s'::regclass;
$cmd$);

-- verify undistribute works
SELECT undistribute_table('table_option_citus_local');
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'table_option_citus_local'::regclass;
SELECT compression FROM columnar.options WHERE relation = 'table_option_citus_local'::regclass;

DROP TABLE table_option_citus_local, table_option_citus_local_2;
SELECT 1 FROM master_remove_node('localhost', :master_port);

-- verify reference table with no columns can be created
-- https://github.com/citusdata/citus/issues/4608
CREATE TABLE zero_col() USING columnar;
SELECT create_reference_table('zero_col');
select result from run_command_on_placements('zero_col', 'select count(*) from %s');
-- add a column so we can ad some data
ALTER TABLE zero_col ADD COLUMN a int;
INSERT INTO zero_col SELECT i FROM generate_series(1, 10) i;
select result from run_command_on_placements('zero_col', 'select count(*) from %s');

CREATE TABLE weird_col_explain (
  "bbbbbbbbbbbbbbbbbbbbbbbbb\!bbbb'bbbbbbbbbbbbbbbbbbbbb''bbbbbbbb" INT,
  "aaaaaaaaaaaa$aaaaaa$$aaaaaaaaaaaaaaaaaaaaaaaaaaaaa'aaaaaaaa'$a'!" INT)
USING columnar;
SELECT create_distributed_table('weird_col_explain', 'bbbbbbbbbbbbbbbbbbbbbbbbb\!bbbb''bbbbbbbbbbbbbbbbbbbbb''''bbbbbbbb');

EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT * FROM weird_col_explain;

\set VERBOSITY terse
EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT *, "bbbbbbbbbbbbbbbbbbbbbbbbb\!bbbb'bbbbbbbbbbbbbbbbbbbbb''bbbbbbbb"
FROM weird_col_explain
WHERE "bbbbbbbbbbbbbbbbbbbbbbbbb\!bbbb'bbbbbbbbbbbbbbbbbbbbb''bbbbbbbb" * 2 >
      "aaaaaaaaaaaa$aaaaaa$$aaaaaaaaaaaaaaaaaaaaaaaaaaaaa'aaaaaaaa'$a'!";
\set VERBOSITY default

-- should not project any columns
EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT COUNT(*) FROM weird_col_explain;

-- some tests with distributed & partitioned tables --

CREATE TABLE dist_part_table(
  dist_col INT,
  part_col TIMESTAMPTZ,
  col1 TEXT
) PARTITION BY RANGE (part_col);

-- create an index before creating a columnar partition
CREATE INDEX dist_part_table_btree ON dist_part_table (col1);

-- columnar partition
CREATE TABLE p0 PARTITION OF dist_part_table
FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
USING columnar;

SELECT create_distributed_table('dist_part_table', 'dist_col');

-- columnar partition
CREATE TABLE p1 PARTITION OF dist_part_table
FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')
USING columnar;

-- row partition
CREATE TABLE p2 PARTITION OF dist_part_table
FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');

INSERT INTO dist_part_table VALUES (1, '2020-03-15', 'str1', POINT(1, 1));

-- insert into columnar partitions
INSERT INTO dist_part_table VALUES (1, '2020-01-15', 'str2', POINT(2, 2));
INSERT INTO dist_part_table VALUES (1, '2020-02-15', 'str3', POINT(3, 3));

-- create another index after creating a columnar partition
CREATE UNIQUE INDEX dist_part_table_unique ON dist_part_table (dist_col, part_col);

-- verify that indexes are created on columnar partitions
SELECT COUNT(*)=2 FROM pg_indexes WHERE tablename = 'p0';
SELECT COUNT(*)=2 FROM pg_indexes WHERE tablename = 'p1';

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_citus_integration CASCADE;

SET client_min_messages TO WARNING;
DROP EXTENSION citus_columnar CASCADE;
