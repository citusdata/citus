CREATE SCHEMA upgrade_basic;
SET search_path TO upgrade_basic, public;

CREATE TABLE t(a int);
CREATE INDEX ON t USING HASH (a);
SELECT create_distributed_table('t', 'a');
INSERT INTO t SELECT * FROM generate_series(1, 5);

CREATE TABLE tp(a int PRIMARY KEY);
SELECT create_distributed_table('tp', 'a');
INSERT INTO tp SELECT * FROM generate_series(1, 5);

-- We store the index of distribution column and here we check that the distribution
-- column index does not change after an upgrade if we drop a column that comes before the
-- distribution column. The index information is in partkey column of pg_dist_partition table.
CREATE TABLE t_ab(a int, b int);
SELECT create_distributed_table('t_ab', 'b');
INSERT INTO t_ab VALUES (1, 11);
INSERT INTO t_ab VALUES (2, 22);
INSERT INTO t_ab VALUES (3, 33);

CREATE TABLE t2(a int, b int);
INSERT INTO t2 VALUES (1, 11);
INSERT INTO t2 VALUES (2, 22);
INSERT INTO t2 VALUES (3, 33);

ALTER TABLE t_ab DROP a;

-- Check that basic reference tables work
CREATE TABLE r(a int PRIMARY KEY);
SELECT create_reference_table('r');
INSERT INTO r SELECT * FROM generate_series(1, 5);
CREATE TABLE tr(pk int, a int REFERENCES r(a) ON DELETE CASCADE ON UPDATE CASCADE);
SELECT create_distributed_table('tr', 'pk');
INSERT INTO tr SELECT c, c FROM generate_series(1, 5) as c;
CREATE TABLE t_range(id int, value_1 int);
SELECT create_distributed_table('t_range', 'id', 'range');
SELECT master_create_empty_shard('t_range') as shardid1 \gset
SELECT master_create_empty_shard('t_range') as shardid2 \gset
UPDATE pg_dist_shard SET shardminvalue = '1', shardmaxvalue = '3' WHERE shardid = :shardid1;
UPDATE pg_dist_shard SET shardminvalue = '5', shardmaxvalue = '7' WHERE shardid = :shardid2;

\copy t_range FROM STDIN with (DELIMITER ',')
1,2
2,3
3,4
\.

\copy t_range FROM STDIN with (DELIMITER ',')
5,2
6,3
7,4
\.

-- There is a difference in partkey Var representation between PG16 and older versions
-- Sanity check here that we can properly do column_to_column_name
SELECT column_to_column_name(logicalrelid, partkey)
FROM pg_dist_partition WHERE partkey IS NOT NULL ORDER BY 1 LIMIT 1;
