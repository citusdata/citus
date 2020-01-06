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

CREATE TABLE t_append(id int, value_1 int);
SELECT master_create_distributed_table('t_append', 'id', 'append');

\copy t_append FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.

\copy t_append FROM STDIN DELIMITER ','
5,2
6,3
7,4
\.
