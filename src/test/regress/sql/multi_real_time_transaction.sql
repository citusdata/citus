SET citus.next_shard_id TO 1610000;

CREATE SCHEMA multi_real_time_transaction;
SET search_path = 'multi_real_time_transaction';
SET citus.shard_replication_factor to 1;

CREATE TABLE test_table(id int, col_1 int, col_2 text);
SELECT create_distributed_table('test_table','id');
\COPY test_table FROM stdin delimiter ',';
1,2,'aa'
2,3,'bb'
3,4,'cc'
4,5,'dd'
5,6,'ee'
6,7,'ff'
\.

CREATE TABLE co_test_table(id int, col_1 int, col_2 text);
SELECT create_distributed_table('co_test_table','id');
\COPY co_test_table FROM stdin delimiter ',';
1,20,'aa10'
2,30,'bb10'
3,40,'cc10'
3,4,'cc1'
3,5,'cc2'
1,2,'cc2'
\.


CREATE TABLE ref_test_table(id int, col_1 int, col_2 text);
SELECT create_reference_table('ref_test_table');
\COPY ref_test_table FROM stdin delimiter ',';
1,2,'rr1'
2,3,'rr2'
3,4,'rr3'
4,5,'rr4'
\.

-- Test with select and router insert
BEGIN;
SELECT COUNT(*) FROM test_table;
INSERT INTO test_table VALUES(7,8,'gg');
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with select and multi-row insert
BEGIN;
SELECT COUNT(*) FROM test_table;
INSERT INTO test_table VALUES (7,8,'gg'),(8,9,'hh'),(9,10,'ii');
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with INSERT .. SELECT
BEGIN;
SELECT COUNT(*) FROM test_table;
INSERT INTO test_table SELECT * FROM co_test_table;
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with COPY
BEGIN;
SELECT COUNT(*) FROM test_table;
\COPY test_table FROM stdin delimiter ',';
8,9,'gg'
9,10,'hh'
10,11,'ii'
\.
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with router update
BEGIN;
SELECT SUM(col_1) FROM test_table;
UPDATE test_table SET col_1 = 0 WHERE id = 2;
DELETE FROM test_table WHERE id = 3;
SELECT SUM(col_1) FROM test_table;
ROLLBACK;

-- Test with multi-shard update
BEGIN;
SELECT SUM(col_1) FROM test_table;
UPDATE test_table SET col_1 = 5;
SELECT SUM(col_1) FROM test_table;
ROLLBACK;

-- Test with subqueries
BEGIN;
SELECT SUM(col_1) FROM test_table;
UPDATE
	test_table
SET
	col_1 = 4
WHERE
	test_table.col_1 IN (SELECT co_test_table.col_1 FROM co_test_table WHERE co_test_table.id = 1)
	AND test_table.id = 1;
SELECT SUM(col_1) FROM test_table;
ROLLBACK;

-- Test with partitioned table
CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
SET citus.shard_replication_factor TO 1;

-- create its partitions
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

-- load some data and distribute tables
INSERT INTO partitioning_test VALUES (1, '2009-06-06');
INSERT INTO partitioning_test VALUES (2, '2010-07-07');
SELECT create_distributed_table('partitioning_test', 'id');

BEGIN;
SELECT COUNT(*) FROM partitioning_test;
INSERT INTO partitioning_test_2009 VALUES (3, '2009-09-09');
INSERT INTO partitioning_test_2010 VALUES (4, '2010-03-03');
SELECT COUNT(*) FROM partitioning_test;
COMMIT;

DROP TABLE partitioning_test;

-- Test with create-drop table
BEGIN;
CREATE TABLE test_table_inn(id int, num_1 int);
SELECT create_distributed_table('test_table_inn','id');
INSERT INTO test_table_inn VALUES(1,3),(4,5),(6,7);
SELECT COUNT(*) FROM test_table_inn;
DROP TABLE test_table_inn;
COMMIT;

-- Test with utility functions
BEGIN;
SELECT COUNT(*) FROM test_table;
CREATE INDEX tt_ind_1 ON test_table(col_1);
ALTER TABLE test_table ADD CONSTRAINT num_check CHECK (col_1 < 50);
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with foreign key
ALTER TABLE test_table ADD CONSTRAINT p_key_tt PRIMARY KEY (id);
ALTER TABLE co_test_table ADD CONSTRAINT f_key_ctt FOREIGN KEY (id) REFERENCES test_table(id) ON DELETE CASCADE;

BEGIN;
DELETE FROM test_table where id = 1 or id = 3;
SELECT * FROM co_test_table;
ROLLBACK;

DROP SCHEMA multi_real_time_transaction CASCADE;
