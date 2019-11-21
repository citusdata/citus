--
-- MULTI_SIZE_QUERIES
--
-- Test checks whether size of distributed tables can be obtained with citus_table_size.
-- To find the relation size and total relation size citus_relation_size and
-- citus_total_relation_size are also tested.

SET citus.next_shard_id TO 1460000;

SET citus.shard_replication_factor to 1;

CREATE TABLE test_table_1_rf1(id int, val_1 int);
SELECT create_distributed_table('test_table_1_rf1','id');
INSERT INTO test_table_1_rf1 values(1,2),(2,3),(3,4),(15,16);

CREATE TABLE test_table_2_rf1(id int, val_1 int);
SELECT create_distributed_table('test_table_2_rf1','id');
INSERT INTO test_table_2_rf1 values(1,2),(2,3),(3,4);

CREATE TABLE ref_table(id int, val_1 int);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table values(1,2),(3,4),(5,6);

CREATE TABLE ref_table_2(id int, val_1 int);
SELECT create_reference_table('ref_table_2');
INSERT INTO ref_table_2 values(3,4),(5,6),(8,9);

SET citus.shard_replication_factor to 2;

CREATE TABLE test_table_3_rf2(id int, val_1 int);
SELECT create_distributed_table('test_table_3_rf2','id');
INSERT INTO test_table_3_rf2 values(1,2),(2,3),(3,4);

CREATE TABLE test_table_4_rf2(id int, val_1 int);
SELECT create_distributed_table('test_table_4_rf2','id');
INSERT INTO test_table_4_rf2 values(1,2),(2,3),(3,4);

-- Hash tables with RF = 1 is supported for router planner queries
SELECT * FROM
	test_table_1_rf1 as tt1 INNER JOIN test_table_1_rf1 as tt2 on tt1.id = tt2.id
	WHERE tt1.id = 1
	ORDER BY 1
	FOR UPDATE;

-- May hav two different filters on partition column, if they resulted on the same shard
SELECT * FROM
	test_table_1_rf1 as tt1 WHERE tt1.id = 1 OR tt1.id = 15
	ORDER BY 1
	FOR UPDATE;

-- Only router plannable queries are supported right now.
SELECT * FROM
	test_table_1_rf1 as tt1 INNER JOIN test_table_1_rf1 as tt2 on tt1.id = tt2.id
	ORDER BY 1
	FOR UPDATE;

-- RF > 1 is not supported
SELECT * FROM
	test_table_1_rf1 as tt1 INNER JOIN test_table_3_rf2 as tt3 on tt1.id = tt3.id
	WHERE tt1.id = 1
	ORDER BY 1
	FOR UPDATE;

SELECT * FROM
	test_table_1_rf1 as tt1 INNER JOIN test_table_3_rf2 as tt3 on tt1.id = tt3.id
	ORDER BY 1
	FOR UPDATE;

SELECT * FROM
	test_table_3_rf2 as tt3 INNER JOIN test_table_4_rf2 as tt4 on tt3.id = tt4.id
	WHERE tt3.id = 1
	ORDER BY 1
	FOR UPDATE;

-- We take executor shard lock for reference tables
SELECT * FROM
	test_table_1_rf1 as tt1 INNER JOIN ref_table as rt1 on tt1.id = rt1.id
	WHERE tt1.id = 1
	ORDER BY 1
	FOR UPDATE;

-- Lock type varies according to the strength of row level lock
SELECT * FROM
	test_table_1_rf1 as tt1 INNER JOIN ref_table as rt1 on tt1.id = rt1.id
	WHERE tt1.id = 1
	ORDER BY 1
	FOR SHARE;

-- You can choose which tables should be affected by locking clause
SELECT * FROM
	ref_table as rt1 INNER JOIN ref_table_2 as rt2 on rt1.id = rt2.id
	ORDER BY 1
	FOR UPDATE
	OF rt1;

-- You can choose which tables should be affected by locking clause
SELECT * FROM
	ref_table as rt1 INNER JOIN ref_table_2 as rt2 on rt1.id = rt2.id
	ORDER BY 1
	FOR UPDATE
	OF rt1
	NOWAIT;

-- queries with CTEs are supported
WITH first_value AS (
	SELECT val_1 FROM test_table_1_rf1 WHERE id = 1 FOR UPDATE)
SELECT * FROM first_value;

-- queries with modifying CTEs are also supported
WITH update_table AS (
    UPDATE test_table_1_rf1 SET val_1 = 10 WHERE id = 1 RETURNING *
)
SELECT * FROM update_table FOR UPDATE;

-- Subqueries also supported
SELECT * FROM (SELECT * FROM test_table_1_rf1 FOR UPDATE) foo WHERE id = 1;


DROP TABLE test_table_1_rf1;
DROP TABLE test_table_2_rf1;
DROP TABLE ref_table;
DROP TABLE ref_table_2;
DROP TABLE test_table_3_rf2;
DROP TABLE test_table_4_rf2;
