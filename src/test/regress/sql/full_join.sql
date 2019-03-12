--
-- Full join with subquery pushdown support 
--

SET citus.next_shard_id TO 9000000;

CREATE SCHEMA full_join;
SET search_path TO full_join, public;

CREATE TABLE test_table_1(id int, val1 int);
CREATE TABLE test_table_2(id bigint, val1 int);
CREATE TABLE test_table_3(id int, val1 bigint);

SELECT create_distributed_table('test_table_1', 'id');
SELECT create_distributed_table('test_table_2', 'id');
SELECT create_distributed_table('test_table_3', 'id');

INSERT INTO test_table_1 VALUES(1,1),(2,2),(3,3);
INSERT INTO test_table_2 VALUES(2,2),(3,3),(4,4);
INSERT INTO test_table_3 VALUES(1,1),(3,3),(4,5);

-- Simple full outer join
SELECT id FROM test_table_1 FULL JOIN test_table_3 using(id) ORDER BY 1;

-- Get all columns as the result of the full join
SELECT * FROM test_table_1 FULL JOIN test_table_3 using(id) ORDER BY 1;

-- Join subqueries using single column
SELECT * FROM 
	(SELECT test_table_1.id FROM test_table_1 FULL JOIN test_table_3 using(id)) as j1
		FULL JOIN
	(SELECT test_table_1.id FROM test_table_1 FULL JOIN test_table_3 using(id)) as j2
	USING(id)
	ORDER BY 1;

-- Join subqueries using multiple columns
SELECT * FROM 
	(SELECT test_table_1.id, test_table_1.val1 FROM test_table_1 FULL JOIN test_table_3 using(id)) as j1
		FULL JOIN
	(SELECT test_table_1.id, test_table_1.val1 FROM test_table_1 FULL JOIN test_table_3 using(id)) as j2
	USING(id, val1)
	ORDER BY 1;

-- Full join using multiple columns
SELECT * FROM test_table_1 FULL JOIN test_table_3 USING(id, val1) ORDER BY 1;

-- Full join with complicated target lists
SELECT count(DISTINCT id), (avg(test_table_1.val1) + id * id)::integer as avg_value, id::numeric IS NOT NULL as not_null 
FROM test_table_1 FULL JOIN test_table_3 using(id)
WHERE id::bigint < 55
GROUP BY id
ORDER BY 2
ASC LIMIT 3;

SELECT max(val1)
FROM test_table_1 FULL JOIN test_table_3 USING(id, val1)
GROUP BY test_table_1.id
ORDER BY 1;

-- Test the left join as well
SELECT max(val1)
FROM test_table_1 LEFT JOIN test_table_3 USING(id, val1)
GROUP BY test_table_1.id
ORDER BY 1;

-- Full outer join with different distribution column types, should error out
SELECT * FROM test_table_1 full join test_table_2 using(id);

-- Test when the non-distributed column has the value of NULL
INSERT INTO test_table_1 VALUES(7, NULL);
INSERT INTO test_table_2 VALUES(7, NULL);
INSERT INTO test_table_3 VALUES(7, NULL);

-- Get all columns as the result of the full join
SELECT * FROM test_table_1 FULL JOIN test_table_3 using(id) ORDER BY 1;

-- Get the same result (with multiple id)
SELECT * FROM test_table_1 FULL JOIN test_table_3 ON (test_table_1.id = test_table_3.id) ORDER BY 1;

-- Full join using multiple columns
SELECT * FROM test_table_1 FULL JOIN test_table_3 USING(id, val1) ORDER BY 1;

-- In order to make the same test with different data types use text-varchar pair
-- instead of using int-bigint pair.
DROP TABLE test_table_1;
DROP TABLE test_table_2;
DROP TABLE test_table_3;

CREATE TABLE test_table_1(id int, val1 text);
CREATE TABLE test_table_2(id int, val1 varchar(30));

SELECT create_distributed_table('test_table_1', 'id');
SELECT create_distributed_table('test_table_2', 'id');

INSERT INTO test_table_1 VALUES(1,'val_1'),(2,'val_2'),(3,'val_3'), (4, NULL);
INSERT INTO test_table_2 VALUES(2,'val_2'),(3,'val_3'),(4,'val_4'), (5, NULL);

-- Simple full outer join
SELECT id FROM test_table_1 FULL JOIN test_table_2 using(id) ORDER BY 1;

-- Get all columns as the result of the full join
SELECT * FROM test_table_1 FULL JOIN test_table_2 using(id) ORDER BY 1;

-- Join subqueries using multiple columns
SELECT * FROM 
	(SELECT test_table_1.id, test_table_1.val1 FROM test_table_1 FULL JOIN test_table_2 using(id)) as j1
		FULL JOIN
	(SELECT test_table_2.id, test_table_2.val1 FROM test_table_1 FULL JOIN test_table_2 using(id)) as j2
	USING(id, val1)
	ORDER BY 1,2;

-- Full join using multiple columns
SELECT * FROM test_table_1 FULL JOIN test_table_2 USING(id, val1) ORDER BY 1,2;

DROP SCHEMA full_join CASCADE;
