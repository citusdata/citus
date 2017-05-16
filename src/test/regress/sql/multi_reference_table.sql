ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1250000;

CREATE TABLE reference_table_test (value_1 int, value_2 float, value_3 text, value_4 timestamp);

-- insert some data, and make sure that cannot be create_distributed_table
INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-01');

-- create the reference table
SELECT create_reference_table('reference_table_test');

-- see that partkey is NULL
SELECT
	partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
	pg_dist_partition
WHERE
	logicalrelid = 'reference_table_test'::regclass;

-- now see that shard min/max values are NULL
SELECT
	shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
	pg_dist_shard
WHERE
	logicalrelid = 'reference_table_test'::regclass;
SELECT
	shardid, shardstate, nodename, nodeport
FROM
	pg_dist_shard_placement
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'reference_table_test'::regclass);

-- check whether data was copied into distributed table
SELECT * FROM reference_table_test;

-- now, execute some modification queries
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_table_test VALUES (3, 3.0, '3', '2016-12-03');
INSERT INTO reference_table_test VALUES (4, 4.0, '4', '2016-12-04');
INSERT INTO reference_table_test VALUES (5, 5.0, '5', '2016-12-05');


-- most of the queries in this file are already tested on multi_router_planner.sql
-- However, for the sake of completeness we need to run similar tests with
-- reference tables as well

-- run some queries on top of the data
SELECT
	*
FROM
	reference_table_test;

SELECT
	*
FROM
	reference_table_test
WHERE
	value_1 = 1;

SELECT
	value_1,
	value_2
FROM
	reference_table_test
ORDER BY
	2 ASC LIMIT 3;

SELECT
	value_1, value_3
FROM
	reference_table_test
WHERE
	value_2 >= 4
ORDER BY
	2 LIMIT 3;

SELECT
	value_1, 15 * value_2
FROM
	reference_table_test
ORDER BY
	2 ASC
LIMIT 2;

SELECT
	value_1, 15 * value_2
FROM
	reference_table_test
ORDER BY
	2 ASC LIMIT 2 OFFSET 2;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	value_2 = 2 OR value_2 = 3;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	value_2 = 2 AND value_2 = 3;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	value_3 = '2' OR value_1 = 3;

SELECT
	value_2, value_4
FROM
	reference_table_test
WHERE
	(
		value_3 = '2' OR value_1 = 3
	)
	AND FALSE;

SELECT
	*
FROM
	reference_table_test
WHERE
	value_2 IN
	(
		SELECT
			value_3::FLOAT
		FROM
			reference_table_test
	)
	AND value_1 < 3;

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_3 IN
	(
		'1', '2'
	);

SELECT
	date_part('day', value_4)
FROM
	reference_table_test
WHERE
	value_3 IN
	(
		'5', '2'
	);

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_2 <= 2 AND value_2 >= 4;

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_2 <= 20 AND value_2 >= 4;

SELECT
	value_4
FROM
	reference_table_test
WHERE
	value_2 >= 5 AND value_2 <= random();

SELECT
	value_1
FROM
	reference_table_test
WHERE
	value_4 BETWEEN '2016-12-01' AND '2016-12-03';

SELECT
	value_1
FROM
	reference_table_test
WHERE
	FALSE;
SELECT
	value_1
FROM
	reference_table_test
WHERE
	int4eq(1, 2);

-- rename output name and do some operations
SELECT
	value_1 as id, value_2 * 15 as age
FROM
	reference_table_test;

-- queries with CTEs are supported
WITH some_data AS ( SELECT value_2, value_4 FROM reference_table_test WHERE value_2 >=3)
SELECT
	*
FROM
	some_data;

-- queries with CTEs are supported even if CTE is not referenced inside query
WITH some_data AS ( SELECT value_2, value_4 FROM reference_table_test WHERE value_2 >=3)
SELECT * FROM reference_table_test ORDER BY 1 LIMIT 1;

-- queries which involve functions in FROM clause are supported if it goes to a single worker.
SELECT
	*
FROM
	reference_table_test, position('om' in 'Thomas')
WHERE
	value_1 = 1;

SELECT
	*
FROM
	reference_table_test, position('om' in 'Thomas')
WHERE
	value_1 = 1 OR value_1 = 2;

-- set operations are supported
SELECT * FROM (
	SELECT * FROM reference_table_test WHERE value_1 = 1
	UNION
	SELECT * FROM reference_table_test WHERE value_1 = 3
) AS combination
ORDER BY value_1;

SELECT * FROM (
	SELECT * FROM reference_table_test WHERE value_1 = 1
	EXCEPT
	SELECT * FROM reference_table_test WHERE value_1 = 3
) AS combination
ORDER BY value_1;

SELECT * FROM (
	SELECT * FROM reference_table_test WHERE value_1 = 1
	INTERSECT
	SELECT * FROM reference_table_test WHERE value_1 = 3
) AS combination
ORDER BY value_1;

-- to make the tests more interested for aggregation tests, ingest some more data
INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_table_test VALUES (3, 3.0, '3', '2016-12-03');

-- some aggregations
SELECT
	value_4, SUM(value_2)
FROM
	reference_table_test
GROUP BY
	value_4
HAVING
	SUM(value_2) > 3
ORDER BY
	1;

SELECT
	value_4,
	value_3,
	SUM(value_2)
FROM
	reference_table_test
GROUP BY
	GROUPING sets ((value_4), (value_3))
ORDER BY 1, 2, 3;


-- distinct clauses also work fine
SELECT DISTINCT
	value_4
FROM
	reference_table_test
ORDER BY
	1;

-- window functions are also supported
SELECT
	value_4, RANK() OVER (PARTITION BY value_1 ORDER BY value_4)
FROM
	reference_table_test;

-- window functions are also supported
SELECT
	value_4, AVG(value_1) OVER (PARTITION BY value_4 ORDER BY value_4)
FROM
	reference_table_test;

SELECT
	count(DISTINCT CASE
			WHEN
				value_2 >= 3
			THEN
				value_2
			ELSE
				NULL
			END) as c
	FROM
		reference_table_test;

SELECT
	value_1,
	count(DISTINCT CASE
			WHEN
				value_2 >= 3
			THEN
				value_2
			ELSE
				NULL
			END) as c
	FROM
		reference_table_test
	GROUP BY
		value_1
	ORDER BY
		1;

-- selects inside a transaction works fine as well

BEGIN;
SELECT * FROM reference_table_test;
SELECT * FROM reference_table_test WHERE value_1 = 1;
END;

-- cursor queries also works fine
BEGIN;
DECLARE test_cursor CURSOR FOR
	SELECT *
		FROM reference_table_test
		WHERE value_1 = 1 OR value_1 = 2
		ORDER BY value_1;
FETCH test_cursor;
FETCH ALL test_cursor;
FETCH test_cursor; -- fetch one row after the last
FETCH BACKWARD test_cursor;
END;

-- table creation queries inside can be router plannable
CREATE TEMP TABLE temp_reference_test as
	SELECT *
	FROM reference_table_test
	WHERE value_1 = 1;

-- all kinds of joins are supported among reference tables
-- first create two more tables
CREATE TABLE reference_table_test_second (value_1 int, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_second');

CREATE TABLE reference_table_test_third (value_1 int, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_third');

-- ingest some data to both tables
INSERT INTO reference_table_test_second VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test_second VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_table_test_second VALUES (3, 3.0, '3', '2016-12-03');

INSERT INTO reference_table_test_third VALUES (4, 4.0, '4', '2016-12-04');
INSERT INTO reference_table_test_third VALUES (5, 5.0, '5', '2016-12-05');

-- some very basic tests
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = t2.value_2
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_third t3
WHERE
	t1.value_2 = t3.value_2
ORDER BY
	1;

SELECT
	DISTINCT t2.value_1
FROM
	reference_table_test_second t2, reference_table_test_third t3
WHERE
	t2.value_2 = t3.value_2
ORDER BY
	1;

-- join on different columns and different data types via casts
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = t2.value_1
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = t2.value_3::int
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2
WHERE
	t1.value_2 = date_part('day', t2.value_4)
ORDER BY
	1;

-- ingest a common row to see more meaningful results with joins involving 3 tables
INSERT INTO reference_table_test_third VALUES (3, 3.0, '3', '2016-12-03');

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2, reference_table_test_third t3
WHERE
	t1.value_2 = date_part('day', t2.value_4) AND t3.value_2 = t1.value_2
ORDER BY
	1;

-- same query on different columns
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1, reference_table_test_second t2, reference_table_test_third t3
WHERE
	t1.value_1 = date_part('day', t2.value_4) AND t3.value_2 = t1.value_1
ORDER BY
	1;

-- with the JOIN syntax
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1 JOIN reference_table_test_second t2 USING (value_1)
							JOIN reference_table_test_third t3 USING (value_1)
ORDER BY
	1;

-- and left/right joins
SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1 LEFT JOIN reference_table_test_second t2 USING (value_1)
							LEFT JOIN reference_table_test_third t3 USING (value_1)
ORDER BY
	1;

SELECT
	DISTINCT t1.value_1
FROM
	reference_table_test t1 RIGHT JOIN reference_table_test_second t2 USING (value_1)
							RIGHT JOIN reference_table_test_third t3 USING (value_1)
ORDER BY
	1;

-- now, lets have some tests on UPSERTs and uniquness
CREATE TABLE reference_table_test_fourth (value_1 int, value_2 float PRIMARY KEY, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_fourth');

-- insert a row
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '1', '2016-12-01');

-- now get the unique key violation
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '1', '2016-12-01');

-- now get null constraint violation due to primary key
INSERT INTO reference_table_test_fourth (value_1, value_3, value_4) VALUES (1, '1.0', '2016-12-01');

-- lets run some upserts
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '1', '2016-12-01') ON CONFLICT DO NOTHING RETURNING *;
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '10', '2016-12-01') ON CONFLICT (value_2) DO
	UPDATE SET value_3 = EXCLUDED.value_3, value_2 = EXCLUDED.value_2
	RETURNING *;
-- update all columns
INSERT INTO reference_table_test_fourth VALUES (1, 1.0, '10', '2016-12-01') ON CONFLICT (value_2) DO
	UPDATE SET value_3 = EXCLUDED.value_3 || '+10', value_2 = EXCLUDED.value_2 + 10, value_1 = EXCLUDED.value_1 + 10, value_4 = '2016-12-10'
	RETURNING *;

-- finally see that shard healths are OK
SELECT
	shardid, shardstate, nodename, nodeport
FROM
	pg_dist_shard_placement
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'reference_table_test_fourth'::regclass);

-- let's not run some update/delete queries on arbitrary columns
DELETE FROM
	reference_table_test
WHERE
	value_1 = 1
RETURNING *;

DELETE FROM
	reference_table_test
WHERE
	value_4 = '2016-12-05'
RETURNING *;

UPDATE
	reference_table_test
SET
	value_2 = 15
WHERE
	value_2 = 2
RETURNING *;

-- and some queries without any filters
UPDATE
	reference_table_test
SET
	value_2 = 15, value_1 = 45
RETURNING *;

DELETE FROM
	reference_table_test
RETURNING *;

-- some tests with function evaluation and sequences
CREATE TABLE reference_table_test_fifth (value_1 serial  PRIMARY KEY, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_fifth');
CREATE SEQUENCE example_ref_value_seq;

-- see that sequences work as expected
INSERT INTO
	reference_table_test_fifth (value_2) VALUES (2)
RETURNING value_1, value_2;
INSERT INTO
	reference_table_test_fifth (value_2) VALUES (2)
RETURNING value_1, value_2;

INSERT INTO
	reference_table_test_fifth (value_2, value_3) VALUES (nextval('example_ref_value_seq'), nextval('example_ref_value_seq')::text)
RETURNING value_1, value_2, value_3;

UPDATE
	reference_table_test_fifth SET value_4 = now()
WHERE
	value_1 = 1
RETURNING value_1, value_2, value_4 > '2000-01-01';


-- test copying FROM / TO
-- first delete all the data
DELETE FROM
	reference_table_test;

COPY reference_table_test FROM STDIN WITH CSV;
1,1.0,1,2016-01-01
\.
COPY reference_table_test (value_2, value_3, value_4) FROM STDIN WITH CSV;
2.0,2,2016-01-02
\.
COPY reference_table_test (value_3) FROM STDIN WITH CSV;
3
\.

COPY reference_table_test FROM STDIN WITH CSV;
,,,
\.

COPY reference_table_test TO STDOUT WITH CSV;

-- INSERT INTO SELECT among reference tables
DELETE FROM
	reference_table_test_second;

INSERT INTO
	reference_table_test_second
	SELECT
		*
	FROM
		reference_table_test
	RETURNING *;

INSERT INTO
	reference_table_test_second (value_2)
	SELECT
		reference_table_test.value_2
	FROM
		reference_table_test JOIN reference_table_test_second USING (value_1)
	RETURNING *;


SET citus.shard_count TO 6;
SET citus.shard_replication_factor TO 2;

CREATE TABLE colocated_table_test (value_1 int, value_2 float, value_3 text, value_4 timestamp);
SELECT create_distributed_table('colocated_table_test', 'value_1');

CREATE TABLE colocated_table_test_2 (value_1 int, value_2 float, value_3 text, value_4 timestamp);
SELECT create_distributed_table('colocated_table_test_2', 'value_1');

DELETE FROM reference_table_test;
INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');

INSERT INTO colocated_table_test VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO colocated_table_test VALUES (2, 2.0, '2', '2016-12-02');

INSERT INTO colocated_table_test_2 VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO colocated_table_test_2 VALUES (2, 2.0, '2', '2016-12-02');


SET client_min_messages TO DEBUG1;
SET citus.log_multi_join_order TO TRUE;

SELECT 
	reference_table_test.value_1
FROM 
	reference_table_test, colocated_table_test
WHERE 
	colocated_table_test.value_1 = reference_table_test.value_1;

SELECT 
	colocated_table_test.value_2
FROM 
	reference_table_test, colocated_table_test 
WHERE 
	colocated_table_test.value_2 = reference_table_test.value_2;

SELECT 
	colocated_table_test.value_2
FROM 
	colocated_table_test, reference_table_test
WHERE 
	reference_table_test.value_1 = colocated_table_test.value_1;

SELECT 
	colocated_table_test.value_2 
FROM 
	reference_table_test, colocated_table_test, colocated_table_test_2
WHERE 
	colocated_table_test.value_2 = reference_table_test.value_2;

SELECT 
	colocated_table_test.value_2 
FROM 
	reference_table_test, colocated_table_test, colocated_table_test_2
WHERE 
	colocated_table_test.value_1 = colocated_table_test_2.value_1 AND colocated_table_test.value_2 = reference_table_test.value_2;

SET citus.task_executor_type to "task-tracker";
SELECT 
	colocated_table_test.value_2 
FROM 
	reference_table_test, colocated_table_test, colocated_table_test_2
WHERE 
	colocated_table_test.value_2 = colocated_table_test_2.value_2 AND colocated_table_test.value_2 = reference_table_test.value_2;

SELECT 
	reference_table_test.value_2 
FROM 
	reference_table_test, colocated_table_test, colocated_table_test_2
WHERE 
	colocated_table_test.value_1 = reference_table_test.value_1 AND colocated_table_test_2.value_1 = reference_table_test.value_1;


SET client_min_messages TO NOTICE;
SET citus.log_multi_join_order TO FALSE;

SET citus.shard_count TO DEFAULT;
SET citus.task_executor_type to "real-time";

-- some INSERT .. SELECT queries that involve both hash distributed and reference tables

-- should error out since we're inserting into reference table where 
-- not all the participants are reference tables
INSERT INTO
	reference_table_test (value_1)
SELECT
	colocated_table_test.value_1
FROM
	colocated_table_test, colocated_table_test_2
WHERE
	colocated_table_test.value_1 = colocated_table_test.value_1;

-- should error out, same as the above
INSERT INTO
	reference_table_test (value_1)
SELECT
	colocated_table_test.value_1
FROM
	colocated_table_test, reference_table_test
WHERE
	colocated_table_test.value_1 = reference_table_test.value_1;

-- now, insert into the hash partitioned table and use reference 
-- tables in the SELECT queries
INSERT INTO
	colocated_table_test (value_1, value_2)
SELECT 
	colocated_table_test_2.value_1, reference_table_test.value_2
FROM
	colocated_table_test_2, reference_table_test
WHERE
	colocated_table_test_2.value_4 = reference_table_test.value_4
RETURNING value_1, value_2;

-- some more complex queries (Note that there are more complex queries in multi_insert_select.sql)
INSERT INTO
	colocated_table_test (value_1, value_2)
SELECT 
	colocated_table_test_2.value_1, reference_table_test.value_2
FROM
	colocated_table_test_2, reference_table_test
WHERE
	colocated_table_test_2.value_2 = reference_table_test.value_2
RETURNING value_1, value_2;

-- partition column value comes from reference table but still first error is
-- on data type mismatch
INSERT INTO
	colocated_table_test (value_1, value_2)
SELECT
	reference_table_test.value_2, colocated_table_test_2.value_1
FROM
	colocated_table_test_2, reference_table_test
WHERE
	colocated_table_test_2.value_4 = reference_table_test.value_4
RETURNING value_1, value_2;

-- partition column value comes from reference table which should error out
INSERT INTO
	colocated_table_test (value_1, value_2)
SELECT
	reference_table_test.value_1, colocated_table_test_2.value_1
FROM
	colocated_table_test_2, reference_table_test
WHERE
	colocated_table_test_2.value_4 = reference_table_test.value_4
RETURNING value_1, value_2;


-- some tests for mark_tables_colocated
-- should error out
SELECT mark_tables_colocated('colocated_table_test_2', ARRAY['reference_table_test']);

-- should work sliently
SELECT mark_tables_colocated('reference_table_test', ARRAY['reference_table_test_fifth']);

-- ensure that reference tables on
-- different queries works as expected
CREATE SCHEMA reference_schema;

-- create with schema prefix
CREATE TABLE reference_schema.reference_table_test_sixth (value_1 serial  PRIMARY KEY, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_schema.reference_table_test_sixth');

SET search_path TO 'reference_schema';

-- create on the schema
CREATE TABLE reference_table_test_seventh (value_1 serial  PRIMARY KEY, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_test_seventh');

-- ingest some data
INSERT INTO reference_table_test_sixth VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test_seventh VALUES (1, 1.0, '1', '2016-12-01');

SET search_path TO 'public';

-- ingest some data
INSERT INTO reference_schema.reference_table_test_sixth VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_schema.reference_table_test_seventh VALUES (2, 2.0, '2', '2016-12-02');

-- some basic queries
SELECT
	value_1
FROM
	reference_schema.reference_table_test_sixth;

SET search_path TO 'reference_schema';
SELECT
	reference_table_test_sixth.value_1
FROM
	reference_table_test_sixth, reference_table_test_seventh
WHERE
	reference_table_test_sixth.value_4 = reference_table_test_seventh.value_4;

-- last test with cross schemas
SET search_path TO 'public';

SELECT
	reftable.value_2, colocated_table_test_2.value_1
FROM
	colocated_table_test_2, reference_schema.reference_table_test_sixth as reftable
WHERE
	colocated_table_test_2.value_4 = reftable.value_4;


-- let's now test TRUNCATE and DROP TABLE 
-- delete all rows and ingest some data
DELETE FROM reference_table_test;

INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-01');
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
INSERT INTO reference_table_test VALUES (3, 3.0, '3', '2016-12-03');
INSERT INTO reference_table_test VALUES (4, 4.0, '4', '2016-12-04');
INSERT INTO reference_table_test VALUES (5, 5.0, '5', '2016-12-05');

SELECT
	count(*)
FROM
	reference_table_test;

-- truncate it and get the result back
TRUNCATE reference_table_test;

SELECT
	count(*)
FROM
	reference_table_test;

-- now try dropping one of the existing reference tables
-- and check the metadata
SELECT logicalrelid FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE '%reference_table_test_fifth%';
SELECT logicalrelid FROM pg_dist_shard WHERE logicalrelid::regclass::text LIKE '%reference_table_test_fifth%';
DROP TABLE reference_table_test_fifth;
SELECT logicalrelid FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE '%reference_table_test_fifth%';
SELECT logicalrelid FROM pg_dist_shard WHERE logicalrelid::regclass::text LIKE '%reference_table_test_fifth%';


-- now test DDL changes
CREATE TABLE reference_table_ddl (value_1 int, value_2 float, value_3 text, value_4 timestamp);
SELECT create_reference_table('reference_table_ddl');

-- CREATE & DROP index and check the workers
CREATE INDEX reference_index_1 ON reference_table_ddl(value_1);
CREATE INDEX reference_index_2 ON reference_table_ddl(value_2, value_3);

-- should be able to create/drop UNIQUE index on a reference table
CREATE UNIQUE INDEX reference_index_3 ON reference_table_ddl(value_1);

-- should be able to add a column
ALTER TABLE reference_table_ddl ADD COLUMN value_5 INTEGER;
ALTER TABLE reference_table_ddl ALTER COLUMN value_5 SET DATA TYPE FLOAT;

ALTER TABLE reference_table_ddl DROP COLUMN value_1;
ALTER TABLE reference_table_ddl ALTER COLUMN value_2 SET DEFAULT 25.0;
ALTER TABLE reference_table_ddl ALTER COLUMN value_3 SET NOT NULL;

-- see that Citus applied all DDLs to the table
\d reference_table_ddl

-- also to the shard placements
\c - - - :worker_1_port
\d reference_table_ddl*
\c - - - :master_port
DROP INDEX reference_index_2;
\c - - - :worker_1_port
\d reference_table_ddl*
\c - - - :master_port

-- as we expect, renaming and setting WITH OIDS does not work for reference tables
ALTER TABLE reference_table_ddl RENAME TO reference_table_ddl_test;
ALTER TABLE reference_table_ddl SET WITH OIDS;

-- now test reference tables against some helper UDFs that Citus provides

-- cannot delete / drop shards from a reference table
SELECT master_apply_delete_command('DELETE FROM reference_table_ddl');

-- cannot add shards
SELECT master_create_empty_shard('reference_table_ddl');

-- master_modify_multiple_shards works, but, does it make sense to use at all?
INSERT INTO reference_table_ddl (value_2, value_3) VALUES (7, 'aa');
SELECT master_modify_multiple_shards('DELETE FROM reference_table_ddl WHERE value_2 = 7');
INSERT INTO reference_table_ddl (value_2, value_3) VALUES (7, 'bb');
SELECT master_modify_multiple_shards('DELETE FROM reference_table_ddl');

-- get/update the statistics
SELECT part_storage_type, part_key, part_replica_count, part_max_size,
           part_placement_policy FROM master_get_table_metadata('reference_table_ddl');
SELECT shardid AS a_shard_id  FROM pg_dist_shard WHERE logicalrelid = 'reference_table_ddl'::regclass \gset
SELECT master_update_shard_statistics(:a_shard_id);

CREATE TABLE append_reference_tmp_table (id INT);
SELECT  master_append_table_to_shard(:a_shard_id, 'append_reference_tmp_table', 'localhost', :master_port);

SELECT master_get_table_ddl_events('reference_table_ddl');

-- in reality, we wouldn't need to repair any reference table shard placements
-- however, the test could be relevant for other purposes
SELECT placementid AS a_placement_id FROM pg_dist_shard_placement WHERE shardid = :a_shard_id AND nodeport = :worker_1_port \gset
SELECT placementid AS b_placement_id FROM pg_dist_shard_placement WHERE shardid = :a_shard_id AND nodeport = :worker_2_port \gset

UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE placementid = :a_placement_id;
SELECT master_copy_shard_placement(:a_shard_id, 'localhost', :worker_2_port, 'localhost', :worker_1_port);
SELECT shardid, shardstate FROM pg_dist_shard_placement WHERE placementid = :a_placement_id;

-- some queries that are captured in functions
CREATE FUNCTION select_count_all() RETURNS bigint AS '
        SELECT
                count(*)
        FROM
                reference_table_test;
' LANGUAGE SQL;

CREATE FUNCTION insert_into_ref_table(value_1 int, value_2 float, value_3 text, value_4 timestamp) 
RETURNS void AS '
       INSERT INTO reference_table_test VALUES ($1, $2, $3, $4);
' LANGUAGE SQL;

TRUNCATE reference_table_test;
SELECT select_count_all();
SELECT insert_into_ref_table(1, 1.0, '1', '2016-12-01');
SELECT insert_into_ref_table(2, 2.0, '2', '2016-12-02');
SELECT insert_into_ref_table(3, 3.0, '3', '2016-12-03');
SELECT insert_into_ref_table(4, 4.0, '4', '2016-12-04');
SELECT insert_into_ref_table(5, 5.0, '5', '2016-12-05');
SELECT insert_into_ref_table(6, 6.0, '6', '2016-12-06');
SELECT select_count_all();
TRUNCATE reference_table_test;

-- some prepared queries and pl/pgsql functions
PREPARE insert_into_ref_table_pr (int, float, text, timestamp) 
	AS INSERT INTO reference_table_test VALUES ($1, $2, $3, $4);

-- reference tables do not have up-to-five execution limit as other tables
EXECUTE insert_into_ref_table_pr(1, 1.0, '1', '2016-12-01');
EXECUTE insert_into_ref_table_pr(2, 2.0, '2', '2016-12-02');
EXECUTE insert_into_ref_table_pr(3, 3.0, '3', '2016-12-03');
EXECUTE insert_into_ref_table_pr(4, 4.0, '4', '2016-12-04');
EXECUTE insert_into_ref_table_pr(5, 5.0, '5', '2016-12-05');
EXECUTE insert_into_ref_table_pr(6, 6.0, '6', '2016-12-06');

-- see the count, then truncate the table
SELECT select_count_all();
TRUNCATE reference_table_test;

-- reference tables work with composite key
-- and we even do not need to create hash
-- function etc.

-- first create the type on all nodes
CREATE TYPE reference_comp_key as (key text, value text);
\c - - - :worker_1_port
CREATE TYPE reference_comp_key as (key text, value text);
\c - - - :worker_2_port
CREATE TYPE reference_comp_key as (key text, value text);

\c - - - :master_port
CREATE TABLE reference_table_composite (id int PRIMARY KEY, data reference_comp_key);
SELECT create_reference_table('reference_table_composite');

-- insert and query some data
INSERT INTO reference_table_composite (id, data) VALUES (1, ('key_1', 'value_1')::reference_comp_key);
INSERT INTO reference_table_composite (id, data) VALUES (2, ('key_2', 'value_2')::reference_comp_key);

SELECT * FROM reference_table_composite;
SELECT (data).key FROM reference_table_composite;

-- make sure that reference tables obeys single shard transactions
TRUNCATE reference_table_test;

BEGIN;
INSERT INTO reference_table_test VALUES (1, 1.0, '1', '2016-12-01');
SELECT * FROM reference_table_test;
ROLLBACK;
SELECT * FROM reference_table_test;

-- now insert a row and commit
BEGIN;
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
COMMIT;
SELECT * FROM reference_table_test;

-- one basic UPDATE test
BEGIN;
UPDATE reference_table_test SET value_1 = 10 WHERE value_1 = 2;
COMMIT;
SELECT * FROM reference_table_test;

-- do not allow mixing transactions
BEGIN;
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
SELECT master_modify_multiple_shards('DELETE FROM colocated_table_test');
ROLLBACK;

-- Do not allow DDL and modification in the same transaction
BEGIN;
ALTER TABLE reference_table_test ADD COLUMN value_dummy INT;
INSERT INTO reference_table_test VALUES (2, 2.0, '2', '2016-12-02');
ROLLBACK;

-- clean up tables
DROP TABLE reference_table_test, reference_table_test_second, reference_table_test_third, 
		   reference_table_test_fourth, reference_table_ddl, reference_table_composite;
DROP SCHEMA reference_schema CASCADE;
