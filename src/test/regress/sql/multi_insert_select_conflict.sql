--
-- MULTI_INSERT_SELECT_CONFLICT
--

CREATE SCHEMA on_conflict;
SET search_path TO on_conflict, public;
SET citus.next_shard_id TO 1900000;
SET citus.shard_replication_factor TO 1;

CREATE TABLE target_table(col_1 int primary key, col_2 int);
SELECT create_distributed_table('target_table','col_1');
INSERT INTO target_table VALUES(1,2),(2,3),(3,4),(4,5),(5,6);

CREATE TABLE source_table_1(col_1 int primary key, col_2 int, col_3 int);
SELECT create_distributed_table('source_table_1','col_1');
INSERT INTO source_table_1 VALUES(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);

CREATE TABLE source_table_2(col_1 int, col_2 int, col_3 int);
SELECT create_distributed_table('source_table_2','col_1');
INSERT INTO source_table_2 VALUES(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10);

SET client_min_messages to debug1;

-- Generate series directly on the coordinator and on conflict do nothing
INSERT INTO target_table (col_1, col_2)
SELECT
	s, s
FROM
	generate_series(1,10) s
ON CONFLICT DO NOTHING;

-- Generate series directly on the coordinator and on conflict update the target table
INSERT INTO target_table (col_1, col_2)
SELECT s, s
FROM
	generate_series(1,10) s
ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 + 1;

-- Since partition columns do not match, pull the data to the coordinator
-- and do not change conflicted values
INSERT INTO target_table
SELECT
	col_2, col_3
FROM
	source_table_1
ON CONFLICT DO NOTHING;

-- Since partition columns do not match, pull the data to the coordinator
-- and update the non-partition column. Query is wrapped by CTE to return
-- ordered result.
WITH inserted_table AS (
	INSERT INTO target_table
	SELECT
		col_2, col_3
	FROM
		source_table_1
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *
) SELECT * FROM inserted_table ORDER BY 1;

-- Subquery should be recursively planned due to the limit and do nothing on conflict
INSERT INTO target_table
SELECT
	col_1, col_2
FROM (
	SELECT
		col_1, col_2, col_3
	FROM
		source_table_1
	LIMIT 5
) as foo
ON CONFLICT DO NOTHING;

-- Subquery should be recursively planned due to the limit and update on conflict
-- Query is wrapped by CTE to return ordered result.
WITH inserted_table AS (
	INSERT INTO target_table
	SELECT
		col_1, col_2
	FROM (
		SELECT
			col_1, col_2, col_3
		FROM
			source_table_1
		LIMIT 5
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *
) SELECT * FROM inserted_table ORDER BY 1;

-- Test with multiple subqueries. Query is wrapped by CTE to return ordered result.
WITH inserted_table AS (
	INSERT INTO target_table
	SELECT
		col_1, col_2
	FROM (
		(SELECT
			col_1, col_2, col_3
		FROM
			source_table_1
		LIMIT 5)
		UNION
		(SELECT
			col_1, col_2, col_3
		FROM
			source_table_2
		LIMIT 5)
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = 0 RETURNING *
) SELECT * FROM inserted_table ORDER BY 1;

-- Get the select part from cte and do nothing on conflict
WITH cte AS MATERIALIZED (
	SELECT col_1, col_2 FROM source_table_1
)
INSERT INTO target_table SELECT * FROM cte ON CONFLICT DO NOTHING;


-- Get the select part from cte and update on conflict
WITH cte AS MATERIALIZED (
	SELECT col_1, col_2 FROM source_table_1
)
INSERT INTO target_table SELECT * FROM cte ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 + 1;
SELECT * FROM target_table ORDER BY 1;


-- Test with multiple CTEs
WITH cte AS(
	SELECT col_1, col_2 FROM source_table_1
), cte_2 AS(
	SELECT col_1, col_2 FROM source_table_2
)
INSERT INTO target_table ((SELECT * FROM cte) UNION (SELECT * FROM cte_2)) ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 + 1;
SELECT * FROM target_table ORDER BY 1;

WITH inserted_table AS MATERIALIZED (
	WITH cte AS MATERIALIZED (
		SELECT col_1, col_2, col_3 FROM source_table_1
	), cte_2 AS MATERIALIZED (
		SELECT col_1, col_2 FROM cte
	)
	INSERT INTO target_table SELECT * FROM cte_2 ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 + 1 RETURNING *
) SELECT * FROM inserted_table ORDER BY 1;

WITH cte AS MATERIALIZED (
	WITH basic AS MATERIALIZED (
		SELECT col_1, col_2 FROM source_table_1
	)
	INSERT INTO target_table (SELECT * FROM basic) ON CONFLICT DO NOTHING RETURNING *
)
UPDATE target_table SET col_2 = 4 WHERE col_1 IN (SELECT col_1 FROM cte);

RESET client_min_messages;

-- Following query is supported by using repartition join for the insert/select
SELECT coordinator_plan($Q$
EXPLAIN (costs off)
WITH cte AS (
	SELECT
		col_1, col_2
   	FROM
   		source_table_1
)
INSERT INTO target_table
SELECT
	source_table_1.col_1,
	source_table_1.col_2
FROM cte, source_table_1
WHERE cte.col_1 = source_table_1.col_1 ON CONFLICT DO NOTHING;
$Q$);


-- Tests with foreign key to reference table
CREATE TABLE test_ref_table (key int PRIMARY KEY);
SELECT create_reference_table('test_ref_table');
INSERT INTO test_ref_table VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
ALTER TABLE target_table ADD CONSTRAINT fkey FOREIGN KEY (col_1) REFERENCES test_ref_table(key) ON DELETE CASCADE;

BEGIN;
	TRUNCATE test_ref_table CASCADE;
	INSERT INTO
		target_table
	SELECT
		col_2,
		col_1
	FROM source_table_1 ON CONFLICT (col_1) DO UPDATE SET col_2 = 55 RETURNING *;
ROLLBACK;

BEGIN;
	DELETE FROM test_ref_table WHERE key > 10;
	WITH r AS (
		INSERT INTO
			target_table
		SELECT
			col_2,
			col_1
		FROM source_table_1 ON CONFLICT (col_1) DO UPDATE SET col_2 = 1 RETURNING *)
	SELECT * FROM r ORDER BY col_1;
ROLLBACK;

-- Following two queries are supported since we no not modify but only select from
-- the target_table after modification on test_ref_table.
BEGIN;
	TRUNCATE test_ref_table CASCADE;
	INSERT INTO
 		source_table_1
	SELECT
		col_2,
		col_1
	FROM target_table ON CONFLICT (col_1) DO UPDATE SET col_2 = 55 RETURNING *;
ROLLBACK;

BEGIN;
	DELETE FROM test_ref_table;
	INSERT INTO
 		source_table_1
	SELECT
 		col_2,
 		col_1
 	FROM target_table ON CONFLICT (col_1) DO UPDATE SET col_2 = 55 RETURNING *;
ROLLBACK;


-- INSERT .. SELECT with different column types
CREATE TABLE source_table_3(col_1 numeric, col_2 numeric, col_3 numeric);
SELECT create_distributed_table('source_table_3','col_1');
INSERT INTO source_table_3 VALUES(1,11,1),(2,22,2),(3,33,3),(4,44,4),(5,55,5);

CREATE TABLE source_table_4(id int, arr_val text[]);
SELECT create_distributed_table('source_table_4','id');
INSERT INTO source_table_4 VALUES(1, '{"abc","cde","efg"}'), (2, '{"xyz","tvu"}');

CREATE TABLE target_table_2(id int primary key, arr_val char(10)[]);
SELECT create_distributed_table('target_table_2','id');
INSERT INTO target_table_2 VALUES(1, '{"abc","def","gyx"}');

SET client_min_messages to debug1;

INSERT INTO target_table
SELECT
	col_1, col_2
FROM
	source_table_3
ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2;
SELECT * FROM target_table ORDER BY 1;

INSERT INTO target_table_2
SELECT
	*
FROM
	source_table_4
ON CONFLICT DO NOTHING;
SELECT * FROM target_table_2 ORDER BY 1;

RESET client_min_messages;

-- Test with shard_replication_factor = 2
SET citus.shard_replication_factor to 2;

DROP TABLE target_table, source_table_1, source_table_2;

CREATE TABLE target_table(col_1 int primary key, col_2 int);
SELECT create_distributed_table('target_table','col_1');
INSERT INTO target_table VALUES(1,2),(2,3),(3,4),(4,5),(5,6);

CREATE TABLE source_table_1(col_1 int, col_2 int, col_3 int);
SELECT create_distributed_table('source_table_1','col_1');
INSERT INTO source_table_1 VALUES(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);

CREATE TABLE source_table_2(col_1 int, col_2 int, col_3 int);
SELECT create_distributed_table('source_table_2','col_1');
INSERT INTO source_table_2 VALUES(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10);

SET client_min_messages to debug1;

-- Generate series directly on the coordinator and on conflict do nothing
INSERT INTO target_table (col_1, col_2)
SELECT
	s, s
FROM
	generate_series(1,10) s
ON CONFLICT DO NOTHING;

-- Test with multiple subqueries
INSERT INTO target_table
SELECT
	col_1, col_2
FROM (
	(SELECT
		col_1, col_2, col_3
	FROM
		source_table_1
	LIMIT 5)
	UNION
	(SELECT
		col_1, col_2, col_3
	FROM
		source_table_2
	LIMIT 5)
) as foo
ON CONFLICT(col_1) DO UPDATE SET col_2 = 0;
SELECT * FROM target_table ORDER BY 1;


WITH cte AS MATERIALIZED(
	SELECT col_1, col_2, col_3 FROM source_table_1
), cte_2 AS MATERIALIZED(
	SELECT col_1, col_2 FROM cte
)
INSERT INTO target_table SELECT * FROM cte_2 ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 + 1;
SELECT * FROM target_table ORDER BY 1;

-- make sure that even if COPY switchover happens
-- the results are correct
SET citus.copy_switchover_threshold TO 1;
TRUNCATE target_table;

-- load some data to make sure copy commands switch over connections
INSERT INTO target_table SELECT i,0 FROM generate_series(0,500)i;

-- make sure that SELECT only uses 1 connection 1 node
-- yet still COPY commands use 1 connection per co-located
-- intermediate result file
SET citus.max_adaptive_executor_pool_size TO 1;

INSERT INTO target_table SELECT * FROM target_table LIMIT 10000 ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 + 1;
SELECT DISTINCT col_2 FROM target_table;

WITH cte_1 AS (INSERT INTO target_table SELECT * FROM target_table LIMIT 10000 ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 + 1 RETURNING *)
SELECT DISTINCT col_2 FROM cte_1;

RESET client_min_messages;
DROP SCHEMA on_conflict CASCADE;
