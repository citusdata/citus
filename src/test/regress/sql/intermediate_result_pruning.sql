CREATE SCHEMA intermediate_result_pruning;
SET search_path TO intermediate_result_pruning;


SET citus.shard_count TO 4;
SET citus.next_shard_id TO 1480000;
SET citus.shard_replication_factor = 1;

CREATE TABLE table_1 (key int, value text);
SELECT create_distributed_table('table_1', 'key');

CREATE TABLE table_2 (key int, value text);
SELECT create_distributed_table('table_2', 'key');


CREATE TABLE table_3 (key int, value text);
SELECT create_distributed_table('table_3', 'key');

CREATE TABLE ref_table (key int, value text);
SELECT create_reference_table('ref_table');


-- load some data
INSERT INTO table_1    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4');
INSERT INTO table_2    VALUES                     (3, '3'), (4, '4'), (5, '5'), (6, '6');
INSERT INTO table_3    VALUES                     (3, '3'), (4, '4'), (5, '5'), (6, '6');
INSERT INTO ref_table  VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6');

-- see which workers are hit for intermediate results
SET client_min_messages TO DEBUG1;

-- a very basic case, where the intermediate result
-- should go to both workers
WITH some_values_1 AS
	(SELECT key FROM table_1 WHERE value IN ('3', '4'))
SELECT
	count(*)
FROM
	some_values_1 JOIN table_2 USING (key);


-- a very basic case, where the intermediate result
-- should only go to one worker because the final query is a router
-- we use random() to prevent postgres inline the CTE(s)
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4'))
SELECT
	count(*)
FROM
	some_values_1 JOIN table_2 USING (key) WHERE table_2.key = 1;

-- a similar query, but with a reference table now
-- given that reference tables are replicated to all nodes
-- we have to broadcast to all nodes
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4'))
SELECT
	count(*)
FROM
	some_values_1 JOIN ref_table USING (key);


-- a similar query as above, but this time use the CTE inside
-- another CTE
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1)
SELECT
	count(*)
FROM
	some_values_2 JOIN table_2 USING (key) WHERE table_2.key = 1;

-- the second CTE does a join with a distributed table
-- and the final query is a router query
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key))
SELECT
	count(*)
FROM
	some_values_2 JOIN table_2 USING (key) WHERE table_2.key = 3;

-- the first CTE is used both within second CTE and the final query
-- the second CTE does a join with a distributed table
-- and the final query is a router query
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key))
SELECT
	count(*)
FROM
	(some_values_2 JOIN table_2 USING (key)) JOIN some_values_1 USING (key) WHERE table_2.key = 3;

-- the first CTE is used both within second CTE and the final query
-- the second CTE does a join with a distributed table but a router query on a worker
-- and the final query is another router query on another worker
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key) WHERE table_2.key = 1)
SELECT
	count(*)
FROM
	(some_values_2 JOIN table_2 USING (key)) JOIN some_values_1 USING (key) WHERE table_2.key = 3;

-- the first CTE is used both within second CTE and the final query
-- the second CTE does a join with a distributed table but a router query on a worker
-- and the final query is a router query on the same worker, so the first result is only
-- broadcasted to a single node
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key) WHERE table_2.key = 1)
SELECT
	count(*)
FROM
	(some_values_2 JOIN table_2 USING (key)) JOIN some_values_1 USING (key) WHERE table_2.key = 1;

-- the same query with the above, but the final query is hitting all shards
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key))
SELECT
	count(*)
FROM
	(some_values_2 JOIN table_2 USING (key)) JOIN some_values_1 USING (key) WHERE table_2.key != 3;

-- even if we add a filter on the first query and make it a router query,
-- the first intermediate result still hits all workers because of the final
-- join is hitting all workers
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key) WHERE table_2.key = 3)
SELECT
	count(*)
FROM
	(some_values_2 JOIN table_2 USING (key)) JOIN some_values_1 USING (key) WHERE table_2.key != 3;

-- the reference table is joined with a distributed table and an intermediate
-- result, but the distributed table hits all shards, so the intermediate
-- result is sent to all nodes
WITH some_values_1 AS
	(SELECT key, random() FROM ref_table WHERE value IN ('3', '4'))
SELECT
	count(*)
FROM
	(some_values_1 JOIN ref_table USING (key)) JOIN table_2 USING (key);

-- similar query as above, but this time the whole query is a router
-- query, so no intermediate results
WITH some_values_1 AS
	(SELECT key, random() FROM ref_table WHERE value IN ('3', '4'))
SELECT
	count(*)
FROM
	(some_values_1 JOIN ref_table USING (key)) JOIN table_2 USING (key) WHERE table_2.key = 1;


-- now, the second CTE has a single shard join with a distributed table
-- so the first CTE should only be broadcasted to that node
-- since the final query doesn't have a join, it should simply be broadcasted
-- to one node
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key) WHERE key = 1)
SELECT
	count(*)
FROM
	some_values_2;


-- the same query inlined inside a CTE, and the final query has a
-- join with a distributed table
WITH top_cte as (
		WITH some_values_1 AS
		(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
		some_values_2 AS
		(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key) WHERE key = 1)
	SELECT
		DISTINCT key
	FROM
		some_values_2
)
SELECT
	count(*)
FROM
	top_cte JOIN table_2 USING (key);


-- very much the same query, but this time the top query is also a router query
-- on a single worker, so all intermediate results only hit a single node
WITH top_cte as (
		WITH some_values_1 AS
		(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
		some_values_2 AS
		(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key) WHERE key = 1)
	SELECT
		DISTINCT key
	FROM
		some_values_2
)
SELECT
	count(*)
FROM
	top_cte JOIN table_2 USING (key) WHERE table_2.key = 2;


-- some_values_1 is first used by a single shard-query, and than with a multi-shard
-- CTE, finally a cartesian product join
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1 JOIN table_2 USING (key) WHERE key = 1),
	some_values_3 AS
	(SELECT key FROM (some_values_2 JOIN table_2 USING (key)) JOIN some_values_1 USING (key))
SELECT * FROM some_values_3 JOIN ref_table ON (true);



-- join on intermediate results, so should only
-- go to a single node
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM table_2 WHERE value IN ('3', '4'))
SELECT count(*) FROM some_values_2 JOIN some_values_1 USING (key);

-- same query with WHERE false make sure that we're not broken
-- for such edge cases
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM table_2 WHERE value IN ('3', '4'))
SELECT count(*) FROM some_values_2 JOIN some_values_1 USING (key) WHERE false;


-- do not use some_values_2 at all, so only 2 intermediate results are
-- broadcasted
WITH some_values_1 AS
	(SELECT key, random() FROM table_1 WHERE value IN ('3', '4')),
	some_values_2 AS
	(SELECT key, random() FROM some_values_1),
	some_values_3 AS
	(SELECT key, random() FROM some_values_1)
SELECT
	count(*)
FROM
	some_values_3;

-- lets have some deeper intermediate results
-- the inner most two results and the final query (which contains only intermediate results)
-- hitting single worker, others hitting all workers
-- (see below query where all intermediate results hit a single node)
SELECT count(*) FROM
(
	SELECT avg(min::int) FROM
	(
		SELECT min(table_1.value) FROM
		(
			SELECT avg(value::int) as avg_ev_type FROM
			(
				SELECT max(value) as mx_val_1 FROM
				(
					SELECT avg(value::int) as avg FROM
					(
						SELECT cnt FROM
						(
							SELECT count(*) as cnt, value
							FROM table_1
							WHERE key = 1
							GROUP BY value
						) as level_1, table_1
						WHERE table_1.key = level_1.cnt AND key = 3
					) as level_2, table_2
					WHERE table_2.key = level_2.cnt AND key = 5
					GROUP BY level_2.cnt
				) as level_3, table_1
				WHERE value::numeric = level_3.avg AND key = 6
				GROUP BY level_3.avg
			) as level_4, table_2
			WHERE level_4.mx_val_1::int = table_2.key
			GROUP BY level_4.mx_val_1
		) as level_5, table_1
		WHERE level_5.avg_ev_type = table_1.key AND key > 111
		GROUP BY level_5.avg_ev_type
	) as level_6, table_1 WHERE table_1.key::int = level_6.min::int
	GROUP BY table_1.value
) as bar;
-- the same query where all intermediate results hits one
-- worker because each and every query is a router query -- but on different nodes
SELECT count(*) FROM
(
	SELECT avg(min::int) FROM
	(
		SELECT min(table_1.value) FROM
		(
			SELECT avg(value::int) as avg_ev_type FROM
			(
				SELECT max(value) as mx_val_1 FROM
				(
					SELECT avg(value::int) as avg FROM
					(
						SELECT cnt FROM
						(
							SELECT count(*) as cnt, value
							FROM table_1
							WHERE key = 1
							GROUP BY value
						) as level_1, table_1
						WHERE table_1.key = level_1.cnt AND key = 3
					) as level_2, table_2
					WHERE table_2.key = level_2.cnt AND key = 5
					GROUP BY level_2.cnt
				) as level_3, table_1
				WHERE value::numeric = level_3.avg AND key = 6
				GROUP BY level_3.avg
			) as level_4, table_2
			WHERE level_4.mx_val_1::int = table_2.key AND table_2.key = 1
			GROUP BY level_4.mx_val_1
		) as level_5, table_1
		WHERE level_5.avg_ev_type = table_1.key AND key = 111
		GROUP BY level_5.avg_ev_type
	) as level_6, table_1
	WHERE table_1.key::int = level_6.min::int AND table_1.key = 4
	GROUP BY table_1.value
) as bar;


-- sanity checks for set operations

-- the intermediate results should just hit a single worker
(SELECT key FROM table_1 WHERE key = 1)
INTERSECT
(SELECT key FROM table_1 WHERE key = 2);

-- the intermediate results should just hit a single worker
WITH cte_1 AS
(
	(SELECT key FROM table_1 WHERE key = 1)
	INTERSECT
	(SELECT key FROM table_1 WHERE key = 2)
),
cte_2 AS
(
	(SELECT key FROM table_1 WHERE key = 3)
	INTERSECT
	(SELECT key FROM table_1 WHERE key = 4)
)
SELECT * FROM cte_1
	UNION
SELECT * FROM cte_2;

-- one final test with SET operations, where
-- we join the results with distributed tables
-- so cte_1 should hit all workers, but still the
-- others should hit single worker each
WITH cte_1 AS
(
	(SELECT key FROM table_1 WHERE key = 1)
	INTERSECT
	(SELECT key FROM table_1 WHERE key = 2)
),
cte_2 AS
(
	SELECT count(*) FROM table_1 JOIN cte_1 USING (key)
)
SELECT * FROM cte_2;


-- sanity checks for non-colocated subquery joins
-- the recursively planned subquery (bar) should hit all
-- nodes
SELECT
	count(*)
FROM
	(SELECT key, random() FROM table_1) as foo,
	(SELECT key, random() FROM table_2) as bar
WHERE
	foo.key != bar.key;

-- the recursively planned subquery (bar) should hit one
-- node because foo goes to a single node
SELECT
	count(*)
FROM
	(SELECT key, random() FROM table_1 WHERE key = 1) as foo,
	(SELECT key, random() FROM table_2) as bar
WHERE
	foo.key != bar.key;


-- sanity checks for modification queries

-- select_data goes to a single node, because it is used in another subquery
-- raw_data is also the final router query, so hits a single shard
-- however, the subquery in WHERE clause of the DELETE query is broadcasted to all
-- nodes
BEGIN;
WITH select_data AS (
	SELECT * FROM table_1
),
raw_data AS (
	DELETE FROM table_2 WHERE key >= (SELECT min(key) FROM select_data WHERE key > 1) RETURNING *
)
SELECT * FROM raw_data;
ROLLBACK;

-- select_data goes to a single node, because it is used in another subquery
-- raw_data is also the final router query, so hits a single shard
-- however, the subquery in WHERE clause of the DELETE query is broadcasted to all
-- nodes
BEGIN;
WITH select_data AS (
	SELECT * FROM table_1
),
raw_data AS (
	DELETE FROM table_2 WHERE value::int >= (SELECT min(key) FROM select_data WHERE key > 1 + random()) RETURNING *
)
SELECT * FROM raw_data;
ROLLBACK;

-- now, we need only two intermediate results as the subquery in WHERE clause is
-- router plannable
BEGIN;
WITH select_data AS (
	SELECT * FROM table_1
),
raw_data AS (
	DELETE FROM table_2 WHERE value::int >= (SELECT min(key) FROM table_1 WHERE key > random()) AND key = 6 RETURNING *
)
SELECT * FROM raw_data;
ROLLBACK;

-- test with INSERT SELECT via coordinator

-- INSERT .. SELECT via coordinator that doesn't have any intermediate results
INSERT INTO table_1
	SELECT * FROM table_2 OFFSET 0;

-- INSERT .. SELECT via coordinator which has intermediate result,
-- and can be pruned to a single worker because the final query is on
-- single shard via filter in key
INSERT INTO table_1
	SELECT * FROM table_2 where value IN (SELECT value FROM table_1 WHERE random() > 1) AND key = 1;

-- a similar query, with more complex subquery
INSERT INTO table_1
	SELECT * FROM table_2 where key = 1 AND
 value::int IN
		(WITH cte_1 AS
		(
			(SELECT key FROM table_1 WHERE key = 1)
			INTERSECT
			(SELECT key FROM table_1 WHERE key = 2)
		),
		cte_2 AS
		(
			(SELECT key FROM table_1 WHERE key = 3)
			INTERSECT
			(SELECT key FROM table_1 WHERE key = 4)
		)
		SELECT * FROM cte_1
			UNION
		SELECT * FROM cte_2);

-- same query, cte is on the FROM clause
-- and this time the final query (and top-level intermediate result)
-- hits all the shards because table_2.key != 1
INSERT INTO table_1
	SELECT table_2.* FROM table_2,
	(WITH cte_1 AS
		(
			(SELECT key FROM table_1 WHERE key = 1)
			INTERSECT
			(SELECT key FROM table_1 WHERE key = 2)
		),
		cte_2 AS
		(
			(SELECT key FROM table_1 WHERE key = 3)
			INTERSECT
			(SELECT key FROM table_1 WHERE key = 4)
		)
		SELECT * FROM cte_1
			UNION
		SELECT * FROM cte_2
	 ) foo
	 where table_2.key != 1 AND
 	foo.key = table_2.value::int;



-- append partitioned/heap-type
SET citus.replication_model TO statement;

-- do not print out 'building index pg_toast_xxxxx_index' messages
SET client_min_messages TO DEFAULT;
CREATE TABLE range_partitioned(range_column text, data int);
SET client_min_messages TO DEBUG1;

SELECT create_distributed_table('range_partitioned', 'range_column', 'range');
SELECT master_create_empty_shard('range_partitioned');
SELECT master_create_empty_shard('range_partitioned');
SELECT master_create_empty_shard('range_partitioned');
SELECT master_create_empty_shard('range_partitioned');
SELECT master_create_empty_shard('range_partitioned');


UPDATE pg_dist_shard SET shardminvalue = 'A', shardmaxvalue = 'D' WHERE shardid = 1480013;
UPDATE pg_dist_shard SET shardminvalue = 'D', shardmaxvalue = 'G' WHERE shardid = 1480014;
UPDATE pg_dist_shard SET shardminvalue = 'G', shardmaxvalue = 'K' WHERE shardid = 1480015;
UPDATE pg_dist_shard SET shardminvalue = 'K', shardmaxvalue = 'O' WHERE shardid = 1480016;
UPDATE pg_dist_shard SET shardminvalue = 'O', shardmaxvalue = 'Z' WHERE shardid = 1480017;

-- final query goes to a single shard
SELECT
	count(*)
FROM
	range_partitioned
WHERE
	range_column = 'A' AND
	data IN (SELECT data FROM range_partitioned);


-- final query goes to three shards, so multiple workers
SELECT
	count(*)
FROM
	range_partitioned
WHERE
	range_column >= 'A' AND range_column <= 'K' AND
	data IN (SELECT data FROM range_partitioned);

-- two shards, both of which are on the first node
WITH some_data AS (
	SELECT data FROM range_partitioned
)
SELECT
	count(*)
FROM
	range_partitioned
WHERE
	range_column IN ('A', 'E') AND
	range_partitioned.data IN (SELECT data FROM some_data);

SET client_min_messages TO DEFAULT;
DROP TABLE table_1, table_2, table_3, ref_table, range_partitioned;
DROP SCHEMA intermediate_result_pruning;

