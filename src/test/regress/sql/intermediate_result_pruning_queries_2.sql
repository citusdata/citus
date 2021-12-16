SET search_path TO "intermediate result pruning";

-- sanity checks for modification queries

-- select_data goes to a single node, because it is used in another subquery
-- raw_data is also the final router query, so hits a single shard
-- however, the subquery in WHERE clause of the DELETE query is broadcasted to all
-- nodes
BEGIN;
WITH select_data AS MATERIALIZED (
	SELECT * FROM table_1
),
raw_data AS MATERIALIZED (
	DELETE FROM table_2 WHERE key >= (SELECT min(key) FROM select_data WHERE key > 1) RETURNING *
)
SELECT * FROM raw_data ORDER BY 1,2;
ROLLBACK;

-- select_data goes to a single node, because it is used in another subquery
-- raw_data is also the final router query, so hits a single shard
-- however, the subquery in WHERE clause of the DELETE query is broadcasted to all
-- nodes
BEGIN;
WITH select_data AS MATERIALIZED (
	SELECT * FROM table_1
),
raw_data AS MATERIALIZED (
	DELETE FROM table_2 WHERE value::int >= (SELECT min(key) FROM select_data WHERE key > 1 + random()) RETURNING *
)
SELECT * FROM raw_data ORDER BY 1,2;
ROLLBACK;

-- now, we need only two intermediate results as the subquery in WHERE clause is
-- router plannable
BEGIN;
WITH select_data AS MATERIALIZED (
	SELECT * FROM table_1
),
raw_data AS MATERIALIZED (
	DELETE FROM table_2 WHERE value::int >= (SELECT min(key) FROM table_1 WHERE key > random()) AND key = 6 RETURNING *
)
SELECT * FROM raw_data ORDER BY 1,2;
ROLLBACK;

-- test with INSERT SELECT via coordinator

-- INSERT .. SELECT via coordinator that doesn't have any intermediate results
-- We use offset 1 to make sure the result needs to be pulled to the coordinator, offset 0 would be optimized away
BEGIN;
INSERT INTO table_1
	SELECT * FROM table_2 OFFSET 1;
ROLLBACK;

-- INSERT .. SELECT via coordinator which has intermediate result,
-- and can be pruned to a single worker because the final query is on
-- single shard via filter in key
BEGIN;
INSERT INTO table_1
	SELECT * FROM table_2 where value IN (SELECT value FROM table_1 WHERE random() > 1) AND key = 1;
ROLLBACK;

-- a similar query, with more complex subquery
BEGIN;
INSERT INTO table_1
	SELECT * FROM table_2 where key = 1 AND
 value::int IN
		(WITH cte_1 AS MATERIALIZED
		(
			(SELECT key FROM table_1 WHERE key = 1)
			INTERSECT
			(SELECT key FROM table_1 WHERE key = 2)
		),
		cte_2 AS MATERIALIZED
		(
			(SELECT key FROM table_1 WHERE key = 3)
			INTERSECT
			(SELECT key FROM table_1 WHERE key = 4)
		)
		SELECT * FROM cte_1
			UNION
		SELECT * FROM cte_2);
ROLLBACK;

-- same query, cte is on the FROM clause
-- and this time the final query (and top-level intermediate result)
-- hits all the shards because table_2.key != 1
BEGIN;
INSERT INTO table_1
	SELECT table_2.* FROM table_2,
	(WITH cte_1 AS MATERIALIZED
		(
			(SELECT key FROM table_1 WHERE key = 1)
			INTERSECT
			(SELECT key FROM table_1 WHERE key = 2)
		),
		cte_2 AS MATERIALIZED
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
 ROLLBACK;


 BEGIN;
 	-- Insert..select is planned differently, make sure we have results everywhere.
-- We put the insert..select in a CTE here to prevent the CTE from being moved
-- into the select, which would follow the regular code path for select.
WITH stats AS MATERIALIZED (
  SELECT count(key) m FROM table_3
),
inserts AS MATERIALIZED (
  INSERT INTO table_2
  SELECT key, count(*)
  FROM table_1
  WHERE key >= (SELECT m FROM stats)
  GROUP BY key
  HAVING count(*) < (SELECT m FROM stats)
  LIMIT 1
  RETURNING *
) SELECT count(*) FROM inserts;
ROLLBACK;
