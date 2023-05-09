-- ===================================================================
-- test recursive planning functionality with subqueries and CTEs
-- ===================================================================
SET search_path TO subquery_and_ctes;


CREATE TABLE users_table_local AS SELECT * FROM users_table;
SET citus.shard_replication_factor TO 1;

CREATE TABLE dist_table (id int, value int);
SELECT create_distributed_table('dist_table', 'id', colocate_with => 'users_table');
INSERT INTO dist_table (id, value) VALUES(1, 2),(2, 3),(3,4);

CREATE FUNCTION func() RETURNS TABLE (id int, value int) AS $$
	SELECT 1, 2
$$ LANGUAGE SQL;

SET client_min_messages TO DEBUG1;

-- CTEs are recursively planned, and subquery foo is also recursively planned
-- final plan becomes a router plan
WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte JOIN dist_cte ON dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- CTEs are colocated, route entire query
WITH cte1 AS (
   SELECT * FROM users_table WHERE user_id = 1
), cte2 AS (
   SELECT * FROM events_table WHERE user_id = 1
)
SELECT cte1.user_id, cte1.value_1, cte2.user_id, cte2.event_type
FROM cte1, cte2
ORDER BY cte1.user_id, cte1.value_1, cte2.user_id, cte2.event_type
LIMIT 5;

-- CTEs aren't colocated, CTEs become intermediate results
WITH cte1 AS MATERIALIZED (
   SELECT * FROM users_table WHERE user_id = 1
), cte2 AS MATERIALIZED (
   SELECT * FROM events_table WHERE user_id = 6
)
SELECT cte1.user_id, cte1.value_1, cte2.user_id, cte2.user_id
FROM cte1, cte2
ORDER BY cte1.user_id, cte1.value_1, cte2.user_id, cte2.event_type
LIMIT 5;

-- users_table & dist_table are colocated, route entire query
WITH cte1 AS (
   SELECT * FROM users_table WHERE user_id = 1
)
UPDATE dist_table dt SET value = cte1.value_1
FROM cte1 WHERE cte1.user_id = dt.id AND dt.id = 1;

-- users_table & events_table & dist_table are colocated, route entire query
WITH cte1 AS (
   SELECT * FROM users_table WHERE user_id = 1
), cte2 AS (
   SELECT * FROM events_table WHERE user_id = 1
)
UPDATE dist_table dt SET value = cte1.value_1 + cte2.event_type
FROM cte1, cte2 WHERE cte1.user_id = dt.id AND dt.id = 1;

-- all relations are not colocated, CTEs become intermediate results
WITH cte1 AS MATERIALIZED (
   SELECT * FROM users_table WHERE user_id = 1
), cte2 AS MATERIALIZED (
   SELECT * FROM events_table WHERE user_id = 6
)
UPDATE dist_table dt SET value = cte1.value_1 + cte2.event_type
FROM cte1, cte2 WHERE cte1.user_id = dt.id AND dt.id = 1;

-- volatile function calls should not be routed
WITH cte1 AS MATERIALIZED (SELECT id, value FROM func())
UPDATE dist_table dt SET value = cte1.value
FROM cte1 WHERE dt.id = 1;

-- CTEs are recursively planned, and subquery foo is also recursively planned
-- final plan becomes a real-time plan since we also have events_table in the
-- range table entries
WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte JOIN dist_cte ON dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo, events_table
	  WHERE foo.user_id = cte.user_id AND events_table.user_id = cte.user_id;

-- CTEs are replaced and subquery in WHERE is also replaced
-- but the query is still real-time query since users_table is in the
-- range table list
WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte JOIN dist_cte ON dist_cte.user_id=local_cte.user_id
)
SELECT DISTINCT cte.user_id
FROM users_table, cte
WHERE
	users_table.user_id = cte.user_id AND
	users_table.user_id IN (SELECT DISTINCT value_2 FROM users_table WHERE value_1 >= 1 AND value_1 <= 20 ORDER BY 1 LIMIT 5)
    ORDER BY 1 DESC;

-- subquery in WHERE clause is planned recursively due to the recurring table
-- in FROM clause
WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte JOIN dist_cte ON dist_cte.user_id=local_cte.user_id
)
SELECT DISTINCT cte.user_id
FROM cte
WHERE
	cte.user_id IN (SELECT DISTINCT user_id FROM users_table WHERE value_1 >= 1 AND value_1 <= 20)
    ORDER BY 1 DESC;

-- CTEs inside a subquery and the final query becomes a router
-- query
SELECT
   user_id
FROM
    (
	     WITH cte AS MATERIALIZED (
	    SELECT
	    	DISTINCT users_table.user_id
	     FROM
	     	users_table, events_table
	     WHERE
	     	users_table.user_id = events_table.user_id AND
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo
ORDER BY 1 DESC;


-- CTEs inside a subquery and the final query becomes a
-- real-time query since the other subquery is safe to pushdown
SELECT
   bar.user_id
FROM
    (
	     WITH cte AS MATERIALIZED (
	    SELECT
	    	DISTINCT users_table.user_id
	     FROM
	     	users_table, events_table
	     WHERE
	     	users_table.user_id = events_table.user_id AND
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo,
    (
	    SELECT
	    	DISTINCT users_table.user_id
	     FROM
	     	users_table, events_table
	     WHERE
	     	users_table.user_id = events_table.user_id AND
	     event_type IN (1,2,3,4)

     ) as bar
WHERE foo.user_id = bar.user_id
ORDER BY 1 DESC;

-- CTEs inside a deeper subquery
-- and also the subquery that contains the CTE is replaced
SELECT
   DISTINCT bar.user_id
FROM
    (
	     WITH cte AS MATERIALIZED (
	    SELECT
	    	DISTINCT users_table.user_id
	     FROM
	     	users_table, events_table
	     WHERE
	     	users_table.user_id = events_table.user_id AND
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo,
    (
	    SELECT
	    	users_table.user_id, some_events.event_type
	     FROM
	     	users_table,
	     	(
	     		WITH cte AS MATERIALIZED (
			    SELECT
			    	event_type, users_table.user_id
			     FROM
			     	users_table, events_table
			     WHERE
			     	users_table.user_id = events_table.user_id AND
			     value_1 IN (1,2)
			     ) SELECT * FROM cte ORDER BY 1 DESC
	     	) as some_events
	     WHERE
	     	users_table.user_id = some_events.user_id AND
	     event_type IN (1,2,3,4)
	     ORDER BY 2,1
	     LIMIT 2

     ) as bar
WHERE foo.user_id = bar.user_id
ORDER BY 1 DESC LIMIT 5;



-- CTEs on the different parts of the query is replaced
-- and subquery foo is also replaced since it contains
-- DISTINCT on a non-partition key
SELECT * FROM
(
	WITH cte AS MATERIALIZED (
		WITH local_cte AS MATERIALIZED (
			SELECT * FROM users_table_local
		),
		dist_cte AS MATERIALIZED (
			SELECT user_id FROM events_table
		)
		SELECT dist_cte.user_id FROM local_cte JOIN dist_cte ON dist_cte.user_id=local_cte.user_id
	)
	SELECT DISTINCT cte.user_id
	FROM users_table, cte
	WHERE
		users_table.user_id = cte.user_id AND
		users_table.user_id IN
			(WITH cte_in_where AS (SELECT DISTINCT value_2 FROM users_table WHERE value_1 >= 1 AND value_1 <= 20 ORDER BY 1 LIMIT 5) SELECT * FROM cte_in_where)
	    ORDER BY 1 DESC
	    ) as foo,
			events_table
		WHERE
			foo.user_id = events_table.value_2
ORDER BY 3 DESC, 2 DESC, 1 DESC
LIMIT 5;


-- now recursively plan subqueries inside the CTEs that contains LIMIT and OFFSET
WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT
			user_id
		FROM
			events_table,
			(SELECT DISTINCT value_2 FROM users_table OFFSET 0) as foo
		WHERE
			events_table.user_id = foo.value_2 AND
			events_table.user_id IN (SELECT DISTINCT value_1 FROM users_table ORDER BY 1 LIMIT 3)
	)
	SELECT dist_cte.user_id FROM local_cte JOIN dist_cte ON dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- the same query, but this time the CTEs also live inside a subquery
SELECT
	*
FROM
(

	WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT
			user_id
		FROM
			events_table,
			(SELECT DISTINCT value_2 FROM users_table OFFSET 0) as foo
		WHERE
			events_table.user_id = foo.value_2 AND
			events_table.user_id IN (SELECT DISTINCT value_1 FROM users_table ORDER BY 1 LIMIT 3)
	)
	SELECT dist_cte.user_id FROM local_cte JOIN dist_cte ON dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)  as cnt
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id

) as foo, users_table WHERE foo.cnt > users_table.value_2
ORDER BY 3 DESC, 1 DESC, 2 DESC, 4 DESC
LIMIT 5;

-- recursive CTES are not supported inside subqueries as well
SELECT
   bar.user_id
FROM
    (
	     WITH RECURSIVE cte AS MATERIALIZED (
	    SELECT
	    	DISTINCT users_table.user_id
	     FROM
	     	users_table, events_table
	     WHERE
	     	users_table.user_id = events_table.user_id AND
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo,
    (
	    SELECT
	    	DISTINCT users_table.user_id
	     FROM
	     	users_table, events_table
	     WHERE
	     	users_table.user_id = events_table.user_id AND
	     event_type IN (1,2,3,4)

     ) as bar
WHERE foo.user_id = bar.user_id
ORDER BY 1 DESC;

CREATE TABLE ref_table_1 (a int);
SELECT create_reference_table('ref_table_1');

CREATE TABLE ref_table_2 (a int);
SELECT create_reference_table('ref_table_2');

CREATE TABLE dist (a int,  b text);
SELECT create_distributed_table('dist', 'a');

INSERT INTO ref_table_1 SELECT * FROM generate_series(1, 10);
INSERT INTO ref_table_2 SELECT * FROM generate_series(1, 10);
INSERT INTO dist SELECT * FROM generate_series(1, 10);

SELECT count(*) FROM
	(SELECT DISTINCT ref_table_1.a + 1 as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a)) as foo
		JOIN
	dist
		ON(dist.a = foo.a);

SELECT count(*) FROM
	(SELECT DISTINCT ref_table_1.a + 1 +ref_table_2.a + ref_table_1.a as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a)) as foo
		JOIN
	dist
		ON(dist.a = foo.a);

SELECT count(*) FROM
	(SELECT ref_table_1.a + 1 as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a) GROUP BY ref_table_1.a + 1) as foo
		JOIN
	dist
		ON(dist.a = foo.a);

SELECT count(*) FROM
	(SELECT ref_table_1.a + ref_table_2.a + 1 as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a) GROUP BY ref_table_1.a + ref_table_2.a + 1) as foo
		JOIN
	dist
		ON(dist.a = foo.a);

SELECT  count(*) FROM (
  SELECT
    a, lag(a) OVER my_win  as lag_event_type, row_number() OVER my_win as row_no
  FROM
    ref_table_1 WINDOW my_win AS (PARTITION BY a + 1)) as foo

		JOIN
	dist
		ON(dist.a = foo.a);


WITH foo AS (
	SELECT DISTINCT ref_table_1.a + 1 as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a)
)
SELECT count(*) FROM
	foo
		JOIN
	dist
		ON(dist.a = foo.a);

WITH foo AS (
	SELECT DISTINCT ref_table_1.a + 1 +ref_table_2.a + ref_table_1.a as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a)
)
SELECT count(*) FROM
	foo
		JOIN
	dist
		ON(dist.a = foo.a);

WITH foo AS (
	SELECT ref_table_1.a + 1 as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a) GROUP BY ref_table_1.a + 1
)
SELECT count(*) FROM
	foo
		JOIN
	dist
		ON(dist.a = foo.a);

WITH foo AS (
	SELECT ref_table_1.a + ref_table_2.a + 1 as a FROM  ref_table_1 JOIN ref_table_2 ON (ref_table_1.a = ref_table_2.a) GROUP BY ref_table_1.a + ref_table_2.a + 1
)
SELECT count(*) FROM
	foo
		JOIN
	dist
		ON(dist.a = foo.a);

WITH foo AS (
  SELECT
    a, lag(a) OVER my_win  as lag_event_type, row_number() OVER my_win as row_no
  FROM
    ref_table_1 WINDOW my_win AS (PARTITION BY a + 1)
)
SELECT count(*) FROM foo JOIN dist ON(dist.a = foo.a);


-- We error-out when there's an error in execution of the query. By repeating it
-- multiple times, we increase the chance of this test failing before PR #1903.
SET client_min_messages TO ERROR;
DO $$
DECLARE
	errors_received INTEGER;
BEGIN
errors_received := 0;
FOR i IN 1..3 LOOP
	BEGIN
		WITH cte as (
			SELECT
				user_id, value_2
			from
				events_table
		)
		SELECT * FROM users_table where value_2 < (
			SELECT
				min(cte.value_2)
			FROM
				cte
			WHERE
				users_table.user_id=cte.user_id
			GROUP BY
				user_id, cte.value_2);
	EXCEPTION WHEN OTHERS THEN
		IF SQLERRM LIKE 'more than one row returned by a subquery%%' THEN
			errors_received := errors_received + 1;
		ELSIF SQLERRM LIKE 'failed to execute task%' THEN
			errors_received := errors_received + 1;
		END IF;
	END;
END LOOP;
RAISE '(%/3) failed to execute one of the tasks', errors_received;
END;
$$;

SET client_min_messages TO WARNING;
DROP SCHEMA subquery_and_ctes CASCADE;
SET search_path TO public;
