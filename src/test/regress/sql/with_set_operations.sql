-- ===================================================================
-- test recursive planning functionality with Set Operations and CTEs
-- ===================================================================

SET client_min_messages TO DEBUG1;


-- use ctes inside unions on the top level
WITH
cte_1 AS (SELECT user_id FROM users_table),
cte_2 AS (SELECT user_id FROM events_table)
(SELECT * FROM cte_1) UNION (SELECT * FROM cte_2)
ORDER BY 1 DESC;


-- use ctes inside unions in a subquery
WITH
cte_1 AS (SELECT user_id FROM users_table),
cte_2 AS (SELECT user_id FROM events_table)
SELECT
	count(*)
FROM (
		(SELECT * FROM cte_1) UNION (SELECT * FROM cte_2)
	) as foo;


-- cte with unions of other ctes
WITH
cte_1 AS (SELECT user_id FROM users_table),
cte_2 AS (SELECT user_id FROM events_table),
cte_3 AS ((SELECT * FROM cte_1) UNION (SELECT * FROM cte_2))
SELECT * FROM cte_3 ORDER BY 1 DESC;

-- cte with unions of distributed table
WITH
cte_1 AS ((SELECT user_id FROM users_table) UNION (SELECT user_id FROM users_table))
SELECT * FROM cte_1 ORDER BY 1 DESC;

-- cte with unions of tables is intersected with another query
WITH
cte_1 AS ((SELECT user_id FROM users_table) UNION (SELECT user_id FROM users_table))
(SELECT * FROM cte_1) INTERSECT (SELECT user_id FROM users_table) ORDER BY 1 DESC;

-- cte with unions of tables is intersected with another query that involves except
WITH
cte_1 AS ((SELECT user_id FROM users_table) UNION (SELECT user_id FROM users_table))
(SELECT * FROM cte_1)
	INTERSECT
((SELECT user_id FROM events_table WHERE user_id < 3) EXCEPT (SELECT user_id FROM users_table WHERE user_id > 4)) ORDER BY 1 DESC;


-- CTE inside a top level EXCEPT
(WITH cte_1 AS (SELECT user_id FROM events_table WHERE user_id < 3) SELECT * FROM cte_1) INTERSECT (SELECT user_id FROM users_table) ORDER BY 1;

-- INTERSECT inside a CTE, which is inside a subquery
SELECT
	DISTINCT users_table.user_id
FROM
	users_table,
	(WITH cte_1 AS (SELECT user_id FROM events_table WHERE user_id < 3 INTERSECT
					SELECT user_id FROM events_table WHERE user_id < 2)
	 SELECT * FROM cte_1) as foo
WHERE
	users_table.user_id = foo.user_id
ORDER BY 1 DESC;

-- UNION is created via outputs of CTEs, which is inside a subquery
-- and the subquery is joined with a distributed table
SELECT
	count(*)
FROM
	events_table,
	(
		WITH
		cte_1 AS (SELECT user_id FROM users_table),
		cte_2 AS (SELECT user_id FROM events_table)
		(SELECT * FROM cte_1) UNION (SELECT * FROM cte_2)
	) as foo
WHERE foo.user_id = events_table.event_type;

-- joins inside unions that are safe to pushdown
(SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id )
INTERSECT
(SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id )
ORDER BY 1 DESC;

-- joins inside unions that are not safe to pushdown
(SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id LIMIT 10)
INTERSECT
(SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id LIMIT 10)
ORDER BY 1 DESC;

-- joins inside unions that are not safe to pushdown inside a subquery
SELECT
	count(*)
FROM
	(SELECT DISTINCT value_2 FROM events_table) as events_table,
	(WITH foo AS
		((SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id )
	 	INTERSECT
		(SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id LIMIT 10))
	 SELECT * FROM foo)
	as foo
WHERE
	foo.user_id = events_table.value_2;

-- joins inside unions some safe to pushdown
SELECT
	count(*)
FROM
	(WITH events_table AS (SELECT DISTINCT user_id FROM events_table) SELECT * FROM events_table) as events_table,
	((SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id )
	INTERSECT
	(SELECT DISTINCT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id LIMIT 10)) as foo
WHERE
	foo.user_id = events_table.user_id;

-- CTE inside unions
(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1) UNION
(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1) ORDER BY 1 DESC;

-- more complex CTEs inside unions
SELECT
	count(*)
FROM
	(
		(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1) UNION
		(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1)
	) as foo,
	users_table
	WHERE users_table.value_2 = foo.user_id;

-- CTEs with less alias than the input subquery
(WITH cte_1(x) AS (SELECT user_id, value_2 FROM users_table) SELECT * FROM cte_1) UNION
(WITH cte_1(x) AS (SELECT user_id, value_2 FROM users_table) SELECT * FROM cte_1) ORDER BY 1 DESC, 2 DESC LIMIT 5;


-- simple subqueries in WHERE with unions
SELECT
	count(*)
FROM
	users_table
WHERE
	value_2 IN
	(
		WITH cte_1 AS
		(
			(SELECT user_id FROM users_table)
			UNION
		 	(SELECT user_id FROM events_table)
		 ) SELECT DISTINCT user_id FROM cte_1
)
ORDER BY 1 DESC;

-- simple subqueries in WHERE with unions and ctes
SELECT
	count(*)
FROM
	users_table
WHERE
	value_2 IN
	(
		WITH
		cte_1 AS (SELECT user_id FROM users_table),
		cte_2 AS (SELECT user_id FROM events_table)
		(SELECT * FROM cte_1) UNION (SELECT * FROM cte_2)
)
ORDER BY 1 DESC;

-- unions and ctes inside subqueries in where clause with a pushdownable correlated subquery
SELECT
	DISTINCT user_id
FROM
	events_table
WHERE
	event_type IN
(
	SELECT
		users_table.user_id
	FROM
		(
			(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1) UNION
			(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1)
		) as foo,
		users_table
		WHERE users_table.value_2 = foo.user_id  AND events_table.user_id = users_table.user_id
)
ORDER BY 1 DESC;

-- unions and ctes inside subqueries in where clause with a not pushdownable correlated subquery
-- should error out
SELECT
	DISTINCT user_id
FROM
	events_table
WHERE
	event_type IN
(
	SELECT
		users_table.user_id
	FROM
		(
			(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1) UNION
			(WITH cte_1 AS (SELECT user_id FROM users_table) SELECT * FROM cte_1)
		) as foo,
		users_table
		WHERE users_table.value_2 = foo.user_id  AND events_table.user_id = users_table.user_id
		LIMIT 5
)
ORDER BY 1 DESC;


SET client_min_messages TO DEFAULT;

SET search_path TO public;
