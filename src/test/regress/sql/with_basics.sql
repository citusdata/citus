-- Test the basic CTE functionality and expected error messages

CREATE TYPE xy AS (x int, y int);
SELECT run_command_on_workers('CREATE TYPE xy AS (x int, y int)');

-- CTEs in FROM should work
WITH cte AS (
	SELECT user_id, value_2 from users_table WHERE user_id IN (1, 2) ORDER BY 1,2 LIMIT 5
)
SELECT * FROM cte;

-- CTEs in WHERE should work
WITH cte AS (
	SELECT user_id from users_table ORDER BY user_id DESC LIMIT 10
)
SELECT
  value_2
FROM
  users_table
WHERE
  user_id IN (SELECT user_id FROM users_table)
ORDER BY
  value_2
LIMIT
  5;

-- nested CTEs should work
WITH cte_1 AS (
	WITH cte_1_1 AS (
	    SELECT user_id, value_2 from users_table WHERE user_id IN (1, 2) ORDER BY 2 LIMIT 5
	),
	cte_1_2 AS (
		SELECT max(user_id) AS user_id FROM cte_1_1
	)
	SELECT user_id FROM cte_1_2 ORDER BY user_id
)
SELECT value_2 FROM users_table WHERE user_id IN (SELECT user_id FROM cte_1) ORDER BY value_2 LIMIT 1;

-- Mix of FROM/WHERE queries
WITH cte_from AS (
	SELECT max(user_id) AS user_id, value_2, value_1 FROM users_table GROUP BY value_2, value_1
),
cte_where AS (
	SELECT value_2 FROM events_table
)
SELECT
  *
FROM
  (SELECT max(user_id), max(value_2) AS value_2 FROM cte_from GROUP BY value_1) f
WHERE
  value_2 IN (SELECT * FROM cte_where)
ORDER BY 
  1, 2
LIMIT
  5;

-- CTE in subquery recursively planned
SELECT user_id FROM (
  WITH cte AS (
    SELECT user_id, value_2 from users_table WHERE user_id IN (1, 2) ORDER BY 2 LIMIT 5
  )
  SELECT user_id FROM cte WHERE value_2 > 0
) a ORDER BY 1 LIMIT 3;

-- CTE outside of FROM/WHERE errors out
WITH cte AS (
	SELECT user_id FROM users_table WHERE value_2 IN (1, 2)
)
SELECT (SELECT * FROM cte);

WITH cte_basic AS (
	SELECT user_id FROM users_table WHERE user_id = 1
)
SELECT
  (SELECT user_id FROM cte_basic), user_id
FROM
  users_table;

-- single-row sublink is acceptable when there is no FROM
WITH cte AS (
	SELECT user_id FROM users_table WHERE value_2 IN (1, 2)
)
SELECT (SELECT * FROM cte ORDER BY 1 LIMIT 1);

-- group by partition column
WITH series AS (
  SELECT s AS once, s*2 AS twice FROM generate_series(1,10) s
)
SELECT user_id, count(*)
FROM
  users_table
JOIN
  series ON (user_id = once)
GROUP BY
  user_id
ORDER BY
  1, 2
LIMIT 5;

-- group by non-partition column
WITH series AS (
  SELECT s AS once, s*2 AS twice FROM generate_series(1,10) s
)
SELECT
  twice, min(user_id)
FROM
  users_table
JOIN
  series ON (user_id = once)
GROUP BY
  twice
HAVING
  twice > 5
ORDER BY
  1, 2
LIMIT 5;

-- distinct in subquery on CTE
WITH one_user AS (
	SELECT user_id from users_table WHERE user_id = 1
)
SELECT
  user_id
FROM
  users_table
WHERE
  value_2 IN (SELECT DISTINCT user_id FROM one_user)
ORDER BY
  user_id
LIMIT
  1;

-- having in subquery on CTE
WITH one_user AS (
	SELECT user_id from users_table WHERE user_id = 1
)
SELECT
  user_id
FROM
  users_table
WHERE
  value_2 IN (SELECT user_id FROM one_user GROUP BY user_id HAVING count(*) > 0)
ORDER BY
  user_id
LIMIT
  1;

-- aggregate in subquery on CTE
WITH top_users AS (
	SELECT user_id, value_2 FROM users_table ORDER BY user_id DESC LIMIT 10
)
SELECT
  user_id
FROM
  (SELECT min(user_id) AS user_id FROM top_users) top_users
JOIN
  users_table USING (user_id);

-- FOR UPDATE in subquery on CTE
WITH top_users AS (
	SELECT user_id, value_2 FROM users_table ORDER BY user_id DESC LIMIT 10
)
SELECT
  user_id
FROM
  (SELECT user_id FROM top_users FOR UPDATE) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- LIMIT in subquery on CTE
WITH top_users AS (
	SELECT user_id, value_2 FROM users_table ORDER BY user_id DESC LIMIT 10
)
SELECT
  user_id
FROM
  (SELECT user_id FROM top_users LIMIT 5) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- OFFSET in subquery on CTE
WITH top_users AS (
	SELECT user_id, value_2 FROM users_table ORDER BY user_id DESC LIMIT 10
)
SELECT
  user_id
FROM
  (SELECT user_id FROM top_users OFFSET 5) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- Unsupported join in CTE
WITH top_users AS (
	SELECT DISTINCT e.user_id FROM users_table u JOIN events_table e ON (u.user_id = e.user_id AND u.value_1 > e.value_2)
)
SELECT
  user_id
FROM
  (SELECT user_id FROM top_users WHERE user_id > 5) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- Join can be supported with another CTE
WITH events_table AS (
  SELECT * FROM events_table
),
top_users AS (
  SELECT DISTINCT e.user_id FROM users_table u JOIN events_table e ON (u.user_id = e.user_id AND u.value_1 > e.value_2)
)
SELECT
  user_id
FROM
  (SELECT user_id FROM top_users WHERE user_id > 5) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- Window functions in CTE
WITH top_users AS (
	SELECT row_number() OVER(), user_id FROM users_table ORDER BY user_id DESC LIMIT 10
)
SELECT
  user_id
FROM
  (SELECT user_id FROM top_users WHERE user_id > 5) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- Window functions that partition by the distribution column in subqueries in CTEs are ok
WITH top_users AS
  (SELECT *
   FROM
     (SELECT row_number() OVER(PARTITION BY user_id) AS row_number,
                          user_id
      FROM users_table) AS foo
)
SELECT user_id
FROM
  (SELECT user_id
   FROM top_users
   WHERE row_number > 5) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- Unsupported aggregate in CTE
WITH top_users AS (
	SELECT array_agg(user_id ORDER BY value_2) user_ids FROM users_table
)
SELECT
  user_id
FROM
  (SELECT unnest(user_ids) user_id FROM top_users) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- array_agg in CTE
WITH top_users AS (
	SELECT array_agg(user_id) user_ids FROM users_table
)
SELECT
  user_id
FROM
  (SELECT unnest(user_ids) user_id FROM top_users) top_users
JOIN
  users_table USING (user_id)
ORDER BY
  user_id
LIMIT
  5;

-- composite type array
WITH top_users AS (
	SELECT array_agg((value_1,value_2)::xy) AS p FROM users_table WHERE user_id % 2 = 0
)
SELECT
  e.user_id, sum(y)
FROM
  (SELECT (unnest(p)).* FROM top_users) tops
JOIN
  events_table e ON (tops.x = e.user_id)
GROUP BY
  e.user_id
ORDER BY
  2 DESC, 1
LIMIT
  5;

-- SELECT * FROM (SELECT * FROM cte UNION SELECT * FROM distributed_table) a; should error out
WITH cte AS (
	SELECT * FROM users_table
)
SELECT * FROM (
	SELECT * FROM cte UNION (SELECT * FROM events_table)
	) a
ORDER BY 
	1,2,3,4,5,6
LIMIT 
	10;

SELECT * FROM (
	SELECT * FROM (WITH cte AS (
			SELECT * FROM users_table
		)
		SELECT * FROM cte
	)b UNION (SELECT * FROM events_table)) a
ORDER BY 
1,2,3,4,5,6
LIMIT 
10;

-- SELECT * FROM (SELECT * FROM cte UNION SELECT * FROM cte) a; should work
WITH cte AS (
	SELECT * FROM users_table WHERE user_id IN (1, 2)
)
SELECT
  *
FROM
  (SELECT * FROM cte UNION (SELECT * FROM cte)) a
ORDER BY 
  1,2,3,4,5,6
LIMIT 
  5;

WITH cte AS (
	SELECT * FROM users_table WHERE user_id IN (1, 2) ORDER BY 1,2,3 LIMIT 5
), 
cte_2 AS (
	SELECT * FROM users_table WHERE user_id  IN (3, 4) ORDER BY 1,2,3 LIMIT 5
)
SELECT * FROM cte UNION ALL SELECT * FROM cte_2;

-- basic recursive CTE which should all error out
WITH RECURSIVE basic_recursive(x) AS (
    VALUES (1)
  UNION ALL
    SELECT user_id + 1 FROM users_table WHERE user_id < 100
)
SELECT sum(x) FROM basic_recursive;

WITH RECURSIVE basic_recursive AS (
    SELECT -1 as user_id, '2017-11-22 20:16:16.614779'::timestamp, -1, -1, -1, -1
  UNION ALL
    SELECT * FROM users_table WHERE user_id>1
)
SELECT * FROM basic_recursive ORDER BY user_id LIMIT 1;


-- basic_recursive in FROM should error out
SELECT
  *
FROM
(WITH RECURSIVE basic_recursive AS (
      SELECT -1 as user_id, '2017-11-22 20:16:16.614779'::timestamp, -1, -1, -1, -1
    UNION ALL
      SELECT * FROM users_table WHERE user_id>1
  )
  SELECT * FROM basic_recursive ORDER BY user_id LIMIT 1) cte_rec;


-- basic_recursive in WHERE with UNION ALL
SELECT
  *
FROM
  users_table
WHERE
  user_id in
(WITH RECURSIVE basic_recursive AS (
      SELECT -1 as user_id
    UNION ALL
      SELECT user_id FROM users_table WHERE user_id>1
  )
  SELECT * FROM basic_recursive ORDER BY user_id LIMIT 1);


-- one recursive one regular CTE should error out
WITH RECURSIVE basic_recursive(x) AS(
    VALUES (1)
  UNION ALL
    SELECT user_id + 1 FROM users_table WHERE user_id < 100
),
basic AS (
    SELECT count(user_id) FROM users_table
)
SELECT x FROM basic, basic_recursive;


-- one recursive one regular which SELECTs from the recursive CTE under a simple SELECT
WITH RECURSIVE basic_recursive(x) AS(
    VALUES (1)
  UNION ALL
    SELECT user_id + 1 FROM users_table WHERE user_id < 100
),
basic AS (
    SELECT count(x) FROM basic_recursive
)
SELECT * FROM basic;


-- recursive CTE in a NESTED manner
WITH regular_cte AS (
  WITH regular_2 AS (
    WITH RECURSIVE recursive AS (
        VALUES (1)
      UNION ALL
        SELECT user_id + 1 FROM users_table WHERE user_id < 100
    )
    SELECT * FROM recursive
  )
  SELECT * FROM regular_2
)
SELECT * FROM regular_cte;

-- CTEs should work with VIEWs as well
CREATE VIEW basic_view AS 
SELECT * FROM users_table;


CREATE VIEW cte_view AS
WITH cte AS (
	SELECT * FROM users_table
)
SELECT user_id, max(value_1) as value_1 FROM cte GROUP BY 1;


WITH cte_user AS (
	SELECT basic_view.user_id,events_table.value_2 FROM basic_view join events_table on (basic_view.user_id = events_table.user_id)
)
SELECT user_id, sum(value_2) FROM cte_user GROUP BY 1 ORDER BY 1, 2;

SELECT * FROM cte_view ORDER BY 1, 2 LIMIT 5;


WITH cte_user_with_view AS 
(
	SELECT * FROM cte_view WHERE user_id < 3
)
SELECT user_id, value_1 FROM cte_user_with_view ORDER BY 1, 2 LIMIT 10 OFFSET 2;

DROP VIEW basic_view;
DROP VIEW cte_view;
