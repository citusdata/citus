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


-- basic_recursive in WHERE should error out
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

