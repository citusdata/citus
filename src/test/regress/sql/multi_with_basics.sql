-- CTE cannot be outside of FROM/WHERE clause
WITH cte_basic AS (
	SELECT * FROM users_table
)
SELECT cte_basic, user_id FROM users_table;


WITH cte_1 AS (
	WITH cte_1_1 AS (
		SELECT * FROM users_table
	),
	cte_1_2 AS (
		SELECT cte_1_1
	)
	SELECT user_id FROM cte_1_2
)
SELECT * FROM users_table WHERE user_id IN (SELECT * FROM cte_1);


-- CTE in FROM/WHERE is usable
WITH cte_1 AS (
	WITH cte_1_1 AS (
		SELECT * FROM users_table WHERE value_2 IN (1, 2, 3)
	)
	SELECT user_id, value_2 FROM users_table WHERE user_id IN (
		SELECT user_id FROM cte_1_1
	)
)
SELECT * FROM cte_1 WHERE value_2 NOT IN (SELECT value_2 FROM cte_1);

WITH cte_1 AS (
	WITH cte_1_1 AS (
		SELECT * FROM users_table WHERE value_2 IN (1, 2, 3)
	)
	SELECT user_id, value_2 FROM users_table WHERE user_id IN (
		SELECT user_id FROM cte_1_1
	)
)
SELECT * FROM cte_1 WHERE value_2 IN (
		SELECT 
			value_2 
		FROM 
			cte_1 
		WHERE 
			user_id IN (2, 3, 4) 
		ORDER BY 
			1 
		LIMIT 
			10
	)
ORDER BY 
	1, 2 
LIMIT 
	10;


-- CTE in SELECT level should not be allowed
SELECT user_id = (
	WITH users_done_event_3 AS (
		SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id=events_table.user_id AND event_type=3
	)
	SELECT * FROM users_done_event_3 WHERE user_id < 4
)
FROM users_table 
ORDER BY 1
LIMIT 10;


-- the same CTEs in FROM and WHERE
SELECT * FROM 
(
	WITH cte AS (
		SELECT user_id, value_2 FROM users_table
	)
	SELECT * FROM cte WHERE value_2 IN (SELECT user_id FROM cte)
) ctes
ORDER BY 
	1, 2
LIMIT 10;


-- two different CTEs in FROM and WHERE
SELECT * FROM 
(
	WITH cte_from AS (
		SELECT user_id, value_2 FROM users_table
	),
	cte_where AS (
		SELECT value_2 FROM events_table
	)
	SELECT * FROM cte_from WHERE value_2 IN (SELECT * FROM cte_where)
) binded_ctes
ORDER BY 
	1, 2
LIMIT 10;


SELECT * FROM 
(
	WITH cte_from AS (
		SELECT user_id, value_2 FROM users_table
	),
	cte_where AS (
		SELECT value_2 FROM events_table-- WHERE user_id in (SELECT user_id FROM cte_from)
	)
	SELECT * FROM cte_from WHERE value_2 IN (SELECT * FROM cte_where)
) binded_ctes
WHERE binded_ctes.user_id IN (
	WITH another_cte AS (
		SELECT user_id FROM events_table WHERE value_2 IN (1, 2, 3)
	)
	SELECT * FROM another_cte
)
ORDER BY
	1, 2
LIMIT
	10;


-- CTEs in FROM should work
WITH cte AS (
	SELECT user_id, value_2 from users_table WHERE user_id = 1 ORDER BY 2 LIMIT 5
)
SELECT * FROM cte;

-- create and use CTE in FROM should work
SELECT * FROM (WITH cte AS (
		SELECT user_id, value_2 from users_table WHERE user_id = 1 ORDER BY 2 LIMIT 5
	)
SELECT * FROM cte) a;

--CTEs in FROM in a subquery should work
SELECT * FROM (WITH cte AS (
		SELECT user_id, value_2 from users_table WHERE user_id = 1 ORDER BY 2 LIMIT 5
	)
SELECT * FROM cte) b;

--CTEs in FROM in a subquery should work even when we create it there
SELECT * FROM (
	SELECT * FROM (WITH cte AS (
		SELECT user_id, value_2 from users_table WHERE user_id = 1 ORDER BY 2 LIMIT 5
		)
		SELECT * FROM cte
	) a) b;

-- CTEs in FROM and as a subquery in FROM should work
WITH users_events AS (
	WITH users_done_2_3 AS (
		SELECT users_table.user_id, users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (2, 3)
	)
	SELECT users_done_2_3.user_id, users_done_2_3.value_2 as value_2_3 FROM users_done_2_3
)
SELECT * FROM (SELECT * FROM users_events WHERE value_2_3 IN (2, 5)) a ORDER BY 1, 2 LIMIT 10;
