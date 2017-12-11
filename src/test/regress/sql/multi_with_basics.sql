-- CTE cannot be outside of FROM/WHERE clause
WITH cte AS (
	SELECT user_id FROM users_table WHERE value_2 IN (1, 2)
)
SELECT (SELECT * FROM cte);


WITH cte_basic AS (
	SELECT * FROM users_table
)
SELECT (SELECT user_id FROM cte_basic), user_id FROM users_table;


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
	SELECT user_id FROM users_done_event_3 WHERE user_id < 4 ORDER BY user_id LIMIT 1
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
	SELECT DISTINCT user_id, value_2 from users_table WHERE user_id IN (1, 2) ORDER BY 2 LIMIT 5 OFFSET 5
)
SELECT * FROM cte;

-- create and use CTE in FROM should work
SELECT * FROM (WITH cte AS (
		SELECT user_id, value_2 from users_table WHERE user_id IN (1, 2) ORDER BY 2 LIMIT 5
	)
SELECT * FROM cte) a;

--CTEs in FROM in a subquery should work
SELECT * FROM (WITH cte AS (
		SELECT user_id, value_2 from users_table WHERE user_id IN (1, 2) ORDER BY 2 LIMIT 5
	)
SELECT * FROM cte) b;

--CTEs in FROM in a subquery should work even when we create it there
SELECT * FROM (
	SELECT * FROM (WITH cte AS (
		SELECT user_id, value_2 from users_table WHERE user_id IN (1, 2) ORDER BY 2 LIMIT 5
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
SELECT * FROM (SELECT * FROM cte UNION (SELECT * FROM cte)) a
ORDER BY 
	1,2,3,4,5,6
LIMIT 
	10;

WITH cte AS (
	SELECT user_id, min(value_2) as val_2 FROM users_table WHERE user_id IN (1, 2, 3) GROUP BY user_id ORDER BY 2 DESC, 1 LIMIT 3 OFFSET 2
), 
cte_2 AS (
	SELECT user_id, max(value_2) as val_2 FROM users_table WHERE user_id IN (3, 4, 5) GROUP BY user_id
)
SELECT DISTINCT ON (user_id) user_id, sum(val_2) OVER () FROM (SELECT * FROM cte UNION (SELECT * FROM cte_2)) a
ORDER BY 
	1,2
LIMIT 
	10;

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

SELECT * FROM cte_view;


WITH cte_user_with_view AS 
(
	SELECT * FROM cte_view WHERE user_id < 3
)
SELECT user_id, value_1 FROM cte_user_with_view ORDER BY 1, 2 LIMIT 10 OFFSET 3;

DROP VIEW basic_view;
DROP VIEW cte_view;