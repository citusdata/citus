CREATE TABLE t1 (id int, val int);
SELECT create_distributed_table('t1', 'id');


-- basic insert query
WITH basic_insert AS (
	INSERT INTO users_table VALUES (1), (2), (3) RETURNING *
)
SELECT
	*
FROM
	basic_insert;


WITH basic_update AS (
	UPDATE users_table SET value_3=42 WHERE user_id=0 RETURNING *
)
SELECT
	*
FROM
	basic_update;


WITH basic_update AS (
	UPDATE users_table SET value_3=42 WHERE value_2=1 RETURNING *
)
SELECT
	*
FROM
	basic_update;


WITH basic_delete AS (
	DELETE FROM users_table WHERE user_id=42 RETURNING *
)
SELECT
	*
FROM
	basic_delete;


WITH basic_delete AS (
	DELETE FROM users_table WHERE value_2=42 RETURNING *
)
SELECT
	*
FROM
	basic_delete;

-- CTEs prior to INSERT...SELECT via the coordinator should work
WITH cte AS (
	SELECT user_id FROM users_table WHERE value_2 IN (1, 2)
)
INSERT INTO t1 (SELECT * FROM cte);


WITH cte_1 AS (
	SELECT user_id, value_2 FROM users_table WHERE value_2 IN (1, 2, 3, 4)
),
cte_2 AS (
	SELECT user_id, value_2 FROM users_table WHERE value_2 IN (3, 4, 5, 6)
)
INSERT INTO t1 (SELECT cte_1.user_id FROM cte_1 join cte_2 on cte_1.value_2=cte_2.value_2);


-- even if this is an INSERT...SELECT, the CTE is under SELECT
WITH cte AS (
	SELECT user_id FROM users_table WHERE value_2 IN (1, 2)
)
INSERT INTO t1 (SELECT (SELECT * FROM cte));


-- CTEs prior to any other modification should error out
WITH cte AS (
	SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
)
DELETE FROM t1 WHERE id IN (SELECT value_2 FROM cte);


WITH cte AS (
	SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
)
UPDATE t1 SET val=-1 WHERE val IN (SELECT * FROM cte);


WITH cte AS (
	WITH basic AS (
		SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
	)
	INSERT INTO t1 (SELECT * FROM basic) RETURNING *
)
UPDATE t1 SET val=-2 WHERE id IN (SELECT id FROM cte);


WITH cte AS (
	WITH basic AS (
		SELECT * FROM events_table
	),
	basic_2 AS (
		SELECT user_id FROM users_table
	)
	INSERT INTO t1 (SELECT user_id FROM events_table) RETURNING *
)
SELECT * FROM cte;
