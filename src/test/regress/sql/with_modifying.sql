-- Tests for modifying CTEs and CTEs in modifications
SET citus.next_shard_id TO 1502000;

CREATE SCHEMA with_modifying;
SET search_path TO with_modifying, public;

CREATE TABLE with_modifying.modify_table (id int, val int);
SELECT create_distributed_table('modify_table', 'id');

CREATE TABLE with_modifying.users_table (LIKE public.users_table INCLUDING ALL);
SELECT create_distributed_table('with_modifying.users_table', 'user_id');
INSERT INTO with_modifying.users_table SELECT * FROM public.users_table;

-- basic insert query in CTE
WITH basic_insert AS (
	INSERT INTO users_table VALUES (1), (2), (3) RETURNING *
)
SELECT
	*
FROM
	basic_insert;

-- single-shard UPDATE in CTE
WITH basic_update AS (
	UPDATE users_table SET value_3=42 WHERE user_id=0 RETURNING *
)
SELECT
	*
FROM
	basic_update;

-- multi-shard UPDATE in CTE
WITH basic_update AS (
	UPDATE users_table SET value_3=42 WHERE value_2=1 RETURNING *
)
SELECT
	*
FROM
	basic_update;

-- single-shard DELETE in CTE
WITH basic_delete AS (
	DELETE FROM users_table WHERE user_id=42 RETURNING *
)
SELECT
	*
FROM
	basic_delete;

-- multi-shard DELETE in CTE
WITH basic_delete AS (
	DELETE FROM users_table WHERE value_2=42 RETURNING *
)
SELECT
	*
FROM
	basic_delete;

-- INSERT...SELECT query in CTE
WITH copy_table AS (
	INSERT INTO users_table SELECT * FROM users_table RETURNING *
)
SELECT
	*
FROM
	copy_table;

-- CTEs prior to INSERT...SELECT via the coordinator should work
WITH cte AS (
	SELECT user_id FROM users_table WHERE value_2 IN (1, 2)
)
INSERT INTO modify_table (SELECT * FROM cte);


WITH cte_1 AS (
	SELECT user_id, value_2 FROM users_table WHERE value_2 IN (1, 2, 3, 4)
),
cte_2 AS (
	SELECT user_id, value_2 FROM users_table WHERE value_2 IN (3, 4, 5, 6)
)
INSERT INTO modify_table (SELECT cte_1.user_id FROM cte_1 join cte_2 on cte_1.value_2=cte_2.value_2);


-- even if this is an INSERT...SELECT, the CTE is under SELECT
WITH cte AS (
	SELECT user_id, value_2 FROM users_table WHERE value_2 IN (1, 2)
)
INSERT INTO modify_table (SELECT (SELECT value_2 FROM cte GROUP BY value_2));


-- CTEs prior to any other modification should error out
WITH cte AS (
	SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
)
DELETE FROM modify_table WHERE id IN (SELECT value_2 FROM cte);


WITH cte AS (
	SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
)
UPDATE modify_table SET val=-1 WHERE val IN (SELECT * FROM cte);


WITH cte AS (
	WITH basic AS (
		SELECT value_2 FROM users_table WHERE user_id IN (1, 2, 3)
	)
	INSERT INTO modify_table (SELECT * FROM basic) RETURNING *
)
UPDATE modify_table SET val=-2 WHERE id IN (SELECT id FROM cte);


WITH cte AS (
	WITH basic AS (
		SELECT * FROM events_table WHERE event_type = 5
	),
	basic_2 AS (
		SELECT user_id FROM users_table
	)
	INSERT INTO modify_table (SELECT user_id FROM events_table) RETURNING *
)
SELECT * FROM cte;

DROP SCHEMA with_modifying CASCADE;
