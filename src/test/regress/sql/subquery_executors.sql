-- ===================================================================
-- test recursive planning functionality with different executors
-- ===================================================================
CREATE SCHEMA subquery_executor;
SET search_path TO subquery_executor, public;


CREATE TABLE users_table_local AS SELECT * FROM users_table;

SET client_min_messages TO DEBUG1;

-- subquery with router planner
SELECT 
	count(*) 
FROM
(
	SELECT value_2 FROM users_table WHERE user_id = 15 OFFSET 0
) as foo, 
(
	SELECT user_id FROM users_table
) as bar
WHERE foo.value_2 = bar.user_id; 

-- subquery with router but not logical plannable
-- should fail
SELECT
	count(*) 
FROM
(
	SELECT  user_id, sum(value_2) over (partition by user_id) AS counter FROM users_table WHERE user_id = 15
) as foo, 
(
	SELECT user_id FROM users_table
) as bar
WHERE foo.counter = bar.user_id; 

-- subquery with real-time query
SELECT 
	count(*) 
FROM
(
	SELECT value_2 FROM users_table WHERE user_id != 15 OFFSET 0
) as foo, 
(
	SELECT user_id FROM users_table
) as bar
WHERE foo.value_2 = bar.user_id; 


-- subquery with repartition query
SET citus.enable_repartition_joins to ON;

SELECT 
	count(*) 
FROM
(
	SELECT DISTINCT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND users_table.user_id < 2
) as foo, 
(
	SELECT user_id FROM users_table
) as bar
WHERE foo.value_2 = bar.user_id; 

-- mixed of all executors (including local execution)
SELECT 
	count(*) 
FROM
(
	SELECT value_2 FROM users_table WHERE user_id = 15 OFFSET 0
) as foo, 
(
	SELECT user_id FROM users_table OFFSET 0
) as bar,
(
	SELECT DISTINCT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND users_table.user_id < 2
) baz,
(
	SELECT user_id FROM users_table_local WHERE user_id = 2
) baw
WHERE foo.value_2 = bar.user_id AND baz.value_2 = bar.user_id AND bar.user_id = baw.user_id; 


SET citus.enable_repartition_joins to OFF;


-- final query is router 
SELECT 
	count(*) 
FROM
(
	SELECT value_2 FROM users_table WHERE user_id = 1 OFFSET 0
) as foo, 
(
	SELECT user_id FROM users_table  WHERE user_id = 2 OFFSET 0
) as bar
WHERE foo.value_2 = bar.user_id; 

-- final query is real-time
SELECT 
	count(*) 
FROM
(
	SELECT value_2 FROM users_table WHERE user_id = 1 OFFSET 0
) as foo, 
(
	SELECT user_id FROM users_table  WHERE user_id != 2
) as bar
WHERE foo.value_2 = bar.user_id; 

SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_executor CASCADE;
SET search_path TO public;
