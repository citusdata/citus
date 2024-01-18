SET citus.enable_repartition_joins to ON;

-- The intermediate result limits chosen below are based on text sizes of the
-- intermediate results. This is a no-op for PG_VERSION_NUM < 14, because the
-- default is false there.
SET citus.enable_binary_protocol = FALSE;

SET citus.max_intermediate_result_size TO 2;
-- should fail because the copy size is ~4kB for each cte
WITH cte AS MATERIALIZED
(
	SELECT * FROM users_table
),
cte2 AS MATERIALIZED (
	SELECT * FROM events_table
)
SELECT cte.user_id, cte.value_2 FROM cte,cte2 ORDER BY 1,2 LIMIT 10;


SET citus.max_intermediate_result_size TO 9;
-- regular adaptive executor CTE should fail
WITH cte AS MATERIALIZED
(
	SELECT
		users_table.user_id, users_table.value_1, users_table.value_2
	FROM
		users_table
		join
		events_table
	on
		(users_table.value_3=events_table.value_3)
),
cte2 AS  MATERIALIZED(
	SELECT * FROM events_table
)
SELECT
	cte.user_id, cte2.value_2
FROM
	cte JOIN cte2 ON (cte.value_1 = cte2.event_type)
ORDER BY
	1,2
LIMIT 10;


-- router queries should be able to get limitted too
SET citus.max_intermediate_result_size TO 2;
-- this should pass, since we fetch small portions in each subplan
with cte as MATERIALIZED (select * from users_table where user_id=1),
cte2 as (select * from users_table where user_id=2),
cte3 as (select * from users_table where user_id=3),
cte4 as (select * from users_table where user_id=4),
cte5 as (select * from users_table where user_id=5)
SELECT sum(c) FROM (
(select count(*) as c from cte)
UNION
(select count(*) as c from cte2)
UNION
(select count(*) as c from cte3)
UNION
(select count(*) as c from cte4)
UNION
(select count(*) as c from cte5)
) as foo;


-- if we fetch the same amount of data at once, it should fail
WITH cte AS MATERIALIZED (SELECT * FROM users_table WHERE user_id IN (1,2,3,4,5))
SELECT * FROM cte ORDER BY 1,2,3,4,5 LIMIT 10;


SET citus.max_intermediate_result_size TO 0;
-- this should fail
WITH cte AS MATERIALIZED (SELECT * FROM users_table WHERE user_id=1),
cte2 AS MATERIALIZED (SELECT * FROM users_table WHERE user_id=2),
cte3 AS MATERIALIZED (SELECT * FROM users_table WHERE user_id=3),
cte4 AS MATERIALIZED (SELECT * FROM users_table WHERE user_id=4),
cte5 AS MATERIALIZED (SELECT * FROM users_table WHERE user_id=5)
SELECT * FROM (
(SELECT * FROM cte)
UNION
(SELECT * FROM cte2)
UNION
(SELECT * FROM cte3)
UNION
(SELECT * FROM cte4)
UNION
(SELECT * FROM cte5)
)a ORDER BY 1,2,3,4,5 LIMIT 10;


-- this fails since cte-subplan exceeds limit even if cte2 and cte3 don't
-- WHERE EXISTS forces materialization in pg12
SET citus.max_intermediate_result_size TO 4;
WITH cte AS MATERIALIZED (
	WITH cte2 AS MATERIALIZED (
		SELECT * FROM users_table
	),
	cte3 AS MATERIALIZED(
		SELECT * FROM events_table
	)
	SELECT * FROM cte2, cte3 WHERE cte2.user_id = cte3.user_id AND cte2.user_id = 1
	AND EXISTS (select * from cte2, cte3)
)
SELECT count(*) FROM cte WHERE EXISTS (select * from cte);


SET citus.max_intermediate_result_size TO 3;
-- this should fail since the cte-subplan exceeds the limit even if the
-- cte2 and cte3 does not
WITH cte AS MATERIALIZED (
	WITH cte2 AS MATERIALIZED (
		SELECT * FROM users_table WHERE user_id IN (3,4,5,6)
	),
	cte3 AS MATERIALIZED(
		SELECT * FROM events_table WHERE event_type = 1
	)
	SELECT * FROM cte2, cte3 WHERE cte2.value_1 IN (SELECT value_2 FROM cte3)
)
SELECT count(*) FROM cte;


-- this will fail in remote execution
SET citus.max_intermediate_result_size TO 2;
WITH cte AS MATERIALIZED (
	WITH cte2 AS MATERIALIZED (
		SELECT * FROM users_table WHERE user_id IN (1, 2)
	),
	cte3 AS MATERIALIZED (
		SELECT * FROM users_table WHERE user_id = 3
	)
	SELECT * FROM cte2 UNION (SELECT * FROM cte3)
),
cte4 AS MATERIALIZED (
	SELECT * FROM events_table
)
SELECT * FROM cte UNION ALL
SELECT * FROM cte4 ORDER BY 1,2,3,4,5 LIMIT 5;


SET citus.max_intermediate_result_size TO 1;
-- this will fail in router_executor
WITH cte AS MATERIALIZED (
	WITH cte2 AS MATERIALIZED (
		SELECT * FROM users_table WHERE user_id IN (1, 2)
	),
	cte3 AS MATERIALIZED (
		SELECT * FROM users_table WHERE user_id = 3
	)
	SELECT * FROM cte2 UNION (SELECT * FROM cte3)
),
cte4 AS MATERIALIZED (
	SELECT * FROM events_table
)
SELECT * FROM cte UNION ALL
SELECT * FROM cte4 ORDER BY 1,2,3,4,5 LIMIT 5;


-- Below that, all should pAS MATERIALIZEDs since -1 disables the limit
SET citus.max_intermediate_result_size TO -1;

-- real_time_executor + router_executor + real_time_executor will pass
WITH cte AS MATERIALIZED (
	WITH cte2 AS MATERIALIZED (
		SELECT * FROM users_table WHERE user_id IN (1, 2)
	),
	cte3 AS MATERIALIZED (
		SELECT * FROM users_table WHERE user_id = 3
	)
	SELECT * FROM cte2 UNION (SELECT * FROM cte3)
),
cte4 AS MATERIALIZED (
	SELECT * FROM events_table
)
SELECT * FROM cte UNION ALL
SELECT * FROM cte4 ORDER BY 1,2,3,4,5 LIMIT 5;

-- regular adaptive executor CTE, should work since -1 disables the limit
WITH cte AS MATERIALIZED
(
	SELECT
		users_table.user_id, users_table.value_1, users_table.value_2
	FROM
		users_table
		join
		events_table
	on
		(users_table.value_2=events_table.value_2)
),
cte2 AS MATERIALIZED (
	SELECT * FROM events_table
)
SELECT
	cte.user_id, cte2.value_2
FROM
	cte JOIN cte2 ON (cte.value_1 = cte2.event_type)
ORDER BY
	1,2
LIMIT 10;


-- regular real-time CTE fetches around ~4kb data in each subplan
WITH cte AS MATERIALIZED
(
	SELECT * FROM users_table
),
cte2 AS MATERIALIZED (
	SELECT * FROM events_table
)
SELECT cte.user_id, cte.value_2 FROM cte,cte2 ORDER BY 1,2 LIMIT 10;


-- regular real-time query fetches ~4kB
WITH cte AS MATERIALIZED
(
	SELECT * FROM users_table WHERE user_id IN (1,2,3,4,5)
)
SELECT * FROM cte ORDER BY 1,2,3,4,5 LIMIT 10;


-- nested CTEs
WITH cte AS MATERIALIZED (
	WITH cte2 AS MATERIALIZED (
		SELECT * FROM users_table
	),
	cte3 AS MATERIALIZED (
		SELECT * FROM events_table
	)
	SELECT
		cte2.user_id, cte2.time, cte3.event_type, cte3.value_2, cte3.value_3
	FROM
		cte2, cte3
	WHERE
		cte2.user_id = cte3.user_id AND cte2.user_id = 1
)
SELECT * FROM cte ORDER BY 1,2,3,4,5 LIMIT 10;
