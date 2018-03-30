-- ===================================================================
-- test recursive planning functionality with subqueries in WHERE
-- ===================================================================
CREATE SCHEMA subquery_in_where;
SET search_path TO subquery_in_where, public;

SET client_min_messages TO DEBUG1;

--CTEs can be used as a recurring tuple with subqueries in WHERE 
WITH event_id
     AS (SELECT user_id AS events_user_id, 
                time    AS events_time, 
                event_type
         FROM   events_table) 
SELECT Count(*) 
FROM   event_id
WHERE  events_user_id IN (SELECT user_id
                          FROM   users_table);

--Correlated subqueries can not be used in WHERE clause 
WITH event_id 
     AS (SELECT user_id AS events_user_id, 
                time    AS events_time, 
                event_type 
         FROM   events_table) 
SELECT Count(*) 
FROM   event_id 
WHERE  events_user_id IN (SELECT user_id 
                          FROM   users_table 
                          WHERE  users_table.time = events_time); 

-- Recurring tuples as empty join tree 
SELECT * 
FROM   (SELECT 1 AS id, 
               2 AS value_1, 
               3 AS value_3) AS tt1 
WHERE  id IN (SELECT user_id 
              FROM   events_table); 

-- Recurring tuples in from clause as CTE and SET operation in WHERE clause
SELECT Count(*)
FROM   (WITH event_id AS
       (SELECT user_id AS events_user_id, time AS events_time, event_type 
        FROM events_table)
       SELECT events_user_id, events_time, event_type
	   FROM event_id 
	   ORDER BY 1,2,3
	   LIMIT 10) AS sub_table 
WHERE  events_user_id IN (
       (SELECT user_id
        FROM users_table
        ORDER BY 1
        LIMIT 10)
		UNION ALL
       (SELECT value_1
        FROM users_table
        ORDER BY 1
        limit 10));

-- Recurring tuples in from clause as SET operation on recursively plannable
-- queries and CTE in WHERE clause
SELECT
	*
FROM
	(
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id ASC
		LIMIT
			10
		)
		UNION ALL
		(SELECT
			value_1
		FROM
			users_table
		ORDER BY
			value_1 ASC
		LIMIT
			10
		)
	) as SUB_TABLE
WHERE
	user_id
IN
	(
	WITH event_id AS (
		SELECT
			user_id as events_user_id, time as events_time, event_type
		FROM
			events_table
	)
	SELECT
		events_user_id
	FROM
		event_id
	ORDER BY
		events_user_id
	LIMIT
		10
	);

-- Complex target list in WHERE clause
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	ORDER BY
		1,2
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
<=ANY (
	SELECT
		max(abs(user_id * 1) + mod(user_id, 3)) as val_1
	FROM
		users_table
	GROUP BY
		user_id
);

-- DISTINCT clause in WHERE
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
IN (
	SELECT
		distinct user_id
	FROM
		users_table
	GROUP BY
		user_id
);

-- AND in WHERE clause
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	ORDER BY 
		1,2,3
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
>=ANY (
	SELECT
		min(user_id)
	FROM
		users_table
	GROUP BY
		user_id
)
AND
	events_user_id
<=ANY (
	SELECT
		max(user_id)
	FROM
		users_table
	GROUP BY
		user_id
);

-- AND in WHERE clause, part of the AND is pushdownable other is not
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	ORDER BY
		1,2,3
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
>=ANY (
	SELECT
		min(user_id)
	FROM
		users_table
	GROUP BY
		user_id
)
AND
	events_user_id
<=ANY (
	SELECT
		max(value_2)
	FROM
		users_table
	GROUP BY
		user_id
);

-- Planning subqueries in WHERE clause in CTE recursively
WITH cte AS (
	SELECT
		*
	FROM
		(SELECT
			*
		FROM
			users_table
		ORDER BY
			user_id ASC,
			value_2 DESC
		LIMIT
			10
		) as sub_table
	WHERE
		user_id
	IN
		(SELECT
			value_2
		FROM
			events_table
		)
)
SELECT
	COUNT(*)
FROM
	cte;

-- Planing subquery in WHERE clause in FROM clause of a subquery recursively
SELECT
	COUNT(*)
FROM
	(SELECT
		*
	FROM
		(SELECT
			*
		FROM
			users_table
		ORDER BY
			user_id ASC,
			value_2 DESC
		LIMIT
			10
		) as sub_table_1
	WHERE
		user_id
	IN
		(SELECT
			value_2
		FROM
			events_table
		)
	) as sub_table_2;

-- Recurring table in the FROM clause of a subquery in the FROM clause
-- Recurring table is created by joining a two recurrign table
SELECT
	SUM(user_id)
FROM
	(SELECT
		*
	FROM
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT 10) as t1
		INNER JOIN
		(SELECT
			user_id as user_id_2
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT
			10) as t2
	ON
		t1.user_id = t2.user_id_2
	WHERE
		t1.user_id
	IN
		(SELECT
			value_2
		FROM
			events_table)
	) as t3
WHERE
	user_id
>ANY
	(SELECT
		min(user_id)
	FROM
		events_table
	GROUP BY
		user_id);

-- Same example with the above query, but now check the rows with EXISTS
SELECT
	SUM(user_id)
FROM
	(SELECT
		*
	FROM
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT 10) as t1
		INNER JOIN
		(SELECT
			user_id as user_id_2
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT
			10) as t2
	ON
		t1.user_id = t2.user_id_2
	WHERE
		t1.user_id
	IN
		(SELECT
			value_2
		FROM
			events_table)
	) as t3
WHERE EXISTS
	(SELECT
		1,2
	FROM
		events_table
	WHERE
		events_table.value_2 = events_table.user_id);

-- Same query with the above one, yet now we check the row's NON-existence
-- by NOT EXISTS. Note that, max value_2 of events_table is 5
SELECT
	SUM(user_id)
FROM
	(SELECT
		*
	FROM
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT 10) as t1
		INNER JOIN
		(SELECT
			user_id as user_id_2
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT
			10) as t2
	ON
		t1.user_id = t2.user_id_2
	WHERE
		t1.user_id
	IN
		(SELECT
			value_2
		FROM
			events_table)
	) as t3
WHERE NOT EXISTS
	(SELECT
		1,2
	FROM
		events_table
	WHERE
		events_table.value_2 = events_table.user_id + 6);

-- Check the existence of row by comparing it with the result of subquery in
-- WHERE clause. Note that subquery is planned recursively since there is no
-- distributed table in the from
SELECT
	*
FROM
	(SELECT
		user_id, value_1
	FROM
		users_table
	ORDER BY
		user_id ASC,
		value_1 ASC
	LIMIT 10) as t3
WHERE row(user_id, value_1) =
	(SELECT
		min(user_id) + 1, min(user_id) + 1
	FROM
		events_table);

-- Recursively plan subquery in WHERE clause when the FROM clause has a subquery
-- generated by generate_series function
SELECT
	*
FROM
	(SELECT
		*
	FROM
		generate_series(1,10)
	) as gst
WHERE
	generate_series
IN
	(SELECT
		value_2
	FROM
		events_table
	)
ORDER BY
	generate_series ASC;

-- Similar to the test above, now we also have a generate_series in WHERE clause
SELECT
	*
FROM
	(SELECT
		*
	FROM
		generate_series(1,10)
	) as gst
WHERE
	generate_series
IN
	(SELECT
		user_id
	FROM
		users_table
	WHERE
		user_id
	IN
		(SELECT
			*
		FROM
			generate_series(1,3)
		)
	)
ORDER BY
	generate_series ASC;
	
-- Local tables also planned recursively, so using it as part of the FROM clause
-- make the clause recurring
CREATE TABLE local_table(id int, value_1 int);
INSERT INTO local_table VALUES(1,1), (2,2);

SELECT
	*
FROM
	(SELECT
		*
	FROM
		local_table) as sub_table
WHERE
	id
IN
	(SELECT
		user_id
	FROM
		users_table);
		
-- Use local table in WHERE clause
SELECT
	COUNT(*)
FROM
	(SELECT
		*
	FROM
		users_table
	ORDER BY
		user_id
	LIMIT 
		10) as sub_table
WHERE
	user_id
IN
	(SELECT
		id
	FROM
		local_table);

SET client_min_messages TO DEFAULT;

DROP TABLE local_table;
DROP SCHEMA subquery_in_where CASCADE;
SET search_path TO public;
