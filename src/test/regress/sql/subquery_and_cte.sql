-- ===================================================================
-- test recursive planning functionality with subqueries and CTEs
-- ===================================================================
CREATE SCHEMA subquery_and_ctes;
SET search_path TO subquery_and_ctes, public;


CREATE TABLE users_table_local AS SELECT * FROM users_table;

SET client_min_messages TO DEBUG1;

-- CTEs are recursively planned, and subquery foo is also recursively planned
-- final plan becomes a router plan
WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT 
	count(*) 
FROM 
	cte,
	  (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo 
	  WHERE foo.user_id = cte.user_id;

-- CTEs are recursively planned, and subquery foo is also recursively planned
-- final plan becomes a real-time plan since we also have events_table in the 
-- range table entries 
WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT 
	count(*) 
FROM 
	cte,
	  (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo, events_table
	  WHERE foo.user_id = cte.user_id AND events_table.user_id = cte.user_id;

-- CTEs are replaced and subquery in WHERE is also replaced
-- but the query is still real-time query since users_table is in the 
-- range table list
WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT DISTINCT cte.user_id
FROM users_table, cte
WHERE 
	users_table.user_id = cte.user_id AND
	users_table.user_id IN (SELECT DISTINCT value_2 FROM users_table WHERE value_1 >= 1 AND value_1 <= 20 ORDER BY 1 LIMIT 5)
    ORDER BY 1 DESC;

-- a very similar query as the above, but this time errors 
-- out because the FROM clause is recurring, the WHERE clause is automatically replaced
WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT DISTINCT cte.user_id
FROM cte
WHERE 
	cte.user_id IN (SELECT DISTINCT user_id FROM users_table WHERE value_1 >= 1 AND value_1 <= 20)
    ORDER BY 1 DESC;

-- CTEs inside a subquery and the final query becomes a router
-- query
SELECT
   user_id
FROM
    (
	     WITH cte AS (
	    SELECT 
	    	DISTINCT users_table.user_id 
	     FROM 
	     	users_table, events_table 
	     WHERE 
	     	users_table.user_id = events_table.user_id AND 
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo;


-- CTEs inside a subquery and the final query becomes a
-- real-time query since the other subquery is safe to pushdown
SELECT
   bar.user_id
FROM
    (
	     WITH cte AS (
	    SELECT 
	    	DISTINCT users_table.user_id 
	     FROM 
	     	users_table, events_table 
	     WHERE 
	     	users_table.user_id = events_table.user_id AND 
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo, 
    (
	    SELECT 
	    	DISTINCT users_table.user_id 
	     FROM 
	     	users_table, events_table 
	     WHERE 
	     	users_table.user_id = events_table.user_id AND 
	     event_type IN (1,2,3,4)
	    
     ) as bar  
WHERE foo.user_id = bar.user_id;

-- CTEs inside a deeper subquery
-- and also the subquery that contains the CTE is replaced
SELECT
   DISTINCT bar.user_id
FROM
    (
	     WITH cte AS (
	    SELECT 
	    	DISTINCT users_table.user_id 
	     FROM 
	     	users_table, events_table 
	     WHERE 
	     	users_table.user_id = events_table.user_id AND 
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo, 
    (
	    SELECT 
	    	users_table.user_id, some_events.event_type
	     FROM 
	     	users_table, 
	     	(
	     		WITH cte AS (
			    SELECT 
			    	event_type, users_table.user_id
			     FROM 
			     	users_table, events_table 
			     WHERE 
			     	users_table.user_id = events_table.user_id AND 
			     value_1 IN (1,2)
			     ) SELECT * FROM cte ORDER BY 1 DESC
	     	) as some_events
	     WHERE 
	     	users_table.user_id = some_events.user_id AND 
	     event_type IN (1,2,3,4)
	     ORDER BY 2,1
	     LIMIT 2
	    
     ) as bar  
WHERE foo.user_id = bar.user_id
ORDER BY 1 DESC LIMIT 5;



-- CTEs on the different parts of the query is replaced 
-- and subquery foo is also replaced since it contains
-- DISTINCT on a non-partition key 
SELECT * FROM 
(
	WITH cte AS (
		WITH local_cte AS (
			SELECT * FROM users_table_local
		),
		dist_cte AS (
			SELECT user_id FROM events_table
		)
		SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
	)
	SELECT DISTINCT cte.user_id
	FROM users_table, cte
	WHERE 
		users_table.user_id = cte.user_id AND
		users_table.user_id IN 
			(WITH cte_in_where AS (SELECT DISTINCT value_2 FROM users_table WHERE value_1 >= 1 AND value_1 <= 20 ORDER BY 1 LIMIT 5) SELECT * FROM cte_in_where)
	    ORDER BY 1 DESC
	    ) as foo, 
			events_table 
		WHERE 
			foo.user_id = events_table.value_2
ORDER BY 3 DESC, 2 DESC, 1 DESC 
LIMIT 5;


-- now recursively plan subqueries inside the CTEs that contains LIMIT and OFFSET
WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT 
			user_id
		FROM 
			events_table, 
			(SELECT DISTINCT value_2 FROM users_table OFFSET 0) as foo
		WHERE 
			events_table.user_id = foo.value_2 AND
			events_table.user_id IN (SELECT DISTINCT value_1 FROM users_table ORDER BY 1 LIMIT 3)
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT 
	count(*) 
FROM 
	cte,
	  (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo 
	  WHERE foo.user_id = cte.user_id;

-- the same query, but this time the CTEs also live inside a subquery
SELECT 
	* 
FROM 
(

	WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT 
			user_id
		FROM 
			events_table, 
			(SELECT DISTINCT value_2 FROM users_table OFFSET 0) as foo
		WHERE 
			events_table.user_id = foo.value_2 AND
			events_table.user_id IN (SELECT DISTINCT value_1 FROM users_table ORDER BY 1 LIMIT 3)
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT 
	count(*)  as cnt
FROM 
	cte,
	  (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo 
	  WHERE foo.user_id = cte.user_id

) as foo, users_table WHERE foo.cnt > users_table.value_2 
ORDER BY 3 DESC, 1 DESC, 2 DESC, 4 DESC
LIMIT 5;

-- recursive CTES are not supported inside subqueries as well
SELECT
   bar.user_id
FROM
    (
	     WITH RECURSIVE cte AS (
	    SELECT 
	    	DISTINCT users_table.user_id 
	     FROM 
	     	users_table, events_table 
	     WHERE 
	     	users_table.user_id = events_table.user_id AND 
	     event_type IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC
     ) as foo, 
    (
	    SELECT 
	    	DISTINCT users_table.user_id 
	     FROM 
	     	users_table, events_table 
	     WHERE 
	     	users_table.user_id = events_table.user_id AND 
	     event_type IN (1,2,3,4)
	    
     ) as bar  
WHERE foo.user_id = bar.user_id;

--CTEs can be used as a recurring tuple with subqueries in WHERE
WITH event_id AS (
	SELECT user_id as events_user_id, time as events_time, event_type
	FROM events_table
)
SELECT
	count(*)
FROM
	event_id
WHERE
	events_user_id IN (SELECT user_id FROM users_table);

--Correlated subqueries can not be used in WHERE clause
WITH event_id AS (
	SELECT user_id as events_user_id, time as events_time, event_type
	FROM events_table
)
SELECT
	count(*)
FROM
	event_id
WHERE
	events_user_id IN (SELECT user_id FROM users_table where users_table.time = events_time);

-- Recurring tuples as empty join tree
SELECT
	*
FROM (
	SELECT
		1 AS id,
		2 AS value_1,
		3 AS value_3
	) AS tt1
WHERE
	id
IN (
	SELECT
		user_id
	FROM
	events_table
   );

-- Recurring tuples in from clause as CTE and SET operation in WHERE clause
SELECT
	COUNT(*)
FROM
	(
		WITH event_id AS (
			SELECT
				user_id as events_user_id, time as events_time, event_type
			FROM
				events_table
		)
		SELECT
			events_user_id, events_time, event_type
		FROM
			event_id
		LIMIT
			10
	) as SUB_TABLE
WHERE
	events_user_id
IN
	(
		(SELECT
			user_id
		FROM
			users_table
		LIMIT
			10
		)
		UNION ALL
		(SELECT
			value_1
		FROM
			users_table
		LIMIT
			10
		)
	);

-- Recurring tuples in fram clause as SET operation on recursively plannable
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
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
<= (
	SELECT
		max(abs(value_1 * 1) + mod(value_1, 3)) as val_1
	FROM
		users_table
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
		distinct value_1
	FROM
		users_table
);

-- AND in WHERE clause
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
> (
	SELECT
		min(value_1)
	FROM
		users_table
)
AND
	events_user_id
< (
	SELECT
		max(value_2)
	FROM
		users_table
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
>
	(SELECT
		min(value_3)
	FROM
		events_table);

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
		events_table.value_2 = user_id);

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
		events_table.value_2 = user_id + 6);

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
		min(value_2) + 1, min(value_2) + 1
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

SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_and_ctes CASCADE;
SET search_path TO public;
