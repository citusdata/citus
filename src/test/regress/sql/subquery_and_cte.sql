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
-- out since we don't support subqueries in WHERE clause
-- when there is only intermediate results on the range table
-- note that this time subquery in WHERE clause is not replaced
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


SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_and_ctes CASCADE;
SET search_path TO public;
