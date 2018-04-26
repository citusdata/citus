-- ===================================================================
-- test recursive planning functionality on failure cases
-- ===================================================================
CREATE SCHEMA not_supported;
SET search_path TO not_supported, public;

SET client_min_messages TO DEBUG1;

CREATE TABLE users_table_local AS SELECT * FROM users_table;

-- we don't support subqueries with local tables when they are not leaf queries
SELECT 
	* 
FROM
	(
		SELECT 
			users_table_local.user_id 
		FROM 
			users_table_local, (SELECT user_id FROM events_table) as evs
		WHERE users_table_local.user_id = evs.user_id
	) as foo;

RESET client_min_messages;
-- we don't support subqueries with local tables when they are not leaf queries
SELECT user_id FROM users_table WHERE user_id IN 
	(SELECT 
		user_id 
	 FROM 
	 	users_table_local JOIN (SELECT user_id FROM events_table_local) as foo 
	 USING (user_id)
	 );

SET client_min_messages TO DEBUG1;

-- we don't support aggregate distinct if the group by is not on partition key, expect for count distinct
-- thus baz and bar are recursively planned but not foo
SELECT 
	* 
FROM 
(
	SELECT avg(DISTINCT value_1), random() FROM users_table GROUP BY user_id OFFSET 3
) as baz,
(
	SELECT count(DISTINCT value_1), random() FROM users_table GROUP BY value_2 OFFSET 3
) as bar,
(
	SELECT avg(DISTINCT value_1), random() FROM users_table GROUP BY value_2 OFFSET 3
) as foo;

-- we don't support array_aggs with ORDER BYs
SELECT 
	* 
FROM
	(
		SELECT 
			array_agg(users_table.user_id ORDER BY users_table.time) 
		FROM 
			users_table, (SELECT user_id FROM events_table) as evs
		WHERE users_table.user_id = evs.user_id
		GROUP BY users_table.user_id 
		LIMIT 5
	) as foo;

-- we don't support recursive subqueries when router executor is disabled
SET citus.enable_router_execution TO false;
SELECT
   user_id
FROM
    (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
    ORDER BY 1 DESC;
SET citus.enable_router_execution TO true;


-- window functions are not allowed if they're not partitioned on the distribution column
SELECT 
	* 
FROM 
(
SELECT
   user_id, time, rnk
FROM
(
  SELECT
    *, rank() OVER my_win as rnk
  FROM
    events_table
    WINDOW my_win AS (PARTITION BY event_type ORDER BY time DESC)
) as foo
ORDER BY
  3 DESC, 1 DESC, 2 DESC
LIMIT
  10) as foo;

-- OUTER JOINs where the outer part is recursively planned and not the other way 
-- around is not supported
SELECT
    foo.value_2
FROM
    	(SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4) LIMIT 5) as foo 
    LEFT JOIN
    	(SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
	ON(foo.value_2 = bar.value_2);


-- Aggregates in subquery without partition column can be planned recursively
-- unless there is a reference to an outer query
SELECT
    * 
FROM
    users_table 
WHERE
    user_id IN 
    (
        SELECT
            SUM(events_table.user_id) 
        FROM
            events_table 
        WHERE
            users_table.user_id = events_table.user_id 
    )
;


-- Having qual without group by on partition column can be planned recursively
-- unless there is a reference to an outer query
SELECT
    * 
FROM
    users_table 
WHERE
    user_id IN 
    (
        SELECT
            SUM(events_table.user_id) 
        FROM
            events_table 
        WHERE
            events_table.user_id = users_table.user_id 
        HAVING
            MIN(value_2) > 2 
    )
;

SET client_min_messages TO DEFAULT;

DROP SCHEMA not_supported CASCADE;
SET search_path TO public;
