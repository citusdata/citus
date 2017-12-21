-- ===================================================================
-- test recursive planning functionality on local tables
-- ===================================================================

CREATE SCHEMA subquery_local_tables;
SET search_path TO subquery_local_tables, public;


CREATE TABLE users_table_local AS SELECT * FROM users_table;
CREATE TABLE events_table_local AS SELECT * FROM events_table;

SET client_min_messages TO DEBUG1;

-- foo is only on the local tables, thus can be replaced
-- bar is on the distributed tables with LIMIT, should be replaced
SELECT
   foo.user_id
FROM
    (SELECT 
    	DISTINCT users_table_local.user_id 
     FROM 
     	users_table_local, events_table_local 
     WHERE 
     	users_table_local.user_id = events_table_local.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo,
    (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (5,6,7,8)
     ORDER BY 1 DESC LIMIT 5
     ) as bar

    WHERE bar.user_id = foo.user_id
    ORDER BY 1 DESC;

-- foo is only on the local tables, thus can be replaced
SELECT
   foo.user_id
FROM
    (SELECT 
    	DISTINCT users_table_local.user_id 
     FROM 
     	users_table_local, events_table_local 
     WHERE 
     	users_table_local.user_id = events_table_local.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo,
    (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (5,6,7,8)
     ) as bar
    WHERE bar.user_id = foo.user_id
    ORDER BY 1 DESC;


-- subqueries in WHERE could be replaced even if they are on the local tables
SELECT DISTINCT user_id
FROM users_table
WHERE 
	user_id IN (SELECT DISTINCT value_2 FROM users_table_local WHERE value_1 = 1) 
ORDER BY 1 LIMIT 5;


-- subquery in FROM -> FROM -> FROM should be replaced if 
-- it contains onle local tables
SELECT 
	DISTINCT user_id 
FROM 
	(
		SELECT users_table.user_id FROM users_table, 
							(
								SELECT 
									event_type, user_id
								FROM
									(SELECT event_type, users_table.user_id FROM users_table, 
															(SELECT user_id, event_type FROM events_table_local WHERE value_2 < 3 OFFSET 3) as foo
															WHERE foo.user_id = users_table.user_id
															) bar

							) as baz
									WHERE baz.user_id = users_table.user_id

	) as sub1
	ORDER BY 1 DESC 
	LIMIT 3;


-- subquery in FROM -> FROM -> WHERE -> WHERE should be replaced if
-- it contains onle local tables
-- Later the upper level query is also recursively planned due to LIMIT
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT 
    	u.user_id, e.event_type::text AS event, e.time
    FROM 
    	users_table AS u,
        events_table AS e
    WHERE u.user_id = e.user_id AND 
    		u.user_id IN 
    		(
    			SELECT 
    				user_id 
    			FROM 
    				users_table 
    			WHERE value_2 >= 5
			    AND  EXISTS (SELECT user_id FROM events_table_local WHERE event_type > 1 AND event_type <= 3 AND value_3 > 1)
				AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 3 AND event_type <= 4  AND value_3 > 1 AND user_id = users_table.user_id)
				LIMIT 5
    		)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1;



-- subquery (i.e., subquery_2) in WHERE->FROM should be replaced due to local tables
SELECT 
	user_id 
FROM 
	users_table 
WHERE
 user_id IN
(
	SELECT
	  user_id
	 FROM (
	  SELECT
	 	 subquery_1.user_id, count_pay
	  FROM
	  (
	    (SELECT
	      users_table.user_id,
	      'action=>1' AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 1 AND
	      users_table.user_id <= 3 AND
	      events_table.event_type > 1 AND events_table.event_type < 3
	      )
	    UNION
	    (SELECT
	      users_table.user_id,
	      'action=>2' AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 1 AND
	      users_table.user_id <= 3 AND
	      events_table.event_type > 2 AND events_table.event_type < 4
	    )
	  ) AS subquery_1
	  LEFT JOIN
	    (SELECT
	       user_id,
	      COUNT(*) AS count_pay
	    FROM
	      users_table_local
	    WHERE
	      user_id >= 1 AND
	      user_id <= 3 AND
	      users_table_local.value_1 > 3 AND users_table_local.value_1 < 5
	    GROUP BY
	      user_id
	    HAVING
	      COUNT(*) > 1
	      LIMIT 10

	      ) AS subquery_2
	  ON
	    subquery_1.user_id = subquery_2.user_id
	  GROUP BY
	    subquery_1.user_id,
	    count_pay) AS subquery_top
	GROUP BY
	  count_pay, user_id
)
GROUP BY user_id
HAVING count(*) > 1 AND sum(value_2) > 29
ORDER BY 1;

SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_local_tables CASCADE;
SET search_path TO public;
