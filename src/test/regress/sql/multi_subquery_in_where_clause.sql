--
-- multi subquery in where queries aims to expand existing subquery pushdown
-- regression tests to cover more cases specifically subqueries in WHERE clause
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql
--
-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
-- ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1430000;
-- ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1430000;

-- subqueries in WHERE with greater operator
SELECT 
  user_id
FROM 
  users_table
WHERE 
  value_2 >  
          (SELECT 
              max(value_2) 
           FROM 
              events_table  
           WHERE 
              users_table.user_id = events_table.user_id AND event_type = 50
           GROUP BY
              user_id
          )
GROUP BY user_id
HAVING count(*) > 66
ORDER BY user_id
LIMIT 5; 

-- subqueries in where with ALL operator
SELECT 
  user_id
FROM 
  users_table   
WHERE 
  value_2 > 545 AND
  value_2 < ALL (SELECT avg(value_3) FROM events_table WHERE users_table.user_id = events_table.user_id GROUP BY user_id)
GROUP BY 
  1
ORDER BY 
  1 DESC
LIMIT 3; 

-- IN operator on non-partition key
SELECT 
  user_id
FROM 
  events_table as e1
WHERE
  event_type IN
            (SELECT 
                event_type
             FROM 
              events_table as e2
             WHERE
              value_2 = 15 AND value_3 > 25 AND
              e1.user_id = e2.user_id
            )
ORDER BY 1;

-- NOT IN on non-partition key
SELECT 
  user_id
FROM 
  events_table as e1
WHERE
  event_type NOT IN
            (SELECT 
                event_type
             FROM 
              events_table as e2
             WHERE
              value_2 = 15 AND value_3 > 25 AND
              e1.user_id = e2.user_id
            )
            GROUP BY 1

HAVING count(*) > 122
ORDER BY 1;

-- non-correlated query with =ANY on partition keys
 SELECT 
  user_id, count(*) 
FROM 
  users_table 
WHERE 
  user_id =ANY(SELECT user_id FROM users_table WHERE value_1 >= 10 AND value_1 <= 20) GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

-- users that appeared more than 118 times
SELECT 
  user_id
FROM 
  users_table
WHERE 118 <=
      (SELECT 
          count(*) 
        FROM 
          events_table 
        WHERE 
        	users_table.user_id = events_table.user_id 
        GROUP BY 
        	user_id)
GROUP BY 
	user_id
ORDER BY
	user_id;


-- the following query doesn't have a meaningul result
-- but it is a valid query with an arbitrary subquery in
-- WHERE clause
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 101 AND value_1 < 110
  AND value_2 >= 5
  AND user_id IN
  (
		SELECT
		  e1.user_id
		FROM (
		  -- Get the first time each user viewed the homepage.
		  SELECT
		    user_id,
		    1 AS view_homepage,
		    min(time) AS view_homepage_time
		  FROM events_table
		     WHERE
		     event_type IN (10, 20, 30, 40, 50, 60, 70, 80, 90)
		  GROUP BY user_id
		) e1 LEFT JOIN LATERAL (
		  SELECT
		    user_id,
		    1 AS use_demo,
		    time AS use_demo_time
		  FROM events_table
		  WHERE
		    user_id = e1.user_id AND
		       event_type IN (11, 21, 31, 41, 51, 61, 71, 81, 91)
		  ORDER BY time
		) e2 ON true LEFT JOIN LATERAL (
		  SELECT
		    user_id,
		    1 AS enter_credit_card,
		    time AS enter_credit_card_time
		  FROM  events_table
		  WHERE
		    user_id = e2.user_id AND
		    event_type IN (12, 22, 32, 42, 52, 62, 72, 82, 92)
		  ORDER BY time
		) e3 ON true LEFT JOIN LATERAL (
		  SELECT
		    1 AS submit_card_info,
		    user_id,
		    time AS enter_credit_card_time
		  FROM  events_table
		  WHERE
		    user_id = e3.user_id AND
		    event_type IN (13, 23, 33, 43, 53, 63, 73, 83, 93)
		  ORDER BY time
		) e4 ON true LEFT JOIN LATERAL (
		  SELECT
		    1 AS see_bought_screen
		  FROM  events_table
		  WHERE
		    user_id = e4.user_id AND
		    event_type IN (14, 24, 34, 44, 54, 64, 74, 84, 94)
		  ORDER BY time
		) e5 ON true
		group by e1.user_id
		HAVING sum(submit_card_info) > 0
)
ORDER BY 1, 2;


-- similar to the above query
-- the following query doesn't have a meaningul result
-- but it is a valid query with an arbitrary subquery in
-- WHERE clause
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
	      'action=>1'AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 10 AND
	      users_table.user_id <= 70 AND
	      events_table.event_type > 10 AND events_table.event_type < 12
	      )
	    UNION
	    (SELECT
	      users_table.user_id,
	      'action=>2'AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 10 AND
	      users_table.user_id <= 70 AND
	      events_table.event_type > 12 AND events_table.event_type < 14
	    )
	  ) AS subquery_1
	  LEFT JOIN
	    (SELECT
	       user_id,
	      COUNT(*) AS count_pay
	    FROM
	      users_table
	    WHERE
	      user_id >= 10 AND
	      user_id <= 70 AND
	      users_table.value_1 > 15 AND users_table.value_1 < 17
	    GROUP BY
	      user_id
	    HAVING
	      COUNT(*) > 1) AS subquery_2
	  ON
	    subquery_1.user_id = subquery_2.user_id
	  GROUP BY
	    subquery_1.user_id,
	    count_pay) AS subquery_top
	GROUP BY
	  count_pay, user_id
)
GROUP BY user_id
HAVING count(*) > 3 AND sum(value_2) > 49000
ORDER BY 1;


-- the following query doesn't have a meaningul result
-- but it is a valid query with an arbitrary subquery in
-- FROM clause involving a complex query in WHERE clause
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
			    AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 100 AND event_type <= 300 AND value_3 > 100 AND user_id = users_table.user_id)
				AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 300 AND event_type <= 350  AND value_3 > 100 AND user_id = users_table.user_id)
    		)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1;

-- 
-- below tests only aims for cases where all relations 
-- are not joined on partition key
--

-- e4 is not joined on the partition key
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 101 AND value_1 < 110
  AND value_2 >= 5
  AND user_id IN
  (
		SELECT
		  e1.user_id
		FROM (
		  -- Get the first time each user viewed the homepage.
		  SELECT
		    user_id,
		    1 AS view_homepage,
		    min(time) AS view_homepage_time
		  FROM events_table
		     WHERE
		     event_type IN (10, 20, 30, 40, 50, 60, 70, 80, 90)
		  GROUP BY user_id
		) e1 LEFT JOIN LATERAL (
		  SELECT
		    user_id,
		    1 AS use_demo,
		    time AS use_demo_time
		  FROM events_table
		  WHERE
		    user_id = e1.user_id AND
		       event_type IN (11, 21, 31, 41, 51, 61, 71, 81, 91)
		  ORDER BY time
		) e2 ON true LEFT JOIN LATERAL (
		  SELECT
		    user_id,
		    1 AS enter_credit_card,
		    time AS enter_credit_card_time
		  FROM  events_table
		  WHERE
		    user_id = e2.user_id AND
		    event_type IN (12, 22, 32, 42, 52, 62, 72, 82, 92)
		  ORDER BY time
		) e3 ON true LEFT JOIN LATERAL (
		  SELECT
		    1 AS submit_card_info,
		    user_id,
		    time AS enter_credit_card_time
		  FROM  events_table
		  WHERE
		    value_2 = e3.user_id AND
		    event_type IN (13, 23, 33, 43, 53, 63, 73, 83, 93)
		  ORDER BY time
		) e4 ON true LEFT JOIN LATERAL (
		  SELECT
		    1 AS see_bought_screen
		  FROM  events_table
		  WHERE
		    user_id = e4.user_id AND
		    event_type IN (14, 24, 34, 44, 54, 64, 74, 84, 94)
		  ORDER BY time
		) e5 ON true
		group by e1.user_id
		HAVING sum(submit_card_info) > 0
);


-- left leaf query does not return partition key
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
	      2 * users_table.user_id as user_id,
	      'action=>1'AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 10 AND
	      users_table.user_id <= 70 AND
	      events_table.event_type > 10 AND events_table.event_type < 12
	      )
	    UNION
	    (SELECT
	      users_table.user_id,
	      'action=>2'AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 10 AND
	      users_table.user_id <= 70 AND
	      events_table.event_type > 12 AND events_table.event_type < 14
	    )
	  ) AS subquery_1
	  LEFT JOIN
	    (SELECT
	       user_id,
	      COUNT(*) AS count_pay
	    FROM
	      users_table
	    WHERE
	      user_id >= 10 AND
	      user_id <= 70 AND
	      users_table.value_1 > 15 AND users_table.value_1 < 17
	    GROUP BY
	      user_id
	    HAVING
	      COUNT(*) > 1) AS subquery_2
	  ON
	    subquery_1.user_id = subquery_2.user_id
	  GROUP BY
	    subquery_1.user_id,
	    count_pay) AS subquery_top
	GROUP BY
	  count_pay, user_id
)
GROUP BY user_id
HAVING count(*) > 3 AND sum(value_2) > 49000
ORDER BY 1;

-- NOT EXISTS query has non-equi join
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
			    AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 100 AND event_type <= 300 AND value_3 > 100 AND user_id = users_table.user_id)
				AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 300 AND event_type <= 350  AND value_3 > 100 AND user_id != users_table.user_id)
    		)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1;

-- subquery in where clause doesn't have a relation
SELECT 
  user_id
FROM 
  users_table
WHERE 
  value_2 >  
          (SELECT 1);

-- OFFSET is not supported in the subquey
SELECT 
  user_id
FROM 
  users_table
WHERE 
  value_2 >  
          (SELECT 
              max(value_2) 
           FROM 
              events_table  
           WHERE 
              users_table.user_id = events_table.user_id AND event_type = 50
           GROUP BY
              user_id
           OFFSET 3
          );

-- semi join is not on the partition key for the third subquery
SELECT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 10 AND value_1 <= 20)
    AND user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 30 AND value_1 <= 40)
    AND value_2 IN (SELECT user_id FROM users_table WHERE  value_1 >= 50 AND value_1 <= 60);

CREATE FUNCTION test_join_function(integer, integer) RETURNS bool
    AS 'select $1 > $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- we disallow JOINs via functions
SELECT user_id, value_2 FROM users_table WHERE
  value_1 = 101
  AND value_2 >= 5
  AND NOT EXISTS (SELECT user_id FROM events_table WHERE event_type=101 AND value_3 > 100 AND test_join_function(events_table.user_id, users_table.user_id))
ORDER BY 1 DESC, 2 DESC
LIMIT 3;

DROP FUNCTION test_join_function(int,int);
