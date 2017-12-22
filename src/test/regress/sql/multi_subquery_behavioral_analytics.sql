--
-- multi subquery behavioral analytics queries aims to expand existing subquery pushdown
-- regression tests to cover more cases
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql
--

--- We don't need shard id sequence here given that we're not creating any shards, so not writing it at all

-- The following line is intended to force Citus to NOT use router planner for the tests in this
-- file. The motivation for doing this is to make sure that single-task queries can be planned
-- by non-router code-paths. Thus, this flag should NOT be used in production. Otherwise, the actual
-- router queries would fail.
SET citus.enable_router_execution TO FALSE;

------------------------------------
-- Vanilla funnel query
------------------------------------
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT u.user_id, e.event_type::text AS event, e.time
    FROM users_table AS u,
         events_table AS e
    WHERE u.user_id = e.user_id
      AND u.user_id >= 1
      AND u.user_id <= 3
      AND e.event_type IN (1, 2)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1;

------------------------------------
--  Funnel grouped by whether or not a user has done an event
--  This has multiple subqueries joinin at the top level
------------------------------------
SELECT user_id, sum(array_length(events_table, 1)), length(hasdone_event), hasdone_event
FROM (
  SELECT
    t1.user_id,
    array_agg(event ORDER BY time) AS events_table,
    COALESCE(hasdone_event, 'Has not done event') AS hasdone_event
  FROM (
    (
      SELECT u.user_id, 'step=>1'::text AS event, e.time
      FROM users_table AS u,
          events_table AS e
      WHERE  u.user_id = e.user_id
      AND u.user_id >= 1
      AND u.user_id <= 3
      AND e.event_type IN (1, 2)
    )
    UNION
    (
      SELECT u.user_id, 'step=>2'::text AS event, e.time
      FROM users_table AS u,
         events_table AS e
      WHERE  u.user_id = e.user_id
      AND u.user_id >= 1
      AND u.user_id <= 3
      AND e.event_type IN (3, 4)
    )
  ) t1 LEFT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e
      WHERE  e.user_id >= 1
      AND e.user_id <= 3
      AND e.event_type IN (5, 6)
  ) t2 ON (t1.user_id = t2.user_id)
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event
ORDER BY user_id;

-- same query but multiple joins are one level below, returns count of row instead of actual rows
SELECT count(*)
FROM (
	SELECT user_id, sum(array_length(events_table, 1)), length(hasdone_event), hasdone_event
	FROM (
	  SELECT
	    t1.user_id,
	    array_agg(event ORDER BY time) AS events_table,
	    COALESCE(hasdone_event, 'Has not done event') AS hasdone_event
	  FROM (
	    (
	      SELECT u.user_id, 'step=>1'::text AS event, e.time
	      FROM users_table AS u,
	          events_table AS e
	      WHERE  u.user_id = e.user_id
	        AND u.user_id >= 1
	        AND u.user_id <= 3
	        AND e.event_type IN (1, 2)
	    )
	    UNION
	    (
	      SELECT u.user_id, 'step=>2'::text AS event, e.time
	      FROM users_table AS u,
	         events_table AS e
	      WHERE  u.user_id = e.user_id
	      AND u.user_id >= 1
	      AND u.user_id <= 3
	      AND e.event_type IN (3, 4)
	    )
	  ) t1 LEFT JOIN (
	      SELECT DISTINCT user_id,
	        'Has done event'::TEXT AS hasdone_event
	      FROM  events_table AS e
	      WHERE  e.user_id >= 1
	      AND e.user_id <= 3
	      AND e.event_type IN (5, 6)
	  ) t2 ON (t1.user_id = t2.user_id)
	  GROUP BY  t1.user_id, hasdone_event
	) t GROUP BY user_id, hasdone_event
	ORDER BY user_id) u;

-- Same queries written without unions
SELECT user_id, sum(array_length(events_table, 1)), length(hasdone_event), hasdone_event
FROM (
  SELECT
    t1.user_id,
    array_agg(event ORDER BY time) AS events_table,
    COALESCE(hasdone_event, 'Has not done event') AS hasdone_event
  FROM (
      SELECT
      	u.user_id,
      	CASE WHEN e.event_type IN (1, 2)  THEN 'step=>1'::text else 'step==>2'::text END AS event,
      	e.time
      FROM users_table AS u,
          events_table AS e
      WHERE  u.user_id = e.user_id
      AND u.user_id >= 1
      AND u.user_id <= 3
      AND e.event_type IN (1, 2, 3, 4)
      GROUP BY 1,2,3
  ) t1 LEFT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e
      WHERE  e.user_id >= 1
      AND e.user_id <= 3
      AND e.event_type IN (5, 6)
  ) t2 ON (t1.user_id = t2.user_id)
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event
ORDER BY user_id;

-- same query but multiple joins are one level below, returns count of row instead of actual rows
SELECT count(*)
FROM (
	SELECT user_id, sum(array_length(events_table, 1)), length(hasdone_event), hasdone_event
	FROM (
	  SELECT
	    t1.user_id,
	    array_agg(event ORDER BY time) AS events_table,
	    COALESCE(hasdone_event, 'Has not done event') AS hasdone_event
	  FROM (
	      SELECT
	      	u.user_id,
	      	CASE WHEN e.event_type in (1, 2)  then 'step=>1'::text else 'step==>2'::text END AS event,
	      	e.time
	      FROM users_table AS u,
	          events_table AS e
	      WHERE  u.user_id = e.user_id
	      AND u.user_id >= 1
	      AND u.user_id <= 3
	      AND e.event_type IN (1, 2, 3, 4)
	      GROUP BY 1,2,3
	  ) t1 LEFT JOIN (
	      SELECT DISTINCT user_id,
	        'Has done event'::TEXT AS hasdone_event
	      FROM  events_table AS e
	      WHERE  e.user_id >= 1
	      AND e.user_id <= 3
	      AND e.event_type IN (5, 6)
	  ) t2 ON (t1.user_id = t2.user_id)
	  GROUP BY  t1.user_id, hasdone_event
	) t GROUP BY user_id, hasdone_event
    ORDER BY user_id) u;

------------------------------------
-- Funnel, grouped by the number of times a user has done an event
------------------------------------
SELECT
  user_id,
  avg(array_length(events_table, 1)) AS event_average,
  count_pay
  FROM (
  SELECT
  subquery_1.user_id,
  array_agg(event ORDER BY time) AS events_table,
  COALESCE(count_pay, 0) AS count_pay
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
      users_table.user_id >= 1 AND
      users_table.user_id <= 3 AND
      events_table.event_type > 1 AND events_table.event_type < 3
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
      users_table.user_id >= 1 AND
      users_table.user_id <= 3 AND
      events_table.event_type > 1 AND events_table.event_type < 4
    )
  ) AS subquery_1
  LEFT JOIN
    (SELECT
       user_id,
      COUNT(*) AS count_pay
    FROM
      users_table
    WHERE
      user_id >= 1 AND
      user_id <= 3 AND
      users_table.value_1 > 2 AND users_table.value_1 < 5
    GROUP BY
      user_id
    HAVING
      COUNT(*) > 1) AS subquery_2
  ON
    subquery_1.user_id = subquery_2.user_id
  GROUP BY
    subquery_1.user_id,
    count_pay) AS subquery_top
WHERE
  array_ndims(events_table) > 0
GROUP BY
  count_pay, user_id
ORDER BY
  event_average DESC, count_pay DESC, user_id DESC;

SELECT
  user_id,
  avg(array_length(events_table, 1)) AS event_average,
  count_pay
  FROM (
  SELECT
  subquery_1.user_id,
  array_agg(event ORDER BY time) AS events_table,
  COALESCE(count_pay, 0) AS count_pay
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
      users_table.user_id >= 1 AND
      users_table.user_id <= 3 AND
      events_table.event_type > 1 AND events_table.event_type < 3
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
      users_table.user_id >= 1 AND
      users_table.user_id <= 3 AND
      events_table.event_type > 1 AND events_table.event_type < 4
    )
  ) AS subquery_1
  LEFT JOIN
    (SELECT
       user_id,
      COUNT(*) AS count_pay
    FROM
      users_table
    WHERE
      user_id >= 1 AND
      user_id <= 3 AND
      users_table.value_1 > 2 AND users_table.value_1 < 4
    GROUP BY
      user_id
    HAVING
      COUNT(*) > 1) AS subquery_2
  ON
    subquery_1.user_id = subquery_2.user_id
  GROUP BY
    subquery_1.user_id,
    count_pay) AS subquery_top
WHERE
  array_ndims(events_table) > 0
GROUP BY
  count_pay, user_id
HAVING
  avg(array_length(events_table, 1)) > 0
ORDER BY
  event_average DESC, count_pay DESC, user_id DESC;

-- Same queries rewritten without using unions
 SELECT
  user_id,
  avg(array_length(events_table, 1)) AS event_average,
  count_pay
  FROM (
  SELECT
  subquery_1.user_id,
  array_agg(event ORDER BY time) AS events_table,
  COALESCE(count_pay, 0) AS count_pay
  FROM
  (
   SELECT
      users_table.user_id,
      CASE 
        WHEN 
          events_table.event_type > 1 AND events_table.event_type < 3
        THEN 'action=>1' 
        ELSE 'action=>2' 
      END AS event,
      events_table.time
    FROM
      users_table,
      events_table
    WHERE
      users_table.user_id = events_table.user_id AND
      users_table.user_id >= 1 AND
      users_table.user_id <= 3 AND
      (events_table.event_type > 1 AND events_table.event_type < 3
      	OR
      events_table.event_type > 2 AND events_table.event_type < 4)
      GROUP BY 1, 2, 3
  ) AS subquery_1
  LEFT JOIN
    (SELECT
       user_id,
      COUNT(*) AS count_pay
    FROM
      users_table
    WHERE
      user_id >= 1 AND
      user_id <= 3 AND
      users_table.value_1 > 3 AND users_table.value_1 < 5
    GROUP BY
      user_id
    HAVING
      COUNT(*) > 1) AS subquery_2
  ON
    subquery_1.user_id = subquery_2.user_id
  GROUP BY
    subquery_1.user_id,
    count_pay) AS subquery_top
WHERE
  array_ndims(events_table) > 0
GROUP BY
  count_pay, user_id
ORDER BY
  event_average DESC, count_pay DESC, user_id DESC;

SELECT
  user_id,
  avg(array_length(events_table, 1)) AS event_average,
  count_pay
  FROM (
  SELECT
  subquery_1.user_id,
  array_agg(event ORDER BY time) AS events_table,
  COALESCE(count_pay, 0) AS count_pay
  FROM
  (
   SELECT
      users_table.user_id,
      CASE WHEN events_table.event_type > 1 AND events_table.event_type < 3 THEN 'action=>1' ELSE 'action=>2' END AS event,
      events_table.time
    FROM
      users_table,
      events_table
    WHERE
      users_table.user_id = events_table.user_id AND
      users_table.user_id >= 1 AND
      users_table.user_id <= 3 AND
      (events_table.event_type > 1 AND events_table.event_type < 3
      	OR
      events_table.event_type > 2 AND events_table.event_type < 4)
    GROUP BY 1, 2, 3
  ) AS subquery_1
  LEFT JOIN
    (SELECT
       user_id,
      COUNT(*) AS count_pay
    FROM
      users_table
    WHERE
      user_id >= 1 AND
      user_id <= 3 AND
      users_table.value_1 > 3 AND users_table.value_1 < 5
    GROUP BY
      user_id
    HAVING
      COUNT(*) > 1) AS subquery_2
  ON
    subquery_1.user_id = subquery_2.user_id
  GROUP BY
    subquery_1.user_id,
    count_pay) AS subquery_top
WHERE
  array_ndims(events_table) > 0
GROUP BY
  count_pay, user_id
HAVING
  avg(array_length(events_table, 1)) > 0
ORDER BY
  event_average DESC, count_pay DESC, user_id DESC;

------------------------------------
-- Most recently seen users_table events_table
------------------------------------
-- Note that we don't use ORDER BY/LIMIT yet
------------------------------------
SELECT
    user_id,
    user_lastseen,
    array_length(event_array, 1)
FROM (
    SELECT
        user_id,
        max(u.time) as user_lastseen,
        array_agg(event_type ORDER BY u.time) AS event_array
    FROM (
        SELECT user_id, time
        FROM users_table
        WHERE
        user_id >= 1 AND
        user_id <= 3 AND
        users_table.value_1 > 1 AND users_table.value_1 < 3
        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table
          WHERE user_id = u.user_id AND
          events_table.event_type > 1 AND events_table.event_type < 3
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC, user_id;

------------------------------------
-- Count the number of distinct users_table who are in segment X and Y and Z
------------------------------------
SELECT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 1 AND value_1 <= 2)
    AND user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 3 AND value_1 <= 4)
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= 5 AND value_1 <= 6)
GROUP BY 
  user_id
ORDER BY
  user_id DESC
  LIMIT 5;

------------------------------------
-- Find customers who have done X, and satisfy other customer specific criteria
------------------------------------
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 1 AND value_1 < 3
  AND value_2 >= 1
  AND EXISTS (SELECT user_id FROM events_table WHERE event_type > 1 AND event_type < 3 AND value_3 > 1 AND user_id = users_table.user_id)
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

------------------------------------
-- Customers who haven’t done X, and satisfy other customer specific criteria
------------------------------------
SELECT user_id, value_2 FROM users_table WHERE
  value_1 = 2
  AND value_2 >= 1
  AND NOT EXISTS (SELECT user_id FROM events_table WHERE event_type=2 AND value_3 > 1 AND user_id = users_table.user_id)
ORDER BY 1 DESC, 2 DESC
LIMIT 3;

------------------------------------
-- Customers who have done X and Y, and satisfy other customer specific criteria
------------------------------------
SELECT user_id, sum(value_2) as cnt FROM users_table WHERE
  value_1 > 1
  AND value_2 >= 1
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type != 1 AND value_3 > 1 AND user_id = users_table.user_id)
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type = 2 AND value_3 > 1 AND user_id = users_table.user_id)
GROUP BY
  user_id
ORDER BY cnt DESC, user_id DESC
LIMIT 5;

------------------------------------
-- Customers who have done X and haven’t done Y, and satisfy other customer specific criteria
------------------------------------
SELECT user_id, value_2 FROM users_table WHERE
  value_2 >= 1
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 1 AND event_type <= 3 AND value_3 > 1 AND user_id = users_table.user_id)
  AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 3 AND event_type <= 4  AND value_3 > 1 AND user_id = users_table.user_id)
ORDER BY 2 DESC, 1 DESC
LIMIT 4;

------------------------------------
-- Customers who have done X more than 2 times, and satisfy other customer specific criteria
------------------------------------
SELECT user_id,
         avg(value_2)
  FROM   users_table
  WHERE  value_1 > 1
         AND value_1 < 3
         AND value_2 >= 1
         AND EXISTS (SELECT user_id
                     FROM   events_table
                     WHERE  event_type > 1
                            AND event_type < 3
                            AND value_3 > 1
                            AND user_id = users_table.user_id
                     GROUP  BY user_id
                     HAVING Count(*) > 2)
GROUP BY 
  user_id
ORDER BY 
  1 DESC, 2 DESC
LIMIT 5;

------------------------------------
-- Find me all users_table who logged in more than once
------------------------------------
SELECT user_id, value_1 from
(
  SELECT 
    user_id, value_1 From users_table
  WHERE 
    value_2 > 1 and user_id = 2 
  GROUP BY 
    value_1, user_id 
  HAVING 
    count(*) > 1
) AS a
ORDER BY 
  user_id ASC, value_1 ASC;

-- same query with additional filter to make it not router plannable
SELECT user_id, value_1 from
(
  SELECT 
    user_id, value_1 From users_table
  WHERE 
    value_2 > 1 and (user_id = 2 OR user_id = 3) 
  GROUP BY 
    value_1, user_id 
  HAVING count(*) > 1
) AS a
ORDER BY 
  user_id ASC, value_1 ASC;

------------------------------------
-- Find me all users_table who has done some event and has filters
------------------------------------
SELECT user_id
FROM events_table
WHERE
	event_type = 3 AND value_2 > 2 AND 
  user_id IN
            (SELECT 
                user_id
             FROM 
              users_table
             WHERE
   	          value_1 = 1 AND value_2 > 2
            )
ORDER BY 1;

------------------------------------
-- Which events_table did people who has done some specific events_table
------------------------------------
SELECT 
  user_id, event_type FROM events_table
WHERE 
  user_id in (SELECT user_id from events_table WHERE event_type > 3 and event_type < 5)
GROUP BY 
  user_id, event_type
ORDER BY 2 DESC, 1 
LIMIT 3;

------------------------------------
-- Find me all the users_table who has done some event more than three times
------------------------------------
SELECT user_id FROM
(
  SELECT
     user_id
  FROM
   	events_table
  WHERE 
    event_type = 2
  GROUP BY 
    user_id 
  HAVING 
    count(*) > 1
) AS a
ORDER BY 
  user_id;

------------------------------------
-- Find my assets that have the highest probability and fetch their metadata
------------------------------------
CREATE TEMP TABLE assets AS
SELECT
    users_table.user_id, users_table.value_1, prob
FROM
   users_table
        JOIN
   (SELECT
      ma.user_id, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob
    FROM
    	users_table AS ma, events_table as short_list
    WHERE
    	short_list.user_id = ma.user_id and ma.value_1 < 2 and short_list.event_type < 2
    ) temp
  ON users_table.user_id = temp.user_id
  WHERE 
    users_table.value_1 < 2;

  -- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM assets;

DROP TABLE assets;

-- count number of distinct users who have value_1 equal to 5 or 13 but not 3
-- original query that fails
SELECT count(*) FROM
(
  SELECT 
    user_id
  FROM 
    users_table
  WHERE 
    (value_1 = '1' OR value_1 = '3') AND 
    user_id NOT IN (select user_id from users_table where value_1 = '4')
  GROUP BY 
    user_id
  HAVING 
    count(distinct value_1) = 2
) as foo;

-- previous push down query
SELECT subquery_count FROM
    (SELECT count(*) as subquery_count FROM
        (SELECT
            user_id
        FROM
            users_table
        WHERE
            (value_1 = '1' OR value_1 = '3')
        GROUP BY 
          user_id
        HAVING 
          count(distinct value_1) = 2) as a
        LEFT JOIN
        (SELECT
            user_id
        FROM
            users_table
        WHERE
            (value_1 = '2')
        GROUP BY 
          user_id) as b 
        ON a.user_id = b.user_id 
        WHERE 
          b.user_id IS NULL
    GROUP BY 
      a.user_id
    ) AS inner_subquery;

-- new pushdown query without single range table entry at top requirement
SELECT count(*) as subquery_count
FROM (
	SELECT
    	user_id
    FROM
		users_table
	WHERE
		(value_1 = '1' OR value_1 = '3')
	GROUP BY 
    user_id
	HAVING 
    count(distinct value_1) = 2
	) as a
	LEFT JOIN (
	SELECT
		user_id
	FROM
		users_table
	WHERE
		(value_1 = '2')
	GROUP BY 
    user_id) AS b
	ON a.user_id = b.user_id
WHERE 
  b.user_id IS NULL
GROUP BY 
  a.user_id;

-- most queries below has limit clause
-- therefore setting subquery_pushdown flag for all
SET citus.subquery_pushdown to ON;

-- multi-subquery-join
-- The first query has filters on partion column to make it router plannable
-- but it is processed by logical planner since we disabled router execution
SELECT
  e1.user_id,
  sum(view_homepage) AS viewed_homepage,
  sum(use_demo) AS use_demo,
  sum(enter_credit_card) AS entered_credit_card,
  sum(submit_card_info) as submit_card_info,
  sum(see_bought_screen) as see_bought_screen
FROM (
  -- Get the first time each user viewed the homepage.
  SELECT
    user_id,
    1 AS view_homepage,
    min(time) AS view_homepage_time
  FROM events_table
     WHERE user_id = 1 and
     event_type IN (1, 2)
  GROUP BY user_id
) e1 LEFT JOIN LATERAL (
  SELECT
    user_id,
    1 AS use_demo,
    time AS use_demo_time
  FROM events_table
  WHERE
    user_id = e1.user_id AND user_id = 1 and
       event_type IN (2, 3)
  ORDER BY time
  LIMIT 1
) e2 ON true LEFT JOIN LATERAL (
  SELECT
    user_id,
    1 AS enter_credit_card,
    time AS enter_credit_card_time
  FROM  events_table
  WHERE
    user_id = e2.user_id AND user_id = 1 and
    event_type IN (3, 4)
  ORDER BY time
  LIMIT 1
) e3 ON true LEFT JOIN LATERAL (
  SELECT
    1 AS submit_card_info,
    user_id,
    time AS enter_credit_card_time
  FROM  events_table
  WHERE
    user_id = e3.user_id AND user_id = 1 and
    event_type IN (4, 5)
  ORDER BY time
  LIMIT 1
) e4 ON true LEFT JOIN LATERAL (
  SELECT
    1 AS see_bought_screen
  FROM  events_table
  WHERE
    user_id = e4.user_id AND user_id = 1 and
    event_type IN (5, 6)
  ORDER BY time
  LIMIT 1
) e5 ON true
WHERE 
  e1.user_id = 1
GROUP BY 
  e1.user_id
LIMIT 1;

-- Same query without all limitations
SELECT
  e1.user_id,
  sum(view_homepage) AS viewed_homepage,
  sum(use_demo) AS use_demo,
  sum(enter_credit_card) AS entered_credit_card,
  sum(submit_card_info) as submit_card_info,
  sum(see_bought_screen) as see_bought_screen
FROM (
  -- Get the first time each user viewed the homepage.
  SELECT
    user_id,
    1 AS view_homepage,
    min(time) AS view_homepage_time
  FROM events_table
     WHERE
     event_type IN (1, 2)
  GROUP BY user_id
) e1 LEFT JOIN LATERAL (
  SELECT
    user_id,
    1 AS use_demo,
    time AS use_demo_time
  FROM events_table
  WHERE
    user_id = e1.user_id AND
       event_type IN (2, 3)
  ORDER BY time
) e2 ON true LEFT JOIN LATERAL (
  SELECT
    user_id,
    1 AS enter_credit_card,
    time AS enter_credit_card_time
  FROM  events_table
  WHERE
    user_id = e2.user_id AND
    event_type IN (3, 4)
  ORDER BY time
) e3 ON true LEFT JOIN LATERAL (
  SELECT
    1 AS submit_card_info,
    user_id,
    time AS enter_credit_card_time
  FROM  events_table
  WHERE
    user_id = e3.user_id AND
    event_type IN (4, 5)
  ORDER BY time
) e4 ON true LEFT JOIN LATERAL (
  SELECT
    1 AS see_bought_screen
  FROM  events_table
  WHERE
    user_id = e4.user_id AND
    event_type IN (5, 6)
  ORDER BY time
) e5 ON true
GROUP BY e1.user_id
ORDER BY 6 DESC  NULLS LAST, 5 DESC  NULLS LAST, 4 DESC  NULLS LAST, 3 DESC  NULLS LAST, 2 DESC  NULLS LAST, 1
LIMIT 15;

-- Same query without all limitations but uses having() to show only those submitted their credit card info
SELECT
  e1.user_id,
  sum(view_homepage) AS viewed_homepage,
  sum(use_demo) AS use_demo,
  sum(enter_credit_card) AS entered_credit_card,
  sum(submit_card_info) as submit_card_info,
  sum(see_bought_screen) as see_bought_screen
FROM (
  -- Get the first time each user viewed the homepage.
  SELECT
    user_id,
    1 AS view_homepage,
    min(time) AS view_homepage_time
  FROM events_table
     WHERE
     event_type IN (1, 2)
  GROUP BY user_id
) e1 LEFT JOIN LATERAL (
  SELECT
    user_id,
    1 AS use_demo,
    time AS use_demo_time
  FROM events_table
  WHERE
    user_id = e1.user_id AND
       event_type IN (2, 3)
  ORDER BY time
) e2 ON true LEFT JOIN LATERAL (
  SELECT
    user_id,
    1 AS enter_credit_card,
    time AS enter_credit_card_time
  FROM  events_table
  WHERE
    user_id = e2.user_id AND
    event_type IN (3, 4)
  ORDER BY time
) e3 ON true LEFT JOIN LATERAL (
  SELECT
    1 AS submit_card_info,
    user_id,
    time AS enter_credit_card_time
  FROM  events_table
  WHERE
    user_id = e3.user_id AND
    event_type IN (4, 5)
  ORDER BY time
) e4 ON true LEFT JOIN LATERAL (
  SELECT
    1 AS see_bought_screen
  FROM  events_table
  WHERE
    user_id = e4.user_id AND
    event_type IN (5, 6)
  ORDER BY time
) e5 ON true
group by e1.user_id
HAVING sum(submit_card_info) > 0
ORDER BY 6 DESC  NULLS LAST, 5 DESC  NULLS LAST, 4 DESC  NULLS LAST, 3 DESC  NULLS LAST, 2 DESC  NULLS LAST, 1
LIMIT 15;

-- Explain analyze on this query fails due to #756
-- avg expression used on order by
SELECT a.user_id, avg(b.value_2) as subquery_avg
FROM (
	 SELECT
    	user_id
    FROM
		  users_table
	   WHERE
		  (value_1 > 2)
	   GROUP BY 
      user_id
	   HAVING 
      count(distinct value_1) > 2
	) as a
	LEFT JOIN (
	SELECT
		user_id, value_2, value_3
	FROM
		users_table
	WHERE
		(value_1 > 3)) AS b
ON a.user_id = b.user_id
WHERE 
  b.user_id IS NOT NULL
GROUP BY 
  a.user_id
ORDER BY 
  avg(b.value_3), 2, 1
LIMIT 5;

-- add having to the same query
SELECT a.user_id, avg(b.value_2) as subquery_avg
FROM (
	SELECT
    	user_id
    FROM
		  users_table
	   WHERE
		(value_1 > 2)
	GROUP BY user_id
	HAVING count(distinct value_1) > 2
	) as a
	LEFT JOIN (
	SELECT
		user_id, value_2, value_3
	FROM
		users_table
	WHERE
		(value_1 > 3)) AS b
ON a.user_id = b.user_id
WHERE 
  b.user_id IS NOT NULL
GROUP BY 
  a.user_id
HAVING 
  sum(b.value_3) > 5
ORDER BY 
  avg(b.value_3), 2, 1
LIMIT 5;

-- avg on the value_3 is not a resjunk
SELECT a.user_id, avg(b.value_2) as subquery_avg, avg(b.value_3)
FROM
	(SELECT 
      user_id
    FROM 
      users_table
	  WHERE 
      (value_1 > 2)
	  GROUP BY 
      user_id
	 HAVING 
    count(distinct value_1) > 2
	) as a
	LEFT JOIN
	(
      SELECT 
        user_id, value_2, value_3
	    FROM 
        users_table
	    WHERE 
        (value_1 > 3)
	) AS b
	ON a.user_id = b.user_id
WHERE 
  b.user_id IS NOT NULL
GROUP BY 
  a.user_id
ORDER BY 
  avg(b.value_3) DESC, 2, 1
LIMIT 5;

-- a powerful query structure that analyzes users/events
-- using (relation JOIN subquery JOIN relation)
SELECT  u.user_id, sub.value_2, sub.value_3, COUNT(e2.user_id) counts
FROM
	users_table u
	LEFT OUTER JOIN LATERAL
	(SELECT 
      *
   FROM 
      events_table e1
   WHERE 
      e1.user_id = u.user_id
    ORDER BY 
      e1.value_3 DESC
    LIMIT 1
    ) sub
	ON true
	LEFT OUTER JOIN events_table e2
	ON e2.user_id = sub.user_id
WHERE 
  e2.value_2 > 1 AND e2.value_2 < 5 AND u.value_2 > 1 AND u.value_2 < 5
GROUP BY 
  u.user_id, sub.value_2, sub.value_3
ORDER BY 
  4 DESC, 1 DESC, 2 ASC, 3 ASC
LIMIT 10;

-- distinct users joined with events
SELECT
  avg(events_table.event_type) as avg_type,
  count(*) as users_count
FROM events_table
	JOIN
	(SELECT 
      DISTINCT user_id
   FROM 
    users_table
  ) as distinct_users
  ON distinct_users.user_id = events_table.user_id
GROUP BY 
  distinct_users.user_id
ORDER BY 
  users_count desc, avg_type DESC
LIMIT 5;

-- reduce the data set, aggregate and join
SELECT
  events_table.event_type,
  users_count.ct
FROM events_table
	JOIN
	(SELECT distinct_users.user_id, count(1) as ct
     FROM
     	(SELECT 
          user_id
    	 FROM 
          users_table
  		) as distinct_users
  	 GROUP BY 
        distinct_users.user_id
    ) as users_count
    ON users_count.user_id = events_table.user_id
ORDER BY 
  users_count.ct desc, event_type DESC
LIMIT 5;

--- now, test (subquery JOIN subquery)
SELECT n1.user_id, count_1, total_count
FROM
	(SELECT 
      user_id, count(1) as count_1
   FROM 
      users_table
   GROUP BY 
      user_id
    ) n1
 	INNER JOIN
  	(
        SELECT 
          user_id, count(1) as total_count
        FROM 
         events_table
     GROUP BY 
        user_id, event_type
    ) n2
    ON (n2.user_id = n1.user_id)
ORDER BY 
  total_count DESC, count_1 DESC, 1 DESC
LIMIT 10;

SELECT a.user_id, avg(b.value_2) as subquery_avg
FROM
	(SELECT 
      user_id
   FROM 
    users_table
	 WHERE 
    (value_1 > 2)
	 GROUP BY 
    user_id
	 HAVING 
    count(distinct value_1) > 2
	) as a
	LEFT JOIN
	(SELECT 
      DISTINCT ON (user_id) user_id, value_2, value_3
   FROM 
        users_table
   WHERE 
        (value_1 > 3)
   ORDER BY  
        1,2,3
    ) AS b
    ON a.user_id = b.user_id
WHERE b.user_id IS NOT NULL
GROUP BY a.user_id
ORDER BY avg(b.value_3), 2, 1
LIMIT 5;

-- distinct clause must include partition column
-- when used in target list
SELECT a.user_id, avg(b.value_2) as subquery_avg
FROM
	(SELECT 
      user_id
   FROM 
      users_table
	 WHERE 
      (value_1 > 2)
	 GROUP BY 
      user_id
	 HAVING 
      count(distinct value_1) > 2
	) as a
	LEFT JOIN
	(SELECT 
      DISTINCT ON (value_2) value_2 , user_id, value_3
	 FROM 
      users_table
	 WHERE 
      (value_1 > 3)
	 ORDER BY 
      1,2,3
	) AS b
	USING (user_id)
GROUP BY user_id;

SELECT a.user_id, avg(b.value_2) as subquery_avg
FROM
	(SELECT 
      user_id
   FROM 
      users_table
	 WHERE 
      (value_1 > 2)
	 GROUP BY 
      user_id
	 HAVING 
      count(distinct value_1) > 2
	) as a
	LEFT JOIN
	(SELECT 
      DISTINCT ON (value_2, user_id) value_2 , user_id, value_3
	 FROM 
      users_table
	 WHERE 
      (value_1 > 3)
	 ORDER BY 
      1,2,3
	) AS b
	ON a.user_id = b.user_id
WHERE 
  b.user_id IS NOT NULL
GROUP BY 
  a.user_id
ORDER BY 
  avg(b.value_3), 2, 1
LIMIT 5;

SELECT user_id, event_type
FROM 
	(SELECT *
	 FROM
	 	(
	 		(SELECT 
          event_type, user_id as a_user_id 
        FROM 
          events_table) AS a
      JOIN
      (SELECT
          ma.user_id AS user_id, ma.value_2 AS value_2,
          (GREATEST(coalesce((ma.value_3 * ma.value_2) / 20, 0.0) + GREATEST(1.0))) / 2 AS prob
       FROM 
          users_table AS ma
       WHERE 
          (ma.value_2 > 1)
       ORDER BY 
          prob DESC, value_2 DESC, user_id DESC
        LIMIT 10
        ) AS ma
        ON (a.a_user_id = ma.user_id)
      ) AS inner_sub
  	ORDER BY 
      prob DESC, value_2 DESC, user_id DESC, event_type DESC
   	LIMIT 10
   	) AS outer_sub
ORDER BY 
  prob DESC, value_2 DESC, user_id DESC, event_type DESC
LIMIT 10;

-- very similar query but produces different result due to
-- ordering difference in the previous one's inner query
SELECT user_id, event_type
FROM
	 (SELECT 
      event_type, user_id as a_user_id 
    FROM 
      events_table) AS a
    JOIN
   (SELECT
     	 ma.user_id AS user_id, ma.value_2 AS value_2,
       (GREATEST(coalesce((ma.value_3 * ma.value_2) / 20, 0.0) + GREATEST(1.0))) / 2 AS prob
      FROM 
        users_table AS ma
      WHERE 
        (ma.value_2 > 1)
      ORDER BY 
        prob DESC, user_id DESC
      LIMIT 10
     ) AS ma
     ON (a.a_user_id = ma.user_id)
ORDER BY 
  prob DESC, event_type DESC, user_id DESC
LIMIT 10;

-- now they produce the same result when ordering fixed in 'outer_sub'
SELECT user_id, event_type
FROM 
	(SELECT *
	 FROM
	 	(
	 		(SELECT 
          event_type, user_id as a_user_id 
       FROM 
        events_table
      ) AS a
      JOIN
      (SELECT
          ma.user_id AS user_id, ma.value_2 AS value_2,
          (GREATEST(coalesce((ma.value_3 * ma.value_2) / 20, 0.0) + GREATEST(1.0))) / 2 AS prob
       FROM 
        users_table AS ma
       WHERE 
          (ma.value_2 > 1)
        ORDER BY 
          prob DESC, user_id DESC
        LIMIT 10
        ) AS ma
         ON (a.a_user_id = ma.user_id)
      ) AS inner_sub
  	ORDER BY 
      prob DESC, event_type DESC, user_id DESC
   	LIMIT 10
   	) AS outer_sub
ORDER BY 
  prob DESC, event_type DESC, user_id DESC
LIMIT 10;

-- this is one complex join query derived from a user's production query
-- first declare the function on workers on master
-- With array_index:
SELECT * FROM  run_command_on_workers('CREATE OR REPLACE FUNCTION array_index(ANYARRAY, ANYELEMENT)
      RETURNS INT AS $$
        SELECT i
        FROM (SELECT generate_series(array_lower($1, 1), array_upper($1, 1))) g(i)
        WHERE $1 [i] = $2
        LIMIT 1;
    $$ LANGUAGE sql')
ORDER BY 1,2;

CREATE OR REPLACE FUNCTION array_index(ANYARRAY, ANYELEMENT)
      RETURNS INT AS $$
        SELECT i
        FROM (SELECT generate_series(array_lower($1, 1), array_upper($1, 1))) g(i)
        WHERE $1 [i] = $2
        LIMIT 1;
    $$ LANGUAGE sql;

SELECT *
FROM
  (SELECT *
   FROM (
           (SELECT user_id AS user_id_e,
                   event_type AS event_type_e
            FROM events_table ) AS ma_e
         JOIN
           (SELECT value_2,
                   value_3,
                   user_id
            FROM
              (SELECT *
               FROM (
                       (SELECT user_id_p AS user_id
                        FROM
                          (SELECT *
                           FROM (
                                   (SELECT 
                                      user_id AS user_id_p
                                    FROM 
                                      events_table
                                    WHERE 
                                      (event_type IN (1,2,3,4,5)) ) AS ma_p
                                 JOIN
                                   (SELECT 
                                      user_id AS user_id_a
                                    FROM 
                                      users_table
                                    WHERE 
                                      (value_2 % 5 = 1) ) AS a 
                                  ON (a.user_id_a = ma_p.user_id_p) ) ) AS a_ma_p ) AS inner_filter_q
                     JOIN
                       (SELECT 
                          value_2, value_3, user_id AS user_id_ck
                        FROM 
                          events_table
                        WHERE 
                          event_type = ANY(ARRAY [4, 5, 6])
                        ORDER BY 
                          value_3 ASC, user_id_ck DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC
                        LIMIT 10 ) 
                       AS ma_ck ON (ma_ck.user_id_ck = inner_filter_q.user_id) ) 
                    AS inner_sub_q
                    ORDER BY 
                        value_3 ASC, user_id_ck DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC
                    LIMIT 10 ) 
                AS outer_sub_q
                ORDER BY 
                  value_3 ASC, user_id DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC
                LIMIT 10) 
            AS inner_search_q 
          ON (ma_e.user_id_e = inner_search_q.user_id) ) 
        AS outer_inner_sub_q
        ORDER BY 
          value_3 ASC, user_id DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC, event_type_e DESC
        LIMIT 10) 
AS outer_outer_sub_q
ORDER BY 
  value_3 ASC, user_id DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC, event_type_e DESC
LIMIT 10;

-- top level select * is removed now there is 
-- a join at top level.
SELECT *
FROM 
	(
		(SELECT
		 	user_id AS user_id_e, event_type as event_type_e
     FROM
      events_table
    ) AS ma_e
    JOIN
      (SELECT
          value_2, value_3, user_id
       FROM
       (SELECT 
          *
        FROM
         	(
         		(SELECT 
                user_id_p AS user_id
             FROM
               	(SELECT 
                    *
                 FROM
                  	(
                    		(SELECT 
                            user_id AS user_id_p
                         FROM 
                          events_table
                         WHERE 
                          (event_type IN (1, 2, 3, 4, 5))
                       	 ) AS ma_p
                        JOIN
                         (SELECT 
                            user_id AS user_id_a
                         	 FROM 
                            users_table
                         	 WHERE 
                            (value_2 % 5 = 1)
                         	) AS a
                         	ON (a.user_id_a = ma_p.user_id_p)
                     	)
                       ) AS a_ma_p
                   ) AS inner_filter_q
                   JOIN
                   (SELECT
                        value_2, value_3, user_id AS user_id_ck
                    FROM 
                      events_table
                    WHERE 
                      event_type = ANY(ARRAY [4, 5, 6])
                    ORDER BY
                     		value_3 ASC, user_id_ck DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC
                    LIMIT 10
                   ) AS ma_ck
                   ON (ma_ck.user_id_ck = inner_filter_q.user_id)
                ) AS inner_sub_q
            ORDER BY
               	value_3 ASC, user_id_ck DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC
                LIMIT 10
            ) AS outer_sub_q
        ORDER BY
       	  value_3 ASC, user_id DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC
        LIMIT 10) AS inner_search_q
       ON (ma_e.user_id_e = inner_search_q.user_id)
     ) AS outer_inner_sub_q
ORDER BY
	value_3 ASC, user_id DESC, array_index(ARRAY [1, 2, 3], (value_2 % 3)) ASC, event_type_e DESC
LIMIT 10;


-- drop created functions
SELECT * FROM run_command_on_workers('DROP FUNCTION array_index(ANYARRAY, ANYELEMENT)')
ORDER BY 1,2;
DROP FUNCTION array_index(ANYARRAY, ANYELEMENT);

-- a query with a constant subquery
SELECT count(*) as subquery_count
FROM (
  SELECT 
    user_id
  FROM
    users_table
  WHERE
    (value_1 = '1' OR value_1 = '3')
  GROUP BY user_id                                                                          
  HAVING count(distinct value_1) = 2
  ) as a
  LEFT JOIN (
  SELECT
    1 as user_id
  ) AS b 
  ON a.user_id = b.user_id 
WHERE b.user_id IS NULL
GROUP BY a.user_id;

-- volatile function in the subquery
SELECT count(*) as subquery_count
FROM (
  SELECT 
      user_id
    FROM
    users_table
  WHERE
    (value_1 = '1' OR value_1 = '3')
  GROUP BY user_id                                                                          
  HAVING count(distinct value_1) = 2
  ) as a
  INNER JOIN (
  SELECT
    random()::int as user_id
  ) AS b 
  ON a.user_id = b.user_id 
WHERE b.user_id IS NULL
GROUP BY a.user_id;

-- this is slightly different, we use RTE_VALUEs here
SELECT Count(*) AS subquery_count 
FROM (SELECT 
        user_id 
      FROM 
        users_table 
      WHERE 
        (value_1 = '1' OR value_1 = '3' ) 
      GROUP BY 
        user_id 
      HAVING 
        Count(DISTINCT value_1) = 2) AS a 
    INNER JOIN 
     (SELECT 
        * 
      FROM 
        (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (user_id, letter)) AS b 
    ON a.user_id = b.user_id 
WHERE b.user_id IS NULL 
GROUP BY a.user_id;  


-- same query without LIMIT/OFFSET returns 30 rows

SET client_min_messages TO DEBUG1;
-- now, lets use a simple expression on the LIMIT and explicit coercion on the OFFSET
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT 
      u.user_id, e.event_type::text AS event, e.time
    FROM 
      users_table AS u,
      events_table AS e
    WHERE 
      u.user_id = e.user_id AND e.event_type IN (1, 2)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1
LIMIT 1+1 OFFSET 1::smallint;

-- now, lets use implicit coersion in LIMIT and a simple expressions on OFFSET
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT 
      u.user_id, e.event_type::text AS event, e.time
    FROM 
      users_table AS u,
      events_table AS e
    WHERE 
      u.user_id = e.user_id AND e.event_type IN (1, 2)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1
LIMIT '3' OFFSET 2+1;

-- create a test function which is marked as volatile
CREATE OR REPLACE FUNCTION volatile_func_test()
      RETURNS INT AS $$
        SELECT 1;
    $$ LANGUAGE sql VOLATILE;

-- Citus should be able to evalute functions/row comparisons on the LIMIT/OFFSET
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT 
      u.user_id, e.event_type::text AS event, e.time
    FROM 
      users_table AS u,
      events_table AS e
    WHERE 
      u.user_id = e.user_id AND e.event_type IN (1, 2, 3, 4)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1
LIMIT volatile_func_test() + (ROW(1,2,NULL) < ROW(1,3,0))::int OFFSET volatile_func_test() + volatile_func_test();

-- now, lets use expressions on both the LIMIT and OFFSET
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT 
      u.user_id, e.event_type::text AS event, e.time
    FROM 
      users_table AS u,
      events_table AS e
    WHERE 
      u.user_id = e.user_id AND e.event_type IN (1, 2)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1
LIMIT (5 > 4)::int OFFSET
                    CASE
                      WHEN 5 != 5 THEN 27
                      WHEN 1 > 5 THEN 28
                      ELSE 2
                  END;

-- we don't allow parameters on the LIMIT/OFFSET clauses
PREPARE parametrized_limit AS
SELECT user_id, array_length(events_table, 1)
FROM (
   SELECT user_id, array_agg(event ORDER BY time) AS events_table
   FROM (
     SELECT u.user_id, e.event_type::text AS event, e.time
     FROM users_table AS u,
          events_table AS e
     WHERE u.user_id = e.user_id
       AND e.event_type IN (1, 2)
   ) t
   GROUP BY user_id
 ) q
 ORDER BY 2 DESC, 1
 LIMIT $1 OFFSET $2;

 EXECUTE parametrized_limit(1,1);

PREPARE parametrized_offset AS
SELECT user_id, array_length(events_table, 1)
FROM (
   SELECT user_id, array_agg(event ORDER BY time) AS events_table
   FROM (
     SELECT u.user_id, e.event_type::text AS event, e.time
     FROM users_table AS u,
          events_table AS e
     WHERE u.user_id = e.user_id
       AND e.event_type IN (1, 2)
   ) t
   GROUP BY user_id
 ) q
 ORDER BY 2 DESC, 1
 LIMIT 1 OFFSET $1;

 EXECUTE parametrized_offset(1);

SET client_min_messages TO DEFAULT;
DROP FUNCTION volatile_func_test();

CREATE FUNCTION test_join_function_2(integer, integer) RETURNS bool
    AS 'select $1 > $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

SELECT run_command_on_workers($f$

CREATE FUNCTION test_join_function_2(integer, integer) RETURNS bool
    AS 'select $1 > $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

$f$);

-- we don't support joins via functions 
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT u.user_id, e.event_type::text AS event, e.time
    FROM users_table AS u,
         events_table AS e
    WHERE test_join_function_2(u.user_id, e.user_id)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1;

-- note that the following query has both equi-joins on the partition keys
-- and non-equi-joins on other columns. We now support query filters
-- having non-equi-joins as long as they have equi-joins on partition keys.
SELECT
    users_table.user_id, users_table.value_1, prob
FROM
   users_table
        JOIN
   (SELECT
      ma.user_id, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob
    FROM
      users_table AS ma, events_table as short_list
    WHERE
      short_list.user_id = ma.user_id and ma.value_1 < 3 and short_list.event_type < 3
    ) temp
  ON users_table.user_id = temp.user_id
  WHERE 
    users_table.value_1 < 3 AND test_join_function_2(users_table.user_id, temp.user_id);

-- we do support the following since there is already an equality on the partition
-- key and we have an additional join via a function
SELECT
    temp.user_id, users_table.value_1, prob
FROM
   users_table
        JOIN
   (SELECT
      ma.user_id, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob, random()
    FROM
      users_table AS ma, events_table as short_list
    WHERE
      short_list.user_id = ma.user_id and ma.value_1 < 3 and short_list.event_type < 4 AND
       test_join_function_2(ma.value_1, short_list.value_2)
    ) temp
  ON users_table.user_id = temp.user_id
  WHERE 
    users_table.value_1 < 3
  ORDER BY 2 DESC, 1 DESC
  LIMIT 10;

-- similarly we do support non equi joins on columns if there is aready
-- an equality join
SELECT
  count(*)
FROM
  (SELECT 
    event_type, random() 
  FROM 
    events_table, users_table 
  WHERE 
    events_table.user_id = users_table.user_id AND 
    events_table.time > users_table.time AND 
    events_table.value_2 IN (0, 4)
  ) as foo;

-- the other way around is not supported
SELECT
  count(*)
FROM
  (SELECT 
    event_type, random() 
  FROM 
    events_table, users_table 
  WHERE 
    events_table.user_id > users_table.user_id AND 
    events_table.time = users_table.time AND 
    events_table.value_2 IN (0, 4)
  ) as foo;

-- we can even allow that on top level joins
SELECT
  count(*)
FROM
  (SELECT 
    event_type, random(), events_table.user_id 
  FROM 
    events_table, users_table 
  WHERE 
    events_table.user_id = users_table.user_id AND 
    events_table.value_2 IN (0, 4)
  ) as foo,
(SELECT 
    event_type, random(), events_table.user_id 
  FROM 
    events_table, users_table 
  WHERE 
    events_table.user_id = users_table.user_id AND 
    events_table.value_2 IN (1, 5)
  ) as bar 
WHERE foo.event_type > bar.event_type
AND foo.user_id = bar.user_id;

-- note that the following is not supported
-- since the top level join is not on the distribution key
SELECT
  count(*)
FROM
  (SELECT 
    event_type, random() 
  FROM 
    events_table, users_table 
  WHERE 
    events_table.user_id = users_table.user_id AND 
    events_table.value_2 IN (0, 4)
  ) as foo,
(SELECT 
    event_type, random() 
  FROM 
    events_table, users_table 
  WHERE 
    events_table.user_id = users_table.user_id AND 
    events_table.value_2 IN (1, 5)
  ) as bar 
WHERE foo.event_type = bar.event_type;

-- DISTINCT in the outer query and DISTINCT in the subquery
SELECT
    DISTINCT users_ids.user_id
FROM 
   (SELECT DISTINCT user_id FROM users_table) as users_ids
        JOIN 
   (SELECT  
      ma.user_id, ma.value_1, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob
    FROM 
      users_table AS ma, events_table as short_list
    WHERE 
      short_list.user_id = ma.user_id and ma.value_1 < 3 and short_list.event_type < 3
    ) temp 
  ON users_ids.user_id = temp.user_id 
  WHERE temp.value_1 < 3
  ORDER BY 1
  LIMIT 5;

-- DISTINCT ON in the outer query and DISTINCT in the subquery
SELECT
    DISTINCT ON (users_ids.user_id) users_ids.user_id, temp.value_1, prob
FROM 
   (SELECT DISTINCT user_id FROM users_table) as users_ids
        JOIN 
   (SELECT  
      ma.user_id, ma.value_1, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob
    FROM 
      users_table AS ma, events_table as short_list
    WHERE 
      short_list.user_id = ma.user_id and ma.value_1 < 3 and short_list.event_type < 2
    ) temp 
  ON users_ids.user_id = temp.user_id 
  WHERE temp.value_1 < 3
  ORDER BY 1, 2
  LIMIT 5;

-- DISTINCT ON in the outer query and DISTINCT ON in the subquery
SELECT
    DISTINCT ON (users_ids.user_id) users_ids.user_id, temp.value_1, prob
FROM 
   (SELECT DISTINCT ON (user_id) user_id, value_1 FROM users_table ORDER BY 1,2) as users_ids
        JOIN 
   (SELECT  
      ma.user_id, ma.value_1, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob
    FROM 
      users_table AS ma, events_table as short_list
    WHERE 
      short_list.user_id = ma.user_id and ma.value_1 < 2 and short_list.event_type < 3
    ) temp 
  ON users_ids.user_id = temp.user_id 
  ORDER BY 1,2
  LIMIT 5;


DROP FUNCTION test_join_function_2(integer, integer);

SELECT run_command_on_workers($f$

  DROP FUNCTION test_join_function_2(integer, integer);

$f$);

SET citus.enable_router_execution TO TRUE;
SET citus.subquery_pushdown to OFF;
