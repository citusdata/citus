------------------------------------
------------------------------------
-- Vanilla funnel query
------------------------------------
------------------------------------
INSERT INTO agg_results (user_id, value_1_agg)
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT u.user_id, e.event_type::text AS event, e.time
    FROM users_table AS u,
         events_table AS e
    WHERE u.user_id = e.user_id
      AND u.user_id >= 10
      AND u.user_id <= 25
      AND e.event_type IN (100, 101, 102)
  ) t
  GROUP BY user_id
) q;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;


------------------------------------
------------------------------------
--  Funnel grouped by whether or not a user has done an event
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results (user_id, value_1_agg, value_2_agg )
SELECT user_id, sum(array_length(events_table, 1)), length(hasdone_event)
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
      AND u.user_id >= 10
      AND u.user_id <= 25
      AND e.event_type IN (100, 101, 102)
    )
    UNION
    (
      SELECT u.user_id, 'step=>2'::text AS event, e.time
      FROM users_table AS u,
         events_table AS e
      WHERE  u.user_id = e.user_id
      AND u.user_id >= 10
      AND u.user_id <= 25
      AND e.event_type IN (103, 104, 105)
    )
  ) t1 LEFT JOIN (
      SELECT DISTINCT user_id, 
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e
      
      WHERE  e.user_id >= 10
      AND e.user_id <= 25
      AND e.event_type IN (106, 107, 108)

  ) t2 ON (t1.user_id = t2.user_id)
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event;

-- get some statistics from the aggregated results to ensure the results are correct
-- SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Funnel, grouped by the number of times a user has done an event
------------------------------------
------------------------------------

TRUNCATE agg_results;

INSERT INTO agg_results (user_id, value_1_agg, value_2_agg)
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
WHERE
  array_ndims(events_table) > 0
GROUP BY
  count_pay, user_id
ORDER BY
  count_pay;

-- get some statistics from the aggregated results to ensure the results are correct
-- SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Most recently seen users_table events_table
------------------------------------
-- Note that we don't use ORDER BY/LIMIT yet
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results (user_id, agg_time, value_2_agg)
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
        user_id >= 10 AND
        user_id <= 70 AND
        users_table.value_1 > 10 AND users_table.value_1 < 12

        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table
          WHERE user_id = u.user_id AND
          events_table.event_type > 10 AND events_table.event_type < 12
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Count the number of distinct users_table who are in segment X and Y and Z
------------------------------------
------------------------------------

TRUNCATE agg_results;

INSERT INTO agg_results (user_id)
SELECT DISTINCT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 10 AND value_1 <= 20)
    AND user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 30 AND value_1 <= 40)
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= 50 AND value_1 <= 60);
    
-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;    

------------------------------------
------------------------------------
-- Count the number of distinct users_table who are in at least two of X and Y and Z segments
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id)
SELECT user_id
FROM users_table
WHERE (value_1 = 10
       OR value_1 = 11
       OR value_1 = 12)
GROUP BY user_id
HAVING count(distinct value_1) >= 2;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Find customers who have done X, and satisfy other customer specific criteria
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 101 AND value_1 < 110
  AND value_2 >= 5
  AND EXISTS (SELECT user_id FROM events_table WHERE event_type>101  AND event_type < 110 AND value_3 > 100 AND user_id=users_table.user_id);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Customers who haven’t done X, and satisfy other customer specific criteria
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 = 101
  AND value_2 >= 5
  AND NOT EXISTS (SELECT user_id FROM events_table WHERE event_type=101 AND value_3 > 100 AND user_id=users_table.user_id);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Customers who have done X and Y, and satisfy other customer specific criteria
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 100
  AND value_2 >= 5
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type!=100 AND value_3 > 100 AND user_id=users_table.user_id)
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type=101 AND value_3 > 100 AND user_id=users_table.user_id);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Customers who have done X and haven’t done Y, and satisfy other customer specific criteria
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_2 >= 5
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 100 AND event_type <= 300 AND value_3 > 100 AND user_id=users_table.user_id)
  AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 300 AND event_type <= 350  AND value_3 > 100 AND user_id=users_table.user_id);
  
-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Customers who have done X more than 2 times, and satisfy other customer specific criteria
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_2_agg)
  SELECT user_id, 
         value_2 
  FROM   users_table
  WHERE  value_1 > 100 
         AND value_1 < 124 
         AND value_2 >= 5 
         AND EXISTS (SELECT user_id 
                     FROM   events_table
                     WHERE  event_type > 100 
                            AND event_type < 124 
                            AND value_3 > 100 
                            AND user_id = users_table.user_id 
                     GROUP  BY user_id 
                     HAVING Count(*) > 2);
                     
-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;
                                  
------------------------------------
------------------------------------
-- Find me all users_table who logged in more than once
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_1_agg)
SELECT user_id, value_1 from
(
  SELECT user_id, value_1 From users_table
  WHERE value_2 > 100 and user_id = 15 GROUP BY value_1, user_id HAVING count(*) > 1
) as a;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Find me all users_table who has done some event and has filters
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id)
Select user_id
From events_table
Where event_type = 16
And value_2 > 50
And user_id in
  (select user_id
   From users_table
   Where value_1 = 15
   And value_2 > 25);  
 
-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;

------------------------------------
------------------------------------
-- Which events_table did people who has done some specific events_table
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_1_agg)
SELECT user_id, event_type FROM events_table
WHERE user_id in (SELECT user_id from events_table WHERE event_type > 500 and event_type < 505)
GROUP BY user_id, event_type;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;
       
------------------------------------
------------------------------------
-- Find me all the users_table who has done some event more than three times
------------------------------------
------------------------------------    
TRUNCATE agg_results;

INSERT INTO agg_results(user_id)
select user_id from
(
  select 
     user_id
   from 
   	events_table
where event_type = 901 group by user_id having count(*) > 3
) as a;
   
-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;
       
------------------------------------
------------------------------------
-- Find my assets that have the highest probability and fetch their metadata
------------------------------------
------------------------------------
TRUNCATE agg_results;

INSERT INTO agg_results(user_id, value_1_agg, value_3_agg)
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
    	short_list.user_id = ma.user_id and ma.value_1 < 50 and short_list.event_type < 50
    ) temp 
  ON users_table.user_id = temp.user_id 
  WHERE users_table.value_1 < 50;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results;
   