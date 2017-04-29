------------------------------------
------------------------------------
-- Vanilla funnel query
------------------------------------
------------------------------------

-- not pushable since the JOIN is not an equi join
INSERT INTO agg_results_third (user_id, value_1_agg)
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT u.user_id, e.event_type::text AS event, e.time
    FROM users_table AS u,
         events_table AS e
    WHERE u.user_id != e.user_id
      AND u.user_id >= 10
      AND u.user_id <= 25
      AND e.event_type IN (100, 101, 102)
  ) t
  GROUP BY user_id
) q;

------------------------------------
------------------------------------
--  Funnel grouped by whether or not a user has done an event
------------------------------------
------------------------------------

-- not pushable since the JOIN is not an equi join left part of the UNION
-- is not equi join
INSERT INTO agg_results_third (user_id, value_1_agg, value_2_agg )
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
      WHERE  u.user_id != e.user_id
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

-- not pushable since the JOIN is not an equi join right part of the UNION
-- is not joined on the partition key
INSERT INTO agg_results_third (user_id, value_1_agg, value_2_agg )
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
      WHERE  u.user_id = e.event_type
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

-- the LEFT JOIN conditon is not on the partition column (i.e., is it part_key divided by 2)
INSERT INTO agg_results_third (user_id, value_1_agg, value_2_agg )
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

  ) t2 ON (t1.user_id = (t2.user_id)/2)
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event;

------------------------------------
------------------------------------
-- Funnel, grouped by the number of times a user has done an event
------------------------------------
------------------------------------

-- not pushable since the right of the UNION query is not joined on
-- the partition key
INSERT INTO agg_results_third (user_id, value_1_agg, value_2_agg)
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
      users_table.user_id != events_table.user_id AND
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

-- not pushable since the JOIN condition is not equi JOIN
-- (subquery_1 JOIN subquery_2)
INSERT INTO agg_results_third (user_id, value_1_agg, value_2_agg)
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
    subquery_1.user_id > subquery_2.user_id
  GROUP BY
    subquery_1.user_id,
    count_pay) AS subquery_top
WHERE
  array_ndims(events_table) > 0
GROUP BY
  count_pay, user_id
ORDER BY
  count_pay;

------------------------------------
------------------------------------
-- Most recently seen users_table events_table
------------------------------------
-- Note that we don't use ORDER BY/LIMIT yet
------------------------------------
------------------------------------
-- not pushable since lateral join is not an equi join
INSERT INTO agg_results_third (user_id, agg_time, value_2_agg)
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
          WHERE user_id != u.user_id AND
          events_table.event_type > 10 AND events_table.event_type < 12
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC;

-- not pushable since lateral join is not on the partition key
INSERT INTO agg_results_third (user_id, agg_time, value_2_agg)
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
          WHERE event_type = u.user_id AND
          events_table.event_type > 10 AND events_table.event_type < 12
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC;

-- not pushable since lateral join is not on the partition key
INSERT INTO agg_results_third (user_id, agg_time, value_2_agg)
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
        
        SELECT user_id, time, value_3 as val_3
        FROM users_table
        WHERE
        user_id >= 10 AND
        user_id <= 70 AND
        users_table.value_1 > 10 AND users_table.value_1 < 12

        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table
          WHERE event_type = u.val_3 AND
          events_table.event_type > 10 AND events_table.event_type < 12
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC;

------------------------------------
------------------------------------
-- Count the number of distinct users_table who are in segment X and Y and Z
------------------------------------
------------------------------------

-- not pushable since partition key is NOT IN
INSERT INTO agg_results_third (user_id)
SELECT DISTINCT user_id
FROM users_table
WHERE user_id NOT IN (SELECT user_id FROM users_table WHERE value_1 >= 10 AND value_1 <= 20)
    AND user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 30 AND value_1 <= 40)
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= 50 AND value_1 <= 60);

-- not pushable since partition key is not selected from the second subquery
INSERT INTO agg_results_third (user_id)
SELECT DISTINCT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 10 AND value_1 <= 20)
    AND user_id IN (SELECT value_1 FROM users_table WHERE value_1 >= 30 AND value_1 <= 40)
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= 50 AND value_1 <= 60);

-- not pushable since second subquery does not return bare partition key
INSERT INTO agg_results_third (user_id)
SELECT DISTINCT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 10 AND value_1 <= 20)
    AND user_id IN (SELECT 3 * user_id FROM users_table WHERE value_1 >= 30 AND value_1 <= 40)
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= 50 AND value_1 <= 60);

------------------------------------
------------------------------------
-- Find customers who have done X, and satisfy other customer specific criteria
------------------------------------
------------------------------------

-- not pushable since join is not an euqi join
INSERT INTO agg_results_third(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 101 AND value_1 < 110
  AND value_2 >= 5
  AND EXISTS (SELECT user_id FROM events_table WHERE event_type>101  AND event_type < 110 AND value_3 > 100 AND user_id!=users_table.user_id);

-- not pushable since the join is not on the partition key
INSERT INTO agg_results_third(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 101 AND value_1 < 110
  AND value_2 >= 5
  AND EXISTS (SELECT user_id FROM events_table WHERE event_type>101  AND event_type < 110 AND value_3 > 100 AND event_type = users_table.user_id);

------------------------------------
------------------------------------
-- Customers who haven’t done X, and satisfy other customer specific criteria
------------------------------------
------------------------------------
-- not pushable since the join is not an equi join
INSERT INTO agg_results_third(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 = 101
  AND value_2 >= 5
  AND NOT EXISTS (SELECT user_id FROM events_table WHERE event_type=101 AND value_3 > 100 AND user_id!=users_table.user_id);

-- not pushable since the join is not the partition key
INSERT INTO agg_results_third(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 = 101
  AND value_2 >= 5
  AND NOT EXISTS (SELECT user_id FROM events_table WHERE event_type=101 AND value_3 > 100 AND event_type=users_table.user_id);

------------------------------------
------------------------------------
-- Customers who have done X and Y, and satisfy other customer specific criteria
------------------------------------
------------------------------------
-- not pushable since the second join is not on the partition key
INSERT INTO agg_results_third(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 100
  AND value_2 >= 5
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type!=100 AND value_3 > 100 AND user_id=users_table.user_id)
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type=101 AND value_3 > 100 AND user_id!=users_table.user_id);

------------------------------------
------------------------------------
-- Customers who have done X and haven’t done Y, and satisfy other customer specific criteria
------------------------------------
------------------------------------

-- not pushable since the first join is not on the partition key
INSERT INTO agg_results_third(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_2 >= 5
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 100 AND event_type <= 300 AND value_3 > 100 AND user_id!=users_table.user_id)
  AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 300 AND event_type <= 350  AND value_3 > 100 AND user_id=users_table.user_id);
  
------------------------------------
------------------------------------
-- Customers who have done X more than 2 times, and satisfy other customer specific criteria
------------------------------------
------------------------------------

-- not pushable since the second join is not an equi join
INSERT INTO agg_results_third(user_id, value_2_agg)
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
                            AND user_id != users_table.user_id 
                     GROUP  BY user_id 
                     HAVING Count(*) > 2);

-- not pushable since the second join is not on the partition key
INSERT INTO agg_results_third(user_id, value_2_agg)
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
                            AND event_type = users_table.user_id 
                     GROUP  BY user_id 
                     HAVING Count(*) > 2);

-- not pushable since the second join is not on the partition key
INSERT INTO agg_results_third(user_id, value_2_agg)
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
                            AND user_id = users_table.value_1 
                     GROUP  BY user_id 
                     HAVING Count(*) > 2);

------------------------------------
------------------------------------
-- Find me all users_table who has done some event and has filters
------------------------------------
------------------------------------

-- not pushable due to NOT IN
INSERT INTO agg_results_third(user_id)
Select user_id
From events_table
Where event_type = 16
And value_2 > 50
And user_id NOT in
  (select user_id
   From users_table
   Where value_1 = 15
   And value_2 > 25); 

-- not pushable since we're not selecting the partition key
INSERT INTO agg_results_third(user_id)
Select user_id
From events_table
Where event_type = 16
And value_2 > 50
And user_id  in
  (select value_3
   From users_table
   Where value_1 = 15
   And value_2 > 25);  
 
 -- not pushable since we're not selecting the partition key
 -- from the events table
INSERT INTO agg_results_third(user_id)
Select user_id
From events_table
Where event_type = 16
And value_2 > 50
And event_type  in
  (select user_id
   From users_table
   Where value_1 = 15
   And value_2 > 25);  

------------------------------------
------------------------------------
-- Which events_table did people who has done some specific events_table
------------------------------------
------------------------------------

-- not pushable due to NOT IN
INSERT INTO agg_results_third(user_id, value_1_agg)
SELECT user_id, event_type FROM events_table
WHERE user_id NOT IN (SELECT user_id from events_table WHERE event_type > 500 and event_type < 505)
GROUP BY user_id, event_type;

-- not pushable due to not selecting the partition key
INSERT INTO agg_results_third(user_id, value_1_agg)
SELECT user_id, event_type FROM events_table
WHERE user_id IN (SELECT value_2 from events_table WHERE event_type > 500 and event_type < 505)
GROUP BY user_id, event_type;

-- not pushable due to not comparing user id from the events table
INSERT INTO agg_results_third(user_id, value_1_agg)
SELECT user_id, event_type FROM events_table
WHERE event_type IN (SELECT user_id from events_table WHERE event_type > 500 and event_type < 505)
GROUP BY user_id, event_type;

------------------------------------
------------------------------------
-- Find my assets that have the highest probability and fetch their metadata
------------------------------------
------------------------------------

-- not pushable since the join is not an equi join
INSERT INTO agg_results_third(user_id, value_1_agg, value_3_agg)
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
      short_list.user_id != ma.user_id and ma.value_1 < 50 and short_list.event_type < 50
    ) temp 
  ON users_table.user_id = temp.user_id 
  WHERE users_table.value_1 < 50;

-- not pushable since the join is not on the partition key
INSERT INTO agg_results_third(user_id, value_1_agg, value_3_agg)
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
      short_list.user_id = ma.value_2 and ma.value_1 < 50 and short_list.event_type < 50
    ) temp 
  ON users_table.user_id = temp.user_id 
  WHERE users_table.value_1 < 50;

-- not supported since one of the queries doesn't have a relation
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
        SELECT user_id, time, value_3 as val_3
        FROM users_table
        WHERE
        user_id >= 10 AND user_id <= 70 AND
        users_table.value_1 > 10 AND users_table.value_1 < 12
        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table, (SELECT 1 as x) as f
          WHERE user_id = u.user_id AND
          events_table.event_type > 10 AND events_table.event_type < 12
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC;
