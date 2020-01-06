------------------------------------
------------------------------------
-- Vanilla funnel query -- single shard
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id, value_1_agg)
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT u.user_id, e.event_type::text AS event, e.time
    FROM users_table AS u,
         events_table AS e
    WHERE u.user_id = e.user_id
      AND u.user_id >= 1
      AND u.user_id <= 2
      AND e.event_type IN (2,3)
  ) t
  GROUP BY user_id
) q
WHERE user_id = 2;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
-- Vanilla funnel query -- two shards
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id, value_1_agg)
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT u.user_id, e.event_type::text AS event, e.time
    FROM users_table AS u,
         events_table AS e
    WHERE u.user_id = e.user_id AND
    (u.user_id = 1 OR u.user_id = 2) AND
    (e.user_id = 1 OR e.user_id = 2)
      AND e.event_type IN (1, 2)
  ) t
  GROUP BY user_id
) q
WHERE (user_id = 1 OR user_id = 2);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
--  Funnel grouped by whether or not a user has done an event -- single shard query
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id, value_1_agg, value_2_agg )
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
      AND u.user_id >= 1
      AND u.user_id <= 2
      AND e.event_type IN (1, 2)
    )
    UNION
    (
      SELECT u.user_id, 'step=>2'::text AS event, e.time
      FROM users_table AS u,
         events_table AS e
      WHERE  u.user_id = e.user_id
      AND u.user_id >= 1
      AND u.user_id <= 2
      AND e.event_type IN (3, 4)
    )
  ) t1 LEFT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e

      WHERE  e.user_id >= 1
      AND e.user_id <= 2
      AND e.event_type IN (5, 6)
  ) t2 ON (t1.user_id = t2.user_id)
  WHERE t1.user_id = 2
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event;


------------------------------------
------------------------------------
--  Funnel grouped by whether or not a user has done an event -- two shards query
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id, value_1_agg, value_2_agg )
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
      AND (e.user_id = 2 OR e.user_id = 3)
      AND e.event_type IN (1, 2)
    )
    UNION
    (
      SELECT u.user_id, 'step=>2'::text AS event, e.time
      FROM users_table AS u,
         events_table AS e
      WHERE  u.user_id = e.user_id
      AND (e.user_id = 2 OR e.user_id = 3)
      AND e.event_type IN (3, 4)
    )
  ) t1 LEFT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e

      WHERE
      (e.user_id = 2 OR e.user_id = 3)
      AND e.event_type IN (4, 5)
  ) t2 ON (t1.user_id = t2.user_id)
  WHERE (t1.user_id = 2 OR t1.user_id = 1)
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;


------------------------------------
------------------------------------
-- Most recently seen users_table events_table -- single shard query
------------------------------------
-- Note that we don't use ORDER BY/LIMIT yet
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id, agg_time, value_2_agg)
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
        user_id <= 5 AND
        users_table.value_1 > 1 AND users_table.value_1 < 4

        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table
          WHERE user_id = u.user_id AND
          events_table.event_type > 1 AND events_table.event_type < 4
        ) t ON true
        WHERE user_id = 5
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
-- Most recently seen users_table events_table -- two shards query
------------------------------------
-- Note that we don't use ORDER BY/LIMIT yet
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id, agg_time, value_2_agg)
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
        user_id <= 5 AND
        (user_id = 5 OR user_id = 1) AND
        users_table.value_1 > 1 AND users_table.value_1 < 4

        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table
          WHERE user_id = u.user_id AND (user_id = 5 OR user_id = 1) AND
          events_table.event_type > 1 AND events_table.event_type < 4
        ) t ON true
        WHERE (user_id = 5 OR user_id = 1)
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;


------------------------------------
------------------------------------
-- Count the number of distinct users_table who are in segment X and Y and Z -- single shard
------------------------------------
------------------------------------

TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id)
SELECT DISTINCT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 1 AND value_1 <= 2)
    AND user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 3 AND value_1 <= 4)
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= 5 AND value_1 <= 6)
    AND user_id = 1;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
-- Count the number of distinct users_table who are in segment X and Y and Z -- two shards
------------------------------------
------------------------------------

TRUNCATE agg_results_second;

INSERT INTO agg_results_second (user_id)
SELECT DISTINCT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 1 AND value_1 <= 2 AND (user_id = 1 OR user_id = 2))
    AND user_id IN (SELECT user_id FROM users_table WHERE value_1 >= 3 AND value_1 <= 4 AND (user_id = 1 OR user_id = 2))
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= 5 AND value_1 <= 6 AND (user_id = 1 OR user_id = 2))
    AND (user_id = 1 OR user_id = 2);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
-- Find customers who have done X, and satisfy other customer specific criteria -- single shard
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 1 AND value_1 < 4
  AND value_2 >= 1
  AND EXISTS (SELECT user_id FROM events_table WHERE event_type>1  AND event_type < 3 AND value_3 > 1 AND user_id=users_table.user_id)
  AND user_id = 2;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;


------------------------------------
------------------------------------
-- Find customers who have done X, and satisfy other customer specific criteria -- two shards
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 1 AND value_1 < 4
  AND value_2 >= 1
  AND EXISTS (SELECT user_id FROM events_table WHERE event_type>0  AND event_type < 2 AND value_3 > 1 AND (user_id = 2 OR user_id = 1) AND user_id=users_table.user_id)
  AND (user_id = 2 OR user_id = 1);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
-- Customers who have done X and haven’t done Y, and satisfy other customer specific criteria -- single shard
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_2 >= 2
  AND user_id = 1
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 1 AND event_type <= 3 AND value_3 > 1 AND user_id=users_table.user_id)
  AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 4 AND event_type <= 5 AND value_3 > 4 AND user_id=users_table.user_id);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
-- Customers who have done X and haven’t done Y, and satisfy other customer specific criteria -- two shards
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second(user_id, value_2_agg)
SELECT user_id, value_2 FROM users_table WHERE
  value_2 >= 2
  AND (user_id = 1 OR user_id = 2)
  AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 1 AND event_type <= 3 AND value_3 > 1 AND user_id=users_table.user_id AND (user_id = 1 OR user_id = 2))
  AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 4 AND event_type <= 5 AND value_3 > 4 AND user_id=users_table.user_id AND (user_id = 1 OR user_id = 2));

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;


------------------------------------
------------------------------------
-- Customers who have done X more than 2 times, and satisfy other customer specific criteria -- single shard
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second(user_id, value_2_agg)
  SELECT user_id,
         value_2
  FROM   users_table
  WHERE  value_1 > 1
         AND value_1 < 3
         AND value_2 >= 1
         AND user_id = 3
         AND EXISTS (SELECT user_id
                     FROM   events_table
                     WHERE  event_type > 1
                            AND event_type < 3
                            AND value_3 > 1
                            AND user_id = users_table.user_id
                            AND user_id = 3
                     GROUP  BY user_id
                     HAVING Count(*) > 2);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

------------------------------------
------------------------------------
-- Customers who have done X more than 2 times, and satisfy other customer specific criteria -- two shards
------------------------------------
------------------------------------
TRUNCATE agg_results_second;

INSERT INTO agg_results_second(user_id, value_2_agg)
  SELECT user_id,
         value_2
  FROM   users_table
  WHERE  value_1 > 1
         AND value_1 < 3
         AND value_2 >= 1
         AND (user_id = 3 or user_id = 4)
         AND EXISTS (SELECT user_id
                     FROM   events_table
                     WHERE  event_type = 2
                            AND value_3 > 1
                            AND user_id = users_table.user_id
                            AND (user_id = 3 or user_id = 4)
                     GROUP  BY user_id
                     HAVING Count(*) > 2);

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_second;

