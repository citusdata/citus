-- ===================================================================
-- test insert select functionality for window functions
-- ===================================================================

INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   user_id, time, rnk
FROM
(
  SELECT
    *, rank() OVER my_win as rnk
  FROM
    events_table
    WINDOW my_win AS (PARTITION BY user_id ORDER BY time DESC)
) as foo;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- the same test with different syntax
INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   user_id, time, rnk
FROM
(
  SELECT
    *, rank() OVER (PARTITION BY user_id ORDER BY time DESC) as rnk
  FROM
    events_table
) as foo;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- similar test with lag
INSERT INTO agg_results_window (user_id, agg_time, value_2_agg, value_3_agg)
SELECT
   user_id, time, lag_event_type, row_no
FROM
(
  SELECT
    *, lag(event_type) OVER my_win  as lag_event_type, row_number() OVER my_win as row_no
  FROM
    events_table WINDOW my_win AS (PARTITION BY user_id ORDER BY time DESC)
) as foo;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- simple window function, partitioned and grouped by on the distribution key
INSERT INTO agg_results_window (user_id, value_1_agg, value_2_agg)
SELECT
   user_id, rnk, tme
FROM
(
  SELECT
    user_id, rank() OVER my_win as rnk, avg(value_2) as tme
  FROM
    events_table
  GROUP BY
    user_id,  date_trunc('day', time)
  WINDOW my_win AS (PARTITION BY user_id ORDER BY avg(event_type) DESC)
) as foo;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- top level query has a group by on the result of the window function
INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   min(user_id), min(time), lag_event_type
FROM
(
  SELECT
    *, lag(event_type) OVER my_win  as lag_event_type
  FROM
    events_table WINDOW my_win AS (PARTITION BY user_id ORDER BY time DESC)
) as foo
GROUP BY
  lag_event_type;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- window functions should work along with joins as well
INSERT INTO agg_results_window (user_id, value_1_agg, value_2_agg)
SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w1
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 2
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time)
) as foo;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- two window functions in a single subquery should work fine as well
INSERT INTO agg_results_window (user_id, value_1_agg, value_2_agg)
SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 2
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time),
  w2 AS (PARTITION BY users_table.user_id, (events_table.value_2 % 25) ORDER BY events_table.time)
) as foo;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- window functions should be fine within subquery joins
INSERT INTO agg_results_window (user_id, value_1_agg, value_2_agg, value_3_agg)
SELECT sub_1.user_id, max(lag_1), max(rank_1), max(rank_2) FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1 as lag_1, rank() OVER w2 as rank_1
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 2
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time),
  w2 AS (PARTITION BY users_table.user_id, (events_table.value_2 % 25) ORDER BY events_table.time)
) as sub_1
JOIN
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1 as lag_2, rank() OVER w2 as rank_2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 2
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.value_2 ORDER BY events_table.time),
  w2 AS (PARTITION BY users_table.user_id, (events_table.value_2 % 50) ORDER BY events_table.time)
) as sub_2
 ON(sub_1.user_id = sub_2.user_id)
 GROUP BY
  sub_1.user_id;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- GROUP BYs and PARTITION BYs should work fine together
INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   avg(user_id), max(time), my_rank
FROM
(
  SELECT
    user_id, date_trunc('day', time) as time, rank() OVER my_win as my_rank
  FROM
    events_table
  GROUP BY
    user_id, date_trunc('day', time)
    WINDOW my_win AS (PARTITION BY user_id ORDER BY count(*) DESC)
) as foo
WHERE
  my_rank > 1
GROUP BY
  my_rank;

 -- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- aggregates in the PARTITION BY is also allows
INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   avg(user_id), max(time), my_rank
FROM
(
  SELECT
    user_id, date_trunc('day', time) as time, rank() OVER my_win as my_rank
  FROM
    events_table
  GROUP BY
    user_id, date_trunc('day', time)
    WINDOW my_win AS (PARTITION BY user_id, avg(event_type%10)::int ORDER BY count(*) DESC)
) as foo
WHERE
  my_rank > 0
GROUP BY
  my_rank;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- GROUP BY should not necessarly be inclusive of partitioning
-- but this query doesn't make much sense
INSERT INTO agg_results_window (user_id, value_1_agg)
SELECT
   avg(user_id), my_rank
FROM
(
  SELECT
    user_id, rank() OVER my_win as my_rank
  FROM
    events_table
  GROUP BY
    user_id
    WINDOW my_win AS (PARTITION BY user_id, max(event_type) ORDER BY count(*) DESC)
) as foo
GROUP BY
  my_rank;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- Group by has more columns than partition by which uses coordinator insert ... select
INSERT INTO agg_results_window(user_id, value_2_agg)
SELECT * FROM (
  SELECT
    DISTINCT user_id, SUM(value_2) OVER (PARTITION BY user_id)
  FROM
    users_table
  GROUP BY
    user_id, value_1, value_2
) a
ORDER BY
  2 DESC, 1
LIMIT
  10;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

INSERT INTO agg_results_window(user_id, value_2_agg)
SELECT user_id, max(sum) FROM (
  SELECT
    user_id, SUM(value_2) OVER (PARTITION BY user_id, value_1)
  FROM
    users_table
  GROUP BY
    user_id, value_1, value_2
) a
GROUP BY user_id;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

  -- Subquery in where with window function
INSERT INTO agg_results_window(user_id)
SELECT
  user_id
FROM
  users_table
WHERE
  value_2 > 1 AND
  value_2 < ALL (
    SELECT
      avg(value_3) OVER (PARTITION BY user_id)
    FROM
      events_table
    WHERE
      users_table.user_id = events_table.user_id
  )
GROUP BY
  user_id;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- Partition by with aggregate functions. This query does not make much sense since the
-- result of aggregate function will be the same for every row in a partition and it is
-- not going to affect the group that the count function will work on.
INSERT INTO agg_results_window(user_id, value_2_agg)
SELECT * FROM (
  SELECT
    user_id, COUNT(*) OVER (PARTITION BY user_id, MIN(value_2))
  FROM
    users_table
  GROUP BY
  1
) a;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- Some more nested queries
INSERT INTO agg_results_window(user_id, value_2_agg, value_3_agg, value_4_agg)
SELECT
  user_id, rank, SUM(ABS(value_2 - value_3)) AS difference, COUNT(*) AS distinct_users
FROM (
  SELECT
    *, rank() OVER (PARTITION BY user_id ORDER BY value_2 DESC)
  FROM (
    SELECT
      user_id, value_2, sum(value_3) OVER (PARTITION BY user_id, value_2) as value_3
    FROM users_table
  ) AS A
) AS A
GROUP BY
  user_id, rank;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT * FROM (
  SELECT DISTINCT
    f3.user_id, ABS(f2.sum - f3.sum)
  FROM (
    SELECT DISTINCT
      user_id, sum(value_3) OVER (PARTITION BY user_id)
    FROM
      users_table
    GROUP BY
      user_id, value_3
  ) f3,
  (
  SELECT DISTINCT
    user_id, sum(value_2) OVER (PARTITION BY user_id)
  FROM
    users_table
  GROUP BY
    user_id, value_2
  ) f2
WHERE
  f3.user_id=f2.user_id
) a;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- test with reference table partitioned on columns from both
INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT *
FROM
(
    SELECT
      DISTINCT user_id, count(id) OVER (PARTITION BY user_id, id)
    FROM
      users_table, users_ref_test_table
) a;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- Window functions with HAVING clause
INSERT INTO agg_results_window (user_id, value_1_agg)
SELECT * FROM (
  SELECT
    DISTINCT user_id, rank() OVER (PARTITION BY user_id ORDER BY value_1)
  FROM
    users_table
  GROUP BY
    user_id, value_1 HAVING count(*) > 1
) a;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- Window functions with HAVING clause which uses coordinator insert ... select
INSERT INTO agg_results_window (user_id, value_1_agg)
SELECT * FROM (
  SELECT
    DISTINCT user_id, rank() OVER (PARTITION BY user_id ORDER BY value_1)
  FROM
    users_table
  GROUP BY
    user_id, value_1 HAVING count(*) > 1
) a
ORDER BY
  2 DESC, 1
LIMIT
  10;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- Window function in View works
CREATE VIEW view_with_window_func AS
SELECT
  DISTINCT user_id, rank() OVER (PARTITION BY user_id ORDER BY value_1)
FROM
  users_table
GROUP BY
  user_id, value_1
HAVING count(*) > 1;

INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT *
FROM
  view_with_window_func;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- Window function in View works and the query uses coordinator insert ... select
INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT *
FROM
  view_with_window_func
LIMIT
  10;

-- get some statistics from the aggregated results to ensure the results are correct
-- since there is a limit but not order, we cannot run avg(user_id)
SELECT count(*) FROM agg_results_window;
TRUNCATE agg_results_window;

INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT
  user_id, max(avg)
FROM
(
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (1, 2, 3, 4, 5))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (6, 7, 8, 9, 10))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (11, 12, 13, 14, 15))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (16, 17, 18, 19, 20))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (21, 22, 23, 24, 25))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (26, 27, 28, 29, 30))
) b
GROUP BY
  user_id
LIMIT
  5;

-- get some statistics from the aggregated results to ensure the results are correct
-- since there is a limit but not order, we cannot test avg or distinct count
SELECT count(*) FROM agg_results_window;
TRUNCATE agg_results_window;

INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT
  user_id, max(avg)
FROM
(
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (1, 2, 3, 4, 5))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (6, 7, 8, 9, 10))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (11, 12, 13, 14, 15))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (16, 17, 18, 19, 20))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (21, 22, 23, 24, 25))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (26, 27, 28, 29, 30))
) b
GROUP BY
  user_id;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT *
FROM (
        ( SELECT user_id,
                 sum(counter)
         FROM
           (SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              users_table
          UNION
            SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              events_table) user_id_1
         GROUP BY
          user_id)
      UNION
        (SELECT
            user_id, sum(counter)
         FROM
           (SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              users_table
          UNION
            SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              events_table) user_id_2
         GROUP BY
            user_id
        )
) AS ftop
LIMIT
  5;

-- get some statistics from the aggregated results to ensure the results are correct
-- since there is a limit but not order, we cannot test avg or distinct count
SELECT count(*) FROM agg_results_window;
TRUNCATE agg_results_window;

INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT *
FROM (
        ( SELECT user_id,
                 sum(counter)
         FROM
           (SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              users_table
          UNION
            SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              events_table) user_id_1
         GROUP BY
          user_id)
      UNION
        (SELECT
            user_id, sum(counter)
         FROM
           (SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              users_table
          UNION
            SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              events_table) user_id_2
         GROUP BY
            user_id
        )
) AS ftop;

-- get some statistics from the aggregated results to ensure the results are correct
SELECT count(*), count(DISTINCT user_id), avg(user_id) FROM agg_results_window;
TRUNCATE agg_results_window;

-- test queries where the window function isn't pushed down
INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
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
  10;

INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   user_id, time, rnk
FROM
(
  SELECT
    *, rank() OVER my_win as rnk
  FROM
    events_table
    WINDOW my_win AS ()
) as foo
ORDER BY
  3 DESC, 1 DESC, 2 DESC
LIMIT
  10;

INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   user_id, time, rnk
FROM
(
  SELECT
    *, rank() OVER my_win as rnk
  FROM
    events_table
    WINDOW my_win AS (ORDER BY time DESC)
) as foo
ORDER BY
  3 DESC, 1 DESC, 2 DESC
LIMIT
  10;

INSERT INTO agg_results_window (user_id, value_1_agg, value_2_agg)
SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 25
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time),
  w2 AS (PARTITION BY users_table.user_id+1, (events_table.value_2 % 25) ORDER BY events_table.time)
) as foo
LIMIT
  10;

INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   user_id, time, my_rank
FROM
(
  SELECT
    user_id, date_trunc('day', time) as time, rank() OVER my_win as my_rank
  FROM
    events_table
  GROUP BY
    user_id, date_trunc('day', time)
    WINDOW my_win AS (ORDER BY avg(event_type))
) as foo
WHERE
  my_rank > 125;

INSERT INTO agg_results_window (user_id, agg_time, value_2_agg)
SELECT
   user_id, time, my_rank
FROM
(
  SELECT
    user_id, date_trunc('day', time) as time, rank() OVER my_win as my_rank
  FROM
    events_table
  GROUP BY
    user_id, date_trunc('day', time)
    WINDOW my_win AS (PARTITION BY date_trunc('day', time) ORDER BY avg(event_type))
) as foo
WHERE
  my_rank > 125;

INSERT INTO agg_results_window (user_id, value_2_agg, value_3_agg)
SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 25
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time),
  w2 AS (ORDER BY events_table.time)
) as foo;

INSERT INTO agg_results_window(user_id, agg_time, value_2_agg)
SELECT * FROM (
  SELECT
    user_id, date_trunc('day', time) as time, sum(rank) OVER w2
  FROM (
    SELECT DISTINCT
        user_id as user_id, time, rank() over w1
      FROM
        users_table
      WINDOW
        w AS (PARTITION BY time), w1 AS (w ORDER BY value_2, value_3)
    ) fab
    WINDOW
      w2 as (PARTITION BY user_id, time)
) a;

INSERT INTO agg_results_window(user_id)
SELECT
  user_id
FROM
  users_table
WHERE
  value_2 > 2 AND
  value_2 < ALL (
    SELECT
      avg(value_3) OVER ()
    FROM
      events_table
    WHERE
      users_table.user_id = events_table.user_id
  )
GROUP BY
  user_id;

INSERT INTO agg_results_window(user_id, value_2_agg)
SELECT * FROM (
  SELECT
    user_id, COUNT(*) OVER (PARTITION BY sum(user_id), MIN(value_2))
  FROM
    users_table
  GROUP BY
    user_id
) a;

INSERT INTO agg_results_window(user_id, value_1_agg)
SELECT *
FROM (
        ( SELECT user_id,
                 sum(counter)
         FROM
           (SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              users_table
          UNION
            SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              events_table) user_id_1
         GROUP BY
          user_id)
      UNION
        (SELECT
            user_id, sum(counter)
         FROM
           (SELECT
              user_id, sum(value_2) over (partition by user_id) AS counter
            FROM
              users_table
          UNION
            SELECT
              user_id, sum(value_2) over (partition by event_type) AS counter
            FROM
              events_table) user_id_2
         GROUP BY
            user_id
        )
) AS ftop;

TRUNCATE agg_results_window;

DROP VIEW view_with_window_func;
