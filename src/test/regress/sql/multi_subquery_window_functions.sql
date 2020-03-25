-- ===================================================================
-- test multi subquery functionality for window functions
-- ===================================================================

CREATE VIEW subq AS
SELECT
  DISTINCT user_id, rank() OVER (PARTITION BY user_id ORDER BY value_1)
FROM
  users_table
GROUP BY
  user_id, value_1
HAVING count(*) > 1;

SELECT
   user_id, time, rnk
FROM
(
  SELECT
    *, rank() OVER my_win as rnk
  FROM
    events_table
    WINDOW my_win AS (PARTITION BY user_id ORDER BY time DESC)
) as foo
ORDER BY
  3 DESC, 1 DESC, 2 DESC
LIMIT
  10;

-- the same test with different syntax
SELECT
   user_id, time, rnk
FROM
(
  SELECT
    *, rank() OVER (PARTITION BY user_id ORDER BY time DESC) as rnk
  FROM
    events_table
) as foo
ORDER BY
  3 DESC, 1 DESC, 2 DESC
LIMIT
  10;

-- similar test with lag
SELECT
   user_id, time, lag_event_type, row_no
FROM
(
  SELECT
    *, lag(event_type) OVER my_win  as lag_event_type, row_number() OVER my_win as row_no
  FROM
    events_table WINDOW my_win AS (PARTITION BY user_id ORDER BY time DESC)
) as foo
ORDER BY
  4 DESC, 3 DESC NULLS LAST, 1 DESC, 2 DESC
LIMIT
  10;

-- simple window function, partitioned and grouped by on the distribution key
SELECT
   user_id, rnk, avg_val_2
FROM
(
  SELECT
    user_id, rank() OVER my_win as rnk, avg(value_2) as avg_val_2
  FROM
    events_table
  GROUP BY
    user_id,  date_trunc('day', time)
  WINDOW my_win AS (PARTITION BY user_id ORDER BY avg(event_type) DESC)
) as foo
ORDER BY
  2 DESC, 1 DESC, 3 DESC
LIMIT
  10;

-- top level query has a group by on the result of the window function
SELECT
   min(user_id), min(time), lag_event_type, count(*)
FROM
(
  SELECT
    *, lag(event_type) OVER my_win  as lag_event_type
  FROM
    events_table WINDOW my_win AS (PARTITION BY user_id ORDER BY time DESC)
) as foo
GROUP BY
  lag_event_type
ORDER BY
  3 DESC NULLS LAST, 1 DESC, 2 DESC
LIMIT
  10;

-- window functions should work along with joins as well
SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w1
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 4
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time)
) as foo
ORDER BY 3 DESC, 1 DESC, 2 DESC NULLS LAST
LIMIT 10;

-- two window functions in a single subquery should work fine as well
SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 4
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time),
  w2 AS (PARTITION BY users_table.user_id, (events_table.value_2 % 25) ORDER BY events_table.time)
) as foo
ORDER BY 3 DESC, 1 DESC, 2 DESC NULLS LAST
LIMIT 10;

-- window functions should be fine within subquery joins
SELECT sub_1.user_id, max(lag_1), max(rank_1), max(rank_2) FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1 as lag_1, rank() OVER w2 as rank_1
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 4
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
    event_type < 4
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.value_2 ORDER BY events_table.time),
  w2 AS (PARTITION BY users_table.user_id, (events_table.value_2 % 50) ORDER BY events_table.time)
) as sub_2
 ON(sub_1.user_id = sub_2.user_id)
 GROUP BY
  sub_1.user_id
 ORDER BY 3 DESC, 4 DESC, 1 DESC, 2 DESC NULLS LAST
LIMIT 10;

-- GROUP BYs and PARTITION BYs should work fine together
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
  my_rank
ORDER BY
  3 DESC, 1 DESC,2 DESC
LIMIT
  10;

-- aggregates in the PARTITION BY is also allows
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
    WINDOW my_win AS (PARTITION BY user_id, avg(event_type%3)::int ORDER BY count(*) DESC)
) as foo
WHERE
  my_rank > 0
GROUP BY
  my_rank
ORDER BY
  3 DESC, 1 DESC,2 DESC
LIMIT
  10;

-- GROUP BY should not necessarly be inclusive of partitioning
-- but this query doesn't make much sense
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
  my_rank
ORDER BY
  2 DESC, 1 DESC
LIMIT
  10;

  -- Using previously defined supported window function on distribution key
SELECT * FROM (
  SELECT
    user_id, date_trunc('day', time) as time, sum(rank) OVER w2
  FROM (
    SELECT DISTINCT
      user_id as user_id, time, rank() over w1
    FROM users_table
    WINDOW
      w AS (PARTITION BY user_id),
      w1 AS (w ORDER BY value_2, value_3)
  ) fab
  WINDOW
    w2 as (PARTITION BY user_id, time)
) a
ORDER BY
  1, 2, 3 DESC
LIMIT
  10;

-- test with reference table partitioned on columns from both
SELECT *
FROM
(
    SELECT
      DISTINCT user_id, it_name, count(id) OVER (PARTITION BY user_id, id)
    FROM
      users_table, users_ref_test_table
    WHERE users_table.value_2 + 40 = users_ref_test_table.k_no
) a
ORDER BY
  1, 2, 3
LIMIT
  20;

-- Group by has more columns than partition by
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

SELECT user_id, max(sum) FROM (
  SELECT
    user_id, SUM(value_2) OVER (PARTITION BY user_id, value_1)
  FROM
    users_table
  GROUP BY
    user_id, value_1, value_2
) a
GROUP BY user_id ORDER BY
  2 DESC,1
LIMIT
  10;

-- Window functions with HAVING clause
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

-- Window function in View works
SELECT *
FROM
  subq
ORDER BY
  2 DESC, 1
LIMIT
  10;

-- Window functions with UNION/UNION ALL works
SELECT
  max(avg)
FROM
(
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (1, 2))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (2, 3))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (3, 4))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (4, 5))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (5, 6))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (1, 6))
) b
GROUP BY user_id
ORDER BY 1 DESC
LIMIT 5;

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
            user_id)) AS ftop
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

-- Subquery in where with window function
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
  user_id
ORDER BY
  user_id DESC
LIMIT
  3;

-- Some more nested queries
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
  user_id, rank
ORDER BY
  difference DESC, rank DESC, user_id
LIMIT 20;

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
) a
ORDER BY
  abs DESC, user_id
LIMIT 10;


-- Partition by with aggregate functions. This query does not make much sense since the
-- result of aggregate function will be the same for every row in a partition and it is
-- not going to affect the group that the count function will work on.
SELECT * FROM (
  SELECT
    user_id, COUNT(*) OVER (PARTITION BY user_id, MIN(value_2))
  FROM
    users_table
  GROUP BY
  1
) a
ORDER BY
  1 DESC
LIMIT
  5;

EXPLAIN (COSTS FALSE, VERBOSE TRUE)
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
                user_id)) AS ftop
    ORDER BY 2 DESC, 1 DESC
    LIMIT 5;

-- test with window functions which aren't pushed down
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

SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 4
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time),
  w2 AS (PARTITION BY users_table.user_id+1, (events_table.value_2 % 25) ORDER BY events_table.time)
) as foo
ORDER BY 3 DESC, 1 DESC, 2 DESC NULLS LAST
LIMIT 10;

SELECT * FROM
(
  SELECT
    DISTINCT users_table.user_id, lag(users_table.user_id) OVER w1, rank() OVER w2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id and
    event_type < 4
  WINDOW w1 AS (PARTITION BY users_table.user_id, events_table.event_type ORDER BY events_table.time),
  w2 AS (ORDER BY events_table.time)
) as foo
ORDER BY
  3 DESC, 1 DESC, 2 DESC NULLS LAST
LIMIT
  10;

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
  my_rank > 1
ORDER BY
  3 DESC, 1 DESC,2 DESC
LIMIT
  10;

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
  my_rank > 1
ORDER BY
  3 DESC, 1 DESC,2 DESC
LIMIT
  10;

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
) a
ORDER BY
1,2,3;


SELECT * FROM (
  SELECT
    user_id, COUNT(*) OVER (PARTITION BY sum(user_id), MIN(value_2))
  FROM
    users_table
  GROUP BY
    user_id
) a
ORDER BY
  1 DESC, 2 DESC;

-- test with reference table partitioned on only a column from reference table
SELECT *
FROM
(
    SELECT
      DISTINCT user_id, it_name, count(id) OVER (PARTITION BY id)
    FROM
      users_table, users_ref_test_table
) a
ORDER BY
  1, 2, 3
LIMIT
  20;

SELECT
  max(avg)
FROM
(
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (1, 2))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (2, 3))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (3, 4))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (4, 5))
    UNION ALL
  (SELECT avg(value_3) over (partition by user_id), user_id FROM events_table where event_type IN (5, 6))
    UNION ALL
  (SELECT avg(value_3) over (partition by event_type), user_id FROM events_table where event_type IN (1, 6))
) b
GROUP BY user_id
ORDER BY 1 DESC
LIMIT 5;

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
            user_id)) AS ftop
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

DROP VIEW subq;
