-- ===================================================================
-- test top level window functions that are pushdownable
-- ===================================================================

-- a very simple window function with an aggregate and a window function
-- distribution column is on the partition by clause
SELECT
	user_id, COUNT(*) OVER (PARTITION BY user_id),
	rank() OVER (PARTITION BY user_id)
FROM
	users_table
ORDER BY
	1 DESC, 2 DESC, 3 DESC
LIMIT 5;

-- a more complicated window clause, including an aggregate
-- in both the window clause and the target entry
SELECT
	user_id, avg(avg(value_3)) OVER (PARTITION BY user_id, MIN(value_2))
FROM
	users_table
GROUP BY
	1
ORDER BY
	2 DESC NULLS LAST, 1 DESC;

-- window clause operates on the results of a subquery
SELECT
	user_id, max(value_1) OVER (PARTITION BY user_id, MIN(value_2))
FROM (
	SELECT
		DISTINCT us.user_id, us.value_2, value_1, random() as r1
	FROM
		users_table as us, events_table
	WHERE
		us.user_id = events_table.user_id AND event_type IN (1,2)
	ORDER BY
		user_id, value_2
	) s
GROUP BY
	1, value_1
ORDER BY
	2 DESC, 1;

-- window function operates on the results of
-- a join
SELECT
	us.user_id,
	SUM(us.value_1) OVER (PARTITION BY us.user_id)
FROM
	users_table us
	JOIN
	events_table ev
	ON (us.user_id = ev.user_id)
GROUP BY
	1,
	value_1
ORDER BY
	1,
	2
LIMIT 5;

-- the same query, but this time join with an alias
SELECT
	user_id, value_1, SUM(j.value_1) OVER (PARTITION BY j.user_id)
FROM
	(users_table us
	JOIN
  		events_table ev
	USING (user_id )
	) j
GROUP BY
	user_id,
	value_1
ORDER BY
	3 DESC, 2 DESC, 1 DESC
LIMIT 5;

-- querying views that have window functions should be ok
CREATE VIEW window_view AS
SELECT
  DISTINCT user_id, rank() OVER (PARTITION BY user_id ORDER BY value_1)
FROM
  users_table
GROUP BY
  user_id, value_1
HAVING count(*) > 1;

-- Window function in View works
SELECT *
FROM
	window_view
ORDER BY
	2 DESC, 1
LIMIT 10;

-- the other way around also should work fine
-- query a view using window functions
CREATE VIEW users_view AS SELECT * FROM users_table;

SELECT
	DISTINCT user_id, rank() OVER (PARTITION BY user_id ORDER BY value_1)
FROM
	users_view
GROUP BY
	user_id, value_1
HAVING count(*) > 4
ORDER BY
	2 DESC, 1;

DROP VIEW users_view, window_view;

-- window function uses columns from two different tables
SELECT
	DISTINCT ON (events_table.user_id, rnk) events_table.user_id, rank() OVER my_win AS rnk
FROM
	events_table, users_table
WHERE
	users_table.user_id = events_table.user_id
WINDOW
	my_win AS (PARTITION BY events_table.user_id, users_table.value_1 ORDER BY events_table.time DESC)
ORDER BY
	rnk DESC, 1 DESC
LIMIT 10;

-- the same query with reference table column is also on the partition by clause
SELECT
	DISTINCT ON (events_table.user_id, rnk) events_table.user_id, rank() OVER my_win AS rnk
FROM
	events_table, users_ref_test_table uref
WHERE
	uref.id = events_table.user_id
WINDOW
	my_win AS (PARTITION BY events_table.user_id, uref.k_no ORDER BY events_table.time DESC)
ORDER BY
	rnk DESC, 1 DESC
LIMIT 10;

-- similar query with no distribution column on the partition by clause
SELECT
	DISTINCT ON (events_table.user_id, rnk) events_table.user_id, rank() OVER my_win AS rnk
FROM
	events_table, users_ref_test_table uref
WHERE
	uref.id = events_table.user_id
WINDOW
	my_win AS (PARTITION BY events_table.value_2, uref.k_no ORDER BY events_table.time DESC)
ORDER BY
	rnk DESC, 1 DESC
LIMIT 10;

-- ORDER BY in the window function is an aggregate
SELECT
	user_id, rank() OVER my_win as rnk, avg(value_2) as avg_val_2
FROM
	events_table
GROUP BY
	user_id, date_trunc('day', time)
WINDOW
	my_win AS (PARTITION BY user_id ORDER BY avg(event_type) DESC)
ORDER BY
	3 DESC, 2 DESC, 1 DESC;

-- lets push the limits of writing complex expressions aling with the window functions
SELECT
	COUNT(*) OVER (PARTITION BY user_id, user_id + 1),
	rank() OVER (PARTITION BY user_id) as cnt1,
	COUNT(*) OVER (PARTITION BY user_id, abs(value_1 - value_2)) as cnt2,
	date_trunc('min', lag(time) OVER (PARTITION BY user_id ORDER BY time)) as datee,
	rank() OVER my_win  as rnnk,
	avg(CASE
			WHEN user_id > 4
				THEN value_1
			ELSE value_2
         END) FILTER (WHERE user_id > 2) OVER my_win_2 as filtered_count,
	sum(user_id * (5.0 / (value_1 + value_2 + 0.1)) * value_3) FILTER (WHERE value_1::text LIKE '%1%') OVER my_win_4 as cnt_with_filter_2
FROM
	users_table
WINDOW
	my_win AS (PARTITION BY user_id, (value_1%3)::int ORDER BY time DESC),
	my_win_2 AS (PARTITION BY user_id, (value_1)::int ORDER BY time DESC),
	my_win_3 AS (PARTITION BY user_id,  date_trunc('min', time)),
	my_win_4 AS (my_win_3 ORDER BY value_2, value_3)
ORDER BY
  cnt_with_filter_2 DESC NULLS LAST, filtered_count DESC NULLS LAST, datee DESC NULLS LAST, rnnk DESC, cnt2 DESC, cnt1 DESC, user_id DESC
LIMIT 5;

-- some tests with GROUP BY along with PARTITION BY
SELECT
	user_id,
	rank() OVER my_win as my_rank,
	avg(avg(event_type)) OVER my_win_2 as avg,
	max(time) as mx_time
FROM
	events_table
GROUP BY
	user_id,
	value_2
WINDOW
	my_win AS (PARTITION BY user_id, max(event_type) ORDER BY count(*) DESC),
	my_win_2 AS (PARTITION BY user_id, avg(user_id) ORDER BY count(*) DESC)
ORDER BY
	avg DESC,
	mx_time DESC,
	my_rank DESC,
	user_id DESC;

-- test for range and rows mode and different window functions
-- mostly to make sure that deparsing works fine
SELECT
	user_id,
	rank() OVER (PARTITION BY user_id ROWS BETWEEN
				 UNBOUNDED PRECEDING AND CURRENT ROW),
	dense_rank() OVER (PARTITION BY user_id RANGE BETWEEN
					   UNBOUNDED PRECEDING AND CURRENT ROW),
	CUME_DIST() OVER (PARTITION BY user_id RANGE BETWEEN
					  UNBOUNDED PRECEDING AND  UNBOUNDED FOLLOWING),
	PERCENT_RANK() OVER (PARTITION BY user_id ORDER BY avg(value_1) RANGE BETWEEN
						 UNBOUNDED PRECEDING AND  UNBOUNDED FOLLOWING)
FROM
	users_table
GROUP BY
	1
ORDER BY
	4 DESC,3 DESC,2 DESC ,1 DESC;

-- test exclude supported
SELECT
	user_id,
	value_1,
	array_agg(value_1) OVER (PARTITION BY user_id ORDER BY value_1 RANGE BETWEEN  UNBOUNDED PRECEDING AND CURRENT ROW),
	array_agg(value_1) OVER (PARTITION BY user_id ORDER BY value_1 RANGE BETWEEN  UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)
FROM
	users_table
WHERE
	user_id > 2 AND user_id < 6
ORDER BY
	user_id, value_1, 3, 4;

-- test <offset> preceding and <offset> following on RANGE window
SELECT
	user_id,
	value_1,
	array_agg(value_1) OVER range_window,
	array_agg(value_1) OVER range_window_exclude
FROM
	users_table
WHERE
	user_id > 2 AND user_id < 6
WINDOW
	range_window as (PARTITION BY user_id ORDER BY value_1 RANGE BETWEEN  1 PRECEDING AND 1 FOLLOWING),
	range_window_exclude as (PARTITION BY user_id ORDER BY value_1 RANGE BETWEEN  1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)
ORDER BY
	user_id, value_1, 3, 4;

-- test <offset> preceding and <offset> following on ROW window
SELECT
	user_id,
	value_1,
	array_agg(value_1) OVER row_window,
	array_agg(value_1) OVER row_window_exclude
FROM
	users_table
WHERE
	user_id > 2 and user_id < 6
WINDOW
	row_window as (PARTITION BY user_id ORDER BY value_1 ROWS BETWEEN  1 PRECEDING AND 1 FOLLOWING),
	row_window_exclude as (PARTITION BY user_id ORDER BY value_1 ROWS BETWEEN  1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)
ORDER BY
	user_id, value_1, 3, 4;

-- repeat above 3 tests without grouping by distribution column
SELECT
	value_2,
	rank() OVER (PARTITION BY value_2 ROWS BETWEEN
				 UNBOUNDED PRECEDING AND CURRENT ROW),
	dense_rank() OVER (PARTITION BY value_2 RANGE BETWEEN
					   UNBOUNDED PRECEDING AND CURRENT ROW),
	CUME_DIST() OVER (PARTITION BY value_2 RANGE BETWEEN
					  UNBOUNDED PRECEDING AND  UNBOUNDED FOLLOWING),
	PERCENT_RANK() OVER (PARTITION BY value_2 ORDER BY avg(value_1) RANGE BETWEEN
						 UNBOUNDED PRECEDING AND  UNBOUNDED FOLLOWING)
FROM
	users_table
GROUP BY
	1
ORDER BY
	4 DESC,3 DESC,2 DESC ,1 DESC;

-- test exclude supported
SELECT
	value_2,
	value_1,
	array_agg(value_1) OVER (PARTITION BY value_2 ORDER BY value_1 RANGE BETWEEN  UNBOUNDED PRECEDING AND CURRENT ROW),
	array_agg(value_1) OVER (PARTITION BY value_2 ORDER BY value_1 RANGE BETWEEN  UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)
FROM
	users_table
WHERE
	value_2 > 2 AND value_2 < 6
ORDER BY
	value_2, value_1, 3, 4;

-- test <offset> preceding and <offset> following on RANGE window
SELECT
	value_2,
	value_1,
	array_agg(value_1) OVER range_window,
	array_agg(value_1) OVER range_window_exclude
FROM
	users_table
WHERE
	value_2 > 2 AND value_2 < 6
WINDOW
	range_window as (PARTITION BY value_2 ORDER BY value_1 RANGE BETWEEN  1 PRECEDING AND 1 FOLLOWING),
	range_window_exclude as (PARTITION BY value_2 ORDER BY value_1 RANGE BETWEEN  1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)
ORDER BY
	value_2, value_1, 3, 4;

-- test <offset> preceding and <offset> following on ROW window
SELECT
	value_2,
	value_1,
	array_agg(value_1) OVER row_window,
	array_agg(value_1) OVER row_window_exclude
FROM
	users_table
WHERE
	value_2 > 2 and value_2 < 6
WINDOW
	row_window as (PARTITION BY value_2 ORDER BY value_1 ROWS BETWEEN  1 PRECEDING AND 1 FOLLOWING),
	row_window_exclude as (PARTITION BY value_2 ORDER BY value_1 ROWS BETWEEN  1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)
ORDER BY
	value_2, value_1, 3, 4;

-- some tests with GROUP BY, HAVING and LIMIT
SELECT
	user_id, sum(event_type) OVER my_win , event_type
FROM
	events_table
GROUP BY
	user_id, event_type
HAVING count(*) > 2
	WINDOW my_win AS (PARTITION BY user_id, max(event_type) ORDER BY count(*) DESC)
ORDER BY
	2 DESC, 3 DESC, 1 DESC
LIMIT
	5;

-- test PARTITION BY avg(...) ORDER BY avg(...)
SELECT
	value_1,
	avg(value_3),
	dense_rank() OVER (PARTITION BY avg(value_3) ORDER BY avg(value_2))
FROM
	users_table
GROUP BY
	1
ORDER BY
	1;

-- Group by has more columns than partition by
SELECT
	DISTINCT user_id, SUM(value_2) OVER (PARTITION BY user_id)
FROM
	users_table
GROUP BY
	user_id, value_1, value_2
HAVING count(*) > 2
ORDER BY
	2 DESC, 1
LIMIT
	10;

SELECT
	DISTINCT ON (user_id) user_id, SUM(value_2) OVER (PARTITION BY user_id)
FROM
	users_table
GROUP BY
	user_id, value_1, value_2
HAVING count(*) > 2
ORDER BY
	1, 2 DESC
LIMIT
	10;

SELECT
	DISTINCT ON (SUM(value_1) OVER (PARTITION BY user_id)) user_id, SUM(value_2) OVER (PARTITION BY user_id)
FROM
	users_table
GROUP BY
	user_id, value_1, value_2
HAVING count(*) > 2
ORDER BY
	(SUM(value_1) OVER (PARTITION BY user_id)) , 2 DESC, 1
LIMIT
	10;

-- not a meaningful query, with interesting syntax
SELECT
	user_id,
	AVG(avg(value_1)) OVER (PARTITION BY user_id, max(user_id), MIN(value_2)),
	AVG(avg(user_id)) OVER (PARTITION BY user_id, min(user_id), AVG(value_1))
FROM
	users_table
GROUP BY
	1
ORDER BY
	3 DESC, 2 DESC, 1 DESC;

SELECT coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
SELECT
	user_id,
	AVG(avg(value_1)) OVER (PARTITION BY user_id, max(user_id), MIN(value_2)),
	AVG(avg(user_id)) OVER (PARTITION BY user_id, min(user_id), AVG(value_1))
FROM
	users_table
GROUP BY
	1
ORDER BY
	3 DESC, 2 DESC, 1 DESC;
$Q$);

SELECT
	value_2,
	AVG(avg(value_1)) OVER (PARTITION BY value_2, max(value_2), MIN(value_2)),
	AVG(avg(value_2)) OVER (PARTITION BY value_2, min(value_2), AVG(value_1))
FROM
	users_table
GROUP BY
	1
ORDER BY
	3 DESC, 2 DESC, 1 DESC;

SELECT
	value_2, user_id,
	AVG(avg(value_1)) OVER (PARTITION BY value_2, max(value_2), MIN(value_2)),
	AVG(avg(value_2)) OVER (PARTITION BY user_id, min(value_2), AVG(value_1))
FROM
	users_table
GROUP BY
	1, 2
ORDER BY
	3 DESC, 2 DESC, 1 DESC;

SELECT user_id, sum(avg(user_id)) OVER ()
FROM users_table
GROUP BY user_id
ORDER BY 1
LIMIT 10;

SELECT
	user_id,
	1 + sum(value_1),
	1 + AVG(value_2) OVER (partition by user_id)
FROM
	users_table
GROUP BY
	user_id, value_2
ORDER BY
	user_id, value_2;

SELECT
	user_id,
	1 + sum(value_1),
	1 + AVG(value_2) OVER (partition by user_id)
FROM
	users_table
GROUP BY
	user_id, value_2
ORDER BY
	2 DESC, 1
LIMIT 5;

-- rank and ordering in the reverse order
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by value_2)
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, value_2 DESC;

-- order by in the window function is same as avg(value_1) DESC
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by 1 / (1 + avg(value_1)))
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, avg(value_1) DESC;

EXPLAIN (COSTS FALSE)
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by 1 / (1 + avg(value_1)))
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, avg(value_1) DESC;

-- order by in the window function is same as avg(value_1) DESC
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by 1 / (1 + avg(value_1)))
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, avg(value_1) DESC;

-- limit is not pushed down to worker !!
EXPLAIN (COSTS FALSE)
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by 1 / (1 + avg(value_1)))
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, avg(value_1) DESC
LIMIT 5;

EXPLAIN (COSTS FALSE)
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by 1 / (1 + avg(value_1)))
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, avg(value_1) DESC
LIMIT 5;

EXPLAIN (COSTS FALSE)
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by 1 / (1 + sum(value_2)))
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, avg(value_1) DESC
LIMIT 5;

EXPLAIN (COSTS FALSE)
SELECT
	user_id,
	avg(value_1),
	RANK() OVER (partition by user_id order by sum(value_2))
FROM
	users_table
GROUP BY user_id, value_2
ORDER BY user_id, avg(value_1) DESC
LIMIT 5;

-- Grouping can be pushed down with aggregates even when window function can't
EXPLAIN (COSTS FALSE)
SELECT user_id, count(value_1), stddev(value_1), count(user_id) OVER (PARTITION BY random())
FROM users_table GROUP BY user_id HAVING avg(value_1) > 2 LIMIT 1;

-- Window function with inlined CTE
WITH cte as (
    SELECT uref.id user_id, events_table.value_2, count(*) c
    FROM events_table
    JOIN users_ref_test_table uref ON uref.id = events_table.user_id
    GROUP BY 1, 2
)
SELECT DISTINCT cte.value_2, cte.c, sum(cte.value_2) OVER (PARTITION BY cte.c)
FROM cte JOIN events_table et ON et.value_2 = cte.value_2 and et.value_2 = cte.c
ORDER BY 1;

-- There was a strange bug where this wouldn't have window functions being pushed down
-- Bug dependent on column ordering
CREATE TABLE daily_uniques (value_2 float, user_id bigint);
SELECT create_distributed_table('daily_uniques', 'user_id');

EXPLAIN (COSTS FALSE) SELECT
  user_id,
  sum(value_2) AS commits,
  RANK () OVER (
    PARTITION BY user_id
    ORDER BY
      sum(value_2) DESC
  )
FROM daily_uniques
GROUP BY user_id
HAVING
  sum(value_2) > 0
ORDER BY commits DESC
LIMIT 10;

DROP TABLE daily_uniques;

-- Partition by reference table column joined to distribution column
SELECT DISTINCT value_2, array_agg(rnk ORDER BY rnk) FROM (
SELECT events_table.value_2, sum(uref.k_no) OVER (PARTITION BY uref.id) AS rnk
FROM events_table
JOIN users_ref_test_table uref ON uref.id = events_table.user_id) sq
GROUP BY 1 ORDER BY 1;

