--
-- CUSTOM_AGGREGATE_SUPPORT
--
-- Create HLL extension if present, print false result otherwise
SELECT CASE WHEN COUNT(*) > 0 THEN
  'CREATE EXTENSION HLL'
ELSE 'SELECT false AS hll_present' END
AS create_cmd FROM pg_available_extensions()
WHERE name = 'hll'
\gset

:create_cmd;

SET citus.shard_count TO 4;
set citus.coordinator_aggregation_strategy to 'disabled';

CREATE TABLE raw_table (day date, user_id int);
CREATE TABLE daily_uniques(day date, unique_users hll);

SELECT create_distributed_table('raw_table', 'user_id');
SELECT create_distributed_table('daily_uniques', 'day');

INSERT INTO raw_table
  SELECT day, user_id % 19
  FROM generate_series('2018-05-24'::timestamp, '2018-06-24'::timestamp, '1 day'::interval) as f(day),
       generate_series(1,100) as g(user_id);
INSERT INTO raw_table
  SELECT day, user_id % 13
  FROM generate_series('2018-06-10'::timestamp, '2018-07-10'::timestamp, '1 day'::interval) as f(day),
       generate_series(1,100) as g(user_id);

-- Run hll on raw data
SELECT hll_cardinality(hll_union_agg(agg))
FROM (
  SELECT hll_add_agg(hll_hash_integer(user_id)) AS agg
  FROM raw_table)a;

-- Aggregate the data into daily_uniques
INSERT INTO daily_uniques
  SELECT day, hll_add_agg(hll_hash_integer(user_id))
  FROM raw_table
  GROUP BY 1;

-- Basic hll_cardinality check on aggregated data
SELECT day, hll_cardinality(unique_users)
FROM daily_uniques
WHERE day >= '2018-06-20' and day <= '2018-06-30'
ORDER BY 2 DESC,1
LIMIT 10;

-- Union aggregated data for one week
SELECT hll_cardinality(hll_union_agg(unique_users))
FROM daily_uniques
WHERE day >= '2018-05-24'::date AND day <= '2018-05-31'::date;


SELECT EXTRACT(MONTH FROM day) AS month, hll_cardinality(hll_union_agg(unique_users))
FROM daily_uniques
WHERE day >= '2018-06-23' AND day <= '2018-07-01'
GROUP BY 1
ORDER BY 1;

SELECT day, hll_cardinality(hll_union_agg(unique_users) OVER seven_days)
FROM daily_uniques
WINDOW seven_days AS (ORDER BY day ASC ROWS 6 PRECEDING)
ORDER BY 1;

SELECT day, (hll_cardinality(hll_union_agg(unique_users) OVER two_days)) - hll_cardinality(unique_users) AS lost_uniques
FROM daily_uniques
WINDOW two_days AS (ORDER BY day ASC ROWS 1 PRECEDING)
ORDER BY 1;

-- Test disabling hash_agg on coordinator query
SET citus.explain_all_tasks to true;
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_union_agg(unique_users)
FROM
  daily_uniques
GROUP BY(1);

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_union_agg(unique_users)
FROM
  daily_uniques
GROUP BY(1);

-- Test disabling hash_agg with operator on coordinator query
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_union_agg(unique_users) || hll_union_agg(unique_users)
FROM
  daily_uniques
GROUP BY(1);

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_union_agg(unique_users) || hll_union_agg(unique_users)
FROM
  daily_uniques
GROUP BY(1);

-- Test disabling hash_agg with expression on coordinator query
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_cardinality(hll_union_agg(unique_users))
FROM
  daily_uniques
GROUP BY(1);

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_cardinality(hll_union_agg(unique_users))
FROM
  daily_uniques
GROUP BY(1);

-- Test disabling hash_agg with having
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_cardinality(hll_union_agg(unique_users))
FROM
  daily_uniques
GROUP BY(1);

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT
  day, hll_cardinality(hll_union_agg(unique_users))
FROM
  daily_uniques
GROUP BY(1)
HAVING hll_cardinality(hll_union_agg(unique_users)) > 1;

DROP TABLE raw_table;
DROP TABLE daily_uniques;

-- Check if TopN aggregates work as expected
-- Create TopN extension if present, print false result otherwise
SELECT CASE WHEN COUNT(*) > 0 THEN
  'CREATE EXTENSION TOPN'
ELSE 'SELECT false AS topn_present' END
AS create_topn FROM pg_available_extensions()
WHERE name = 'topn'
\gset

:create_topn;

CREATE TABLE customer_reviews (day date, user_id int, review int);
CREATE TABLE popular_reviewer(day date, reviewers jsonb);

SELECT create_distributed_table('customer_reviews', 'user_id');
SELECT create_distributed_table('popular_reviewer', 'day');

INSERT INTO customer_reviews
  SELECT day, user_id % 7, review % 5
  FROM generate_series('2018-05-24'::timestamp, '2018-06-24'::timestamp, '1 day'::interval) as f(day),
       generate_series(1,30) as g(user_id), generate_series(0,30) AS r(review);
INSERT INTO customer_reviews
  SELECT day, user_id % 13, review % 3
  FROM generate_series('2018-06-10'::timestamp, '2018-07-10'::timestamp, '1 day'::interval) as f(day),
       generate_series(1,30) as g(user_id), generate_series(0,30) AS r(review);

-- Run topn on raw data
SELECT (topn(agg, 10)).*
FROM (
  SELECT topn_add_agg(user_id::text) AS agg
  FROM customer_reviews
  )a
ORDER BY 2 DESC, 1;

-- Aggregate the data into popular_reviewer
INSERT INTO popular_reviewer
  SELECT day, topn_add_agg(user_id::text)
  FROM customer_reviews
  GROUP BY 1;

-- Basic topn check on aggregated data
SELECT day, (topn(reviewers, 10)).*
FROM popular_reviewer
WHERE day >= '2018-06-20' and day <= '2018-06-30'
ORDER BY 3 DESC, 1, 2
LIMIT 10;

-- Union aggregated data for one week
SELECT (topn(agg, 10)).*
FROM (
	SELECT topn_union_agg(reviewers) AS agg
	FROM popular_reviewer
	WHERE day >= '2018-05-24'::date AND day <= '2018-05-31'::date
	)a
ORDER BY 2 DESC, 1;

SELECT month, (topn(agg, 5)).*
FROM (
	SELECT EXTRACT(MONTH FROM day) AS month, topn_union_agg(reviewers) AS agg
	FROM popular_reviewer
	WHERE day >= '2018-06-23' AND day <= '2018-07-01'
	GROUP BY 1
	ORDER BY 1
	)a
ORDER BY 1, 3 DESC, 2;

-- TODO the following queries will be supported after we fix #2265
-- They work for PG9.6 but not for PG10
SELECT (topn(topn_union_agg(reviewers), 10)).*
FROM popular_reviewer
WHERE day >= '2018-05-24'::date AND day <= '2018-05-31'::date
ORDER BY 2 DESC, 1;

SELECT (topn(topn_add_agg(user_id::text), 10)).*
FROM customer_reviews
ORDER BY 2 DESC, 1;

SELECT day, (topn(agg, 10)).*
FROM (
	SELECT day, topn_union_agg(reviewers) OVER seven_days AS agg
	FROM popular_reviewer
	WINDOW seven_days AS (ORDER BY day ASC ROWS 6 PRECEDING)
	)a
ORDER BY 3 DESC, 1, 2
LIMIT 10;

SELECT day, (topn(topn_add_agg(user_id::text) OVER seven_days, 10)).*
FROM customer_reviews
WINDOW seven_days AS (ORDER BY day ASC ROWS 6 PRECEDING)
ORDER BY 3 DESC, 1, 2
LIMIT 10;

DROP TABLE customer_reviews;
DROP TABLE popular_reviewer;
