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

\c - - - :worker_1_port
:create_cmd;

\c - - - :worker_2_port
:create_cmd;

\c - - - :master_port

SET citus.shard_count TO 4;

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

-- These are going to be supported after window function support
SELECT day, hll_cardinality(hll_union_agg(unique_users) OVER seven_days)
FROM daily_uniques
WINDOW seven_days AS (ORDER BY day ASC ROWS 6 PRECEDING);

SELECT day, (hll_cardinality(hll_union_agg(unique_users) OVER two_days)) - hll_cardinality(unique_users) AS lost_uniques
FROM daily_uniques
WINDOW two_days AS (ORDER BY day ASC ROWS 1 PRECEDING);

DROP TABLE raw_table;
DROP TABLE daily_uniques;
