--
-- TDIGEST_AGGREGATE_SUPPORT
--   test the integration of github.com/tvondra/tdigest aggregates into the citus planner
--   for push down parts of the aggregate to use parallelized execution and reduced data
--   transfer sizes for aggregates not grouped by the distribution column
--

SET citus.next_shard_id TO 20070000;
CREATE SCHEMA tdigest_aggregate_support;
SET search_path TO tdigest_aggregate_support, public;

-- create the tdigest extension when installed
SELECT CASE WHEN COUNT(*) > 0
    THEN 'CREATE EXTENSION tdigest WITH SCHEMA public'
    ELSE 'SELECT false AS tdigest_present' END
AS create_cmd FROM pg_available_extensions()
WHERE name = 'tdigest'
\gset
:create_cmd;

SET citus.shard_count TO 4;
SET citus.coordinator_aggregation_strategy TO 'disabled'; -- prevent aggregate execution when the aggregate can't be pushed down

CREATE TABLE latencies (a int, b int, latency double precision);
SELECT create_distributed_table('latencies', 'a');
SELECT setseed(0.42); -- make the random data inserted deterministic
INSERT INTO latencies
SELECT (random()*20)::int AS a,
       (random()*20)::int AS b,
       random()*10000.0 AS latency
FROM generate_series(1, 10000);

-- explain no grouping to verify partially pushed down for tdigest(value, compression)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest(latency, 100)
FROM latencies;

-- explain grouping by distribution column is completely pushed down for tdigest(value, compression)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest(latency, 100)
FROM latencies
GROUP BY a;

-- explain grouping by non-distribution column is partially pushed down for tdigest(value, compression)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT b, tdigest(latency, 100)
FROM latencies
GROUP BY b;

-- explain no grouping to verify partially pushed down for tdigest_precentile(value, compression, quantile)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile(latency, 100, 0.99)
FROM latencies;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile(value, compression, quantile)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile(latency, 100, 0.99)
FROM latencies
GROUP BY a;

-- explain grouping by non-distribution column is partially pushed down for tdigest_precentile(value, compression, quantile)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT b, tdigest_percentile(latency, 100, 0.99)
FROM latencies
GROUP BY b;

-- explain no grouping to verify partially pushed down for tdigest_precentile(value, compression, quantiles[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile(latency, 100, ARRAY[0.99, 0.95])
FROM latencies;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile(value, compression, quantiles[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile(latency, 100, ARRAY[0.99, 0.95])
FROM latencies
GROUP BY a;

-- explain grouping by non-distribution column is partially pushed down for tdigest_precentile(value, compression, quantiles[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT b, tdigest_percentile(latency, 100, ARRAY[0.99, 0.95])
FROM latencies
GROUP BY b;

-- explain no grouping to verify partially pushed down for tdigest_precentile_of(value, compression, hypotetical_value)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile_of(latency, 100, 9000)
FROM latencies;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile_of(value, compression, hypotetical_value)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile_of(latency, 100, 9000)
FROM latencies
GROUP BY a;

-- explain grouping by non-distribution column is partially pushed down for tdigest_precentile_of(value, compression, hypotetical_value)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT b, tdigest_percentile_of(latency, 100, 9000)
FROM latencies
GROUP BY b;

-- explain no grouping to verify partially pushed down for tdigest_precentile_of(value, compression, hypotetical_values[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile_of(latency, 100, ARRAY[9000, 9500])
FROM latencies;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile_of(value, compression, hypotetical_values[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile_of(latency, 100, ARRAY[9000, 9500])
FROM latencies
GROUP BY a;

-- explain grouping by non-distribution column is partially pushed down for tdigest_precentile_of(value, compression, hypotetical_values[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT b, tdigest_percentile_of(latency, 100, ARRAY[9000, 9500])
FROM latencies
GROUP BY b;

-- verifying results - should be stable due to seed while inserting the data, if failure due to data these queries could be removed or check for certain ranges
SELECT tdigest(latency, 100) FROM latencies;
SELECT tdigest_percentile(latency, 100, 0.99) FROM latencies;
SELECT tdigest_percentile(latency, 100, ARRAY[0.99, 0.95]) FROM latencies;
SELECT tdigest_percentile_of(latency, 100, 9000) FROM latencies;
SELECT tdigest_percentile_of(latency, 100, ARRAY[9000, 9500]) FROM latencies;

CREATE TABLE latencies_rollup (a int, tdigest tdigest);
SELECT create_distributed_table('latencies_rollup', 'a', colocate_with => 'latencies');

INSERT INTO latencies_rollup
SELECT a, tdigest(latency, 100)
FROM latencies
GROUP BY a;

EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest(tdigest)
FROM latencies_rollup;

-- explain grouping by distribution column is completely pushed down for tdigest(tdigest)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest(tdigest)
FROM latencies_rollup
GROUP BY a;

-- explain no grouping to verify partially pushed down for tdigest_precentile(tdigest, quantile)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile(tdigest, 0.99)
FROM latencies_rollup;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile(tdigest, quantile)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile(tdigest, 0.99)
FROM latencies_rollup
GROUP BY a;

-- explain no grouping to verify partially pushed down for tdigest_precentile(value, compression, quantiles[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile(tdigest, ARRAY[0.99, 0.95])
FROM latencies_rollup;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile(value, compression, quantiles[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile(tdigest, ARRAY[0.99, 0.95])
FROM latencies_rollup
GROUP BY a;

-- explain no grouping to verify partially pushed down for tdigest_precentile_of(value, compression, hypotetical_value)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile_of(tdigest, 9000)
FROM latencies_rollup;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile_of(value, compression, hypotetical_value)
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile_of(tdigest, 9000)
FROM latencies_rollup
GROUP BY a;

-- explain no grouping to verify partially pushed down for tdigest_precentile_of(value, compression, hypotetical_values[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT tdigest_percentile_of(tdigest, ARRAY[9000, 9500])
FROM latencies_rollup;

-- explain grouping by distribution column is completely pushed down for tdigest_precentile_of(value, compression, hypotetical_values[])
EXPLAIN (COSTS OFF, VERBOSE)
SELECT a, tdigest_percentile_of(tdigest, ARRAY[9000, 9500])
FROM latencies_rollup
GROUP BY a;

-- verifying results - should be stable due to seed while inserting the data, if failure due to data these queries could be removed or check for certain ranges
SELECT tdigest(tdigest) FROM latencies_rollup;
SELECT tdigest_percentile(tdigest, 0.99) FROM latencies_rollup;
SELECT tdigest_percentile(tdigest, ARRAY[0.99, 0.95]) FROM latencies_rollup;
SELECT tdigest_percentile_of(tdigest, 9000) FROM latencies_rollup;
SELECT tdigest_percentile_of(tdigest, ARRAY[9000, 9500]) FROM latencies_rollup;

SET client_min_messages TO WARNING; -- suppress cascade messages
DROP SCHEMA tdigest_aggregate_support CASCADE;
