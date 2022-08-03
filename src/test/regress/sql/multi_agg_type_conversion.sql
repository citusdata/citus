--
-- MULTI_AGG_TYPE_CONVERSION
--

-- Test aggregate type conversions using sums of integers and division operator
SELECT sum(l_suppkey) FROM lineitem;
SELECT sum(l_suppkey) / 2 FROM lineitem;
SELECT sum(l_suppkey) / 2::numeric FROM lineitem;
SELECT sum(l_suppkey)::int8 / 2 FROM lineitem;


-- Create a new table to test type conversions on different types, and load
-- data into this table. Then, apply aggregate functions and divide / multiply
-- the results to test type conversions.

CREATE TABLE aggregate_type (
       float_value float(20) not null,
       double_value float(40) not null,
       interval_value interval not null);
SELECT create_distributed_table('aggregate_type', 'float_value', 'append');
SELECT master_create_empty_shard('aggregate_type') AS shardid \gset

\set agg_type_data_file :abs_srcdir '/data/agg_type.data'
copy aggregate_type FROM :'agg_type_data_file' with (append_to_shard :shardid);

-- Test conversions using aggregates on floats and division

SELECT min(float_value), max(float_value),
       sum(float_value), count(float_value), avg(float_value)
FROM aggregate_type;

SELECT min(float_value) / 2, max(float_value) / 2,
       sum(float_value) / 2, count(float_value) / 2, avg(float_value) / 2
FROM aggregate_type;

-- Test conversions using aggregates on large floats and multiplication

SELECT min(double_value), max(double_value),
       sum(double_value), count(double_value), avg(double_value)
FROM aggregate_type;

SELECT min(double_value) * 2, max(double_value) * 2,
       sum(double_value) * 2, count(double_value) * 2, avg(double_value) * 2
FROM aggregate_type;

-- Test conversions using aggregates on intervals and division. We also use the
-- default configuration value for IntervalStyle.

SET IntervalStyle TO 'postgres';

SELECT min(interval_value), max(interval_value),
       sum(interval_value), count(interval_value), avg(interval_value)
FROM aggregate_type;

SELECT min(interval_value) / 2, max(interval_value) / 2,
       sum(interval_value) / 2, count(interval_value) / 2, avg(interval_value) / 2
FROM aggregate_type;
