-- Test functions for partitioning intermediate results
CREATE SCHEMA partitioned_intermediate_results;
SET search_path TO 'partitioned_intermediate_results';


-- hash partitioned intermediate results
BEGIN;
SELECT * FROM worker_partition_query_result('squares_hash',
                                            'SELECT i, i * i FROM generate_series(1, 10) i', 0, 'hash',
                                            '{-2147483648,-1073741824,0,1073741824}'::text[],
                                            '{-1073741825,-1,1073741823,2147483647}'::text[], false);
SELECT hashint4(x), x, x2 FROM
read_intermediate_result('squares_hash_0', 'text') AS res (x int, x2 int)
ORDER BY x;

SELECT hashint4(x), x, x2 FROM
read_intermediate_result('squares_hash_1', 'text') AS res (x int, x2 int)
ORDER BY x;

SELECT hashint4(x), x, x2 FROM
read_intermediate_result('squares_hash_2', 'text') AS res (x int, x2 int)
ORDER BY x;

SELECT hashint4(x), x, x2 FROM
read_intermediate_result('squares_hash_3', 'text') AS res (x int, x2 int)
ORDER BY x;

END;

-- range partitioned intermediate results
BEGIN;
SELECT * FROM worker_partition_query_result('squares_range',
                                            'SELECT i, i * i FROM generate_series(1, 10) i',
                                            1, /* partition by x^2 */
                                            'range',
                                            '{0,21,41,61}'::text[],
                                            '{20,40,60,100}'::text[],
                                            true /* binary format */);
SELECT x, x2 FROM
read_intermediate_result('squares_range_0', 'binary') AS res (x int, x2 int)
ORDER BY x;

SELECT x, x2 FROM
read_intermediate_result('squares_range_1', 'binary') AS res (x int, x2 int)
ORDER BY x;

SELECT x, x2 FROM
read_intermediate_result('squares_range_2', 'binary') AS res (x int, x2 int)
ORDER BY x;

SELECT x, x2 FROM
read_intermediate_result('squares_range_3', 'binary') AS res (x int, x2 int)
ORDER BY x;

END;

-- 1M rows, just in case. text format.
BEGIN;
SELECT * FROM worker_partition_query_result('doubles_hash',
                                            'SELECT i, i * 2 FROM generate_series(1, 1000000) i', 0, 'hash',
                                            '{-2147483648,-1073741824,0,1073741824}'::text[],
                                            '{-1073741825,-1,1073741823,2147483647}'::text[], false);
SELECT count(*) FROM read_intermediate_results(ARRAY['doubles_hash_0',
                                                     'doubles_hash_1',
                                                     'doubles_hash_2',
                                                     'doubles_hash_3'], 'text') AS res (x int, x2 int);
END;

-- 1M rows, just in case. binary format.
BEGIN;
SELECT * FROM worker_partition_query_result('doubles_range',
                                            'SELECT i, i * 2 FROM generate_series(1, 1000000) i', 0, 'range',
                                            '{0,250001,500001,750001}'::text[],
                                            '{250000,500000,750000,1000000}'::text[], true);
SELECT count(*) FROM read_intermediate_results(ARRAY['doubles_range_0',
                                                     'doubles_range_1',
                                                     'doubles_range_2',
                                                     'doubles_range_3'], 'binary') AS res (x int, x2 int);
END;

--
-- Some error cases
--

-- not allowed outside transaction block
SELECT * FROM worker_partition_query_result('squares_range',
                                            'SELECT i, i * i FROM generate_series(1, 10) i',
                                            1, 'range', '{0}'::text[], '{20}'::text[], true);

BEGIN;
SAVEPOINT s1;
-- syntax error in query
SELECT worker_partition_query_result('squares_range',
                                     'SELECxT i, i * i FROM generate_series(1, 10) i',
                                     1, 'range',
                                     '{0,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- invalid result prefix
SELECT worker_partition_query_result('squares_range/a/',
                                     'SELECT i, i * i FROM generate_series(1, 10) i',
                                     1, 'range',
                                     '{0,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- empty min/max values
SELECT worker_partition_query_result('squares_range',
                                     'SELECT i, i * i FROM generate_series(1, 10) i',
                                     1, 'range', ARRAY[]::text[], ARRAY[]::text[], true);
ROLLBACK TO SAVEPOINT s1;

-- append partitioning
SELECT worker_partition_query_result('squares_range',
                                     'SELECT i, i * i FROM generate_series(1, 10) i',
                                     1, 'append',
                                     '{0,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- query with no results
CREATE TABLE t(a int);
SELECT worker_partition_query_result('squares_range',
                                     'INSERT INTO t VALUES (1), (2)',
                                     1, 'range',
                                     '{0,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- negative partition index
SELECT worker_partition_query_result('squares_range',
                                     'SELECT i, i * i FROM generate_series(1, 10) i',
                                     -1, 'range',
                                     '{0,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- too large partition index
SELECT worker_partition_query_result('squares_range',
                                     'SELECT i, i * i FROM generate_series(1, 10) i',
                                     2, 'range',
                                     '{0,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- min/max values of different lengths
SELECT worker_partition_query_result('squares_range',
                                     'SELECT i, i * i FROM generate_series(1, 10) i',
                                     1, 'range',
                                     '{0,21,41,61,101}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- null values in min/max values of hash partitioned results
SELECT worker_partition_query_result('squares_hash',
                                     'SELECT i, i * i FROM generate_series(1, 10) i',
                                     1, 'hash',
                                     '{NULL,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;

-- multiple queries
SELECT worker_partition_query_result('squares_hash',
                                     'SELECT i, i * i FROM generate_series(1, 10) i; SELECT 4, 16;',
                                     1, 'hash',
                                     '{NULL,21,41,61}'::text[],
                                     '{20,40,60,100}'::text[],
                                     true);
ROLLBACK TO SAVEPOINT s1;
ROLLBACK;

--
-- Procedure for conveniently testing worker_partition_query_result(). It uses
-- worker_partition_query_results to partition result of query using the same
-- scheme as the distributed table rel, and then compares if it did the partitioning
-- the same way as shards of rel.
--
CREATE OR REPLACE PROCEDURE test_partition_query_results(rel regclass, query text,
                                                        binaryCopy boolean DEFAULT true)
AS $$
DECLARE
  partition_min_values text[];
  partition_max_values text[];
  partition_column_index int;
  partition_method citus.distribution_type;
  partitioned_results_row_counts text[];
  distributed_table_row_counts text[];
  tuple_def text;
  partition_result_names text[];
  non_empty_partitions int[];
  rows_different int;
BEGIN
  -- get tuple definition
  SELECT string_agg(a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
  INTO tuple_def
  FROM pg_catalog.pg_attribute a
  WHERE a.attrelid = rel::oid AND a.attnum > 0 AND NOT a.attisdropped;

  -- get min/max value arrays
  SELECT array_agg(shardminvalue ORDER BY shardid),
         array_agg(shardmaxvalue ORDER BY shardid)
  INTO partition_min_values, partition_max_values
  FROM pg_dist_shard
  WHERE logicalrelid=rel;

  -- get partition column index and partition method
  SELECT (regexp_matches(partkey, ':varattno ([0-9]+)'))[1]::int - 1,
         (CASE WHEN partmethod='h' THEN 'hash' ELSE 'range' END)
  INTO partition_column_index, partition_method
  FROM pg_dist_partition
  WHERE logicalrelid=rel;

  -- insert into the distributed table
  EXECUTE 'INSERT INTO ' || rel::text || ' ' || query;

  -- repartition the query locally
  SELECT array_agg(rows_written::text ORDER BY partition_index),
         array_agg(partition_index) FILTER (WHERE rows_written > 0)
  INTO partitioned_results_row_counts,
       non_empty_partitions
  FROM worker_partition_query_result('test_prefix', query, partition_column_index,
                                     partition_method, partition_min_values,
                                     partition_max_values, binaryCopy);

  SELECT array_agg('test_prefix_' || i::text)
  INTO partition_result_names
  FROM unnest(non_empty_partitions) i;

  EXECUTE 'SELECT count(*) FROM ((' || query || ') EXCEPT (SELECT * FROM read_intermediate_results($1,$2) AS res (' || tuple_def || '))) t'
  INTO rows_different
  USING partition_result_names, (CASE WHEN binaryCopy THEN 'binary' ELSE 'text' END)::pg_catalog.citus_copy_format;

  -- commit so results are available in run_command_on_shards
  COMMIT;

  -- rows per shard of the distributed table
  SELECT array_agg(result order by shardid) INTO distributed_table_row_counts
  FROM run_command_on_shards(rel, 'SELECT count(*) FROM %s');

  IF partitioned_results_row_counts = distributed_table_row_counts THEN
    RAISE NOTICE 'Rows per partition match ...';
  ELSE
    RAISE 'FAILED: rows per partition do not match, expecting % got %', distributed_table_row_counts, partitioned_results_row_counts;
  END IF;

  IF rows_different = 0 THEN
    RAISE NOTICE 'Row values match ...';
  ELSE
    RAISE 'FAILED: Could not find % of expected rows in partitions', rows_different;
  END IF;

  RAISE NOTICE 'PASSED.';
END;
$$ LANGUAGE plpgsql;

\set VERBOSITY terse

-- hash partitioning, 32 shards
SET citus.shard_count TO 32;
CREATE TABLE t(a int, b int);
SELECT create_distributed_table('t', 'a');
CALL test_partition_query_results('t', 'SELECT x, x * x FROM generate_series(1, 100) x');
DROP TABLE t;

-- hash partitioning, 1 shard
SET citus.shard_count TO 1;
CREATE TABLE t(a int, b int);
SELECT create_distributed_table('t', 'a');
CALL test_partition_query_results('t', 'SELECT x, x * x FROM generate_series(1, 100) x');
DROP TABLE t;

-- hash partitioning, 17 shards (so hash partitions aren't uniform)
SET citus.shard_count TO 17;
CREATE TABLE t(a int, b int);
SELECT create_distributed_table('t', 'a');
CALL test_partition_query_results('t', 'SELECT x, x * x FROM generate_series(1, 100) x');
DROP TABLE t;

-- hash partitioning, date partition column
SET citus.shard_count TO 8;
CREATE TABLE t(a DATE, b int);
SELECT create_distributed_table('t', 'a');
CALL test_partition_query_results('t', 'SELECT (''1985-05-18''::date + (x::text || '' days'')::interval)::date, x * x FROM generate_series(1, 100) x');
DROP TABLE t;

-- hash partitioning, int4  range partition column
SET citus.shard_count TO 8;
CREATE TABLE t(a int4range, b int);
SELECT create_distributed_table('t', 'a');
CALL test_partition_query_results('t', 'SELECT int4range(x,2*x+10), x * x FROM generate_series(1, 100) x');
DROP TABLE t;

-- range partitioning, int partition column
CREATE TABLE t(key int, value int);
SELECT create_distributed_table('t', 'key', 'range');
CALL public.create_range_partitioned_shards('t', '{0,25,50,76}',
                                          '{24,49,75,200}');
CALL test_partition_query_results('t', 'SELECT x, x * x * x FROM generate_series(1, 105) x');
DROP TABLE t;

-- not covering ranges, should ERROR
CREATE TABLE t(key int, value int);
SELECT create_distributed_table('t', 'key', 'range');
CALL public.create_range_partitioned_shards('t', '{0,25,50,100}',
                                          '{24,49,75,200}');
CALL test_partition_query_results('t', 'SELECT x, x * x * x FROM generate_series(1, 105) x');
DROP TABLE t;

-- overlapping ranges, we allow this in range partitioned distributed tables, should be fine
CREATE TABLE t(key int, value int);
SELECT create_distributed_table('t', 'key', 'range');
CALL public.create_range_partitioned_shards('t', '{0,25,50,76}',
                                          '{50,49,90,200}');
CALL test_partition_query_results('t', 'SELECT x, x * x * x FROM generate_series(1, 105) x');
DROP TABLE t;

-- range partitioning, composite partition column
CREATE TYPE composite_key_type AS (f1 int, f2 text);
SET citus.shard_count TO 8;
CREATE TABLE t(key composite_key_type, value int);
SELECT create_distributed_table('t', 'key', 'range');
CALL public.create_range_partitioned_shards('t', '{"(0,a)","(25,a)","(50,a)","(75,a)"}',
                                          '{"(24,z)","(49,z)","(74,z)","(100,z)"}');
CALL test_partition_query_results('t', 'SELECT (x, ''f2_'' || x::text)::composite_key_type, x * x * x FROM generate_series(1, 100) x');
DROP TABLE t;
DROP TYPE composite_key_type;

-- unsorted ranges
CREATE TABLE t(key int, value int);
SELECT create_distributed_table('t', 'key', 'range');
CALL public.create_range_partitioned_shards('t', '{50,25,76,0}',
                                          '{75,49,200,24}');
CALL test_partition_query_results('t', 'SELECT x, x * x * x FROM generate_series(1, 105) x');
DROP TABLE t;


SET client_min_messages TO WARNING;
DROP SCHEMA partitioned_intermediate_results CASCADE;

\set VERBOSITY default
SET client_min_messages TO DEFAULT;
SET citus.shard_count TO DEFAULT;
