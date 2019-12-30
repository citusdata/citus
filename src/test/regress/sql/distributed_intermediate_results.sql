-- Test functions for partitioning intermediate results
CREATE SCHEMA distributed_intermediate_results;
SET search_path TO 'distributed_intermediate_results';

SET citus.next_shard_id TO 4213581;

--
-- Helper UDFs
--

-- partition_task_list_results tests the internal PartitionTasklistResults function
CREATE OR REPLACE FUNCTION pg_catalog.partition_task_list_results(resultIdPrefix text,
                                                                  query text,
                                                                  target_table regclass,
                                                                  binaryFormat bool DEFAULT true)
    RETURNS TABLE(resultId text,
                  nodeId int,
                  rowCount bigint,
                  targetShardId bigint,
                  targetShardIndex int)
    LANGUAGE C STRICT VOLATILE
    AS 'citus', $$partition_task_list_results$$;

--
-- We don't have extensive tests for partition_task_results, since it will be
-- tested by higher level "INSERT/SELECT with repartitioning" tests anyway.
--

--
-- partition_task_list_results, hash partitioning, binary format
--

CREATE TABLE source_table(a int);
SET citus.shard_count TO 3;
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT * FROM generate_series(1, 100);

CREATE TABLE target_table(a int);
SET citus.shard_count TO 2;
SELECT create_distributed_table('target_table', 'a');

-- should error out
SELECT partition_task_list_results('test', $$ SELECT avg(a) FROM source_table $$, 'target_table');
SELECT partition_task_list_results('test', $$ SELECT * FROM generate_series(1, 2) $$, 'target_table');

BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT resultId, nodeport, rowcount, targetShardId, targetShardIndex
  FROM partition_task_list_results('test', $$ SELECT * FROM source_table $$, 'target_table')
          NATURAL JOIN pg_dist_node;
SELECT * FROM distributed_result_info ORDER BY resultId;
-- fetch from workers
SELECT nodeport, fetch_intermediate_results((array_agg(resultId)), 'localhost', nodeport) > 0 AS fetched
  FROM distributed_result_info GROUP BY nodeport ORDER BY nodeport;
-- read all fetched result files
SELECT count(*), sum(x) FROM
  read_intermediate_results((SELECT array_agg(resultId) FROM distributed_result_info),
                            'binary') AS res (x int);
END;

DROP TABLE source_table, target_table, distributed_result_info;

--
-- partition_task_list_results, range partitioning, text format
--
CREATE TABLE source_table(a int);
SELECT create_distributed_table('source_table', 'a', 'range');
CALL public.create_range_partitioned_shards('source_table',
                                            '{0,25,50,76}',
                                            '{24,49,75,200}');
INSERT INTO source_table SELECT * FROM generate_series(1, 100);

CREATE TABLE target_table(a int);
SELECT create_distributed_table('target_table', 'a', 'range');
CALL public.create_range_partitioned_shards('target_table',
                                            '{0,25,50,76}',
                                            '{24,49,75,200}');

BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT resultId, nodeport, rowcount, targetShardId, targetShardIndex
  FROM partition_task_list_results('test', $$ SELECT (3 * a * a) % 100 FROM source_table $$,
                                   'target_table', false)
          NATURAL JOIN pg_dist_node;
SELECT * FROM distributed_result_info ORDER BY resultId;
-- fetch from workers
SELECT nodeport, fetch_intermediate_results((array_agg(resultId)), 'localhost', nodeport) > 0 AS fetched
  FROM distributed_result_info GROUP BY nodeport ORDER BY nodeport;
-- Read all fetched result files. Sum(x) should be 4550, verified by
-- racket -e '(for/sum ([i (range 1 101)]) (modulo (* 3 i i) 100))'
SELECT count(*), sum(x) FROM
  read_intermediate_results((SELECT array_agg(resultId) FROM distributed_result_info),
                            'text') AS res (x int);
END;

DROP TABLE source_table, target_table, distributed_result_info;

SET client_min_messages TO WARNING;
DROP SCHEMA distributed_intermediate_results CASCADE;

\set VERBOSITY default
SET client_min_messages TO DEFAULT;
SET citus.shard_count TO DEFAULT;
