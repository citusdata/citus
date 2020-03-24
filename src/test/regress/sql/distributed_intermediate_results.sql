-- Test functions for partitioning intermediate results
CREATE SCHEMA distributed_intermediate_results;
SET search_path TO 'distributed_intermediate_results';

SET citus.next_shard_id TO 4213581;
SET citus.shard_replication_factor TO 1;

-- redistribute_task_list_results test the internal RedistributeTaskListResult
CREATE OR REPLACE FUNCTION pg_catalog.redistribute_task_list_results(resultIdPrefix text,
                                                                     query text,
                                                                     target_table regclass,
                                                                     binaryFormat bool DEFAULT true)
    RETURNS TABLE(shardid bigint,
                  colocated_results text[])
    LANGUAGE C STRICT VOLATILE
    AS 'citus', $$redistribute_task_list_results$$;

--
-- We don't have extensive tests for partition_task_results or
-- redistribute_task_list_results, since they will be tested by higher level
-- "INSERT/SELECT with repartitioning" tests anyway.
--

--
-- Case 1.
-- hash partitioning, binary format
-- * partition_task_list_results
-- * redistribute_task_list_results
--

CREATE TABLE source_table(a int);
SET citus.shard_count TO 3;
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT * FROM generate_series(1, 100);

CREATE TABLE target_table(a int);
SET citus.shard_count TO 2;
SELECT create_distributed_table('target_table', 'a');

CREATE TABLE colocated_with_target(a int);
SELECT create_distributed_table('colocated_with_target', 'a');
-- one value per shard, so we can route calls to read_intermediate_shards
INSERT INTO colocated_with_target VALUES (1), (2);

-- partition_task_list_results

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
ROLLBACK;

-- redistribute_task_list_results
-- Verify that redistribute_task_list_results colocated fragments properly by reading the
-- expected colocated results on the same node as each of two shards.
BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT * FROM redistribute_task_list_results('test', $$ SELECT * FROM source_table $$, 'target_table');
SELECT * FROM distributed_result_info ORDER BY shardid;
WITH shard_1 AS (
    SELECT t.* FROM colocated_with_target, (
      SELECT * FROM read_intermediate_results('{test_from_4213581_to_0,test_from_4213582_to_0}'::text[], 'binary') AS res (x int)) t
      WHERE colocated_with_target.a = 1
), shard_2 AS (
    SELECT t.* FROM colocated_with_target, (
      SELECT * FROM read_intermediate_results('{test_from_4213582_to_1,test_from_4213583_to_1}'::text[], 'binary') AS res (x int)) t
      WHERE colocated_with_target.a = 2
), all_rows AS (
    (SELECT * FROM shard_1) UNION (SELECT * FROM shard_2)
)
SELECT count(*), sum(x) FROM all_rows;
ROLLBACK;

DROP TABLE source_table, target_table, colocated_with_target;

--
-- Case 2.
-- range partitioning, text format
-- * partition_task_list_results
-- * redistribute_task_list_results
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
CREATE TABLE colocated_with_target(a int);
SELECT create_distributed_table('colocated_with_target', 'a', 'range');
CALL public.create_range_partitioned_shards('colocated_with_target',
                                            '{0,25,50,76}',
                                            '{24,49,75,200}');
-- one value per shard, so we can route calls to read_intermediate_shards
INSERT INTO colocated_with_target VALUES (1), (26), (51), (77);

-- partition_task_list_results
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
ROLLBACK;

-- redistribute_task_list_results
-- Verify that redistribute_task_list_results colocated fragments properly by reading the
-- expected colocated results on the same node as each of two shards.
BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT * FROM redistribute_task_list_results('test', $$ SELECT (3 * a * a) % 100 FROM source_table $$, 'target_table');
SELECT * FROM distributed_result_info ORDER BY shardid;

WITH shard_1 AS (
    SELECT t.* FROM colocated_with_target, (
      SELECT * FROM read_intermediate_results('{test_from_4213588_to_0,test_from_4213589_to_0,test_from_4213590_to_0,test_from_4213591_to_0}'::text[], 'binary') AS res (x int)) t
      WHERE colocated_with_target.a = 1
), shard_2 AS (
    SELECT t.* FROM colocated_with_target, (
      SELECT * FROM read_intermediate_results('{test_from_4213588_to_1,test_from_4213589_to_1,test_from_4213590_to_1,test_from_4213591_to_1}'::text[], 'binary') AS res (x int)) t
      WHERE colocated_with_target.a = 26
), shard_3 AS (
    SELECT t.* FROM colocated_with_target, (
      SELECT * FROM read_intermediate_results('{test_from_4213588_to_2,test_from_4213589_to_2,test_from_4213590_to_2,test_from_4213591_to_2}'::text[], 'binary') AS res (x int)) t
      WHERE colocated_with_target.a = 51
), shard_4 AS (
    SELECT t.* FROM colocated_with_target, (
      SELECT * FROM read_intermediate_results('{test_from_4213588_to_3,test_from_4213589_to_3,test_from_4213590_to_3,test_from_4213591_to_3}'::text[], 'binary') AS res (x int)) t
      WHERE colocated_with_target.a = 77
), all_rows AS (
    (SELECT * FROM shard_1) UNION ALL (SELECT * FROM shard_2) UNION ALL
    (SELECT * FROM shard_3) UNION ALL (SELECT * FROM shard_4)
)
SELECT count(*), sum(x) FROM all_rows;
ROLLBACK;

DROP TABLE source_table, target_table, colocated_with_target;


--
-- Case 3.
-- range partitioning, text format, replication factor 2 (both source and destination)
-- composite distribution column
--
-- only redistribute_task_list_results
--
CREATE TYPE composite_key_type AS (f1 int, f2 text);
SET citus.shard_replication_factor TO 2;

-- source
CREATE TABLE source_table(key composite_key_type, value int, mapped_key composite_key_type);
SELECT create_distributed_table('source_table', 'key', 'range');
CALL public.create_range_partitioned_shards('source_table', '{"(0,a)","(25,a)"}','{"(24,z)","(49,z)"}');

INSERT INTO source_table VALUES ((0, 'a'), 1, (0, 'a'));  -- shard 1 -> shard 1
INSERT INTO source_table VALUES ((1, 'b'), 2, (26, 'b')); -- shard 1 -> shard 2
INSERT INTO source_table VALUES ((2, 'c'), 3, (3, 'c'));  -- shard 1 -> shard 1
INSERT INTO source_table VALUES ((4, 'd'), 4, (27, 'd')); -- shard 1 -> shard 2
INSERT INTO source_table VALUES ((30, 'e'), 5, (30, 'e')); -- shard 2 -> shard 2
INSERT INTO source_table VALUES ((31, 'f'), 6, (31, 'f')); -- shard 2 -> shard 2
INSERT INTO source_table VALUES ((32, 'g'), 7, (8, 'g'));  -- shard 2 -> shard 1

-- target
CREATE TABLE target_table(key composite_key_type, value int);
SELECT create_distributed_table('target_table', 'key', 'range');
CALL public.create_range_partitioned_shards('target_table', '{"(0,a)","(25,a)"}','{"(24,z)","(49,z)"}');

-- colocated with target, used for routing calls to read_intermediate_results
CREATE TABLE colocated_with_target(key composite_key_type, value_sum int);
SELECT create_distributed_table('colocated_with_target', 'key', 'range');
CALL public.create_range_partitioned_shards('colocated_with_target', '{"(0,a)","(25,a)"}','{"(24,z)","(49,z)"}');
-- one value per shard, so we can route calls to read_intermediate_shards
INSERT INTO colocated_with_target VALUES ((0,'a'), 0);
INSERT INTO colocated_with_target VALUES ((25, 'a'), 0);

BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT * FROM redistribute_task_list_results('test', $$ SELECT mapped_key, value FROM source_table $$, 'target_table');
SELECT * FROM distributed_result_info ORDER BY shardid;

UPDATE colocated_with_target SET value_sum=(SELECT sum(y) FROM read_intermediate_results('{test_from_4213600_to_0,test_from_4213601_to_0}'::text[], 'binary') AS res (x composite_key_type, y int))
  WHERE key=(0,'a')::composite_key_type;
UPDATE colocated_with_target SET value_sum=(SELECT sum(y) FROM read_intermediate_results('{test_from_4213600_to_1,test_from_4213601_to_1}'::text[], 'binary') AS res (x composite_key_type, y int))
  WHERE key=(25,'a')::composite_key_type;

SELECT * FROM colocated_with_target ORDER BY key;

END;

-- verify that replicas of colocated_with_target are consistent (i.e. copies
-- of result files in both nodes were same when calling read_intermediate_results()
-- in the above UPDATE calls).

\c - - :public_worker_1_host :worker_1_port
SELECT * FROM distributed_intermediate_results.colocated_with_target_4213604 ORDER BY key;
SELECT * FROM distributed_intermediate_results.colocated_with_target_4213605 ORDER BY key;

\c - - :public_worker_2_host :worker_2_port
SELECT * FROM distributed_intermediate_results.colocated_with_target_4213604 ORDER BY key;
SELECT * FROM distributed_intermediate_results.colocated_with_target_4213605 ORDER BY key;

\c - - :master_host :master_port

SET search_path TO 'distributed_intermediate_results';
DROP TABLE source_table, target_table, colocated_with_target, distributed_result_info;
DROP TYPE composite_key_type;

--
-- Case 4. target relation is a reference table or an append partitioned table
--

CREATE TABLE source_table(a int);
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT * FROM generate_series(1, 100);

CREATE TABLE target_table_reference(a int);
SELECT create_reference_table('target_table_reference');

CREATE TABLE target_table_append(a int);
SELECT create_distributed_table('target_table_append', 'a', 'append');

BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT * FROM redistribute_task_list_results('test', $$ SELECT * FROM source_table $$, 'target_table_reference');
ROLLBACK;

BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT * FROM redistribute_task_list_results('test', $$ SELECT * FROM source_table $$, 'target_table_append');
ROLLBACK;

-- clean-up

SET client_min_messages TO WARNING;
DROP SCHEMA distributed_intermediate_results CASCADE;

\set VERBOSITY default
SET client_min_messages TO DEFAULT;
SET citus.shard_count TO DEFAULT;
SET citus.shard_replication_factor TO DEFAULT;
