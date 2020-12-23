--
-- Test failures related to distributed_intermediate_results.c
--


CREATE SCHEMA failure_distributed_results;
SET search_path TO 'failure_distributed_results';

-- do not cache any connections
SET citus.max_cached_conns_per_worker TO 0;

-- we don't want to see the prepared transaction numbers in the warnings
SET client_min_messages TO WARNING;

SELECT citus.mitmproxy('conn.allow()');

SET citus.next_shard_id TO 100800;

-- always try the 1st replica before the 2nd replica.
SET citus.task_assignment_policy TO 'first-replica';

--
-- Case 1.
-- Source is replicated, target is not.
-- Fail the partition query on the first node.
--
CREATE TABLE source_table(a int);
SET citus.shard_replication_factor TO 2;
SET citus.shard_count TO 5;
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT * FROM generate_series(1, 100);

CREATE TABLE target_table(a int);
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('target_table', 'a');

-- without failure results from 100802 should be stored on 9060
BEGIN;
CREATE TABLE distributed_result_info AS
  SELECT resultId, nodeport, rowcount, targetShardId, targetShardIndex
  FROM partition_task_list_results('test', $$ SELECT * FROM source_table $$, 'target_table')
          NATURAL JOIN pg_dist_node;
SELECT * FROM distributed_result_info ORDER BY resultId;
SELECT fetch_intermediate_results('{test_from_100802_to_1,test_from_100802_to_2}'::text[], 'localhost', :worker_2_port) > 0 AS fetched;
SELECT count(*), sum(x) FROM
  read_intermediate_results('{test_from_100802_to_1,test_from_100802_to_2}'::text[],'binary') AS res (x int);
ROLLBACk;

-- with failure, results from 100802 should be retried and succeed on 57637
SELECT citus.mitmproxy('conn.onQuery(query="worker_partition_query_result.*test_from_100802").kill()');
BEGIN;
SET LOCAL client_min_messages TO DEBUG1;
CREATE TABLE distributed_result_info AS
  SELECT resultId, nodeport, rowcount, targetShardId, targetShardIndex
  FROM partition_task_list_results('test', $$ SELECT * FROM source_table $$, 'target_table')
          NATURAL JOIN pg_dist_node;
SELECT * FROM distributed_result_info ORDER BY resultId;
-- fetch from worker 2 should fail
SAVEPOINT s1;
SELECT fetch_intermediate_results('{test_from_100802_to_1,test_from_100802_to_2}'::text[], 'localhost', :worker_2_port) > 0 AS fetched;
ROLLBACK TO SAVEPOINT s1;
-- fetch from worker 1 should succeed
SELECT fetch_intermediate_results('{test_from_100802_to_1,test_from_100802_to_2}'::text[], 'localhost', :worker_1_port) > 0 AS fetched;
-- make sure the results read are same as the previous transaction block
SELECT count(*), sum(x) FROM
  read_intermediate_results('{test_from_100802_to_1,test_from_100802_to_2}'::text[],'binary') AS res (x int);
ROLLBACk;

SET search_path TO 'public';
RESET citus.shard_replication_factor;
RESET citus.shard_count;
RESET citus.task_assignment_policy;
DROP SCHEMA failure_distributed_results CASCADE;
RESET client_min_messages;
