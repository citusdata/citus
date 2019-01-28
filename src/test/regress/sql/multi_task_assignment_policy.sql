--
-- MULTI_TASK_ASSIGNMENT
--


SET citus.next_shard_id TO 880000;

-- print whether we're using version > 9 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 9 AS version_above_nine;


SET citus.explain_distributed_queries TO off;


-- Check that our policies for assigning tasks to worker nodes run as expected.
-- To test this, we first create a shell table, and then manually insert shard
-- and shard placement data into system catalogs. We next run Explain command,
-- and check that tasks are assigned to worker nodes as expected.

CREATE TABLE task_assignment_test_table (test_id integer);
SELECT create_distributed_table('task_assignment_test_table', 'test_id', 'append');

-- Create logical shards with shardids 200, 201, and 202

INSERT INTO pg_dist_shard (logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue)
	SELECT pg_class.oid, series.index, 'r', 1, 1000
	FROM pg_class, generate_series(200, 202) AS series(index)
	WHERE pg_class.relname = 'task_assignment_test_table';

-- Create shard placements for shard 200 and 201

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 200, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 2;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 201, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 2;

-- Create shard placements for shard 202

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 202, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport DESC
       LIMIT 2;

-- Start transaction block to avoid auto commits. This avoids additional debug
-- messages from getting printed at real transaction starts and commits.

BEGIN;

-- Increase log level to see which worker nodes tasks are assigned to. Note that
-- the following log messages print node name and port numbers; and node numbers
-- in regression tests depend upon PG_VERSION_NUM.

SET client_min_messages TO DEBUG3;

-- First test the default greedy task assignment policy

SET citus.task_assignment_policy TO 'greedy';

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

-- Next test the first-replica task assignment policy

SET citus.task_assignment_policy TO 'first-replica';

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

-- Finally test the round-robin task assignment policy

SET citus.task_assignment_policy TO 'round-robin';

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

RESET citus.task_assignment_policy;
RESET client_min_messages;

COMMIT;

BEGIN;

SET LOCAL client_min_messages TO DEBUG3;
SET LOCAL citus.explain_distributed_queries TO off;

-- Check how task_assignment_policy impact planning decisions for reference tables

CREATE TABLE task_assignment_reference_table (test_id  integer);
SELECT create_reference_table('task_assignment_reference_table');

SET LOCAL citus.task_assignment_policy TO 'greedy';
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;

SET LOCAL citus.task_assignment_policy TO 'first-replica';
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;

-- here we expect debug output showing two different hosts for subsequent queries
SET LOCAL citus.task_assignment_policy TO 'round-robin';
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;

ROLLBACK;



-- we should be able to use round-robin with router queries that 
-- only contains intermediate results
BEGIN;
CREATE TABLE task_assignment_test_table_2 (test_id integer);
SELECT create_distributed_table('task_assignment_test_table_2', 'test_id');

WITH q1 AS (SELECT * FROM task_assignment_test_table_2) SELECT * FROM q1;
SET LOCAL citus.task_assignment_policy TO 'round-robin';
WITH q1 AS (SELECT * FROM task_assignment_test_table_2) SELECT * FROM q1;
ROLLBACK;


