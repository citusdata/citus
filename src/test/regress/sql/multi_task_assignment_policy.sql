--
-- MULTI_TASK_ASSIGNMENT
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 880000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 880000;

SET citus.explain_distributed_queries TO off;


-- Check that our policies for assigning tasks to worker nodes run as expected.
-- To test this, we first create a shell table, and then manually insert shard
-- and shard placement data into system catalogs. We next run Explain command,
-- and check that tasks are assigned to worker nodes as expected.

CREATE TABLE task_assignment_test_table (test_id integer);
SELECT master_create_distributed_table('task_assignment_test_table', 'test_id', 'append');

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

-- Round-robin task assignment relies on the current jobId. We therefore need to
-- ensure that jobIds start with an odd number here; this way, task assignment
-- debug messages always produce the same output. Also, we make sure that the
-- following case statement always prints out "1" as the query's result.

SELECT case when (currval('pg_dist_jobid_seq') % 2) = 0
       	    then nextval('pg_dist_jobid_seq') % 2
	    else 1 end;

-- Finally test the round-robin task assignment policy

SET citus.task_assignment_policy TO 'round-robin';

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

RESET citus.task_assignment_policy;
RESET client_min_messages;

COMMIT;
