\c - - - :master_port
CREATE SCHEMA single_node;
SET search_path TO single_node;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 93630500;

SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

CREATE TABLE test(x int, y int);
SELECT create_distributed_table('test','x');
CREATE TABLE ref(a int, b int);
SELECT create_reference_table('ref');
CREATE TABLE local(c int, d int);

INSERT INTO test VALUES (1, 2), (3, 4), (5, 6), (2, 7), (4, 5);
INSERT INTO ref VALUES (1, 2), (5, 6), (7, 8);
INSERT INTO local VALUES (1, 2), (3, 4), (7, 8);

-- Check repartition joins are supported
SET citus.enable_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
RESET citus.enable_single_hash_repartition_joins;

SET citus.task_assignment_policy TO 'round-robin';
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET citus.task_assignment_policy TO 'greedy';
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET citus.task_assignment_policy TO 'first-replica';
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

RESET citus.enable_repartition_joins;



-- connect to the follower and check that a simple select query works, the follower
-- is still in the default cluster and will send queries to the primary nodes
\c - - - :follower_master_port
SET search_path TO single_node;
SELECT * FROM test WHERE x = 1;
SELECT count(*) FROM test;
SELECT * FROM test ORDER BY x;

SELECT count(*) FROM ref;
SELECT * FROM ref ORDER BY a;
SELECT * FROM test, ref WHERE x = a ORDER BY x;

SELECT count(*) FROM local;
SELECT * FROM local ORDER BY c;
SELECT * FROM ref, local WHERE a = c ORDER BY a;

SET citus.enable_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET citus.task_assignment_policy TO 'round-robin';
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET citus.task_assignment_policy TO 'greedy';
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET citus.task_assignment_policy TO 'first-replica';
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

RESET citus.enable_repartition_joins;
RESET citus.enable_single_hash_repartition_joins;

-- Confirm that dummy placements work
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
-- Confirm that they work with round-robin task assignment policy
SET citus.task_assignment_policy TO 'round-robin';
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
SET citus.task_assignment_policy TO 'greedy';
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
SET citus.task_assignment_policy TO 'first-replica';
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
RESET citus.task_assignment_policy;


-- now, connect to the follower but tell it to use secondary nodes. There are no
-- secondary nodes so this should fail.

-- (this is :follower_master_port but substitution doesn't work here)
\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always'"
SET search_path TO single_node;

SELECT * FROM test WHERE x = 1;

-- add the the follower as secondary nodes and try again, the SELECT statement
-- should work this time
\c -reuse-previous=off regression - - :master_port
SET search_path TO single_node;

SELECT 1 FROM master_add_node('localhost', :follower_master_port, groupid => 0, noderole => 'secondary', nodecluster => 'second-cluster');
SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always\ -c\ citus.cluster_name=second-cluster'"
SET search_path TO single_node;

SELECT * FROM test WHERE x = 1;
SELECT count(*) FROM test;
SELECT * FROM test ORDER BY x;

SELECT count(*) FROM ref;
SELECT * FROM ref ORDER BY a;
SELECT * FROM test, ref WHERE x = a ORDER BY x;

SELECT count(*) FROM local;
SELECT * FROM local ORDER BY c;
SELECT * FROM ref, local WHERE a = c ORDER BY a;

SET citus.enable_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
RESET citus.enable_repartition_joins;
RESET citus.enable_single_hash_repartition_joins;

-- Confirm that dummy placements work
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
-- Confirm that they work with round-robin task assignment policy
SET citus.task_assignment_policy TO 'round-robin';
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
RESET citus.task_assignment_policy;

-- Simple columnar follower test
\c -reuse-previous=off regression - - :master_port

CREATE TABLE columnar_test (a int, b int) USING columnar;

INSERT INTO columnar_test(a, b) VALUES (1, 1);
INSERT INTO columnar_test(a, b) VALUES (1, 2);
TRUNCATE columnar_test;
INSERT INTO columnar_test(a, b) VALUES (1, 3);
INSERT INTO columnar_test(a, b) VALUES (1, 4);
BEGIN;
INSERT INTO columnar_test SELECT g, g*10
  FROM generate_series(10001,20000) g;
ROLLBACK;
VACUUM columnar_test;
INSERT INTO columnar_test(a, b) VALUES (1, 5);
INSERT INTO columnar_test(a, b) VALUES (1, 6);
VACUUM FULL columnar_test;
INSERT INTO columnar_test(a, b) VALUES (1, 7);
INSERT INTO columnar_test(a, b) VALUES (1, 8);

\c - - - :follower_master_port
SELECT * FROM columnar_test ORDER BY 1,2;


\c -reuse-previous=off regression - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path TO single_node;

CREATE TABLE dist_table (a INT, b INT);
SELECT create_distributed_table ('dist_table', 'a', shard_count:=4);
INSERT INTO dist_table VALUES (1, 1);

\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always\ -c\ citus.cluster_name=second-cluster'"
SET search_path TO single_node;

SELECT * FROM dist_table;

SELECT global_pid AS follower_coordinator_gpid FROM get_all_active_transactions() WHERE process_id = pg_backend_pid() \gset
SELECT pg_typeof(:follower_coordinator_gpid);

SELECT pg_cancel_backend(:follower_coordinator_gpid);

SET citus.log_remote_commands TO ON;
SELECT pg_cancel_backend(:follower_coordinator_gpid) FROM dist_table WHERE a = 1;


-- Cleanup
\c -reuse-previous=off regression - - :master_port
SET search_path TO single_node;
SET client_min_messages TO WARNING;
DROP SCHEMA single_node CASCADE;
-- Remove the coordinator again
SELECT 1 FROM master_remove_node('localhost', :master_port);
-- Remove the secondary coordinator again
SELECT 1 FROM master_remove_node('localhost', :follower_master_port);
