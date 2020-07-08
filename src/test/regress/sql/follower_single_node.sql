\c - - - :master_port
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

-- connect to the follower and check that a simple select query works, the follower
-- is still in the default cluster and will send queries to the primary nodes
\c - - - :follower_master_port
SELECT * FROM test WHERE x = 1;
SELECT count(*) FROM test;
SELECT * FROM test ORDER BY x;

SELECT count(*) FROM ref;
SELECT * FROM ref ORDER BY a;
SELECT * FROM test, ref WHERE x = a ORDER BY x;

SELECT count(*) FROM local;
SELECT * FROM local ORDER BY c;
SELECT * FROM ref, local WHERE a = c ORDER BY a;

-- Check repartion joins are support
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
RESET citus.enable_repartition_joins;

-- Confirm that dummy placements work
EXPLAIN SELECT sum(x) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
-- Confirm that they work with round-robin task assignment policy
SET citus.task_assignment_policy TO 'round-robin';
EXPLAIN SELECT sum(x) FROM test WHERE false GROUP BY GROUPING SETS (x,y);

-- now, connect to the follower but tell it to use secondary nodes. There are no
-- secondary nodes so this should fail.

-- (this is :follower_master_port but substitution doesn't work here)
\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always'"

SELECT * FROM test WHERE x = 1;

-- add the the follower as secondary nodes and try again, the SELECT statement
-- should work this time
\c - - - :master_port

SELECT 1 FROM master_add_node('localhost', :follower_master_port, groupid => 0, noderole => 'secondary');
SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always'"

SELECT * FROM test WHERE x = 1;
SELECT count(*) FROM test;
SELECT * FROM test ORDER BY x;

SELECT count(*) FROM ref;
SELECT * FROM ref ORDER BY a;
SELECT * FROM test, ref WHERE x = a ORDER BY x;

SELECT count(*) FROM local;
SELECT * FROM local ORDER BY c;
SELECT * FROM ref, local WHERE a = c ORDER BY a;

-- Check repartion joins are support
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
RESET citus.enable_repartition_joins;

-- Confirm that dummy placements work
EXPLAIN SELECT sum(x) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
-- Confirm that they work with round-robin task assignment policy
SET citus.task_assignment_policy TO 'round-robin';
EXPLAIN SELECT sum(x) FROM test WHERE false GROUP BY GROUPING SETS (x,y);

-- Cleanup
\c - - - :master_port
DROP TABLE test, ref, local;
-- Remove the coordinator again
SELECT 1 FROM master_remove_node('localhost', :master_port);
-- Remove the secondary coordinator again
SELECT 1 FROM master_remove_node('localhost', :follower_master_port);
