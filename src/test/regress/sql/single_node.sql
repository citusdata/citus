CREATE SCHEMA single_node;
SET search_path TO single_node;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 90630500;

-- idempotently add node to allow this test to run without add_coordinator
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

CREATE TABLE test(x int, y int);
SELECT create_distributed_table('test','x');
CREATE TABLE ref(a int, b int);
SELECT create_reference_table('ref');
CREATE TABLE local(c int, d int);

-- Confirm the basics work
INSERT INTO test VALUES (1, 2), (3, 4), (5, 6), (2, 7), (4, 5);
SELECT * FROM test WHERE x = 1;
SELECT count(*) FROM test;
SELECT * FROM test ORDER BY x;

INSERT INTO ref VALUES (1, 2), (5, 6), (7, 8);
SELECT count(*) FROM ref;
SELECT * FROM ref ORDER BY a;
SELECT * FROM test, ref WHERE x = a ORDER BY x;

INSERT INTO local VALUES (1, 2), (3, 4), (7, 8);
SELECT count(*) FROM local;
SELECT * FROM local ORDER BY c;
SELECT * FROM ref, local WHERE a = c ORDER BY a;

-- Check repartion joins are support
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
RESET citus.enable_repartition_joins;

-- INSERT SELECT router
BEGIN;
INSERT INTO test(x, y) SELECT x, y FROM test WHERE x = 1;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT analytical query
BEGIN;
INSERT INTO test(x, y) SELECT count(x), max(y) FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT repartition
BEGIN;
INSERT INTO test(x, y) SELECT y, x FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from reference table into distributed
BEGIN;
INSERT INTO test(x, y) SELECT a, b FROM ref;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from local table into distributed
BEGIN;
INSERT INTO test(x, y) SELECT c, d FROM local;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT x, y FROM test;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT c, d FROM local;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT x, y FROM test;
SELECT count(*) from local;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT a, b FROM ref;
SELECT count(*) from local;
ROLLBACK;

-- Confirm that dummy placements work
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
-- Confirm that they work with round-robin task assignment policy
SET citus.task_assignment_policy TO 'round-robin';
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
RESET citus.task_assignment_policy;


-- Cleanup
SET client_min_messages TO WARNING;
DROP SCHEMA single_node CASCADE;
-- Remove the coordinator again
SELECT 1 FROM master_remove_node('localhost', :master_port);
-- restart nodeid sequence so that multi_cluster_management still has the same
-- nodeids
ALTER SEQUENCE pg_dist_node_nodeid_seq RESTART 1;
