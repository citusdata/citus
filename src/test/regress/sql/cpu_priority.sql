CREATE SCHEMA cpu_priority;
SET search_path TO cpu_priority;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 11568900;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

-- This test depends on the fact that CI and dev machines don't have their
-- resource limits configured in a way that allows raising CPU priority. This
-- still tries to test as much functionality as possible by expecting certain
-- error messages to appear, but it's not the environment production is
-- supposed to be running as.

CREATE USER cpu_priority_user1;
CREATE USER cpu_priority_user2;
CREATE USER cpu_priority_user3;
CREATE USER cpu_priority_user_background;
GRANT ALL ON SCHEMA cpu_priority to cpu_priority_user1;
GRANT ALL ON SCHEMA cpu_priority to cpu_priority_user2;
GRANT ALL ON SCHEMA cpu_priority to cpu_priority_user3;
GRANT ALL ON SCHEMA cpu_priority to cpu_priority_user_background;

CREATE TABLE t1(a int);
CREATE TABLE t2(a int);
CREATE TABLE t3(a int);
CREATE TABLE t4(a int);
SELECT create_distributed_table('t1', 'a');
SELECT create_distributed_table('t2', 'a');
SELECT create_distributed_table('t3', 'a');
SELECT create_distributed_table('t4', 'a');
INSERT INTO t1 SELECT generate_series(1, 100);
INSERT INTO t2 SELECT generate_series(1, 100);
INSERT INTO t3 SELECT generate_series(1, 100);
INSERT INTO t4 SELECT generate_series(1, 100);

ALTER TABLE t1 OWNER TO cpu_priority_user1;
ALTER TABLE t2 OWNER TO cpu_priority_user2;
ALTER TABLE t3 OWNER TO cpu_priority_user3;

-- gives a warning because this is not allowed
SET citus.cpu_priority = -20;
-- no-op should be allowed
SET citus.cpu_priority = 0;
-- lowering should be allowed
SET citus.cpu_priority = 1;
-- resetting should be allowed, but warn;
RESET citus.cpu_priority;

SET citus.propagate_set_commands = local;
BEGIN;
    SET LOCAL citus.cpu_priority = 10;
    SELECT count(*) FROM t1;
    -- warning is expected here because raising isn't allowed by the OS
COMMIT;

-- reconnect to get a new backend to reset our priority
\c - - - -
SET search_path TO cpu_priority;

-- Make sure shard moves use citus.cpu_priority_for_logical_replication_senders
-- in their CREATE SUBSCRIPTION commands.
SET citus.log_remote_commands TO ON;
SET citus.grep_remote_commands = '%CREATE SUBSCRIPTION%';
SELECT master_move_shard_placement(11568900, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');
SET citus.cpu_priority_for_logical_replication_senders = 15;
SELECT master_move_shard_placement(11568900, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical');
SET citus.max_high_priority_background_processes = 3;
SELECT master_move_shard_placement(11568900, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');
SELECT public.wait_for_resource_cleanup();

-- Make sure shard splits use citus.cpu_priority_for_logical_replication_senders
-- in their CREATE SUBSCRIPTION commands.
SELECT pg_catalog.citus_split_shard_by_split_points(
    11568900,
    ARRAY['-1500000000'],
    ARRAY[:worker_1_node, :worker_2_node],
    'force_logical');

ALTER USER cpu_priority_user_background SET citus.cpu_priority = 5;

\c - cpu_priority_user_background - -

show citus.cpu_priority;
show citus.cpu_priority_for_logical_replication_senders;
show citus.max_high_priority_background_processes;
-- not alowed to change any of the settings related to CPU priority
SET citus.cpu_priority = 4;
SET citus.cpu_priority = 6;
SET citus.cpu_priority_for_logical_replication_senders = 15;
SET citus.max_high_priority_background_processes = 3;


\c - postgres - -
SET search_path TO cpu_priority;

SET client_min_messages TO WARNING;
DROP SCHEMA cpu_priority CASCADE;
DROP USER cpu_priority_user1;
DROP USER cpu_priority_user2;
DROP USER cpu_priority_user3;
DROP USER cpu_priority_user_background;
