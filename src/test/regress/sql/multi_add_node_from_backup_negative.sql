--
-- Test for negative scenarios in clone promotion functionality
--

--try to add follower_worker_1 as a clone of worker_1 to the cluster
-- this should fail as previous test has already promoted worker_1 to a primary node
SELECT citus_add_clone_node('localhost', :follower_worker_1_port, 'localhost', :worker_1_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;

--try to add worker_node2 as a clone of worker_node1
-- this should fail as it is not a valid replica of worker_1
SELECT citus_add_clone_node('localhost', :follower_worker_2_port, 'localhost', :worker_1_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;

--try add
-- create a distributed table and load data
CREATE TABLE backup_test(id int, value text);
SELECT create_distributed_table('backup_test', 'id', 'hash');
INSERT INTO backup_test SELECT g, 'test' || g FROM generate_series(1, 10) g;

-- Create reference table
CREATE TABLE ref_table(id int PRIMARY KEY);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table SELECT i FROM generate_series(1, 5) i;

SELECT COUNT(*) from backup_test;
SELECT COUNT(*) from ref_table;

-- verify initial shard placement
SELECT nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;

-- Try to add replica of worker_node2 as a clone of worker_node1
SELECT citus_add_clone_node('localhost', :follower_worker_2_port, 'localhost', :worker_1_port) AS clone_node_id \gset

-- Test 1: Try to promote a non-existent clone node
SELECT citus_promote_clone_and_rebalance(clone_nodeid =>99999);

-- Test 2: Try to promote a regular worker node (not a clone)
SELECT citus_promote_clone_and_rebalance(clone_nodeid => 1);

-- Test 3: Try to promote with invalid timeout (negative)
SELECT citus_promote_clone_and_rebalance(clone_nodeid => 1,
         catchup_timeout_seconds => -100);

-- register the new node as a clone, This should pass
SELECT citus_add_clone_node('localhost', :follower_worker_2_port, 'localhost', :worker_2_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;

SELECT :clone_node_id;

\c - - - :follower_worker_2_port
SET statement_timeout = '60s';
DO $$
BEGIN
    PERFORM pg_wal_replay_pause();
    WHILE pg_get_wal_replay_pause_state() <> 'paused' LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;

\c - - - :worker_2_port
CREATE TABLE promotion_catchup_wal (id integer);
DROP TABLE promotion_catchup_wal;

\c - - - :master_port
SET client_min_messages = WARNING;
-- the promotion waits for the clone, whose replay is paused, until its
-- catch-up timeout, or until statement_timeout cancels it
SET statement_timeout = '60s';
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id, catchup_timeout_seconds => 1);
SET statement_timeout = '1s';
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id, catchup_timeout_seconds => 0);
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id, catchup_timeout_seconds => 30);
SET statement_timeout = '60s';
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id, catchup_timeout_seconds => 1);
RESET statement_timeout;
RESET client_min_messages;

-- none of them changed the metadata of the clone
SELECT nodeisclone AND NOT isactive AS inactive_clone FROM pg_dist_node WHERE nodeid = :clone_node_id;

\c - - - :follower_worker_2_port
DO $$
BEGIN
    IF NOT pg_is_in_recovery() THEN
        RAISE EXCEPTION 'timeout promoted the clone';
    END IF;
    PERFORM pg_wal_replay_resume();
END $$;
\c - - - :master_port

-- Test 4: Try to promote clone with invalid strategy name
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id, rebalance_strategy => 'invalid_strategy');

SELECT * from pg_dist_node ORDER by nodeid;

-- the strategy is checked before pg_promote(), so the clone is still a replica
\c - - - :follower_worker_2_port
SELECT pg_is_in_recovery() AS clone_in_recovery;
\c - - - :master_port

-- Transaction blocks are rejected, but a function or a DO block can still run
-- other commands before the promotion in its transaction. The promotion errors
-- out after them, since their changes would only commit after it: changes of
-- data, of metadata only, and local ones.
DO $$
BEGIN
    INSERT INTO backup_test VALUES (11, 'test11');
    PERFORM citus_promote_clone_and_rebalance(nodeid) FROM pg_dist_node WHERE nodeisclone;
END $$;
DO $$
BEGIN
    PERFORM citus_set_node_property(nodename, nodeport, 'shouldhaveshards', false)
    FROM pg_dist_node WHERE nodeid = 1;
    PERFORM citus_promote_clone_and_rebalance(nodeid) FROM pg_dist_node WHERE nodeisclone;
END $$;
DO $$
BEGIN
    CREATE TABLE promotion_local_table (id int);
    PERFORM citus_promote_clone_and_rebalance(nodeid) FROM pg_dist_node WHERE nodeisclone;
END $$;

\c - - - :follower_worker_2_port
SELECT pg_is_in_recovery() AS clone_in_recovery;
\c - - - :master_port

-- Test 9: The promotion cannot run inside a transaction block, where its write
-- fence would last until the transaction ends, and a rollback could not undo
-- pg_promote(), so the clone stays a replica
BEGIN;
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id);
ROLLBACK;

-- Verify no data is lost
SELECT COUNT(*) from backup_test;
SELECT COUNT(*) from ref_table;

SELECT * from pg_dist_node ORDER by nodeid;

\c - - - :follower_worker_2_port
SELECT pg_is_in_recovery() AS clone_in_recovery;
\c - - - :master_port

-- The promotion also errors out, and the clone stays a replica, if the primary
-- has placements of unlogged distributed or reference tables, since physical
-- replication does not copy their data.
CREATE UNLOGGED TABLE unlogged_dist (id int);
SELECT create_distributed_table('unlogged_dist', 'id', shard_count => 8);
SELECT count(*) > 0 AS primary_has_unlogged_shards
FROM pg_dist_shard_placement JOIN pg_dist_shard USING (shardid)
WHERE logicalrelid = 'unlogged_dist'::regclass AND nodeport = :worker_2_port;
SET client_min_messages TO WARNING;
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id);
RESET client_min_messages;
DROP TABLE unlogged_dist;
CREATE UNLOGGED TABLE unlogged_ref (id int);
SELECT create_reference_table('unlogged_ref');
INSERT INTO unlogged_ref SELECT generate_series(1, 10);
SET client_min_messages TO WARNING;
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id);
RESET client_min_messages;

-- once the table is logged, its data reaches the clone
ALTER TABLE unlogged_ref SET LOGGED;

-- A partitioned table has no data of its own, so it does not stop the
-- promotion. Before PostgreSQL 18, one can be unlogged while its partitions
-- are not, and cannot be made logged.
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 18 AS server_version_ge_18 \gset
\if :server_version_ge_18
CREATE TABLE unlogged_parent (id int) PARTITION BY RANGE (id);
\else
CREATE UNLOGGED TABLE unlogged_parent (id int) PARTITION BY RANGE (id);
\endif
CREATE TABLE logged_part PARTITION OF unlogged_parent FOR VALUES FROM (0) TO (100);
SELECT create_distributed_table('unlogged_parent', 'id', shard_count => 8);
INSERT INTO unlogged_parent SELECT generate_series(0, 99);

-- A standalone promotion of the same clone succeeds. The clone gets an identity
-- of its own, so it does not act as worker_2, e.g. in 2PC recovery.
SET client_min_messages TO WARNING;
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id);
RESET client_min_messages;
SELECT isactive, nodeisclone FROM pg_dist_node WHERE nodeid = :clone_node_id;
SELECT groupid AS clone_group_id FROM pg_dist_node WHERE nodeid = :clone_node_id \gset
\c - - - :worker_2_port
SELECT metadata->>'server_id' AS primary_server_id FROM pg_dist_node_metadata \gset
\c - - - :follower_worker_2_port
SELECT pg_is_in_recovery() AS clone_in_recovery, groupid = :clone_group_id AS clone_has_own_group_id,
       metadata->>'server_id' <> :'primary_server_id' AS clone_has_own_server_id
FROM pg_dist_local_group, pg_dist_node_metadata;
SELECT count(*) AS logged_ref_rows FROM unlogged_ref;
SELECT count(*) AS partitioned_rows FROM unlogged_parent;
\c - - - :master_port
DROP TABLE unlogged_ref;
DROP TABLE unlogged_parent;

-- place the shards of the tables that the next tests create as before
SELECT 1 FROM citus_set_node_property('localhost', :follower_worker_2_port, 'shouldhaveshards', false);

-- Test 5: Try to add and promote another proper replica
SELECT master_add_node('localhost', :worker_3_port) AS nodeid_3 \gset
SELECT citus_add_clone_node('localhost', :follower_worker_3_port, 'localhost', :worker_3_port) AS clone_node_id_3 \gset

set citus.shard_count = 100;
CREATE TABLE backup_test2(id int, value text);
SELECT create_distributed_table('backup_test2', 'id', 'hash');
INSERT INTO backup_test2 SELECT g, 'test' || g FROM generate_series(1, 10) g;

-- Create reference table
CREATE TABLE ref_table2(id int PRIMARY KEY);
SELECT create_reference_table('ref_table2');
INSERT INTO ref_table2 SELECT i FROM generate_series(1, 5) i;

SELECT * from get_snapshot_based_node_split_plan('localhost', :worker_3_port, 'localhost', :follower_worker_3_port);

SET client_min_messages to 'LOG';
SET statement_timeout = '120s';
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id_3, catchup_timeout_seconds => 0);
RESET statement_timeout;
SET client_min_messages to DEFAULT;

SELECT COUNT(*) from backup_test;
SELECT COUNT(*) from ref_table;

SELECT * from pg_dist_node ORDER by nodeid;

-- check the shard placement

SELECT nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;

set citus.shard_count to default;

-- cleanup
DROP TABLE backup_test;
DROP TABLE ref_table;
DROP TABLE backup_test2;
DROP TABLE ref_table2;
