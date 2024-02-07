--
-- MUTLI_SHARD_REBALANCER
--

SET citus.next_shard_id TO 433000;
SET citus.propagate_session_settings_for_loopback_connection TO ON;

-- Because of historic reasons this test was written in a way that assumes that
-- by_shard_count is the default strategy.
SELECT citus_set_default_rebalance_strategy('by_shard_count');
-- Lower the minimum disk size that a shard group is considered as. Otherwise
-- we need to create shards of more than 100MB.
ALTER SYSTEM SET citus.rebalancer_by_disk_size_base_cost = 0;
SELECT pg_reload_conf();

CREATE TABLE ref_table_test(a int primary key);
SELECT create_reference_table('ref_table_test');
CREATE TABLE dist_table_test(a int primary key);
SELECT create_distributed_table('dist_table_test', 'a');
CREATE TABLE postgres_table_test(a int primary key);

-- make sure that all rebalance operations works fine when
-- reference tables are replicated to the coordinator
SET client_min_messages TO ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId=>0);
RESET client_min_messages;

-- should just be noops even if we add the coordinator to the pg_dist_node
SELECT rebalance_table_shards('dist_table_test');
CALL citus_cleanup_orphaned_resources();
SELECT rebalance_table_shards();
CALL citus_cleanup_orphaned_resources();


-- test that calling rebalance_table_shards without specifying relation
-- wouldn't move shard of the citus local table.
SET citus.next_shard_id TO 433100;
CREATE TABLE citus_local_table(a int, b int);
SELECT citus_add_local_table_to_metadata('citus_local_table');
INSERT INTO citus_local_table VALUES (1, 2);

SELECT rebalance_table_shards();
CALL citus_cleanup_orphaned_resources();

-- Check that rebalance_table_shards and get_rebalance_table_shards_plan fail
-- for any type of table, but distributed tables.
SELECT rebalance_table_shards('ref_table_test');
SELECT rebalance_table_shards('postgres_table_test');
SELECT rebalance_table_shards('citus_local_table');
SELECT get_rebalance_table_shards_plan('ref_table_test');
SELECT get_rebalance_table_shards_plan('postgres_table_test');
SELECT get_rebalance_table_shards_plan('citus_local_table');

-- Check that citus_move_shard_placement fails for shards belonging reference
-- tables or citus local tables
SELECT citus_move_shard_placement(433000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
SELECT citus_move_shard_placement(433100, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- show that citus local table shard is still on the coordinator
SELECT tablename FROM pg_catalog.pg_tables where tablename like 'citus_local_table_%';
-- also check that we still can access shard relation, not the shell table
SELECT count(*) FROM citus_local_table;

-- verify drain_node uses the localhostname guc by seeing it fail to connect to a non-existing name
ALTER SYSTEM SET citus.local_hostname TO 'foobar';
SELECT pg_reload_conf();
SELECT pg_sleep(.1); -- wait to make sure the config has changed before running the GUC

SELECT master_drain_node('localhost', :master_port);
CALL citus_cleanup_orphaned_resources();

ALTER SYSTEM RESET citus.local_hostname;
SELECT pg_reload_conf();
SELECT pg_sleep(.1); -- wait to make sure the config has changed before running the GUC

SELECT master_drain_node('localhost', :master_port);
CALL citus_cleanup_orphaned_resources();

-- show that citus local table shard is still on the coordinator
SELECT tablename FROM pg_catalog.pg_tables where tablename like 'citus_local_table_%';
-- also check that we still can access shard relation, not the shell table
SELECT count(*) FROM citus_local_table;

-- show that we do not create a shard rebalancing plan for citus local table
SELECT get_rebalance_table_shards_plan();

DROP TABLE citus_local_table;

CREATE TABLE dist_table_test_2(a int);
SET citus.shard_count TO 4;

SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('dist_table_test_2', 'a');

-- Mark tables as coordinator replicated in order to be able to test replicate_table_shards
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid IN
  ('dist_table_test_2'::regclass);

-- replicate_table_shards should fail when the hostname GUC is set to a non-reachable node
ALTER SYSTEM SET citus.local_hostname TO 'foobar';
SELECT pg_reload_conf();
SELECT pg_sleep(.1); -- wait to make sure the config has changed before running the GUC

SELECT replicate_table_shards('dist_table_test_2',  max_shard_copies := 4,  shard_transfer_mode:='block_writes');

ALTER SYSTEM RESET citus.local_hostname;
SELECT pg_reload_conf();
SELECT pg_sleep(.1); -- wait to make sure the config has changed before running the GUC

-- replicate reference table should ignore the coordinator
SET citus.node_connection_timeout to '35s';
BEGIN;
    SET LOCAL citus.shard_replication_factor TO 2;
    SET citus.log_remote_commands TO ON;
    SET SESSION citus.max_adaptive_executor_pool_size TO 5;
    SELECT replicate_table_shards('dist_table_test_2',  max_shard_copies := 4,  shard_transfer_mode:='block_writes');
COMMIT;

RESET citus.node_connection_timeout;
RESET citus.log_remote_commands;

DROP TABLE dist_table_test, dist_table_test_2, ref_table_test, postgres_table_test;
RESET citus.shard_count;
RESET citus.shard_replication_factor;

-- Create a user to test multiuser usage of rebalancer functions
-- We explicitely don't create this user on worker nodes yet, so we can
-- test some more error handling. We create them later there.
SET citus.enable_create_role_propagation TO OFF;
CREATE USER testrole;
GRANT ALL ON SCHEMA public TO testrole;

CREATE OR REPLACE FUNCTION shard_placement_rebalance_array(
    worker_node_list json[],
    shard_placement_list json[],
    threshold float4 DEFAULT 0,
    max_shard_moves int DEFAULT 1000000,
    drain_only bool DEFAULT false,
    improvement_threshold float4 DEFAULT 0.5
)
RETURNS json[]
AS 'citus'
LANGUAGE C STRICT VOLATILE;


CREATE OR REPLACE FUNCTION shard_placement_replication_array(worker_node_list json[],
                                                             shard_placement_list json[],
                                                             shard_replication_factor int)
RETURNS json[]
AS 'citus'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION worker_node_responsive(worker_node_name text, worker_node_port int)
RETURNS boolean
AS 'citus'
LANGUAGE C STRICT VOLATILE;

SET citus.next_shard_id TO 123000;

SELECT worker_node_responsive(node_name, node_port::int)
    FROM master_get_active_worker_nodes()
    ORDER BY node_name, node_port ASC;

-- Check that worker_node_responsive returns false for dead nodes
-- Note that PostgreSQL tries all possible resolutions of localhost on failing
-- connections. This causes different error details to be printed on different
-- environments. Therefore, we first set verbosity to terse.

\set VERBOSITY terse

SELECT worker_node_responsive('localhost', 1);

\set VERBOSITY default

-- Check that with threshold=0.0 shard_placement_rebalance_array returns enough
-- moves to make the cluster completely balanced.

SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1"}',
          '{"node_name": "hostname2"}']::json[],
    ARRAY['{"shardid":1, "nodename":"hostname1"}',
          '{"shardid":2, "nodename":"hostname1"}',
          '{"shardid":3, "nodename":"hostname1"}',
          '{"shardid":4, "nodename":"hostname1"}',
          '{"shardid":5, "nodename":"hostname1"}',
          '{"shardid":6, "nodename":"hostname2"}']::json[],
    0.0
));

-- Check that with two nodes and threshold=1.0 shard_placement_rebalance_array
-- doesn't return any moves, even if it is completely unbalanced.

SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1"}',
          '{"node_name": "hostname2"}']::json[],
    ARRAY['{"shardid":1, "nodename":"hostname1"}',
          '{"shardid":2, "nodename":"hostname1"}',
          '{"shardid":3, "nodename":"hostname1"}']::json[],
    1.0
));

-- Check that with three nodes and threshold=1.0
-- shard_placement_rebalance_array returns moves when it is completely unbalanced
SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1"}',
          '{"node_name": "hostname2"}',
          '{"node_name": "hostname3"}'
        ]::json[],
    ARRAY['{"shardid":1, "nodename":"hostname1"}',
          '{"shardid":2, "nodename":"hostname1"}',
          '{"shardid":3, "nodename":"hostname1"}']::json[],
    1.0
));


-- Check that with with three nodes and threshold=2.0
-- shard_placement_rebalance_array doesn't return any moves, even if it is
-- completely unbalanced. (with three nodes)


SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1"}',
          '{"node_name": "hostname2"}',
          '{"node_name": "hostname3"}'
        ]::json[],
    ARRAY['{"shardid":1, "nodename":"hostname1"}',
          '{"shardid":2, "nodename":"hostname1"}',
          '{"shardid":3, "nodename":"hostname1"}']::json[],
    2.0
));

-- Check that with threshold=0.0 shard_placement_rebalance_array doesn't return
-- any moves if the cluster is already balanced.

SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1"}',
          '{"node_name": "hostname2"}']::json[],
    ARRAY['{"shardid":1, "nodename":"hostname1"}',
          '{"shardid":2, "nodename":"hostname1"}',
          '{"shardid":3, "nodename":"hostname1"}',
          '{"shardid":4, "nodename":"hostname2"}',
          '{"shardid":5, "nodename":"hostname2"}',
          '{"shardid":6, "nodename":"hostname2"}']::json[],
    0.0
));

-- Check that shard_placement_replication_array returns a shard copy operation
-- for each of the shards in an inactive node.

SELECT unnest(shard_placement_replication_array(
    ARRAY['{"node_name": "hostname1"}',
          '{"node_name": "hostname2"}']::json[],
    ARRAY['{"shardid":1, "nodename":"hostname1"}',
          '{"shardid":2, "nodename":"hostname2"}',
          '{"shardid":1, "nodename":"hostname3"}',
          '{"shardid":2, "nodename":"hostname3"}']::json[],
    2
));

-- Check that shard_placement_replication_array errors out if all placements of
-- a shard are placed on inactive nodes.

SELECT unnest(shard_placement_replication_array(
    ARRAY['{"node_name": "hostname1"}']::json[],
    ARRAY['{"shardid":1, "nodename":"hostname2"}',
          '{"shardid":1, "nodename":"hostname3"}']::json[],
    2
));

-- Check that shard_placement_replication_array errors out if replication factor
-- is more than number of active nodes.

SELECT unnest(shard_placement_replication_array(
    ARRAY['{"node_name": "hostname1"}']::json[],
    ARRAY['{"shardid":1, "nodename":"hostname1"}']::json[],
    2
));

SET client_min_messages TO WARNING;

set citus.shard_count = 4;
-- Create a distributed table with all shards on a single node, so that we can
-- use this as an under-replicated
SET citus.shard_replication_factor TO 1;
SELECT * from master_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', false);
CREATE TABLE replication_test_table(int_column int);
SELECT create_distributed_table('replication_test_table', 'int_column');
UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid = 'replication_test_table'::regclass;
INSERT INTO replication_test_table SELECT * FROM generate_series(1, 100);

-- Ensure that shard_replication_factor is 2 during replicate_table_shards
-- and rebalance_table_shards tests
SET citus.shard_replication_factor TO 2;
SELECT * from master_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', true);

CREATE VIEW replication_test_table_placements_per_node AS
    SELECT count(*) FROM pg_dist_shard_placement NATURAL JOIN pg_dist_shard
    WHERE logicalrelid = 'replication_test_table'::regclass
    AND shardstate != 4
    GROUP BY nodename, nodeport
    ORDER BY nodename, nodeport;


SELECT * FROM replication_test_table_placements_per_node;

-- Test replicate_table_shards, which will in turn test update_shard_placement
-- in copy mode.

-- Check excluded_shard_list by excluding three shards with smaller ids

SELECT replicate_table_shards('replication_test_table',
                              excluded_shard_list := excluded_shard_list,
                              shard_transfer_mode:='block_writes')
    FROM (
        SELECT (array_agg(DISTINCT shardid ORDER BY shardid))[1:3] AS excluded_shard_list
        FROM pg_dist_shard
        WHERE logicalrelid = 'replication_test_table'::regclass
    ) T;

SELECT * FROM replication_test_table_placements_per_node;

-- Check that with shard_replication_factor=1 we don't do any copies

SELECT replicate_table_shards('replication_test_table',
                              shard_replication_factor := 1,
                              shard_transfer_mode:='block_writes');

SELECT * FROM replication_test_table_placements_per_node;

-- Check that max_shard_copies limits number of copy operations

SELECT replicate_table_shards('replication_test_table',
                              max_shard_copies := 2,
                              shard_transfer_mode:='block_writes');

SELECT * FROM replication_test_table_placements_per_node;

-- Replicate the remaining under-replicated shards

SELECT replicate_table_shards('replication_test_table', shard_transfer_mode:='block_writes');

SELECT * FROM replication_test_table_placements_per_node;

-- Check that querying the table doesn't error out

SELECT count(*) FROM replication_test_table;

DROP TABLE public.replication_test_table CASCADE;

-- Test rebalance_table_shards, which will in turn test update_shard_placement
-- in move mode.

SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 6;
CREATE TABLE rebalance_test_table(int_column int);
SELECT create_distributed_table('rebalance_test_table', 'int_column');
UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid = 'rebalance_test_table'::regclass;

CREATE VIEW table_placements_per_node AS
SELECT nodeport, logicalrelid::regclass, count(*)
FROM pg_dist_shard_placement NATURAL JOIN pg_dist_shard
WHERE shardstate != 4
GROUP BY logicalrelid::regclass, nodename, nodeport
ORDER BY logicalrelid::regclass, nodename, nodeport;

-- Create six shards with replication factor 1 and move them to the same
-- node to create an unbalanced cluster.

CREATE OR REPLACE PROCEDURE create_unbalanced_shards(rel text)
LANGUAGE SQL
AS $$
    SET citus.shard_replication_factor TO 1;

    SELECT count(master_move_shard_placement(shardid,
            src.nodename, src.nodeport::int,
            dst.nodename, dst.nodeport::int,
            shard_transfer_mode:='block_writes'))
    FROM pg_dist_shard s JOIN
    pg_dist_shard_placement src USING (shardid),
    (SELECT nodename, nodeport FROM pg_dist_shard_placement ORDER BY nodeport DESC LIMIT 1) dst
    WHERE src.nodeport < dst.nodeport AND s.logicalrelid = rel::regclass;
    CALL citus_cleanup_orphaned_resources();
$$;

CALL create_unbalanced_shards('rebalance_test_table');

SET citus.shard_replication_factor TO 2;

-- Upload the test data to the shards

INSERT INTO rebalance_test_table SELECT * FROM generate_series(1, 100);

-- Verify that there is one node with all placements

SELECT * FROM table_placements_per_node;

-- check rebalances use the localhost guc by seeing it fail when the GUC is set to a non-existing host
ALTER SYSTEM SET citus.local_hostname TO 'foobar';
SELECT pg_reload_conf();
SELECT pg_sleep(.1); -- wait to make sure the config has changed before running the GUC

SELECT rebalance_table_shards('rebalance_test_table',
                              excluded_shard_list := excluded_shard_list,
                              threshold := 0,
                              shard_transfer_mode:='block_writes')
FROM (
         SELECT (array_agg(DISTINCT shardid ORDER BY shardid))[1:4] AS excluded_shard_list
         FROM pg_dist_shard
         WHERE logicalrelid = 'rebalance_test_table'::regclass
     ) T;
CALL citus_cleanup_orphaned_resources();

ALTER SYSTEM RESET citus.local_hostname;
SELECT pg_reload_conf();
SELECT pg_sleep(.1); -- wait to make sure the config has changed before running the GUC

-- Check excluded_shard_list by excluding four shards with smaller ids

SELECT rebalance_table_shards('rebalance_test_table',
    excluded_shard_list := excluded_shard_list,
    threshold := 0,
    shard_transfer_mode:='block_writes')
FROM (
    SELECT (array_agg(DISTINCT shardid ORDER BY shardid))[1:4] AS excluded_shard_list
    FROM pg_dist_shard
    WHERE logicalrelid = 'rebalance_test_table'::regclass
) T;
CALL citus_cleanup_orphaned_resources();

SELECT * FROM table_placements_per_node;

-- Check that max_shard_moves limits number of move operations

-- First check that we error if not table owner
-- Turn on NOTICE messages
SET ROLE testrole;
-- Make sure that rebalance is stopped if source or target nodes are
-- unresponsive.
SELECT rebalance_table_shards('rebalance_test_table',
    shard_transfer_mode:='block_writes');
\c - - - :worker_1_port
SET citus.enable_create_role_propagation TO OFF;
CREATE USER testrole;
GRANT ALL ON SCHEMA public TO testrole;
\c - - - :master_port
SET client_min_messages TO WARNING;
SET ROLE testrole;
SELECT rebalance_table_shards('rebalance_test_table',
    shard_transfer_mode:='block_writes');
\c - - - :worker_2_port
SET citus.enable_create_role_propagation TO OFF;
CREATE USER testrole;
GRANT ALL ON SCHEMA public TO testrole;
\c - - - :master_port
SET client_min_messages TO WARNING;
SET citus.next_shard_id TO 123010;
SET ROLE testrole;
SELECT rebalance_table_shards('rebalance_test_table',
    shard_transfer_mode:='block_writes');
RESET ROLE;
CALL citus_cleanup_orphaned_resources();
-- Confirm no moves took place at all during these errors
SELECT * FROM table_placements_per_node;
CALL citus_cleanup_orphaned_resources();

SELECT rebalance_table_shards('rebalance_test_table',
    threshold := 0, max_shard_moves := 1,
    shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();

SELECT * FROM table_placements_per_node;

-- Check that threshold=1 doesn't move any shards

SELECT rebalance_table_shards('rebalance_test_table', threshold := 1, shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();

SELECT * FROM table_placements_per_node;

-- Move the remaining shards using threshold=0

SELECT rebalance_table_shards('rebalance_test_table', threshold := 0, shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();

SELECT * FROM table_placements_per_node;

-- Check that shard is completely balanced and rebalancing again doesn't have
-- any effects.

SELECT rebalance_table_shards('rebalance_test_table', threshold := 0, shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();

SELECT * FROM table_placements_per_node;

-- Check that querying the table doesn't error out

SELECT count(*) FROM rebalance_test_table;

DROP TABLE rebalance_test_table;

-- Test schema support


CREATE SCHEMA test_schema_support;

SELECT COUNT(*) FROM pg_dist_shard_placement;

CREATE TABLE test_schema_support.nation_hash (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('test_schema_support.nation_hash', 'n_nationkey', 'hash');

CREATE TABLE test_schema_support.nation_hash2 (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);

SELECT create_distributed_table('test_schema_support.nation_hash2', 'n_nationkey', 'hash');

-- Mark tables as coordinator replicated in order to be able to test replicate_table_shards
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid IN
  ('test_schema_support.nation_hash2'::regclass, 'test_schema_support.nation_hash'::regclass);

-- Shard count before replication
SELECT COUNT(*) FROM pg_dist_shard_placement;

SET search_path TO public;
SELECT replicate_table_shards('test_schema_support.nation_hash', shard_replication_factor:=2, max_shard_copies:=1, shard_transfer_mode:='block_writes');

-- Confirm replication, both tables replicated due to colocation
SELECT COUNT(*) FROM pg_dist_shard_placement;

-- Test with search_path is set
SET search_path TO test_schema_support;
SELECT replicate_table_shards('nation_hash2', shard_replication_factor:=2, shard_transfer_mode:='block_writes');

-- Confirm replication
SELECT COUNT(*) FROM pg_dist_shard_placement;

DROP TABLE test_schema_support.nation_hash;
DROP TABLE test_schema_support.nation_hash2;

-- Test rebalancer with schema
-- Next few operations is to create imbalanced distributed table

CREATE TABLE test_schema_support.imbalanced_table_local (
    id integer not null
);
INSERT INTO test_schema_support.imbalanced_table_local VALUES(1);
INSERT INTO test_schema_support.imbalanced_table_local VALUES(2);
INSERT INTO test_schema_support.imbalanced_table_local VALUES(3);
INSERT INTO test_schema_support.imbalanced_table_local VALUES(4);

CREATE TABLE test_schema_support.imbalanced_table (
    id integer not null
);

SET citus.shard_count = 3;
SET citus.shard_replication_factor TO 1;
SELECT * from master_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', false);
SELECT create_distributed_table('test_schema_support.imbalanced_table', 'id');
INSERT INTO test_schema_support.imbalanced_table SELECT * FROM generate_series(1, 100);
UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid = 'test_schema_support.imbalanced_table'::regclass;
SELECT * from master_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', true);
SET citus.shard_count = 4;

-- copy one of the shards to the other node, this is to test that the
-- rebalancer takes into account all copies of a placement
SET citus.shard_replication_factor TO 2;
SELECT replicate_table_shards('test_schema_support.imbalanced_table', max_shard_copies := 1, shard_transfer_mode := 'block_writes');
SET citus.shard_replication_factor TO 1;

-- imbalanced_table is now imbalanced

-- Shard counts in each node before rebalance
SELECT * FROM public.table_placements_per_node;

-- Row count in imbalanced table before rebalance
SELECT COUNT(*) FROM imbalanced_table;

-- Test rebalance operation
SELECT rebalance_table_shards('imbalanced_table', threshold:=0, shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();

-- Confirm rebalance
-- Shard counts in each node after rebalance
SELECT * FROM public.table_placements_per_node;

-- Row count in imbalanced table after rebalance
SELECT COUNT(*) FROM imbalanced_table;

DROP TABLE test_schema_support.imbalanced_table;
DROP TABLE test_schema_support.imbalanced_table_local;

SET citus.shard_replication_factor TO 1;
SET citus.shard_count = 4;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 136;

CREATE TABLE colocated_rebalance_test(id integer);
CREATE TABLE colocated_rebalance_test2(id integer);
SELECT create_distributed_table('colocated_rebalance_test', 'id');


-- make sure that we do not allow shards on target nodes
-- that are not eligable to move shards

-- Try to move shards to a non-existing node
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', 10000, 'block_writes')
FROM pg_dist_shard_placement
WHERE nodeport = :worker_2_port;
CALL citus_cleanup_orphaned_resources();

-- Try to move shards to a node where shards are not allowed
SELECT * from master_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', false);
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes')
FROM pg_dist_shard_placement
WHERE nodeport = :worker_2_port;
SELECT * from master_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', true);

-- Try to move shards to a non-active node
UPDATE pg_dist_node SET isactive = false WHERE nodeport = :worker_1_port;
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes')
FROM pg_dist_shard_placement
WHERE nodeport = :worker_2_port;
UPDATE pg_dist_node SET isactive = true WHERE nodeport = :worker_1_port;

-- Try to move shards to a secondary node
UPDATE pg_dist_node SET noderole = 'secondary' WHERE nodeport = :worker_1_port;
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes')
FROM pg_dist_shard_placement
WHERE nodeport = :worker_2_port;
UPDATE pg_dist_node SET noderole = 'primary' WHERE nodeport = :worker_1_port;

-- Move all shards to worker1
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes')
FROM pg_dist_shard_placement
WHERE nodeport = :worker_2_port;
CALL citus_cleanup_orphaned_resources();

SELECT create_distributed_table('colocated_rebalance_test2', 'id');

-- Confirm all shards for both tables are on worker1
SELECT * FROM public.table_placements_per_node;

-- Confirm that the plan for drain_only doesn't show any moves
SELECT * FROM get_rebalance_table_shards_plan('colocated_rebalance_test', threshold := 0, drain_only := true);
-- Running with drain_only shouldn't do anything
SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes', drain_only := true);
CALL citus_cleanup_orphaned_resources();

-- Confirm that nothing changed
SELECT * FROM public.table_placements_per_node;

-- Confirm that the plan shows 2 shards of both tables moving back to worker2
SELECT * FROM get_rebalance_table_shards_plan('colocated_rebalance_test', threshold := 0);
-- Confirm that this also happens when using rebalancing by disk size even if the tables are empty
SELECT * FROM get_rebalance_table_shards_plan('colocated_rebalance_test', rebalance_strategy := 'by_disk_size');
-- Check that we can call this function
SELECT * FROM get_rebalance_progress();
-- Actually do the rebalance
SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
-- Check that we can call this function without a crash
SELECT * FROM get_rebalance_progress();

-- Confirm that the shards are now there
SELECT * FROM public.table_placements_per_node;

CALL citus_cleanup_orphaned_resources();
select * from pg_dist_placement ORDER BY placementid;


-- Move all shards to worker1 again
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes')
FROM pg_dist_shard NATURAL JOIN pg_dist_placement NATURAL JOIN pg_dist_node
WHERE nodeport = :worker_2_port AND logicalrelid = 'colocated_rebalance_test'::regclass;

-- Confirm that the shards are now all on worker1
SELECT * FROM public.table_placements_per_node;

-- Explicitly don't run citus_cleanup_orphaned_resources, rebalance_table_shards
-- should do that for automatically.
SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');

-- Confirm that the shards are now moved
SELECT * FROM public.table_placements_per_node;


CREATE TABLE non_colocated_rebalance_test(id integer);
SELECT create_distributed_table('non_colocated_rebalance_test', 'id', colocate_with := 'none');
-- confirm that both colocation groups are balanced
SELECT * FROM public.table_placements_per_node;

-- testing behaviour when setting isdatanode to 'marked for draining'
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);

SELECT * FROM get_rebalance_table_shards_plan('colocated_rebalance_test', threshold := 0);
SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

SELECT * FROM get_rebalance_table_shards_plan('non_colocated_rebalance_test', threshold := 0);
SELECT * FROM rebalance_table_shards('non_colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;
SELECT * FROM rebalance_table_shards('non_colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

-- testing behaviour when setting shouldhaveshards to false and rebalancing all
-- colocation groups with drain_only=true
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);
-- we actually shouldn't need the ORDER BY clause as the output will be in execution order
-- but this one involves different colocation groups and which colocation group is first moved is not consistent
SELECT * FROM get_rebalance_table_shards_plan(threshold := 0, drain_only := true) ORDER BY shardid;
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes', drain_only := true);
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

-- testing behaviour when setting shouldhaveshards to false and rebalancing all
-- colocation groups with drain_only=false
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);
-- we actually shouldn't need the ORDER BY clause as the output will be in execution order
-- but this one involves different colocation groups and which colocation group is first moved is not consistent
SELECT * FROM get_rebalance_table_shards_plan(threshold := 0) ORDER BY shardid;
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

-- Make it a data node again
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

-- testing behaviour of master_drain_node
SELECT * from master_drain_node('localhost', :worker_2_port, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
select shouldhaveshards from pg_dist_node where nodeport = :worker_2_port;
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;


-- Drop some tables for clear consistent error
DROP TABLE test_schema_support.non_colocated_rebalance_test;
DROP TABLE test_schema_support.colocated_rebalance_test2;

-- testing behaviour when a transfer fails when using master_drain_node
SELECT * from master_drain_node('localhost', :worker_2_port);
-- Make sure shouldhaveshards is false
select shouldhaveshards from pg_dist_node where nodeport = :worker_2_port;
-- Make sure no actual nodes are moved
SELECT * FROM public.table_placements_per_node;

-- Make it a data node again
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

-- Leave no trace on workers
RESET search_path;

\set VERBOSITY terse
DROP SCHEMA test_schema_support CASCADE;
\set VERBOSITY default

REVOKE ALL ON SCHEMA public FROM testrole;
DROP USER testrole;

-- Test costs
set citus.shard_count = 4;
SET citus.next_shard_id TO 123040;
CREATE TABLE tab (x int);
SELECT create_distributed_table('tab','x');
-- The following numbers are chosen such that they are placed on different
-- shards.
INSERT INTO tab SELECT 1 from generate_series(1, 30000);
INSERT INTO tab SELECT 2 from generate_series(1, 10000);
INSERT INTO tab SELECT 3 from generate_series(1, 10000);
INSERT INTO tab SELECT 6 from generate_series(1, 10000);
VACUUM FULL tab;
ANALYZE tab;

\c - - - :worker_1_port
SELECT table_schema, table_name, row_estimate,
    CASE
        WHEN total_bytes BETWEEN 1900000 AND 2300000 THEN 2179072
        WHEN total_bytes BETWEEN 900000 AND 1200000 THEN 1089536
        WHEN total_bytes BETWEEN 300000 AND 440000 THEN 368640
        ELSE total_bytes
    END
  FROM (
  SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r'
          AND c.oid NOT IN (SELECT logicalrelid FROM pg_dist_partition)
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :worker_2_port
SELECT table_schema, table_name, row_estimate,
    CASE
        WHEN total_bytes BETWEEN 1900000 AND 2300000 THEN 2179072
        WHEN total_bytes BETWEEN 900000 AND 1200000 THEN 1089536
        WHEN total_bytes BETWEEN 300000 AND 440000 THEN 368640
        ELSE total_bytes
    END
  FROM (
  SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r'
          AND c.oid NOT IN (SELECT logicalrelid FROM pg_dist_partition)
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;

\c - - - :master_port

SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'by_disk_size');
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'by_disk_size', threshold := 0);

SELECT * FROM rebalance_table_shards('tab', shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'by_disk_size', shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'by_disk_size', shard_transfer_mode:='block_writes', threshold := 0);
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

-- Check that sizes of colocated tables are added together for rebalances
set citus.shard_count = 4;
SET citus.next_shard_id TO 123050;
CREATE TABLE tab2 (x int);
SELECT create_distributed_table('tab2','x', colocate_with := 'tab');
INSERT INTO tab2 SELECT 1 from generate_series(1, 0);
INSERT INTO tab2 SELECT 2 from generate_series(1, 60000);
INSERT INTO tab2 SELECT 3 from generate_series(1, 10000);
INSERT INTO tab2 SELECT 6 from generate_series(1, 10000);
VACUUM FULL tab, tab2;
ANALYZE tab, tab2;

\c - - - :worker_1_port
SELECT table_schema, table_name, row_estimate,
    CASE
        WHEN total_bytes BETWEEN 1900000 AND 2300000 THEN 2179072
        WHEN total_bytes BETWEEN 900000 AND 1200000 THEN 1089536
        WHEN total_bytes BETWEEN 300000 AND 440000 THEN 368640
        ELSE total_bytes
    END
  FROM (
  SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r'
          AND c.oid NOT IN (SELECT logicalrelid FROM pg_dist_partition)
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :worker_2_port
SELECT table_schema, table_name, row_estimate,
    CASE
        WHEN total_bytes BETWEEN 1900000 AND 2300000 THEN 2179072
        WHEN total_bytes BETWEEN 900000 AND 1200000 THEN 1089536
        WHEN total_bytes BETWEEN 300000 AND 440000 THEN 368640
        ELSE total_bytes
    END
  FROM (
  SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r'
          AND c.oid NOT IN (SELECT logicalrelid FROM pg_dist_partition)
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;

\c - - - :master_port
-- disk sizes can be slightly different, so ORDER BY shardid gives us a consistent output
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'by_disk_size') ORDER BY shardid;
-- supports improvement_threshold
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'by_disk_size', improvement_threshold := 0);
SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'by_disk_size', shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;
VACUUM FULL tab, tab2;
ANALYZE tab, tab2;

\c - - - :worker_1_port
SELECT table_schema, table_name, row_estimate,
    CASE
        WHEN total_bytes BETWEEN 1900000 AND 2300000 THEN 2179072
        WHEN total_bytes BETWEEN 900000 AND 1200000 THEN 1089536
        WHEN total_bytes BETWEEN 300000 AND 440000 THEN 368640
        ELSE total_bytes
    END
  FROM (
  SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r'
          AND c.oid NOT IN (SELECT logicalrelid FROM pg_dist_partition)
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :worker_2_port
SELECT table_schema, table_name, row_estimate,
    CASE
        WHEN total_bytes BETWEEN 1900000 AND 2300000 THEN 2179072
        WHEN total_bytes BETWEEN 900000 AND 1200000 THEN 1089536
        WHEN total_bytes BETWEEN 300000 AND 440000 THEN 368640
        ELSE total_bytes
    END
  FROM (
  SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r'
          AND c.oid NOT IN (SELECT logicalrelid FROM pg_dist_partition)
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :master_port

DROP TABLE tab2;

CREATE OR REPLACE FUNCTION capacity_high_worker_2(nodeidarg int)
    RETURNS real AS $$
    SELECT
        (CASE WHEN nodeport = 57638 THEN 1000 ELSE 1 END)::real
    FROM pg_dist_node where nodeid = nodeidarg
    $$ LANGUAGE sql;

\set VERBOSITY terse

SELECT citus_add_rebalance_strategy(
        'capacity_high_worker_2',
        'citus_shard_cost_1',
        'capacity_high_worker_2',
        'citus_shard_allowed_on_node_true',
        0
    );

SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'capacity_high_worker_2');
SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'capacity_high_worker_2', shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

SELECT citus_set_default_rebalance_strategy('capacity_high_worker_2');
SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM rebalance_table_shards('tab', shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

CREATE OR REPLACE FUNCTION only_worker_1(shardid bigint, nodeidarg int)
    RETURNS boolean AS $$
    SELECT
        (CASE WHEN nodeport = 57637 THEN TRUE ELSE FALSE END)
    FROM pg_dist_node where nodeid = nodeidarg
    $$ LANGUAGE sql;

SELECT citus_add_rebalance_strategy(
        'only_worker_1',
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'only_worker_1',
        0
    );

SELECT citus_set_default_rebalance_strategy('only_worker_1');
SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM rebalance_table_shards('tab', shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM public.table_placements_per_node;

SELECT citus_set_default_rebalance_strategy('by_shard_count');
SELECT * FROM get_rebalance_table_shards_plan('tab');

-- Check all the error handling cases
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'non_existing');
SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'non_existing');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM master_drain_node('localhost', :worker_2_port, rebalance_strategy := 'non_existing');
CALL citus_cleanup_orphaned_resources();
SELECT citus_set_default_rebalance_strategy('non_existing');


UPDATE pg_dist_rebalance_strategy SET default_strategy=false;
SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM rebalance_table_shards('tab');
CALL citus_cleanup_orphaned_resources();
SELECT * FROM master_drain_node('localhost', :worker_2_port);
CALL citus_cleanup_orphaned_resources();
UPDATE pg_dist_rebalance_strategy SET default_strategy=true WHERE name='by_shard_count';

CREATE OR REPLACE FUNCTION shard_cost_no_arguments()
    RETURNS real AS $$ SELECT 1.0::real $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION shard_cost_bad_arg_type(text)
    RETURNS real AS $$ SELECT 1.0::real $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION shard_cost_bad_return_type(bigint)
    RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION node_capacity_no_arguments()
    RETURNS real AS $$ SELECT 1.0::real $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION node_capacity_bad_arg_type(text)
    RETURNS real AS $$ SELECT 1.0::real $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION node_capacity_bad_return_type(int)
    RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION shard_allowed_on_node_no_arguments()
    RETURNS boolean AS $$ SELECT true $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION shard_allowed_on_node_bad_arg1(text, int)
    RETURNS boolean AS $$ SELECT true $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION shard_allowed_on_node_bad_arg2(bigint, text)
    RETURNS boolean AS $$ SELECT true $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION shard_allowed_on_node_bad_return_type(bigint, int)
    RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'shard_cost_no_arguments',
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'shard_cost_bad_arg_type',
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'shard_cost_bad_return_type',
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        0,
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0
    );

SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'node_capacity_no_arguments',
        'citus_shard_allowed_on_node_true',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'node_capacity_bad_arg_type',
        'citus_shard_allowed_on_node_true',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'node_capacity_bad_return_type',
        'citus_shard_allowed_on_node_true',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        0,
        'citus_shard_allowed_on_node_true',
        0
    );

SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'shard_allowed_on_node_no_arguments',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'shard_allowed_on_node_bad_arg1',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'shard_allowed_on_node_bad_arg2',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'shard_allowed_on_node_bad_return_type',
        0
    );
SELECT citus_add_rebalance_strategy(
        'insert_should_fail',
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        0,
        0
    );


-- Confirm that manual insert/update has the same checks
INSERT INTO
    pg_catalog.pg_dist_rebalance_strategy(
        name,
        shard_cost_function,
        node_capacity_function,
        shard_allowed_on_node_function,
        default_threshold
    ) VALUES (
        'shard_cost_no_arguments',
        'shard_cost_no_arguments',
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0
    );
UPDATE pg_dist_rebalance_strategy SET shard_cost_function='shard_cost_no_arguments' WHERE name='by_disk_size';

-- Confirm that only a single default strategy can exist
INSERT INTO
    pg_catalog.pg_dist_rebalance_strategy(
        name,
        default_strategy,
        shard_cost_function,
        node_capacity_function,
        shard_allowed_on_node_function,
        default_threshold
    ) VALUES (
        'second_default',
        true,
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0
    );
UPDATE pg_dist_rebalance_strategy SET default_strategy=true WHERE name='by_disk_size';
-- ensure the trigger allows updating the default strategy
UPDATE pg_dist_rebalance_strategy SET default_strategy=true WHERE name='by_shard_count';

-- Confirm that default strategy should be higher than minimum strategy
SELECT citus_add_rebalance_strategy(
        'default_threshold_too_low',
        'citus_shard_cost_1',
        'capacity_high_worker_2',
        'citus_shard_allowed_on_node_true',
        0,
        0.1
    );

-- Make it a data node again
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
DROP TABLE tab;


-- we don't need the coordinator on pg_dist_node anymore
SELECT 1 FROM master_remove_node('localhost', :master_port);
SELECT public.wait_until_metadata_sync(60000);

--
-- Make sure that rebalance_table_shards() and replicate_table_shards() replicate
-- reference tables to the coordinator
--

SET client_min_messages TO WARNING;

CREATE TABLE dist_table_test_3(a int);
SET citus.shard_count TO 4;

SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('dist_table_test_3', 'a');

CREATE TABLE ref_table(a int);
SELECT create_reference_table('ref_table');

SELECT 1 FROM master_add_node('localhost', :master_port, groupId=>0);

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

SET citus.shard_replication_factor TO 2;
SELECT replicate_table_shards('dist_table_test_3',  max_shard_copies := 4,  shard_transfer_mode:='block_writes');

-- Mark table as coordinator replicated in order to be able to test replicate_table_shards
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid = 'dist_table_test_3'::regclass;

SELECT replicate_table_shards('dist_table_test_3',  max_shard_copies := 4,  shard_transfer_mode:='block_writes');

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

SELECT 1 FROM master_remove_node('localhost', :master_port);
SELECT public.wait_until_metadata_sync(30000);

CREATE TABLE rebalance_test_table(int_column int);
SELECT create_distributed_table('rebalance_test_table', 'int_column', 'append');

CALL create_unbalanced_shards('rebalance_test_table');

SELECT 1 FROM master_add_node('localhost', :master_port, groupId=>0);

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

SELECT rebalance_table_shards('rebalance_test_table', shard_transfer_mode:='block_writes');
CALL citus_cleanup_orphaned_resources();

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

DROP TABLE dist_table_test_3, rebalance_test_table, ref_table;

SELECT 1 FROM master_remove_node('localhost', :master_port);
SELECT public.wait_until_metadata_sync(30000);


-- reference table 2 will not have a replica identity, causing the rebalancer to not work
-- when ran in the default mode. Instead we need to change the shard transfer mode to make
-- it work. This verifies the shard transfer mode used in the rebalancer is used for the
-- ensurance of reference table existence.

CREATE TABLE t1 (a int PRIMARY KEY, b int);
CREATE TABLE r1 (a int PRIMARY KEY, b int);
CREATE TABLE r2 (a int, b int);

-- we remove worker 2 before creating the tables, this will allow us to have an active
-- node without the reference tables

SELECT 1 from master_remove_node('localhost', :worker_2_port);
SELECT public.wait_until_metadata_sync(30000);

SELECT create_distributed_table('t1','a');
SELECT create_reference_table('r1');
SELECT create_reference_table('r2');

-- add data so to actually copy data when forcing logical replication for reference tables
INSERT INTO r1 VALUES (1,2), (3,4);
INSERT INTO r2 VALUES (1,2), (3,4);

SELECT 1 from master_add_node('localhost', :worker_2_port);

-- since r2 has no replica identity we expect an error here
SELECT rebalance_table_shards();
CALL citus_cleanup_orphaned_resources();

DROP TABLE t1, r1, r2;

-- verify there are no distributed tables before we perform the following tests. Preceding
-- test suites should clean up their distributed tables.
SELECT count(*) FROM pg_dist_partition;

-- verify a system with a new node won't copy distributed table shards without reference tables

SELECT 1 from master_remove_node('localhost', :worker_2_port);
SELECT public.wait_until_metadata_sync(30000);

CREATE TABLE r1 (a int PRIMARY KEY, b int);
SELECT create_reference_table('r1');

CREATE TABLE d1 (a int PRIMARY KEY, b int);
SELECT create_distributed_table('d1', 'a');

ALTER SEQUENCE pg_dist_groupid_seq RESTART WITH 15;
SELECT 1 from master_add_node('localhost', :worker_2_port);

-- count the number of placements for the reference table to verify it is not available on
-- all nodes
SELECT count(*)
FROM pg_dist_shard
JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'r1'::regclass;

-- #7426 We can't move shards to the fresh node before we copy reference tables there.
-- rebalance_table_shards() will do the copy, but the low-level
-- citus_move_shard_placement() should raise an error
SELECT citus_move_shard_placement(pg_dist_shard.shardid, nodename, nodeport, 'localhost', :worker_2_port)
    FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
    WHERE logicalrelid = 'd1'::regclass AND nodename = 'localhost' AND nodeport = :worker_1_port LIMIT 1;

SELECT replicate_reference_tables();

-- After replication, the move should succeed.
SELECT citus_move_shard_placement(pg_dist_shard.shardid, nodename, nodeport, 'localhost', :worker_2_port)
    FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
    WHERE logicalrelid = 'd1'::regclass AND nodename = 'localhost' AND nodeport = :worker_1_port LIMIT 1;

DROP TABLE d1, r1;

-- verify a system having only reference tables will copy the reference tables when
-- executing the rebalancer

SELECT 1 from master_remove_node('localhost', :worker_2_port);
SELECT public.wait_until_metadata_sync(30000);

CREATE TABLE r1 (a int PRIMARY KEY, b int);
SELECT create_reference_table('r1');

ALTER SEQUENCE pg_dist_groupid_seq RESTART WITH 15;
SELECT 1 from master_add_node('localhost', :worker_2_port);

-- count the number of placements for the reference table to verify it is not available on
-- all nodes
SELECT count(*)
FROM pg_dist_shard
JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'r1'::regclass;

-- rebalance with _only_ a reference table, this should trigger the copy
SELECT rebalance_table_shards();
CALL citus_cleanup_orphaned_resources();

-- verify the reference table is on all nodes after the rebalance
SELECT count(*)
FROM pg_dist_shard
JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'r1'::regclass;

-- cleanup tables
DROP TABLE r1;


-- lastly we need to verify that reference tables are copied before the replication factor
-- of other tables is increased. Without the copy of reference tables the replication might
-- fail.

SELECT 1 from master_remove_node('localhost', :worker_2_port);
SELECT public.wait_until_metadata_sync(30000);

CREATE TABLE t1 (a int PRIMARY KEY, b int);
CREATE TABLE r1 (a int PRIMARY KEY, b int);
SELECT create_distributed_table('t1', 'a');
SELECT create_reference_table('r1');

SELECT 1 from master_add_node('localhost', :worker_2_port);

-- count the number of placements for the reference table to verify it is not available on
-- all nodes
SELECT count(*)
FROM pg_dist_shard
JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'r1'::regclass;

SELECT replicate_table_shards('t1',  shard_replication_factor := 2);

-- Mark table as coordinator replicated in order to be able to test replicate_table_shards
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid IN
  ('t1'::regclass);
SELECT replicate_table_shards('t1',  shard_replication_factor := 2);

-- verify the reference table is on all nodes after replicate_table_shards
SELECT count(*)
FROM pg_dist_shard
JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'r1'::regclass;

DROP TABLE t1, r1;

-- Test rebalancer with index on a table

DROP TABLE IF EXISTS test_rebalance_with_index;
CREATE TABLE test_rebalance_with_index (measureid integer PRIMARY KEY);
SELECT create_distributed_table('test_rebalance_with_index', 'measureid');
CREATE INDEX rebalance_with_index ON test_rebalance_with_index(measureid);

INSERT INTO test_rebalance_with_index VALUES(0);
INSERT INTO test_rebalance_with_index VALUES(1);
INSERT INTO test_rebalance_with_index VALUES(2);

SELECT * FROM master_drain_node('localhost', :worker_2_port);
CALL citus_cleanup_orphaned_resources();
UPDATE pg_dist_node SET shouldhaveshards=true WHERE nodeport = :worker_2_port;

SELECT rebalance_table_shards();
CALL citus_cleanup_orphaned_resources();
DROP TABLE test_rebalance_with_index CASCADE;


-- Test rebalancer with disabled worker

SET citus.next_shard_id TO 433500;
SET citus.shard_replication_factor TO 2;

DROP TABLE IF EXISTS test_rebalance_with_disabled_worker;
CREATE TABLE test_rebalance_with_disabled_worker (a int);
SELECT create_distributed_table('test_rebalance_with_disabled_worker', 'a', colocate_with:='none');

SELECT citus_disable_node('localhost', :worker_2_port);
SELECT public.wait_until_metadata_sync(30000);

-- errors out because shard replication factor > shard allowed node count
SELECT rebalance_table_shards('test_rebalance_with_disabled_worker');

-- set replication factor to one, and try again
SET citus.shard_replication_factor TO 1;
SELECT rebalance_table_shards('test_rebalance_with_disabled_worker');
SET citus.shard_replication_factor TO 2;

SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);

DROP TABLE test_rebalance_with_disabled_worker;

-- Test rebalance with all shards excluded

DROP TABLE IF EXISTS test_with_all_shards_excluded;
CREATE TABLE test_with_all_shards_excluded(a int PRIMARY KEY);
SELECT create_distributed_table('test_with_all_shards_excluded', 'a', colocate_with:='none', shard_count:=4);

SELECT shardid FROM pg_dist_shard ORDER BY shardid ASC;

SELECT rebalance_table_shards('test_with_all_shards_excluded', excluded_shard_list:='{102073, 102074, 102075, 102076}');

DROP TABLE test_with_all_shards_excluded;

SET citus.shard_count TO 2;

CREATE TABLE "events.Energy Added" (user_id int, time timestamp with time zone, data jsonb, PRIMARY KEY (user_id, time )) PARTITION BY RANGE ("time");
CREATE INDEX idx_btree_hobbies ON "events.Energy Added" USING BTREE ((data->>'location'));
SELECT create_distributed_table('"events.Energy Added"', 'user_id');
CREATE TABLE "Energy Added_17634"  PARTITION OF "events.Energy Added" FOR VALUES  FROM ('2018-04-13 00:00:00+00') TO ('2018-04-14 00:00:00+00');
CREATE TABLE "Energy Added_17635"  PARTITION OF "events.Energy Added" FOR VALUES  FROM ('2018-04-14 00:00:00+00') TO ('2018-04-15 00:00:00+00');

create table colocated_t1 (a int);
select create_distributed_table('colocated_t1','a',colocate_with=>'"events.Energy Added"');

create table colocated_t2 (a int);
select create_distributed_table('colocated_t2','a',colocate_with=>'"events.Energy Added"');

create table colocated_t3 (a int);
select create_distributed_table('colocated_t3','a',colocate_with=>'"events.Energy Added"');

SET client_min_messages TO DEBUG4;
SELECT * FROM get_rebalance_table_shards_plan('colocated_t1', rebalance_strategy := 'by_disk_size');
RESET client_min_messages;

DROP TABLE "events.Energy Added", colocated_t1, colocated_t2, colocated_t3;
RESET citus.shard_count;
DROP VIEW table_placements_per_node;
DELETE FROM pg_catalog.pg_dist_rebalance_strategy WHERE name='capacity_high_worker_2';
DELETE FROM pg_catalog.pg_dist_rebalance_strategy WHERE name='only_worker_1';

-- add colocation groups with shard group count < worker count
-- the rebalancer should balance those "unbalanced shards" evenly as much as possible
SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);
create table single_shard_colocation_1a (a int primary key);
create table single_shard_colocation_1b (a int primary key);
create table single_shard_colocation_1c (a int primary key);
SET citus.shard_replication_factor = 1;
select create_distributed_table('single_shard_colocation_1a','a', colocate_with => 'none', shard_count => 1);
select create_distributed_table('single_shard_colocation_1b','a',colocate_with=>'single_shard_colocation_1a');
select create_distributed_table('single_shard_colocation_1c','a',colocate_with=>'single_shard_colocation_1b');

create table single_shard_colocation_2a (a bigint);
create table single_shard_colocation_2b (a bigint);
select create_distributed_table('single_shard_colocation_2a','a', colocate_with => 'none', shard_count => 1);
select create_distributed_table('single_shard_colocation_2b','a',colocate_with=>'single_shard_colocation_2a');

-- all shards are placed on the first worker node
SELECT sh.logicalrelid, pl.nodeport
    FROM pg_dist_shard sh JOIN pg_dist_shard_placement pl ON sh.shardid = pl.shardid
    WHERE sh.logicalrelid::text IN ('single_shard_colocation_1a', 'single_shard_colocation_1b', 'single_shard_colocation_1c', 'single_shard_colocation_2a', 'single_shard_colocation_2b')
    ORDER BY sh.logicalrelid;

-- add the second node back, then rebalance
ALTER SEQUENCE pg_dist_groupid_seq RESTART WITH 16;
select 1 from citus_add_node('localhost', :worker_2_port);
select rebalance_table_shards();

-- verify some shards are moved to the new node
SELECT sh.logicalrelid, pl.nodeport
    FROM pg_dist_shard sh JOIN pg_dist_shard_placement pl ON sh.shardid = pl.shardid
    WHERE sh.logicalrelid::text IN ('single_shard_colocation_1a', 'single_shard_colocation_1b', 'single_shard_colocation_1c', 'single_shard_colocation_2a', 'single_shard_colocation_2b')
    ORDER BY sh.logicalrelid;

DROP TABLE single_shard_colocation_1a, single_shard_colocation_1b, single_shard_colocation_1c, single_shard_colocation_2a, single_shard_colocation_2b CASCADE;

-- test the same with coordinator shouldhaveshards = false and shard_count = 2
-- so that the shard allowed node count would be 2 when rebalancing
-- for such cases, we only count the nodes that are allowed for shard placements
UPDATE pg_dist_node SET shouldhaveshards=false WHERE nodeport = :master_port;

create table two_shard_colocation_1a (a int primary key);
create table two_shard_colocation_1b (a int primary key);
SET citus.shard_replication_factor = 1;

select create_distributed_table('two_shard_colocation_1a','a', colocate_with => 'none', shard_count => 2);
select create_distributed_table('two_shard_colocation_1b','a',colocate_with=>'two_shard_colocation_1a');

create table two_shard_colocation_2a (a int primary key);
create table two_shard_colocation_2b (a int primary key);
select create_distributed_table('two_shard_colocation_2a','a', colocate_with => 'none', shard_count => 2);
select create_distributed_table('two_shard_colocation_2b','a',colocate_with=>'two_shard_colocation_2a');

-- move shards of colocation group 1 to worker1
SELECT citus_move_shard_placement(sh.shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port)
    FROM pg_dist_shard sh JOIN pg_dist_shard_placement pl ON sh.shardid = pl.shardid
    WHERE sh.logicalrelid = 'two_shard_colocation_1a'::regclass
        AND pl.nodeport = :worker_2_port
    LIMIT 1;
-- move shards of colocation group 2 to worker2
SELECT citus_move_shard_placement(sh.shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port)
    FROM pg_dist_shard sh JOIN pg_dist_shard_placement pl ON sh.shardid = pl.shardid
    WHERE sh.logicalrelid = 'two_shard_colocation_2a'::regclass
        AND pl.nodeport = :worker_1_port
    LIMIT 1;

-- current state:
-- coordinator: []
-- worker 1: [1_1, 1_2]
-- worker 2: [2_1, 2_2]
SELECT sh.logicalrelid, pl.nodeport
    FROM pg_dist_shard sh JOIN pg_dist_shard_placement pl ON sh.shardid = pl.shardid
    WHERE sh.logicalrelid::text IN ('two_shard_colocation_1a', 'two_shard_colocation_1b', 'two_shard_colocation_2a', 'two_shard_colocation_2b')
    ORDER BY sh.logicalrelid, pl.nodeport;

-- If we take the coordinator into account, the rebalancer considers this as balanced and does nothing (shard_count < worker_count)
-- but because the coordinator is not allowed for shards, rebalancer will distribute each colocation group to both workers
select rebalance_table_shards(shard_transfer_mode:='block_writes');

-- final state:
-- coordinator: []
-- worker 1: [1_1, 2_1]
-- worker 2: [1_2, 2_2]
SELECT sh.logicalrelid, pl.nodeport
    FROM pg_dist_shard sh JOIN pg_dist_shard_placement pl ON sh.shardid = pl.shardid
    WHERE sh.logicalrelid::text IN ('two_shard_colocation_1a', 'two_shard_colocation_1b', 'two_shard_colocation_2a', 'two_shard_colocation_2b')
    ORDER BY sh.logicalrelid, pl.nodeport;

-- cleanup
DROP TABLE two_shard_colocation_1a, two_shard_colocation_1b, two_shard_colocation_2a, two_shard_colocation_2b CASCADE;

-- verify we detect if one of the tables do not have a replica identity or primary key
-- and error out in case of shard transfer mode = auto
SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

create table table_with_primary_key (a int primary key);
select create_distributed_table('table_with_primary_key','a');
create table table_without_primary_key (a bigint);
select create_distributed_table('table_without_primary_key','a');

-- add the second node back, then rebalance
ALTER SEQUENCE pg_dist_groupid_seq RESTART WITH 16;
select 1 from citus_add_node('localhost', :worker_2_port);
select rebalance_table_shards();

DROP TABLE table_with_primary_key, table_without_primary_key;
SELECT citus_set_default_rebalance_strategy('by_disk_size');
ALTER SYSTEM RESET citus.rebalancer_by_disk_size_base_cost;
SELECT pg_reload_conf();
\c - - - :worker_1_port
SET citus.enable_ddl_propagation TO OFF;
REVOKE ALL ON SCHEMA public FROM testrole;
DROP USER testrole;

\c - - - :worker_2_port
SET citus.enable_ddl_propagation TO OFF;
REVOKE ALL ON SCHEMA public FROM testrole;
DROP USER testrole;
DROP TABLE test_rebalance_with_disabled_worker_433500, test_rebalance_with_disabled_worker_433501, test_rebalance_with_disabled_worker_433502, test_rebalance_with_disabled_worker_433503;
