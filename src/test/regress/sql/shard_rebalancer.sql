--
-- MUTLI_SHARD_REBALANCER
--

CREATE TABLE dist_table_test(a int primary key);
SELECT create_distributed_table('dist_table_test', 'a');
CREATE TABLE ref_table_test(a int primary key);
SELECT create_reference_table('ref_table_test');

-- make sure that all rebalance operations works fine when
-- reference tables are replicated to the coordinator
SELECT 1 FROM master_add_node('localhost', :master_port, groupId=>0);

-- should just be noops even if we add the coordinator to the pg_dist_node
SELECT rebalance_table_shards('dist_table_test');
SELECT rebalance_table_shards();

-- test that calling rebalance_table_shards without specifying relation
-- wouldn't move shard of the citus local table.
CREATE TABLE citus_local_table(a int, b int);
SELECT citus_add_local_table_to_metadata('citus_local_table');
INSERT INTO citus_local_table VALUES (1, 2);

SELECT rebalance_table_shards();

-- show that citus local table shard is still on the coordinator
SELECT tablename FROM pg_catalog.pg_tables where tablename like 'citus_local_table_%';
-- also check that we still can access shard relation, not the shell table
SELECT count(*) FROM citus_local_table;

SELECT master_drain_node('localhost', :master_port);

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
SET citus.replication_model TO "statement";
SELECT create_distributed_table('dist_table_test_2', 'a');

-- replicate reference table should ignore the coordinator
SET citus.shard_replication_factor TO 2;
SELECT replicate_table_shards('dist_table_test_2',  max_shard_copies := 4,  shard_transfer_mode:='block_writes');

DROP TABLE dist_table_test, dist_table_test_2, ref_table_test;
RESET citus.shard_count;
RESET citus.shard_replication_factor;
RESET citus.replication_model;

-- Create a user to test multiuser usage of rebalancer functions
CREATE USER testrole;
GRANT ALL ON SCHEMA public TO testrole;

CREATE OR REPLACE FUNCTION shard_placement_rebalance_array(
    worker_node_list json[],
    shard_placement_list json[],
    threshold float4 DEFAULT 0,
    max_shard_moves int DEFAULT 1000000,
    drain_only bool DEFAULT false
)
RETURNS json[]
AS 'citus'
LANGUAGE C STRICT VOLATILE;


CREATE FUNCTION shard_placement_replication_array(worker_node_list json[],
                                                  shard_placement_list json[],
                                                  shard_replication_factor int)
RETURNS json[]
AS 'citus'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION worker_node_responsive(worker_node_name text, worker_node_port int)
RETURNS boolean
AS 'citus'
LANGUAGE C STRICT VOLATILE;

-- this function is dropped in Citus10, added here for tests
CREATE OR REPLACE FUNCTION pg_catalog.master_create_distributed_table(table_name regclass,
                                                                      distribution_column text,
                                                                      distribution_method citus.distribution_type)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus', $$master_create_distributed_table$$;
COMMENT ON FUNCTION pg_catalog.master_create_distributed_table(table_name regclass,
                                                               distribution_column text,
                                                               distribution_method citus.distribution_type)
    IS 'define the table distribution functions';

-- this function is dropped in Citus10, added here for tests
CREATE OR REPLACE FUNCTION pg_catalog.master_create_worker_shards(table_name text, shard_count integer,
                                                                  replication_factor integer DEFAULT 2)
    RETURNS void
    AS 'citus', $$master_create_worker_shards$$
    LANGUAGE C STRICT;

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
    ARRAY['{"node_name": "hostname1", "node_port": 5432}',
          '{"node_name": "hostname2", "node_port": 5432}']::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":2, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":3, "shardid":3, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":4, "shardid":4, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":5, "shardid":5, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":6, "shardid":6, "shardstate":1, "shardlength":1, "nodename":"hostname2", "nodeport":5432}']::json[],
    0.0
));

-- Check that with two nodes and threshold=1.0 shard_placement_rebalance_array
-- doesn't return any moves, even if it is completely unbalanced.

SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}',
          '{"node_name": "hostname2", "node_port": 5432}']::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":2, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":3, "shardid":3, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}']::json[],
    1.0
));

-- Check that with three nodes and threshold=1.0
-- shard_placement_rebalance_array returns moves when it is completely unbalanced
SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}',
          '{"node_name": "hostname2", "node_port": 5432}',
          '{"node_name": "hostname3", "node_port": 5432}'
        ]::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":2, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":3, "shardid":3, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}']::json[],
    1.0
));


-- Check that with with three nodes and threshold=2.0
-- shard_placement_rebalance_array doesn't return any moves, even if it is
-- completely unbalanced. (with three nodes)


SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}',
          '{"node_name": "hostname2", "node_port": 5432}',
          '{"node_name": "hostname3", "node_port": 5432}'
        ]::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":2, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":3, "shardid":3, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}']::json[],
    2.0
));

-- Check that with threshold=0.0 shard_placement_rebalance_array doesn't return
-- any moves if the cluster is already balanced.

SELECT unnest(shard_placement_rebalance_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}',
          '{"node_name": "hostname2", "node_port": 5432}']::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":2, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":3, "shardid":3, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":4, "shardid":4, "shardstate":1, "shardlength":1, "nodename":"hostname2", "nodeport":5432}',
          '{"placementid":5, "shardid":5, "shardstate":1, "shardlength":1, "nodename":"hostname2", "nodeport":5432}',
          '{"placementid":6, "shardid":6, "shardstate":1, "shardlength":1, "nodename":"hostname2", "nodeport":5432}']::json[],
    0.0
));

-- Check that shard_placement_replication_array returns a shard copy operation
-- for each of the shards in an inactive node.

SELECT unnest(shard_placement_replication_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}',
          '{"node_name": "hostname2", "node_port": 5432}']::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":2, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname2", "nodeport":5432}',
          '{"placementid":3, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname3", "nodeport":5432}',
          '{"placementid":4, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname3", "nodeport":5432}']::json[],
    2
));

-- Check that shard_placement_replication_array returns a shard copy operation
-- for each of the inactive shards.

SELECT unnest(shard_placement_replication_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}',
          '{"node_name": "hostname2", "node_port": 5432}']::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":2, "shardid":2, "shardstate":3, "shardlength":1, "nodename":"hostname1", "nodeport":5432}',
          '{"placementid":3, "shardid":1, "shardstate":3, "shardlength":1, "nodename":"hostname2", "nodeport":5432}',
          '{"placementid":4, "shardid":2, "shardstate":1, "shardlength":1, "nodename":"hostname2", "nodeport":5432}']::json[],
    2
));

-- Check that shard_placement_replication_array errors out if all placements of
-- a shard are placed on inactive nodes.

SELECT unnest(shard_placement_replication_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}']::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname2", "nodeport":5432}',
          '{"placementid":2, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname3", "nodeport":5432}']::json[],
    2
));

-- Check that shard_placement_replication_array errors out if replication factor
-- is more than number of active nodes.

SELECT unnest(shard_placement_replication_array(
    ARRAY['{"node_name": "hostname1", "node_port": 5432}']::json[],
    ARRAY['{"placementid":1, "shardid":1, "shardstate":1, "shardlength":1, "nodename":"hostname1", "nodeport":5432}']::json[],
    2
));

-- Ensure that shard_replication_factor is 2 during replicate_table_shards
-- and rebalance_table_shards tests

SET citus.shard_replication_factor TO 2;

-- Turn off NOTICE messages

SET client_min_messages TO WARNING;

-- Create a single-row test data for shard rebalancer test shards

CREATE TABLE shard_rebalancer_test_data AS SELECT 1::int as int_column;

-- Test replicate_table_shards, which will in turn test update_shard_placement
-- in copy mode.

CREATE TABLE replication_test_table(int_column int);
SELECT master_create_distributed_table('replication_test_table', 'int_column', 'append');

CREATE VIEW replication_test_table_placements_per_node AS
    SELECT count(*) FROM pg_dist_shard_placement NATURAL JOIN pg_dist_shard
    WHERE logicalrelid = 'replication_test_table'::regclass
    GROUP BY nodename, nodeport
    ORDER BY nodename, nodeport;

-- Create four shards with replication factor 2, and delete the placements
-- with smaller port number to simulate under-replicated shards.

SELECT count(master_create_empty_shard('replication_test_table'))
    FROM generate_series(1, 4);

DELETE FROM pg_dist_shard_placement WHERE placementid in (
    SELECT pg_dist_shard_placement.placementid
    FROM pg_dist_shard_placement NATURAL JOIN pg_dist_shard
    WHERE logicalrelid = 'replication_test_table'::regclass
        AND (nodename, nodeport) = (SELECT nodename, nodeport FROM pg_dist_shard_placement
                                    ORDER BY nodename, nodeport limit 1)
);

-- Upload the test data to the shards

SELECT count(master_append_table_to_shard(shardid, 'shard_rebalancer_test_data',
                                          host(inet_server_addr()), inet_server_port()))
    FROM pg_dist_shard
    WHERE logicalrelid = 'replication_test_table'::regclass;

-- Verify that there is one node with all placements

SELECT * FROM replication_test_table_placements_per_node;

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

SELECT replicate_table_shards('replication_test_table');

SELECT * FROM replication_test_table_placements_per_node;

-- Check that querying the table doesn't error out

SELECT count(*) FROM replication_test_table;

DROP TABLE public.replication_test_table CASCADE;

-- Test rebalance_table_shards, which will in turn test update_shard_placement
-- in move mode.

CREATE TABLE rebalance_test_table(int_column int);
SELECT master_create_distributed_table('rebalance_test_table', 'int_column', 'append');

CREATE VIEW table_placements_per_node AS
SELECT nodeport, logicalrelid::regclass, count(*)
FROM pg_dist_shard_placement NATURAL JOIN pg_dist_shard
GROUP BY logicalrelid::regclass, nodename, nodeport
ORDER BY logicalrelid::regclass, nodename, nodeport;

-- Create six shards with replication factor 1 and move them to the same
-- node to create an unbalanced cluster.

CREATE PROCEDURE create_unbalanced_shards(rel text)
LANGUAGE SQL
AS $$
    SET citus.shard_replication_factor TO 1;

    SELECT count(master_create_empty_shard(rel))
    FROM generate_series(1, 6);

    SELECT count(master_move_shard_placement(shardid,
            src.nodename, src.nodeport::int,
            dst.nodename, dst.nodeport::int,
            shard_transfer_mode:='block_writes'))
    FROM pg_dist_shard s JOIN
    pg_dist_shard_placement src USING (shardid),
    (SELECT nodename, nodeport FROM pg_dist_shard_placement ORDER BY nodeport DESC LIMIT 1) dst
    WHERE src.nodeport < dst.nodeport AND s.logicalrelid = rel::regclass;
$$;

CALL create_unbalanced_shards('rebalance_test_table');

SET citus.shard_replication_factor TO 2;

-- Upload the test data to the shards

SELECT count(master_append_table_to_shard(shardid, 'shard_rebalancer_test_data',
        host(inet_server_addr()), inet_server_port()))
FROM pg_dist_shard
WHERE logicalrelid = 'rebalance_test_table'::regclass;

-- Verify that there is one node with all placements

SELECT * FROM table_placements_per_node;

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

SELECT * FROM table_placements_per_node;

-- Check that max_shard_moves limits number of move operations

-- First check that we error if not table owner
SET ROLE testrole;
SELECT rebalance_table_shards('rebalance_test_table',
    threshold := 0, max_shard_moves := 1,
    shard_transfer_mode:='block_writes');
RESET ROLE;

SELECT rebalance_table_shards('rebalance_test_table',
    threshold := 0, max_shard_moves := 1,
    shard_transfer_mode:='block_writes');

SELECT * FROM table_placements_per_node;

-- Check that threshold=1 doesn't move any shards

SELECT rebalance_table_shards('rebalance_test_table', threshold := 1, shard_transfer_mode:='block_writes');

SELECT * FROM table_placements_per_node;

-- Move the remaining shards using threshold=0

SELECT rebalance_table_shards('rebalance_test_table', threshold := 0);

SELECT * FROM table_placements_per_node;

-- Check that shard is completely balanced and rebalancing again doesn't have
-- any effects.

SELECT rebalance_table_shards('rebalance_test_table', threshold := 0, shard_transfer_mode:='block_writes');

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

SELECT master_create_distributed_table('test_schema_support.nation_hash', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support.nation_hash', 4, 1);

CREATE TABLE test_schema_support.nation_hash2 (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);

SELECT master_create_distributed_table('test_schema_support.nation_hash2', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support.nation_hash2', 4, 1);

-- Shard count before replication
SELECT COUNT(*) FROM pg_dist_shard_placement;

SET search_path TO public;
SELECT replicate_table_shards('test_schema_support.nation_hash', shard_transfer_mode:='block_writes');

-- Confirm replication
SELECT COUNT(*) FROM pg_dist_shard_placement;

-- Test with search_path is set
SET search_path TO test_schema_support;
SELECT replicate_table_shards('nation_hash2', shard_transfer_mode:='block_writes');

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

SELECT master_create_distributed_table('test_schema_support.imbalanced_table', 'id', 'append');

SET citus.shard_replication_factor TO 1;
SELECT * from master_create_empty_shard('test_schema_support.imbalanced_table');
SELECT master_append_table_to_shard(123018, 'test_schema_support.imbalanced_table_local', 'localhost', :master_port);

SET citus.shard_replication_factor TO 2;
SELECT * from master_create_empty_shard('test_schema_support.imbalanced_table');
SELECT master_append_table_to_shard(123019, 'test_schema_support.imbalanced_table_local', 'localhost', :master_port);

SET citus.shard_replication_factor TO 1;
SELECT * from master_create_empty_shard('test_schema_support.imbalanced_table');
SELECT master_append_table_to_shard(123020, 'test_schema_support.imbalanced_table_local', 'localhost', :master_port);

-- imbalanced_table is now imbalanced

-- Shard counts in each node before rebalance
SELECT * FROM public.table_placements_per_node;

-- Row count in imbalanced table before rebalance
SELECT COUNT(*) FROM imbalanced_table;

-- Try force_logical
SELECT rebalance_table_shards('imbalanced_table', threshold:=0, shard_transfer_mode:='force_logical');

-- Test rebalance operation
SELECT rebalance_table_shards('imbalanced_table', threshold:=0, shard_transfer_mode:='block_writes');

-- Confirm rebalance
-- Shard counts in each node after rebalance
SELECT * FROM public.table_placements_per_node;

-- Row count in imbalanced table after rebalance
SELECT COUNT(*) FROM imbalanced_table;

DROP TABLE public.shard_rebalancer_test_data;
DROP TABLE test_schema_support.imbalanced_table;
DROP TABLE test_schema_support.imbalanced_table_local;

SET citus.shard_replication_factor TO 1;

CREATE TABLE colocated_rebalance_test(id integer);
CREATE TABLE colocated_rebalance_test2(id integer);
SELECT create_distributed_table('colocated_rebalance_test', 'id');

-- Move all shards to worker1
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes')
FROM pg_dist_shard_placement
WHERE nodeport = :worker_2_port;


SELECT create_distributed_table('colocated_rebalance_test2', 'id');

-- Confirm all shards for both tables are on worker1
SELECT * FROM public.table_placements_per_node;

-- Confirm that the plan for drain_only doesn't show any moves
SELECT * FROM get_rebalance_table_shards_plan('colocated_rebalance_test', threshold := 0, drain_only := true);
-- Running with drain_only shouldn't do anything
SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes', drain_only := true);

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
-- Check that we can call this function without a crash
SELECT * FROM get_rebalance_progress();

-- Confirm that the nodes are now there
SELECT * FROM public.table_placements_per_node;


CREATE TABLE non_colocated_rebalance_test(id integer);
SELECT create_distributed_table('non_colocated_rebalance_test', 'id', colocate_with := 'none');
-- confirm that both colocation groups are balanced
SELECT * FROM public.table_placements_per_node;

-- testing behaviour when setting isdatanode to 'marked for draining'
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);

SELECT * FROM get_rebalance_table_shards_plan('colocated_rebalance_test', threshold := 0);
SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;

SELECT * FROM get_rebalance_table_shards_plan('non_colocated_rebalance_test', threshold := 0);
SELECT * FROM rebalance_table_shards('non_colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

SELECT * FROM rebalance_table_shards('colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;
SELECT * FROM rebalance_table_shards('non_colocated_rebalance_test', threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;

-- testing behaviour when setting shouldhaveshards to false and rebalancing all
-- colocation groups with drain_only=true
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);
SELECT * FROM get_rebalance_table_shards_plan(threshold := 0, drain_only := true);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes', drain_only := true);
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;

-- testing behaviour when setting shouldhaveshards to false and rebalancing all
-- colocation groups with drain_only=false
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);
SELECT * FROM get_rebalance_table_shards_plan(threshold := 0);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;

-- Make it a data node again
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

-- testing behaviour of master_drain_node
SELECT * from master_drain_node('localhost', :worker_2_port, shard_transfer_mode := 'block_writes');
select shouldhaveshards from pg_dist_node where nodeport = :worker_2_port;
SELECT * FROM public.table_placements_per_node;

-- Put shards back
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
SELECT * FROM rebalance_table_shards(threshold := 0, shard_transfer_mode := 'block_writes');
SELECT * FROM public.table_placements_per_node;


-- Drop some tables for clear consistent error
DROP TABLE test_schema_support.colocated_rebalance_test2;

-- Leave no trace on workers
RESET search_path;

\set VERBOSITY terse
DROP SCHEMA test_schema_support CASCADE;
\set VERBOSITY default

REVOKE ALL ON SCHEMA public FROM testrole;
DROP USER testrole;

-- Test costs
set citus.shard_count = 4;
CREATE TABLE tab (x int);
SELECT create_distributed_table('tab','x');
-- The following numbers are chosen such that they are placed on different
-- shards.
INSERT INTO tab SELECT 1 from generate_series(1, 30000);
INSERT INTO tab SELECT 2 from generate_series(1, 10000);
INSERT INTO tab SELECT 3 from generate_series(1, 10000);
INSERT INTO tab SELECT 6 from generate_series(1, 10000);
ANALYZE tab;

\c - - - :worker_1_port
SELECT table_schema, table_name, row_estimate, total_bytes
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
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :worker_2_port
SELECT table_schema, table_name, row_estimate, total_bytes
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
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;

\c - - - :master_port

SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'by_disk_size');
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'by_disk_size', threshold := 0);

SELECT * FROM rebalance_table_shards('tab', shard_transfer_mode:='block_writes');
SELECT * FROM public.table_placements_per_node;

SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'by_disk_size', shard_transfer_mode:='block_writes');
SELECT * FROM public.table_placements_per_node;

SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'by_disk_size', shard_transfer_mode:='block_writes', threshold := 0);
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
ANALYZE tab, tab2;

\c - - - :worker_1_port
SELECT table_schema, table_name, row_estimate, total_bytes
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
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :worker_2_port
SELECT table_schema, table_name, row_estimate, total_bytes
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
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;

\c - - - :master_port
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'by_disk_size');
SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'by_disk_size', shard_transfer_mode:='block_writes');
SELECT * FROM public.table_placements_per_node;
ANALYZE tab, tab2;

\c - - - :worker_1_port
SELECT table_schema, table_name, row_estimate, total_bytes
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
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :worker_2_port
SELECT table_schema, table_name, row_estimate, total_bytes
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
  ) a
WHERE table_schema = 'public'
) a ORDER BY table_name;
\c - - - :master_port

DROP TABLE tab2;

CREATE OR REPLACE FUNCTION capacity_high_worker_1(nodeidarg int)
    RETURNS real AS $$
    SELECT
        (CASE WHEN nodeport = 57637 THEN 1000 ELSE 1 END)::real
    FROM pg_dist_node where nodeid = nodeidarg
    $$ LANGUAGE sql;

SELECT citus_add_rebalance_strategy(
        'capacity_high_worker_1',
        'citus_shard_cost_1',
        'capacity_high_worker_1',
        'citus_shard_allowed_on_node_true',
        0
    );

SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'capacity_high_worker_1');
SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'capacity_high_worker_1', shard_transfer_mode:='block_writes');
SELECT * FROM public.table_placements_per_node;

SELECT citus_set_default_rebalance_strategy('capacity_high_worker_1');
SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM rebalance_table_shards('tab', shard_transfer_mode:='block_writes');
SELECT * FROM public.table_placements_per_node;

CREATE FUNCTION only_worker_2(shardid bigint, nodeidarg int)
    RETURNS boolean AS $$
    SELECT
        (CASE WHEN nodeport = 57638 THEN TRUE ELSE FALSE END)
    FROM pg_dist_node where nodeid = nodeidarg
    $$ LANGUAGE sql;

SELECT citus_add_rebalance_strategy(
        'only_worker_2',
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'only_worker_2',
        0
    );

SELECT citus_set_default_rebalance_strategy('only_worker_2');
SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM rebalance_table_shards('tab', shard_transfer_mode:='block_writes');
SELECT * FROM public.table_placements_per_node;

SELECT citus_set_default_rebalance_strategy('by_shard_count');
SELECT * FROM get_rebalance_table_shards_plan('tab');

-- Check all the error handling cases
SELECT * FROM get_rebalance_table_shards_plan('tab', rebalance_strategy := 'non_existing');
SELECT * FROM rebalance_table_shards('tab', rebalance_strategy := 'non_existing');
SELECT * FROM master_drain_node('localhost', :worker_2_port, rebalance_strategy := 'non_existing');
SELECT citus_set_default_rebalance_strategy('non_existing');


UPDATE pg_dist_rebalance_strategy SET default_strategy=false;
SELECT * FROM get_rebalance_table_shards_plan('tab');
SELECT * FROM rebalance_table_shards('tab');
SELECT * FROM master_drain_node('localhost', :worker_2_port);
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
        'capacity_high_worker_1',
        'citus_shard_allowed_on_node_true',
        0,
        0.1
    );

-- Make it a data node again
SELECT * from master_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);
DROP TABLE tab;


-- we don't need the coordinator on pg_dist_node anymore
SELECT 1 FROM master_remove_node('localhost', :master_port);


--
-- Make sure that rebalance_table_shards() and replicate_table_shards() replicate
-- reference tables to the coordinator when replicate_reference_tables_on_activate
-- is off.
--

SET citus.replicate_reference_tables_on_activate TO off;
SET client_min_messages TO WARNING;

CREATE TABLE dist_table_test_3(a int);
SET citus.shard_count TO 4;

SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO "statement";
SELECT create_distributed_table('dist_table_test_3', 'a');

CREATE TABLE ref_table(a int);
SELECT create_reference_table('ref_table');

SELECT 1 FROM master_add_node('localhost', :master_port, groupId=>0);

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

SET citus.shard_replication_factor TO 2;
SELECT replicate_table_shards('dist_table_test_3',  max_shard_copies := 4,  shard_transfer_mode:='block_writes');

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

SELECT 1 FROM master_remove_node('localhost', :master_port);

CREATE TABLE rebalance_test_table(int_column int);
SELECT master_create_distributed_table('rebalance_test_table', 'int_column', 'append');

CALL create_unbalanced_shards('rebalance_test_table');

SELECT 1 FROM master_add_node('localhost', :master_port, groupId=>0);

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

SELECT rebalance_table_shards('rebalance_test_table', shard_transfer_mode:='block_writes');

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

DROP TABLE dist_table_test_3, rebalance_test_table, ref_table;

SELECT 1 FROM master_remove_node('localhost', :master_port);

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

SELECT create_distributed_table('t1','a');
SELECT create_reference_table('r1');
SELECT create_reference_table('r2');

-- add data so to actually copy data when forcing logical replication for reference tables
INSERT INTO r1 VALUES (1,2), (3,4);
INSERT INTO r2 VALUES (1,2), (3,4);

SELECT 1 from master_add_node('localhost', :worker_2_port);

SELECT rebalance_table_shards();

DROP TABLE t1, r1, r2;

-- verify there are no distributed tables before we perform the following tests. Preceding
-- test suites should clean up their distributed tables.
SELECT count(*) FROM pg_dist_partition;

-- verify a system having only reference tables will copy the reference tables when
-- executing the rebalancer

SELECT 1 from master_remove_node('localhost', :worker_2_port);

CREATE TABLE r1 (a int PRIMARY KEY, b int);
SELECT create_reference_table('r1');

SELECT 1 from master_add_node('localhost', :worker_2_port);

-- count the number of placements for the reference table to verify it is not available on
-- all nodes
SELECT count(*)
FROM pg_dist_shard
JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'r1'::regclass;

-- rebalance with _only_ a reference table, this should trigger the copy
SELECT rebalance_table_shards();

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

-- verify the reference table is on all nodes after replicate_table_shards
SELECT count(*)
FROM pg_dist_shard
JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'r1'::regclass;

DROP TABLE t1, r1;
