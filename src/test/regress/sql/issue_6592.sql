-- https://github.com/citusdata/citus/issues/6592

-- first, make sure to remove the coordinator if it was already added
SET client_min_messages to ERROR;
SELECT COUNT(*)>=0 FROM (
    SELECT master_remove_node(nodename, nodeport)
    FROM pg_dist_node
    WHERE nodename = 'localhost' AND nodeport = :master_port
) q;
RESET client_min_messages;

SET citus.next_shard_id TO 180000;
CREATE TABLE ref_table_to_be_dropped_6592 (key int);
SELECT create_reference_table('ref_table_to_be_dropped_6592');
CREATE TABLE ref_table_oid AS SELECT oid FROM pg_class WHERE relname = 'ref_table_to_be_dropped_6592';
SET citus.enable_ddl_propagation TO OFF;
DROP TABLE ref_table_to_be_dropped_6592 CASCADE; -- citus_drop_all_shards doesn't drop shards and metadata

-- add the coordinator to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- error out for the dropped reference table
CREATE TABLE citus_local_table(id int, other_column int);
SELECT citus_add_local_table_to_metadata('citus_local_table');
RESET citus.enable_ddl_propagation;
DROP TABLE citus_local_table;

\c - - - :worker_1_port
SET citus.enable_ddl_propagation TO OFF;
DELETE FROM pg_dist_partition WHERE logicalrelid = 'ref_table_to_be_dropped_6592'::regclass;
DELETE FROM pg_dist_placement WHERE shardid = 180000;
DELETE FROM pg_dist_shard WHERE shardid = 180000;
DROP TABLE IF EXISTS ref_table_to_be_dropped_6592;
DROP TABLE IF EXISTS ref_table_to_be_dropped_6592_180000;
\c - - - :worker_2_port
SET citus.enable_ddl_propagation TO OFF;
DELETE FROM pg_dist_partition WHERE logicalrelid = 'ref_table_to_be_dropped_6592'::regclass;
DELETE FROM pg_dist_placement WHERE shardid = 180000;
DELETE FROM pg_dist_shard WHERE shardid = 180000;
DROP TABLE IF EXISTS ref_table_to_be_dropped_6592;
DROP TABLE IF EXISTS ref_table_to_be_dropped_6592_180000;
\c - - - :master_port
DELETE FROM pg_dist_placement WHERE shardid = 180000;
DELETE FROM pg_dist_shard WHERE shardid = 180000;
DELETE FROM pg_dist_partition WHERE logicalrelid IN (SELECT oid FROM ref_table_oid);
DROP TABLE ref_table_oid;
SELECT 1 FROM citus_remove_node('localhost', :master_port);

-- test the same when creating a distributed-schema table from a worker

SET citus.next_shard_id TO 180100;
CREATE TABLE other_ref_table_to_be_dropped (key int);
SELECT create_reference_table('other_ref_table_to_be_dropped');
CREATE TABLE other_ref_table_oid AS SELECT oid FROM pg_class WHERE relname = 'other_ref_table_to_be_dropped';

SET citus.enable_ddl_propagation TO OFF;
DROP TABLE other_ref_table_to_be_dropped CASCADE; -- citus_drop_all_shards doesn't drop shards and metadata
RESET citus.enable_ddl_propagation;

-- add the coordinator to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- As we always grab the next shard id from the coordinator, we need to alter
-- the sequence on the coordinator, so we first store it. Since the connection
-- that we internally use to get the next shard id from the coordinator might
-- change, we cannot just set citus.next_shard_id on the coordinator because
-- doing so wouldn't affect the further connections to the coordinator.
SELECT last_value::bigint INTO pg_dist_shardid_seq_prev_state FROM pg_catalog.pg_dist_shardid_seq;
ALTER SEQUENCE pg_dist_shardid_seq RESTART WITH 180150;

\c - - - :worker_1_port

SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA s1;
RESET citus.enable_schema_based_sharding;

-- errors out for the dropped reference table
SET citus.shard_replication_factor TO 1;
CREATE TABLE s1.t1 (a int);

\c - - - :worker_1_port
SET citus.enable_ddl_propagation TO OFF;
DELETE FROM pg_dist_partition WHERE logicalrelid = 'other_ref_table_to_be_dropped'::regclass;
DELETE FROM pg_dist_placement WHERE shardid = 180100;
DELETE FROM pg_dist_shard WHERE shardid = 180100;
DROP TABLE IF EXISTS other_ref_table_to_be_dropped;
DROP TABLE IF EXISTS other_ref_table_to_be_dropped_180100;
\c - - - :worker_2_port
SET citus.enable_ddl_propagation TO OFF;
DELETE FROM pg_dist_partition WHERE logicalrelid = 'other_ref_table_to_be_dropped'::regclass;
DELETE FROM pg_dist_placement WHERE shardid = 180100;
DELETE FROM pg_dist_shard WHERE shardid = 180100;
DROP TABLE IF EXISTS other_ref_table_to_be_dropped;
DROP TABLE IF EXISTS other_ref_table_to_be_dropped_180100;
\c - - - :master_port
DELETE FROM pg_dist_placement WHERE shardid = 180100;
DELETE FROM pg_dist_shard WHERE shardid = 180100;
DELETE FROM pg_dist_partition WHERE logicalrelid IN (SELECT oid FROM other_ref_table_oid);
DROP TABLE other_ref_table_oid;
DROP SCHEMA s1;
SELECT 1 FROM citus_remove_node('localhost', :master_port);

-- reset pg_dist_shardid_seq on the coordinator
DO $proc$
DECLARE
    v_last_value bigint;
BEGIN
    SELECT last_value INTO v_last_value FROM pg_dist_shardid_seq_prev_state;
    EXECUTE format('ALTER SEQUENCE pg_dist_shardid_seq RESTART WITH %s', v_last_value);
END$proc$;

DROP TABLE pg_dist_shardid_seq_prev_state;
