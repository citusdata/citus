-- https://github.com/citusdata/citus/issues/6592
SET citus.next_shard_id TO 180000;
CREATE TABLE ref_table_to_be_dropped_6592 (key int);
SELECT create_reference_table('ref_table_to_be_dropped_6592');
CREATE TABLE ref_table_oid AS SELECT oid FROM pg_class WHERE relname = 'ref_table_to_be_dropped_6592';
SET citus.enable_ddl_propagation TO OFF;
DROP TABLE ref_table_to_be_dropped_6592 CASCADE; -- citus_drop_all_shards doesn't drop shards and metadata
RESET citus.enable_ddl_propagation;

-- error out for the dropped reference table
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

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
