SET citus.next_shard_id TO 1220000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1390000;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 1;

-- Tests functions related to cluster membership

-- add the nodes to the cluster with the same nodeids and groupids that
-- multi_cluster_management.sql creates
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 18;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 16;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 16;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 14;
SELECT 1 FROM master_add_node('localhost', :worker_1_port);

-- make sure coordinator is always in metadata.
SELECT citus_set_coordinator_host('localhost');

-- Create the same colocation groups as multi_cluster_management.sql
SET citus.shard_count TO 16;
SET citus.shard_replication_factor TO 1;
CREATE TABLE cluster_management_test (col_1 text, col_2 int);
SELECT create_distributed_table('cluster_management_test', 'col_1', 'hash');
DROP TABLE cluster_management_test;

CREATE TABLE test_reference_table (y int primary key, name text);
SELECT create_reference_table('test_reference_table');
DROP TABLE test_reference_table;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 2;
CREATE TABLE cluster_management_test (col_1 text, col_2 int);
SELECT create_distributed_table('cluster_management_test', 'col_1', 'hash');
DROP TABLE cluster_management_test;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
CREATE TABLE test_dist (x int, y int);
SELECT create_distributed_table('test_dist', 'x');
DROP TABLE test_dist;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 30;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 18;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 83;
