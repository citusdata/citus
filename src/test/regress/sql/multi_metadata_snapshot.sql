--
-- MULTI_METADATA_SNAPSHOT
--

-- Tests for metadata snapshot functions.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1310000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1310000;

SELECT nextval('pg_catalog.pg_dist_shard_placement_placementid_seq') AS last_placement_id
\gset
ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq RESTART 100000;

-- Create the necessary test utility function
CREATE FUNCTION master_metadata_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';
    
COMMENT ON FUNCTION master_metadata_snapshot()
    IS 'commands to create the metadata snapshot';
    
-- Show that none of the existing tables are qualified to be MX tables
SELECT * FROM pg_dist_partition WHERE partmethod='h' AND repmodel='s';

-- Show that, with no MX tables, metadata snapshot contains only the delete commands and 
-- pg_dist_node entries
SELECT unnest(master_metadata_snapshot());

-- Create a test table with constraints and SERIAL
CREATE TABLE mx_test_table (col_1 int UNIQUE, col_2 text NOT NULL, col_3 SERIAL);
SELECT master_create_distributed_table('mx_test_table', 'col_1', 'hash');
SELECT master_create_worker_shards('mx_test_table', 8, 1);

-- Set the replication model of the test table to streaming replication so that it is 
-- considered as an MX table
UPDATE pg_dist_partition SET repmodel='s' WHERE logicalrelid='mx_test_table'::regclass;

-- Show that the created MX table is included in the metadata snapshot
SELECT unnest(master_metadata_snapshot());

-- Show that CREATE INDEX commands are included in the metadata snapshot
CREATE INDEX mx_index ON mx_test_table(col_2);
SELECT unnest(master_metadata_snapshot());

-- Show that schema changes are included in the metadata snapshot
CREATE SCHEMA mx_testing_schema;
ALTER TABLE mx_test_table SET SCHEMA mx_testing_schema;
SELECT unnest(master_metadata_snapshot());

-- Show that append distributed tables are not included in the metadata snapshot
CREATE TABLE non_mx_test_table (col_1 int, col_2 text);
SELECT master_create_distributed_table('non_mx_test_table', 'col_1', 'append');
UPDATE pg_dist_partition SET repmodel='s' WHERE logicalrelid='non_mx_test_table'::regclass;
SELECT unnest(master_metadata_snapshot());

-- Show that range distributed tables are not included in the metadata snapshot
UPDATE pg_dist_partition SET partmethod='r' WHERE logicalrelid='non_mx_test_table'::regclass;
SELECT unnest(master_metadata_snapshot());

-- Test start_metadata_sync_to_node UDF

-- Ensure that hasmetadata=false for all nodes
SELECT count(*) FROM pg_dist_node WHERE hasmetadata=true;

-- Run start_metadata_sync_to_node and check that it marked hasmetadata for that worker
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT nodeid, hasmetadata FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_1_port;

-- Check that the metadata has been copied to the worker
\c - - - :worker_1_port
SELECT * FROM pg_dist_local_group;
SELECT * FROM pg_dist_node ORDER BY nodeid;
SELECT * FROM pg_dist_partition ORDER BY logicalrelid;
SELECT * FROM pg_dist_shard ORDER BY shardid;
SELECT * FROM pg_dist_shard_placement ORDER BY shardid;
\d mx_testing_schema.mx_test_table

-- Check that pg_dist_colocation is not synced
SELECT * FROM pg_dist_colocation ORDER BY colocationid;

-- Make sure that truncate trigger has been set for the MX table on worker
SELECT count(*) FROM pg_trigger WHERE tgrelid='mx_testing_schema.mx_test_table'::regclass;

-- Make sure that start_metadata_sync_to_node considers foreign key constraints
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA mx_testing_schema_2;

CREATE TABLE mx_testing_schema.fk_test_1 (col1 int, col2 text, col3 int, UNIQUE(col1, col3));
CREATE TABLE mx_testing_schema_2.fk_test_2 (col1 int, col2 int, col3 text, 
	FOREIGN KEY (col1, col2) REFERENCES mx_testing_schema.fk_test_1 (col1, col3));

SELECT create_distributed_table('mx_testing_schema.fk_test_1', 'col1');
SELECT create_distributed_table('mx_testing_schema_2.fk_test_2', 'col1');

UPDATE 
	pg_dist_partition SET repmodel='s' 
WHERE 
	logicalrelid='mx_testing_schema.fk_test_1'::regclass
	OR logicalrelid='mx_testing_schema_2.fk_test_2'::regclass;
		
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- Check that foreign key metadata exists on the worker
\c - - - :worker_1_port
\d mx_testing_schema_2.fk_test_2
\c - - - :master_port

RESET citus.shard_replication_factor;

-- Check that repeated calls to start_metadata_sync_to_node has no side effects
\c - - - :master_port
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
\c - - - :worker_1_port
SELECT * FROM pg_dist_local_group;
SELECT * FROM pg_dist_node ORDER BY nodeid;
SELECT * FROM pg_dist_partition ORDER BY logicalrelid;
SELECT * FROM pg_dist_shard ORDER BY shardid;
SELECT * FROM pg_dist_shard_placement ORDER BY shardid;
\d mx_testing_schema.mx_test_table
SELECT count(*) FROM pg_trigger WHERE tgrelid='mx_testing_schema.mx_test_table'::regclass;

-- Make sure that start_metadata_sync_to_node cannot be called inside a transaction
\c - - - :master_port
BEGIN;
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);
ROLLBACK;

SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_port;

-- Check that stop_metadata_sync_to_node function sets hasmetadata of the node to false 
\c - - - :master_port
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_1_port;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_1_port;

-- Cleanup
\c - - - :worker_1_port
DROP TABLE mx_testing_schema.mx_test_table;
DELETE FROM pg_dist_node;
DELETE FROM pg_dist_partition;
DELETE FROM pg_dist_shard;
DELETE FROM pg_dist_shard_placement;
\d mx_testing_schema.mx_test_table

\c - - - :master_port
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);
DROP TABLE mx_testing_schema.mx_test_table CASCADE;

ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq RESTART :last_placement_id;
