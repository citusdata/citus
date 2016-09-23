--
-- MULTI_METADATA_SNAPSHOT
--

-- Tests for metadata snapshot functions.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1310000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1310000;


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
