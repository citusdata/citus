--
-- MULTI_METADATA_SYNC
--

-- Tests for metadata snapshot functions, metadata syncing functions and propagation of
-- metadata changes to MX tables.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1310000;

SELECT nextval('pg_catalog.pg_dist_shard_placement_placementid_seq') AS last_placement_id
\gset
ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq RESTART 100000;

SELECT nextval('pg_catalog.pg_dist_groupid_seq') AS last_group_id \gset
SELECT nextval('pg_catalog.pg_dist_node_nodeid_seq') AS last_node_id \gset

-- Create the necessary test utility function
CREATE FUNCTION master_metadata_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';
    
COMMENT ON FUNCTION master_metadata_snapshot()
    IS 'commands to create the metadata snapshot';
    
-- Show that none of the existing tables are qualified to be MX tables
SELECT * FROM pg_dist_partition WHERE partmethod='h' AND repmodel='s';

-- Show that, with no MX tables, metadata snapshot contains only the delete commands,
-- pg_dist_node entries and reference tables
SELECT unnest(master_metadata_snapshot());

-- Create a test table with constraints and SERIAL
CREATE TABLE mx_test_table (col_1 int UNIQUE, col_2 text NOT NULL, col_3 BIGSERIAL);
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
SELECT * FROM pg_dist_shard_placement ORDER BY shardid, nodename, nodeport;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_testing_schema.mx_test_table'::regclass;
\d mx_testing_schema.mx_test_table_col_1_key
\d mx_testing_schema.mx_index

-- Check that pg_dist_colocation is not synced
SELECT * FROM pg_dist_colocation ORDER BY colocationid;

-- Make sure that truncate trigger has been set for the MX table on worker
SELECT count(*) FROM pg_trigger WHERE tgrelid='mx_testing_schema.mx_test_table'::regclass;

-- Make sure that start_metadata_sync_to_node considers foreign key constraints
\c - - - :master_port

-- Since we're superuser, we can set the replication model to 'streaming' to
-- create some MX tables
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

CREATE SCHEMA mx_testing_schema_2;

CREATE TABLE mx_testing_schema.fk_test_1 (col1 int, col2 text, col3 int, UNIQUE(col1, col3));
CREATE TABLE mx_testing_schema_2.fk_test_2 (col1 int, col2 int, col3 text, 
	FOREIGN KEY (col1, col2) REFERENCES mx_testing_schema.fk_test_1 (col1, col3));

SELECT create_distributed_table('mx_testing_schema.fk_test_1', 'col1');
SELECT create_distributed_table('mx_testing_schema_2.fk_test_2', 'col1');
		
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- Check that foreign key metadata exists on the worker
\c - - - :worker_1_port
SELECT "Constraint", "Definition" FROM table_fkeys WHERE relid='mx_testing_schema_2.fk_test_2'::regclass;

\c - - - :master_port
DROP TABLE mx_testing_schema_2.fk_test_2;
DROP TABLE mx_testing_schema.fk_test_1;

RESET citus.shard_replication_factor;
RESET citus.replication_model;

-- Check that repeated calls to start_metadata_sync_to_node has no side effects
\c - - - :master_port
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
\c - - - :worker_1_port
SELECT * FROM pg_dist_local_group;
SELECT * FROM pg_dist_node ORDER BY nodeid;
SELECT * FROM pg_dist_partition ORDER BY logicalrelid;
SELECT * FROM pg_dist_shard ORDER BY shardid;
SELECT * FROM pg_dist_shard_placement ORDER BY shardid, nodename, nodeport;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_testing_schema.mx_test_table'::regclass;
\d mx_testing_schema.mx_test_table_col_1_key
\d mx_testing_schema.mx_index
SELECT count(*) FROM pg_trigger WHERE tgrelid='mx_testing_schema.mx_test_table'::regclass;

-- Make sure that start_metadata_sync_to_node cannot be called inside a transaction
\c - - - :master_port
BEGIN;
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);
ROLLBACK;

SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_port;

-- Check that the distributed table can be queried from the worker
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

CREATE TABLE mx_query_test (a int, b text, c int);
SELECT create_distributed_table('mx_query_test', 'a');

SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='mx_query_test'::regclass;

INSERT INTO mx_query_test VALUES (1, 'one', 1);
INSERT INTO mx_query_test VALUES (2, 'two', 4);
INSERT INTO mx_query_test VALUES (3, 'three', 9);
INSERT INTO mx_query_test VALUES (4, 'four', 16);
INSERT INTO mx_query_test VALUES (5, 'five', 24);

\c - - - :worker_1_port
SELECT * FROM mx_query_test ORDER BY a;
INSERT INTO mx_query_test VALUES (6, 'six', 36);
UPDATE mx_query_test SET c = 25 WHERE a = 5;

\c - - - :master_port
SELECT * FROM mx_query_test ORDER BY a;

\c - - - :master_port
DROP TABLE mx_query_test;

-- Check that stop_metadata_sync_to_node function sets hasmetadata of the node to false 
\c - - - :master_port
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_1_port;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_1_port;


-- Test DDL propagation in MX tables
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SET citus.shard_count = 5;
SET citus.multi_shard_commit_protocol TO '2pc';
CREATE SCHEMA mx_test_schema_1;
CREATE SCHEMA mx_test_schema_2;

-- Create MX tables
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
CREATE TABLE mx_test_schema_1.mx_table_1 (col1 int UNIQUE, col2 text);
CREATE INDEX mx_index_1 ON mx_test_schema_1.mx_table_1 (col1);

CREATE TABLE mx_test_schema_2.mx_table_2 (col1 int, col2 text);
CREATE INDEX mx_index_2 ON mx_test_schema_2.mx_table_2 (col2);
ALTER TABLE mx_test_schema_2.mx_table_2 ADD CONSTRAINT mx_fk_constraint FOREIGN KEY(col1) REFERENCES mx_test_schema_1.mx_table_1(col1);

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_test_schema_1.mx_table_1'::regclass;
\d mx_test_schema_1.mx_table_1_col1_key
\d mx_test_schema_1.mx_index_1

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_test_schema_2.mx_table_2'::regclass;
\d mx_test_schema_2.mx_index_2
SELECT "Constraint", "Definition" FROM table_fkeys WHERE relid='mx_test_schema_2.mx_table_2'::regclass;

SELECT create_distributed_table('mx_test_schema_1.mx_table_1', 'col1');
SELECT create_distributed_table('mx_test_schema_2.mx_table_2', 'col1');

-- Check that created tables are marked as streaming replicated tables
SELECT 
	logicalrelid, repmodel 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid = 'mx_test_schema_1.mx_table_1'::regclass
	OR logicalrelid = 'mx_test_schema_2.mx_table_2'::regclass
ORDER BY 
	logicalrelid;

-- See the shards and placements of the mx tables 
SELECT 
	logicalrelid, shardid, nodename, nodeport
FROM 
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE 
	logicalrelid = 'mx_test_schema_1.mx_table_1'::regclass
	OR logicalrelid = 'mx_test_schema_2.mx_table_2'::regclass
ORDER BY 
	logicalrelid, shardid;
	
-- Check that metadata of MX tables exist on the metadata worker
\c - - - :worker_1_port

-- Check that tables are created
\dt mx_test_schema_?.mx_table_?

-- Check that table metadata are created
SELECT 
	logicalrelid, repmodel 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid = 'mx_test_schema_1.mx_table_1'::regclass
	OR logicalrelid = 'mx_test_schema_2.mx_table_2'::regclass;

-- Check that shard and placement data are created
SELECT 
	logicalrelid, shardid, nodename, nodeport
FROM 
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE 
	logicalrelid = 'mx_test_schema_1.mx_table_1'::regclass
	OR logicalrelid = 'mx_test_schema_2.mx_table_2'::regclass
ORDER BY 
	logicalrelid, shardid;

-- Check that metadata of MX tables don't exist on the non-metadata worker
\c - - - :worker_2_port

\d mx_test_schema_1.mx_table_1
\d mx_test_schema_2.mx_table_2

SELECT * FROM pg_dist_partition;
SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement ORDER BY shardid, nodename, nodeport;

-- Check that CREATE INDEX statement is propagated
\c - - - :master_port
SET citus.multi_shard_commit_protocol TO '2pc';
SET client_min_messages TO 'ERROR';
CREATE INDEX mx_index_3 ON mx_test_schema_2.mx_table_2 USING hash (col1);
ALTER TABLE mx_test_schema_2.mx_table_2 ADD CONSTRAINT mx_table_2_col1_key UNIQUE (col1);
\c - - - :worker_1_port
\d mx_test_schema_2.mx_index_3
\d mx_test_schema_2.mx_table_2_col1_key

-- Check that DROP INDEX statement is propagated
\c - - - :master_port
SET citus.multi_shard_commit_protocol TO '2pc';
DROP INDEX mx_test_schema_2.mx_index_3;
\c - - - :worker_1_port
\d mx_test_schema_2.mx_index_3

-- Check that ALTER TABLE statements are propagated
\c - - - :master_port
SET citus.multi_shard_commit_protocol TO '2pc';
ALTER TABLE mx_test_schema_1.mx_table_1 ADD COLUMN col3 NUMERIC;
ALTER TABLE mx_test_schema_1.mx_table_1 ALTER COLUMN col3 SET DATA TYPE INT;
ALTER TABLE 
	mx_test_schema_1.mx_table_1 
ADD CONSTRAINT 
	mx_fk_constraint 
FOREIGN KEY 
	(col1)
REFERENCES
	mx_test_schema_2.mx_table_2(col1);
\c - - - :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_test_schema_1.mx_table_1'::regclass;
SELECT "Constraint", "Definition" FROM table_fkeys WHERE relid='mx_test_schema_1.mx_table_1'::regclass;

-- Check that foreign key constraint with NOT VALID works as well
\c - - - :master_port
SET citus.multi_shard_commit_protocol TO '2pc';
ALTER TABLE mx_test_schema_1.mx_table_1 DROP CONSTRAINT mx_fk_constraint; 
ALTER TABLE 
	mx_test_schema_1.mx_table_1 
ADD CONSTRAINT 
	mx_fk_constraint_2
FOREIGN KEY 
	(col1)
REFERENCES
	mx_test_schema_2.mx_table_2(col1)
NOT VALID;
\c - - - :worker_1_port
SELECT "Constraint", "Definition" FROM table_fkeys WHERE relid='mx_test_schema_1.mx_table_1'::regclass;

-- Check that mark_tables_colocated call propagates the changes to the workers 
\c - - - :master_port
SELECT nextval('pg_catalog.pg_dist_colocationid_seq') AS last_colocation_id \gset
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 10000;
SET citus.shard_count TO 7;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

CREATE TABLE mx_colocation_test_1 (a int);
SELECT create_distributed_table('mx_colocation_test_1', 'a');

CREATE TABLE mx_colocation_test_2 (a int);
SELECT create_distributed_table('mx_colocation_test_2', 'a');

-- Check the colocation IDs of the created tables
SELECT 
	logicalrelid, colocationid
FROM 
	pg_dist_partition 
WHERE
	logicalrelid = 'mx_colocation_test_1'::regclass
	OR logicalrelid = 'mx_colocation_test_2'::regclass
ORDER BY logicalrelid;
	
-- Reset the colocation IDs of the test tables
DELETE FROM 
	pg_dist_colocation
WHERE EXISTS (
	SELECT 1 
	FROM pg_dist_partition 
	WHERE 
		colocationid = pg_dist_partition.colocationid 
		AND pg_dist_partition.logicalrelid = 'mx_colocation_test_1'::regclass);
UPDATE 
	pg_dist_partition 
SET 
	colocationid = 0
WHERE 
	logicalrelid = 'mx_colocation_test_1'::regclass 
	OR logicalrelid = 'mx_colocation_test_2'::regclass;

-- Mark tables colocated and see the changes on the master and the worker
SELECT mark_tables_colocated('mx_colocation_test_1', ARRAY['mx_colocation_test_2']);
SELECT 
	logicalrelid, colocationid 
FROM 
	pg_dist_partition 
WHERE
	logicalrelid = 'mx_colocation_test_1'::regclass
	OR logicalrelid = 'mx_colocation_test_2'::regclass;
\c - - - :worker_1_port
SELECT 
	logicalrelid, colocationid 
FROM 
	pg_dist_partition 
WHERE
	logicalrelid = 'mx_colocation_test_1'::regclass
	OR logicalrelid = 'mx_colocation_test_2'::regclass;

\c - - - :master_port	

-- Check that DROP TABLE on MX tables works
DROP TABLE mx_colocation_test_1;
DROP TABLE mx_colocation_test_2;
\d mx_colocation_test_1
\d mx_colocation_test_2

\c - - - :worker_1_port
\d mx_colocation_test_1
\d mx_colocation_test_2
	
-- Check that dropped MX table can be recreated again
\c - - - :master_port	
SET citus.shard_count TO 7;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

CREATE TABLE mx_temp_drop_test (a int);
SELECT create_distributed_table('mx_temp_drop_test', 'a');
SELECT logicalrelid, repmodel FROM pg_dist_partition WHERE logicalrelid = 'mx_temp_drop_test'::regclass;

DROP TABLE mx_temp_drop_test;

CREATE TABLE mx_temp_drop_test (a int);
SELECT create_distributed_table('mx_temp_drop_test', 'a');
SELECT logicalrelid, repmodel FROM pg_dist_partition WHERE logicalrelid = 'mx_temp_drop_test'::regclass;

DROP TABLE mx_temp_drop_test;

-- Check that MX tables can be created with SERIAL columns, but error out on metadata sync
\c - - - :master_port	
SET citus.shard_count TO 3;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

CREATE TABLE mx_table_with_small_sequence(a int, b SERIAL);
SELECT create_distributed_table('mx_table_with_small_sequence', 'a');

SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
DROP TABLE mx_table_with_small_sequence;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- Show that create_distributed_table errors out if the table has a SERIAL column and 
-- there are metadata workers
CREATE TABLE mx_table_with_small_sequence(a int, b SERIAL);
SELECT create_distributed_table('mx_table_with_small_sequence', 'a');
DROP TABLE mx_table_with_small_sequence;

-- Create an MX table with (BIGSERIAL) sequences
CREATE TABLE mx_table_with_sequence(a int, b BIGSERIAL, c BIGSERIAL);
SELECT create_distributed_table('mx_table_with_sequence', 'a');
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_table_with_sequence'::regclass;
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq

-- Check that the sequences created on the metadata worker as well
\c - - - :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_table_with_sequence'::regclass;
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq

-- Check that the sequences on the worker have their own space
SELECT nextval('mx_table_with_sequence_b_seq');
SELECT nextval('mx_table_with_sequence_c_seq');

-- Check that adding a new metadata node sets the sequence space correctly
\c - - - :master_port
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);

\c - - - :worker_2_port
SELECT groupid FROM pg_dist_local_group;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_table_with_sequence'::regclass;
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq
SELECT nextval('mx_table_with_sequence_b_seq');
SELECT nextval('mx_table_with_sequence_c_seq');

-- Check that dropping the mx table with sequences works as expected, even the metadata
-- syncing is stopped to one of the workers
\c - - - :master_port
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);
DROP TABLE mx_table_with_sequence;
\d mx_table_with_sequence
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq

-- Check that the sequences are dropped from the workers
\c - - - :worker_1_port
\d mx_table_with_sequence
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq

-- Check that the sequences are dropped from the workers
\c - - - :worker_2_port
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq

-- Check that MX sequences play well with non-super users
\c - - - :master_port

-- Remove a node so that shards and sequences won't be created on table creation. Therefore,
-- we can test that start_metadata_sync_to_node can actually create the sequence with proper
-- owner
CREATE TABLE pg_dist_shard_placement_temp AS SELECT * FROM pg_dist_shard_placement;
CREATE TABLE pg_dist_partition_temp AS SELECT * FROM pg_dist_partition;
DELETE FROM pg_dist_shard_placement;
DELETE FROM pg_dist_partition;
SELECT master_remove_node('localhost', :worker_2_port);

 -- the master user needs superuser permissions to change the replication model
CREATE USER mx_user WITH SUPERUSER;
\c - - - :worker_1_port
CREATE USER mx_user;
\c - - - :worker_2_port
CREATE USER mx_user;

\c - mx_user - :master_port
-- Create an mx table as a different user
CREATE TABLE mx_table (a int, b BIGSERIAL);
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
SELECT create_distributed_table('mx_table', 'a');

\c - postgres - :master_port
SELECT master_add_node('localhost', :worker_2_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);

\c - mx_user - :worker_1_port
SELECT nextval('mx_table_b_seq');
INSERT INTO mx_table (a) VALUES (37);
INSERT INTO mx_table (a) VALUES (38);
SELECT * FROM mx_table ORDER BY a;

\c - mx_user - :worker_2_port
SELECT nextval('mx_table_b_seq');
INSERT INTO mx_table (a) VALUES (39);
INSERT INTO mx_table (a) VALUES (40);
SELECT * FROM mx_table ORDER BY a;

\c - mx_user - :master_port
DROP TABLE mx_table;

\c - postgres - :master_port
INSERT INTO pg_dist_shard_placement SELECT * FROM pg_dist_shard_placement_temp;
INSERT INTO pg_dist_partition SELECT * FROM pg_dist_partition_temp;
DROP TABLE pg_dist_shard_placement_temp;
DROP TABLE pg_dist_partition_temp;
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

DROP USER mx_user;
\c - - - :worker_1_port
DROP USER mx_user;
\c - - - :worker_2_port
DROP USER mx_user;

-- Check that create_reference_table creates the metadata on workers
\c - - - :master_port
CREATE TABLE mx_ref (col_1 int, col_2 text);
SELECT create_reference_table('mx_ref');
\dt mx_ref

\c - - - :worker_1_port
\dt mx_ref
SELECT
	logicalrelid, partmethod, repmodel, shardid, placementid, nodename, nodeport
FROM
	pg_dist_partition 
	NATURAL JOIN pg_dist_shard
	NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_ref'::regclass
ORDER BY
 	nodeport;
	
SELECT shardid AS ref_table_shardid FROM pg_dist_shard WHERE logicalrelid='mx_ref'::regclass \gset

-- Check that DDL commands are propagated to reference tables on workers
\c - - - :master_port
ALTER TABLE mx_ref ADD COLUMN col_3 NUMERIC DEFAULT 0;
CREATE INDEX mx_ref_index ON mx_ref(col_1);
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ref'::regclass;
\d mx_ref_index

\c - - - :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ref'::regclass;
\d mx_ref_index
	
-- Check that metada is cleaned successfully upon drop table
\c - - - :master_port
DROP TABLE mx_ref;
\d mx_ref

\c - - - :worker_1_port
\d mx_ref
SELECT * FROM pg_dist_shard WHERE shardid=:ref_table_shardid;
SELECT * FROM pg_dist_shard_placement WHERE shardid=:ref_table_shardid;

-- Check that master_add_node propagates the metadata about new placements of a reference table
\c - - - :master_port
CREATE TABLE tmp_shard_placement AS SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
DELETE FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT master_remove_node('localhost', :worker_2_port);
CREATE TABLE mx_ref (col_1 int, col_2 text);
SELECT create_reference_table('mx_ref');

SELECT shardid, nodename, nodeport 
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid='mx_ref'::regclass;

\c - - - :worker_1_port
SELECT shardid, nodename, nodeport 
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid='mx_ref'::regclass;

\c - - - :master_port
SELECT master_add_node('localhost', :worker_2_port);

SELECT shardid, nodename, nodeport 
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid='mx_ref'::regclass
ORDER BY shardid, nodeport;

\c - - - :worker_1_port
SELECT shardid, nodename, nodeport 
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid='mx_ref'::regclass
ORDER BY shardid, nodeport;

\c - - - :master_port
INSERT INTO pg_dist_shard_placement (SELECT * FROM tmp_shard_placement);
DROP TABLE tmp_shard_placement;

-- Cleanup
\c - - - :master_port
DROP TABLE mx_test_schema_2.mx_table_2 CASCADE;
DROP TABLE mx_test_schema_1.mx_table_1 CASCADE;
DROP TABLE mx_testing_schema.mx_test_table;
DROP TABLE mx_ref;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

RESET citus.shard_count;
RESET citus.shard_replication_factor;
RESET citus.replication_model;
RESET citus.multi_shard_commit_protocol;

ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART :last_colocation_id;
ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq RESTART :last_placement_id;
