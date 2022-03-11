--
-- MULTI_METADATA_SYNC
--

-- Tests for metadata snapshot functions, metadata syncing functions and propagation of
-- metadata changes to MX tables.

-- Turn metadata sync off at first
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1310000;
SET citus.replicate_reference_tables_on_activate TO off;

SELECT nextval('pg_catalog.pg_dist_placement_placementid_seq') AS last_placement_id
\gset
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 100000;

SELECT nextval('pg_catalog.pg_dist_groupid_seq') AS last_group_id \gset
SELECT nextval('pg_catalog.pg_dist_node_nodeid_seq') AS last_node_id \gset

-- Create the necessary test utility function
SET citus.enable_metadata_sync TO OFF;
CREATE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';
RESET citus.enable_metadata_sync;

COMMENT ON FUNCTION activate_node_snapshot()
    IS 'commands to activate node snapshot';

-- Show that none of the existing tables are qualified to be MX tables
SELECT * FROM pg_dist_partition WHERE partmethod='h' AND repmodel='s';

-- Since password_encryption default has been changed to sha from md5 with PG14
-- we are updating it manually just for consistent test results between PG versions.
ALTER SYSTEM SET password_encryption TO md5;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
ALTER ROLE CURRENT_USER WITH PASSWORD 'dummypassword';

-- Show that, with no MX tables, activate node snapshot contains only the delete commands,
-- pg_dist_node entries, pg_dist_object entries and roles.
SELECT unnest(activate_node_snapshot()) order by 1;

-- this function is dropped in Citus10, added here for tests
SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION pg_catalog.master_create_distributed_table(table_name regclass,
                                                                      distribution_column text,
                                                                      distribution_method citus.distribution_type)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus', $$master_create_distributed_table$$;
RESET citus.enable_metadata_sync;
COMMENT ON FUNCTION pg_catalog.master_create_distributed_table(table_name regclass,
                                                               distribution_column text,
                                                               distribution_method citus.distribution_type)
    IS 'define the table distribution functions';

-- Create a test table with constraints and SERIAL and default from user defined sequence
CREATE SEQUENCE user_defined_seq;
CREATE TABLE mx_test_table (col_1 int UNIQUE, col_2 text NOT NULL, col_3 BIGSERIAL, col_4 BIGINT DEFAULT nextval('user_defined_seq'));
set citus.shard_count to 8;
set citus.shard_replication_factor to 1;
SELECT create_distributed_table('mx_test_table', 'col_1');
reset citus.shard_count;
reset citus.shard_replication_factor;

-- Set the replication model of the test table to streaming replication so that it is
-- considered as an MX table
UPDATE pg_dist_partition SET repmodel='s' WHERE logicalrelid='mx_test_table'::regclass;

-- Show that the created MX table is and its sequences are included in the activate node snapshot
SELECT unnest(activate_node_snapshot()) order by 1;

-- Show that CREATE INDEX commands are included in the activate node snapshot
CREATE INDEX mx_index ON mx_test_table(col_2);
SELECT unnest(activate_node_snapshot()) order by 1;

-- Show that schema changes are included in the activate node snapshot
CREATE SCHEMA mx_testing_schema;
ALTER TABLE mx_test_table SET SCHEMA mx_testing_schema;
SELECT unnest(activate_node_snapshot()) order by 1;

-- Show that append distributed tables are not included in the activate node snapshot
CREATE TABLE non_mx_test_table (col_1 int, col_2 text);
SELECT master_create_distributed_table('non_mx_test_table', 'col_1', 'append');
UPDATE pg_dist_partition SET repmodel='s' WHERE logicalrelid='non_mx_test_table'::regclass;
SELECT unnest(activate_node_snapshot()) order by 1;

-- Show that range distributed tables are not included in the activate node snapshot
UPDATE pg_dist_partition SET partmethod='r' WHERE logicalrelid='non_mx_test_table'::regclass;
SELECT unnest(activate_node_snapshot()) order by 1;


-- Test start_metadata_sync_to_node and citus_activate_node UDFs

-- Ensure that hasmetadata=false for all nodes
SELECT count(*) FROM pg_dist_node WHERE hasmetadata=true;

-- Show that metadata can not be synced on secondary node
SELECT groupid AS worker_1_group FROM pg_dist_node WHERE nodeport = :worker_1_port \gset
SELECT master_add_node('localhost', 8888, groupid => :worker_1_group, noderole => 'secondary');
SELECT start_metadata_sync_to_node('localhost', 8888);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport = 8888;
SELECT stop_metadata_sync_to_node('localhost', 8888);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport = 8888;

-- Add a node to another cluster to make sure it's also synced
SELECT master_add_secondary_node('localhost', 8889, 'localhost', :worker_1_port, nodecluster => 'second-cluster');

\c - - - :master_port
-- Run start_metadata_sync_to_node and citus_activate_node and check that it marked hasmetadata for that worker
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);

SELECT nodeid, hasmetadata FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_1_port;

-- Check that the metadata has been copied to the worker
\c - - - :worker_1_port
SELECT groupid FROM pg_dist_local_group;
SELECT * FROM pg_dist_node ORDER BY nodeid;
SELECT * FROM pg_dist_partition WHERE logicalrelid::text LIKE 'mx_testing_schema%' ORDER BY logicalrelid;
SELECT * FROM pg_dist_shard  WHERE logicalrelid::text LIKE 'mx_testing_schema%' ORDER BY shardid;
SELECT * FROM pg_dist_shard_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid::text LIKE 'mx_testing_schema%') ORDER BY shardid, nodename, nodeport;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_testing_schema.mx_test_table'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_testing_schema.mx_test_table_col_1_key'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_testing_schema.mx_index'::regclass;

-- Check that pg_dist_colocation is synced
SELECT * FROM pg_dist_colocation ORDER BY colocationid;

-- Make sure that truncate trigger has been set for the MX table on worker
SELECT count(*) FROM pg_trigger WHERE tgrelid='mx_testing_schema.mx_test_table'::regclass;

-- Make sure that citus_activate_node considers foreign key constraints
\c - - - :master_port

-- Since we're superuser, we can set the replication model to 'streaming' to
-- create some MX tables
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA mx_testing_schema_2;

CREATE TABLE mx_testing_schema.fk_test_1 (col1 int, col2 text, col3 int, UNIQUE(col1, col3));
CREATE TABLE mx_testing_schema_2.fk_test_2 (col1 int, col2 int, col3 text,
	FOREIGN KEY (col1, col2) REFERENCES mx_testing_schema.fk_test_1 (col1, col3));

SELECT create_distributed_table('mx_testing_schema.fk_test_1', 'col1');
SELECT create_distributed_table('mx_testing_schema_2.fk_test_2', 'col1');

SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);

-- Check that foreign key metadata exists on the worker
\c - - - :worker_1_port
SELECT "Constraint", "Definition" FROM table_fkeys WHERE relid='mx_testing_schema_2.fk_test_2'::regclass;

\c - - - :master_port
DROP TABLE mx_testing_schema_2.fk_test_2;
DROP TABLE mx_testing_schema.fk_test_1;

RESET citus.shard_replication_factor;

-- Check that repeated calls to citus_activate_node has no side effects
\c - - - :master_port
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
\c - - - :worker_1_port
SELECT groupid FROM pg_dist_local_group;
SELECT * FROM pg_dist_node ORDER BY nodeid;
SELECT * FROM pg_dist_partition WHERE logicalrelid::text LIKE 'mx_testing_schema%' ORDER BY logicalrelid;
SELECT * FROM pg_dist_shard WHERE logicalrelid::text LIKE 'mx_testing_schema%' ORDER BY shardid;
SELECT * FROM pg_dist_shard_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid::text LIKE 'mx_testing_schema%') ORDER BY shardid, nodename, nodeport;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_testing_schema.mx_test_table'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_testing_schema.mx_test_table_col_1_key'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_testing_schema.mx_index'::regclass;
SELECT count(*) FROM pg_trigger WHERE tgrelid='mx_testing_schema.mx_test_table'::regclass;

-- Make sure that citus_activate_node can be called inside a transaction and rollbacked
\c - - - :master_port
BEGIN;
SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);
ROLLBACK;

SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_port;

-- Check that the distributed table can be queried from the worker
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);

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
CREATE SCHEMA mx_test_schema_1;
CREATE SCHEMA mx_test_schema_2;

-- Create MX tables
SET citus.shard_replication_factor TO 1;
CREATE TABLE mx_test_schema_1.mx_table_1 (col1 int UNIQUE, col2 text);
CREATE INDEX mx_index_1 ON mx_test_schema_1.mx_table_1 (col1);

CREATE TABLE mx_test_schema_2.mx_table_2 (col1 int, col2 text);
CREATE INDEX mx_index_2 ON mx_test_schema_2.mx_table_2 (col2);
ALTER TABLE mx_test_schema_2.mx_table_2 ADD CONSTRAINT mx_fk_constraint FOREIGN KEY(col1) REFERENCES mx_test_schema_1.mx_table_1(col1);

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_test_schema_1.mx_table_1'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_test_schema_1.mx_table_1_col1_key'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_test_schema_1.mx_index_1'::regclass;

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_test_schema_2.mx_table_2'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_test_schema_2.mx_index_2'::regclass;
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

SELECT * FROM pg_dist_partition WHERE logicalrelid::text LIKE 'mx_test_schema%';
SELECT * FROM pg_dist_shard WHERE logicalrelid::text LIKE 'mx_test_schema%';
SELECT * FROM pg_dist_shard_placement ORDER BY shardid, nodename, nodeport;

-- Check that CREATE INDEX statement is propagated
\c - - - :master_port
SET client_min_messages TO 'ERROR';
CREATE INDEX mx_index_3 ON mx_test_schema_2.mx_table_2 USING hash (col1);
ALTER TABLE mx_test_schema_2.mx_table_2 ADD CONSTRAINT mx_table_2_col1_key UNIQUE (col1);
\c - - - :worker_1_port
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_test_schema_2.mx_index_3'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_test_schema_2.mx_table_2_col1_key'::regclass;

-- Check that DROP INDEX statement is propagated
\c - - - :master_port
DROP INDEX mx_test_schema_2.mx_index_3;
\c - - - :worker_1_port
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_test_schema_2.mx_index_3'::regclass;

-- Check that ALTER TABLE statements are propagated
\c - - - :master_port
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

-- Check that update_distributed_table_colocation call propagates the changes to the workers
\c - - - :master_port
SELECT nextval('pg_catalog.pg_dist_colocationid_seq') AS last_colocation_id \gset
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 10000;
SET citus.shard_count TO 7;
SET citus.shard_replication_factor TO 1;

CREATE TABLE mx_colocation_test_1 (a int);
SELECT create_distributed_table('mx_colocation_test_1', 'a');

CREATE TABLE mx_colocation_test_2 (a int);
SELECT create_distributed_table('mx_colocation_test_2', 'a', colocate_with:='none');

-- Reset the colocation IDs of the test tables
DELETE FROM
	pg_dist_colocation
WHERE EXISTS (
	SELECT 1
	FROM pg_dist_partition
	WHERE
		colocationid = pg_dist_partition.colocationid
		AND pg_dist_partition.logicalrelid = 'mx_colocation_test_1'::regclass);

-- Check the colocation IDs of the created tables
SELECT
	logicalrelid, colocationid
FROM
	pg_dist_partition
WHERE
	logicalrelid = 'mx_colocation_test_1'::regclass
	OR logicalrelid = 'mx_colocation_test_2'::regclass
ORDER BY logicalrelid;

-- Update colocation and see the changes on the master and the worker
SELECT update_distributed_table_colocation('mx_colocation_test_1', colocate_with => 'mx_colocation_test_2');
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

CREATE TABLE mx_temp_drop_test (a int);
SELECT create_distributed_table('mx_temp_drop_test', 'a');
SELECT logicalrelid, repmodel FROM pg_dist_partition WHERE logicalrelid = 'mx_temp_drop_test'::regclass;

DROP TABLE mx_temp_drop_test;

CREATE TABLE mx_temp_drop_test (a int);
SELECT create_distributed_table('mx_temp_drop_test', 'a');
SELECT logicalrelid, repmodel FROM pg_dist_partition WHERE logicalrelid = 'mx_temp_drop_test'::regclass;

DROP TABLE mx_temp_drop_test;

-- Check that MX tables can be created with SERIAL columns
\c - - - :master_port
SET citus.shard_count TO 3;
SET citus.shard_replication_factor TO 1;

SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

-- sync table with serial column after create_distributed_table
CREATE TABLE mx_table_with_small_sequence(a int, b SERIAL, c SMALLSERIAL);
SELECT create_distributed_table('mx_table_with_small_sequence', 'a');
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
DROP TABLE mx_table_with_small_sequence;

-- Show that create_distributed_table works with a serial column
CREATE TABLE mx_table_with_small_sequence(a int, b SERIAL, c SMALLSERIAL);
SELECT create_distributed_table('mx_table_with_small_sequence', 'a');
INSERT INTO mx_table_with_small_sequence VALUES (0);

\c - - - :worker_1_port
-- Insert doesn't work because the defaults are of type int and smallint
INSERT INTO mx_table_with_small_sequence VALUES (1), (3);

\c - - - :master_port
SET citus.shard_replication_factor TO 1;

-- Create an MX table with (BIGSERIAL) sequences
CREATE TABLE mx_table_with_sequence(a int, b BIGSERIAL, c BIGSERIAL);
SELECT create_distributed_table('mx_table_with_sequence', 'a');
INSERT INTO mx_table_with_sequence VALUES (0);
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_table_with_sequence'::regclass;
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq

-- Check that the sequences created on the metadata worker as well
\c - - - :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_table_with_sequence'::regclass;
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq

-- Insert works because the defaults are of type bigint
INSERT INTO mx_table_with_sequence VALUES (1), (3);

-- check that pg_depend records exist on the worker
SELECT refobjsubid FROM pg_depend
WHERE objid = 'mx_table_with_sequence_b_seq'::regclass AND refobjid = 'mx_table_with_sequence'::regclass;

SELECT refobjsubid FROM pg_depend
WHERE objid = 'mx_table_with_sequence_c_seq'::regclass AND refobjid = 'mx_table_with_sequence'::regclass;

-- Check that the sequences on the worker have their own space
SELECT nextval('mx_table_with_sequence_b_seq');
SELECT nextval('mx_table_with_sequence_c_seq');

-- Check that adding a new metadata node sets the sequence space correctly
\c - - - :master_port
SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);

\c - - - :worker_2_port
SELECT groupid FROM pg_dist_local_group;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_table_with_sequence'::regclass;
\ds mx_table_with_sequence_b_seq
\ds mx_table_with_sequence_c_seq
SELECT nextval('mx_table_with_sequence_b_seq');
SELECT nextval('mx_table_with_sequence_c_seq');

-- Insert doesn't work because the defaults are of type int and smallint
INSERT INTO mx_table_with_small_sequence VALUES (2), (4);
-- Insert works because the defaults are of type bigint
INSERT INTO mx_table_with_sequence VALUES (2), (4);

-- Check that dropping the mx table with sequences works as expected
\c - - - :master_port

-- check our small sequence values
SELECT a, b, c FROM mx_table_with_small_sequence ORDER BY a,b,c;

--check our bigint sequence values
SELECT a, b, c FROM mx_table_with_sequence ORDER BY a,b,c;

-- Check that dropping the mx table with sequences works as expected
DROP TABLE mx_table_with_small_sequence, mx_table_with_sequence;
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
-- we can test that citus_activate_node can actually create the sequence with proper
-- owner
CREATE TABLE pg_dist_placement_temp AS SELECT * FROM pg_dist_placement;
CREATE TABLE pg_dist_partition_temp AS SELECT * FROM pg_dist_partition;
CREATE TABLE pg_dist_object_temp AS SELECT * FROM pg_catalog.pg_dist_object;
DELETE FROM pg_dist_placement;
DELETE FROM pg_dist_partition;
DELETE FROM pg_catalog.pg_dist_object;
SELECT groupid AS old_worker_2_group FROM pg_dist_node WHERE nodeport = :worker_2_port \gset
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
SELECT create_distributed_table('mx_table', 'a');

\c - postgres - :master_port
SELECT master_add_node('localhost', :worker_2_port);

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

-- put the metadata back into a consistent state
\c - postgres - :master_port
INSERT INTO pg_dist_placement SELECT * FROM pg_dist_placement_temp;
INSERT INTO pg_dist_partition SELECT * FROM pg_dist_partition_temp;
INSERT INTO pg_catalog.pg_dist_object SELECT * FROM pg_dist_object_temp ON CONFLICT ON CONSTRAINT pg_dist_object_pkey DO NOTHING;
DROP TABLE pg_dist_placement_temp;
DROP TABLE pg_dist_partition_temp;
DROP TABLE pg_dist_object_temp;
UPDATE pg_dist_placement
  SET groupid = (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port)
  WHERE groupid = :old_worker_2_group;
\c - - - :worker_1_port
UPDATE pg_dist_placement
  SET groupid = (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port)
  WHERE groupid = :old_worker_2_group;
\c - - - :worker_2_port
UPDATE pg_dist_placement
  SET groupid = (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port)
  WHERE groupid = :old_worker_2_group;

\c - - - :master_port
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

-- make sure that adding/removing nodes doesn't cause
-- multiple colocation entries for reference tables
SELECT count(*) FROM pg_dist_colocation WHERE distributioncolumntype = 0;

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

-- make sure we have the pg_dist_colocation record on the worker
SELECT count(*) FROM pg_dist_colocation WHERE distributioncolumntype = 0;

-- Check that DDL commands are propagated to reference tables on workers
\c - - - :master_port
ALTER TABLE mx_ref ADD COLUMN col_3 NUMERIC DEFAULT 0;
CREATE INDEX mx_ref_index ON mx_ref(col_1);
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ref'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_ref_index'::regclass;


\c - - - :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ref'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_ref_index'::regclass;

-- Check that metada is cleaned successfully upon drop table
\c - - - :master_port
DROP TABLE mx_ref;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_ref_index'::regclass;

\c - - - :worker_1_port
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'mx_ref_index'::regclass;
SELECT * FROM pg_dist_shard WHERE shardid=:ref_table_shardid;
SELECT * FROM pg_dist_shard_placement WHERE shardid=:ref_table_shardid;

-- Check that master_add_node propagates the metadata about new placements of a reference table
\c - - - :master_port
SELECT groupid AS old_worker_2_group
  FROM pg_dist_node WHERE nodeport = :worker_2_port \gset
CREATE TABLE tmp_placement AS
  SELECT * FROM pg_dist_placement WHERE groupid = :old_worker_2_group;
DELETE FROM pg_dist_placement
  WHERE groupid = :old_worker_2_group;
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
SET client_min_messages TO ERROR;
SELECT master_add_node('localhost', :worker_2_port);
RESET client_min_messages;

SELECT shardid, nodename, nodeport
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid='mx_ref'::regclass
ORDER BY shardid, nodeport;

\c - - - :worker_1_port
SELECT shardid, nodename, nodeport
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid='mx_ref'::regclass
ORDER BY shardid, nodeport;

-- Get the metadata back into a consistent state
\c - - - :master_port
INSERT INTO pg_dist_placement (SELECT * FROM tmp_placement);
DROP TABLE tmp_placement;

UPDATE pg_dist_placement
  SET groupid = (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port)
  WHERE groupid = :old_worker_2_group;

\c - - - :worker_1_port
UPDATE pg_dist_placement
  SET groupid = (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port)
  WHERE groupid = :old_worker_2_group;

-- Confirm that shouldhaveshards is 'true'
\c - - - :master_port
select shouldhaveshards from pg_dist_node where nodeport = 8888;
\c - postgres - :worker_1_port
select shouldhaveshards from pg_dist_node where nodeport = 8888;


-- Check that setting shouldhaveshards to false is correctly transferred to other mx nodes
\c - - - :master_port
SELECT * from master_set_node_property('localhost', 8888, 'shouldhaveshards', false);
select shouldhaveshards from pg_dist_node where nodeport = 8888;

\c - postgres - :worker_1_port
select shouldhaveshards from pg_dist_node where nodeport = 8888;

-- Check that setting shouldhaveshards to true is correctly transferred to other mx nodes
\c - postgres - :master_port
SELECT * from master_set_node_property('localhost', 8888, 'shouldhaveshards', true);
select shouldhaveshards from pg_dist_node where nodeport = 8888;

\c - postgres - :worker_1_port
select shouldhaveshards from pg_dist_node where nodeport = 8888;

\c - - - :master_port
--
-- Check that metadata commands error out if any nodes are out-of-sync
--

-- increase metadata_sync intervals to avoid metadata sync while we test
ALTER SYSTEM SET citus.metadata_sync_interval TO 300000;
ALTER SYSTEM SET citus.metadata_sync_retry_interval TO 300000;
SELECT pg_reload_conf();

SET citus.shard_replication_factor TO 1;

CREATE TABLE dist_table_1(a int);
SELECT create_distributed_table('dist_table_1', 'a');

UPDATE pg_dist_node SET metadatasynced=false WHERE nodeport=:worker_1_port;
SELECT hasmetadata, metadatasynced FROM pg_dist_node WHERE nodeport=:worker_1_port;

CREATE TABLE dist_table_2(a int);
SELECT create_distributed_table('dist_table_2', 'a');
SELECT create_reference_table('dist_table_2');

ALTER TABLE dist_table_1 ADD COLUMN b int;

SELECT master_add_node('localhost', :master_port, groupid => 0);
SELECT citus_disable_node_and_wait('localhost', :worker_1_port);
SELECT citus_disable_node_and_wait('localhost', :worker_2_port);
SELECT master_remove_node('localhost', :worker_1_port);
SELECT master_remove_node('localhost', :worker_2_port);

-- master_update_node should succeed
SELECT nodeid AS worker_2_nodeid FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
SELECT master_update_node(:worker_2_nodeid, 'localhost', 4444);
SELECT master_update_node(:worker_2_nodeid, 'localhost', :worker_2_port);

ALTER SYSTEM SET citus.metadata_sync_interval TO DEFAULT;
ALTER SYSTEM SET citus.metadata_sync_retry_interval TO DEFAULT;
SELECT pg_reload_conf();

-- make sure that all the nodes have valid metadata before moving forward
SELECT wait_until_metadata_sync(60000);

SELECT master_add_node('localhost', :worker_2_port);

CREATE SEQUENCE mx_test_sequence_0;
CREATE SEQUENCE mx_test_sequence_1;

-- test create_distributed_table
CREATE TABLE test_table (id int DEFAULT nextval('mx_test_sequence_0'));
SELECT create_distributed_table('test_table', 'id');

-- shouldn't work since it's partition column
ALTER TABLE test_table ALTER COLUMN id SET DEFAULT nextval('mx_test_sequence_1');

-- test different plausible commands
ALTER TABLE test_table ADD COLUMN id2 int DEFAULT nextval('mx_test_sequence_1');
ALTER TABLE test_table ALTER COLUMN id2 DROP DEFAULT;
ALTER TABLE test_table ALTER COLUMN id2 SET DEFAULT nextval('mx_test_sequence_1');

SELECT unnest(activate_node_snapshot()) order by 1;

-- shouldn't work since test_table is MX
ALTER TABLE test_table ADD COLUMN id3 bigserial;

-- shouldn't work since the above operations should be the only subcommands
ALTER TABLE test_table ADD COLUMN id4 int DEFAULT nextval('mx_test_sequence_1') CHECK (id4 > 0);
ALTER TABLE test_table ADD COLUMN id4 int, ADD COLUMN id5 int DEFAULT nextval('mx_test_sequence_1');
ALTER TABLE test_table ALTER COLUMN id1 SET DEFAULT nextval('mx_test_sequence_1'), ALTER COLUMN id2 DROP DEFAULT;
ALTER TABLE test_table ADD COLUMN id4 bigserial CHECK (id4 > 0);

\c - - - :worker_1_port
\ds

\c - - - :master_port
CREATE SEQUENCE local_sequence;

-- verify that DROP SEQUENCE will propagate the command to workers for
-- the distributed sequences mx_test_sequence_0 and mx_test_sequence_1
DROP SEQUENCE mx_test_sequence_0, mx_test_sequence_1, local_sequence CASCADE;

\c - - - :worker_1_port
\ds

\c - - - :master_port
DROP TABLE test_table CASCADE;

-- Cleanup
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);
DROP TABLE mx_test_schema_2.mx_table_2 CASCADE;
DROP TABLE mx_test_schema_1.mx_table_1 CASCADE;
DROP TABLE mx_testing_schema.mx_test_table;
DROP TABLE mx_ref;
DROP TABLE dist_table_1, dist_table_2;

SET client_min_messages TO ERROR;
SET citus.enable_ddl_propagation TO off; -- for enterprise
CREATE USER non_super_metadata_user;
SET citus.enable_ddl_propagation TO on;
RESET client_min_messages;
SELECT run_command_on_workers('CREATE USER non_super_metadata_user');
GRANT EXECUTE ON FUNCTION start_metadata_sync_to_node(text,int) TO non_super_metadata_user;
GRANT EXECUTE ON FUNCTION stop_metadata_sync_to_node(text,int,bool) TO non_super_metadata_user;
GRANT ALL ON pg_dist_node TO non_super_metadata_user;
GRANT ALL ON pg_dist_local_group TO non_super_metadata_user;
GRANT ALL ON SCHEMA citus TO non_super_metadata_user;
GRANT INSERT ON ALL TABLES IN SCHEMA citus TO non_super_metadata_user;
GRANT USAGE ON SCHEMA mx_testing_schema TO non_super_metadata_user;
GRANT USAGE ON SCHEMA mx_testing_schema_2 TO non_super_metadata_user;
GRANT USAGE ON SCHEMA mx_test_schema_1 TO non_super_metadata_user;
GRANT USAGE ON SCHEMA mx_test_schema_2 TO non_super_metadata_user;
SELECT run_command_on_workers('GRANT ALL ON pg_dist_node TO non_super_metadata_user');
SELECT run_command_on_workers('GRANT ALL ON pg_dist_local_group TO non_super_metadata_user');
SELECT run_command_on_workers('GRANT ALL ON SCHEMA citus TO non_super_metadata_user');
SELECT run_command_on_workers('ALTER SEQUENCE user_defined_seq OWNER TO non_super_metadata_user');
SELECT run_command_on_workers('GRANT ALL ON ALL TABLES IN SCHEMA citus TO non_super_metadata_user');
SELECT run_command_on_workers('GRANT USAGE ON SCHEMA mx_testing_schema TO non_super_metadata_user');
SELECT run_command_on_workers('GRANT USAGE ON SCHEMA mx_testing_schema_2 TO non_super_metadata_user');
SELECT run_command_on_workers('GRANT USAGE ON SCHEMA mx_test_schema_1 TO non_super_metadata_user');
SELECT run_command_on_workers('GRANT USAGE ON SCHEMA mx_test_schema_2 TO non_super_metadata_user');

SET ROLE non_super_metadata_user;

-- user must be super user stop/start metadata
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

RESET ROLE;

SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

RESET citus.shard_count;
RESET citus.shard_replication_factor;

ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART :last_colocation_id;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART :last_placement_id;

-- Activate them at the end
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);
