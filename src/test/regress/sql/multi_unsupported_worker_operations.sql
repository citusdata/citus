--
-- MULTI_UNSUPPORTED_WORKER_OPERATIONS
--

-- Tests for ensuring unsupported functions on workers error out.

SET citus.next_shard_id TO 1270000;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 1370000;

-- Set the colocation id to a safe value so that
-- it is not affected by future changes to colocation id sequence
SELECT nextval('pg_catalog.pg_dist_colocationid_seq') AS last_colocation_id \gset
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 150000;

-- Prepare the environment
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 5;

-- Create test tables
CREATE TABLE mx_table (col_1 int, col_2 text, col_3 BIGSERIAL);
SELECT create_distributed_table('mx_table', 'col_1');

CREATE TABLE mx_table_2 (col_1 int, col_2 text, col_3 BIGSERIAL);
SELECT create_distributed_table('mx_table_2', 'col_1');

CREATE TABLE mx_ref_table (col_1 int, col_2 text);
SELECT create_reference_table('mx_ref_table');

-- Check that the created tables are colocated MX tables
SELECT logicalrelid, repmodel, colocationid
FROM pg_dist_partition
WHERE logicalrelid IN ('mx_table'::regclass, 'mx_table_2'::regclass)
ORDER BY logicalrelid;

COPY mx_table (col_1, col_2) FROM STDIN WITH (FORMAT 'csv');
-37, 'lorem'
65536, 'ipsum'
80, 'dolor'
7344, 'sit'
65832, 'amet'
\.

INSERT INTO mx_ref_table VALUES (-37, 'morbi');
INSERT INTO mx_ref_table VALUES (-78, 'sapien');
INSERT INTO mx_ref_table VALUES (-34, 'augue');

SELECT * FROM mx_table ORDER BY col_1;

-- Try commands from metadata worker
\c - - - :worker_1_port

CREATE TABLE mx_table_worker(col_1 text);

-- create_distributed_table
SELECT create_distributed_table('mx_table_worker', 'col_1');

-- create_reference_table
SELECT create_reference_table('mx_table_worker');

SELECT count(*) FROM pg_dist_partition WHERE logicalrelid='mx_table_worker'::regclass;
DROP TABLE mx_table_worker;

\c - - - :master_port
DROP TABLE mx_ref_table;
CREATE UNIQUE INDEX mx_test_uniq_index ON mx_table(col_1);
\c - - - :worker_1_port

-- changing isdatanode
SELECT * from master_set_node_property('localhost', 8888, 'shouldhaveshards', false);
SELECT * from master_set_node_property('localhost', 8888, 'shouldhaveshards', true);

-- DDL commands
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.mx_table'::regclass;
CREATE INDEX mx_test_index ON mx_table(col_2);
ALTER TABLE mx_table ADD COLUMN col_4 int;
ALTER TABLE mx_table_2 ADD CONSTRAINT mx_fk_constraint FOREIGN KEY(col_1) REFERENCES mx_table(col_1);
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.mx_table'::regclass;
\d mx_test_index

-- citus_drop_all_shards
SELECT citus_drop_all_shards('mx_table'::regclass, 'public', 'mx_table');
SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid='mx_table'::regclass;

-- master_add_inactive_node

SELECT 1 FROM master_add_inactive_node('localhost', 5432);
SELECT count(1) FROM pg_dist_node WHERE nodename='localhost' AND nodeport=5432;

-- master_remove_node
\c - - - :master_port
DROP INDEX mx_test_uniq_index;
SELECT 1 FROM master_add_inactive_node('localhost', 5432);

\c - - - :worker_1_port
SELECT master_remove_node('localhost', 5432);
SELECT count(1) FROM pg_dist_node WHERE nodename='localhost' AND nodeport=5432;

\c - - - :master_port
SELECT master_remove_node('localhost', 5432);

\c - - - :worker_1_port

UPDATE pg_dist_partition SET colocationid = 0 WHERE logicalrelid='mx_table_2'::regclass;

SELECT update_distributed_table_colocation('mx_table', colocate_with => 'mx_table_2');
SELECT colocationid FROM pg_dist_partition WHERE logicalrelid='mx_table_2'::regclass;

SELECT colocationid AS old_colocation_id
FROM pg_dist_partition
WHERE logicalrelid='mx_table'::regclass \gset

UPDATE pg_dist_partition
SET colocationid = :old_colocation_id
WHERE logicalrelid='mx_table_2'::regclass;

-- start_metadata_sync_to_node
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_port;

-- stop_metadata_sync_to_node
\c - - - :master_port
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);
\c - - - :worker_1_port

SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

\c - - - :master_port
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_port;
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_port;
\c - - - :worker_2_port
SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'mx\_%table%';
SELECT count(*) FROM pg_dist_node;
\c - - - :worker_1_port

-- DROP TABLE
-- terse verbosity because pg10 has slightly different output
\set VERBOSITY terse
DROP TABLE mx_table;
\set VERBOSITY default
SELECT count(*) FROM mx_table;

-- master_drop_distributed_table_metadata
SELECT master_remove_distributed_table_metadata_from_workers('mx_table'::regclass, 'public', 'mx_table');
SELECT master_remove_partition_metadata('mx_table'::regclass, 'public', 'mx_table');
SELECT count(*) FROM mx_table;

-- citus_copy_shard_placement
SELECT logicalrelid, shardid AS testshardid, nodename, nodeport
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid = 'mx_table'::regclass AND nodeport=:worker_1_port
ORDER BY shardid
LIMIT 1 \gset

SELECT groupid AS worker_2_group FROM pg_dist_node WHERE nodeport = :worker_2_port \gset
INSERT INTO pg_dist_placement (groupid, shardid, shardstate, shardlength)
VALUES (:worker_2_group, :testshardid, 3, 0);

SELECT citus_copy_shard_placement(:testshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

SELECT shardid, nodename, nodeport, shardstate
FROM pg_dist_shard_placement
WHERE shardid = :testshardid
ORDER BY nodeport;

DELETE FROM pg_dist_placement WHERE groupid = :worker_2_group AND shardid = :testshardid;

-- master_get_new_placementid
SELECT master_get_new_placementid();

-- Show that sequences can be created and dropped on worker nodes
CREATE TABLE some_table_with_sequence(a int, b BIGSERIAL, c BIGSERIAL);
DROP TABLE some_table_with_sequence;
CREATE SEQUENCE some_sequence;
DROP SEQUENCE some_sequence;

-- Show that dropping the sequence of an MX table is not supported on worker nodes
DROP SEQUENCE mx_table_col_3_seq CASCADE;

-- Cleanup
\c - - - :master_port
DROP TABLE mx_table;
DROP TABLE mx_table_2;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART :last_colocation_id;

RESET citus.shard_replication_factor;
