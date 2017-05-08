--
-- MULTI_UNSUPPORTED_WORKER_OPERATIONS
--

-- Tests for ensuring unsupported functions on workers error out.

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1270000;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 1370000;

-- Set the colocation id to a safe value so that 
-- it is not affected by future changes to colocation id sequence
SELECT nextval('pg_catalog.pg_dist_colocationid_seq') AS last_colocation_id \gset
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 150000;

-- Prepare the environment
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
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

SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

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

-- master_create_distributed_table
SELECT master_create_distributed_table('mx_table_worker', 'col_1', 'hash');

-- create_distributed_table
SELECT create_distributed_table('mx_table_worker', 'col_1');

-- create_reference_table
SELECT create_reference_table('mx_table_worker');

SELECT count(*) FROM pg_dist_partition WHERE logicalrelid='mx_table_worker'::regclass;
DROP TABLE mx_table_worker;

-- master_create_worker_shards
CREATE TEMP TABLE pg_dist_shard_temp AS 
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'mx_table'::regclass;

DELETE FROM pg_dist_shard WHERE logicalrelid = 'mx_table'::regclass;

SELECT master_create_worker_shards('mx_table', 5, 1);
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='mx_table'::regclass;

INSERT INTO pg_dist_shard SELECT * FROM pg_dist_shard_temp;
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='mx_table'::regclass;


-- INSERT/UPDATE/DELETE/COPY on reference tables
SELECT * FROM mx_ref_table ORDER BY col_1;
INSERT INTO mx_ref_table (col_1, col_2) VALUES (-6, 'vestibulum');
UPDATE mx_ref_table SET col_2 = 'habitant' WHERE col_1 = -37;
DELETE FROM mx_ref_table WHERE col_1 = -78;
COPY mx_ref_table (col_1, col_2) FROM STDIN WITH (FORMAT 'csv');
SELECT * FROM mx_ref_table ORDER BY col_1;

\c - - - :master_port
DROP TABLE mx_ref_table;
CREATE UNIQUE INDEX mx_test_uniq_index ON mx_table(col_1);
\c - - - :worker_1_port

-- DDL commands
\d mx_table
CREATE INDEX mx_test_index ON mx_table(col_2);
ALTER TABLE mx_table ADD COLUMN col_4 int;
ALTER TABLE mx_table_2 ADD CONSTRAINT mx_fk_constraint FOREIGN KEY(col_1) REFERENCES mx_table(col_1);
\d mx_table

-- master_modify_multiple_shards
SELECT master_modify_multiple_shards('UPDATE mx_table SET col_2=''none''');
SELECT count(*) FROM mx_table WHERE col_2='none';
SELECT count(*) FROM mx_table WHERE col_2!='none';
SELECT master_modify_multiple_shards('DELETE FROM mx_table');
SELECT count(*) FROM mx_table;

-- master_drop_all_shards
SELECT master_drop_all_shards('mx_table'::regclass, 'public', 'mx_table');
SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid='mx_table'::regclass;

-- master_apply_delete_command
SELECT master_apply_delete_command('DELETE FROM mx_table');
SELECT count(*) FROM mx_table;

-- master_add_node

SELECT master_add_node('localhost', 5432);
SELECT * FROM pg_dist_node WHERE nodename='localhost' AND nodeport=5432;

-- master_remove_node
\c - - - :master_port
DROP INDEX mx_test_uniq_index;
SELECT master_add_node('localhost', 5432);

\c - - - :worker_1_port
SELECT master_remove_node('localhost', 5432);
SELECT * FROM pg_dist_node WHERE nodename='localhost' AND nodeport=5432;

\c - - - :master_port
SELECT master_remove_node('localhost', 5432);

-- TRUNCATE
\c - - - :worker_1_port
TRUNCATE mx_table;
SELECT count(*) FROM mx_table;

-- INSERT / SELECT pulls results to worker
BEGIN;
SET LOCAL client_min_messages TO DEBUG;
INSERT INTO mx_table_2 SELECT * FROM mx_table;
END;

SELECT count(*) FROM mx_table_2;

-- mark_tables_colocated
UPDATE pg_dist_partition SET colocationid = 0 WHERE logicalrelid='mx_table_2'::regclass;

SELECT mark_tables_colocated('mx_table', ARRAY['mx_table_2']);
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
DELETE FROM pg_dist_node;
SELECT worker_drop_distributed_table(logicalrelid) FROM pg_dist_partition;
\c - - - :worker_1_port

-- DROP TABLE
DROP TABLE mx_table;
SELECT count(*) FROM mx_table;

-- master_drop_distributed_table_metadata
SELECT master_drop_distributed_table_metadata('mx_table'::regclass, 'public', 'mx_table');
SELECT count(*) FROM mx_table;

-- master_copy_shard_placement
SELECT logicalrelid, shardid AS testshardid, nodename, nodeport 
FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE logicalrelid = 'mx_table'::regclass AND nodeport=:worker_1_port
ORDER BY shardid
LIMIT 1 \gset

INSERT INTO pg_dist_shard_placement (nodename, nodeport, shardid, shardstate, shardlength)
VALUES ('localhost', :worker_2_port, :testshardid, 3, 0);

SELECT master_copy_shard_placement(:testshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

SELECT shardid, nodename, nodeport, shardstate 
FROM pg_dist_shard_placement
WHERE shardid = :testshardid
ORDER BY nodeport;

DELETE FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port AND shardid = :testshardid;

-- master_get_new_placementid
SELECT master_get_new_placementid();

-- Show that sequences can be created and dropped on worker nodes
CREATE TABLE some_table_with_sequence(a int, b BIGSERIAL, c BIGSERIAL);
DROP TABLE some_table_with_sequence;
CREATE SEQUENCE some_sequence;
DROP SEQUENCE some_sequence;

-- Show that dropping the sequence of an MX table with cascade harms the table and shards
BEGIN;
\d mx_table
DROP SEQUENCE mx_table_col_3_seq CASCADE;
\d mx_table
ROLLBACK;

-- Cleanup
\c - - - :master_port
DROP TABLE mx_table;
DROP TABLE mx_table_2;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
\c - - - :worker_1_port
DELETE FROM pg_dist_node;
SELECT worker_drop_distributed_table(logicalrelid) FROM pg_dist_partition;
\c - - - :master_port
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART :last_colocation_id;

RESET citus.shard_replication_factor;
RESET citus.replication_model;
