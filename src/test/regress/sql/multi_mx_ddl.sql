-- Tests related to distributed DDL commands on mx cluster
SELECT * FROM mx_ddl_table ORDER BY key;

-- CREATE INDEX
CREATE INDEX ddl_test_index ON mx_ddl_table(value);

CREATE INDEX CONCURRENTLY ddl_test_concurrent_index ON mx_ddl_table(value);

-- ADD COLUMN
ALTER TABLE mx_ddl_table ADD COLUMN version INTEGER;

-- SET DEFAULT
ALTER TABLE mx_ddl_table ALTER COLUMN version SET DEFAULT 1;

UPDATE mx_ddl_table SET version=0.1 WHERE version IS NULL;

-- SET NOT NULL
ALTER TABLE mx_ddl_table ALTER COLUMN version SET NOT NULL;

-- See that the changes are applied on coordinator, worker tables and shards
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table'::regclass;
SELECT "relname", "Column", "Type", "Definition" FROM index_attrs WHERE
    relname LIKE 'ddl_test%_index';

\c - - - :worker_1_port

-- make sure we don't break the following tests by hiding the shard names
SET citus.override_table_visibility TO FALSE;

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table'::regclass;
SELECT "relname", "Column", "Type", "Definition" FROM index_attrs WHERE
    relname LIKE 'ddl_test%_index';
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table_1220088'::regclass;
SELECT "relname", "Column", "Type", "Definition" FROM index_attrs WHERE
    relname LIKE 'ddl_test%_index_1220088';

\c - - - :worker_2_port

-- make sure we don't break the following tests by hiding the shard names
SET citus.override_table_visibility TO FALSE;

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table'::regclass;
SELECT "relname", "Column", "Type", "Definition" FROM index_attrs WHERE
    relname LIKE 'ddl_test%_index';
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table_1220089'::regclass;
SELECT "relname", "Column", "Type", "Definition" FROM index_attrs WHERE
    relname LIKE 'ddl_test%_index_1220089';

INSERT INTO mx_ddl_table VALUES (37, 78, 2);
INSERT INTO mx_ddl_table VALUES (38, 78);

-- Switch to the coordinator
\c - - - :master_port


-- SET DATA TYPE
ALTER TABLE mx_ddl_table ALTER COLUMN version SET DATA TYPE double precision;

INSERT INTO mx_ddl_table VALUES (78, 83, 2.1);

\c - - - :worker_1_port
SELECT * FROM mx_ddl_table ORDER BY key;

-- Switch to the coordinator
\c - - - :master_port

-- DROP INDEX
DROP INDEX ddl_test_index;

DROP INDEX CONCURRENTLY ddl_test_concurrent_index;

-- DROP DEFAULT
ALTER TABLE mx_ddl_table ALTER COLUMN version DROP DEFAULT;

-- DROP NOT NULL
ALTER TABLE mx_ddl_table ALTER COLUMN version DROP NOT NULL;

-- DROP COLUMN
ALTER TABLE mx_ddl_table DROP COLUMN version;


-- See that the changes are applied on coordinator, worker tables and shards
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table'::regclass;
\di ddl_test*_index

\c - - - :worker_1_port

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table'::regclass;
\di ddl_test*_index
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table_1220088'::regclass;
\di ddl_test*_index_1220088

\c - - - :worker_2_port

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table'::regclass;
\di ddl_test*_index
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='mx_ddl_table_1220089'::regclass;
\di ddl_test*_index_1220089

-- Show that DDL commands are done within a two-phase commit transaction
\c - - - :master_port

CREATE INDEX ddl_test_index ON mx_ddl_table(value);

DROP INDEX ddl_test_index;

-- show that sequences owned by mx tables result in unique values
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
CREATE TABLE mx_sequence(key INT, value BIGSERIAL);
SELECT create_distributed_table('mx_sequence', 'key');

\c - - - :worker_1_port

SELECT last_value AS worker_1_lastval FROM mx_sequence_value_seq \gset

\c - - - :worker_2_port

SELECT last_value AS worker_2_lastval FROM mx_sequence_value_seq \gset

\c - - - :master_port

-- don't look at the actual values because they rely on the groupids of the nodes
-- which can change depending on the tests which have run before this one
SELECT :worker_1_lastval = :worker_2_lastval;

-- the type of sequences can't be changed
ALTER TABLE mx_sequence ALTER value TYPE BIGINT;
ALTER TABLE mx_sequence ALTER value TYPE INT;

-- test distributed tables owned by extension
CREATE TABLE seg_test (x int);
INSERT INTO seg_test VALUES (42);

-- pretend this table belongs to an extension
CREATE EXTENSION seg;
ALTER EXTENSION seg ADD TABLE seg_test;

\c - - - :worker_1_port

-- pretend the extension created the table on the worker as well
CREATE TABLE seg_test (x int);
ALTER EXTENSION seg ADD TABLE seg_test;

\c - - - :worker_2_port

-- pretend the extension created the table on the worker as well
CREATE TABLE seg_test (x int);
ALTER EXTENSION seg ADD TABLE seg_test;

\c - - - :master_port

-- sync table metadata, but skip CREATE TABLE
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
SELECT create_distributed_table('seg_test', 'x');

-- table should not show up in citus_tables or citus_shards
SELECT count(*) FROM citus_tables WHERE table_name = 'seg_test'::regclass;
SELECT count(*) FROM citus_shards WHERE table_name = 'seg_test'::regclass;

\c - - - :worker_1_port

-- should be able to see contents from worker
SELECT * FROM seg_test;

-- table should not show up in citus_tables or citus_shards
SELECT count(*) FROM citus_tables WHERE table_name = 'seg_test'::regclass;
SELECT count(*) FROM citus_shards WHERE table_name = 'seg_test'::regclass;

\c - - - :master_port

-- test metadata sync in the presence of an extension-owned table
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - - - :worker_1_port

-- should be able to see contents from worker
SELECT * FROM seg_test;

\c - - - :master_port

CREATE SCHEMA ext_owned_tables;

SET search_path TO ext_owned_tables;

CREATE sequence my_seq_ext_1;
SELECT run_command_on_workers($$CREATE sequence ext_owned_tables.my_seq_ext_1;$$);
CREATE sequence my_seq_ext_2;
SELECT run_command_on_workers($$CREATE sequence ext_owned_tables.my_seq_ext_2;$$);

-- test distributed tables owned by extension
CREATE TABLE seg_test (x int, y bigserial, z int default nextval('my_seq_ext_1'));
SELECT run_command_on_workers($$CREATE TABLE ext_owned_tables.seg_test (x int, y bigserial, z int default nextval('ext_owned_tables.my_seq_ext_1'))$$);

INSERT INTO seg_test VALUES (42);

CREATE TABLE tcn_test (x int, y bigserial, z int default nextval('my_seq_ext_2'));
SELECT run_command_on_workers($$CREATE TABLE ext_owned_tables.tcn_test (x int, y bigserial, z int default nextval('ext_owned_tables.my_seq_ext_2'));$$);

INSERT INTO tcn_test VALUES (42);

-- pretend this table belongs to an extension
ALTER EXTENSION seg ADD TABLE ext_owned_tables.seg_test;
ALTER EXTENSION seg ADD SEQUENCE ext_owned_tables.my_seq_ext_1;
SELECT run_command_on_workers($$ALTER EXTENSION seg ADD TABLE ext_owned_tables.seg_test;$$);
SELECT run_command_on_workers($$ALTER EXTENSION seg ADD SEQUENCE ext_owned_tables.my_seq_ext_1;$$);


CREATE EXTENSION tcn;
ALTER EXTENSION tcn ADD TABLE ext_owned_tables.tcn_test;
ALTER EXTENSION tcn ADD SEQUENCE ext_owned_tables.my_seq_ext_2;
SELECT run_command_on_workers($$ALTER EXTENSION tcn ADD TABLE ext_owned_tables.tcn_test;$$);
SELECT run_command_on_workers($$ALTER EXTENSION tcn ADD SEQUENCE ext_owned_tables.my_seq_ext_2;$$);

SELECT create_reference_table('seg_test');
SELECT create_distributed_table('tcn_test', 'x');

-- test metadata re-sync in the presence of an extension-owned table
-- and serial/sequences
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- also drops table on both worker and master
SET client_min_messages TO ERROR;
DROP SCHEMA ext_owned_tables CASCADE;
