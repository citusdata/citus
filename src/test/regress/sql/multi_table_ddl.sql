--
-- MULTI_TABLE_DDL
--
-- Tests around changing the schema and dropping of a distributed table
-- Test DEFAULTS coming from SERIAL pseudo-types, user-defined sequences
--


SET citus.next_shard_id TO 870000;


CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT create_distributed_table('testtableddl', 'distributecol', 'append');

-- verify that the citus extension can't be dropped while distributed tables exist
DROP EXTENSION citus;

-- verify that the distribution column can't have its type changed
ALTER TABLE testtableddl ALTER COLUMN distributecol TYPE text;

-- verify that the distribution column can't be dropped
ALTER TABLE testtableddl DROP COLUMN distributecol;

-- verify that the table can be dropped in a transaction block
\set VERBOSITY terse
BEGIN;
DROP TABLE testtableddl;
COMMIT;
\set VERBOSITY default

-- recreate testtableddl
CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT create_distributed_table('testtableddl', 'distributecol', 'append');

-- verify that the table can be dropped
DROP TABLE testtableddl;

-- verify that the table can dropped even if shards exist
CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);

-- create table and do create empty shard test here, too
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('testtableddl', 'distributecol', 'append');
SELECT 1 FROM master_create_empty_shard('testtableddl');

-- now actually drop table and shards
DROP TABLE testtableddl;

RESET citus.shard_replication_factor;

-- ensure no metadata of distributed tables are remaining
SELECT * FROM pg_dist_partition;
SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

-- check that the extension now can be dropped (and recreated)
DROP EXTENSION citus;
CREATE EXTENSION citus;

-- re-add the nodes to the cluster
SELECT 1 FROM citus_set_coordinator_host('localhost');
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- create a table with a SERIAL column
CREATE TABLE testserialtable(id serial, group_id integer);

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('testserialtable', 'group_id', 'hash');

-- cannot add additional serial columns when metadata is synced
ALTER TABLE testserialtable ADD COLUMN other_id serial;

-- and we shouldn't be able to change a distributed sequence's owner
ALTER SEQUENCE testserialtable_id_seq OWNED BY NONE;

-- or create a sequence with a distributed owner
CREATE SEQUENCE standalone_sequence OWNED BY testserialtable.group_id;

-- EDIT: this doesn't error out for now in order to allow adding
-- new serial columns (they always come with owned_by command)
-- should be fixed later in ALTER SEQUENCE preprocessing

-- or even change a manual sequence to be owned by a distributed table
CREATE SEQUENCE standalone_sequence;
ALTER SEQUENCE standalone_sequence OWNED BY testserialtable.group_id;

-- cannot even change owner to the same distributed table if the sequence is distributed
ALTER SEQUENCE testserialtable_id_seq OWNED BY testserialtable.id;

-- drop distributed table
\c - - - :master_port
DROP TABLE testserialtable;

-- verify owned sequence is dropped
\c - - - :worker_1_port
\ds

\c - - - :master_port

-- test DEFAULT coming from SERIAL pseudo-types and user-defined sequences
CREATE SEQUENCE test_sequence_0;
CREATE SEQUENCE test_sequence_1;

CREATE TABLE test_table (id1 int DEFAULT nextval('test_sequence_0'));
SELECT create_distributed_table('test_table', 'id1');

-- shouldn't work since it's partition column
ALTER TABLE test_table ALTER COLUMN id1 SET DEFAULT nextval('test_sequence_1');

-- test different plausible commands
ALTER TABLE test_table ADD COLUMN id2 int DEFAULT nextval('test_sequence_1');
ALTER TABLE test_table ALTER COLUMN id2 DROP DEFAULT;
ALTER TABLE test_table ALTER COLUMN id2 SET DEFAULT nextval('test_sequence_1');

-- shouldn't work since the above operations should be the only subcommands
ALTER TABLE test_table ADD COLUMN id4 int DEFAULT nextval('test_sequence_1') CHECK (id4 > 0);
ALTER TABLE test_table ADD COLUMN id4 int, ADD COLUMN id5 int DEFAULT nextval('test_sequence_1');
ALTER TABLE test_table ALTER COLUMN id3 SET DEFAULT nextval('test_sequence_1'), ALTER COLUMN id2 DROP DEFAULT;

-- shouldn't work because of metadata syncing
ALTER TABLE test_table ADD COLUMN id3 bigserial;
ALTER TABLE test_table ADD COLUMN id4 bigserial CHECK (id4 > 0);

CREATE SEQUENCE pg_temp.temp_sequence;
CREATE TABLE table_with_temp_sequence (
  dist_key int,
  seq_col bigint default nextval('pg_temp.temp_sequence')
);
SELECT create_distributed_table('table_with_temp_sequence', 'dist_key');

DROP TABLE test_table CASCADE;
DROP SEQUENCE test_sequence_0;
DROP SEQUENCE test_sequence_1;
