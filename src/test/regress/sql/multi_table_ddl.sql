--
-- MULTI_TABLE_DDL
--
-- Tests around changing the schema and dropping of a distributed table


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
SELECT 1 FROM master_add_node(:'worker_1_host', :worker_1_port);
SELECT 1 FROM master_add_node(:'worker_2_host', :worker_2_port);

-- create a table with a SERIAL column
CREATE TABLE testserialtable(id serial, group_id integer);

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('testserialtable', 'group_id', 'hash');

-- should not be able to add additional serial columns
ALTER TABLE testserialtable ADD COLUMN other_id serial;

-- and we shouldn't be able to change a distributed sequence's owner
ALTER SEQUENCE testserialtable_id_seq OWNED BY NONE;

-- or create a sequence with a distributed owner
CREATE SEQUENCE standalone_sequence OWNED BY testserialtable.group_id;

-- or even change a manual sequence to be owned by a distributed table
CREATE SEQUENCE standalone_sequence;
ALTER SEQUENCE standalone_sequence OWNED BY testserialtable.group_id;

-- an edge case, but it's OK to change an owner to the same distributed table
ALTER SEQUENCE testserialtable_id_seq OWNED BY testserialtable.id;

-- drop distributed table
\c - - - :master_port
DROP TABLE testserialtable;

-- verify owned sequence is dropped
\c - - - :worker_1_port
\ds
