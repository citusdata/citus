--
-- MULTI_TABLE_DDL
--
-- Tests around changing the schema and dropping of a distributed table


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 870000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 870000;


CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT master_create_distributed_table('testtableddl', 'distributecol', 'append');

-- verify that the citus extension can't be dropped while distributed tables exist
DROP EXTENSION citus;

-- verify that the distribution column can't have its type changed
ALTER TABLE testtableddl ALTER COLUMN distributecol TYPE text;

-- verify that the distribution column can't be dropped
ALTER TABLE testtableddl DROP COLUMN distributecol;

-- verify that the table cannot be dropped in a transaction block
BEGIN;
DROP TABLE testtableddl;
ROLLBACK;

-- verify that the table can be dropped
DROP TABLE testtableddl;

-- verify that the table can dropped even if shards exist
CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT master_create_distributed_table('testtableddl', 'distributecol', 'append');
SELECT 1 FROM master_create_empty_shard('testtableddl');
DROP TABLE testtableddl;

-- ensure no metadata of distributed tables are remaining
SELECT * FROM pg_dist_partition;
SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

-- check that the extension now can be dropped (and recreated)
DROP EXTENSION citus;
CREATE EXTENSION citus;
