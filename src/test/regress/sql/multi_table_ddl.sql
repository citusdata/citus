--
-- MULTI_TABLE_DDL
--
-- Tests around changing the schema and dropping of a distributed table

CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT master_create_distributed_table('testtableddl', 'distributecol', 'append');

-- verify that the citusdb extension can't be dropped while distributed tables exist
DROP EXTENSION citusdb;

-- verify that the distribution column can't have its type changed
ALTER TABLE testtableddl ALTER COLUMN distributecol TYPE text;

-- verify that the distribution column can't be dropped
ALTER TABLE testtableddl DROP COLUMN distributecol;

-- verify that the table cannot be dropped while shards exist
SELECT 1 FROM master_create_empty_shard('testtableddl');
DROP TABLE testtableddl;

-- not even with cascade
DROP TABLE testtableddl CASCADE;

-- but it can be dropped after dropping the shards
SELECT master_apply_delete_command('DELETE FROM testtableddl');
DROP TABLE testtableddl;

-- ensure no metadata of distributed tables are remaining
SELECT * FROM pg_dist_partition;
SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

-- check that the extension now can be dropped (and recreated). We reconnect
-- before creating the extension to expire extension specific variables which
-- are cached for performance.
DROP EXTENSION citusdb;
\c
CREATE EXTENSION citusdb;
