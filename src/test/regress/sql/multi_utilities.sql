
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 990000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 990000;


-- ===================================================================
-- test utility statement functionality
-- ===================================================================

CREATE TABLE sharded_table ( name text, id bigint );
SELECT master_create_distributed_table('sharded_table', 'id', 'hash');
SELECT master_create_worker_shards('sharded_table', 2, 1);

-- COPY out is supported with distributed tables
COPY sharded_table TO STDOUT;
COPY (SELECT COUNT(*) FROM sharded_table) TO STDOUT;

-- cursors may not involve distributed tables
DECLARE all_sharded_rows CURSOR FOR SELECT * FROM sharded_table;

-- verify PREPARE functionality
PREPARE sharded_insert AS INSERT INTO sharded_table VALUES ('adam', 1);
PREPARE sharded_update AS UPDATE sharded_table SET name = 'bob' WHERE id = 1;
PREPARE sharded_delete AS DELETE FROM sharded_table WHERE id = 1;
PREPARE sharded_query  AS SELECT name FROM sharded_table WHERE id = 1;

EXECUTE sharded_query;
EXECUTE sharded_insert;
EXECUTE sharded_query;
EXECUTE sharded_update;
EXECUTE sharded_query;
EXECUTE sharded_delete;
EXECUTE sharded_query;

-- try to drop shards with where clause
SELECT master_apply_delete_command('DELETE FROM sharded_table WHERE id > 0');

-- drop all shards
SELECT master_apply_delete_command('DELETE FROM sharded_table');

-- drop table
DROP TABLE sharded_table;

-- VACUUM tests

-- create a table with a single shard (for convenience)
CREATE TABLE dustbunnies (id integer, name text);
SELECT master_create_distributed_table('dustbunnies', 'id', 'hash');
SELECT master_create_worker_shards('dustbunnies', 1, 2);

-- add some data to the distributed table
\copy dustbunnies from stdin with csv
1,bugs
2,babs
3,buster
4,roger
\.

-- delete all rows from the shard, then run VACUUM against the table on the master
DELETE FROM dustbunnies;
VACUUM dustbunnies;

-- update statistics, then verify that the four dead rows are gone
\c - - - :worker_1_port
SELECT pg_sleep(.500);
SELECT pg_stat_get_vacuum_count('dustbunnies_990002'::regclass);
