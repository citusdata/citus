
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

-- run VACUUM and ANALYZE against the table on the master
VACUUM dustbunnies;
ANALYZE dustbunnies;

-- verify that the VACUUM and ANALYZE ran
\c - - - :worker_1_port
SELECT pg_sleep(.500);
SELECT pg_stat_get_vacuum_count('dustbunnies_990002'::regclass);
SELECT pg_stat_get_analyze_count('dustbunnies_990002'::regclass);

-- disable auto-VACUUM for next test
ALTER TABLE dustbunnies_990002 SET (autovacuum_enabled = false);
SELECT relfrozenxid AS frozenxid FROM pg_class WHERE oid='dustbunnies_990002'::regclass
\gset

-- send a VACUUM FREEZE after adding a new row
\c - - - :master_port
INSERT INTO dustbunnies VALUES (5, 'peter');
VACUUM (FREEZE) dustbunnies;

-- verify that relfrozenxid increased
\c - - - :worker_1_port
SELECT relfrozenxid::text::integer > :frozenxid AS frozen_performed FROM pg_class
WHERE oid='dustbunnies_990002'::regclass;

-- get file node to verify VACUUM FULL
SELECT relfilenode AS oldnode FROM pg_class WHERE oid='dustbunnies_990002'::regclass
\gset

-- send a VACUUM FULL
\c - - - :master_port
VACUUM (FULL) dustbunnies;

-- verify that relfrozenxid increased
\c - - - :worker_1_port
SELECT relfilenode != :oldnode AS table_rewritten FROM pg_class
WHERE oid='dustbunnies_990002'::regclass;
