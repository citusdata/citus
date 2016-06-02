
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
