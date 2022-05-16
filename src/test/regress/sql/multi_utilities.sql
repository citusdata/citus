
SET citus.next_shard_id TO 990000;

-- ===================================================================
-- test utility statement functionality
-- ===================================================================
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

CREATE TABLE sharded_table ( name text, id bigint );
SELECT create_distributed_table('sharded_table', 'id', 'hash');

-- COPY out is supported with distributed tables
COPY sharded_table TO STDOUT;
COPY (SELECT COUNT(*) FROM sharded_table) TO STDOUT;

BEGIN;
SET TRANSACTION READ ONLY;
COPY sharded_table TO STDOUT;
COPY (SELECT COUNT(*) FROM sharded_table) TO STDOUT;
COMMIT;

-- ANALYZE is supported in a transaction block
BEGIN;
ANALYZE sharded_table;
ANALYZE sharded_table;
END;

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

-- drop all shards
SELECT citus_drop_all_shards('sharded_table','','');
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 999001;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1400000;
CREATE TABLE lockable_table ( name text, id bigint );
SELECT create_distributed_table('lockable_table', 'id', 'hash', colocate_with := 'none');
SET citus.shard_count TO 2;
SET citus.next_shard_id TO 990002;

-- lock shard metadata: take some share locks and exclusive locks
BEGIN;
SELECT lock_shard_metadata(5, ARRAY[999001, 999002, 999002]);
SELECT lock_shard_metadata(7, ARRAY[999001, 999003, 999004]);

SELECT
    CASE
        WHEN l.objsubid = 5 THEN 'shard'
        WHEN l.objsubid = 4 THEN 'shard_metadata'
        ELSE 'colocated_shards_metadata'
    END AS locktype,
    objid,
    classid,
    mode,
    granted
FROM pg_locks l
WHERE l.locktype = 'advisory'
ORDER BY locktype, objid, classid, mode;
END;

-- lock shard metadata: unsupported lock type
SELECT lock_shard_metadata(0, ARRAY[990001, 999002]);

-- lock shard metadata: invalid shard ID
SELECT lock_shard_metadata(5, ARRAY[0]);

-- lock shard metadata: lock nothing
SELECT lock_shard_metadata(5, ARRAY[]::bigint[]);

-- lock shard resources: take some share locks and exclusive locks
BEGIN;
SELECT lock_shard_resources(5, ARRAY[999001, 999002, 999002]);
SELECT lock_shard_resources(7, ARRAY[999001, 999003, 999004]);

SELECT locktype, objid, mode, granted
FROM pg_locks
WHERE objid IN (999001, 999002, 999003, 999004)
ORDER BY objid, mode;
END;

-- lock shard metadata: unsupported lock type
SELECT lock_shard_resources(0, ARRAY[990001, 999002]);

-- lock shard metadata: invalid shard ID
SELECT lock_shard_resources(5, ARRAY[-1]);

-- lock shard metadata: lock nothing
SELECT lock_shard_resources(5, ARRAY[]::bigint[]);

-- drop table
DROP TABLE sharded_table;
DROP TABLE lockable_table;

-- VACUUM tests

-- create a table with a single shard (for convenience)
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 2;

CREATE TABLE dustbunnies (id integer, name text, age integer);
SELECT create_distributed_table('dustbunnies', 'id', 'hash');

-- add some data to the distributed table
\copy dustbunnies (id, name) from stdin with csv
1,bugs
2,babs
3,buster
4,roger
\.

CREATE TABLE second_dustbunnies(id integer, name text, age integer);
SELECT master_create_distributed_table('second_dustbunnies', 'id', 'hash');
SELECT master_create_worker_shards('second_dustbunnies', 1, 2);

-- run VACUUM and ANALYZE against the table on the master
\c - - :master_host :master_port
VACUUM dustbunnies;
ANALYZE dustbunnies;

-- send a VACUUM FULL and a VACUUM ANALYZE

VACUUM (FULL) dustbunnies;
VACUUM ANALYZE dustbunnies;

\c - - :public_worker_1_host :worker_1_port
-- disable auto-VACUUM for next test
ALTER TABLE dustbunnies_990002 SET (autovacuum_enabled = false);
SELECT relfrozenxid AS frozenxid FROM pg_class WHERE oid='dustbunnies_990002'::regclass
\gset

-- send a VACUUM FREEZE after adding a new row
\c - - :master_host :master_port
INSERT INTO dustbunnies VALUES (5, 'peter');
VACUUM (FREEZE) dustbunnies;

-- verify that relfrozenxid increased
\c - - :public_worker_1_host :worker_1_port
SELECT relfrozenxid::text::integer > :frozenxid AS frozen_performed FROM pg_class
WHERE oid='dustbunnies_990002'::regclass;

-- check there are no nulls in either column
SELECT attname, null_frac FROM pg_stats
WHERE tablename = 'dustbunnies_990002' ORDER BY attname;

-- add NULL values, then perform column-specific ANALYZE
\c - - :master_host :master_port
INSERT INTO dustbunnies VALUES (6, NULL, NULL);
ANALYZE dustbunnies (name);

-- verify that name's NULL ratio is updated but age's is not
\c - - :public_worker_1_host :worker_1_port
SELECT attname, null_frac FROM pg_stats
WHERE tablename = 'dustbunnies_990002' ORDER BY attname;

\c - - :master_host :master_port
SET citus.log_remote_commands TO ON;

-- check for multiple table vacuum
VACUUM dustbunnies, second_dustbunnies;

-- and do not propagate when using targeted VACUUM without DDL propagation
SET citus.enable_ddl_propagation to false;
VACUUM dustbunnies;
ANALYZE dustbunnies;
SET citus.enable_ddl_propagation to DEFAULT;

-- test worker_hash
SELECT worker_hash(123);
SELECT worker_hash('1997-08-08'::date);

-- test a custom type (this test should run after multi_data_types)
SELECT worker_hash('(1, 2)');
SELECT worker_hash('(1, 2)'::test_composite_type);

SELECT citus_truncate_trigger();

-- make sure worker_create_or_alter_role does not crash with NULL input
SELECT worker_create_or_alter_role(NULL, NULL, NULL);
SELECT worker_create_or_alter_role(NULL, 'create role dontcrash', NULL);

-- confirm that citus_create_restore_point works
SELECT 1 FROM citus_create_restore_point('regression-test');

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 970000;

SET citus.log_remote_commands TO OFF;

CREATE TABLE local_vacuum_table(id int);

CREATE TABLE reference_vacuum_table(id int);
SELECT create_reference_table('reference_vacuum_table');

CREATE TABLE distributed_vacuum_table(id int);
SELECT create_distributed_table('distributed_vacuum_table', 'id');

SET citus.log_remote_commands TO ON;

-- should propagate to all workers because no table is specified
VACUUM;

-- should not propagate because no distributed table is specified
VACUUM local_vacuum_table;

-- should propagate to all workers because table is reference table
VACUUM reference_vacuum_table;

-- should propagate to all workers because table is distributed table
VACUUM distributed_vacuum_table;

-- only distributed_vacuum_table and reference_vacuum_table should propagate
VACUUM distributed_vacuum_table, local_vacuum_table, reference_vacuum_table;

-- only reference_vacuum_table should propagate
VACUUM local_vacuum_table, reference_vacuum_table;

-- should not propagate because ddl propagation is disabled
SET citus.enable_ddl_propagation TO OFF;
VACUUM distributed_vacuum_table;
SET citus.enable_ddl_propagation TO ON;

SET citus.log_remote_commands TO OFF;

-- ANALYZE tests
CREATE TABLE local_analyze_table(id int);

CREATE TABLE reference_analyze_table(id int);
SELECT create_reference_table('reference_analyze_table');

CREATE TABLE distributed_analyze_table(id int);
SELECT create_distributed_table('distributed_analyze_table', 'id');

SET citus.log_remote_commands TO ON;

-- should propagate to all workers because no table is specified
ANALYZE;

-- should not propagate because no distributed table is specified
ANALYZE local_analyze_table;

-- should propagate to all workers because table is reference table
ANALYZE reference_analyze_table;

-- should propagate to all workers because table is distributed table
ANALYZE distributed_analyze_table;

-- only distributed_analyze_table and reference_analyze_table should propagate
ANALYZE distributed_analyze_table, local_analyze_table, reference_analyze_table;

-- only reference_analyze_table should propagate
ANALYZE local_analyze_table, reference_analyze_table;

-- should not propagate because ddl propagation is disabled
SET citus.enable_ddl_propagation TO OFF;
ANALYZE distributed_analyze_table;
SET citus.enable_ddl_propagation TO ON;
