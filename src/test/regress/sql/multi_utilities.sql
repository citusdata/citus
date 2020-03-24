
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

-- try to drop shards with where clause
SELECT master_apply_delete_command('DELETE FROM sharded_table WHERE id > 0');

-- drop all shards
SELECT master_apply_delete_command('DELETE FROM sharded_table');
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

-- following approach adapted from PostgreSQL's stats.sql file

-- save relevant stat counter values in refreshable view
\c - - :public_worker_1_host :worker_1_port
CREATE MATERIALIZED VIEW prevcounts AS
SELECT analyze_count, vacuum_count FROM pg_stat_user_tables
WHERE relname='dustbunnies_990002';

-- create function that sleeps until those counters increment
create function wait_for_stats() returns void as $$
declare
  start_time timestamptz := clock_timestamp();
  analyze_updated bool;
  vacuum_updated bool;
begin
  -- we don't want to wait forever; loop will exit after 10 seconds
  for i in 1 .. 100 loop

    -- check to see if analyze has been updated
    SELECT (st.analyze_count >= pc.analyze_count + 1) INTO analyze_updated
      FROM pg_stat_user_tables AS st, pg_class AS cl, prevcounts AS pc
     WHERE st.relname='dustbunnies_990002' AND cl.relname='dustbunnies_990002';

     -- check to see if vacuum has been updated
    SELECT (st.vacuum_count >= pc.vacuum_count + 1) INTO vacuum_updated
      FROM pg_stat_user_tables AS st, pg_class AS cl, prevcounts AS pc
     WHERE st.relname='dustbunnies_990002' AND cl.relname='dustbunnies_990002';

    exit when analyze_updated or vacuum_updated;

    -- wait a little
    perform pg_sleep(0.1);

    -- reset stats snapshot so we can test again
    perform pg_stat_clear_snapshot();

  end loop;

  -- report time waited in postmaster log (where it won't change test output)
  raise log 'wait_for_stats delayed % seconds',
    extract(epoch from clock_timestamp() - start_time);
end
$$ language plpgsql;

\c - - :public_worker_2_host :worker_2_port
CREATE MATERIALIZED VIEW prevcounts AS
SELECT analyze_count, vacuum_count FROM pg_stat_user_tables
WHERE relname='dustbunnies_990001';
-- create function that sleeps until those counters increment
create function wait_for_stats() returns void as $$
declare
  start_time timestamptz := clock_timestamp();
  analyze_updated bool;
  vacuum_updated bool;
begin
  -- we don't want to wait forever; loop will exit after 10 seconds
  for i in 1 .. 100 loop

    -- check to see if analyze has been updated
    SELECT (st.analyze_count >= pc.analyze_count + 1) INTO analyze_updated
      FROM pg_stat_user_tables AS st, pg_class AS cl, prevcounts AS pc
     WHERE st.relname='dustbunnies_990001' AND cl.relname='dustbunnies_990001';

     -- check to see if vacuum has been updated
    SELECT (st.vacuum_count >= pc.vacuum_count + 1) INTO vacuum_updated
      FROM pg_stat_user_tables AS st, pg_class AS cl, prevcounts AS pc
     WHERE st.relname='dustbunnies_990001' AND cl.relname='dustbunnies_990001';

    exit when analyze_updated or vacuum_updated;

    -- wait a little
    perform pg_sleep(0.1);

    -- reset stats snapshot so we can test again
    perform pg_stat_clear_snapshot();

  end loop;

  -- report time waited in postmaster log (where it won't change test output)
  raise log 'wait_for_stats delayed % seconds',
    extract(epoch from clock_timestamp() - start_time);
end
$$ language plpgsql;

-- run VACUUM and ANALYZE against the table on the master
\c - - :master_host :master_port
VACUUM dustbunnies;
ANALYZE dustbunnies;

-- verify that the VACUUM and ANALYZE ran
\c - - :public_worker_1_host :worker_1_port
SELECT wait_for_stats();
REFRESH MATERIALIZED VIEW prevcounts;
SELECT pg_stat_get_vacuum_count('dustbunnies_990002'::regclass);
SELECT pg_stat_get_analyze_count('dustbunnies_990002'::regclass);

-- get file node to verify VACUUM FULL
SELECT relfilenode AS oldnode FROM pg_class WHERE oid='dustbunnies_990002'::regclass
\gset

-- send a VACUUM FULL and a VACUUM ANALYZE
\c - - :master_host :master_port
VACUUM (FULL) dustbunnies;
VACUUM ANALYZE dustbunnies;

-- verify that relfilenode changed
\c - - :public_worker_1_host :worker_1_port
SELECT relfilenode != :oldnode AS table_rewritten FROM pg_class
WHERE oid='dustbunnies_990002'::regclass;

-- verify the VACUUM ANALYZE incremented both vacuum and analyze counts
SELECT wait_for_stats();
SELECT pg_stat_get_vacuum_count('dustbunnies_990002'::regclass);
SELECT pg_stat_get_analyze_count('dustbunnies_990002'::regclass);

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
-- verify warning for unqualified VACUUM
VACUUM;

-- check for multiple table vacuum
VACUUM dustbunnies, second_dustbunnies;

-- check the current number of vacuum and analyze run on dustbunnies
SELECT run_command_on_workers($$SELECT wait_for_stats()$$);
SELECT run_command_on_workers($$SELECT pg_stat_get_vacuum_count(tablename::regclass) from pg_tables where tablename LIKE 'dustbunnies_%' limit 1$$);
SELECT run_command_on_workers($$SELECT pg_stat_get_analyze_count(tablename::regclass) from pg_tables where tablename LIKE 'dustbunnies_%' limit 1$$);

-- and warning when using targeted VACUUM without DDL propagation
SET citus.enable_ddl_propagation to false;
VACUUM dustbunnies;
ANALYZE dustbunnies;
SET citus.enable_ddl_propagation to DEFAULT;

-- should not propagate the vacuum and analyze
SELECT run_command_on_workers($$SELECT wait_for_stats()$$);
SELECT run_command_on_workers($$SELECT pg_stat_get_vacuum_count(tablename::regclass) from pg_tables where tablename LIKE 'dustbunnies_%' limit 1$$);
SELECT run_command_on_workers($$SELECT pg_stat_get_analyze_count(tablename::regclass) from pg_tables where tablename LIKE 'dustbunnies_%' limit 1$$);

-- test worker_hash
SELECT worker_hash(123);
SELECT worker_hash('1997-08-08'::date);

-- test a custom type (this test should run after multi_data_types)
SELECT worker_hash('(1, 2)');
SELECT worker_hash('(1, 2)'::test_composite_type);

SELECT citus_truncate_trigger();

-- confirm that citus_create_restore_point works
SELECT 1 FROM citus_create_restore_point('regression-test');
