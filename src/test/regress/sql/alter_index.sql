CREATE SCHEMA alterindex;

SET search_path TO "alterindex";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

-- test alter index set statistics
CREATE TABLE t1 (a int, b int);
SELECT create_distributed_table('t1','a');
CREATE INDEX test_idx on t1 ((a+b));
ALTER INDEX test_idx ALTER COLUMN 1 SET STATISTICS 4646;
ALTER INDEX test_idx ALTER COLUMN 1 SET STATISTICS -4646;
ALTER INDEX test_idx ALTER COLUMN 3 SET STATISTICS 4646;

-- test alter index set statistics before distribution
CREATE TABLE t2 (a int, b int);
CREATE INDEX test_idx2 on t2 ((a+b), (a-b), (a*b));
ALTER INDEX test_idx2 ALTER COLUMN 2 SET STATISTICS 3737;
ALTER INDEX test_idx2 ALTER COLUMN 3 SET STATISTICS 3737;
ALTER INDEX test_idx2 ALTER COLUMN 2 SET STATISTICS 99999;
SELECT create_distributed_table('t2','a');

-- verify statistics is set
-- pg17 Changed `attstattarget` in `pg_attribute` to use `NullableDatum`, allowing null representation for default statistics target in PostgreSQL 17.
-- https://github.com/postgres/postgres/commit/6a004f1be87d34cfe51acf2fe2552d2b08a79273
SELECT c.relname,
       CASE WHEN a.attstattarget = -1 THEN NULL ELSE a.attstattarget END AS attstattarget
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid AND c.relname LIKE 'test\_idx%'
ORDER BY c.relname, a.attnum;

\c - - - :worker_1_port
SELECT c.relname,
       CASE WHEN a.attstattarget = -1 THEN NULL ELSE a.attstattarget END AS attstattarget
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid AND c.relname SIMILAR TO 'test\_idx%\_\d%'
ORDER BY c.relname, a.attnum;
\c - - - :master_port

SET client_min_messages TO WARNING;
DROP SCHEMA alterindex CASCADE;
