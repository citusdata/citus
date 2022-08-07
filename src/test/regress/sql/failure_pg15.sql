--
-- FAILURE_PG15
--
-- Each of the following tests: failure_ddl.sql, failure_truncate.sql
-- failure_multi_dml.sql, failure_vacuum.sql
-- has a part with alternative output for PG15 resulting
-- from removal of duplicate error messages
-- Relevant PG commit: 618c16707a6d6e8f5c83ede2092975e4670201ad
-- This test file has been created to avoid 4 alternative output files

CREATE SCHEMA pg15_failure;
SET citus.force_max_query_parallelization TO ON;
SET search_path TO 'pg15_failure';

-- do not cache any connections
SET citus.max_cached_conns_per_worker TO 0;

-- we don't want to see the prepared transaction numbers in the warnings
SET client_min_messages TO WARNING;

SELECT citus.mitmproxy('conn.allow()');

SET citus.next_shard_id TO 100700;

-- we'll start with replication factor 1, 2PC and parallel mode
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 1;

CREATE TABLE test_table (key int, value int);
SELECT create_distributed_table('test_table', 'key');

-- from failure_ddl.sql

-- but now kill just after the worker sends response to
-- COMMIT command, so we'll have lots of warnings but the command
-- should have been committed both on the distributed table and the placements
SET client_min_messages TO WARNING;
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COMMIT").kill()');
ALTER TABLE test_table ADD COLUMN new_column INT;
SELECT citus.mitmproxy('conn.allow()');

SET client_min_messages TO ERROR;

SELECT array_agg(name::text ORDER BY name::text) FROM public.table_attrs where relid = 'test_table'::regclass;
SELECT run_command_on_placements('test_table', $$SELECT array_agg(name::text ORDER BY name::text) FROM public.table_attrs where relid = '%s'::regclass;$$) ORDER BY 1;

-- the following tests rely the column not exists, so drop manually
ALTER TABLE test_table DROP COLUMN new_column;

-- from failure_truncate.sql

CREATE VIEW unhealthy_shard_count AS
  SELECT count(*)
  FROM pg_dist_shard_placement pdsp
  JOIN
  pg_dist_shard pds
  ON pdsp.shardid=pds.shardid
  WHERE logicalrelid='pg15_failure.test_table'::regclass AND shardstate != 1;

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

SET client_min_messages TO WARNING;
-- now kill just after the worker sends response to
-- COMMIT command, so we'll have lots of warnings but the command
-- should have been committed both on the distributed table and the placements
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COMMIT").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;


-- from failure_multi_dml.sql

SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- two placements

CREATE TABLE dml_test (id integer, name text);
SELECT create_distributed_table('dml_test', 'id');

COPY dml_test FROM STDIN WITH CSV;
1,Alpha
2,Beta
3,Gamma
4,Delta
\.

---- test multiple statements against a single shard, but with two placements

-- fail at PREPARED COMMIT as we use 2PC
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

-- all changes should be committed because we injected
-- the failure on the COMMIT time. And, we should not
-- mark any placements as INVALID
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();
SELECT shardid FROM pg_dist_shard_placement WHERE shardstate = 3;

SET citus.task_assignment_policy TO "round-robin";
SELECT * FROM dml_test ORDER BY id ASC;
SELECT * FROM dml_test ORDER BY id ASC;
RESET citus.task_assignment_policy;

-- from failure_vacuum.sql

CREATE TABLE vacuum_test (key int, value int);
SELECT create_distributed_table('vacuum_test', 'key');

SELECT citus.clear_network_traffic();

SELECT citus.mitmproxy('conn.onQuery(query="^VACUUM").kill()');
VACUUM vacuum_test;

SELECT citus.mitmproxy('conn.onQuery(query="^ANALYZE").kill()');
ANALYZE vacuum_test;

SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
ANALYZE vacuum_test;

SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();

-- ANALYZE transactions being critical is an open question, see #2430
-- show that we never mark as INVALID on COMMIT FAILURE
SELECT shardid, shardstate FROM pg_dist_shard_placement where shardstate != 1 AND
shardid in ( SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'vacuum_test'::regclass);

-- Clean up
SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA pg15_failure CASCADE;
