CREATE SCHEMA single_node;
SET search_path TO single_node;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 90630500;
SET citus.replication_model TO 'streaming';

-- adding the coordinator as inactive is disallowed
SELECT 1 FROM master_add_inactive_node('localhost', :master_port, groupid => 0);

-- idempotently add node to allow this test to run without add_coordinator
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);

-- coordinator cannot be disabled
SELECT 1 FROM master_disable_node('localhost', :master_port);

RESET client_min_messages;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

CREATE TABLE test(x int, y int);
SELECT create_distributed_table('test','x');

CREATE TYPE new_type AS (n int, m text);
CREATE TABLE test_2(x int, y int, z new_type);
SELECT create_distributed_table('test_2','x');

CREATE TABLE ref(a int, b int);
SELECT create_reference_table('ref');
CREATE TABLE local(c int, d int);

CREATE TABLE public.another_schema_table(a int, b int);
SELECT create_distributed_table('public.another_schema_table', 'a');

-- Confirm the basics work
INSERT INTO test VALUES (1, 2), (3, 4), (5, 6), (2, 7), (4, 5);
SELECT * FROM test WHERE x = 1;
SELECT count(*) FROM test;
SELECT * FROM test ORDER BY x;
UPDATE test SET y = y + 1 RETURNING *;
WITH cte_1 AS (UPDATE test SET y = y - 1 RETURNING *) SELECT * FROM cte_1 ORDER BY 1,2;

-- Test upsert with constraint
CREATE TABLE upsert_test
(
	part_key int UNIQUE,
	other_col int,
	third_col int
);

-- distribute the table
SELECT create_distributed_table('upsert_test', 'part_key');

-- do a regular insert
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1), (2, 2) RETURNING *;

SET citus.log_remote_commands to true;

-- observe that there is a conflict and the following query does nothing
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT DO NOTHING RETURNING *;

-- same as the above with different syntax
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO NOTHING RETURNING *;

-- again the same query with another syntax
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING RETURNING *;

BEGIN;

-- force local execution
SELECT count(*) FROM upsert_test WHERE part_key = 1;

SET citus.log_remote_commands to false;

-- multi-shard pushdown query that goes through local execution
INSERT INTO upsert_test (part_key, other_col) SELECT part_key, other_col FROM upsert_test ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING RETURNING *;

-- multi-shard pull-to-coordinator query that goes through local execution

INSERT INTO upsert_test (part_key, other_col) SELECT part_key, other_col FROM upsert_test LIMIT 100 ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING RETURNING *;

COMMIT;

-- to test citus local tables
select undistribute_table('upsert_test');
-- create citus local table
select create_citus_local_table('upsert_test');
-- test the constraint with local execution
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING RETURNING *;

DROP TABLE upsert_test;

CREATE SCHEMA "Quoed.Schema";
SET search_path TO "Quoed.Schema";


CREATE TABLE "long_constraint_upsert\_test"
(
	part_key int,
	other_col int,
	third_col int,

	CONSTRAINT "looo oooo ooooo ooooooooooooooooo oooooooo oooooooo ng quoted  \aconstraint" UNIQUE (part_key)
);
-- distribute the table and create shards
SELECT create_distributed_table('"long_constraint_upsert\_test"', 'part_key');


INSERT INTO "long_constraint_upsert\_test" (part_key, other_col) VALUES (1, 1) ON CONFLICT ON CONSTRAINT  "looo oooo ooooo ooooooooooooooooo oooooooo oooooooo ng quoted  \aconstraint" DO NOTHING RETURNING *;

ALTER TABLE "long_constraint_upsert\_test" RENAME TO simple_table_name;

INSERT INTO simple_table_name (part_key, other_col) VALUES (1, 1) ON CONFLICT ON CONSTRAINT  "looo oooo ooooo ooooooooooooooooo oooooooo oooooooo ng quoted  \aconstraint" DO NOTHING RETURNING *;

-- this is currently not supported, but once we support
-- make sure that the following query also works fine
ALTER TABLE simple_table_name RENAME CONSTRAINT "looo oooo ooooo ooooooooooooooooo oooooooo oooooooo ng quoted  \aconstraint"  TO simple_constraint_name;
--INSERT INTO simple_table_name (part_key, other_col) VALUES (1, 1) ON CONFLICT ON CONSTRAINT  simple_constraint_name DO NOTHING RETURNING *;

SET search_path TO single_node;
DROP SCHEMA  "Quoed.Schema" CASCADE;

-- test partitioned index creation with long name
CREATE TABLE test_index_creation1
(
    tenant_id integer NOT NULL,
    timeperiod timestamp without time zone NOT NULL,
    field1 integer NOT NULL,
    inserted_utc timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, timeperiod)
) PARTITION BY RANGE (timeperiod);

CREATE TABLE test_index_creation1_p2020_09_26
PARTITION OF test_index_creation1 FOR VALUES FROM ('2020-09-26 00:00:00') TO ('2020-09-27 00:00:00');
CREATE TABLE test_index_creation1_p2020_09_27
PARTITION OF test_index_creation1 FOR VALUES FROM ('2020-09-27 00:00:00') TO ('2020-09-28 00:00:00');

select create_distributed_table('test_index_creation1', 'tenant_id');

-- should be able to create indexes with INCLUDE/WHERE
CREATE INDEX ix_test_index_creation5 ON test_index_creation1
	USING btree(tenant_id, timeperiod)
	INCLUDE (field1) WHERE (tenant_id = 100);

-- test if indexes are created
SELECT 1 AS created WHERE EXISTS(SELECT * FROM pg_indexes WHERE indexname LIKE '%test_index_creation%');

-- test citus size functions in transaction with modification
CREATE TABLE test_citus_size_func (a int);
SELECT create_distributed_table('test_citus_size_func', 'a');
INSERT INTO test_citus_size_func VALUES(1), (2);

BEGIN;
	-- DDL with citus_table_size
	ALTER TABLE test_citus_size_func ADD COLUMN newcol INT;
	SELECT citus_table_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- DDL with citus_relation_size
	ALTER TABLE test_citus_size_func ADD COLUMN newcol INT;
	SELECT citus_relation_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- DDL with citus_total_relation_size
	ALTER TABLE test_citus_size_func ADD COLUMN newcol INT;
	SELECT citus_total_relation_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- single shard insert with citus_table_size
	INSERT INTO test_citus_size_func VALUES (3);
	SELECT citus_table_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- multi shard modification with citus_table_size
	INSERT INTO test_citus_size_func  SELECT * FROM  test_citus_size_func;
	SELECT citus_table_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- single shard insert with citus_relation_size
	INSERT INTO test_citus_size_func VALUES (3);
	SELECT citus_relation_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- multi shard modification with citus_relation_size
	INSERT INTO test_citus_size_func  SELECT * FROM  test_citus_size_func;
	SELECT citus_relation_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- single shard insert with citus_total_relation_size
	INSERT INTO test_citus_size_func VALUES (3);
	SELECT citus_total_relation_size('test_citus_size_func');
ROLLBACK;

BEGIN;
	-- multi shard modification with citus_total_relation_size
	INSERT INTO test_citus_size_func  SELECT * FROM  test_citus_size_func;
	SELECT citus_total_relation_size('test_citus_size_func');
ROLLBACK;

-- we should be able to limit intermediate results
BEGIN;
       SET LOCAL citus.max_intermediate_result_size TO 0;
       WITH cte_1 AS (SELECT * FROM test OFFSET 0) SELECT * FROM cte_1;
ROLLBACK;

-- the first cte (cte_1) does not exceed the limit
-- but the second (cte_2) exceeds, so we error out
BEGIN;
	SET LOCAL citus.max_intermediate_result_size TO '1kB';
	INSERT INTO  test SELECT i,i from generate_series(0,1000)i;

	-- only pulls 1 row, should not hit the limit
	WITH cte_1 AS (SELECT * FROM test LIMIT 1) SELECT count(*) FROM cte_1;

	-- cte_1 only pulls 1 row, but cte_2 all rows
	WITH cte_1 AS (SELECT * FROM test LIMIT 1),
	     cte_2 AS (SELECT * FROM test OFFSET 0)
	SELECT count(*) FROM cte_1, cte_2;
ROLLBACK;

-- single shard and multi-shard delete
-- inside a transaction block
BEGIN;
	DELETE FROM test WHERE y = 5;
	INSERT INTO test VALUES (4, 5);

	DELETE FROM test WHERE x = 1;
	INSERT INTO test VALUES (1, 2);
COMMIT;

CREATE INDEX single_node_i1 ON test(x);
CREATE INDEX single_node_i2 ON test(x,y);
REINDEX SCHEMA single_node;

-- PG 11 does not support CONCURRENTLY
-- and we do not want to add a new output
-- file just for that. Enable the test
-- once we remove PG_VERSION_11
--REINDEX SCHEMA CONCURRENTLY single_node;

-- keep one of the indexes
-- drop w/wout tx blocks
BEGIN;
	DROP INDEX single_node_i2;
ROLLBACK;
DROP INDEX single_node_i2;

-- change the schema w/wout TX block
BEGIN;
	ALTER TABLE public.another_schema_table SET SCHEMA single_node;
ROLLBACK;
ALTER TABLE public.another_schema_table SET SCHEMA single_node;

BEGIN;
	TRUNCATE test;
	SELECT * FROM test;
ROLLBACK;

VACUUM test;
VACUUM test, test_2;
VACUUM ref, test;
VACUUM ANALYZE test(x);
ANALYZE ref;
ANALYZE test_2;
VACUUM local;
VACUUM local, ref, test, test_2;
VACUUM FULL test, ref;

BEGIN;
	ALTER TABLE test ADD COLUMN z INT DEFAULT 66;
	SELECT count(*) FROM test WHERE z = 66;
ROLLBACK;

-- explain analyze should work on a single node
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE)
	SELECT * FROM test;

-- common utility command
SELECT pg_size_pretty(citus_relation_size('test'::regclass));

-- basic view queries
CREATE VIEW single_node_view AS
	SELECT count(*) as cnt FROM test t1 JOIN test t2 USING (x);
SELECT * FROM single_node_view;
SELECT * FROM single_node_view, test WHERE test.x = single_node_view.cnt;

-- copy in/out
BEGIN;
	COPY test(x) FROM PROGRAM 'seq 32';
	SELECT count(*) FROM test;
	COPY (SELECT count(DISTINCT x) FROM test) TO STDOUT;
	INSERT INTO test SELECT i,i FROM generate_series(0,100)i;
ROLLBACK;

-- alter table inside a tx block
BEGIN;
	ALTER TABLE test ADD COLUMN z single_node.new_type;

	INSERT INTO test VALUES (99, 100, (1, 'onder')::new_type) RETURNING *;
ROLLBACK;

-- prepared statements with custom types
PREPARE single_node_prepare_p1(int, int, new_type) AS
	INSERT INTO test_2 VALUES ($1, $2, $3);

EXECUTE single_node_prepare_p1(1, 1, (95, 'citus9.5')::new_type);
EXECUTE single_node_prepare_p1(2 ,2, (94, 'citus9.4')::new_type);
EXECUTE single_node_prepare_p1(3 ,2, (93, 'citus9.3')::new_type);
EXECUTE single_node_prepare_p1(4 ,2, (92, 'citus9.2')::new_type);
EXECUTE single_node_prepare_p1(5 ,2, (91, 'citus9.1')::new_type);
EXECUTE single_node_prepare_p1(6 ,2, (90, 'citus9.0')::new_type);

PREPARE use_local_query_cache(int) AS SELECT count(*) FROM test_2 WHERE x =  $1;

EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);

SET client_min_messages TO DEBUG2;
-- the 6th execution will go through the planner
-- the 7th execution will skip the planner as it uses the cache
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);

RESET client_min_messages;


-- partitioned table should be fine, adding for completeness
CREATE TABLE collections_list (
	key bigint,
	ts timestamptz DEFAULT now(),
	collection_id integer,
	value numeric,
	PRIMARY KEY(key, collection_id)
) PARTITION BY LIST (collection_id );

SELECT create_distributed_table('collections_list', 'key');
CREATE TABLE collections_list_0
	PARTITION OF collections_list (key, ts, collection_id, value)
	FOR VALUES IN ( 0 );
CREATE TABLE collections_list_1
	PARTITION OF collections_list (key, ts, collection_id, value)
	FOR VALUES IN ( 1 );

INSERT INTO collections_list SELECT i, '2011-01-01', i % 2, i * i FROM generate_series(0, 100) i;
SELECT count(*) FROM collections_list WHERE key < 10 AND collection_id = 1;
SELECT count(*) FROM collections_list_0 WHERE key < 10 AND collection_id = 1;
SELECT count(*) FROM collections_list_1 WHERE key = 11;
ALTER TABLE collections_list DROP COLUMN ts;
SELECT * FROM collections_list, collections_list_0 WHERE collections_list.key=collections_list_0.key  ORDER BY 1 DESC,2 DESC,3 DESC,4 DESC LIMIT 1;

-- make sure that parallel accesses are good
SET citus.force_max_query_parallelization TO ON;
SELECT * FROM test_2 ORDER BY 1 DESC;
DELETE FROM test_2 WHERE y = 1000 RETURNING *;
RESET citus.force_max_query_parallelization ;

BEGIN;
	INSERT INTO test_2 VALUES (7 ,2, (83, 'citus8.3')::new_type);
	SAVEPOINT s1;
	INSERT INTO test_2 VALUES (9 ,1, (82, 'citus8.2')::new_type);
	SAVEPOINT s2;
	ROLLBACK TO SAVEPOINT s1;
	SELECT * FROM test_2 WHERE z = (83, 'citus8.3')::new_type OR z = (82, 'citus8.2')::new_type;
	RELEASE SAVEPOINT s1;
COMMIT;

SELECT * FROM test_2 WHERE z = (83, 'citus8.3')::new_type OR z = (82, 'citus8.2')::new_type;

-- final query is only intermediate result

-- we want PG 11/12/13 behave consistently, the CTEs should be MATERIALIZED
SET citus.enable_cte_inlining TO FALSE;
WITH cte_1 AS (SELECT * FROM test_2) SELECT * FROM cte_1 ORDER BY 1,2;

-- final query is router query
WITH cte_1 AS (SELECT * FROM test_2) SELECT * FROM cte_1, test_2 WHERE  test_2.x = cte_1.x AND test_2.x = 7 ORDER BY 1,2;

-- final query is a distributed query
WITH cte_1 AS (SELECT * FROM test_2) SELECT * FROM cte_1, test_2 WHERE  test_2.x = cte_1.x AND test_2.y != 2 ORDER BY 1,2;

-- query pushdown should work
SELECT
	*
FROM
	(SELECT x, count(*) FROM test_2 GROUP BY x) as foo,
	(SELECT x, count(*) FROM test_2 GROUP BY x) as bar
WHERE
	foo.x = bar.x
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
LIMIT 1;

-- make sure that foreign keys work fine
ALTER TABLE test_2 ADD CONSTRAINT first_pkey PRIMARY KEY (x);
ALTER TABLE test ADD CONSTRAINT foreign_key FOREIGN KEY (x) REFERENCES test_2(x) ON DELETE CASCADE;

-- show that delete on test_2 cascades to test
SELECT * FROM test WHERE x = 5;
DELETE FROM test_2 WHERE x = 5;
SELECT * FROM test WHERE x = 5;
INSERT INTO test_2 VALUES (5 ,2, (91, 'citus9.1')::new_type);
INSERT INTO test VALUES (5, 6);

INSERT INTO ref VALUES (1, 2), (5, 6), (7, 8);
SELECT count(*) FROM ref;
SELECT * FROM ref ORDER BY a;
SELECT * FROM test, ref WHERE x = a ORDER BY x;

INSERT INTO local VALUES (1, 2), (3, 4), (7, 8);
SELECT count(*) FROM local;
SELECT * FROM local ORDER BY c;
SELECT * FROM ref, local WHERE a = c ORDER BY a;

-- Check repartion joins are supported
SET citus.enable_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET search_path TO public;
SET citus.enable_single_hash_repartition_joins TO OFF;
SELECT * FROM single_node.test t1, single_node.test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM single_node.test t1, single_node.test t2 WHERE t1.x = t2.y ORDER BY t1.x;
SET search_path TO single_node;

SET citus.task_assignment_policy TO 'round-robin';
SET citus.enable_single_hash_repartition_joins TO ON;
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET citus.task_assignment_policy TO 'greedy';
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

SET citus.task_assignment_policy TO 'first-replica';
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x;

RESET citus.enable_repartition_joins;
RESET citus.enable_single_hash_repartition_joins;

-- INSERT SELECT router
BEGIN;
INSERT INTO test(x, y) SELECT x, y FROM test WHERE x = 1;
SELECT count(*) from test;
ROLLBACK;


-- INSERT SELECT pushdown
BEGIN;
INSERT INTO test(x, y) SELECT x, y FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT analytical query
BEGIN;
INSERT INTO test(x, y) SELECT count(x), max(y) FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT repartition
BEGIN;
INSERT INTO test(x, y) SELECT y, x FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from reference table into distributed
BEGIN;
INSERT INTO test(x, y) SELECT a, b FROM ref;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from local table into distributed
BEGIN;
INSERT INTO test(x, y) SELECT c, d FROM local;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT x, y FROM test;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT c, d FROM local;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT x, y FROM test;
SELECT count(*) from local;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT a, b FROM ref;
SELECT count(*) from local;
ROLLBACK;

-- Confirm that dummy placements work
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
-- Confirm that they work with round-robin task assignment policy
SET citus.task_assignment_policy TO 'round-robin';
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);
RESET citus.task_assignment_policy;

SELECT count(*) FROM test;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT x, y FROM test;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT c, d FROM local;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT x, y FROM test;
SELECT count(*) from local;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT a, b FROM ref;
SELECT count(*) from local;
ROLLBACK;

-- query fails on the shards should be handled
-- nicely
SELECT x/0 FROM test;

-- Add "fake" pg_dist_transaction records and run recovery
-- to show that it is recovered

-- Temporarily disable automatic 2PC recovery
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

BEGIN;
CREATE TABLE should_commit (value int);
PREPARE TRANSACTION 'citus_0_should_commit';

-- zero is the coordinator's group id, so we can hard code it
INSERT INTO pg_dist_transaction VALUES (0, 'citus_0_should_commit');
SELECT recover_prepared_transactions();

-- the table should be seen
SELECT * FROM should_commit;

-- set the original back
ALTER SYSTEM RESET citus.recover_2pc_interval;
SELECT pg_reload_conf();

RESET citus.task_executor_type;

-- make sure undistribute table works fine
ALTER TABLE test DROP CONSTRAINT foreign_key;
SELECT undistribute_table('test_2');
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'test_2'::regclass;

CREATE PROCEDURE call_delegation(x int) LANGUAGE plpgsql AS $$
BEGIN
	 INSERT INTO test (x) VALUES ($1);
END;$$;
SELECT * FROM pg_dist_node;
SELECT create_distributed_function('call_delegation(int)', '$1', 'test');

CREATE FUNCTION function_delegation(int) RETURNS void AS $$
BEGIN
UPDATE test SET y = y + 1 WHERE x <  $1;
END;
$$ LANGUAGE plpgsql;
SELECT create_distributed_function('function_delegation(int)', '$1', 'test');

SET client_min_messages TO DEBUG1;
CALL call_delegation(1);
SELECT function_delegation(1);

SET client_min_messages TO WARNING;
DROP TABLE test CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.get_all_active_client_backend_count()
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'citus', $$get_all_active_client_backend_count$$;

-- set the cached connections to zero
-- and execute a distributed query so that
-- we end up with zero cached connections afterwards
ALTER SYSTEM SET citus.max_cached_conns_per_worker TO 0;
SELECT pg_reload_conf();

-- disable deadlock detection and re-trigger 2PC recovery
-- once more when citus.max_cached_conns_per_worker is zero
-- so that we can be sure that the connections established for
-- maintanince daemon is closed properly.
-- this is to prevent random failures in the tests (otherwise, we
-- might see connections established for this operations)
ALTER SYSTEM SET citus.distributed_deadlock_detection_factor TO -1;
ALTER SYSTEM SET citus.recover_2pc_interval TO '1ms';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

-- now that last 2PC recovery is done, we're good to disable it
ALTER SYSTEM SET citus.recover_2pc_interval TO '-1';
SELECT pg_reload_conf();


\c - - - :master_port
-- sometimes Postgres is a little slow to terminate the backends
-- even if PGFinish is sent. So, to prevent any flaky tests, sleep
SELECT pg_sleep(0.1);
-- since max_cached_conns_per_worker == 0 at this point, the
-- backend(s) that execute on the shards will be terminated
-- so show that there is only a single client backend,
-- which is actually the backend that executes here
SET search_path TO single_node;
SELECT count(*) from should_commit;
SELECT pg_catalog.get_all_active_client_backend_count();
BEGIN;
	SET LOCAL citus.shard_count TO 32;
	SET LOCAL citus.force_max_query_parallelization TO ON;
	SET LOCAL citus.enable_local_execution TO false;

	CREATE TABLE test (a int);
	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('test', 'a');
	SELECT count(*) FROM test;

	-- now, we should have additional 32 connections
	SELECT pg_catalog.get_all_active_client_backend_count();
ROLLBACK;


\c - - - :master_port
SET search_path TO single_node;


-- simulate that even if there is no connection slots
-- to connect, Citus can switch to local execution
SET citus.force_max_query_parallelization TO false;
SET citus.log_remote_commands TO ON;
ALTER SYSTEM SET citus.local_shared_pool_size TO -1;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SET citus.executor_slow_start_interval TO 10;
SELECT count(*) from another_schema_table;

UPDATE another_schema_table SET b = b;

-- INSERT .. SELECT pushdown and INSERT .. SELECT via repartitioning
-- not that we ignore INSERT .. SELECT via coordinator as it relies on
-- COPY command
INSERT INTO another_schema_table SELECT * FROM another_schema_table;
INSERT INTO another_schema_table SELECT b::int, a::int FROM another_schema_table;

-- multi-row INSERTs
INSERT INTO another_schema_table VALUES (1,1), (2,2), (3,3), (4,4), (5,5),(6,6),(7,7);

-- intermediate results
WITH cte_1 AS (SELECT * FROM another_schema_table LIMIT 1000)
	SELECT count(*) FROM cte_1;

-- if the local execution is disabled, we cannot failover to
-- local execution and the queries would fail
SET citus.enable_local_execution TO  false;
SELECT count(*) from another_schema_table;
UPDATE another_schema_table SET b = b;
INSERT INTO another_schema_table SELECT * FROM another_schema_table;
INSERT INTO another_schema_table SELECT b::int, a::int FROM another_schema_table;
WITH cte_1 AS (SELECT * FROM another_schema_table LIMIT 1000)
	SELECT count(*) FROM cte_1;

INSERT INTO another_schema_table VALUES (1,1), (2,2), (3,3), (4,4), (5,5),(6,6),(7,7);

-- set the values to originals back
ALTER SYSTEM RESET citus.max_cached_conns_per_worker;
ALTER SYSTEM RESET citus.distributed_deadlock_detection_factor;
ALTER SYSTEM RESET citus.recover_2pc_interval;
ALTER SYSTEM RESET citus.distributed_deadlock_detection_factor;
ALTER SYSTEM RESET citus.local_shared_pool_size;
SELECT pg_reload_conf();

-- suppress notices
SET client_min_messages TO error;

-- cannot remove coordinator since a reference table exists on coordinator and no other worker nodes are added
SELECT 1 FROM master_remove_node('localhost', :master_port);

-- Cleanup
DROP SCHEMA single_node CASCADE;
-- Remove the coordinator again
SELECT 1 FROM master_remove_node('localhost', :master_port);
-- restart nodeid sequence so that multi_cluster_management still has the same
-- nodeids
ALTER SEQUENCE pg_dist_node_nodeid_seq RESTART 1;
