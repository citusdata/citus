CREATE SCHEMA shared_connection_stats;
SET search_path TO shared_connection_stats;

SET citus.next_shard_id TO 14000000;

-- returns the reserved connections per backend
-- given that the code aggresively cleans up reserved connections
-- this function returns empty set in all the tests
-- In fact, we're testing that no reserved connections remain
CREATE OR REPLACE FUNCTION citus_reserved_connection_stats(
        OUT hostname text,
        OUT port int,
        OUT database_name text,
        OUT used_reserved_connection bool)
RETURNS SETOF RECORD
LANGUAGE C STRICT
AS 'citus', $$citus_reserved_connection_stats$$;

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
ALTER SYSTEM SET citus.recover_2pc_interval TO '1h';
SELECT pg_reload_conf();

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
CREATE TABLE test (a int);
SELECT create_distributed_table('test', 'a');
INSERT INTO test SELECT i FROM generate_series(0,100)i;

-- show that no connections are cached
SELECT
	connection_count_to_node
FROM
	citus_remote_connection_stats()
WHERE
	port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
	database_name = 'regression'
ORDER BY
	hostname, port;

-- single shard queries require single connection per node
BEGIN;
	SELECT count(*) FROM test WHERE a = 1;
	SELECT count(*) FROM test WHERE a = 2;
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
COMMIT;

-- show that no connections are cached
SELECT
	connection_count_to_node
FROM
	citus_remote_connection_stats()
WHERE
	port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
	database_name = 'regression'
ORDER BY
	hostname, port;

-- executor is only allowed to establish a single connection per node
BEGIN;
	SET LOCAL citus.max_adaptive_executor_pool_size TO 1;
	SELECT count(*) FROM test;
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
COMMIT;

-- show that no connections are cached
SELECT
	connection_count_to_node
FROM
	citus_remote_connection_stats()
WHERE
	port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
	database_name = 'regression'
ORDER BY
	hostname, port;

-- sequential mode is allowed to establish a single connection per node
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM test;
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
COMMIT;

-- show that no connections are cached
SELECT
	connection_count_to_node
FROM
	citus_remote_connection_stats()
WHERE
	port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
	database_name = 'regression'
ORDER BY
	hostname, port;

-- now, decrease the shared pool size, and still force
-- one connection per placement
ALTER SYSTEM SET citus.max_shared_pool_size TO 5;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

BEGIN;
	SET LOCAL citus.node_connection_timeout TO 1000;
	SET LOCAL citus.force_max_query_parallelization TO ON;
	SELECT count(*) FROM test;
COMMIT;

-- pg_sleep forces almost 1 connection per placement
-- now, some of the optional connections would be skipped,
-- and only 5 connections are used per node
BEGIN;
	SET LOCAL citus.max_adaptive_executor_pool_size TO 16;
	with cte_1 as (select pg_sleep(0.1) is null, a from test) SELECT a from cte_1 ORDER By 1 LIMIT 1;
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
COMMIT;


SHOW citus.max_shared_pool_size;

-- by default max_shared_pool_size equals to max_connections;
ALTER SYSTEM RESET citus.max_shared_pool_size;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

SHOW citus.max_shared_pool_size;
SHOW max_connections;

-- now, each node gets 16 connections as we force 1 connection per placement
BEGIN;
	SET LOCAL citus.force_max_query_parallelization TO ON;
	SELECT count(*) FROM test;
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
COMMIT;

BEGIN;
	-- now allow at most 1 connection, and ensure that intermediate
	-- results don't require any extra connections
	SET LOCAL citus.max_adaptive_executor_pool_size TO 1;
	SET LOCAL citus.task_assignment_policy TO "round-robin";
	SELECT cnt FROM (SELECT count(*) as cnt, random() FROM test LIMIT 1) as foo;

	-- queries with intermediate results don't use any extra connections
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
COMMIT;

BEGIN;
	-- now allow at most 2 connections for COPY
	SET LOCAL citus.max_adaptive_executor_pool_size TO 2;
    COPY test FROM PROGRAM 'seq 32';

	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;

-- now, show that COPY doesn't open more connections than the shared_pool_size

-- now, decrease the shared pool size, and show that COPY doesn't exceed that
ALTER SYSTEM SET citus.max_shared_pool_size TO 3;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

BEGIN;

COPY test FROM PROGRAM 'seq 32';

	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;

BEGIN;
	-- in this test, we trigger touching only one of the workers
        -- the first copy touches 3 shards
	COPY test FROM STDIN;
2
4
5
\.

	-- we see one worker has 3 connections, the other is 1, which is not
        -- an already established connection, but a reservation
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;

-- in this second COPY, we access the same node but different shards
-- so we test the case where the second COPY cannot get any new connections
-- due to adaptive connection management, and can still continue
COPY test FROM  STDIN;
6
8
9
\.
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;


BEGIN;
	-- in this test, we trigger touching only one of the workers
        -- the first copy touches 3 shards
	SELECT count(*) FROM test WHERE a IN (2,4,5);

	-- we see one worker has 3 connections, the other is 1, which is not
        -- an already established connection, but a reservation
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;

-- in this second COPY, we access the same node but different shards
-- so we test the case where the second COPY cannot get any new connections
-- due to adaptive connection management, and can still continue
COPY test FROM  STDIN;
6
8
9
\.
	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;


BEGIN;
	-- when COPY is used with _max_query_parallelization
	-- it ignores the shared pool size
	SET LOCAL citus.force_max_query_parallelization TO ON;
	SET LOCAL citus.max_adaptive_executor_pool_size TO 16;
	COPY test FROM PROGRAM 'seq 32';

	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;

-- INSERT SELECT with RETURNING/ON CONFLICT clauses does not honor shared_pool_size
-- in underlying COPY commands
BEGIN;
	SELECT pg_sleep(0.1);
	-- make sure that we hit at least 4 shards per node, where 20 rows
	-- is enough
	INSERT INTO test SELECT i FROM generate_series(0,20) i RETURNING *;

	SELECT
		connection_count_to_node > current_setting('citus.max_shared_pool_size')::int
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;

-- COPY operations to range partitioned tables will honor max_shared_pool_size
-- as we use a single connection to each worker
CREATE TABLE range_table(a int);
SELECT create_distributed_table('range_table', 'a', 'range');
CALL public.create_range_partitioned_shards('range_table',
                                            '{0,25,50,76}',
                                            '{24,49,75,200}');
BEGIN;
	SELECT pg_sleep(0.1);
	COPY range_table FROM PROGRAM 'seq 32';

	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;


-- COPY operations to reference tables will use one connection per worker
-- so we will always honor max_shared_pool_size.
CREATE TABLE ref_table(a int);
SELECT create_reference_table('ref_table');

BEGIN;
	SELECT pg_sleep(0.1);
	COPY ref_table FROM PROGRAM 'seq 32';

	SELECT
		connection_count_to_node
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
ROLLBACK;

-- reset max_shared_pool_size to default
ALTER SYSTEM RESET citus.max_shared_pool_size;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

-- now show that when max_cached_conns_per_worker > 1
-- Citus forces the first execution to open at least 2
-- connections that are cached. Later, that 2 cached
-- connections are user
BEGIN;
	SET LOCAL citus.max_cached_conns_per_worker TO 2;
	SELECT count(*) FROM test;
	SELECT
		connection_count_to_node >= 2
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
	SELECT count(*) FROM test;
	SELECT
		connection_count_to_node >= 2
	FROM
		citus_remote_connection_stats()
	WHERE
		port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
		database_name = 'regression'
	ORDER BY
		hostname, port;
COMMIT;

-- we should not have any reserved connection
-- as all of them have already been either used
-- or cleaned up
SELECT * FROM citus_reserved_connection_stats();

-- reconnect to get rid of cached connections
\c - - - :master_port
SET search_path TO shared_connection_stats;

BEGIN;
	INSERT INTO test SELECT i FROM generate_series(0,10)i;

	-- after COPY finishes, citus should see the used
	-- reserved connections
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;
ROLLBACK;


BEGIN;
	-- even if we hit a single shard, all the other reserved
	-- connections should be cleaned-up because we do not
 	-- reserve for the second call as we have the cached
	-- connections
	INSERT INTO test SELECT 1 FROM generate_series(0,100)i;
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;
ROLLBACK;

BEGIN;
	TRUNCATE test;
	CREATE UNIQUE INDEX test_unique_index ON test(a);

	-- even if we hit a single shard and later fail, all the
	-- other reserved connections should be cleaned-up
	INSERT INTO test SELECT 1 FROM generate_series(0,10)i;
ROLLBACK;
SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;

BEGIN;
	-- hits a single shard
	INSERT INTO test SELECT 1 FROM generate_series(0,10)i;

	-- if COPY hits a single shard, we should have reserved connections
	-- to  the other nodes
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;

	-- we should be able to see this again if the query hits
	-- the same shard
	INSERT INTO test SELECT 1 FROM generate_series(0,10)i;
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;

	-- but when the  query hits the other shard(s), we should
	-- see that all the  reserved connections are used
	INSERT INTO test SELECT i FROM generate_series(0,10)i;
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;
ROLLBACK;

-- at the end of the transaction, all should be cleared
SELECT * FROM citus_reserved_connection_stats();

BEGIN;
	SELECT count(*) FROM test;

	-- the above command used at least one connection per node
	-- so the next commands would not need any reserved connections
	INSERT INTO test SELECT 1 FROM generate_series(0,10)i;
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;
	INSERT INTO test SELECT i FROM generate_series(0,10)i;
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;
COMMIT;

-- checkout the reserved connections with cached connections
ALTER SYSTEM SET citus.max_cached_conns_per_worker TO 1;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

-- cache connections to the nodes
SET citus.force_max_query_parallelization TO ON;
SELECT count(*) FROM test;
BEGIN;
	-- we should not have any reserved connections
	-- because we already have available connections
	COPY test FROM PROGRAM 'seq 32';
	SELECT * FROM citus_reserved_connection_stats() ORDER BY 1,2;
COMMIT;

-- should close all connections
SET citus.max_cached_connection_lifetime TO '0s';
SELECT count(*) FROM test;

-- show that no connections are cached
SELECT
	connection_count_to_node
FROM
	citus_remote_connection_stats()
WHERE
	port IN (SELECT node_port FROM master_get_active_worker_nodes()) AND
	database_name = 'regression'
ORDER BY
	hostname, port;

-- in case other tests relies on these setting, reset them
ALTER SYSTEM RESET citus.distributed_deadlock_detection_factor;
ALTER SYSTEM RESET citus.recover_2pc_interval;
ALTER SYSTEM RESET citus.max_cached_conns_per_worker;
SELECT pg_reload_conf();

BEGIN;
SET LOCAL client_min_messages TO WARNING;
DROP SCHEMA shared_connection_stats CASCADE;
COMMIT;
