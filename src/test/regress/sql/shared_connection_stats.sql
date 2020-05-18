CREATE SCHEMA shared_connection_stats;
SET search_path TO shared_connection_stats;

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
	SELECT count(*), pg_sleep(0.1) FROM test;
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
COPY test FROM STDIN;
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
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

-- now, show that COPY doesn't open more connections than the shared_pool_size

-- now, decrease the shared pool size, and show that COPY doesn't exceed that
ALTER SYSTEM SET citus.max_shared_pool_size TO 3;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

BEGIN;

COPY test FROM STDIN;
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
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

-- in case other tests relies on these setting, reset them
ALTER SYSTEM RESET citus.distributed_deadlock_detection_factor;
ALTER SYSTEM RESET citus.recover_2pc_interval;
ALTER SYSTEM RESET citus.max_cached_conns_per_worker;
SELECT pg_reload_conf();

DROP SCHEMA shared_connection_stats CASCADE;
