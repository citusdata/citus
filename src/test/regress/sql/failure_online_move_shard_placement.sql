--
-- failure_online_move_shard_placement
--

-- The tests cover moving shard placements using logical replication.

CREATE SCHEMA IF NOT EXISTS move_shard;
SET SEARCH_PATH = move_shard;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 100;
SET citus.shard_replication_factor TO 1;
SET citus.max_adaptive_executor_pool_size TO 1;
SELECT pg_backend_pid() as pid \gset

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE t(id int PRIMARY KEY, int_data int, data text);
CREATE INDEX index_failure ON t(id);
SELECT create_distributed_table('t', 'id');

CREATE VIEW shards_in_workers AS
SELECT shardid,
       (CASE WHEN nodeport = :worker_1_port THEN 'worker1' ELSE 'worker2' END) AS worker
FROM pg_dist_placement NATURAL JOIN pg_dist_node
WHERE shardstate != 4
ORDER BY 1,2 ASC;

-- Insert some data
INSERT INTO t SELECT x, x+1, MD5(random()::text) FROM generate_series(1,100000) AS f(x);

-- Initial shard placements
SELECT * FROM shards_in_workers;

-- failure on sanity checks
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS move_shard.t CASCADE").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cancellation on sanity checks
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS move_shard.t CASCADE").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure on move_shard table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE move_shard.t").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cancellation on move_shard table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE move_shard.t").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure during COPY phase
SELECT citus.mitmproxy('conn.onQuery(query="COPY").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cancelation during COPY phase
SELECT citus.mitmproxy('conn.onQuery(query="COPY").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure when enabling the subscriptions
SELECT citus.mitmproxy('conn.onQuery(query="^ALTER SUBSCRIPTION .* ENABLE").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure when enabling the subscriptions
-- This test can be enabled again once this postgres bug is fixed:
-- https://www.postgresql.org/message-id/flat/HE1PR8303MB0075BF78AF1BE904050DA16BF7729%40HE1PR8303MB0075.EURPRD83.prod.outlook.com
-- SELECT citus.mitmproxy('conn.onQuery(query="^ALTER SUBSCRIPTION .* ENABLE").cancel(' || :pid || ')');
-- SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure on dropping subscription
SELECT citus.mitmproxy('conn.onQuery(query="^ALTER SUBSCRIPTION .* (ENABLE|DISABLE)").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

-- cancel on dropping subscription
SELECT citus.mitmproxy('conn.onQuery(query="^ALTER SUBSCRIPTION .* (ENABLE|DISABLE)").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

-- try again
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);
SELECT master_move_shard_placement(101, 'localhost', :worker_2_proxy_port, 'localhost', :worker_1_port);

-- failure on polling last write-ahead log location reported to origin WAL sender
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT min\(latest_end_lsn").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cancellation on polling last write-ahead log location reported to origin WAL sender
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT min\(latest_end_lsn").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure on disabling subscription (right before dropping it)
SELECT citus.mitmproxy('conn.onQuery(query="^ALTER SUBSCRIPTION .* DISABLE").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);
-- should succeed with warnings (subscription not dropped)
-- move the shard back
SELECT master_move_shard_placement(101, 'localhost', :worker_2_proxy_port, 'localhost', :worker_1_port);

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

-- failure on setting lock_timeout (right before dropping subscriptions & replication slots)
SELECT citus.mitmproxy('conn.onQuery(query="^SET LOCAL lock_timeout").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);
-- should succeed with warnings (objects not dropped)
-- move the shard back
SELECT master_move_shard_placement(101, 'localhost', :worker_2_proxy_port, 'localhost', :worker_1_port);

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

-- cancellation on disabling subscription (right before dropping it)
SELECT citus.mitmproxy('conn.onQuery(query="^ALTER SUBSCRIPTION .* DISABLE").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

-- disable maintenance daemon cleanup, to prevent the flaky test
ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();
SET citus.next_operation_id TO 777;

-- failure on dropping subscription
SELECT citus.mitmproxy('conn.onQuery(query="^DROP SUBSCRIPTION").killall()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

SELECT citus.mitmproxy('conn.allow()');
-- first, manually drop the subscsription object. But the record for it will remain on pg_dist_cleanup
-- we expect the drop query will succeed on only one node
SELECT 1 FROM run_command_on_workers('DROP SUBSCRIPTION citus_shard_move_subscription_10_777');

-- reset back
ALTER SYSTEM RESET citus.defer_shard_delete_interval;
SELECT pg_reload_conf();

-- cleanup leftovers
-- then, verify we don't see any error for already dropped subscription
SELECT public.wait_for_resource_cleanup();

-- cancellation on dropping subscription
SELECT citus.mitmproxy('conn.onQuery(query="^DROP SUBSCRIPTION").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure on creating the primary key
SELECT citus.mitmproxy('conn.onQuery(query="t_pkey").kill()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cancellation on creating the primary key
SELECT citus.mitmproxy('conn.onQuery(query="t_pkey").cancel(' || :pid || ')');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

-- failure on create index
SELECT citus.mitmproxy('conn.matches(b"CREATE INDEX").killall()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

-- lets create few more indexes and fail with both
-- parallel mode and sequential mode
CREATE INDEX index_failure_2 ON t(id);
CREATE INDEX index_failure_3 ON t(id);
CREATE INDEX index_failure_4 ON t(id);
CREATE INDEX index_failure_5 ON t(id);

-- failure on the third create index
ALTER SYSTEM SET citus.max_adaptive_executor_pool_size TO 1;
SELECT pg_reload_conf();

SELECT citus.mitmproxy('conn.matches(b"CREATE INDEX").killall()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- failure on parallel create index
ALTER SYSTEM RESET citus.max_adaptive_executor_pool_size;
SELECT pg_reload_conf();
SELECT citus.mitmproxy('conn.matches(b"CREATE INDEX").killall()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);

-- Verify that the shard is not moved and the number of rows are still 100k
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM shards_in_workers;
SELECT count(*) FROM t;

-- Verify that shard can be moved after a temporary failure
SELECT citus.mitmproxy('conn.allow()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);
SELECT * FROM shards_in_workers;
SELECT count(*) FROM t;

-- cleanup leftovers
SELECT citus.mitmproxy('conn.allow()');
SELECT public.wait_for_resource_cleanup();

DROP SCHEMA move_shard CASCADE ;
