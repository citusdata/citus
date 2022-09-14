--
-- failure_on_create_subscription
--

-- Since the result of these tests depends on the success of background
-- process that creating the replication slot on the publisher. These
-- tests are separated.

CREATE SCHEMA IF NOT EXISTS move_shard;
SET SEARCH_PATH = move_shard;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 100;
SET citus.shard_replication_factor TO 1;
SELECT pg_backend_pid() as pid \gset

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE t(id int PRIMARY KEY, int_data int, data text);

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

-- failure on creating the subscription
SELECT citus.mitmproxy('conn.onQuery(query="CREATE SUBSCRIPTION").kill()');
-- Log minimal messages to avoid printing the error message and make the test flaky
SET client_min_messages TO ERROR;
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);
RESET client_min_messages;

-- Verify that the shard is not moved and the number of rows are still 100k
SELECT * FROM shards_in_workers;
SELECT count(*) FROM t;

-- Verify that shard can be moved after a temporary failure
SELECT citus.mitmproxy('conn.allow()');
SELECT master_move_shard_placement(101, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port);
SELECT * FROM shards_in_workers;
SELECT count(*) FROM t;

DROP SCHEMA move_shard CASCADE ;
