--
-- failure_offline_move_shard_placement
--

-- The tests cover moving shard placements without using logical replication.

CREATE SCHEMA IF NOT EXISTS move_shard_offline;
SET SEARCH_PATH = move_shard_offline;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 200;
SET citus.shard_replication_factor TO 1;
SELECT pg_backend_pid() as pid \gset

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE t(id int PRIMARY KEY, int_data int);

SELECT create_distributed_table('t', 'id');

CREATE VIEW shards_in_workers AS
  SELECT shardid,
         (CASE WHEN nodeport = :worker_1_port THEN 'worker1' ELSE 'worker2' END) AS worker
  FROM pg_dist_placement NATURAL JOIN pg_dist_node
  WHERE shardstate != 4
  ORDER BY 1,2 ASC;

CREATE VIEW indices_on_shard_201 AS
  SELECT * FROM run_command_on_workers( $cmd$
    SELECT CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END
    FROM pg_index WHERE indrelid = 'move_shard_offline.t_201'::regclass
  $cmd$);

CREATE VIEW find_index_for_shard_201_in_workers AS
  SELECT CASE nodeport WHEN :worker_1_port THEN 'worker1' ELSE 'worker2' END
  FROM indices_on_shard_201 WHERE result = 't';

-- Insert some data
INSERT INTO t SELECT x, x+1 FROM generate_series(1,100) AS f(x);

-- Initial shard placements
SELECT * FROM shards_in_workers;
SELECT * FROM find_index_for_shard_201_in_workers;

-- failure on sanity checks
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS move_shard_offline.t CASCADE").kill()');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

-- cancellation on sanity checks
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS move_shard_offline.t CASCADE").cancel(' || :pid || ')');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

-- failure on move_shard table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE move_shard_offline.t").kill()');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

-- cancellation on move_shard table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE move_shard_offline.t").cancel(' || :pid || ')');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

-- failure on blocking COPY operation on target node
SELECT citus.mitmproxy('conn.onQuery(query="COPY").kill()');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

-- cancellation on blocking COPY operation on target node
SELECT citus.mitmproxy('conn.onQuery(query="COPY").cancel(' || :pid || ')');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

-- failure on adding constraints on target node
SELECT citus.mitmproxy('conn.onQuery(query="ADD CONSTRAINT").kill()');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

-- cancellation on adding constraints on target node
SELECT citus.mitmproxy('conn.onQuery(query="ADD CONSTRAINT").cancel(' || :pid || ')');
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');

SELECT public.wait_for_resource_cleanup();

-- Verify that the shard is not moved and the number of rows are still 100k
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM shards_in_workers;
SELECT count(*) FROM t;
SELECT * FROM find_index_for_shard_201_in_workers;

-- Verify that shard can be moved after a temporary failure
SELECT master_move_shard_placement(201, 'localhost', :worker_1_port, 'localhost', :worker_2_proxy_port, 'block_writes');
SELECT public.wait_for_resource_cleanup();
SELECT * FROM shards_in_workers;
SELECT count(*) FROM t;
SELECT * FROM find_index_for_shard_201_in_workers;

DROP SCHEMA move_shard_offline CASCADE;
