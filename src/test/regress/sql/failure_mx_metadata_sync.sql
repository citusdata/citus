--
-- failure_mx_metadata_sync.sql
--
CREATE SCHEMA IF NOT EXISTS mx_metadata_sync;
SET SEARCH_PATH = mx_metadata_sync;
SET citus.shard_count TO 2;
SET citus.next_shard_id TO 16000000;
SET citus.shard_replication_factor TO 1;

SELECT pg_backend_pid() as pid \gset
SELECT citus.mitmproxy('conn.allow()');

\set VERBOSITY terse
SET client_min_messages TO ERROR;

CREATE TABLE t1 (id int PRIMARY KEY);
SELECT create_distributed_table('t1', 'id');
INSERT INTO t1 SELECT x FROM generate_series(1,100) AS f(x);

-- Initially turn metadata sync off because we'll ingest errors to start/stop metadata sync operations
SELECT stop_metadata_sync_to_node('localhost', :worker_2_proxy_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_proxy_port;

-- Failure to set groupid in the worker
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE pg_dist_local_group SET groupid").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE pg_dist_local_group SET groupid").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to drop all tables in pg_dist_partition
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_partition").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_partition").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to delete pg_dist_node entries from the worker
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_node").cancel(' || :pid || ')');
SELECT start_metadata_sync_to_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_node").kill()');
SELECT start_metadata_sync_to_node('localhost', :worker_2_proxy_port);

-- Failure to populate pg_dist_node in the worker
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO pg_dist_node").cancel(' || :pid || ')');
SELECT start_metadata_sync_to_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO pg_dist_node").kill()');
SELECT start_metadata_sync_to_node('localhost', :worker_2_proxy_port);

-- Verify that coordinator knows worker does not have valid metadata
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_proxy_port;

-- Verify we can activate node after unsuccessful attempts
SELECT citus.mitmproxy('conn.allow()');
SELECT 1 FROM citus_activate_node('localhost', :worker_2_proxy_port);
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_proxy_port;

-- Check failures on DDL command propagation
CREATE TABLE t2 (id int PRIMARY KEY);

SELECT citus.mitmproxy('conn.onParse(query="citus_internal_add_placement_metadata").kill()');
SELECT create_distributed_table('t2', 'id');

SELECT citus.mitmproxy('conn.onParse(query="citus_internal_add_shard_metadata").cancel(' || :pid || ')');
SELECT create_distributed_table('t2', 'id');

-- Verify that the table was not distributed
SELECT count(*) > 0 AS is_table_distributed
FROM pg_dist_partition
WHERE logicalrelid='t2'::regclass;

-- Failure to set groupid in the worker
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE pg_dist_local_group SET groupid").cancel(' || :pid || ')');
SELECT stop_metadata_sync_to_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE pg_dist_local_group SET groupid").kill()');
SELECT stop_metadata_sync_to_node('localhost', :worker_2_proxy_port);

-- Failure to delete pg_dist_node entries from the worker
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_node").cancel(' || :pid || ')');
SELECT stop_metadata_sync_to_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_node").kill()');
SELECT stop_metadata_sync_to_node('localhost', :worker_2_proxy_port);

\c - - - :worker_2_port
SELECT count(*) FROM pg_dist_node;

\c - - - :master_port
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_proxy_port;
-- Verify we can stop metadata sync after unsuccessful attempts
SELECT citus.mitmproxy('conn.allow()');
SELECT stop_metadata_sync_to_node('localhost', :worker_2_proxy_port);

\c - - - :worker_2_port
SELECT count(*) FROM pg_dist_node;

\c - - - :master_port
SELECT hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_proxy_port;

-- turn metadata sync back on
SELECT start_metadata_sync_to_node('localhost', :worker_2_proxy_port);

SET SEARCH_PATH = mx_metadata_sync;
DROP TABLE t1;
DROP TABLE t2;
DROP SCHEMA mx_metadata_sync CASCADE;
