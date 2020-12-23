CREATE SCHEMA failure_failover_to_local_execution;
SET search_path TO failure_failover_to_local_execution;
SELECT citus.mitmproxy('conn.allow()');

SET citus.next_shard_id TO 1980000;

-- on Citus-mx, we can have
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_proxy_port);

SET citus.replication_model TO 'streaming';
SET citus.shard_replication_factor TO 1;
CREATE TABLE failover_to_local (key int PRIMARY KEY, value varchar(10));
SELECT create_distributed_table('failover_to_local', 'key');

\c - - - :worker_2_port
SET search_path TO failure_failover_to_local_execution;

-- we don't want any cached connections
SET citus.max_cached_conns_per_worker to 0;
INSERT INTO failover_to_local SELECT i, i::text FROM generate_series(0,20)i;


-- even if the connection establishment fails, Citus can
-- failover to local exection
SET citus.node_connection_timeout TO 400;
SELECT citus.mitmproxy('conn.delay(500)');
SET citus.log_local_commands TO ON;
SET client_min_messages TO DEBUG1;
SELECT count(*) FROM failover_to_local;
RESET client_min_messages;
SELECT citus.mitmproxy('conn.allow()');

-- if the remote query execution fails, Citus
-- does not try to fallback to local execution
SELECT key / 0 FROM failover_to_local;

-- if the local execution is disabled, Citus does
-- not try to fallback to local execution
SET citus.enable_local_execution TO false;
SELECT citus.mitmproxy('conn.delay(500)');
SET citus.log_local_commands TO ON;
SELECT count(*) FROM failover_to_local;
SELECT citus.mitmproxy('conn.allow()');
RESET citus.enable_local_execution;

-- even if we are on a multi-shard command, Citus can
-- failover to the local execution if no connection attempts succeed
SELECT citus.mitmproxy('conn.onAuthenticationOk().kill()');
SET citus.log_remote_commands TO ON;
SELECT count(*) FROM failover_to_local;

SELECT citus.mitmproxy('conn.allow()');

\c - - - :master_port
SET client_min_messages TO ERROR;
DROP SCHEMA failure_failover_to_local_execution CASCADE;

