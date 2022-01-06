--
-- failure_connection_establishment.sql tests some behaviour of connection management when
-- it fails to connect.
--
-- Failure cases covered:
--  - timeout
--

SELECT citus.mitmproxy('conn.allow()');

CREATE SCHEMA fail_connect;
SET search_path TO 'fail_connect';

SET citus.shard_count TO 4;
SET citus.max_cached_conns_per_worker TO 0;
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1450000;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 1450000;

CREATE TABLE products (
    product_no integer,
    name text,
    price numeric
);
SELECT create_distributed_table('products', 'product_no');

-- Can only add primary key constraint on distribution column (or group of columns
-- including distribution column)
-- Command below should error out since 'name' is not a distribution column
ALTER TABLE products ADD CONSTRAINT p_key PRIMARY KEY(name);


-- we will insert a connection delay here as this query was the cause for an investigation
-- into connection establishment problems
SET citus.node_connection_timeout TO 400;
SELECT citus.mitmproxy('conn.delay(500)');

ALTER TABLE products ADD CONSTRAINT p_key PRIMARY KEY(product_no);

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE r1 (
    id int PRIMARY KEY,
    name text
);
INSERT INTO r1 (id, name) VALUES
(1,'foo'),
(2,'bar'),
(3,'baz');

SELECT create_reference_table('r1');

SELECT citus.clear_network_traffic();
SELECT citus.mitmproxy('conn.delay(500)');

-- we cannot control which replica of the reference table will be queried and there is
-- only one specific client we can control the connection for.
-- by using round-robin task_assignment_policy we can force to hit both machines.
-- and in the end, dumping the network traffic shows that the connection establishment
-- is initiated to the node behind the proxy
SET client_min_messages TO ERROR;
SET citus.task_assignment_policy TO 'round-robin';
-- suppress the warning since we can't control which shard is chose first. Failure of this
-- test would be if one of the queries does not return the result but an error.
SELECT name FROM r1 WHERE id = 2;
SELECT name FROM r1 WHERE id = 2;

-- verify a connection attempt was made to the intercepted node, this would have cause the
-- connection to have been delayed and thus caused a timeout
SELECT * FROM citus.dump_network_traffic() WHERE conn=0;

SELECT citus.mitmproxy('conn.allow()');

-- similar test with the above but this time on a
-- distributed table instead of a reference table
-- and with citus.force_max_query_parallelization is set
SET citus.force_max_query_parallelization TO ON;
SELECT citus.mitmproxy('conn.delay(500)');
-- suppress the warning since we can't control which shard is chose first. Failure of this
-- test would be if one of the queries does not return the result but an error.
SELECT count(*) FROM products;
SELECT count(*) FROM products;

SELECT citus.mitmproxy('conn.allow()');
SET citus.shard_replication_factor TO 1;
CREATE TABLE single_replicatated(key int);
SELECT create_distributed_table('single_replicatated', 'key');

-- this time the table is single replicated and we're still using the
-- the max parallelization flag, so the query should fail
SET citus.force_max_query_parallelization TO ON;
SELECT citus.mitmproxy('conn.delay(500)');
SELECT count(*) FROM single_replicatated;

SET citus.force_max_query_parallelization TO OFF;

-- one similar test, and this time on modification queries
-- to see that connection establishement failures could
-- fail the transaction (but not mark any placements as INVALID)
SELECT citus.mitmproxy('conn.allow()');
BEGIN;
SELECT
	count(*) as invalid_placement_count
FROM
	pg_dist_shard_placement
WHERE
	shardstate = 3 AND
	shardid IN (SELECT shardid from pg_dist_shard where logicalrelid = 'single_replicatated'::regclass);
SELECT citus.mitmproxy('conn.delay(500)');
INSERT INTO single_replicatated VALUES (100);
COMMIT;
SELECT
	count(*) as invalid_placement_count
FROM
	pg_dist_shard_placement
WHERE
	shardstate = 3 AND
	shardid IN (SELECT shardid from pg_dist_shard where logicalrelid = 'single_replicatated'::regclass);

-- show that INSERT failed
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM single_replicatated WHERE key = 100;


RESET client_min_messages;

-- verify get_global_active_transactions works when a timeout happens on a connection
SELECT * FROM get_global_active_transactions() WHERE datid != 0;

-- tests for connectivity checks
SET client_min_messages TO ERROR;

-- kill the connection after authentication is ok
SELECT citus.mitmproxy('conn.onAuthenticationOk().kill()');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);

-- cancel the connection after authentication is ok
SELECT citus.mitmproxy('conn.onAuthenticationOk().cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);

-- kill the connection after connectivity check query is sent
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT 1$").kill()');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);

-- cancel the connection after connectivity check query is sent
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT 1$").cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);

-- kill the connection after connectivity check command is complete
SELECT citus.mitmproxy('conn.onCommandComplete(command="SELECT 1").kill()');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);

-- cancel the connection after connectivity check command is complete
SELECT citus.mitmproxy('conn.onCommandComplete(command="SELECT 1").cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);

-- verify that the checks are not successful when timeouts happen on a connection
SELECT citus.mitmproxy('conn.delay(500)');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);

-- tests for citus_check_cluster_node_health

-- kill all connectivity checks that originate from this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT citus_check_connection_to_node").kill()');
SELECT * FROM citus_check_cluster_node_health();

-- suggested summary queries for connectivity checks
SELECT bool_and(coalesce(result, false)) FROM citus_check_cluster_node_health();
SELECT result, count(*) FROM citus_check_cluster_node_health() GROUP BY result ORDER BY 1;

-- cancel all connectivity checks that originate from this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT citus_check_connection_to_node").cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_cluster_node_health();

-- kill all but first connectivity checks that originate from this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT citus_check_connection_to_node").after(1).kill()');
SELECT * FROM citus_check_cluster_node_health();

-- cancel all but first connectivity checks that originate from this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT citus_check_connection_to_node").after(1).cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_cluster_node_health();

-- kill all connections to this node
SELECT citus.mitmproxy('conn.onAuthenticationOk().kill()');
SELECT * FROM citus_check_cluster_node_health();

-- cancel all connections to this node
SELECT citus.mitmproxy('conn.onAuthenticationOk().cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_cluster_node_health();

-- kill connection checks to this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT 1$").kill()');
SELECT * FROM citus_check_cluster_node_health();

-- cancel connection checks to this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT 1$").cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_cluster_node_health();


RESET client_min_messages;
SELECT citus.mitmproxy('conn.allow()');
SET citus.node_connection_timeout TO DEFAULT;
DROP SCHEMA fail_connect CASCADE;
SET search_path TO default;
