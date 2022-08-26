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

CREATE TABLE r1 (
    id int PRIMARY KEY,
    name text
);
INSERT INTO r1 (id, name) VALUES
(1,'foo'),
(2,'bar'),
(3,'baz');

SELECT create_reference_table('r1');

-- Confirm that the first placement for both tables is on the second worker
-- node. This is necessary so  we can use the first-replica task assignment
-- policy to first hit the node that we generate timeouts for.
SELECT placementid, p.shardid, logicalrelid, LEAST(2, groupid) groupid
FROM pg_dist_placement p JOIN pg_dist_shard s ON p.shardid = s.shardid
ORDER BY placementid;
SET citus.task_assignment_policy TO 'first-replica';

-- we will insert a connection delay here as this query was the cause for an
-- investigation into connection establishment problems
SET citus.node_connection_timeout TO 900;
SELECT citus.mitmproxy('conn.connect_delay(1400)');
ALTER TABLE products ADD CONSTRAINT p_key PRIMARY KEY(product_no);
RESET citus.node_connection_timeout;
SELECT citus.mitmproxy('conn.allow()');

-- Make sure that we fall back to a working node for reads, even if it's not
-- the first choice in our task assignment policy.
SET citus.node_connection_timeout TO 900;
SELECT citus.mitmproxy('conn.connect_delay(1400)');
-- tests for connectivity checks
SELECT name FROM r1 WHERE id = 2;
RESET citus.node_connection_timeout;
SELECT citus.mitmproxy('conn.allow()');

-- similar test with the above but this time on a distributed table instead of
-- a reference table and with citus.force_max_query_parallelization is set
SET citus.force_max_query_parallelization TO ON;
SET citus.node_connection_timeout TO 900;
SELECT citus.mitmproxy('conn.connect_delay(1400)');
SELECT count(*) FROM products;
RESET citus.node_connection_timeout;
SELECT citus.mitmproxy('conn.allow()');

SET citus.shard_replication_factor TO 1;
CREATE TABLE single_replicatated(key int);
SELECT create_distributed_table('single_replicatated', 'key');

-- this time the table is single replicated and we're still using the
-- the max parallelization flag, so the query should fail
SET citus.force_max_query_parallelization TO ON;
SET citus.node_connection_timeout TO 900;
SELECT citus.mitmproxy('conn.connect_delay(1400)');
SELECT count(*) FROM single_replicatated;
RESET citus.force_max_query_parallelization;
RESET citus.node_connection_timeout;
SELECT citus.mitmproxy('conn.allow()');


-- one similar test, and this time on modification queries
-- to see that connection establishement failures could
-- fail the transaction (but not mark any placements as INVALID)
BEGIN;
SELECT
	count(*) as invalid_placement_count
FROM
	pg_dist_shard_placement
WHERE
	shardstate = 3 AND
	shardid IN (SELECT shardid from pg_dist_shard where logicalrelid = 'single_replicatated'::regclass);
SET citus.node_connection_timeout TO 900;
SELECT citus.mitmproxy('conn.connect_delay(1400)');
INSERT INTO single_replicatated VALUES (100);
COMMIT;
RESET citus.node_connection_timeout;
SELECT citus.mitmproxy('conn.allow()');
SELECT
	count(*) as invalid_placement_count
FROM
	pg_dist_shard_placement
WHERE
	shardstate = 3 AND
	shardid IN (SELECT shardid from pg_dist_shard where logicalrelid = 'single_replicatated'::regclass);

SELECT count(*) FROM single_replicatated WHERE key = 100;


RESET client_min_messages;

-- verify get_global_active_transactions works when a timeout happens on a connection
SELECT * FROM get_global_active_transactions() WHERE transaction_number != 0;

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
SET citus.node_connection_timeout TO 900;
SELECT citus.mitmproxy('conn.connect_delay(1400)');
SELECT * FROM citus_check_connection_to_node('localhost', :worker_2_proxy_port);
RESET citus.node_connection_timeout;
SELECT citus.mitmproxy('conn.allow()');

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

-- kill connection checks to this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT 1$").kill()');
SELECT * FROM citus_check_cluster_node_health();

-- cancel connection checks to this node
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT 1$").cancel(' || pg_backend_pid() || ')');
SELECT * FROM citus_check_cluster_node_health();


RESET client_min_messages;
RESET citus.node_connection_timeout;
SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA fail_connect CASCADE;
SET search_path TO default;
