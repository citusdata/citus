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
SELECT citus.dump_network_traffic();

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

-- use OFFSET 1 to prevent printing the line where source 
-- is the worker
SELECT citus.dump_network_traffic() ORDER BY 1 OFFSET 1;

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

-- one similar test, but this time on modification queries
-- to see that connection establishement failures could
-- mark placement INVALID
SELECT citus.mitmproxy('conn.allow()');
BEGIN;
SELECT 
	count(*) as invalid_placement_count
FROM 
	pg_dist_shard_placement 
WHERE 
	shardstate = 3 AND 
	shardid IN (SELECT shardid from pg_dist_shard where logicalrelid = 'products'::regclass);
SELECT citus.mitmproxy('conn.delay(500)');
INSERT INTO products VALUES (100, '100', 100);
COMMIT;
SELECT 
	count(*) as invalid_placement_count
FROM 
	pg_dist_shard_placement 
WHERE 
	shardstate = 3 AND 
	shardid IN (SELECT shardid from pg_dist_shard where logicalrelid = 'products'::regclass);

-- show that INSERT went through
SELECT count(*) FROM products WHERE product_no = 100;


RESET client_min_messages;

-- verify get_global_active_transactions works when a timeout happens on a connection
SELECT get_global_active_transactions();


SELECT citus.mitmproxy('conn.allow()');
SET citus.node_connection_timeout TO DEFAULT;
DROP SCHEMA fail_connect CASCADE;
SET search_path TO default;
