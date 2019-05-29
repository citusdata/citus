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
-- by using round-robin task_assignment_policy we can force to hit both machines. We will
-- use two output files to match both orders to verify there is 1 that times out and falls
-- through to read from the other machine
SET citus.task_assignment_policy TO 'round-robin';
-- suppress the warning since we can't control which shard is chose first. Failure of this
-- test would be if one of the queries does not return the result but an error.
SET client_min_messages TO ERROR;
SELECT name FROM r1 WHERE id = 2;
SELECT name FROM r1 WHERE id = 2;

-- verify a connection attempt was made to the intercepted node, this would have cause the
-- connection to have been delayed and thus caused a timeout
SELECT citus.dump_network_traffic();

RESET client_min_messages;

-- verify get_global_active_transactions works when a timeout happens on a connection
SELECT get_global_active_transactions();


SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA fail_connect CASCADE;
SET search_path TO default;
