--
-- failure_parallel_connection.sql tests some behaviour of connection management
-- where Citus is expected to use multiple connections.
--
-- In other words, we're not testing any failures in this test. We're trying to make
-- sure that Citus uses 1-connection per placement of distributed table even after
-- a join with distributed table
--

SELECT citus.mitmproxy('conn.allow()');

CREATE SCHEMA fail_parallel_connection;
SET search_path TO 'fail_parallel_connection';

SET citus.shard_count TO 4;
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1880000;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 1880000;

CREATE TABLE distributed_table (
	key int,
	value int
);
SELECT create_distributed_table('distributed_table', 'key');

CREATE TABLE reference_table (
	key int,
	value int
);
SELECT create_reference_table('reference_table');

-- make sure that access to the placements of the distributed
-- tables use 1 connection
SET citus.force_max_query_parallelization TO ON;

BEGIN;
	SELECT count(*) FROM distributed_table JOIN reference_table USING (key);

	SELECT citus.mitmproxy('conn.onQuery(query="^SELECT count").after(1).kill()');

	-- this query should not fail because each placement should be acceessed
	-- over a seperate connection
	SELECT count(*) FROM distributed_table JOIN reference_table USING (key);
COMMIT;


SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA fail_parallel_connection CASCADE;
SET search_path TO default;
