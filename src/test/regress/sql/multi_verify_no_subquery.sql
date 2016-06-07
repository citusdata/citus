--
-- MULTI_VERIFY_NO_SUBQUERY
--

-- This test checks that we simply emit an error message instead of trying to
-- process a distributed unsupported SQL subquery.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1030000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1030000;


SELECT * FROM lineitem WHERE l_orderkey IN
	(SELECT l_orderkey FROM lineitem WHERE l_quantity > 0);

SELECT l_quantity FROM lineitem WHERE EXISTS
	(SELECT 1 FROM orders WHERE o_orderkey = l_orderkey);

SELECT l_quantity FROM lineitem WHERE l_orderkey IN (SELECT o_orderkey FROM orders);

SELECT l_orderkey FROM lineitem WHERE l_quantity > ALL(SELECT o_orderkey FROM orders);

SELECT l_quantity FROM lineitem WHERE l_orderkey = (SELECT min(o_orderkey) FROM orders);
