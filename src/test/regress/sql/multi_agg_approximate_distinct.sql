--
-- MULTI_AGG_APPROXIMATE_DISTINCT
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 340000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 340000;


-- Try to execute count(distinct) when approximate distincts aren't enabled

SELECT count(distinct l_orderkey) FROM lineitem;

-- Check approximate count(distinct) at different precisions / error rates

SET citus.count_distinct_error_rate = 0.1;
SELECT count(distinct l_orderkey) FROM lineitem;

SET citus.count_distinct_error_rate = 0.01;
SELECT count(distinct l_orderkey) FROM lineitem;

-- Check approximate count(distinct) for different data types

SELECT count(distinct l_partkey) FROM lineitem;

SELECT count(distinct l_extendedprice) FROM lineitem;

SELECT count(distinct l_shipdate) FROM lineitem;

SELECT count(distinct l_comment) FROM lineitem;

-- Check that we can execute approximate count(distinct) on complex expressions

SELECT count(distinct (l_orderkey * 2 + 1)) FROM lineitem;

SELECT count(distinct extract(month from l_shipdate)) AS my_month FROM lineitem;

SELECT count(distinct l_partkey) / count(distinct l_orderkey) FROM lineitem;

-- Check that we can execute approximate count(distinct) on select queries that
-- contain different filter, join, sort and limit clauses

SELECT count(distinct l_orderkey) FROM lineitem
	WHERE octet_length(l_comment) + octet_length('randomtext'::text) > 40;

SELECT count(DISTINCT l_orderkey) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_quantity < 5;

SELECT count(DISTINCT l_orderkey) as distinct_order_count, l_quantity FROM lineitem
	WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY distinct_order_count ASC, l_quantity ASC
	LIMIT 10;

-- If we have an order by on count(distinct) that we intend to push down to
-- worker nodes, we need to error out. Otherwise, we are fine.

SET citus.limit_clause_row_fetch_count = 1000;
SELECT l_returnflag, count(DISTINCT l_shipdate) as count_distinct, count(*) as total 
	FROM lineitem
	GROUP BY l_returnflag
	ORDER BY count_distinct
	LIMIT 10;

SELECT l_returnflag, count(DISTINCT l_shipdate) as count_distinct, count(*) as total 
	FROM lineitem
	GROUP BY l_returnflag
	ORDER BY total
	LIMIT 10;

-- Check that we can revert config and disable count(distinct) approximations

SET citus.count_distinct_error_rate = 0.0;
SELECT count(distinct l_orderkey) FROM lineitem;
