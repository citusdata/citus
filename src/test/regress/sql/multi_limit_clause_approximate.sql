--
-- MULTI_LIMIT_CLAUSE_APPROXIMATE
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 720000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 720000;


-- Display debug messages on limit clause push down.

SET client_min_messages TO DEBUG1;

-- We first look at results with limit optimization disabled. This first query
-- has a group and an order by. The order by clause is a commutative aggregate
-- function.

SELECT l_partkey, sum(l_partkey * (1 + l_suppkey)) AS aggregate FROM lineitem
	GROUP BY l_partkey
	ORDER BY aggregate DESC LIMIT 10;

-- Enable limit optimization to fetch one third of each shard's data

SET citus.limit_clause_row_fetch_count TO 600;

SELECT l_partkey, sum(l_partkey * (1 + l_suppkey)) AS aggregate FROM lineitem
	GROUP BY l_partkey
	ORDER BY aggregate DESC LIMIT 10;

-- Disable limit optimization for our second test. This time, we have a query
-- that joins several tables, and that groups and orders the results.

RESET citus.limit_clause_row_fetch_count;

SELECT c_custkey, c_name, count(*) as lineitem_count
	FROM customer, orders, lineitem
	WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
	GROUP BY c_custkey, c_name
	ORDER BY lineitem_count DESC, c_custkey LIMIT 10;

-- Now, enable limit optimization to fetch half of each task's results. For this
-- test, we also change a config setting to ensure that we don't repartition any
-- of the tables during the query.

SET citus.limit_clause_row_fetch_count TO 150;
SET citus.large_table_shard_count TO 2;

SELECT c_custkey, c_name, count(*) as lineitem_count
	FROM customer, orders, lineitem
	WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
	GROUP BY c_custkey, c_name
	ORDER BY lineitem_count DESC, c_custkey LIMIT 10;

RESET citus.large_table_shard_count;

-- We now test scenarios where applying the limit optimization wouldn't produce
-- meaningful results. First, we check that we don't push down the limit clause
-- for non-commutative aggregates.

SELECT l_partkey, avg(l_suppkey) AS average FROM lineitem
	GROUP BY l_partkey
	ORDER BY average DESC, l_partkey LIMIT 10;

-- Next, check that we don't apply the limit optimization for expressions that
-- have aggregates within them

SELECT l_partkey, round(sum(l_suppkey)) AS complex_expression FROM lineitem
	GROUP BY l_partkey
	ORDER BY complex_expression DESC LIMIT 10;

-- Check that query execution works as expected for other queries without limits

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 10.0
	GROUP BY l_quantity
	ORDER BY count_quantity ASC, l_quantity ASC;

RESET citus.limit_clause_row_fetch_count;
RESET client_min_messages;
