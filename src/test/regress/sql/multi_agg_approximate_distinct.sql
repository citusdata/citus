--
-- MULTI_AGG_APPROXIMATE_DISTINCT
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 340000;

-- Create HLL extension if present, print false result otherwise
SELECT CASE WHEN COUNT(*) > 0 THEN
	'CREATE EXTENSION HLL'
ELSE 'SELECT false AS hll_present' END
AS create_cmd FROM pg_available_extensions()
WHERE name = 'hll'
\gset

:create_cmd;

\c - - - :worker_1_port
:create_cmd;

\c - - - :worker_2_port
:create_cmd;

\c - - - :master_port

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

-- Check that approximate count(distinct) works at a table in a schema other than public
-- create necessary objects
CREATE SCHEMA test_count_distinct_schema;

CREATE TABLE test_count_distinct_schema.nation_hash(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT master_create_distributed_table('test_count_distinct_schema.nation_hash', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_count_distinct_schema.nation_hash', 4, 2);

\copy test_count_distinct_schema.nation_hash FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SET search_path TO public;
SET citus.count_distinct_error_rate TO 0.01;
SELECT COUNT (DISTINCT n_regionkey) FROM test_count_distinct_schema.nation_hash;

-- test with search_path is set
SET search_path TO test_count_distinct_schema;
SELECT COUNT (DISTINCT n_regionkey) FROM nation_hash;
SET search_path TO public;

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

SELECT
	l_orderkey,
	count(l_partkey) FILTER (WHERE l_shipmode = 'AIR'),
	count(DISTINCT l_partkey) FILTER (WHERE l_shipmode = 'AIR'),
	count(DISTINCT CASE WHEN l_shipmode = 'AIR' THEN l_partkey ELSE NULL END)
	FROM lineitem
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- Check that we can revert config and disable count(distinct) approximations

SET citus.count_distinct_error_rate = 0.0;
SELECT count(distinct l_orderkey) FROM lineitem;
