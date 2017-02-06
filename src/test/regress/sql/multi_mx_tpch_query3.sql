--
-- MULTI_MX_TPCH_QUERY3
--

-- Query #3 from the TPC-H decision support benchmark. Unlike other TPC-H tests,
-- we don't set citus.large_table_shard_count here, and instead use the default value
-- coming from postgresql.conf or multi_task_tracker_executor.conf.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1260000;

-- connect to the coordinator
\c - - - :master_port

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
FROM
	customer_mx,
	orders_mx,
	lineitem_mx
WHERE
	c_mktsegment = 'BUILDING'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate < date '1995-03-15'
	AND l_shipdate > date '1995-03-15'
GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
ORDER BY
	revenue DESC,
	o_orderdate;

-- connect one of the workers
\c - - - :worker_1_port

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1260000;

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
FROM
	customer_mx,
	orders_mx,
	lineitem_mx
WHERE
	c_mktsegment = 'BUILDING'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate < date '1995-03-15'
	AND l_shipdate > date '1995-03-15'
GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
ORDER BY
	revenue DESC,
	o_orderdate;

-- connect to the other node
\c - - - :worker_2_port

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1260000;

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
FROM
	customer_mx,
	orders_mx,
	lineitem_mx
WHERE
	c_mktsegment = 'BUILDING'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate < date '1995-03-15'
	AND l_shipdate > date '1995-03-15'
GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
ORDER BY
	revenue DESC,
	o_orderdate;
