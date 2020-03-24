--
-- MULTI_MX_TPCH_QUERY3
--

-- Query #3 from the TPC-H decision support benchmark.


-- connect to the coordinator
\c - - :master_host :master_port

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
\c - - :public_worker_1_host :worker_1_port

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
\c - - :public_worker_2_host :worker_2_port

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
