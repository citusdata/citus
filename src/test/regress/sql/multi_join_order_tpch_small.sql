--
-- MULTI_JOIN_ORDER_TPCH_SMALL
--

-- Enable configuration to print table join order

SET citus.explain_distributed_queries TO off;
SET citus.log_multi_join_order TO TRUE;
SET client_min_messages TO LOG;

-- Query #6 from the TPC-H decision support benchmark

EXPLAIN SELECT
	sum(l_extendedprice * l_discount) as revenue
FROM
	lineitem
WHERE
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1 year'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;

-- Query #3 from the TPC-H decision support benchmark

EXPLAIN SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
FROM
	customer,
	orders,
	lineitem
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

-- Query #10 from the TPC-H decision support benchmark

EXPLAIN SELECT
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
FROM
	customer,
	orders,
	lineitem,
	nation
WHERE
	c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate >= date '1993-10-01'
	AND o_orderdate < date '1993-10-01' + interval '3' month
	AND l_returnflag = 'R'
	AND c_nationkey = n_nationkey
GROUP BY
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
ORDER BY
	revenue DESC;

-- Query #19 from the TPC-H decision support benchmark (modified)

EXPLAIN SELECT
	sum(l_extendedprice* (1 - l_discount)) as revenue
FROM
	lineitem,
	part
WHERE
	(
		p_partkey = l_partkey
		AND (p_brand = 'Brand#12' OR p_brand= 'Brand#14' OR p_brand='Brand#15')
		AND l_quantity >= 10
		AND l_shipmode in ('AIR', 'AIR REG', 'TRUCK')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	)
	OR
	(
		p_partkey = l_partkey
		AND (p_brand = 'Brand#23' OR p_brand='Brand#24')
		AND l_quantity >= 20
		AND l_shipmode in ('AIR', 'AIR REG', 'TRUCK')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	)
	OR
	(
		p_partkey = l_partkey
		AND (p_brand = 'Brand#33' OR p_brand = 'Brand#34' OR p_brand = 'Brand#35')
		AND l_quantity >= 1
		AND l_shipmode in ('AIR', 'AIR REG', 'TRUCK')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	);

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;
