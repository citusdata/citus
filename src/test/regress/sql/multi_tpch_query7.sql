--
-- MULTI_TPCH_QUERY7
--


-- Change configuration to treat lineitem AND orders tables as large

SET citus.large_table_shard_count TO 2;

-- Query #7 from the TPC-H decision support benchmark

SELECT
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
FROM
	(
	SELECT
		n1.n_name as supp_nation,
		n2.n_name as cust_nation,
		extract(year FROM l_shipdate) as l_year,
		l_extendedprice * (1 - l_discount) as volume
	FROM
		supplier,
		lineitem,
		orders,
		customer,
		nation n1,
		nation n2
	WHERE
		s_suppkey = l_suppkey
		AND o_orderkey = l_orderkey
		AND c_custkey = o_custkey
		AND s_nationkey = n1.n_nationkey
		AND c_nationkey = n2.n_nationkey
		AND (
			(n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
			OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
		)
		AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
	) as shipping
GROUP BY
	supp_nation,
	cust_nation,
	l_year
ORDER BY
	supp_nation,
	cust_nation,
	l_year;
