--
-- MULTI_MX_TPCH_QUERY7_NESTED
--


-- connect to the coordinator
\c - - - :master_port

-- Query #7 from the TPC-H benchmark; modified to include sub-selects

SELECT
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) AS revenue
FROM
	(
	SELECT
		supp_nation,
		cust_nation,
		extract(year FROM l_shipdate) AS l_year,
		l_extendedprice * (1 - l_discount) AS volume
	FROM
		supplier_mx,
		lineitem_mx,
		orders_mx,
		customer_mx,
		(
		SELECT 
			n1.n_nationkey AS supp_nation_key,
			n2.n_nationkey AS cust_nation_key,
			n1.n_name AS supp_nation,
			n2.n_name AS cust_nation
		FROM 
			nation_mx n1,
			nation_mx n2
		WHERE 
			(
			(n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
			OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')   
			)
		) AS temp
        WHERE
		s_suppkey = l_suppkey
		AND o_orderkey = l_orderkey
		AND c_custkey = o_custkey
		AND s_nationkey = supp_nation_key
		AND c_nationkey = cust_nation_key
		AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
	) AS shipping
GROUP BY
	supp_nation,
	cust_nation,
	l_year
ORDER BY
	supp_nation,
	cust_nation,
	l_year;

-- connect to one of the workers
\c - - - :worker_1_port

-- Query #7 from the TPC-H benchmark; modified to include sub-selects

SELECT
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) AS revenue
FROM
	(
	SELECT
		supp_nation,
		cust_nation,
		extract(year FROM l_shipdate) AS l_year,
		l_extendedprice * (1 - l_discount) AS volume
	FROM
		supplier_mx,
		lineitem_mx,
		orders_mx,
		customer_mx,
		(
		SELECT 
			n1.n_nationkey AS supp_nation_key,
			n2.n_nationkey AS cust_nation_key,
			n1.n_name AS supp_nation,
			n2.n_name AS cust_nation
		FROM 
			nation_mx n1,
			nation_mx n2
		WHERE 
			(
			(n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
			OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')   
			)
		) AS temp
        WHERE
		s_suppkey = l_suppkey
		AND o_orderkey = l_orderkey
		AND c_custkey = o_custkey
		AND s_nationkey = supp_nation_key
		AND c_nationkey = cust_nation_key
		AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
	) AS shipping
GROUP BY
	supp_nation,
	cust_nation,
	l_year
ORDER BY
	supp_nation,
	cust_nation,
	l_year;

-- connect to the coordinator
\c - - - :worker_2_port

-- Query #7 from the TPC-H benchmark; modified to include sub-selects

SELECT
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) AS revenue
FROM
	(
	SELECT
		supp_nation,
		cust_nation,
		extract(year FROM l_shipdate) AS l_year,
		l_extendedprice * (1 - l_discount) AS volume
	FROM
		supplier_mx,
		lineitem_mx,
		orders_mx,
		customer_mx,
		(
		SELECT 
			n1.n_nationkey AS supp_nation_key,
			n2.n_nationkey AS cust_nation_key,
			n1.n_name AS supp_nation,
			n2.n_name AS cust_nation
		FROM 
			nation_mx n1,
			nation_mx n2
		WHERE 
			(
			(n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
			OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')   
			)
		) AS temp
        WHERE
		s_suppkey = l_suppkey
		AND o_orderkey = l_orderkey
		AND c_custkey = o_custkey
		AND s_nationkey = supp_nation_key
		AND c_nationkey = cust_nation_key
		AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
	) AS shipping
GROUP BY
	supp_nation,
	cust_nation,
	l_year
ORDER BY
	supp_nation,
	cust_nation,
	l_year;
