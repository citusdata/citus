--
-- MULTI_TPCH_QUERY7_NESTED
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 960000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 960000;


-- Change configuration to treat lineitem AND orders tables AS large

SET citus.large_table_shard_count TO 2;

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
		supplier,
		lineitem,
		orders,
		customer,
		(
		SELECT 
			n1.n_nationkey AS supp_nation_key,
			n2.n_nationkey AS cust_nation_key,
			n1.n_name AS supp_nation,
			n2.n_name AS cust_nation
		FROM 
			nation n1,
			nation n2
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
