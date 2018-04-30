--
-- MULTI_MX_TPCH_QUERY14
--


-- connect to the coordinator
\c - - - :master_port

-- Query #14 from the TPC-H decision support benchmark

SELECT
	100.00 * sum(case
		   	 when p_type like 'PROMO%'
			 then l_extendedprice * (1 - l_discount)
			 else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
	lineitem_mx,
	part_mx
WHERE
	l_partkey = p_partkey
	AND l_shipdate >= date '1995-09-01'
	AND l_shipdate < date '1995-09-01' + interval '1' year;

-- connect one of the workers
\c - - - :worker_1_port

-- Query #14 from the TPC-H decision support benchmark

SELECT
	100.00 * sum(case
		   	 when p_type like 'PROMO%'
			 then l_extendedprice * (1 - l_discount)
			 else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
	lineitem_mx,
	part_mx
WHERE
	l_partkey = p_partkey
	AND l_shipdate >= date '1995-09-01'
	AND l_shipdate < date '1995-09-01' + interval '1' year;

	-- connect to the other node
\c - - - :worker_2_port

-- Query #14 from the TPC-H decision support benchmark

SELECT
	100.00 * sum(case
		   	 when p_type like 'PROMO%'
			 then l_extendedprice * (1 - l_discount)
			 else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
	lineitem_mx,
	part_mx
WHERE
	l_partkey = p_partkey
	AND l_shipdate >= date '1995-09-01'
	AND l_shipdate < date '1995-09-01' + interval '1' year;