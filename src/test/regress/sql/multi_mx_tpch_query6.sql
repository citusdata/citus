--
-- MULTI_MX_TPCH_QUERY6
--


-- connect to the coordinator
\c - - - :master_port

-- Query #6 from the TPC-H decision support benchmark

SELECT
	sum(l_extendedprice * l_discount) as revenue
FROM
	lineitem_mx
WHERE
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1 year'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;

-- connect to one of the worker nodes
\c - - - :worker_1_port

-- Query #6 from the TPC-H decision support benchmark

SELECT
	sum(l_extendedprice * l_discount) as revenue
FROM
	lineitem_mx
WHERE
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1 year'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;

-- connect to the other worker node
\c - - - :worker_2_port

-- Query #6 from the TPC-H decision support benchmark

SELECT
	sum(l_extendedprice * l_discount) as revenue
FROM
	lineitem_mx
WHERE
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1 year'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
