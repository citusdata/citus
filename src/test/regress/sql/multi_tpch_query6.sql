--
-- MULTI_TPCH_QUERY6
--


-- Query #6 from the TPC-H decision support benchmark

SELECT
	sum(l_extendedprice * l_discount) as revenue
FROM
	lineitem
WHERE
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1 year'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
