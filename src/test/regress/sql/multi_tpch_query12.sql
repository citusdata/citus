--
-- MULTI_TPCH_QUERY12
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 910000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 910000;


-- Change configuration to treat lineitem and orders tables as large

SET citus.large_table_shard_count TO 2;

-- Query #12 from the TPC-H decision support benchmark

SELECT
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			 OR o_orderpriority = '2-HIGH'
		then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			 AND o_orderpriority <> '2-HIGH'
		then 1
		else 0
		end) AS low_line_count
FROM
	orders,
	lineitem
WHERE
	o_orderkey = l_orderkey
	AND l_shipmode in ('MAIL', 'SHIP')
	AND l_commitdate < l_receiptdate
	AND l_shipdate < l_commitdate
	AND l_receiptdate >= date '1994-01-01'
	AND l_receiptdate < date '1994-01-01' + interval '1' year
GROUP BY
	l_shipmode
ORDER BY
	l_shipmode;
