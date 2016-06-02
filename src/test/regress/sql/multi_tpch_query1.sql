--
-- MULTI_TPCH_QUERY1
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 890000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 890000;


-- Change configuration to treat lineitem and orders tables as large

SET citus.large_table_shard_count TO 2;

-- Query #1 from the TPC-H decision support benchmark

SELECT
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
FROM
	lineitem
WHERE
	l_shipdate <= date '1998-12-01' - interval '90 days'
GROUP BY
	l_returnflag,
	l_linestatus
ORDER BY
	l_returnflag,
	l_linestatus;
