--
-- MULTI_TPCH_QUERY10
--

-- Query #10 from the TPC-H decision support benchmark. Unlike other TPC-H tests,
-- we don't set citus.large_table_shard_count here, and instead use the default value
-- coming from postgresql.conf or multi_task_tracker_executor.conf.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 900000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 900000;


SELECT
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
	revenue DESC
LIMIT 20;
