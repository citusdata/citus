--
-- MULTI_LARGE_SHARDID
--

-- Load data into distributed tables, and run TPC-H query #1 and #6. This test
-- differs from previous tests in that it modifies the *internal* shardId
-- generator, forcing the distributed database to use 64-bit shard identifiers.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 100200300400500;

CREATE TABLE lineitem_large_shard_id AS SELECT * FROM lineitem;
SELECT create_distributed_table('lineitem_large_shard_id', 'l_orderkey');

-- Query #1 from the TPC-H decision support benchmark.

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
	lineitem_large_shard_id
WHERE
	l_shipdate <= date '1998-12-01' - interval '90 days'
GROUP BY
	l_returnflag,
	l_linestatus
ORDER BY
	l_returnflag,
	l_linestatus;

-- Query #6 from the TPC-H decision support benchmark.

SELECT
	sum(l_extendedprice * l_discount) as revenue
FROM
	lineitem_large_shard_id
WHERE
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1 year'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
