--
-- MULTI_REPARTITION_JOIN_PLANNING
--
-- Tests that cover repartition join planning. Note that we explicitly start a
-- transaction block here so that we don't emit debug messages with changing
-- transaction ids in them.


SET citus.next_shard_id TO 690000;
SET citus.enable_unique_job_ids TO off;
SET citus.enable_repartition_joins to ON;
SET citus.shard_replication_factor to 1;

create schema repartition_join;
DROP TABLE IF EXISTS repartition_join.order_line;
CREATE TABLE order_line (
  ol_w_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_o_id int NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int NOT NULL,
  ol_quantity decimal(2,0) NOT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
);

DROP TABLE IF EXISTS repartition_join.stock;
CREATE TABLE stock (
  s_w_id int NOT NULL,
  s_i_id int NOT NULL,
  s_quantity decimal(4,0) NOT NULL,
  PRIMARY KEY (s_w_id,s_i_id)
);

SELECT create_distributed_table('order_line','ol_w_id');
SELECT create_distributed_table('stock','s_w_id');

BEGIN;
SET client_min_messages TO DEBUG;

-- Debug4 log messages display jobIds within them. We explicitly set the jobId
-- sequence here so that the regression output becomes independent of the number
-- of jobs executed prior to running this test.


-- Multi-level repartition join to verify our projection columns are correctly
-- referenced and propagated across multiple repartition jobs. The test also
-- validates that only the minimal necessary projection columns are transferred
-- between jobs.

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, part_append, orders, customer_append
WHERE
	l_orderkey = o_orderkey AND
	l_partkey = p_partkey AND
	c_custkey = o_custkey AND
        (l_quantity > 5.0 OR l_extendedprice > 1200.0) AND
        p_size > 8 AND o_totalprice > 10.0 AND
        c_acctbal < 5000.0 AND l_partkey < 1000
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey;

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, orders
WHERE
	l_suppkey = o_shippriority AND
        l_quantity < 5.0 AND o_totalprice <> 4.0
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey;

-- Check that grouping by primary key allows o_shippriority to be in the target list
SELECT
	o_orderkey, o_shippriority, count(*)
FROM
	lineitem, orders
WHERE
	l_suppkey = o_shippriority
GROUP BY
	o_orderkey
ORDER BY
	o_orderkey;

-- Check that grouping by primary key allows o_shippriority to be in the target
-- list
-- Postgres removes o_shippriority from the group by clause here
SELECT
	o_orderkey, o_shippriority, count(*)
FROM
	lineitem, orders
WHERE
	l_suppkey = o_shippriority
GROUP BY
	o_orderkey, o_shippriority
ORDER BY
	o_orderkey;

-- Check that calling any_value manually works as well
SELECT
	o_orderkey, any_value(o_shippriority)
FROM
	lineitem, orders
WHERE
	l_suppkey = o_shippriority
GROUP BY
	o_orderkey, o_shippriority
ORDER BY
	o_orderkey;


-- Check that grouping by primary key allows s_quantity to be in the having
-- list
-- Postgres removes s_quantity from the group by clause here

select  s_i_id
    from  stock, order_line
    where ol_i_id=s_i_id
    group by s_i_id, s_w_id, s_quantity
    having   s_quantity > random()
;

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;
COMMIT;

drop schema repartition_join;
