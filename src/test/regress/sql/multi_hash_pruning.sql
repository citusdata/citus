--
-- MULTI_HASH_PRUNING
--

-- Tests for shard and join pruning logic on hash partitioned tables.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 630000;

-- Create a table partitioned on integer column and update partition type to
-- hash. Then load data into this table and update shard min max values with
-- hashed ones. Hash value of 1, 2, 3  and 4 are consecutively -1905060026,
-- 1134484726, -28094569 and -1011077333.

CREATE TABLE orders_hash_partitioned (
	o_orderkey integer,
	o_custkey integer,
	o_orderstatus char(1),
	o_totalprice decimal(15,2),
	o_orderdate date,
	o_orderpriority char(15),
	o_clerk char(15),
	o_shippriority integer,
	o_comment varchar(79) );
SELECT master_create_distributed_table('orders_hash_partitioned', 'o_orderkey', 'hash');
SELECT master_create_worker_shards('orders_hash_partitioned', 4, 1);

SET client_min_messages TO DEBUG2;

-- Check that we can prune shards for simple cases, boolean expressions and
-- immutable functions.


SELECT count(*) FROM orders_hash_partitioned;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 1;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 2;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 3;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 4;
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 AND o_clerk = 'aaa';
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = abs(-1);

-- disable router planning
SET citus.enable_router_execution TO 'false';
SELECT count(*) FROM orders_hash_partitioned;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 1;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 2;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 3;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 4;
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 AND o_clerk = 'aaa';
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = abs(-1);

SET citus.enable_router_execution TO DEFAULT;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey is NULL;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey is not NULL;
SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey > 2;

SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey = 2;
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_clerk = 'aaa';
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR (o_orderkey = 3 AND o_clerk = 'aaa');
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey is NULL;
SELECT count(*) FROM
       (SELECT o_orderkey FROM orders_hash_partitioned WHERE o_orderkey = 1) AS orderkeys;

-- Check that we don't support pruning for ANY (array expression) and give
-- a notice message when used with the partition column
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = ANY ('{1,2,3}');

-- Check that we don't show the message if the operator is not
-- equality operator
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey < ALL ('{1,2,3}');

-- Check that we don't give a spurious hint message when non-partition 
-- columns are used with ANY/IN/ALL
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_totalprice IN (2, 5);

-- Check that we cannot prune for mutable functions.

SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = random();
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = random() OR o_orderkey = 1;
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = random() AND o_orderkey = 1;

-- Check that we can do join pruning.

SELECT count(*)
	FROM orders_hash_partitioned orders1, orders_hash_partitioned orders2
	WHERE orders1.o_orderkey = orders2.o_orderkey;

SELECT count(*)
	FROM orders_hash_partitioned orders1, orders_hash_partitioned orders2
	WHERE orders1.o_orderkey = orders2.o_orderkey
	AND orders1.o_orderkey = 1
	AND orders2.o_orderkey is NULL;
