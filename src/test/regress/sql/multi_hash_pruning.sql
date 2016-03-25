--
-- MULTI_HASH_PRUNING
--

-- Tests for shard and join pruning logic on hash partitioned tables.

-- Create a table partitioned on integer column and update partition type to
-- hash. Then stage data to this table and update shard min max values with
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
SELECT master_create_distributed_table('orders_hash_partitioned', 'o_orderkey', 'append');

UPDATE pg_dist_partition SET partmethod = 'h'
	WHERE logicalrelid = 'orders_hash_partitioned'::regclass;

-- Create logical shards with shardids 110, 111, 112 and 113

INSERT INTO pg_dist_shard (logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue)
	VALUES ('orders_hash_partitioned'::regclass, 110, 't', -1905060026, -1905060026),
	       ('orders_hash_partitioned'::regclass, 111, 't', 1134484726, 1134484726),
	       ('orders_hash_partitioned'::regclass, 112, 't', -1905060026, -28094569),
	       ('orders_hash_partitioned'::regclass, 113, 't', -1011077333, 0);

-- Create shard placements for shards 110, 111, 112 and 113

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 110, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 1;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 111, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 1;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 112, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 1;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 113, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 1;

SET client_min_messages TO DEBUG2;

-- Check that we can prune shards for simple cases, boolean expressions and
-- immutable functions.

EXPLAIN SELECT count(*) FROM orders_hash_partitioned;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 1;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 2;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 3;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = 4;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey is NULL;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey is not NULL;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey > 2;

EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey = 2;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_clerk = 'aaa';
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 AND o_clerk = 'aaa';
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR (o_orderkey = 3 AND o_clerk = 'aaa');
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey is NULL;
EXPLAIN SELECT count(*) FROM
       (SELECT o_orderkey FROM orders_hash_partitioned WHERE o_orderkey = 1) AS orderkeys;

EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = abs(-1);

-- Check that we don't support pruning for ANY (array expression) and give
-- a notice message when used with the partition column
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = ANY ('{1,2,3}');

-- Check that we don't show the message if the operator is not
-- equality operator
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey < ALL ('{1,2,3}');

-- Check that we don't give a spurious hint message when non-partition 
-- columns are used with ANY/IN/ALL
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_totalprice IN (2, 5);

-- Check that we cannot prune for mutable functions.

EXPLAIN SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = random();
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = random() OR o_orderkey = 1;
EXPLAIN SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = random() AND o_orderkey = 1;

-- Check that we can do join pruning.

EXPLAIN SELECT count(*)
	FROM orders_hash_partitioned orders1, orders_hash_partitioned orders2
	WHERE orders1.o_orderkey = orders2.o_orderkey;

EXPLAIN SELECT count(*)
	FROM orders_hash_partitioned orders1, orders_hash_partitioned orders2
	WHERE orders1.o_orderkey = orders2.o_orderkey
	AND orders1.o_orderkey = 1
	AND orders2.o_orderkey is NULL;

SET client_min_messages TO NOTICE;
