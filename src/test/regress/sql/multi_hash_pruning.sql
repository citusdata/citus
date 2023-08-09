--
-- MULTI_HASH_PRUNING
--

-- Tests for shard and join pruning logic on hash partitioned tables.


SET citus.next_shard_id TO 630000;
SET citus.shard_count to 4;
SET citus.shard_replication_factor to 1;

-- many of the tests in this file is intended for testing non-fast-path
-- router planner, so we're explicitly disabling it in this file.
-- We've bunch of other tests that triggers fast-path-router
SET citus.enable_fast_path_router_planner TO false;


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
SELECT create_distributed_table('orders_hash_partitioned', 'o_orderkey');

CREATE TABLE lineitem_hash_partitioned (
    l_orderkey integer not null,
    l_partkey integer not null,
    l_suppkey integer not null,
    l_linenumber integer not null,
    l_quantity decimal(15, 2) not null,
    l_extendedprice decimal(15, 2) not null,
    l_discount decimal(15, 2) not null,
    l_tax decimal(15, 2) not null,
    l_returnflag char(1) not null,
    l_linestatus char(1) not null,
    l_shipdate date not null,
    l_commitdate date not null,
    l_receiptdate date not null,
    l_shipinstruct char(25) not null,
    l_shipmode char(10) not null,
    l_comment varchar(44) not null,
    PRIMARY KEY(l_orderkey, l_linenumber) );
SELECT create_distributed_table('lineitem_hash_partitioned', 'l_orderkey');

INSERT INTO orders_hash_partitioned (o_orderkey, o_custkey, o_totalprice, o_shippriority, o_clerk) VALUES
	(1, 11, 10, 111, 'aaa'),
	(2, 22, 20, 222, 'bbb'),
	(3, 33, 30, 333, 'ccc'),
	(4, 44, 40, 444, 'ddd');

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
	WHERE o_orderkey = 1 OR (o_orderkey = 3 AND o_clerk = 'ccc');
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey is NULL;
SELECT count(*) FROM
       (SELECT o_orderkey FROM orders_hash_partitioned WHERE o_orderkey = 1) AS orderkeys;

SET client_min_messages TO DEBUG3;

-- Check that we support runing for ANY/IN with literal.
SELECT count(*) FROM lineitem_hash_part
	WHERE l_orderkey = ANY ('{1,2,3}');

SELECT count(*) FROM lineitem_hash_part
	WHERE l_orderkey IN (1,2,3);

-- Check whether we can deal with null arrays
SELECT count(*) FROM lineitem_hash_part
	WHERE l_orderkey IN (NULL);

SELECT count(*) FROM lineitem_hash_part
	WHERE l_orderkey = ANY (NULL);

SELECT count(*) FROM lineitem_hash_part
	WHERE l_orderkey IN (NULL) OR TRUE;

SELECT count(*) FROM lineitem_hash_part
	WHERE l_orderkey = ANY (NULL) OR TRUE;

-- Check whether we support IN/ANY in subquery
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey IN (SELECT l_orderkey FROM lineitem_hash_part);
SELECT count(*) FROM lineitem_hash_part WHERE l_orderkey = ANY (SELECT l_orderkey FROM lineitem_hash_part);

-- Check whether we support range queries
SELECT count(*) FROM lineitem
	WHERE l_orderkey >= 1 AND l_orderkey <= 3;

SELECT count(*) FROM lineitem
	WHERE (l_orderkey >= 1 AND l_orderkey <= 3) AND (l_quantity > 11 AND l_quantity < 22);

-- Check whether we support IN/ANY in subquery
SELECT count(*) FROM lineitem
	WHERE l_orderkey = ANY ('{1,2,3}');

SELECT count(*) FROM lineitem
	WHERE l_orderkey IN (1,2,3);

SELECT count(*) FROM lineitem
	WHERE l_orderkey = ANY(NULL) OR TRUE;

SELECT count(*) FROM lineitem_range
	WHERE l_orderkey = ANY ('{1,2,3}');

SELECT count(*) FROM lineitem_range
	WHERE l_orderkey IN (1,2,3);

SELECT count(*) FROM lineitem_range
	WHERE l_orderkey = ANY(NULL) OR TRUE;

-- Check that we don't show the message if the operator is not
-- equality operator
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey < ALL ('{1,2,3}');

-- Check that we don't give a spurious hint message when non-partition
-- columns are used with ANY/IN/ALL
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_totalprice IN (20, 30);

-- Check that we cannot prune for mutable functions.

SELECT count(*) FROM orders_hash_partitioned WHERE o_orderkey = (random() + 100);
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = (random() + 100) OR o_orderkey = 1;
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = (random() + 100) AND o_orderkey = 1;

-- Check that we can do join pruning.

SELECT count(*)
	FROM orders_hash_partitioned orders1, orders_hash_partitioned orders2
	WHERE orders1.o_orderkey = orders2.o_orderkey;

SELECT count(*)
	FROM orders_hash_partitioned orders1, orders_hash_partitioned orders2
	WHERE orders1.o_orderkey = orders2.o_orderkey
	AND orders1.o_orderkey = 1
	AND orders2.o_orderkey is NULL;


-- All shards used without constraints
SELECT count(*) FROM orders_hash_partitioned;

-- Shards restricted correctly with prunable constraint
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1;

-- Shards restricted correctly with prunable constraint ANDed with unprunable expression using OR
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 AND (o_custkey = 11 OR o_custkey = 22);

-- Shards restricted correctly with prunable constraints ORed
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey = 1 OR o_orderkey = 2);

-- Shards restricted correctly with prunable constraints ANDed with unprunable expression using OR
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey = 1 OR o_orderkey = 2) AND (o_custkey = 11 OR o_custkey = 22);

-- Shards restricted correctly with many different prunable constraints ORed
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey = 1 AND o_custkey = 11) OR (o_orderkey = 1 AND o_custkey = 33) OR (o_orderkey = 2 AND o_custkey = 22) OR (o_orderkey = 2 AND o_custkey = 44);

-- Shards restricted correctly with prunable SAO constraint ANDed with unprunable expression using OR
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey IN (1,2)) AND (o_custkey = 11 OR o_custkey = 22 OR o_custkey = 33);

-- Shards restricted correctly with prunable SAO constraint ANDed with multiple unprunable expressions
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey IN (1,2)) AND (o_totalprice < 11 OR o_totalprice > 19) AND o_shippriority > 100 AND (o_custkey = 11 OR o_custkey = 22);

-- Shards restricted correctly with prunable SAO constraints ORed
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey IN (1,2) AND o_custkey = 11) OR (o_orderkey IN (2,3) AND o_custkey = 22);

-- All shards used with prunable expression ORed with unprunable expression
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey IN (1,2) OR o_custkey = 33;

-- Shards restricted correctly with prunable constraint ORed
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR ((o_orderkey = 2 AND o_custkey = 22) OR (o_orderkey = 3 AND o_custkey = 33));

-- Shards restricted correctly with prunable constraint ORed with falsy expression
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR (o_orderkey = 2 AND (o_custkey = 11 OR (o_orderkey = 3 AND o_custkey = 44)));

-- Shards restricted correctly with prunable SAO constraint ORed with prunable nested EQ constraint
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey IN (1,2)) AND (o_custkey = 11 OR o_custkey = 22 OR o_custkey = 33) AND o_totalprice <= 20;

-- Shards restricted correctly with prunable SAO constraint ANDed with unprunable expressions
SELECT count(*) FROM orders_hash_partitioned
	WHERE (o_orderkey IN (1,2)) AND (o_custkey = 11 OR o_custkey = 33) AND o_custkey = 22;

-- All shards used with prunable SAO constraint ORed with unprunable nested expression
SELECT count(*) FROM orders_hash_partitioned
	WHERE ((o_orderkey IN (1,2)) AND (o_custkey = 11 OR o_custkey = 22)) OR o_custkey = 33;

-- Shards restricted correctly with prunable SAO constraint ORed with prunable nested EQ constraint
SELECT count(*) FROM orders_hash_partitioned
	WHERE ((o_orderkey IN (1,2)) AND (o_custkey = 11 OR o_custkey = 22)) OR (o_orderkey = 3 AND o_custkey = 33);

-- All shards used with ORed top level unprunable expression
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_custkey = 11 OR (o_orderkey = 2 AND o_custkey = 22);

-- Single shard used when deeply nested prunable expression is restrictive with nested ANDs
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR (o_orderkey = 2 AND (o_orderkey = 3 OR (o_orderkey = 1 AND o_custkey = 11)));

-- Single shard used when top prunable expression is restrictive with nested ANDs
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 AND ((o_orderkey = 2 OR o_orderkey = 3) AND (o_custkey = 11 OR o_custkey = 22));

-- Deeply nested prunable expression affects used shards
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR ((o_orderkey = 2 OR o_orderkey = 3) AND (o_custkey = 22 OR o_custkey = 33));

-- Deeply nested non prunable expression uses all shards
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR ((o_orderkey = 2 OR o_custkey = 11) AND (o_custkey = 22 OR o_custkey = 33));

-- a OR partkey != x Uses all shards
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey != 2;

-- a OR partkey IS NULL Uses all shards
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey IS NULL;

-- a OR partkey IS NOT NULL Uses all shards
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey = 1 OR o_orderkey IS NOT NULL;

-- Check that NOT is handled with NEQs ORed
SELECT count(*) FROM orders_hash_partitioned
	WHERE NOT (o_orderkey != 2 OR o_orderkey != 3);

-- Check that NOT is handled with EQs ORed
SELECT count(*) FROM orders_hash_partitioned
	WHERE NOT (o_orderkey = 2 OR o_orderkey = 3);

-- Check that NOT is handled with NEQs ANDed
SELECT count(*) FROM orders_hash_partitioned
	WHERE NOT (o_orderkey != 2 AND o_orderkey != 3);

-- Check that NOT is handled with EQs ANDed
SELECT count(*) FROM orders_hash_partitioned
	WHERE NOT (o_orderkey = 2 AND o_orderkey = 3);


-- Check that subquery NOT is pruned when ANDed to a valid constraint
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey IN (1,2) AND o_custkey NOT IN (SELECT o_custkey FROM orders_hash_partitioned WHERE o_orderkey = 1);

-- Check that subquery NOT is unpruned when ORed to a valid constraint
SELECT count(*) FROM orders_hash_partitioned
	WHERE o_orderkey IN (1,2) OR o_custkey NOT IN (SELECT o_custkey FROM orders_hash_partitioned WHERE o_orderkey = 3);

SET client_min_messages TO DEFAULT;

-- left joins should prune shards based on the left hand side of the left join
-- it should only assign 2 tasks as there is a filter on the left table pruning to 2
-- shards
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM orders_hash_partitioned
LEFT JOIN lineitem_hash_partitioned ON (o_orderkey = l_orderkey)
WHERE o_orderkey IN (1, 2);

EXPLAIN (COSTS OFF)
SELECT count(*)
FROM orders_hash_partitioned
INNER JOIN lineitem_hash_partitioned ON (o_orderkey = l_orderkey)
WHERE o_orderkey IN (1, 2);

-- same principle but on a right join
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM orders_hash_partitioned
RIGHT JOIN lineitem_hash_partitioned ON (o_orderkey = l_orderkey)
WHERE l_orderkey IN (1, 2);

-- full outerjoin should only prune partitions that will not return any rows. In short it
-- should cause a union of the FROM and FULL OUTER JOIN tables.
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM orders_hash_partitioned
FULL OUTER JOIN lineitem_hash_partitioned ON (o_orderkey = l_orderkey)
WHERE o_orderkey IN (1, 2)
   OR l_orderkey IN (2, 3);

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM orders_hash_partitioned
FULL OUTER JOIN lineitem_hash_partitioned ON (o_orderkey = l_orderkey)
WHERE o_orderkey IN (1, 2)
   AND l_orderkey IN (2, 3);
$Q$);

SET citus.task_executor_type TO DEFAULT;

DROP TABLE lineitem_hash_partitioned;
