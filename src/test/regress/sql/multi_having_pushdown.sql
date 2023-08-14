--
-- MULTI_HAVING_PUSHDOWN
--

SET citus.next_shard_id TO 590000;

CREATE TABLE lineitem_hash (LIKE lineitem);
SELECT create_distributed_table('lineitem_hash', 'l_orderkey', 'hash');

CREATE TABLE orders_hash (LIKE orders);
SELECT create_distributed_table('orders_hash', 'o_orderkey', 'hash');

-- push down when table is distributed by hash and grouped by partition column
EXPLAIN (COSTS FALSE)
    SELECT l_orderkey, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash
    GROUP BY l_orderkey HAVING sum(l_quantity) > 24
    ORDER BY 2 DESC, 1 ASC LIMIT 3;

-- but don't push down when table is distributed by append
EXPLAIN (COSTS FALSE)
    SELECT l_orderkey, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem
    GROUP BY l_orderkey HAVING sum(l_quantity) > 24
    ORDER BY 2 DESC, 1 ASC LIMIT 3;

-- and don't push down when not grouped by partition column
EXPLAIN (COSTS FALSE)
    SELECT l_shipmode, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash
    GROUP BY l_shipmode HAVING sum(l_quantity) > 24
    ORDER BY 2 DESC, 1 ASC LIMIT 3;

-- push down if grouped by multiple rows one of which is partition column
EXPLAIN (COSTS FALSE)
    SELECT l_shipmode, l_orderkey, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash
    GROUP BY l_shipmode, l_orderkey HAVING sum(l_quantity) > 24
    ORDER BY 3 DESC, 1, 2 LIMIT 3;

-- couple more checks with joins
EXPLAIN (COSTS FALSE)
    SELECT sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash, orders_hash
    WHERE o_orderkey = l_orderkey
    GROUP BY l_orderkey, l_shipmode HAVING sum(l_quantity) > 24
    ORDER BY 1 DESC LIMIT 3;

EXPLAIN (COSTS FALSE)
    SELECT sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash, orders_hash
    WHERE o_orderkey = l_orderkey
    GROUP BY l_shipmode, o_clerk HAVING sum(l_quantity) > 24
    ORDER BY 1 DESC LIMIT 3;

DROP TABLE lineitem_hash;
DROP TABLE orders_hash;

SELECT max(value_1)
FROM users_table
GROUP BY user_id
HAVING max(value_2) > 4 AND min(value_2) < 1
ORDER BY 1;

SELECT max(value_1)
FROM users_table
GROUP BY user_id
HAVING max(value_2) > 4 AND min(value_2) < 1 OR count(*) > 10
ORDER BY 1;

SELECT max(value_1)
FROM users_table
GROUP BY user_id
HAVING max(value_2) > 4 AND min(value_2) < 1 AND count(*) > 20
ORDER BY 1;

SELECT max(value_1)
FROM users_table
GROUP BY user_id
HAVING max(value_2) > 0 AND count(*) FILTER (WHERE value_3=2) > 3 AND min(value_2) IN (0,1,2,3);
