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

-- HAVING aggregate arguments should preserve their original variable references
CREATE TABLE having_dist_1 (key boolean);
CREATE TABLE having_dist_2 (key boolean);
CREATE TABLE having_ref (value boolean);

SELECT create_distributed_table('having_dist_1', 'key');
SELECT create_distributed_table('having_dist_2', 'key');
SELECT create_reference_table('having_ref');

INSERT INTO having_dist_1 VALUES (true);
INSERT INTO having_dist_2 VALUES (true);
INSERT INTO having_ref VALUES (false), (NULL);

SELECT having_dist_1.key, having_dist_2.key, having_ref.value
FROM having_dist_1
JOIN having_dist_2 ON having_dist_1.key = having_dist_2.key,
having_ref
GROUP BY having_dist_1.key, having_dist_2.key, having_ref.value
HAVING EVERY(having_ref.value);

DROP TABLE having_dist_1;
DROP TABLE having_dist_2;
DROP TABLE having_ref;

-- HAVING FILTER expressions should preserve their original variable references
CREATE TABLE having_int_dist_1 (key int);
CREATE TABLE having_int_dist_2 (key int);
CREATE TABLE having_filter_ref (value boolean);

SELECT create_distributed_table('having_int_dist_1', 'key');
SELECT create_distributed_table('having_int_dist_2', 'key');
SELECT create_reference_table('having_filter_ref');

INSERT INTO having_int_dist_1 VALUES (1);
INSERT INTO having_int_dist_2 VALUES (1);
INSERT INTO having_filter_ref VALUES (true), (false), (NULL);

SELECT having_int_dist_1.key, having_int_dist_2.key, having_filter_ref.value
FROM having_int_dist_1
JOIN having_int_dist_2 ON having_int_dist_1.key = having_int_dist_2.key,
having_filter_ref
GROUP BY having_int_dist_1.key, having_int_dist_2.key, having_filter_ref.value
HAVING count(*) FILTER (WHERE having_filter_ref.value) > 0;

DROP TABLE having_int_dist_1;
DROP TABLE having_int_dist_2;
DROP TABLE having_filter_ref;
