--
-- MULTI_HAVING_PUSHDOWN
--

CREATE TABLE lineitem_hash (LIKE lineitem);
SELECT create_distributed_table('lineitem_hash', 'l_orderkey', 'hash');
INSERT INTO lineitem_hash SELECT * FROM lineitem;

CREATE TABLE orders_hash (LIKE orders);
SELECT create_distributed_table('orders_hash', 'o_orderkey', 'hash');
INSERT INTO orders_hash SELECT * FROM orders;

SET client_min_messages TO DEBUG1;

-- push down when table is distributed by hash and grouped by partition column
SELECT l_orderkey, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash
    GROUP BY l_orderkey HAVING sum(l_quantity) > 24
    ORDER BY 2 DESC, 1 ASC LIMIT 3;

-- but don't push down when table is distributed by append
SELECT l_orderkey, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem
    GROUP BY l_orderkey HAVING sum(l_quantity) > 24
    ORDER BY 2 DESC, 1 ASC LIMIT 3;

-- and don't push down when not grouped by partition column
SELECT l_shipmode, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash
    GROUP BY l_shipmode HAVING sum(l_quantity) > 24
    ORDER BY 2 DESC, 1 ASC LIMIT 3;

-- push down if grouped by multiple rows one of which is partition column
SELECT l_shipmode, l_orderkey, sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash
    GROUP BY l_shipmode, l_orderkey HAVING sum(l_quantity) > 24
    ORDER BY 3 DESC, 1, 2 LIMIT 3;

-- couple more checks with joins
SELECT sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash, orders_hash
    WHERE o_orderkey = l_orderkey
    GROUP BY l_orderkey, o_orderkey, l_shipmode HAVING sum(l_quantity) > 24
    ORDER BY 1 DESC LIMIT 3;

SELECT sum(l_extendedprice * l_discount) as revenue
    FROM lineitem_hash, orders_hash
    WHERE o_orderkey = l_orderkey
    GROUP BY l_shipmode, o_clerk HAVING sum(l_quantity) > 24
    ORDER BY 1 DESC LIMIT 3;

SET client_min_messages TO NOTICE;

DROP TABLE lineitem_hash;
DROP TABLE orders_hash;
