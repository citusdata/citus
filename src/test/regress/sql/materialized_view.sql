---
--- materialized_view
---

-- This file contains test cases for materialized view support.


-- materialized views work
-- insert into... select works with views
CREATE SCHEMA materialized_view;
SET search_path TO materialized_view, public;

CREATE VIEW air_shipped_lineitems AS SELECT * FROM lineitem_hash_part WHERE l_shipmode = 'AIR';
CREATE TABLE temp_lineitem(LIKE lineitem_hash_part);
SELECT create_distributed_table('temp_lineitem', 'l_orderkey', 'hash', 'lineitem_hash_part');
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems;
SELECT count(*) FROM temp_lineitem;
-- following is a where false query, should not be inserting anything
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems WHERE l_shipmode = 'MAIL';
SELECT count(*) FROM temp_lineitem;

-- can create and query materialized views
CREATE MATERIALIZED VIEW mode_counts
AS SELECT l_shipmode, count(*) FROM temp_lineitem GROUP BY l_shipmode;

SELECT * FROM mode_counts WHERE l_shipmode = 'AIR' ORDER BY 2 DESC, 1 LIMIT 10;

-- materialized views are local, cannot join with distributed tables
SELECT count(*) FROM mode_counts JOIN temp_lineitem USING (l_shipmode);

-- new data is not immediately reflected in the view
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems;
SELECT * FROM mode_counts WHERE l_shipmode = 'AIR' ORDER BY 2 DESC, 1 LIMIT 10;

-- refresh updates the materialised view with new data
REFRESH MATERIALIZED VIEW mode_counts;
SELECT * FROM mode_counts WHERE l_shipmode = 'AIR' ORDER BY 2 DESC, 1 LIMIT 10;

DROP MATERIALIZED VIEW mode_counts;

DROP TABLE temp_lineitem CASCADE;

-- Refresh single-shard materialized view
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_hash_part.o_orderdate, total_price.price_sum
FROM lineitem_hash_part, orders_hash_part, (SELECT SUM(l_extendedprice) AS price_sum FROM lineitem_hash_part where l_orderkey=3) AS total_price
WHERE lineitem_hash_part.l_orderkey=orders_hash_part.o_orderkey AND lineitem_hash_part.l_orderkey=3;

REFRESH MATERIALIZED VIEW materialized_view;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

-- Refresh multi-shard materialized view
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_hash_part.o_orderdate, total_price.price_sum
FROM lineitem_hash_part, orders_hash_part, (SELECT SUM(l_extendedprice) AS price_sum FROM lineitem_hash_part) AS total_price
WHERE lineitem_hash_part.l_orderkey=orders_hash_part.o_orderkey;

REFRESH MATERIALIZED VIEW materialized_view;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

-- Refresh materialized view with CTE
CREATE MATERIALIZED VIEW materialized_view AS
WITH total_price AS (SELECT SUM(l_extendedprice) AS price_sum FROM lineitem_hash_part)
SELECT orders_hash_part.o_orderdate, total_price.price_sum
FROM lineitem_hash_part, orders_hash_part, total_price
WHERE lineitem_hash_part.l_orderkey=orders_hash_part.o_orderkey;

REFRESH MATERIALIZED VIEW materialized_view;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

-- Refresh materialized view with join
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_hash_part.o_orderdate, quantity_sum
FROM orders_hash_part JOIN (
    SELECT l_orderkey, SUM(l_quantity) AS quantity_sum
    FROM lineitem_hash_part
    GROUP BY l_orderkey
    ) AS total_quantity
    ON total_quantity.l_orderkey=orders_hash_part.o_orderkey;

REFRESH MATERIALIZED VIEW materialized_view;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

-- Refresh materialized view with reference tables
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_reference.o_orderdate, total_price.price_sum
FROM lineitem_hash_part, orders_reference, (SELECT SUM(o_totalprice) AS price_sum FROM orders_reference) AS total_price
WHERE lineitem_hash_part.l_orderkey=orders_reference.o_orderkey;

REFRESH MATERIALIZED VIEW materialized_view;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

-- Refresh materialized view with table distributed after creation of view
CREATE TABLE lineitem_local_to_hash_part AS SELECT * FROM lineitem_hash_part;
CREATE TABLE orders_local_to_hash_part AS SELECT * FROM orders_hash_part;
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_local_to_hash_part.o_orderdate, total_price.price_sum
FROM lineitem_local_to_hash_part, orders_local_to_hash_part, (SELECT SUM(l_extendedprice) AS price_sum FROM lineitem_local_to_hash_part) AS total_price
WHERE lineitem_local_to_hash_part.l_orderkey=orders_local_to_hash_part.o_orderkey;

SELECT create_distributed_table('lineitem_local_to_hash_part', 'l_orderkey');
SELECT create_distributed_table('orders_local_to_hash_part', 'o_orderkey');

REFRESH MATERIALIZED VIEW materialized_view;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;
DROP TABLE lineitem_local_to_hash_part;
DROP TABLE orders_local_to_hash_part;

-- Refresh materialized view WITH DATA
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_hash_part.o_orderdate, total_price.price_sum
FROM lineitem_hash_part, orders_hash_part, (SELECT SUM(l_extendedprice) AS price_sum FROM lineitem_hash_part) AS total_price
WHERE lineitem_hash_part.l_orderkey=orders_hash_part.o_orderkey;

REFRESH MATERIALIZED VIEW materialized_view WITH DATA;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

-- Refresh materialized view WITH NO DATA
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_hash_part.o_orderdate, total_price.price_sum
FROM lineitem_hash_part, orders_hash_part, (SELECT SUM(l_extendedprice) AS price_sum FROM lineitem_hash_part) AS total_price
WHERE lineitem_hash_part.l_orderkey=orders_hash_part.o_orderkey;

REFRESH MATERIALIZED VIEW materialized_view WITH NO DATA;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

-- Refresh materialized view CONCURRENTLY
CREATE MATERIALIZED VIEW materialized_view AS
SELECT orders_hash_part.o_orderdate, SUM(total_price.price_sum)
FROM lineitem_hash_part, orders_hash_part, (SELECT SUM(l_extendedprice) AS price_sum FROM lineitem_hash_part) AS total_price
WHERE lineitem_hash_part.l_orderkey=orders_hash_part.o_orderkey
GROUP BY orders_hash_part.o_orderdate;

CREATE UNIQUE INDEX materialized_view_index ON materialized_view (o_orderdate);
REFRESH MATERIALIZED VIEW CONCURRENTLY materialized_view;
SELECT count(*) FROM materialized_view;
DROP MATERIALIZED VIEW materialized_view;

DROP SCHEMA materialized_view CASCADE;