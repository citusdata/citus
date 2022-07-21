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

-- can create router materialized views
CREATE MATERIALIZED VIEW mode_counts_router
AS SELECT l_shipmode, count(*) FROM temp_lineitem WHERE  l_orderkey = 1 GROUP BY l_shipmode;
SELECT  * FROM mode_counts_router;

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

-- modify statements on/with views
-- create two tables and a view
CREATE TABLE large (id int, tenant_id int);
CREATE TABLE small (id int, tenant_id int);

SELECT create_distributed_table('large','tenant_id');
SELECT create_distributed_table('small','tenant_id');

\copy small FROM STDIN DELIMITER ','
250, 25
470, 13
8,2
6,3
7,4
1,2
\.

CREATE MATERIALIZED VIEW small_view AS SELECT * from small where id < 100;

\copy large FROM STDIN DELIMITER ','
1,2
2,3
5,4
6,5
\.

-- running any kind of modify statements "on" materialized views is not supported by postgres
UPDATE small_view SET id = 1;

-- for now, using materialized views in modify statements' FROM / WHERE clauses is not supported
UPDATE large SET id=20 FROM small_view WHERE small_view.id=large.id;
SELECT * FROM large ORDER BY 1, 2;

-- test on a router executable update statement, this will also fail
UPDATE large SET id=28 FROM small_view WHERE small_view.id=large.id and small_view.tenant_id=2 and large.tenant_id=2;
SELECT * FROM large ORDER BY 1, 2;

-- delete statement on large with subquery, this should succeed
DELETE FROM large WHERE tenant_id in (SELECT tenant_id FROM small_view);
SELECT * FROM large ORDER BY 1, 2;

-- INSERT INTO views is already not supported by PostgreSQL
INSERT INTO small_view VALUES(3, 3);

DROP TABLE small CASCADE;
DROP TABLE large;

-- now, run the same modify statement tests on a partitioned table
CREATE TABLE small (id int, tenant_id int);

CREATE TABLE large_partitioned (id int, tenant_id int) partition by range(tenant_id);

CREATE TABLE large_partitioned_p1 PARTITION OF large_partitioned FOR VALUES FROM (1) TO (10);
CREATE TABLE large_partitioned_p2 PARTITION OF large_partitioned FOR VALUES FROM (10) TO (20);
CREATE TABLE large_partitioned_p3 PARTITION OF large_partitioned FOR VALUES FROM (20) TO (100);

SELECT create_distributed_table('large_partitioned','tenant_id');
SELECT create_distributed_table('small','tenant_id');

\copy small FROM STDIN DELIMITER ','
250, 25
470, 13
8,2
6,3
7,4
1,2
\.

CREATE MATERIALIZED VIEW small_view AS SELECT * from small where id < 100;

\copy large_partitioned FROM STDIN DELIMITER ','
1,2
2,3
5,4
6,5
29,15
26,32
60,51
\.

-- running modify statements "on" views is still not supported, hence below two statements will fail
UPDATE small_view SET id = 1;
DELETE FROM small_view;

-- using mat. view in modify statements' FROM / WHERE clauses is not valid yet
UPDATE large_partitioned SET id=20 FROM small_view WHERE small_view.id=large_partitioned.id;
SELECT * FROM large_partitioned ORDER BY 1, 2;

-- delete statement on large_partitioned
DELETE FROM large_partitioned WHERE id in (SELECT id FROM small_view);
SELECT * FROM large_partitioned ORDER BY 1, 2;

-- we should still have identical rows for next test statement, then insert new rows to both tables
INSERT INTO large_partitioned VALUES(14, 14);
INSERT INTO small VALUES(14, 14);

-- delete statement with CTE
WITH all_small_view_ids AS (SELECT id FROM small_view)
DELETE FROM large_partitioned WHERE id in (SELECT * FROM all_small_view_ids);

-- make sure that materialized view in a CTE/subquery can be joined with a distributed table
WITH cte AS (SELECT *, random() FROM small_view) SELECT count(*) FROM cte JOIN small USING(id);
SELECT count(*) FROM (SELECT *, random() FROM small_view) as subquery JOIN small USING(id);

CREATE MATERIALIZED VIEW only_intermedate_result AS
	WITH cte_1 AS (SELECT * FROM small OFFSET 0) SELECT * FROM cte_1 ORDER BY 1,2;
SELECT * FROM only_intermedate_result ORDER BY 1,2;
INSERT INTO small VALUES (1000000,1000000);
REFRESH MATERIALIZED VIEW only_intermedate_result;
SELECT * FROM only_intermedate_result ORDER BY 1,2;

DROP TABLE large_partitioned;
DROP TABLE small CASCADE;
