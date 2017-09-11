--
-- MULTI_LIMIT_CLAUSE
--


CREATE TABLE lineitem_hash (LIKE lineitem);
SELECT create_distributed_table('lineitem_hash', 'l_orderkey', 'hash');
INSERT INTO lineitem_hash SELECT * FROM lineitem;

-- Display debug messages on limit clause push down.

SET client_min_messages TO DEBUG1;

-- Check that we can correctly handle the Limit clause in distributed queries.
-- Note that we don't have the limit optimization enabled for these queries, and
-- will end up fetching all rows to the master database.

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY count_quantity ASC, l_quantity ASC;

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY count_quantity DESC, l_quantity DESC;

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY count_quantity ASC, l_quantity ASC LIMIT 5;

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY count_quantity ASC, l_quantity ASC LIMIT 10;

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY count_quantity DESC, l_quantity DESC LIMIT 10;

-- Check that we can handle limits for simple sort clauses. We order by columns
-- in the first two tests, and then by a simple expression in the last test.

SELECT min(l_orderkey) FROM  lineitem;
SELECT l_orderkey FROM lineitem ORDER BY l_orderkey ASC LIMIT 1;

SELECT max(l_orderkey) FROM lineitem;
SELECT l_orderkey FROM lineitem ORDER BY l_orderkey DESC LIMIT 1;
SELECT * FROM lineitem ORDER BY l_orderkey DESC, l_linenumber DESC LIMIT 3;

SELECT max(extract(epoch from l_shipdate)) FROM lineitem;
SELECT * FROM lineitem
	ORDER BY extract(epoch from l_shipdate) DESC, l_orderkey DESC LIMIT 3;

-- Exercise the scenario where order by clauses don't have any aggregates, and
-- that we can push down the limit as a result. Check that when this happens, we
-- also sort on all group by clauses behind the covers.

SELECT l_quantity, l_discount, avg(l_partkey) FROM lineitem
	GROUP BY l_quantity, l_discount
	ORDER BY l_quantity LIMIT 1;

-- Results from the previous query should match this query's results.

SELECT l_quantity, l_discount, avg(l_partkey) FROM lineitem
	GROUP BY l_quantity, l_discount
	ORDER BY l_quantity, l_discount LIMIT 1;


-- We can push down LIMIT clause when we group by partition column of a hash
-- partitioned table.
SELECT l_orderkey, count(DISTINCT l_partkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 1 DESC LIMIT 5;

SELECT l_orderkey
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 5;

-- Don't push down if not grouped by partition column.
SELECT max(l_orderkey)
	FROM lineitem_hash
	GROUP BY l_linestatus
	ORDER BY 1 DESC LIMIT 2;

-- Don't push down if table is distributed by append
SELECT l_orderkey, max(l_shipdate)
	FROM lineitem
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 1 LIMIT 5;

-- Push down if grouped by multiple columns one of which is partition column.
SELECT
	l_linestatus, l_orderkey, max(l_shipdate)
	FROM lineitem_hash
	GROUP BY l_linestatus, l_orderkey
	ORDER BY 3 DESC, 1, 2 LIMIT 5;

-- Don't push down if grouped by multiple columns none of which is partition column.
SELECT
	l_linestatus, l_shipmode, max(l_shipdate)
	FROM lineitem_hash
	GROUP BY l_linestatus, l_shipmode
	ORDER BY 3 DESC, 1, 2 LIMIT 5;


SET client_min_messages TO NOTICE;
DROP TABLE lineitem_hash;
