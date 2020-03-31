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

-- Push down limit even if there is distinct on
SELECT
	DISTINCT ON (l_orderkey, l_linenumber) l_orderkey, l_linenumber
	FROM lineitem_hash
	GROUP BY l_orderkey, l_linenumber
	ORDER BY l_orderkey, l_linenumber
	LIMIT 5;

-- Don't push down limit when group by clause not included in distinct on
SELECT
	DISTINCT ON (l_linenumber) l_orderkey, l_linenumber
	FROM lineitem_hash
	GROUP BY l_orderkey, l_linenumber
	ORDER BY l_linenumber, l_orderkey
	LIMIT 5;

-- Push down limit when there is const in distinct on
-- referring to a column such that group by clause
-- list is contained in distinct on
SELECT
	DISTINCT ON (l_linenumber, 1) l_orderkey, l_linenumber
	FROM lineitem_hash
	GROUP BY l_orderkey, l_linenumber
	ORDER BY l_linenumber, l_orderkey
	LIMIT 5;

-- Don't push down limit when there is const expression in distinct on
-- even if there is a group by on the expression
-- This is due to fact that postgres removes (1+1) from distinct on
-- clause but keeps it in group by list.
SELECT
	DISTINCT ON (l_linenumber, 1+1, l_linenumber) l_orderkey, l_linenumber
	FROM lineitem_hash
	GROUP BY l_orderkey, (1+1), l_linenumber
	ORDER BY l_linenumber, (1+1), l_orderkey
	LIMIT 5;

-- Don't push down limit when there is const reference
-- does not point to a column to make distinct clause superset
-- of group by
SELECT
	DISTINCT ON (l_linenumber, 2) l_orderkey, l_linenumber
	FROM lineitem_hash
	GROUP BY l_orderkey, l_linenumber
	ORDER BY l_linenumber, l_orderkey
	LIMIT 5;

-- Push down limit even when there is a column expression
-- in distinct clause provided that distinct clause covers
-- group by expression, and there is no aggregates in the query.
SELECT
	DISTINCT ON (l_orderkey + 1) l_orderkey + 1
	FROM lineitem_hash
	GROUP BY l_orderkey + 1
	ORDER BY l_orderkey + 1
	LIMIT 5;

-- Limit is not pushed down when there are aggregates in the query
-- This is because group by is not on distribution column itself
-- but on an expression on distribution column
SELECT
	DISTINCT ON (l_orderkey + 1, count(*)) l_orderkey + 1, count(*)
	FROM lineitem_hash
	GROUP BY l_orderkey + 1
	ORDER BY l_orderkey + 1 , 2
	LIMIT 5;

-- same query with column instead of column expression, limit is pushed down
-- because group by is on distribution column
SELECT
	DISTINCT ON (l_orderkey, count(*)) l_orderkey, count(*)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY l_orderkey , 2
	LIMIT 5;

-- limit is not pushed down because distinct clause
-- does not cover group by clause
SELECT
	DISTINCT ON (count(*)) l_orderkey, count(*)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 1
	LIMIT 2;

-- push down limit if there is a window function in distinct on
SELECT
	DISTINCT ON (l_orderkey, RANK() OVER (partition by l_orderkey)) l_orderkey, RANK() OVER (partition by l_orderkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY l_orderkey , 2
	LIMIT 5;

-- do not push down limit if there is an aggragete in distinct on
-- we should be able to push this down, but query goes to subquery
-- planner and we can't safely determine it is grouped by partition
-- column.
SELECT
	DISTINCT ON (l_orderkey, RANK() OVER (partition by l_orderkey)) l_orderkey, count(*), RANK() OVER (partition by l_orderkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY l_orderkey , 3, 2
	LIMIT 5;

-- limit is not pushed down due to same reason
SELECT
	DISTINCT ON (l_orderkey, count(*) OVER (partition by l_orderkey)) l_orderkey, l_linenumber, count(*), count(*) OVER (partition by l_orderkey)
	FROM lineitem_hash
	GROUP BY l_orderkey, l_linenumber
	ORDER BY l_orderkey , count(*) OVER (partition by l_orderkey), count(*), l_linenumber
	LIMIT 5;

-- limit is not pushed down since distinct clause is not superset of group clause
SELECT
	DISTINCT ON (RANK() OVER (partition by l_orderkey)) l_orderkey, RANK() OVER (partition by l_orderkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 1
	LIMIT 5;

SET client_min_messages TO NOTICE;

-- non constants should not push down
CREATE OR REPLACE FUNCTION my_limit()
RETURNS INT AS $$
BEGIN
  RETURN 5;
END; $$ language plpgsql VOLATILE;

SELECT l_orderkey FROM lineitem_hash ORDER BY l_orderkey LIMIT my_limit();
SELECT l_orderkey FROM lineitem_hash ORDER BY l_orderkey LIMIT 10 OFFSET my_limit();

DROP FUNCTION my_limit();

-- subqueries should error out
SELECT l_orderkey FROM lineitem_hash ORDER BY l_orderkey LIMIT (SELECT 10);
SELECT l_orderkey FROM lineitem_hash ORDER BY l_orderkey LIMIT 10 OFFSET (SELECT 10);

DROP TABLE lineitem_hash;
