--
-- MULTI_ARRAY_AGG
--


SET citus.next_shard_id TO 520000;
SET citus.coordinator_aggregation_strategy TO 'disabled';

CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$;

-- Check multi_cat_agg() aggregate which is used to implement array_agg()

SELECT array_cat_agg(i) FROM (VALUES (ARRAY[1,2]), (NULL), (ARRAY[3,4])) AS t(i);

-- Check that we don't support distinct and order by with array_agg()

SELECT array_agg(distinct l_orderkey) FROM lineitem;

SELECT array_agg(l_orderkey ORDER BY l_partkey) FROM lineitem;

SELECT array_agg(distinct l_orderkey ORDER BY l_orderkey) FROM lineitem;

-- Check array_agg() for different data types and LIMIT clauses

SELECT array_sort(array_agg(l_partkey)) FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

SELECT array_sort(array_agg(l_extendedprice)) FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

SELECT array_sort(array_agg(l_shipdate)) FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

SELECT array_sort(array_agg(l_shipmode)) FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

-- Check that we can execute array_agg() within other functions

SELECT array_length(array_agg(l_orderkey), 1) FROM lineitem;

-- Check that we can execute array_agg() on select queries that hit multiple
-- shards and contain different aggregates, filter clauses and other complex
-- expressions. Note that the l_orderkey ranges are such that the matching rows
-- lie in different shards.

SELECT l_quantity, count(*), avg(l_extendedprice), array_sort(array_agg(l_orderkey)) FROM lineitem
	WHERE l_quantity < 5 AND l_orderkey > 5500 AND l_orderkey < 9500
	GROUP BY l_quantity ORDER BY l_quantity;

SELECT l_quantity, array_sort(array_agg(extract (month FROM o_orderdate))) AS my_month
	FROM lineitem, orders WHERE l_orderkey = o_orderkey AND l_quantity < 5
	AND l_orderkey > 5500 AND l_orderkey < 9500 GROUP BY l_quantity ORDER BY l_quantity;

SELECT l_quantity, array_sort(array_agg(l_orderkey * 2 + 1)) FROM lineitem WHERE l_quantity < 5
	AND octet_length(l_comment) + octet_length('randomtext'::text) > 40
	AND l_orderkey > 5500 AND l_orderkey < 9500 GROUP BY l_quantity ORDER BY l_quantity;

-- Check that we can execute array_agg() with an expression containing NULL values

SELECT array_agg(case when l_quantity > 20 then l_quantity else NULL end)
	FROM lineitem WHERE l_orderkey < 10;

-- Check that we return NULL in case there are no input rows to array_agg()

SELECT array_agg(l_orderkey) FROM lineitem WHERE l_quantity < 0;
