--
-- MULTI_JSON_OBJECT_AGG
--


SET citus.next_shard_id TO 520000;
SET citus.coordinator_aggregation_strategy TO 'disabled';

CREATE OR REPLACE FUNCTION count_keys (json)
RETURNS bigint LANGUAGE SQL
AS $$
SELECT count(*) FROM (SELECT * FROM json_object_keys($1)) t
$$;

CREATE OR REPLACE FUNCTION keys_sort (json)
RETURNS json LANGUAGE SQL
AS $$
SELECT json_object_agg(key, value) FROM (
	SELECT * FROM json_each($1) ORDER BY key
) t
$$;

-- Check multi_cat_agg() aggregate which is used to implement json_object_agg()

SELECT json_cat_agg(i) FROM
	(VALUES ('{"c":[], "b":2}'::json), (NULL), ('{"d":null, "a":{"b":3}, "b":2}'::json)) AS t(i);

-- Check that we don't support distinct and order by with json_object_agg()

SELECT json_object_agg(distinct l_shipmode, l_orderkey) FROM lineitem;

SELECT json_object_agg(l_shipmode, l_orderkey ORDER BY l_shipmode) FROM lineitem;

SELECT json_object_agg(distinct l_orderkey, l_shipmode ORDER BY l_orderkey) FROM lineitem;

-- Check json_object_agg() for different data types and LIMIT clauses

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, l_partkey))
	FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, l_extendedprice))
	FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, l_shipmode))
	FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, l_shipdate))
	FROM lineitem GROUP BY l_orderkey
	ORDER BY l_orderkey LIMIT 10;

-- Check that we can execute json_object_agg() within other functions

SELECT count_keys(json_object_agg(l_shipdate, l_orderkey)) FROM lineitem;

-- Check that we can execute json_object_agg() on select queries that hit multiple
-- shards and contain different aggregates, filter clauses and other complex
-- expressions. Note that the l_orderkey ranges are such that the matching rows
-- lie in different shards.

SELECT l_quantity, count(*), avg(l_extendedprice),
		keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, l_shipdate))
	FROM lineitem
	WHERE l_quantity < 5 AND l_orderkey > 5000 AND l_orderkey < 5300
	GROUP BY l_quantity ORDER BY l_quantity;

SELECT l_quantity, keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text,
											extract (month FROM o_orderdate)))
	FROM lineitem, orders WHERE l_orderkey = o_orderkey AND l_quantity < 5
	AND l_orderkey > 5000 AND l_orderkey < 5300 GROUP BY l_quantity ORDER BY l_quantity;

SELECT l_quantity, keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, l_orderkey * 2 + 1))
	FROM lineitem WHERE l_quantity < 5
	AND octet_length(l_comment) + octet_length('randomtext'::text) > 40
	AND l_orderkey > 5000 AND l_orderkey < 6000 GROUP BY l_quantity ORDER BY l_quantity;

-- Check that we can execute json_object_agg() with an expression containing NULL values

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text,
								case when l_quantity > 20 then l_quantity else NULL end))
	FROM lineitem WHERE l_orderkey < 5;

-- Check that we can execute json_object_agg() with an expression containing different types

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text,
								case when l_quantity > 20 then to_json(l_quantity) else '"f"'::json end))
	FROM lineitem WHERE l_orderkey < 5;

-- Check that we can execute json_object_agg() with an expression containing json arrays

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, json_build_array(l_quantity, l_shipdate)))
	FROM lineitem WHERE l_orderkey < 3;

-- Check that we can execute json_object_agg() with an expression containing arrays

SELECT keys_sort(json_object_agg(l_orderkey::text || l_linenumber::text, ARRAY[l_quantity, l_orderkey]))
	FROM lineitem WHERE l_orderkey < 3;

-- Check that we return NULL in case there are no input rows to json_object_agg()

SELECT json_object_agg(l_shipdate, l_orderkey) FROM lineitem WHERE l_quantity < 0;
