--
-- MULTI_COMPLEX_EXPRESSIONS
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 420000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 420000;


-- Check that we can correctly handle complex expressions and aggregates.

SELECT sum(l_quantity) / avg(l_quantity) FROM lineitem;

SELECT sum(l_quantity) / (10 * avg(l_quantity)) FROM lineitem;

SELECT (sum(l_quantity) / (10 * avg(l_quantity))) + 11 FROM lineitem;

SELECT avg(l_quantity) as average FROM lineitem;

SELECT 100 * avg(l_quantity) as average_times_hundred FROM lineitem;

SELECT 100 * avg(l_quantity) / 10 as average_times_ten FROM lineitem;

SELECT l_quantity, 10 * count(*) count_quantity FROM lineitem 
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Check that we can handle complex select clause expressions.

SELECT count(*) FROM lineitem
	WHERE octet_length(l_comment || l_comment) > 40;

SELECT count(*) FROM lineitem
	WHERE octet_length(concat(l_comment, l_comment)) > 40;

SELECT count(*) FROM lineitem
	WHERE octet_length(l_comment) + octet_length('randomtext'::text) > 40;

SELECT count(*) FROM lineitem
	WHERE octet_length(l_comment) + 10 > 40;

SELECT count(*) FROM lineitem
	WHERE (l_receiptdate::timestamp - l_shipdate::timestamp) > interval '5 days';

-- can push down queries where no columns present on the WHERE clause
SELECT count(*) FROM lineitem WHERE random() = -0.1;

-- boolean tests can be pushed down
SELECT count(*) FROM lineitem
	WHERE (l_partkey > 10000) is true;

-- scalar array operator expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE l_partkey = ANY(ARRAY[19353, 19354, 19355]);

-- some more scalar array operator expressions
SELECT count(*) FROM lineitem
	WHERE l_partkey = ALL(ARRAY[19353]);

-- operator expressions involving arrays
SELECT count(*) FROM lineitem
	WHERE ARRAY[19353, 19354, 19355] @> ARRAY[l_partkey];

-- coerced via io expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE (l_quantity/100)::int::bool::text::bool;

-- case expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE (CASE WHEN l_orderkey > 4000 THEN l_partkey / 100 > 1 ELSE false END);

-- coalesce expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE COALESCE((l_partkey/50000)::bool, false);

-- nullif expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE NULLIF((l_partkey/50000)::bool, false);

-- null test expressions can be pushed down
SELECT count(*) FROM orders
	WHERE o_comment IS NOT null;

-- functions can be pushed down
SELECT count(*) FROM lineitem
	WHERE isfinite(l_shipdate);

-- constant expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE 0 != 0;

-- distinct expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE l_partkey IS DISTINCT FROM 50040;

-- row compare expression can be pushed down
SELECT count(*) FROM lineitem
	WHERE row(l_partkey, 2, 3) > row(2000, 2, 3);

-- combination of different expressions can be pushed down
SELECT count(*) FROM lineitem
	WHERE
		  (l_quantity/100)::int::bool::text::bool AND
		  CASE WHEN l_orderkey > 4000 THEN l_partkey / 100 > 1 ELSE false END AND
		  COALESCE((l_partkey/50000)::bool, false) AND
		  NULLIF((l_partkey/50000)::bool, false) AND
		  isfinite(l_shipdate) AND
		  l_partkey IS DISTINCT FROM 50040 AND
		  row(l_partkey, 2, 3) > row(2000, 2, 3);

-- constant expression in the WHERE clause with a column in the target list
SELECT l_linenumber FROM lineitem
	WHERE
		1!=0
	ORDER BY
		l_linenumber
	LIMIT 1;

-- constant expression in the WHERE clause with expressions and a column the target list
SELECT count(*) * l_discount as total_discount, count(*), sum(l_tax), l_discount FROM lineitem
	WHERE
		1!=0
	GROUP BY
		l_discount
	ORDER BY
		total_discount DESC, sum(l_tax) DESC;

-- distinct expressions in the WHERE clause with a column in the target list
SELECT l_linenumber FROM lineitem
	WHERE
		l_linenumber IS DISTINCT FROM 1 AND
		l_orderkey IS DISTINCT FROM 8997
	ORDER BY
		l_linenumber
	LIMIT 1;

-- distinct expressions in the WHERE clause with expressions and a column the target list
SELECT max(l_linenumber), min(l_discount), l_receiptdate FROM lineitem
	WHERE
		l_linenumber IS DISTINCT FROM 1 AND
		l_orderkey IS DISTINCT FROM 8997
	GROUP BY
		l_receiptdate
	ORDER BY
		l_receiptdate
	LIMIT 1;

-- Check that we can handle implicit and explicit join clause definitions.

SELECT count(*) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_quantity < 5;

SELECT count(*) FROM lineitem
	JOIN orders ON l_orderkey = o_orderkey AND l_quantity < 5;

SELECT count(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey
	WHERE l_quantity < 5;

-- Check that we make sure local joins are between columns only.

SELECT count(*) FROM lineitem, orders WHERE l_orderkey + 1 = o_orderkey;
