--
-- MULTI_COMPLEX_EXPRESSIONS
--

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

SELECT count(*) FROM lineitem WHERE random() = 0.1;

-- Check that we can handle implicit and explicit join clause definitions.

SELECT count(*) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_quantity < 5;

SELECT count(*) FROM lineitem
	JOIN orders ON l_orderkey = o_orderkey AND l_quantity < 5;

SELECT count(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey
	WHERE l_quantity < 5;

-- Check that we make sure local joins are between columns only.

SELECT count(*) FROM lineitem, orders WHERE l_orderkey + 1 = o_orderkey;
