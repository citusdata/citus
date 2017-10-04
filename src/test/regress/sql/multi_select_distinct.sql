--
-- MULTI_SELECT_DISTINCT
--
-- Tests select distinct, and select distinct on features.
--


-- function calls are supported
SELECT DISTINCT l_orderkey, now() FROM lineitem_hash_part LIMIT 0;

SELECT DISTINCT l_partkey, 1 + (random() * 0)::int FROM lineitem_hash_part ORDER BY 1 DESC LIMIT 3; 

-- const expressions are supported
SELECT DISTINCT l_orderkey, 1+1 FROM lineitem_hash_part ORDER BY 1 LIMIT 5;

-- non const expressions are also supported
SELECT DISTINCT l_orderkey, l_partkey + 1 FROM lineitem_hash_part ORDER BY 1, 2 LIMIT 5;

-- column expressions are supported
SELECT DISTINCT l_orderkey, l_shipinstruct || l_shipmode FROM lineitem_hash_part ORDER BY 2 , 1 LIMIT 5;

-- function calls with const input are supported
SELECT DISTINCT l_orderkey, strpos('AIR', 'A') FROM lineitem_hash_part ORDER BY 1,2 LIMIT 5;

-- function calls with non-const input are supported
SELECT DISTINCT l_orderkey, strpos(l_shipmode, 'I')
	FROM lineitem_hash_part
	WHERE strpos(l_shipmode, 'I') > 1
	ORDER BY 2, 1
	LIMIT 5;

-- distinct on partition column
-- verify counts match with respect to count(distinct)
CREATE TEMP TABLE temp_orderkeys AS SELECT DISTINCT l_orderkey FROM lineitem_hash_part;
SELECT COUNT(*) FROM temp_orderkeys;
SELECT COUNT(DISTINCT l_orderkey) FROM lineitem_hash_part;

SELECT DISTINCT l_orderkey FROM lineitem_hash_part WHERE l_orderkey < 500 and l_partkey < 5000 order by 1;

-- distinct on non-partition column
SELECT DISTINCT l_partkey FROM lineitem_hash_part WHERE l_orderkey > 5 and l_orderkey < 20 order by 1;

SELECT DISTINCT l_shipmode FROM lineitem_hash_part ORDER BY 1 DESC;

-- distinct with multiple columns 
SELECT DISTINCT l_orderkey, o_orderdate
	FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey) 
	WHERE l_orderkey < 10
	ORDER BY l_orderkey;

-- distinct on partition column with aggregate
-- this is the same as the one without distinct due to group by
SELECT DISTINCT l_orderkey, count(*)
	FROM lineitem_hash_part
	WHERE l_orderkey < 200
	GROUP BY 1
	HAVING count(*) > 5
	ORDER BY 2 DESC, 1;
	
-- explain the query to see actual plan
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_orderkey, count(*)
		FROM lineitem_hash_part
		WHERE l_orderkey < 200
		GROUP BY 1
		HAVING count(*) > 5
		ORDER BY 2 DESC, 1;

-- distinct on non-partition column with aggregate
-- this is the same as non-distinct version due to group by
SELECT DISTINCT l_partkey, count(*)
	FROM lineitem_hash_part
	GROUP BY 1 
	HAVING count(*) > 2
	ORDER BY 1;
	
-- explain the query to see actual plan
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_partkey, count(*)
		FROM lineitem_hash_part
		GROUP BY 1 
		HAVING count(*) > 2
		ORDER BY 1;

-- distinct on non-partition column and avg
SELECT DISTINCT l_partkey, avg(l_linenumber)
	FROM lineitem_hash_part
	WHERE l_partkey < 500
	GROUP BY 1 
	HAVING avg(l_linenumber) > 2
	ORDER BY 1;

-- distinct on multiple non-partition columns
SELECT DISTINCT l_partkey, l_suppkey
	FROM lineitem_hash_part
	WHERE l_shipmode = 'AIR' AND l_orderkey < 100
	ORDER BY 1, 2;
	
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_partkey, l_suppkey
		FROM lineitem_hash_part
		WHERE l_shipmode = 'AIR' AND l_orderkey < 100
		ORDER BY 1, 2;

-- distinct on partition column
SELECT DISTINCT ON (l_orderkey) l_orderkey, l_partkey, l_suppkey
	FROM lineitem_hash_part
	WHERE l_orderkey < 35
	ORDER BY 1;
	
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (l_orderkey) l_orderkey, l_partkey, l_suppkey
		FROM lineitem_hash_part
		WHERE l_orderkey < 35
		ORDER BY 1;

-- distinct on non-partition column
-- note order by is required here
-- otherwise query results will be different since
-- distinct on clause is on non-partition column
SELECT DISTINCT ON (l_partkey) l_partkey, l_orderkey
	FROM lineitem_hash_part
	ORDER BY 1,2
	LIMIT 20;

EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (l_partkey) l_partkey, l_orderkey
		FROM lineitem_hash_part
		ORDER BY 1,2
		LIMIT 20;

-- distinct on with joins
-- each customer's first order key
SELECT DISTINCT ON (o_custkey) o_custkey, l_orderkey
	FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey) 
	WHERE o_custkey < 15
	ORDER BY 1,2;

EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (o_custkey) o_custkey, l_orderkey
		FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey) 
		WHERE o_custkey < 15
		ORDER BY 1,2;

-- explain without order by
-- notice master plan has order by on distinct on column
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (o_custkey) o_custkey, l_orderkey
		FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey) 
		WHERE o_custkey < 15;

-- each customer's each order's first l_partkey
SELECT DISTINCT ON (o_custkey, l_orderkey) o_custkey, l_orderkey, l_linenumber, l_partkey
	FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey) 
	WHERE o_custkey < 20
	ORDER BY 1,2,3;

-- explain without order by
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (o_custkey, l_orderkey) o_custkey, l_orderkey, l_linenumber, l_partkey
		FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey) 
		WHERE o_custkey < 20;

-- each customer's each order's last l_partkey
SELECT DISTINCT ON (o_custkey, l_orderkey) o_custkey, l_orderkey, l_linenumber, l_partkey
	FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey) 
	WHERE o_custkey < 15
	ORDER BY 1,2,3 DESC;

-- subqueries
SELECT DISTINCT l_orderkey, l_partkey
	FROM (
		SELECT l_orderkey, l_partkey
		FROM lineitem_hash_part
		) q
	ORDER BY 1,2
	LIMIT 10;

EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_orderkey, l_partkey
		FROM (
			SELECT l_orderkey, l_partkey
			FROM lineitem_hash_part
			) q
		ORDER BY 1,2
		LIMIT 10;

SELECT DISTINCT l_orderkey, cnt
	FROM (
		SELECT l_orderkey, count(*) as cnt
		FROM lineitem_hash_part
		GROUP BY 1
		) q
	ORDER BY 1,2
	LIMIT 10;

EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_orderkey, cnt
		FROM (
			SELECT l_orderkey, count(*) as cnt
			FROM lineitem_hash_part
			GROUP BY 1
			) q
		ORDER BY 1,2
		LIMIT 10;
-- distinct on partition column
-- random() is added to inner query to prevent flattening
SELECT DISTINCT ON (l_orderkey) l_orderkey, l_partkey
	FROM (
		SELECT l_orderkey, l_partkey, (random()*10)::int + 2 as r
		FROM lineitem_hash_part
		) q
	WHERE r > 1
	ORDER BY 1,2
	LIMIT 10;

EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (l_orderkey) l_orderkey, l_partkey
		FROM (
			SELECT l_orderkey, l_partkey, (random()*10)::int + 2 as r
			FROM lineitem_hash_part
			) q
		WHERE r > 1
		ORDER BY 1,2
		LIMIT 10;

-- distinct on non-partition column
SELECT DISTINCT ON (l_partkey) l_orderkey, l_partkey
	FROM (
		SELECT l_orderkey, l_partkey, (random()*10)::int + 2 as r
		FROM lineitem_hash_part
		) q
	WHERE r > 1
	ORDER BY 2,1
	LIMIT 10;

EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (l_partkey) l_orderkey, l_partkey
		FROM (
			SELECT l_orderkey, l_partkey, (random()*10)::int + 2 as r
			FROM lineitem_hash_part
			) q
		WHERE r > 1
		ORDER BY 2,1
		LIMIT 10;
