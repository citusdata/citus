--
-- MULTI_SELECT_DISTINCT
--
-- Tests select distinct, and select distinct on features.
--

ANALYZE lineitem_hash_part;

-- function calls are supported
SELECT DISTINCT l_orderkey, now() FROM lineitem_hash_part LIMIT 0;

SELECT DISTINCT l_orderkey, avg(l_linenumber)
FROM lineitem_hash_part
GROUP BY l_orderkey
HAVING avg(l_linenumber) = (select avg(distinct l_linenumber))
LIMIT 10;

SELECT DISTINCT l_orderkey
FROM lineitem_hash_part
GROUP BY l_orderkey
HAVING (select avg(distinct l_linenumber) = l_orderkey)
LIMIT 10;

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

-- row types are supported
SELECT DISTINCT (l_orderkey, l_partkey) AS pair FROM lineitem_hash_part ORDER BY 1 LIMIT 5;

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

-- check the plan if the hash aggreate is disabled
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_orderkey, count(*)
		FROM lineitem_hash_part
		WHERE l_orderkey < 200
		GROUP BY 1
		HAVING count(*) > 5
		ORDER BY 2 DESC, 1;

SET enable_hashagg TO on;

-- distinct on aggregate of group by columns, we try to check whether we handle
-- queries which does not have any group by column in distinct columns properly.
SELECT DISTINCT count(*)
	FROM lineitem_hash_part
	GROUP BY l_suppkey, l_linenumber
	ORDER BY 1;

-- explain the query to see actual plan. We expect to see Aggregate node having
-- group by key on count(*) column, since columns in the Group By doesn't guarantee
-- the uniqueness of the result.
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT count(*)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1;

-- check the plan if the hash aggreate is disabled. We expect to see sort+unique
-- instead of aggregate plan node to handle distinct.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT count(*)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1;

SET enable_hashagg TO on;

-- Now we have only part of group clause columns in distinct, yet it is still not
-- enough to use Group By columns to guarantee uniqueness of result list.
SELECT DISTINCT l_suppkey, count(*)
	FROM lineitem_hash_part
	GROUP BY l_suppkey, l_linenumber
	ORDER BY 1
	LIMIT 10;

-- explain the query to see actual plan. Similar to the explain of the query above.
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_suppkey, count(*)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1
		LIMIT 10;

-- check the plan if the hash aggreate is disabled. Similar to the explain of
-- the query above.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_suppkey, count(*)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1
		LIMIT 10;

SET enable_hashagg TO on;

-- Similar to the above query, not with count but avg. Only difference with the
-- above query is that, we create run two aggregate functions in workers.
SELECT DISTINCT l_suppkey, avg(l_partkey)
	FROM lineitem_hash_part
	GROUP BY l_suppkey, l_linenumber
	ORDER BY 1,2
	LIMIT 10;

-- explain the query to see actual plan. Similar to the explain of the query above.
-- Only aggregate functions will be changed.
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_suppkey, avg(l_partkey)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1,2
		LIMIT 10;

-- check the plan if the hash aggreate is disabled. This explain errors out due
-- to a bug right now, expectation must be corrected after fixing it.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT l_suppkey, avg(l_partkey)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1,2
		LIMIT 10;

SET enable_hashagg TO on;

-- Similar to the above query but with distinct on
SELECT DISTINCT ON (l_suppkey) avg(l_partkey)
	FROM lineitem_hash_part
	GROUP BY l_suppkey, l_linenumber
	ORDER BY l_suppkey,1
	LIMIT 10;

-- explain the query to see actual plan. We expect to see sort+unique to handle
-- distinct on.
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (l_suppkey) avg(l_partkey)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY l_suppkey,1
		LIMIT 10;

-- check the plan if the hash aggreate is disabled. We expect to see sort+unique to
-- handle distinct on.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (l_suppkey) avg(l_partkey)
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY l_suppkey,1
		LIMIT 10;

SET enable_hashagg TO on;

-- distinct with expression and aggregation
SELECT DISTINCT avg(ceil(l_partkey / 2))
	FROM lineitem_hash_part
	GROUP BY l_suppkey, l_linenumber
	ORDER BY 1
	LIMIT 10;

-- explain the query to see actual plan
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT avg(ceil(l_partkey / 2))
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1
		LIMIT 10;

-- check the plan if the hash aggreate is disabled. This explain errors out due
-- to a bug right now, expectation must be corrected after fixing it.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT avg(ceil(l_partkey / 2))
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1
		LIMIT 10;

SET enable_hashagg TO on;

-- expression among aggregations.
SELECT DISTINCT sum(l_suppkey) + count(l_partkey) AS dis
	FROM lineitem_hash_part
	GROUP BY l_suppkey, l_linenumber
	ORDER BY 1
	LIMIT 10;

-- explain the query to see actual plan
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT sum(l_suppkey) + count(l_partkey) AS dis
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1
		LIMIT 10;

-- check the plan if the hash aggreate is disabled. This explain errors out due
-- to a bug right now, expectation must be corrected after fixing it.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT sum(l_suppkey) + count(l_partkey) AS dis
		FROM lineitem_hash_part
		GROUP BY l_suppkey, l_linenumber
		ORDER BY 1
		LIMIT 10;

SET enable_hashagg TO on;

-- distinct on all columns, note Group By columns guarantees uniqueness of the
-- result list.
SELECT DISTINCT *
	FROM lineitem_hash_part
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
	ORDER BY 1,2
	LIMIT 10;

-- explain the query to see actual plan. We expect to see only one aggregation
-- node since group by columns guarantees the uniqueness.
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT *
		FROM lineitem_hash_part
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
		ORDER BY 1,2
		LIMIT 10;

-- check the plan if the hash aggreate is disabled. We expect to see only one
-- aggregation node since group by columns guarantees the uniqueness.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT *
		FROM lineitem_hash_part
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
		ORDER BY 1,2
		LIMIT 10;

SET enable_hashagg TO on;

-- distinct on count distinct
SELECT DISTINCT count(DISTINCT l_partkey), count(DISTINCT l_shipmode)
	FROM lineitem_hash_part
	GROUP BY l_orderkey
	ORDER BY 1,2;

-- explain the query to see actual plan. We expect to see aggregation plan for
-- the outer distinct.
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT count(DISTINCT l_partkey), count(DISTINCT l_shipmode)
		FROM lineitem_hash_part
		GROUP BY l_orderkey
		ORDER BY 1,2;

-- check the plan if the hash aggreate is disabled. We expect to see sort + unique
-- plans for the outer distinct.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT count(DISTINCT l_partkey), count(DISTINCT l_shipmode)
		FROM lineitem_hash_part
		GROUP BY l_orderkey
		ORDER BY 1,2;

SET enable_hashagg TO on;

-- distinct on aggregation with filter and expression
SELECT DISTINCT ceil(count(case when l_partkey > 100000 THEN 1 ELSE 0 END) / 2) AS count
	FROM lineitem_hash_part
	GROUP BY l_suppkey
	ORDER BY 1;

-- explain the query to see actual plan
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ceil(count(case when l_partkey > 100000 THEN 1 ELSE 0 END) / 2) AS count
		FROM lineitem_hash_part
		GROUP BY l_suppkey
		ORDER BY 1;

-- check the plan if the hash aggreate is disabled
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ceil(count(case when l_partkey > 100000 THEN 1 ELSE 0 END) / 2) AS count
		FROM lineitem_hash_part
		GROUP BY l_suppkey
		ORDER BY 1;

SET enable_hashagg TO on;

-- explain the query to see actual plan with array_agg aggregation.
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT array_agg(l_linenumber), array_length(array_agg(l_linenumber), 1)
		FROM lineitem_hash_part
		GROUP BY l_orderkey
		ORDER BY 2
		LIMIT 15;

-- check the plan if the hash aggreate is disabled.
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT array_agg(l_linenumber), array_length(array_agg(l_linenumber), 1)
		FROM lineitem_hash_part
		GROUP BY l_orderkey
		ORDER BY 2
		LIMIT 15;

SET enable_hashagg TO on;

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
	ORDER BY 1, 2, 3;

EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (l_orderkey) l_orderkey, l_partkey, l_suppkey
		FROM lineitem_hash_part
		WHERE l_orderkey < 35
		ORDER BY 1, 2, 3;

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

SELECT coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (o_custkey) o_custkey, l_orderkey
		FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey)
		WHERE o_custkey < 15
		ORDER BY 1,2;
$Q$);

-- explain without order by
-- notice master plan has order by on distinct on column
SELECT coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (o_custkey) o_custkey, l_orderkey
		FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey)
		WHERE o_custkey < 15;
$Q$);

-- each customer's each order's first l_partkey
SELECT DISTINCT ON (o_custkey, l_orderkey) o_custkey, l_orderkey, l_linenumber, l_partkey
	FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey)
	WHERE o_custkey < 20
	ORDER BY 1,2,3;

-- explain without order by
SELECT coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
	SELECT DISTINCT ON (o_custkey, l_orderkey) o_custkey, l_orderkey, l_linenumber, l_partkey
		FROM lineitem_hash_part JOIN orders_hash_part ON (l_orderkey = o_orderkey)
		WHERE o_custkey < 20;
$Q$);

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
