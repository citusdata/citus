--
-- MULTI_SUBQUERY
--
-- no need to set shardid sequence given that we're not creating any shards

SET citus.next_shard_id TO 570032;
SET citus.coordinator_aggregation_strategy TO 'disabled';

-- Check that we error out if shard min/max values are not exactly same.
SELECT
	avg(unit_price)
FROM
	(SELECT
		l_orderkey,
		avg(o_totalprice) AS unit_price
	FROM
		lineitem_subquery,
		orders_subquery
	WHERE
		l_orderkey = o_orderkey
	GROUP BY
		l_orderkey) AS unit_prices;

-- Update metadata in order to make all shards equal
-- note that the table is created on multi_insert_select_create_table.sql
UPDATE
	pg_dist_shard
SET
	shardmaxvalue = '14947'
WHERE
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'orders_subquery'::regclass ORDER BY shardid DESC LIMIT 1);

SET client_min_messages TO DEBUG1;
-- If group by is not on partition column then we recursively plan
SELECT
	avg(order_count)
FROM
	(SELECT
		l_suppkey,
		count(*) AS order_count
	FROM
		lineitem_subquery
	GROUP BY
		l_suppkey) AS order_counts;

-- Check that we recursively plan if join is not on partition columns.
SELECT
	avg(unit_price)
FROM
	(SELECT
		l_orderkey,
		avg(o_totalprice / l_quantity) AS unit_price
	FROM
		lineitem_subquery,
		orders_subquery
	GROUP BY
		l_orderkey) AS unit_prices;

RESET client_min_messages;

-- Subqueries without relation with a volatile functions (non-constant) are planned recursively
SELECT count(*) FROM (
   SELECT l_orderkey FROM lineitem_subquery JOIN (SELECT random()::int r) sub ON (l_orderkey = r) WHERE r > 10
) b;

SET client_min_messages TO DEBUG;

-- If there is non relation subqueries then we recursively plan
SELECT count(*) FROM
(
   (SELECT l_orderkey FROM lineitem_subquery) UNION ALL
   (SELECT 1::bigint)
) b;


-- If queries in union do not include partition columns then we recursively plan
SELECT count(*) FROM
(
   (SELECT l_orderkey FROM lineitem_subquery) UNION
   (SELECT l_partkey FROM lineitem_subquery)
) b;

-- Check that we push down union queries if partition column is selected (no DEBUG messages)

SELECT count(*) FROM
(
   (SELECT l_orderkey FROM lineitem_subquery) UNION
   (SELECT l_orderkey FROM lineitem_subquery)
) b;

RESET client_min_messages;

-- we'd error out if inner query has Limit but subquery_pushdown is not set
-- but we recursively plan the query
SELECT
	avg(o_totalprice/l_quantity)
FROM
		(SELECT
			l_orderkey,
			l_quantity
		FROM
			lineitem_subquery
		ORDER BY
			l_orderkey, l_quantity
		LIMIT 10
		) lineitem_quantities
	JOIN LATERAL
		(SELECT
			o_totalprice
		FROM
			orders_subquery
		WHERE
			lineitem_quantities.l_orderkey = o_orderkey) orders_price ON true;

-- Limit is only supported when subquery_pushdown is set
-- Check that we error out if inner query has limit but outer query has not.
SET citus.subquery_pushdown to ON;
SELECT
	avg(o_totalprice/l_quantity)
FROM
		(SELECT
			l_orderkey,
			l_quantity
		FROM
			lineitem_subquery
		ORDER BY
			l_orderkey, l_quantity
		LIMIT 10
		) lineitem_quantities
	JOIN LATERAL
		(SELECT
			o_totalprice
		FROM
			orders_subquery
		WHERE
			lineitem_quantities.l_orderkey = o_orderkey) orders_price ON true;

-- reset the flag for next query
SET citus.subquery_pushdown to OFF;

-- some queries without a subquery uses subquery planner
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey < l_quantity)
ORDER BY l_orderkey DESC
LIMIT 10;

-- query is still supported if contains additional join
-- clauses that includes arithmetic expressions
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey < l_quantity + 3)
ORDER BY l_orderkey DESC
LIMIT 10;

-- implicit typecasts in joins is supported
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey::int8 = o_orderkey::int8)
WHERE
	(o_orderkey < l_quantity + 3)
ORDER BY l_orderkey DESC
LIMIT 10;

-- non-implicit typecasts in joins is not supported
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey::int8 = o_orderkey::int4)
WHERE
	(o_orderkey < l_quantity + 3)
ORDER BY l_orderkey DESC
LIMIT 10;

-- implicit typecast supported in equi-join
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey::int8 = o_orderkey::int8)
ORDER BY l_orderkey DESC
LIMIT 10;

-- non-implicit typecast is not supported in equi-join
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey::int4 = o_orderkey::int8)
ORDER BY l_orderkey DESC
LIMIT 10;

-- type casts in filters are supported as long as
-- a valid equi-join exists
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey::int8 < l_quantity::int8 + 3)
ORDER BY l_orderkey DESC
LIMIT 10;

-- even if type cast is non-implicit
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey::int4 < l_quantity::int8 + 3)
ORDER BY l_orderkey DESC
LIMIT 10;

-- query is not supported if contains an partition column
-- equi join that includes arithmetic expressions
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey + 1)
WHERE
	(o_orderkey < l_quantity)
ORDER BY l_orderkey DESC
LIMIT 10;

-- query is not supported if there is a single
-- join clause with arithmetic expression. It fails
-- with a different error message
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey + 1)
ORDER BY l_orderkey DESC
LIMIT 10;

-- query is not supported if does not have equi-join clause
SELECT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey < o_orderkey)
WHERE
	(o_orderkey < l_quantity)
ORDER BY l_orderkey DESC
LIMIT 10;

-- outer joins on reference tables with functions works
SELECT DISTINCT ON (t1.user_id) t1.user_id, t2.value_1, t2.value_2, t2.value_3
FROM events_table t1
LEFT JOIN users_reference_table t2 ON t1.user_id = trunc(t2.user_id)
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
LIMIT 5;

-- outer joins on reference tables with simple expressions should work
SELECT DISTINCT ON (t1.user_id) t1.user_id, t2.value_1, t2.value_2, t2.value_3
FROM events_table t1
LEFT JOIN users_reference_table t2 ON t1.user_id > t2.user_id
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
LIMIT 5;

-- outer joins on distributed tables with simple expressions should not work
SELECT DISTINCT ON (t1.user_id) t1.user_id, t2.value_1, t2.value_2, t2.value_3
FROM events_table t1
LEFT JOIN users_table t2 ON t1.user_id > t2.user_id
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
LIMIT 5;

-- outer joins on reference tables with expressions should work
SELECT DISTINCT ON (t1.user_id) t1.user_id, t2.value_1, t2.value_2, t2.value_3
FROM events_table t1
LEFT JOIN users_reference_table t2 ON t1.user_id = (CASE WHEN t2.user_id > 3 THEN 3 ELSE t2.user_id END)
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
LIMIT 5;

-- outer joins on distributed tables and reference tables with expressions should work
SELECT DISTINCT ON (t1.user_id) t1.user_id, t2.value_1, t2.value_2, t2.value_3
 FROM
 users_table t0 LEFT JOIN
 events_table t1  ON t0.user_id = t1.user_id
 LEFT JOIN users_reference_table t2 ON t1.user_id = trunc(t2.user_id)
 ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
 LIMIT 5;

-- outer joins on distributed tables with expressions should not work
SELECT DISTINCT ON (t1.user_id) t1.user_id, t2.value_1, t2.value_2, t2.value_3
 FROM
 users_table t0 LEFT JOIN
 events_table t1  ON t0.user_id = trunc(t1.user_id)
 LEFT JOIN users_reference_table t2 ON t1.user_id = trunc(t2.user_id)
 ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
 LIMIT 5;

-- outer joins as subqueries should work
-- https://github.com/citusdata/citus/issues/2739
SELECT user_id, value_1, event_type
FROM (
	SELECT a.user_id, a.value_1, b.event_type
	FROM users_table a
	LEFT JOIN events_table b ON a.user_id = b.user_id
) lo
ORDER BY 1, 2, 3
LIMIT 5;

-- inner joins on reference tables with functions works
SELECT DISTINCT ON (t1.user_id) t1.user_id, t2.value_1, t2.value_2, t2.value_3
FROM events_table t1
JOIN users_reference_table t2 ON t1.user_id = trunc(t2.user_id)
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
LIMIT 5;

-- distinct queries work
SELECT DISTINCT l_orderkey
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey < l_quantity)
ORDER BY l_orderkey DESC
LIMIT 10;

-- count(distinct) queries work
SELECT COUNT(DISTINCT l_orderkey)
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey < l_quantity);

-- the same queries returning a non-partition column
SELECT l_quantity
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey < l_quantity)
ORDER BY l_quantity DESC
LIMIT 10;

-- distinct queries work
SELECT DISTINCT l_quantity
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey < l_quantity)
ORDER BY l_quantity DESC
LIMIT 10;

-- count(distinct) queries work
SELECT COUNT(DISTINCT l_quantity)
FROM
	lineitem_subquery l
JOIN
	orders_subquery o
ON (l_orderkey = o_orderkey)
WHERE
	(o_orderkey < l_quantity);


-- Check that we support count distinct with a subquery

SELECT
	count(DISTINCT a)
FROM (
	SELECT
		count(*) a
	FROM
		lineitem_subquery
	GROUP BY
	   l_orderkey
) z;

-- We do not support distinct aggregates other than count distinct with a subquery

SELECT
	sum(DISTINCT a)
FROM (
	SELECT
		count(*) a
	FROM
		lineitem_subquery
	GROUP BY
	   l_orderkey
) z;

SELECT
	avg(DISTINCT a)
FROM (
	SELECT
		count(*) a
	FROM
		lineitem_subquery
	GROUP BY
	   l_orderkey
) z;

-- Check supported subquery types.

SELECT
	o_custkey,
	sum(order_count) as total_order_count
FROM
	(SELECT
		o_orderkey,
		o_custkey,
		count(*) AS order_count
	FROM
		orders_subquery
	WHERE
		o_orderkey > 0 AND
		o_orderkey < 12000
	GROUP BY
		o_orderkey, o_custkey) AS order_counts
GROUP BY
	o_custkey
ORDER BY
	total_order_count DESC,
	o_custkey ASC
LIMIT 10;

SELECT
	avg(unit_price)
FROM
	(SELECT
		l_orderkey,
		avg(o_totalprice / l_quantity) AS unit_price
	FROM
		lineitem_subquery,
		orders_subquery
	WHERE
		l_orderkey = o_orderkey
	GROUP BY
		l_orderkey) AS unit_prices
WHERE
	unit_price > 1000 AND
	unit_price < 10000;

-- Check unsupported subqueries in target list
SELECT (SELECT 1) FROM orders_subquery;
SELECT sum((SELECT 1)) FROM orders_subquery;

-- Check that if subquery is pulled, we don't error and run query properly.

SELECT count(*) FROM
(
	SELECT l_orderkey FROM (
		(SELECT l_orderkey FROM lineitem_subquery) UNION
		(SELECT l_orderkey FROM lineitem_subquery)
	) a
	WHERE l_orderkey = 1
) b;

SELECT count(*) FROM
(
	SELECT * FROM (
		(SELECT * FROM lineitem_subquery) UNION
		(SELECT * FROM lineitem_subquery)
	) a
	WHERE l_orderkey = 1
) b;

SELECT max(l_orderkey) FROM
(
	SELECT l_orderkey FROM (
		SELECT
			l_orderkey
		FROM
			lineitem_subquery
		WHERE
			l_orderkey < 20000
		GROUP BY
			l_orderkey
  ) z
) y;

-- Subqueries filter by 2 different users
SELECT *
FROM
  (SELECT *
   FROM
     (SELECT user_id,
             sum(value_2) AS counter
      FROM events_table
      WHERE user_id = 2
      GROUP BY user_id) AS foo,
     (SELECT user_id,
             sum(value_2) AS counter
      FROM events_table
      WHERE user_id = 3
      GROUP BY user_id) AS bar
   WHERE foo.user_id = bar.user_id ) AS baz;

-- Subqueries filter by different users, one of which overlaps
SELECT *
FROM
  (SELECT *
   FROM
     (SELECT user_id,
             sum(value_2) AS counter
      FROM events_table
      WHERE user_id = 2
        OR user_id = 3
      GROUP BY user_id) AS foo,

     (SELECT user_id,
             sum(value_2) AS counter
      FROM events_table
      WHERE user_id = 2
      GROUP BY user_id) AS bar
   WHERE foo.user_id = bar.user_id ) AS baz
ORDER BY 1,2
LIMIT 5;

-- Add one more shard to one relation, then test if we error out because of different
-- shard counts for joining relations.

SELECT master_create_empty_shard('orders_subquery') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 15000, shardmaxvalue = 20000
WHERE shardid = :new_shard_id;

SELECT
	avg(unit_price)
FROM
	(SELECT
		l_orderkey,
		avg(o_totalprice / l_quantity) AS unit_price
	FROM
		lineitem_subquery,
		orders_subquery
	WHERE
		l_orderkey = o_orderkey
	GROUP BY
		l_orderkey) AS unit_prices;

-- Check that we can prune shards in subqueries with VARCHAR partition columns

CREATE TABLE subquery_pruning_varchar_test_table
(
   a varchar,
   b int
);
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('subquery_pruning_varchar_test_table', 'a', 'hash');

-- temporarily disable router executor to test pruning behaviour of subquery pushdown
SET citus.enable_router_execution TO off;
SET client_min_messages TO DEBUG2;

SELECT * FROM
	(SELECT count(*) FROM subquery_pruning_varchar_test_table WHERE a = 'onder' GROUP BY a)
AS foo;

SELECT * FROM
	(SELECT count(*) FROM subquery_pruning_varchar_test_table WHERE 'eren' = a GROUP BY a)
AS foo;

SET client_min_messages TO NOTICE;

-- test subquery join on VARCHAR partition column
SELECT * FROM
	(SELECT
		a_inner AS a
	FROM
		(SELECT
			subquery_pruning_varchar_test_table.a AS a_inner
		FROM
		 	subquery_pruning_varchar_test_table
		GROUP BY
		  	subquery_pruning_varchar_test_table.a
		HAVING
		  	count(subquery_pruning_varchar_test_table.a) < 3)
		AS f1,

		(SELECT
		 	subquery_pruning_varchar_test_table.a
		FROM
		  	subquery_pruning_varchar_test_table
		GROUP BY
		  	subquery_pruning_varchar_test_table.a
		HAVING
		  	sum(coalesce(subquery_pruning_varchar_test_table.b,0)) > 20.0)
		AS f2
	WHERE
		f1.a_inner = f2.a
	GROUP BY
		a_inner)
AS foo;

RESET citus.enable_router_execution;

-- Test https://github.com/citusdata/citus/issues/3424
insert into subquery_pruning_varchar_test_table values ('1', '1'), (2, '1'), (3, '2'), (3, '1'), (4, '4'), (5, '6');

WITH cte_1 AS (SELECT b max FROM subquery_pruning_varchar_test_table)
SELECT a
FROM subquery_pruning_varchar_test_table
JOIN cte_1 ON a = max::text
GROUP BY a HAVING a = (SELECT a)
ORDER BY 1;

-- Test https://github.com/citusdata/citus/issues/3432
SELECT t1.event_type FROM events_table t1
GROUP BY t1.event_type HAVING t1.event_type > avg((SELECT t2.value_2 FROM users_table t2 ORDER BY 1 DESC LIMIT 1))
ORDER BY 1;

SELECT t1.event_type FROM events_table t1
GROUP BY t1.event_type HAVING t1.event_type > avg(2 + (SELECT t2.value_2 FROM users_table t2 ORDER BY 1 DESC LIMIT 1))
ORDER BY 1;

SELECT t1.event_type FROM events_table t1
GROUP BY t1.event_type HAVING t1.event_type > avg(t1.value_2 + (SELECT t2.value_2 FROM users_table t2 ORDER BY 1 DESC LIMIT 1))
ORDER BY 1;

RESET citus.coordinator_aggregation_strategy;
SELECT t1.event_type FROM events_table t1
GROUP BY t1.event_type HAVING t1.event_type > corr(t1.value_3, t1.value_2 + (SELECT t2.value_2 FROM users_table t2 ORDER BY 1 DESC LIMIT 1))
ORDER BY 1;

SELECT t1.event_type FROM events_table t1
GROUP BY t1.event_type HAVING t1.event_type * 5 > sum(distinct t1.value_3)
ORDER BY 1;
SET citus.coordinator_aggregation_strategy TO 'disabled';

-- Test https://github.com/citusdata/citus/issues/3433
CREATE TABLE keyval1 (key int, value int);
SELECT create_distributed_table('keyval1', 'key');

CREATE TABLE keyval2 (key int, value int);
SELECT create_distributed_table('keyval2', 'key');

CREATE TABLE keyvalref (key int, value int);
SELECT create_reference_table('keyvalref');

EXPLAIN (COSTS OFF)
SELECT count(*) FROM keyval1 GROUP BY key HAVING sum(value) > (SELECT sum(value) FROM keyvalref GROUP BY key);

-- For some reason 'ORDER BY 1 DESC LIMIT 1' triggers recursive planning
EXPLAIN (COSTS OFF)
SELECT count(*) FROM keyval1 GROUP BY key HAVING sum(value) > (SELECT sum(value) FROM keyvalref GROUP BY key ORDER BY 1 DESC LIMIT 1);

EXPLAIN (COSTS OFF)
SELECT count(*) FROM keyval1 GROUP BY key HAVING sum(value) > (SELECT sum(value) FROM keyval2 GROUP BY key ORDER BY 1 DESC LIMIT 1);

EXPLAIN (COSTS OFF)
SELECT count(*) FROM keyval1 k1 WHERE k1.key = 2 GROUP BY key HAVING sum(value) > (SELECT sum(value) FROM keyval2 k2 WHERE k2.key = 2 GROUP BY key ORDER BY 1 DESC LIMIT 1);

-- Simple join subquery pushdown
SELECT
	avg(array_length(events, 1)) AS event_average
FROM
	(SELECT
		tenant_id,
		user_id,
		array_agg(event_type ORDER BY event_time) AS events
	FROM
		(SELECT
			(users.composite_id).tenant_id,
			(users.composite_id).user_id,
			event_type,
			events.event_time
		FROM
			users,
			events
		WHERE
			(users.composite_id) = (events.composite_id) AND
			users.composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
			users.composite_id <= '(1, 9223372036854775807)'::user_composite_type AND
			event_type IN ('click', 'submit', 'pay')) AS subquery
	GROUP BY
		tenant_id,
		user_id) AS subquery;

-- Union and left join subquery pushdown
SELECT
	avg(array_length(events, 1)) AS event_average,
	hasdone
FROM
	(SELECT
		subquery_1.tenant_id,
		subquery_1.user_id,
		array_agg(event ORDER BY event_time) AS events,
		COALESCE(hasdone, 'Has not done paying') AS hasdone
	FROM
	(
		(SELECT
			(users.composite_id).tenant_id,
			(users.composite_id).user_id,
			(users.composite_id) as composite_id,
			'action=>1'AS event,
			events.event_time
		FROM
			users,
			events
		WHERE
			(users.composite_id) = (events.composite_id) AND
			users.composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
			users.composite_id <= '(1, 9223372036854775807)'::user_composite_type AND
			event_type = 'click')
		UNION
		(SELECT
			(users.composite_id).tenant_id,
			(users.composite_id).user_id,
			(users.composite_id) as composite_id,
			'action=>2'AS event,
			events.event_time
		FROM
			users,
			events
		WHERE
			(users.composite_id) = (events.composite_id) AND
			users.composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
			users.composite_id <= '(1, 9223372036854775807)'::user_composite_type AND
			event_type = 'submit')
	) AS subquery_1
	LEFT JOIN
	(SELECT
		DISTINCT ON ((composite_id).tenant_id, (composite_id).user_id) composite_id,
		(composite_id).tenant_id,
		(composite_id).user_id,
		'Has done paying'::TEXT AS hasdone
	FROM
		events
	WHERE
		events.composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
		events.composite_id <= '(1, 9223372036854775807)'::user_composite_type AND
		event_type = 'pay') AS subquery_2
	ON
		subquery_1.composite_id = subquery_2.composite_id
	GROUP BY
		subquery_1.tenant_id,
		subquery_1.user_id,
		hasdone) AS subquery_top
GROUP BY
	hasdone
ORDER BY
    event_average DESC;

-- Union, left join and having subquery pushdown
SELECT
	avg(array_length(events, 1)) AS event_average,
	count_pay
	FROM (
  SELECT
	subquery_1.tenant_id,
	subquery_1.user_id,
	array_agg(event ORDER BY event_time) AS events,
	COALESCE(count_pay, 0) AS count_pay
  FROM
	(
		(SELECT
			(users.composite_id).tenant_id,
			(users.composite_id).user_id,
			(users.composite_id),
			'action=>1'AS event,
			events.event_time
		FROM
			users,
			events
		WHERE
			(users.composite_id) = (events.composite_id) AND
			users.composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
			users.composite_id <= '(1, 9223372036854775807)'::user_composite_type AND
			event_type = 'click')
		UNION
		(SELECT
			(users.composite_id).tenant_id,
			(users.composite_id).user_id,
			(users.composite_id),
			'action=>2'AS event,
			events.event_time
		FROM
			users,
			events
		WHERE
			(users.composite_id) = (events.composite_id) AND
			users.composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
			users.composite_id <= '(1, 9223372036854775807)'::user_composite_type AND
			event_type = 'submit')
	) AS subquery_1
	LEFT JOIN
		(SELECT
			(composite_id).tenant_id,
			(composite_id).user_id,
			composite_id,
			COUNT(*) AS count_pay
		FROM
			events
		WHERE
			events.composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
			events.composite_id <= '(1, 9223372036854775807)'::user_composite_type AND
			event_type = 'pay'
		GROUP BY
			composite_id
		HAVING
			COUNT(*) > 2) AS subquery_2
	ON
		subquery_1.composite_id = subquery_2.composite_id
	GROUP BY
		subquery_1.tenant_id,
		subquery_1.user_id,
		count_pay) AS subquery_top
WHERE
	array_ndims(events) > 0
GROUP BY
	count_pay
ORDER BY
	count_pay;

-- Lateral join subquery pushdown
-- set subquery_pushdown since there is limit in the query
SET citus.subquery_pushdown to ON;
SELECT
	tenant_id,
	user_id,
	user_lastseen,
	event_array
FROM
	(SELECT
		tenant_id,
		user_id,
		max(lastseen) as user_lastseen,
		array_agg(event_type ORDER BY event_time) AS event_array
	FROM
		(SELECT
			(composite_id).tenant_id,
			(composite_id).user_id,
			composite_id,
			lastseen
		FROM
			users
		WHERE
			composite_id >= '(1, -9223372036854775808)'::user_composite_type AND
			composite_id <= '(1, 9223372036854775807)'::user_composite_type
		ORDER BY
			lastseen DESC
		LIMIT
			10
		) AS subquery_top
		LEFT JOIN LATERAL
			(SELECT
				event_type,
				event_time
			FROM
				events
			WHERE
				(composite_id) = subquery_top.composite_id
			ORDER BY
				event_time DESC
			LIMIT
				99) AS subquery_lateral
		ON
			true
		GROUP BY
			tenant_id,
			user_id
	) AS shard_union
ORDER BY
	user_lastseen DESC
LIMIT
	10;

-- cleanup the tables and the type & functions
-- also set the min messages to WARNING to skip
-- CASCADE NOTICE messagez
SET client_min_messages TO WARNING;
DROP TABLE users, events, subquery_pruning_varchar_test_table, keyval1, keyval2, keyvalref;

DROP TYPE user_composite_type CASCADE;

SET client_min_messages TO DEFAULT;

SET citus.subquery_pushdown to OFF;
