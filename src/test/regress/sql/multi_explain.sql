--
-- MULTI_EXPLAIN
--

SET citus.next_shard_id TO 570000;

\a\t

RESET citus.task_executor_type;
SET citus.explain_distributed_queries TO on;

-- Function that parses explain output as JSON
CREATE FUNCTION explain_json(query text)
RETURNS jsonb
AS $BODY$
DECLARE
  result jsonb;
BEGIN
  EXECUTE format('EXPLAIN (FORMAT JSON) %s', query) INTO result;
  RETURN result;
END;
$BODY$ LANGUAGE plpgsql;

-- Function that parses explain output as XML
CREATE FUNCTION explain_xml(query text)
RETURNS xml
AS $BODY$
DECLARE
  result xml;
BEGIN
  EXECUTE format('EXPLAIN (FORMAT XML) %s', query) INTO result;
  RETURN result;
END;
$BODY$ LANGUAGE plpgsql;

-- VACUMM related tables to ensure test outputs are stable
VACUUM ANALYZE lineitem;
VACUUM ANALYZE orders;

-- Test Text format
EXPLAIN (COSTS FALSE, FORMAT TEXT)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test disable hash aggregate
SET enable_hashagg TO off;
EXPLAIN (COSTS FALSE, FORMAT TEXT)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

SET enable_hashagg TO on;

-- Test JSON format
EXPLAIN (COSTS FALSE, FORMAT JSON)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Validate JSON format
SELECT true AS valid FROM explain_json($$
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity$$);

-- Test XML format
EXPLAIN (COSTS FALSE, FORMAT XML)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Validate XML format
SELECT true AS valid FROM explain_xml($$
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity$$);

-- Test YAML format
EXPLAIN (COSTS FALSE, FORMAT YAML)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test Text format
EXPLAIN (COSTS FALSE, FORMAT TEXT)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test analyze (with TIMING FALSE and SUMMARY FALSE for consistent output)
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test verbose
EXPLAIN (COSTS FALSE, VERBOSE TRUE)
	SELECT sum(l_quantity) / avg(l_quantity) FROM lineitem;

-- Test join
EXPLAIN (COSTS FALSE)
	SELECT * FROM lineitem
	JOIN orders ON l_orderkey = o_orderkey AND l_quantity < 5.0
	ORDER BY l_quantity LIMIT 10;

-- Test insert
EXPLAIN (COSTS FALSE)
	INSERT INTO lineitem VALUES (1,0), (2, 0), (3, 0), (4, 0);

-- Test update
EXPLAIN (COSTS FALSE)
	UPDATE lineitem
	SET l_suppkey = 12
	WHERE l_orderkey = 1 AND l_partkey = 0;

-- Test analyze (with TIMING FALSE and SUMMARY FALSE for consistent output)
BEGIN;
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE)
	UPDATE lineitem
	SET l_suppkey = 12
	WHERE l_orderkey = 1 AND l_partkey = 0;
ROLLBACk;

-- Test delete
EXPLAIN (COSTS FALSE)
	DELETE FROM lineitem
	WHERE l_orderkey = 1 AND l_partkey = 0;

-- Test zero-shard update
EXPLAIN (COSTS FALSE)
	UPDATE lineitem
	SET l_suppkey = 12
	WHERE l_orderkey = 1 AND l_orderkey = 0;

-- Test zero-shard delete
EXPLAIN (COSTS FALSE)
	DELETE FROM lineitem
	WHERE l_orderkey = 1 AND l_orderkey = 0;

-- Test single-shard SELECT
EXPLAIN (COSTS FALSE)
	SELECT l_quantity FROM lineitem WHERE l_orderkey = 5;

SELECT true AS valid FROM explain_xml($$
	SELECT l_quantity FROM lineitem WHERE l_orderkey = 5$$);

SELECT true AS valid FROM explain_json($$
	SELECT l_quantity FROM lineitem WHERE l_orderkey = 5$$);

-- Test CREATE TABLE ... AS
EXPLAIN (COSTS FALSE)
	CREATE TABLE explain_result AS
	SELECT * FROM lineitem;

-- Test having
EXPLAIN (COSTS FALSE, VERBOSE TRUE)
	SELECT sum(l_quantity) / avg(l_quantity) FROM lineitem
	HAVING sum(l_quantity) > 100;

-- Test having without aggregate
EXPLAIN (COSTS FALSE, VERBOSE TRUE)
	SELECT l_quantity FROM lineitem
	GROUP BY l_quantity
	HAVING l_quantity > (100 * random());


-- Subquery pushdown tests with explain
EXPLAIN (COSTS OFF)
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
EXPLAIN (COSTS OFF)
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
	hasdone;

-- Union, left join and having subquery pushdown
EXPLAIN (COSTS OFF)
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
-- set subquery_pushdown due to limit in the query
SET citus.subquery_pushdown to ON;
EXPLAIN (COSTS OFF)
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

RESET citus.subquery_pushdown;

-- Test all tasks output
SET citus.explain_all_tasks TO on;

EXPLAIN (COSTS FALSE)
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;

SELECT true AS valid FROM explain_xml($$
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030$$);

SELECT true AS valid FROM explain_json($$
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030$$);

-- Test multi shard update
EXPLAIN (COSTS FALSE)
	UPDATE lineitem_hash_part
	SET l_suppkey = 12;

EXPLAIN (COSTS FALSE)
	UPDATE lineitem_hash_part
	SET l_suppkey = 12
	WHERE l_orderkey = 1 OR l_orderkey = 3;

-- Test multi shard delete
EXPLAIN (COSTS FALSE)
	DELETE FROM lineitem_hash_part;

-- Test analyze (with TIMING FALSE and SUMMARY FALSE for consistent output)
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

SET citus.explain_all_tasks TO off;

-- Test update with subquery
EXPLAIN (COSTS FALSE)
	UPDATE lineitem_hash_part
	SET l_suppkey = 12
	FROM orders_hash_part
	WHERE orders_hash_part.o_orderkey = lineitem_hash_part.l_orderkey;

-- Test delete with subquery
EXPLAIN (COSTS FALSE)
	DELETE FROM lineitem_hash_part
	USING orders_hash_part
	WHERE orders_hash_part.o_orderkey = lineitem_hash_part.l_orderkey;

-- Test track tracker
SET citus.task_executor_type TO 'task-tracker';

EXPLAIN (COSTS FALSE)
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;

-- Test re-partition join

EXPLAIN (COSTS FALSE)
	SELECT count(*)
	FROM lineitem, orders, customer_append, supplier_single_shard
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

EXPLAIN (COSTS FALSE, FORMAT JSON)
	SELECT count(*)
	FROM lineitem, orders, customer_append, supplier_single_shard
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

SELECT true AS valid FROM explain_json($$
	SELECT count(*)
	FROM lineitem, orders, customer_append, supplier_single_shard
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey$$);

EXPLAIN (COSTS FALSE, FORMAT XML)
	SELECT count(*)
	FROM lineitem, orders, customer_append, supplier_single_shard
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

SELECT true AS valid FROM explain_xml($$
	SELECT count(*)
	FROM lineitem, orders, customer_append, supplier
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey$$);

-- make sure that EXPLAIN works without
-- problems for queries that inlvolves only
-- reference tables
SELECT true AS valid FROM explain_xml($$
	SELECT count(*)
	FROM nation
	WHERE n_name = 'CHINA'$$);

SELECT true AS valid FROM explain_xml($$
	SELECT count(*)
	FROM nation, supplier
	WHERE nation.n_nationkey = supplier.s_nationkey$$);


EXPLAIN (COSTS FALSE, FORMAT YAML)
	SELECT count(*)
	FROM lineitem, orders, customer, supplier_single_shard
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

-- ensure local plans display correctly
CREATE TABLE lineitem_clone (LIKE lineitem);
EXPLAIN (COSTS FALSE) SELECT avg(l_linenumber) FROM lineitem_clone;

-- ensure distributed plans don't break
EXPLAIN (COSTS FALSE) SELECT avg(l_linenumber) FROM lineitem;

-- ensure EXPLAIN EXECUTE doesn't crash
PREPARE task_tracker_query AS
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;
EXPLAIN (COSTS FALSE) EXECUTE task_tracker_query;

RESET citus.task_executor_type;

PREPARE router_executor_query AS SELECT l_quantity FROM lineitem WHERE l_orderkey = 5;
EXPLAIN EXECUTE router_executor_query;

PREPARE real_time_executor_query AS
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;
EXPLAIN (COSTS FALSE) EXECUTE real_time_executor_query;

-- EXPLAIN EXECUTE of parametrized prepared statements is broken, but
-- at least make sure to fail without crashing
PREPARE router_executor_query_param(int) AS SELECT l_quantity FROM lineitem WHERE l_orderkey = $1;
EXPLAIN EXECUTE router_executor_query_param(5);

-- test explain in a transaction with alter table to test we use right connections
BEGIN;

CREATE TABLE explain_table(id int);
SELECT create_distributed_table('explain_table', 'id');

ALTER TABLE explain_table ADD COLUMN value int;

ROLLBACK;

-- test explain with local INSERT ... SELECT
EXPLAIN (COSTS OFF)
INSERT INTO lineitem_hash_part
SELECT o_orderkey FROM orders_hash_part LIMIT 3;

SELECT true AS valid FROM explain_json($$
  INSERT INTO lineitem_hash_part (l_orderkey)
  SELECT o_orderkey FROM orders_hash_part LIMIT 3;
$$);

EXPLAIN (COSTS OFF)
INSERT INTO lineitem_hash_part (l_orderkey, l_quantity)
SELECT o_orderkey, 5 FROM orders_hash_part LIMIT 3;

EXPLAIN (COSTS OFF)
INSERT INTO lineitem_hash_part (l_orderkey)
SELECT s FROM generate_series(1,5) s;

-- WHERE EXISTS forces pg12 to materialize cte
EXPLAIN (COSTS OFF)
WITH cte1 AS (SELECT s FROM generate_series(1,10) s)
INSERT INTO lineitem_hash_part
WITH cte1 AS (SELECT * FROM cte1 WHERE EXISTS (SELECT * FROM cte1) LIMIT 5)
SELECT s FROM cte1 WHERE EXISTS (SELECT * FROM cte1);

EXPLAIN (COSTS OFF)
INSERT INTO lineitem_hash_part
( SELECT s FROM generate_series(1,5) s) UNION
( SELECT s FROM generate_series(5,10) s);

-- explain with recursive planning
-- prevent PG 11 - PG 12 outputs to diverge
SET citus.enable_cte_inlining TO false;
EXPLAIN (COSTS OFF, VERBOSE true)
WITH keys AS (
  SELECT DISTINCT l_orderkey FROM lineitem_hash_part
),
series AS (
  SELECT s FROM generate_series(1,10) s
)
SELECT l_orderkey FROM series JOIN keys ON (s = l_orderkey)
ORDER BY s;

SET citus.enable_cte_inlining TO true;

SELECT true AS valid FROM explain_json($$
  WITH result AS (
    SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity
  ),
  series AS (
    SELECT s FROM generate_series(1,10) s
  )
  SELECT * FROM result JOIN series ON (s = count_quantity) JOIN orders_hash_part ON (s = o_orderkey)
$$);

SELECT true AS valid FROM explain_xml($$
  WITH result AS (
    SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity
  ),
  series AS (
    SELECT s FROM generate_series(1,10) s
  )
  SELECT * FROM result JOIN series ON (s = l_quantity) JOIN orders_hash_part ON (s = o_orderkey)
$$);
