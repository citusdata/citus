--
-- MULTI_EXPLAIN
--
-- This test file has an alternative output because of the following in PG16:
-- https://github.com/postgres/postgres/commit/1349d2790bf48a4de072931c722f39337e72055e
-- https://github.com/postgres/postgres/commit/f4c7c410ee4a7baa06f51ebb8d5333c169691dd3
-- The alternative output can be deleted when we drop support for PG15
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16;

SET citus.next_shard_id TO 570000;

\a\t

SET citus.explain_distributed_queries TO on;
SET citus.enable_repartition_joins to ON;

-- Ensure tuple data in explain analyze output is the same on all PG versions
SET citus.enable_binary_protocol = TRUE;

-- Function that parses explain output as JSON
CREATE OR REPLACE FUNCTION explain_json(query text)
RETURNS jsonb
AS $BODY$
DECLARE
  result jsonb;
BEGIN
  EXECUTE format('EXPLAIN (FORMAT JSON) %s', query) INTO result;
  RETURN result;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION explain_analyze_json(query text)
RETURNS jsonb
AS $BODY$
DECLARE
  result jsonb;
BEGIN
  EXECUTE format('EXPLAIN (ANALYZE TRUE, FORMAT JSON) %s', query) INTO result;
  RETURN result;
END;
$BODY$ LANGUAGE plpgsql;

-- Function that parses explain output as XML
CREATE OR REPLACE FUNCTION explain_xml(query text)
RETURNS xml
AS $BODY$
DECLARE
  result xml;
BEGIN
  EXECUTE format('EXPLAIN (FORMAT XML) %s', query) INTO result;
  RETURN result;
END;
$BODY$ LANGUAGE plpgsql;

-- Function that parses explain output as XML
CREATE OR REPLACE FUNCTION explain_analyze_xml(query text)
RETURNS xml
AS $BODY$
DECLARE
  result xml;
BEGIN
  EXECUTE format('EXPLAIN (ANALYZE true, FORMAT XML) %s', query) INTO result;
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

SELECT true AS valid FROM explain_analyze_json($$
	WITH a AS (
		SELECT l_quantity, count(*) count_quantity FROM lineitem
		GROUP BY l_quantity ORDER BY count_quantity, l_quantity LIMIT 10)
	SELECT count(*) FROM a
$$);

-- Test XML format
EXPLAIN (COSTS FALSE, FORMAT XML)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Validate XML format
SELECT true AS valid FROM explain_xml($$
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity$$);

SELECT true AS valid FROM explain_analyze_xml($$
	WITH a AS (
		SELECT l_quantity, count(*) count_quantity FROM lineitem
		GROUP BY l_quantity ORDER BY count_quantity, l_quantity LIMIT 10)
	SELECT count(*) FROM a
$$);

-- Test YAML format
EXPLAIN (COSTS FALSE, FORMAT YAML)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test Text format
EXPLAIN (COSTS FALSE, FORMAT TEXT)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test analyze (with TIMING FALSE and SUMMARY FALSE for consistent output)
SELECT public.plan_normalize_memory($Q$
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;
$Q$);

-- EXPLAIN ANALYZE doesn't show worker tasks for repartition joins yet
SET citus.shard_count TO 3;
CREATE TABLE t1(a int, b int);
CREATE TABLE t2(a int, b int);
SELECT create_distributed_table('t1', 'a'), create_distributed_table('t2', 'a');
BEGIN;
SET LOCAL citus.enable_repartition_joins TO true;
EXPLAIN (COSTS off, ANALYZE on, TIMING off, SUMMARY off) SELECT count(*) FROM t1, t2 WHERE t1.a=t2.b;
-- Confirm repartiton join in distributed subplan works
EXPLAIN (COSTS off, ANALYZE on, TIMING off, SUMMARY off)
WITH repartition AS (SELECT count(*) FROM t1, t2 WHERE t1.a=t2.b)
SELECT count(*) from repartition;
END;
DROP TABLE t1, t2;

-- Test query text output, with ANALYZE ON
SELECT public.plan_normalize_memory($Q$
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE, VERBOSE TRUE)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;
$Q$);

-- Test query text output, with ANALYZE OFF
EXPLAIN (COSTS FALSE, ANALYZE FALSE, TIMING FALSE, SUMMARY FALSE, VERBOSE TRUE)
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

SELECT success FROM run_command_on_workers('alter system set enable_nestloop to off');
SELECT success FROM run_command_on_workers('alter system set enable_sort to off');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

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

SELECT success FROM run_command_on_workers('alter system reset enable_nestloop');
SELECT success FROM run_command_on_workers('alter system reset enable_sort');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

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
SELECT public.plan_normalize_memory($Q$
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;
$Q$);

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
DROP TABLE lineitem_clone;

-- ensure distributed plans don't break
EXPLAIN (COSTS FALSE) SELECT avg(l_linenumber) FROM lineitem;

-- ensure EXPLAIN EXECUTE doesn't crash
PREPARE task_tracker_query AS
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;
EXPLAIN (COSTS FALSE) EXECUTE task_tracker_query;


PREPARE router_executor_query AS SELECT l_quantity FROM lineitem WHERE l_orderkey = 5;
EXPLAIN EXECUTE router_executor_query;

PREPARE real_time_executor_query AS
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;
EXPLAIN (COSTS FALSE) EXECUTE real_time_executor_query;


-- EXPLAIN EXECUTE of parametrized prepared statements is broken, but
-- at least make sure to fail without crashing
PREPARE router_executor_query_param(int) AS SELECT l_quantity FROM lineitem WHERE l_orderkey = $1;
EXPLAIN EXECUTE router_executor_query_param(5);
EXPLAIN (ANALYZE ON, COSTS OFF, TIMING OFF, SUMMARY OFF) EXECUTE router_executor_query_param(5);

\set VERBOSITY TERSE
PREPARE multi_shard_query_param(int) AS UPDATE lineitem SET l_quantity = $1;
BEGIN;
EXPLAIN (COSTS OFF) EXECUTE multi_shard_query_param(5);
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE ON, COSTS OFF, TIMING OFF, SUMMARY OFF) EXECUTE multi_shard_query_param(5);
ROLLBACK;
\set VERBOSITY DEFAULT

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
SELECT public.explain_with_pg17_initplan_format($Q$
EXPLAIN (COSTS OFF)
WITH cte1 AS (SELECT s FROM generate_series(1,10) s)
INSERT INTO lineitem_hash_part
WITH cte1 AS (SELECT * FROM cte1 WHERE EXISTS (SELECT * FROM cte1) LIMIT 5)
SELECT s FROM cte1 WHERE EXISTS (SELECT * FROM cte1);
$Q$);

EXPLAIN (COSTS OFF)
INSERT INTO lineitem_hash_part
( SELECT s FROM generate_series(1,5) s) UNION
( SELECT s FROM generate_series(5,10) s);

-- explain with recursive planning
EXPLAIN (COSTS OFF, VERBOSE true)
WITH keys AS MATERIALIZED (
  SELECT DISTINCT l_orderkey FROM lineitem_hash_part
),
series AS  MATERIALIZED (
  SELECT s FROM generate_series(1,10) s
)
SELECT l_orderkey FROM series JOIN keys ON (s = l_orderkey)
ORDER BY s;


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


--
-- Test EXPLAIN ANALYZE udfs
--

\a\t

\set default_opts '''{"costs": false, "timing": false, "summary": false}'''::jsonb

CREATE TABLE explain_analyze_test(a int, b text);
INSERT INTO explain_analyze_test VALUES (1, 'value 1'), (2, 'value 2'), (3, 'value 3'), (4, 'value 4');

-- simple select
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', :default_opts) as (a int);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
END;

-- insert into select
BEGIN;
SELECT * FROM worker_save_query_explain_analyze($Q$
       INSERT INTO explain_analyze_test SELECT i, i::text FROM generate_series(1, 5) i $Q$,
	   :default_opts) as (a int);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
ROLLBACK;

-- select from table
BEGIN;
SELECT * FROM worker_save_query_explain_analyze($Q$SELECT * FROM explain_analyze_test$Q$,
	   											:default_opts) as (a int, b text);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
ROLLBACK;

-- insert into with returning
BEGIN;
SELECT * FROM worker_save_query_explain_analyze($Q$
       INSERT INTO explain_analyze_test SELECT i, i::text FROM generate_series(1, 5) i
	   RETURNING a, b$Q$,
	   :default_opts) as (a int, b text);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
ROLLBACK;

-- delete with returning
BEGIN;
SELECT * FROM worker_save_query_explain_analyze($Q$
       DELETE FROM explain_analyze_test WHERE a % 2 = 0
	   RETURNING a, b$Q$,
	   :default_opts) as (a int, b text);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
ROLLBACK;

-- delete without returning
BEGIN;
SELECT * FROM worker_save_query_explain_analyze($Q$
       DELETE FROM explain_analyze_test WHERE a % 2 = 0$Q$,
	   :default_opts) as (a int);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
ROLLBACK;

-- multiple queries (should ERROR)
SELECT * FROM worker_save_query_explain_analyze('SELECT 1; SELECT 2', :default_opts) as (a int);

-- error in query
SELECT * FROM worker_save_query_explain_analyze('SELECT x', :default_opts) as (a int);

-- error in format string
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', '{"format": "invlaid_format"}') as (a int);

-- test formats
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', '{"format": "text", "costs": false}') as (a int);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', '{"format": "json", "costs": false}') as (a int);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', '{"format": "xml", "costs": false}') as (a int);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', '{"format": "yaml", "costs": false}') as (a int);
SELECT explain_analyze_output FROM worker_last_saved_explain_analyze();
END;

-- costs on, timing off
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('SELECT * FROM explain_analyze_test', '{"timing": false, "costs": true}') as (a int);
SELECT explain_analyze_output ~ 'Seq Scan.*\(cost=0.00.*\) \(actual rows.*\)' FROM worker_last_saved_explain_analyze();
END;

-- costs off, timing on
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('SELECT * FROM explain_analyze_test', '{"timing": true, "costs": false}') as (a int);
SELECT explain_analyze_output ~ 'Seq Scan on explain_analyze_test \(actual time=.* rows=.* loops=1\)' FROM worker_last_saved_explain_analyze();
END;

-- summary on
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', '{"timing": false, "costs": false, "summary": true}') as (a int);
SELECT explain_analyze_output ~ 'Planning Time:.*Execution Time:.*' FROM worker_last_saved_explain_analyze();
END;

-- buffers on
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('SELECT * FROM explain_analyze_test', '{"timing": false, "costs": false, "buffers": true}') as (a int);
SELECT explain_analyze_output ~ 'Buffers:' FROM worker_last_saved_explain_analyze();
END;

-- verbose on
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('SELECT * FROM explain_analyze_test', '{"timing": false, "costs": false, "verbose": true}') as (a int);
SELECT explain_analyze_output ~ 'Output: a, b' FROM worker_last_saved_explain_analyze();
END;

-- make sure deleted at transaction end
SELECT * FROM worker_save_query_explain_analyze('SELECT 1', '{}') as (a int);
SELECT count(*) FROM worker_last_saved_explain_analyze();

-- should be deleted at the end of prepare commit
BEGIN;
SELECT * FROM worker_save_query_explain_analyze('UPDATE explain_analyze_test SET a=6 WHERE a=4', '{}') as (a int);
SELECT count(*) FROM worker_last_saved_explain_analyze();
PREPARE TRANSACTION 'citus_0_1496350_7_0';
SELECT count(*) FROM worker_last_saved_explain_analyze();
COMMIT PREPARED 'citus_0_1496350_7_0';

-- verify execution time makes sense
BEGIN;
SELECT count(*) FROM worker_save_query_explain_analyze('SELECT pg_sleep(0.05)', :default_opts) as (a int);
SELECT execution_duration BETWEEN 30 AND 200 FROM worker_last_saved_explain_analyze();
END;

--
-- verify we handle parametrized queries properly
--

CREATE TABLE t(a int);
INSERT INTO t VALUES (1), (2), (3);

-- simple case
PREPARE save_explain AS
SELECT $1, * FROM worker_save_query_explain_analyze('SELECT $1::int', :default_opts) as (a int);
EXECUTE save_explain(1);
deallocate save_explain;


-- Call a UDF first to make sure that we handle stacks of executorBoundParams properly.
--
-- The prepared statement will first call f() which will force new executor run with new
-- set of parameters. Then it will call worker_save_query_explain_analyze with a
-- parametrized query. If we don't have the correct set of parameters here, it will fail.
CREATE FUNCTION f() RETURNS INT
AS $$
PREPARE pp1 AS SELECT $1 WHERE $2 = $3;
EXECUTE pp1(4, 5, 5);
deallocate pp1;
SELECT 1$$ LANGUAGE sql volatile;

PREPARE save_explain AS
 SELECT $1, CASE WHEN i < 2 THEN
             f() = 1
		    ELSE
			 EXISTS(SELECT * FROM worker_save_query_explain_analyze('SELECT $1::int', :default_opts) as (a int)
			        WHERE a = 1)
			END
 FROM generate_series(1, 4) i;
EXECUTE save_explain(1);

deallocate save_explain;
DROP FUNCTION f();
DROP TABLE t;

SELECT * FROM explain_analyze_test ORDER BY a;

\a\t

--
-- Test different cases of EXPLAIN ANALYZE
--

SET citus.shard_count TO 4;
SET client_min_messages TO WARNING;
SELECT create_distributed_table('explain_analyze_test', 'a');

\set default_analyze_flags '(ANALYZE on, COSTS off, TIMING off, SUMMARY off)'
\set default_explain_flags '(ANALYZE off, COSTS off, TIMING off, SUMMARY off)'

-- router SELECT
EXPLAIN :default_analyze_flags SELECT * FROM explain_analyze_test WHERE a = 1;

-- multi-shard SELECT
EXPLAIN :default_analyze_flags SELECT count(*) FROM explain_analyze_test;

-- empty router SELECT
EXPLAIN :default_analyze_flags SELECT * FROM explain_analyze_test WHERE a = 10000;

-- empty multi-shard SELECT
EXPLAIN :default_analyze_flags SELECT * FROM explain_analyze_test WHERE b = 'does not exist';

-- router DML
BEGIN;
EXPLAIN :default_analyze_flags DELETE FROM explain_analyze_test WHERE a = 1;
EXPLAIN :default_analyze_flags UPDATE explain_analyze_test SET b = 'b' WHERE a = 2;
SELECT * FROM explain_analyze_test ORDER BY a;
ROLLBACK;

-- multi-shard DML
BEGIN;
EXPLAIN :default_analyze_flags UPDATE explain_analyze_test SET b = 'b' WHERE a IN (1, 2);
EXPLAIN :default_analyze_flags DELETE FROM explain_analyze_test;
SELECT * FROM explain_analyze_test ORDER BY a;
ROLLBACK;

-- router DML with RETURNING with empty result
EXPLAIN :default_analyze_flags UPDATE explain_analyze_test SET b = 'something' WHERE a = 10000 RETURNING *;
-- multi-shard DML with RETURNING with empty result
EXPLAIN :default_analyze_flags UPDATE explain_analyze_test SET b = 'something' WHERE b = 'does not exist' RETURNING *;


-- single-row insert
BEGIN;
EXPLAIN :default_analyze_flags INSERT INTO explain_analyze_test VALUES (5, 'value 5');
ROLLBACK;

-- multi-row insert
BEGIN;
EXPLAIN :default_analyze_flags INSERT INTO explain_analyze_test VALUES (5, 'value 5'), (6, 'value 6');
ROLLBACK;

-- distributed insert/select
BEGIN;
EXPLAIN :default_analyze_flags INSERT INTO explain_analyze_test SELECT * FROM explain_analyze_test;
ROLLBACK;

DROP TABLE explain_analyze_test;

-- test EXPLAIN ANALYZE works fine with primary keys
CREATE TABLE explain_pk(a int primary key, b int);
SELECT create_distributed_table('explain_pk', 'a');

BEGIN;
EXPLAIN :default_analyze_flags INSERT INTO explain_pk VALUES (1, 2), (2, 3);
SELECT * FROM explain_pk ORDER BY 1;
ROLLBACK;

-- test EXPLAIN ANALYZE with non-text output formats
BEGIN;
EXPLAIN (COSTS off, ANALYZE on, TIMING off, SUMMARY off, FORMAT JSON) INSERT INTO explain_pk VALUES (1, 2), (2, 3);
ROLLBACK;

EXPLAIN (COSTS off, ANALYZE on, TIMING off, SUMMARY off, FORMAT JSON) SELECT * FROM explain_pk;

BEGIN;
EXPLAIN (COSTS off, ANALYZE on, TIMING off, SUMMARY off, FORMAT XML) INSERT INTO explain_pk VALUES (1, 2), (2, 3);
ROLLBACK;

EXPLAIN (COSTS off, ANALYZE on, TIMING off, SUMMARY off, FORMAT XML) SELECT * FROM explain_pk;

DROP TABLE explain_pk;

-- test EXPLAIN ANALYZE with CTEs and subqueries
CREATE TABLE dist_table(a int, b int);
SELECT create_distributed_table('dist_table', 'a');
CREATE TABLE ref_table(a int);
SELECT create_reference_table('ref_table');

INSERT INTO dist_table SELECT i, i*i FROM generate_series(1, 10) i;
INSERT INTO ref_table SELECT i FROM generate_series(1, 10) i;

EXPLAIN :default_analyze_flags
WITH r AS (
	SELECT GREATEST(random(), 2) r, a FROM dist_table
)
SELECT count(distinct a) from r NATURAL JOIN ref_table;

EXPLAIN :default_analyze_flags
SELECT count(distinct a) FROM (SELECT GREATEST(random(), 2) r, a FROM dist_table) t NATURAL JOIN ref_table;

SELECT public.explain_with_pg17_initplan_format($Q$
EXPLAIN (ANALYZE on, COSTS off, TIMING off, SUMMARY off)
SELECT count(distinct a) FROM dist_table
WHERE EXISTS(SELECT random() < 2 FROM dist_table NATURAL JOIN ref_table);
$Q$);

BEGIN;
EXPLAIN :default_analyze_flags
WITH r AS (
	INSERT INTO dist_table SELECT a, a * a FROM dist_table
	RETURNING a
), s AS (
	SELECT random() < 2, a * a a2 FROM r
)
SELECT count(distinct a2) FROM s;
ROLLBACK;

-- https://github.com/citusdata/citus/issues/4074
prepare ref_select(int) AS select * from ref_table where 1 = $1;
explain :default_analyze_flags execute ref_select(1);
deallocate ref_select;

DROP TABLE ref_table, dist_table;

-- test EXPLAIN ANALYZE with different replication factors
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;
CREATE TABLE dist_table_rep1(a int);
SELECT create_distributed_table('dist_table_rep1', 'a');

SET citus.shard_replication_factor = 2;
CREATE TABLE dist_table_rep2(a int);
SELECT create_distributed_table('dist_table_rep2', 'a');

EXPLAIN :default_analyze_flags INSERT INTO dist_table_rep1 VALUES(1), (2), (3), (4), (10), (100) RETURNING *;
EXPLAIN :default_analyze_flags SELECT * from dist_table_rep1;

EXPLAIN :default_analyze_flags INSERT INTO dist_table_rep2 VALUES(1), (2), (3), (4), (10), (100) RETURNING *;
EXPLAIN :default_analyze_flags SELECT * from dist_table_rep2;

prepare p1 as SELECT * FROM dist_table_rep1;
EXPLAIN :default_analyze_flags EXECUTE p1;
EXPLAIN :default_analyze_flags EXECUTE p1;
EXPLAIN :default_analyze_flags EXECUTE p1;
EXPLAIN :default_analyze_flags EXECUTE p1;
EXPLAIN :default_analyze_flags EXECUTE p1;
EXPLAIN :default_analyze_flags EXECUTE p1;

prepare p2 AS SELECT * FROM dist_table_rep1 WHERE a = $1;
EXPLAIN :default_analyze_flags EXECUTE p2(1);
EXPLAIN :default_analyze_flags EXECUTE p2(1);
EXPLAIN :default_analyze_flags EXECUTE p2(1);
EXPLAIN :default_analyze_flags EXECUTE p2(1);
EXPLAIN :default_analyze_flags EXECUTE p2(1);
EXPLAIN :default_analyze_flags EXECUTE p2(1);
EXPLAIN :default_analyze_flags EXECUTE p2(10);
EXPLAIN :default_analyze_flags EXECUTE p2(100);

prepare p3 AS SELECT * FROM dist_table_rep1 WHERE a = 1;
EXPLAIN :default_analyze_flags EXECUTE p3;
EXPLAIN :default_analyze_flags EXECUTE p3;
EXPLAIN :default_analyze_flags EXECUTE p3;
EXPLAIN :default_analyze_flags EXECUTE p3;
EXPLAIN :default_analyze_flags EXECUTE p3;
EXPLAIN :default_analyze_flags EXECUTE p3;

DROP TABLE dist_table_rep1, dist_table_rep2;

-- https://github.com/citusdata/citus/issues/2009
CREATE TABLE simple (id integer, name text);
SELECT create_distributed_table('simple', 'id');
PREPARE simple_router AS SELECT *, $1 FROM simple WHERE id = 1;

EXPLAIN :default_explain_flags EXECUTE simple_router(1);
EXPLAIN :default_analyze_flags EXECUTE simple_router(1);
EXPLAIN :default_analyze_flags EXECUTE simple_router(1);
EXPLAIN :default_analyze_flags EXECUTE simple_router(1);
EXPLAIN :default_analyze_flags EXECUTE simple_router(1);
EXPLAIN :default_analyze_flags EXECUTE simple_router(1);
EXPLAIN :default_analyze_flags EXECUTE simple_router(1);
EXPLAIN :default_analyze_flags EXECUTE simple_router(1);

deallocate simple_router;

-- prepared multi-row insert
PREPARE insert_query AS INSERT INTO simple VALUES ($1, 2), (2, $2);
EXPLAIN :default_explain_flags EXECUTE insert_query(3, 4);
EXPLAIN :default_analyze_flags EXECUTE insert_query(3, 4);
deallocate insert_query;

-- prepared updates
PREPARE update_query AS UPDATE simple SET name=$1 WHERE name=$2;
EXPLAIN :default_explain_flags EXECUTE update_query('x', 'y');
EXPLAIN :default_analyze_flags EXECUTE update_query('x', 'y');
deallocate update_query;

-- prepared deletes
PREPARE delete_query AS DELETE FROM simple WHERE name=$1 OR name=$2;
EXPLAIN (COSTS OFF) EXECUTE delete_query('x', 'y');
EXPLAIN :default_analyze_flags EXECUTE delete_query('x', 'y');
deallocate delete_query;

-- prepared distributed insert/select
-- we don't support EXPLAIN for prepared insert/selects of other types.
PREPARE distributed_insert_select AS INSERT INTO simple SELECT * FROM simple WHERE name IN ($1, $2);
EXPLAIN :default_explain_flags EXECUTE distributed_insert_select('x', 'y');
EXPLAIN :default_analyze_flags EXECUTE distributed_insert_select('x', 'y');
deallocate distributed_insert_select;

DROP TABLE simple;

-- prepared cte
BEGIN;
PREPARE cte_query AS
WITH keys AS (
  SELECT count(*) FROM
   (SELECT DISTINCT l_orderkey, GREATEST(random(), 2) FROM lineitem_hash_part WHERE l_quantity > $1) t
),
series AS (
  SELECT s FROM generate_series(1, $2) s
),
delete_result AS (
  DELETE FROM lineitem_hash_part WHERE l_quantity < $3 RETURNING *
)
SELECT s FROM series;

EXPLAIN :default_explain_flags EXECUTE cte_query(2, 10, -1);
EXPLAIN :default_analyze_flags EXECUTE cte_query(2, 10, -1);

ROLLBACK;

-- https://github.com/citusdata/citus/issues/2009#issuecomment-653036502
CREATE TABLE users_table_2 (user_id int primary key, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint);
SELECT create_reference_table('users_table_2');

PREPARE p4 (int, int) AS insert into users_table_2 ( value_1, user_id) select value_1, user_id + $2  FROM users_table_2 ON CONFLICT (user_id) DO UPDATE SET value_2 = EXCLUDED.value_1 + $1;
EXPLAIN :default_explain_flags execute p4(20,20);
EXPLAIN :default_analyze_flags execute p4(20,20);

-- simple test to confirm we can fetch long (>4KB) plans
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM users_table_2 WHERE value_1::text = '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000X';

DROP TABLE users_table_2;

-- sorted explain analyze output
CREATE TABLE explain_analyze_execution_time (a int);
INSERT INTO explain_analyze_execution_time VALUES (2);
SELECT create_distributed_table('explain_analyze_execution_time', 'a');
-- show that we can sort the output wrt execution time
-- we do the following hack to make the test outputs
-- be consistent. First, ingest a single row then add
-- pg_sleep() call on the query. Postgres will only
-- sleep for the shard that has the single row, so that
-- will definitely be slower
set citus.explain_analyze_sort_method to "taskId";
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE) select a, CASE WHEN pg_sleep(0.4) IS NULL THEN  'x' END from explain_analyze_execution_time;
set citus.explain_analyze_sort_method to "execution-time";
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE) select a, CASE WHEN pg_sleep(0.4) IS NULL THEN  'x' END from explain_analyze_execution_time;
-- reset back
reset citus.explain_analyze_sort_method;
DROP TABLE explain_analyze_execution_time;

CREATE SCHEMA multi_explain;
SET search_path TO multi_explain;

-- test EXPLAIN ANALYZE when original query returns no columns
CREATE TABLE reference_table(a int);
SELECT create_reference_table('reference_table');
INSERT INTO reference_table VALUES (1);

EXPLAIN :default_analyze_flags SELECT FROM reference_table;

CREATE TABLE distributed_table_1(a int, b int);
SELECT create_distributed_table('distributed_table_1','a');
INSERT INTO distributed_table_1 values (1,1);

EXPLAIN :default_analyze_flags SELECT row_number() OVER() AS r FROM distributed_table_1;

CREATE TABLE distributed_table_2(a int, b int);
SELECT create_distributed_table('distributed_table_2','a');
INSERT INTO distributed_table_2 VALUES (1,1);

EXPLAIN :default_analyze_flags
WITH r AS (SELECT row_number() OVER () AS r FROM distributed_table_1)
SELECT * FROM distributed_table_2
JOIN r ON (r = distributed_table_2.b)
LIMIT 3;

EXPLAIN :default_analyze_flags SELECT FROM (SELECT * FROM reference_table) subquery;

PREPARE dummy_prep_stmt(int) AS SELECT FROM distributed_table_1;
EXPLAIN :default_analyze_flags EXECUTE dummy_prep_stmt(50);

CREATE TYPE multi_explain.int_wrapper_type AS (int_field int);
CREATE TABLE tbl (a int, b multi_explain.int_wrapper_type);
SELECT create_distributed_table('tbl', 'a');

EXPLAIN :default_analyze_flags SELECT * FROM tbl;

PREPARE q1(int_wrapper_type) AS WITH a AS (SELECT * FROM tbl WHERE b = $1 AND a = 1 OFFSET 0) SELECT * FROM a;
EXPLAIN (COSTS false) EXECUTE q1('(1)');
EXPLAIN :default_analyze_flags EXECUTE q1('(1)');

PREPARE q2(int_wrapper_type) AS WITH a AS (UPDATE tbl SET b = $1 WHERE a = 1 RETURNING *) SELECT * FROM a;
EXPLAIN (COSTS false) EXECUTE q2('(1)');
EXPLAIN :default_analyze_flags EXECUTE q2('(1)');

-- check when auto explain + analyze is enabled, we do not allow local execution.
CREATE SCHEMA test_auto_explain;
SET search_path TO 'test_auto_explain';

CREATE TABLE test_ref_table (key int PRIMARY KEY);
SELECT create_reference_table('test_ref_table');

LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
set auto_explain.log_analyze to true;

-- the following should not be locally executed since explain analyze is on
select * from test_ref_table;

DROP SCHEMA test_auto_explain CASCADE;

SET client_min_messages TO ERROR;
DROP SCHEMA multi_explain CASCADE;
