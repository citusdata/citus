--
-- MULTI_EXPLAIN
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 570000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 570000;


\a\t

SET citus.task_executor_type TO 'real-time';
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

-- Test Text format
EXPLAIN (COSTS FALSE, FORMAT TEXT)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

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
	INSERT INTO lineitem VALUES(1,0);

-- Test update
EXPLAIN (COSTS FALSE)
	UPDATE lineitem
	SET l_suppkey = 12
	WHERE l_orderkey = 1 AND l_partkey = 0;

-- Test delete
EXPLAIN (COSTS FALSE)
	DELETE FROM lineitem
	WHERE l_orderkey = 1 AND l_partkey = 0;

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

-- Test all tasks output
SET citus.explain_all_tasks TO on;

EXPLAIN (COSTS FALSE)
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;

SELECT true AS valid FROM explain_xml($$
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030$$);

SELECT true AS valid FROM explain_json($$
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030$$);

-- Test track tracker
SET citus.task_executor_type TO 'task-tracker';
SET citus.explain_all_tasks TO off;

EXPLAIN (COSTS FALSE)
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;

-- Test re-partition join
SET citus.large_table_shard_count TO 1;

EXPLAIN (COSTS FALSE)
	SELECT count(*)
	FROM lineitem, orders, customer, supplier
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

EXPLAIN (COSTS FALSE, FORMAT JSON)
	SELECT count(*)
	FROM lineitem, orders, customer, supplier
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

SELECT true AS valid FROM explain_json($$
	SELECT count(*)
	FROM lineitem, orders, customer, supplier
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey$$);

EXPLAIN (COSTS FALSE, FORMAT XML)
	SELECT count(*)
	FROM lineitem, orders, customer, supplier
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

SELECT true AS valid FROM explain_xml($$
	SELECT count(*)
	FROM lineitem, orders, customer, supplier
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey$$);

EXPLAIN (COSTS FALSE, FORMAT YAML)
	SELECT count(*)
	FROM lineitem, orders, customer, supplier
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;
