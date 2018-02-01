--
-- MULTI_MX_EXPLAIN
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1320000;
\c - - - :worker_1_port
\c - - - :worker_2_port
\c - - - :master_port

\a\t

SET citus.task_executor_type TO 'real-time';
SET citus.explain_distributed_queries TO on;

VACUUM ANALYZE lineitem_mx;
VACUUM ANALYZE orders_mx;
VACUUM ANALYZE customer_mx;
VACUUM ANALYZE supplier_mx;

\c - - - :worker_1_port
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

\c - - - :worker_2_port
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
	SELECT l_quantity, count(*) count_quantity FROM lineitem_mx
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test JSON format
EXPLAIN (COSTS FALSE, FORMAT JSON)
	SELECT l_quantity, count(*) count_quantity FROM lineitem_mx
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Validate JSON format
SELECT true AS valid FROM explain_json($$
	SELECT l_quantity, count(*) count_quantity FROM lineitem_mx
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity$$);

\c - - - :worker_1_port

-- Test XML format
EXPLAIN (COSTS FALSE, FORMAT XML)
	SELECT l_quantity, count(*) count_quantity FROM lineitem_mx
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Validate XML format
SELECT true AS valid FROM explain_xml($$
	SELECT l_quantity, count(*) count_quantity FROM lineitem_mx
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity$$);

-- Test YAML format
EXPLAIN (COSTS FALSE, FORMAT YAML)
	SELECT l_quantity, count(*) count_quantity FROM lineitem_mx
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test Text format
EXPLAIN (COSTS FALSE, FORMAT TEXT)
	SELECT l_quantity, count(*) count_quantity FROM lineitem_mx
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

\c - - - :worker_2_port

-- Test verbose
EXPLAIN (COSTS FALSE, VERBOSE TRUE)
	SELECT sum(l_quantity) / avg(l_quantity) FROM lineitem_mx;

-- Test join
EXPLAIN (COSTS FALSE)
	SELECT * FROM lineitem_mx
	JOIN orders_mx ON l_orderkey = o_orderkey AND l_quantity < 5.0
	ORDER BY l_quantity LIMIT 10;

-- Test insert
EXPLAIN (COSTS FALSE)
	INSERT INTO lineitem_mx VALUES(1,0);

-- Test update
EXPLAIN (COSTS FALSE)
	UPDATE lineitem_mx
	SET l_suppkey = 12
	WHERE l_orderkey = 1 AND l_partkey = 0;

-- Test delete
EXPLAIN (COSTS FALSE)
	DELETE FROM lineitem_mx
	WHERE l_orderkey = 1 AND l_partkey = 0;

-- make the outputs more consistent
VACUUM ANALYZE lineitem_mx;
VACUUM ANALYZE orders_mx;
VACUUM ANALYZE customer_mx;
VACUUM ANALYZE supplier_mx;

-- Test single-shard SELECT
EXPLAIN (COSTS FALSE)
	SELECT l_quantity FROM lineitem_mx WHERE l_orderkey = 5;

SELECT true AS valid FROM explain_xml($$
	SELECT l_quantity FROM lineitem_mx WHERE l_orderkey = 5$$);

SELECT true AS valid FROM explain_json($$
	SELECT l_quantity FROM lineitem_mx WHERE l_orderkey = 5$$);

-- Test CREATE TABLE ... AS
EXPLAIN (COSTS FALSE)
	CREATE TABLE explain_result AS
	SELECT * FROM lineitem_mx;

-- Test all tasks output
SET citus.explain_all_tasks TO on;

EXPLAIN (COSTS FALSE)
	SELECT avg(l_linenumber) FROM lineitem_mx WHERE l_orderkey > 9030;

SELECT true AS valid FROM explain_xml($$
	SELECT avg(l_linenumber) FROM lineitem_mx WHERE l_orderkey > 9030$$);

SELECT true AS valid FROM explain_json($$
	SELECT avg(l_linenumber) FROM lineitem_mx WHERE l_orderkey > 9030$$);

-- Test track tracker
SET citus.task_executor_type TO 'task-tracker';
SET citus.explain_all_tasks TO off;

EXPLAIN (COSTS FALSE)
	SELECT avg(l_linenumber) FROM lineitem_mx WHERE l_orderkey > 9030;

-- Test re-partition join

EXPLAIN (COSTS FALSE)
	SELECT count(*)
	FROM lineitem_mx, orders_mx, customer_mx, supplier_mx
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

EXPLAIN (COSTS FALSE, FORMAT JSON)
	SELECT count(*)
	FROM lineitem_mx, orders_mx, customer_mx, supplier_mx
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

SELECT true AS valid FROM explain_json($$
	SELECT count(*)
	FROM lineitem_mx, orders_mx, customer_mx, supplier_mx
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey$$);

EXPLAIN (COSTS FALSE, FORMAT XML)
	SELECT count(*)
	FROM lineitem_mx, orders_mx, customer_mx, supplier_mx
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;

SELECT true AS valid FROM explain_xml($$
	SELECT count(*)
	FROM lineitem_mx, orders_mx, customer_mx, supplier_mx
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey$$);

EXPLAIN (COSTS FALSE, FORMAT YAML)
	SELECT count(*)
	FROM lineitem_mx, orders_mx, customer_mx, supplier_mx
	WHERE l_orderkey = o_orderkey
	AND o_custkey = c_custkey
	AND l_suppkey = s_suppkey;
