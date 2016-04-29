--
-- MULTI_EXPLAIN
--

\a\t

SET citus.explain_distributed_queries TO on;

-- Test Text format
EXPLAIN (COSTS FALSE, FORMAT TEXT)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test JSON format
EXPLAIN (COSTS FALSE, FORMAT JSON)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

-- Test XML format
EXPLAIN (COSTS FALSE, FORMAT XML)
	SELECT l_quantity, count(*) count_quantity FROM lineitem
	GROUP BY l_quantity ORDER BY count_quantity, l_quantity;

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
	JOIN orders ON l_orderkey = o_orderkey AND l_quantity < 5
	ORDER BY l_quantity DESC LIMIT 10;

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
EXPLAIN 
	SELECT l_quantity FROM lineitem WHERE l_orderkey = 5;

-- Test CREATE TABLE ... AS
EXPLAIN CREATE TABLE explain_result AS
	SELECT * FROM lineitem;

-- Test all tasks output
SET citus.explain_all_tasks TO on;

EXPLAIN (COSTS FALSE)
	SELECT avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;

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
