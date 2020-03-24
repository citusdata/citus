-- Test two concurrent reparttition joins from two different workers
-- This test runs the below query from the :worker_1_port and the
-- concurrent test runs the same query on :worker_2_port. Note that, both
-- tests use the same sequence ids but the queries should not fail.
\c - - :public_worker_1_host :worker_1_port

SET citus.task_executor_type TO "task-tracker";
CREATE TEMP TABLE t1 AS
SELECT
    l1.l_comment
FROM
    lineitem_mx l1, orders_mx l2
WHERE
	l1.l_comment = l2.o_comment;
