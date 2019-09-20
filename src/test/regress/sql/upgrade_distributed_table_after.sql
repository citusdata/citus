SET search_path TO upgrade_distributed_table_before, public;

SELECT * FROM t ORDER BY a;
SELECT * FROM t WHERE a = 1;

INSERT INTO t SELECT * FROM generate_series(10, 15);

SELECT * FROM t WHERE a = 10;
SELECT * FROM t WHERE a = 11;

-- test task-tracker executor
SET citus.task_executor_type TO 'task-tracker';

SELECT * FROM t WHERE a = 10;
SELECT * FROM t WHERE a = 11;
SELECT * FROM t WHERE a < 3 ORDER BY a;

RESET citus.task_executor_type;

-- test adaptive executor
SET citus.task_executor_type to "adaptive";

SELECT * FROM t WHERE a = 10;
SELECT * FROM t WHERE a = 11;
SELECT * FROM t WHERE a < 3 ORDER BY a;

RESET citus.task_executor_type;

-- test real-time executor
SET citus.task_executor_type to "real-time";

SELECT * FROM t WHERE a = 10;
SELECT * FROM t WHERE a = 11;
SELECT * FROM t WHERE a < 3 ORDER BY a;

RESET citus.task_executor_type;

-- test distributed type
INSERT INTO t1 VALUES (1, (2,3)::tc1);
SELECT * FROM t1;
ALTER TYPE tc1 RENAME TO tc1_newname;
INSERT INTO t1 VALUES (3, (4,5)::tc1_newname);

TRUNCATE TABLE t;

SELECT * FROM T;

DROP TABLE t;

DROP SCHEMA upgrade_distributed_table_before CASCADE;
