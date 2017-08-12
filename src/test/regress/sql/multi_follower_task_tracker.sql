\c - - - :master_port

-- do some setup

CREATE TABLE tab(a int, b int);
SELECT create_distributed_table('tab', 'a');

INSERT INTO tab (a, b) VALUES (1, 1);
INSERT INTO tab (a, b) VALUES (1, 2);

\c - - - :follower_master_port

SET citus.task_executor_type TO 'real-time';
SELECT * FROM tab;
SET citus.task_executor_type TO 'task-tracker';
SELECT * FROM tab;

-- clean up

\c - - - :master_port

DROP TABLE tab;
