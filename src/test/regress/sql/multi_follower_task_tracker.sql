\c - - :master_host :master_port

-- do some setup

CREATE TABLE tab(a int, b int);
SELECT create_distributed_table('tab', 'a');

INSERT INTO tab (a, b) VALUES (1, 1);
INSERT INTO tab (a, b) VALUES (1, 2);

\c - - - :follower_master_port

RESET citus.task_executor_type;
SELECT * FROM tab;
SET citus.task_executor_type TO 'task-tracker';
SELECT * FROM tab;

-- clean up

\c - - :master_host :master_port

DROP TABLE tab;
